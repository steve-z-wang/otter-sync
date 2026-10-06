// Actual Node -> Rust -> PostgreSQL protocol4 vertical paths and fences.
import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {Pool} from 'pg';
import {WebSocket} from 'ws';
import {once} from 'node:events';
import {createBackend,MutationRejected} from '../../../packages/server/index.mts';
import {pg} from '../../../packages/postgres/index.mts';
const native=createRequire(import.meta.url)('../../../bindings/node/axton-node.node');
const pool=new Pool({connectionString:process.env.DATABASE_URL});const database=pg(pool);
const context={protocol:4,binding:{backend:'api',viewer:'alice',stream:'User:alice',contract:'app'},materialization:'pending',incarnation:'store1'};
const fields=['id','title'].map(name=>({name,nullable:false,type:{kind:'scalar',name:'string'}}));
const model={name:'Todo',version:1,identity:['id'],fields};
const output={name:'todo',kind:'model',cardinality:'single',source:'handlerIdentity',model:'Todo',modelReadVersion:1,handlerType:{kind:'identity',model:'Todo',fields:[{name:'id',type:{kind:'scalar',name:'string'}}]}};
const config={schema:{enums:[],models:[model],resultModels:[{...model,enums:[]}],actions:[{name:'Find',version:1,kind:'query',inputs:[],outputs:[output]}]},mutations:[],loaders:['Todo']};
context.materialization=native.serverMaterializationId(JSON.stringify(config),'1');
let call=0;const id=()=>`01890f47-1234-7123-8123-${String(++call).padStart(12,'0')}`;
const protocol4={backendId:'api',contractId:'app',projectionGeneration:'1',authorizeStream:(viewer,stream)=>stream===`User:${viewer}`};
const q=async(sql,args=[])=> (await pool.query(sql,args)).rows;
const loader=async({tx,ids})=>Promise.all(ids.map(async({id})=>(await tx.query('SELECT id,title FROM v04_todo WHERE id=$1',[id])).rows[0]??null));
const backend=(queries)=>createBackend({config,native,database,protocol4,authenticate:()=> 'alice',loaders:{todo:loader},queries});
before(async()=>{await q(await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8'));await q('CREATE TABLE v04_todo(id text PRIMARY KEY,title text NOT NULL)');await q("INSERT INTO v04_todo VALUES('t1','A'),('race','A')");});
after(()=>pool.end());
test('Query/Fetch snapshots and a background write reach the actual Stream path',async()=>{
 let app=backend({find:async({ctx})=>{ctx.stream('User:alice').track.todo({id:'t1'});return {todo:{id:'t1'}};}});
 for(const store of [true,false]){const reply=JSON.parse(await app.fetch('alice',JSON.stringify({context,callId:id(),model:'Todo',version:1,identity:{id:'t1'},store})));assert.deepEqual(reply.records,[{model:'Todo',identity:{id:'t1'},cursor:null,state:{title:'A'}}]);}
 assert.equal((await q('SELECT count(*)::int n FROM axton_record'))[0].n,0,'ordinary Fetch allocates no identity authority');
 const queryId=id();const query=JSON.parse(await app.action('alice',JSON.stringify({context,callId:queryId,name:'Find',version:1,args:{},store:false})));assert.equal(query.records[0].cursor,null);assert.equal((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0].head,'1');
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title='B' WHERE id='t1'");invalidate.todo({id:'t1'});});
 const request={context,callId:id(),after:0,models:{Todo:1},limit:1};const page=JSON.parse(await app.pull('alice',JSON.stringify(request)));assert.equal(page.to,1);assert.equal(page.head,2);assert.equal(page.units[0].changes[0].record.cursor,2);assert.equal(page.units[0].changes[0].record.state.title,'B');
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title='C' WHERE id='t1'");invalidate.todo({id:'t1'});});
 assert.deepEqual(JSON.parse(await app.pull('alice',JSON.stringify(request))),page,'saved page is immutable after another commit');
 assert.deepEqual(JSON.parse(await app.action('alice',JSON.stringify({context,callId:queryId,name:'Find',version:1,args:{},store:false}))),query,'Query replay does not track again');
 await assert.rejects(app.fetch('bob',JSON.stringify({context,callId:id(),model:'Todo',version:1,identity:{id:'t1'}})),/context_mismatch/);
});
test('Query track upgrade retries the whole stale Serializable transaction before saving',async()=>{
 let attempts=0;let read;const readStarted=new Promise(resolve=>read=resolve);let release;const writerDone=new Promise(resolve=>release=resolve);
 const app=backend({find:async({ctx})=>{attempts++;await ctx.tx.query("SELECT title FROM v04_todo WHERE id='race'");if(attempts===1){read();await writerDone;}ctx.stream('User:alice').track.todo({id:'race'});return {todo:{id:'race'}};}});
 const request={context,callId:id(),name:'Find',version:1,args:{}};const pending=app.action('alice',JSON.stringify(request));await readStarted;
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title='B' WHERE id='race'");invalidate.todo({id:'race'});});release();
 const reply=JSON.parse(await pending);assert.equal(attempts,2,'entire handler reruns after no-op UPDATE snapshot conflict');assert.equal(reply.records[0].state.title,'B');const saved=(await q('SELECT response FROM axton_call WHERE call_id=$1',[request.callId]))[0];assert.deepEqual(JSON.parse(saved.response),reply,'stale attempt saved no response');
});

test('multiple settlements in one caller-owned transaction retain one distinct identity group',async()=>{
 const app=backend({find:async()=>({todo:{id:'t1'}})});const tx=await pool.connect();
 try {await tx.query('BEGIN ISOLATION LEVEL SERIALIZABLE');await app.acquirePublicationFence(tx);
  await tx.query("UPDATE v04_todo SET title='D' WHERE id='t1'");await app.publish(tx,async({invalidate})=>invalidate.todo({id:'t1'}));
  await tx.query("UPDATE v04_todo SET title='E' WHERE id='t1'");await app.publish(tx,async({invalidate})=>invalidate.todo({id:'t1'}));
  const groups=(await tx.query('SELECT keys FROM axton_publication_group WHERE transaction_id=pg_current_xact_id()')).rows;assert.equal(groups.length,1);assert.deepEqual(groups[0].keys,[{model:'Todo',identityKey:'{"id":"t1"}'}]);await tx.query('COMMIT');
 }catch(error){await tx.query('ROLLBACK');throw error;}finally{tx.release();}
});
test('later overlapping unique transfer closes an earlier group across the network page',async()=>{
 const app=backend({find:async()=>({todo:{id:'t1'}})});await q("INSERT INTO v04_todo VALUES('a','X'),('b','Y'),('c','Z')");
 await q('ALTER TABLE v04_todo ADD CONSTRAINT title_unique UNIQUE(title) DEFERRABLE INITIALLY DEFERRED');
 const prior=Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0].head);
 await app.transaction(async({stream})=>stream('User:alice').track.todo([{id:'a'},{id:'b'}]));
 await app.transaction(async({stream})=>stream('User:alice').track.todo({id:'c'}));
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title=CASE id WHEN 'b' THEN 'Z' ELSE 'Y' END WHERE id IN ('b','c')");invalidate.todo([{id:'b'},{id:'c'}]);});
 const page=JSON.parse(await app.pull('alice',JSON.stringify({context,callId:id(),after:prior,models:{Todo:1},limit:1})));
 assert.equal(page.units.length,1);assert.equal(page.to,prior+2,'original group prefix only');assert.equal(page.head,prior+5);const records=page.units[0].changes.map(v=>v.record);assert.deepEqual(records.map(v=>v.identity.id).sort(),['a','b','c']);assert.equal(records.find(v=>v.identity.id==='b').state.title,'Z');assert.equal(records.find(v=>v.identity.id==='c').state.title,'Y');assert.ok(records.find(v=>v.identity.id==='c').cursor>page.to,'closure companion is ahead of original prefix');
});

test('Bootstrap persists finite marked identity coverage across movement, Remove and restart',async()=>{
 const marked=structuredClone(config);marked.schema.models[0].bootstrap=true;let starts=0;
 const opts={config:marked,native,database,protocol4,authenticate:()=> 'alice',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'t1'}})},bootstrap:async({ctx})=>{starts++;ctx.stream('User:alice').track.todo([{id:'a'},{id:'b'}]);}};
 const app=createBackend(opts);const active={...context,materialization:app.materializationId};
 const started=JSON.parse(await app.pull('alice',JSON.stringify({kind:'start',context:active,callId:id(),models:{Todo:1},budget:100})));
 assert.ok(started.total>=2);assert.equal(starts,1);
 await assert.rejects(app.pull('alice',JSON.stringify({kind:'tail',context:active,callId:id(),manifestId:started.manifestId})),/bootstrap.coverage_incomplete/);
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title='Anew' WHERE id='a'");invalidate.todo({id:'a'});});
 // Historical Remove is retained evidence, not a public application write API.
 await database.transaction(async tx=>{await app.acquirePublicationFence(tx);const record=(await tx.query("SELECT id FROM axton_record WHERE model='Todo' AND identity_key=$1",['{"id":"b"}'])).rows[0].id;const head=(await tx.query("UPDATE axton_stream SET head=head+1 WHERE stream='User:alice' RETURNING head")).rows[0].head;await tx.query("DELETE FROM axton_stream_member WHERE stream='User:alice' AND record_id=$1",[record]);await tx.query("UPDATE axton_stream_log SET kind='remove',cursor=$2 WHERE stream='User:alice' AND record_id=$1",[record,head]);await database.persistence(tx).call({op:'savePublicationGroups',positions:[{stream:'User:alice',model:'Todo',identityKey:'{"id":"b"}',cursor:Number(head),kind:'remove'}]});});
 let from=0;const changes=[];
 while(from<started.total){const page=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context:active,callId:id(),manifestId:started.manifestId,from,limit:1})));changes.push(...page.items.map(v=>v.change));from=page.to;}
 assert.equal(changes.find(c=>c.kind==='upsert'&&c.record.identity.id==='a').record.state.title,'Anew');assert.equal(changes.find(c=>c.kind==='remove'&&c.key.identity.id==='b').kind,'remove');
 const restarted=createBackend(opts);const request={kind:'tail',context:active,callId:id(),manifestId:started.manifestId};const tail=JSON.parse(await restarted.pull('alice',JSON.stringify(request)));assert.ok(tail.head>=started.start);assert.equal(starts,1,'resume/tail does not rerun Bootstrap handler');assert.deepEqual(JSON.parse(await restarted.pull('alice',JSON.stringify(request))),tail);
});

test('unmarked held authority rematerializes without scanning initial Bootstrap range',async()=>{
 const app=backend({find:async()=>({todo:{id:'t1'}})});
 const fresh=JSON.parse(await app.pull('alice',JSON.stringify({kind:'start',context,callId:id(),models:{Todo:1},budget:100})));
 assert.equal(fresh.total,0,'initial Bootstrap excludes unmarked historical authority');
 const heldKeys=[{model:'Todo',identity:{id:'a'}},{model:'Todo',identity:{id:'b'}}];
 const start=JSON.parse(await app.pull('alice',JSON.stringify({kind:'start',context,callId:id(),models:{Todo:1},budget:100,heldKeys})));
 assert.equal(start.total,2);
 const page=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context,callId:id(),manifestId:start.manifestId,from:0,limit:10})));
 assert.deepEqual(page.items.map(i=>i.change.kind),['upsert','remove']);
 assert.equal(page.items[0].change.record.state.title,'Anew');
 await assert.rejects(app.pull('alice',JSON.stringify({kind:'start',context,callId:id(),models:{Todo:1},budget:100,heldKeys:[{model:'Todo',identity:{id:'never-tracked'}}]})),/manifest.identity_untracked/);
});

test('Bootstrap ignores unmarked historical transaction companions but closes real unique transfers',async()=>{
 const marked=structuredClone(config);marked.schema.models[0].bootstrap=true;marked.schema.models[0].unique=[['title']];
 const media={...structuredClone(model),name:'Media'};marked.schema.models.push(media);marked.schema.resultModels.push({...media,enums:[]});marked.loaders.push('Media');
 let starts=0;const app=createBackend({config:marked,native,database,protocol4,authenticate:()=> 'alice',loaders:{todo:loader,media:loader},queries:{find:async()=>({todo:{id:'t1'}})},bootstrap:async()=>{starts++;}});
 const active={...context,materialization:app.materializationId};
 await q("INSERT INTO v04_todo VALUES('m1','Media'),('u1','U1'),('u2','U2')");
 await app.transaction(async({stream})=>{stream('User:alice').track.todo({id:'u1'});stream('User:alice').track.media({id:'m1'});});
 const start=JSON.parse(await app.pull('alice',JSON.stringify({kind:'start',context:active,callId:id(),models:{Todo:1,Media:1},budget:100})));
 let u1Ordinal;for(let from=0;from<start.total;from++){const page=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context:active,callId:id(),manifestId:start.manifestId,from,limit:1})));assert.ok([...page.items.map(i=>i.change),...(page.companions??[])].every(c=>(c.record??c.key).model!=='Media'));if((page.items[0].change.record??page.items[0].change.key).identity.id==='u1')u1Ordinal=from;}
 assert.notEqual(u1Ordinal,undefined);
 await app.transaction(async({tx,stream,invalidate})=>{await tx.query("UPDATE v04_todo SET title=CASE id WHEN 'u1' THEN 'U2' ELSE 'U1' END WHERE id IN ('u1','u2')");stream('User:alice').track.todo({id:'u2'});invalidate.todo({id:'u1'});});
 const page=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context:active,callId:id(),manifestId:start.manifestId,from:u1Ordinal,limit:1})));
 assert.equal(page.to,u1Ordinal+1);assert.ok(page.companions.some(c=>c.record.identity.id==='u2'),'new tracked unique value owner shares atomic page');assert.equal(starts,1);
});

test('owned historical receipt recovery survives added Model and Bootstrap toggle without rerunning Bootstrap',async()=>{
 const evolved=structuredClone(config);evolved.schema.models[0].bootstrap=true;
 const extra={...structuredClone(model),name:'Extra'};evolved.schema.models.push(extra);evolved.schema.resultModels.push({...extra,enums:[]});evolved.loaders.push('Extra');
 let starts=0;const app=createBackend({config:evolved,native,database,protocol4:{...protocol4,materializations:{[context.materialization]:{schema:config.schema,projectionGeneration:'1'}}},authenticate:()=> 'alice',loaders:{todo:loader,extra:loader},queries:{find:async()=>({todo:{id:'t1'}})},bootstrap:async()=>{starts++;}});
 const active={...context,materialization:app.materializationId};
 await app.pull('alice',JSON.stringify({kind:'start',context:active,callId:id(),models:{Todo:1,Extra:1},budget:100}));assert.equal(starts,1);
 const target=(await q("SELECT l.cursor FROM axton_stream_log l JOIN axton_record r ON r.id=l.record_id WHERE l.stream='User:alice' AND r.model='Todo' AND r.identity_key=$1",['{"id":"t1"}']))[0];
 const baseline=Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0].head);assert.ok(Number(target.cursor)<baseline);
 const receiptCall=id();const key={model:'Todo',identity:{id:'t1'}};
 const receipt={context,intentDigest:'a'.repeat(64),completion:{callId:receiptCall,outcome:{status:'succeeded',result:{}}},targets:[{kind:'stream',key,cursor:Number(target.cursor),fallback:{...key,cursor:null,state:{title:'E'}}}]};
 await q('INSERT INTO axton_call(owner_id,call_id,request,response) VALUES($1,$2,$3,$4)',['alice',receiptCall,'saved frozen accepted intent',JSON.stringify(receipt)]);
 const started=JSON.parse(await app.pull('alice',JSON.stringify({kind:'materialize',context:active,callId:id(),receiptTargets:{callId:receiptCall,keys:[key]},budget:10})));
 assert.equal(started.total,1);assert.equal(starts,1,'Call recovery does not run application Bootstrap');
 const page=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context:active,callId:id(),manifestId:started.manifestId,from:0,limit:1})));
 assert.equal(page.items[0].change.record.cursor,Number(target.cursor));assert.equal(page.context.materialization,active.materialization);assert.equal(starts,1);
 const reset={...active,incarnation:'new-store'};await assert.rejects(app.pull('alice',JSON.stringify({kind:'materialize',context:reset,callId:id(),receiptTargets:{callId:receiptCall,keys:[key]},budget:10})),/receipt.invalid/);
 await assert.rejects(app.pull('alice',JSON.stringify({kind:'materialize',context:active,callId:id(),receiptTargets:{callId:receiptCall,keys:[{model:'Todo',identity:{id:'a'}}]},budget:10})),/receipt.invalid/);
});

test('actual Mutation no-op keeps original target cursor and private targets never enroll',async()=>{
 const mutable=structuredClone(config);mutable.schema.actions.push({name:'Edit',version:1,kind:'mutation',inputs:[{kind:'model',name:'todo',model:'Todo',operation:'update',cardinality:'single',allowedPatchFields:['title']}],outputs:[]});
 let calls=0;const app=createBackend({config:mutable,native,database,protocol4,authenticate:()=> 'alice',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'t1'}})},mutations:{edit:async()=>{calls++;}}});
 const before=Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0].head);
 const request={context,callId:id(),name:'Edit',version:1,args:{todo:{id:'t1',title:'E'}},models:{Todo:1}};
 const receipt=JSON.parse(await app.action('alice',JSON.stringify(request)));assert.equal(receipt.completion.outcome.status,'succeeded');assert.equal(receipt.targets[0].kind,'stream');assert.ok(receipt.targets[0].cursor<before);assert.equal(receipt.targets[0].fallback.cursor,null);assert.equal(calls,1);assert.equal(Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0].head),before,'no-op never republishes');
 assert.deepEqual(JSON.parse(await app.action('alice',JSON.stringify(request))),receipt);assert.equal(calls,1,'durable replay does not rerun handler');
 await q("INSERT INTO v04_todo VALUES('private','Private')");const privateRequest={...request,callId:id(),args:{todo:{id:'private',title:'Private'}}};
 const privateReceipt=JSON.parse(await app.action('alice',JSON.stringify(privateRequest)));assert.equal(privateReceipt.targets[0].kind,'private');assert.equal(privateReceipt.targets[0].record.cursor,null);assert.equal(Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0].head),before);assert.equal((await q("SELECT count(*)::int n FROM axton_record WHERE identity_key=$1",['{"id":"private"}']))[0].n,0,'private acknowledgement allocates no record authority');
});

test('Bootstrap unique companions cover a separate earlier transaction release before identity order',async()=>{
 const unique=structuredClone(config);unique.schema.models[0].bootstrap=true;unique.schema.models[0].unique=[['title']];
 const app=createBackend({config:unique,native,database,protocol4,authenticate:()=> 'alice',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'t1'}})}});const active={...context,materialization:app.materializationId};
 await q("INSERT INTO v04_todo VALUES('zz-release','ReleaseX'),('aa-acquire','ReleaseY')");
 await app.transaction(async({stream})=>stream('User:alice').track.todo({id:'zz-release'}));await app.transaction(async({stream})=>stream('User:alice').track.todo({id:'aa-acquire'}));
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title='ReleaseZ' WHERE id='zz-release'");invalidate.todo({id:'zz-release'});});
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title='ReleaseX' WHERE id='aa-acquire'");invalidate.todo({id:'aa-acquire'});});
 const start=JSON.parse(await app.pull('alice',JSON.stringify({kind:'start',context:active,callId:id(),models:{Todo:1},budget:100})));
 let acquire;for(let from=0;from<start.total;from++){const page=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context:active,callId:id(),manifestId:start.manifestId,from,limit:1})));if((page.items[0].change.record??page.items[0].change.key).identity.id==='aa-acquire'){acquire=page;break;}}
 assert.ok(acquire);assert.ok(acquire.companions?.some(c=>c.kind==='upsert'&&c.record.identity.id==='zz-release'&&c.record.state.title==='ReleaseZ'),'prior independent release accompanies identity-ordered acquisition');
 assert.equal(acquire.to,acquire.from+1,'companions do not cover later identity ordinal');
});

test('actual v04 live subscription resumes one Stream from requested durable cursor',async()=>{
 const app=backend({find:async()=>({todo:{id:'t1'}})});await app.transaction(async({stream})=>stream('User:bob').track.todo({id:'t1'}));
 const active={...context,binding:{...context.binding,viewer:'bob',stream:'User:bob'}};
 const opened=await app.negotiateLive('bob',JSON.stringify({context:active,models:{Todo:1},cursor:0}));
 try {assert.equal(opened.actions[0].type,'listen');assert.equal(opened.actions[0].stream,'User:bob');const ack=JSON.parse(opened.actions[1].frame);assert.equal(ack.cursor,0);assert.equal(ack.head,1);
 const pull=opened.actions[2];assert.equal(pull.type,'pullV04');const request=JSON.parse(pull.request);assert.equal(request.after,0);assert.deepEqual(request.context,active);
 const page=await app.pull('bob',pull.request);const actions=app.liveEvent(opened.handle,{type:'pulled',page});assert.equal(actions[0].type,'send');assert.equal(JSON.parse(actions[0].frame).to,1);
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title='Live' WHERE id='t1'");invalidate.todo({id:'t1'});});
 const next=app.liveEvent(opened.handle,{type:'committed',stream:'User:bob'});assert.equal(JSON.parse(next[0].request).after,1);const update=await app.pull('bob',next[0].request);assert.equal(JSON.parse(update).units[0].changes[0].record.state.title,'Live');
 }finally{app.liveClose(opened.handle);}
});

test('public v04 handler contexts expose initiating Stream and explicit multi-Stream selection',async()=>{
 const app=backend({find:async({ctx})=>{ctx.stream.track.todo({id:'t1'});ctx.streams(['User:bob','User:carol']).track.todo({id:'t1'});assert.equal('invalidate' in ctx,false);return {todo:{id:'t1'}};}});
 const read=JSON.parse(await app.action('alice',JSON.stringify({context,callId:id(),name:'Find',version:1,args:{}})));assert.equal(read.completion.outcome.status,'succeeded');
 assert.equal((await q("SELECT count(*)::int n FROM axton_stream_member m JOIN axton_record r ON r.id=m.record_id WHERE r.model='Todo' AND r.identity_key=$1 AND m.stream=ANY($2::text[])",['{"id":"t1"}',['User:alice','User:bob','User:carol']]))[0].n,3);
});

test('bounded authority payload failure saves no partial page or coverage',async()=>{
 const app=createBackend({config,native,database,protocol4:{...protocol4,maxUnitBytes:100},authenticate:()=> 'alice',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'t1'}})}});
 const active={...context,binding:{...context.binding,viewer:'bob',stream:'User:bob'}};const callId=id();
 await assert.rejects(app.pull('bob',JSON.stringify({context:active,callId,after:0,models:{Todo:1},limit:1})),/constraint_group_capacity/);
 assert.equal((await q('SELECT count(*)::int n FROM axton_call WHERE owner_id=$1 AND call_id=$2',['bob',callId]))[0].n,0);
});

test('HTTP carrier preserves explicit v04 context and capacity refusal codes',async()=>{
 const app=createBackend({config,native,database,protocol4:{...protocol4,maxUnitBytes:100},authenticate:()=> 'bob',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'t1'}})}});const server=await app.listen({port:0});
 try {const active={...context,binding:{...context.binding,viewer:'bob',stream:'User:bob'}};const response=await fetch(server.url+'/sync/pull',{method:'POST',body:JSON.stringify({context:active,callId:id(),after:0,models:{Todo:1},limit:1})});assert.equal(response.status,413);assert.equal((await response.json()).code,'constraint_group_capacity');
 const refused=await fetch(server.url+'/sync/fetch',{method:'POST',body:JSON.stringify({context,callId:id(),model:'Todo',version:1,identity:{id:'t1'}})});assert.equal(refused.status,409);assert.equal((await refused.json()).code,'context_mismatch');
 }finally{await server.close();}
});
test('actual WebSocket executor carries v04 ACK and Delta then a committed update',async()=>{
 const app=createBackend({config,native,database,protocol4,authenticate:()=> 'carol',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'t1'}})}});const server=await app.listen({port:0});const ws=new WebSocket(server.url.replace('http:','ws:')+'/sync/live');
 const frames=[];let notify;ws.on('message',text=>{frames.push(JSON.parse(String(text)));notify?.();});
 const next=async()=>{if(!frames.length)await new Promise((resolve,reject)=>{const timeout=setTimeout(()=>reject(new Error('live frame timeout')),5000);notify=()=>{clearTimeout(timeout);notify=undefined;resolve();};});return frames.shift();};
 try {await once(ws,'open');const active={...context,binding:{...context.binding,viewer:'carol',stream:'User:carol'}};ws.send(JSON.stringify({context:active,models:{Todo:1},cursor:0}));const ack=await next();assert.equal(ack.cursor,0);assert.equal(ack.head,1);const first=await next();assert.equal(first.from,0);assert.equal(first.to,1);
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title='Socket' WHERE id='t1'");invalidate.todo({id:'t1'});});const update=await next();assert.equal(update.from,1);assert.equal(update.units[0].changes[0].record.state.title,'Socket');
 }finally{ws.close();await server.close();}
});

test('Mutation writes, canonical tombstones and refusals commit atomically with exact receipts',async()=>{
 const mutable=structuredClone(config);mutable.schema.actions.push({name:'Edit',version:1,kind:'mutation',inputs:[{kind:'model',name:'todo',model:'Todo',operation:'update',cardinality:'single',allowedPatchFields:['title']}],outputs:[]},{name:'Delete',version:1,kind:'mutation',inputs:[{kind:'model',name:'todo',model:'Todo',operation:'delete',cardinality:'single'}],outputs:[]});
 const app=createBackend({config:mutable,native,database,protocol4,authenticate:()=> 'alice',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'t1'}})},mutations:{edit:async({ctx,args})=>{await ctx.tx.query('UPDATE v04_todo SET title=$2 WHERE id=$1',[args.todo.id,args.todo.title]);ctx.invalidate.todo({id:args.todo.id});if(args.todo.title==='Refused')throw new MutationRejected('edit.denied');},delete:async({ctx,args})=>{await ctx.tx.query('DELETE FROM v04_todo WHERE id=$1',[args.todo.id]);ctx.invalidate.todo({id:args.todo.id});}}});
 const edit={context,callId:id(),name:'Edit',version:1,args:{todo:{id:'t1',title:'Accepted'}},models:{Todo:1}};const receipt=JSON.parse(await app.action('alice',JSON.stringify(edit)));assert.equal(receipt.completion.outcome.status,'succeeded');assert.equal(receipt.targets[0].fallback.state.title,'Accepted');const required=receipt.targets[0].cursor;
 const saved=(await q('SELECT response FROM axton_call WHERE owner_id=$1 AND call_id=$2',['alice',edit.callId]))[0];assert.deepEqual(JSON.parse(saved.response),receipt);
 const page=JSON.parse(await app.pull('alice',JSON.stringify({context,callId:id(),after:required-1,models:{Todo:1},limit:1})));assert.equal(page.units[0].changes.find(c=>c.record?.identity.id==='t1').record.cursor,required);
 const priorHead=Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0].head);const refused=JSON.parse(await app.action('alice',JSON.stringify({...edit,callId:id(),args:{todo:{id:'t1',title:'Refused'}}})));assert.equal(refused.completion.outcome.code,'edit.denied');assert.deepEqual(refused.targets,[]);assert.equal((await q("SELECT title FROM v04_todo WHERE id='t1'"))[0].title,'Accepted');assert.equal(Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0].head),priorHead);
 const removed=JSON.parse(await app.action('alice',JSON.stringify({...edit,callId:id(),args:{todo:{id:'b',title:'Removed-private'}}})));assert.equal(removed.targets[0].kind,'private','formerly removed target does not re-enroll');
 const deleted=JSON.parse(await app.action('alice',JSON.stringify({...edit,callId:id(),name:'Delete',args:{todo:{id:'t1'}}})));assert.equal(deleted.targets[0].kind,'stream');assert.equal(deleted.targets[0].fallback.state,null);assert.ok(deleted.targets[0].cursor>required);
});

test('saved frozen Mutation replay survives schema expansion and active target recovery returns current tombstone',async()=>{
 const calls=await q("SELECT request,response FROM axton_call WHERE owner_id='alice'");let frozen,saved;for(const row of calls){try{const parsed=JSON.parse(row.request);if(parsed.kind==='mutation'&&parsed.intent.name==='Edit'&&parsed.intent.args.todo.id==='t1'&&JSON.parse(row.response).completion.outcome.status==='succeeded'){frozen=parsed.intent;saved=JSON.parse(row.response);break;}}catch{}}
 assert.ok(frozen);const evolved=structuredClone(config);const old=structuredClone(evolved.schema.models[0]);evolved.schema.models[0].version=2;evolved.schema.models[0].bootstrap=true;evolved.schema.models[0].fields.push({name:'done',nullable:true,type:{kind:'scalar',name:'boolean'}});const extra={...structuredClone(model),name:'Extra'};evolved.schema.models.push(extra);evolved.schema.resultModels.push({...extra,enums:[]});evolved.models=[{...old,enums:[]},{...evolved.schema.models[0],enums:[]},{...extra,enums:[]}];evolved.loaders.push('Extra');evolved.schema.actions.push({name:'Edit',version:1,kind:'mutation',inputs:[{kind:'model',name:'todo',model:'Todo',operation:'update',cardinality:'single',allowedPatchFields:['title']}],outputs:[]});let executions=0;
 const app=createBackend({config:evolved,native,database,protocol4:{...protocol4,materializations:{[context.materialization]:{schema:config.schema,projectionGeneration:'1'}}},authenticate:()=> 'alice',loaders:{todo:{v1:loader,v2:async(call)=>(await loader(call)).map(row=>row?{...row,done:true}:null)},extra:loader},queries:{find:async()=>({todo:{id:'t1'}})},mutations:{edit:async()=>{executions++;}}});
 assert.deepEqual(JSON.parse(await app.action('alice',JSON.stringify(frozen))),saved);assert.equal(executions,0);
 const active={...context,materialization:app.materializationId};const started=JSON.parse(await app.pull('alice',JSON.stringify({kind:'materialize',context:active,callId:id(),receiptTargets:{callId:frozen.callId,keys:[{model:'Todo',identity:{id:'t1'}}]},budget:10})));const page=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context:active,callId:id(),manifestId:started.manifestId,from:0,limit:1})));assert.equal(page.items[0].change.record.state,null);assert.ok(page.items[0].change.record.cursor>saved.targets[0].cursor);
});
test('identity-inclusive unique keys do not force whole-Model manifest companions',async()=>{
 const primary=structuredClone(config);primary.schema.models[0].bootstrap=true;primary.schema.models[0].unique=[['id']];const app=createBackend({config:primary,native,database,protocol4,authenticate:()=> 'alice',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'t1'}})}});const active={...context,materialization:app.materializationId};const start=JSON.parse(await app.pull('alice',JSON.stringify({kind:'start',context:active,callId:id(),models:{Todo:1},budget:100})));assert.ok(start.total>1);const page=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context:active,callId:id(),manifestId:start.manifestId,from:0,limit:1})));assert.equal(page.items.length,1);assert.equal(page.companions,undefined);
});

test('authority captures the position after Loader preparation publishes',async()=>{
 await q("INSERT INTO v04_todo VALUES('prepared','Before')");
 let preparations=0;let app;
 app=createBackend({config,native,database,protocol4,authenticate:()=> 'alice',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'prepared'}})},loaderHooks:{todo:{prepareForViewer:async({tx,ids,invalidate})=>{if(!ids.some(v=>v.id==='prepared'))return;preparations++;if(preparations===1){await tx.query("UPDATE v04_todo SET title='Prepared' WHERE id='prepared'");invalidate.todo({id:'prepared'});}}}}});
 const before=Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0].head);
 await app.transaction(async({streams})=>streams(['User:alice']).track.todo({id:'prepared'}));
 const page=JSON.parse(await app.pull('alice',JSON.stringify({context,callId:id(),after:before,models:{Todo:1},limit:1})));
 const record=page.units[0].changes.find(c=>c.record?.identity.id==='prepared').record;
 const current=Number((await q("SELECT l.cursor FROM axton_stream_log l JOIN axton_record r ON r.id=l.record_id WHERE l.stream='User:alice' AND r.identity_key=$1",['{"id":"prepared"}']))[0].cursor);
 assert.equal(record.state.title,'Prepared');assert.equal(record.cursor,current,'prepared content is labeled with the final real pair cursor');assert.equal(page.head,current);assert.equal(preparations,1,'canonical read does not rerun preparation');
});

test('manifest and Mutation evidence follow all preparation publications',async()=>{
 await q("INSERT INTO v04_todo VALUES('prep-root','Root-before'),('prep-child','Child-before'),('prep-call','Call-before')");
 const mutable=structuredClone(config);mutable.schema.models[0].bootstrap=true;mutable.schema.actions.push({name:'PrepareCall',version:1,kind:'mutation',inputs:[{kind:'model',name:'todo',model:'Todo',operation:'update',cardinality:'single',allowedPatchFields:['title']}],outputs:[{...output,source:{kind:'inputIdentity',inputIdentity:'todo'}}]});
 const prepared=new Set();let hookCalls=0;
 const app=createBackend({config:mutable,native,database,protocol4,authenticate:()=> 'prep',loaders:{todo:loader},queries:{find:async()=>({todo:{id:'prep-root'}})},mutations:{prepareCall:async()=>{}},loaderHooks:{todo:{prepareForViewer:async({tx,ids,streams,invalidate})=>{for(const {id:key} of ids){hookCalls++;if(prepared.has(key))continue;prepared.add(key);await tx.query('UPDATE v04_todo SET title=$2 WHERE id=$1',[key,key+'-prepared']);invalidate.todo({id:key});if(key==='prep-root')streams(['User:prep']).track.todo({id:'prep-child'});}}}}});
 const active={...context,binding:{...context.binding,viewer:'prep',stream:'User:prep'},materialization:app.materializationId};
 await app.transaction(async({streams})=>streams(['User:prep']).track.todo([{id:'prep-root'},{id:'prep-call'}]));
 const started=JSON.parse(await app.pull('prep',JSON.stringify({kind:'start',context:active,callId:id(),models:{Todo:1},budget:10})));
 const page=JSON.parse(await app.pull('prep',JSON.stringify({kind:'page',context:active,callId:id(),manifestId:started.manifestId,from:1,limit:1})));
 const changes=[...page.items.map(i=>i.change),...(page.companions??[])];assert.ok(changes.some(c=>c.record?.identity.id==='prep-child'),'replanning prepares a newly published dependency companion');
 for(const c of changes){const row=c.record;if(!row)continue;const current=Number((await q("SELECT l.cursor FROM axton_stream_log l JOIN axton_record r ON r.id=l.record_id WHERE l.stream='User:prep' AND r.identity_key=$1",[JSON.stringify(row.identity)]))[0].cursor);assert.equal(row.cursor,current);assert.equal(row.state.title,row.identity.id+'-prepared');}
 const beforeHooks=hookCalls;const callId=id();const receipt=JSON.parse(await app.action('prep',JSON.stringify({context:active,callId,name:'PrepareCall',version:1,args:{todo:{id:'prep-call',title:'ignored'}},models:{Todo:1}})));assert.equal(receipt.completion.outcome.status,'succeeded');assert.equal(hookCalls-beforeHooks,1,'input/output identity preparation is deduplicated');assert.equal(receipt.targets[0].fallback.state.title,'prep-call-prepared');assert.equal(receipt.completion.outcome.result.todo.title,'prep-call-prepared');
 const current=Number((await q("SELECT l.cursor FROM axton_stream_log l JOIN axton_record r ON r.id=l.record_id WHERE l.stream='User:prep' AND r.identity_key=$1",['{"id":"prep-call"}']))[0].cursor);assert.equal(receipt.targets[0].cursor,current);
 const hooks=hookCalls;await app.action('prep',JSON.stringify({context:active,callId,name:'PrepareCall',version:1,args:{todo:{id:'prep-call',title:'ignored'}},models:{Todo:1}}));assert.equal(hookCalls,hooks,'durable receipt replay does not rerun preparation');
});

test('an unrelated failed Loader does not prevent requesting an earlier independent unit',async()=>{
 await q("INSERT INTO v04_todo VALUES('progress-good','Progress-good'),('progress-bad','Progress-bad')");
 const app=createBackend({config,native,database,protocol4,authenticate:()=> 'progress',onError:()=>{},loaders:{todo:async(call)=>{if(call.ids.some(i=>i.id==='progress-bad'))throw new Error('persistent Loader failure');return loader(call);}},queries:{find:async()=>({todo:{id:'progress-good'}})}});
 const active={...context,binding:{...context.binding,viewer:'progress',stream:'User:progress'}};
 await app.transaction(async({streams})=>streams(['User:progress']).track.todo({id:'progress-good'}));
 await app.transaction(async({streams})=>streams(['User:progress']).track.todo({id:'progress-bad'}));
 await assert.rejects(app.pull('progress',JSON.stringify({context:active,callId:id(),after:0,models:{Todo:1},limit:2})),/loader.failed/);
 const prefix=JSON.parse(await app.pull('progress',JSON.stringify({context:active,callId:id(),after:0,models:{Todo:1},limit:1})));assert.equal(prefix.to,1);assert.equal(prefix.units.length,1);assert.equal(prefix.units[0].changes[0].record.identity.id,'progress-good');
 await assert.rejects(app.pull('progress',JSON.stringify({context:active,callId:id(),after:prefix.to,models:{Todo:1},limit:1})),/loader.failed/);
});


test('legal loader.invalid content survives Delta and Manifest committed replay',async()=>{
 const app=backend({find:async()=>({todo:{id:'loader.invalid'}})});
 await q("INSERT INTO v04_todo VALUES('loader.invalid','loader.invalid')");
 const prior=Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0]?.head??0);
 await app.transaction(async({stream})=>stream('User:alice').track.todo({id:'loader.invalid'}));
 const delta={context,callId:id(),after:prior,models:{Todo:1},limit:1};
 const wire=await app.pull('alice',JSON.stringify(delta));assert.equal(JSON.parse(wire).units[0].changes[0].record.state.title,'loader.invalid');assert.equal(await app.pull('alice',JSON.stringify(delta)),wire);
 const start=JSON.parse(await app.pull('alice',JSON.stringify({kind:'start',context,callId:id(),models:{Todo:1},budget:100,heldKeys:[{model:'Todo',identity:{id:'loader.invalid'}}]})));
 const manifest={kind:'page',context,callId:id(),manifestId:start.manifestId,from:0,limit:1};
 const page=await app.pull('alice',JSON.stringify(manifest));assert.equal(JSON.parse(page).items[0].change.record.state.title,'loader.invalid');assert.equal(await app.pull('alice',JSON.stringify(manifest)),page);
});


test('caught Query tracking refusal rolls back all enrollment and saves stable failed replay; 1000 succeeds',async()=>{
 await q("INSERT INTO v04_todo VALUES('query-result','Query result')");
 for(const mode of ['overflow','invalid','limit']){
  let attempts=0;const errors=[];
  const app=createBackend({config,native,database,protocol4,authenticate:()=> 'alice',loaders:{todo:loader},onError:e=>errors.push(e),queries:{find:async({ctx})=>{
   attempts++;const track=ctx.stream('User:alice').track.todo;
   for(let n=0;n<1000;n++)track({id:`query-${mode}-${n}`});
   try{if(mode==='overflow')track({id:'query-overflow-extra'});else if(mode==='invalid')track({});}catch{}
   return {todo:{id:'query-result'}};
  }}});
  const state=async()=>({head:(await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0]?.head??'0',members:(await q("SELECT count(*)::int n FROM axton_stream_member WHERE stream='User:alice'"))[0].n,records:(await q('SELECT count(*)::int n FROM axton_record'))[0].n});
  const before=await state();const request={context,callId:id(),name:'Find',version:1,args:{}};
  const wire=await app.action('alice',JSON.stringify(request));const reply=JSON.parse(wire);
  if(mode==='limit'){assert.equal(reply.completion.outcome.status,'succeeded');assert.equal((await state()).members,before.members+1000);assert.equal(Number((await state()).head),Number(before.head)+1000);}
  else {assert.deepEqual(reply.completion.outcome,{status:'failed',code:'handler.failed',execution:'rejected'});assert.deepEqual(await state(),before);assert.equal(errors.length,1);}
  assert.equal(await app.action('alice',JSON.stringify(request)),wire);assert.equal(attempts,1,'saved replay does not rerun handler');
  assert.equal((await q('SELECT response FROM axton_call WHERE call_id=$1',[request.callId]))[0].response,wire);
 }
});

test('Delta and Manifest project identical final keys, and Remove bypasses preparation and Loader',async()=>{
 let preparations=0,canonical=0;
 const app=createBackend({config,native,database,protocol4,authenticate:()=> 'alice',queries:{find:async()=>({todo:{id:'parity-live'}})},loaderHooks:{todo:{prepareForViewer:async({ids})=>{preparations++;assert.ok(ids.every(({id})=>id!=='parity-remove'));}}},loaders:{todo:async(call)=>{canonical++;assert.ok(call.ids.every(({id})=>id!=='parity-remove'));return loader(call);}}});
 await q("INSERT INTO v04_todo VALUES('parity-live','Parity live'),('parity-remove','Parity remove')");
 const before=Number((await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0]?.head??0);
 await app.transaction(async({stream})=>stream('User:alice').track.todo([{id:'parity-live'},{id:'parity-remove'}]));
 await app.transaction(async({tx,invalidate})=>{await tx.query("UPDATE v04_todo SET title='Parity current' WHERE id='parity-live'");invalidate.todo({id:'parity-live'});});
 await database.transaction(async tx=>{await app.acquirePublicationFence(tx);const record=(await tx.query("SELECT id FROM axton_record WHERE model='Todo' AND identity_key=$1",['{"id":"parity-remove"}'])).rows[0].id;const head=(await tx.query("UPDATE axton_stream SET head=head+1 WHERE stream='User:alice' RETURNING head")).rows[0].head;await tx.query("DELETE FROM axton_stream_member WHERE stream='User:alice' AND record_id=$1",[record]);await tx.query("UPDATE axton_stream_log SET kind='remove',cursor=$2 WHERE stream='User:alice' AND record_id=$1",[record,head]);await database.persistence(tx).call({op:'savePublicationGroups',positions:[{stream:'User:alice',model:'Todo',identityKey:'{"id":"parity-remove"}',cursor:Number(head),kind:'remove'}]});});
 const delta=JSON.parse(await app.pull('alice',JSON.stringify({context,callId:id(),after:before,models:{Todo:1},limit:1})));
 const heldKeys=['parity-live','parity-remove'].map(id=>({model:'Todo',identity:{id}}));
 const start=JSON.parse(await app.pull('alice',JSON.stringify({kind:'start',context,callId:id(),models:{Todo:1},budget:10,heldKeys})));
 const manifest=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context,callId:id(),manifestId:start.manifestId,from:0,limit:10})));
 const sorted=changes=>changes.toSorted((a,b)=>(a.record??a.key).identity.id.localeCompare((b.record??b.key).identity.id));
 assert.deepEqual(sorted(delta.units[0].changes),sorted(manifest.items.map(item=>item.change)));
 assert.equal(delta.units[0].changes.find(c=>c.kind==='upsert').record.state.title,'Parity current');
 assert.equal(delta.units[0].changes.find(c=>c.kind==='remove').key.identity.id,'parity-remove');
 assert.ok(preparations>0&&canonical>0);
});
