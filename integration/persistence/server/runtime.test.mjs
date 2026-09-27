import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import * as serverSdk from '../../../packages/server/index.mts';
import {createBackend,MutationRejected,EngineError} from '../../../packages/server/index.mts';
import {prisma,prismaDriver} from '../../../packages/postgres/index.mts';
const require=createRequire(import.meta.url);
const {PrismaClient}=require('../../bindings/node/generated/client');
const native=require('../../../bindings/node/axton-node.node');
const db=new PrismaClient();
// The server suite runs on the Prisma shim (its handlers use Prisma's raw API); driver-conformance.test.mjs proves every shim.
const database=()=>prisma(db);
const store=tx=>prisma(db).persistence(tx);
/** A business change made outside a handler: touch the records and add each to one Channel. */
const external=(backendLike,name,records)=>backendLike.transaction(({channel,touch})=>{channel(name).add(records);for(const {model,identity} of records)touch[model.charAt(0).toLowerCase()+model.slice(1)](identity);});
const run=prismaDriver(db).transaction;
const schema={enums:[],models:[{name:'Task',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false},{name:'title',type:{kind:'scalar',name:'string'},nullable:false}]}]};
const config={schema,mutations:[{name:'edit',version:1,slots:[{name:'task',model:'Task',operation:'update',cardinality:'single',allowedPatchFields:['title']}]}]};
const ticketModel={name:'Ticket',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'uuid'},nullable:false}]};
const authenticate=async req=>req.headers.authorization==='Bearer alice'?'alice':null;
let called=0,prepared=0,lastInput,lastHandles;const loaderCalls=[];
const write=(tx,id,title)=>tx.$executeRawUnsafe('INSERT INTO business_task(id,title) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET title=$2',id,title);
const readTasks=({ids,tx})=>Promise.all(ids.map(async identity=>{const rows=await tx.$queryRawUnsafe('SELECT title FROM business_task WHERE id=$1',identity.id);return rows[0]??null;}));
// The patch title steers the handler: every mutation writes its row and, unless told to stay quiet, adds its record to `shared`.
const backend=createBackend({config,database:database(),authenticate,handlers:{
 async edit({input,tx,channel,touch}){
  called++;lastInput=input;lastHandles={channel,touch};const {identity,patch}=input.task;
  await write(tx,identity.id,patch.title);
  if(patch.title==='empty-channel')channel('');
  if(patch.title==='bad-records')channel('shared').add('x');
  if(patch.title==='bogus-record')channel('shared').add([{bogus:true}]);
  if(patch.title==='quiet')return;
  const shared=channel('shared');shared.task.add(identity);
  if(patch.title==='refuse')throw new MutationRejected('task.refused');if(patch.title==='crash')throw new Error('business crash');
  if(patch.title==='two')channel('other').task.add(identity);
  if(patch.title==='extra'){const extra={id:`${identity.id}-extra`};await write(tx,extra.id,'extra too');touch.task(extra);shared.task.add(extra);}
  if(patch.title==='publish-only')channel('other').task.add({id:'pub-only'});
 }},
 loaders:{async task(call){loaderCalls.push(Object.keys(call));return readTasks(call);}},
 loaderHooks:{task:{async prepareForViewer(){prepared++}}},
});
const mutation=(ordinal,title,id='a')=>({ordinal,name:'edit',operations:[{model:'Task',op:'update',identity:{id},values:{title}}]});
const push=(clientId,batchSequence,mutations)=>JSON.stringify({clientId,batchSequence,mutations,models:{Task:1}});
const authority=(id,stamp,state)=>({identity:{id},model:'Task',stamp,state});
const pullBody=(cursors,models={Task:1})=>JSON.stringify({cursors,models});
const pull=(scope='shared',fromCursor=0)=>backend.pull('alice',pullBody({[scope]:fromCursor})).then(JSON.parse);
const to=(page,scope='shared')=>page.cursors[scope].to;
const count=async table=>Number((await db.$queryRawUnsafe(`SELECT count(*) AS count FROM ${table}`))[0].count);
const key=id=>`{"id":"${id}"}`;
const recordStamp=async id=>{const rows=await db.$queryRawUnsafe('SELECT stamp FROM axton_record WHERE model=$1 AND identity_key=$2','Task',key(id));return rows.length?Number(rows[0].stamp):null;};
const invalidations=async id=>(await db.$queryRawUnsafe('SELECT channel, cursor, stamp FROM axton_invalidation WHERE identity_key=$1 ORDER BY channel',key(id))).map(r=>[r.channel,Number(r.cursor),Number(r.stamp)]);
const head=async channel=>{const rows=await db.$queryRawUnsafe('SELECT head FROM axton_channel WHERE channel=$1',channel);return rows.length?Number(rows[0].head):0;};
before(async()=>{for(const sql of (await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8')).split(';').map(x=>x.trim()).filter(Boolean))await db.$executeRawUnsafe(sql);await db.$executeRawUnsafe('CREATE TABLE business_task(id text PRIMARY KEY,title text NOT NULL)');});
after(()=>db.$disconnect());
test('native exports production runtime',()=>{assert.equal(typeof native.processPush,'function');assert.equal(typeof native.processPull,'function');assert.equal(typeof native.settleExternal,'function');assert.equal(native.publish,undefined);assert.equal(typeof native.validateConfig,'function');
 assert.equal(typeof native.negotiateLive,'function');assert.equal(typeof native.pullLive,'function');
 assert.equal(typeof native.liveEvent,'function');assert.equal(typeof native.liveClose,'function');
 assert.throws(()=>native.liveEvent(0,JSON.stringify({type:'closed'})),error=>JSON.parse(error.message).code==='live.invalid_event','an event on a handle that is not open is a host defect');
 native.liveClose(0);
});
test('backend validates config and complete registrations at startup',()=>{
 const base={...config,schema:structuredClone(schema)};
 assert.throws(()=>createBackend({config:{...base,mutations:[{name:'bad',version:0,slots:[]}]},native,database:database(),authenticate,handlers:{},loaders:{task:async()=>[]}}),/invalid mutation descriptor/);
 assert.throws(()=>createBackend({config:base,native,database:database(),authenticate,handlers:{},loaders:{task:async()=>[]}}),/Missing handler edit for edit v1/);
 assert.throws(()=>createBackend({config:base,native,database:database(),authenticate,handlers:{edit:async()=>{}},loaders:{}}),/Missing loader task for Task v1/);
 // A hand-written config gets the compiler's accessor rules: unique, and neither add nor remove.
 const withModel=name=>({...base,schema:{...base.schema,models:[...base.schema.models,{...base.schema.models[0],name}]}});
 const loaders={task:async()=>[],add:async()=>[]};
 assert.throws(()=>createBackend({config:withModel('Add'),native,database:database(),authenticate,handlers:{edit:async()=>{}},loaders}),/Model Add generates the accessor add, which a Channel reserves/);
 assert.throws(()=>createBackend({config:withModel('task'),native,database:database(),authenticate,handlers:{edit:async()=>{}},loaders}),/Models Task and task both generate the accessor task/);
});
test('loader registration names every retained model version and a function means v1 only',()=>{
 const base={...config,schema:structuredClone(schema)};
 const contract=version=>({name:'Task',version,identity:['id'],fields:schema.models[0].fields,enums:[]});
 const register=(models,loaders,currentVersion=1)=>{const c={...base,schema:structuredClone(schema),models};c.schema.models[0].version=currentVersion;return createBackend({config:c,native,database:database(),authenticate,handlers:{edit:async()=>{}},loaders});};
 const both=[contract(1),contract(2)];
 assert.throws(()=>register(both,{task:async()=>[]},2),/Loader task must register v1, v2 of Task; a function registers v1 only/);
 assert.throws(()=>register([contract(2)],{task:async()=>[]},2),/Loader task must register v2 of Task; a function registers v1 only/);
 assert.throws(()=>register(both,{task:{v1:async()=>[]}},2),/Missing loader task\.v2 for Task v2/);
 assert.throws(()=>register(both,{task:{v1:async()=>[],v2:async()=>[],v3:async()=>[]}},2),/Unknown loader task\.v3 for Task: retained versions are v1, v2/);
 assert.throws(()=>register(both,{task:{v1:async()=>[],v2:'later'}},2),/Loader task\.v2 for Task v2 must be a function/);
 assert.throws(()=>register(both,{task:null},2),/Missing loader task for Task v1, v2/);
 // The engine refuses a schema whose current version is not a retained contract.
 assert.throws(()=>register([contract(1)],{task:{v1:async()=>[]}},2),/not a retained contract/);
 register(both,{task:{v1:async()=>[],v2:async()=>[]}},2);
 register([contract(1)],{task:{v1:async()=>[]}});
 register([contract(1)],{task:async()=>[]});
 register([],{task:async()=>[]});
});
test('handler registration names every retained version and a function means v1 only',()=>{
 const base={...config,schema:structuredClone(schema)};
 const register=(mutations,handlers)=>createBackend({config:{...base,mutations},native,database:database(),authenticate,handlers,loaders:{task:async()=>[]}});
 const both=[config.mutations[0],{...config.mutations[0],version:2}];
 assert.throws(()=>register(both,{edit:async()=>{}}),/Handler edit must register v1, v2 of edit; a function registers v1 only/);
 assert.throws(()=>register([{...config.mutations[0],version:2}],{edit:async()=>{}}),/Handler edit must register v2 of edit; a function registers v1 only/);
 assert.throws(()=>register(both,{edit:{v1:async()=>{}}}),/Missing handler edit\.v2 for edit v2/);
 assert.throws(()=>register(both,{edit:{v1:async()=>{},v2:async()=>{},v3:async()=>{}}}),/Unknown handler edit\.v3 for edit: retained versions are v1, v2/);
 assert.throws(()=>register(both,{edit:{v1:async()=>{},v2:'later'}}),/Handler edit\.v2 for edit v2 must be a function/);
 assert.throws(()=>register(both,{edit:null}),/Missing handler edit for edit v1, v2/);
 register(both,{edit:{v1:async()=>{},v2:async()=>{}}});
 register([config.mutations[0]],{edit:{v1:async()=>{}}});
 register([config.mutations[0]],{edit:async()=>{}});
});
test('push commits business + compacted publication + exact durable receipt together',async()=>{
 const request=push('dedup',1,[mutation(1,'first')]);const receipt=await backend.push('alice',request);const calls=called;
 assert.equal(receipt,'{"batchSequence":1,"clientId":"dedup","records":[{"identity":{"id":"a"},"model":"Task","stamp":1,"state":{"title":"first"}}],"rejections":[]}','the receipt is canonical JSON: keys sorted, the loader\'s authority for every changed record');
 // Replay is keyed by (clientId, batchSequence): the same frozen bytes and a changed body both return the stored receipt without a handler call, a business write, a publication or a subscriber wake.
 let wakes=0;const unsubscribe=backend.onCommitted('shared',()=>{wakes++;});const rows=await count('axton_invalidation');
 assert.equal(await backend.push('alice',request),receipt);assert.equal(called,calls);
 assert.equal(await backend.push('alice',push('dedup',1,[mutation(1,'changed')])),receipt);assert.equal(called,calls);
 await new Promise(resolve=>setImmediate(resolve));unsubscribe();assert.equal(wakes,0,'replayed receipts must not wake subscribers');
 assert.deepEqual(await db.$queryRawUnsafe("SELECT title FROM business_task WHERE id='a'"),[{title:'first'}]);assert.equal(await count('axton_invalidation'),rows);
 await assert.rejects(()=>backend.push('bob',request),/owner_mismatch/);
 await assert.rejects(()=>backend.push('alice',push('dedup',3,[mutation(1,'gap')])),/gap/);
 const page=await pull();assert.deepEqual(page,{cursors:{shared:{from:0,to:1,head:1}},changes:[{model:'Task',identity:{id:'a'},stamp:1,state:{title:'first'}}]});assert.equal(prepared,2,'the push readback and the pull each prepared the loader once; the replays did not');
});
test('explicit rejection rolls back only mutation and its publication',async()=>{
 const result=JSON.parse(await backend.push('alice',push('refusal',1,[mutation(1,'good','b'),mutation(2,'refuse','c'),mutation(3,'last','d')])));
 assert.deepEqual(result.rejections,[{ordinal:2,code:'task.refused'}]);assert.equal(await count('business_task'),3);
 assert.deepEqual(result.records,[authority('b',1,{title:'good'}),authority('d',1,{title:'last'})],'only the successful mutations contribute authority');assert.equal(to(await pull()),3);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='c'")).length,0);
});
test('rejected mutation publishes nothing even though it declared a membership first',async()=>{
 const head=to(await pull());
 const result=JSON.parse(await backend.push('alice',push('refuse-only',1,[mutation(1,'refuse','refuse-only-a')])));
 assert.deepEqual(result.rejections,[{ordinal:1,code:'task.refused'}]);
 assert.equal(to(await pull()),head);
});
test('a handler that throws rejects only its mutation and reaches onError',async()=>{
 const errors=[];
 const crashBackend=createBackend({config,database:database(),authenticate,onError:e=>errors.push(e),handlers:{async edit({input,tx}){
  await write(tx,input.task.identity.id,input.task.patch.title);
  if(input.task.patch.title==='crash')throw new Error('business crash');
 }},loaders:{task:readTasks}});
 const result=JSON.parse(await crashBackend.push('alice',push('crash',1,[mutation(1,'before','e'),mutation(2,'crash','f'),mutation(3,'after','g')])));
 assert.deepEqual(result.rejections,[{ordinal:2,code:'handler.failed'}]);
 assert.deepEqual(result.records,[authority('e',1,{title:'before'}),authority('g',1,{title:'after'})]);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='f'")).length,0,'the failed mutation rolled back its write');
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM axton_client WHERE client_id='crash'")).length,1,'the batch as a whole still committed');
 assert.equal(errors.length,1);assert.equal(errors[0].message,'business crash');
});
test('a handler whose SQL fails leaves an aborted transaction that the savepoint rollback recovers into one rejection',async()=>{
 const errors=[];
 const sqlBackend=createBackend({config,database:database(),authenticate,onError:e=>errors.push(e),handlers:{async edit({input,tx}){
  await write(tx,input.task.identity.id,input.task.patch.title);
  // A PostgreSQL error (not a JavaScript one) marks the whole transaction aborted;
  // only ROLLBACK TO SAVEPOINT can make it usable again for the next mutation.
  if(input.task.patch.title==='sql')await tx.$executeRawUnsafe('INSERT INTO business_task(id,title) VALUES($1,$2)',input.task.identity.id,'duplicate');
 }},loaders:{task:readTasks}});
 const result=JSON.parse(await sqlBackend.push('alice',push('sql-fail',1,[mutation(1,'first','sql-a'),mutation(2,'sql','sql-b'),mutation(3,'third','sql-c')])));
 assert.deepEqual(result.rejections,[{ordinal:2,code:'handler.failed'}]);
 assert.deepEqual(result.records,[authority('sql-a',1,{title:'first'}),authority('sql-c',1,{title:'third'})]);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='sql-b'")).length,0,'the failed mutation rolled back its write');
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id IN ('sql-a','sql-c')")).length,2,'the mutations around it committed');
 assert.equal(errors.length,1);assert.match(String(errors[0].message??errors[0]),/23505|already exists/);
});
test('a loader that throws during readback rejects only that mutation',async()=>{
 const errors=[];
 const throwingBackend=createBackend({config,database:database(),authenticate,onError:e=>errors.push(e),handlers:{async edit({input,tx}){await write(tx,input.task.identity.id,input.task.patch.title);}},loaders:{task:async call=>{if(call.ids.some(id=>id.id==='loader-throw-bad'))throw new Error('loader broke');return readTasks(call);}}});
 const result=JSON.parse(await throwingBackend.push('alice',push('loader-throw',1,[mutation(1,'ok','loader-throw-ok'),mutation(2,'bad','loader-throw-bad'),mutation(3,'ok2','loader-throw-ok2')])));
 assert.deepEqual(result.rejections,[{ordinal:2,code:'loader.failed'}]);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='loader-throw-bad'")).length,0,'the loader failure rolled back its write');
 assert.deepEqual(result.records,[authority('loader-throw-ok',1,{title:'ok'}),authority('loader-throw-ok2',1,{title:'ok2'})]);
 assert.equal(errors.length,1);assert.equal(errors[0].message,'loader broke');
});
test('a handler that breaks the transaction still fails the delivery',async()=>{
 const before=called;let broken=false;
 const breakingBackend=createBackend({config,database:database(),authenticate,handlers:{async edit({input,tx}){
  called++;
  await write(tx,input.task.identity.id,input.task.patch.title);
  if(!broken){broken=true;try{await tx.$executeRawUnsafe('SELECT 1/0');}catch{}}
 }},loaders:{task:readTasks}});
 // The handler swallows its own PostgreSQL error, but the aborted transaction
 // then poisons the engine's next host call (release/advanceStamp): a whole
 // delivery failure, not a per-mutation one, whatever shape the error takes.
 await assert.rejects(()=>breakingBackend.push('alice',push('broken',1,[mutation(1,'break','broken-a')])));
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='broken-a'")).length,0,'nothing committed');
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM axton_client WHERE client_id='broken'")).length,0);
 const receipt=JSON.parse(await breakingBackend.push('alice',push('broken',1,[mutation(1,'break','broken-a')])));
 assert.deepEqual(receipt.rejections,[]);
 assert.equal(called,before+2,'the retry ran the handler again');
});
test('a mixed batch rejects the unsupported version alone and commits the rest',async()=>{
 const before=called;
 const result=JSON.parse(await backend.push('alice',push('version',1,[{...mutation(1,'ignored','v'),version:2},mutation(2,'committed','w')])));
 assert.deepEqual(result.rejections,[{ordinal:1,code:'mutation_version_unsupported'}]);
 assert.equal(called,before+1);
 assert.deepEqual(result.records,[authority('w',1,{title:'committed'})]);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='w'")).length,1);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='v'")).length,0);
});
test('loaders receive no channel',async()=>{
 await pull('shared',0);assert.ok(loaderCalls.length>1,'pushes and pulls both reached the loader');
 for(const keys of loaderCalls)assert.deepEqual([...keys].sort(),['ids','tx','userId']);
});
test('a pull reaches the loader of the declared model version and normalizes rows with that contract',async()=>{
 // Task v2 adds a nullable `note`; v1 keeps {id, title}. Each client declares
 // the version it reads and is served by that version's loader and contract.
 const c=structuredClone(config);c.mutations=[];c.schema.models[0].version=2;
 const v1={name:'Task',version:1,identity:['id'],fields:schema.models[0].fields,enums:[]};
 c.schema.models[0].fields=[...schema.models[0].fields,{name:'note',type:{kind:'scalar',name:'string'},nullable:true}];
 const v2={name:'Task',version:2,identity:['id'],fields:c.schema.models[0].fields,enums:[]};
 c.models=[v1,v2];
 const reached=[];
 const versioned=createBackend({config:c,database:database(),authenticate,handlers:{},loaders:{task:{
  async v1({ids}){reached.push(1);return ids.map(id=>({id:id.id,title:'old'}))},
  async v2({ids}){reached.push(2);return ids.map(id=>({id:id.id,title:'new',note:'n'}))},
 }}});
 const page=await versioned.pull('alice',pullBody({shared:0},{Task:2}));
 const changes=JSON.parse(page).changes.filter(ch=>ch.state!==null);
 assert.ok(changes.length>0);assert.deepEqual(changes[0].state,{title:'new',note:'n'});
 assert.deepEqual([...new Set(reached)],[2],"only the declared version's loader ran");
 const old=await versioned.pull('alice',pullBody({shared:0},{Task:1}));
 assert.deepEqual(JSON.parse(old).changes.filter(ch=>ch.state!==null)[0].state,{title:'old'},'the same data reaches a v1 client in its own shape');
 assert.deepEqual(reached.at(-1),1);
 // A missing declaration is a malformed request; an unretained version or an
 // unknown model is refused with the model named.
 await assert.rejects(()=>versioned.pull('alice',JSON.stringify({cursors:{shared:0}})),error=>error instanceof EngineError&&error.code==='request.invalid');
 await assert.rejects(()=>versioned.pull('alice',pullBody({shared:0},{Task:3})),error=>error instanceof EngineError&&error.code==='model_version_unsupported'&&error.details.model==='Task'&&error.details.version===3);
 await assert.rejects(()=>versioned.pull('alice',pullBody({shared:0},{Task:2,Ghost:1})),error=>error instanceof EngineError&&error.code==='model_version_unsupported'&&error.details.model==='Ghost');
 // A row outside the served contract is a loader defect, never silently trimmed:
 // that record fails alone as loader.invalid.
 const wide=createBackend({config:c,database:database(),authenticate,handlers:{},loaders:{task:{
  async v1({ids}){return ids.map(id=>({id:id.id,title:'old'}))},
  async v2({ids}){return ids.map(id=>({id:id.id,title:'new',note:'n',extra:true}))},
 }}});
 const widePage=JSON.parse(await wide.pull('alice',pullBody({shared:0},{Task:2})));
 assert.ok(widePage.changes.length>0);
 assert.ok(widePage.changes.every(c=>c.error==='loader.invalid'&&c.state===null),'every malformed row fails alone');
});
test('compaction materializes latest state; deletion is aligned null',async()=>{
 await backend.push('alice',push('dedup',2,[mutation(1,'updated')]));await assert.rejects(()=>backend.push('alice',push('dedup',1,[mutation(1,'first')])),/overlap/);
 await backend.transaction(async({tx,channel,touch})=>{await tx.$executeRawUnsafe("DELETE FROM business_task WHERE id='a'");touch.task({id:'a'});channel('shared').task.add({id:'a'});});
 const page=await pull();assert.equal(page.changes.length,4);assert.deepEqual(page.changes.find(c=>c.identity.id==='a').state,null);assert.equal(to(page),6);
 await assert.rejects(()=>pull('shared',999),/cursor ahead/);
});
test('50-row pages retain original cursor progression and remainder reaches head',async()=>{
 await backend.transaction(async({tx,channel,touch})=>{const shared=channel('shared');for(let i=0;i<51;i++){const id={id:`page-${i}`};await tx.$executeRawUnsafe('INSERT INTO business_task(id,title) VALUES($1,$2)',id.id,'page');touch.task(id);shared.task.add(id);}});
 const first=await pull('shared',6);assert.equal(first.changes.length,50);assert.equal(to(first),56);assert.equal(first.cursors.shared.head,57);const last=await pull('shared',56);assert.equal(last.changes.length,1);assert.equal(to(last),57);assert.equal(last.cursors.shared.head,57);
});
test('concurrent same-client retry executes once under PostgreSQL lock',async()=>{const before=called;const request=push('race',1,[mutation(1,'race','race')]);const receipts=await Promise.all([backend.push('alice',request),backend.push('alice',request)]);assert.equal(receipts[0],receipts[1]);assert.equal(called,before+1);});
test('declarations roll back with the user transaction, and an unknown Model is refused at the declaration',async()=>{const before=to(await pull('shared',56));await assert.rejects(()=>backend.transaction(async({channel,touch})=>{touch.task({id:'rollback'});channel('shared').task.add({id:'rollback'});throw new Error('cancel');}),/cancel/);assert.equal(to(await pull('shared',56)),before);await assert.rejects(()=>external(backend,'shared',[{model:'Unknown',identity:{id:'x'}}]),/unknown Model Unknown/);});
test('loader defects fail only their records; the page is served and the cursor advances',async()=>{
 const make=load=>createBackend({config:{...config,mutations:[]},database:database(),authenticate,handlers:{},loaders:{task:load}});
 const misaligned=JSON.parse(await make(async()=>[]).pull('alice',pullBody({shared:0})));
 assert.ok(misaligned.changes.length>0);
 assert.ok(misaligned.changes.every(c=>c.error==='loader.invalid'),'an answer that cannot be matched fails each record');
 assert.ok(to(misaligned)>0,'the cursor advances past the failed records');
 const reported=[];
 const malformed=JSON.parse(await createBackend({config:{...config,mutations:[]},database:database(),authenticate,onError:e=>reported.push(e.message),handlers:{},loaders:{task:async({ids})=>ids.map(id=>id.id==='a'?({title:'x',unexpected:true}):({title:'ok'}))}}).pull('alice',pullBody({shared:0})));
 assert.equal(reported.length,1,reported.join('; '));assert.match(reported[0],/Task contract does not accept: \{"id":"a"\}/);
 const bad=malformed.changes.filter(c=>c.error);
 assert.deepEqual(bad.map(c=>[c.identity.id,c.error]),[['a','loader.invalid']],'only the malformed row fails');
 assert.ok(malformed.changes.filter(c=>!c.error).every(c=>c.state===null||c.state.title==='ok'));
});
test('registered translator rejects one mutation; malformed translator code aborts transaction',async()=>{
 const make=code=>createBackend({config,database:database(),authenticate,translateRejection:()=>code,handlers:{async edit({tx}){await tx.$executeRawUnsafe("INSERT INTO business_task(id,title) VALUES('translated','temporary')");throw new Error('product refusal');}},loaders:{async task(){return []}}});
 const receipt=JSON.parse(await make('product.denied').push('alice',push('translated',1,[mutation(1,'x')])));assert.deepEqual(receipt.rejections,[{ordinal:1,code:'product.denied'}]);assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='translated'")).length,0);
 await assert.rejects(()=>make('Not a machine code').push('alice',push('bad-translator',1,[mutation(1,'x')])),/stable machine code/);assert.equal((await db.$queryRawUnsafe("SELECT * FROM axton_client WHERE client_id='bad-translator'")).length,0);
});

test('HTTP adapter authenticates and serves the real native persistence path',async()=>{
 const server=await backend.listen({port:0});const url=server.url;
 try {
  const denied=await fetch(`${url}/sync/pull`,{method:'POST',body:pullBody({shared:0})});assert.equal(denied.status,401);
  const result=await fetch(`${url}/sync/mutations`,{method:'POST',headers:{authorization:'Bearer alice'},body:push('http',1,[mutation(1,'network','http')])});assert.equal(result.status,200);assert.deepEqual((await result.json()).rejections,[]);
  const page=await fetch(`${url}/sync/pull`,{method:'POST',headers:{authorization:'Bearer alice'},body:pullBody({shared:58})});assert.equal(page.status,200);assert.equal((await page.json()).changes[0].state.title,'network');
  const bad=await fetch(`${url}/sync/pull`,{method:'POST',headers:{authorization:'Bearer alice'},body:'{'});assert.equal(bad.status,400);
 }finally{await server.close();}
});
test('an undefined loader entry fails only its record, reaches onError and never becomes a tombstone',async()=>{
 const errors=[];
 const bad=createBackend({config:{...config,mutations:[]},database:database(),authenticate,onError:e=>errors.push(e.message),handlers:{},loaders:{async task({ids}){return ids.map(id=>id.id==='a'?undefined:null)}}});
 const page=JSON.parse(await bad.pull('alice',pullBody({shared:0})));
 const failed=page.changes.filter(c=>c.error);
 assert.deepEqual(failed.map(c=>[c.identity.id,c.error,c.state]),[['a','loader.failed',null]]);
 assert.ok(errors.some(m=>/undefined entry/.test(m)),errors.join('; '));
});
test('a declaration naming an unknown Model fails its handler: only that mutation and its write roll back',async()=>{
 const errors=[];
 const broken=createBackend({config,database:database(),authenticate,onError:e=>errors.push(e.message),handlers:{async edit({tx,channel}){await tx.$executeRawUnsafe("INSERT INTO business_task(id,title) VALUES('caught','bad')");channel('shared').add([{model:'Unknown',identity:{id:'caught'}}]);}},loaders:{async task({ids}){return ids.map(()=>null)}}});
 const receipt=JSON.parse(await broken.push('alice',push('caught',1,[mutation(1,'x')])));
 assert.deepEqual(receipt.rejections,[{ordinal:1,code:'handler.failed'}]);assert.match(errors[0],/unknown Model Unknown/);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='caught'")).length,0);assert.equal((await db.$queryRawUnsafe("SELECT * FROM axton_client WHERE client_id='caught'")).length,1,'the batch still commits');
});
test('a nonfinite loader value fails its record rather than clearing to null',async()=>{
 const expanded=structuredClone(config);expanded.schema.models[0].fields.push({name:'score',type:{kind:'scalar',name:'float'},nullable:true});
 const errors=[];
 expanded.mutations=[];const bad=createBackend({config:expanded,database:database(),authenticate,onError:e=>errors.push(e.message),handlers:{},loaders:{async task({ids}){return ids.map(()=>({title:'x',score:NaN}))}}});
 const page=JSON.parse(await bad.pull('alice',pullBody({shared:56})));
 assert.ok(page.changes.length>0);
 assert.ok(page.changes.every(c=>c.error==='loader.failed'&&c.state===null));
 assert.ok(errors.some(m=>/nonfinite/.test(m)),errors.join('; '));
});
test('backend.transaction rolls the business write back when a declaration is refused',async()=>{
 await assert.rejects(()=>backend.transaction(async({tx,channel})=>{await tx.$executeRawUnsafe("INSERT INTO business_task(id,title) VALUES('external','bad')");channel('shared').add([{model:'Unknown',identity:{id:'x'}}]);}),/unknown Model Unknown/);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='external'")).length,0);
 // A malformed UUID is refused at the declaration, by the engine's own rule, before anything settles.
 const tickets=createBackend({config:{mutations:[],schema:{enums:[],models:[ticketModel]}},database:database(),authenticate,handlers:{},loaders:{async ticket({ids}){return ids.map(()=>null)}}});
 let declared=false;
 await assert.rejects(()=>tickets.transaction(async({tx,touch})=>{await tx.$executeRawUnsafe("INSERT INTO business_task(id,title) VALUES('external','bad')");try{touch.ticket({id:'not-a-uuid'});}finally{declared=true;}}),/Ticket identity field id must be a UUID/);
 assert.ok(declared,'the refusal was synchronous, inside the body');
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='external'")).length,0);
});
test('a malformed UUID declared in a push rejects only its mutation; well-formed identities settle beside it',async()=>{
 // The accepted forms are the engine's: were the collector more lenient, the
 // engine would refuse the settlement and abort the whole delivery.
 const errors=[];
 const slot={name:'Slot',identity:['at'],fields:[{name:'at',type:{kind:'scalar',name:'dateTime'},nullable:false}]};
 const slots={'uuid-a':'2026-01-01T00:00:00.123456789+05:30','uuid-b':'2026-03-01T00:00:00Z','uuid-c':'2024-02-29T00:00:00z'};
 const mixed=createBackend({config:{...config,schema:{enums:[],models:[...schema.models,ticketModel,slot]}},database:database(),authenticate,onError:e=>errors.push(e.message),handlers:{async edit({input,tx,touch}){
  const {identity,patch}=input.task;await write(tx,identity.id,patch.title);
  touch.ticket({id:patch.title});touch.slot({at:slots[identity.id]});
 }},loaders:{task:readTasks,async ticket({ids}){return ids.map(()=>null)},async slot({ids}){return ids.map(()=>null)}}});
 const receipt=JSON.parse(await mixed.push('alice',push('uuid-batch',1,[mutation(1,'123e4567-e89b-42d3-a456-426614174000','uuid-a'),mutation(2,'not-a-uuid','uuid-b'),mutation(3,'123E4567-E89B-82D3-B456-426614174001','uuid-c')])));
 assert.deepEqual(receipt.rejections,[{ordinal:2,code:'handler.failed'}]);
 assert.equal(errors.length,1);assert.match(errors[0],/Ticket identity field id must be a UUID/);
 assert.deepEqual(receipt.records.map(r=>r.identity.id),['uuid-a','uuid-c'],'the unrelated mutations commit');
 assert.deepEqual((await db.$queryRawUnsafe("SELECT id FROM business_task WHERE id LIKE 'uuid-%' ORDER BY id")).map(r=>r.id),['uuid-a','uuid-c']);
 assert.deepEqual((await db.$queryRawUnsafe("SELECT model, identity_key FROM axton_record WHERE model IN ('Ticket','Slot') ORDER BY model, identity_key")).map(r=>[r.model,r.identity_key]),[
  ['Slot','{"at":"2024-02-29T00:00:00.000Z"}'],['Slot','{"at":"2025-12-31T18:30:00.123Z"}'],
  ['Ticket','{"id":"123e4567-e89b-42d3-a456-426614174000"}'],['Ticket','{"id":"123e4567-e89b-82d3-b456-426614174001"}'],
 ],'the engine canonicalized each accepted identity');
});
test('repeatable-read runner keeps head, scan, and loader coherent across concurrent publication',async()=>{
 await external(backend,'snapshot',[{model:'Task',identity:{id:'a'}}]);let changed=false;
 const reader=createBackend({config:{...config,mutations:[]},database:{transaction:run,persistence:tx=>{const storage=store(tx);return {call:async r=>{const result=await storage.call(r);if(r.op==='head'&&!changed){changed=true;await external(backend,'snapshot',[{model:'Task',identity:{id:'b'}}]);}return result;}}}},authenticate,handlers:{},loaders:{async task({ids}){return ids.map(()=>null)}}});
 const page=JSON.parse(await reader.pull('alice',pullBody({snapshot:0})));assert.equal(to(page,'snapshot'),1);assert.equal(page.changes.length,1);
 const next=JSON.parse(await reader.pull('alice',pullBody({snapshot:1})));assert.equal(to(next,'snapshot'),2);assert.equal(next.changes.length,1);
});

const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const openSocket=async port=>{
 const socket=new serverSdk.WebSocket(`ws://127.0.0.1:${port}/sync/live`,{headers:{authorization:'Bearer alice'}});
 await new Promise((resolve,reject)=>{socket.addEventListener('open',resolve,{once:true});socket.addEventListener('error',reject,{once:true});});
 return socket;
};
const nextMessage=socket=>new Promise((resolve,reject)=>{
 const timer=setTimeout(()=>{cleanup();reject(new Error('timed out waiting for live frame'));},2000);
 const message=event=>{cleanup();resolve(JSON.parse(String(event.data)));};
 const closed=()=>{cleanup();reject(new Error('socket closed before frame'));};
 const cleanup=()=>{clearTimeout(timer);socket.removeEventListener('message',message);socket.removeEventListener('close',closed);};
 socket.addEventListener('message',message);socket.addEventListener('close',closed);
});

test('live transport negotiates, wakes only after commit, reconnects, and cleans up',async()=>{
 const server=await backend.listen({port:0});const port=Number(new URL(server.url).port);
 const socket=await openSocket(port);const frames=[];socket.addEventListener('message',event=>frames.push(JSON.parse(String(event.data))));
 socket.send(JSON.stringify({type:'subscribe',channels:['shared','bob','shared'],models:{Task:1}}));
 while(frames.length<1)await delay(5);
 assert.equal(frames[0].type,'subscribed');assert.deepEqual(Object.keys(frames[0].cursors),['bob','shared']);assert.equal(frames[0].cursors.bob,0,'a fresh channel is at head 0');assert.ok(frames[0].cursors.shared>0,'the acknowledgement carries the current head');

 let release,ready;const held=new Promise(resolve=>{release=resolve;});const started=new Promise(resolve=>{ready=resolve;});
 const committing=backend.transaction(async({tx,channel,touch})=>{
   await tx.$executeRawUnsafe("INSERT INTO business_task(id,title) VALUES('live-external','committed')");
   touch.task({id:'live-external'});channel('shared').task.add({id:'live-external'});ready();await held;
 });
 await started;await delay(80);assert.equal(frames.length,1,'uncommitted publication must stay silent');release();await committing;
 while(frames.length<2)await delay(5);
 assert.deepEqual(frames[1].changes.at(-1),{model:'Task',identity:{id:'live-external'},stamp:1,state:{title:'committed'}});assert.deepEqual(Object.keys(frames[1].cursors),['shared'],'a frame names only the channels that moved');assert.equal(frames[1].cursors.shared.from,frames[0].cursors.shared);

 const pageStart=frames.length;
 await backend.transaction(async({tx,channel,touch})=>{const shared=channel('shared');for(let i=0;i<51;i++){const id=`live-page-${i}`;await tx.$executeRawUnsafe('INSERT INTO business_task(id,title) VALUES($1,$2)',id,'paged');touch.task({id});shared.task.add({id});}});
 while(frames.length<pageStart+2)await delay(5);assert.equal(frames[pageStart].changes.length,50);assert.equal(frames[pageStart+1].changes.length,1);assert.equal(frames[pageStart+1].cursors.shared.from,frames[pageStart].cursors.shared.to);assert.ok(frames[pageStart].cursors.shared.to<frames[pageStart].cursors.shared.head,'a full frame says the channel continues');

 const beforeRollback=frames.length;
 await assert.rejects(()=>backend.transaction(async({channel,touch})=>{touch.task({id:'live-rollback'});channel('shared').task.add({id:'live-rollback'});throw new Error('rollback live');}),/rollback live/);
 await delay(80);assert.equal(frames.length,beforeRollback);

 socket.close();await new Promise(resolve=>socket.addEventListener('close',resolve,{once:true}));
 const reconnected=await openSocket(port);reconnected.send(JSON.stringify({type:'subscribe',channels:['shared'],models:{Task:1}}));await nextMessage(reconnected);
 const pagePromise=nextMessage(reconnected);await backend.push('alice',push('live-push',1,[mutation(1,'from push','live-push')]));
 const page=await pagePromise;assert.deepEqual(page.changes.at(-1).state,{title:'from push'});
 const afterPush=[];reconnected.addEventListener('message',event=>afterPush.push(event));await backend.push('alice',push('live-push',1,[mutation(1,'from push','live-push')]));
 await delay(80);assert.equal(afterPush.length,0,'duplicate receipt must not wake live subscribers');
 const protocol=await openSocket(port);protocol.send(JSON.stringify({type:'subscribe',channels:['shared'],models:{Task:1}}));await nextMessage(protocol);protocol.send('{}');
 const closeCode=await new Promise(resolve=>protocol.addEventListener('close',event=>resolve(event.code),{once:true}));assert.equal(closeCode,1002);
 reconnected.close();await new Promise(resolve=>reconnected.addEventListener('close',resolve,{once:true}));
 await server.close();
});

test('a publication committed between negotiation and the acknowledgement is delivered by the first drain',async()=>{
 // The negotiation transaction commits, then the wrapper holds the result on a
 // gate; a push commits meanwhile, before any listener exists for the socket.
 const base=prisma(db);let hold;
 const gated={transaction:async body=>{const result=await base.transaction(body);const gate=hold;hold=undefined;if(gate)await gate;return result;},persistence:base.persistence};
 const gatedBackend=createBackend({config,database:gated,authenticate,handlers:{async edit({input,tx,channel}){const {identity,patch}=input.task;await write(tx,identity.id,patch.title);channel('shared').task.add(identity);}},loaders:{task:readTasks}});
 const server=await gatedBackend.listen({port:0});const port=Number(new URL(server.url).port);
 try{
  const socket=await openSocket(port);const frames=[];socket.addEventListener('message',event=>frames.push(JSON.parse(String(event.data))));
  let release;hold=new Promise(resolve=>{release=resolve;});
  socket.send(JSON.stringify({type:'subscribe',channels:['shared'],models:{Task:1}}));
  await delay(100);assert.equal(frames.length,0,'the acknowledgement is held on the gate');
  await gatedBackend.push('alice',push('between',1,[mutation(1,'between negotiation and ack','live-between')]));
  await delay(50);assert.equal(frames.length,0,'no listener exists yet, so the commit wakes nobody');
  release();
  while(frames.length<2)await delay(5);
  assert.equal(frames[0].type,'subscribed');assert.deepEqual(Object.keys(frames[0].cursors),['shared']);
  assert.deepEqual(frames[1].changes.at(-1),{model:'Task',identity:{id:'live-between'},stamp:1,state:{title:'between negotiation and ack'}});
  socket.close();await new Promise(resolve=>socket.addEventListener('close',resolve,{once:true}));
 }finally{await server.close();}
});
test('loader safely converts PostgreSQL BigInt scalar and list values without widening wire range', async () => {
  const int = {kind: 'scalar', name: 'int'};
  let value = 9007199254740991n;
  const bigintBackend = createBackend({
    config: {mutations: [], schema: {enums: [], models: [{name: 'Counter', identity: ['id'], fields: [
      {name: 'id', type: {kind: 'scalar', name: 'string'}, nullable: false},
      {name: 'count', type: int, nullable: false},
      {name: 'counts', type: {kind: 'list', element: int}, nullable: false},
    ]}]}},
    database: prisma(db), authenticate,
    handlers: {},
    loaders: {async counter({tx}) {return tx.$queryRawUnsafe(
      'SELECT $1::bigint AS count, ARRAY[$1::bigint,(-$1)::bigint] AS counts', value,
    )}},
  });
  await external(bigintBackend,'bigints',[{model: 'Counter', identity: {id: 'one'}}]);
  const request = pullBody({bigints: 0},{Counter:1});
  let page = JSON.parse(await bigintBackend.pull('alice', request));
  assert.deepEqual(page.changes[0].state, {count: Number.MAX_SAFE_INTEGER, counts: [Number.MAX_SAFE_INTEGER, -Number.MAX_SAFE_INTEGER]});
  value = -9007199254740991n;
  page = JSON.parse(await bigintBackend.pull('alice', request));
  assert.equal(page.changes[0].state.count, -Number.MAX_SAFE_INTEGER);
  for (const overflow of [9007199254740992n, -9007199254740992n]) {
    value = overflow;
    page = JSON.parse(await bigintBackend.pull('alice', request));
    assert.equal(page.changes[0].error, 'loader.failed');
    assert.equal(page.changes[0].state, null);
  }
});
test('listen answers pull over HTTP with authentication and closes cleanly',async()=>{
 const server=await backend.listen({port:0});
 try{
  const denied=await fetch(`${server.url}/sync/pull`,{method:'POST',body:'{}'});assert.equal(denied.status,401);
  const ok=await fetch(`${server.url}/sync/pull`,{method:'POST',headers:{authorization:'Bearer alice'},body:pullBody({shared:0})});assert.equal(ok.status,200);
 }finally{await server.close();}
});
test('onError captures server-side failures and HTTP responds with {code:"server"}',async()=>{
 const errors=[];
 const boomBackend=createBackend({config,database:database(),authenticate:async()=>{throw new Error('boom')},onError:e=>errors.push(e),handlers:{async edit(){}},loaders:{async task({ids}){return ids.map(()=>null)}}});
 const server=await boomBackend.listen({port:0});
 try{
  const result=await fetch(`${server.url}/sync/mutations`,{method:'POST',headers:{authorization:'Bearer alice'},body:push('boom',1,[mutation(1,'x','boom-a')])});
  assert.equal(result.status,500);
  assert.deepEqual(await result.json(),{code:'server'});
  assert.equal(errors.length,1);
  assert.equal(errors[0].message,'boom');
 }finally{await server.close();}
});
test('slot arguments are plain data; a handler declares with their identity, and its handles close when it settles',async()=>{
 await backend.push('alice',push('plain',1,[mutation(1,'hello','plain-a')]));
 assert.deepEqual(Object.keys(lastInput.task),['identity','patch']);
 assert.deepEqual(Object.getOwnPropertySymbols(lastInput.task),[],'no hidden record tag');
 assert.deepEqual((await invalidations('plain-a')).map(([channel,,stamp])=>[channel,stamp]),[['shared',1]]);
 assert.throws(()=>lastHandles.channel('shared'),/closed/);assert.throws(()=>lastHandles.touch.task({id:'plain-a'}),/closed/);
});
test('a handler that declares no membership still returns readback records and touches no channel',async()=>{
 const channels=await count('axton_channel');
 const receipt=JSON.parse(await backend.push('alice',push('quiet',1,[mutation(1,'quiet','quiet-a')])));
 assert.deepEqual(receipt,{batchSequence:1,clientId:'quiet',records:[authority('quiet-a',1,{title:'quiet'})],rejections:[]});
 assert.equal(await recordStamp('quiet-a'),1);assert.deepEqual(await invalidations('quiet-a'),[]);assert.equal(await count('axton_channel'),channels);
 const again=JSON.parse(await backend.push('alice',push('quiet',2,[mutation(2,'quiet','quiet-a')])));assert.deepEqual(again.records,[authority('quiet-a',2,{title:'quiet'})],'every successful change advances the stamp, published or not');
});
test('a touched extra record added to a Channel is settled and delivered there, but is not caller authority',async()=>{
 const receipt=JSON.parse(await backend.push('alice',push('extra',1,[mutation(1,'extra','extra-a')])));
 assert.deepEqual(receipt.records,[authority('extra-a',1,{title:'extra'})],'only the uploaded target is read back');assert.equal(await recordStamp('extra-a-extra'),1,'the touched record is still settled');
 assert.deepEqual((await invalidations('extra-a')).map(([channel,,stamp])=>[channel,stamp]),[['shared',1]]);assert.deepEqual((await invalidations('extra-a-extra')).map(([channel,,stamp])=>[channel,stamp]),[['shared',1]]);
});
test('an added record is not a changed one: it is initialised at stamp 1 once, and re-adding a member publishes nothing',async()=>{
 assert.equal(await recordStamp('pub-only'),null,'no metadata before the first enrolment');
 const first=JSON.parse(await backend.push('alice',push('pub-only',1,[mutation(1,'publish-only','pub-only-a')])));
 assert.deepEqual(first.records,[authority('pub-only-a',1,{title:'publish-only'})],'an added record is not a changed one');
 const other=await head('other');assert.deepEqual(await invalidations('pub-only'),[['other',other,1]]);assert.equal(await recordStamp('pub-only'),1,'first enrolment initialises the stamp');
 const second=JSON.parse(await backend.push('alice',push('pub-only',2,[mutation(2,'publish-only','pub-only-b')])));
 assert.deepEqual(second.records,[authority('pub-only-b',1,{title:'publish-only'})]);
 assert.equal(await head('other'),other,'an existing unchanged member is not published again');assert.deepEqual(await invalidations('pub-only'),[['other',other,1]],'the invalidation keeps its cursor');assert.equal(await recordStamp('pub-only'),1,'enrolling an unchanged record never advances it');
 const page=await pull('other',0);assert.deepEqual(page.changes.find(c=>c.identity.id==='pub-only'),{model:'Task',identity:{id:'pub-only'},stamp:1,state:null});
 assert.equal(page.changes.some(c=>c.identity.id.startsWith('pub-only-')),false,'the changed records went to shared only');
});
test('a loader refusal during push rejects only that mutation; so does a thrown loader error',async()=>{
 const make=load=>createBackend({config,database:database(),authenticate,handlers:{async edit({input,tx,channel}){await write(tx,input.task.identity.id,input.task.patch.title);channel('shared').task.add(input.task.identity);}},loaders:{task:load}});
 const refusing=make(async call=>{if(call.ids.some(id=>id.id==='ld-forbidden'))throw new MutationRejected('task.forbidden');return readTasks(call);});
 const receipt=JSON.parse(await refusing.push('alice',push('loader-refuse',1,[mutation(1,'ok','ld-ok'),mutation(2,'hidden','ld-forbidden'),mutation(3,'ok','ld-last')])));
 assert.deepEqual(receipt.rejections,[{ordinal:2,code:'task.forbidden'}]);assert.deepEqual(receipt.records,[authority('ld-last',1,{title:'ok'}),authority('ld-ok',1,{title:'ok'})]);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='ld-forbidden'")).length,0,'the refused mutation rolled back its write');assert.equal(await recordStamp('ld-forbidden'),null,'and its stamp');assert.deepEqual(await invalidations('ld-forbidden'),[],'and its publication');
 const translated=createBackend({config,database:database(),authenticate,translateRejection:()=>'task.translated',handlers:{async edit({input,tx}){await write(tx,input.task.identity.id,input.task.patch.title);}},loaders:{async task(){throw new Error('product read refusal');}}});
 assert.deepEqual(JSON.parse(await translated.push('alice',push('loader-translated',1,[mutation(1,'x','ld-translated')]))).rejections,[{ordinal:1,code:'task.translated'}]);
 const crashing=JSON.parse(await make(async()=>{throw new Error('loader crash');}).push('alice',push('loader-crash',1,[mutation(1,'x','ld-crash')])));
 assert.deepEqual(crashing.rejections,[{ordinal:1,code:'loader.failed'}]);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id='ld-crash'")).length,0);assert.equal((await db.$queryRawUnsafe("SELECT * FROM axton_client WHERE client_id='loader-crash'")).length,1,'the batch still commits');
});
test('a declaration validates its Channel and records at the call; the invalid call rejects only that mutation as a failure',async()=>{
 const badchan=JSON.parse(await backend.push('alice',push('badchan',1,[mutation(1,'empty-channel','bad-a')])));assert.equal(badchan.rejections[0].code,'handler.failed');
 const badrecs=JSON.parse(await backend.push('alice',push('badrecs',1,[mutation(1,'bad-records','bad-b')])));assert.equal(badrecs.rejections[0].code,'handler.failed');
 const badbogus=JSON.parse(await backend.push('alice',push('badbogus',1,[mutation(1,'bogus-record','bad-c')])));assert.equal(badbogus.rejections[0].code,'handler.failed');
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM business_task WHERE id IN ('bad-a','bad-b','bad-c')")).length,0,'the failed mutations rolled back their writes');
});
test('a handler may keep using the transaction after a declaration; the membership settles with the commit',async()=>{
 const deferredBackend=createBackend({config,database:database(),authenticate,handlers:{async edit({input,tx,channel}){
  const {identity,patch}=input.task;
  channel('deferred').task.add(identity);
  await write(tx,identity.id,patch.title);
  await tx.$queryRawUnsafe('SELECT 1');
 }},loaders:{task:readTasks}});
 const receipt=JSON.parse(await deferredBackend.push('alice',push('deferred',1,[mutation(1,'deferred','deferred-a')])));
 assert.deepEqual(receipt,{batchSequence:1,clientId:'deferred',records:[authority('deferred-a',1,{title:'deferred'})],rejections:[]});
 const page=JSON.parse(await deferredBackend.pull('alice',pullBody({deferred:0})));
 assert.deepEqual(page.changes.at(-1),{model:'Task',identity:{id:'deferred-a'},stamp:1,state:{title:'deferred'}});
});
test('an all-rejected batch settles with no records',async()=>{
 const receipt=JSON.parse(await backend.push('alice',push('allrej',1,[mutation(1,'refuse','rej-a')])));
 assert.deepEqual(receipt,{batchSequence:1,clientId:'allrej',records:[],rejections:[{ordinal:1,code:'task.refused'}]});
});
test('advanceStamp increments without a channel: no invalidation, no channel head',async()=>{
 const stamps=await db.$transaction(async tx=>{const storage=store(tx);const ref={model:'Task',identityKey:key('stamped')};return [await storage.call({op:'advanceStamp',...ref}),await storage.call({op:'advanceStamp',...ref})];});
 assert.deepEqual(stamps,[1,2]);assert.equal(await recordStamp('stamped'),2);assert.deepEqual(await invalidations('stamped'),[]);
 assert.equal((await db.$queryRawUnsafe("SELECT * FROM axton_channel WHERE channel LIKE 'stamp%'")).length,0);
});
test('one push enrolled in two channels carries the same stamp to both and advances each head once',async()=>{
 const before=[await head('shared'),await head('other')];
 const receipt=JSON.parse(await backend.push('alice',push('two-channels',1,[mutation(1,'two','two-a')])));
 assert.deepEqual(receipt.records,[authority('two-a',1,{title:'two'})]);
 assert.deepEqual(await invalidations('two-a'),[['other',before[1]+1,1],['shared',before[0]+1,1]]);
 assert.deepEqual([await head('shared'),await head('other')],[before[0]+1,before[1]+1]);
});
test('an external write advances the stamp on every call; a push re-enrolling an unchanged member neither advances nor publishes it',async()=>{
 const change=()=>external(backend,'other',[{model:'Task',identity:{id:'pub-only'}}]);
 const start=await recordStamp('pub-only');await change();await change();assert.equal(await recordStamp('pub-only'),start+2,'an external notification reports a business change');
 const [[,cursor,stamp]]=await invalidations('pub-only');assert.equal(stamp,start+2);
 await backend.push('alice',push('pub-only',3,[mutation(3,'publish-only','pub-only-c')]));
 assert.equal(await recordStamp('pub-only'),start+2,'membership alone is distribution');assert.deepEqual(await invalidations('pub-only'),[['other',cursor,start+2]]);
});
test('concurrent first publications initialise one stamp of 1 and never overwrite an established one',async()=>{
 const ensure=tx=>store(tx).call({op:'ensureStamp',model:'Task',identityKey:key('ensure-race')});
 // The production runner: REPEATABLE READ with serialization retries, so a
 // loser that sees the winner's row only after its snapshot retries and reads 1.
 
 assert.deepEqual(await Promise.all([run(ensure),run(ensure),run(ensure)]),[1,1,1]);
 assert.equal((await db.$queryRawUnsafe('SELECT * FROM axton_record WHERE identity_key=$1',key('ensure-race'))).length,1);
 await db.$transaction(tx=>store(tx).call({op:'advanceStamp',model:'Task',identityKey:key('ensure-race')}));
 assert.equal(await db.$transaction(ensure),2,'ensureStamp keeps an advanced stamp');
});
test('a rolled-back transaction removes a first initialisation together with its publication',async()=>{
 await assert.rejects(()=>db.$transaction(async tx=>{const storage=store(tx);const stamp=await storage.call({op:'ensureStamp',model:'Task',identityKey:key('undone')});await storage.call({op:'publish',channel:'undone',model:'Task',identity:{id:'undone'},identityKey:key('undone'),stamp});throw new Error('cancel');}),/cancel/);
 assert.equal(await recordStamp('undone'),null);assert.deepEqual(await invalidations('undone'),[]);assert.equal(await head('undone'),0);
});
test('publish refuses a record without metadata or with a stamp that is not its current one',async()=>{
 await assert.rejects(()=>db.$transaction(tx=>store(tx).call({op:'publish',channel:'stale',model:'Task',identity:{id:'unstamped'},identityKey:key('unstamped'),stamp:1})),/Record metadata missing/);
 await assert.rejects(()=>db.$transaction(async tx=>{const storage=store(tx);await storage.call({op:'ensureStamp',model:'Task',identityKey:key('stale')});await storage.call({op:'publish',channel:'stale',model:'Task',identity:{id:'stale'},identityKey:key('stale'),stamp:2});}),/names stamp 2 .* is at stamp 1/);
 assert.equal(await head('stale'),0);assert.deepEqual(await invalidations('stale'),[]);
});
test('scan pairs the invalidation cursor with the current record stamp; a missing record row is a storage defect',async()=>{
 await backend.push('alice',push('join',1,[mutation(1,'published','join-a')]));
 const [[,cursor,stored]]=await invalidations('join-a');assert.equal(stored,1);
 // A change that reaches no Channel: the business write and its stamp alone
 // (a member's change through settlement would move the invalidation).
 await db.$transaction(async tx=>{await write(tx,'join-a','quiet');await store(tx).call({op:'advanceStamp',model:'Task',identityKey:key('join-a')});});
 assert.equal(await recordStamp('join-a'),2);assert.deepEqual(await invalidations('join-a'),[['shared',cursor,1]],'no publication: the invalidation row is untouched');
 const page=await pull('shared',cursor-1);
 assert.deepEqual(page.changes[0],{model:'Task',identity:{id:'join-a'},stamp:2,state:{title:'quiet'}},'the original cursor with the current stamp and content');
 const rows=await db.$transaction(tx=>store(tx).call({op:'scan',channel:'shared',after:cursor-1,limit:1}));assert.deepEqual(rows.map(r=>[r.cursor,r.stamp]),[[cursor,2]]);
 // Enrolled first: a scan answers members only.
 await db.$transaction(async tx=>{const storage=store(tx);const stamp=await storage.call({op:'ensureStamp',model:'Task',identityKey:key('orphan')});await storage.call({op:'setMembership',channel:'orphan',model:'Task',identityKey:key('orphan'),present:true});await storage.call({op:'publish',channel:'orphan',model:'Task',identity:{id:'orphan'},identityKey:key('orphan'),stamp});});
 // The membership foreign key forbids dropping a member's record row; forge the defect with triggers off.
 await db.$transaction(async tx=>{await tx.$executeRawUnsafe('SET LOCAL session_replication_role = replica');await tx.$executeRawUnsafe('DELETE FROM axton_record WHERE identity_key=$1',key('orphan'));});
 await assert.rejects(()=>db.$transaction(tx=>store(tx).call({op:'scan',channel:'orphan',after:0,limit:50})),/Record metadata missing/);
 await assert.rejects(()=>pull('orphan',0),/Record metadata missing/);
});
test('concurrent notifies of one record receive distinct stamps',async()=>{
 const change=()=>external(backend,'race-stamp',[{model:'Task',identity:{id:'stamp-race'}}]);
 await Promise.all([change(),change(),change(),change()]);
 assert.equal(await recordStamp('stamp-race'),4);
 const page=await pull('race-stamp',0);assert.equal(page.changes.length,1);assert.equal(page.changes[0].stamp,4);
});
import {createServer as createProxyServer,request as httpRequest} from 'node:http';
import {connect as tcpConnect} from 'node:net';
/** A minimal reverse proxy: plain HTTP forwarding plus a TCP pass-through of the WebSocket upgrade, optionally stripping headers. */
async function reverseProxy(upstreamUrl,{strip=[]}={}){
 const target=new URL(upstreamUrl);
 const forwardHeaders=headers=>{const copy={...headers};for(const name of strip)delete copy[name];return copy;};
 const proxy=createProxyServer((req,res)=>{
  const out=httpRequest({host:target.hostname,port:target.port,method:req.method,path:req.url,headers:forwardHeaders(req.headers)},up=>{res.writeHead(up.statusCode,up.headers);up.pipe(res);});
  out.on('error',()=>{res.statusCode=502;res.end();});req.pipe(out);
 });
 proxy.on('upgrade',(req,socket,head)=>{
  const upstream=tcpConnect(Number(target.port),target.hostname,()=>{
   const lines=[`${req.method} ${req.url} HTTP/1.1`,...Object.entries(forwardHeaders(req.headers)).map(([k,v])=>`${k}: ${Array.isArray(v)?v.join(', '):v}`)];
   upstream.write(lines.join('\r\n')+'\r\n\r\n');if(head.length)upstream.write(head);
   socket.pipe(upstream);upstream.pipe(socket);
  });
  upstream.on('error',()=>socket.destroy());socket.on('error',()=>upstream.destroy());
 });
 await new Promise(resolve=>proxy.listen(0,'127.0.0.1',resolve));
 return {url:`http://127.0.0.1:${proxy.address().port}`,close:()=>new Promise(resolve=>{proxy.closeAllConnections();proxy.close(()=>resolve());})};
}
test('a reverse proxy forwarding HTTP and the WebSocket upgrade with headers serves push, pull and live; a stripped Authorization header is refused',async()=>{
 const server=await backend.listen({port:0});const proxy=await reverseProxy(server.url);
 try{
  const pushed=await fetch(`${proxy.url}/sync/mutations`,{method:'POST',headers:{authorization:'Bearer alice'},body:push('proxied',1,[mutation(1,'through proxy','proxy-a')])});
  assert.equal(pushed.status,200);assert.deepEqual((await pushed.json()).rejections,[]);
  const seen=[];for(let fromCursor=0;;){const page=await fetch(`${proxy.url}/sync/pull`,{method:'POST',headers:{authorization:'Bearer alice'},body:pullBody({shared:fromCursor})});
   assert.equal(page.status,200);const body=await page.json();seen.push(...body.changes);if(body.cursors.shared.to>=body.cursors.shared.head)break;fromCursor=body.cursors.shared.to;}
  assert.ok(seen.some(c=>c.identity.id==='proxy-a'&&c.state.title==='through proxy'),'the pushed record is pulled through the proxy');
  const socket=new serverSdk.WebSocket(`${proxy.url.replace('http','ws')}/sync/live`,{headers:{authorization:'Bearer alice'}});
  await new Promise((resolve,reject)=>{socket.on('open',resolve);socket.on('error',reject);});
  const frames=[];socket.on('message',data=>frames.push(JSON.parse(String(data))));
  socket.send(JSON.stringify({type:'subscribe',channels:['shared'],models:{Task:1}}));while(frames.length<1)await delay(5);
  assert.equal(frames[0].type,'subscribed');
  await backend.push('alice',push('proxied',2,[mutation(2,'live through proxy','proxy-a')]));
  while(frames.length<2)await delay(5);assert.equal(frames.at(-1).changes.at(-1).state.title,'live through proxy');
  socket.close();await new Promise(resolve=>socket.on('close',resolve));
  const stripping=await reverseProxy(server.url,{strip:['authorization']});
  try{
   const denied=await fetch(`${stripping.url}/sync/pull`,{method:'POST',headers:{authorization:'Bearer alice'},body:'{}'});assert.equal(denied.status,401);
   const refused=new serverSdk.WebSocket(`${stripping.url.replace('http','ws')}/sync/live`,{headers:{authorization:'Bearer alice'}});
   const failure=await new Promise(resolve=>{refused.on('error',resolve);refused.on('open',()=>resolve(null));});
   assert.ok(failure,'the upgrade is refused without the header');assert.match(String(failure.message),/401/);
  }finally{await stripping.close();}
 }finally{await proxy.close();await server.close();}
});
test('HTTP maps engine codes to statuses: 403, 409 gap/overlap/version fields, 404, 405, 413, 400',async()=>{
 const server=await backend.listen({port:0});const url=server.url;
 const post=(path,body,headers={authorization:'Bearer alice'})=>fetch(`${url}${path}`,{method:'POST',headers,body});
 try{
  await backend.push('alice',push('map',1,[mutation(1,'one','map-a')]));
  const gap=await post('/sync/mutations',push('map',5,[mutation(1,'x','map-a')]));assert.equal(gap.status,409);assert.deepEqual(await gap.json(),{code:'gap'});
  const overlap=await post('/sync/mutations',push('map',1,[mutation(9,'other','map-a')]));assert.equal(overlap.status,200,'a retry of the accepted sequence returns its receipt');
  await backend.push('alice',push('map',2,[mutation(2,'second','map-a')]));
  const behind=await post('/sync/mutations',push('map',1,[mutation(1,'one','map-a')]));assert.equal(behind.status,409);assert.deepEqual(await behind.json(),{code:'overlap'});
  const bobBackend=createBackend({config,database:database(),authenticate:async req=>req.headers.authorization==='Bearer bob'?'bob':null,handlers:{async edit(){}},loaders:{async task({ids}){return ids.map(()=>null)}}});
  const bobServer=await bobBackend.listen({port:0});
  try{const owner=await fetch(`${bobServer.url}/sync/mutations`,{method:'POST',headers:{authorization:'Bearer bob'},body:push('map',3,[mutation(3,'x','map-a')])});assert.equal(owner.status,403);assert.deepEqual(await owner.json(),{code:'client.owner_mismatch'});}
  finally{await bobServer.close();}
  const version=await post('/sync/mutations',push('map',3,[{...mutation(3,'x','map-a'),version:7}]));assert.equal(version.status,200,'an unsupported mutation version rejects only that mutation; the batch still settles');
  assert.deepEqual((await version.json()).rejections,[{ordinal:3,code:'mutation_version_unsupported'}]);
  const ahead=await post('/sync/pull',pullBody({shared:1e9}));assert.equal(ahead.status,400);assert.deepEqual(await ahead.json(),{code:'request.invalid'});
  const undeclared=await post('/sync/pull',JSON.stringify({cursors:{shared:0}}));assert.equal(undeclared.status,400);assert.deepEqual(await undeclared.json(),{code:'request.invalid'});
  const unretained=await post('/sync/pull',pullBody({shared:0},{Task:9}));assert.equal(unretained.status,409);
  assert.deepEqual(await unretained.json(),{code:'model_version_unsupported',model:'Task',version:9});
  const array=await post('/sync/pull','[]');assert.equal(array.status,400);assert.deepEqual(await array.json(),{code:'request.invalid'});
  const missing=await post('/sync/nowhere','{}');assert.equal(missing.status,404);assert.deepEqual(await missing.json(),{code:'not_found'});
  const get=await fetch(`${url}/sync/pull`,{headers:{authorization:'Bearer alice'}});assert.equal(get.status,405);assert.equal(get.headers.get('allow'),'POST');assert.deepEqual(await get.json(),{code:'method_not_allowed'});
  const large=await post('/sync/pull',JSON.stringify({cursors:{shared:0},padding:'x'.repeat(1_048_577)}));assert.equal(large.status,413);assert.deepEqual(await large.json(),{code:'request_too_large'});
 }finally{await server.close();}
});
test('HTTP classifies native failures by code, not message wording; unknown codes fall back to 500',async()=>{
 const errors=[];
 const reason=(code,message,details)=>Object.assign(new Error(JSON.stringify({code,message,...(details?{details}:{})})),{});
 const fake={validateConfig(){},async processPush(){throw reason('gap','the batch sequence 5 skips ahead of 1 (reworded)');},async processPull(){throw reason('mutation_version_unsupported','anything',{ordinal:2,name:'edit',version:9});},async publish(){return '[]';},async negotiateLive(){throw reason('request.invalid','no');},async pullLive(){throw reason('loader.unregistered','unregistered loader');},liveEvent(){return '[]';},liveClose(){}};
 const memory={transaction:body=>body({}),persistence:()=>({call:async()=>null})};
 const fakeBackend=createBackend({config,database:memory,native:fake,authenticate,onError:e=>errors.push(e),handlers:{async edit(){}},loaders:{async task({ids}){return ids.map(()=>null)}}});
 await assert.rejects(()=>fakeBackend.push('alice','{}'),error=>error instanceof EngineError&&error.code==='gap'&&error.message.includes('reworded'));
 const server=await fakeBackend.listen({port:0});
 try{
  const gap=await fetch(`${server.url}/sync/mutations`,{method:'POST',headers:{authorization:'Bearer alice'},body:'{}'});assert.equal(gap.status,409);assert.deepEqual(await gap.json(),{code:'gap'});
  const version=await fetch(`${server.url}/sync/pull`,{method:'POST',headers:{authorization:'Bearer alice'},body:'{}'});assert.equal(version.status,409);assert.deepEqual(await version.json(),{code:'mutation_version_unsupported',ordinal:2,name:'edit',version:9});
  assert.equal(errors.length,0,'classified refusals are not server errors');
  const socket=new serverSdk.WebSocket(`${server.url.replace('http','ws')}/sync/live`,{headers:{authorization:'Bearer alice'}});
  const closed=await new Promise(resolve=>{socket.on('open',()=>socket.send(JSON.stringify({type:'subscribe',channels:['shared'],models:{Task:1}})));socket.on('close',(code,reasonText)=>resolve({code,reason:String(reasonText)}));socket.on('error',()=>{});});
  assert.equal(closed.code,1002);assert.equal(errors.length,0);
  fake.negotiateLive=async()=>JSON.stringify({handle:1,actions:[{type:'listen',scope:'shared'},{type:'send',frame:JSON.stringify({type:'subscribed',cursors:{shared:0}})},{type:'pull',cursors:{shared:0},models:{Task:1}}]});
  const drained=new serverSdk.WebSocket(`${server.url.replace('http','ws')}/sync/live`,{headers:{authorization:'Bearer alice'}});
  const drainClose=await new Promise(resolve=>{drained.on('open',()=>drained.send(JSON.stringify({type:'subscribe',channels:['shared'],models:{Task:1}})));drained.on('close',code=>resolve(code));drained.on('error',()=>{});});
  assert.equal(drainClose,1011);assert.equal(errors.length,1);assert.ok(errors[0] instanceof EngineError);assert.equal(errors[0].code,'loader.unregistered');
  fake.processPush=async()=>{throw reason('storage.invalid','receipt missing');};
  const unknown=await fetch(`${server.url}/sync/mutations`,{method:'POST',headers:{authorization:'Bearer alice'},body:'{}'});assert.equal(unknown.status,500);assert.deepEqual(await unknown.json(),{code:'server'});
  assert.equal(errors.length,2);assert.equal(errors[1].code,'storage.invalid');assert.equal(errors[1].message,'receipt missing');
  fake.processPush=async()=>{throw new Error('not json at all');};
  const plain=await fetch(`${server.url}/sync/mutations`,{method:'POST',headers:{authorization:'Bearer alice'},body:'{}'});assert.equal(plain.status,500);
  assert.equal(errors.length,3);assert.ok(!(errors[2] instanceof EngineError));assert.equal(errors[2].message,'not json at all');
 }finally{await server.close();}
});
test('the Prisma driver retries only serialization failures, a bounded number of times, and reports the last one',async()=>{
 const attempts=[];const bodies=[];
 const failing=(codes)=>({async $transaction(body,options){attempts.push(options);const code=codes.shift();await body({attempt:attempts.length});bodies.push(attempts.length);if(code)throw Object.assign(new Error(`fail ${code.code}`),code);return 'committed';}});
 const conflict={code:'P2034'};const rawConflict={code:'P2010',meta:{code:'40001'}};const deadlock={code:'P2010',meta:{code:'40P01'}};const unique={code:'P2002'};
 assert.equal(await prismaDriver(failing([conflict,rawConflict,deadlock])).transaction(async()=>'body'),'committed','the fourth attempt succeeds within the default of three retries');
 assert.equal(attempts.length,4);assert.deepEqual(bodies,[1,2,3,4],'the body runs once per attempt');assert.deepEqual(attempts[0],{isolationLevel:'RepeatableRead',timeout:20000});
 attempts.length=0;bodies.length=0;
 await assert.rejects(()=>prismaDriver(failing([conflict,conflict,conflict,conflict])).transaction(async()=>{}),error=>error.code==='P2034'&&error.message==='fail P2034');
 assert.equal(attempts.length,4,'three retries after the first attempt, then the failure is reported');
 attempts.length=0;
 await assert.rejects(()=>prismaDriver(failing([conflict,conflict]),{retries:1}).transaction(async()=>{}),error=>error.code==='P2034');
 assert.equal(attempts.length,2,'retries is the number of additional attempts');
 attempts.length=0;
 await assert.rejects(()=>prismaDriver(failing([unique])).transaction(async()=>{}),error=>error.code==='P2002');
 assert.equal(attempts.length,1,'a non-serialization failure is not retried');
 attempts.length=0;
 const bundled=prisma(failing([conflict]),{retries:1,timeout:5});await bundled.transaction(async()=>{});
 assert.deepEqual(attempts.map(o=>o.timeout),[5,5],'prisma() passes retries and timeout to the runner');
});
test('a RepeatableRead conflict on the real database retries the whole body once and commits it exactly once',async()=>{
 await db.$executeRawUnsafe("INSERT INTO axton_channel(channel,head) VALUES('serial',0) ON CONFLICT(channel) DO UPDATE SET head=0");
 let bodies=0;let entered,release;const inside=new Promise(resolve=>{entered=resolve;});const gate=new Promise(resolve=>{release=resolve;});
 const first=run(async tx=>{bodies++;const [{head}]=await tx.$queryRawUnsafe("SELECT head FROM axton_channel WHERE channel='serial'");if(bodies===1){entered();await gate;}
  await tx.$executeRawUnsafe("UPDATE axton_channel SET head=head+1 WHERE channel='serial'");return Number(head);});
 await inside;
 await db.$executeRawUnsafe("UPDATE axton_channel SET head=head+10 WHERE channel='serial'");
 release();
 assert.equal(await first,10,'the retried body read the snapshot taken after the concurrent commit');
 assert.equal(bodies,2,'the first attempt failed with a serialization error after the concurrent update and the body ran again');
 assert.equal(Number((await db.$queryRawUnsafe("SELECT head FROM axton_channel WHERE channel='serial'"))[0].head),11,'the rolled-back attempt left nothing behind and the retry committed once');
 const exhausted=prismaDriver(db,{retries:0}).transaction;bodies=0;let entered2,release2;const inside2=new Promise(resolve=>{entered2=resolve;});const gate2=new Promise(resolve=>{release2=resolve;});
 const second=exhausted(async tx=>{bodies++;await tx.$queryRawUnsafe("SELECT head FROM axton_channel WHERE channel='serial'");entered2();await gate2;await tx.$executeRawUnsafe("UPDATE axton_channel SET head=head+1 WHERE channel='serial'");});
 await inside2;await db.$executeRawUnsafe("UPDATE axton_channel SET head=head+10 WHERE channel='serial'");release2();
 await assert.rejects(second,error=>error.code==='P2034'||(error.code==='P2010'&&error.meta?.code==='40001'));
 assert.equal(bodies,1);assert.equal(Number((await db.$queryRawUnsafe("SELECT head FROM axton_channel WHERE channel='serial'"))[0].head),21,'with no retries the conflict is reported and the transaction leaves no trace');
});
test('an upgrade whose authentication completes after close begins is refused with 503; missing and invalid credentials are refused with 401',async()=>{
 let release;const gate=new Promise(resolve=>{release=resolve;});const seen=[];
 const gated=createBackend({config,database:database(),authenticate:async req=>{seen.push(req.headers.authorization);if(req.headers.authorization==='Bearer slow'){await gate;return 'alice';}return req.headers.authorization==='Bearer alice'?'alice':null;},handlers:{async edit(){}},loaders:{async task({ids}){return ids.map(()=>null)}}});
 const server=await gated.listen({port:0});const ws=server.url.replace('http','ws');
 const refusal=socket=>new Promise(resolve=>{socket.on('error',error=>resolve(String(error.message)));socket.on('open',()=>resolve('open'));});
 try{
  assert.match(await refusal(new serverSdk.WebSocket(`${ws}/sync/live`)),/401/,'no credentials');
  assert.match(await refusal(new serverSdk.WebSocket(`${ws}/sync/live`,{headers:{authorization:'Bearer mallory'}})),/401/,'unknown credentials');
  const slow=new serverSdk.WebSocket(`${ws}/sync/live`,{headers:{authorization:'Bearer slow'}});const outcome=refusal(slow);
  while(!seen.includes('Bearer slow'))await delay(5);
  const closing=server.close();release();
  assert.match(await outcome,/503/,'authenticated after close began: refused, not served');
  await closing;
 }finally{await server.close();}
});

test('a version dispatches only to its own handler and a function registers v1',async()=>{
 const seen=[];
 const record=tag=>async({input,channel})=>{seen.push([tag,input.task.patch.title]);channel('registration').task.add(input.task.identity);};
 const make=(handlers,mutations=config.mutations)=>createBackend({config:{...config,schema:structuredClone(schema),mutations},database:database(),authenticate,handlers,loaders:{async task({ids}){return ids.map(()=>null)}}});
 const shorthand=JSON.parse(await make({edit:record('function')}).push('alice',push('register-function',1,[mutation(1,'same','reg-a')])));
 const explicit=JSON.parse(await make({edit:{v1:record('v1 key')}}).push('alice',push('register-v1-key',1,[mutation(1,'same','reg-a')])));
 assert.deepEqual(seen,[['function','same'],['v1 key','same']],'both registrations reach the same v1 handler');
 assert.deepEqual(shorthand.rejections,[]);assert.deepEqual(explicit.rejections,[]);
 assert.deepEqual(shorthand.records,[authority('reg-a',1,null)]);assert.deepEqual(explicit.records,[authority('reg-a',2,null)],'only the stamp differs');
 assert.deepEqual((await invalidations('reg-a')).map(([channel,,stamp])=>[channel,stamp]),[['registration',2]]);
 const two=make({edit:{v1:record('v1'),v2:record('v2')}},[config.mutations[0],{...config.mutations[0],version:2}]);
 await two.push('alice',push('register-dispatch',1,[mutation(1,'from v1','reg-b')]));
 await two.push('alice',push('register-dispatch',2,[{...mutation(1,'from v2','reg-c'),version:2}]));
 assert.deepEqual(seen.slice(2),[['v1','from v1'],['v2','from v2']],'no fallback between versions');
});
test('concurrent same-client delivery with different bodies commits at most one under the PostgreSQL lock',async()=>{
 const before=called;const shared=async()=>Number((await db.$queryRawUnsafe("SELECT head FROM axton_channel WHERE channel='shared'"))[0].head);
 // A concurrent delivery of the same sequence with a different body commits at most one of the two: the loser waits on the row lock, then replays the winner's receipt.
 const head=await shared();const changed=push('race-body',1,[mutation(1,'winner','race-body')]);const other=push('race-body',1,[mutation(1,'loser','race-body')]);
 const pair=await Promise.all([backend.push('alice',changed),backend.push('alice',other)]);assert.equal(pair[0],pair[1]);assert.equal(called,before+1);
 assert.equal(await shared(),head+1,'exactly one publication');const [row]=await db.$queryRawUnsafe("SELECT title FROM business_task WHERE id='race-body'");assert.ok(['winner','loser'].includes(row.title));
 const [client]=await db.$queryRawUnsafe("SELECT sequence, receipt FROM axton_client WHERE client_id='race-body'");assert.equal(Number(client.sequence),1);assert.equal(client.receipt,pair[0]);
});
test('fresh framework tables omit request_hash; a table that still carries the column keeps replaying receipts',async()=>{
 const columns=async()=>(await db.$queryRawUnsafe("SELECT column_name FROM information_schema.columns WHERE table_name='axton_client'")).map(row=>row.column_name).sort();
 assert.deepEqual(await columns(),['client_id','owner_id','receipt','sequence']);
 const migration=(await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8')).split(';').map(x=>x.trim()).filter(Boolean);
 const before=called;const request=push('legacy-column',1,[mutation(1,'legacy','legacy-column')]);let receipt;
 await db.$executeRawUnsafe('ALTER TABLE axton_client ADD COLUMN request_hash text');
 try{
  for(const sql of migration)await db.$executeRawUnsafe(sql);
  assert.deepEqual(await columns(),['client_id','owner_id','receipt','request_hash','sequence'],'re-applying migration.sql leaves an existing table untouched');
  receipt=await backend.push('alice',request);assert.equal(await backend.push('alice',push('legacy-column',1,[mutation(1,'changed','legacy-column')])),receipt);assert.equal(called,before+1);
  const [row]=await db.$queryRawUnsafe("SELECT request_hash, sequence FROM axton_client WHERE client_id='legacy-column'");assert.equal(row.request_hash,null);assert.equal(Number(row.sequence),1);
 }finally{await db.$executeRawUnsafe('ALTER TABLE axton_client DROP COLUMN request_hash');}
 assert.deepEqual(await columns(),['client_id','owner_id','receipt','sequence']);
 assert.equal(await backend.push('alice',request),receipt,'the stored receipt survives dropping the unused column');assert.equal(called,before+1);
 await assert.rejects(()=>backend.push('alice',push('legacy-column',3,[mutation(2,'gap','legacy-column')])),/gap/);
 // Only the most recently committed sequence replays; once sequence 2 commits, sequence 1 is an overlap even with its original body.
 const next=await backend.push('alice',push('legacy-column',2,[mutation(2,'next','legacy-column')]));assert.notEqual(next,receipt);assert.equal(called,before+2);
 await assert.rejects(()=>backend.push('alice',request),/overlap/);assert.equal(await backend.push('alice',push('legacy-column',2,[mutation(2,'changed','legacy-column')])),next);assert.equal(called,before+2);
});

test('the live stream serves the declared model version and refuses an unretained one at the handshake',async()=>{
 const c=structuredClone(config);c.mutations=[];c.schema.models[0].version=2;
 const v1={name:'Task',version:1,identity:['id'],fields:schema.models[0].fields,enums:[]};
 c.schema.models[0].fields=[...schema.models[0].fields,{name:'note',type:{kind:'scalar',name:'string'},nullable:true}];
 c.models=[v1,{name:'Task',version:2,identity:['id'],fields:c.schema.models[0].fields,enums:[]}];
 const versioned=createBackend({config:c,database:database(),authenticate,handlers:{},loaders:{task:{
  async v1({ids}){return ids.map(id=>({id:id.id,title:'old'}))},
  async v2({ids}){return ids.map(id=>({id:id.id,title:'new',note:'n'}))},
 }}});
 const server=await versioned.listen({port:0});const port=Number(new URL(server.url).port);
 try{
  const shapes=[];
  for(const [version,expected] of [[1,{title:'old'}],[2,{title:'new',note:'n'}]]){
   const socket=await openSocket(port);const frames=[];socket.addEventListener('message',event=>frames.push(JSON.parse(String(event.data))));
   socket.send(JSON.stringify({type:'subscribe',channels:['shared'],models:{Task:version}}));
   while(frames.length<1)await delay(5);
   assert.equal(frames[0].type,'subscribed');
   // Streaming starts at the head: publish after the handshake; the commit
   // wakes subscribers, so a page follows.
   await external(versioned,'shared',[{model:'Task',identity:{id:`live-v${version}`}}]);
   while(frames.length<2)await delay(5);
   shapes.push(frames[1].changes.find(ch=>ch.state!==null).state);
   socket.close();await new Promise(resolve=>socket.addEventListener('close',resolve,{once:true}));
   assert.deepEqual(shapes.at(-1),expected,`a v${version} subscriber reads v${version} records`);
  }
  const refused=await openSocket(port);
  const closed=new Promise(resolve=>refused.addEventListener('close',event=>resolve({code:event.code,reason:String(event.reason)}),{once:true}));
  refused.send(JSON.stringify({type:'subscribe',channels:['shared'],models:{Task:3}}));
  assert.deepEqual(await closed,{code:1002,reason:'model_version_unsupported'});
  const undeclared=await openSocket(port);
  const closedToo=new Promise(resolve=>undeclared.addEventListener('close',event=>resolve(event.code),{once:true}));
  undeclared.send(JSON.stringify({type:'subscribe',channels:['shared']}));
  assert.equal(await closedToo,1002);
 }finally{await server.close();}
});

test('backend.transaction publishes in the application transaction and wakes after commit',async()=>{
 let woke=0;const unsubscribe=backend.onCommitted('shared',()=>{woke++;});
 const from=await head('shared');
 const result=await backend.transaction(async({tx,channel,touch})=>{await write(tx,'tx-1','via transaction');touch.task({id:'tx-1'});channel('shared').task.add({id:'tx-1'});return 'done';});
 assert.equal(result,'done');await delay(0);assert.equal(woke,1,'one wake after commit');
 const page=await pull('shared',from);assert.ok(page.changes.some(c=>c.identity.id==='tx-1'&&c.state.title==='via transaction'),JSON.stringify(page));
 unsubscribe();
});
test('backend.transaction rolls back a failing body and wakes nobody',async()=>{
 let woke=0;const unsubscribe=backend.onCommitted('shared',()=>{woke++;});
 const before=await head('shared');
 await assert.rejects(()=>backend.transaction(async({tx,channel,touch})=>{await write(tx,'tx-rollback','never');touch.task({id:'tx-rollback'});channel('shared').task.add({id:'tx-rollback'});throw new Error('cancel');}),/cancel/);
 await delay(0);assert.equal(woke,0);assert.equal(await head('shared'),before);
 assert.equal((await db.$queryRawUnsafe("SELECT count(*) AS count FROM business_task WHERE id='tx-rollback'"))[0].count,0n);
 unsubscribe();
});
test('backend.transaction wakes a connected live subscriber without reconnect',async()=>{
 const server=await backend.listen({port:0});const port=Number(new URL(server.url).port);
 const socket=await openSocket(port);const frames=[];socket.addEventListener('message',event=>frames.push(JSON.parse(String(event.data))));
 socket.send(JSON.stringify({type:'subscribe',channels:['shared'],models:{Task:1}}));while(frames.length<1)await delay(5);
 await backend.transaction(async({tx,channel,touch})=>{await write(tx,'tx-live','live via transaction');touch.task({id:'tx-live'});channel('shared').task.add({id:'tx-live'});});
 while(frames.length<2)await delay(5);
 assert.deepEqual(frames[1].changes.at(-1).identity,{id:'tx-live'});assert.deepEqual(frames[1].changes.at(-1).state,{title:'live via transaction'});
 socket.close();await new Promise(resolve=>socket.addEventListener('close',resolve,{once:true}));await server.close();
});
test('notify and bindTransaction are gone; backend.transaction is the only external write path',async()=>{
 assert.equal(backend.notify,undefined);assert.equal(backend.bindTransaction,undefined);
 const before=await head('shared');
 await backend.transaction(async({tx,channel,touch})=>{await write(tx,'tx-only-path','only path');touch.task({id:'tx-only-path'});channel('shared').task.add({id:'tx-only-path'});});
 assert.equal(await head('shared'),before+1);
});
test('re-adding an unchanged member keeps its stamp and publishes nothing; a later touch reaches its Channel without another add',async()=>{
 await external(backend,'shared',[{model:'Task',identity:{id:'stamp-probe'}}]);
 const stampOf=()=>recordStamp('stamp-probe');
 const first=await stampOf();
 const headBefore=await head('shared');
 await backend.transaction(async({channel})=>{channel('shared').task.add({id:'stamp-probe'});});
 assert.equal(await stampOf(),first,'membership-only records keep their stamp');assert.equal(await head('shared'),headBefore,'and an existing member is not published again');
 await backend.transaction(async({touch})=>{touch.task({id:'stamp-probe'});});
 assert.equal(await stampOf(),first+1,'a touch advances the stamp without a membership declaration');assert.equal(await head('shared'),headBefore+1,'and reach the Channel the record is a member of');
});
test('a pull covers every channel in one request and delivers a record shared by two channels once',async()=>{
 await db.$executeRawUnsafe("INSERT INTO business_task(id,title) VALUES('multi-a','A'),('multi-b','B') ON CONFLICT(id) DO NOTHING");
 await backend.transaction(async({channel,touch})=>{touch.task({id:'multi-a'});touch.task({id:'multi-b'});channel('multi-x').task.add({id:'multi-a'});channel('multi-x').task.add({id:'multi-b'});channel('multi-y').task.add({id:'multi-a'});});
 const page=JSON.parse(await backend.pull('alice',pullBody({'multi-x':0,'multi-y':0})));
 assert.deepEqual(Object.keys(page.cursors),['multi-x','multi-y']);
 assert.deepEqual(page.cursors['multi-x'],{from:0,to:2,head:2});assert.deepEqual(page.cursors['multi-y'],{from:0,to:1,head:1});
 assert.deepEqual(page.changes.map(c=>c.identity.id),['multi-a','multi-b'],'multi-a is delivered once although both channels changed it');
 assert.equal(page.changes[0].stamp,await recordStamp('multi-a'));
 const nothing=JSON.parse(await backend.pull('alice',pullBody({'multi-x':2,'multi-y':1})));
 assert.deepEqual(nothing,{cursors:{'multi-x':{from:2,to:2,head:2},'multi-y':{from:1,to:1,head:1}},changes:[]});
});
test('a loader that throws for one id fails only that record and reaches onError',async()=>{
 const errors=[];const calls=[];
 const flaky=createBackend({config:{...config,mutations:[]},database:prisma(db),authenticate,onError:e=>errors.push(e),handlers:{},loaders:{task:async({ids,tx})=>{calls.push(ids.length);if(ids.some(id=>id.id==='multi-b'))throw new Error('b is broken');return readTasks({ids,tx});}}});
 const page=JSON.parse(await flaky.pull('alice',pullBody({'multi-x':0})));
 const a=page.changes.find(c=>c.identity.id==='multi-a'),b=page.changes.find(c=>c.identity.id==='multi-b');
 assert.deepEqual(a.state,{title:'A'});assert.equal(a.error,undefined);
 assert.equal(b.error,'loader.failed');assert.equal(b.state,null);assert.equal(b.stamp,await recordStamp('multi-b'));
 assert.deepEqual(calls,[2,1,1],'the batched call, then one call per identity');
 assert.equal(errors.length,2,'the batched failure and the single failure both reach onError');assert.equal(errors[0].message,'b is broken');
});
test('a loader refusal for one id is an error change carrying the refusal code',async()=>{
 const refusing=createBackend({config:{...config,mutations:[]},database:prisma(db),authenticate,handlers:{},loaders:{task:async({ids,tx})=>{if(ids.some(id=>id.id==='multi-a'))throw new MutationRejected('task.forbidden');return readTasks({ids,tx});}}});
 const page=JSON.parse(await refusing.pull('alice',pullBody({'multi-x':0})));
 assert.equal(page.changes.find(c=>c.identity.id==='multi-a').error,'task.forbidden');
 assert.deepEqual(page.changes.find(c=>c.identity.id==='multi-b').state,{title:'B'});
});
test('onError defaults to console.error so nothing is dropped silently',async()=>{
 const seen=[];const original=console.error;console.error=(...args)=>seen.push(args);
 try{
  const quiet=createBackend({config:{...config,mutations:[]},database:prisma(db),authenticate,handlers:{},loaders:{task:async({ids})=>{if(ids.length===1)throw new Error('single');throw new Error('batched');}}});
  const page=JSON.parse(await quiet.pull('alice',pullBody({'multi-x':0})));
  assert.ok(page.changes.every(c=>c.error==='loader.failed'));
  assert.ok(seen.some(args=>args[0]?.message==='batched'),'the batched failure was logged');
 }finally{console.error=original;}
});
// A bounded bootstrap request on the same `/sync/pull` route: one channel, the
// committed historical progress B and the fixed subscription origin S (#151).
const bootstrapBody=(channel,after,until,models={Task:1})=>JSON.stringify({mode:'bootstrap',channel,models,after,until});
const bootstrap=(channel,after,until)=>backend.pull('alice',bootstrapBody(channel,after,until)).then(JSON.parse);
/** Publish `count` fresh records to one channel, one cursor each. */
const fill=(name,prefix,count)=>backend.transaction(async({tx,channel,touch})=>{
 const target=channel(name);
 for(let i=0;i<count;i++){const id={id:`${prefix}-${i}`};await write(tx,id.id,`title ${i}`);touch.task(id);target.task.add(id);}
});
test('a bounded bootstrap request pages the historical interval and stops at the fixed origin',async()=>{
 await fill('boot','boot',60);
 const origin=await head('boot');assert.equal(origin,60);
 // Publications above S keep arriving; the walk must not chase the head.
 await fill('boot','boot-late',5);
 const later=await head('boot');assert.equal(later,origin+5);
 const first=await bootstrap('boot',0,origin);
 assert.equal(first.mode,'bootstrap');assert.equal(first.channel,'boot');
 assert.equal(first.from,0);assert.equal(first.to,50);assert.equal(first.until,origin);assert.equal(first.head,later);
 assert.notEqual(first.to,first.until,'a full scan below the origin continues');
 assert.equal(first.records.length,50);
 assert.deepEqual(first.records[0].state,{title:'title 0'});
 assert.equal(first.records[0].stamp,await recordStamp('boot-0'));
 const second=await bootstrap('boot',first.to,origin);
 assert.equal(second.from,50);assert.equal(second.to,origin);assert.equal(second.until,origin);
 assert.equal(second.to,second.until,'the interval is complete');
 assert.equal(second.records.length,10);
 assert.ok(second.records.every(r=>r.error===undefined&&r.state!==null));
 assert.deepEqual([...new Set([...first.records,...second.records].map(r=>r.identity.id))].length,60,'every historical record once');
 assert.ok(second.records.every(r=>!r.identity.id.startsWith('boot-late')),'nothing above the origin is loaded');
 // An exhausted interval is an empty terminal page carrying the current head.
 const done=await bootstrap('boot',origin,origin);
 assert.deepEqual(done,{mode:'bootstrap',channel:'boot',from:origin,to:origin,until:origin,head:await head('boot'),records:[]});
 // An empty Scope completes at zero, and an origin above the head is refused.
 assert.deepEqual(await bootstrap('boot-empty',0,0),{mode:'bootstrap',channel:'boot-empty',from:0,to:0,until:0,head:0,records:[]});
 await assert.rejects(()=>bootstrap('boot',0,later+1),/ahead of head/);
});
test('a record republished above the origin leaves the historical interval between pages',async()=>{
 await fill('moved','moved',60);
 const origin=await head('moved');
 const first=await bootstrap('moved',0,origin);
 assert.equal(first.to,50);assert.equal(first.records.length,50);
 // `moved-55` sits in (50, origin]; republishing it moves its only row above
 // the origin, where the subscription's own delivery covers it.
 await external(backend,'moved',[{model:'Task',identity:{id:'moved-55'}}]);
 assert.ok((await invalidations('moved-55')).some(([channel,cursor])=>channel==='moved'&&cursor>origin));
 const second=await bootstrap('moved',first.to,origin);
 assert.equal(second.to,origin,'the interval still completes at the fixed origin');
 assert.equal(second.head,await head('moved'));
 assert.equal(second.records.length,9);
 assert.ok(!second.records.some(r=>r.identity.id==='moved-55'),'the republished record is no longer historical');
});
test('a bootstrap page reads content and stamps at its own repeatable-read snapshot',async()=>{
 await backend.transaction(async({tx,channel,touch})=>{await write(tx,'coherent','first');touch.task({id:'coherent'});channel('coherent').task.add({id:'coherent'});});
 const origin=await head('coherent');const before=await recordStamp('coherent');
 // A concurrent transaction rewrites and republishes the record after the page
 // transaction has read its head; the page must still answer its own snapshot.
 let changed=false;
 const reader=createBackend({config:{...config,mutations:[]},database:{transaction:run,persistence:tx=>{const storage=store(tx);return {call:async r=>{
  const result=await storage.call(r);
  if(r.op==='head'&&!changed){changed=true;await backend.transaction(async({tx:other,touch})=>{await write(other,'coherent','second');touch.task({id:'coherent'});});}
  return result;
 }}}},authenticate,handlers:{},loaders:{task:async({ids,tx})=>readTasks({ids,tx})}});
 const page=JSON.parse(await reader.pull('alice',bootstrapBody('coherent',0,origin)));
 assert.ok(changed,'the concurrent publication committed during the page');
 assert.equal(page.head,origin,'the head the page reports is its snapshot head');
 assert.equal(page.to,origin);
 assert.deepEqual(page.records,[{model:'Task',identity:{id:'coherent'},stamp:before,state:{title:'first'}}],'cursor, stamp and content come from one snapshot');
 assert.equal(await recordStamp('coherent'),before+1,'the concurrent write did commit');
 assert.ok(await head('coherent')>origin);
});
test('the pull route dispatches by mode over HTTP and refuses any other mode',async()=>{
 await fill('route','route',2);
 const origin=await head('route');
 const server=await backend.listen({port:0});
 try{
  const post=body=>fetch(`${server.url}/sync/pull`,{method:'POST',headers:{authorization:'Bearer alice'},body});
  const page=await post(bootstrapBody('route',0,origin));assert.equal(page.status,200);
  const decoded=await page.json();
  assert.equal(decoded.mode,'bootstrap');assert.equal(decoded.to,origin);assert.equal(decoded.records.length,2);
  const ordinary=await post(pullBody({route:0}));assert.equal(ordinary.status,200);
  assert.equal((await ordinary.json()).mode,undefined,'an absent mode still answers an ordinary page');
  for(const mode of ['snapshot',null,1]){
   const refused=await post(JSON.stringify({mode,channel:'route',models:{Task:1},after:0,until:origin}));
   assert.equal(refused.status,400,`mode ${JSON.stringify(mode)}`);
   assert.deepEqual(await refused.json(),{code:'request.invalid'});
  }
  const malformed=await post(bootstrapBody('  ',0,origin));assert.equal(malformed.status,400);
 }finally{await server.close();}
 // The same dispatch through the direct backend call, with no HTTP in between.
 await assert.rejects(()=>backend.pull('alice',JSON.stringify({mode:'snapshot',channel:'route',models:{Task:1},after:0,until:origin})),/mode/);
 await assert.rejects(()=>bootstrap('route',2,1),/request.invalid|origin/);
});
