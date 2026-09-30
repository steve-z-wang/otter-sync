// Channel membership against real PostgreSQL (#140): delivery filtered by
// membership in both pull modes, the settlement's transaction ordering under
// real concurrency, and rollback and failure isolation. Concurrency is
// coordinated with latches, never sleeps: a transaction fixes its Repeatable
// Read snapshot by reading the record's stamp and memberships before any
// competing operation is let through, and every assertion about ordering is
// made on the committed outcome together with the attempts the driver retried.
import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {Pool} from 'pg';
import {createBackend,CallRejected} from '../../../packages/server/index.mts';
import {pg} from '../../../packages/postgres/index.mts';
const require=createRequire(import.meta.url);
const native=require('../../../bindings/node/axton-node.node');
const url=process.env.DATABASE_URL;
const check=new Pool({connectionString:url});
const q=async(sql,params=[])=>(await check.query(sql,params)).rows;
const pool=new Pool({connectionString:url});
const database=pg(pool);
const {driver}=database;
const string=name=>({name,type:{kind:'scalar',name:'string'},nullable:false});
const fields=[string('id'),string('title')];
// Mark(todo Todo.update): writes the title, then runs the plan named by it.
const config={schema:{enums:[],
 models:[{name:'Todo',version:1,identity:['id'],fields}],
 resultModels:[{name:'Todo',version:1,identity:['id'],fields,enums:[]}],
 actions:[{name:'Mark',version:1,inputs:[{kind:'model',name:'todo',model:'Todo',operation:'update',cardinality:'single',allowedPatchFields:['title']}],outputs:[]}]},
 mutations:[],loaders:['Todo']};
const key=id=>JSON.stringify({id});
const write=(tx,id,title)=>driver.query(tx,'INSERT INTO member_todo(id,title) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET title=$2',[id,title]);
const plans=new Map();
let refuse=new Set(),failReadsFor=null;
const loader=async({tx,ids,userId})=>{
 if(userId===failReadsFor)throw new Error('subscriber read failed');
 const rows=await driver.query(tx,'SELECT id,title FROM member_todo WHERE id = ANY($1)',[ids.map(i=>i.id)]);
 return ids.map(({id})=>{if(refuse.has(id))throw new CallRejected('todo.forbidden');const row=rows.find(r=>r.id===id);return row?{title:row.title}:null;});
};
const reported=[];
const make=db=>createBackend({config,native,database:db,authenticate:()=>'alice',onError:error=>reported.push(error),
 mutations:{async mark({ctx,args}){await write(ctx.tx,args.todo.id,args.todo.title);await plans.get(args.todo.title)?.(ctx);}},
 loaders:{todo:loader}});
const backend=make(database);
const pull=async(cursors,owner='alice')=>JSON.parse(await backend.pull(owner,JSON.stringify({capabilities:['channel-membership-v1'],cursors,models:{Todo:1}})));
const load=async(channel,after,until)=>JSON.parse(await backend.pull('alice',JSON.stringify({capabilities:['channel-membership-v1'],mode:'bootstrap',channel,models:{Todo:1},after,until})));
const push=(db,clientId,calls)=>db.push('alice',JSON.stringify({capabilities:['channel-membership-v1'],clientId,batchSequence:1,models:{Todo:1},
 mutations:calls.map(([callId,id,title],i)=>({ordinal:i+1,callId,name:'Mark',version:1,args:{todo:{id,title}}}))})).then(JSON.parse);
const seed=(id,title)=>q('INSERT INTO member_todo(id,title) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET title=$2',[id,title]);
const head=async channel=>Number((await q('SELECT head FROM axton_channel WHERE channel=$1',[channel]))[0]?.head??0);
const stamp=async id=>Number((await q('SELECT stamp FROM axton_record WHERE model=$1 AND identity_key=$2',['Todo',key(id)]))[0]?.stamp??0);
/** The pair's one log row: its latest cursor and kind. */
const position=async(channel,id)=>{const [row]=await q('SELECT l.cursor,l.kind FROM axton_channel_log l JOIN axton_record r ON r.id=l.record_id WHERE l.channel=$1 AND r.model=$2 AND r.identity_key=$3',[channel,'Todo',key(id)]);return row?[Number(row.cursor),row.kind]:null;};
const members=async id=>(await q('SELECT m.channel FROM axton_channel_member m JOIN axton_record r ON r.id=m.record_id WHERE r.model=$1 AND r.identity_key=$2 ORDER BY m.channel',['Todo',key(id)])).map(r=>r.channel);
const delivered=page=>page.changes.filter(c=>c.kind==='upsert').map(c=>[c.identity.id,c.stamp,c.state]);
const range=(page,channel)=>page.cursors[channel];
const add=(channel,ids)=>backend.transaction(async call=>{for(const id of ids)call.channel(channel).todo.add({id});});
const remove=(channel,ids)=>backend.transaction(async call=>{for(const id of ids)call.channel(channel).todo.remove({id});});
const touch=(id,title)=>backend.transaction(async({tx,touch})=>{if(title===null)await driver.query(tx,'DELETE FROM member_todo WHERE id=$1',[id]);else await write(tx,id,title);touch.todo({id});});
const ids=(prefix,count)=>Array.from({length:count},(_,i)=>`${prefix}-${String(i).padStart(3,'0')}`);
const settled=()=>new Promise(resolve=>setImmediate(resolve));
before(async()=>{
 await q(await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8'));
 await q('CREATE TABLE IF NOT EXISTS member_todo(id text PRIMARY KEY,title text NOT NULL)');
});
after(async()=>{await pool.end();await check.end();});

test('removing every remaining row yields a terminal page that advances to the head',async()=>{
 const channel='all-removed',records=ids('all-removed',3);
 for(const id of records)await seed(id,'v1');
 await add(channel,records);
 await remove(channel,records);
 assert.equal(await head(channel),6,'each removal takes a position after the head');
 assert.deepEqual(await position(channel,records[0]),[4,'remove'],'and replaces the pair\'s upsert row');
 for(const from of [0,1,2,5]){
  const page=await pull({[channel]:from});
  assert.deepEqual(page.changes.map(c=>[c.kind,c.identity.id]),records.slice(Math.max(0,from-3)).map(id=>['remove',id]),`from ${from}`);
  assert.deepEqual(range(page,channel),{from,to:6,head:6},'the removal page advances to the head');
 }
 const page=await load(channel,0,6);
 assert.ok(page.changes.every(c=>c.kind==='remove'));
 assert.deepEqual([page.from,page.to,page.until,page.head],[0,6,6,6],'the interval is finished');
});

test('removed rows exceeding a page do not starve the active rows after them',async()=>{
 const channel='starve',records=ids('starve',115);
 for(const id of records)await seed(id,'v1');
 await add(channel,records);
 await remove(channel,records.slice(0,60));
 const first=await pull({[channel]:0});
 assert.deepEqual(first.changes.map(c=>c.identity.id),records.slice(60,110),'membership filters before the limit');
 assert.deepEqual(range(first,channel),{from:0,to:110,head:175});
 const second=await pull({[channel]:110});
 assert.deepEqual(second.changes.filter(c=>c.kind==='upsert').map(c=>c.identity.id),records.slice(110));
 assert.equal(second.changes.filter(c=>c.kind==='remove').length,45);
 assert.deepEqual(range(second,channel),{from:110,to:160,head:175},'removals count toward the page limit');
 const history=await load(channel,0,115);
 assert.deepEqual(history.changes.map(r=>r.identity.id),records.slice(60,110));
 assert.equal(history.to,110,'a full page of members is not terminal');
 const rest=await load(channel,110,115);
 assert.deepEqual([rest.changes.map(r=>r.identity.id),rest.to],[records.slice(110),115]);
 const crossing=await load(channel,0,100);
 assert.deepEqual([crossing.changes.map(r=>r.identity.id),crossing.to],[records.slice(60,100),100],'a scan crossing the origin is terminal');
});

test('a record removed and then touched elsewhere is not exposed through its old channel',async()=>{
 const id='exposed';
 await seed(id,'shared');
 await backend.transaction(async({channel})=>{channel('exposed-a').todo.add({id});channel('exposed-b').todo.add({id});});
 await remove('exposed-a',[id]);
 await touch(id,'after removal');
 assert.equal(await stamp(id),2);
 assert.deepEqual(await position('exposed-a',id),[2,'remove'],'the pair\'s row is its removal');
 for(const from of [0,1]){
  const page=await pull({'exposed-a':from});
  assert.deepEqual(page.changes.map(c=>[c.kind,c.identity.id]),[['remove',id]],`from ${from}`);
  assert.deepEqual(range(page,'exposed-a'),{from,to:2,head:2});
 }
 assert.deepEqual((await load('exposed-a',0,1)).changes,[]);
 assert.deepEqual(delivered(await pull({'exposed-b':1})),[[id,2,{title:'after removal'}]]);
});

test('re-adding a removed record publishes its current state at a fresh position without a new stamp',async()=>{
 await seed('readd','v1');await seed('readd-other','v1');
 await add('readd',['readd','readd-other']);
 await touch('readd','v2');
 assert.deepEqual(await position('readd','readd'),[3,'upsert']);
 await remove('readd',['readd']);
 assert.deepEqual(await position('readd','readd'),[4,'remove']);
 await add('readd',['readd']);
 assert.equal(await stamp('readd'),2,'re-adding is not a change');
 assert.deepEqual(await position('readd','readd'),[5,'upsert'],'a fresh cursor at the unchanged stamp');
 const page=await pull({readd:3});
 assert.deepEqual(delivered(page),[['readd',2,{title:'v2'}]]);
 assert.deepEqual(range(page,'readd'),{from:3,to:5,head:5});
});

test('a record removed and re-added above a Bootstrap origin is covered by delivery; removal arrives as an identity event',async()=>{
 const channel='origin',records=['origin-e1','origin-e2','origin-m','origin-x'];
 for(const id of records)await seed(id,'history');
 await add(channel,records);
 const origin=await head(channel);
 assert.equal(origin,4);
 await remove(channel,['origin-m']);
 await add(channel,['origin-m']);
 await remove(channel,['origin-x']);
 const page=await load(channel,0,origin);
 assert.deepEqual(page.changes.map(r=>r.identity.id),['origin-e1','origin-e2']);
 assert.deepEqual([page.to,page.head],[origin,7],'terminal, with a barrier that covers the re-added position');
 const live=await pull({[channel]:origin});
 assert.deepEqual(delivered(live),[['origin-m',1,{title:'history'}]]);
 assert.ok(live.changes.some(c=>c.kind==='remove'&&c.identity.id==='origin-x'));
 assert.deepEqual(range(live,channel),{from:4,to:7,head:7});
});

test('a deleted record stays enrolled and yields null; recreating the identity distributes it again; delete with removal tells the channel nothing',async()=>{
 const id='deleted';
 await seed(id,'v1');
 await add('deleted',[id]);
 await touch(id,null);
 assert.deepEqual(await members(id),['deleted']);
 assert.deepEqual(delivered(await pull({deleted:1})),[[id,2,null]]);
 await touch(id,'again');
 assert.deepEqual(delivered(await pull({deleted:2})),[[id,3,{title:'again'}]],'the same membership receives the recreated record');
 await backend.transaction(async({tx,channel,touch})=>{await driver.query(tx,'DELETE FROM member_todo WHERE id=$1',[id]);touch.todo({id});channel('deleted').todo.remove({id});});
 assert.equal(await stamp(id),4);
 assert.deepEqual([await head('deleted'),await position('deleted',id)],[4,[4,'remove']],'the final relationship wins: a removal, not a deletion, is published');
 const page=await pull({deleted:0});
 assert.ok(page.changes.every(c=>c.kind==='remove'));
 assert.deepEqual(range(page,'deleted'),{from:0,to:4,head:4});
});

test('explicit removal fabricates no deletion: it keeps business rows and stamps and replaces the pair\'s position with a removal',async()=>{
 const id='kept';
 await seed(id,'kept');
 await add('kept',[id]);
 assert.deepEqual([await stamp(id),await head('kept'),await position('kept',id)],[1,1,[1,'upsert']]);
 await remove('kept',[id]);
 assert.deepEqual([await stamp(id),await head('kept'),await position('kept',id)],[1,2,[2,'remove']]);
 assert.deepEqual(await q('SELECT title FROM member_todo WHERE id=$1',[id]),[{title:'kept'}]);
 assert.deepEqual(await members(id),[]);
 const page=await pull({kept:0});
 assert.deepEqual(page.changes,[{kind:'remove',channel:'kept',cursor:2,model:'Todo',identity:{id}}]);
 assert.deepEqual(range(page,'kept'),{from:0,to:2,head:2});
});

// ---- Concurrency ----------------------------------------------------------

/** The record's stamp and memberships in the caller's snapshot; the first read fixes it. */
const view=async(tx,id)=>({
 stamp:Number((await driver.query(tx,'SELECT stamp FROM axton_record WHERE model=$1 AND identity_key=$2',['Todo',key(id)]))[0]?.stamp??0),
 members:(await driver.query(tx,'SELECT m.channel FROM axton_channel_member m JOIN axton_record r ON r.id=m.record_id WHERE r.model=$1 AND r.identity_key=$2 ORDER BY m.channel',['Todo',key(id)])).map(r=>r.channel),
});
/**
 * `held` fixes its snapshot, then `other` runs to commit, then `held` goes on
 * from that older snapshot. Answers the view of each of `held`'s attempts.
 */
const stale=async(id,held,other)=>{
 const views=[];let fixed,release;
 const snapshot=new Promise(r=>{fixed=r;});const gate=new Promise(r=>{release=r;});
 const heldTx=backend.transaction(async call=>{views.push(await view(call.tx,id));if(views.length===1){fixed();await gate;}await held(call);});
 await Promise.race([snapshot,heldTx.then(()=>{throw new Error('held committed before its snapshot was fixed');})]);
 await backend.transaction(other);
 release();
 await heldTx;
 return views;
};
/** Both fix their snapshots before either is let through; answers each one's attempts. */
const together=async(id,first,second)=>{
 const views=[[],[]];let arrived=0,open;const both=new Promise(r=>{open=r;});
 const run=(n,body)=>backend.transaction(async call=>{views[n].push(await view(call.tx,id));if(views[n].length===1){if(++arrived===2)open();await both;}await body(call);});
 await Promise.all([run(0,first),run(1,second)]);
 return views;
};
const touchBody=(id,title)=>async({tx,touch})=>{await write(tx,id,title);touch.todo({id});};
const addBody=(channel,id)=>async({channel:c})=>{c(channel).todo.add({id});};
const removeBody=(channel,id)=>async({channel:c})=>{c(channel).todo.remove({id});};
const outcome=async(id,channels)=>({stamp:await stamp(id),members:await members(id),
 ...Object.fromEntries(await Promise.all(channels.map(async c=>[c,{head:await head(c),position:await position(c,id)}])))});
/** Which body the driver retried: the one serialized second. */
const retried=views=>{const counts=views.map(v=>v.length);assert.ok(counts.includes(1)&&counts.includes(2)&&counts.length===2,`exactly one retry: ${counts}`);return counts.indexOf(2);};

test('touch and add serialize in either order: the enrolled Channel always holds the final stamp',async t=>{
 const orders=[];
 for(const initial of ['present','absent']){
  // Each order's committed outcome for the record and the Channel C it is
  // added to; a present record is already a member of B at stamp 1.
  const expected=initial==='present'
   ?{touchFirst:{stamp:2,C:{head:1,position:[1,'upsert']}},addFirst:{stamp:2,C:{head:2,position:[2,'upsert']}}}
   :{touchFirst:{stamp:1,C:{head:1,position:[1,'upsert']}},addFirst:{stamp:2,C:{head:2,position:[2,'upsert']}}};
  const prepare=async id=>{if(initial==='present'){await seed(id,'v1');await add(`${id}-B`,[id]);}return `${id}-C`;};
  const verify=async(id,order,label)=>{
   const C=`${id}-C`,got=await outcome(id,[C]);
   const want=initial==='present'?[`${id}-B`,C]:[C];
   assert.deepEqual({stamp:got.stamp,members:got.members,C:got[C]},{...expected[order],members:want},`${label}: ${order}`);
   assert.deepEqual(delivered(await pull({[C]:0})),[[id,got.stamp,{title:'touched'}]],`${label}: C delivers the touched content`);
  };
  // add commits while touch holds an older snapshot: touch retries and publishes to C.
  {
   const id=`ta-${initial}-stale-touch`,C=await prepare(id);
   const views=await stale(id,touchBody(id,'touched'),addBody(C,id));
   assert.equal(views.length,2,'the stale touch retried');
   assert.ok(views[1].members.includes(C),'its retry read the new membership');
   await verify(id,'addFirst','stale touch');
  }
  // touch commits while add holds an older snapshot: add retries at the new stamp.
  {
   const id=`ta-${initial}-stale-add`,C=await prepare(id);
   const views=await stale(id,addBody(C,id),touchBody(id,'touched'));
   assert.equal(views.length,2,'the stale add retried');
   assert.equal(views[1].stamp,views[0].stamp+1,'its retry read the advanced stamp');
   await verify(id,'touchFirst','stale add');
  }
  // Both snapshots fixed, then released together: one wins, the other retries.
  for(let trial=0;trial<4;trial++){
   const id=`ta-${initial}-race-${trial}`,C=await prepare(id);
   const views=await together(id,touchBody(id,'touched'),addBody(C,id));
   const order=retried(views)===1?'touchFirst':'addFirst';
   orders.push(`${initial}:${order}`);
   await verify(id,order,`race ${trial}`);
  }
 }
 t.diagnostic(`released together, committed orders: ${orders.join(' ')}`);
});

test('touch and remove serialize in either order; the stale touch retries and publishes nothing to the removed Channel',async t=>{
 const orders=[];
 const prepare=async id=>{await seed(id,'v1');await backend.transaction(async({channel})=>{channel(`${id}-B`).todo.add({id});channel(`${id}-C`).todo.add({id});});return `${id}-C`;};
 // Either way C ends with the pair's removal; touching first published one more position before it.
 const expected={touchFirst:{head:3,position:[3,'remove']},removeFirst:{head:2,position:[2,'remove']}};
 const verify=async(id,order,label)=>{
  const C=`${id}-C`,got=await outcome(id,[C]);
  assert.deepEqual({stamp:got.stamp,members:got.members,C:got[C]},{stamp:2,members:[`${id}-B`],C:expected[order]},`${label}: ${order}`);
  assert.ok((await pull({[C]:0})).changes.every(c=>c.kind==='remove'),`${label}: C returns removal without content`);
 };
 {
  const id='tr-stale-touch',C=await prepare(id);
  const views=await stale(id,touchBody(id,'touched'),removeBody(C,id));
  assert.equal(views.length,2,'the stale touch retried');
  assert.deepEqual(views[1].members,[`${id}-B`],'its retry read the removal');
  await verify(id,'removeFirst','stale touch');
 }
 {
  const id='tr-stale-remove',C=await prepare(id);
  const views=await stale(id,removeBody(C,id),touchBody(id,'touched'));
  assert.equal(views.length,2,'the stale remove retried');
  assert.equal(views[1].stamp,2,'its retry read the touch');
  await verify(id,'touchFirst','stale remove');
 }
 for(let trial=0;trial<4;trial++){
  const id=`tr-race-${trial}`,C=await prepare(id);
  const views=await together(id,touchBody(id,'touched'),removeBody(C,id));
  const order=retried(views)===1?'touchFirst':'removeFirst';
  orders.push(order);
  await verify(id,order,`race ${trial}`);
 }
 t.diagnostic(`released together, committed orders: ${orders.join(' ')}`);
});

test('membership-only races keep the stamp; a duplicate add publishes once; a remove of an absent record orders before a first enrollment',async t=>{
 const orders=[];
 const fixed=async id=>{await seed(id,'v5');await q('INSERT INTO axton_record(model,identity_key,stamp) VALUES($1,$2,5)',['Todo',key(id)]);};
 // Two adds of the same new member: the retried one sees it and publishes nothing.
 {
  const id='mo-dup';await fixed(id);
  const views=await stale(id,addBody('mo-dup',id),addBody('mo-dup',id));
  assert.equal(views.length,2);
  assert.deepEqual(views[1].members,['mo-dup'],'the retry read the committed membership');
  assert.deepEqual(await outcome(id,['mo-dup']),{stamp:5,members:['mo-dup'],'mo-dup':{head:1,position:[1,'upsert']}});
 }
 for(let trial=0;trial<3;trial++){
  const id=`mo-dup-race-${trial}`;await fixed(id);
  retried(await together(id,addBody(`mo-dup-race-${trial}`,id),addBody(`mo-dup-race-${trial}`,id)));
  assert.deepEqual(await outcome(id,[`mo-dup-race-${trial}`]),{stamp:5,members:[`mo-dup-race-${trial}`],[`mo-dup-race-${trial}`]:{head:1,position:[1,'upsert']}});
 }
 // Add and remove of one Channel: either order, at the unchanged stamp.
 for(let trial=0;trial<4;trial++){
  const id=`mo-ar-${trial}`,channel=`mo-ar-${trial}`;await fixed(id);
  const views=await together(id,addBody(channel,id),removeBody(channel,id));
  const order=retried(views)===1?'addFirst':'removeFirst';
  orders.push(order);
  // Adding first publishes the member and then its removal; removing an absent member first publishes nothing.
  assert.deepEqual(await outcome(id,[channel]),{stamp:5,...(order==='addFirst'?{members:[],[channel]:{head:2,position:[2,'remove']}}:{members:[channel],[channel]:{head:1,position:[1,'upsert']}})},order);
 }
 // A record with no metadata: the removal locks nothing and writes nothing, so
 // either way it is ordered before the first enrollment, which is kept.
 for(const held of ['remove','add']){
  const id=`mo-absent-${held}`,channel=`mo-absent-${held}`;
  const views=held==='remove'?await stale(id,removeBody(channel,id),addBody(channel,id)):await stale(id,addBody(channel,id),removeBody(channel,id));
  assert.equal(views.length,1,`${held} held: nothing to conflict on`);
  assert.deepEqual(await outcome(id,[channel]),{stamp:1,members:[channel],[channel]:{head:1,position:[1,'upsert']}},`${held} held`);
 }
 t.diagnostic(`add/remove released together, committed orders: ${orders.join(' ')}`);
});

// ---- Rollback and isolation ----------------------------------------------

const callId=n=>`01890f47-1234-7123-8123-${n.toString(16).padStart(12,'0')}`;
const snapshotOf=async(records,channels)=>({
 rows:await q('SELECT id,title FROM member_todo WHERE id = ANY($1) ORDER BY id',[records]),
 stamps:await q('SELECT identity_key,stamp::int FROM axton_record WHERE identity_key = ANY($1) ORDER BY identity_key',[records.map(key)]),
 members:await q('SELECT m.channel,r.identity_key FROM axton_channel_member m JOIN axton_record r ON r.id=m.record_id WHERE r.identity_key = ANY($1) ORDER BY m.channel,r.identity_key',[records.map(key)]),
 tags:await q('SELECT t.channel,t.name FROM axton_channel_tag t WHERE t.channel = ANY($1) ORDER BY t.channel,t.name',[channels]),
 channels:await q('SELECT channel,head::int FROM axton_channel WHERE channel = ANY($1) ORDER BY channel',[channels]),
 positions:await q('SELECT l.channel,r.identity_key,l.cursor::int,l.kind FROM axton_channel_log l JOIN axton_record r ON r.id=l.record_id WHERE l.channel = ANY($1) ORDER BY l.channel,r.identity_key',[channels]),
});
const listen=channels=>{const woken=[];const stops=channels.map(c=>backend.onCommitted(c,()=>woken.push(c)));return {woken,stop:()=>stops.forEach(s=>s())};};

test('a call rejected after settlement rolls back its writes, relationships, stamps, heads and wake; the batch continues',async()=>{
 const channels=['rb-kept','rb-rolled','rb-old'],untouched=['rb-refused','rb-other','rb-old'];
 await seed('rb-other','v1');await seed('rb-old','v1');await add('rb-old',['rb-old','rb-other']);
 plans.set('rb-keep',ctx=>{ctx.channel('rb-kept').todo.add({id:'rb-kept'});});
 plans.set('rb-declare',async ctx=>{
  await write(ctx.tx,'rb-other','changed by the rejected call');
  ctx.touch.todo({id:'rb-other'});
  ctx.channel('rb-rolled').todo.add({id:'rb-refused'});
  ctx.channel('rb-rolled').add([{model:'Todo',identity:{id:'rb-other'}}]);
  ctx.channel('rb-old').todo.remove({id:'rb-old'});
 });
 refuse=new Set(['rb-refused']);
 const before=await snapshotOf(untouched,['rb-rolled','rb-old']);
 const wakes=listen(channels);
 let receipt;
 try{receipt=await push(backend,'rb',[[callId(0x501),'rb-kept','rb-keep'],[callId(0x502),'rb-refused','rb-declare']]);}
 finally{refuse=new Set();}
 await settled();wakes.stop();
 assert.deepEqual(receipt.completions.map(c=>c.outcome.status),['succeeded','failed']);
 assert.deepEqual([receipt.completions[1].outcome.code,receipt.completions[1].outcome.execution],['todo.forbidden','rejected']);
 assert.deepEqual(await snapshotOf(untouched,['rb-rolled','rb-old']),before,'business rows, stamps, memberships, heads and positions are as before the rejected call');
 assert.deepEqual(await q('SELECT id FROM member_todo WHERE id=$1',['rb-refused']),[]);
 assert.deepEqual(await q('SELECT channel FROM axton_channel WHERE channel=$1',['rb-rolled']),[],'the Channel its enrollment created rolled back too');
 assert.deepEqual(await members('rb-kept'),['rb-kept'],'the preceding call committed');
 assert.deepEqual(wakes.woken,['rb-kept'],'only the committed publication wakes');
 const saved=await q('SELECT response FROM axton_call WHERE call_id=$1',[callId(0x502)]);
 assert.deepEqual(JSON.parse(saved[0].response).completion.outcome,receipt.completions[1].outcome,'the saved outcome is the rejection, not the rolled-back settlement');
});

test('a failed transaction rolls back every table, the saved outcome and the wake; the retried batch settles once',async()=>{
 const records=['tx-a','tx-b'],channels=['tx-new','tx-old'];
 await seed('tx-b','v1');await add('tx-old',['tx-b']);
 plans.set('tx-declare',async ctx=>{
  ctx.channel('tx-new').todo.add({id:'tx-a'});
  await write(ctx.tx,'tx-b','changed');
  ctx.touch.todo({id:'tx-b'});
  ctx.channel('tx-old').todo.remove({id:'tx-b'});
  ctx.channel('tx-new').todo.add({id:'tx-b'});
 });
 const normal=database;
 const broken={driver,transaction:normal.transaction,persistence:tx=>({call:async request=>{if(request.op==='saveReceipt')throw new Error('forced saveReceipt fault');return normal.persistence(tx).call(request);}})};
 const before=await snapshotOf(records,channels);
 const failing=make(broken);
 const woken=[];const stops=channels.map(c=>failing.onCommitted(c,()=>woken.push(c)));
 await assert.rejects(()=>push(failing,'tx',[[callId(0x601),'tx-a','tx-declare']]),/forced saveReceipt fault/);
 await settled();stops.forEach(s=>s());
 assert.deepEqual(await snapshotOf(records,channels),before,'business rows, stamps, memberships, heads and positions rolled back');
 assert.deepEqual(await q('SELECT call_id FROM axton_call WHERE call_id=$1',[callId(0x601)]),[],'the saved outcome rolled back');
 assert.deepEqual(await q('SELECT client_id FROM axton_client WHERE client_id=$1',['tx']),[]);
 assert.deepEqual(woken,[],'a rolled-back transaction wakes nobody');
 const wakes=listen(channels);
 const receipt=await push(backend,'tx',[[callId(0x601),'tx-a','tx-declare']]);
 await settled();wakes.stop();
 assert.equal(receipt.completions[0].outcome.status,'succeeded');
 assert.deepEqual(wakes.woken.sort(),['tx-new','tx-old'],'the committed retry wakes the Channels it published to, the removal\'s included');
 const after=await snapshotOf(records,channels);
 assert.deepEqual(after.members,[{channel:'tx-new',identity_key:key('tx-a')},{channel:'tx-new',identity_key:key('tx-b')}]);
 const replay=await push(backend,'tx-replay',[[callId(0x601),'tx-a','tx-declare']]);
 assert.deepEqual(replay.completions,receipt.completions);
 assert.deepEqual(await snapshotOf(records,channels),after,'a replay settles nothing again');
});

test('a later subscriber read failure is isolated from the committed mutation',async()=>{
 plans.set('iso-declare',ctx=>{ctx.channel('iso').todo.add({id:'iso'});});
 const receipt=await push(backend,'iso',[[callId(0x701),'iso','iso-declare']]);
 assert.equal(receipt.completions[0].outcome.status,'succeeded');
 const committed=await snapshotOf(['iso'],['iso']);
 failReadsFor='bob';
 let page;
 try{page=await pull({iso:0},'bob');}finally{failReadsFor=null;}
 assert.deepEqual(page.changes.map(c=>[c.identity.id,c.state,c.error]),[['iso',null,'loader.failed']],'the subscriber gets an error change');
 assert.deepEqual(range(page,'iso'),{from:0,to:1,head:1},'and its cursor still advances');
 assert.ok(reported.some(error=>/subscriber read failed/.test(error.message)),'the read failure reaches onError');
 assert.deepEqual(await snapshotOf(['iso'],['iso']),committed,'the committed mutation is untouched');
 const replay=await push(backend,'iso-replay',[[callId(0x701),'iso','iso-declare']]);
 assert.deepEqual(replay.completions,receipt.completions,'its saved outcome still replays');
 assert.deepEqual(delivered(await pull({iso:0})),[['iso',1,{title:'iso-declare'}]]);
});
// #180 on the pg shim, at PostgreSQL's default Read Committed: a transaction the
// application opened on its own pool client, as a host that owns its transaction does.
const ownedPg=async body=>{
 const client=await pool.connect();
 try{await client.query('BEGIN');try{const result=await body(client);await client.query('COMMIT');return result;}catch(error){await client.query('ROLLBACK');throw error;}}
 finally{client.release();}
};
test('backend.publish settles in a caller-owned Read Committed transaction and reads memberships committed before its touch',async()=>{
 await seed('owned-rc','v1');await add('owned-rc-a',['owned-rc']);
 let entered,release;const inside=new Promise(resolve=>{entered=resolve;});const gate=new Promise(resolve=>{release=resolve;});
 const caller=ownedPg(async tx=>{
  await driver.query(tx,'SELECT 1',[]);entered();await gate;
  await write(tx,'owned-rc','v2');
  return backend.publish(tx,({touch})=>{touch.todo({id:'owned-rc'});});
 });
 await inside;
 // Committed after the caller began and before it touches: its settlement must still see it.
 await add('owned-rc-b',['owned-rc']);
 release();
 const wake=await caller;
 await settled();let woke=0;const unsubscribe=backend.onCommitted('owned-rc-b',()=>{woke++;});
 assert.equal(await stamp('owned-rc'),2);
 assert.deepEqual(await position('owned-rc-a','owned-rc'),[2,'upsert']);
 assert.deepEqual(await position('owned-rc-b','owned-rc'),[2,'upsert'],'the newer member hears the change');
 await settled();assert.equal(woke,0);wake();await settled();assert.equal(woke,1);
 assert.deepEqual(delivered(await pull({'owned-rc-b':1})),[['owned-rc',2,{title:'v2'}]]);
 unsubscribe();
});
