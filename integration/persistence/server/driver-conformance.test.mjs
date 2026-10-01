// Every PostgreSQL shim (`pg`, `prisma`, `drizzle`) must give AXTON the same
// persistence behavior through the two-method driver. The assertions read the
// database through a separate `pg` pool so they do not depend on the tool
// under test; business writes inside handlers go through `driver.query`, which
// is what makes the same suite run unchanged against every shim.
import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {Pool} from 'pg';
import {drizzle as drizzleOrm} from 'drizzle-orm/node-postgres';
import {createBackend} from '../../../packages/server/index.mts';
import {pg,prisma,answer,pgDriver,withRetries,retryDelay,RETRY_BACKOFF_BASE_MS,RETRY_BACKOFF_CAP_MS} from '../../../packages/postgres/index.mts';
import {drizzle} from '../../../packages/postgres/src/drizzle.mts';
const require=createRequire(import.meta.url);
const {PrismaClient}=require('../../bindings/node/generated/client');
const url=process.env.DATABASE_URL;
const check=new Pool({connectionString:url});
const q=async(sql,params=[])=>(await check.query(sql,params)).rows;
const schema={enums:[],models:[{name:'Task',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false},{name:'title',type:{kind:'scalar',name:'string'},nullable:false}]}]};
const config={schema,mutations:[{name:'edit',version:1,slots:[{name:'task',model:'Task',operation:'update',cardinality:'single',allowedPatchFields:['title']}]}]};
const authenticate=async req=>req.headers.authorization==='Bearer alice'?'alice':null;
const key=id=>JSON.stringify({id});
before(async()=>{await q(await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8'));await q('CREATE TABLE IF NOT EXISTS conformance_task(id text PRIMARY KEY,title text NOT NULL)');
 // The lock-then-recheck races (#202). No foreign keys: a key check would make
 // even the old level fail serialization, hiding the stale re-check.
 for(const sql of ['CREATE TABLE IF NOT EXISTS race_book(id text PRIMARY KEY)','CREATE TABLE IF NOT EXISTS race_author(id text PRIMARY KEY,book_id text NOT NULL)','CREATE TABLE IF NOT EXISTS race_archive(author_id text PRIMARY KEY)','CREATE TABLE IF NOT EXISTS race_post(id text PRIMARY KEY)','CREATE TABLE IF NOT EXISTS race_star(id text PRIMARY KEY,post_id text NOT NULL)'])await q(sql);
});
after(()=>check.end());

test('pg: a connection whose ROLLBACK fails is released as broken, a healthy one is released for reuse',async()=>{
 const releases=[];
 const fakePool=(failRollback)=>({connect:async()=>({
  query:async sql=>{if(sql==='ROLLBACK'&&failRollback)throw new Error('connection lost');return {rows:[]};},
  release:arg=>releases.push(arg),
 })});
 await assert.rejects(()=>pgDriver(fakePool(true)).transaction(async()=>{throw new Error('body failed');}),/body failed/);
 assert.ok(releases.at(-1) instanceof Error,'the broken connection is discarded');
 await assert.rejects(()=>pgDriver(fakePool(false)).transaction(async()=>{throw new Error('body failed');}),/body failed/);
 assert.equal(releases.at(-1),undefined,'a rolled-back connection goes back to the pool');
 assert.equal(await pgDriver(fakePool(false)).transaction(async()=>7),7);
 assert.equal(releases.at(-1),undefined);
});

// Lock-then-recheck (#202). `first` locks a parent row and changes its
// children, then holds its transaction open. `second` fixes its snapshot while
// `first` is still open - AXTON's own claim statements do that before a
// handler runs - then locks the same parent, waiting for `first` to commit,
// and re-checks the children to decide its write. Only promises order the two:
// `second` reads its snapshot before `first` is allowed to commit.
const race=async(transaction,{first,second})=>{
 const runs={first:0,second:0};
 let locked,fixed,commit;
 const firstLocked=new Promise(resolve=>{locked=resolve;});const secondFixed=new Promise(resolve=>{fixed=resolve;});const firstGate=new Promise(resolve=>{commit=resolve;});
 const a=transaction(async tx=>{runs.first++;await first(tx);if(runs.first===1){locked();await firstGate;}});
 await Promise.race([firstLocked,a.then(()=>{throw new Error('first committed before holding its lock');})]);
 const b=transaction(async tx=>{runs.second++;await second(tx,runs.second===1?fixed:()=>{});});
 await Promise.race([secondFixed,b.then(()=>{throw new Error('second finished before fixing its snapshot');})]);
 commit();
 await a;await b;
 return runs;
};
// Delete case: an Author leaves a Book while an Archive is written for them.
// `leave` locks the Book, removes the Author and every Archive of theirs;
// `archive` locks the Book, re-checks that the Author is still there and
// writes their Archive. Either serial order leaves neither.
const archiveRace=async(transaction,query,name)=>{
 const book=`${name}-book`,author=`${name}-author`;
 await q('INSERT INTO race_book(id) VALUES($1)',[book]);await q('INSERT INTO race_author(id,book_id) VALUES($1,$2)',[author,book]);
 const runs=await race(transaction,{
  first:async tx=>{
   await query(tx,'SELECT id FROM race_book WHERE id=$1 FOR UPDATE',[book]);
   await query(tx,'DELETE FROM race_author WHERE id=$1',[author]);
   await query(tx,'DELETE FROM race_archive WHERE author_id=$1',[author]);
  },
  second:async(tx,fixed)=>{
   await query(tx,'SELECT id FROM race_book WHERE id=$1',[book]);fixed();
   await query(tx,'SELECT id FROM race_book WHERE id=$1 FOR UPDATE',[book]);
   if((await query(tx,'SELECT id FROM race_author WHERE id=$1 AND book_id=$2',[author,book])).length>0)
    await query(tx,'INSERT INTO race_archive(author_id) VALUES($1)',[author]);
  },
 });
 return {runs,authors:await q('SELECT id FROM race_author WHERE id=$1',[author]),archives:await q('SELECT author_id FROM race_archive WHERE author_id=$1',[author])};
};
// Insert case, a phantom: a Star is added to a Post while the Post is deleted.
// `star` locks the Post, re-checks it exists and inserts the Star; `remove`
// locks the Post, deletes its Stars, then the Post. Either serial order leaves
// neither.
const starRace=async(transaction,query,name)=>{
 const post=`${name}-post`,star=`${name}-star`;
 await q('INSERT INTO race_post(id) VALUES($1)',[post]);
 const runs=await race(transaction,{
  first:async tx=>{
   if((await query(tx,'SELECT id FROM race_post WHERE id=$1 FOR UPDATE',[post])).length>0)
    await query(tx,'INSERT INTO race_star(id,post_id) VALUES($1,$2)',[star,post]);
  },
  second:async(tx,fixed)=>{
   await query(tx,'SELECT id FROM race_post WHERE id=$1',[post]);fixed();
   await query(tx,'SELECT id FROM race_post WHERE id=$1 FOR UPDATE',[post]);
   await query(tx,'DELETE FROM race_star WHERE post_id=$1',[post]);
   await query(tx,'DELETE FROM race_post WHERE id=$1',[post]);
  },
 });
 return {runs,posts:await q('SELECT id FROM race_post WHERE id=$1',[post]),stars:await q('SELECT id FROM race_star WHERE post_id=$1',[post])};
};

const shims=[];
{const pool=new Pool({connectionString:url});shims.push({name:'pg',database:pg(pool),close:()=>pool.end()});}
{const client=new PrismaClient();shims.push({name:'prisma',database:prisma(client),close:()=>client.$disconnect()});}
{const pool=new Pool({connectionString:url});shims.push({name:'drizzle',database:drizzle(drizzleOrm(pool)),close:()=>pool.end()});}

for(const shim of shims){
 const {database}=shim;const {driver}=database;
 const inTx=body=>driver.transaction(tx=>body(tx,(sql,params=[])=>driver.query(tx,sql,params),r=>answer(driver,tx,r)));
 const p=name=>`${shim.name}-${name}`;
 /** One final member state, as settlement hands it to applyChannelMembers. */
 const member=(channel,id,{present=true,tags=[],publish=true}={})=>({channel,model:'Task',identity:{id},identityKey:key(id),present,tags,publish});
 test(`[${shim.name}] call claims commit with business writes and replay without overwriting the original`,async()=>{
  const id=p('call-commit'), row=p('call-row');
  const claim={op:'claimCall',owner:'alice',callId:id,request:'{"name":"first"}'};
  const response='{"status":"succeeded","result":1}';
  let bodies=0;
  let signalFirst,releaseFirst;
  const firstClaimed=new Promise(resolve=>{signalFirst=resolve;});
  const firstGate=new Promise(resolve=>{releaseFirst=resolve;});
  const invoke=(hold=false,onClaimStart)=>inTx(async(tx,query,a)=>{
   const pid=onClaimStart?Number((await query('SELECT pg_backend_pid() AS pid'))[0].pid):null;
   const claimPending=a(claim);
   onClaimStart?.(pid);
   const claimed=await claimPending;
   if(claimed.fresh){
    if(hold){signalFirst();await firstGate;}
    bodies++;
    await query('INSERT INTO conformance_task(id,title) VALUES($1,$2)',[row,'once']);
    await a({op:'saveCall',owner:'alice',callId:id,response});
   }
   return claimed;
  });
  const firstTx=invoke(true);
  await Promise.race([firstClaimed,firstTx.then(()=>{throw new Error('first claim never held the transaction');})]);
  let secondDone=false;
  let signalSecond;
  const secondClaimStarted=new Promise(resolve=>{signalSecond=resolve;});
  const secondTx=invoke(false,signalSecond).finally(()=>{secondDone=true;});
  const secondPid=await secondClaimStarted;
  let blocked=false;
  for(let attempt=0;attempt<100;attempt++){
   const rows=await q('SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1',[secondPid]);
   if(rows[0]?.wait_event_type==='Lock'){blocked=true;break;}
   await new Promise(resolve=>setTimeout(resolve,10));
  }
  const finishedBeforeCommit=secondDone;
  releaseFirst();
  const [first,second]=await Promise.all([firstTx,secondTx]);
  assert.equal(finishedBeforeCommit,false,'the duplicate cannot finish before the first transaction commits');
  assert.equal(blocked,true,'the second claim reached PostgreSQL and waited on the first transaction');
  assert.deepEqual([first.fresh,second.fresh],[true,false]);
  assert.equal(bodies,1);
  assert.deepEqual(await q('SELECT title FROM conformance_task WHERE id=$1',[row]),[{title:'once'}]);
  assert.deepEqual(await q('SELECT owner_id,call_id,request,response FROM axton_call WHERE call_id=$1',[id]),[{owner_id:'alice',call_id:id,request:claim.request,response}]);
  assert.deepEqual(await invoke(),{fresh:false,request:claim.request,response});
  assert.deepEqual(await inTx((tx,_,a)=>a({...claim,request:'{"name":"different"}'})),{fresh:false,request:claim.request,response});
  assert.deepEqual(await inTx((tx,_,a)=>a({...claim,owner:'bob'})),{fresh:true,request:claim.request,response:null});
  assert.deepEqual(await q('SELECT response FROM axton_call WHERE owner_id=$1 AND call_id=$2',['bob',id]),[{response:null}]);
  await assert.rejects(()=>inTx((tx,_,a)=>a({...claim,owner:'bob'})),/incomplete stored response/);
  await assert.rejects(()=>inTx((tx,_,a)=>a({op:'saveCall',owner:'bob',callId:id,response})),/Call not claimed/);
  await assert.rejects(()=>inTx((tx,_,a)=>a({op:'saveCall',owner:'alice',callId:id,response:'other'})),/Call .*already completed|Call .*not claimed/);
 });
 test(`[${shim.name}] rollback removes the call claim and its business write`,async()=>{
  const id=p('call-rollback'),row=p('call-undone');
  await assert.rejects(()=>inTx(async(tx,query,a)=>{
   assert.deepEqual(await a({op:'claimCall',owner:'alice',callId:id,request:'{}'}),{fresh:true,request:'{}',response:null});
   await query('INSERT INTO conformance_task(id,title) VALUES($1,$2)',[row,'undone']);
   await a({op:'saveCall',owner:'alice',callId:id,response:'{}'});
   throw new Error('cancel');
  }),/cancel/);
  assert.deepEqual(await q('SELECT * FROM axton_call WHERE call_id=$1',[id]),[]);
  assert.deepEqual(await q('SELECT * FROM conformance_task WHERE id=$1',[row]),[]);
 });
 test(`[${shim.name}] a call claimed under a released savepoint can be saved by its transaction`,async()=>{
  const id=p('call-subtransaction');
  await inTx(async(tx,query,a)=>{
   await query('SAVEPOINT axton_claim_nested');
   assert.deepEqual(await a({op:'claimCall',owner:'alice',callId:id,request:'{}'}),{fresh:true,request:'{}',response:null});
   assert.deepEqual(await query('SELECT claim_tx = pg_current_xact_id() AS owned FROM axton_call WHERE owner_id=$1 AND call_id=$2',['alice',id]),[{owned:true}]);
   await query('RELEASE SAVEPOINT axton_claim_nested');
   assert.deepEqual(await query('SELECT claim_tx = pg_current_xact_id() AS owned FROM axton_call WHERE owner_id=$1 AND call_id=$2',['alice',id]),[{owned:true}]);
   await a({op:'saveCall',owner:'alice',callId:id,response:'{"ok":true}'});
  });
  assert.deepEqual(await q('SELECT response FROM axton_call WHERE owner_id=$1 AND call_id=$2',['alice',id]),[{response:'{"ok":true}'}]);
 });
 test(`[${shim.name}] a push writes business rows and AXTON metadata in one transaction and a pull reads them back`,async()=>{
  const backend=createBackend({config,database,authenticate,handlers:{async edit({input,tx,scope: channel}){await driver.query(tx,'INSERT INTO conformance_task(id,title) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET title=$2',[input.task.identity.id,input.task.patch.title]);channel(p('shared')).add.task(input.task.identity);}},loaders:{async task({ids,tx}){const rows=await driver.query(tx,'SELECT id,title FROM conformance_task WHERE id = ANY($1)',[ids.map(i=>i.id)]);return ids.map(i=>{const r=rows.find(r=>r.id===i.id);return r?{title:r.title}:null;});}}});
  const receipt=JSON.parse(await backend.push('alice',JSON.stringify({capabilities:['channel-membership-v1'],clientId:p('c'),batchSequence:1,models:{Task:1},mutations:[{ordinal:1,name:'edit',operations:[{model:'Task',op:'update',identity:{id:p('t')},values:{title:'typed'}}]}]})));
  assert.deepEqual(receipt.records,[{identity:{id:p('t')},model:'Task',stamp:1,state:{title:'typed'}}]);
  assert.deepEqual(await q('SELECT title FROM conformance_task WHERE id=$1',[p('t')]),[{title:'typed'}]);
  assert.equal(Number((await q('SELECT sequence FROM axton_client WHERE client_id=$1',[p('c')]))[0].sequence),1);
  const page=JSON.parse(await backend.pull('alice',JSON.stringify({capabilities:['channel-membership-v1'],cursors:{[p('shared')]:0},models:{Task:1}})));
  assert.equal(page.changes.length,1);assert.deepEqual(page.changes[0].state,{title:'typed'});assert.equal(page.changes[0].stamp,1);
  assert.equal(await backend.push('alice',JSON.stringify({capabilities:['channel-membership-v1'],clientId:p('c'),batchSequence:1,models:{Task:1},mutations:[{ordinal:1,name:'edit',operations:[]}]})),JSON.stringify(receipt),'a retry answers from the stored receipt');
 });
 test(`[${shim.name}] claim creates and locks the client row; saveReceipt refuses another owner; head of an unknown channel is 0`,async()=>{
  const claimed=await inTx((tx,_,a)=>a({op:'claim',owner:'alice',clientId:p('claim')}));
  assert.deepEqual(claimed,{clientId:p('claim'),owner:'alice',sequence:0,receipt:null});
  await assert.rejects(()=>inTx((tx,_,a)=>a({op:'saveReceipt',owner:'bob',clientId:p('claim'),sequence:1,receipt:'{}'})),/Receipt owner mismatch/);
  await inTx((tx,_,a)=>a({op:'saveReceipt',owner:'alice',clientId:p('claim'),sequence:2**40,receipt:'{"big":true}'}));
  const again=await inTx((tx,_,a)=>a({op:'claim',owner:'alice',clientId:p('claim')}));
  assert.deepEqual(again,{clientId:p('claim'),owner:'alice',sequence:2**40,receipt:'{"big":true}'},'bigint counters round-trip beyond 32 bits');
  assert.equal(await inTx((tx,_,a)=>a({op:'head',channel:p('nowhere')})),0);
 });
 test(`[${shim.name}] advanceStamp increments without a channel; ensureStamp initialises once and keeps an advanced stamp`,async()=>{
  const ref={model:'Task',identityKey:key(p('stamp'))};
  assert.deepEqual(await inTx(async(tx,_,a)=>[await a({op:'advanceStamp',...ref}),await a({op:'advanceStamp',...ref})]),[1,2]);
  assert.deepEqual(await q('SELECT stamp::int AS stamp FROM axton_record WHERE identity_key=$1',[ref.identityKey]),[{stamp:2}]);
  assert.deepEqual(await q('SELECT l.* FROM axton_channel_log l JOIN axton_record r ON r.id=l.record_id WHERE r.identity_key=$1',[ref.identityKey]),[]);
  const race={model:'Task',identityKey:key(p('race'))};
  const ensure=()=>inTx((tx,_,a)=>a({op:'ensureStamp',...race}));
  assert.deepEqual(await Promise.all([ensure(),ensure(),ensure()]),[1,1,1],'concurrent first publications agree on 1 (serialization retries)');
  assert.equal((await q('SELECT * FROM axton_record WHERE identity_key=$1',[race.identityKey])).length,1);
  await inTx((tx,_,a)=>a({op:'advanceStamp',...race}));
  assert.equal(await ensure(),2);
 });
 test(`[${shim.name}] applyChannelMembers reserves one consecutive range per Channel in delta order, answers a kept member's position and refuses a record without metadata`,async()=>{
  const [a,b]=[p('range-a'),p('range-b')];const [x,y,z]=[p('rx'),p('ry'),p('rz')];
  const apply=deltas=>inTx((tx,_,f)=>f({op:'applyChannelMembers',deltas}));
  const at=positions=>positions.map(r=>[r.channel,JSON.parse(r.identityKey).id,r.cursor,r.kind]);
  await assert.rejects(()=>apply([member(a,x)]),/Record metadata missing/);
  assert.deepEqual(await q('SELECT * FROM axton_channel WHERE channel=$1',[a]),[],'the refused call created no Channel');
  await inTx(async(tx,_,f)=>{for(const id of [x,y,z])await f({op:'ensureStamp',model:'Task',identityKey:key(id)});});
  assert.deepEqual(at(await apply([member(a,x,{tags:['t1']}),member(a,y),member(b,z,{tags:['t1','t2']})])),[[a,x,1,'upsert'],[a,y,2,'upsert'],[b,z,1,'upsert']]);
  assert.deepEqual(at(await apply([member(a,x,{tags:['t2'],publish:false}),member(a,y,{present:false}),member(a,z),member(b,z,{tags:['t1','t2'],publish:false})])),
   [[a,x,1,'upsert'],[a,y,3,'remove'],[a,z,4,'upsert'],[b,z,1,'upsert']],'kept deltas answer their positions; published ones continue each range');
  assert.deepEqual(await q('SELECT channel,head::int AS head FROM axton_channel WHERE channel = ANY($1) ORDER BY channel',[[a,b]]),[{channel:a,head:4},{channel:b,head:1}]);
  const read=await inTx((tx,_,f)=>f({op:'readChannelMembers',channel:a,explicitKeys:[x,y,z].map(id=>({model:'Task',identityKey:key(id)})),tags:[]}));
  assert.deepEqual(read.map(r=>[JSON.parse(r.identityKey).id,r.tags]).sort(),[[x,['t2']],[z,[]]],'exactly the delta tags; the removed member is gone');
  assert.deepEqual(await q('SELECT cursor::int,kind FROM axton_channel_log WHERE channel=$1 ORDER BY cursor',[a]),[{cursor:1,kind:'upsert'},{cursor:3,kind:'remove'},{cursor:4,kind:'upsert'}]);
  assert.deepEqual(await q('SELECT name FROM axton_channel_tag WHERE channel=$1 ORDER BY name',[a]),[{name:'t2'}],'t1 lost its last member and was collected');
 });
 test(`[${shim.name}] a thrown body rolls back a first initialisation together with its publication`,async()=>{
  const id=p('undone');
  await assert.rejects(()=>inTx(async(tx,_,f)=>{await f({op:'ensureStamp',model:'Task',identityKey:key(id)});await f({op:'applyChannelMembers',deltas:[member(p('undone'),id,{tags:['t']})]});throw new Error('cancel');}),/cancel/);
  assert.deepEqual(await q('SELECT * FROM axton_record WHERE identity_key=$1',[key(id)]),[]);
  assert.deepEqual(await q('SELECT * FROM axton_channel_log WHERE channel=$1',[p('undone')]),[]);
  assert.deepEqual(await q('SELECT * FROM axton_channel_tag WHERE channel=$1',[p('undone')]),[]);
  assert.deepEqual(await q('SELECT * FROM axton_channel WHERE channel=$1',[p('undone')]),[]);
 });
 test(`[${shim.name}] scan retains removals before its limit and reads current stamps only for upserts`,async()=>{
  const channel=p('scan');const [id,gone,other]=[p('scan'),p('scan-gone'),p('scan-other')];
  await inTx(async(tx,_,f)=>{for(const m of [gone,other,id])await f({op:'ensureStamp',model:'Task',identityKey:key(m)});await f({op:'applyChannelMembers',deltas:[gone,other,id].map(m=>member(channel,m))});await f({op:'advanceStamp',model:'Task',identityKey:key(id)});});
  const scan=(after=0,limit=50)=>inTx((tx,_,f)=>f({op:'scan',channel,after,limit}));
  assert.deepEqual((await scan()).map(r=>r.identity.id),[gone,other,id]);
  await inTx((tx,_,f)=>f({op:'applyChannelMembers',deltas:[member(channel,gone,{present:false}),member(channel,other,{present:false})]}));
  assert.deepEqual(await scan(0,1),[{channel,kind:'upsert',cursor:3,model:'Task',identityKey:key(id),identity:{id},stamp:2}],'the first retained position fills the limit');
  assert.deepEqual(await scan(3,1),[{channel,kind:'remove',cursor:4,model:'Task',identityKey:key(gone),identity:{id:gone}}],'a removal fills a page without authority fields');
  assert.deepEqual((await q('SELECT cursor::int,kind FROM axton_channel_log WHERE channel=$1 ORDER BY cursor',[channel])).map(r=>[r.cursor,r.kind]),[[3,'upsert'],[4,'remove'],[5,'remove']],'one compacted row per pair');
  // The log's foreign key forbids dropping a record row; forge the defect with triggers off.
  await inTx(async(tx,query)=>{await query('SET LOCAL session_replication_role = replica');await query('DELETE FROM axton_record WHERE identity_key=$1',[key(id)]);});
  await assert.rejects(()=>scan(),/Record metadata missing/);
 });
 test(`[${shim.name}] savepoints isolate one mutation's writes and the transaction continues after a rollback`,async()=>{
  const id=p('sp');
  await inTx(async(tx,query,a)=>{
   await query('INSERT INTO conformance_task(id,title) VALUES($1,$2)',[id+'-kept','kept']);
   await a({op:'savepoint',ordinal:1});
   await query('INSERT INTO conformance_task(id,title) VALUES($1,$2)',[id+'-undone','undone']);
   await assert.rejects(()=>query('INSERT INTO conformance_task(id,title) VALUES($1,$2)',[id+'-undone','duplicate']));
   await a({op:'rollback',ordinal:1});
   await a({op:'release',ordinal:1});
   await query('INSERT INTO conformance_task(id,title) VALUES($1,$2)',[id+'-after','after']);
  });
  assert.deepEqual((await q('SELECT id FROM conformance_task WHERE id LIKE $1 ORDER BY id',[id+'-%'])).map(r=>r.id),[id+'-after',id+'-kept']);
  await assert.rejects(()=>inTx((tx,_,a)=>a({op:'savepoint',ordinal:0})),/Invalid savepoint ordinal/);
 });
 test(`[${shim.name}] a serialization conflict retries the whole body and commits once; with no retries it is reported`,async()=>{
  const channel=p('serial');
  await q("INSERT INTO axton_channel(channel,head) VALUES($1,0) ON CONFLICT(channel) DO UPDATE SET head=0",[channel]);
  let bodies=0;let entered,release;const inside=new Promise(r=>{entered=r;});const gate=new Promise(r=>{release=r;});
  const first=driver.transaction(async tx=>{bodies++;const [{head}]=await driver.query(tx,'SELECT head FROM axton_channel WHERE channel=$1',[channel]);if(bodies===1){entered();await gate;}await driver.query(tx,'UPDATE axton_channel SET head=head+1 WHERE channel=$1',[channel]);return Number(head);});
  await inside;await q('UPDATE axton_channel SET head=head+10 WHERE channel=$1',[channel]);release();
  assert.equal(await first,10);assert.equal(bodies,2);
  assert.equal(Number((await q('SELECT head FROM axton_channel WHERE channel=$1',[channel]))[0].head),11);
 });
 test(`[${shim.name}] Channel operations add, read and remove members idempotently, need record metadata and create no Channel before a position`,async()=>{
  const id=p('member');const rec={model:'Task',identityKey:key(id)};const tagged=p('member-tagged');
  const [a,b,c]=[p('member-a'),p('member-b'),p('member-c')];
  const apply=(...deltas)=>inTx((tx,_,x)=>x({op:'applyChannelMembers',deltas}));
  const read=(channel,ids,tags=[])=>inTx((tx,_,x)=>x({op:'readChannelMembers',channel,explicitKeys:ids.map(i=>({model:'Task',identityKey:key(i)})),tags})).then(rows=>rows.map(r=>[JSON.parse(r.identityKey).id,[...r.tags].sort()]).sort());
  const heads=async()=>Object.fromEntries((await q('SELECT channel,head::int AS head FROM axton_channel WHERE channel = ANY($1)',[[a,b,c]])).map(r=>[r.channel,r.head]));
  assert.equal(await inTx((tx,_,x)=>x({op:'lockRecord',...rec})),null,'an absent record locks nothing');
  assert.deepEqual(await inTx((tx,_,x)=>x({op:'memberships',...rec})),[]);
  assert.equal(await inTx((tx,_,x)=>x({op:'lockChannels',channels:[a,b]})),null,'locking absent Channels is a no-op');
  assert.deepEqual(await heads(),{},'and creates none');
  assert.deepEqual(await read(a,[id],['t']),[]);
  await assert.rejects(()=>apply(member(a,id)),/Record metadata missing/,'a member needs its record row');
  assert.deepEqual(await q('SELECT * FROM axton_record WHERE identity_key=$1',[rec.identityKey]),[],'neither lock nor membership creates the record row');
  assert.deepEqual(await heads(),{},'the refused enrolment rolled its channel row back');
  assert.equal(await inTx((tx,_,x)=>x({op:'ensureStamp',...rec})),1);
  assert.equal(await inTx((tx,_,x)=>x({op:'ensureStamp',model:'Task',identityKey:key(tagged)})),1);
  await apply(member(a,id),member(a,tagged,{tags:['t','u']}),member(b,id));
  assert.deepEqual(await inTx((tx,_,x)=>x({op:'memberships',...rec})),[a,b],'the answer is sorted and unique');
  assert.deepEqual(await heads(),{[a]:2,[b]:1},'a first member takes the first position of a new Channel');
  assert.equal(await inTx((tx,_,x)=>x({op:'lockChannels',channels:[a,b]})),null);
  assert.deepEqual(await read(a,[id,id,tagged],['t']),[[id,[]],[tagged,['t','u']]],'named and tagged, each once with its complete tags');
  assert.deepEqual(await read(a,[],['u']),[[tagged,['t','u']]]);
  assert.deepEqual(await apply(member(a,id,{publish:false})).then(r=>r.map(x=>x.cursor)),[1],'an unchanged member keeps its position');
  assert.deepEqual(await heads(),{[a]:2,[b]:1});
  await apply(member(b,id,{present:false}));
  assert.deepEqual(await inTx((tx,_,x)=>x({op:'memberships',...rec})),[a]);
  assert.deepEqual(await heads(),{[a]:2,[b]:2},'a removal takes a position; it never rewinds or deletes a head');
  assert.deepEqual(await read(b,[id]),[],'and removing it again finds nothing to remove');
  assert.equal(await inTx((tx,_,x)=>x({op:'lockRecord',...rec})),1,'the lock answers the current stamp');
  assert.deepEqual(await q('SELECT stamp::int AS stamp FROM axton_record WHERE identity_key=$1',[rec.identityKey]),[{stamp:1}],'and preserves it');
  assert.equal(await inTx((tx,_,x)=>x({op:'advanceStamp',...rec})),2);
  assert.equal(await inTx((tx,_,x)=>x({op:'lockRecord',...rec})),2);
 });
 test(`[${shim.name}] savepoint and transaction rollback restore members, tags, positions and heads`,async()=>{
  const id=p('member-undo');const rec={model:'Task',identityKey:key(id)};
  const [a,b,c]=[p('undo-a'),p('undo-b'),p('undo-c')];
  const apply=(x,...deltas)=>x({op:'applyChannelMembers',deltas});
  const members=x=>x({op:'memberships',...rec});
  const tables=async()=>({log:await q('SELECT channel,cursor::int,kind FROM axton_channel_log WHERE channel = ANY($1) ORDER BY channel',[[a,b,c]]),tags:await q('SELECT channel,name FROM axton_channel_tag WHERE channel = ANY($1) ORDER BY channel,name',[[a,b,c]]),heads:await q('SELECT channel,head::int FROM axton_channel WHERE channel = ANY($1) ORDER BY channel',[[a,b,c]])});
  await inTx(async(tx,_,x)=>{await x({op:'ensureStamp',...rec});await apply(x,member(a,id,{tags:['keep']}));});
  const before=await tables();
  await inTx(async(tx,_,x)=>{
   await x({op:'savepoint',ordinal:1});
   await apply(x,member(a,id,{present:false}),member(b,id,{tags:['new']}));
   assert.deepEqual(await members(x),[b]);
   await x({op:'rollback',ordinal:1});await x({op:'release',ordinal:1});
   assert.deepEqual(await members(x),[a],'the savepoint restored both edits');
  });
  await assert.rejects(()=>inTx(async(tx,_,x)=>{await apply(x,member(a,id,{present:false}),member(c,id));assert.deepEqual(await members(x),[c]);throw new Error('cancel');}),/cancel/);
  assert.deepEqual(await q('SELECT m.channel FROM axton_channel_member m JOIN axton_record r ON r.id=m.record_id WHERE r.identity_key=$1',[rec.identityKey]),[{channel:a}],'a rolled-back transaction restores the relationship it removed and drops the one it added');
  assert.deepEqual(await tables(),before,'tags, positions and heads as before; channels created only by rolled-back enrolments are gone');
  assert.deepEqual(await inTx((tx,_,x)=>members(x)),[a]);
 });
 test(`[${shim.name}] a membership-only writer's no-op record UPDATE makes a stale-snapshot writer of the same record retry`,async()=>{
  // B fixes its snapshot by reading the record's memberships, then waits. A
  // enrolls the record in a Channel and commits. B then writes the record row.
  // With A's lockRecord guard the write conflicts and the runner restarts B,
  // whose second attempt sees A's membership. Without the guard, Repeatable
  // Read let B commit on its stale view (the foreign key's KEY SHARE lock
  // only); at Serializable, the level every shim runs since #202, B's stale
  // membership read and A's key check of the row B writes form a cycle, so B
  // restarts either way. The guard is what still makes B restart when A is a
  // transaction AXTON does not open: a caller-owned transaction around
  // backend.publish at Repeatable Read, which takes no predicate locks. The
  // mixed-level trials below pin that; a guard weakened to SELECT … FOR UPDATE
  // or removed lets B commit on its stale view there.
  const trial=async(name,guard,writerA=inTx)=>{
   const rec={model:'Task',identityKey:key(p(name))};const channel=p(`${name}-ch`);
   await inTx((tx,_,x)=>x({op:'ensureStamp',...rec}));
   const seen=[];let entered,release;const inside=new Promise(r=>{entered=r;});const gate=new Promise(r=>{release=r;});
   const writerB=inTx(async(tx,_,x)=>{
    seen.push(await x({op:'memberships',...rec}));
    if(seen.length===1){entered();await gate;}
    return x({op:'advanceStamp',...rec});
   });
   await Promise.race([inside,writerB.then(()=>{throw new Error('B finished before its snapshot was held');})]);
   await writerA(async(tx,_,x)=>{if(guard)assert.equal(await x({op:'lockRecord',...rec}),1);await x({op:'applyChannelMembers',deltas:[{channel,...rec,identity:JSON.parse(rec.identityKey),present:true,tags:[],publish:true}]});});
   release();
   const stamp=await writerB;
   return {seen,stamp,channel,rec};
  };
  const guarded=await trial('rr-guarded',true);
  assert.deepEqual(guarded.seen,[[],[guarded.channel]],'B ran twice: its stale first attempt failed serialization, its retry read the new membership');
  assert.equal(guarded.stamp,2,'only the retried attempt advanced the stamp');
  assert.deepEqual(await q('SELECT stamp::int AS stamp FROM axton_record WHERE identity_key=$1',[guarded.rec.identityKey]),[{stamp:2}]);
  const unguarded=await trial('rr-unguarded',false);
  assert.deepEqual(unguarded.seen,[[],[unguarded.channel]],'at Serializable the stale writer restarts even without the no-op UPDATE');
  assert.equal(unguarded.stamp,2);
  // Mixed levels: A at Repeatable Read on a raw pg client (a caller-owned
  // transaction), B the shim's Serializable writer.
  const pool=new Pool({connectionString:url});const persistence=pgDriver(pool);
  const repeatableRead=async body=>{
   const client=await pool.connect();
   try{await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ');const result=await body(client,(sql,params=[])=>persistence.query(client,sql,params),r=>answer(persistence,client,r));await client.query('COMMIT');return result;}
   catch(error){await client.query('ROLLBACK');throw error;}
   finally{client.release();}
  };
  try{
   const mixedGuarded=await trial('rr-mixed-guarded',true,repeatableRead);
   assert.deepEqual(mixedGuarded.seen,[[],[mixedGuarded.channel]],'the guard\'s row write makes the Serializable writer fail serialization and retry');
   assert.equal(mixedGuarded.stamp,2);
   const mixedUnguarded=await trial('rr-mixed-unguarded',false,repeatableRead);
   assert.deepEqual(mixedUnguarded.seen,[[]],'control: without the guard a Repeatable Read enrolment leaves the Serializable writer committing on its stale view');
   assert.equal(mixedUnguarded.stamp,2);
  }finally{await pool.end();}
  // The same conflict when B's write reaches the row while A still holds it:
  // B waits on A's row lock, and A's commit makes B restart rather than proceed.
  const rec={model:'Task',identityKey:key(p('rr-blocked'))};const channel=p('rr-blocked-ch');
  await inTx((tx,_,x)=>x({op:'ensureStamp',...rec}));
  const seen=[];let pid,snapshot,locked,commitA;
  const bSnapshot=new Promise(r=>{snapshot=r;});const aLocked=new Promise(r=>{locked=r;});const aGate=new Promise(r=>{commitA=r;});
  const writerB=inTx(async(tx,query,x)=>{
   pid??=Number((await query('SELECT pg_backend_pid() AS pid'))[0].pid);
   seen.push(await x({op:'memberships',...rec}));
   if(seen.length===1){snapshot();await aLocked;}
   return x({op:'advanceStamp',...rec});
  });
  await bSnapshot;
  const writerA=inTx(async(tx,_,x)=>{assert.equal(await x({op:'lockRecord',...rec}),1);await x({op:'applyChannelMembers',deltas:[{channel,...rec,identity:JSON.parse(rec.identityKey),present:true,tags:[],publish:true}]});locked();await aGate;});
  let blocked=false;
  for(let attempt=0;attempt<200&&!blocked;attempt++){
   const rows=await q('SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1',[pid]);
   blocked=rows[0]?.wait_event_type==='Lock';
   if(!blocked)await new Promise(resolve=>setTimeout(resolve,10));
  }
  commitA();
  await writerA;
  assert.equal(await writerB,2);
  assert.equal(blocked,true,'B reached PostgreSQL and waited on the row A locked');
  assert.deepEqual(seen,[[],[channel]],'A\'s commit made B restart; the retry read the new membership');
 });
 test(`[${shim.name}] lock-then-recheck, delete case: an Archive for an Author who already left never commits; exactly one transaction retries`,async()=>{
  const {runs,authors,archives}=await archiveRace(driver.transaction,driver.query,p('leave'));
  assert.deepEqual(runs,{first:1,second:2},'the re-check that read the departed Author failed serialization and ran again');
  assert.deepEqual(authors,[],'the Author left');
  assert.deepEqual(archives,[],'the serial outcome: the retry saw the Author gone and wrote no Archive');
 });
 test(`[${shim.name}] lock-then-recheck, insert case: a delete's cleanup never misses a Star created at the same moment; exactly one transaction retries`,async()=>{
  const {runs,posts,stars}=await starRace(driver.transaction,driver.query,p('star'));
  assert.deepEqual(runs,{first:1,second:2},'the cleanup that missed the phantom Star failed serialization and ran again');
  assert.deepEqual(posts,[],'the Post is deleted');
  assert.deepEqual(stars,[],'the serial outcome: the retry saw the Star and deleted it with its Post');
 });
 test(`[${shim.name}] close`,async()=>{await shim.close();});
}

// Control: the same races at Repeatable Read, the level every shim used before
// #202, commit the stale decision. This is what the tests above rule out.
test('control: at Repeatable Read both lock-then-recheck races commit on a stale re-check',async()=>{
 const pool=new Pool({connectionString:url});
 const repeatableRead=async body=>{
  const client=await pool.connect();
  try{await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ');const result=await body(client);await client.query('COMMIT');return result;}
  catch(error){await client.query('ROLLBACK');throw error;}
  finally{client.release();}
 };
 const query=async(client,sql,params)=>(await client.query(sql,params)).rows;
 try{
  const leave=await archiveRace(repeatableRead,query,'rr-leave');
  assert.deepEqual(leave.runs,{first:1,second:1});
  assert.deepEqual([leave.authors,leave.archives],[[],[{author_id:'rr-leave-author'}]],'an Archive for an Author who already left');
  const star=await starRace(repeatableRead,query,'rr-star');
  assert.deepEqual(star.runs,{first:1,second:1});
  assert.deepEqual([star.posts,star.stars],[[],[{id:'rr-star-star'}]],'an orphan Star the cleanup missed');
 }finally{await pool.end();}
});

test('a retry waits a jittered, doubling, capped delay first; the last failure and a non-retryable one are thrown at once',async()=>{
 assert.deepEqual([RETRY_BACKOFF_BASE_MS,RETRY_BACKOFF_CAP_MS],[20,400]);
 assert.deepEqual([0,1,2,3,4,5,9].map(n=>retryDelay(n,()=>0.999999)),[19,39,79,159,319,399,399],'just below min(cap, base * 2^n)');
 assert.deepEqual([0,1,2].map(n=>retryDelay(n,()=>0)),[0,0,0],'full jitter reaches zero');
 for(let n=0;n<200;n++){const d=retryDelay(n%6);assert.ok(Number.isInteger(d)&&d>=0&&d<Math.min(400,20*2**(n%6)));}
 const events=[];const conflict=Object.assign(new Error('conflict'),{code:'40001'});
 const failing=failures=>async()=>{events.push('attempt');if(failures-->0)throw conflict;return 'done';};
 const delay=n=>{events.push(`wait ${n}`);return 1;};
 const retryable=error=>error?.code==='40001';
 assert.equal(await withRetries(failing(2),retryable,3,delay),'done');
 assert.deepEqual(events,['attempt','wait 0','attempt','wait 1','attempt']);
 events.length=0;
 await assert.rejects(()=>withRetries(failing(9),retryable,2,delay),error=>error===conflict);
 assert.deepEqual(events,['attempt','wait 0','attempt','wait 1','attempt'],'no wait after the last attempt');
 events.length=0;
 await assert.rejects(()=>withRetries(async()=>{events.push('attempt');throw new Error('other');},retryable,3,delay),/other/);
 assert.deepEqual(events,['attempt'],'a non-retryable failure is thrown without waiting');
});

test('the pg driver retries only serialization failures, a bounded number of times',async()=>{
 const attempts=[];const failing=codes=>({async connect(){return {async query(sql){if(sql==='COMMIT'){const code=codes.shift();if(code){attempts.push(code);throw Object.assign(new Error(code),{code});}}return {rows:[]};},release(){}};}});
 assert.equal(await pgDriver(failing(['40001','40P01'])).transaction(async()=>'body'),'body');assert.deepEqual(attempts,['40001','40P01']);
 await assert.rejects(()=>pgDriver(failing(['40001','40001','40001','40001'])).transaction(async()=>{}),error=>error.code==='40001');
 await assert.rejects(()=>pgDriver(failing(['23505'])).transaction(async()=>{}),error=>error.code==='23505');
 await assert.rejects(()=>pgDriver(failing(['40001','40001']),{retries:1}).transaction(async()=>{}),error=>error.code==='40001');
});
