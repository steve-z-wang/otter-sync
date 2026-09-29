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
import {pg,prisma,drizzle,answer,pgDriver,withRetries,retryDelay,RETRY_BACKOFF_BASE_MS,RETRY_BACKOFF_CAP_MS} from '../../../packages/postgres/index.mts';
import * as SQL from '../../../packages/postgres/src/sql.mts';
const require=createRequire(import.meta.url);
const {PrismaClient}=require('../../bindings/node/generated/client');
const url=process.env.DATABASE_URL;
const check=new Pool({connectionString:url});
const q=async(sql,params=[])=>(await check.query(sql,params)).rows;
const schema={enums:[],models:[{name:'Task',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false},{name:'title',type:{kind:'scalar',name:'string'},nullable:false}]}]};
const config={schema,mutations:[{name:'edit',version:1,slots:[{name:'task',model:'Task',operation:'update',cardinality:'single',allowedPatchFields:['title']}]}]};
const authenticate=async req=>req.headers.authorization==='Bearer alice'?'alice':null;
const key=id=>JSON.stringify({id});
let migration;
before(async()=>{migration=(await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8')).split(';').map(x=>x.trim()).filter(Boolean);for(const sql of migration)await q(sql);await q('CREATE TABLE IF NOT EXISTS conformance_task(id text PRIMARY KEY,title text NOT NULL)');
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

// Read footprint (#210). At Serializable only reads take SIREAD (predicate)
// locks, and on a near-empty table a primary key has one leaf page, so one
// point read covers every other key's insert. These are the SIREAD locks the
// transaction on backend `pid` holds on the named relations, as `locktype relname`.
const pidOf=async query=>Number((await query('SELECT pg_backend_pid() AS pid'))[0].pid);
const sireadOn=async(pid,relations)=>(await q("SELECT l.locktype,c.relname FROM pg_locks l JOIN pg_class c ON c.oid=l.relation WHERE l.mode='SIReadLock' AND l.pid=$1 AND c.relname=ANY($2) ORDER BY 1,2",[pid,relations])).map(r=>`${r.locktype} ${r.relname}`);

const shims=[];
{const pool=new Pool({connectionString:url});shims.push({name:'pg',database:pg(pool),close:()=>pool.end()});}
{const client=new PrismaClient();shims.push({name:'prisma',database:prisma(client),close:()=>client.$disconnect()});}
{const pool=new Pool({connectionString:url});shims.push({name:'drizzle',database:drizzle(drizzleOrm(pool)),close:()=>pool.end()});}

for(const shim of shims){
 const {database}=shim;const {driver}=database;
 const inTx=body=>driver.transaction(tx=>body(tx,(sql,params=[])=>driver.query(tx,sql,params),r=>answer(driver,tx,r)));
 const p=name=>`${shim.name}-${name}`;
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
 test(`[${shim.name}] a fresh call claim reads nothing from axton_call; a duplicate reads and locks the stored call`,async()=>{
  const id=p('claim-footprint'),call=['axton_call','axton_call_pkey'];
  const fresh=await inTx(async(tx,query,a)=>{
   const pid=await pidOf(query);
   const claimed=await a({op:'claimCall',owner:'alice',callId:id,request:'{"n":1}'});
   const locks=await sireadOn(pid,call);
   await a({op:'saveCall',owner:'alice',callId:id,response:'{"ok":1}'});
   return {claimed,locks};
  });
  assert.deepEqual(fresh,{claimed:{fresh:true,request:'{"n":1}',response:null},locks:[]},'the inserted row answers the claim; no SIREAD lock on the table or its key');
  const duplicate=await inTx(async(tx,query,a)=>{
   const pid=await pidOf(query);
   return {claimed:await a({op:'claimCall',owner:'alice',callId:id,request:'{"n":2}'}),locks:await sireadOn(pid,call)};
  });
  assert.deepEqual(duplicate.claimed,{fresh:false,request:'{"n":1}',response:'{"ok":1}'});
  assert.ok(duplicate.locks.length>0,'a duplicate reads the stored call');
 });
 test(`[${shim.name}] readStamps of new keys reads nothing from axton_record`,async()=>{
  const keys=[key(p('stamp-new-a')),key(p('stamp-new-b'))];
  const read=await inTx(async(tx,query,a)=>{
   const pid=await pidOf(query);
   return {stamps:await a({op:'readStamps',model:'Task',identityKeys:keys}),locks:await sireadOn(pid,['axton_record','axton_record_pkey'])};
  });
  assert.deepEqual(read,{stamps:[1,1],locks:[]});
 });
 test(`[${shim.name}] a push writes business rows and AXTON metadata in one transaction and a pull reads them back`,async()=>{
  const backend=createBackend({config,database,authenticate,handlers:{async edit({input,tx,channel}){await driver.query(tx,'INSERT INTO conformance_task(id,title) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET title=$2',[input.task.identity.id,input.task.patch.title]);channel(p('shared')).task.add(input.task.identity);}},loaders:{async task({ids,tx}){const rows=await driver.query(tx,'SELECT id,title FROM conformance_task WHERE id = ANY($1)',[ids.map(i=>i.id)]);return ids.map(i=>{const r=rows.find(r=>r.id===i.id);return r?{title:r.title}:null;});}}});
  const receipt=JSON.parse(await backend.push('alice',JSON.stringify({clientId:p('c'),batchSequence:1,models:{Task:1},mutations:[{ordinal:1,name:'edit',operations:[{model:'Task',op:'update',identity:{id:p('t')},values:{title:'typed'}}]}]})));
  assert.deepEqual(receipt.records,[{identity:{id:p('t')},model:'Task',stamp:1,state:{title:'typed'}}]);
  assert.deepEqual(await q('SELECT title FROM conformance_task WHERE id=$1',[p('t')]),[{title:'typed'}]);
  assert.equal(Number((await q('SELECT sequence FROM axton_client WHERE client_id=$1',[p('c')]))[0].sequence),1);
  const page=JSON.parse(await backend.pull('alice',JSON.stringify({cursors:{[p('shared')]:0},models:{Task:1}})));
  assert.equal(page.changes.length,1);assert.deepEqual(page.changes[0].state,{title:'typed'});assert.equal(page.changes[0].stamp,1);
  assert.equal(await backend.push('alice',JSON.stringify({clientId:p('c'),batchSequence:1,models:{Task:1},mutations:[{ordinal:1,name:'edit',operations:[]}]})),JSON.stringify(receipt),'a retry answers from the stored receipt');
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
  assert.deepEqual(await q('SELECT * FROM axton_invalidation WHERE identity_key=$1',[ref.identityKey]),[]);
  const race={model:'Task',identityKey:key(p('race'))};
  const ensure=()=>inTx((tx,_,a)=>a({op:'ensureStamp',...race}));
  assert.deepEqual(await Promise.all([ensure(),ensure(),ensure()]),[1,1,1],'concurrent first publications agree on 1 (serialization retries)');
  assert.equal((await q('SELECT * FROM axton_record WHERE identity_key=$1',[race.identityKey])).length,1);
  await inTx((tx,_,a)=>a({op:'advanceStamp',...race}));
  assert.equal(await ensure(),2);
 });
 test(`[${shim.name}] publish allocates only the channel cursor at the record's current stamp and refuses a missing or stale stamp`,async()=>{
  const id=p('pub');const ref={model:'Task',identity:{id},identityKey:key(id)};
  await assert.rejects(()=>inTx((tx,_,a)=>a({op:'publish',channel:p('ch'),...ref,stamp:1})),/Record metadata missing/);
  const published=await inTx(async(tx,_,a)=>{const stamp=await a({op:'ensureStamp',model:'Task',identityKey:ref.identityKey});return a({op:'publish',channel:p('ch'),...ref,stamp});});
  assert.deepEqual(published,{cursor:1,stamp:1});
  await assert.rejects(()=>inTx((tx,_,a)=>a({op:'publish',channel:p('ch'),...ref,stamp:5})),/names stamp 5 .* is at stamp 1/);
  assert.deepEqual(await q('SELECT channel,cursor::int AS cursor,stamp::int AS stamp,identity FROM axton_invalidation WHERE identity_key=$1',[ref.identityKey]),[{channel:p('ch'),cursor:1,stamp:1,identity:{id}}]);
  assert.equal(await inTx((tx,_,a)=>a({op:'head',channel:p('ch')})),1);
 });
 test(`[${shim.name}] a thrown body rolls back a first initialisation together with its publication`,async()=>{
  const id=p('undone');
  await assert.rejects(()=>inTx(async(tx,_,a)=>{const stamp=await a({op:'ensureStamp',model:'Task',identityKey:key(id)});await a({op:'publish',channel:p('undone'),model:'Task',identity:{id},identityKey:key(id),stamp});throw new Error('cancel');}),/cancel/);
  assert.deepEqual(await q('SELECT * FROM axton_record WHERE identity_key=$1',[key(id)]),[]);
  assert.deepEqual(await q('SELECT * FROM axton_invalidation WHERE identity_key=$1',[key(id)]),[]);
  assert.deepEqual(await q('SELECT * FROM axton_channel WHERE channel=$1',[p('undone')]),[]);
 });
 test(`[${shim.name}] scan answers members only before its limit, pairs the invalidation cursor with the current record stamp and reports missing metadata`,async()=>{
  const channel=p('scan');const [id,gone,other]=[p('scan'),p('scan-gone'),p('scan-other')];
  const set=(member,present)=>({op:'setMembership',channel,model:'Task',identityKey:key(member),present});
  // Enrolled first: a scan answers members only.
  await inTx(async(tx,_,a)=>{for(const member of [gone,other,id]){const stamp=await a({op:'ensureStamp',model:'Task',identityKey:key(member)});await a(set(member,true));await a({op:'publish',channel,model:'Task',identity:{id:member},identityKey:key(member),stamp});}await a({op:'advanceStamp',model:'Task',identityKey:key(id)});});
  const scan=(after=0,limit=50)=>inTx((tx,_,a)=>a({op:'scan',channel,after,limit}));
  assert.deepEqual((await scan()).map(r=>r.identity.id),[gone,other,id]);
  await inTx((tx,_,a)=>a(set(gone,false)));
  await inTx((tx,_,a)=>a(set(other,false)));
  assert.deepEqual(await scan(0,1),[{channel,cursor:3,model:'Task',identityKey:key(id),identity:{id},stamp:2}],'removed rows are filtered before the limit');
  assert.deepEqual((await q('SELECT cursor::int FROM axton_invalidation WHERE channel=$1 ORDER BY cursor',[channel])).map(r=>r.cursor),[1,2,3],'and retained');
  // The membership foreign key forbids dropping a member's record row; forge the defect with triggers off.
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
 test(`[${shim.name}] membership adds, lists and removes idempotently, needs record metadata and never allocates a cursor`,async()=>{
  const id=p('member');const rec={model:'Task',identityKey:key(id)};
  const [a,b,c]=[p('member-a'),p('member-b'),p('member-c')];
  const set=(channel,present)=>({op:'setMembership',channel,...rec,present});
  const heads=async()=>Object.fromEntries((await q('SELECT channel,head::int AS head FROM axton_channel WHERE channel = ANY($1)',[[a,b,c]])).map(r=>[r.channel,r.head]));
  assert.equal(await inTx((tx,_,x)=>x({op:'lockRecord',...rec})),null,'an absent record locks nothing');
  assert.deepEqual(await inTx((tx,_,x)=>x({op:'memberships',...rec})),[]);
  assert.equal(await inTx((tx,_,x)=>x(set(a,false))),null,'removing a non-member of an absent record is a no-op');
  // Each tool reports the violation its own way (drizzle wraps it as the cause).
  const foreignKey=error=>{for(let e=error;e;e=e.cause)if(/foreign key/.test(e.message))return true;return false;};
  await assert.rejects(()=>inTx((tx,_,x)=>x(set(a,true))),foreignKey,'a member needs its record row');
  assert.deepEqual(await q('SELECT * FROM axton_record WHERE identity_key=$1',[rec.identityKey]),[],'neither lock nor membership creates the record row');
  assert.deepEqual(await heads(),{},'the refused enrolment rolled its channel row back');
  assert.equal(await inTx((tx,_,x)=>x({op:'ensureStamp',...rec})),1);
  assert.deepEqual(await inTx(async(tx,_,x)=>{for(const ch of [b,a,a])assert.equal(await x(set(ch,true)),null);return x({op:'memberships',...rec});}),[a,b],'duplicate insert is idempotent; the answer is sorted and unique');
  assert.deepEqual(await heads(),{[a]:0,[b]:0},'enrolment creates the channel at head zero');
  assert.deepEqual(await inTx((tx,_,x)=>x({op:'memberships',...rec})),[a,b],'membership survives into a new transaction');
  assert.deepEqual(await inTx(async(tx,_,x)=>{await x(set(b,false));await x(set(b,false));await x(set(c,false));return x({op:'memberships',...rec});}),[a],'duplicate delete and removing a non-member are no-ops');
  assert.deepEqual(await q('SELECT channel FROM axton_membership WHERE identity_key=$1',[rec.identityKey]),[{channel:a}]);
  assert.deepEqual(await heads(),{[a]:0,[b]:0},'removal neither increments nor deletes a channel head');
  assert.equal(await inTx((tx,_,x)=>x({op:'lockRecord',...rec})),1,'the lock answers the current stamp');
  assert.deepEqual(await q('SELECT stamp::int AS stamp FROM axton_record WHERE identity_key=$1',[rec.identityKey]),[{stamp:1}],'and preserves it');
  assert.equal(await inTx((tx,_,x)=>x({op:'advanceStamp',...rec})),2);
  assert.equal(await inTx((tx,_,x)=>x({op:'lockRecord',...rec})),2);
  assert.deepEqual(await q('SELECT * FROM axton_invalidation WHERE identity_key=$1',[rec.identityKey]),[],'membership alone publishes nothing');
  assert.deepEqual(await inTx((tx,_,x)=>x({op:'publish',channel:a,model:'Task',identity:{id},identityKey:rec.identityKey,stamp:2})),{cursor:1,stamp:2},'publication allocates the first real position');
  await inTx(async(tx,_,x)=>{await x(set(a,true));await x(set(a,false));await x(set(a,true));});
  assert.deepEqual(await heads(),{[a]:1,[b]:0},'re-enrolment never resets or advances a published head');
  assert.deepEqual(await inTx((tx,_,x)=>x({op:'memberships',...rec})),[a]);
 });
 test(`[${shim.name}] savepoint and transaction rollback restore membership relationships`,async()=>{
  const id=p('member-undo');const rec={model:'Task',identityKey:key(id)};
  const [a,b,c]=[p('undo-a'),p('undo-b'),p('undo-c')];
  const set=(channel,present)=>({op:'setMembership',channel,...rec,present});
  const members=x=>x({op:'memberships',...rec});
  await inTx(async(tx,_,x)=>{await x({op:'ensureStamp',...rec});await x(set(a,true));});
  await inTx(async(tx,_,x)=>{
   await x({op:'savepoint',ordinal:1});
   await x(set(b,true));await x(set(a,false));
   assert.deepEqual(await members(x),[b]);
   await x({op:'rollback',ordinal:1});await x({op:'release',ordinal:1});
   assert.deepEqual(await members(x),[a],'the savepoint restored both edits');
  });
  await assert.rejects(()=>inTx(async(tx,_,x)=>{await x(set(a,false));await x(set(c,true));assert.deepEqual(await members(x),[c]);throw new Error('cancel');}),/cancel/);
  assert.deepEqual(await q('SELECT channel FROM axton_membership WHERE identity_key=$1',[rec.identityKey]),[{channel:a}],'a rolled-back transaction restores the relationship it removed and drops the one it added');
  assert.deepEqual(await q('SELECT channel FROM axton_channel WHERE channel = ANY($1)',[[b,c]]),[],'channels created only by rolled-back enrolments are gone');
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
   await writerA(async(tx,_,x)=>{if(guard)assert.equal(await x({op:'lockRecord',...rec}),1);await x({op:'setMembership',channel,...rec,present:true});});
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
  const writerA=inTx(async(tx,_,x)=>{assert.equal(await x({op:'lockRecord',...rec}),1);await x({op:'setMembership',channel,...rec,present:true});locked();await aGate;});
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

// Near-empty framework tables that were never analyzed, in a schema of their
// own, so the suite's other rows and autovacuum's statistics stay out.
const nearEmpty=async schema=>{
 await q(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);await q(`CREATE SCHEMA ${schema}`);
 const pool=new Pool({connectionString:url,options:`-c search_path=${schema}`});
 for(const sql of migration)await pool.query(sql);
 return {pool,driver:pgDriver(pool),q:async(sql,params=[])=>(await pool.query(sql,params)).rows,close:async()=>{await pool.end();await q(`DROP SCHEMA ${schema} CASCADE`);}};
};

test('[pg] on near-empty tables readStamps reads an existing stamp by its primary key, never the whole model or a heap page',async()=>{
 const t=await nearEmpty('axton_near_empty_stamps');
 try{
  await t.q("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Task',$1,3),('Task',$2,1),('Task',$3,1)",[key('e1'),key('e2'),key('other')]);
  const xmins=()=>t.q("SELECT identity_key,xmin::text FROM axton_record WHERE model='Task' ORDER BY identity_key");
  const before=await xmins();
  const read=await t.driver.transaction(async tx=>{
   const pid=await pidOf(async sql=>(await tx.query(sql)).rows);
   return {stamps:await answer(t.driver,tx,{op:'readStamps',model:'Task',identityKeys:[key('n1'),key('e1'),key('e2')]}),locks:await sireadOn(pid,['axton_record','axton_record_pkey'])};
  });
  assert.deepEqual(read.stamps,[1,3,1],'request order; the missing key inserted at 1');
  assert.deepEqual(read.locks.filter(lock=>lock==='relation axton_record'||lock==='page axton_record'),[],`only tuples and key pages are read: ${read.locks}`);
  assert.ok(read.locks.includes('tuple axton_record'),'the existing rows are read');
  assert.deepEqual((await xmins()).filter(row=>row.identity_key!==key('n1')),before,'existing rows are not rewritten');
 }finally{await t.close();}
});

test('[pg] readStamps still fails serialization for a key inserted or re-stamped after its snapshot',async()=>{
 const pool=new Pool({connectionString:url});const driver=pgDriver(pool);
 const existing=key('late-existing');
 await q("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Task',$1,1)",[existing]);
 const late=async(write,identityKey)=>{
  const client=await pool.connect();
  try{
   await client.query('BEGIN ISOLATION LEVEL SERIALIZABLE');await client.query('SELECT 1');
   await write();
   await assert.rejects(()=>answer(driver,client,{op:'readStamps',model:'Task',identityKeys:[identityKey]}),error=>error.code==='40001');
  }finally{await client.query('ROLLBACK');client.release();}
 };
 try{
  await late(()=>q(SQL.ENSURE_STAMP,['Task',key('late-new')]),key('late-new'));
  await late(()=>q(SQL.ADVANCE_STAMP,['Task',existing]),existing);
 }finally{await pool.end();}
});

test('[pg] two disjoint deliveries on near-empty tables both commit on their first attempt, in several interleavings of their steps',async()=>{
 const t=await nearEmpty('axton_near_empty_deliveries');
 const steps=(client,id)=>[
  ()=>answer(t.driver,client,{op:'claimCall',owner:'alice',callId:id,request:'{}'}),
  ()=>answer(t.driver,client,{op:'readStamps',model:'Task',identityKeys:[key(`${id}-a`),key(`${id}-b`)]}),
  ()=>answer(t.driver,client,{op:'saveCall',owner:'alice',callId:id,response:'{}'}),
  ()=>client.query('COMMIT'),
 ];
 try{
  // Each of these orders aborted one side with 40001 while claimCall re-read its fresh row and READ_STAMPS joined the whole model.
  for(const order of ['ABABABAB','AABBAABB','AABBBBAA','ABBBBAAA','BAABBAAB']){
   const clients={A:await t.pool.connect(),B:await t.pool.connect()};
   try{
    for(const client of Object.values(clients))await client.query('BEGIN ISOLATION LEVEL SERIALIZABLE');
    const run={A:steps(clients.A,`${order}-A`),B:steps(clients.B,`${order}-B`)},next={A:0,B:0};
    for(const side of order){
     const step=next[side]++;
     await run[side][step]().catch(error=>{throw new Error(`${order}: ${side} step ${step} failed: ${error.code} ${error.message}`);});
    }
   }finally{for(const client of Object.values(clients)){await client.query('ROLLBACK').catch(()=>{});client.release();}}
  }
  assert.equal((await t.q("SELECT count(*)::int AS n FROM axton_call WHERE response='{}'"))[0].n,10,'every delivery saved its call');
 }finally{await t.close();}
});

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
