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
import {pg,prisma,drizzle,answer,pgDriver} from '../../../packages/postgres/index.mts';
const require=createRequire(import.meta.url);
const {PrismaClient}=require('../../bindings/node/generated/client');
const url=process.env.DATABASE_URL;
const check=new Pool({connectionString:url});
const q=async(sql,params=[])=>(await check.query(sql,params)).rows;
const schema={enums:[],models:[{name:'Task',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false},{name:'title',type:{kind:'scalar',name:'string'},nullable:false}]}]};
const config={schema,mutations:[{name:'edit',version:1,slots:[{name:'task',model:'Task',operation:'update',cardinality:'single',allowedPatchFields:['title']}]}]};
const authenticate=async req=>req.headers.authorization==='Bearer alice'?'alice':null;
const key=id=>JSON.stringify({id});
before(async()=>{for(const sql of (await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8')).split(';').map(x=>x.trim()).filter(Boolean))await q(sql);await q('CREATE TABLE IF NOT EXISTS conformance_task(id text PRIMARY KEY,title text NOT NULL)');});
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
  // B fixes its Repeatable Read snapshot by reading the record's memberships,
  // then waits. A enrolls the record in a Channel and commits. B then writes
  // the record row. With A's lockRecord guard the write conflicts and the
  // runner restarts B, whose second attempt sees A's membership; without the
  // guard (the foreign key's KEY SHARE lock only) B commits on its stale view.
  const trial=async(name,guard)=>{
   const rec={model:'Task',identityKey:key(p(name))};const channel=p(`${name}-ch`);
   await inTx((tx,_,x)=>x({op:'ensureStamp',...rec}));
   const seen=[];let entered,release;const inside=new Promise(r=>{entered=r;});const gate=new Promise(r=>{release=r;});
   const writerB=inTx(async(tx,_,x)=>{
    seen.push(await x({op:'memberships',...rec}));
    if(seen.length===1){entered();await gate;}
    return x({op:'advanceStamp',...rec});
   });
   await Promise.race([inside,writerB.then(()=>{throw new Error('B finished before its snapshot was held');})]);
   await inTx(async(tx,_,x)=>{if(guard)assert.equal(await x({op:'lockRecord',...rec}),1);await x({op:'setMembership',channel,...rec,present:true});});
   release();
   const stamp=await writerB;
   return {seen,stamp,channel,rec};
  };
  const guarded=await trial('rr-guarded',true);
  assert.deepEqual(guarded.seen,[[],[guarded.channel]],'B ran twice: its stale first attempt failed serialization, its retry read the new membership');
  assert.equal(guarded.stamp,2,'only the retried attempt advanced the stamp');
  assert.deepEqual(await q('SELECT stamp::int AS stamp FROM axton_record WHERE identity_key=$1',[guarded.rec.identityKey]),[{stamp:2}]);
  const unguarded=await trial('rr-unguarded',false);
  assert.deepEqual(unguarded.seen,[[]],'control: without the no-op UPDATE the stale writer commits once, never seeing the new membership');
  assert.equal(unguarded.stamp,2);
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
 test(`[${shim.name}] close`,async()=>{await shim.close();});
}

test('the pg driver retries only serialization failures, a bounded number of times',async()=>{
 const attempts=[];const failing=codes=>({async connect(){return {async query(sql){if(sql==='COMMIT'){const code=codes.shift();if(code){attempts.push(code);throw Object.assign(new Error(code),{code});}}return {rows:[]};},release(){}};}});
 assert.equal(await pgDriver(failing(['40001','40P01'])).transaction(async()=>'body'),'body');assert.deepEqual(attempts,['40001','40P01']);
 await assert.rejects(()=>pgDriver(failing(['40001','40001','40001','40001'])).transaction(async()=>{}),error=>error.code==='40001');
 await assert.rejects(()=>pgDriver(failing(['23505'])).transaction(async()=>{}),error=>error.code==='23505');
 await assert.rejects(()=>pgDriver(failing(['40001','40001']),{retries:1}).transaction(async()=>{}),error=>error.code==='40001');
});
