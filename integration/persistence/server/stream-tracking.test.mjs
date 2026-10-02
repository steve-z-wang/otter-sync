import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {Pool,Client} from 'pg';
import {answer,pg,prisma,persistence} from '../../../packages/postgres/index.mts';
import {createRequire} from 'node:module';
import {createBackend} from '../../../packages/server/index.mts';
import {drizzle} from '../../../packages/postgres/src/drizzle.mts';
import {drizzle as drizzleOrm} from 'drizzle-orm/node-postgres';
const require=createRequire(import.meta.url);
const native=require('../../../bindings/node/axton-node.node');
import * as SQL from '../../../packages/postgres/src/sql.mts';
const pool=new Pool({connectionString:process.env.DATABASE_URL});
const {driver}=pg(pool);
const q=async(sql,params=[])=>(await pool.query(sql,params)).rows;
const source=name=>readFile(new URL(`../../../packages/postgres/${name}`,import.meta.url),'utf8');
const fixture=name=>readFile(new URL(`fixtures/${name}`,import.meta.url),'utf8');
const key=id=>JSON.stringify({id});
const record=(model,id,mode='ensure')=>({model,identityKey:key(id),mode});
const adapter=(body,observe=()=>{},observeHost=()=>{})=>driver.transaction(tx=>body(r=>{observeHost(r.op);return answer({query:(tx,sql,p)=>{observe(sql,p);return driver.query(tx,sql,p);}},tx,r);},(sql,p=[])=>driver.query(tx,sql,p)));
const pairs=(records,streams)=>streams.flatMap(stream=>records.map(({model,identityKey})=>({stream,model,identityKey,identity:JSON.parse(identityKey),publish:true})));
before(async()=>{await q(await source('migration.sql'));await q('CREATE TABLE IF NOT EXISTS stream_business(id text PRIMARY KEY)');});
after(()=>pool.end());
test('mixed guard ensures, locks and advances in canonical request order; absent locks stay absent',async()=>{
 const records=[record('Mixed','a'),record('Mixed','b','lock'),record('Mixed','c','advance'),record('Mixed','d','advance'),record('Mixed','e','lock')];
 await adapter(async call=>{await call({op:'guardRecords',records:[record('Mixed','b'),record('Mixed','d')]});});
 const got=await adapter(call=>call({op:'guardRecords',records}));
 assert.deepEqual(got,[1,1,1,2,null]);
 assert.equal((await q("SELECT count(*)::int n FROM axton_record WHERE model='Mixed'")).at(0).n,4);
 let writes=0;
 await assert.rejects(()=>adapter(call=>call({op:'guardRecords',records:[record('Mixed','a'),record('Mixed','a')]}),()=>writes++),/repeats/);
 assert.equal(writes,0);
});
for(const [name,nRecords,nStreams] of [['records',2001,3],['streams',2,1001]])test(`bulk ${name}: independent guard, pair and head chunks`,async()=>{
 const records=Array.from({length:nRecords},(_,i)=>record(i<Math.ceil(nRecords/2)?'BulkA':'BulkB',`${name}-${String(i).padStart(5,'0')}`));
 const streams=Array.from({length:nStreams},(_,i)=>`${name}:${String(i).padStart(5,'0')}`);
 const deltas=pairs(records,streams);const seen=new Map();const host=[];
 await adapter(async call=>{
  await call({op:'lockStreams',streams});
  assert.equal((await call({op:'guardRecords',records})).length,nRecords);
  assert.equal((await call({op:'applyStreamMembers',deltas})).length,deltas.length);
  assert.equal((await call({op:'readTracking',records:records.map(({mode,...r})=>r),pairs:deltas.map(({identity,publish,...p})=>p)})).length,deltas.length);
 },sql=>seen.set(sql,(seen.get(sql)??0)+1),op=>host.push(op));
 assert.deepEqual(host,['lockStreams','guardRecords','applyStreamMembers','readTracking']);
 assert.equal(seen.get(SQL.GUARD_RECORDS),Math.ceil(nRecords/1000));
 assert.equal(seen.get(SQL.LOCK_STREAMS),Math.ceil(nStreams/1000));
 assert.equal(seen.get(SQL.RESERVE_HEADS),Math.ceil(nStreams/1000));
 assert.equal(seen.get(SQL.WRITE_STREAM_LOG),Math.ceil(deltas.length/1000));
 assert.equal(seen.get(SQL.INSERT_STREAM_MEMBERS),Math.ceil(deltas.length/1000));
 assert.equal(seen.get(SQL.READ_TRACKING),Math.ceil(deltas.length/1000));
 const kept=await adapter(call=>call({op:'applyStreamMembers',deltas:deltas.map(d=>({...d,publish:false}))}));
 assert.deepEqual(kept.map(p=>p.cursor),deltas.map((_,i)=>i%nRecords+1));
});
test('bulk readTracking includes disjoint explicit-only pairs and deduplicates cross-chunk overlap',async()=>{
 const keys=['all','explicit','unrequested'].map(id=>record('ReadBranches',id));
 await adapter(async call=>{
  await call({op:'guardRecords',records:keys});
  await call({op:'applyStreamMembers',deltas:pairs(keys,['read:holders'])});
 });
 const candidate=r=>({model:r.model,identityKey:r.identityKey});
 const records=[candidate(keys[0]),...Array.from({length:1000},(_,i)=>candidate(record('ReadAbsent',String(i))))];
 const requested=[{...candidate(keys[1]),stream:'read:holders'},...Array.from({length:999},(_,i)=>({...candidate(record('ReadAbsent',String(i))),stream:'read:missing'})),{...candidate(keys[0]),stream:'read:holders'},{...candidate(keys[2]),stream:'read:missing'}];
 let reads=0;
 const result=await adapter(call=>call({op:'readTracking',records,pairs:requested}),sql=>{if(sql===SQL.READ_TRACKING)reads++;});
 assert.equal(reads,2);
 assert.deepEqual(result.sort((a,b)=>a.identityKey.localeCompare(b.identityKey)),keys.slice(0,2).map(r=>({...candidate(r),stream:'read:holders'})));
});
test('bulk chunk 2 failure rolls back business, guards, pairs, heads, logs and call claims',async()=>{
 const records=Array.from({length:1001},(_,i)=>record('Rollback',String(i).padStart(5,'0')));let count=0;
 await assert.rejects(()=>adapter(async(call,sql)=>{
  await sql("INSERT INTO stream_business VALUES('rollback')");
  await call({op:'claimCall',owner:'rollback',callId:'rollback',request:'{}'});
  await call({op:'guardRecords',records});
  await call({op:'applyStreamMembers',deltas:pairs(records,['rollback'])});
 },sql=>{if(sql===SQL.WRITE_STREAM_LOG&&++count===2)throw Error('chunk 2');}),/chunk 2/);
 for(const table of ['axton_stream','axton_stream_log','axton_stream_member'])assert.equal((await q(`SELECT count(*)::int n FROM ${table} WHERE stream='rollback'`))[0].n,0);
 assert.equal((await q("SELECT count(*)::int n FROM axton_record WHERE model='Rollback'"))[0].n,0);
 assert.equal((await q("SELECT count(*)::int n FROM axton_call WHERE owner_id='rollback'"))[0].n,0);
 assert.equal((await q("SELECT count(*)::int n FROM stream_business WHERE id='rollback'"))[0].n,0);
});
const latch=()=>{let resolve;return {promise:new Promise(r=>resolve=r),resolve:()=>resolve()};};
test('mixed guard ordered INSERT blocks at low key before acquiring later existing keys',async()=>{
 const holder=await pool.connect(),writer=await pool.connect(),observer=await pool.connect();
 try{
  await adapter(call=>call({op:'guardRecords',records:[record('Order','a'),record('Order','z')]}));
  await holder.query('BEGIN');await holder.query(SQL.LOCK_RECORD,['Order',key('a')]);
  await writer.query('BEGIN');const pid=(await writer.query('SELECT pg_backend_pid() pid')).rows[0].pid;
  const running=writer.query(SQL.GUARD_RECORDS,[JSON.stringify([record('Order','a','ensure'),record('Order','m','advance'),record('Order','z','lock')])]);
  let blocked=false;for(let n=0;n<100;n++){blocked=(await observer.query('SELECT cardinality(pg_blocking_pids($1))>0 blocked',[pid])).rows[0].blocked;if(blocked)break;await new Promise(r=>setTimeout(r,10));}
  assert.ok(blocked,'writer waits on low existing key');
  await observer.query('BEGIN');await observer.query("SET LOCAL lock_timeout='200ms'");
  await observer.query(SQL.LOCK_RECORD,['Order',key('z')]);await observer.query('ROLLBACK');
  await holder.query('COMMIT');assert.deepEqual((await running).rows.map(r=>Number(r.stamp)),[1,1,1]);await writer.query('COMMIT');
 }finally{for(const c of [holder,writer,observer]){await c.query('ROLLBACK');c.release();}}
});
test('mixed guards across chunks retry stale snapshots and two first creates as whole transactions',async()=>{
 const records=Array.from({length:1002},(_,i)=>record('Race',String(i).padStart(5,'0'),i%3===0?'ensure':i%3===1?'advance':'lock'));
 await adapter(call=>call({op:'guardRecords',records:records.filter((_,i)=>i%2===0).map(r=>({...r,mode:'ensure'}))}));
 const fixed=latch(),gate=latch();let attempts=0;
 const stale=adapter(async(call,sql)=>{attempts++;await sql("SELECT count(*) FROM axton_record WHERE model='Race'");if(attempts===1){fixed.resolve();await gate.promise;}return call({op:'guardRecords',records});});
 await fixed.promise;
 await adapter(call=>call({op:'guardRecords',records:records.map(r=>({...r,mode:'advance'}))}));gate.resolve();await stale;
 assert.ok(attempts>1,'runner retries whole stale transaction');
 const first=[record('First','a','advance'),record('First','b','ensure')];
 await Promise.all([adapter(call=>call({op:'guardRecords',records:first})),adapter(call=>call({op:'guardRecords',records:first}))]);
 assert.deepEqual((await q("SELECT stamp::int stamp FROM axton_record WHERE model='First' ORDER BY identity_key")).map(r=>r.stamp),[2,1]);
});
// Replace only an existing ensure's no-op write for the negative control. All
// enrollment/member writes and the stale invalidator still use the real adapter.
const readOnlyEnsure=`SELECT (v->>'ordinal')::bigint ord,r.stamp
 FROM jsonb_array_elements($1::jsonb) v
 JOIN axton_record r ON r.model=v->>'model' AND r.identity_key=v->>'identityKey'
 ORDER BY ord FOR UPDATE OF r`;
for(const weakened of [false,true])test(`Repeatable Read enrollment ${weakened?'weakened guard control misses the new holder':'no-op fence retries the entire stale invalidation and refreshes holders'}`,{timeout:10000},async()=>{
 const model=weakened?'FenceControl':'FenceProtected';
 const records=[record(model,'entry')];const oldStream=`${model}:old`,newStream=`${model}:new`;
 await adapter(async call=>{
  await call({op:'guardRecords',records});
  await call({op:'applyStreamMembers',deltas:pairs(records,[oldStream])});
 });
 const snapshot=latch(),enrolled=latch();let attempts=0;const failures=[],holders=[];
 const transaction=async(body,isolation,weak=false)=>{
  for(let attempt=0;attempt<3;attempt++){
   const tx=await pool.connect();
   try{
    await tx.query(`BEGIN ISOLATION LEVEL ${isolation}`);
    await tx.query("SET LOCAL statement_timeout='3000ms'");
    const call=r=>answer({query:async(_,sql,params)=>{
     if(weak&&sql===SQL.GUARD_RECORDS){assert.ok(JSON.parse(params[0]).every(r=>r.mode==='ensure'));sql=readOnlyEnsure;}
     return (await tx.query(sql,params)).rows;
    }},tx,r);
    const value=await body(call,tx);await tx.query('COMMIT');return value;
   }catch(error){await tx.query('ROLLBACK');if(error.code!=='40001'||attempt===2)throw error;failures.push(error.code);}
   finally{tx.release();}
  }
 };
 const stale=transaction(async(call,tx)=>{
  attempts++;
  assert.equal((await tx.query('SHOW transaction_isolation')).rows[0].transaction_isolation,'serializable');
  // Fix authority and holder snapshots before enrollment commits.
  assert.equal((await tx.query('SELECT stamp::int FROM axton_record WHERE model=$1',[model])).rows[0].stamp,1);
  const before=await call({op:'readTracking',records:records.map(({mode,...r})=>r),pairs:[]});
  holders.push(before.map(r=>r.stream).sort());
  await tx.query('INSERT INTO stream_business VALUES($1)',[model]);
  if(attempts===1){snapshot.resolve();await enrolled.promise;}
  assert.deepEqual(await call({op:'guardRecords',records:records.map(r=>({...r,mode:'advance'}))}),[2]);
  const refreshed=await call({op:'readTracking',records:records.map(({mode,...r})=>r),pairs:[]});
  await call({op:'applyStreamMembers',deltas:pairs(records,refreshed.map(r=>r.stream))});
 },'SERIALIZABLE');
 try{
  await snapshot.promise;
  await transaction(async(call,tx)=>{
   assert.equal((await tx.query('SHOW transaction_isolation')).rows[0].transaction_isolation,'repeatable read');
   const version=async()=>(await tx.query('SELECT xmin::text version FROM axton_record WHERE model=$1',[model])).rows[0].version;
   const before=await version();
   assert.deepEqual(await call({op:'guardRecords',records}),[1]);
   assert.equal((await version())===before,weakened,'only the actual no-op write replaces the catalog row version');
   await call({op:'applyStreamMembers',deltas:pairs(records,[newStream])});
  },'REPEATABLE READ',weakened);
 }finally{enrolled.resolve();}
 await stale;
 assert.equal(attempts,weakened?1:2);
 assert.deepEqual(failures,weakened?[]:['40001']);
 assert.deepEqual(holders,weakened?[[oldStream]]:[[oldStream],[newStream,oldStream].sort()]);
 const heads=await q('SELECT stream,head::int head FROM axton_stream WHERE stream=ANY($1) ORDER BY stream',[[oldStream,newStream]]);
 assert.deepEqual(heads,[{stream:newStream,head:weakened?1:2},{stream:oldStream,head:2}].sort((a,b)=>a.stream.localeCompare(b.stream)));
 assert.equal((await q('SELECT stamp::int stamp FROM axton_record WHERE model=$1',[model]))[0].stamp,2);
 assert.equal((await q('SELECT count(*)::int n FROM stream_business WHERE id=$1',[model]))[0].n,1,'failed attempt business write rolled back before the entire body reran');
});
const databaseUrl=name=>{const u=new URL(process.env.DATABASE_URL);u.pathname='/'+name;return u.toString();};
const scratch=async name=>{await q(`DROP DATABASE IF EXISTS ${name}`);await q(`CREATE DATABASE ${name}`);const u=new URL(process.env.DATABASE_URL);u.pathname='/'+name;const c=new Client({connectionString:u.toString()});await c.connect();return c;};
const upgrade=async c=>{try{await c.query(await source('migrations/2026-10-01-streams.sql'));}catch(e){await c.query('ROLLBACK');throw e;}};
const sortRows=(a,b)=>JSON.stringify(Object.entries(a.row).sort()).localeCompare(JSON.stringify(Object.entries(b.row).sort()));
const snapshot=async c=>{const rows={};for(const t of ['axton_record','axton_stream','axton_stream_member','axton_stream_log','axton_client','axton_call','fixture_todo'])rows[t]=(await c.query(`SELECT to_jsonb(t) row FROM ${t} t ORDER BY to_jsonb(t)::text`)).rows;return rows;};
test('Stream upgrade Channel-to-Scope chain: preserves retained data, removal cursors and opaque JSON; idempotent repeat',async()=>{
 const c=await scratch('axton_stream_upgrade_chain');
 try{
  await c.query(await fixture('v02-framework.sql'));await c.query(await fixture('postgres-state.sql'));await c.query(await fixture('postgres-optional-retained-v01.sql'));
  await c.query(await source('migrations/2026-09-30-scopes.sql'));
  const rawResponse=' {"completion":{"result":{"scope":"opaque","memberships":[{"scope":"nested"}]}},"continuation":{"scope":"opaque"}} ';
  await c.query('INSERT INTO axton_call(owner_id,call_id,request,response) VALUES($1,$2,$3,$4)',['alice','byte-identical',' {"scope":"request"} ',rawResponse]);
  const before={};for(const t of ['axton_record','axton_scope','axton_scope_member','axton_scope_log','axton_membership','axton_invalidation','axton_client','axton_call','fixture_todo'])before[t]=(await c.query(`SELECT to_jsonb(t) row FROM ${t} t ORDER BY to_jsonb(t)::text`)).rows;
  await upgrade(c);
  for(const t of ['axton_record','fixture_todo'])assert.deepEqual((await snapshot(c))[t],before[t]);
  for(const t of ['axton_scope','axton_scope_member','axton_scope_log','axton_membership','axton_invalidation']){
   const expected=before[t].map(({row})=>({row:Object.fromEntries(Object.entries(row).map(([k,v])=>[k==='scope'?'stream':k,v]))}));
   assert.deepEqual((await c.query(`SELECT to_jsonb(t) row FROM ${t.replace('axton_scope','axton_stream')} t ORDER BY to_jsonb(t)::text`)).rows.sort(sortRows),expected.sort(sortRows));
  }
  for(const t of ['axton_client','axton_call']){
   const expected=before[t].map(({row})=>{const field=t==='axton_client'?'receipt':'response';if(row[field]){const e=JSON.parse(row[field]);for(const claim of e.memberships??[]){claim.stream=claim.scope;delete claim.scope;}row={...row,[field]:e};}return row;});
   const actual=(await c.query(`SELECT to_jsonb(t) row FROM ${t} t ORDER BY to_jsonb(t)::text`)).rows.map(({row})=>{const f=t==='axton_client'?'receipt':'response';return {...row,[f]:row[f]?JSON.parse(row[f]):row[f]};});
   assert.deepEqual(actual,expected);
  }
  assert.equal((await c.query("SELECT response FROM axton_call WHERE call_id='byte-identical'")).rows[0].response,rawResponse,'a response with no top-level claims is byte-identical');
  assert.equal((await c.query("SELECT to_regclass('axton_stream_tag') t")).rows[0].t,null);
  const after=await snapshot(c);const catalog=(await c.query("SELECT relname FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname LIKE 'axton_%' ORDER BY relname")).rows;
  await upgrade(c);assert.deepEqual(await snapshot(c),after);assert.deepEqual((await c.query("SELECT relname FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname LIKE 'axton_%' ORDER BY relname")).rows,catalog);
  await c.query(await source('migration.sql'));
 }finally{await c.end();}
});
test('Stream upgrade conflicts and malformed top-level claims roll back all catalog and data changes',async()=>{
 for(const sql of ["CREATE TABLE axton_stream(stream text)","ALTER TABLE axton_scope_member ADD COLUMN stream text","DROP TABLE axton_scope_log","ALTER TABLE axton_scope_tag ADD COLUMN business_note text", "CREATE TABLE business_tag_reference(tag_id bigint REFERENCES axton_scope_tag(id))",`UPDATE axton_call SET response='{"memberships":[{"scope":"x","stream":"x","model":"Todo","identity":{},"cursor":1}]}' WHERE call_id LIKE '%003'`]){
  const c=await scratch('axton_stream_invalid');try{
   await c.query(await fixture('v02-framework.sql'));await c.query(await fixture('postgres-state.sql'));await c.query(await source('migrations/2026-09-30-scopes.sql'));await c.query(sql);
   const before=(await c.query("SELECT c.relname,a.attname FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid WHERE c.relnamespace=current_schema()::regnamespace ORDER BY 1,2")).rows;
   const saved=(await c.query('SELECT receipt FROM axton_client')).rows;
   await assert.rejects(()=>upgrade(c));assert.deepEqual((await c.query("SELECT c.relname,a.attname FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid WHERE c.relnamespace=current_schema()::regnamespace ORDER BY 1,2")).rows,before);assert.deepEqual((await c.query('SELECT receipt FROM axton_client')).rows,saved);
  }finally{await c.end();}
 }
});

const PREVIOUS_SCHEMA=`
CREATE TABLE IF NOT EXISTS axton_client (
 client_id text PRIMARY KEY,
 owner_id text NOT NULL,
 sequence bigint NOT NULL DEFAULT 0 CHECK(sequence >= 0 AND sequence <= 9007199254740991),
 receipt text
);
CREATE TABLE IF NOT EXISTS axton_call (
 owner_id text NOT NULL,
 call_id text NOT NULL,
 request text NOT NULL,
 response text,
 claim_tx xid8 NOT NULL DEFAULT pg_current_xact_id(),
 PRIMARY KEY(owner_id,call_id)
);
CREATE TABLE IF NOT EXISTS axton_channel (
 channel text PRIMARY KEY,
 head bigint NOT NULL CHECK(head >= 0 AND head <= 9007199254740991)
);
CREATE TABLE IF NOT EXISTS axton_record (
 model text NOT NULL,
 identity_key text NOT NULL,
 stamp bigint NOT NULL CHECK(stamp > 0 AND stamp <= 9007199254740991),
 PRIMARY KEY(model,identity_key)
);
CREATE TABLE IF NOT EXISTS axton_invalidation (
 channel text NOT NULL REFERENCES axton_channel(channel),
 model text NOT NULL,
 identity_key text NOT NULL,
 identity jsonb NOT NULL,
 cursor bigint NOT NULL CHECK(cursor > 0 AND cursor <= 9007199254740991),
 stamp bigint NOT NULL CHECK(stamp > 0 AND stamp <= 9007199254740991),
 PRIMARY KEY(channel,model,identity_key),
 UNIQUE(channel,cursor)
);
CREATE TABLE IF NOT EXISTS axton_membership (
 channel text NOT NULL REFERENCES axton_channel(channel),
 model text NOT NULL,
 identity_key text NOT NULL,
 PRIMARY KEY(model, identity_key, channel),
 FOREIGN KEY(model, identity_key) REFERENCES axton_record(model, identity_key)
);
CREATE INDEX IF NOT EXISTS axton_membership_channel
 ON axton_membership(channel, model, identity_key);
`;
const seedPrevious=async client=>{
 await client.query(PREVIOUS_SCHEMA);
 const k=id=>JSON.stringify({id});
 await client.query("INSERT INTO axton_channel(channel,head) VALUES('m-A',5),('m-B',2),('m-E',0)");
 await client.query(`INSERT INTO axton_record(model,identity_key,stamp) VALUES
  ('Todo',$1,3),('Todo',$2,1),('Todo',$3,2),('Todo',$4,1),('Todo',$5,1),('Todo',$6,4),('Note',$1,1)`,[k('a'),k('b'),k('c'),k('d'),k('aa'),k('e')]);
 await client.query(`INSERT INTO axton_membership(channel,model,identity_key) VALUES
  ('m-A','Todo',$1),('m-A','Todo',$3),('m-A','Todo',$4),('m-A','Todo',$5),('m-A','Note',$1),('m-B','Todo',$2),('m-E','Todo',$1)`,[k('a'),k('b'),k('c'),k('d'),k('aa')]);
 await client.query(`INSERT INTO axton_invalidation(channel,model,identity_key,identity,cursor,stamp) VALUES
  ('m-A','Todo',$1,$1::text::jsonb,5,3),('m-A','Todo',$2,$2::text::jsonb,4,1),('m-A','Todo',$3,$3::text::jsonb,2,2),('m-B','Todo',$2,$2::text::jsonb,2,1)`,[k('a'),k('b'),k('c')]);
};

test('Stream upgrade prior Channel path retains unlogged memberships and historical removals through both existing scripts',async()=>{
 const c=await scratch('axton_stream_prior_channel');try{
  await seedPrevious(c);
  await c.query(await source('migrations/2026-09-30-channel-members.sql'));
  await c.query(await source('migrations/2026-09-30-scopes.sql'));
  await upgrade(c);
  const rows=(await c.query("SELECT r.model,r.identity->>'id' id,l.cursor::int,l.kind FROM axton_stream_log l JOIN axton_record r ON r.id=l.record_id WHERE l.stream='m-A' ORDER BY l.cursor")).rows;
  assert.deepEqual(rows.map(r=>[r.model,r.id,r.cursor,r.kind]),[['Todo','c',2,'upsert'],['Todo','b',4,'remove'],['Todo','a',5,'upsert'],['Note','a',6,'upsert'],['Todo','aa',7,'upsert'],['Todo','d',8,'upsert']]);
  assert.equal((await c.query('SELECT count(*)::int n FROM axton_stream_member')).rows[0].n,7);
  assert.equal((await c.query('SELECT count(*)::int n FROM axton_membership')).rows[0].n,7);
  const fresh=await scratch('axton_stream_fresh_catalog');try{
   await fresh.query(await source('migration.sql'));
   const catalog=async c=>(await c.query("SELECT conrelid::regclass::text tbl,conname,pg_get_constraintdef(oid) body FROM pg_constraint WHERE conrelid::regclass::text=ANY($1) ORDER BY 1,2",[['axton_client','axton_call','axton_record','axton_stream','axton_stream_member','axton_stream_log']])).rows;
   assert.deepEqual(await catalog(c),await catalog(fresh));
  }finally{await fresh.end();}
  const version=(await c.query('SELECT xmin::text,to_jsonb(t) row FROM axton_stream_log t ORDER BY cursor')).rows;
  await upgrade(c);assert.deepEqual((await c.query('SELECT xmin::text,to_jsonb(t) row FROM axton_stream_log t ORDER BY cursor')).rows,version);
 }finally{await c.end();}
});

test('bulk mixed guard ordinality spans contiguous chunks',async()=>{
 const records=Array.from({length:1001},(_,i)=>record('Ordinal',String(i).padStart(5,'0'),i%2?'advance':'ensure'));
 const ordinals=[];
 await adapter(call=>call({op:'guardRecords',records}),(sql,params)=>{if(sql===SQL.GUARD_RECORDS)ordinals.push(JSON.parse(params[0]).map(r=>r.ordinal));});
 assert.deepEqual(ordinals.map(g=>[g[0],g.at(-1)]),[[1,1000],[1001,1001]]);
});

test('bulk backend reversed caller operands over a chunk boundary serialize mixed guards and retry the whole settlement',async()=>{
 const {createBackend}=await import('../../../packages/server/index.mts');
 const records=Array.from({length:1002},(_,i)=>({model:i<501?'ReverseA':'ReverseB',identity:{id:String(i).padStart(5,'0')}}));
 const models=['ReverseA','ReverseB'].map(name=>({name,version:1,identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false}]}));
 const config={schema:{enums:[],models},mutations:[],loaders:['ReverseA','ReverseB']};
 const app=createBackend({config,native,database:pg(pool),authenticate:()=> 'alice',loaders:{reverseA:async({ids})=>ids.map(()=>({})),reverseB:async({ids})=>ids.map(()=>({}))}});
 await adapter(call=>call({op:'guardRecords',records:records.filter((_,i)=>i%2===0).map(r=>({model:r.model,identityKey:JSON.stringify(r.identity),mode:'ensure'}))}));
 const runs=[0,0];const both=latch();let arrived=0;
 const changed=records.filter((_,i)=>i%3===0);
 const run=n=>app.transaction(async({tx,stream})=>{
  runs[n]++;await driver.query(tx,'SELECT 1',[]);
  if(runs[n]===1){if(++arrived===2)both.resolve();await both.promise;}
  stream(n===0?['reverse:a','reverse:b']:['reverse:b','reverse:a']).track(n===0?records:[...records].reverse());
  stream(['reverse:a','reverse:b']).invalidate(n===0?changed:[...changed].reverse());
 });
 await Promise.all([run(0),run(1)]);
 assert.ok(runs[0]+runs[1]>2,'one whole backend transaction retries');
 const stored=await q("SELECT model,identity_key,stamp::int stamp FROM axton_record WHERE model=ANY($1) ORDER BY model COLLATE \"C\",identity_key COLLATE \"C\"",[['ReverseA','ReverseB']]);
 assert.equal(stored.length,1002);
 for(let i=0;i<records.length;i++)assert.equal(stored[i].stamp,i%3===0?(i%2===0?3:2):1,`mixed guard ${i}`);
 assert.deepEqual((await q("SELECT stream,head::int FROM axton_stream WHERE stream LIKE 'reverse:%' ORDER BY stream")).map(r=>[r.stream,r.head]),[['reverse:a',1336],['reverse:b',1336]]);
});

test('bulk log ordinality spans contiguous pair chunks',async()=>{
 const records=Array.from({length:1001},(_,i)=>record('LogOrdinal',String(i).padStart(5,'0')));
 const ordinals=[];
 await adapter(async call=>{await call({op:'guardRecords',records});await call({op:'applyStreamMembers',deltas:pairs(records,['log-ordinal'])});},(sql,params)=>{if(sql===SQL.WRITE_STREAM_LOG)ordinals.push(JSON.parse(params[0]).map(r=>r.ordinal));});
 assert.deepEqual(ordinals.map(g=>[g[0],g.at(-1)]),[[1,1000],[1001,1001]]);
});

test('migrated direct, alternate batch, exact receipt and Load replay bypass poisoned handlers and Loaders on every shim',async()=>{
 const c=await scratch('axton_stream_replay');
 const schema=JSON.parse(await fixture('schema.json'));
 const input=JSON.parse(await fixture('envelopes.json'));
 const pool=new Pool({connectionString:databaseUrl('axton_stream_replay')});
 const {PrismaClient}=require('../../bindings/node/generated/client');
 const pr=new PrismaClient({datasourceUrl:databaseUrl('axton_stream_replay')});
 try{
  await c.query(await fixture('v02-framework.sql'));await c.query(await fixture('postgres-state.sql'));
  const noClaimRequest=JSON.stringify(input.loadRequest).replaceAll('000000000003','000000000005');
  const noClaimResponse=JSON.stringify(input.noClaimLoadResponse).replaceAll('000000000003','000000000005');
  await c.query('INSERT INTO axton_call(owner_id,call_id,request,response) VALUES($1,$2,$3,$4)',['alice','01890f47-1234-7123-8123-000000000005',noClaimRequest,noClaimResponse]);
  await c.query(await source('migrations/2026-09-30-scopes.sql'));await upgrade(c);
  let invoked=0;
  const poison=()=>{invoked++;throw new Error('poisoned fresh execution');};
  const state=async()=>{const o={};for(const t of ['axton_stream','axton_record','axton_stream_member','axton_stream_log'])o[t]=(await c.query(`SELECT xmin::text,to_jsonb(t) AS row FROM ${t} t ORDER BY to_jsonb(t)::text`)).rows;return o;};
  const before=await state();
  for(const [name,database] of [['pg',pg(pool)],['prisma',prisma(pr)],['drizzle',drizzle(drizzleOrm(pool))]]){
   const app=createBackend({config:{schema,mutations:[],loaders:['Todo']},native,database,authenticate:()=>'alice',mutations:{edit:poison},loads:{scan:poison},loaders:{todo:poison},onError:()=>{}});
   const action=input.actionRequest;
   const direct=JSON.parse(await app.action('alice',JSON.stringify({capabilities:['stream-authority-v1'],call:{callId:action.callId,name:action.name,version:action.version,args:action.args},models:action.models})));
   assert.equal(direct.records[0].state.title,'saved snapshot',name);
   assert.deepEqual(direct.memberships,[{stream:'Channel:business-scope',model:'Todo',identity:{id:'live'},cursor:10}]);
   const body=clientId=>JSON.stringify({capabilities:['stream-authority-v1'],clientId,batchSequence:1,models:action.models,mutations:[{ordinal:1,callId:action.callId,name:action.name,version:1,args:action.args}]});
   const alternate=JSON.parse(await app.push('alice',body(`alternate-${name}`)));
   assert.deepEqual(alternate.records,direct.records);assert.equal(Object.hasOwn(alternate,'memberships'),false,'fresh alternate Push omits saved Action claims');
   const receipt=JSON.parse(await app.push('alice',body('fixture-client')));
   assert.deepEqual(receipt.records,direct.records);assert.deepEqual(receipt.memberships,direct.memberships);
   const load=JSON.parse(await app.loads('alice',JSON.stringify({capabilities:['stream-authority-v1'],loads:[input.loadIntent]}))).loads[0];
   assert.equal(load.records[0].state.title,'saved snapshot');assert.deepEqual(load.outcome.next,input.loadResponse.outcome.next);assert.deepEqual(load.memberships,direct.memberships);
   const noClaim=JSON.parse(await app.loads('alice',JSON.stringify({capabilities:['stream-authority-v1'],loads:[{...input.loadIntent,callId:'01890f47-1234-7123-8123-000000000005'}]}))).loads[0];
   assert.equal(Object.hasOwn(noClaim,'memberships'),false,'saved no-claim response has no fabricated claims');
   assert.deepEqual(noClaim.outcome.next,input.noClaimLoadResponse.outcome.next);
   assert.equal(invoked,0,`${name}: saved state executes no handler/Loader`);
   assert.deepEqual(await state(),before,`${name}: replay never reenrolls withdrawn pair or rewrites head/stamp/log/tag`);
  }
  const app=createBackend({config:{schema,mutations:[],loaders:['Todo']},native,database:pg(pool),authenticate:()=>'alice',mutations:{edit:poison},loads:{scan:poison},loaders:{todo:poison},onError:()=>{}});
  await app.action('alice',JSON.stringify({capabilities:['stream-authority-v1'],call:{callId:'01890f47-1234-7123-8123-000000000099',name:'Edit',version:1,args:input.actionRequest.args},models:{Todo:1}}));
  assert.equal(invoked,1,'a fresh call really reaches the poisoned handler');
 }finally{await pr.$disconnect();await pool.end();await c.end();}
});

test('bulk backend chunk 2 fault rolls back every table and never wakes its Stream',async()=>{
 const model={name:'WakeRollback',version:1,identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false}]};
 const records=Array.from({length:1001},(_,i)=>({id:String(i).padStart(5,'0')}));let logChunks=0;
 const watchedDriver={...driver,query:(tx,sql,params)=>{if(sql===SQL.WRITE_STREAM_LOG && ++logChunks===2)throw Error('second log chunk');return driver.query(tx,sql,params);}};
 const app=createBackend({config:{schema:{enums:[],models:[model]},mutations:[],loaders:['WakeRollback']},native,database:persistence(watchedDriver),authenticate:()=> 'alice',loaders:{wakeRollback:async({ids})=>ids.map(()=>({}))}});
 const wakes=[];const stop=app.onCommitted('rollback-wake',()=>wakes.push('rollback-wake'));
 await assert.rejects(()=>app.transaction(async({tx,stream})=>{
  await driver.query(tx,"INSERT INTO stream_business VALUES('rollback-wake')",[]);
  await answer(driver,tx,{op:'claimCall',owner:'rollback-wake',callId:'rollback-wake',request:'{}'});
  await answer(driver,tx,{op:'claim',owner:'rollback-wake',clientId:'rollback-wake'});
  stream('rollback-wake').track.wakeRollback(records);
 }),/second log chunk/);
 await new Promise(r=>setImmediate(r));stop();assert.deepEqual(wakes,[]);
 for(const table of ['axton_stream','axton_stream_log','axton_stream_member'])assert.equal((await q(`SELECT count(*)::int n FROM ${table} WHERE stream='rollback-wake'`))[0].n,0);
 for(const [table,where] of [['axton_record',"model='WakeRollback'"],['axton_call',"owner_id='rollback-wake'"],['axton_client',"client_id='rollback-wake'"],['stream_business',"id='rollback-wake'"]])assert.equal((await q(`SELECT count(*)::int n FROM ${table} WHERE ${where}`))[0].n,0);
});

test('fresh Stream DDL installs identically through whole-file pg and split prepared Prisma statements',async()=>{
 const {sqlStatements}=await import('../../../packages/postgres/src/statements.mts');
 assert.deepEqual(sqlStatements("SELECT 'a;b'; DO $f$ BEGIN PERFORM 1; END $f$; -- ignored;\n"),["SELECT 'a;b'","DO $f$ BEGIN PERFORM 1; END $f$"]);
 const whole=await scratch('axton_stream_whole'),split=await scratch('axton_stream_split');
 const {PrismaClient}=require('../../bindings/node/generated/client');const pr=new PrismaClient({datasourceUrl:databaseUrl('axton_stream_split')});
 try{
  const text=await source('migration.sql');await whole.query(text);
  for(let round=0;round<2;round++)for(const sql of sqlStatements(text))await pr.$executeRawUnsafe(sql);
  const catalog=async c=>({columns:(await c.query("SELECT table_name,column_name,data_type,is_nullable,is_identity,is_generated FROM information_schema.columns WHERE table_schema='public' ORDER BY 1,2")).rows,
   constraints:(await c.query("SELECT conrelid::regclass::text tbl,conname,pg_get_constraintdef(oid) body FROM pg_constraint WHERE connamespace=current_schema()::regnamespace ORDER BY 1,2")).rows,
   triggers:(await c.query("SELECT tgname,pg_get_triggerdef(oid) body FROM pg_trigger WHERE NOT tgisinternal ORDER BY 1")).rows});
  assert.deepEqual(await catalog(split),await catalog(whole));
 }finally{await pr.$disconnect();await whole.end();await split.end();}
});

// The repair changes framework authority only. Raw text snapshots deliberately
// include historical top-level claims and opaque nested business memberships.
const repair=async c=>{try{await c.query(await source('migrations/2026-10-01-local-authority.sql'));}catch(e){await c.query('ROLLBACK');throw e;}};
const authoritySnapshot=async c=>{
 const out={};for(const table of ['axton_record','axton_stream','axton_stream_member','axton_stream_log','axton_client','axton_call','authority_business'])out[table]=(await c.query(`SELECT to_jsonb(t) row FROM ${table} t ORDER BY to_jsonb(t)::text`)).rows;
 return out;
};
const authorityFixture=async c=>{
 await c.query(await source('migration.sql'));
 await c.query('CREATE TABLE authority_business(id text PRIMARY KEY, payload text NOT NULL)');
 const business=' {"memberships": [ {"stream":"business", "value":1} ]} ';
 const response=' {"memberships":[{"stream":"A","model":"Entry","identity":{"id":"e"},"cursor":11}],"result":{"memberships":["opaque"]}} ';
 await c.query('INSERT INTO authority_business VALUES($1,$2)',['e',business]);
 await c.query('INSERT INTO axton_call(owner_id,call_id,request,response) VALUES($1,$2,$3,$4)',['alice','saved',' {"args":{"memberships":["business"]}} ',response]);
 await c.query('INSERT INTO axton_client(client_id,owner_id,sequence,receipt) VALUES($1,$2,3,$3)',['saved-client','alice',response]);
 await c.query("INSERT INTO axton_stream(stream,head) VALUES('A',11),('B',4),('U',20)");
 const id=(await c.query("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Entry',$1,7) RETURNING id",[key('e')])).rows[0].id;
 const unrelated=(await c.query("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Entry',$1,9) RETURNING id",[key('u')])).rows[0].id;
 await c.query("INSERT INTO axton_stream_member(stream,record_id) VALUES('B',$1),('U',$2)",[id,unrelated]);
 await c.query("INSERT INTO axton_stream_log(stream,record_id,cursor,kind) VALUES('A',$1,11,'remove'),('B',$1,4,'upsert'),('U',$2,20,'upsert')",[id,unrelated]);
 return {id,unrelated};
};
test('local authority repair restamps withdrawals once, preserves saved bytes and current viewer authority',async()=>{
 const name='axton_authority_repair',c=await scratch(name),p=new Pool({connectionString:databaseUrl(name)});
 try{
  const {id,unrelated}=await authorityFixture(c),before=await authoritySnapshot(c);
  let calls=0;
  const model={name:'Entry',version:1,identity:['id'],fields:[{name:'id',nullable:false,type:{kind:'scalar',name:'string'}},{name:'payload',nullable:false,type:{kind:'scalar',name:'string'}}]};
  const app=createBackend({config:{schema:{enums:[],models:[model]},mutations:[],loaders:['Entry']},native,database:pg(p),authenticate:()=> 'alice',loaders:{entry:async({ids,userId,tx})=>{calls++;const rows=await tx.query('SELECT * FROM authority_business WHERE id=ANY($1)',[ids.map(x=>x.id)]);return ids.map(({id})=>userId==='removed'?null:rows.rows.find(r=>r.id===id)?{payload:rows.rows.find(r=>r.id===id).payload}:null);}}});
  const pull=(owner,cursors)=>app.pull(owner,JSON.stringify({capabilities:['stream-authority-v1'],cursors,models:{Entry:1}})).then(JSON.parse);
  const historical=await pull('removed',{A:10});
  assert.equal(historical.changes[0].kind,'remove');assert.equal(Object.hasOwn(historical.changes[0],'state'),false);assert.equal(calls,0,'Remove evidence invokes no Loader');
  await repair(c);
  assert.equal((await c.query("SELECT stamp FROM axton_record WHERE model='Entry' AND identity_key=$1",[key('e')])).rows[0].stamp,'8');
  assert.deepEqual((await c.query('SELECT stream,kind,cursor::text cursor FROM axton_stream_log WHERE record_id=$1 ORDER BY stream',[id])).rows.map(r=>[r.stream,r.kind,r.cursor]),[['A','upsert','12'],['B','upsert','5']]);
  assert.equal((await c.query('SELECT count(*)::int n FROM axton_stream_member WHERE record_id=$1',[id])).rows[0].n,2);
  const after=await authoritySnapshot(c);
  for(const table of ['axton_call','axton_client','authority_business'])assert.deepEqual(after[table],before[table],table+' exact bytes');
  for(const table of ['axton_record','axton_stream_member','axton_stream_log'])assert.deepEqual(after[table].filter(({row})=>row.id===Number(unrelated)||row.record_id===Number(unrelated)),before[table].filter(({row})=>row.id===Number(unrelated)||row.record_id===Number(unrelated)));
  assert.deepEqual(after.axton_stream.filter(({row})=>row.stream==='U'),before.axton_stream.filter(({row})=>row.stream==='U'));
  const seq=async()=>(await c.query('SELECT last_value,is_called FROM axton_stream_member_id_seq')).rows;
  const sequence=await seq();await repair(c);assert.deepEqual(await authoritySnapshot(c),after);assert.deepEqual(await seq(),sequence,'replay allocates no tracking IDs');
  const removed=await pull('removed',{A:11}),surviving=await pull('surviving',{B:4});
  assert.equal(removed.changes[0].stamp,8);assert.equal(removed.changes[0].state,null,'current Loader decides absence');
  assert.equal(surviving.changes[0].stamp,8);assert.equal(surviving.changes[0].state.payload,before.authority_business[0].row.payload,'another viewer retains valid content');
  assert.equal((await c.query('SELECT count(*)::int n FROM axton_stream_member WHERE record_id=$1',[id])).rows[0].n,2,'null does not erase tracking');
 }finally{await p.end();await c.end();}
});
test('local authority repair handles more than 1000 pairs and globally unions several withdrawals per identity',async()=>{
 const c=await scratch('axton_authority_bulk');try{
  await authorityFixture(c);
  await c.query("INSERT INTO axton_stream(stream,head) VALUES('bulk',1001),('other',1001),('survivor',1001)");
  await c.query(`INSERT INTO axton_record(model,identity_key,stamp) SELECT 'BulkRepair',format('{"id":"%s"}',lpad(n::text,5,'0')),7 FROM generate_series(1,1001) n`);
  await c.query("INSERT INTO axton_stream_member(stream,record_id) SELECT 'survivor',id FROM axton_record WHERE model='BulkRepair'");
  await c.query(`INSERT INTO axton_stream_log(stream,record_id,cursor,kind) SELECT s.stream,r.id,row_number() OVER(PARTITION BY s.stream ORDER BY r.identity_key),CASE WHEN s.stream='survivor' THEN 'upsert' ELSE 'remove' END FROM axton_record r CROSS JOIN (VALUES('bulk'),('other'),('survivor')) s(stream) WHERE r.model='BulkRepair'`);
  await repair(c);
  assert.deepEqual((await c.query("SELECT DISTINCT stamp FROM axton_record WHERE model='BulkRepair'")).rows,[{stamp:'8'}]);
  assert.equal((await c.query("SELECT count(*)::int n FROM axton_stream_member m JOIN axton_record r ON r.id=m.record_id WHERE r.model='BulkRepair'")).rows[0].n,3003);
  assert.deepEqual((await c.query("SELECT stream,head FROM axton_stream WHERE stream IN ('bulk','other','survivor') ORDER BY stream")).rows,[{stream:'bulk',head:'2002'},{stream:'other',head:'2002'},{stream:'survivor',head:'2002'}]);
  assert.equal((await c.query("SELECT count(*)::int n FROM axton_stream_log WHERE stream='bulk' AND cursor>1001 AND kind='upsert'")).rows[0].n,1001,'all repaired positions follow the old cursor');
  const before=await authoritySnapshot(c);await repair(c);assert.deepEqual(await authoritySnapshot(c),before);
 }finally{await c.end();}
});
test('local authority repair refuses counter overflow and late SQL failure atomically, preserving saved bytes',async()=>{
 for(const fault of ['stamp','head','late']){
  const c=await scratch('axton_authority_failure');try{
   await authorityFixture(c);
   if(fault==='stamp')await c.query("UPDATE axton_record SET stamp=9007199254740991 WHERE identity_key=$1",[key('e')]);
   if(fault==='head')await c.query("UPDATE axton_stream SET head=9007199254740991 WHERE stream='A'");
   if(fault==='late')await c.query(`CREATE FUNCTION authority_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.stream='B' THEN RAISE EXCEPTION 'injected late repair failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER authority_fail BEFORE UPDATE ON axton_stream_log FOR EACH ROW EXECUTE FUNCTION authority_fail()`);
   const before=await authoritySnapshot(c);
   await assert.rejects(()=>repair(c),fault==='late'?/injected late repair failure/:/check constraint/);
   assert.deepEqual(await authoritySnapshot(c),before,fault+' rolls back rows and preserves exact saved text');
   assert.equal((await c.query("SELECT count(*)::int n FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'axton_authority_%'")).rows[0].n,0,'temporary repair state rolls back');
  }finally{await c.end();}
 }
});
test('local authority repair refuses unsupported shapes before modifying data',async()=>{
 for(const mutation of ["ALTER TABLE axton_call RENAME COLUMN request TO missing_request","ALTER TABLE axton_stream_log RENAME COLUMN cursor TO missing_cursor","ALTER TABLE axton_record ALTER COLUMN stamp TYPE numeric", "ALTER TABLE axton_record DROP CONSTRAINT axton_record_stamp_check, ADD CHECK(stamp>0 OR stamp<=9007199254740991)", "CREATE TABLE axton_scope_member(scope text)"]){
  const c=await scratch('axton_authority_invalid');try{
   await authorityFixture(c);await c.query(mutation);const before=await authoritySnapshot(c);
   await assert.rejects(()=>repair(c),/incomplete|unsupported/);assert.deepEqual(await authoritySnapshot(c),before);
  }finally{await c.end();}
 }
});
test('local authority repair accepts the historical Channel-to-Scope-to-Stream layout and preserves upgraded saved text',async()=>{
 const c=await scratch('axton_authority_upgraded');try{
  await c.query(await fixture('v02-framework.sql'));await c.query(await fixture('postgres-state.sql'));
  await c.query(await source('migrations/2026-09-30-scopes.sql'));await upgrade(c);
  const saved=async()=>(await c.query('SELECT request,response FROM axton_call ORDER BY call_id')).rows;
  const receipts=(await c.query('SELECT receipt FROM axton_client ORDER BY client_id')).rows;
  const before=await saved();
  const removed=(await c.query("SELECT DISTINCT r.id,r.stamp FROM axton_record r JOIN axton_stream_log l ON l.record_id=r.id WHERE l.kind='remove'")).rows;
  assert.ok(removed.length>0,'original fixture contains withdrawals');await repair(c);
  assert.equal((await c.query("SELECT count(*)::int n FROM axton_stream_log WHERE kind='remove'")).rows[0].n,0);
  for(const r of removed)assert.equal((await c.query('SELECT stamp FROM axton_record WHERE id=$1',[r.id])).rows[0].stamp,String(BigInt(r.stamp)+1n));
  assert.deepEqual(await saved(),before);assert.deepEqual((await c.query('SELECT receipt FROM axton_client ORDER BY client_id')).rows,receipts);
 }finally{await c.end();}
});
