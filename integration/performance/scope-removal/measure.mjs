// Diagnostic only: production backend + PostgreSQL adapter + existing native addon.
// No native builds, generation, application client or alternate removal reducer.
import assert from 'node:assert/strict';
import {readFile,writeFile,stat} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {createHash} from 'node:crypto';
import os from 'node:os';
import {execFileSync} from 'node:child_process';
import {Pool} from 'pg';
import {createBackend} from '../../../packages/server/index.mts';
import {pg} from '../../../packages/postgres/index.mts';
import * as SQL from '../../../packages/postgres/src/sql.mts';
const native=createRequire(import.meta.url)('../../../bindings/node/axton-node.node');
assert.ok(process.env.DATABASE_URL,'runner must provide its disposable DATABASE_URL');
assert.equal(new URL(process.env.DATABASE_URL).hostname,'127.0.0.1','local disposable PostgreSQL only');
const pool=new Pool({connectionString:process.env.DATABASE_URL});
let measured=null,loaderCalls=0;
const countedPool={async connect(){const client=await pool.connect();return {
 async query(sql,params){const start=performance.now();const result=await client.query(sql,params);
  if(measured){const name=Object.entries(SQL).find(([,value])=>value===sql)?.[0]??sql;
   measured.statements[name]=(measured.statements[name]??0)+1;
   measured.returnedOrAffectedRows[name]=(measured.returnedOrAffectedRows[name]??0)+result.rowCount;
   if(sql.startsWith('BEGIN'))measured.transactionStart=start;
   if(sql==='COMMIT')measured.transactionMs=performance.now()-measured.transactionStart;
  }return result;},release(error){client.release(error);}};}};
const string=name=>({name,type:{kind:'scalar',name:'string'},nullable:false});
const fields=[string('id'),string('title')];
const config={schema:{enums:[],models:[{name:'Todo',version:1,identity:['id'],fields}],
 resultModels:[{name:'Todo',version:1,identity:['id'],fields,enums:[]}],actions:[]},mutations:[],loaders:['Todo']};
const backend=createBackend({config,native,database:pg(countedPool),authenticate:()=>'alice',
 onError:error=>{throw error;},mutations:{},loaders:{todo:async({ids})=>{
  loaderCalls++;return ids.map(()=>({title:'fixture'}));}}});
const query=async(sql,params=[])=>(await pool.query(sql,params)).rows;
const snapshots=async()=>{
 const counts={};for(const table of ['axton_record','axton_scope','axton_scope_member','axton_scope_tag','axton_scope_member_tag','axton_scope_log','measurement_todo']){
  counts[table]=Number((await query(`SELECT count(*) AS n FROM ${table}`))[0].n);
 }return counts;};
try {
 await pool.query(await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8'));
 await pool.query('CREATE TABLE measurement_todo(id text PRIMARY KEY,title text NOT NULL)');
 const metadata={time:new Date().toISOString(),head:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),
  nativeAddon:{sha256:createHash('sha256').update(await readFile(new URL('../../../bindings/node/axton-node.node',import.meta.url))).digest('hex'),mtime:(await stat(new URL('../../../bindings/node/axton-node.node',import.meta.url))).mtime.toISOString()},
  platform:os.platform(),release:os.release(),arch:os.arch(),cpus:os.cpus().length,cpu:os.cpus()[0].model,memoryBytes:os.totalmem(),node:process.version,
  postgres:(await query('SELECT version() AS version'))[0].version,
  settings:await query("SELECT name,setting,unit FROM pg_settings WHERE name IN ('fsync','synchronous_commit','full_page_writes','wal_level','shared_buffers','max_connections') ORDER BY name"),
  statementBoundary:'Every successful pg client.query during public backend.transaction removal, including BEGIN SERIALIZABLE and COMMIT. Trigger/internal PostgreSQL substatements are excluded. rowCount is returned/affected rows of each top-level SQL call; cascading trigger rows are separately visible in fixture counts.',
  timingBoundary:'publicMs: performance.now before/after awaited backend.transaction (includes native reduction, pool, serialization and commit); transactionMs: before BEGIN query until COMMIT resolves (includes callback and JS/native work while transaction is open).',
  walBoundary:'pg_wal_lsn_diff(pg_current_wal_insert_lsn after committed removal, before removal), outside timed/count boundary; isolated cluster, physical WAL bytes including commit/triggers/FPI; not per-statement WAL.',
  responseBoundary:'Exact UTF-8 backend.pull JSON bodies, all <=50-event pages until the returned scope range.to===range.head; no additional client empty probe. No HTTP headers/compression. identityBytes sums UTF-8 JSON.stringify(change.identity), fixed one-field identities; response SHA256 hashes concatenated raw bodies.',samples:[]};
 for(const n of [1,1000,10000])for(let sample=1;sample<=3;sample++){
  await pool.query('TRUNCATE axton_scope_member_tag,axton_scope_member,axton_scope_tag,axton_scope_log,axton_scope,axton_record,measurement_todo RESTART IDENTITY CASCADE');
  const scope='Measure:remove';const ids=Array.from({length:n},(_,i)=>`member-${String(i).padStart(5,'0')}`);
  await pool.query("INSERT INTO measurement_todo SELECT 'member-'||lpad(i::text,5,'0'),'fixture' FROM generate_series(0,$1::int-1) i",[n]);
  await backend.transaction(({scope: c})=>c(scope).add(ids.map(id=>({model:'Todo',identity:{id}}))).tag(['X','Y']));
  const before=await snapshots();assert.equal(before.axton_scope_member,n);assert.equal(before.axton_scope_member_tag,2*n);
  const lsn=(await query('SELECT pg_current_wal_insert_lsn() AS lsn'))[0].lsn;
  loaderCalls=0;measured={statements:{},returnedOrAffectedRows:{}};
  const start=performance.now();await backend.transaction(({scope: c})=>c(scope).where({ tags: { all: ['X'] } }).remove());
  const publicMs=performance.now()-start;const removal=measured;measured=null;
  const removalLoaderCalls=loaderCalls;assert.equal(removalLoaderCalls,0);
  const walBytes=Number((await query('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1) AS bytes',[lsn]))[0].bytes);
  const after=await snapshots();assert.deepEqual(after,{...before,axton_scope_member:0,axton_scope_member_tag:0,axton_scope_tag:0});
  const log=await query('SELECT kind,count(*)::int AS n,min(cursor)::int AS first,max(cursor)::int AS last FROM axton_scope_log GROUP BY kind');
  assert.deepEqual(log,[{kind:'remove',n,first:n+1,last:2*n}]);
  let cursor=n,wireBytes=0,identityBytes=0,events=0,pages=0;const seen=new Set(),pageEventCounts=[],hash=createHash('sha256');let firstPageRaw,terminalPageRaw;
  const pullStart=performance.now();
  while(pages<Math.ceil(n/50)+2){
   const raw=await backend.pull('alice',JSON.stringify({capabilities:['scope-membership-v1'],cursors:{[scope]:cursor},models:{Todo:1}}));
   const page=JSON.parse(raw);pages++;wireBytes+=Buffer.byteLength(raw);hash.update(raw);pageEventCounts.push(page.changes.length);
   assert.ok(page.changes.length<=50);assert.equal(page.cursors[scope].from,cursor);
   for(const change of page.changes){assert.equal(change.kind,'remove');assert.equal(change.scope,scope);assert.equal(change.model,'Todo');assert.ok(!('state' in change));assert.ok(!seen.has(change.identity.id));seen.add(change.identity.id);identityBytes+=Buffer.byteLength(JSON.stringify(change.identity));events++;}
   cursor=page.cursors[scope].to;if(!firstPageRaw)firstPageRaw=raw;
   assert.equal(page.cursors[scope].head,2*n);
   if(cursor===page.cursors[scope].head){terminalPageRaw=raw;break;}
   assert.ok(page.changes.length>0,'a nonterminal page must progress');
  }
  const pullMs=performance.now()-pullStart;assert.ok(terminalPageRaw,'must fully drain until to===head');assert.equal(events,n);assert.equal(cursor,2*n);assert.deepEqual([...seen].sort(),ids);assert.equal(loaderCalls,0,'removal and all removal pulls invoke no Loader');
  delete removal.transactionStart;
  metadata.samples.push({members:n,sample,fixtureBefore:before,fixtureAfter:after,log,publicMs,...removal,
   statementCount:Object.values(removal.statements).reduce((a,b)=>a+b,0),walBytes,removalLoaderCalls,totalLoaderCalls:loaderCalls,
   response:{pages,pageEventCounts,events,wireBytes,identityBytes,pullMs,sha256:hash.digest('hex'),...(sample===1?{firstPageRaw,terminalPageRaw}:{})}});
  console.error(JSON.stringify({members:n,sample,publicMs,transactionMs:removal.transactionMs,statements:Object.values(removal.statements).reduce((a,b)=>a+b,0),walBytes,pages,wireBytes,identityBytes,loaderCalls}));
 }
 await writeFile(process.argv[2]??'/tmp/scope-removal-evidence.json',JSON.stringify(metadata,null,2)+'\n');
}finally{await pool.end();}
