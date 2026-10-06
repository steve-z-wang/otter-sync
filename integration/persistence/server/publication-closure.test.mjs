// Actual PostgreSQL closure semantics and bounded identity transport, independent
// of wall-clock performance. Historical groups are synthetic persistence fixtures.
import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {Pool} from 'pg';
import {pg,answer} from '../../../packages/postgres/index.mts';
const pool=new Pool({connectionString:process.env.DATABASE_URL});const database=pg(pool);
before(async()=>{await pool.query(await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8'));});
after(()=>pool.end());
const key=n=>({model:'Todo',identityKey:JSON.stringify({id:`k${n}`})});
async function history(stream,groups){
 await pool.query('INSERT INTO axton_stream(stream,head) VALUES($1,$2)',[stream,groups.length]);
 await pool.query('INSERT INTO axton_publication_group(stream,transaction_id,from_cursor,through_cursor,keys) SELECT $1,ord::text::xid8,ord-1,ord,v FROM jsonb_array_elements($2::jsonb) WITH ORDINALITY x(v,ord)',[stream,JSON.stringify(groups)]);
}
async function closure(stream){
 const queries=[];const driver={...database.driver,query:async(tx,sql,params)=>{const rows=await database.driver.query(tx,sql,params);queries.push({sql,rows});return rows;}};
 const result=await database.transaction(async tx=>{await answer(driver,tx,{op:'publicationFence'});queries.length=0;return answer(driver,tx,{op:'readPublicationGroups',stream,after:0,limit:1});});
 return {result,queries};
}
test('10000 repeated historical groups return one bounded related identity and preserve the root span',async()=>{
 await history('closure-repeat',Array.from({length:10000},()=>[key(0)]));
 const {result,queries}=await closure('closure-repeat');
 assert.deepEqual(result,[{from:0,through:1,keys:[key(0)]}]);
 assert.equal(queries.length,2);assert.equal(queries[1].rows.length,1,'history is deduplicated before returning to JavaScript');
});
test('chain overlap reaches the exact transitive closure without unrelated history',async()=>{
 const groups=Array.from({length:64},(_,n)=>[key(n),key(n+1)]);
 groups.push(...Array.from({length:10000},(_,n)=>[key(n+1000)]));
 await history('closure-chain',groups);
 const {result}=await closure('closure-chain');
 assert.equal(result[0].from,0);assert.equal(result[0].through,1);
 assert.deepEqual(result[0].keys.map(k=>k.identityKey).sort(),Array.from({length:65},(_,n)=>key(n).identityKey).sort());
});
test('unrelated groups allocate no closure identities',async()=>{
 await history('closure-unrelated',[[key(0)],...Array.from({length:10000},(_,n)=>[key(n+1)])]);
 const {result,queries}=await closure('closure-unrelated');assert.deepEqual(result,[{from:0,through:1,keys:[key(0)]}]);assert.equal(queries[1].rows.length,1);
});
test('distinct identity and encoded metadata capacity both refuse without truncated success',async()=>{
 await history('closure-limit',[[key(0)],Array.from({length:10000},(_,n)=>key(n))]);
 const {result}=await closure('closure-limit');assert.deepEqual(result[0].keys.map(k=>k.identityKey).sort(),Array.from({length:10000},(_,n)=>key(n).identityKey).sort());
 await history('closure-count',[[key(0)],Array.from({length:10001},(_,n)=>key(n))]);
 await assert.rejects(closure('closure-count'),error=>error.code==='constraint_group_capacity');
 const groups=Array.from({length:50},(_,group)=>[key(0),...Array.from({length:100},(_,n)=>key(`${group}-${n}-${'x'.repeat(256)}`))]);
 await history('closure-bytes',[[key(0)],...groups]);
 await assert.rejects(closure('closure-bytes'),error=>error.code==='constraint_group_capacity');
});
test('malformed historical keys are refused rather than normalized by SQL projection',async()=>{
 await history('closure-malformed',[[key(0)],[key(0),{...key(1),extra:'invalid'}]]);
 await assert.rejects(closure('closure-malformed'),/Unknown record key field extra/);
});
