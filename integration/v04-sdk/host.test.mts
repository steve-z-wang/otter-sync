import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Pool} from 'pg';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {pg,type PgClient} from '../../packages/postgres/index.mts';
import {createBackend,devAuth,CallRejected} from './backend.ts';
import {GeneratedClient} from './client.ts';

test('generated SDK uses real fresh Query/Fetch, Bootstrap and settled Mutation paths',async()=>{
 const pool=new Pool({connectionString:process.env.DATABASE_URL});
 const dir=await mkdtemp(join(tmpdir(),'axton-sdk-host-'));
 let queries=0,bootstraps=0,invalidIdentity=false;
 await pool.query(await readFile(new URL('../../packages/postgres/migration.sql',import.meta.url),'utf8'));
 await pool.query('CREATE TABLE sdk_entry(id text PRIMARY KEY,text text NOT NULL)');
 const backend=createBackend<PgClient>({database:pg(pool),authenticate:devAuth(),protocol5:{authorizeStream:(viewer,stream)=>stream===`User:${viewer}`},mutations:{
  async publish({ctx,args}) {
   if(args.entry.text==='refuse') throw new CallRejected('publish.refused');
   await ctx.tx.query('INSERT INTO sdk_entry VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET text=excluded.text',[args.entry.id,args.entry.text.trim()]);
   ctx.stream.track.entry(args.entry.id);ctx.invalidate.entry(args.entry.id);
   return {entry:{id:args.entry.id}};
  }
 },queries:{async find({ctx,args}){queries++;const {rows}=await ctx.tx.query('SELECT id FROM sdk_entry WHERE id=$1',[args.id]);return {entry:rows[0] ? invalidIdentity ? {id:String(rows[0].id),text:'not an identity'} : {id:String(rows[0].id)}:null};}},loaders:{entry:async({tx,ids})=>{
  const {rows}=await tx.query('SELECT id,text FROM sdk_entry WHERE id=ANY($1)',[ids.map(x=>x.id)]);
  const values=new Map(rows.map(row=>[String(row.id),{id:String(row.id),text:String(row.text)}]));return ids.map(x=>values.get(x.id)??null);
 },draft:undefined},bootstrap:async()=>{bootstraps++;}});
 const server=await backend.listen({port:0});
 const connection={url:server.url,token:'alice'};
 const client=await GeneratedClient.open({path:join(dir,'db'),stream:'User:alice',connection});
 try {
  await client.bootstrap();assert.equal(bootstraps,1);
  await client.transaction(async tx=>{await tx.models.draft.create({id:'d',text:'companion'});});
  const call=await client.mutations.publish(async tx=>{await tx.models.draft.delete({id:'d'});return {entry:{id:'e',text:' normalized '},call:'business field'};});
  assert.equal(await client.models.draft.get({id:'d'}),null);
  const result=await call.wait();assert.equal(result.error,null);assert.equal(result.result?.entry.text,'normalized');
  assert.equal((await client.models.entry.get({id:'e'}))?.text,'normalized');
  const refused=await client.mutations.publish(async tx=>{await tx.models.draft.create({id:'refused-draft',text:'owned'});return {entry:{id:'refused-entry',text:'refuse'},call:'business'};});
  assert.equal((await refused.wait()).error?.code,'publish.refused');
  assert.equal(await client.models.draft.get({id:'refused-draft'}),null);
  assert.equal(await client.models.entry.get({id:'refused-entry'}),null);
  await pool.query("INSERT INTO sdk_entry VALUES('read','snapshot')");
  const first=await client.queries.find({id:'read'},{store:false});assert.equal(first.entry?.text,'snapshot');
  assert.equal(await client.models.entry.get({id:'read'}),null);
  await pool.query("UPDATE sdk_entry SET text='changed' WHERE id='read'");
  assert.equal((await client.queries.find({id:'read'},{store:false})).entry?.text,'changed');assert.equal(queries,2);
  assert.equal((await client.queries.find({id:'read'},{store:false})).entry?.text,'changed');assert.equal(queries,3);
  assert.equal((await client.fetch.entry({id:'read'},{store:false}))?.text,'changed');assert.equal(await client.models.entry.get({id:'read'}),null);
  assert.equal((await client.fetch.entry({id:'read'}))?.text,'changed');assert.equal((await client.models.entry.get({id:'read'}))?.text,'changed');
  assert.equal((await client.queries.find({id:'missing'})).entry,null);
  assert.equal(await client.fetch.entry({id:'missing'}),null);
  invalidIdentity=true;
  await assert.rejects(client.queries.find({id:'read'},{store:false}));
  invalidIdentity=false;
  assert.equal((await client.models.entry.get({id:'read'}))?.text,'changed');
  if(process.env.AXTON_DART) { const output=await promisify(execFile)(process.env.AXTON_DART,['run','host.dart',server.url],{cwd:new URL('.',import.meta.url).pathname,timeout:30000});assert.match(output.stdout,/Dart generated real HTTP/); }
 } finally {await client.close();await server.close();await pool.end();await rm(dir,{recursive:true,force:true});}
});

test('generated Dart scalar codecs settle through actual retained host contracts',async()=>{
 if(!process.env.AXTON_DART) return;
 const {createBackend:createDateBackend}=await import('../action-runtime-dart/backend.ts');
 const pool=new Pool({connectionString:process.env.DATABASE_URL});
 const backend=createDateBackend<PgClient>({database:pg(pool),authenticate:devAuth(),protocol5:{authorizeStream:(viewer,stream)=>stream===`User:${viewer}`},mutations:{
  echo:async({args})=>({result:args.at,moods:args.moods,maybe:args.maybe}),
  touch:async({args})=>({stamp:args.note.at}),ping:async()=>{},
 },queries:{now:async({args})=>({at:args.at}),notesSince:async()=>({notes:[],pinned:[]})},loaders:{note:async({ids})=>ids.map(()=>null)},bootstrap:async()=>{}});
 const server=await backend.listen({port:0});
 try {const result=await promisify(execFile)(process.env.AXTON_DART,['run','host.dart',server.url],{cwd:new URL('../action-runtime-dart/',import.meta.url).pathname,timeout:30000});assert.match(result.stdout,/DateTime\/enum real host: PASS/);}
 finally {await server.close();await pool.end();}
});
