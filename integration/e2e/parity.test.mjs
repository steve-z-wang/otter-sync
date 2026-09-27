import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {createExample} from './fixtures/round-trip/server.mts';
import {Client} from '../../packages/client-js/index.mts';

// One script, two runtimes, one server: the Node client and the Dart client each
// run the same operations (catch up, an accepted edit, a rejected edit, a direct
// local create) and dump the same normalized view of their local state. The
// dumps must be identical. Cursors and client ids are excluded: they legitimately
// differ between two clients. parity_client.dart is the Dart half.
//
// Each runtime starts from the same published state: `reseed` puts entry-1 back to
// its initial text and notifies the channel, so the second runtime does not
// inherit the first one's result. A subscription's origin is the first head its
// handshake acknowledges (#150), so each runtime signals `READY` once it is
// initialized and `reseed` runs then; nothing published earlier would reach it,
// and #151 owns loading a Scope's history explicitly. Each dump also records what
// the runtime saw after catch-up and after the accepted edit, and those are
// asserted per runtime, so a runtime that advanced its cursor while keeping old
// content cannot pass by agreeing with the other one.
const INITIAL='Hello from the server';
async function reseed(app){
 await app.backend.transaction(async({tx,touch})=>{
  await tx.entry.update({where:{id:'entry-1'},data:{text:INITIAL}});
  touch.entry({id:'entry-1'});
 });
 assert.equal((await app.db.entry.findUnique({where:{id:'entry-1'}})).text,INITIAL);
}
const edit=text=>({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text}}]});
const waitFor=async(condition,label)=>{for(let i=0;i<1000;i++){if(await condition())return;await new Promise(r=>setTimeout(r,10));}throw Error(`timed out waiting for ${label}`);};

async function nodeScript(url,directory,schema,ready){
 const client=await Client.open({path:join(directory,'parity-node.sqlite'),schema});
 try{
  const subscription=await client.subscribe('book:demo');
  const connection=await client.connect({url,token:'demo-user'});
  const settled=async()=>(await client.syncState()).pending===0;
  await waitFor(async()=>subscription.status.initialization==='ready','first initialization');
  assert.equal(await client.read('Entry',{id:'entry-1'}),null,'node: subscribing loaded no earlier record');
  await ready();
  await waitFor(async()=>(await client.read('Entry',{id:'entry-1'}))!==null,'initial catch-up');
  const initial=(await client.read('Entry',{id:'entry-1'})).text;
  await client.mutate(edit('  parity  '));await waitFor(settled,'accepted edit');
  const afterAccepted=(await client.read('Entry',{id:'entry-1'})).text;
  await client.mutate(edit('reject'));await waitFor(settled,'rejected edit');
  await client.transaction(tx=>tx.direct({model:'Entry',op:'create',identity:{id:'local-only'},values:{text:'local',note:null}}));
  await connection.close();
  const entries=(await client.query('Entry')).sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0);
  const status=await client.syncState();
  return {
   initial,afterAccepted,
   entries:entries.map(row=>({id:row.id,text:row.text,note:row.note})),
   pending:status.pending,beforeImages:status.beforeImages,channels:status.channels,rejections:status.rejections,
   entry1:await client.syncState('Entry',{id:'entry-1'}),
   localOnly:await client.syncState('Entry',{id:'local-only'}),
  };
 }finally{await client.close();}
}

async function dartScript(url,directory,ready){
 const root=fileURLToPath(new URL('../..',import.meta.url));
 let output='';
 const code=await new Promise((resolve,reject)=>{
  const child=spawn('dart',[`--packages=${join(root,'packages/dart/.dart_tool/package_config.json')}`,join(root,'integration/e2e/parity_client.dart'),url,directory],{cwd:join(root,'packages/dart'),env:{...process.env,AXTON_LIBRARY:process.env.AXTON_LIBRARY ?? join(root,`target/debug/libaxton_dart.${process.platform==='darwin'?'dylib':'so'}`)},stdio:['ignore','pipe','inherit']});
  let waiting=ready;
  child.stdout.on('data',data=>{output+=data;if(waiting&&output.includes('READY\n')){const publish=waiting;waiting=undefined;Promise.resolve(publish()).catch(reject);}});
  child.on('error',reject);child.on('exit',resolve);
 });
 assert.equal(code,0,`Dart parity client exited ${code}: ${output}`);
 const line=output.split('\n').find(l=>l.startsWith('PARITY '));
 assert.ok(line,`no PARITY line in Dart output: ${output}`);
 return JSON.parse(line.slice('PARITY '.length));
}

test('the Node and Dart clients reach identical local state from one script against one server',{timeout:60000},async()=>{
 const app=await createExample();const directory=await mkdtemp(join(tmpdir(),'axton-parity-'));let server;
 try{
  await app.initialize();server=await app.listen(0);
  const node=await nodeScript(server.url,directory,app.schema,()=>reseed(app));
  const dart=await dartScript(server.url,directory,()=>reseed(app));
  for(const [runtime,dump] of [['node',node],['dart',dart]]){
   assert.equal(dump.initial,INITIAL,`${runtime}: catch-up delivered the seeded value`);
   assert.equal(dump.afterAccepted,'parity',`${runtime}: the accepted edit's normalized value came back from the server`);
  }
  assert.deepEqual(dart,node,'the two runtimes disagree on local state after the same script');
  // Guard against agreeing on the wrong thing: the script's outcomes are visible.
  assert.equal(node.entries.find(e=>e.id==='entry-1').text,'parity');
  assert.equal(node.entries.find(e=>e.id==='local-only').text,'local');
  assert.equal(node.pending,0);
  assert.equal(node.rejections.length,1);assert.equal(node.rejections[0].code,'entry.denied');
  assert.equal((await app.db.entry.findUnique({where:{id:'entry-1'}})).text,'parity');
 }finally{await server?.close();await app.close();await rm(directory,{recursive:true,force:true});}
});
