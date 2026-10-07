import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WebSocketServer } from 'ws';
import { Client } from './index.mts';
import { delivery05, emptyHandshake, read05 } from '../../integration/bindings/client-js/store-fixture.mjs';

async function until(predicate) {
  const end = Date.now() + 5000;
  while (!await predicate()) {
    if (Date.now() > end) throw Error('protocol test timed out');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

test('bound Stream Remove retains guards and newer canonical null protects deletion', async () => {
 const dir=await mkdtemp(join(tmpdir(),'axton-sdk-scope-'));
 const schema=JSON.parse(await readFile(new URL('../../fixtures/schemas/entry.json',import.meta.url),'utf8'));
 const envelopes=[],errors=[];
 const page=(context,from,to,changes)=>delivery05({...context,bootstrap:false,after:from,through:to},changes);
 const upsert=(cursor,state)=>({kind:'record',cursor,key:{model:'Entry',identity:{id:'e'}},state});
 let context,socket;
 const server=createServer(async(request,response)=>{
  let text='';for await(const chunk of request)text+=chunk;
  const body=JSON.parse(text);envelopes.push(body);
  if(request.url==='/sync/handshake') response.end(JSON.stringify({...emptyHandshake(body),head:1}));
  else if(request.url==='/sync/fetch') response.end(JSON.stringify(read05(body,{id:'e',text:'read',note:null},[{key:{model:'Entry',identity:{id:'e'}},cursor:null,state:{text:'read',note:null}}])));
  else {
    context={protocol:body.protocol,storeId:body.storeId,stream:body.stream,materialization:body.materialization};
    response.end(JSON.stringify(delivery05(body,body.after===0&&body.through===1?[upsert(1,{text:'held',note:null})]:[])));
  }
 });
 const ws=new WebSocketServer({server});ws.on('connection',current=>{socket=current;current.on('message',text=>{const body=JSON.parse(text.toString());envelopes.push(body);current.send(JSON.stringify({...emptyHandshake(body),head:1}));});});
 server.listen(0,'127.0.0.1');await once(server,'listening');
 const client=await Client.open({path:join(dir,'db'),schema,stream:'User:viewer',connection:{url:`http://127.0.0.1:${server.address().port}`,token:'secret',options:{onError:error=>errors.push(error)}}});
 try {
  await Promise.race([client.bootstrap(),new Promise((_,reject)=>setTimeout(()=>reject(Error('Bootstrap timeout')),5000).unref())]);await until(()=>socket&&context);
  assert.equal((await client.read('Entry',{id:'e'})).text,'held');
  socket.send(JSON.stringify(page(context,1,2,[{kind:'remove',cursor:2,key:{model:'Entry',identity:{id:'e'}}}])));
  await until(async()=> (await client.syncState()).cursors['User:viewer']===2);
  assert.equal((await client.read('Entry',{id:'e'})).text,'held');
  await client.fetchModel('Entry',1,{id:'e'},value=>value);
  assert.equal((await client.read('Entry',{id:'e'})).text,'read','Remove releases live-content protection');
  socket.send(JSON.stringify(page(context,2,3,[upsert(3,null)])));
  await until(async()=> (await client.syncState()).cursors['User:viewer']===3);
  assert.equal(await client.read('Entry',{id:'e'}),null);
  assert.equal((await client.fetchModel('Entry',1,{id:'e'},value=>value)).text,'read');
  assert.equal(await client.read('Entry',{id:'e'}),null,'ordinary snapshot cannot resurrect protected absence');
  assert.ok(envelopes.every(x=>x.stream==='User:viewer' && x.protocol===5));assert.deepEqual(errors,[]);
 } finally {await client.close();for(const current of ws.clients)current.terminate();await new Promise(resolve=>ws.close(resolve));await new Promise(resolve=>server.close(resolve));await rm(dir,{recursive:true,force:true});}
});
