import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WebSocketServer } from 'ws';
import { Client } from './index.mts';

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
 const page=(context,from,to,changes)=>({context,pageId:`page-${from}-${to}`,from,to,head:to,units:from===to?[]:[{through:to,changes}]});
 const upsert=(cursor,state)=>({kind:'upsert',record:{cursor,model:'Entry',identity:{id:'e'},state}});
 let context,socket;
 const server=createServer(async(request,response)=>{
  let text='';for await(const chunk of request)text+=chunk;
  const body=JSON.parse(text);envelopes.push(body);context=body.context;
  if(request.url==='/sync/fetch') response.end(JSON.stringify({context,completion:{callId:body.callId,outcome:{status:'succeeded',result:{id:'e',text:'read',note:null}}},records:[{model:'Entry',identity:{id:'e'},cursor:null,state:{text:'read',note:null}}]}));
  else if(body.kind==='start')response.end(JSON.stringify({context,manifestId:'fixed',start:0,total:0}));
  else if(body.kind==='tail')response.end(JSON.stringify({context,manifestId:'fixed',head:1}));
  else response.end(JSON.stringify(body.after===0?page(context,0,1,[upsert(1,{text:'held',note:null})]):page(context,body.after,body.after,[])));
 });
 const ws=new WebSocketServer({server});ws.on('connection',current=>{socket=current;current.on('message',text=>{const body=JSON.parse(text.toString());envelopes.push(body);context=body.context;current.send(JSON.stringify({context,cursor:body.cursor,head:1}));});});
 server.listen(0,'127.0.0.1');await once(server,'listening');
 const client=await Client.open({path:join(dir,'db'),schema,stream:'User:viewer',connection:{url:`http://127.0.0.1:${server.address().port}`,token:'secret',identity:{backend:'scope',viewer:'viewer',contract:'v04'},options:{onError:error=>errors.push(error)}}});
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
  assert.ok(envelopes.every(x=>x.context?.binding.stream==='User:viewer'));assert.deepEqual(errors,[]);
 } finally {await client.close();for(const current of ws.clients)current.terminate();await new Promise(resolve=>ws.close(resolve));await new Promise(resolve=>server.close(resolve));await rm(dir,{recursive:true,force:true});}
});
