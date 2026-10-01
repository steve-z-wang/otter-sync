import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import WebSocket, { WebSocketServer } from 'ws';
import { createClient } from '../../../packages/client-js/runtime.mts';
import { Transaction } from '../../../packages/client-react-native/transaction.mts';
import { createServerConnection } from '../../../packages/client-react-native/live.mts';

// Node test carrier for the RN socket event API; real sockets and Rust/SQLite underneath.
class NativeSocket extends WebSocket {
  constructor(url, protocols, options) { super(url,protocols,options); this.on('error',()=>{}); }
}
const native=createRequire(import.meta.url)('../../../bindings/node/axton-node.node');
const Client=createClient(native,Transaction,options=>createServerConnection(options,NativeSocket));
async function until(predicate){
  const deadline=Date.now()+5000;
  while(Date.now()<deadline){if(await predicate())return;await new Promise(r=>setTimeout(r,5));}
  throw Error('condition timed out');
}

test('mobile transport authenticates real HTTP/WS and streams without polling',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'axton-rn-network-'));
  const schema=JSON.parse(await readFile(new URL('../../../fixtures/schemas/entry.json',import.meta.url),'utf8'));
  const client=await Client.open({path:join(directory,'client.sqlite'),schema});
  const requests=[];
  const http=createServer(async(req,res)=>{
    const chunks=[];for await(const chunk of req)chunks.push(chunk);
    const body=JSON.parse(Buffer.concat(chunks));
    requests.push({authorization:req.headers.authorization,url:req.url,body});
    res.end(JSON.stringify({cursors:Object.fromEntries(Object.entries(body.cursors).map(([c,n])=>[c,{from:n,to:Math.max(n,1),head:Math.max(n,1)}])),changes:[]}));
  });
  await new Promise(r=>http.listen(0,'127.0.0.1',r));
  const sockets=new WebSocketServer({server:http});
  let peer,authorization,head=0;
  sockets.on('connection',(socket,request)=>{
    peer=socket;authorization=request.headers.authorization;
    socket.on('message',message=>{
      const sub=JSON.parse(message);
      socket.send(JSON.stringify({type:'subscribed',cursors:Object.fromEntries(sub.scopes.map(c=>[c,head]))}));
    });
  });
  try{
    await client.subscribe('scope');
    const connection=await client.connect({url:`http://127.0.0.1:${http.address().port}`,token:'alice'});
    // The first handshake is this subscription's origin: it commits head 0 and
    // fetches no history. The server then publishes while the socket is closed,
    // so the next handshake is above the cursor and one HTTP catch-up follows.
    await until(async()=>(await client.syncState()).cursors.scope===0);
    assert.equal(authorization,'Bearer alice');
    assert.equal(requests.length,0,'no history is pulled for a fresh subscription');
    await connection.pause();await until(()=>sockets.clients.size===0);
    head=1;await connection.resume();
    await until(()=>requests.length===1);
    assert.equal(authorization,'Bearer alice');
    assert.equal(requests[0].authorization,'Bearer alice');
    assert.equal(requests[0].url,'/sync/pull');
    assert.deepEqual(requests[0].body.cursors,{scope:0},'the catch-up starts at the committed cursor');
    await until(async()=>(await client.syncState()).cursors.scope===1);
    const change={cursors:{scope:{from:1,to:2,head:2}},changes:[{scope:'scope',cursor:2,kind:'upsert',model:'Entry',identity:{id:'one'},stamp:1,state:{text:'live',note:null}}]};
    peer.send(JSON.stringify(change));
    await until(async()=>(await client.read('Entry',{id:'one'}))?.text==='live');
    peer.send(JSON.stringify(change));
    await new Promise(r=>setTimeout(r,100));
    assert.equal(requests.length,1,'ordinary/duplicate live pages must not issue pull requests');
    assert.equal((await client.syncState()).cursors.scope,2);
    await client.close();
    const before=requests.length;
    await new Promise(r=>setTimeout(r,50));
    assert.equal(requests.length,before,'closed connection still issued work');
  }finally{
    await client.close();for(const socket of sockets.clients)socket.terminate();
    await new Promise(r=>sockets.close(r));await new Promise(r=>http.close(r));
    await rm(directory,{recursive:true,force:true});
  }
});
