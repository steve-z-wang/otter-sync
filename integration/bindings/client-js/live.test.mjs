import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { WebSocketServer } from 'ws';
import * as runtime from '../../../packages/client-js/index.mts';
import {createServerConnection} from '../../../packages/client-js/live.mts';

const timeout = (p) => Promise.race([p, new Promise((_, reject) => { const t = setTimeout(() => reject(Error('timeout')), 3000); t.unref(); })]);

const subscribe = JSON.stringify({type:'subscribe',streams:['scope']});
// The acknowledgement carries every scope's head; `heads` is the fake server's state.
const ack = (sub, heads={}) => JSON.stringify({type:'subscribed',cursors:Object.fromEntries(sub.streams.map(c=>[c,heads[c]??0]))});
const handlers = (over = {}) => ({ message: async () => {}, overflow: async () => {}, closed: () => {}, ...over });

test('internal socket sends the subscribe frame, delivers frames in order, and cancellation ends the socket', async () => {
  assert.equal(typeof createServerConnection, 'function');
  const server = new WebSocketServer({port:0});
  await once(server,'listening');
  const abort = new AbortController();
  const frames = [];
  const connected = once(server,'connection');
  const live = createServerConnection({url:`http://127.0.0.1:${server.address().port}`,token:'secret'});
  live.open(subscribe, abort.signal, handlers({ message: async text => { frames.push(JSON.parse(text)); } }));
  try {
    const [socket, request] = await timeout(connected);
    assert.equal(request.headers.authorization,'Bearer secret');
    const [message] = await timeout(once(socket,'message'));
    assert.deepEqual(JSON.parse(message),{type:'subscribe',streams:['scope']});
    const closed = once(socket,'close');
    socket.send(ack(JSON.parse(message)));
    socket.send(JSON.stringify({cursors:{scope:{from:12,to:13,head:13}},changes:[]}));
    await timeout(new Promise(resolve => { const check = () => frames.length === 2 ? resolve() : setImmediate(check); check(); }));
    assert.equal(frames[0].type,'subscribed', 'the transport does not interpret frames');
    assert.equal(frames[1].cursors.scope.to,13);
    abort.abort();
    await timeout(closed);
  } finally { abort.abort(); for (const s of server.clients) s.terminate(); await new Promise(r => server.close(r)); }
});

test('live transport cancellation does not wait for a stalled token', async () => {
  const abort = new AbortController();
  let closed = 0;
  const live = createServerConnection({url:'http://127.0.0.1:1',token:()=>new Promise(()=>{})});
  live.open(subscribe, abort.signal, handlers({ closed: () => { closed++; } }));
  abort.abort();
  await new Promise(r => setTimeout(r, 20));
  assert.equal(closed, 0, 'an aborted socket is not reported as closed');
});

test('invalid WebSocket credentials reject the session rather than leaking a rejected task', async () => {
 const live = createServerConnection({url:'http://127.0.0.1:1',token:'invalid\nheader'});
 const failure = new Promise(resolve => live.open(subscribe, new AbortController().signal, handlers({ closed: resolve })));
 assert.match(String((await timeout(failure)).message), /header|character/i);
});

import { createServer } from 'node:http';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
async function openClient() {
 const dir = await mkdtemp(join(tmpdir(),'axton-live-'));
 const schema = JSON.parse(await readFile(new URL('../../../fixtures/schemas/entry.json',import.meta.url),'utf8'));
 const client = await runtime.Client.open({path:join(dir,'client.sqlite'),schema});
 return {client, async close() { await client.close(); await rm(dir,{recursive:true,force:true}); }};
}
// `stamp` defaults to the cursor; a page for a record the client already holds at that
// stamp must carry a newer one, because retained content is compared by stamp alone.
const page = (text, cursor=0, stamp=cursor+1, head=cursor+1) => ({cursors:{scope:{from:cursor,to:cursor+1,head}},changes:[{stream:'scope',cursor:cursor+1,kind:'upsert',model:'Entry',identity:{id:'live'},stamp,state:{text,note:null}}]});
// An HTTP answer that moves every requested scope to `head` with no changes.
const emptyPage=(b,head)=>({cursors:Object.fromEntries(Object.entries(b.cursors).map(([c,n])=>[c,{from:n,to:Math.max(n,head?.[c]??n),head:Math.max(n,head?.[c]??n)}])),changes:[]});
// A fake push receipt in the wire shape the client accepts: it answers the batch it
// was asked (clientId and batchSequence echoed) and carries the authoritative state
// of every record the batch's wire operations target, once per record, at a stamp
// the fake server hands out monotonically. The "server" normalizes text by trimming
// it, so a test can tell the receipt's content from the client's prediction.
function receiptFor(body, stamps) {
 const records=new Map();
 for (const mutation of body.mutations) for (const op of mutation.operations ?? []) {
  const state=op.op==='delete'?null:{text:String(op.values?.text ?? '').trim(),note:op.values?.note ?? null};
  records.set(`${op.model}\0${JSON.stringify(op.identity)}`,{model:op.model,identity:op.identity,stamp:++stamps.next,state});
 }
 return {clientId:body.clientId,batchSequence:body.batchSequence,rejections:[],records:[...records.values()]};
}
async function until(predicate) {
 const deadline=Date.now()+5000;
 while(Date.now()<deadline) { if(await predicate()) return; await new Promise(r=>setTimeout(r,5)); }
 throw Error('condition timed out');
}

// A subscription initializes at the head its first handshake acknowledges, so
// every catch-up here is established the way a client meets one: the server
// publishes while the socket is closed, and the next handshake is above the
// cursor the last one committed.
test('client replaces subscriptions from saved cursors and guards queued obsolete pages', async()=>{
 const fixture=await openClient(); const {client}=fixture; const errors=[];
 const pulls=[];let answer='empty';const heads={scope:0};
 const http=createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);const body=JSON.parse(Buffer.concat(chunks));
  if(body.mode==='bootstrap'){res.end(JSON.stringify({mode:'bootstrap',stream:body.stream,from:body.after,to:body.until,until:body.until,head:heads.scope,changes:body.until>body.after?[page('first').changes[0]]:[]}));return;}
  pulls.push(body);
  if(answer==='published')res.end(JSON.stringify({cursors:{scope:{from:body.cursors.scope,to:3,head:3}},changes:[{stream:'scope',cursor:3,kind:'upsert',model:'Entry',identity:{id:'live'},stamp:4,state:{text:'published while offline',note:null}}]}));
  else if(answer==='recovered')res.end(JSON.stringify({cursors:{scope:{from:body.cursors.scope,to:11,head:11}},changes:[page('recovered',10).changes[0]]}));
  else res.end(JSON.stringify(emptyPage(body,heads)));});
 await new Promise(r=>http.listen(0,'127.0.0.1',r));
 const server=new WebSocketServer({server:http});
 const sockets=[]; const handshakes=[];
 server.on('connection',s=>{sockets.push(s);s.on('message',m=>{const sub=JSON.parse(m);handshakes.push(sub);s.send(ack(sub,heads));});});
 try {
  const connection=await client.connect({url:`http://127.0.0.1:${http.address().port}`,token:'secret'},{onError:e=>errors.push(e)});
  await client.subscribe('scope'); await until(()=>handshakes.length===1);
  await until(async()=>(await client.syncState()).cursors.scope===0,'the acknowledged head is the first boundary');
  heads.scope=1;sockets[0].send(JSON.stringify(page('first')));
  await until(async()=>(await client.read('Entry',{id:'live'}))?.text==='first');
  assert.equal(pulls.length,0,'at the head: the acknowledgement starts no catch-up');
  const gate=Promise.withResolvers(),entered=Promise.withResolvers();
  const tx=client.transaction(async()=>{entered.resolve();await gate.promise;});await entered.promise;
  // Obsolescence follows admission order: queue replacement before the frame.
  const removed=client.unsubscribe('scope');const restored=client.subscribe('scope');
  sockets[0].send(JSON.stringify(page('stale',1)));
  gate.resolve();await tx;await removed;await restored;
  await until(()=>handshakes.length>=2);
  assert.equal((await client.read('Entry',{id:'live'})).text,'first','unsubscribing retains the downloaded record; the queued obsolete page is dropped, not applied');
  // The recreated subscription starts at the head its own handshake acknowledged.
  await until(async()=>(await client.syncState()).cursors.scope===1);
  await new Promise(r=>setTimeout(r,50));assert.equal(pulls.length,0,'a recreated subscription loads no history');
  // The record is retained at stamp 1: the fresh page needs a newer stamp to replace it.
  heads.scope=2;sockets.at(-1).send(JSON.stringify(page('fresh',1,3)));
  await until(async()=>(await client.read('Entry',{id:'live'}))?.text==='fresh');
  await connection.pause();await until(()=>server.clients.size===0);
  await connection.resume();await until(()=>handshakes.length>=3);
  await new Promise(r=>setTimeout(r,50));assert.equal(pulls.length,0,'heads equal to the cursors: no catch-up on reconnect');
  // The server publishes while the socket is closed: the next handshake
  // acknowledges 3 over the saved cursor 2, and the gap is fetched from the
  // cursor, not replaced by the head.
  await connection.pause();await until(()=>server.clients.size===0);
  heads.scope=3;answer='published';
  await connection.resume();await until(()=>pulls.length===1);
  assert.deepEqual(pulls[0].cursors,{scope:2},'the catch-up starts at the saved cursor');
  await until(async()=>(await client.read('Entry',{id:'live'}))?.text==='published while offline');
  await until(async()=>(await client.syncState()).cursors.scope===3);
  assert.equal(errors.length,0);
  answer='recovered';sockets.at(-1).send(JSON.stringify(page('gap',10)));await until(async()=>(await client.read('Entry',{id:'live'}))?.text==='recovered');
  assert.deepEqual(pulls.at(-1).cursors,{scope:3},'a stream gap is repaired from the durable cursor');assert.equal(errors.length,0);
  await connection.close();await until(()=>server.clients.size===0);
 } finally { await fixture.close(); for(const s of server.clients)s.terminate();await new Promise(r=>server.close(r));await new Promise(r=>http.close(r)); }
});

test('client retries upgrade authentication and survives failed refresh',async()=>{
 const fixture=await openClient();const {client}=fixture;const errors=[];
 const server=createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);const b=JSON.parse(Buffer.concat(chunks));res.end(JSON.stringify(emptyPage(b)));});const ws=new WebSocketServer({noServer:true});
 let token='expired', refreshes=0, accepted=0;
 server.on('upgrade',(req,socket,head)=>{if(req.headers.authorization!=='Bearer valid'){socket.end('HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n');return;}ws.handleUpgrade(req,socket,head,s=>{accepted++;s.on('message',m=>s.send(ack(JSON.parse(m))));});});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try {
  await client.subscribe('scope');
  await client.connect({url:`http://127.0.0.1:${server.address().port}`,token:()=>token},{onError:e=>errors.push(e),refreshAuth:async()=>{if(++refreshes===1)throw Error('refresh failed');token='valid';}});
  await until(()=>accepted===1);
  assert.equal(refreshes,2);assert.ok(errors.some(e=>e.message==='refresh failed'));
 } finally {await fixture.close();for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>server.close(r));}
});

test('close cancels opening handshake and ignores its eventual server response',async()=>{
 const fixture=await openClient();const server=createServer();const entered=Promise.withResolvers();let pending;
 server.on('upgrade',(_req,socket)=>{pending=socket;socket.resume();socket.on('end',()=>socket.destroy());entered.resolve();});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try {
  await fixture.client.subscribe('scope');
  const connection=await fixture.client.connect({url:`http://127.0.0.1:${server.address().port}`,token:'secret'});
  await timeout(entered.promise);const closed=once(pending,'close');
  await timeout(connection.close());await timeout(closed);
 }finally{await fixture.close();pending?.destroy();await new Promise(r=>server.close(r));}
});

test('client close abandons a stalled live token and never opens after it resolves',async()=>{
 const fixture=await openClient();let requests=0;const token=Promise.withResolvers();const called=Promise.withResolvers();
 const server=new WebSocketServer({port:0});await once(server,'listening');server.on('connection',()=>requests++);
 try {
  await fixture.client.subscribe('scope');
  await fixture.client.connect({url:`http://127.0.0.1:${server.address().port}`,token:()=>{called.resolve();return token.promise;}});
  await timeout(called.promise);await timeout(fixture.client.close());token.resolve('late');
  await new Promise(r=>setImmediate(r));assert.equal(requests,0);
 }finally{await fixture.close();for(const s of server.clients)s.terminate();await new Promise(r=>server.close(r));}
});

test('unified connection acknowledges listeners then catches up through HTTP before live delivery', async()=>{
 const fixture=await openClient();const events=[];let pulls=0;
 const server=createServer(async(req,res)=>{
  const chunks=[];for await(const chunk of req)chunks.push(chunk);
  const body=JSON.parse(Buffer.concat(chunks));
  assert.equal(req.url,'/sync/pull');assert.ok(events.includes('ack'));
  pulls++;events.push(`pull:${body.cursors.scope}`);
  res.setHeader('content-type','application/json');res.end(JSON.stringify(page('caught up')));
 });
 const ws=new WebSocketServer({server});let socket;const heads={scope:0};
 ws.on('connection',s=>{socket=s;s.on('message',m=>{events.push('ack');s.send(ack(JSON.parse(m),heads));});});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try {
  await fixture.client.subscribe('scope');
  const options={url:`http://127.0.0.1:${server.address().port}`,token:'secret'};
  const connection=await fixture.client.connect(options);
  await until(()=>events.includes('ack'));
  await until(async()=>(await fixture.client.syncState()).cursors.scope===0,'the first boundary');
  assert.equal(pulls,0,'the acknowledged head is the origin: no history is fetched');
  // The server publishes while the socket is closed; the next handshake
  // acknowledges a head above the committed cursor.
  await connection.pause();await until(()=>ws.clients.size===0);
  heads.scope=1;
  await connection.resume();
  await until(()=>events.filter(e=>e==='ack').length===2);
  await new Promise(r=>setTimeout(r,100));
  assert.equal(pulls,1,'a head beyond the cursor in the acknowledgement starts one HTTP catch-up');
  await until(async()=>(await fixture.client.read('Entry',{id:'live'}))?.text==='caught up');
  socket.send(JSON.stringify(page('continuous',1)));
  await until(async()=>(await fixture.client.read('Entry',{id:'live'}))?.text==='continuous');
  await new Promise(r=>setTimeout(r,60));assert.equal(pulls,1,'ordinary live updates do not poll HTTP');
  await connection.close();
 } finally {await fixture.close();for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>server.close(r));}
});

// `handshakes` records every subscribe frame the fake server answered. A test
// that streams a page must wait for it: the acknowledgement is the first frame
// of a session, and a page that arrives before it is the protocol violation the
// client ends the session for.
async function syncFixture(onPull, heads={}) {
 const requests=[];const sockets=[];const handshakes=[];const stamps={next:0};
 const server=createServer(async(req,res)=>{
  const chunks=[];for await(const c of req)chunks.push(c);const body=JSON.parse(Buffer.concat(chunks));requests.push({url:req.url,body});
  if(req.url==='/sync/mutations')res.end(JSON.stringify(receiptFor(body,stamps)));
  else await onPull(body,res,requests.filter(r=>r.url==='/sync/pull').length);
 });
 const ws=new WebSocketServer({server});ws.on('connection',s=>{sockets.push(s);s.on('message',m=>{handshakes.push(JSON.parse(m));s.send(ack(JSON.parse(m),heads));});});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 return {requests,sockets,handshakes,heads,config:{url:`http://127.0.0.1:${server.address().port}`,token:'secret'},async close(){for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>server.close(r));}};
}
test('late HTTP catch-up after unsubscribe and resubscribe cannot resurrect the obsolete generation',async()=>{
 const fixture=await openClient();const network=await syncFixture((b,res)=>res.end(JSON.stringify(page('obsolete'))),{scope:0});
 const original=globalThis.fetch;const entered=Promise.withResolvers(),gate=Promise.withResolvers();let first=true;
 globalThis.fetch=async(...args)=>{const response=await original(...args);if(first){first=false;entered.resolve();await gate.promise;}return response;};
 try {
  await fixture.client.subscribe('scope');const connection=await fixture.client.connect(network.config);
  // The first session initializes at head 0; the server then publishes while
  // the socket is closed, so the next one starts the catch-up this test holds.
  await until(async()=>(await fixture.client.syncState()).cursors.scope===0,'the first boundary');
  await connection.pause();await until(()=>network.sockets.every(s=>s.readyState===s.CLOSED));
  network.heads.scope=1;await connection.resume();
  await timeout(entered.promise);await fixture.client.unsubscribe('scope');await fixture.client.subscribe('scope');
  // The recreated subscription initializes at the head of its own handshake
  // and the stream is the truth from there.
  await until(()=>network.handshakes.length>=3,'the third session is acknowledged before its stream carries a page');
  network.sockets.at(-1).send(JSON.stringify(page('fresh',1,3)));
  await until(async()=>(await fixture.client.read('Entry',{id:'live'}))?.text==='fresh');
  gate.resolve();await new Promise(r=>setTimeout(r,30));assert.equal((await fixture.client.read('Entry',{id:'live'})).text,'fresh');
  assert.equal((await fixture.client.syncState()).cursors.scope,2,'the obsolete answer moved no cursor');
 }finally{gate.resolve();globalThis.fetch=original;await fixture.close();await network.close();}
});

test('pause cancels held catch-up and a late HTTP token cannot start a request',async()=>{
 const fixture=await openClient();const network=await syncFixture((b,res)=>res.end(JSON.stringify(emptyPage(b,{scope:1}))),{scope:0});
 const called=Promise.withResolvers(),token=Promise.withResolvers();let calls=0;
 // The third token is the one the held catch-up needs: the first two open the
 // socket of the session that initializes at head 0 and of the one that finds
 // the head above it.
 try{
  await fixture.client.subscribe('scope');const connection=await fixture.client.connect({...network.config,token:()=>++calls===3?(called.resolve(),token.promise):'secret'});
  await until(async()=>(await fixture.client.syncState()).cursors.scope===0,'the first boundary');
  await timeout(connection.pause());await until(()=>network.sockets.every(s=>s.readyState===s.CLOSED));
  network.heads.scope=1;await timeout(connection.resume());
  await timeout(called.promise);await timeout(connection.pause());token.resolve('late');await new Promise(r=>setTimeout(r,30));
  assert.equal(network.requests.length,0,'a token that arrives after the pause cannot start the request');
  assert.equal((await fixture.client.syncState()).cursors.scope,0,'the held catch-up moved nothing');
  await connection.resume();await until(()=>network.requests.length===1);
  assert.deepEqual(network.requests[0].body.cursors,{scope:0},'the catch-up still starts at the committed cursor');
 }finally{token.resolve('late');await fixture.close();await network.close();}
});

test('one incoming page path covers duplicates, applies overlap directly and recovers genuine gaps',async()=>{
 const fixture=await openClient();let head=1;
 // This scenario is about the stream: the handshake acknowledges head 0, and
 // every page below arrives on the socket.
 const network=await syncFixture((b,res)=>res.end(JSON.stringify({cursors:{scope:{from:b.cursors.scope,to:head,head}},changes:[page(`HTTP ${head}`,head-1).changes[0]]})),{scope:0});
 try{
  await fixture.client.subscribe('scope');await fixture.client.connect(network.config);
  await until(async()=>(await fixture.client.syncState()).cursors.scope===0);
  network.sockets[0].send(JSON.stringify(page('first')));
  await until(async()=>(await fixture.client.syncState()).cursors.scope===1);
  network.sockets[0].send(JSON.stringify(page('duplicate')));await new Promise(r=>setTimeout(r,20));
  assert.equal(network.requests.length,0,'a page the cursor already covers needs no HTTP pull');
  assert.equal((await fixture.client.read('Entry',{id:'live'})).text,'first','the duplicate did not apply');
  head=2;network.sockets[0].send(JSON.stringify({cursors:{scope:{from:0,to:2,head:2}},changes:[page('overlap',1).changes[0]]}));
  await until(async()=>(await fixture.client.syncState()).cursors.scope===2);assert.equal(network.requests.length,0,'overlap must not issue an HTTP pull');assert.equal((await fixture.client.read('Entry',{id:'live'})).text,'overlap');
  head=4;network.sockets[0].send(JSON.stringify(page('gap',3)));
  await until(async()=>(await fixture.client.syncState()).cursors.scope===4);assert.deepEqual(network.requests.at(-1).body.cursors,{scope:2});assert.equal((await fixture.client.read('Entry',{id:'live'})).text,'HTTP 4');
 }finally{await fixture.close();await network.close();}
});

test('HTTP catch-up failures surface and retry without treating the failure as an empty page',async()=>{
 const fixture=await openClient();const errors=[];
 const network=await syncFixture((b,res,n)=>{if(n===1){res.statusCode=503;res.end('unavailable');}else res.end(JSON.stringify(page('retried')));},{scope:0});
 try{
  await fixture.client.subscribe('scope');const connection=await fixture.client.connect(network.config,{onError:e=>errors.push(e)});
  await until(async()=>(await fixture.client.syncState()).cursors.scope===0,'the first boundary');
  // Published while the socket was closed: the next handshake starts a catch-up.
  await connection.pause();await until(()=>network.sockets.every(s=>s.readyState===s.CLOSED));
  network.heads.scope=1;await connection.resume();
  await until(async()=>(await fixture.client.read('Entry',{id:'live'}))?.text==='retried');
  assert.equal(errors.length,1,`the failure is the only error: ${errors.map(e=>e.message)}`);assert.equal(errors[0].status,503);assert.deepEqual(network.requests[1].body.cursors,{scope:0},'the retry asks from the cursor, not from an assumed empty page');
 }finally{await fixture.close();await network.close();}
});

test('a reusable server config isolates cancellation and no-scope clients only push',async()=>{
 const a=await openClient(),b=await openClient();const network=await syncFixture((body,res)=>res.end(JSON.stringify(emptyPage(body))),{scope:0});
 try{
  const ca=await a.client.connect(network.config);await b.client.subscribe('scope');const cb=await b.client.connect(network.config);
  await until(()=>network.handshakes.length===1,'only the client with a Scope opens a socket, and its page follows the acknowledgement');
  network.sockets[0].send(JSON.stringify(page('shared')));
  await until(async()=>(await b.client.read('Entry',{id:'live'}))?.text==='shared');assert.equal(network.sockets.length,1);
  await a.client.mutate({name:'Create',operations:[{model:'Entry',op:'create',identity:{id:'local'},values:{text:'  push without scopes  ',note:null}}]});
  await until(async()=>(await a.client.syncState()).pending===0);assert.equal(network.requests.filter(r=>r.url==='/sync/mutations').length,1);assert.equal(network.sockets.length,1);
  assert.equal((await a.client.read('Entry',{id:'local'})).text,'push without scopes','the receipt alone completes the batch and the row shows the server-returned state; no scope is involved');
  assert.deepEqual(network.requests.find(r=>r.url==='/sync/mutations').body.models,{Entry:1},'the push declares the read contracts its receipt is served at');
  await ca.close();network.sockets[0].send(JSON.stringify(page('still connected',1)));
  await until(async()=>(await b.client.read('Entry',{id:'live'}))?.text==='still connected');await cb.close();
 }finally{await a.close();await b.close();await network.close();}
});

test('removed public modes fail clearly instead of silently opening local-only',async()=>{
 assert.equal(runtime.websocketTransport,undefined);assert.equal(runtime.httpTransport,undefined);assert.equal(runtime.Client.prototype.connectLive,undefined);assert.equal(runtime.Client.prototype.sync,undefined);
 const fixture=await openClient();try{await assert.rejects(fixture.client.connect(async()=>''),/requires server/);}finally{await fixture.close();}
});

// The SDK no longer cancels the socket before it writes: an unsubscribe is a
// local commit, and the Downlink worker decides what the committed membership
// means for the session it holds ([#150](https://github.com/zanminwang/axton/issues/150)).
test('an unsubscribe waiting on SQLite leaves the socket alone; the committed change ends it',async()=>{
 const fixture=await openClient();const network=await syncFixture((b,res)=>res.end(JSON.stringify(emptyPage(b))));
 const token=Promise.withResolvers(),called=Promise.withResolvers(),gate=Promise.withResolvers(),entered=Promise.withResolvers();
 try{
  await fixture.client.subscribe('scope');await fixture.client.connect({...network.config,token:()=>{called.resolve();return token.promise;}});
  await timeout(called.promise);
  const transaction=fixture.client.transaction(async()=>{entered.resolve();await gate.promise;});await entered.promise;
  const unsubscribe=fixture.client.unsubscribe('scope');token.resolve('secret');
  await until(()=>network.sockets.length===1,'the pending authentication still opens its socket');
  gate.resolve();await transaction;await unsubscribe;
  await until(()=>network.sockets.every(s=>s.readyState===s.CLOSED),'the committed change ends the session');
  await new Promise(r=>setTimeout(r,50));
  assert.equal(network.sockets.length,1,'with no Scope left the lane opens nothing');
  assert.deepEqual((await fixture.client.syncState()).streams,[]);
 }finally{gate.resolve();token.resolve('secret');await fixture.close();await network.close();}
});

test('bounded receive overflow preserves in-flight HTTP progress and recovers the latest head',async()=>{
 const fixture=await openClient();let head=1;const entered=Promise.withResolvers(),gate=Promise.withResolvers();
 const network=await syncFixture(async(b,res,n)=>{const response={cursors:{scope:{from:b.cursors.scope,to:head,head}},changes:[page(`head ${head}`,head-1).changes[0]]};if(n===1){entered.resolve();await gate.promise;}res.end(JSON.stringify(response));},{scope:0});
 try{
  await fixture.client.subscribe('scope');const connection=await fixture.client.connect(network.config);
  await until(async()=>(await fixture.client.syncState()).cursors.scope===0,'the first boundary');
  // The catch-up this test holds belongs to the session that finds head 1 over
  // the cursor the first one committed.
  await connection.pause();await until(()=>network.sockets.every(s=>s.readyState===s.CLOSED));
  network.heads.scope=1;await connection.resume();
  await timeout(entered.promise);
  const opened=network.sockets.length;
  const socket=network.sockets.at(-1);socket._socket.cork();for(let cursor=1;cursor<=200;cursor++)socket.send(JSON.stringify(page(`live ${cursor}`,cursor)));socket._socket.uncork();
  head=201;gate.resolve();await until(async()=>(await fixture.client.syncState()).cursors.scope===201);
  assert.equal(network.sockets.length,opened,'overflow must not restart and starve HTTP catch-up');
  assert.ok(network.requests.length<=4,`bounded queue coalesces recovery work: ${network.requests.length}`);
 }finally{gate.resolve();await fixture.close();await network.close();}
});

test('push completes from its receipt while the WebSocket upgrade is refused; HTTP catch-up runs only once the upgrade is allowed',async()=>{
 const fixture=await openClient();const {client}=fixture;const errors=[];
 let allowUpgrades=false,upgradeAttempts=0,pushes=0,pulls=0;const stamps={next:0};
 const server=createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);const body=JSON.parse(Buffer.concat(chunks));
  if(req.url==='/sync/mutations'){pushes++;res.end(JSON.stringify(receiptFor(body,stamps)));return;}
  // The catch-up page carries a stamp newer than the receipt's, so it is authority that updates the row.
  pulls++;const caught=page('from catch-up',body.cursors.scope);res.end(JSON.stringify({...caught,changes:[{...caught.changes[0],stamp:stamps.next+1}]}));});
 const ws=new WebSocketServer({noServer:true});
 const heads={scope:0};let acks=0;
 server.on('upgrade',(req,socket,head)=>{upgradeAttempts++;if(!allowUpgrades){socket.end('HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n');return;}ws.handleUpgrade(req,socket,head,s=>{s.on('message',m=>{acks++;s.send(ack(JSON.parse(m),heads));});});});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try{
  await client.transaction(tx=>tx.direct({model:'Entry',op:'create',identity:{id:'live'},values:{text:'local'}}));
  await client.subscribe('scope');
  await client.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'live'},values:{text:'  edited offline  '}}]});
  assert.equal((await client.read('Entry',{id:'live'})).text,'  edited offline  ','the local prediction is visible before the push');
  const connection=await client.connect({url:`http://127.0.0.1:${server.address().port}`,token:'secret'},{onError:e=>errors.push(e)});
  await until(()=>pushes===1&&upgradeAttempts>=2);
  assert.equal(pulls,0,'no HTTP catch-up runs without an acknowledged WebSocket: there is no polling fallback');
  await until(async()=>(await client.syncState()).pending===0);
  assert.equal(pulls,0,'the batch completed from its receipt alone: no page was delivered');
  assert.equal((await client.read('Entry',{id:'live'})).text,'edited offline','the row shows the server-returned state as soon as the response is applied');
  assert.ok(errors.some(e=>/live failed: 503/.test(String(e.message))),`upgrade refusals reach onError: ${errors.map(e=>e.message)}`);
  allowUpgrades=true;
  await until(()=>acks===1,'the upgrade is acknowledged once it is allowed');
  await until(async()=>(await client.syncState()).cursors.scope===0,'the first boundary is the acknowledged head');
  assert.equal(pulls,0,'an acknowledged session at its head still pulls no history');
  // The server publishes while the socket is closed: only then is there a gap
  // for HTTP to fetch.
  await connection.pause();await until(()=>ws.clients.size===0);
  heads.scope=1;await connection.resume();
  await until(async()=>(await client.read('Entry',{id:'live'})).text==='from catch-up');
  assert.equal(pushes,1,'the receipt was not re-requested');
  assert.ok(pulls>=1,'catch-up ran over HTTP once the upgrade was acknowledged');
  assert.equal((await client.syncState()).pending,0);
 }finally{await fixture.close();for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>server.close(r));}
});
test('a 401 on both lanes at once shares one refreshAuth; both lanes recover with the new token',async()=>{
 const fixture=await openClient();const {client}=fixture;const errors=[];
 let token='expired',refreshes=0,unauthorized=0,pushes=0,accepted=0,release;const gate=new Promise(r=>{release=r;});const stamps={next:0};
 const server=createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);const body=JSON.parse(Buffer.concat(chunks));
  if(req.headers.authorization!=='Bearer valid'){unauthorized++;res.statusCode=401;res.end();return;}
  // The receipt names no scope: the push completes on its own, whatever the live lane is doing.
  if(req.url==='/sync/mutations'){pushes++;res.end(JSON.stringify(receiptFor(body,stamps)));return;}
  res.end(JSON.stringify(emptyPage(body)));});
 const ws=new WebSocketServer({noServer:true});
 server.on('upgrade',(req,socket,head)=>{if(req.headers.authorization!=='Bearer valid'){unauthorized++;socket.end('HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n');return;}ws.handleUpgrade(req,socket,head,s=>{accepted++;s.on('message',m=>s.send(ack(JSON.parse(m))));});});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try{
  await client.transaction(tx=>tx.direct({model:'Entry',op:'create',identity:{id:'live'},values:{text:'local'}}));
  await client.subscribe('scope');
  await client.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'live'},values:{text:'edited offline'}}]});
  await client.connect({url:`http://127.0.0.1:${server.address().port}`,token:()=>token},{onError:e=>errors.push(e),refreshAuth:async()=>{refreshes++;await gate;token='valid';}});
  await until(()=>unauthorized>=2);
  await new Promise(r=>setTimeout(r,100));
  assert.equal(unauthorized,2,'each lane was refused once and neither retried while the refresh was pending');
  assert.equal(refreshes,1,'the second lane joined the pending refresh instead of starting another');
  release();
  await until(()=>accepted>=1&&pushes>=1);
  await until(async()=>(await client.syncState()).pending===0);
  assert.equal((await client.read('Entry',{id:'live'})).text,'edited offline','the receipt completed the batch without any page');
  assert.equal(refreshes,1,'no further refresh once the token is valid');
  assert.equal(unauthorized,2);
 }finally{await fixture.close();for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>server.close(r));}
});
test('a socket the server closes is reconnected after the backoff, resubscribed, and streaming resumes',async()=>{
 const fixture=await openClient();const {client}=fixture;const errors=[];const t0=Date.now();const upgrades=[];const subscribes=[];const sockets=[];
 const server=createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);const body=JSON.parse(Buffer.concat(chunks));res.end(JSON.stringify(emptyPage(body)));});
 const ws=new WebSocketServer({noServer:true});
 server.on('upgrade',(req,socket,head)=>{upgrades.push(Date.now()-t0);ws.handleUpgrade(req,socket,head,s=>{sockets.push(s);s.on('message',m=>{subscribes.push(JSON.parse(m));s.send(ack(JSON.parse(m)));});});});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try{
  await client.transaction(tx=>tx.direct({model:'Entry',op:'create',identity:{id:'live'},values:{text:'local'}}));
  await client.subscribe('scope');
  await client.connect({url:`http://127.0.0.1:${server.address().port}`,token:'secret'},{onError:e=>errors.push(e)});
  await until(()=>subscribes.length===1);
  const closedAt=Date.now()-t0;sockets[0].close(1001,'closing');
  await until(()=>upgrades.length===2);
  const waited=upgrades[1]-closedAt;
  assert.ok(waited>=180,`the reconnect waited ${waited} ms; the first retry is due 250 ms later, minus 20% jitter`);
  assert.ok(errors.some(e=>/live disconnected: 1001/.test(String(e.message))),`the close reaches onError: ${errors.map(e=>e.message)}`);
  await until(()=>subscribes.length===2);
  assert.deepEqual(subscribes[1],{type:'subscribe',streams:['scope'],models:{Entry:1},capabilities:['stream-authority-v1']},'the new socket subscribes again without an application event, declaring its read contracts');
  sockets[1].send(JSON.stringify(page('after reconnect',0)));
  await until(async()=>(await client.read('Entry',{id:'live'}))?.text==='after reconnect');
  assert.equal(upgrades.length,2,'one reconnect; no busy loop');
 }finally{await fixture.close();for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>server.close(r));}
});

test('an owner-mismatch refusal reaches onError and leaves the batch frozen for a resend',async()=>{
 const fixture=await openClient();const {client}=fixture;const errors=[];const bodies=[];
 const server=createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);const body=JSON.parse(Buffer.concat(chunks));
  bodies.push(body);res.statusCode=403;res.setHeader('content-type','application/json');res.end(JSON.stringify({code:'client.owner_mismatch'}));});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try{
  await client.mutate({name:'Create',operations:[{model:'Entry',op:'create',identity:{id:'live'},values:{text:'local',note:null}}]});
  assert.equal((await client.syncState()).pending,1);
  await client.connect({url:`http://127.0.0.1:${server.address().port}`,token:'secret'},{onError:e=>errors.push(e)});
  await until(()=>bodies.length>=2,'the frozen batch is resent after the refusal');
  assert.ok(errors.some(e=>/client\.owner_mismatch/.test(String(e.message))),`the refusal's code reaches onError: ${errors.map(e=>e.message)}`);
  assert.equal((await client.syncState()).pending,1,'the refused batch stays pending, not dropped or completed');
  assert.deepEqual(bodies[1],bodies[0],'the same request body is resent on the next cycle');
 }finally{await fixture.close();await new Promise(r=>server.close(r));}
});

test('what a page cannot apply reaches onError as an AxtonReport: read failures, skipped changes and divergence',async()=>{
 const fixture=await openClient();const {client}=fixture;const errors=[];
 const network=await syncFixture((b,res)=>res.end(JSON.stringify(emptyPage(b))));
 // The push lane is refused so the local edit stays queued while authority lands beneath it.
 const original=network.requests;
 try{
  await client.subscribe('scope');await client.connect(network.config,{onError:e=>errors.push(e)});
  await until(()=>network.handshakes.length===1);
  network.sockets[0].send(JSON.stringify(page('first')));
  await until(async()=>(await client.read('Entry',{id:'live'}))?.text==='first');
  network.sockets[0].send(JSON.stringify({cursors:{scope:{from:1,to:3,head:3}},changes:[
   {stream:'scope',cursor:2,kind:'upsert',model:'Entry',identity:{id:'live'},stamp:9,error:'loader.failed'},
   {stream:'scope',cursor:3,kind:'upsert',model:'Entry',identity:{id:'bad'},stamp:2,state:{text:5,note:null}},
  ]}));
  await until(()=>errors.length===2);
  assert.ok(errors.every(e=>e instanceof runtime.AxtonReport));
  assert.equal(errors[0].kind,'readFailed');assert.equal(errors[0].code,'loader.failed');assert.deepEqual(errors[0].identity,{id:'live'});assert.equal(errors[0].stamp,9);
  assert.equal(errors[1].kind,'skipped');assert.deepEqual(errors[1].identity,{id:'bad'});
  assert.equal((await client.read('Entry',{id:'live'})).text,'first','a read failure keeps the local content');
  assert.equal((await client.syncState()).cursors.scope,3,'the page still moved the cursor');
  assert.match(errors[0].message,/readFailed: Entry .* stamp 9 \(loader.failed\)/);
 }finally{await fixture.close();await network.close();assert.equal(original,network.requests);}
});

test('a queued edit whose replay fails over new authority is reported as diverged and still sent',async()=>{
 const fixture=await openClient();const {client}=fixture;const errors=[];let allowPush=false;const stamps={next:10};
 const server=createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);const body=JSON.parse(Buffer.concat(chunks));
  if(req.url==='/sync/mutations'){if(!allowPush){res.statusCode=503;res.end('later');return;}res.end(JSON.stringify(receiptFor(body,stamps)));return;}
  res.end(JSON.stringify(emptyPage(body)));});
 const ws=new WebSocketServer({server});const sockets=[];const handshakes=[];ws.on('connection',s=>{sockets.push(s);s.on('message',m=>{handshakes.push(JSON.parse(m));s.send(ack(JSON.parse(m)));});});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try{
  await client.subscribe('scope');await client.connect({url:`http://127.0.0.1:${server.address().port}`,token:'secret'},{onError:e=>errors.push(e)});
  await until(()=>handshakes.length===1);
  sockets[0].send(JSON.stringify(page('first')));
  await until(async()=>(await client.read('Entry',{id:'live'}))?.text==='first');
  await client.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'live'},values:{text:'edited offline'}}]});
  assert.equal((await client.read('Entry',{id:'live'})).text,'edited offline');
  // The server deleted the record: the update cannot replay over nothing.
  sockets[0].send(JSON.stringify({cursors:{scope:{from:1,to:2,head:2}},changes:[{stream:'scope',cursor:2,kind:'upsert',model:'Entry',identity:{id:'live'},stamp:2,state:null}]}));
  await until(()=>errors.some(e=>e instanceof runtime.AxtonReport));
  const diverged=errors.find(e=>e instanceof runtime.AxtonReport);
  assert.equal(diverged.kind,'diverged');assert.equal(typeof diverged.ordinal,'number');assert.deepEqual(diverged.identity,{id:'live'});
  assert.equal(await client.read('Entry',{id:'live'}),null,"the server's row (a deletion) is visible");
  const status=await client.syncState();assert.equal(status.pending,1,'the mutation is still queued');
  // The push lane retries after its backoff; the receipt completes the diverged mutation.
  allowPush=true;await until(async()=>(await client.syncState()).pending===0);
  assert.equal((await client.read('Entry',{id:'live'})).text,'edited offline','the diverged edit was sent and completed from its receipt');
 }finally{await fixture.close();for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>server.close(r));}
});

test('a receipt record the client cannot apply reaches onError and the batch still completes',async()=>{
 const fixture=await openClient();const {client}=fixture;const errors=[];const stamps={next:10};let pushes=0;
 const server=createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);const body=JSON.parse(Buffer.concat(chunks));
  if(req.url==='/sync/mutations'){pushes++;const receipt=receiptFor(body,stamps);receipt.records[0].state={text:5,note:null};res.end(JSON.stringify(receipt));return;}
  res.end(JSON.stringify(emptyPage(body)));});
 const ws=new WebSocketServer({server});ws.on('connection',s=>s.on('message',m=>s.send(ack(JSON.parse(m)))));
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try{
  await client.connect({url:`http://127.0.0.1:${server.address().port}`,token:'secret'},{onError:e=>errors.push(e)});
  await client.mutate({name:'Create',operations:[{model:'Entry',op:'create',identity:{id:'odd'},values:{text:'local',note:null}}]});
  await until(async()=>(await client.syncState()).pending===0);
  const skipped=errors.find(e=>e instanceof runtime.AxtonReport);
  assert.ok(skipped,`a report reached onError: ${errors}`);
  assert.equal(skipped.kind,'skipped');assert.deepEqual(skipped.identity,{id:'odd'});
  assert.equal(skipped.detail.batch,1);assert.equal(typeof skipped.detail.error,'string');
  assert.equal(pushes,1,'the batch completed; the receipt was not re-requested');
 }finally{await fixture.close();for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>server.close(r));}
});

// Every runtime route posts to its own path; a route the transport does not
// know is refused rather than posted to another one (#173).
test('the HTTP transport maps every runtime route and refuses an unknown one', async () => {
  const { createServer } = await import('node:http');
  const paths = [];
  const server = createServer((req, res) => { paths.push(req.url); req.resume(); req.on('end', () => res.end('{}')); });
  server.listen(0); await once(server, 'listening');
  try {
    const live = createServerConnection({url:`http://127.0.0.1:${server.address().port}`,token:'secret'});
    for (const route of ['push','pull','action','load']) assert.equal(await timeout(live.push(route, '{}')), '{}');
    assert.deepEqual(paths, ['/sync/mutations','/sync/pull','/sync/actions','/sync/loads']);
    await assert.rejects(live.push('nope', '{}'), /unknown route nope/);
    await assert.rejects(live.push('toString', '{}'), /unknown route toString/);
    assert.equal(paths.length, 4, 'nothing was posted for an unknown route');
  } finally { await new Promise(r => server.close(r)); }
});
/** A fake backend admitting only `x-app-build` 7 or later, on every route and the upgrade (#181). */
async function admissionServer(){
 const seen=[];const stamps={next:0};const state={marked:true};
 const refusal='{"minimumBuild":7}';
 const old=req=>Number(req.headers['x-app-build'])<7;
 const server=createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);
  seen.push([req.url,req.headers['x-app-build'],req.headers.authorization]);
  if(old(req)){res.writeHead(426,{'content-type':'application/json',...(state.marked?{'axton-admission':'refused'}:{})});res.end(refusal);return;}
  const body=JSON.parse(Buffer.concat(chunks));
  res.end(JSON.stringify(req.url==='/sync/mutations'?receiptFor(body,stamps):emptyPage(body)));});
 const ws=new WebSocketServer({noServer:true});
 server.on('upgrade',(req,socket,head)=>{seen.push([req.url,req.headers['x-app-build'],req.headers.authorization]);
  if(old(req)){socket.end(`HTTP/1.1 426 Upgrade Required\r\nContent-Type: application/json\r\n${state.marked?'axton-admission: refused\r\n':''}Content-Length: ${refusal.length}\r\n\r\n${refusal}`);return;}
  ws.handleUpgrade(req,socket,head,s=>s.on('message',m=>s.send(ack(JSON.parse(m)))));});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 return {seen,state,url:`http://127.0.0.1:${server.address().port}`,async close(){for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>server.close(r));}};
}
test('client headers reach every HTTP route and the upgrade; AXTON\'s own headers cannot be replaced',async()=>{
 const server=await admissionServer();
 try{
  const live=createServerConnection({url:server.url,token:'secret',headers:{'x-app-build':'7'}});
  for(const route of ['push','pull','action','fetch','load'])await timeout(live.push(route,'{"cursors":{},"mutations":[]}'));
  const opened=Promise.withResolvers();const abort=new AbortController();
  live.open(subscribe,abort.signal,handlers({message:async()=>opened.resolve()}));
  await timeout(opened.promise);abort.abort();
  assert.deepEqual(server.seen,[
   ['/sync/mutations','7','Bearer secret'],['/sync/pull','7','Bearer secret'],['/sync/actions','7','Bearer secret'],
   ['/sync/fetch','7','Bearer secret'],['/sync/loads','7','Bearer secret'],['/sync/live','7','Bearer secret']]);
  for(const name of ['Authorization','content-type','Sec-WebSocket-Key','upgrade'])
   assert.throws(()=>createServerConnection({url:server.url,token:'t',headers:{[name]:'x'}}),/reserved header/);
  assert.throws(()=>createServerConnection({url:server.url,token:'t',headers:{'x-build':7}}),/header x-build must be a string/);
 }finally{await server.close();}
});
test('a marked refusal carries its status and body on HTTP and on the upgrade; an unmarked 426 is an ordinary failure',async()=>{
 const server=await admissionServer();
 try{
  const live=createServerConnection({url:server.url,token:'secret',headers:{'x-app-build':'6'}});
  const refused=async()=>{
   const http=await timeout(live.push('pull','{}').then(()=>assert.fail('admitted'),e=>e));
   const closed=Promise.withResolvers();
   live.open(subscribe,new AbortController().signal,handlers({closed:closed.resolve}));
   const upgrade=await timeout(closed.promise);
   return [http,upgrade].map(e=>[e.status,e.refusal]);
  };
  assert.deepEqual(await refused(),[[426,'{"minimumBuild":7}'],[426,'{"minimumBuild":7}']]);
  server.state.marked=false;
  assert.deepEqual(await refused(),[[426,undefined],[426,undefined]]);
 }finally{await server.close();}
});
test('an admission refusal reaches onError once as an AdmissionRefused, the connection stops, and connecting with current headers syncs',async()=>{
 const fixture=await openClient();const {client}=fixture;const errors=[];const server=await admissionServer();
 try{
  await client.subscribe('scope');
  await client.mutate({name:'Edit',operations:[{model:'Entry',op:'create',identity:{id:'live'},values:{text:'written'}}]});
  await client.connect({url:server.url,token:'secret',headers:{'x-app-build':'6'}},{onError:e=>errors.push(e),refreshAuth:async()=>assert.fail('a refusal is not an authentication failure')});
  await until(()=>errors.length>0);
  const refused=server.seen.length;
  await new Promise(r=>setTimeout(r,600));
  assert.equal(errors.length,1,`reported once: ${errors.map(e=>e.message)}`);
  assert.ok(errors[0] instanceof runtime.AdmissionRefused);
  assert.equal(errors[0].status,426);assert.deepEqual(errors[0].body,{minimumBuild:7});
  assert.equal(server.seen.length,refused,'nothing is retried after the refusal');
  assert.equal((await client.syncState()).pending,1,'the batch stays for a later connection');
  // The refused connection ended its handle: the same client connects again.
  server.seen.length=0;
  await client.connect({url:server.url,token:'secret',headers:{'x-app-build':'7'}},{onError:e=>errors.push(e)});
  await until(async()=>(await client.syncState()).pending===0);
  assert.equal(errors.length,1);
  assert.ok(server.seen.every(([,build,token])=>build==='7'&&token==='Bearer secret'),JSON.stringify(server.seen));
 }finally{await fixture.close();await server.close();}
});
