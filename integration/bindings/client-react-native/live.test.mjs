import test from 'node:test';
import assert from 'node:assert/strict';
import {createServerConnection} from '../../../packages/frontend/client-react-native/bindings/live.mts';
const tick=()=>new Promise(r=>setImmediate(r));
const subscribe=JSON.stringify({type:'subscribe',streams:['scope'],models:{Entry:1},capabilities:['stream-authority-v1']});
const handlers=(over={})=>({message:async()=>{},overflow:async()=>{},closed:()=>{},...over});
class Socket {
  static last;
  constructor(url,protocols,options){this.url=url;this.options=options;Socket.last=this;}
  send(data){this.sent=data;}
  close(){this.closed=true;}
  frame(text){this.onmessage?.({data:text});}
}

test('native transport forwards Rust frames unchanged and bounds delivery without interpreting them',async()=>{
  const gate=Promise.withResolvers();let overflows=0;const frames=[];
  const abort=new AbortController();
  const live=createServerConnection({url:'http://localhost:4242',token:'alice'},Socket);
  live.open(subscribe,abort.signal,handlers({message:async frame=>{frames.push(frame);if(frames.length===1)await gate.promise;},overflow:async()=>{overflows++;}}));
  await tick();const socket=Socket.last;socket.onopen();
  assert.equal(socket.options.headers.authorization,'Bearer alice');
  assert.equal(socket.sent,subscribe);
  socket.frame('first raw frame');await tick();
  for(let n=0;n<70;n++)socket.frame(String(n));
  assert.deepEqual(frames,['first raw frame']);
  gate.resolve();await tick();
  assert.equal(overflows,1);
  assert.deepEqual(frames,['first raw frame','64','65','66','67','68','69']);
  abort.abort();assert.equal(socket.closed,true);
  socket.frame('late');await tick();assert.equal(frames.includes('late'),false);
});

test('cancellation ends token wait and prevents late socket creation',async()=>{
  const token=Promise.withResolvers(),abort=new AbortController();Socket.last=undefined;let closed=0;
  createServerConnection({url:'http://localhost',token:()=>token.promise},Socket).open(subscribe,abort.signal,handlers({closed:()=>closed++}));
  await tick();abort.abort();token.resolve('alice');await tick();
  assert.equal(Socket.last,undefined);assert.equal(closed,0);
});

test('acknowledgement validation belongs to Rust, so transport delivers every text frame',async()=>{
  const frames=[],abort=new AbortController();
  createServerConnection({url:'http://localhost',token:'alice'},Socket).open(subscribe,abort.signal,handlers({message:async frame=>frames.push(frame)}));
  await tick();const socket=Socket.last;socket.onopen();
  const invalidAck=JSON.stringify({type:'subscribed',cursors:{other:0}});
  socket.frame(invalidAck);socket.frame('not JSON');await tick();
  assert.deepEqual(frames,[invalidAck,'not JSON']);assert.notEqual(socket.closed,true);abort.abort();
});

test('native send failure reports closure instead of escaping the callback',async()=>{
  const failure=Promise.withResolvers();
  class ThrowingSocket extends Socket {send(){throw Error('socket send failed');}}
  createServerConnection({url:'http://localhost',token:'alice'},ThrowingSocket).open(subscribe,new AbortController().signal,handlers({closed:failure.resolve}));
  await tick();const socket=Socket.last;assert.doesNotThrow(()=>socket.onopen());
  assert.match((await failure.promise).message,/socket send failed/);assert.equal(socket.closed,true);
});

test('cancellation while delivering a frame discards buffered and late frames',async()=>{
  const gate=Promise.withResolvers(),frames=[],abort=new AbortController();
  createServerConnection({url:'http://localhost',token:'alice'},Socket).open(subscribe,abort.signal,handlers({message:async frame=>{frames.push(frame);await gate.promise;}}));
  await tick();const socket=Socket.last;socket.frame('first');socket.frame('buffered');
  abort.abort();gate.resolve();await tick();socket.frame('late');await tick();
  assert.equal(socket.closed,true);assert.deepEqual(frames,['first']);
});

test('native socket closure reports failure and a new open creates a new socket',async()=>{
  const live=createServerConnection({url:'http://localhost',token:'alice'},Socket),failure=Promise.withResolvers();
  live.open(subscribe,new AbortController().signal,handlers({closed:failure.resolve}));
  await tick();const socket=Socket.last;socket.onclose({code:1006,reason:'network lost'});
  assert.match((await failure.promise).message,/live disconnected: 1006 network lost/);assert.equal(socket.closed,true);
  const again=Promise.withResolvers();live.open(subscribe,new AbortController().signal,handlers({closed:again.resolve}));
  await tick();assert.notEqual(Socket.last,socket);Socket.last.onerror({message:'refused'});assert.match((await again.promise).message,/refused/);
});

test('client headers ride the native upgrade beside the credential, which they cannot replace',async()=>{
  const abort=new AbortController();
  createServerConnection({url:'http://localhost',token:'alice',headers:{'x-app-build':'7'}},Socket).open(subscribe,abort.signal,handlers());
  await tick();
  assert.deepEqual(Socket.last.options.headers,{'x-app-build':'7',authorization:'Bearer alice'});
  abort.abort();
  assert.throws(()=>createServerConnection({url:'http://localhost',token:'alice',headers:{authorization:'Bearer mallory'}},Socket),/reserved header authorization/);
});
