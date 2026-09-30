import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createBackend, WebSocket } from '../../../packages/server/index.mts';
const require = createRequire(import.meta.url);
const native = require('../../../bindings/node/axton-node.node');
const config = { schema: { enums: [], models: [{ name:'Entry', identity:['id'], fields:[{name:'id',nullable:false,type:{kind:'scalar',name:'string'}}] }] }, mutations:[], loaders:['Entry'] };
const routes = ['/sync/mutations','/sync/actions','/sync/fetch','/sync/loads','/sync/pull'];
const app = () => {
  const effects = [];
  const backend = createBackend({config,native,authenticate:()=> 'alice',handlers:{},loaders:{entry:async()=>{effects.push('loader');return [];}},database:{transaction:async body=>body({}),persistence:()=>({call:async request=>{effects.push(request.op);throw new Error('unexpected host effect');}})}});
  return {backend,effects};
};
test('native external ingress refuses unsupported protocol before host effects', async () => {
  const host = async () => { throw new Error('host must not run'); };
  for (const name of ['processPush','processAction','processFetch','processLoad','processPull','negotiateLive']) {
    await assert.rejects(()=>native[name](JSON.stringify(config),'alice','{}',host),error=>JSON.parse(error.message).code==='protocol.unsupported',name);
  }
  assert.throws(()=>native.validateLoadBatch('{}'),error=>JSON.parse(error.message).code==='protocol.unsupported');
});
test('HTTP unsupported protocol is 426 on every route and malformed metadata is 400',async()=>{
  const {backend,effects}=app();const listening=await backend.listen({port:0});
  try {
    for (const route of routes) {
      const refusal=await fetch(listening.url+route,{method:'POST',body:'{}'});
      assert.equal(refusal.status,426,route);assert.deepEqual(await refusal.json(),{code:'protocol.unsupported'});
      const malformed=await fetch(listening.url+route,{method:'POST',body:'{"capabilities":true}'});
      assert.equal(malformed.status,400,route);assert.deepEqual(await malformed.json(),{code:'request.invalid'});
    }
    assert.deepEqual(effects,[]);
  }finally{await listening.close();}
});
test('live subscribe refuses unsupported capability before acknowledgement',async()=>{
  const {backend,effects}=app();const errors=[];const listening=await backend.listen({port:0,onError:error=>errors.push(error)});
  const socket=new WebSocket(listening.url.replace('http:','ws:')+'/sync/live');const frames=[];
  socket.on('message',frame=>frames.push(String(frame)));
  try {
    const closed=await new Promise((resolve,reject)=>{socket.on('error',reject);socket.on('open',()=>socket.send(JSON.stringify({type:'subscribe',channels:['room'],models:{Entry:1}})));socket.on('close',(code,reason)=>resolve({code,reason:String(reason)}));});
    assert.deepEqual(closed,{code:1002,reason:'protocol.unsupported'});assert.deepEqual(frames,[]);assert.deepEqual(effects,[]);assert.deepEqual(errors,[]);
  }finally{socket.close();await listening.close();}
});
