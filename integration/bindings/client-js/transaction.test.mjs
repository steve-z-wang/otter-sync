import test from 'node:test';
import assert from 'node:assert/strict';
import { AsyncLocalStorage } from 'node:async_hooks';
import {Transaction,strictJson} from '../../../packages/client-js/transaction.mts';
test('public callback context is disabled only after settlement, including rejection and finish failure',async()=>{
 const originalRun=AsyncLocalStorage.prototype.run;
 const originalDisable=AsyncLocalStorage.prototype.disable;
 const ran=[];const disabled=[];
 AsyncLocalStorage.prototype.run=function(...args){ran.push(this);return originalRun.apply(this,args);};
 AsyncLocalStorage.prototype.disable=function(...args){disabled.push(this);return originalDisable.apply(this,args);};
 try {
  for(const outcome of ['success','rejection','finish failure']){
   const tx=new Transaction(async()=>{throw Error('finish failed');});
   let release,entered;const gate=new Promise(resolve=>{release=resolve;});const started=new Promise(resolve=>{entered=resolve;});
   const callback=tx.runCallback(async()=>{assert.equal(tx.inCallback(),true);entered();await gate;assert.equal(tx.inCallback(),true);if(outcome==='rejection')throw Error('callback failed');return 'done';});
   await started;
   const context=ran.at(-1);
   assert.ok(context);
   assert.equal(tx.inCallback(),false);
   assert.equal(disabled.includes(context),false);
   release();
   if(outcome==='rejection')await assert.rejects(callback,/callback failed/);
   else assert.equal(await callback,'done');
   assert.equal(disabled.filter(store=>store===context).length,1);
   if(outcome==='finish failure'){
    await assert.rejects(tx.direct({}),/finish failed/);
    await assert.rejects(tx.finish(),/finish failed/);
   }else await tx.finish();
   assert.equal(disabled.filter(store=>store===context).length,1);
  }
 }finally{
  AsyncLocalStorage.prototype.run=originalRun;
  AsyncLocalStorage.prototype.disable=originalDisable;
 }
});
test('raw transactions do not expose named mutation enqueue',()=>{
 const tx=new Transaction(async()=>{});
 assert.equal('mutate' in tx,false);
});
test('forgotten calls drain before rollback and cannot escape callback lifetime',async()=>{
 const calls=[];let release;const gate=new Promise(r=>release=r);const tx=new Transaction(async r=>{if(r.kind==='direct')await gate;calls.push(r.kind);});
 void tx.direct({});const ending=tx.finish();assert.deepEqual(calls,[]);release();await assert.rejects(ending,/unawaited/);assert.deepEqual(calls,['direct']);await assert.rejects(tx.direct({}),/closed/);
});
test('parallel awaited commands preserve invocation order',async()=>{
 const calls=[];const tx=new Transaction(async r=>{await new Promise(r=>setTimeout(r,2));calls.push(r.operation.n);});await Promise.all([tx.direct({n:1}),tx.direct({n:2})]);await tx.finish();assert.deepEqual(calls,[1,2]);
});
test('caught failed operation poisons outer transaction, savepoint confines failure',async()=>{
 const calls=[];const tx=new Transaction(async r=>{calls.push(r.kind);if(r.kind==='direct')throw Error('bad');});await tx.direct({}).catch(()=>{});await assert.rejects(tx.finish(),/bad/);
 const tx2=new Transaction(async r=>{if(r.kind==='direct')throw Error('bad');});await tx2.savepoint(()=>tx2.direct({})).catch(()=>{});await tx2.finish();
});
test('overlapping savepoints fail without popping each others stack',async()=>{
 const calls=[];let release;const gate=new Promise(r=>release=r);const tx=new Transaction(async r=>{calls.push(r.kind);});const first=tx.savepoint(async()=>{await gate;});await new Promise(r=>setImmediate(r));await assert.rejects(tx.savepoint(async()=>{}),/overlapping/);release();await assert.rejects(first,/overlapping/);await assert.rejects(tx.finish(),/overlapping/);assert.deepEqual(calls,['savepoint']);
});
test('nonfinite JSON is rejected instead of becoming a nullable clear',()=>{assert.throws(()=>strictJson({value:NaN}),/finite/);assert.throws(()=>strictJson({value:Infinity}),/finite/);assert.equal(strictJson({value:null}),'\{"value":null\}');});
test('unawaited nested scope poisons transaction without releasing another scope',async()=>{
 const calls=[];let release;const gate=new Promise(r=>release=r);const tx=new Transaction(async r=>{calls.push(r.kind);});let child;
 await assert.rejects(tx.savepoint(async()=>{await tx.direct({});child=tx.savepoint(async()=>{await gate;throw Error('late child');}).catch(()=>{});}),/unawaited nested/);
 release();await child;await assert.rejects(tx.finish(),/unawaited nested/);assert.deepEqual(calls,['savepoint','direct','savepoint']);
});
test('late savepoint callback after transaction closes cannot issue stack commands',async()=>{
 const calls=[];let release;const gate=new Promise(r=>release=r);const tx=new Transaction(async r=>{calls.push(r.kind);});const scope=tx.savepoint(async()=>{await gate;});await new Promise(r=>setImmediate(r));await assert.rejects(tx.finish(),/unawaited/);release();await assert.rejects(scope,/closed/);assert.deepEqual(calls,['savepoint']);
});
