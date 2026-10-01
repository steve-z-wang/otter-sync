import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Transaction } from '../../../packages/client-react-native/transaction.mts';
import * as runtime from '../../../packages/client-js/runtime.mts';
const edit = (text) => ({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'e'},values:{text}}]});
async function within(promise) {
  let timer;
  try { return await Promise.race([promise, new Promise(resolve => {timer=setTimeout(() => resolve('timeout'),100);})]); }
  finally { clearTimeout(timer); }
}
test('shared runtime accepts the mobile transaction and native carrier', () => {
  assert.equal(typeof runtime?.createClient, 'function');
});
test('cancelled mobile store callback releases decoded changes while user work is unresolved', async () => {
  const runtimePath=fileURLToPath(new URL('../../../packages/client-js/runtime.mts',import.meta.url));
  const txPath=fileURLToPath(new URL('../../../packages/client-react-native/transaction.mts',import.meta.url));
  const source=`
    import {createClient} from ${JSON.stringify(runtimePath)};
    import {Transaction} from ${JSON.stringify(txPath)};
    let wake; const outbox=[]; let reference; let release;
    const gate=new Promise(resolve=>{release=resolve});
    const carrier={
      runtimeOpen(request,notify){wake=notify;outbox.push({type:'taskCompleted',
        requestId:JSON.parse(request).requestId,ok:true,value:{clientId:'c',schema:{}}});
        setImmediate(()=>wake('1'));return '1'},
      runtimeSubmit(_id,message){if(JSON.parse(message).type==='close')
        outbox.push({type:'runtimeClosed'});setImmediate(()=>wake('1'))},
      runtimeDrain:()=>JSON.stringify(outbox.splice(0)),runtimeDetach(){},
    };
    const Client=createClient(carrier,Transaction,()=>{throw Error('unused')});
    const client=await Client.open({path:'unused',schema:{},onStore:{Entry:(_tx,changes)=>{
      reference=new WeakRef(changes);return gate;
    }}});
    outbox.push({type:'effect',effectId:'e',operation:{kind:'storeCallback',
      transactionId:'t',model:'Entry',changes:[{kind:'delete',identity:{id:'x'}}]}});
    wake('1');await new Promise(setImmediate);
    outbox.push({type:'cancelEffect',effectId:'e'});wake('1');
    await client.close();
    for(let i=0;i<8;i++){await new Promise(setImmediate);global.gc();}
    process.stdout.write(reference.deref()===undefined?'collected':'retained');
    release();
  `;
  const {error,stdout,stderr}=await new Promise(resolve=>execFile(process.execPath,
    ['--expose-gc','--input-type=module','-e',source],{timeout:20000},
    (error,stdout,stderr)=>resolve({error,stdout,stderr})));
  assert.equal(error,null,stderr);
  assert.equal(stdout,'collected');
});
test('mobile store callback runs without a public task and uses transaction scopes', async () => {
  let wake;
  const outbox=[];
  const admitted=[];
  let opened;
  const carrier={
    runtimeOpen(request, notify) {
      opened=JSON.parse(request); wake=notify;
      outbox.push({type:'taskCompleted',requestId:opened.requestId,ok:true,value:{clientId:'c',schema:{}}});
      setImmediate(()=>wake('1'));
      return '1';
    },
    runtimeSubmit(_id,message) {
      const input=JSON.parse(message); admitted.push(input);
      if(input.type==='transactionCommand')
        outbox.push({type:'taskCompleted',requestId:input.requestId,ok:true,value:null});
      if(input.type==='close') outbox.push({type:'runtimeClosed'});
      setImmediate(()=>wake('1'));
    },
    runtimeDrain:()=>JSON.stringify(outbox.splice(0)), runtimeDetach() {},
  };
  const Client=runtime.createClient(carrier,Transaction,()=>{throw Error('no network');});
  let callback;
  let held;
  let entered;
  let release;
  const started=new Promise(resolve=>entered=resolve);
  const gate=new Promise(resolve=>release=resolve);
  const onStore={Entry:async(tx,changes)=>{
    if(changes[0]?.identity?.id==='hold') {
      held=tx; entered(); await gate; return;
    }
    if(changes[0]?.identity?.id==='unawaited') {
      void tx.streams.subscribe('x'); return;
    }
    assert.deepEqual(changes,[{kind:'delete',identity:{id:'e'}}]);
    assert.equal(await within(client.mutate({name:'M',operations:[]}).then(()=>'',e=>e.message)),'transaction_active');
    assert.equal(await tx.streams.subscribe('project:p1'),undefined);
    callback=tx;
  }};
  const client=await Client.open({path:'unused',schema:{},onStore});
  onStore.Entry=()=>{throw Error('changed handler');};
  assert.deepEqual(opened.storeHooks,['Entry']);
  assert.equal('onStore' in opened,false);
  outbox.push({type:'effect',effectId:'store',operation:{kind:'storeCallback',transactionId:'tx',model:'Entry',changes:[{kind:'delete',identity:{id:'e'}}]}});
  wake('1');
  await new Promise(resolve=>setImmediate(resolve));
  await new Promise(resolve=>setImmediate(resolve));
  assert.deepEqual(admitted.find(x=>x.type==='transactionCommand').command,{kind:'stream',stream:'project:p1',subscribed:true});
  assert.deepEqual(admitted.find(x=>x.type==='callbackResult'),{type:'callbackResult',effectId:'store',transactionId:'tx',ok:true});
  await assert.rejects(callback.read('Entry',{id:'e'}),/closed/);
  outbox.push({type:'effect',effectId:'held',operation:{kind:'storeCallback',transactionId:'tx2',model:'Entry',changes:[{kind:'delete',identity:{id:'hold'}}]}});
  wake('1');
  await started;
  outbox.push({type:'cancelEffect',effectId:'held'});
  wake('1');
  await assert.rejects(held.read('Entry',{id:'hold'}),/closed/);
  release();
  outbox.push({type:'effect',effectId:'unawaited',operation:{kind:'storeCallback',transactionId:'tx3',model:'Entry',changes:[{kind:'delete',identity:{id:'unawaited'}}]}});
  wake('1');
  await new Promise(resolve=>setImmediate(resolve));
  await new Promise(resolve=>setImmediate(resolve));
  assert.match(admitted.find(x=>x.type==='callbackResult'&&x.effectId==='unawaited').error,/unawaited transaction operation/);
  await client.close();
});
{
  const native = createRequire(import.meta.url)('../../../bindings/node/axton-node.node');
  const Client = runtime.createClient(native, Transaction, () => { throw Error('network not configured'); });
  const schema={models:[{name:'Entry',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false},{name:'text',type:{kind:'scalar',name:'string'},nullable:false}],relations:[],unique:[]}],enums:[],clientPolicies:[]};
  test('mobile scope preserves rollback, isolation and persistent identity on real SQLite', async () => {
    const directory=await mkdtemp(join(tmpdir(),'axton-rn-'));
    const path=join(directory,'client.sqlite');
    let client=await Client.open({path,schema});
    try {
      const id=client.clientId;
      await assert.rejects(client.transaction(async tx => {
        await tx.direct({model:'Entry',op:'create',identity:{id:'rolled-back'},values:{text:'no'}});
        throw Error('abort');
      }), /abort/);
      assert.equal(await client.read('Entry',{id:'rolled-back'}),null);
      let retained;
      await client.transaction(async tx => {
        retained=tx;
        await tx.direct({model:'Entry',op:'create',identity:{id:'kept'},values:{text:'yes'}});
      });
      await assert.rejects(retained.read('Entry',{id:'kept'}),/closed/);
      await client.close();
      client=await Client.open({path,schema});
      assert.equal(client.clientId,id);
      assert.equal((await client.read('Entry',{id:'kept'})).text,'yes');
      let release, entered;
      const gate=new Promise(r=>release=r);
      const ready=new Promise(r=>entered=r);
      const transaction=client.transaction(async tx=>{
        await tx.direct({model:'Entry',op:'update',identity:{id:'kept'},values:{text:'after'}});
        entered(); await gate;
      });
      await ready;
      let readResolved=false;
      const read=client.read('Entry',{id:'kept'}).then(row=>{readResolved=true; return row;});
      await new Promise(r=>setImmediate(r));
      assert.equal(readResolved,false);
      release(); await transaction;
      assert.equal((await read).text,'after');
    } finally {await client.close(); await rm(directory,{recursive:true,force:true});}
  });
}
{
  const native = createRequire(import.meta.url)('../../../bindings/node/axton-node.node');
  const Client = runtime.createClient(native, Transaction, () => { throw Error('network not configured'); });
  const schema={models:[{name:'Entry',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false},{name:'text',type:{kind:'scalar',name:'string'},nullable:false}],relations:[],unique:[]}],enums:[],clientPolicies:[]};
  test('mobile client mutation rejects promptly during a public callback, including unrelated callers', async () => {
    const directory=await mkdtemp(join(tmpdir(),'axton-rn-guard-'));
    const client=await Client.open({path:join(directory,'client.sqlite'),schema});
    let release;
    try {
      await client.transaction(tx=>tx.direct({model:'Entry',op:'create',identity:{id:'e'},values:{text:'A'}}));
      let entered;
      const started=new Promise(resolve=>entered=resolve);
      const gate=new Promise(resolve=>release=resolve);
      const transaction=client.transaction(async tx=>{
        assert.equal('mutate' in tx,false);
        entered();
        assert.equal(await within(client.mutate(edit('inside')).then(()=> 'committed',error=>error.message)), 'transaction_active');
        await gate;
      });
      await started;
      assert.equal(await within(client.mutate(edit('unrelated')).then(()=> 'committed',error=>error.message)), 'transaction_active');
      release(); await transaction;
      assert.equal((await client.syncState()).pending,0);
      assert.equal(await client.mutate(edit('after')),1);
      assert.equal((await client.read('Entry',{id:'e'})).text,'after');
    } finally { release?.(); await client.close(); await rm(directory,{recursive:true,force:true}); }
  });
  test('mobile standalone submission rolls back optimistic writes and queue on enqueue failure', async () => {
    const directory=await mkdtemp(join(tmpdir(),'axton-rn-atomic-'));
    const client=await Client.open({path:join(directory,'client.sqlite'),schema});
    try {
      await assert.rejects(client.mutate({name:'Broken',operations:[
        {model:'Entry',op:'create',identity:{id:'failed'},values:{text:'optimistic'}},
        {model:'Missing',op:'create',identity:{id:'missing'},values:{text:'invalid'}},
      ]}));
      assert.equal((await client.syncState()).pending,0);
      assert.equal(await client.read('Entry',{id:'failed'}),null);
      assert.equal(await client.mutate({name:'Create',operations:[{model:'Entry',op:'create',identity:{id:'good'},values:{text:'committed'}}]}),1);
    } finally { await client.close(); await rm(directory,{recursive:true,force:true}); }
  });
}
{
  const native = createRequire(import.meta.url)('../../../bindings/node/axton-node.node');
  const Client = runtime.createClient(native, Transaction, () => { throw Error('network not configured'); });
  const schema={models:[{name:'Entry',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false},{name:'text',type:{kind:'scalar',name:'string'},nullable:false}],relations:[],unique:[]}],enums:[],clientPolicies:[]};
  test('concurrent top-level mobile transactions serialize instead of interleaving', async () => {
    const directory=await mkdtemp(join(tmpdir(),'axton-rn-serial-'));
    const client=await Client.open({path:join(directory,'client.sqlite'),schema});
    try {
      const order=[];
      let release; const gate=new Promise(r=>release=r);
      const first=client.transaction(async tx=>{
        order.push('first:begin');
        await tx.direct({model:'Entry',op:'create',identity:{id:'a'},values:{text:'first'}});
        await gate;
        order.push('first:end');
      });
      const second=client.transaction(async tx=>{
        order.push('second:begin');
        assert.equal((await tx.read('Entry',{id:'a'}))?.text,'first','second transaction must observe the committed first');
        await tx.direct({model:'Entry',op:'create',identity:{id:'b'},values:{text:'second'}});
        order.push('second:end');
      });
      await new Promise(r=>setTimeout(r,20));
      assert.deepEqual(order,['first:begin']);
      release();
      await Promise.all([first,second]);
      assert.deepEqual(order,['first:begin','first:end','second:begin','second:end']);
      assert.equal((await client.query('Entry')).length,2);
    } finally {await client.close(); await rm(directory,{recursive:true,force:true});}
  });
}
