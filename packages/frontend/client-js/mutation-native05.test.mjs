import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createClient } from './api/runtime.mts';
import { Transaction } from './api/transaction.mts';
const native = createRequire(import.meta.url)('../../../bindings/node/axton-node.node');
const Client = createClient(native, Transaction, () => ({ push: async () => { throw Error('offline'); }, open() {} }));
const connection = { url:'http://127.0.0.1:1', token:'offline' };
const schema = JSON.parse(await readFile(new URL('../../../integration/v05-sdk/schema.json',import.meta.url),'utf8'));
test('real native callback-before-input is atomic and Call wait requires commit', async () => {
  const directory = await mkdtemp(join(tmpdir(),'axton-sdk05-'));
  const client = await Client.open({path:join(directory,'store'),schema,stream:'User:alice',connection});
  try {
    await client.transaction(async tx => {
      await tx.direct({model:'Draft',op:'create',identity:{id:'d'},values:{text:'draft'}});
      const call = await tx.submitMutation('Publish',1,async local => {
        await local.direct({model:'Draft',op:'delete',identity:{id:'d'}});
        return {entry:{id:'e',text:'written'},call:'legal'};
      }, value=>value);
      await assert.rejects(call.wait(), error => error.code==='transaction_uncommitted');
    });
    assert.equal(await client.read('Draft',{id:'d'}),null);
    assert.equal((await client.read('Entry',{id:'e'})).text,'written');
    await assert.rejects(client.submitMutation('Publish',1,async local => {
      await local.direct({model:'Draft',op:'create',identity:{id:'bad'},values:{text:'rollback'}});
      return {entry:{id:'bad'},call:'invalid'};
    },value=>value));
    assert.equal(await client.read('Draft',{id:'bad'}),null);
  } finally { await client.close(); await rm(directory,{recursive:true,force:true}); }
});
test('unawaited companion command rolls back its Mutation scope',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'axton-unawaited04-'));
 const client=await Client.open({path:join(directory,'a'),schema,stream:'User:alice',connection});
 try {
  await assert.rejects(client.submitMutation('Publish',1,async local=>{
   void local.direct({model:'Draft',op:'create',identity:{id:'unawaited'},values:{text:'bad'}});
   return {entry:{id:'e',text:'bad'},call:'legal'};
  },value=>value),/unawaited transaction operation/);
  assert.equal(await client.read('Draft',{id:'unawaited'}),null);
  assert.equal(await client.read('Entry',{id:'e'}),null);
 } finally {await client.close();await rm(directory,{recursive:true,force:true});}
});
test('captured transaction cannot join a different client callback', async () => {
  const directory=await mkdtemp(join(tmpdir(),'axton-foreign04-'));
  const a=await Client.open({path:join(directory,'a'),schema,stream:'User:alice',connection});
  const b=await Client.open({path:join(directory,'b'),schema,stream:'User:alice',connection});
  try {
    await assert.rejects(a.transaction(async txA => {
      await b.transaction(async () => {
        await assert.rejects(txA.direct({model:'Draft',op:'create',identity:{id:'foreign'},values:{text:'bad'}}), /foreign transaction scope/);
      });
    }), /foreign transaction scope/);
    assert.equal(await a.read('Draft',{id:'foreign'}),null);
    assert.equal(await b.read('Draft',{id:'foreign'}),null);
  } finally { await a.close(); await b.close(); await rm(directory,{recursive:true,force:true}); }
});
test('captured empty savepoint cannot join another Store callback',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'axton-savepoint-foreign04-'));
 const a=await Client.open({path:join(directory,'a'),schema,stream:'User:alice',connection});
 const b=await Client.open({path:join(directory,'b'),schema,stream:'User:alice',connection});
 let ran=false;
 try {
  await assert.rejects(a.transaction(async txA=>{
   await b.transaction(async()=>{
    await assert.rejects(txA.savepoint(async()=>{ran=true;return 42;}),/foreign transaction scope/);
   });
  }),/foreign transaction scope/);
  assert.equal(ran,false);
 } finally {await a.close();await b.close();await rm(directory,{recursive:true,force:true});}
});
