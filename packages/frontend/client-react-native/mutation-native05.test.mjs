import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile,mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createClient} from '../client-js/api/runtime.mts';
import {Transaction} from './api/transaction.mts';
const native=createRequire(import.meta.url)('../../../bindings/node/axton-node.node');
const Client=createClient(native,Transaction,()=>({push:async()=>{throw Error('offline');},open(){}}));
const schema=JSON.parse(await readFile(new URL('../../../integration/v05-sdk/schema.json',import.meta.url),'utf8'));
const connection={url:'http://127.0.0.1:1',token:'offline'};
test('RN transaction adapter uses actual native owned callback and permits other Store ordinary reads',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'axton-rn05-'));
 const a=await Client.open({path:join(directory,'a'),schema,stream:'User:alice',connection});
 const b=await Client.open({path:join(directory,'b'),schema,stream:'User:alice',connection});
 try {
  await a.transaction(async tx=>{
   assert.equal(await b.read('Entry',{id:'e'}),null);
   const call=await tx.submitMutation('Publish',1,async local=>{
    await local.direct({model:'Draft',op:'create',identity:{id:'d'},values:{text:'companion'}});
    return {entry:{id:'e',text:'written'},call:'legal'};
   },value=>value);
   await assert.rejects(call.wait(),error=>error.code==='transaction_uncommitted');
  });
  assert.equal((await a.read('Draft',{id:'d'})).text,'companion');
  assert.equal((await a.read('Entry',{id:'e'})).text,'written');
  await assert.rejects(a.transaction(async()=>{await b.transaction(async()=>{});}),/overlapping transaction callbacks require async context support/);
 } finally {await a.close();await b.close();await rm(directory,{recursive:true,force:true});}
});
