import test from 'node:test';
import assert from 'node:assert/strict';
import { ActionRegistry } from './api/actions.mts';
test('Call.wait consults durable completion after registration without treating pending as terminal', async () => {
  let lookups=0;
  const registry = new ActionRegistry(undefined, async callId => { lookups++; return {callId,outcome:{status:'succeeded',result:7}}; });
  const call=registry.register('saved',value=>Number(value));
  const result=await Promise.race([call.wait(),new Promise((_,reject)=>setTimeout(()=>reject(Error('durable lookup missing')),100))]);
  assert.deepEqual(result,{result:7,error:null});
  assert.equal(lookups,1);
});
