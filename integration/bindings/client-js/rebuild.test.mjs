import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,rm,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Client} from '../../../packages/client-js/index.mts';
import {openStore} from './store-fixture.mjs';
const schema=JSON.parse(await readFile(new URL('../../../fixtures/schemas/entry.json',import.meta.url),'utf8'));
schema.actions=[{name:'Edit',kind:'mutation',version:1,inputs:[{kind:'model',name:'entry',model:'Entry',operation:'update',cardinality:'single',allowedFields:['text','note']}],outputs:[]}];
test('offline schema evolution retains named pending work in the same physical Store',async()=>{
 const dir=await mkdtemp(join(tmpdir(),'axton-rematerialize-'));const path=join(dir,'client.sqlite');let client;
 try{
  client=await openStore(Client,{path,schema});
  await client.direct({model:'Entry',op:'create',identity:{id:'e'},values:{text:'A'}});
  await client.submitMutation('Edit',1,{entry:{id:'e',text:'pending'}},x=>x);
  await client.close();
  const evolved=structuredClone(schema);const entry=evolved.models[0];const old=structuredClone(entry);
  entry.version=2;entry.bootstrap=true;entry.fields.push({name:'extra',nullable:true,type:{kind:'scalar',name:'string'}});
  evolved.resultModels=[{...old,version:1,enums:[]},{...entry,version:2,enums:[]}];
  evolved.models.push({name:'NewModel',version:1,bootstrap:true,identity:['id'],fields:[{name:'id',nullable:false,type:{kind:'scalar',name:'string'}}]});
  client=await openStore(Client,{path,schema:evolved});
  assert.equal((await client.syncState()).pending,1);
  assert.deepEqual(await client.read('Entry',{id:'e'}),{id:'e',text:'pending',note:null,extra:null});
  await assert.rejects(client.resetStore(),/pending/);
  await client.resetStore({discardPending:true});assert.equal(await client.read('Entry',{id:'e'}),null);
  await client.close();client=await openStore(Client,{path,schema:evolved});
  assert.equal((await client.syncState()).pending,0);assert.equal(await client.read('Entry',{id:'e'}),null);
  await stat(path);await assert.rejects(stat(`${path}.1`),error=>error.code==='ENOENT');
 }finally{await client?.close();await rm(dir,{recursive:true,force:true});}
});
