import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
const {GeneratedClient}=await import(pathToFileURL(`${process.argv[2]}/client.ts`).href);
const path=process.argv[3];
let first;
for(let run=0;run<2;run++){
 const c=await GeneratedClient.open({path});
 try{
  assert.equal(c.client.clientId,'fixture-client');
  assert.equal((await c.models.todo.get({id:'live'})).channel,'second queued Channel');
  assert.equal((await c.client.readSql('SELECT count(*) AS n FROM axton_scope_member',[]))[0].n,4);
  assert.equal((await c.client.readSql("SELECT count(*) AS n FROM sqlite_master WHERE name LIKE 'axton_channel%'",[]))[0].n,0);
  assert.equal((await c.client.readSql("SELECT cursor,reconcile_run FROM axton_subscription WHERE scope='Channel:business-scope'",[]))[0].cursor,11);
  const bytes=await c.client.freeze();
  const logical=JSON.parse(bytes);delete logical.capabilities;
  const expected=JSON.parse(await readFile(new URL('../../../crates/sqlite/tests/fixtures/frozen-push-logical.json',import.meta.url),'utf8'));
  assert.deepEqual(logical,expected);
  if(run===0)first=bytes;else assert.equal(bytes,first);
 }finally{await c.close();}
}
console.log('generated JS original-v0.2 native reopen twice: holds, cursors, opaque channel and frozen work preserved');
