import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createClient} from '../../../packages/client-js/runtime.mts';
import {openStore} from './store-fixture.mjs';
const native=createRequire(import.meta.url)('../../../bindings/node/axton-node.node');
const schema={models:[],enums:[],actions:[{name:'Ping',kind:'query',version:1,inputs:[],outputs:[]}]};
const until=async(probe)=>{const deadline=Date.now()+5000;while(!probe()){assert.ok(Date.now()<deadline,'Bootstrap fixture timed out');await new Promise(r=>setTimeout(r,2));}};
export function bootstrapSuite(test,Transaction){
 async function harness(body){
  const directory=await mkdtemp(join(tmpdir(),'axton-bootstrap-'));const requests=[];let releaseTail;
  let held=false;const gate=new Promise(resolve=>releaseTail=resolve);
  const Client=createClient(native,Transaction,()=>({open(){},async push(kind,text){
   assert.equal(kind,'pull');const value=JSON.parse(text);requests.push(value);
   if(value.kind==='start')return JSON.stringify({context:value.context,manifestId:'fixed',start:0,total:0});
   if(value.kind==='tail'){if(held)await gate;return JSON.stringify({context:value.context,manifestId:'fixed',head:0});}
   return JSON.stringify({context:value.context,pageId:'empty',from:value.after,to:value.after,head:value.after,units:[]});
  }}));
  let client=await openStore(Client,{path:join(directory,'db'),schema});
  try{await body({get client(){return client},requests,hold:()=>held=true,release:()=>{held=false;releaseTail()},until,
   online:()=>client.connect({url:'http://fixture',token:'viewer'}),
   reopen:async()=>{await client.close();client=await openStore(Client,{path:join(directory,'db'),schema});return client;}
  });}finally{releaseTail();await client.close();await rm(directory,{recursive:true,force:true});}
 }
 test('Bootstrap first await covers tail capture and local completion',()=>harness(async f=>{
  f.hold();await f.online();let done=false;const pending=f.client.bootstrap().then(()=>done=true);
  await until(()=>f.requests.some(x=>x.kind==='tail'));assert.equal(done,false);f.release();await pending;assert.equal(done,true);
 }));
 test('offline Bootstrap registers durable work and resumes on connection',()=>harness(async f=>{
  let done=false;const pending=f.client.bootstrap().then(()=>done=true);await new Promise(setImmediate);
  assert.equal(done,false);assert.deepEqual(f.requests,[]);await f.online();await pending;
  assert.equal(f.requests.filter(x=>x.kind==='start').length,1);
 }));
 test('close ends Bootstrap waiter and reopen resumes exact saved manifest',()=>harness(async f=>{
  f.hold();await f.online();const outcome=f.client.bootstrap().then(()=>null,error=>error);
  await until(()=>f.requests.some(x=>x.kind==='tail'));const first=f.requests.find(x=>x.kind==='tail');
  await f.client.close();assert.match((await outcome).message,/client_closed/);f.release();await f.reopen();await f.online();await f.client.bootstrap();
  assert.equal(f.requests.filter(x=>x.kind==='start').length,1);assert.equal(f.requests.filter(x=>x.kind==='tail').at(-1).manifestId,first.manifestId);
 }));
 test('Bootstrap from local transaction rejects promptly without registering',()=>harness(async f=>{
  await f.online();await f.client.bootstrap();const before=f.requests.filter(x=>x.kind==='start').length;await f.client.transaction(async()=>{await assert.rejects(f.client.bootstrap(),/transaction_active/);});
  assert.equal(f.requests.filter(x=>x.kind==='start').length,before);
 }));
}
