// Replays every request in fixtures/protocol/host-operations.json through the
// real TypeScript host with a fake persistence, and checks the answers against
// the same fixture crates/server/tests/host_contract.rs round-trips in Rust.
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {createBackend,MutationRejected} from '../../../packages/server/index.mts';
import {HOST_OPERATIONS} from '../../../packages/server/host-contract.mts';
import {answer,persistence} from '../../../packages/postgres/index.mts';
const require=createRequire(import.meta.url);
const native=require('../../../bindings/node/axton-node.node');
const fixture=JSON.parse(await readFile(new URL('../../../fixtures/protocol/host-operations.json',import.meta.url),'utf8'));
const entry=op=>fixture.operations.find(o=>o.op===op);
const response=(op,variant)=>{
 const found=entry(op).responses.find(r=>variant?r.variant===variant:true);
 assert.ok(found,`${op}/${variant??'first'} is in the fixture`);
 return found.value;
};

const taskFields=[
 {name:'id',type:{kind:'scalar',name:'string'},nullable:false},
 {name:'title',type:{kind:'scalar',name:'string'},nullable:false}];
const tasksOutput={name:'tasks',kind:'model',cardinality:'list',source:'handlerIdentity',model:'Task',modelReadVersion:1,
 handlerType:{kind:'identity',model:'Task',fields:[{name:'id',type:{kind:'scalar',name:'string'}}]}};
const schema={enums:[],actions:[{name:'Send',version:1,inputs:[],outputs:[{name:'message',kind:'value',type:{kind:'scalar',name:'string'},cardinality:'single',source:'handlerValue'}]}],
 models:[{name:'Task',identity:['id'],fields:taskFields}],
 resultModels:[{name:'Task',version:1,identity:['id'],fields:taskFields,enums:[]}],
 loads:[{name:'Tasks',version:1,inputs:[],outputs:[tasksOutput],input:{models:[],enums:[]},outputEnums:[]}]};
const config={schema,mutations:[{name:'edit',version:1,slots:[
 {name:'task',model:'Task',operation:'update',cardinality:'single',allowedPatchFields:['title']}]}]};

/** Answers the persistence half of the contract from the fixture. */
const fakePersistence=seen=>({
 async call(request){
  seen.push(request);
  switch(request.op){
   case 'claim':return response('claim','claimed');
   case 'claimCall':return response('claimCall','fresh');
   case 'saveReceipt':case 'saveCall':case 'savepoint':case 'rollback':case 'release':return null;
   case 'head':return response('head','cursor');
   case 'scan':return response('scan','rows');
   case 'advanceStamp':return response('advanceStamp','stamped');
   case 'ensureStamp':return response('ensureStamp','stamped');
   case 'readStamps':return response('readStamps','stamped');
   case 'publish':return response('publish','published');
   case 'lockRecord':return response('lockRecord','locked');
   case 'memberships':return response('memberships','members');
   case 'setMembership':return null;
   default:throw new Error(`fake persistence reached ${request.op}`);
  }
 },
});

/**
 * Drives the host callback with the fixture requests instead of a real push.
 * Returns [op, response] for every replayed request, plus what the persistence saw.
 */
async function replay(requests,{reject=false,fail=false,onError,page}={}){
 const seen=[],answers=[],handled=[],loaded=[],paged=[];
 const backend=createBackend({
  config,
  native:{
   validateConfig:c=>native.validateConfig(c),
   processPush:async(_config,_owner,_request,callback)=>{
    for(const request of requests)answers.push([request.op,JSON.parse(await callback(JSON.stringify(request)))]);
    return '{"batchSequence":1,"clientId":"alice","records":[],"rejections":[]}';
   },
   processPull:async()=>'{}',
   settleExternal:async()=>'[]',
   negotiateLive:async()=>'{}',
   pullLive:async()=>'{}',
  },
  database:{transaction:body=>body({}),persistence:()=>fakePersistence(seen)},
  authenticate:()=>'alice',
  onError,
  // Two touches (the update slot's t-1, which the engine also targets on its
  // own, and t-2), both added to one Channel and the target to another: the
  // fixture's settlement.
  mutations:{async send(){return {message:'sent'};}},
  loads:{async tasks(call){
   paged.push(call);
   if(reject)throw new MutationRejected('tasks.refused');
   if(fail)throw new Error('boom');
   return page?page():response('handleLoad','settled');
  }},
  handlers:{async edit({input,channel,touch}){
   handled.push(input);
   touch.task(input.task.identity);
   touch.task({id:'t-2'});
   channel('shared').add([{model:'Task',identity:input.task.identity},{model:'Task',identity:{id:'t-2'}}]);
   channel('other').task.add(input.task.identity);
   if(reject)throw new MutationRejected('task.refused');
   if(fail)throw new Error('boom');
  }},
  loaders:{async task(call){loaded.push(call);if(fail)throw new Error('boom');return response('load','rows');}},
 });
 await backend.push('alice','{}');
 return {answers,seen,handled,loaded,paged};
}

test('the fixture and the TypeScript union cover the same operations',()=>{
 assert.deepEqual([...fixture.operations.map(o=>o.op)].sort(),[...HOST_OPERATIONS].sort());
 assert.equal(new Set(fixture.operations.map(o=>o.op)).size,fixture.operations.length);
});

test('every fixture request replays through the TypeScript host to the fixture answer',async()=>{
 const requests=fixture.operations.map(o=>o.request);
 const {answers,seen,handled,loaded,paged}=await replay(requests);
 assert.deepEqual(answers.map(([op])=>op),HOST_OPERATIONS);
 const expected={
  claim:response('claim','claimed'),saveReceipt:null,claimCall:response('claimCall','fresh'),saveCall:null,head:response('head','cursor'),
  scan:response('scan','rows'),savepoint:null,rollback:null,release:null,
  handle:response('handle','settled'),handleAction:response('handleAction','settled'),handleLoad:response('handleLoad','settled'),load:response('load','rows'),
  advanceStamp:response('advanceStamp','stamped'),ensureStamp:response('ensureStamp','stamped'),readStamps:response('readStamps','stamped'),publish:response('publish','published'),
  lockRecord:response('lockRecord','locked'),memberships:response('memberships','members'),setMembership:null,
 };
 assert.equal(Object.keys(expected).length,HOST_OPERATIONS.length,'every operation has an expected answer');
 for(const [op,answer] of answers)assert.deepEqual(answer,expected[op],`${op} answer`);
 // handle and load reach application code; everything else reaches persistence,
 // savepoint/rollback/release included - they are bookkept *and* forwarded.
 assert.deepEqual(seen.map(r=>r.op),HOST_OPERATIONS.filter(op=>op!=='handle'&&op!=='handleAction'&&op!=='handleLoad'&&op!=='load'));
 assert.equal(handled.length,1);
 assert.deepEqual(handled[0].task.patch,entry('handle').request.arguments.task.patch);
 assert.equal(loaded.length,1);assert.deepEqual(loaded[0].ids,entry('load').request.identities);assert.equal(loaded[0].userId,entry('load').request.owner);
 assert.equal('channel' in loaded[0],false,'loads name no channel');
 const loadRequest=entry('handleLoad').request;
 assert.equal(paged.length,1);
 assert.deepEqual(paged[0].continuation,loadRequest.continuation);
 assert.deepEqual(paged[0].args,loadRequest.arguments);
 assert.deepEqual(Object.keys(paged[0].ctx).sort(),['callId','loadId','tx','userId'],'a Load context is read-only: no touch or channel');
 assert.deepEqual([paged[0].ctx.userId,paged[0].ctx.callId,paged[0].ctx.loadId],[loadRequest.owner,loadRequest.callId,loadRequest.loadId]);
});

test('a Load handler rejection and throw answer as data like an Action',async()=>{
 const request=entry('handleLoad').request;
 assert.deepEqual((await replay([request],{reject:true})).answers,[['handleLoad',response('handleLoad','rejected')]]);
 const errors=[];
 const {answers}=await replay([request],{fail:true,onError:e=>errors.push(e)});
 assert.equal(typeof answers[0][1].error,'string');
 assert.equal(errors[0].message,'boom');
});

test('a Load continuation that is not bounded portable JSON is a saved rejection, never a coerced value or a transaction abort',async()=>{
 const request=entry('handleLoad').request;
 const deep=depth=>{let value=0;for(let i=0;i<depth;i++)value=[value];return value;};
 const cycle={};cycle.self=cycle;
 const hole=[1,,3];
 class Cursor{constructor(){this.at=1;}}
 const invalid=[
  ['BigInt',{state:{n:1n}}],
  ['toJSON',{state:{toJSON(){return 1;}}}],
  ['Date',{state:new Date(0)}],
  ['class instance',{state:new Cursor()}],
  ['NaN',{state:NaN}],
  ['Infinity',{state:[Infinity]}],
  ['unsafe integer',{state:2**53}],
  ['undefined member',{state:{a:undefined}}],
  ['undefined state',{state:undefined}],
  ['array hole',{state:hole}],
  ['cycle',{state:cycle}],
  ['depth 65',{state:deep(65)}],
  ['over 64 KiB',{state:'x'.repeat(64*1024)}],
  ['missing next',undefined],
  ['empty wrapper',{}],
  ['extra member',{state:1,more:2}],
  ['bare state',7],
  // JSON.stringify escapes a lone UTF-16 surrogate that Rust cannot decode:
  // without this check the page would fail as a host fault and retry forever.
  ['lone surrogate',{state:'\ud800'}],
  ['nested lone surrogate',{state:{after:['ok','x\udfff']}}],
  ['lone surrogate key',{state:{'\udc00':1}}],
 ];
 for(const [label,next] of invalid){
  const errors=[];
  const {answers}=await replay([request],{onError:e=>errors.push(e),page:()=>({data:{tasks:[]},...(next===undefined&&label==='missing next'?{}:{next})})});
  assert.deepEqual(answers,[['handleLoad',{rejection:'load.invalid_continuation'}]],label);
  assert.equal(errors.length,1,`${label} is reported`);
 }
 const valid=[{state:deep(64)},{state:null},{state:'x'.repeat(64*1024-2)},{state:{nested:[1,-0,2.5,'s',true,null,{}]}},{state:{'\u{1f600}':'pair \ud83d\ude00'}},null];
 for(const next of valid){
  const {answers}=await replay([request],{page:()=>({data:{tasks:[{id:'t-1'}]},next})});
  assert.deepEqual(answers[0][1],{data:{tasks:[{id:'t-1'}]},next:JSON.parse(JSON.stringify(next))});
 }
 // Data the bridge cannot encode is the page's failure, still not an abort.
 const errors=[];
 const {answers}=await replay([request],{onError:e=>errors.push(e),page:()=>({data:{tasks:[{id:undefined}]},next:null})});
 assert.equal(typeof answers[0][1].error,'string');
 assert.equal(errors.length,1);
 const notObject=await replay([request],{onError:()=>{},page:()=>null});
 assert.equal(typeof notObject.answers[0][1].error,'string');
 // Reading the answer is inside the handler's error boundary: a throwing
 // getter is the handler's failure, and a continuation that throws while it
 // is inspected is not portable. Neither escapes as a host fault.
 const getter=await replay([request],{onError:()=>{},page:()=>({data:{tasks:[]},get next(){throw new Error('getter boom');}})});
 assert.deepEqual(getter.answers[0][1],{error:'getter boom'});
 const rejectingGetter=await replay([request],{page:()=>({data:{tasks:[]},get next(){throw new MutationRejected('tasks.gone');}})});
 assert.deepEqual(rejectingGetter.answers[0][1],{rejection:'tasks.gone'});
 const trapped=new Proxy({},{ownKeys(){throw new Error('trap');}});
 const proxied=await replay([request],{onError:()=>{},page:()=>({data:{tasks:[]},next:trapped})});
 assert.deepEqual(proxied.answers[0][1],{rejection:'load.invalid_continuation'});
 const deepTrap=await replay([request],{onError:()=>{},page:()=>({data:{tasks:[]},next:{state:{a:trapped}}})});
 assert.deepEqual(deepTrap.answers[0][1],{rejection:'load.invalid_continuation'});
});

test('the same handle request settles as the fixture rejection when the handler refuses',async()=>{
 const {answers}=await replay([entry('handle').request],{reject:true});
 assert.deepEqual(answers,[['handle',response('handle','rejected')]]);
});

test('a thrown handler error answers as a failure and reaches onError',async()=>{
 const errors=[];
 const {answers}=await replay([entry('handle').request],{fail:true,onError:e=>errors.push(e)});
 assert.equal(answers.length,1);assert.equal(answers[0][0],'handle');
 assert.equal(typeof answers[0][1].error,'string');
 assert.equal(errors.length,1);assert.equal(errors[0].message,'boom');
});

test('a thrown loader error answers as a failure and reaches onError',async()=>{
 const errors=[];
 const {answers}=await replay([entry('load').request],{fail:true,onError:e=>errors.push(e)});
 assert.equal(answers.length,1);assert.equal(answers[0][0],'load');
 assert.equal(typeof answers[0][1].error,'string');
 assert.equal(errors.length,1);assert.equal(errors[0].message,'boom');
});

test('the PostgreSQL persistence answers the persistence half through a two-method driver and refuses application operations',async()=>{
 const driver={transaction:body=>body('tx'),query:async(tx,sql)=>sql.startsWith('SELECT head')?[{head:6}]:[]};
 const bound=persistence(driver).persistence('tx');
 assert.equal(await bound.call(entry('head').request),response('head','cursor'));
 assert.equal(await bound.call(entry('savepoint').request),null);
 assert.equal(await answer(driver,'tx',entry('head').request),6);
 for(const op of ['handle','handleAction','handleLoad','load'])
  await assert.rejects(()=>bound.call(entry(op).request),/Unsupported persistence operation/);
 await assert.rejects(()=>bound.call({op:'vacuum'}),/Unsupported persistence operation vacuum/);
});

test('the PostgreSQL persistence validates membership requests and the rows it answers from',async()=>{
 const driverAnswering=rows=>{const seen=[];return {seen,driver:{transaction:body=>body('tx'),query:async(tx,sql,params)=>{seen.push([sql,params]);return rows(sql);}}};};
 const lock=entry('lockRecord').request,members=entry('memberships').request,set=entry('setMembership').request;
 {
  const {driver,seen}=driverAnswering(()=>[]);
  assert.equal(await answer(driver,'tx',lock),null,'no row: nothing locked, nothing created');
  assert.deepEqual(await answer(driver,'tx',members),[]);
  assert.equal(await answer(driver,'tx',set),null);
  assert.equal(await answer(driver,'tx',{...set,present:false}),null);
  assert.deepEqual(seen.map(([sql,params])=>[sql.split(/\s+/).slice(0,3).join(' '),params]),[
   ['UPDATE axton_record SET',['Task',lock.identityKey]],
   ['SELECT channel FROM',['Task',members.identityKey]],
   ['INSERT INTO axton_channel(channel,head)',['shared']],
   ['INSERT INTO axton_membership(channel,model,identity_key)',['shared','Task',set.identityKey]],
   ['DELETE FROM axton_membership',['shared','Task',set.identityKey]],
  ],'adding ensures the channel then inserts; removing only deletes');
 }
 {
  const {driver}=driverAnswering(sql=>sql.startsWith('UPDATE')?[{stamp:4n}]:[{channel:'shared'},{channel:'other'}]);
  assert.equal(await answer(driver,'tx',lock),response('lockRecord','locked'));
  assert.deepEqual(await answer(driver,'tx',members),['shared','other'],'rows keep the database order');
 }
 for(const [rows,pattern] of [[[{stamp:0}],/Stored stamp/],[[{stamp:2**53}],/Stored stamp/],[[{stamp:1},{stamp:1}],/more than one/]]){
  const {driver}=driverAnswering(()=>rows);
  await assert.rejects(()=>answer(driver,'tx',lock),pattern);
 }
 for(const [rows,pattern] of [[[{channel:'a'},{channel:'a'}],/Duplicate membership/],[[{channel:' '}],/Invalid membership channel/],[[{channel:null}],/Invalid membership channel/]]){
  const {driver}=driverAnswering(()=>rows);
  await assert.rejects(()=>answer(driver,'tx',members),pattern);
 }
 for(const [request,pattern] of [
  [{...set,present:undefined},/present must be a boolean/],
  [{...set,present:'true'},/present must be a boolean/],
  [{...set,channel:''},/Invalid membership channel/],
  [{...set,channel:'  '},/Invalid membership channel/],
  [{...set,surprise:1},/Unknown setMembership field surprise/],
  [{...lock,channel:'shared'},/Unknown lockRecord field channel/],
  [{...members,present:true},/Unknown memberships field present/],
 ]){
  const {driver,seen}=driverAnswering(()=>[]);
  await assert.rejects(()=>answer(driver,'tx',request),pattern);
  assert.deepEqual(seen,[],'a malformed request runs no statement');
 }
});
