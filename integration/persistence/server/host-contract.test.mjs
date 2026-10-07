// Replays every request in fixtures/protocol/host-operations.json through the
// real TypeScript host with a fake persistence, and checks the answers against
// the same fixture crates/server/tests/host_contract.rs round-trips in Rust.
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {createBackend,MutationRejected,isRetryableTransactionError} from '../../../packages/server/index.mts';
import {HOST_OPERATIONS} from '../../../packages/server/host-contract.mts';
import {answer,persistence} from '../../../packages/postgres/index.mts';
import {withRetries} from '../../../packages/postgres/src/driver.mts';
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
   case 'protocol05':return null;
   case 'publicationFence':case 'savePublicationGroups':return null;
   case 'readPublicationGroups':return response('readPublicationGroups');
   case 'readCall':return null;
   case 'createManifest':case 'readManifest':case 'captureTail':return response(request.op);
   case 'readPositions':return response('readPositions');
   case 'claim':return response('claim','claimed');
   case 'claimCall':return response('claimCall','fresh');
   case 'saveReceipt':case 'saveCall':case 'savepoint':case 'rollback':case 'release':return null;
   case 'head':return response('head','cursor');
   case 'scan':return response('scan','rows');
   case 'advanceStamp':return response('advanceStamp','stamped');
   case 'ensureStamp':return response('ensureStamp','stamped');
   case 'readStamps':return response('readStamps','stamped');
   case 'lockRecord':return response('lockRecord','locked');
   case 'readTracking':return response('readTracking','holders');
   case 'guardRecords':return response('guardRecords','stamps');
   case 'lockStreams':return null;
   case 'applyStreamMembers':return response('applyStreamMembers','positions');
   default:throw new Error(`fake persistence reached ${request.op}`);
  }
 },
});

/**
 * Drives the host callback with the fixture requests instead of a real push.
 * Returns [op, response] for every replayed request, plus what the persistence saw.
 */
async function replay(requests,{reject=false,fail=false,onError,page,options={},sent=[]}={}){
 const seen=[],answers=[],handled=[],loaded=[],paged=[];
 const backend=createBackend({
  config,
  protocol4:{backendId:"api",contractId:"app",projectionGeneration:"1",authorizeStream:()=>true},
  native:{
   validateConfig:c=>native.validateConfig(c),
   serverMaterializationId:(c,g)=>native.serverMaterializationId(c,g),
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
  // Two invalidations (the update slot's t-1, which the engine also targets on its
  // own, and t-2), both added to one Stream and the target to another: the
  // fixture's settlement.
  mutations:{async send({ctx}){sent.push(ctx);return {message:'sent'};}},
  loads:{async tasks(call){
   paged.push(call);
   if(reject)throw new MutationRejected('tasks.refused');
   if(fail)throw new Error('boom');
   return page?page(call):response('handleLoad','settled');
  }},
  handlers:{async edit({input,stream,invalidate}){
   handled.push(input);
   invalidate.task(input.task.identity);
   invalidate.task({id:'t-2'});
   stream('shared').track([{model:'Task',identity:input.task.identity},{model:'Task',identity:{id:'t-2'}}]);
   stream('other').track.task(input.task.identity);
   if(reject)throw new MutationRejected('task.refused');
   if(fail)throw new Error('boom');
  }},
  loaders:{async task(call){loaded.push(call);if(fail)throw new Error('boom');return response('load','rows');}},
  ...options,
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
 const sent=[];
 const {answers,seen,handled,loaded,paged}=await replay(requests,{sent});
 assert.deepEqual(answers.map(([op])=>op),HOST_OPERATIONS);
 const expected={protocol05:null,
  handleBootstrap:{declarations:[]},readCall:null,createManifest:response('createManifest'),readManifest:response('readManifest'),captureTail:response('captureTail'),
  admitContext:true,publicationFence:null,savePublicationGroups:null,readPublicationGroups:response('readPublicationGroups'),readPositions:response('readPositions'),
  claim:response('claim','claimed'),saveReceipt:null,claimCall:response('claimCall','fresh'),saveCall:null,head:response('head','cursor'),
  scan:response('scan','rows'),savepoint:null,rollback:null,release:null,
  handle:response('handle','settled'),handleAction:response('handleAction','settled'),handleLoad:response('handleLoad','settled'),load:response('load','rows'),
  advanceStamp:response('advanceStamp','stamped'),ensureStamp:response('ensureStamp','stamped'),readStamps:response('readStamps','stamped'),
  lockRecord:response('lockRecord','locked'),readTracking:response('readTracking','holders'),guardRecords:response('guardRecords','stamps'),lockStreams:null,
  applyStreamMembers:response('applyStreamMembers','positions'),
 };
 assert.equal(Object.keys(expected).length,HOST_OPERATIONS.length,'every operation has an expected answer');
 for(const [op,answer] of answers)assert.deepEqual(answer,expected[op],`${op} answer`);
 // handle and load reach application code; everything else reaches persistence,
 // savepoint/rollback/release included - they are bookkept *and* forwarded.
 assert.deepEqual(seen.map(r=>r.op),HOST_OPERATIONS.filter(op=>op!=='handleBootstrap'&&op!=='admitContext'&&op!=='handle'&&op!=='handleAction'&&op!=='handleLoad'&&op!=='load'));
 assert.equal(handled.length,1);
 assert.deepEqual(handled[0].task.patch,entry('handle').request.arguments.task.patch);
 assert.equal(loaded.length,1);assert.deepEqual(loaded[0].ids,entry('load').request.identities);assert.equal(loaded[0].userId,entry('load').request.owner);
 assert.equal('stream' in loaded[0],false,'a Loader has no stream context');
 assert.equal('invalidate' in loaded[0],false,'a Loader declares no change');
 const loadRequest=entry('handleLoad').request;
 assert.equal(paged.length,1);
 assert.deepEqual(paged[0].continuation,loadRequest.continuation);
 assert.deepEqual(paged[0].args,loadRequest.arguments);
 assert.deepEqual(Object.keys(paged[0].ctx).sort(),['callId','loadId','stream','tx','userId'],'a Load context adds to Scopes and declares no change: no touch');
 assert.equal(typeof paged[0].ctx.stream,'function');
 // A Mutation keeps its full declaration handles.
 assert.equal(sent.length,1);
 assert.deepEqual(Object.keys(sent[0]).sort(),['callId','invalidate','stream','tx','userId']);
 assert.equal(typeof sent[0].invalidate.task,'function');
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
 // The Stream operations answer through the same two methods.
 assert.equal(await bound.call(entry('lockStreams').request),null);
 assert.deepEqual(await bound.call(entry('readTracking').request),[],'no rows, no members');
});

test('the PostgreSQL persistence refuses malformed bulk requests before any statement runs',async()=>{
 const seen=[];const driver={transaction:body=>body('tx'),query:async(tx,sql)=>{seen.push(sql);return [];}};
 const read=entry('readTracking').request,apply=entry('applyStreamMembers').request;
 const delta=apply.deltas[0],guard=entry('guardRecords').request;
 for(const [request,pattern] of [
  [{op:'lockStreams',streams:[]},/at least one/],
  [{op:'lockStreams',streams:['a','a']},/repeats/],
  [{op:'lockStreams',streams:[' ']},/Invalid tracking stream/],
  [{...read,extra:1},/Unknown readTracking field extra/],
  [{...read,records:[{model:'Task'}]},/identityKey must be a non-empty string/],
  [{...read,records:[read.records[0],read.records[0]]},/repeats/],
  [{...guard,records:[{...guard.records[0],mode:['advance']}]},/guard mode/],
  [{...guard,records:[guard.records[1],guard.records[0]]},/canonical order/],
  [{...apply,deltas:[{...delta,stamp:1}]},/Unknown record key field stamp/],
  [{...apply,deltas:[{...delta,publish:1}]},/publish must be boolean/],
  [{...apply,deltas:[{...delta,tags:['x']}]},/Unknown record key field tags/],
  [{...apply,deltas:[delta,delta]},/repeats/],
 ]){
  seen.length=0;
  await assert.rejects(()=>answer(driver,'tx',request),pattern);
  assert.deepEqual(seen,[],`${pattern}: no statement`);
 }
});

test('a published position wakes its Stream after commit, a kept one does not, and a settlement conflict retries the whole transaction',async()=>{
 const deltas=entry('applyStreamMembers').request.deltas;
 const request={op:'applyStreamMembers',deltas:[{...deltas[0],stream:'kept'},...deltas.slice(1)]};
 let attempts=0;
 const seen=[];
 const backend=createBackend({config,
  native:{validateConfig:c=>native.validateConfig(c),
   settleExternal:async(_config,_settlement,callback)=>{
    if(++attempts===1)throw new Error(JSON.stringify({code:'transaction.conflict',message:'Task {"id":"t-1"} joined Stream b after settlement locked its Streams; the transaction must retry'}));
    await callback(JSON.stringify(request));
    return '[]';
   }},
  database:{transaction:body=>withRetries(()=>body({}),isRetryableTransactionError,3,()=>0),persistence:()=>fakePersistence(seen)},
  authenticate:()=>'alice',handlers:{async edit(){}},mutations:{async send(){return {message:'sent'};}},
  loads:{async tasks(){return response('handleLoad','settled');}},loaders:{async task(){return [];}}});
 const woken=[];
 for(const scope of ['kept','shared'])backend.onCommitted(scope,()=>woken.push(scope));
 await backend.transaction(async()=>{});
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(attempts,2,'the conflict reached the driver retry loop once');
 assert.deepEqual(seen.map(r=>r.op),['applyStreamMembers']);
 assert.deepEqual(woken,['shared'],'only a published position wakes');
 assert.equal(isRetryableTransactionError({code:'transaction.conflict'}),true);
 assert.equal(isRetryableTransactionError({code:'handler.invalid'}),false);
});

test('the PostgreSQL persistence validates aligned guards and tracking rows',async()=>{
 const driverAnswering=rows=>{const seen=[];return {seen,driver:{transaction:body=>body('tx'),query:async(tx,sql,params)=>{seen.push([sql,params]);return rows(sql);}}};};
 const lock=entry('lockRecord').request,read=entry('readTracking').request;
 const guard=entry('guardRecords').request;
 {
  const {driver}=driverAnswering(()=>[]);
  assert.equal(await answer(driver,'tx',lock),null,'no row: nothing locked, nothing created');
  assert.deepEqual(await answer(driver,'tx',read),[]);
 }
 {
  const {driver}=driverAnswering(()=>[{stream:'shared',model:'Task',identity_key:'{"id":"t-1"}'}]);
  assert.deepEqual(await answer(driver,'tx',read),response('readTracking','holders'));
 }
 for(const rows of [[{stamp:0}],[{stamp:2**53}],[{stamp:1},{stamp:1}]]){
  const {driver}=driverAnswering(()=>rows);
  await assert.rejects(()=>answer(driver,'tx',lock),/Stored stamp|more than one/);
 }
 for(const stream of [' ',null]){
  const {driver}=driverAnswering(()=>[{stream,model:'Task',identity_key:'{"id":"t-1"}'}]);
  await assert.rejects(()=>answer(driver,'tx',read),/Invalid tracking stream/);
 }
 {
  const {driver}=driverAnswering(()=>[{ord:1,stamp:8},{ord:2,stamp:null}]);
  assert.deepEqual(await answer(driver,'tx',guard),response('guardRecords','stamps'));
 }
 for(const mode of ['advance','ensure']){
  const {driver}=driverAnswering(()=>[{ord:1,stamp:null}]);
  await assert.rejects(()=>answer(driver,'tx',{op:'guardRecords',records:[{...guard.records[0],mode}]}),/null|stamp/);
 }
 for(const rows of [[],[{ord:2,stamp:1},{ord:1,stamp:1}],[{ord:1,stamp:0},{ord:2,stamp:null}]]){
  const {driver}=driverAnswering(()=>rows);
  await assert.rejects(()=>answer(driver,'tx',guard),/number of stamps|order|Stored stamp/);
 }
});

const enrolledPage=()=>{const {tracking,...page}=response('handleLoad','enrolled');return page;};

test('a Load declares its enrollment through ctx.stream and the host attaches it to the fixture answer',async()=>{
 const request=entry('handleLoad').request;
 const {answers}=await replay([request],{page:({ctx})=>{
  const shared=ctx.stream('shared');
  shared.track.task({id:'t-1'});
  ctx.stream('other').track([{model:'Task',identity:{id:'t-1'}}]);
  // Repeats are one pair: the answer lists each pair once, in first-declaration order.
  shared.track.task({id:'t-1',title:'not identity'});
  shared.track([{model:'Task',identity:{id:'t-1'}}]);
  return enrolledPage();
 }});
 assert.equal(answers.length,1);
 assert.equal(JSON.stringify(answers[0][1]),JSON.stringify(response('handleLoad','enrolled')),'exactly the fixture answer, member order included');
});

test('the Load Stream handle tracks only and closes when the handler settles',async()=>{
 const request=entry('handleLoad').request;
 let escaped,handle;
 const {answers}=await replay([request],{page:({ctx})=>{
  handle=ctx.stream('shared');
  escaped=handle.track.task;
  escaped({id:'t-1'});
  return enrolledPage();
 }});
 assert.deepEqual(answers[0][1].tracking,[{kind:'track',stream:'shared',record:{model:'Task',identity:{id:'t-1'}}}]);
 assert.deepEqual(Object.keys(handle),['track']);
 assert.deepEqual(Object.keys(handle.track),['task']);
 assert.deepEqual(Object.keys(escaped),[]);
 for(const absent of ['remove','invalidate','tag'])assert.equal(absent in handle,false,absent);
 assert.equal('remove' in escaped,false);
 assert.ok(Object.isFrozen(handle)&&Object.isFrozen(handle.track));
 assert.throws(()=>escaped({id:'t-2'}),/closed/);
 assert.throws(()=>handle.track([{model:'Task',identity:{id:'t-2'}}]),/closed/);
 // Reading the answer happens after the handles closed: a getter cannot declare.
 const errors=[];
 const getter=await replay([request],{onError:e=>errors.push(e),page:({ctx})=>{
  const late=ctx.stream('late');
  return {get data(){late.track.task({id:'t-1'});return {tasks:[{id:'t-1'}]};},next:null};
 }});
 assert.equal(getter.answers[0][1].tracking,undefined);
 assert.match(getter.answers[0][1].error,/closed/);
});

test('only declarations feed the enrollment: a returned tracking property is ignored and none is sent when empty',async()=>{
 const request=entry('handleLoad').request;
 const forged=[{kind:'track',stream:'forged',record:{model:'Task',identity:{id:'t-1'}}}];
 const {answers}=await replay([request],{page:()=>({...response('handleLoad','settled'),tracking:forged})});
 assert.deepEqual(answers[0][1],response('handleLoad','settled'));
 assert.equal('tracking' in answers[0][1],false);
 const selected=await replay([request],{page:({ctx})=>{ctx.stream('selected');return response('handleLoad','settled');}});
 assert.equal('tracking' in selected.answers[0][1],false,'selecting a Stream enrolls nothing');
});

test('an enrollment past its bound fails the page as load.page_too_large, even when the handler caught it',async()=>{
 const request=entry('handleLoad').request;
 const overflow=ctx=>{for(let n=0;n<=1000;n++)ctx.stream(`c${n}`).track.task({id:'t-1'});};
 const outcomes=[
  ['caught, then a normal answer',({ctx})=>{try{overflow(ctx);}catch{}return enrolledPage();}],
  ['thrown out of the handler',({ctx})=>{overflow(ctx);return enrolledPage();}],
  ['caught, then a rejection',({ctx})=>{try{overflow(ctx);}catch{}throw new MutationRejected('tasks.refused');}],
  ['caught, then an invalid continuation',({ctx})=>{try{overflow(ctx);}catch{}return {data:{tasks:[]},next:{state:NaN}};}],
  ['after a caught invalid declaration',({ctx})=>{try{ctx.stream('c').track.task({});}catch{}try{overflow(ctx);}catch{}return enrolledPage();}],
 ];
 for(const [label,page] of outcomes){
  const errors=[];
  const {answers}=await replay([request],{onError:e=>errors.push(e),page});
  assert.deepEqual(answers,[['handleLoad',{rejection:'load.page_too_large'}]],label);
  assert.equal(errors.length,1,`${label}: reported once`);
  assert.match(errors[0].message,/more than 1000 Stream\/record pairs/,label);
 }
});

test('a refused declaration fails the page with its message, even when the handler caught it',async()=>{
 const request=entry('handleLoad').request;
 const invalid=[
  ['blank Stream',ctx=>ctx.stream(' '),/Stream name/],
  ['missing identity',ctx=>ctx.stream('c').track.task({}),/Task identity field id is missing/],
  // Not a host fault: an identity the bridge could not send fails the page as the handler's.
  ['lone surrogate identity',ctx=>ctx.stream('c').track.task({id:'t-\ud800'}),/identity must be Unicode text, without a lone surrogate/],
  ['lone surrogate identity in a list',ctx=>ctx.stream('c').track([{model:'Task',identity:{id:'t-1'}},{model:'Task',identity:{id:'\udc00'}}]),/identity must be Unicode text, without a lone surrogate/],
  ['raw identity in a list',ctx=>ctx.stream('c').track([{id:'t-1'}]),/record reference/],
  ['unknown Model',ctx=>ctx.stream('c').track([{model:'Nope',identity:{id:'t-1'}}]),/unknown Model Nope/],
 ];
 for(const [label,declare,pattern] of invalid)
  for(const [how,page] of [
   ['caught, then a normal answer',({ctx})=>{ctx.stream('shared').track.task({id:'t-1'});try{declare(ctx);}catch{}return enrolledPage();}],
   ['caught, then a rejection',({ctx})=>{try{declare(ctx);}catch{}throw new MutationRejected('tasks.refused');}],
   ['thrown out of the handler',({ctx})=>{declare(ctx);return enrolledPage();}],
  ]){
   const errors=[];
   const {answers}=await replay([request],{onError:e=>errors.push(e),options:{translateRejection:()=>'translated'},page});
   assert.equal(answers.length,1);
   const answer=answers[0][1];
   assert.deepEqual(Object.keys(answer),['error'],`${label} ${how}: a failure, never a partial enrollment`);
   assert.match(answer.error,pattern,`${label} ${how}`);
   assert.equal(errors.length,1,`${label} ${how}: reported`);
   assert.equal(errors[0].message,answer.error);
  }
});

test('a Query and Loader carry no declarations; an external transaction has track and invalidate',async()=>{
 const queryConfig=structuredClone(config);
 queryConfig.schema.actions.push({name:'Ask',version:1,kind:'query',inputs:[],outputs:[{name:'message',kind:'value',type:{kind:'scalar',name:'string'},cardinality:'single',source:'handlerValue'}]});
 const asked=[];
 const {answers,loaded}=await replay([{...entry('handleAction').request,name:'Ask'},entry('load').request],{options:{config:queryConfig,queries:{async ask({ctx}){asked.push(ctx);return {message:'asked'};}}}});
 assert.deepEqual(answers[0],['handleAction',{outputs:{message:'asked'},changes:[],declarations:[]}]);
 assert.deepEqual(Object.keys(asked[0]).sort(),['callId','tx','userId']);
 assert.deepEqual(Object.keys(loaded[0]).sort(),['ids','tx','userId']);
 const backend=createBackend({config,native:{validateConfig:c=>native.validateConfig(c),settleExternal:async()=>'[]'},
  database:{transaction:body=>body({}),persistence:()=>fakePersistence([])},authenticate:()=>'alice',
  handlers:{async edit(){}},mutations:{async send(){return {message:'sent'};}},
  loads:{async tasks(){return response('handleLoad','settled');}},loaders:{async task(){return [];}}});
 await backend.transaction(async call=>{
  assert.deepEqual(Object.keys(call).sort(),['invalidate','stream','streams','tx']);
  assert.equal(typeof call.streams(['c']).track.task,'function');
  assert.equal(typeof call.stream('c').track.task,'function');
  assert.equal(typeof call.stream(['c','d']).invalidate.task,'function');
  assert.equal(typeof call.invalidate.task,'function');
 });
});

test('tracking combines all-holder and explicit-pair candidates, deduplicating chunk overlap',async()=>{
 const key={model:'Task',identityKey:'{"id":"t-1"}'};
 const pairs=[{...key,stream:'shared'}];
 const seen=[];
 const driver={transaction:body=>body('tx'),query:async(tx,sql,params)=>{
  seen.push(params);
  return [{model:'Task',identity_key:key.identityKey,stream:'shared'}];
 }};
 assert.deepEqual(await answer(driver,'tx',{op:'readTracking',records:[key],pairs}),pairs);
 assert.deepEqual(JSON.parse(seen[0][0]),[key]);
 assert.deepEqual(JSON.parse(seen[0][1]),pairs);
 const count=seen.length;
 assert.deepEqual(await answer(driver,'tx',{op:'readTracking',records:[],pairs:[]}),[]);
 assert.equal(seen.length,count,'an empty read performs no SQL');
});


test('post-commit legacy diagnostics preserve payloads and tolerate throwing observers',async()=>{
 const page={changes:[{model:'Task',identity:{id:'t-1'},error:'loader.invalid'}]};
 const receipt={rejections:[{ordinal:0,code:'loader.invalid'}]};
 const payloads=[['pull',page],['push',receipt],['pull',{units:[{changes:[{record:{state:{title:'loader.invalid'}}}]}]}],['pull',{items:[{change:{record:{state:{title:'loader.invalid'}}}}]}]];
 for(const [method,payload] of payloads){
  const errors=[];const wire=JSON.stringify(payload);
  const app=createBackend({config,native:{validateConfig:c=>native.validateConfig(c),processPull:async()=>wire,processPush:async()=>wire},database:{transaction:body=>body({}),persistence:()=>fakePersistence([])},authenticate:()=> 'alice',mutations:{send:async()=>({message:'sent'})},handlers:{edit:async()=>{}},loads:{tasks:async()=>enrolledPage()},loaders:{task:async()=>[]},onError:error=>{errors.push(error);throw new Error('observer failed');}});
  assert.equal(await app[method]('alice','{}'),wire);
  assert.equal(errors.length,payload===page||payload===receipt?1:0);
 }
});


test('bound Query tracking fails closed after caught collector refusals and accepts exactly 1000 pairs',async()=>{
 const queryConfig=structuredClone(config);
 queryConfig.schema.actions.push({name:'Ask',version:1,kind:'query',inputs:[],outputs:[{name:'message',kind:'value',type:{kind:'scalar',name:'string'},cardinality:'single',source:'handlerValue'}]});
 const request={...entry('handleAction').request,name:'Ask',context:entry('admitContext').request.context};
 for(const mode of ['overflow','invalid','limit']){
  let escaped;const errors=[];
  const {answers}=await replay([request],{onError:e=>errors.push(e),options:{config:queryConfig,queries:{async ask({ctx}){
   escaped=ctx.stream('User:alice').track.task;
   for(let n=0;n<1000;n++)escaped({id:`q${n}`});
   try{if(mode==='overflow')escaped({id:'too-many'});else if(mode==='invalid')escaped({});}catch{}
   return {message:'asked'};
  }}}});
  if(mode==='limit'){assert.equal(answers[0][1].declarations.length,1000);assert.equal(errors.length,0);}
  else {assert.deepEqual(Object.keys(answers[0][1]),['error']);assert.match(answers[0][1].error,mode==='overflow'?/more than 1000/:/identity field id is missing/);assert.equal(errors.length,1);}
  assert.throws(()=>escaped({id:'late'}),/closed/);
 }
});
