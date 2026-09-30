import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createEffects,createLoadEffects,enrollmentBytes,LOAD_ENROLLMENT_PAIRS,LOAD_ENROLLMENT_BYTES} from '../../../packages/server/effects.mts';
import {Todo,Moment,Pin} from '../../action-runtime-ts/backend.ts';

// The real compiled schema: Todo {id String}, Moment {at DateTime} and the
// composite Pin {todo String, at DateTime}.
const {schema}=JSON.parse(await readFile(new URL('../../action-runtime-ts/backend.json',import.meta.url),'utf8'));
const models=schema.models;
const fresh=()=>createEffects(models);
const empty={changes:[],memberships:[]};
const add=(channel,model,identity)=>({channel,model,identity,present:true});
const remove=(channel,model,identity)=>({channel,model,identity,present:false});

test('declarations snapshot identities at the call, in declaration order',()=>{
 const effects=fresh();
 const identity={id:'A'};
 const channel=effects.channel('project:1');
 channel.todo.add(identity);
 identity.id='B';
 effects.touch.todo({id:'A'});
 assert.deepEqual(effects.settlement(),{
  changes:[{model:'Todo',identity:{id:'A'}}],
  memberships:[{channel:'project:1',model:'Todo',identity:{id:'A'},present:true}],
 });
});

test('membership declarations keep their order and repeats; the engine reduces them',()=>{
 const effects=fresh();
 const a=effects.channel('a'),b=effects.channel('b');
 a.todo.add({id:'1'});
 b.todo.remove({id:'2'});
 a.todo.add({id:'1'});
 a.todo.remove({id:'1'});
 effects.channel('a').todo.add({id:'1'});
 assert.deepEqual(effects.settlement().memberships,[
  add('a','Todo',{id:'1'}),remove('b','Todo',{id:'2'}),add('a','Todo',{id:'1'}),
  remove('a','Todo',{id:'1'}),add('a','Todo',{id:'1'}),
 ]);
 assert.deepEqual(effects.settlement().changes,[],'membership alone declares no change');
});

test('a touch keeps one change per record, in first-declaration order',()=>{
 const effects=fresh();
 effects.touch.todo({id:'first'});
 effects.touch.todo({id:'x'});
 effects.touch.todo({id:'first'});
 effects.touch.todo({id:'x',title:'ignored'});
 assert.deepEqual(effects.settlement(),{changes:[
  {model:'Todo',identity:{id:'first'}},{model:'Todo',identity:{id:'x'}},
 ],memberships:[]});
});

test('only identity fields are copied, and Date components are encoded at the call',()=>{
 const effects=fresh();
 const at=new Date('2026-01-01T00:00:00.000Z');
 const record={at,title:'whole record'};
 effects.channel('c').moment.add(record);
 effects.touch.moment(record);
 at.setUTCFullYear(2030);
 record.title='changed';
 const pin={todo:'t',at:new Date('2026-02-01T00:00:00.000Z'),label:'x'};
 effects.channel('c').pin.remove(pin);
 effects.touch.pin(pin);
 pin.todo='other';
 pin.at.setUTCFullYear(2031);
 // A string DateTime component (a legacy wire identity) passes through for the engine to canonicalize.
 effects.touch.moment({at:'2026-03-01T00:00:00.000Z'});
 assert.deepEqual(effects.settlement(),{
  changes:[
   {model:'Moment',identity:{at:'2026-01-01T00:00:00.000Z'}},
   {model:'Pin',identity:{todo:'t',at:'2026-02-01T00:00:00.000Z'}},
   {model:'Moment',identity:{at:'2026-03-01T00:00:00.000Z'}},
  ],
  memberships:[
   add('c','Moment',{at:'2026-01-01T00:00:00.000Z'}),
   remove('c','Pin',{todo:'t',at:'2026-02-01T00:00:00.000Z'}),
  ],
 });
});

test('mixed calls take explicit references, including the generated constructors',()=>{
 const effects=fresh();
 const at=new Date('2026-01-01T00:00:00.000Z');
 const refs=[Todo({id:'A'}),Moment({at}),Pin({todo:'t',at}),{model:'Todo',identity:{id:'B',title:'extra'}}];
 effects.channel('mixed').add(refs);
 at.setUTCFullYear(2040);
 effects.channel('mixed').remove([Todo({id:'A'})]);
 effects.channel('mixed').add([]);
 effects.channel('mixed').remove([]);
 assert.deepEqual(effects.settlement(),{changes:[],memberships:[
  add('mixed','Todo',{id:'A'}),add('mixed','Moment',{at:'2026-01-01T00:00:00.000Z'}),
  add('mixed','Pin',{todo:'t',at:'2026-01-01T00:00:00.000Z'}),add('mixed','Todo',{id:'B'}),
  remove('mixed','Todo',{id:'A'}),
 ]});
});

test('missing, null, malformed and unknown references fail at the call',()=>{
 const effects=fresh();
 const channel=effects.channel('c');
 for(const identity of [undefined,null,'A',{},{id:null},{id:1},{title:'no id'}])
  assert.throws(()=>channel.todo.add(identity),/Todo/,JSON.stringify(identity));
 assert.throws(()=>effects.touch.todo({}),/Todo identity field id/);
 assert.throws(()=>effects.touch.moment({at:new Date('nope')}),/Moment identity field at/);
 assert.throws(()=>effects.touch.moment({at:'yesterday'}),/Moment identity field at/);
 assert.throws(()=>effects.touch.pin({todo:'t'}),/Pin identity field at/);
 assert.throws(()=>channel.add('x'),/array of record references/);
 assert.throws(()=>channel.add(Todo({id:'A'})),/array of record references/);
 assert.throws(()=>channel.add([null]),/record reference/);
 assert.throws(()=>channel.add([{model:'Nope',identity:{id:'x'}}]),/unknown Model Nope/);
 assert.throws(()=>channel.add([{model:'Todo'}]),/Todo identity/);
 // A raw identity names no Model, so a mixed call cannot place it.
 assert.throws(()=>channel.add([{id:'A'}]),/record reference/);
 assert.throws(()=>channel.remove([{id:'A'}]),/record reference/);
 assert.equal(effects.touch.nope,undefined);
 assert.equal(channel.nope,undefined);
 assert.deepEqual(effects.settlement(),empty,'nothing failed half-way into the declarations');
});

test('UUID, enum and date-time components follow the engine rules at the declaration',()=>{
 const effects=createEffects([...models,
  {name:'Ticket',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'uuid'},nullable:false}]},
  {name:'Tag',identity:['kind'],fields:[{name:'kind',type:{kind:'enum',name:'Kind'},nullable:false}]},
 ],[{name:'Kind',values:['a','b']}]);
 const channel=effects.channel('c');
 // 36 characters, hyphenated, RFC 4122 variant, version 1 to 8.
 for(const id of ['not-a-uuid','123e4567e89b42d3a456426614174000','{123e4567-e89b-42d3-a456-426614174000}','urn:uuid:123e4567-e89b-42d3-a456-426614174000','123e4567-e89b-02d3-a456-426614174000','123e4567-e89b-92d3-a456-426614174000','123e4567-e89b-42d3-c456-426614174000','123e4567-e89b-42d3-a456-42661417400g']){
  assert.throws(()=>effects.touch.ticket({id}),/Ticket identity field id must be a UUID/,id);
  assert.throws(()=>channel.ticket.add({id}),/Ticket identity field id must be a UUID/,id);
  assert.throws(()=>channel.add([{model:'Ticket',identity:{id}}]),/Ticket identity field id must be a UUID/,id);
 }
 // An enum component is one of its enum's values, exactly.
 for(const kind of ['c','A',''])assert.throws(()=>effects.touch.tag({kind}),/Tag identity field kind must be one of a, b/,kind);
 assert.throws(()=>channel.tag.remove({kind:'c'}),/one of a, b/);
 // A date-time string is zoned RFC 3339 with real calendar fields; a Date must encode to one.
 for(const at of ['2026-01-01T00:00:00','2026-01-01 00:00:00Z','2026-01-01t00:00:00Z','2026-13-01T00:00:00Z','2026-02-29T00:00:00Z','2026-04-31T00:00:00Z','2026-01-01T24:00:00Z','2026-01-01T00:60:00Z','2026-01-01T00:00:00+0100','2026-01-01T00:00:00+24:00','2026-01-01T00:00:00.1234567890Z','26-01-01T00:00:00Z',new Date('+010000-01-01T00:00:00Z')])
  assert.throws(()=>effects.touch.moment({at}),/Moment identity field at must be a valid Date or a zoned RFC 3339 date-time string/,String(at));
 assert.throws(()=>channel.add([Todo({id:'A'}),{model:'Ticket',identity:{id:'not-a-uuid'}}]),/UUID/);
 assert.deepEqual(effects.settlement(),empty,'no refused declaration appended an intent');
 effects.touch.ticket({id:'123E4567-E89B-82D3-B456-426614174000'});
 effects.touch.tag({kind:'b'});
 for(const at of ['2024-02-29T00:00:00Z','2026-01-01T00:00:00.123456789+05:30','2026-01-01T00:00:00z'])effects.touch.moment({at});
 assert.equal(effects.settlement().changes.length,5);
});

test('a mixed call with a later invalid element appends nothing, even when caught',()=>{
 const effects=fresh();
 const channel=effects.channel('c');
 assert.throws(()=>channel.add([Todo({id:'A'}),Todo({id:'B'}),{id:'C'}]),/record reference/);
 try{channel.remove([Todo({id:'A'}),{model:'Todo',identity:{}}]);}catch{}
 assert.deepEqual(effects.settlement(),empty);
 channel.add([Todo({id:'A'})]);
 assert.deepEqual(effects.settlement().memberships,[add('c','Todo',{id:'A'})]);
});

test('a Channel name must be a nonblank string; selecting one declares nothing',()=>{
 const effects=fresh();
 for(const name of ['','  ','\n',undefined,null,1,{}])
  assert.throws(()=>effects.channel(name),/Channel name/,String(name));
 effects.channel('selected');
 assert.deepEqual(effects.settlement(),empty);
});

test('handles are null-prototype dictionaries; __proto__, constructor and function member names are ordinary Models',()=>{
 const scalar={kind:'scalar',name:'string'};
 const effects=createEffects([
  {name:'__proto__',identity:['id'],fields:[{name:'id',type:scalar,nullable:false}]},
  {name:'Constructor',identity:['__proto__'],fields:[{name:'__proto__',type:scalar,nullable:false}]},
  {name:'ToString',identity:['id'],fields:[{name:'id',type:scalar,nullable:false}]},
  // A Channel handle is no function, so function members need no reservation either.
  ...['Name','Length','Bind','Apply','Call'].map(name=>({name,identity:['id'],fields:[{name:'id',type:scalar,nullable:false}]})),
 ]);
 assert.equal(Object.getPrototypeOf(effects.touch),null);
 const keys=['__proto__','constructor','toString','name','length','bind','apply','call'];
 assert.deepEqual(Object.keys(effects.touch),keys);
 const channel=effects.channel('c');
 assert.equal(Object.getPrototypeOf(channel),null);
 assert.deepEqual(Object.keys(channel),[...keys,'add','remove']);
 assert.equal(typeof channel,'object');
 channel.name.add({id:'n'});
 channel.length.remove({id:'l'});
 effects.touch.call({id:'k'});
 channel.__proto__.add({id:'p'});
 channel.constructor.remove({['__proto__']:'c'});
 effects.touch.toString({id:'s'});
 effects.touch.__proto__({id:'p'});
 const identity=Object.defineProperty({},'__proto__',{value:'c',enumerable:true});
 assert.deepEqual(effects.settlement(),{
  changes:[{model:'Call',identity:{id:'k'}},{model:'ToString',identity:{id:'s'}},{model:'__proto__',identity:{id:'p'}}],
  memberships:[add('c','Name',{id:'n'}),remove('c','Length',{id:'l'}),add('c','__proto__',{id:'p'}),remove('c','Constructor',identity)],
 });
 assert.equal(Object.hasOwn(effects.settlement().memberships[3].identity,'__proto__'),true);
 assert.equal({}.id,undefined,'Object.prototype is untouched');
 assert.equal(Object.getPrototypeOf(fresh().touch),null);
});

test('a malformed runtime config is refused: duplicate accessors and the reserved Channel keys',()=>{
 const model=name=>({name,identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false}]});
 assert.throws(()=>createEffects([model('Todo'),model('todo')]),/Models Todo and todo both generate the accessor todo/);
 assert.throws(()=>createEffects([model('Add')]),/Model Add generates the accessor add, which a Channel reserves/);
 assert.throws(()=>createEffects([model('remove')]),/Model remove generates the accessor remove, which a Channel reserves/);
 assert.throws(()=>createEffects([{name:'Todo',fields:[]}]),/Model Todo/);
 assert.throws(()=>createEffects([{name:'Todo',identity:['id'],fields:[]}]),/Todo identity field id/);
 assert.throws(()=>createEffects([{name:'Tag',identity:['kind'],fields:[{name:'kind',type:{kind:'enum',name:'Kind'}}]}]),/Tag identity field kind names an enum the configuration does not declare/);
});

test('closing refuses every later declaration, including through escaped handles; settlement stays readable',()=>{
 const effects=fresh();
 const channel=effects.channel('c');
 const todo=channel.todo;
 const touch=effects.touch;
 channel.todo.add({id:'A'});
 touch.todo({id:'A'});
 effects.close();
 const closed=/closed/;
 assert.throws(()=>todo.add({id:'B'}),closed);
 assert.throws(()=>channel.todo.remove({id:'B'}),closed);
 assert.throws(()=>channel.add([]),closed);
 assert.throws(()=>channel.remove([Todo({id:'B'})]),closed);
 assert.throws(()=>touch.todo({id:'B'}),closed);
 assert.throws(()=>effects.channel('c'),closed);
 const expected={changes:[{model:'Todo',identity:{id:'A'}}],memberships:[add('c','Todo',{id:'A'})]};
 const settled=effects.settlement();
 assert.deepEqual(settled,expected);
 // The answer is a copy of owned, frozen declarations.
 settled.changes.push({model:'Todo',identity:{id:'forged'}});
 assert.throws(()=>{settled.memberships[0].present=false;},TypeError);
 assert.throws(()=>{settled.memberships[0].identity.id='forged';},TypeError);
 assert.deepEqual(effects.settlement(),expected);
 effects.close();
 assert.deepEqual(effects.settlement(),expected,'closing twice is harmless');
});

test('a Model outside the loaded set is device-only: every declaration naming it fails at the call',()=>{
 const effects=createEffects(models,[],new Set(['Todo','Pin']));
 const refused=caller=>({message:`${caller}: Model Moment has no Loader, so it is device-only and cannot be published`});
 const at=new Date(0);
 assert.throws(()=>effects.touch.moment({at}),refused('touch.moment'));
 assert.throws(()=>effects.channel('c').moment.add({at}),refused('channel("c").moment.add'));
 assert.throws(()=>effects.channel('c').moment.remove({at}),refused('channel("c").moment.remove'));
 assert.throws(()=>effects.channel('c').add([Todo({id:'t'}),Moment({at})]),refused('channel("c").add'));
 assert.throws(()=>effects.channel('c').remove([Moment({at})]),refused('channel("c").remove'));
 // Loaded Models declare as before; the refused mixed call appended nothing.
 effects.touch.todo({id:'t'});
 effects.channel('c').todo.add({id:'t'});
 assert.deepEqual(effects.settlement(),{changes:[{model:'Todo',identity:{id:'t'}}],memberships:[add('c','Todo',{id:'t'})]});
 // Without a loaded set every Model declares, as a collector always did.
 fresh().touch.moment({at});
});

// The add-only collector a Load handler declares through: `ctx.channel(name)`.
const limits=JSON.parse(await readFile(new URL('../../../fixtures/protocol/load-enrollment-limits.json',import.meta.url),'utf8'));
const scalar=name=>({kind:'scalar',name});
const ticket={name:'Ticket',identity:['id'],fields:[{name:'id',type:scalar('uuid'),nullable:false}]};
const freshLoad=(loaded)=>createLoadEffects([...models,ticket],[],loaded);

test('a Load Channel handle adds only: no remove or touch exists on any runtime object',()=>{
 const effects=freshLoad();
 const channel=effects.channel('c');
 assert.equal(Object.getPrototypeOf(channel),null);
 assert.ok(Object.isFrozen(channel));
 assert.deepEqual(Object.keys(channel),['todo','moment','pin','ticket','add']);
 for(const key of ['todo','moment','pin','ticket']){
  assert.equal(Object.getPrototypeOf(channel[key]),null);
  assert.ok(Object.isFrozen(channel[key]),key);
  assert.deepEqual(Object.keys(channel[key]),['add'],key);
  assert.equal('remove' in channel[key],false,key);
 }
 assert.equal('remove' in channel,false);
 assert.equal('touch' in channel,false);
 assert.equal('touch' in effects,false,'the collector has no touch to hand out');
 assert.equal('settlement' in effects,false,'nor a change settlement');
 assert.ok(Object.isFrozen(effects));
 // The facade cannot be widened after the fact.
 assert.throws(()=>{channel.remove=()=>{};},TypeError);
 assert.throws(()=>{channel.todo.remove=()=>{};},TypeError);
});

test('Load declarations snapshot typed identities at the call, in first-declaration order',()=>{
 const effects=freshLoad();
 const identity={id:'A',title:'not identity'};
 const at=new Date('2026-01-01T00:00:00.000Z');
 const pin={todo:'t',at:new Date('2026-02-01T00:00:00.000Z'),label:'x'};
 const channel=effects.channel('project:1');
 channel.todo.add(identity);
 channel.moment.add({at});
 channel.pin.add(pin);
 channel.ticket.add({id:'123e4567-e89b-42d3-a456-426614174000'});
 identity.id='B';at.setUTCFullYear(2030);pin.todo='other';pin.at.setUTCFullYear(2031);
 assert.deepEqual(effects.memberships(),[
  add('project:1','Todo',{id:'A'}),
  add('project:1','Moment',{at:'2026-01-01T00:00:00.000Z'}),
  add('project:1','Pin',{todo:'t',at:'2026-02-01T00:00:00.000Z'}),
  add('project:1','Ticket',{id:'123e4567-e89b-42d3-a456-426614174000'}),
 ]);
 assert.equal(effects.failure(),undefined);
 // Owned, frozen copies.
 const listed=effects.memberships();
 listed.push(add('forged','Todo',{id:'x'}));
 assert.throws(()=>{listed[0].identity.id='forged';},TypeError);
 assert.equal(effects.memberships().length,4);
});

test('a repeated Channel/record pair is one intent, however it is spelled; another Channel is another pair',()=>{
 const effects=freshLoad();
 const a=effects.channel('a');
 a.todo.add({id:'1'});
 a.add([Todo({id:'1'}),Todo({id:'1'})]);
 effects.channel('a').todo.add({id:'1'});
 effects.channel('b').todo.add({id:'1'});
 // The engine canonicalizes a UUID to lower case and a date-time to UTC
 // milliseconds; the collector deduplicates by the same canonical record.
 a.ticket.add({id:'123E4567-E89B-42D3-A456-426614174000'});
 a.ticket.add({id:'123e4567-e89b-42d3-a456-426614174000'});
 a.moment.add({at:new Date('2026-01-01T00:00:00.000Z')});
 a.moment.add({at:'2026-01-01T05:30:00.000999+05:30'});
 a.moment.add({at:'2026-01-01T00:00:00z'});
 a.add([Pin({todo:'t',at:new Date(0)}),Pin({todo:'t',at:'1970-01-01T00:00:00Z'})]);
 assert.deepEqual(effects.memberships(),[
  add('a','Todo',{id:'1'}),add('b','Todo',{id:'1'}),
  add('a','Ticket',{id:'123E4567-E89B-42D3-A456-426614174000'}),
  add('a','Moment',{at:'2026-01-01T00:00:00.000Z'}),
  add('a','Pin',{todo:'t',at:'1970-01-01T00:00:00.000Z'}),
 ]);
});

test('a mixed Load list takes explicit references and appends nothing when any element fails',()=>{
 const effects=freshLoad();
 const at=new Date('2026-01-01T00:00:00.000Z');
 const channel=effects.channel('mixed');
 channel.add([Todo({id:'A'}),Moment({at}),Pin({todo:'t',at}),{model:'Todo',identity:{id:'B',title:'extra'}}]);
 channel.add([]);
 assert.deepEqual(effects.memberships(),[
  add('mixed','Todo',{id:'A'}),add('mixed','Moment',{at:'2026-01-01T00:00:00.000Z'}),
  add('mixed','Pin',{todo:'t',at:'2026-01-01T00:00:00.000Z'}),add('mixed','Todo',{id:'B'}),
 ]);
 assert.equal(effects.failure(),undefined);
 assert.throws(()=>channel.add([Todo({id:'C'}),{id:'D'}]),/record reference/);
 assert.equal(effects.memberships().length,4,'the refused list appended nothing');
});

test('a Load identity with a lone surrogate is refused at the declaration; a mixed list appends none of its pairs',()=>{
 const effects=freshLoad();
 const channel=effects.channel('c');
 // A well-formed pair of surrogates is Unicode text and declares as usual.
 channel.todo.add({id:'emoji \ud83d\ude00'});
 assert.throws(()=>channel.todo.add({id:'\ud800'}),{message:'channel("c").todo.add: Todo identity field id must be Unicode text, without a lone surrogate'});
 assert.throws(()=>channel.pin.add({todo:'t\udfff',at:new Date(0)}),{message:'channel("c").pin.add: Pin identity field todo must be Unicode text, without a lone surrogate'});
 assert.throws(()=>channel.add([Todo({id:'A'}),Todo({id:'B\ud800'}),Todo({id:'C'})]),{message:'channel("c").add: Todo identity field id must be Unicode text, without a lone surrogate'});
 assert.deepEqual(effects.memberships(),[add('c','Todo',{id:'emoji \ud83d\ude00'})],'no refused pair, and nothing from the refused list');
 assert.equal(effects.failure().kind,'invalid','the caught refusal leaves the collector failed');
 assert.match(effects.failure().error.message,/todo\.add: Todo identity field id must be Unicode text/,'the first refusal wins');
});

test('every refused Load declaration leaves the collector failed with its first error, even when caught',()=>{
 const cases=[
  ['blank Channel',e=>e.channel('  '),/Channel name/],
  ['non-string Channel',e=>e.channel(7),/Channel name/],
  // A lone surrogate is not Unicode text: the engine could not decode the answer.
  ['lone surrogate Channel',e=>e.channel('a\ud800'),/Channel name/],
  // Nor in a string identity component: the host could not send the answer.
  ['lone surrogate identity',e=>e.channel('c').todo.add({id:'a\udc00'}),/Todo identity field id must be Unicode text/],
  ['lone surrogate identity in a list',e=>e.channel('c').add([Todo({id:'A'}),Pin({todo:'\ud800b',at:new Date(0)})]),/Pin identity field todo must be Unicode text/],
  ['missing identity',e=>e.channel('c').todo.add({}),/Todo identity field id is missing/],
  ['malformed identity',e=>e.channel('c').todo.add({id:1}),/Todo identity field id must be a string/],
  ['bad UUID',e=>e.channel('c').ticket.add({id:'nope'}),/UUID/],
  ['invalid Date',e=>e.channel('c').moment.add({at:new Date('nope')}),/Moment identity field at/],
  ['not a list',e=>e.channel('c').add(Todo({id:'A'})),/array of record references/],
  ['raw identity in a list',e=>e.channel('c').add([{id:'A'}]),/record reference/],
  ['unknown Model',e=>e.channel('c').add([{model:'Nope',identity:{id:'x'}}]),/unknown Model Nope/],
 ];
 for(const [label,declare,pattern] of cases){
  const effects=freshLoad();
  effects.channel('ok').todo.add({id:'kept'});
  let thrown;
  try{declare(effects);}catch(error){thrown=error;}
  assert.match(thrown?.message??'',pattern,label);
  // Later valid declarations still record, but cannot clear the failure.
  effects.channel('ok').todo.add({id:'later'});
  const failure=effects.failure();
  assert.equal(failure?.kind,'invalid',label);
  assert.equal(failure.error,thrown,`${label}: the first refused declaration`);
  try{effects.channel('').todo;}catch{}
  assert.equal(effects.failure().error,thrown,`${label}: the first failure wins`);
 }
});

test('a device-only Model is refused at every Load declaration naming it',()=>{
 const effects=freshLoad(new Set(['Todo','Pin']));
 const at=new Date(0);
 const refused=caller=>({message:`${caller}: Model Moment has no Loader, so it is device-only and cannot be published`});
 assert.throws(()=>effects.channel('c').moment.add({at}),refused('channel("c").moment.add'));
 assert.throws(()=>effects.channel('c').add([Todo({id:'t'}),Moment({at})]),refused('channel("c").add'));
 assert.equal(effects.failure().kind,'invalid');
 effects.channel('c').todo.add({id:'t'});
 assert.deepEqual(effects.memberships(),[add('c','Todo',{id:'t'})]);
});

test('closing refuses every later Load declaration, including through escaped handles',()=>{
 const effects=freshLoad();
 const channel=effects.channel('c');
 const todo=channel.todo;
 todo.add({id:'A'});
 effects.close();
 assert.throws(()=>todo.add({id:'B'}),/closed/);
 assert.throws(()=>channel.add([Todo({id:'B'})]),/closed/);
 assert.throws(()=>effects.channel('c'),/closed/);
 assert.deepEqual(effects.memberships(),[add('c','Todo',{id:'A'})],'still readable after close');
 effects.close();
});

test('the Load enrollment bounds are the shared fixture, and a pair measures as the engine encodes it',()=>{
 assert.equal(LOAD_ENROLLMENT_PAIRS,limits.pairs);
 assert.equal(LOAD_ENROLLMENT_BYTES,limits.bytes);
 assert.ok(limits.intents.length>0);
 for(const {name,intent,bytes,encoded} of limits.intents){
  assert.equal(Buffer.byteLength(encoded,'utf8'),bytes,`${name}: the fixture agrees with itself`);
  assert.equal(enrollmentBytes(intent),bytes,name);
 }
});

const pairBytes=channel=>enrollmentBytes(add(channel,'Todo',{id:'t1'}));

test('Load enrollment is bounded by distinct pairs after deduplication; the pair past the bound throws and stays failed',()=>{
 const channels=Array.from({length:LOAD_ENROLLMENT_PAIRS},(_,n)=>`c${n}`);
 const effects=freshLoad();
 for(const name of [...channels,...channels])effects.channel(name).todo.add({id:'t1'});
 assert.equal(effects.memberships().length,LOAD_ENROLLMENT_PAIRS,'exactly the bound, each pair declared twice');
 assert.equal(effects.failure(),undefined);
 let thrown;
 try{effects.channel('one-more').todo.add({id:'t1'});}catch(error){thrown=error;}
 assert.match(thrown?.message??'',/more than 1000 Channel\/record pairs/);
 assert.deepEqual(effects.failure(),{kind:'overflow',error:thrown});
 assert.equal(effects.memberships().length,LOAD_ENROLLMENT_PAIRS,'the crossing pair is not stored');
 // A repeat of a counted pair is still no new pair.
 effects.channel('c0').todo.add({id:'t1'});
 assert.equal(effects.failure().kind,'overflow','the overflow is kept');
});

test('Load enrollment is bounded by encoded bytes after deduplication, one byte over fails',()=>{
 const count=256,each=LOAD_ENROLLMENT_BYTES/count;
 const channels=Array.from({length:count},(_,n)=>{const name=String(n).padStart(3,'0');return name+'x'.repeat(each-pairBytes(name));});
 assert.equal(channels.reduce((sum,c)=>sum+pairBytes(c),0),LOAD_ENROLLMENT_BYTES);
 const effects=freshLoad();
 for(const name of channels)effects.channel(name).todo.add({id:'t1'});
 effects.channel(channels[0]).add([Todo({id:'t1'})]);
 assert.equal(effects.failure(),undefined,'a repeated pair counts once');
 assert.equal(effects.memberships().length,count);
 const over=freshLoad();
 for(const name of channels.slice(0,-1))over.channel(name).todo.add({id:'t1'});
 assert.throws(()=>over.channel(channels.at(-1)+'x').todo.add({id:'t1'}),/more than 1048576 bytes/);
 assert.equal(over.failure().kind,'overflow');
});

test('a mixed list that crosses the bound appends none of its pairs; overflow outranks an earlier invalid declaration',()=>{
 const effects=freshLoad();
 try{effects.channel('c').todo.add({});}catch{}
 assert.equal(effects.failure().kind,'invalid');
 for(let n=0;n<LOAD_ENROLLMENT_PAIRS-1;n++)effects.channel('c').todo.add({id:`t${n}`});
 assert.throws(()=>effects.channel('c').add([Todo({id:'t0'}),Todo({id:'new-1'}),Todo({id:'new-1'}),Todo({id:'new-2'})]),/more than 1000/);
 assert.equal(effects.memberships().length,LOAD_ENROLLMENT_PAIRS-1,'nothing from the crossing list');
 assert.equal(effects.failure().kind,'overflow');
 // Duplicates within a list count once: exactly one new pair fits.
 const fits=freshLoad();
 for(let n=0;n<LOAD_ENROLLMENT_PAIRS-1;n++)fits.channel('c').todo.add({id:`t${n}`});
 fits.channel('c').add([Todo({id:'t0'}),Todo({id:'new-1'}),Todo({id:'new-1'})]);
 assert.equal(fits.memberships().length,LOAD_ENROLLMENT_PAIRS);
 assert.equal(fits.failure(),undefined);
});
