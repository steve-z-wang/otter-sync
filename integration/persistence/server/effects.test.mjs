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
const add=(scope,model,identity,tags=[])=>({kind:'add',scope,record:{model,identity},tags});
const remove=(scope,model,identity)=>({kind:'remove',scope,record:{model,identity}});
const removeTag=(scope,tag)=>({kind:'select',scope,predicate:{tags:{all:[tag]}},action:{kind:'remove'}});
const tagAdd=(scope,model,identity,tags)=>({...add(scope,model,identity,tags),kind:'tagAdd'});

test('declarations snapshot identities at the call, in declaration order',()=>{
 const effects=fresh();
 const identity={id:'A'};
 const scope=effects.scope('project:1');
 scope.add.todo(identity);
 identity.id='B';
 effects.touch.todo({id:'A'});
 assert.deepEqual(effects.settlement(),{
  changes:[{model:'Todo',identity:{id:'A'}}],
  memberships:[add('project:1','Todo',{id:'A'})],
 });
});

test('membership declarations keep their order and repeats; the engine reduces them',()=>{
 const effects=fresh();
 const a=effects.scope('a'),b=effects.scope('b');
 a.add.todo({id:'1'});
 b.remove.todo({id:'2'});
 a.add.todo({id:'1'});
 a.remove.todo({id:'1'});
 effects.scope('a').add.todo({id:'1'});
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
 effects.scope('c').add.moment(record);
 effects.touch.moment(record);
 at.setUTCFullYear(2030);
 record.title='changed';
 const pin={todo:'t',at:new Date('2026-02-01T00:00:00.000Z'),label:'x'};
 effects.scope('c').remove.pin(pin);
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
 effects.scope('mixed').add(refs);
 at.setUTCFullYear(2040);
 effects.scope('mixed').remove([Todo({id:'A'})]);
 effects.scope('mixed').add([]);
 effects.scope('mixed').remove([]);
 assert.deepEqual(effects.settlement(),{changes:[],memberships:[
  add('mixed','Todo',{id:'A'}),add('mixed','Moment',{at:'2026-01-01T00:00:00.000Z'}),
  add('mixed','Pin',{todo:'t',at:'2026-01-01T00:00:00.000Z'}),add('mixed','Todo',{id:'B'}),
  remove('mixed','Todo',{id:'A'}),
 ]});
});

test('missing, null, malformed and unknown references fail at the call',()=>{
 const effects=fresh();
 const scope=effects.scope('c');
 for(const identity of [undefined,null,{},{id:null},{id:1},{title:'no id'}])
  assert.throws(()=>scope.add.todo(identity),/Todo/,JSON.stringify(identity));
 assert.throws(()=>effects.touch.todo({}),/Todo identity field id/);
 assert.throws(()=>effects.touch.moment({at:new Date('nope')}),/Moment identity field at/);
 assert.throws(()=>effects.touch.moment({at:'yesterday'}),/Moment identity field at/);
 assert.throws(()=>effects.touch.pin({todo:'t'}),/Pin identity field at/);
 assert.throws(()=>scope.add('x'),/record reference/);
 assert.throws(()=>scope.add({id:'A'}),/record reference/);
 assert.throws(()=>scope.add([null]),/record reference/);
 assert.throws(()=>scope.add([{model:'Nope',identity:{id:'x'}}]),/unknown Model Nope/);
 assert.throws(()=>scope.add([{model:'Todo'}]),/Todo identity/);
 // A raw identity names no Model, so a mixed call cannot place it.
 assert.throws(()=>scope.add([{id:'A'}]),/record reference/);
 assert.throws(()=>scope.remove([{id:'A'}]),/record reference/);
 assert.equal(effects.touch.nope,undefined);
 assert.equal(scope.nope,undefined);
 assert.deepEqual(effects.settlement(),empty,'nothing failed half-way into the declarations');
});

test('UUID, enum and date-time components follow the engine rules at the declaration',()=>{
 const effects=createEffects([...models,
  {name:'Ticket',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'uuid'},nullable:false}]},
  {name:'Tag',identity:['kind'],fields:[{name:'kind',type:{kind:'enum',name:'Kind'},nullable:false}]},
 ],[{name:'Kind',values:['a','b']}]);
 const scope=effects.scope('c');
 // 36 characters, hyphenated, RFC 4122 variant, version 1 to 8.
 for(const id of ['not-a-uuid','123e4567e89b42d3a456426614174000','{123e4567-e89b-42d3-a456-426614174000}','urn:uuid:123e4567-e89b-42d3-a456-426614174000','123e4567-e89b-02d3-a456-426614174000','123e4567-e89b-92d3-a456-426614174000','123e4567-e89b-42d3-c456-426614174000','123e4567-e89b-42d3-a456-42661417400g']){
  assert.throws(()=>effects.touch.ticket({id}),/Ticket identity field id must be a UUID/,id);
  assert.throws(()=>scope.add.ticket({id}),/Ticket identity field id must be a UUID/,id);
  assert.throws(()=>scope.add([{model:'Ticket',identity:{id}}]),/Ticket identity field id must be a UUID/,id);
 }
 // An enum component is one of its enum's values, exactly.
 for(const kind of ['c','A',''])assert.throws(()=>effects.touch.tag({kind}),/Tag identity field kind must be one of a, b/,kind);
 assert.throws(()=>scope.remove.tag({kind:'c'}),/one of a, b/);
 // A date-time string is zoned RFC 3339 with real calendar fields; a Date must encode to one.
 for(const at of ['2026-01-01T00:00:00','2026-01-01 00:00:00Z','2026-01-01t00:00:00Z','2026-13-01T00:00:00Z','2026-02-29T00:00:00Z','2026-04-31T00:00:00Z','2026-01-01T24:00:00Z','2026-01-01T00:60:00Z','2026-01-01T00:00:00+0100','2026-01-01T00:00:00+24:00','2026-01-01T00:00:00.1234567890Z','26-01-01T00:00:00Z',new Date('+010000-01-01T00:00:00Z')])
  assert.throws(()=>effects.touch.moment({at}),/Moment identity field at must be a valid Date or a zoned RFC 3339 date-time string/,String(at));
 assert.throws(()=>scope.add([Todo({id:'A'}),{model:'Ticket',identity:{id:'not-a-uuid'}}]),/UUID/);
 assert.deepEqual(effects.settlement(),empty,'no refused declaration appended an intent');
 effects.touch.ticket({id:'123E4567-E89B-82D3-B456-426614174000'});
 effects.touch.tag({kind:'b'});
 for(const at of ['2024-02-29T00:00:00Z','2026-01-01T00:00:00.123456789+05:30','2026-01-01T00:00:00z'])effects.touch.moment({at});
 assert.equal(effects.settlement().changes.length,5);
});

test('a mixed call with a later invalid element appends nothing, even when caught',()=>{
 const effects=fresh();
 const scope=effects.scope('c');
 assert.throws(()=>scope.add([Todo({id:'A'}),Todo({id:'B'}),{id:'C'}]),/record reference/);
 try{scope.remove([Todo({id:'A'}),{model:'Todo',identity:{}}]);}catch{}
 assert.deepEqual(effects.settlement(),empty);
 scope.add([Todo({id:'A'})]);
 assert.deepEqual(effects.settlement().memberships,[add('c','Todo',{id:'A'})]);
});

test('a Scope name must be a nonblank string; selecting one declares nothing',()=>{
 const effects=fresh();
 for(const name of ['','  ','\n',undefined,null,1,{}])
  assert.throws(()=>effects.scope(name),/name/,String(name));
 effects.scope('selected');
 assert.deepEqual(effects.settlement(),empty);
});

test('handles are null-prototype dictionaries; __proto__, constructor and function member names are ordinary Models',()=>{
 const scalar={kind:'scalar',name:'string'};
 const effects=createEffects([
  {name:'__proto__',identity:['id'],fields:[{name:'id',type:scalar,nullable:false}]},
  {name:'Constructor',identity:['__proto__'],fields:[{name:'__proto__',type:scalar,nullable:false}]},
  {name:'ToString',identity:['id'],fields:[{name:'id',type:scalar,nullable:false}]},
  // A Scope handle is no function, so function members need no reservation either.
  ...['Name','Length','Bind','Apply','Call'].map(name=>({name,identity:['id'],fields:[{name:'id',type:scalar,nullable:false}]})),
 ]);
 assert.equal(Object.getPrototypeOf(effects.touch),null);
 const keys=['__proto__','constructor','toString','name','length','bind','apply','call'];
 assert.deepEqual(Object.keys(effects.touch),keys);
 const scope=effects.scope('c');
 assert.equal(Object.getPrototypeOf(Object.getPrototypeOf(scope)),null);
 assert.deepEqual(Object.keys(scope.add),keys);
 assert.deepEqual(Object.keys(scope.remove),keys);
 assert.deepEqual(Object.keys(scope),['add','remove','tag','where']);
 assert.equal(typeof scope,'object');
 scope.add.name({id:'n'});
 scope.remove.length({id:'l'});
 effects.touch.call({id:'k'});
 scope.add.__proto__({id:'p'});
 scope.remove.constructor({['__proto__']:'c'});
 effects.touch.toString({id:'s'});
 effects.touch.__proto__({id:'p'});
 const identity=Object.defineProperty({},'__proto__',{value:'c',enumerable:true});
 assert.deepEqual(effects.settlement(),{
  changes:[{model:'Call',identity:{id:'k'}},{model:'ToString',identity:{id:'s'}},{model:'__proto__',identity:{id:'p'}}],
  memberships:[add('c','Name',{id:'n'}),remove('c','Length',{id:'l'}),add('c','__proto__',{id:'p'}),remove('c','Constructor',identity)],
 });
 assert.equal(Object.hasOwn(effects.settlement().memberships[3].record.identity,'__proto__'),true);
 assert.equal({}.id,undefined,'Object.prototype is untouched');
 assert.equal(Object.getPrototypeOf(fresh().touch),null);
});

test('runtime configuration refuses duplicate accessors and allows operation names as Models',()=>{
 const model=name=>({name,identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false}]});
 assert.throws(()=>createEffects([model('Todo'),model('todo')]),/Models Todo and todo both generate the accessor todo/);
 createEffects([model('Add')]).scope('U').add.add('A');
 createEffects([model('remove')]).scope('U').remove.remove('A');
 assert.throws(()=>createEffects([{name:'Todo',fields:[]}]),/Model Todo/);
 assert.throws(()=>createEffects([{name:'Todo',identity:['id'],fields:[]}]),/Todo identity field id/);
 assert.throws(()=>createEffects([{name:'Tag',identity:['kind'],fields:[{name:'kind',type:{kind:'enum',name:'Kind'}}]}]),/Tag identity field kind names an enum the configuration does not declare/);
});

test('closing refuses every later declaration, including through escaped handles; settlement stays readable',()=>{
 const effects=fresh();
 const scope=effects.scope('c');
 const todo={add: scope.add.todo};
 const touch=effects.touch;
 scope.add.todo({id:'A'});
 touch.todo({id:'A'});
 effects.close();
 const closed=/closed/;
 assert.throws(()=>scope.add.todo({id:'B'}),closed);
 assert.throws(()=>todo.add({id:'B'}),closed);
 assert.throws(()=>scope.remove.todo({id:'B'}),closed);
 assert.throws(()=>scope.add([]),closed);
 assert.throws(()=>scope.remove([Todo({id:'B'})]),closed);
 assert.throws(()=>touch.todo({id:'B'}),closed);
 assert.throws(()=>effects.scope('c'),closed);
 // Tagged adds and tag selectors are declarations too.
 assert.throws(()=>scope.add.todo({id:'B'}).tag(['X']),closed);
 assert.throws(()=>scope.add([Todo({id:'B'})]).tag(['X']),closed);
 assert.throws(()=>scope.where({ tags: { all: ['X'] } }).remove(),closed);
 const expected={changes:[{model:'Todo',identity:{id:'A'}}],memberships:[add('c','Todo',{id:'A'})]};
 const settled=effects.settlement();
 assert.deepEqual(settled,expected);
 // The answer is a copy of owned, frozen declarations.
 settled.changes.push({model:'Todo',identity:{id:'forged'}});
 assert.throws(()=>{settled.memberships[0].kind='remove';},TypeError);
 assert.throws(()=>{settled.memberships[0].record.identity.id='forged';},TypeError);
 assert.throws(()=>{settled.memberships[0].tags.push('forged');},TypeError);
 assert.deepEqual(effects.settlement(),expected);
 effects.close();
 assert.deepEqual(effects.settlement(),expected,'closing twice is harmless');
});

test('a Model outside the loaded set is device-only: every declaration naming it fails at the call',()=>{
 const effects=createEffects(models,[],new Set(['Todo','Pin']));
 const refused=caller=>({message:`${caller}: Model Moment has no Loader, so it is device-only and cannot be published`});
 const at=new Date(0);
 assert.throws(()=>effects.touch.moment({at}),refused('touch.moment'));
 assert.throws(()=>effects.scope('c').add.moment({at}),refused('scope.add.moment'));
 assert.throws(()=>effects.scope('c').remove.moment({at}),refused('scope.remove.moment'));
 assert.throws(()=>effects.scope('c').add([Todo({id:'t'}),Moment({at})]),refused('scope.add'));
 assert.throws(()=>effects.scope('c').remove([Moment({at})]),refused('scope.remove'));
 // Loaded Models declare as before; the refused mixed call appended nothing.
 effects.touch.todo({id:'t'});
 effects.scope('c').add.todo({id:'t'});
 assert.deepEqual(effects.settlement(),{changes:[{model:'Todo',identity:{id:'t'}}],memberships:[add('c','Todo',{id:'t'})]});
 // Without a loaded set every Model declares, as a collector always did.
 fresh().touch.moment({at});
});

// Tags label an add; `remove({tag})` selects by label. Both declare in order.
const scalarModel=name=>({name,identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'},nullable:false}]});
const entries=()=>createEffects([scalarModel('Entry')]);

test('an add without tags, with {} or with an empty list adds no label',()=>{
 const effects=fresh();
 const c=effects.scope('c');
 c.add.todo({id:'1'});
 c.add.todo({id:'2'});
 c.add.todo({id:'3'});
 c.add.todo({id:'4'});
 c.add.todo({id:'5'});
 c.add([Todo({id:'6'})]);
 c.add([Todo({id:'7'})]);
 assert.deepEqual(effects.settlement().memberships,['1','2','3','4','5','6','7'].map(id=>add('c','Todo',{id})));
});

test('tags are copied once per declaration, deduplicated in first-seen order and kept as spelled',()=>{
 const effects=fresh();
 const c=effects.scope('c');
 c.add.todo({id:'A'}).tag(['X','Y','X','x',' X','Y']);
 c.add([Todo({id:'B'}),Moment({at:new Date(0)})]).tag(['Journal:1','Journal:1']);
 assert.deepEqual(effects.settlement().memberships,[
  add('c','Todo',{id:'A'}),tagAdd('c','Todo',{id:'A'},['X','Y','x',' X']),
  add('c','Todo',{id:'B'}),add('c','Moment',{at:'1970-01-01T00:00:00.000Z'}),
  tagAdd('c','Todo',{id:'B'},['Journal:1']),tagAdd('c','Moment',{at:'1970-01-01T00:00:00.000Z'},['Journal:1']),
 ]);
});

test('a later change to the caller array or options cannot alter a collected intent',()=>{
 const effects=entries();
 const tags=['X'];
 const options={tags};
 const u=effects.scope('U');
 u.add.entry({id:'a'}).tag(options.tags);
 u.where({ tags: { all: ['X'] } }).remove();
 tags.push('Y');
 tags[0]='Z';
 options.tags=['W'];
 const expected=[add('U','Entry',{id:'a'}),tagAdd('U','Entry',{id:'a'},['X']),removeTag('U','X')];
 const settled=effects.settlement().memberships;
 assert.deepEqual(settled,expected);
 assert.ok(Object.isFrozen(settled[1].tags),'the collected tags are frozen');
 assert.ok(Object.isFrozen(settled[0].record)&&Object.isFrozen(settled[1]),'each intent is frozen');
 assert.throws(()=>{settled[1].tags.push('forged');},TypeError);
 assert.throws(()=>{settled[2].action.kind='forged';},TypeError);
 assert.deepEqual(effects.settlement().memberships,expected);
});

test('declarations keep their order across adds, removals and tag selectors',()=>{
 const effects=fresh();
 const u=effects.scope('U');
 u.add.todo({id:'A'}).tag(['X']);
 u.where({ tags: { all: ['X'] } }).remove();
 u.add.todo({id:'B'}).tag(['X']);
 effects.scope('V').remove([Todo({id:'A'})]);
 u.where({ tags: { all: ['Y'] } }).remove();
 u.remove.todo({id:'B'});
 assert.deepEqual(effects.settlement(),{changes:[],memberships:[
  add('U','Todo',{id:'A'}),tagAdd('U','Todo',{id:'A'},['X']),removeTag('U','X'),add('U','Todo',{id:'B'}),tagAdd('U','Todo',{id:'B'},['X']),
  remove('V','Todo',{id:'A'}),removeTag('U','Y'),remove('U','Todo',{id:'B'}),
 ]});
});

test('an invalid or oversized label refuses the whole declaration',()=>{
 const effects=fresh();
 const c=effects.scope('c');
 // 256 UTF-8 bytes is the bound, however many characters spell it.
 const longest='é'.repeat(128);
 assert.equal(Buffer.byteLength(longest,'utf8'),256);
 c.add.todo({id:'ok'}).tag([longest,'x'.repeat(256),'😀'.repeat(64)]);
 const refused=[
  [[''],/invalid label/],
  [['  '],/invalid label/],
  [['\n\t'],/invalid label/],
  [[1],/invalid label/],
  [[null],/invalid label/],
  [[undefined],/invalid label/],
  [['X',{}],/invalid label/],
  [[longest+'a'],/invalid label/],
  [['x'.repeat(257)],/invalid label/],
  [['a\ud800'],/invalid label/],
 ];
 for(const [tags,pattern] of refused){
  assert.throws(()=>c.tag(tags).add.todo({id:'A'}),pattern,JSON.stringify(tags));
  assert.throws(()=>c.tag(tags).add([Todo({id:'A'})]),pattern,JSON.stringify(tags));
 }
 for(const labels of [null,[],{tags:['X']},{0:'X',length:1}])
  assert.throws(()=>c.tag(labels).add.todo('A'),/labels/,JSON.stringify(labels));
 assert.deepEqual(effects.settlement().memberships,[add('c','Todo',{id:'ok'}),tagAdd('c','Todo',{id:'ok'},[longest,'x'.repeat(256),'😀'.repeat(64)])],'no refused declaration appended an intent');
});

test('an add declares at most 64 distinct labels; repeats do not count',()=>{
 const effects=fresh();
 const c=effects.scope('c');
 const tags=Array.from({length:64},(_,n)=>`t${n}`);
 c.add.todo({id:'A'}).tag([...tags,...tags]);
 assert.throws(()=>c.tag([...tags,'t64']).add.todo({id:'B'}),/1 to 64 distinct labels/);
 assert.throws(()=>c.tag([...tags,'t64']).add([Todo({id:'B'})]),/1 to 64 distinct labels/);
 assert.deepEqual(effects.settlement().memberships,[add('c','Todo',{id:'A'}),tagAdd('c','Todo',{id:'A'},tags)]);
});

test('mixed remove refuses legacy tag selectors and malformed references; selection is explicit',()=>{
 const effects=fresh(), c=effects.scope('c');
 for(const selector of [{},{tag:''},{tag:'X'},{tag:'X',extra:1},{tags:['X']},null,undefined,'X'])
  assert.throws(()=>c.remove(selector),/record reference/);
 assert.throws(()=>c.remove.todo({tag:'X'}),/Todo identity field id is missing/);
 assert.throws(()=>c.add({tag:'X'}),/record reference/);
 assert.deepEqual(effects.settlement().memberships,[]);
 c.where({tags:{all:['X']}}).remove();
 c.where({tags:{all:[' spaced ']}}).remove();
 assert.deepEqual(effects.settlement().memberships,[removeTag('c','X'),removeTag('c',' spaced ')]);
});

// The add-only collector a Load handler declares through: `ctx.scope(name)`.
const limits=JSON.parse(await readFile(new URL('../../../fixtures/protocol/load-enrollment-limits.json',import.meta.url),'utf8'));
const scalar=name=>({kind:'scalar',name});
const ticket={name:'Ticket',identity:['id'],fields:[{name:'id',type:scalar('uuid'),nullable:false}]};
const freshLoad=(loaded)=>createLoadEffects([...models,ticket],[],loaded);

test('a Load Scope handle adds only: no remove or touch exists on any runtime object',()=>{
 const effects=freshLoad();
 const scope=effects.scope('c');
 assert.equal(Object.getPrototypeOf(Object.getPrototypeOf(scope)),null);
 assert.ok(Object.isFrozen(scope));
 assert.deepEqual(Object.keys(scope),['add','tag']);
 assert.deepEqual(Object.keys(scope.add),['todo','moment','pin','ticket']);
 assert.equal('remove' in scope,false);
 assert.equal('touch' in scope,false);
 assert.equal('touch' in effects,false,'the collector has no touch to hand out');
 assert.equal('settlement' in effects,false,'nor a change settlement');
 assert.ok(Object.isFrozen(effects));
 // The facade cannot be widened after the fact.
 assert.throws(()=>{scope.remove=()=>{};},TypeError);
 assert.throws(()=>{scope.add.todo=()=>{};},TypeError);
});

test('Load declarations snapshot typed identities at the call, in first-declaration order',()=>{
 const effects=freshLoad();
 const identity={id:'A',title:'not identity'};
 const at=new Date('2026-01-01T00:00:00.000Z');
 const pin={todo:'t',at:new Date('2026-02-01T00:00:00.000Z'),label:'x'};
 const scope=effects.scope('project:1');
 scope.add.todo(identity);
 scope.add.moment({at});
 scope.add.pin(pin);
 scope.add.ticket({id:'123e4567-e89b-42d3-a456-426614174000'});
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
 assert.throws(()=>{listed[0].record.identity.id='forged';},TypeError);
 assert.equal(effects.memberships().length,4);
});

test('a repeated Scope/record pair is one intent, however it is spelled; another Scope is another pair',()=>{
 const effects=freshLoad();
 const a=effects.scope('a');
 a.add.todo({id:'1'});
 a.add([Todo({id:'1'}),Todo({id:'1'})]);
 effects.scope('a').add.todo({id:'1'});
 effects.scope('b').add.todo({id:'1'});
 // The engine canonicalizes a UUID to lower case and a date-time to UTC
 // milliseconds; the collector deduplicates by the same canonical record.
 a.add.ticket({id:'123E4567-E89B-42D3-A456-426614174000'});
 a.add.ticket({id:'123e4567-e89b-42d3-a456-426614174000'});
 a.add.moment({at:new Date('2026-01-01T00:00:00.000Z')});
 a.add.moment({at:'2026-01-01T05:30:00.000999+05:30'});
 a.add.moment({at:'2026-01-01T00:00:00z'});
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
 const scope=effects.scope('mixed');
 scope.add([Todo({id:'A'}),Moment({at}),Pin({todo:'t',at}),{model:'Todo',identity:{id:'B',title:'extra'}}]);
 scope.add([]);
 assert.deepEqual(effects.memberships(),[
  add('mixed','Todo',{id:'A'}),add('mixed','Moment',{at:'2026-01-01T00:00:00.000Z'}),
  add('mixed','Pin',{todo:'t',at:'2026-01-01T00:00:00.000Z'}),add('mixed','Todo',{id:'B'}),
 ]);
 assert.equal(effects.failure(),undefined);
 assert.throws(()=>scope.add([Todo({id:'C'}),{id:'D'}]),/record reference/);
 assert.equal(effects.memberships().length,4,'the refused list appended nothing');
});

test('a Load identity with a lone surrogate is refused at the declaration; a mixed list appends none of its pairs',()=>{
 const effects=freshLoad();
 const scope=effects.scope('c');
 // A well-formed pair of surrogates is Unicode text and declares as usual.
 scope.add.todo({id:'emoji \ud83d\ude00'});
 assert.throws(()=>scope.add.todo({id:'\ud800'}),{message:'scope: Todo identity field id must be Unicode text, without a lone surrogate'});
 assert.throws(()=>scope.add.pin({todo:'t\udfff',at:new Date(0)}),{message:'scope: Pin identity field todo must be Unicode text, without a lone surrogate'});
 assert.throws(()=>scope.add([Todo({id:'A'}),Todo({id:'B\ud800'}),Todo({id:'C'})]),{message:'scope: Todo identity field id must be Unicode text, without a lone surrogate'});
 assert.deepEqual(effects.memberships(),[add('c','Todo',{id:'emoji \ud83d\ude00'})],'no refused pair, and nothing from the refused list');
 assert.equal(effects.failure().kind,'invalid','the caught refusal leaves the collector failed');
 assert.match(effects.failure().error.message,/scope: Todo identity field id must be Unicode text/,'the first refusal wins');
});

test('every refused Load declaration leaves the collector failed with its first error, even when caught',()=>{
 const cases=[
  ['blank Scope',e=>e.scope('  '),/name/],
  ['non-string Scope',e=>e.scope(7),/name/],
  // A lone surrogate is not Unicode text: the engine could not decode the answer.
  ['lone surrogate Scope',e=>e.scope('a\ud800'),/name/],
  // Nor in a string identity component: the host could not send the answer.
  ['lone surrogate identity',e=>e.scope('c').add.todo({id:'a\udc00'}),/Todo identity field id must be Unicode text/],
  ['lone surrogate identity in a list',e=>e.scope('c').add([Todo({id:'A'}),Pin({todo:'\ud800b',at:new Date(0)})]),/Pin identity field todo must be Unicode text/],
  ['missing identity',e=>e.scope('c').add.todo({}),/Todo identity field id is missing/],
  ['malformed identity',e=>e.scope('c').add.todo({id:1}),/Todo identity field id must be a string/],
  ['bad UUID',e=>e.scope('c').add.ticket({id:'nope'}),/UUID/],
  ['invalid Date',e=>e.scope('c').add.moment({at:new Date('nope')}),/Moment identity field at/],
  ['not a reference',e=>e.scope('c').add({id:'A'}),/record reference/],
  ['raw identity in a list',e=>e.scope('c').add([{id:'A'}]),/record reference/],
  ['unknown Model',e=>e.scope('c').add([{model:'Nope',identity:{id:'x'}}]),/unknown Model Nope/],
 ];
 for(const [label,declare,pattern] of cases){
  const effects=freshLoad();
  effects.scope('ok').add.todo({id:'kept'});
  let thrown;
  try{declare(effects);}catch(error){thrown=error;}
  assert.match(thrown?.message??'',pattern,label);
  // Later valid declarations still record, but cannot clear the failure.
  effects.scope('ok').add.todo({id:'later'});
  const failure=effects.failure();
  assert.equal(failure?.kind,'invalid',label);
  assert.equal(failure.error,thrown,`${label}: the first refused declaration`);
  try{effects.scope('').todo;}catch{}
  assert.equal(effects.failure().error,thrown,`${label}: the first failure wins`);
 }
});

test('a device-only Model is refused at every Load declaration naming it',()=>{
 const effects=freshLoad(new Set(['Todo','Pin']));
 const at=new Date(0);
 const refused=caller=>({message:`${caller}: Model Moment has no Loader, so it is device-only and cannot be published`});
 assert.throws(()=>effects.scope('c').add.moment({at}),refused('scope.add.moment'));
 assert.throws(()=>effects.scope('c').add([Todo({id:'t'}),Moment({at})]),refused('scope.add'));
 assert.equal(effects.failure().kind,'invalid');
 effects.scope('c').add.todo({id:'t'});
 assert.deepEqual(effects.memberships(),[add('c','Todo',{id:'t'})]);
});

test('closing refuses every later Load declaration, including through escaped handles',()=>{
 const effects=freshLoad();
 const scope=effects.scope('c');
 const todo={add: scope.add.todo};
 scope.add.todo({id:'A'});
 effects.close();
 assert.throws(()=>scope.add.todo({id:'B'}),/closed/);
 assert.throws(()=>todo.add({id:'B'}),/closed/);
 assert.throws(()=>scope.add([Todo({id:'B'})]),/closed/);
 assert.throws(()=>effects.scope('c'),/closed/);
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

const pairBytes=scope=>enrollmentBytes(add(scope,'Todo',{id:'t1'}));

test('Load enrollment is bounded by distinct pairs after deduplication; the pair past the bound throws and stays failed',()=>{
 const scopes=Array.from({length:LOAD_ENROLLMENT_PAIRS},(_,n)=>`c${n}`);
 const effects=freshLoad();
 for(const name of [...scopes,...scopes])effects.scope(name).add.todo({id:'t1'});
 assert.equal(effects.memberships().length,LOAD_ENROLLMENT_PAIRS,'exactly the bound, each pair declared twice');
 assert.equal(effects.failure(),undefined);
 let thrown;
 try{effects.scope('one-more').add.todo({id:'t1'});}catch(error){thrown=error;}
 assert.match(thrown?.message??'',/more than 1000 Scope\/record pairs/);
 assert.deepEqual(effects.failure(),{kind:'overflow',error:thrown});
 assert.equal(effects.memberships().length,LOAD_ENROLLMENT_PAIRS,'the crossing pair is not stored');
 // A repeat of a counted pair is still no new pair.
 effects.scope('c0').add.todo({id:'t1'});
 assert.equal(effects.failure().kind,'overflow','the overflow is kept');
});

test('Load enrollment is bounded by encoded bytes after deduplication, one byte over fails',()=>{
 const count=256,each=LOAD_ENROLLMENT_BYTES/count;
 const scopes=Array.from({length:count},(_,n)=>{const name=String(n).padStart(3,'0');return name+'x'.repeat(each-pairBytes(name));});
 assert.equal(scopes.reduce((sum,c)=>sum+pairBytes(c),0),LOAD_ENROLLMENT_BYTES);
 const effects=freshLoad();
 for(const name of scopes)effects.scope(name).add.todo({id:'t1'});
 effects.scope(scopes[0]).add([Todo({id:'t1'})]);
 assert.equal(effects.failure(),undefined,'a repeated pair counts once');
 assert.equal(effects.memberships().length,count);
 const over=freshLoad();
 for(const name of scopes.slice(0,-1))over.scope(name).add.todo({id:'t1'});
 assert.throws(()=>over.scope(scopes.at(-1)+'x').add.todo({id:'t1'}),/more than 1048576 bytes/);
 assert.equal(over.failure().kind,'overflow');
});

test('a mixed list that crosses the bound appends none of its pairs; overflow outranks an earlier invalid declaration',()=>{
 const effects=freshLoad();
 try{effects.scope('c').add.todo({});}catch{}
 assert.equal(effects.failure().kind,'invalid');
 for(let n=0;n<LOAD_ENROLLMENT_PAIRS-1;n++)effects.scope('c').add.todo({id:`t${n}`});
 assert.throws(()=>effects.scope('c').add([Todo({id:'t0'}),Todo({id:'new-1'}),Todo({id:'new-1'}),Todo({id:'new-2'})]),/more than 1000/);
 assert.equal(effects.memberships().length,LOAD_ENROLLMENT_PAIRS-1,'nothing from the crossing list');
 assert.equal(effects.failure().kind,'overflow');
 // Duplicates within a list count once: exactly one new pair fits.
 const fits=freshLoad();
 for(let n=0;n<LOAD_ENROLLMENT_PAIRS-1;n++)fits.scope('c').add.todo({id:`t${n}`});
 fits.scope('c').add([Todo({id:'t0'}),Todo({id:'new-1'}),Todo({id:'new-1'})]);
 assert.equal(fits.memberships().length,LOAD_ENROLLMENT_PAIRS);
 assert.equal(fits.failure(),undefined);
});

test('a Load add takes the same tags: copied, deduplicated, with validated add boundaries preserved',()=>{
 const effects=freshLoad();
 const tags=['X','X','Y'];
 const c=effects.scope('c');
 c.add.todo({id:'1'}).tag(tags);
 c.add([Todo({id:'2'})]);
 c.add([Todo({id:'1'}),Todo({id:'2'})]).tag(['Z','X']);
 c.add.todo({id:'1'});
 tags.push('W');
 assert.deepEqual(effects.memberships(),[add('c','Todo',{id:'1'}),tagAdd('c','Todo',{id:'1'},['X','Y']),add('c','Todo',{id:'2'}),tagAdd('c','Todo',{id:'1'},['Z']),tagAdd('c','Todo',{id:'2'},['Z','X'])]);
 assert.equal(effects.failure(),undefined);
 assert.ok(Object.isFrozen(effects.memberships()[0].tags));
});

test('an invalid Load label or options leave the collector failed and append nothing',()=>{
 for(const [label,declare,pattern] of [
  ['blank tag',e=>e.scope('c').tag([' ']),/invalid label/],
  ['oversized tag',e=>e.scope('c').tag(['x'.repeat(257)]),/invalid label/],
  ['65 tags',e=>e.scope('c').tag(Array.from({length:65},(_,n)=>`t${n}`)),/1 to 64 distinct labels/],
  ['bad labels',e=>e.scope('c').tag({tags:['X']}),/labels/],
 ]){
  const effects=freshLoad();
  let thrown;
  try{declare(effects);}catch(error){thrown=error;}
  assert.match(thrown?.message??'',pattern,label);
  assert.deepEqual(effects.failure(),{kind:'invalid',error:thrown},label);
  assert.deepEqual(effects.memberships(),[],label);
 }
});

test('an escaped Load handle refuses tagged adds after close',()=>{
 const effects=freshLoad();
 const scope=effects.scope('c');
 effects.close();
 assert.throws(()=>scope.add.todo({id:'A'}).tag(['X']),/closed/);
 assert.throws(()=>scope.add([Todo({id:'A'})]).tag(['X']),/closed/);
 assert.equal('remove' in scope,false,'a Load handle has no selector either');
});

test('Load enrollment bytes count tags, including tags merged into a repeated pair',()=>{
 assert.equal(enrollmentBytes(add('c','Todo',{id:'t1'},['X'])),enrollmentBytes(add('c','Todo',{id:'t1'}))+3);
 const count=256,each=LOAD_ENROLLMENT_BYTES/count;
 const scopes=Array.from({length:count},(_,n)=>{const name=String(n).padStart(3,'0');return name+'x'.repeat(each-pairBytes(name));});
 const effects=freshLoad();
 for(const name of scopes)effects.scope(name).add.todo({id:'t1'});
 assert.equal(effects.failure(),undefined,'exactly the bound');
 // A repeated pair adds no pair, but its new label adds bytes past the bound.
 assert.throws(()=>effects.scope(scopes[0]).add.todo({id:'t1'}).tag(['X']),/more than 1048576 bytes/);
 assert.equal(effects.failure().kind,'overflow');
 assert.deepEqual(effects.memberships()[0],add(scopes[0],'Todo',{id:'t1'}),'the crossing merge stored nothing');
});

test('a Load keeps valid 64+1 add boundaries rather than emitting an invalid 65-tag declaration',()=>{
 const effects=freshLoad();
 const tags=Array.from({length:65},(_,i)=>`t${i}`);
 const c=effects.scope('c');
 c.add.todo({id:'1'}).tag(tags.slice(0,64));
 c.add.todo({id:'1'}).tag([tags[0],tags[64]]);
 assert.deepEqual(effects.memberships(),[add('c','Todo',{id:'1'}),tagAdd('c','Todo',{id:'1'},tags.slice(0,64)),tagAdd('c','Todo',{id:'1'},[tags[64]])]);
 assert.equal(effects.failure(),undefined);
});

test('canonical scope declarations preserve invocation order without content changes',()=>{
 const effects=fresh(), scope=effects.scope('U');
 scope.add.todo(['A','B']).tag(['X','Y']);
 scope.tag('X').remove.todo('A');
 scope.where({tags:{only:['X']}}).remove();
 assert.deepEqual(effects.settlement().memberships.map(x=>x.kind),['add','add','tagAdd','tagAdd','tagRemove','select']);
 assert.deepEqual(effects.settlement().changes,[]);
});

test('canonical scope captures lists, identities, labels and nested predicates; late tags stay late',()=>{
 const effects=fresh(), scope=effects.scope('U');
 const ids=[{id:'A'}],tags=['X'],predicate={and:[{tags:{only:['X']}}]};
 const added=scope.add.todo(ids);
 ids[0].id='B';ids.push({id:'C'});
 scope.remove.todo('A');added.tag(tags);tags[0]='Y';
 const selection=scope.where(predicate);predicate.and[0].tags.only[0]='Y';selection.remove();
 assert.deepEqual(effects.settlement().memberships.map(x=>x.kind),['add','remove','tagAdd','select']);
 assert.equal(effects.settlement().memberships[2].record.identity.id,'A');
 assert.deepEqual(effects.settlement().memberships[2].tags,['X']);
 assert.deepEqual(effects.settlement().memberships[3].predicate,{and:[{tags:{only:['X']}}]});
 effects.close();
 for(const call of [()=>added.tag('Z'),()=>selection.remove(),()=>scope.tag('Z'),()=>scope.add.todo('B'),()=>effects.touch(Todo({id:'A'}))])assert.throws(call,/closed/);
});

test('canonical invalid operands and predicates record no prefix',()=>{
 const effects=fresh(),scope=effects.scope('U');
 assert.throws(()=>scope.add([Todo({id:'A'}),{model:'Missing',identity:{id:'B'}}]));
 assert.throws(()=>scope.add.todo(['A',{}]));
 for(const predicate of [{},null,{tags:null},{tags:{only:null}},{tags:{all:[]}},{and:[]},{unknown:true},{not:null},{tags:{only:[],unknown:[]}}])assert.throws(()=>scope.where(predicate));
 for(const label of [[],null,' ',['X',null],['x'.repeat(257)]])assert.throws(()=>scope.tag(label));
 assert.deepEqual(effects.settlement(),empty);
 scope.where({tags:{only:[]}}).remove();
});

test('canonical callable namespaces permit function-property Models',()=>{
 const names=['Tag','Name','Length','Call','Prototype'];
 const effects=createEffects(names.map(name=>({name,identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'}}]})));
 const scope=effects.scope('U');
 for(const name of names){const key=name[0].toLowerCase()+name.slice(1);scope.add[key]('A').tag('X');scope.tag('X').remove[key]('A');scope.where[key]({tags:{only:['X']}}).remove();effects.touch[key]('A')}
 assert.equal(effects.settlement().memberships.length,20);assert.equal(effects.settlement().changes.length,5);
});

test('Load scope only exposes add and explicit label add, preserves ordering and poisons failures',()=>{
 const effects=createLoadEffects(models),scope=effects.scope('U');
 scope.add.todo('A').tag('X');scope.tag('Y').add.todo('A');
 assert.deepEqual(effects.memberships().map(x=>x.kind),['add','tagAdd','tagAdd']);
 assert.equal(scope.remove,undefined);assert.equal(scope.where,undefined);assert.equal(scope.tag('X').remove,undefined);
 assert.throws(()=>scope.add.todo(['B',{}]));assert.equal(effects.failure().kind,'invalid');
 effects.close();assert.throws(()=>scope.tag('Z'),/closed/);
});

test('canonical predicate bounds agree with host limits',()=>{
 const scope=fresh().scope('U'),leaf={tags:{only:[]}};
 let depth=leaf;for(let i=1;i<16;i++)depth={not:depth};scope.where(depth);
 assert.throws(()=>scope.where({not:depth}));
 scope.where({and:Array.from({length:127},()=>leaf)});
 assert.throws(()=>scope.where({and:Array.from({length:128},()=>leaf)}));
 scope.where({tags:{all:Array.from({length:64},(_,i)=>String(i))}});
 assert.throws(()=>scope.where({tags:{all:Array.from({length:65},(_,i)=>String(i))}}));
 assert.throws(()=>scope.where({tags:{all:Array(300).fill('X'.repeat(256))}}));
});

test('only argument-free tag remove detaches and Load label failures poison',()=>{
 const effects=fresh(),tag=effects.scope('U').tag('X');
 assert.throws(()=>tag.remove(undefined));assert.deepEqual(effects.settlement(),empty);
 assert.throws(()=>tag.remove([{model:'Todo',identity:{id:'A'}},undefined]));assert.deepEqual(effects.settlement(),empty);
 tag.remove();assert.deepEqual(effects.settlement().memberships,[{kind:'detachTags',scope:'U',tags:['X']}]);
 for(const call of [scope=>scope.tag([]),scope=>scope.tag('X').add(undefined),scope=>scope.add.todo('A').tag([])]){
  const load=createLoadEffects(models);assert.throws(()=>call(load.scope('U')));assert.equal(load.failure().kind,'invalid');
 }
 const load=createLoadEffects(models);assert.throws(()=>load.scope('\ud800'));assert.equal(load.failure().kind,'invalid');
});

test('canonical typed sparse operands reject before any effect',()=>{
 for(const [name,call] of [
  ['add',(effects,scope,ids)=>scope.add.todo(ids)],
  ['remove',(effects,scope,ids)=>scope.remove.todo(ids)],
  ['tag add',(effects,scope,ids)=>scope.tag('X').add.todo(ids)],
  ['tag remove',(effects,scope,ids)=>scope.tag('X').remove.todo(ids)],
  ['touch',(effects,scope,ids)=>effects.touch.todo(ids)],
 ]){
  for(const ids of [['A',,'B'],[{id:'A'},,{id:'B'}]]){
   const effects=fresh();
   assert.throws(()=>call(effects,effects.scope('U'),ids),undefined,name);
   assert.deepEqual(effects.settlement(),empty,name);
  }
 }
});

test('Load typed sparse operands poison before enrollment or label effects',()=>{
 for(const call of [
  (scope,ids)=>scope.add.todo(ids),
  (scope,ids)=>scope.tag('X').add.todo(ids),
 ]){
  const effects=freshLoad(),scope=effects.scope('U');
  let thrown;
  try{call(scope,['A',,'B']);}catch(error){thrown=error;}
  assert.ok(thrown);
  assert.deepEqual(effects.failure(),{kind:'invalid',error:thrown});
  assert.deepEqual(effects.memberships(),[]);
 }
});

test('canonical sparse predicate groups reject before selection effects',()=>{
 for(const group of ['and','or']){
  for(const children of [Array(1),[{tags:{only:[]}},,{tags:{all:['X']}}]]){
   for(const typed of [false,true]){
    const effects=fresh(),scope=effects.scope('U');
    const where=typed?scope.where.todo:scope.where;
    assert.throws(()=>where({[group]:children}).remove());
    assert.deepEqual(effects.settlement(),empty);
   }
  }
 }
});

test('canonical empty operand arrays retain no-effect semantics',()=>{
 const effects=fresh(),scope=effects.scope('U');
 scope.add.todo([]).tag('X');scope.remove.todo([]);
 scope.tag('X').add.todo([]);scope.tag('X').remove.todo([]);effects.touch.todo([]);
 assert.deepEqual(effects.settlement(),empty);
 const load=freshLoad();
 load.scope('U').add.todo([]).tag('X');load.scope('U').tag('X').add.todo([]);
 assert.deepEqual(load.memberships(),[]);assert.equal(load.failure(),undefined);
});

test('collectors expose only canonical Scope contexts',()=>{
 assert.equal(fresh().channel,undefined);
 assert.equal(createLoadEffects(models).channel,undefined);
 assert.equal(fresh().scope('U').todo,undefined);
});
