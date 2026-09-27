import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createEffects} from '../../../packages/server/effects.mts';
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
