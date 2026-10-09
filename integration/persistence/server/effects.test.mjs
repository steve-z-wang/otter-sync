import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createEffects,createLoadEffects,bootstrapEffectsFor,enrollmentBytes,LOAD_ENROLLMENT_PAIRS,LOAD_ENROLLMENT_BYTES} from '../../../packages/backend/server/bindings/effects.mts';
const {schema}=JSON.parse(await readFile(new URL('../../action-runtime-ts/backend.json',import.meta.url),'utf8'));
const models=schema.models;
const fresh=()=>createEffects(models);
const ref=(model,identity)=>({model,identity});
const track=(stream,model,identity)=>({kind:'track',stream,record:ref(model,identity)});
const invalid=(streams,model,identity)=>({kind:'invalidate',streams,record:ref(model,identity)});
const empty={changes:[],declarations:[]};
test('Bootstrap retains track-only validation, atomic declarations, sticky failures and lifetime checks',()=>{
 const e=bootstrapEffectsFor(models,[],new Set(['Todo']))(),h=e.stream('S');
 assert.equal(h.invalidate,undefined);assert.equal(e.invalidate,undefined);
 h.track.todo(['kept','kept']);assert.deepEqual(e.tracking(),[track('S','Todo',{id:'kept'})]);
 let caught;try{h.track.todo(['prefix',{}]);}catch(error){caught=error;}
 assert.deepEqual(e.failure(),{kind:'invalid',error:caught});
 assert.deepEqual(e.tracking(),[track('S','Todo',{id:'kept'})]);
 assert.throws(()=>h.track.moment(new Date(0)),/no Loader/);
 assert.throws(()=>h.track.todo('\ud800'),/Unicode/);
 e.close();assert.throws(()=>h.track.todo([]),/closed/);
});
test('Cartesian tracks and selected invalidations capture names and records once',()=>{
 const effects=fresh(),names=['A','B','A'],ids=[{id:'t'},{id:'u'}];
 const handle=effects.stream(names);handle.track.todo(ids);handle.invalidate.todo(['t','t']);
 names[0]='changed';ids[0].id='changed';
 assert.deepEqual(effects.settlement(),{changes:[],declarations:[track('A','Todo',{id:'t'}),track('A','Todo',{id:'u'}),track('B','Todo',{id:'t'}),track('B','Todo',{id:'u'}),invalid(['A','B'],'Todo',{id:'t'}),invalid(['A','B'],'Todo',{id:'t'})]});
 effects.close();assert.throws(()=>handle.track.todo('late'),/closed/);
});
test('global invalidation and mixed references snapshot complete Date identities',()=>{
 const e=fresh(),at=new Date(0);e.invalidate([ref('Todo',{id:'t',extra:1}),ref('Moment',{at})]);e.stream('S').track.pin({todo:'t',at});at.setFullYear(2030);
 assert.deepEqual(e.settlement().declarations,[invalid(null,'Todo',{id:'t'}),invalid(null,'Moment',{at:'1970-01-01T00:00:00.000Z'}),track('S','Pin',{todo:'t',at:'1970-01-01T00:00:00.000Z'})]);
 assert.throws(()=>e.invalidate.pin({todo:'t'}),/at/);
});
test('ordinary invalid calls append no prefix and preserve prior declarations',()=>{
 const e=fresh();e.stream('S').track.todo('kept');const before=e.settlement();
 for(const call of [()=>e.invalidate([ref('Todo',{id:'prefix'}),ref('Todo',{})]),()=>e.stream('S').track.todo(['prefix',{}]),()=>e.stream(['S',' ']),()=>e.stream('S').invalidate.todo(['a',,'b'])])assert.throws(call);
 assert.deepEqual(e.settlement(),before);e.invalidate.todo('later');assert.equal(e.settlement().declarations.length,2);
});
test('empty arrays are no-ops and escaped empty handles still check lifetime',()=>{
 const e=fresh(),h=e.stream([]);h.track.todo('t');h.invalidate.todo('t');e.invalidate([]);e.stream('S').track.todo([]);assert.deepEqual(e.settlement(),empty);
 e.close();for(const call of [()=>h.track.todo([]),()=>h.invalidate([]),()=>e.invalidate([]),()=>e.stream([])])assert.throws(call,/closed/);
});
test('Loader-less declarations are refused before append',()=>{
 const e=createEffects(models,[],new Set(['Todo']));assert.throws(()=>e.stream('S').track([ref('Todo',{id:'t'}),ref('Moment',{at:new Date(0)})]),/no Loader/);assert.throws(()=>e.invalidate.moment(new Date(0)),/no Loader/);assert.deepEqual(e.settlement(),empty);
});
test('names are nonblank opaque case-sensitive strings and sparse names fail',()=>{
 const e=fresh();for(const names of ['', ' ',null,1,['S',null],['S',,'T']])assert.throws(()=>e.stream(names));e.stream(['S','s',' S']).track.todo('t');assert.deepEqual(e.settlement().declarations.map(d=>d.stream),['S','s',' S']);
});
test('callable namespaces safely expose function and prototype property Models',()=>{
 const names=['__proto__','Constructor','Name','Length','Call','Prototype'];const e=createEffects(names.map(name=>({name,identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'string'}}]})));const h=e.stream('S');
 for(const name of names){const key=name[0].toLowerCase()+name.slice(1);h.track[key]('t');h.invalidate[key]('t');e.invalidate[key]('t');}
 assert.equal(Object.getPrototypeOf(h.track),null);assert.equal(e.settlement().declarations.length,18);
});
test('Load exposes only tracking and failed declarations remain sticky',()=>{
 const e=createLoadEffects(models),h=e.stream('S');assert.equal(h.invalidate,undefined);assert.equal(e.invalidate,undefined);h.track.todo('kept');let caught;try{h.track.todo(['prefix',{}]);}catch(error){caught=error;}h.track.todo('later');assert.deepEqual(e.failure(),{kind:'invalid',error:caught});assert.deepEqual(e.tracking(),[track('S','Todo',{id:'kept'}),track('S','Todo',{id:'later'})]);e.close();assert.throws(()=>h.track.todo([]),/closed/);
});
test('Load bounds count expanded distinct pairs atomically and overflow outranks invalid',()=>{
 const e=createLoadEffects(models);try{e.stream('').track.todo('t');}catch{}const ids=Array.from({length:501},(_,i)=>`t${i}`);assert.throws(()=>e.stream(['A','B']).track.todo(ids),/1000/);assert.equal(e.failure().kind,'overflow');assert.deepEqual(e.tracking(),[]);
 const fits=createLoadEffects(models);fits.stream(['A','B','A']).track.todo([...ids.slice(0,500),...ids.slice(0,500)]);assert.equal(fits.tracking().length,LOAD_ENROLLMENT_PAIRS);assert.equal(fits.failure(),undefined);
});
test('Load canonicalizes UUID and Date identities for pair deduplication',()=>{
 const ticket={name:'Ticket',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'uuid'}}]};const e=createLoadEffects([...models,ticket]);e.stream('S').track.ticket(['123E4567-E89B-42D3-A456-426614174000','123e4567-e89b-42d3-a456-426614174000']);e.stream('S').track.moment(['2026-01-01T01:00:00+01:00',new Date('2026-01-01T00:00:00Z')]);assert.equal(e.tracking().length,2);assert.throws(()=>e.stream('S').track.ticket('nope'),/UUID/);
});
test('Load byte bounds use encoded TrackIntent and refuse the crossing batch',()=>{
 const base=track('S','Todo',{id:'t'});assert.equal(enrollmentBytes(base),Buffer.byteLength(JSON.stringify(base)));const size=LOAD_ENROLLMENT_BYTES/256;const names=Array.from({length:256},(_,i)=>{const prefix=String(i).padStart(3,'0');return prefix+'x'.repeat(size-enrollmentBytes(track(prefix,'Todo',{id:'t'})));});
 const e=createLoadEffects(models);e.stream(names).track.todo('t');assert.equal(e.tracking().length,256);assert.equal(e.failure(),undefined);e.stream(names).track.todo('t');assert.throws(()=>e.stream('extra').track.todo('t'),/1048576/);assert.equal(e.tracking().length,256);
 const over=createLoadEffects(models);assert.throws(()=>over.stream([...names.slice(0,-1),names.at(-1)+'x']).track.todo('t'),/1048576/);assert.deepEqual(over.tracking(),[]);
});
test('owned declaration snapshots are frozen and settlement arrays are copies',()=>{
 const e=fresh();e.stream(['S']).invalidate.todo('t');const out=e.settlement();assert.throws(()=>out.declarations[0].streams.push('forged'));assert.throws(()=>{out.declarations[0].record.identity.id='forged';});out.declarations.length=0;assert.equal(e.settlement().declarations.length,1);
});
test('ordinary declarations canonicalize UUID and zoned Date operands before capture',()=>{
 const ticket={name:'Ticket',identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:'uuid'}}]};const e=createEffects([...models,ticket]);e.invalidate.ticket('123E4567-E89B-42D3-A456-426614174000');e.stream('S').track.moment('2026-01-01T01:00:00+01:00');assert.deepEqual(e.settlement().declarations,[invalid(null,'Ticket',{id:'123e4567-e89b-42d3-a456-426614174000'}),track('S','Moment',{at:'2026-01-01T00:00:00.000Z'})]);
});
test('scalar and enum identity validation never appends malformed records',()=>{
 const scalar=(name,type)=>({name,identity:['id'],fields:[{name:'id',type:{kind:'scalar',name:type}}]});const e=createEffects([scalar('Count','int'),scalar('Value','float'),scalar('Flag','boolean'),{name:'Status',identity:['id'],fields:[{name:'id',type:{kind:'enum',name:'State'}}]}],[{name:'State',values:['open','closed']}]);
 for(const [accessor,values] of [['count',[1.5,NaN,Infinity,Number.MAX_SAFE_INTEGER+1]],['value',[NaN,Infinity,'1']],['flag',[0,'true']],['status',['OPEN','other']]])for(const value of values)assert.throws(()=>e.invalidate[accessor](value));assert.deepEqual(e.settlement(),empty);
 e.invalidate.count(1);e.invalidate.value(1.5);e.invalidate.flag(false);e.invalidate.status('open');assert.equal(e.settlement().declarations.length,4);
});
test('invalid calendar and incomplete composite operands are refused before any prefix',()=>{
 const e=fresh();for(const at of [new Date('nope'),'2026-02-29T00:00:00Z','2026-01-01T24:00:00Z','2026-01-01T00:00:00','2026-01-01T00:00:00+24:00'])assert.throws(()=>e.stream('S').track.moment([{at:new Date(0)},{at}]));assert.throws(()=>e.invalidate.pin([{todo:'t',at:new Date(0)},{todo:'t'}]));assert.deepEqual(e.settlement(),empty);
});
