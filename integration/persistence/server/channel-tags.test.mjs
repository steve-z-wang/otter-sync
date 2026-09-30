// Channel tags and synchronized removal against real PostgreSQL: the
// eight-table schema and its association trigger, the Channel operations as
// settlement drives them, rollback, concurrency with real barriers, the
// forward upgrade from the six-table schema and a 10,000-member removal.
// Every assertion reads the committed tables through a separate pool.
import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {Pool,Client} from 'pg';
import {createBackend} from '../../../packages/server/index.mts';
import {pg,answer,persistence} from '../../../packages/postgres/index.mts';
import * as SQL from '../../../packages/postgres/src/sql.mts';
import {sqlStatements} from '../../../packages/postgres/src/statements.mts';
const require=createRequire(import.meta.url);
const native=require('../../../bindings/node/axton-node.node');
const url=process.env.DATABASE_URL;
const check=new Pool({connectionString:url});
const q=async(sql,params=[])=>(await check.query(sql,params)).rows;
const pool=new Pool({connectionString:url});
const database=pg(pool);
const {driver}=database;
const MAX=9007199254740991;
const source=path=>readFile(new URL(`../../../packages/postgres/${path}`,import.meta.url),'utf8');
const string=name=>({name,type:{kind:'scalar',name:'string'},nullable:false});
const fields=[string('id'),string('title')];
const config={schema:{enums:[],
 models:[{name:'Todo',version:1,identity:['id'],fields}],
 resultModels:[{name:'Todo',version:1,identity:['id'],fields,enums:[]}],
 actions:[{name:'Mark',version:1,inputs:[{kind:'model',name:'todo',model:'Todo',operation:'update',cardinality:'single',allowedPatchFields:['title']}],outputs:[]}]},
 mutations:[],loaders:['Todo']};
let loaderCalls=0;
const loader=async({tx,ids})=>{
 loaderCalls++;
 const rows=await driver.query(tx,'SELECT id,title FROM tag_todo WHERE id = ANY($1)',[ids.map(i=>i.id)]);
 return ids.map(({id})=>{const row=rows.find(r=>r.id===id);return row?{title:row.title}:null;});
};
const make=db=>createBackend({config,native,database:db,authenticate:()=>'alice',onError:()=>{},
 mutations:{async mark(){}},loaders:{todo:loader}});
const backend=make(database);
/** A backend whose every statement passes `seen(sql)` first; a throw there fails the statement. */
const watched=seen=>make(persistence({transaction:body=>driver.transaction(body),query:(tx,sql,params)=>{seen(sql);return driver.query(tx,sql,params);}}));
const key=id=>JSON.stringify({id});
const write=(tx,id,title)=>driver.query(tx,'INSERT INTO tag_todo(id,title) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET title=$2',[id,title]);
const add=(channel,id,tags)=>backend.transaction(({channel:c})=>{c(channel).todo.add({id},tags?{tags}:undefined);});
const removeTag=(channel,tag)=>backend.transaction(({channel:c})=>{c(channel).remove({tag});});
const remove=(channel,id)=>backend.transaction(({channel:c})=>{c(channel).todo.remove({id});});
const touch=(id,title)=>backend.transaction(async({tx,touch})=>{await write(tx,id,title);touch.todo({id});});
/** The adapter itself, in one transaction: `call` answers a host request. */
const adapter=body=>driver.transaction(tx=>body(request=>answer(driver,tx,request),(sql,params=[])=>driver.query(tx,sql,params)));
const delta=(channel,id,{present=true,tags=[],publish=true}={})=>({channel,model:'Todo',identity:{id},identityKey:key(id),present,tags,publish});

/** The live members of `channel` with their sorted tags, by identity. */
const members=async channel=>(await q(
 `SELECT r.identity_key AS key,COALESCE(array_agg(t.name ORDER BY t.name) FILTER (WHERE t.name IS NOT NULL),'{}') AS tags
  FROM axton_channel_member m JOIN axton_record r ON r.id=m.record_id
  LEFT JOIN axton_channel_member_tag mt ON mt.member_id=m.id LEFT JOIN axton_channel_tag t ON t.id=mt.tag_id
  WHERE m.channel=$1 GROUP BY r.identity_key ORDER BY r.identity_key`,[channel])).map(r=>[JSON.parse(r.key).id,r.tags]);
/** The compacted log of `channel` in cursor order. */
const log=async channel=>(await q('SELECT r.identity_key AS key,l.cursor::text AS cursor,l.kind FROM axton_channel_log l JOIN axton_record r ON r.id=l.record_id WHERE l.channel=$1 ORDER BY l.cursor',[channel])).map(r=>[JSON.parse(r.key).id,Number(r.cursor),r.kind]);
const head=async channel=>Number((await q('SELECT head FROM axton_channel WHERE channel=$1',[channel]))[0]?.head??0);
const tagNames=async channel=>(await q('SELECT name FROM axton_channel_tag WHERE channel=$1 ORDER BY name',[channel])).map(r=>r.name);
const state=async channel=>({members:await members(channel),log:await log(channel),head:await head(channel),tags:await tagNames(channel)});
const stamps=async ids=>(await q("SELECT identity_key,stamp::text FROM axton_record WHERE model='Todo' AND identity_key = ANY($1) ORDER BY identity_key",[ids.map(key)])).map(r=>({identity_key:r.identity_key,stamp:Number(r.stamp)}));
const seed=(id,title='v1')=>q('INSERT INTO tag_todo(id,title) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET title=$2',[id,title]);

before(async()=>{
 // One simple-protocol call: the file holds dollar-quoted trigger functions.
 await q(await source('migration.sql'));
 await q('CREATE TABLE IF NOT EXISTS tag_todo(id text PRIMARY KEY,title text NOT NULL)');
});
after(async()=>{await pool.end();await check.end();});

// ---- Pairs, tags and removal ----------------------------------------------

test('a pair is one member whose adds union their tags; a tag-only change publishes nothing; same-named tags of two Channels are distinct rows',async()=>{
 await add('u-C','u-a',['X']);
 await add('u-C','u-a',['Y']);
 await backend.transaction(({channel})=>{channel('u-C').todo.add({id:'u-a'},{tags:['X']});channel('u-C').todo.add({id:'u-a'});});
 assert.deepEqual(await state('u-C'),{members:[['u-a',['X','Y']]],log:[['u-a',1,'upsert']],head:1,tags:['X','Y']});
 assert.deepEqual(await q('SELECT count(*)::int AS n FROM axton_channel_member WHERE channel=$1',['u-C']),[{n:1}],'membership is unique per pair');
 await add('u-D','u-a',['X']);
 const ids=await q("SELECT channel,id::text FROM axton_channel_tag WHERE channel IN ('u-C','u-D') AND name='X' ORDER BY channel");
 assert.equal(ids.length,2);assert.notEqual(ids[0].id,ids[1].id,'a tag belongs to its Channel');
 await removeTag('u-D','X');
 assert.deepEqual(await state('u-C'),{members:[['u-a',['X','Y']]],log:[['u-a',1,'upsert']],head:1,tags:['X','Y']},'removing X in D leaves C untouched');
 assert.deepEqual(await state('u-D'),{members:[],log:[['u-a',2,'remove']],head:2,tags:[]});
});

test('removing X from A (X, Y) and B (X) removes both whole memberships: no member, no association, two remove rows and no Loader call',async()=>{
 const channel='x-bob';
 for(const id of ['x-A','x-B'])await seed(id);
 await backend.transaction(({channel:c})=>{c(channel).todo.add({id:'x-A'},{tags:['X','Y']});c(channel).todo.add({id:'x-B'},{tags:['X']});});
 const before=await stamps(['x-A','x-B']);
 loaderCalls=0;
 await removeTag(channel,'X');
 assert.equal(loaderCalls,0,'a removal loads nothing');
 assert.deepEqual(await q('SELECT count(*)::int AS n FROM axton_channel_member WHERE channel=$1',[channel]),[{n:0}]);
 assert.deepEqual(await q('SELECT count(*)::int AS n FROM axton_channel_member_tag mt JOIN axton_channel_tag t ON t.id=mt.tag_id WHERE t.channel=$1',[channel]),[{n:0}]);
 assert.deepEqual(await q('SELECT kind,count(*)::int AS n FROM axton_channel_log WHERE channel=$1 GROUP BY kind',[channel]),[{kind:'remove',n:2}]);
 assert.deepEqual(await log(channel),[['x-A',3,'remove'],['x-B',4,'remove']],'positions in record-key order above the head');
 assert.equal(await head(channel),4);
 assert.deepEqual(await tagNames(channel),[],'unused tags are collected');
 assert.deepEqual(await stamps(['x-A','x-B']),before,'record metadata and stamps remain');
 assert.deepEqual(await q('SELECT count(*)::int AS n FROM tag_todo WHERE id IN ($1,$2)',['x-A','x-B']),[{n:2}],'business rows remain');
});

test('the specification example: removing X keeps C (Y) at its old position; A goes despite its Y label',async()=>{
 const channel='User:bob';
 await backend.transaction(({channel:c})=>{
  c(channel).todo.add({id:'ex-A'},{tags:['X','Y']});c(channel).todo.add({id:'ex-B'},{tags:['X']});c(channel).todo.add({id:'ex-C'},{tags:['Y']});
 });
 await removeTag(channel,'X');
 assert.deepEqual(await state(channel),{members:[['ex-C',['Y']]],log:[['ex-C',3,'upsert'],['ex-A',4,'remove'],['ex-B',5,'remove']],head:5,tags:['Y']});
});

test('removing an absent record or an unmatched tag, twice, allocates no cursor, creates no Channel and calls no Loader',async()=>{
 await add('idem','idem-a',['X']);
 loaderCalls=0;
 for(let n=0;n<2;n++){
  await remove('idem','idem-nobody');
  await removeTag('idem','nothing');
  await remove('idem-none','idem-a');
  await removeTag('idem-none','X');
 }
 assert.deepEqual(await state('idem'),{members:[['idem-a',['X']]],log:[['idem-a',1,'upsert']],head:1,tags:['X']});
 assert.deepEqual(await q('SELECT channel FROM axton_channel WHERE channel=$1',['idem-none']),[]);
 assert.equal(loaderCalls,0);
 await remove('idem','idem-a');
 await remove('idem','idem-a');
 assert.deepEqual(await state('idem'),{members:[],log:[['idem-a',2,'remove']],head:2,tags:[]},'a second removal of the same record is a no-op');
});

test('a whole-member removal discards every tag; a later add starts with only its own tags; the log keeps one compacted row per pair',async()=>{
 await add('whole','w-a',['X','Y']);
 await remove('whole','w-a');
 assert.deepEqual(await state('whole'),{members:[],log:[['w-a',2,'remove']],head:2,tags:[]});
 await add('whole','w-a',['Z']);
 assert.deepEqual(await state('whole'),{members:[['w-a',['Z']]],log:[['w-a',3,'upsert']],head:3,tags:['Z']},'old tags do not revive');
 await remove('whole','w-a');
 assert.deepEqual(await log('whole'),[['w-a',4,'remove']],'one row per pair: the latest position replaced the others');
 assert.deepEqual(await q("SELECT count(*)::int AS n FROM axton_channel_log l JOIN axton_record r ON r.id=l.record_id WHERE l.channel='whole' AND r.identity_key=$1",[key('w-a')]),[{n:1}]);
});

test('declarations reduce in order inside one transaction: a selector sees earlier adds, a removed and re-added member publishes one upsert',async()=>{
 await add('ord','ord-E',['X','Y']);
 await backend.transaction(({channel})=>{
  const c=channel('ord');
  c.todo.add({id:'ord-A'},{tags:['X']});
  c.remove({tag:'X'});
  c.todo.add({id:'ord-B'},{tags:['X']});
  c.todo.add({id:'ord-E'},{tags:['Z']});
 });
 assert.deepEqual(await state('ord'),{members:[['ord-B',['X']],['ord-E',['Z']]],log:[['ord-B',2,'upsert'],['ord-E',3,'upsert']],head:3,tags:['X','Z']},
  'A was never a lasting member and got no position; E was released and re-added with only Z');
});

test('the reverse lookup answers every Channel of a record, and a touch reaches exactly those, keeping their tags',async()=>{
 await seed('rev-a');
 await backend.transaction(({channel})=>{channel('rev-1').todo.add({id:'rev-a'},{tags:['X']});channel('rev-2').todo.add({id:'rev-a'});});
 assert.deepEqual(await adapter(call=>call({op:'memberships',model:'Todo',identityKey:key('rev-a')})),['rev-1','rev-2']);
 await touch('rev-a','v2');
 assert.deepEqual([await log('rev-1'),await log('rev-2')],[[['rev-a',2,'upsert']],[['rev-a',2,'upsert']]]);
 assert.deepEqual(await members('rev-1'),[['rev-a',['X']]],'a touch keeps the tags');
 await removeTag('rev-1','X');
 assert.deepEqual(await adapter(call=>call({op:'memberships',model:'Todo',identityKey:key('rev-a')})),['rev-2']);
 await touch('rev-a','v3');
 assert.deepEqual([await head('rev-1'),await head('rev-2')],[3,3],'the removed Channel hears nothing more');
 assert.ok((await q("SELECT indexdef FROM pg_indexes WHERE indexname='axton_channel_member_record'"))[0].indexdef.includes('(record_id, channel)'),'the lookup has its index');
});

test('a rolled-back transaction leaves every member, tag, association, log row and head as it was',async()=>{
 await add('rb','rb-a',['X']);
 const before=await state('rb');
 await assert.rejects(()=>backend.transaction(async({tx,channel})=>{await write(tx,'rb-b','never');channel('rb').todo.add({id:'rb-b'},{tags:['X','N']});throw new Error('cancel add');}),/cancel add/);
 await assert.rejects(()=>backend.transaction(async({channel})=>{channel('rb').remove({tag:'X'});channel('rb-new').todo.add({id:'rb-a'});throw new Error('cancel removal');}),/cancel removal/);
 assert.deepEqual(await state('rb'),before);
 assert.deepEqual(await q("SELECT channel FROM axton_channel WHERE channel='rb-new'"),[]);
 assert.deepEqual(await q("SELECT id FROM tag_todo WHERE id='rb-b'"),[]);
 assert.deepEqual(await stamps(['rb-b']),[],'the rolled-back add initialised no stamp');
});

// ---- Schema guarantees, by direct SQL ---------------------------------------

/** The SQLSTATE of a refused statement, whichever layer wrapped it. */
const refused=async(sql,params=[])=>{try{await q(sql,params);}catch(error){return error.code;}return 'accepted';};

test('the association trigger refuses a tag of another Channel on insert and update; Channel ownership of members and tags is immutable',async()=>{
 await q("INSERT INTO axton_channel(channel,head) VALUES('own-P',0),('own-Q',0)");
 await q("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Todo',$1,1)",[key('own-a')]);
 const [{id:record}]=await q("SELECT id::text FROM axton_record WHERE model='Todo' AND identity_key=$1",[key('own-a')]);
 const [{id:member}]=await q("INSERT INTO axton_channel_member(channel,record_id) VALUES('own-P',$1) RETURNING id::text",[record]);
 const [{id:tagP}]=await q("INSERT INTO axton_channel_tag(channel,name) VALUES('own-P','X') RETURNING id::text");
 const [{id:tagQ}]=await q("INSERT INTO axton_channel_tag(channel,name) VALUES('own-Q','X') RETURNING id::text");
 assert.equal(await refused('INSERT INTO axton_channel_member_tag(member_id,tag_id) VALUES($1,$2)',[member,tagQ]),'23514','a member cannot carry a tag of another Channel');
 assert.equal(await refused('INSERT INTO axton_channel_member_tag(member_id,tag_id) VALUES($1,$2)',[member,tagP]),'accepted');
 assert.equal(await refused('UPDATE axton_channel_member_tag SET tag_id=$2 WHERE member_id=$1',[member,tagQ]),'23514','nor be moved onto one');
 assert.equal(await refused("UPDATE axton_channel_member SET channel='own-Q' WHERE id=$1",[member]),'23514','a member never changes Channel');
 assert.equal(await refused("UPDATE axton_channel_tag SET channel='own-Q', name='moved' WHERE id=$1",[tagP]),'23514','a tag never changes Channel');
 assert.equal(await refused("INSERT INTO axton_channel_member(channel,record_id) VALUES('own-P',$1)",[record]),'23505','one member per pair');
 assert.equal(await refused("INSERT INTO axton_channel_tag(channel,name) VALUES('own-P','X')"),'23505','one tag per name in a Channel');
 assert.equal(await refused("INSERT INTO axton_channel_log(channel,record_id,cursor,kind) VALUES('own-P',$1,1,'touch')",[record]),'23514','kind is upsert or remove');
 assert.equal(await refused("INSERT INTO axton_channel_log(channel,record_id,cursor,kind) VALUES('own-P',$1,$2,'upsert')",[record,String(MAX+1)]),'23514','cursors stay safe integers');
 await q("INSERT INTO axton_channel_log(channel,record_id,cursor,kind) VALUES('own-P',$1,1,'upsert')",[record]);
 assert.equal(await refused('DELETE FROM axton_record WHERE id=$1',[record]),'23503','a record referenced by a member or log row stays');
 await q('DELETE FROM axton_channel_member WHERE id=$1',[member]);
 assert.deepEqual(await q('SELECT * FROM axton_channel_member_tag WHERE member_id=$1',[member]),[],'deleting a member cascades to its associations');
 assert.deepEqual(await q("SELECT name FROM axton_channel_tag WHERE channel='own-P'"),[{name:'X'}],'but keeps the tag row for collection by the adapter');
 assert.equal(await refused('DELETE FROM axton_record WHERE id=$1',[record]),'23503','the log row still holds the record');
});

test('applyChannelMembers reserves one range per Channel, keeps unpublished deltas cursor-neutral, and refuses head overflow before commit',async()=>{
 for(const id of ['rng-a','rng-b','rng-c'])await q("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Todo',$1,1)",[key(id)]);
 const first=await adapter(call=>call({op:'applyChannelMembers',deltas:[delta('rng-1','rng-a',{tags:['X']}),delta('rng-1','rng-b'),delta('rng-2','rng-c')]}));
 assert.deepEqual(first.map(p=>[p.channel,JSON.parse(p.identityKey).id,p.cursor,p.kind]),[['rng-1','rng-a',1,'upsert'],['rng-1','rng-b',2,'upsert'],['rng-2','rng-c',1,'upsert']]);
 const kept=await adapter(call=>call({op:'applyChannelMembers',deltas:[delta('rng-1','rng-a',{tags:['Y'],publish:false}),delta('rng-1','rng-b',{present:false,tags:[]})]}));
 assert.deepEqual(kept.map(p=>[p.cursor,p.kind]),[[1,'upsert'],[3,'remove']],'the kept delta answers its existing position');
 assert.deepEqual(await state('rng-1'),{members:[['rng-a',['Y']]],log:[['rng-a',1,'upsert'],['rng-b',3,'remove']],head:3,tags:['Y']},'set with exactly its tags; X collected');
 const cursorNeutral=await adapter(call=>call({op:'applyChannelMembers',deltas:[delta('rng-1','rng-a',{tags:['Y','Z'],publish:false})]}));
 assert.deepEqual(cursorNeutral.map(p=>p.cursor),[1]);assert.equal(await head('rng-1'),3,'a metadata-only change moves no head');
 // Overflow: two positions do not fit below the bound; one does.
 await q("INSERT INTO axton_channel(channel,head) VALUES('rng-max',$1)",[String(MAX-1)]);
 await assert.rejects(()=>adapter(call=>call({op:'applyChannelMembers',deltas:[delta('rng-max','rng-a'),delta('rng-max','rng-b')]})),/overflow|9007199254740991/);
 assert.deepEqual(await state('rng-max'),{members:[],log:[],head:MAX-1,tags:[]},'nothing of the refused call committed');
 const last=await adapter(call=>call({op:'applyChannelMembers',deltas:[delta('rng-max','rng-a')]}));
 assert.deepEqual(last.map(p=>p.cursor),[MAX],'the last safe cursor is usable');
 await q("UPDATE axton_record SET stamp=$2 WHERE model='Todo' AND identity_key=$1",[key('rng-c'),String(MAX)]);
 await assert.rejects(()=>adapter(call=>call({op:'advanceStamp',model:'Todo',identityKey:key('rng-c')})),error=>error.code==='23514','a content stamp never passes the bound');
 assert.deepEqual(await stamps(['rng-c']),[{identity_key:key('rng-c'),stamp:MAX}]);
});

test('readChannelMembers answers named and tagged members once with complete tags, whatever the order or repetition of its keys',async()=>{
 await backend.transaction(({channel})=>{const c=channel('read');c.todo.add({id:'read-a'},{tags:['X','Y']});c.todo.add({id:'read-b'},{tags:['X']});c.todo.add({id:'read-c'});});
 const read=(explicitKeys,tags)=>adapter(call=>call({op:'readChannelMembers',channel:'read',explicitKeys:explicitKeys.map(id=>({model:'Todo',identityKey:key(id)})),tags}));
 const sorted=rows=>rows.map(r=>[JSON.parse(r.identityKey).id,[...r.tags].sort()]).sort((a,b)=>a[0]<b[0]?-1:1);
 assert.deepEqual(sorted(await read(['read-c','read-a','read-c','read-z'],['X'])),[['read-a',['X','Y']],['read-b',['X']],['read-c',[]]]);
 assert.deepEqual(sorted(await read([],['nothing'])),[]);
 assert.deepEqual(sorted(await read(['read-c'],[])),[['read-c',[]]]);
 assert.deepEqual(sorted(await adapter(call=>call({op:'readChannelMembers',channel:'read-none',explicitKeys:[{model:'Todo',identityKey:key('read-a')}],tags:['X']}))),[],'another Channel answers none of them');
});

// ---- Forward upgrade from the six-table schema ------------------------------

/** `packages/postgres/migration.sql` as released in 0.1.x: the schema the upgrade starts from. */
const PREVIOUS_SCHEMA=`
CREATE TABLE IF NOT EXISTS axton_client (
 client_id text PRIMARY KEY,
 owner_id text NOT NULL,
 sequence bigint NOT NULL DEFAULT 0 CHECK(sequence >= 0 AND sequence <= 9007199254740991),
 receipt text
);
CREATE TABLE IF NOT EXISTS axton_call (
 owner_id text NOT NULL,
 call_id text NOT NULL,
 request text NOT NULL,
 response text,
 claim_tx xid8 NOT NULL DEFAULT pg_current_xact_id(),
 PRIMARY KEY(owner_id,call_id)
);
CREATE TABLE IF NOT EXISTS axton_channel (
 channel text PRIMARY KEY,
 head bigint NOT NULL CHECK(head >= 0 AND head <= 9007199254740991)
);
CREATE TABLE IF NOT EXISTS axton_record (
 model text NOT NULL,
 identity_key text NOT NULL,
 stamp bigint NOT NULL CHECK(stamp > 0 AND stamp <= 9007199254740991),
 PRIMARY KEY(model,identity_key)
);
CREATE TABLE IF NOT EXISTS axton_invalidation (
 channel text NOT NULL REFERENCES axton_channel(channel),
 model text NOT NULL,
 identity_key text NOT NULL,
 identity jsonb NOT NULL,
 cursor bigint NOT NULL CHECK(cursor > 0 AND cursor <= 9007199254740991),
 stamp bigint NOT NULL CHECK(stamp > 0 AND stamp <= 9007199254740991),
 PRIMARY KEY(channel,model,identity_key),
 UNIQUE(channel,cursor)
);
CREATE TABLE IF NOT EXISTS axton_membership (
 channel text NOT NULL REFERENCES axton_channel(channel),
 model text NOT NULL,
 identity_key text NOT NULL,
 PRIMARY KEY(model, identity_key, channel),
 FOREIGN KEY(model, identity_key) REFERENCES axton_record(model, identity_key)
);
CREATE INDEX IF NOT EXISTS axton_membership_channel
 ON axton_membership(channel, model, identity_key);
`;
const EIGHT=['axton_client','axton_call','axton_channel','axton_record','axton_channel_member','axton_channel_tag','axton_channel_member_tag','axton_channel_log'];
const databaseUrl=name=>{const u=new URL(url);u.pathname=`/${name}`;return u.toString();};
/** A fresh database in the cluster and a client on it. */
const scratch=async name=>{
 await q(`DROP DATABASE IF EXISTS ${name}`);await q(`CREATE DATABASE ${name}`);
 const client=new Client({connectionString:databaseUrl(name)});await client.connect();return client;
};
/** Apply the upgrade file as one simple-protocol call; a failure leaves the explicit transaction to roll back. */
const upgrade=async client=>{
 try{await client.query(await source('migrations/2026-09-30-channel-members.sql'));}
 catch(error){await client.query('ROLLBACK');throw error;}
};
/** Everything the eight tables are made of, independent of column order and sequence positions. */
const catalog=async client=>{
 const rows=async sql=>(await client.query(sql,[EIGHT])).rows;
 return {
  columns:await rows(`SELECT table_name,column_name,data_type,is_nullable,column_default,is_identity,identity_generation,is_generated,generation_expression FROM information_schema.columns WHERE table_schema='public' AND table_name=ANY($1) ORDER BY 1,2`),
  constraints:await rows(`SELECT conrelid::regclass::text AS tbl,conname,pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conrelid::regclass::text=ANY($1) ORDER BY 1,2`),
  indexes:await rows(`SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND tablename=ANY($1) ORDER BY 1,2`),
  triggers:await rows(`SELECT tgrelid::regclass::text AS tbl,tgname,pg_get_triggerdef(oid) AS def FROM pg_trigger WHERE NOT tgisinternal AND tgrelid::regclass::text=ANY($1) ORDER BY 1,2`),
  functions:(await client.query(`SELECT proname,prosrc FROM pg_proc WHERE proname LIKE 'axton%' ORDER BY 1`)).rows,
 };
};
/** Every row of every framework table, old and new, with its row version. */
const contents=async client=>{
 const out={};
 for(const table of [...EIGHT,'axton_membership','axton_invalidation']){
  const exists=(await client.query('SELECT to_regclass($1) AS t',[table])).rows[0].t;
  if(exists)out[table]=(await client.query(`SELECT xmin::text AS version,to_jsonb(t) AS row FROM ${table} t ORDER BY to_jsonb(t)::text`)).rows;
 }
 return out;
};
/** An old database holding memberships and retained invalidations of both kinds, and unlogged members. */
const seedPrevious=async client=>{
 await client.query(PREVIOUS_SCHEMA);
 const k=id=>JSON.stringify({id});
 await client.query("INSERT INTO axton_channel(channel,head) VALUES('m-A',5),('m-B',2),('m-E',0)");
 await client.query(`INSERT INTO axton_record(model,identity_key,stamp) VALUES
  ('Todo',$1,3),('Todo',$2,1),('Todo',$3,2),('Todo',$4,1),('Todo',$5,1),('Todo',$6,4),('Note',$1,1)`,[k('a'),k('b'),k('c'),k('d'),k('aa'),k('e')]);
 await client.query(`INSERT INTO axton_membership(channel,model,identity_key) VALUES
  ('m-A','Todo',$1),('m-A','Todo',$3),('m-A','Todo',$4),('m-A','Todo',$5),('m-A','Note',$1),('m-B','Todo',$2),('m-E','Todo',$1)`,[k('a'),k('b'),k('c'),k('d'),k('aa')]);
 await client.query(`INSERT INTO axton_invalidation(channel,model,identity_key,identity,cursor,stamp) VALUES
  ('m-A','Todo',$1,$1::text::jsonb,5,3),('m-A','Todo',$2,$2::text::jsonb,4,1),('m-A','Todo',$3,$3::text::jsonb,2,2),('m-B','Todo',$2,$2::text::jsonb,2,1)`,[k('a'),k('b'),k('c')]);
};
const logOf=async(client,channel)=>(await client.query('SELECT r.model,r.identity->>\'id\' AS id,l.cursor::int,l.kind FROM axton_channel_log l JOIN axton_record r ON r.id=l.record_id WHERE l.channel=$1 ORDER BY l.cursor',[channel])).rows.map(r=>[r.model,r.id,r.cursor,r.kind]);
const headsOf=async client=>Object.fromEntries((await client.query('SELECT channel,head::int FROM axton_channel ORDER BY channel')).rows.map(r=>[r.channel,r.head]));

test('the forward upgrade copies memberships, maps invalidations to current presence, logs unlogged members above the head and converges with a fresh install',async()=>{
 const fresh=await scratch('axton_fresh'),old=await scratch('axton_upgraded');
 try{
  await fresh.query(await source('migration.sql'));
  await seedPrevious(old);
  const legacy=async()=>({memberships:(await old.query('SELECT * FROM axton_membership ORDER BY channel,model,identity_key')).rows,invalidations:(await old.query('SELECT * FROM axton_invalidation ORDER BY channel,cursor')).rows});
  const kept=await legacy();
  await upgrade(old);
  assert.deepEqual(await catalog(old),await catalog(fresh),'fresh and upgraded schemas converge');
  assert.deepEqual(await legacy(),kept,'the old tables are retained unchanged');
  assert.deepEqual((await old.query("SELECT identity FROM axton_record WHERE model='Todo' AND identity_key='{\"id\":\"aa\"}'")).rows,[{identity:{id:'aa'}}],'identity is the decoded key');
  assert.deepEqual(await logOf(old,'m-A'),[['Todo','c',2,'upsert'],['Todo','b',4,'remove'],['Todo','a',5,'upsert'],['Note','a',6,'upsert'],['Todo','aa',7,'upsert'],['Todo','d',8,'upsert']],
   'retained cursors are preserved; a non-member is a remove; unlogged members follow the old head in record-key order');
  assert.deepEqual(await logOf(old,'m-B'),[['Todo','b',2,'upsert']]);
  assert.deepEqual(await logOf(old,'m-E'),[['Todo','a',1,'upsert']]);
  assert.deepEqual(await headsOf(old),{'m-A':8,'m-B':2,'m-E':1});
  assert.deepEqual((await old.query('SELECT count(*)::int AS n FROM axton_channel_member')).rows,[{n:7}]);
  assert.deepEqual((await old.query('SELECT count(*)::int AS n FROM axton_channel_tag')).rows,[{n:0}],'tags start empty');
  // The upgraded database serves the runtime.
  const upgradedPool=new Pool({connectionString:databaseUrl('axton_upgraded')});
  try{
   const d=pg(upgradedPool).driver;
   const call=r=>d.transaction(tx=>answer(d,tx,r));
   assert.deepEqual((await call({op:'scan',channel:'m-A',after:0,limit:50})).map(r=>[r.model,r.identity.id,r.cursor,r.kind,r.stamp??null]),[['Todo','c',2,'upsert',2],['Todo','b',4,'remove',null],['Todo','a',5,'upsert',3],['Note','a',6,'upsert',1],['Todo','aa',7,'upsert',1],['Todo','d',8,'upsert',1]]);
   const removed=await call({op:'applyChannelMembers',deltas:[{channel:'m-A',model:'Todo',identity:{id:'c'},identityKey:JSON.stringify({id:'c'}),present:false,tags:[],publish:true}]});
   assert.deepEqual(removed.map(p=>[p.cursor,p.kind]),[[9,'remove']]);
  }finally{await upgradedPool.end();}
  // A repeated upgrade verifies and changes nothing, even after runtime writes: the removal is not undone from the old tables.
  const before=await contents(old);
  await upgrade(old);
  assert.deepEqual(await contents(old),before,'no row was written or rewritten');
  assert.deepEqual(await catalog(old),await catalog(fresh));
  await upgrade(fresh);
  assert.deepEqual((await fresh.query('SELECT count(*)::int AS n FROM axton_channel_log')).rows,[{n:0}],'on a fresh install the upgrade is a verified no-op too');
 }finally{await fresh.end();await old.end();}
});

test('migration.sql split into single statements, as the Prisma consumers run it, installs the same schema; the splitter keeps quoted and dollar-quoted semicolons and drops comments',async()=>{
 assert.deepEqual(sqlStatements("-- a; comment\nSELECT 'a;b' AS \"x;y\"; /* c; */ SELECT $$d;e$$;\nDO $f$ BEGIN PERFORM 1; END $f$;\n-- trailing; comment\n"),
  ["SELECT 'a;b' AS \"x;y\"","SELECT $$d;e$$","DO $f$ BEGIN PERFORM 1; END $f$"]);
 const {PrismaClient}=require('../../bindings/node/generated/client');
 const whole=await scratch('axton_whole'),split=await scratch('axton_split');
 const prisma=new PrismaClient({datasourceUrl:databaseUrl('axton_split')});
 try{
  const text=await source('migration.sql');
  await whole.query(text);
  const statements=sqlStatements(text);
  assert.ok(statements.length>=13&&statements.every(sql=>/^(CREATE|DO)\s/.test(sql)),`every chunk is one DDL statement: ${statements.map(sql=>sql.slice(0,24)).join(' | ')}`);
  // Prisma prepares each call, so a chunk holding two statements or a stray comment fragment fails here.
  for(let round=0;round<2;round++)for(const sql of statements)await prisma.$executeRawUnsafe(sql);
  assert.deepEqual(await catalog(split),await catalog(whole),'one statement at a time, twice, installs exactly the whole-file schema');
 }finally{await prisma.$disconnect();await whole.end();await split.end();}
});

test('an upgrade whose data disagrees fails whole: a mismatched identity, a key that is not JSON, a position without metadata or above its head',async()=>{
 const k=id=>JSON.stringify({id});
 const cases=[
  ['identity',async c=>{await c.query("INSERT INTO axton_channel VALUES('c',1)");await c.query("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Todo',$1,1)",[k('y')]);await c.query("INSERT INTO axton_invalidation VALUES('c','Todo',$1,$2::text::jsonb,1,1)",[k('y'),k('x')]);},/disagree/],
  ['json',async c=>{await c.query("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Todo','not json',1)");},/json/i],
  ['metadata',async c=>{await c.query("INSERT INTO axton_channel VALUES('c',1)");await c.query("INSERT INTO axton_invalidation VALUES('c','Todo',$1,$1::text::jsonb,1,1)",[k('ghost')]);},/metadata/],
  ['head',async c=>{await c.query("INSERT INTO axton_channel VALUES('c',1)");await c.query("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Todo',$1,1)",[k('y')]);await c.query("INSERT INTO axton_membership VALUES('c','Todo',$1)",[k('y')]);await c.query("INSERT INTO axton_invalidation VALUES('c','Todo',$1,$1::text::jsonb,4,1)",[k('y')]);},/head/],
 ];
 for(const [label,prepare,pattern] of cases){
  const c=await scratch('axton_refused');
  try{
   await c.query(PREVIOUS_SCHEMA);await prepare(c);
   const before=await contents(c);
   await assert.rejects(()=>upgrade(c),pattern,label);
   assert.deepEqual(await contents(c),before,`${label}: nothing changed`);
   assert.deepEqual((await c.query("SELECT to_regclass('axton_channel_member') AS t,(SELECT count(*)::int FROM pg_attribute WHERE attrelid='axton_record'::regclass AND attname='id') AS id")).rows,[{t:null,id:0}],`${label}: no schema change survived`);
  }finally{await c.end();}
 }
 const empty=await scratch('axton_refused');
 try{await assert.rejects(()=>upgrade(empty),/migration\.sql/,'an empty database is installed from migration.sql');}finally{await empty.end();}
});

// ---- Concurrency with a real barrier ----------------------------------------

/**
 * Both transactions fix their snapshots, then wait until both have, then
 * declare. Answers how many attempts each body made: the driver retries the
 * one that lost the race whole.
 */
const together=async(first,second)=>{
 const runs=[0,0];let arrived=0,open;const both=new Promise(resolve=>{open=resolve;});
 const run=(n,body)=>backend.transaction(async call=>{
  runs[n]++;
  await driver.query(call.tx,'SELECT 1',[]);
  if(runs[n]===1){if(++arrived===2)open();await both;}
  await body(call);
 });
 await Promise.all([run(0,first),run(1,second)]);
 return runs;
};
/**
 * The committed state must be the serial outcome of the order the retry
 * reveals: the body that ran once committed first. A retry commits no extra
 * cursor, so heads and positions equal that serial outcome exactly.
 */
const serial=(runs,[firstThenSecond,secondThenFirst])=>{
 assert.deepEqual([...runs].sort(),[1,2],`exactly one transaction retried: ${runs}`);
 return runs[0]===1?{order:'first',expected:firstThenSecond}:{order:'second',expected:secondThenFirst};
};

test('add versus tag removal: one serial outcome, and the retried transaction adds no cursor',async t=>{
 const orders=[];
 for(let trial=0;trial<3;trial++){
  const channel=`cc-add-${trial}`;
  await backend.transaction(({channel:c})=>{c(channel).todo.add({id:`${channel}-A`},{tags:['X']});c(channel).todo.add({id:`${channel}-B`},{tags:['X']});});
  const [A,B,N]=['A','B','N'].map(s=>`${channel}-${s}`);
  const runs=await together(({channel:c})=>{c(channel).todo.add({id:N},{tags:['X']});},({channel:c})=>{c(channel).remove({tag:'X'});});
  const {order,expected}=serial(runs,[
   {members:[],log:[[A,4,'remove'],[B,5,'remove'],[N,6,'remove']],head:6,tags:[]},
   {members:[[N,['X']]],log:[[A,3,'remove'],[B,4,'remove'],[N,5,'upsert']],head:5,tags:['X']},
  ]);
  orders.push(order);
  assert.deepEqual(await state(channel),expected,`trial ${trial}: ${order} committed first`);
 }
 t.diagnostic(`add committed first in: ${orders.map(o=>o==='first'?'yes':'no').join(' ')}`);
});

test('a tag union versus an empty selector: one serial outcome',async()=>{
 for(let trial=0;trial<3;trial++){
  const channel=`cc-union-${trial}`,A=`${channel}-A`;
  await add(channel,A,['X']);
  const runs=await together(({channel:c})=>{c(channel).todo.add({id:A},{tags:['Y']});},({channel:c})=>{c(channel).remove({tag:'Y'});});
  const {order,expected}=serial(runs,[
   {members:[],log:[[A,2,'remove']],head:2,tags:[]},
   {members:[[A,['X','Y']]],log:[[A,1,'upsert']],head:1,tags:['X','Y']},
  ]);
  assert.deepEqual(await state(channel),expected,`trial ${trial}: ${order === 'first' ? 'the union' : 'the empty selector'} committed first`);
 }
});

test('a touch versus a tag removal: one serial outcome, never an upsert of an absent member',async()=>{
 for(let trial=0;trial<3;trial++){
  const K=`cc-touch-K-${trial}`,L=`cc-touch-L-${trial}`,A=`cc-touch-${trial}`;
  await seed(A);
  await backend.transaction(({channel})=>{channel(K).todo.add({id:A},{tags:['X']});channel(L).todo.add({id:A});});
  const runs=await together(async({tx,touch})=>{await write(tx,A,'touched');touch.todo({id:A});},({channel})=>{channel(K).remove({tag:'X'});});
  const {order,expected}=serial(runs,[
   {K:{members:[],log:[[A,3,'remove']],head:3,tags:[]},L:[[A,2,'upsert']],stamp:2},
   {K:{members:[],log:[[A,2,'remove']],head:2,tags:[]},L:[[A,2,'upsert']],stamp:2},
  ]);
  assert.deepEqual({K:await state(K),L:await log(L),stamp:(await stamps([A]))[0].stamp},expected,`trial ${trial}: ${order}`);
  const page=JSON.parse(await backend.pull('alice',JSON.stringify({capabilities:['channel-membership-v1'],cursors:{[K]:0},models:{Todo:1}})));
  assert.deepEqual(page.changes,[{channel:K,cursor:expected.K.head,kind:'remove',model:'Todo',identity:{id:A}}],'K delivers only identity removal');
 }
});

test('two first writers to a Channel with no row serialize on its insert: distinct consecutive cursors, or one member with both tags',async()=>{
 for(let trial=0;trial<3;trial++){
  const channel=`cc-new-${trial}`,[A,B]=[`${channel}-A`,`${channel}-B`];
  const runs=await together(({channel:c})=>{c(channel).todo.add({id:A},{tags:['X']});},({channel:c})=>{c(channel).todo.add({id:B},{tags:['X']});});
  const {expected}=serial(runs,[
   {members:[[A,['X']],[B,['X']]],log:[[A,1,'upsert'],[B,2,'upsert']],head:2,tags:['X']},
   {members:[[A,['X']],[B,['X']]],log:[[B,1,'upsert'],[A,2,'upsert']],head:2,tags:['X']},
  ]);
  assert.deepEqual(await state(channel),expected,`trial ${trial}`);
  const same=`cc-same-${trial}`,S=`${same}-S`;
  const again=await together(({channel:c})=>{c(same).todo.add({id:S},{tags:['X']});},({channel:c})=>{c(same).todo.add({id:S},{tags:['Y']});});
  const {expected:joined}=serial(again,[
   {members:[[S,['X','Y']]],log:[[S,1,'upsert']],head:1,tags:['X','Y']},
   {members:[[S,['X','Y']]],log:[[S,1,'upsert']],head:1,tags:['X','Y']},
  ]);
  assert.deepEqual(await state(same),joined,`trial ${trial}: the second add unions its tag without a position`);
 }
});

// ---- 10,000 members ---------------------------------------------------------

test('removing 10,000 tagged members: a failure after the log writes rolls everything back; the committed removal calls no Loader and runs bounded statement groups',async t=>{
 const channel='bulk',N=10000;
 const ids=Array.from({length:N},(_,i)=>`bulk-${String(i).padStart(5,'0')}`);
 const counted=()=>{const seen=new Map();let total=0;return {seen,get total(){return total;},hook:sql=>{total++;seen.set(sql,(seen.get(sql)??0)+1);}};};
 const named=seen=>Object.fromEntries([...seen].map(([sql,n])=>[Object.entries(SQL).find(([,text])=>text===sql)?.[0]??sql.slice(0,40),n]));
 const adding=counted();
 let started=performance.now();
 await watched(adding.hook).transaction(({channel:c})=>{c(channel).add(ids.map(id=>({model:'Todo',identity:{id}})),{tags:['X','keep']});});
 const addMs=performance.now()-started;
 assert.equal(await head(channel),N);
 const snapshot=async()=>({
  members:(await q('SELECT count(*)::int AS n FROM axton_channel_member WHERE channel=$1',[channel]))[0].n,
  associations:(await q('SELECT count(*)::int AS n FROM axton_channel_member_tag mt JOIN axton_channel_member m ON m.id=mt.member_id WHERE m.channel=$1',[channel]))[0].n,
  tags:await tagNames(channel),
  log:(await q('SELECT kind,count(*)::int AS n,sum(cursor)::text AS sum FROM axton_channel_log WHERE channel=$1 GROUP BY kind',[channel])),
  head:await head(channel),
  business:(await q("SELECT count(*)::int AS n FROM tag_todo WHERE id='bulk-domain'"))[0].n,
 });
 const before=await snapshot();
 assert.deepEqual(before.log,[{kind:'upsert',n:N,sum:String(N*(N+1)/2)}]);
 // Fail after the log statements ran and before the members are deleted.
 const failing=counted();let logged=false;
 const injected=watched(sql=>{failing.hook(sql);if(sql===SQL.WRITE_CHANNEL_LOG)logged=true;if(sql===SQL.DELETE_CHANNEL_MEMBERS)throw new Error('injected before member deletion');});
 await assert.rejects(()=>injected.transaction(async({tx,channel:c})=>{await write(tx,'bulk-domain','never');c(channel).remove({tag:'X'});}),/injected before member deletion/);
 assert.equal(logged,true,'the log writes had run');
 assert.deepEqual(await snapshot(),before,'no partial domain, member, tag, log or head change');
 // The committed removal.
 const removing=counted();loaderCalls=0;
 started=performance.now();
 await watched(removing.hook).transaction(({channel:c})=>{c(channel).remove({tag:'X'});});
 const removeMs=performance.now()-started;
 assert.equal(loaderCalls,0,'zero Loader calls');
 assert.deepEqual(await snapshot(),{members:0,associations:0,tags:[],log:[{kind:'remove',n:N,sum:String((N+1+2*N)*N/2)}],head:2*N,business:0});
 const batches=Math.ceil(N/SQL.CHANNEL_BATCH);
 // lockChannels, one tag read, one reservation, then per batch a log write
 // and a member deletion, and one collection of the dropped tags.
 assert.ok(removing.total<=4+2*batches,`bounded statement groups: ${removing.total} statements for ${N} members`);
 assert.ok(removing.total>=batches,'not an unrealistically constant number of row writes');
 t.diagnostic(`add ${N}: ${adding.total} statements, ${Math.round(addMs)} ms: ${JSON.stringify(named(adding.seen))}`);
 t.diagnostic(`remove ${N}: ${removing.total} statements, ${Math.round(removeMs)} ms: ${JSON.stringify(named(removing.seen))}`);
});
