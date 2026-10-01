import {Book,Comment,Entry as EntryRef,type Handlers,type Loaders,type EntryV1,type MutationContext,type QueryContext,type HandlerCall,type TransactionCall,type AddBookInput} from './backend.ts';
import {strict as assert} from 'node:assert';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {GeneratedClient,CallError,type Call,type BootstrapPhase,type BootstrapStatus,type Subscription,type SubscriptionStatus,type RefusedAct,type FailedAct,type FailedTask,type SubmittedAct} from './client.ts';
import type {StoreHooks, StoreChange, EntryIdentity, Placement, Status, Composition, PublishEntryOutput, RenameOutput, SubmitMutationOptions, SubmitMutationPort, UnsentResolutionPort} from './generated.ts';
import {ApplicationTransaction,CompanionContext,makeTransactionMutations} from './generated.ts';
import type {Transaction as RawTransaction} from '../../packages/client-js/index.mts';
import {Client as RawClient} from '../../packages/client-js/index.mts';
import {CreateEntry,EditEntry,RemoveEntries,decodeEntry,encodeEntry,EntryModel,EntryLiveModel,GeneratedTransaction,Mutate,type Entry,type ReadPort,type LivePort,type WritePort,type MutationName,type SyncState} from './generated.ts';
const row:Entry={id:'123e4567-e89b-42d3-a456-426614174000',title:'hello',note:null,at:new Date('2026-01-01T00:00:00Z'),tags:['x'],status:'active'};
const externalHooks: StoreHooks = {
 async entry(tx, changes) {
  for (const change of changes) {
   const id: string = change.identity.id;
   if (change.kind === 'upsert') {
    const at: Date = change.row.at;
    const status: 'active' | 'archived' = change.row.status;
    await tx.models.entry.get({id});
    await tx.scopes.subscribe(`entry:${id}`);
    void [at,status];
   } else {
    await tx.scopes.unsubscribe(`entry:${id}`);
    // @ts-expect-error deletes have no row
    void change.row;
   }
  }
  // @ts-expect-error remote actions are unavailable in a store transaction
  void tx.queries;
  // @ts-expect-error Fetch is unavailable in a store transaction
  void tx.fetch;
 },
};
const typedChange: StoreChange<EntryIdentity, Entry> = {kind:'upsert',identity:{id:row.id},row};
void [externalHooks,typedChange];
function check(v:unknown,m:string){if(!v)throw Error(m)}
async function until(predicate:()=>boolean,what:string){
 const deadline=Date.now()+5000;
 while(Date.now()<deadline){if(predicate())return;await new Promise(resolve=>setTimeout(resolve,5));}
 throw Error(`${what} timed out`);
}
const create=CreateEntry({entry:row});
check(!('id' in (create.operations[0] as {values:object}).values),'identity leaked into state');
check(JSON.stringify(decodeEntry(encodeEntry(row)))===JSON.stringify(row),'source conversion');
const patch=EditEntry({entry:{identity:{id:row.id},values:{note:null}}});
check(JSON.stringify((patch.operations[0] as {values:object}).values)==='{"note":null}','presence semantics');
check(RemoveEntries({entries:[]}).operations.length===0,'optional/list');
const reads:ReadPort={async read(){return encodeEntry(row)},async querySpec(){return [encodeEntry(row)]},async related(){return null},async referencing(){return []}};
if(false){
 const entries=new EntryModel(reads);
 // @ts-expect-error lists cannot be query predicates
 entries.query({where:{tags:[]}});
 // @ts-expect-error enum ordering is not defined
 entries.query({orderBy:[{field:'status',direction:'ascending'}]});
 // @ts-expect-error date filter must be a Date
 entries.query({where:{at:'2026-01-01'}});
 const live:LivePort={...reads,async direct(){},watch(){return ()=>{}},async syncState(){return {pending:[],rejections:[]}}};
 new EntryLiveModel(live).watch({},(rows)=>rows[0]?.at.getTime());
 // A record's sync state is typed by model: the identity is the model's, pending names are the schema's mutations.
 const state:Promise<SyncState>=new EntryLiveModel(live).syncState({id:row.id});
 void state.then(s=>{const name:MutationName=s.pending[0]!.name;const diverged:boolean|undefined=s.pending[0]!.diverged;void name;void diverged;});
 // @ts-expect-error syncState takes the model's identity
 new EntryLiveModel(live).syncState({title:'x'});
 // @ts-expect-error a pending name is one of the schema's mutations
 const unknown:SyncState['pending'][number]={ordinal:1,name:'NoSuchMutation',phase:'queued',prerequisites:[]};
 // Mutations run outside a transaction through any port that can enqueue one.
 const direct:Promise<number>=new Mutate({async mutate(){return 1}}).editEntry({entry:{identity:{id:row.id},values:{note:null}}});
 const writes:WritePort={...reads,async direct(){}};
 // @ts-expect-error watch is not available inside a transaction
 new GeneratedTransaction(writes).models.entry.watch({},()=>{});
 // @ts-expect-error named mutations are unavailable in local transactions
 new GeneratedTransaction(writes).mutate;
 // @ts-expect-error actions are unavailable in local transactions
 new GeneratedTransaction(writes).actions;
 // @ts-expect-error Fetch is unavailable in local transactions
 new GeneratedTransaction(writes).fetch;
 const rawTx={} as RawTransaction;
 // @ts-expect-error raw transactions cannot enqueue named mutations
 rawTx.mutate;
 // @ts-expect-error raw transactions have no action namespace
 rawTx.actions;

 // @ts-expect-error identity is immutable in patch
 EditEntry({entry:{identity:{id:row.id},values:{id:'bad'}}});
 // @ts-expect-error mutation forbids tags
 EditEntry({entry:{identity:{id:row.id},values:{tags:[]}}});
 // @ts-expect-error nonnullable title
 EditEntry({entry:{identity:{id:row.id},values:{title:null}}});
 // @ts-expect-error enum typo
 const bad:Entry={...row,status:'typo'};

 type Tx={rows:Map<string,object>};
 const shorthand:Handlers<Tx>['addBook']=async({input,tx,scope: scope,touch})=>{tx.rows.set(input.book.id,input.book);touch.book(input.book);scope('c').add.book(input.book)};
 const grouped:Handlers<Tx>['editEntry']={
  async v1({input,scope: scope}){scope('c').add.entry(input.target.identity)},
  // Legacy slot handlers declare through the same handles; mixed lists take explicit references and may be empty.
  async v2({input,scope: scope,touch}){touch.entry(input.entry.identity);scope('c').add([EntryRef(input.entry.identity),Book({id:'b'})]);scope('audit').remove([])},
  // @ts-expect-error v3 is not a retained version of EditEntry
  async v3(){},
 };
 // @ts-expect-error a mutation with two retained versions cannot register a bare function
 const bare:Handlers<Tx>['editEntry']=async()=>{};
 // @ts-expect-error every retained version must be registered
 const partial:Handlers<Tx>['editEntry']={v2:async({input,scope: scope})=>{scope('c').add.entry(input.entry.identity)}};
 // @ts-expect-error handlers declare through `scope` and `touch`; there is no notify and no return value
 const legacy:Handlers<Tx>['addBook']=async({notify})=>{notify({scope:'c',records:[]})};
 // @ts-expect-error the old publish API is gone
 const published:Handlers<Tx>['addBook']=async({publish})=>{publish({scope:'c'})};
 // @ts-expect-error the old changes collector is gone
 const changed:Handlers<Tx>['addBook']=async({changes})=>{changes.add({model:'Book',identity:{id:'b'}})};

 // The generated declaration API, per schema: operation before Model.
 const declare=(ctx:MutationContext<Tx>,queryCtx:QueryContext<Tx>,call:HandlerCall<Tx,AddBookInput>,external:TransactionCall<Tx>)=>{
  ctx.scope('project:1').add.book({id:'A'});
  ctx.scope('project:1').remove.book({id:'A'});
  ctx.touch.book({id:'A'});
  // @ts-expect-error missing identity
  ctx.scope('project:1').add.book({});
  // @ts-expect-error old API is gone
  ctx.publish({scope:'project:1'});
  // @ts-expect-error Query has no membership writer
  queryCtx.scope('project:1').add.book({id:'A'});
  // @ts-expect-error Query has no change declaration
  queryCtx.touch.book({id:'A'});
  // A Scope handle exposes each Model under its operation namespaces.
  const project=ctx.scope('project:1');
  project.add([Book({id:'A'}),Comment({id:'c'}),EntryRef({id:row.id})]);
  project.remove.comment({id:'c'});
  call.scope('project:1').add.entry({id:row.id});
  call.touch.counter({id:'n'});
  external.scope('project:1').remove([Book({id:'A'})]);
  external.touch.draft({id:row.id});
  // @ts-expect-error a raw identity names no Model
  project.add([{id:'A'}]);
  // @ts-expect-error a UUID identity is a string
  external.touch.entry({id:1});
  // @ts-expect-error the Scope's mixed verbs take references, not identities
  project.remove({id:'A'});
  // Chained labels and explicit selections support typed and mixed record declarations.
  ctx.scope('project:1').add.entry({id:row.id}).tag(['X']);
  project.add([Book({id:'A'}),EntryRef({id:row.id})]).tag(['X','Y']);
  project.add.book({id:'A'});
  project.add([Comment({id:'c'})]);
  project.where({ tags: { all: ['X'] } }).remove();
  external.scope('project:1').where({ tags: { all: ['X'] } }).remove();
  call.scope('project:1').add([Book({id:'A'})]).tag(['X']);
  // @ts-expect-error a tagged add still names the Model's identity: a Book id is a string
  project.add.book({id:1}).tag(['X']);
  // @ts-expect-error an Entry identity is not a Book identity
  project.add.entry({title:'t'}).tag(['X']);
  // @ts-expect-error labels are strings or lists of strings
  project.add.book({id:'A'}).tag(3);
  // @ts-expect-error label options are retired
  project.add.book({id:'A'}, {tag:'X'});
  // @ts-expect-error a Model's remove takes an identity, not a tag selector
  project.remove.book({tag:'X'});
  // @ts-expect-error a remove carries no tags
  project.remove([Book({id:'A'})]).tag(['X']);
  // @ts-expect-error a tag selector is not a record reference
  project.remove({tag:'X'});
  project.add.book([{id:'A'}]);
 };
 // @ts-expect-error a handler has no return value to select a scope with
 const returned:Handlers<Tx>['addBook']=async()=>({scope:'c'});
 // @ts-expect-error loaders receive no scope
 const scopeled:Loaders<Tx>['book']=async({ids,scope: scope})=>ids.map(id=>({...id,title:String(scope)}));
 // A schema without Loads declares no Load context or its add-only Scope.
 // @ts-expect-error legacy LoadChannel is never declared
 type NoLoadScope=import('./backend.ts').LoadChannel;
 // @ts-expect-error no Load is declared, so the backend declares no LoadContext
 type NoLoadContext=import('./backend.ts').LoadContext<Tx>;

 // Loaders follow the same shape; a retained older contract has its own record type.
 const v1Row:EntryV1={id:row.id,title:'old',note:null,at:row.at,status:'active'};
 const versionedLoaders:Loaders<Tx>['entry']={
  async v1({ids}){return ids.map(()=>v1Row)},
  async v2({ids}){return ids.map(()=>row)},
  // @ts-expect-error v3 is not a retained version of Entry
  async v3(){return []},
 };
 const shorthandLoader:Loaders<Tx>['book']=async({ids})=>ids.map(id=>({...id,title:'t'}));
 // @ts-expect-error a model with two retained versions cannot register a bare function
 const bareLoader:Loaders<Tx>['entry']=async()=>[];
 // @ts-expect-error every retained model version must be registered
 const partialLoader:Loaders<Tx>['entry']={v2:async({ids})=>ids.map(()=>row)};
 // A Model without a Loader is device-only (#187): a Loaders map may omit it.
 const deviceOnly:Loaders<Tx>={book:shorthandLoader,entry:versionedLoaders};
 const noLoaders:Loaders<Tx>={};
 // A standalone version of an optional member is typed through NonNullable.
 const entryV2:NonNullable<Loaders<Tx>['entry']>['v2']=async({ids})=>ids.map(()=>row);
 // @ts-expect-error a Model that registers a Loader registers every retained version
 const partialModel:Loaders<Tx>={entry:{v2:async({ids})=>ids.map(()=>row)}};
 // @ts-expect-error a v1 loader cannot return a value outside the v1 contract
 const wrongEnum:EntryV1={...v1Row,status:'typo'};
 // @ts-expect-error the v1 contract has no tags
 const extra:EntryV1={...v1Row,tags:[]};
}
const tx=new GeneratedTransaction({...reads,async direct(op){check(JSON.stringify(op)===JSON.stringify({model:'Entry',op:'delete',identity:{id:row.id}}),'local write');}});
async function main(){check((await tx.models.entry.get({id:row.id}))?.at instanceof Date,'read decode');check((await tx.models.entry.query()).length===1,'query facade');check(await new Mutate({async mutate(m){check(JSON.stringify(m)===JSON.stringify(create),'forwarding');return 1}}).createEntry({entry:row})===1,'mutate facade');await tx.models.entry.delete({id:row.id});}
main();
// The generated client carries the schema check and the rebuild call.
type Rebuilt=Awaited<ReturnType<GeneratedClient['rebuild']>>;
type SchemaCheck=Awaited<ReturnType<GeneratedClient['syncState']>>['schema'];
const rebuildShape=(report:Rebuilt,state:SchemaCheck):[number,number,boolean]=>[report.leftPending,report.leftDirect,state.rebuilt];
void rebuildShape;
function misuse(app:GeneratedClient){
 // @ts-expect-error rebuild takes an options object
 void app.rebuild(true);
}
void misuse;
// The generated Scope facade ([#150](https://github.com/zanminwang/axton/issues/150)):
// one handle per registration, typed handle members, and the `scopes`
// spelling on that same ledger path.
const scopeDirectory=await mkdtemp(join(tmpdir(),'generated-scopes-'));
const client=await GeneratedClient.open({path:join(scopeDirectory,'state.sqlite')});
try{
 const [a,b]=await Promise.all([
  client.scopes.subscribe("project:123"),
  client.scopes.subscribe("project:123"),
 ]);
 assert.equal(a,b);
 assert.equal(a.status.initialization,"pending");
 await a.unsubscribe();
 const c=await client.scopes.subscribe("project:123");
 await a.unsubscribe();
 assert.equal(c.status.active,true);
 // The handle is the runtime's: its Scope, its immutable status, its observer
 // cancellation and its removal are all named through the generated module.
 const handle:Subscription=c;
 const scope:string=handle.scope;
 const status:SubscriptionStatus=handle.status;
 const stopWatching:()=>void=handle.watch(snapshot=>void snapshot.connection);
 stopWatching();
 check(scope==='project:123'&&status.connection==='offline','typed handle members');
 // A second Scope registers durable intent before any server boundary.
 const retained:Subscription=await client.scopes.subscribe('project:456');
 assert.equal(retained.status.initialization,'pending','scopes registers through the same ledger');
 assert.equal(await client.scopes.subscribe('project:456'),retained,'and shares one handle per registration');
 const removal:Promise<void>=retained.unsubscribe();
 await removal;
 assert.equal(retained.status.active,false);
 await handle.unsubscribe();
}finally{await client.close();await rm(scopeDirectory,{recursive:true,force:true});}

// Whole-Scope bootstrap through the generated facade
// ([#151](https://github.com/zanminwang/axton/issues/151)): the handle's
// `bootstrap()` and the `bootstrap` part of its typed status are named through
// the generated module, and two concurrent calls register one task.
type FakeSocket={on(event:string,listener:(data:unknown)=>void):void;send(data:string):void;terminate():void};
type FakeServer={clients:Set<FakeSocket>;on(event:'connection',listener:(socket:FakeSocket)=>void):void;close(done:()=>void):void};
const {WebSocketServer}=createRequire(import.meta.url)('ws') as
 {WebSocketServer:new(options:{server:unknown})=>FakeServer};
const bootstrapDirectory=await mkdtemp(join(tmpdir(),'generated-bootstrap-'));
const loads:{after:number;until:number}[]=[];
let release=()=>{};
const held=new Promise<void>(resolve=>{release=resolve;});
const http=createServer(async(request,response)=>{
 const chunks:Buffer[]=[];for await(const chunk of request)chunks.push(chunk as Buffer);
 const body=JSON.parse(Buffer.concat(chunks).toString()) as {mode?:string;scope:string;after:number;until:number};
 // Only a bootstrap page is expected here, and the test transport holds it.
 loads.push({after:body.after,until:body.until});
 await held;
 response.end(JSON.stringify({mode:'bootstrap',scope:body.scope,from:body.after,to:body.until,until:body.until,head:body.until,changes:[]}));
});
await new Promise<void>(resolve=>http.listen(0,'127.0.0.1',()=>resolve()));
const sockets=new WebSocketServer({server:http});
sockets.on('connection',socket=>{
 socket.on('message',message=>{
  const subscribe=JSON.parse(String(message)) as {scopes:string[]};
  socket.send(JSON.stringify({type:'subscribed',cursors:Object.fromEntries(subscribe.scopes.map(scope=>[scope,0]))}));
 });
});
const address=http.address();
const port=typeof address==='object'&&address!==null?address.port:0;
const loading=await GeneratedClient.open({path:join(bootstrapDirectory,'state.sqlite')});
try{
 const subscription=await loading.scopes.subscribe("project:123");
 const initial:BootstrapStatus=subscription.status.bootstrap;
 const phase:BootstrapPhase=initial.phase;
 assert.deepEqual({...initial},{phase:'not-requested',error:null});
 check(phase==='not-requested','the typed load phase of a registration that asked for nothing');
 await loading.connect({url:`http://127.0.0.1:${port}`,token:'secret'});
 await until(()=>subscription.status.initialization==='ready','the committed boundary');
 const first:Promise<void>=subscription.bootstrap();
 const second:Promise<void>=subscription.bootstrap();
 let settled=false;const both=Promise.all([first,second]).then(()=>{settled=true;});
 await until(()=>subscription.status.bootstrap.phase==='loading','a registered load');
 await until(()=>loads.length===1,'the one page the run asked for');
 assert.equal(settled,false,'the held response keeps both calls pending');
 release();
 await both;
 assert.equal(subscription.status.bootstrap.phase,'complete');
 assert.equal(loads.length,1,'two concurrent calls registered one task');
 // No task-cancel and no forced-refresh method is part of the handle's type;
 // `negative/misuse.ts` and `negative/misuse.dart` hold those refusals.
 await subscription.bootstrap();
 assert.equal(loads.length,1,'a completed run resolves locally and asks for nothing more');
}finally{
 await loading.close();
 for(const socket of sockets.clients)socket.terminate();
 await new Promise<void>(resolve=>sockets.close(()=>resolve()));
 await new Promise<void>(resolve=>http.close(()=>resolve()));
 await rm(bootstrapDirectory,{recursive:true,force:true});
}

// Exercise the generated adapter with incoming wire records: DateTime and enum
// conversion use the same Model decoder as ordinary reads, and registration
// captures the function value at open.
const rawClass=RawClient as unknown as {open:(options:any)=>Promise<any>};
const originalRawOpen=rawClass.open;
let registered:Record<string,(tx:unknown,changes:unknown[])=>void|Promise<void>>|undefined;
rawClass.open=async options=>{registered=options.onStore;return {async close(){}};};
const delivered:string[]=[];
const decoderHooks:StoreHooks={entry:(_tx,changes)=>{
 for(const change of changes){
  if(change.kind==='upsert'){
   assert.equal(change.row.at instanceof Date,true);
   assert.equal(change.row.status,'active');
   delivered.push(`${change.identity.id}:${change.row.at.toISOString()}`);
  }else{
   assert.equal('row' in change,false);
   delivered.push(`delete:${change.identity.id}`);
  }
 }
}};
try{
 const adapted=await GeneratedClient.open({path:'unused-for-captured-adapter',onStore:decoderHooks});
 try{
  Object.assign(decoderHooks,{entry:()=>{throw Error('mutable map replaced registration');}});
  assert.ok(registered?.Entry);
  const rawRow=encodeEntry(row);
  await registered.Entry({scopes:{}},[{kind:'upsert',identity:{id:row.id},row:rawRow},{kind:'delete',identity:{id:row.id}}]);
  assert.deepEqual(delivered,[`${row.id}:2026-01-01T00:00:00.000Z`,`delete:${row.id}`]);
 }finally{await adapted.close();}
}finally{rawClass.open=originalRawOpen;}

// Model Fetch ([#153](https://github.com/zanminwang/axton/issues/153)): one
// typed method per Model; the result is the complete snapshot or null.
async function checkFetch(client: GeneratedClient, id: string) {
  const result: Entry | null = await client.fetch.entry({ id });
  const preview: Entry | null = await client.fetch.entry({ id }, { store: false });
  // @ts-expect-error Fetch storage accepts a boolean, not an output map
  await client.fetch.entry({ id }, { store: { entry: false } });
  // @ts-expect-error no persistent once option
  await client.fetch.entry({ id }, { once: true });
  return [result, preview];
}
async function checkFetchShapes(client:GeneratedClient,at:Date){
 const placed:Placement|null=await client.fetch.placement({shelf:'s',at});
 const stored:Entry|null=await client.fetch.entry({id:'e'},{store:true});
 const when:Date|undefined=stored?.at;
 const status:Status|undefined=stored?.status;
 const tags:string[]|undefined=stored?.tags;
 const nullable:string|null|undefined=stored?.note;
 return [placed,when,status,tags,nullable];
}
void [checkFetch,checkFetchShapes];

// The generated facade over the native runtime and a real HTTP route: default
// storage with onStore, `store: false`, a composite DateTime identity, absence,
// a typed backend refusal and joined callers with independent objects.
const fetchDirectory=await mkdtemp(join(tmpdir(),'generated-fetch-'));
const fetchRequests:{path:string|undefined;authorization:string|undefined;body:{callId:string;model:string;version:number;identity:Record<string,unknown>;store?:boolean}}[]=[];
let releaseFetch=()=>{};
const heldFetch=new Promise<void>(resolve=>{releaseFetch=resolve;});
const fetchServer=createServer(async(request,response)=>{
 const chunks:Buffer[]=[];for await(const chunk of request)chunks.push(chunk as Buffer);
 const body=JSON.parse(Buffer.concat(chunks).toString()) as typeof fetchRequests[number]['body'];
 fetchRequests.push({path:request.url,authorization:request.headers.authorization,body});
 if(body.model==='Book')await heldFetch;
 const identity=body.identity;
 const state:Record<string,unknown>|null=
  body.model==='Placement'?{label:'placed'}:
  body.model==='Book'?{title:'remote book'}:
  body.model==='Entry'&&identity.id!=='00000000-0000-4000-8000-000000000000'?{title:'remote',note:null,at:'2026-02-03T04:05:06.000Z',tags:['x'],status:'archived'}:
  null;
 const outcome=body.model==='Counter'
  ?{status:'failed',code:'loader.failed',execution:'rejected'}
  :{status:'succeeded',result:state===null?null:{...identity,...state}};
 const records=body.model==='Counter'||body.store===false?[]:[{model:body.model,identity,stamp:1,state}];
 response.end(JSON.stringify({completion:{callId:body.callId,outcome},records}));
});
await new Promise<void>(resolve=>fetchServer.listen(0,'127.0.0.1',()=>resolve()));
const fetchAddress=fetchServer.address();
const fetchPort=typeof fetchAddress==='object'&&fetchAddress!==null?fetchAddress.port:0;
const storedChanges:string[]=[];
const fetching=await GeneratedClient.open({
 path:join(fetchDirectory,'state.sqlite'),
 server:{url:`http://127.0.0.1:${fetchPort}`,token:'secret'},
 onStore:{entry:(_tx,changes)=>{for(const change of changes)storedChanges.push(change.kind==='upsert'?`${change.row.status}@${change.row.at.toISOString()}`:'delete');}},
});
try{
 const entry=await fetching.fetch.entry({id:row.id});
 assert.ok(entry?.at instanceof Date);
 assert.equal(entry.at.toISOString(),'2026-02-03T04:05:06.000Z');
 assert.equal(entry.status,'archived');
 assert.deepEqual(entry.tags,['x']);
 assert.equal((await fetching.models.entry.get({id:row.id}))?.title,'remote','stored by default');
 assert.deepEqual(storedChanges,['archived@2026-02-03T04:05:06.000Z'],'onStore ran with decoded rows');
 assert.deepEqual({path:fetchRequests[0]!.path,authorization:fetchRequests[0]!.authorization},{path:'/sync/fetch',authorization:'Bearer secret'});
 assert.equal(fetchRequests[0]!.body.version,2,'the Model read version the schema declares');
 const other='123e4567-e89b-42d3-a456-426614174999';
 const preview=await fetching.fetch.entry({id:other},{store:false});
 assert.equal(preview?.title,'remote');
 assert.equal(fetchRequests[1]!.body.store,false);
 assert.equal(await fetching.models.entry.get({id:other}),null,'store false writes nothing');
 assert.equal(storedChanges.length,1,'store false runs no onStore');
 const at=new Date('2026-03-04T05:06:07.000Z');
 const placed=await fetching.fetch.placement({shelf:'s',at});
 assert.ok(placed?.at instanceof Date);
 assert.equal(placed.at.getTime(),at.getTime());
 assert.equal(placed.label,'placed');
 assert.equal(new Date(fetchRequests[2]!.body.identity.at as string).getTime(),at.getTime());
 assert.equal((await fetching.models.placement.get({shelf:'s',at}))?.label,'placed');
 assert.equal(await fetching.fetch.entry({id:'00000000-0000-4000-8000-000000000000'}),null);
 await assert.rejects(fetching.fetch.counter({id:'n'}),(error:unknown)=>error instanceof CallError&&error.code==='loader.failed'&&error.execution==='rejected');
 const joined=[fetching.fetch.book({id:'b'}),fetching.fetch.book({id:'b'})];
 await until(()=>fetchRequests.some(request=>request.body.model==='Book'),'the joined request');
 releaseFetch();
 const [first,second]=await Promise.all(joined);
 assert.equal(fetchRequests.filter(request=>request.body.model==='Book').length,1,'joined callers share one request');
 assert.deepEqual(first,second);
 assert.notEqual(first,second,'each caller decodes its own object');
}finally{
 await fetching.close();
 await new Promise<void>(resolve=>fetchServer.close(()=>resolve()));
 await rm(fetchDirectory,{recursive:true,force:true});
}

// Transactional Mutation enqueue: `tx.mutations` queues typed Mutations in an
// application transaction and returns each one's Call; a Mutation's `local`
// callback gets Models only. A scripted port stands in for the runtime's raw
// transaction, so this checks the generated facade, not the runtime.
const compositionId='123e4567-e89b-42d3-a456-426614174001';
const compositionRow={id:compositionId,title:'draft',body:'text'};
function scriptedTransaction(){
 const submitted:{name:string;version:number;args:object;options:SubmitMutationOptions|undefined}[]=[];
 const outer:object[]=[];
 const companions:object[]=[];
 const resolutions:object[]=[];
 const companionPort:WritePort={...reads,async read(){return compositionRow},async direct(op){companions.push(op);}};
 const port:WritePort&SubmitMutationPort&UnsentResolutionPort&{scopes:GeneratedTransaction['scopes']}={
  ...reads,
  async direct(op){outer.push(op);},
  scopes:{async subscribe(){},async unsubscribe(){}},
  rejections:{async dismiss(id){resolutions.push({dismiss:id});}},
  failures:{async retry(taskKeys){resolutions.push({retry:taskKeys});},async drop(ordinal){resolutions.push({drop:ordinal});}},
  async submitMutation<T>(name:string,version:number,args:object,decode:(value:unknown)=>T,options?:SubmitMutationOptions):Promise<Call<T>>{
   submitted.push({name,version,args,options});
   await options?.local?.(companionPort);
   const result=decode(name==='PublishEntry'?{published:encodeEntry(row)}:null);
   return {status:'pending',async wait(){return {result,error:null};}};
  },
 };
 return {port,submitted,outer,companions,resolutions};
}
// Typed shapes: the callback returns any value, including one or several Calls.
async function transactionShapes(client:GeneratedClient){
 const scalar:Call<RenameOutput>=await client.transaction(tx=>tx.mutations.rename({id:compositionId,title:'t'}));
 const published:Call<PublishEntryOutput>=await client.transaction(async tx=>{
  const composition=await tx.models.composition.get({id:compositionId});
  if(!composition)throw Error('Composition not found');
  return await tx.mutations.publishEntry({entry:row,composition:composition.id},{local:async local=>{await local.models.composition.delete({id:composition.id});}});
 });
 const pair:{first:Call<PublishEntryOutput>;second:Call<void>}=await client.transaction(async tx=>{
  const first=await tx.mutations.publishEntry({entry:row,composition:compositionId},{store:{published:false},local:async local=>{await local.models.composition.update({id:compositionId},{title:'published'});}});
  const second=await tx.mutations.rename({id:compositionId,title:'t'},{store:false});
  return {first,second};
 });
 const counted:number=await client.transaction(async tx=>{await tx.models.composition.create(compositionRow);await tx.scopes.subscribe('c');return 1;});
 const nothing:void=await client.transaction(async tx=>{await tx.models.composition.delete({id:compositionId});});
 const outcome=await published.wait();
 const at:Date|undefined=outcome.result?.published.at;
 // Unsent work: typed streams on the client, resolutions in the transaction.
 const stop:()=>void=client.rejections.watch((items:RefusedAct[])=>{const act:SubmittedAct|undefined=items[0]?.act;void act?.args;});
 client.failures.watch((items:FailedAct[])=>{const task:FailedTask|undefined=items[0]?.tasks[0];void task?.error;},(error:unknown)=>void error);
 client.outbound.watchPending((count:number)=>void count);
 const refused:RefusedAct|null=await client.rejections.get(1);
 await client.rejections.dismiss(1);
 await client.failures.retry(['key']);
 await client.failures.drop(1);
 await client.transaction(async tx=>{await tx.failures.drop(1);await tx.failures.retry(['key']);await tx.rejections.dismiss(2);return tx.mutations.rename({id:compositionId,title:'fixed'});});
 // @ts-expect-error task keys are a list
 await client.failures.retry('key');
 return [scalar,pair,counted,nothing,at,stop,refused];
}
void transactionShapes;
async function checkTransactionMutations(){
 const scripted=scriptedTransaction();
 const tx=new ApplicationTransaction(scripted.port);
 let context:CompanionContext|undefined;
 let seen:Composition|null=null;
 const call:Call<PublishEntryOutput>=await tx.mutations.publishEntry({entry:row,composition:compositionId},{store:{published:false},local:async local=>{
  context=local;
  seen=await local.models.composition.get({id:compositionId});
  await local.models.composition.delete({id:compositionId});
 }});
 assert.deepEqual(seen,compositionRow,'the callback reads through its own port');
 assert.deepEqual(scripted.companions,[{model:'Composition',op:'delete',identity:{id:compositionId}}]);
 assert.deepEqual(scripted.outer,[],'companion writes never use the outer transaction port');
 const [publish]=scripted.submitted;
 assert.deepEqual({name:publish!.name,version:publish!.version},{name:'PublishEntry',version:1});
 assert.deepEqual(publish!.args,{entry:{...encodeEntry(row)},composition:compositionId},'business args only');
 assert.deepEqual(Object.keys(publish!.options!).sort(),['local','store']);
 assert.deepEqual(publish!.options!.store,{published:false});
 const outcome=await call.wait();
 assert.ok(outcome.result?.published.at instanceof Date,'the Call decodes the declared output');
 // Without a callback nothing but the store policy is passed.
 const renamed:Call<void>=await tx.mutations.rename({id:compositionId,title:'x'});
 assert.equal((await renamed.wait()).result,undefined);
 assert.equal(scripted.submitted[1]!.options,undefined);
 await tx.mutations.rename({id:compositionId,title:'y'},{store:false});
 assert.deepEqual(scripted.submitted[2]!.options,{store:false});
 assert.deepEqual(scripted.submitted[2]!.args,{id:compositionId,title:'y'});
 // Type hiding is backed by absent members at runtime.
 assert.equal('call' in tx.mutations,false,'no direct route in a transaction');
 assert.deepEqual(Object.keys(tx.mutations).sort(),['publishEntry','rename']);
 assert.ok(context instanceof CompanionContext);
 for(const member of ['mutations','scopes','transaction','savepoint'])assert.equal(member in context,false,member);
 assert.equal('watch' in context.models.composition,false);
 await tx.models.composition.delete({id:compositionId});
 assert.deepEqual(scripted.outer,[{model:'Composition',op:'delete',identity:{id:compositionId}}],'ordinary writes stay independent');
 // A port that only submits is enough for the Mutation facade; the
 // resolutions of unsent work are the application transaction's alone.
 const submitOnly:SubmitMutationPort={submitMutation:(name,version,args,decode,options)=>scripted.port.submitMutation(name,version,args,decode,options)};
 await makeTransactionMutations(submitOnly).rename({id:compositionId,title:'z'});
 // Resolutions of unsent work reach the raw transaction unchanged.
 await tx.failures.drop(3);
 await tx.failures.retry(['k']);
 await tx.rejections.dismiss(4);
 assert.deepEqual(scripted.resolutions,[{drop:3},{retry:['k']},{dismiss:4}]);
 // The generated client passes the application facade to `transaction` and
 // the local-only facade to onStore.
 let hooks:Record<string,(tx:unknown,changes:unknown[])=>void|Promise<void>>|undefined;
 let storeTx:unknown;
 rawClass.open=async options=>{hooks=options.onStore;return {async close(){},transaction:(body:(raw:unknown)=>Promise<unknown>)=>body(scripted.port)};};
 try{
  const adapted=await GeneratedClient.open({path:'unused-for-captured-adapter',onStore:{composition:(tx)=>{storeTx=tx;}}});
  try{
   const returned=await adapted.transaction(async tx=>{
    assert.ok(tx instanceof ApplicationTransaction);
    const first=await tx.mutations.rename({id:compositionId,title:'1'});
    const second=await tx.mutations.rename({id:compositionId,title:'2'});
    return {first,second,label:'value'};
   });
   assert.equal(returned.label,'value','the callback value is returned unchanged');
   assert.equal(scripted.submitted.length,6);
   await hooks!.Composition!(scripted.port,[]);
   assert.ok(storeTx instanceof GeneratedTransaction);
   assert.equal(storeTx instanceof ApplicationTransaction,false);
   assert.equal('mutations' in (storeTx as object),false,'onStore queues no Mutation');
  }finally{await adapted.close();}
 }finally{rawClass.open=originalRawOpen;}
}
await checkTransactionMutations();
