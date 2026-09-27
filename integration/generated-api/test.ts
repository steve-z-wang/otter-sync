import {Book,Comment,Entry as EntryRef,type Handlers,type Loaders,type EntryV1,type MutationContext,type QueryContext,type HandlerCall,type TransactionCall,type AddBookInput} from './backend.ts';
import {strict as assert} from 'node:assert';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {GeneratedClient,type BootstrapPhase,type BootstrapStatus,type Subscription,type SubscriptionStatus} from './client.ts';
import type {Transaction as RawTransaction} from '../../packages/client-js/index.mts';
import {CreateEntry,EditEntry,RemoveEntries,decodeEntry,encodeEntry,EntryModel,EntryLiveModel,GeneratedTransaction,Mutate,type Entry,type ReadPort,type LivePort,type WritePort,type MutationName,type SyncState} from './generated.ts';
const row:Entry={id:'123e4567-e89b-42d3-a456-426614174000',title:'hello',note:null,at:new Date('2026-01-01T00:00:00Z'),tags:['x'],status:'active'};
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
 const shorthand:Handlers<Tx>['addBook']=async({input,tx,channel,touch})=>{tx.rows.set(input.book.id,input.book);touch.book(input.book);channel('c').book.add(input.book)};
 const grouped:Handlers<Tx>['editEntry']={
  async v1({input,channel}){channel('c').entry.add(input.target.identity)},
  // Legacy slot handlers declare through the same handles; mixed lists take explicit references and may be empty.
  async v2({input,channel,touch}){touch.entry(input.entry.identity);channel('c').add([EntryRef(input.entry.identity),Book({id:'b'})]);channel('audit').remove([])},
  // @ts-expect-error v3 is not a retained version of EditEntry
  async v3(){},
 };
 // @ts-expect-error a mutation with two retained versions cannot register a bare function
 const bare:Handlers<Tx>['editEntry']=async()=>{};
 // @ts-expect-error every retained version must be registered
 const partial:Handlers<Tx>['editEntry']={v2:async({input,channel})=>{channel('c').entry.add(input.entry.identity)}};
 // @ts-expect-error handlers declare through `channel` and `touch`; there is no notify and no return value
 const legacy:Handlers<Tx>['addBook']=async({notify})=>{notify({channel:'c',records:[]})};
 // @ts-expect-error the old publish API is gone
 const published:Handlers<Tx>['addBook']=async({publish})=>{publish({channel:'c'})};
 // @ts-expect-error the old changes collector is gone
 const changed:Handlers<Tx>['addBook']=async({changes})=>{changes.add({model:'Book',identity:{id:'b'}})};

 // The generated declaration API, per schema: resource before verb.
 const declare=(ctx:MutationContext<Tx>,queryCtx:QueryContext<Tx>,call:HandlerCall<Tx,AddBookInput>,external:TransactionCall<Tx>)=>{
  ctx.channel('project:1').book.add({id:'A'});
  ctx.channel('project:1').book.remove({id:'A'});
  ctx.touch.book({id:'A'});
  // @ts-expect-error missing identity
  ctx.channel('project:1').book.add({});
  // @ts-expect-error old API is gone
  ctx.publish({channel:'project:1'});
  // @ts-expect-error Query has no membership writer
  queryCtx.channel('project:1').book.add({id:'A'});
  // @ts-expect-error Query has no change declaration
  queryCtx.touch.book({id:'A'});
  // A Channel handle is an ordinary value; every Model is a property beside add and remove.
  const project=ctx.channel('project:1');
  project.add([Book({id:'A'}),Comment({id:'c'}),EntryRef({id:row.id})]);
  project.comment.remove({id:'c'});
  call.channel('project:1').entry.add({id:row.id});
  call.touch.counter({id:'n'});
  external.channel('project:1').remove([Book({id:'A'})]);
  external.touch.draft({id:row.id});
  // @ts-expect-error a raw identity names no Model
  project.add([{id:'A'}]);
  // @ts-expect-error a UUID identity is a string
  external.touch.entry({id:1});
  // @ts-expect-error the Channel's mixed verbs take references, not identities
  project.remove({id:'A'});
 };
 // @ts-expect-error a handler has no return value to select a channel with
 const returned:Handlers<Tx>['addBook']=async()=>({channel:'c'});
 // @ts-expect-error loaders receive no channel
 const channelled:Loaders<Tx>['book']=async({ids,channel})=>ids.map(id=>({...id,title:String(channel)}));

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
// one handle per registration, typed handle members, and the retained `channels`
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
 // The retained spelling is the same ledger path, not a second algorithm: with
 // no server it registers durable intent that has no boundary yet.
 const retained:Subscription=await client.channels.subscribe('project:456');
 assert.equal(retained.status.initialization,'pending','channels registers through the same ledger');
 assert.equal(await client.channels.subscribe('project:456'),retained,'and shares one handle per registration');
 const removal:Promise<void>=client.channels.unsubscribe('project:456');
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
const {WebSocketServer}=createRequire(import.meta.url)('../../packages/server/node_modules/ws') as
 {WebSocketServer:new(options:{server:unknown})=>FakeServer};
const bootstrapDirectory=await mkdtemp(join(tmpdir(),'generated-bootstrap-'));
const loads:{after:number;until:number}[]=[];
let release=()=>{};
const held=new Promise<void>(resolve=>{release=resolve;});
const http=createServer(async(request,response)=>{
 const chunks:Buffer[]=[];for await(const chunk of request)chunks.push(chunk as Buffer);
 const body=JSON.parse(Buffer.concat(chunks).toString()) as {mode?:string;channel:string;after:number;until:number};
 // Only a bootstrap page is expected here, and the test transport holds it.
 loads.push({after:body.after,until:body.until});
 await held;
 response.end(JSON.stringify({mode:'bootstrap',channel:body.channel,from:body.after,to:body.until,until:body.until,head:body.until,records:[]}));
});
await new Promise<void>(resolve=>http.listen(0,'127.0.0.1',()=>resolve()));
const sockets=new WebSocketServer({server:http});
sockets.on('connection',socket=>{
 socket.on('message',message=>{
  const subscribe=JSON.parse(String(message)) as {channels:string[]};
  socket.send(JSON.stringify({type:'subscribed',cursors:Object.fromEntries(subscribe.channels.map(channel=>[channel,0]))}));
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
