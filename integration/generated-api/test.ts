import {Book,Comment,Entry as EntryRef,type Handlers,type Loaders,type EntryV1,type MutationContext,type QueryContext,type HandlerCall,type TransactionCall,type AddBookInput} from './backend.ts';
import {strict as assert} from 'node:assert';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {GeneratedClient,CallError,type Call,type BootstrapPhase,type BootstrapStatus,type RefusedAct,type FailedAct,type FailedTask,type SubmittedAct} from './client.ts';
import type {EntryIdentity, Placement, Status, Composition, PublishEntryOutput, RenameOutput, SubmitMutationPort, UnsentResolutionPort} from './generated.ts';
import {ApplicationTransaction,CompanionContext,makeTransactionMutations} from './generated.ts';
import type {Transaction as RawTransaction} from '../../packages/client-js/index.mts';
import {Client as RawClient} from '../../packages/client-js/index.mts';
import {CreateEntry,EditEntry,RemoveEntries,decodeEntry,encodeEntry,EntryModel,EntryLiveModel,GeneratedTransaction,type Entry,type ReadPort,type LivePort,type WritePort,type MutationName,type SyncState} from './generated.ts';
const row:Entry={id:'123e4567-e89b-42d3-a456-426614174000',title:'hello',note:null,at:new Date('2026-01-01T00:00:00Z'),tags:['x'],status:'active'};
const shared: import('./generated.ts').EntryFields = row;
const identified: import('./generated.ts').IdentifiedFields = shared;
void identified;
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
 // @ts-expect-error anonymous remote mutation facade is retired
 void ({} as GeneratedClient).mutate;
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
 const shorthand:Handlers<Tx>['addBook']=async({input,tx,streams: scope,invalidate:touch})=>{tx.rows.set(input.book.id,input.book);touch.book(input.book);scope(['c']).track.book(input.book)};
 const grouped:Handlers<Tx>['editEntry']={
  async v1({input,streams: scope}){scope(['c']).track.entry(input.target.identity)},
  // Legacy slot handlers declare through the same handles; mixed lists take explicit references and may be empty.
  async v2({input,streams: scope,invalidate:touch}){touch.entry(input.entry.identity);scope(['c']).track([EntryRef(input.entry.identity),Book({id:'b'})]);scope(['audit']).invalidate([])},
  // @ts-expect-error v3 is not a retained version of EditEntry
  async v3(){},
 };
 // @ts-expect-error a mutation with two retained versions cannot register a bare function
 const bare:Handlers<Tx>['editEntry']=async()=>{};
 // @ts-expect-error every retained version must be registered
 const partial:Handlers<Tx>['editEntry']={v2:async({input,streams: scope})=>{scope(['c']).track.entry(input.entry.identity)}};
 // @ts-expect-error handlers declare through `scope` and `touch`; there is no notify and no return value
 const legacy:Handlers<Tx>['addBook']=async({notify})=>{notify({scope:'c',records:[]})};
 // @ts-expect-error the old publish API is gone
 const published:Handlers<Tx>['addBook']=async({publish})=>{publish({scope:'c'})};
 // @ts-expect-error the old changes collector is gone
 const changed:Handlers<Tx>['addBook']=async({changes})=>{changes.add({model:'Book',identity:{id:'b'}})};

 // The generated declaration API, per schema: operation before Model.
 const declare=(ctx:MutationContext<Tx>,queryCtx:QueryContext<Tx>,call:HandlerCall<Tx,AddBookInput>,external:TransactionCall<Tx>)=>{
  ctx.streams(['project:1']).track.book({id:'A'});
  ctx.streams(['project:1']).invalidate.book({id:'A'});
  ctx.invalidate.book({id:'A'});
  // @ts-expect-error missing identity
  ctx.streams(['project:1']).track.book({});
  // @ts-expect-error old API is gone
  ctx.publish({scope:'project:1'});
  queryCtx.stream.track.book({id:'A'});
  queryCtx.streams(['project:1']).track.book({id:'A'});
  // @ts-expect-error Query track-only handle cannot invalidate
  queryCtx.stream.invalidate.book({id:'A'});
  // @ts-expect-error Query has no change declaration
  queryCtx.invalidate.book({id:'A'});
  // A Scope handle exposes each Model under its operation namespaces.
  const project=ctx.streams(['project:1']);
  project.track([Book({id:'A'}),Comment({id:'c'}),EntryRef({id:row.id})]);
  project.invalidate.comment({id:'c'});
  call.streams(['project:1']).track.entry({id:row.id});
  call.invalidate.counter({id:'n'});
  external.streams(['project:1']).invalidate([Book({id:'A'})]);
  external.invalidate.draft({id:row.id});
  // @ts-expect-error a raw identity names no Model
  project.track([{id:'A'}]);
  // @ts-expect-error a UUID identity is a string
  external.invalidate.entry({id:1});
  // @ts-expect-error the Scope's mixed verbs take references, not identities
  project.invalidate({id:'A'});
  // Mixed, single and list operands, including the composite identity.
  ctx.streams(['project:1','audit']).track([Book({id:'A'}),EntryRef({id:row.id})]);
  project.track.book([{id:'A'}]);
  ctx.streams(['project:1']).track(Book({id:'A'}));
  ctx.invalidate([Book({id:'A'}),Comment({id:'c'})]);
  // @ts-expect-error tags are retired
  project.track.book({id:'A'}).tag('X');
  // @ts-expect-error selectors are retired
  project.where({ tags: { all: ['X'] } });
  // @ts-expect-error tracking has no public withdrawal
  project.remove(Book({id:'A'}));
 };
 // @ts-expect-error a handler has no return value to select a scope with
 const returned:Handlers<Tx>['addBook']=async()=>({scope:'c'});
 // @ts-expect-error loaders receive no scope
 const scopeled:Loaders<Tx>['book']=async({ids,streams: scope})=>ids.map(id=>({...id,title:String(scope)}));
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
check((await tx.models.entry.get({id:row.id}))?.at instanceof Date,'read decode');
check((await tx.models.entry.query()).length===1,'query facade');await tx.models.entry.delete({id:row.id});
// The generated adapter encodes typed input/callback through one named port.
const submissions:{name:string;version:number;args:object}[]=[];
const registry=new (await import('../../packages/client-js/actions.mts')).ActionRegistry();
const port:SubmitMutationPort={async submitMutation(name,version,input,decode){const args=typeof input==='function'?await input({...reads,async direct(){}}):input;submissions.push({name,version,args});return registry.register(String(submissions.length),decode);}};
const mutations=makeTransactionMutations(port);
const directCall:Call<PublishEntryOutput>=await mutations.publishEntry({entry:row,composition:row.id});
const callbackCall:Call<PublishEntryOutput>=await mutations.publishEntry(async scope=>{const local:Composition|null=await scope.models.composition.get({id:row.id});void local;return {entry:row,composition:row.id};});
check(directCall.status==='pending'&&callbackCall.status==='pending','generated adapter does not invent settlement');
assert.equal(submissions.length,2);assert.deepEqual(submissions[0],submissions[1]);
assert.equal((submissions[0]!.args as {entry:{at:string}}).entry.at,'2026-01-01T00:00:00.000Z');
if(false){
 const client={} as GeneratedClient;
 void GeneratedClient.open({path:'s',stream:'User:viewer'});
 // @ts-expect-error removed Store identity
 void GeneratedClient.open({path:'s',stream:'User:u',connection:{url:'http://unused',token:'x',identity:{backend:'b',viewer:'u',contract:'c'}}});
 // @ts-expect-error no Query invalidation facade
 void client.queries.invalidate;
 void client.fetch.entry({id:row.id});
 void client.fetch.entry({id:row.id},{store:false});
 void client.queries.readEntry({id:row.id});
 void client.queries.readEntry({id:row.id},{store:false});
 // @ts-expect-error removed Query once option
 void client.queries.readEntry({id:row.id},{once:false});
 // @ts-expect-error removed Query refresh option
 void client.queries.readEntry({id:row.id},{refresh:false});
 // @ts-expect-error open requires a stream
 void GeneratedClient.open({path:'only.sqlite'});
 // @ts-expect-error retired public hook configuration
 void GeneratedClient.open({path:'s',stream:'User:viewer',connection:{url:'http://unused',token:'x'},onStore:{}});
 // @ts-expect-error no per-client multiple Stream facade
 void client.streams;
 // @ts-expect-error no Load jobs
 void client.loads;
 // @ts-expect-error no split direct Mutation lane
 void client.mutations.call;
 // @ts-expect-error no callback options; callback returns input
 void client.mutations.rename({id:row.id,title:'new'},{local:async()=>{}});
 const application={} as ApplicationTransaction;
 void application.mutations.rename(async local=>{await local.models.composition.delete({id:row.id});return {id:row.id,title:'new'};});
 // @ts-expect-error no Query inside a transaction
 void application.queries;
 // @ts-expect-error callback must return typed input
 void client.mutations.rename(async()=>({id:row.id,title:123}));
}
