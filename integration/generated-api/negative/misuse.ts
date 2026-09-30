// Generated TypeScript API misuse that must NOT compile. `tsc -p
// integration/generated-api` type-checks this file, and every
// `@ts-expect-error` below has to be the error it names; the Dart twin is
// misuse.dart ([#150](https://github.com/zanminwang/axton/issues/150)).
import type {GeneratedClient, GeneratedTransaction, Subscription, Draft, DraftCreate, AddDraftArgs, StoreHooks, Entry, ApplicationTransaction, CompanionContext, PublishEntryOutput} from '../client.ts';

const badHooks:StoreHooks={
 entry:async(tx,changes)=>{
  const change=changes[0]!;
  if(change.kind==='upsert'){
  // @ts-expect-error the incoming row has no invented field
   void change.row.missing;
  }
  // @ts-expect-error onStore queues no Mutation: its transaction is local-only
  void tx.mutations;
  // @ts-expect-error Fetch is unavailable within the onStore transaction
  void tx.fetch;
  // @ts-expect-error onStore subscribes locally; it has no server Channel enrollment
  tx.channel('c').entry.add({id:'e'});
 },
 // @ts-expect-error unknown Models cannot register hooks
 unknown:async()=>{},
};
void badHooks;

export function scopeMisuse(client:GeneratedClient,subscription:Subscription){
 // @ts-expect-error a status snapshot is immutable
 subscription.status.active=false;
 // @ts-expect-error the Scope a handle names is fixed for its lifetime
 subscription.scope='other';
 // @ts-expect-error the first Scope API deliberately omits a get-only accessor
 void client.scopes.get('project:123');
 // The load status is part of that immutable snapshot, and this milestone
 // introduces no task-cancel or forced-refresh API
 // ([#151](https://github.com/zanminwang/axton/issues/151)).
 // @ts-expect-error a load status is immutable too
 subscription.status.bootstrap.phase='complete';
 // @ts-expect-error a registered task cannot be cancelled
 void subscription.bootstrap.cancel();
 // @ts-expect-error there is no forced refresh
 void subscription.refresh();
}

// Creation defaults (#27): only a create input may omit defaulted fields.
export function createMisuse(){
 // @ts-expect-error a field without a creation default is still required
 const missing:DraftCreate={};
 // @ts-expect-error the complete record keeps every field required
 const incomplete:Draft={body:'x',mood:'calm',created:new Date(),note:null,memo:null};
 // @ts-expect-error an omitted field is left out, not set to undefined
 const undef:DraftCreate={memo:null,body:undefined};
 const ok:AddDraftArgs={draft:{memo:null}};
 return [missing,incomplete,undef,ok];
}
// A schema without Loads generates no `loads` facade (#173).
export function loadMisuse(client:GeneratedClient){
 // @ts-expect-error no Load is declared, so there is no loads facade
 return client.loads;
}

// Model Fetch ([#153](https://github.com/zanminwang/axton/issues/153)): the
// generated identity and a boolean `store` are the whole input, and Fetch is
// absent from local transactions.
export async function fetchMisuse(client:GeneratedClient,tx:GeneratedTransaction,row:Entry,at:Date){
 // @ts-expect-error a composite identity needs every component
 await client.fetch.placement({shelf:'s'});
 // @ts-expect-error a DateTime identity component is a Date
 await client.fetch.placement({shelf:'s',at:'2026-01-01'});
 // @ts-expect-error a UUID identity is a string
 await client.fetch.entry({id:1});
 // @ts-expect-error an identity literal names only identity fields
 await client.fetch.entry({id:row.id,title:row.title});
 // @ts-expect-error there is no refresh option either
 await client.fetch.entry({id:row.id},{refresh:true});
 // @ts-expect-error storage is a boolean
 await client.fetch.entry({id:row.id},{store:'false'});
 // @ts-expect-error a Model the schema does not declare has no Fetch method
 await client.fetch.unknown({id:row.id});
 // @ts-expect-error Fetch is unavailable within a local transaction
 void tx.fetch;
 // @ts-expect-error the transaction port cannot fetch
 void tx.transaction.fetchModel;
 // The result is the snapshot or null, never a live handle.
 const result=await client.fetch.placement({shelf:'s',at});
 // @ts-expect-error the result may be null
 void result.label;
}

// Transactional Mutation enqueue: an application transaction queues typed
// Mutations only, `local` is a transaction-only option, and a Mutation's
// `local` callback has Models only. The runtime refuses the same misuse.
export async function transactionMisuse(client:GeneratedClient,tx:ApplicationTransaction,local:CompanionContext,row:Entry){
 const id=row.id;
 // @ts-expect-error there is no direct route inside a transaction
 await tx.mutations.call.rename({id,title:'t'});
 // @ts-expect-error Queries are unavailable inside a transaction
 void tx.queries;
 // @ts-expect-error Fetch is unavailable inside a transaction
 void tx.fetch;
 // @ts-expect-error a Mutation keeps its declared args
 await tx.mutations.rename({id,title:1});
 // @ts-expect-error `local` is an option, never a business arg
 await tx.mutations.rename({id,title:'t',local:async()=>{}});
 // @ts-expect-error the callback receives the companion context, not a raw port
 await tx.mutations.rename({id,title:'t'},{local:async(local:{direct(operation:object):Promise<void>;channels:object})=>{void local;}});
 // @ts-expect-error a store selection names only store-eligible outputs
 await tx.mutations.publishEntry({entry:row,composition:id},{store:{missing:false}});
 // @ts-expect-error the Call observes the declared output
 const wrong:Promise<import('../client.ts').Call<string>>=tx.mutations.publishEntry({entry:row,composition:id});
 // @ts-expect-error standalone Mutations take no local callback
 await client.mutations.rename({id,title:'t'},{local:async()=>{}});
 // @ts-expect-error direct Mutations take no local callback
 await client.mutations.call.rename({id,title:'t'},{local:async()=>{}});
 // @ts-expect-error the callback queues no Mutation
 void local.mutations;
 // @ts-expect-error the callback modifies no Channel
 void local.channels;
 // @ts-expect-error the callback opens no savepoint and exposes no raw port
 void local.transaction.savepoint;
 // @ts-expect-error the callback cannot watch
 local.models.composition.watch({},()=>{});
 // @ts-expect-error the callback reads no sync state
 void local.models.composition.syncState;
 await tx.mutations.publishEntry({entry:row,composition:id},{local:async inner=>{
  // @ts-expect-error nested enqueue is unavailable inside the callback
  await inner.mutations.rename({id,title:'t'});
 }});
 const call:PublishEntryOutput|undefined=(await (await tx.mutations.publishEntry({entry:row,composition:id})).wait()).result;
 return [wrong,call];
}
