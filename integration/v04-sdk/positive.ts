import type {GeneratedClient, PublishInput} from './client.ts';
async function typed(client:GeneratedClient,input:PublishInput) {
  const call=await client.mutations.publish(input);
  await call.wait();
  await client.mutations.publish(async tx => { await tx.models.draft.delete({id:'d'}); return input; });
  await client.transaction(async tx => { await tx.mutations.publish(input); });
  await client.queries.find({id:'e'},{store:false,once:true,refresh:true});
  // @ts-expect-error Mutation options were retired
  await client.mutations.publish(input,{local:async()=>{}});
  // @ts-expect-error no direct Mutation lane
  await client.mutations.call.publish(input);
  // @ts-expect-error no durable Query lane
  await client.queries.enqueue.find({id:'e'});
  // @ts-expect-error only one request boolean
  await client.queries.find({id:'e'},{store:{entry:true}});
  // @ts-expect-error no anonymous remote write
  await client.mutate.entry.create({id:'e',text:'x'});
  // @ts-expect-error no multi Stream subscription
  await client.streams.subscribe('other');
  // @ts-expect-error callback returns exact typed Input
  await client.mutations.publish(async()=>({call:42}));
}
