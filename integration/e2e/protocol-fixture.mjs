// Internal wire fixture for ACK-loss/transaction tests. Applications use Client.connect(server).
/** The read contracts a client of `schema` declares: every model at its version. */
export const declaredModels=schema=>Object.fromEntries(schema.models.map(model=>[model.name,model.version??1]));
export async function syncProtocol(client, transport, models) {
 let caughtUp=false;
 for (;;) {
  const frozen=await client.freeze();
  if(frozen!==null) {
   await client.acknowledge(JSON.parse(frozen).batchSequence,JSON.parse(await transport('push',frozen)));
   caughtUp=false;
   continue;
  }
  if(caughtUp)return;
  const status=await client.syncState();
  // Only an initialized subscription has a delivery position to pull from: a
  // registration still waiting for its first acknowledged head is in `channels`
  // and not in `cursors`, and a null boundary is never read as zero (#150).
  const cursors={...status.cursors};
  if(Object.keys(cursors).length===0)return;
  // One pull covers every initialized channel; it repeats while any channel continues.
  const page=JSON.parse(await transport('pull',JSON.stringify({capabilities:["channel-membership-v1"],cursors,models})));await client.applyPull(page);
  caughtUp=Object.values(page.cursors).every(range=>range.to>=range.head);
 }
}
