import 'generated.dart';
Future<void> typedForms(GeneratedClient client) async {
  final input=PublishInput(entry:EntryCreate(id:'e',text:'hello'),call:'legal field');
  final call=await client.mutations.publish(input);
  await call.wait();
  await client.mutations.publish.withTransaction((tx) async {
    await tx.models.draft.delete(const DraftIdentity(id:'d'));
    return input;
  });
  await client.transaction((tx) async { await tx.mutations.publish(input); });
  await client.queries.find(id:'e',store:false);
}
