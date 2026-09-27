import 'generated.dart';
import 'model_only/generated.dart' as model_only;
import 'model_free/generated.dart' as model_free;

Future<void> invalid(
  GeneratedClient client,
  Call<EchoOutput> call,
) async {
  await client.mutations.echo(at: 'string', moods: [Mood.calm], maybe: null);
  await client.mutations.echo(
    at: DateTime.utc(2026),
    moods: ['calm'],
    maybe: null,
  );
  await client.mutations.touch(
    note: Note(id: 'n', at: DateTime.utc(2026), mood: Mood.calm, label: null),
    changed: TouchChangedUpdate(id: 'n', mood: Present(Mood.loud)),
  );
  await client.transaction((tx) async {
    tx.models.note.watch();
  });
  call.result;
  await client.actions.ping();
  final Call<NowOutput> direct = await client.queries.now(at: DateTime.utc(2026));
  await client.transaction((tx) async {
    tx.queries;
  });
  direct.hashCode;
}

// A schema without Loads has no `loads` facade.
void noLoads(model_only.GeneratedClient only, model_free.GeneratedClient free) {
  only.loads;
  free.loads;
}

// Only a schema with a current Mutation queues Mutations in a transaction,
// and only its current Mutations (Clock v2 is a Query); generated_test.dart
// uses the model-free facade's `ping`.
Future<void> transactionScope(model_only.GeneratedClient only, model_free.GeneratedClient free) async {
  await only.transaction((tx) async => tx.mutations);
  await free.transaction((tx) async => tx.mutations.clock);
}
