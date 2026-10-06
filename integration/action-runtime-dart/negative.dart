import 'generated.dart';
import 'model_only/generated.dart' as model_only;
import 'model_free/generated.dart' as model_free;

Future<void> invalid(GeneratedClient client, Call<EchoOutput> call) async {
  await client.mutations.echo(EchoInput(at: 'string', moods: [Mood.calm], maybe: null)); // error: argument_type_not_assignable
  await client.mutations.echo(EchoInput(at: DateTime.utc(2026), moods: ['calm'], maybe: null)); // error: list_element_type_not_assignable
  TouchChangedUpdate(id: 'n', mood: Present(Mood.loud)); // error: undefined_named_parameter
  await client.transaction((tx) async { tx.models.note.watch(); }); // error: undefined_method
  call.result; // error: undefined_getter
  client.actions; // error: undefined_getter
  final Call<NowOutput> direct = await client.queries.now(at: DateTime.utc(2026)); // error: invalid_assignment
  await client.transaction((tx) async { tx.queries; }); // error: undefined_getter
  client.mutations.call; // error: undefined_getter
  client.queries.enqueue; // error: undefined_getter
  client.mutations.echo((tx) async => EchoInput(at: DateTime.utc(2026), moods: [], maybe: null)); // error: argument_type_not_assignable
  await client.mutations.echo.withTransaction((tx) async { tx.mutations; return EchoInput(at: DateTime.utc(2026), moods: [], maybe: null); }); // error: undefined_getter
  client.queries.now(at: DateTime.utc(2026), store: {'at': false}); // error: argument_type_not_assignable
  direct.hashCode;
}
void noRetiredManagers(model_only.GeneratedClient only, model_free.GeneratedClient free) {
  only.loads; // error: undefined_getter
  free.loads; // error: undefined_getter
  only.streams; // error: undefined_getter
}
Future<void> transactionScope(model_only.GeneratedClient only, model_free.GeneratedClient free) async {
  await only.transaction((tx) async => tx.mutations); // error: undefined_getter
  await free.transaction((tx) async => tx.mutations.clock); // error: undefined_getter
}
