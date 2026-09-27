import 'dart:async';

import 'generated.dart';

const todo = Todo(id: 't', title: 'Task', state: Status.open, note: null);
const identity = TodoIdentity(id: 't');
const created = TodoCreate(id: 't', title: 'Task', state: Status.open, note: null);
const changed = AddTodoPatchUpdate(id: 't', title: Present('Renamed'));
const clearNote = TodoPatch(note: Present<String?>(null));
const deleted = TodoDelete(id: 't');
const composite = ProjectIdentity(tenantId: 'tenant', id: 'project');
const oldInput = AddTodoV1Input(
  todo: AddTodoV1TodoCreate(id: 'old', title: 'Old', state: AddTodoV1Status.closed),
  gone: [], status: AddTodoV1Status.open, tags: [],
);
const oldOutput = AddTodoV1HandlerOutput(
  relatedTodo: TodoV1Identity(id: 'old'), matches: [], count: 1,
);
const handlerOutput = AddTodoHandlerOutput(
  relatedTodo: TodoIdentity(id: 't'), matches: [TodoIdentity(id: 't')],
  count: 1, state: null,
);
const linkOutput = LinkHandlerOutput(
  relatedProject: ProjectIdentity(tenantId: 'tenant', id: 'project'),
);
const stateListOutput = StateListHandlerOutput(states: [Status.open, Status.closed, Status.archived]);
const oldStateListOutput = StateListV1HandlerOutput(states: [StateListV1OutputStatus.open, StateListV1OutputStatus.closed]);
const getTodosOutput = GetTodosHandlerOutput(todos: [TodoIdentity(id: 't')]);
const findOutput = FindTodosHandlerOutput(todos: [TodoIdentity(id: 't')], nextCursor: null);

Future<AddTodoHandlerOutput> handle(
  MutationHandlerCall<Object, AddTodoInput> call,
) async {
  final Object context = call.ctx;
  // Handlers receive complete records, whatever the client omitted.
  final Todo input = call.args.todo;
  final AddTodoPatchUpdate? patch = call.args.patch;
  context.hashCode;
  input.id;
  patch?.title;
  return handlerOutput;
}

Future<FindTodosHandlerOutput> find(
  QueryHandlerCall<Object, FindTodosInput> call,
) async {
  final String text = call.args.text;
  final String? cursor = call.args.cursor;
  text.length;
  cursor?.length;
  return findOutput;
}

// GetTodos retains v1 as a Mutation and v2 as a Query.
abstract class GetTodosContracts
    implements MutationGetTodosHandlers<Object>, QueryGetTodosHandlers<Object> {}

Future<void> useClient(GeneratedClient client) async {
  await client.models.todo.create(todo);
  await client.models.todo.update(identity, const TodoPatch(title: Present('New')));
  await client.models.todo.update(identity, clearNote);
  await client.models.todo.delete(identity);
  final Todo? found = await client.models.todo.get(identity);
  final List<Todo> rows = await client.models.todo.query();
  final Stream<List<Todo>> stream = client.models.todo.watch();
  await client.transaction((tx) async {
    await tx.models.todo.create(todo);
    await tx.models.todo.update(identity, const TodoPatch(title: Present('Local')));
    await tx.models.todo.delete(identity);
    await tx.models.todo.get(identity);
    await tx.models.todo.query();
  });
  // Mutations: durable by default, direct under `call`.
  final Call<AddTodoOutput> action = await client.mutations.addTodo(
    todo: created, patch: changed, gone: [deleted], status: null, tags: [],
  );
  final CallStatus status = action.status;
  final CallOutcome<AddTodoOutput> outcome = await action.wait();
  if (outcome is CallSuccess<AddTodoOutput>) {
    // Operands are targets, not results: only explicit outputs are present.
    final Todo? related = outcome.result.relatedTodo;
    final List<Todo> matches = outcome.result.matches;
    related?.id;
    matches.length;
  } else if (outcome is CallFailure<AddTodoOutput>) {
    final String code = outcome.error.code;
    code.length;
  }
  final AddTodoOutput direct = await client.mutations.call.addTodo(
    todo: created, gone: [], status: Status.open, tags: [],
  );
  // A delete operand has no implicit result.
  await client.mutations.call.removeTodo(todo: deleted);
  // Explicit results (#140): Edit returns no business value; EditAndRead
  // returns the loaded Todo its handler selected, independent of the input.
  await client.mutations.call.edit(todo: const EditTodoUpdate(id: 'A', title: Present('Renamed')));
  final Call<EditOutput> editCall = await client.mutations.edit(todo: const EditTodoUpdate(id: 'A'));
  final EditAndReadOutput read = await client.mutations.call.editAndRead(
    todo: const EditAndReadTodoUpdate(id: 'A', state: Present(Status.closed)),
    store: const EditAndReadStore.outputs(todo: false),
  );
  final String readTitle = read.todo.title;
  await client.mutations.call.ping();
  final Call<void> searched = await client.mutations.search(query: null);
  await client.mutations.call.search(query: 'term');
  await client.mutations.call.deleteTodo(todo: deleted);
  await client.mutations.sendEmail(to: 'team@example.test', subject: 'Todo', body: 'Created');
  // Queries: direct by default, durable under `enqueue`.
  final GetTodosOutput todos = await client.queries.getTodos();
  final FindTodosOutput page = await client.queries.findTodos(text: 'design', cursor: null);
  final String? next = page.nextCursor;
  final Call<FindTodosOutput> queued =
      await client.queries.enqueue.findTodos(text: 'design', cursor: next);
  final CallOutcome<FindTodosOutput> queuedOutcome = await queued.wait();
  // store selectors ride beside args on every route; results keep their types.
  final GetTodosOutput unstored =
      await client.queries.getTodos(store: const GetTodosStore.none());
  await client.queries.enqueue.getTodos(store: const GetTodosStore.outputs(todos: false));
  await client.queries.findTodos(text: 'x', cursor: null, store: const FindTodosStore.outputs(todos: false));
  await client.mutations.call.ping(store: const PingStore.none());
  await client.mutations.ping(store: const PingStore.all());
  // once reuses a saved complete result; refresh replaces it; invalidate discards it.
  final FindTodosOutput cached = await client.queries.findTodos(text: 'x', cursor: null, once: true);
  await client.queries.findTodos(text: 'x', cursor: null, once: true, refresh: true, store: const FindTodosStore.none());
  await client.queries.getTodos(once: true);
  await client.queries.invalidate.findTodos(text: 'x', cursor: null);
  await client.queries.invalidate.getTodos();
  cached.hashCode;
  // A business input named store keeps its name; the selector is outputStore.
  final OpenTodoOutput opened = await client.mutations.call.openTodo(
    store: 'business',
    outputStore: const OpenTodoStore.outputs(suggestions: false),
  );
  final Call<OpenTodoOutput> openCall = await client.mutations.openTodo(
    store: null,
    outputStore: const OpenTodoStore.all(),
  );
  searched.status;
  queuedOutcome.hashCode;
  unstored.todos.length;
  opened.suggestions.length;
  openCall.status;
  final List<Todo> selected = todos.todos;
  found?.id;
  rows.length;
  stream.hashCode;
  status.name;
  direct.count;
  editCall.status;
  readTitle.length;
  selected.length;
  // Creation defaults (#27): create inputs may omit defaulted fields; a
  // defaulted nullable field uses Present to send an explicit null.
  await client.models.note.create(const NoteCreate(memo: null));
  await client.models.note.create(const NoteCreate(memo: 'm', tag: Present(null), pinned: true));
  await client.models.note.create(Note(id: 'full', body: 'b', pinned: false, at: DateTime.utc(2026), tag: null, memo: null));
  await client.transaction((tx) => tx.models.note.create(const NoteCreate(memo: null)));
  final AddNotesOutput saved = await client.mutations.call.addNotes(
    note: const NoteCreate(memo: null),
    many: [const NoteCreate(memo: 'x'), todoNote],
  );
  saved.saved.at;
  await client.mutations.addNotes(note: const NoteCreate(memo: null), maybe: null, many: const []);
}

final todoNote = Note(id: 'n', body: '', pinned: false, at: DateTime.utc(2026), tag: 't', memo: null);

Future<AddNotesHandlerOutput> handleNotes(
  MutationHandlerCall<Object, AddNotesInput> call,
) async {
  final Note note = call.args.note;
  final String id = note.id;
  final DateTime at = note.at;
  final List<Note> many = call.args.many;
  at.hashCode;
  many.length;
  return AddNotesHandlerOutput(saved: NoteIdentity(id: id));
}

Future<EditHandlerOutput> handleEdit(
  MutationHandlerCall<Object, EditInput> call,
) async {
  call.args.todo.id;
}

Future<EditAndReadHandlerOutput> handleEditAndRead(
  MutationHandlerCall<Object, EditAndReadInput> call,
) async {
  call.args.todo.id;
  // The handler supplies the output identity; it is never taken from the input.
  return const EditAndReadHandlerOutput(todo: TodoIdentity(id: 'B'));
}

// Client Loads (#173): business inputs are named parameters; `once` and
// `refresh` are call-site options, `callOnce`/`callRefresh` beside inputs
// that already take those names.
Future<void> useLoads(GeneratedClient client) async {
  final Load job = await client.loads.projectTodos(projectId: 'p', status: Status.open, tags: ['a'], once: true);
  final Load refreshed = await client.loads.projectTodos(projectId: 'p', status: null, tags: [], once: true, refresh: true);
  final Load ordinary = await client.loads.projectTodos(projectId: 'p', status: null, tags: []);
  final Load noArgs = await client.loads.recentTodos(once: true);
  final Load flagged = await client.loads.flaggedTodos(once: true, refresh: 'yes', callOnce: true, callRefresh: false);
  // An input named client still reaches the generated route's client field.
  final Load byClient = await client.loads.clientTodos(client: 'c', once: true);
  final LoadStatus status = job.status;
  final String name = status.name;
  final LoadPhase phase = status.phase;
  final int pages = status.pages;
  final LoadException? error = status.error;
  final StreamSubscription<LoadStatus> observer = job.watch().listen((next) => next.phase);
  await observer.cancel();
  await job.wait();
  await job.cancel();
  await job.retry();
  await job.forget();
  job.dispose();
  final Load? restored = await client.loads.get(job.id);
  final List<LoadStatus> recent = await client.loads.list(limit: 10);
  final List<LoadStatus> defaults = await client.loads.list();
  await client.loads.invalidate.projectTodos(projectId: 'p', status: null, tags: []);
  await client.loads.invalidate.recentTodos();
  await client.loads.invalidate.flaggedTodos(once: false, refresh: 'no');
  await client.loads.invalidate.clientTodos(client: 'c');
  try {
    await job.wait();
  } on LoadException catch (thrown) {
    thrown.code;
    thrown.message;
  }
  [refreshed, ordinary, noArgs, flagged, byClient, name, phase, pages, error, restored, recent, defaults];
}

void main() {
  useLoads;
  composite.id;
  oldInput.todo.id;
  oldOutput.count;
  linkOutput.relatedProject?.id;
  getTodosOutput.todos.length;
  stateListOutput.states.length;
  oldStateListOutput.states.length;
  handle;
  handleNotes;
  handleEdit;
  handleEditAndRead;
  find;
  useClient;
}
