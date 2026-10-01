import 'generated.dart';

const todo = Todo(id: 't', title: 'Task', state: Status.open, note: null);
const identity = TodoIdentity(id: 't');
const created = TodoCreate(id: 't', title: 'Task', state: Status.open, note: null);

Future<void> misuse(GeneratedClient client, Call<AddTodoOutput> call) async {
  await client.mutations.addTodo(todo: created, gone: [], tags: []); // required nullable status
  await client.mutations.addTodo(todo: created, gone: [], status: null, tags: [], status: Status.open); // duplicate shape
  await client.mutations.addTodo(todo: created, gone: [], status: null, tags: [], patch: TodoUpdate(id: 't')); // restricted shape
  await client.mutations.addTodo(todo: created, gone: [], status: null, tags: [], patch: AddTodoPatchUpdate(id: 't', state: Present(Status.open))); // invalid patch field
  await client.mutations.call.addTodo(todo: created, gone: [], status: null, tags: [null]); // non-null list member
  await client.transaction((tx) async {
    tx.mutations.call; // an application transaction queues Mutations; no direct route
    tx.models.todo.watch(); // watch excluded from transaction
  });
  call.result; // no framework result field
  call.error; // no framework error field
  call.subscribe; // no subscription API
  final AddTodoHandlerOutput scalarIdentity = AddTodoHandlerOutput(relatedTodo: 't', matches: [], count: 1, state: null);
  final AddTodoHandlerOutput fullModel = AddTodoHandlerOutput(relatedTodo: todo, matches: [], count: 1, state: null);
  final AddTodoHandlerOutput missingRelated = AddTodoHandlerOutput(matches: [], count: 1, state: null);
  final AddTodoHandlerOutput invalidList = AddTodoHandlerOutput(relatedTodo: identity, matches: [todo], count: 1, state: null);
  final AddTodoHandlerOutput missingCount = AddTodoHandlerOutput(relatedTodo: null, matches: [], state: null);
  final LinkHandlerOutput wrongComposite = LinkHandlerOutput(relatedProject: TodoIdentity(id: 't'));
  scalarIdentity.hashCode;
  fullModel.hashCode;
  missingRelated.hashCode;
  invalidList.hashCode;
  missingCount.hashCode;
  wrongComposite.hashCode;
}

final oldArchived = AddTodoV1Input(
  todo: AddTodoV1TodoCreate(id: 'old', title: 'Old', state: Status.archived),
  gone: [], status: AddTodoV1Status.open, tags: [],
);

Future<void> missingSearchQuery(GeneratedClient client) async {
  await client.mutations.search();
}

final stateListScalar = StateListHandlerOutput(states: Status.open);
final stateListInvalid = StateListHandlerOutput(states: [Status.invalid]);

final oldStateListScalar = StateListV1HandlerOutput(states: StateListV1OutputStatus.open);
final oldStateListArchived = StateListV1HandlerOutput(states: [StateListV1OutputStatus.archived]);

Future<void> storeMisuse(GeneratedClient client) async {
  await client.mutations.call.ping(store: const PingStore.outputs());
  await client.queries.getTodos(store: const OpenTodoStore.none());
  await client.mutations.call.openTodo(store: null, outputStore: const OpenTodoStore.outputs(count: false));
  await client.queries.getTodos(store: false);
}

Future<void> routeMisuse(GeneratedClient client) async {
  await client.actions.ping(); // retired namespace
  await client.mutations.findTodos(text: 'x', cursor: null); // a Query is not a Mutation
  await client.queries.addTodo(todo: created, gone: [], status: null, tags: []); // a Mutation is not a Query
  final FindTodosOutput queued = await client.queries.enqueue.findTodos(text: 'x', cursor: null); // durable returns Call
  final Call<FindTodosOutput> direct = await client.queries.findTodos(text: 'x', cursor: null); // direct returns output
  await client.queries.findTodos(text: 'x'); // nullable input is still required
  await client.transaction((tx) async {
    tx.queries; // Queries excluded from application transaction
  });
  queued.hashCode;
  direct.hashCode;
}

Future<void> onceMisuse(GeneratedClient client) async {
  await client.mutations.ping(once: true); // Mutations have no once control
  await client.mutations.call.ping(once: true); // direct Mutations have no once control
  await client.queries.enqueue.getTodos(once: true); // queued Queries have no once control
  await client.queries.enqueue.getTodos(refresh: true); // queued Queries have no refresh control
  await client.queries.invalidate.getTodos(store: const GetTodosStore.none()); // invalidation takes business args only
  await client.queries.invalidate.addTodo(todo: created, gone: [], status: null, tags: []); // only Queries are invalidated
  final GetTodosOutput nothing = await client.queries.invalidate.getTodos(); // invalidation returns no value
  nothing.hashCode;
}

Future<void> createMisuse(GeneratedClient client) async {
  await client.models.note.create(const NoteCreate()); // memo has no default
  await client.models.note.create(NoteCreate(memo: null, tag: 't')); // defaulted nullable needs Present
  final AddNotesInput args = AddNotesInput(note: const NoteCreate(memo: null), many: const []); // handler args are complete
  args.hashCode;
}

Future<void> explicitResultMisuse(GeneratedClient client) async {
  final EditAndReadHandlerOutput missing = EditAndReadHandlerOutput(); // output required despite same-name input
  final EditAndReadHandlerOutput record = EditAndReadHandlerOutput(todo: todo); // handler returns an identity
  final EditAndReadOutput edited = await client.mutations.call.edit(todo: const EditTodoUpdate(id: 'A')); // no business result
  final TodoIdentity removed = await client.mutations.call.removeTodo(todo: const TodoDelete(id: 'A')); // delete has no implicit result
  missing.hashCode;
  record.hashCode;
  edited.hashCode;
  removed.hashCode;
}

Future<void> loadMisuse(GeneratedClient client, Load job) async {
  await client.loads.projectTodos(projectId: 1, status: null, tags: []); // arg type
  await client.loads.projectTodos(projectId: 'p', tags: []); // required nullable arg
  await client.loads.projectTodos(projectId: 'p', status: null, tags: [], cursor: 'x'); // undeclared arg
  await client.loads.projectTodos(projectId: 'p', status: null, tags: [], once: 'yes'); // once is a bool
  await client.loads.projectTodos(projectId: 'p', status: null, tags: [], store: false); // no store option
  await client.loads.flaggedTodos(once: true, refresh: 'x', once: true); // the input owns once
  await client.loads.invalidate.projectTodos(projectId: 'p', status: null, tags: [], once: true); // no options
  final LoadStatus result = await job.wait(); // no aggregate result
  final String phase = job.status.phase; // a LoadPhase, not a string
  await client.loads.list(limit: '10'); // limit is an int
  client.queries.projectTodos; // a Load is not a Query
  client.loads.addTodo; // a Mutation is not a Load
  result.hashCode;
  phase.hashCode;
}

Future<void> transactionMisuse(GeneratedClient client, GeneratedTransaction storeTx) async {
  await client.transaction((tx) async {
    tx.loads; // no Load inside a transaction
    tx.fetch; // no Fetch inside a transaction
    tx.mutations.findTodos; // a Query is not a transaction Mutation
    tx.mutations.getTodos; // GetTodos is a Query in its latest version
    tx.mutations.projectTodos; // a Load is not a transaction Mutation
    await tx.mutations.ping(once: true); // no once control in a transaction
    await tx.mutations.addTodo(todo: created, gone: [], status: null, tags: [], local: (TodoTxModel todos) async {}); // the callback receives the companion context
    await tx.mutations.addTodo(todo: created, gone: [], status: null, tags: [], local: (local) async {
      local.mutations; // the callback queues no Mutation
      local.loads; // the callback starts no Load
      local.fetch; // the callback fetches nothing
      local.queries; // the callback runs no Query
      local.scopes; // the callback modifies no Scope
      local.transaction; // the callback opens no savepoint and has no raw port
      local.models.todo.watch(); // the callback cannot watch
    });
  });
  await client.mutations.ping(local: (local) async {}); // the durable route takes no local callback
  await client.mutations.call.ping(local: (local) async {}); // the direct route takes no local callback
  await client.queries.enqueue.getTodos(local: (local) async {}); // queued Queries take no local callback
  storeTx.mutations; // onStore queues no Mutation
  storeTx.loads; // onStore starts no Load
}
