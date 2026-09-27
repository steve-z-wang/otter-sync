import type { Call, CallOutcome, GeneratedClient } from './client.ts';
import { Project, Todo } from './backend.ts';
import type { NoteCreate, OpenTodoOutput, AddTodoInput, AddTodoOutput, EditOutput, EditAndReadOutput, FindTodosOutput, TodoCreate, TodoUpdate, TodoDelete, TodoIdentity, ProjectIdentity, PingOutput } from './generated.ts';
import type { AddTodoHandlerOutput, AddTodoV1Input, EditAndReadHandlerOutput, EditHandlerOutput, AddTodoV1HandlerOutput, FindTodosHandlerOutput, GetTodosV1HandlerOutput, MutationContext, PingHandlerOutput, QueryContext, RemoveTodoHandlerOutput, Mutations, Queries, Loaders, StateListHandlerOutput, StateListV1HandlerOutput } from './backend.ts';

const created: TodoCreate = { id: 't', title: 'Task', state: 'open', note: null };
const patch: TodoUpdate<'title'> = { id: 't', title: 'Renamed' };
const deletion: TodoDelete = { id: 't' };
const input: AddTodoInput = { todo: created, patch, gone: [deletion], status: null, tags: [] };
const identity: TodoIdentity = { id: 't' };
const composite: ProjectIdentity = { tenantId: 'tenant', id: 'project' };
const oldInput: AddTodoV1Input = { todo: { id: 'old', title: 'Old', state: 'closed' }, gone: [], status: 'open', tags: [] };
const oldOutput: AddTodoV1HandlerOutput = { relatedTodo: { id: 'old' }, matches: [], count: 1 };
const stateListOutput: StateListHandlerOutput = { states: ['open', 'closed', 'archived'] };
const oldStateListOutput: StateListV1HandlerOutput = { states: ['open', 'closed'] };
const handlerOutput: AddTodoHandlerOutput = { relatedTodo: identity, matches: [identity], count: 1, state: null };

async function clientContract(client: GeneratedClient) {
  await client.models.todo.create(created);
  await client.models.todo.update(identity, { title: 'New' });
  await client.models.todo.delete(identity);
  await client.models.todo.get(identity);
  await client.models.todo.query();
  client.models.todo.watch({}, () => {});
  await client.transaction(async tx => {
    await tx.models.todo.create(created);
    await tx.models.todo.update(identity, { title: 'New' });
    await tx.models.todo.delete(identity);
    await tx.models.todo.get(identity);
    await tx.models.todo.query();
  });
  // Mutations: durable by default, direct under `call`.
  const call: Call<AddTodoOutput> = await client.mutations.addTodo(input);
  const status: 'pending' | 'succeeded' | 'failed' = call.status;
  const outcome: CallOutcome<AddTodoOutput> = await call.wait();
  if (outcome.error === null) {
    // Operands are targets, not results: only explicit outputs are present.
    const related: string | undefined = outcome.result.relatedTodo?.title;
    void related;
  } else {
    const code: string = outcome.error.code;
    void code;
  }
  const final: AddTodoOutput = await client.mutations.call.addTodo(input);
  // A delete operand has no implicit result.
  const removed: void = await client.mutations.call.removeTodo({ todo: { id: 't' } });
  // Explicit results (#140): Edit returns no business value; EditAndRead
  // returns the loaded Todo its handler selected, independent of the input.
  const edited: EditOutput = await client.mutations.call.edit({ todo: { id: 'A', title: 'Renamed' } });
  const editedCall: Call<void> = await client.mutations.edit({ todo: { id: 'A' } });
  const read: EditAndReadOutput = await client.mutations.call.editAndRead({ todo: { id: 'A', state: 'closed' } });
  const readTitle: string = read.todo.title;
  await client.mutations.editAndRead({ todo: { id: 'A' } }, { store: { todo: false } });
  const noOutput: PingOutput = await client.mutations.call.ping({});
  await client.mutations.call.deleteTodo({ todo: deletion });
  const email: Call<void> = await client.mutations.sendEmail({ to: 'team@example.test', subject: 'Todo', body: 'Created' });
  // Queries: direct by default, durable under `enqueue`.
  const selected = await client.queries.getTodos({});
  const rows: readonly { id: string }[] = selected.todos;
  const found: FindTodosOutput = await client.queries.findTodos({ text: 'design', cursor: null });
  const next: string | null = found.nextCursor;
  const queued: Call<FindTodosOutput> = await client.queries.enqueue.findTodos({ text: 'design', cursor: next });
  const queuedOutcome: CallOutcome<FindTodosOutput> = await queued.wait();
  // store is an invocation option beside args on every route; results keep their types.
  const opened: OpenTodoOutput = await client.mutations.call.openTodo({ store: 'business' }, { store: false });
  const suggestion: string | undefined = opened.suggestions[0]?.title;
  await client.mutations.call.openTodo({ store: null }, { store: { suggestions: false } });
  await client.mutations.call.openTodo({ store: null }, { store: { mainTodo: true, related: false } });
  const openCall: Call<OpenTodoOutput> = await client.mutations.openTodo({ store: null }, { store: true });
  const openOutcome: CallOutcome<OpenTodoOutput> = await openCall.wait();
  await client.mutations.addTodo(input, { store: { matches: false } });
  await client.queries.getTodos({}, {});
  await client.queries.findTodos({ text: 'x', cursor: null }, { store: false });
  await client.queries.enqueue.findTodos({ text: 'x', cursor: null }, { store: { todos: false } });
  await client.queries.enqueue.getTodos({}, { store: true });
  await client.mutations.call.ping({}, { store: false });
  await client.mutations.deleteTodo({ todo: deletion }, { store: true });
  // once reuses a saved complete result; refresh replaces it; invalidate discards it.
  const cachedTodos: FindTodosOutput = await client.queries.findTodos({ text: 'x', cursor: null }, { once: true });
  await client.queries.findTodos({ text: 'x', cursor: null }, { once: true, refresh: true, store: false });
  await client.queries.findTodos({ text: 'x', cursor: null }, { once: false });
  await client.queries.getTodos({}, { once: true, store: { todos: false } });
  const invalidated: void = await client.queries.invalidate.findTodos({ text: 'x', cursor: null });
  await client.queries.invalidate.getTodos({});
  void [cachedTodos, invalidated];
  // Creation defaults (#27): create inputs may omit defaulted fields, locally and in Mutations.
  const draft: NoteCreate = { memo: null };
  await client.models.note.create(draft);
  await client.models.note.create({ memo: 'm', tag: null, pinned: true });
  await client.transaction(tx => tx.models.note.create({ memo: null, at: new Date(0) }));
  const saved = await client.mutations.call.addNotes({ note: draft, many: [{ memo: 'x' }, { id: 'given', memo: null }] });
  const savedAt: Date = saved.saved.at;
  await client.mutations.addNotes({ note: { memo: null }, maybe: null, many: [] });
  void [status, final, removed, edited, editedCall, readTitle, noOutput, email, rows, suggestion, openOutcome, queuedOutcome, savedAt];
}

type Tx = { db: unknown };
const pingHandlerResult: PingHandlerOutput = undefined;
const removeHandlerResult: RemoveTodoHandlerOutput = undefined;
const editHandlerResult: EditHandlerOutput = undefined;
// The handler supplies the output identity; input A and output B are independent.
const output: EditAndReadHandlerOutput = { todo: { id: "B" } };
// A composite identity names every component; a mixed list takes explicit references.
const mutationContext = (ctx: MutationContext<Tx>) => [ctx.tx, ctx.userId, ctx.callId, ctx.channel('tenant:t').project.add({ tenantId: 't', id: 'p' }), ctx.touch.todo({ id: 't' }), ctx.channel('tenant:t').remove([Project({ tenantId: 't', id: 'p' }), Todo({ id: 't' })])];
const queryContext = (ctx: QueryContext<Tx>) => [ctx.tx, ctx.userId, ctx.callId];
const findOutput: FindTodosHandlerOutput = { todos: [{ id: 't' }], nextCursor: null };
const oldGetTodos: GetTodosV1HandlerOutput = { todos: [] };
const handlers: Mutations<Tx> = {
  addTodo: { async v1({ ctx, args }) { void ctx.tx.db; void args.todo.id; return { relatedTodo: { id: args.todo.id }, matches: [], count: 1 }; }, async v2({ ctx, args }) { void ctx.tx.db; void args.todo.note; return handlerOutput; } },
  link: { async v1({ args }) { return { relatedProject: { tenantId: args.project.tenantId, id: args.project.id } }; } },
  ping: { async v1({ ctx, args }) { void ctx.tx.db; void args; } },
  removeTodo: { async v1({ args }) { void args.todo.id; } },
  openTodo: { async v1({ args }) { void args.store; return { mainTodo: { id: 't' }, suggestions: [], related: null, count: 0 }; } },
  search: { async v1({ args }) { void args.query; } },
  deleteTodo: { async v1({ args }) { void args.todo.id; } },
  edit: async ({ args }) => { void args.todo.id; },
  editAndRead: async ({ args }) => { void args.todo.id; return output; },
  sendEmail: { async v1({ args }) { void args.to; void args.subject; void args.body; } },
  // v1 of GetTodos stays a Mutation; its v2 is registered as a Query.
  getTodos: async ({ ctx }) => { ctx.touch.todo({ id: 't' }); return oldGetTodos; },
  stateList: { async v1() { return oldStateListOutput; }, async v2() { return stateListOutput; } },
  // Handlers receive the expanded create: every defaulted field is present.
  addNotes: async ({ args }) => { const id: string = args.note.id; const at: Date = args.note.at; const pinned: boolean = args.note.pinned; const tag: string | null = args.note.tag; const ids: string[] = args.many.map(n => n.id); void [at, pinned, tag, ids, args.maybe?.id]; return { saved: { id } }; },
};
const queries: Queries<Tx> = {
  findTodos: async ({ ctx, args }) => { void ctx.tx.db; void ctx.userId; void args.text; void args.cursor; return findOutput; },
  getTodos: { async v2({ ctx }) { void ctx.callId; return { todos: [{ id: 't' }] }; } },
};
const loaders: Loaders<Tx> = {
  todo: { async v1() { return []; }, async v2() { return []; } },
  project: async () => [],
  note: async () => [],
};
void [handlers, queries, mutationContext, queryContext, loaders, clientContract, composite, oldInput, oldOutput, pingHandlerResult, removeHandlerResult, editHandlerResult, oldStateListOutput];
