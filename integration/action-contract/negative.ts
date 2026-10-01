import * as actionBackend from "./backend.ts";
import type { Call, GeneratedClient, Load, LoadOptions } from "./client.ts";
import type { AddTodoInput, AddTodoOutput, EditAndReadOutput, FindTodosOutput, Note, NoteCreate, Todo, TodoIdentity, TodoUpdate, ProjectIdentity, PingOutput } from './generated.ts';
import type { AddNotesInput as AddNotesHandlerInput, AddTodoHandlerOutput, AddTodoV1Input, AddTodoV1HandlerOutput, EditAndReadHandlerOutput, EditHandlerOutput, FindTodosHandlerOutput, LinkHandlerOutput, LoadContext, Loaders, Loads, PingHandlerOutput, ProjectTodosHandlerOutput, ProjectTodosInput, QueryContext, RemoveTodoHandlerOutput, Mutations, Queries, StateListHandlerOutput, StateListV1HandlerOutput } from './backend.ts';
import type { Database } from '../../packages/server/index.mts';

declare const client: GeneratedClient;
declare const concrete: GeneratedClient;
void [concrete.mutations, concrete.queries];
declare const call: Call<AddTodoOutput>;
declare const todo: Todo;
declare const input: AddTodoInput;
// @ts-expect-error A live call has no result property; wait for its outcome.
call.result;
// @ts-expect-error A live call has no error property; wait for its outcome.
call.error;
// @ts-expect-error Call status is read-only.
call.status = 'failed';
// @ts-expect-error No output shortcut exists on a call handle.
call.output;
// @ts-expect-error No cancel method exists on a call handle.
call.cancel();
// @ts-expect-error An application transaction queues Mutations; it has no direct route.
client.transaction(async tx => tx.mutations.call.addTodo(input));
// @ts-expect-error No Query can run inside an application transaction.
client.transaction(async tx => tx.queries.findTodos({ text: 'x', cursor: null }));
// @ts-expect-error The retired actions namespace does not exist.
void concrete.actions;
// @ts-expect-error A Query is not under mutations.
client.mutations.findTodos({ text: 'x', cursor: null });
// @ts-expect-error A Mutation is not under queries.
client.queries.addTodo(input);
// @ts-expect-error A direct Mutation is not under queries.enqueue.
client.queries.enqueue.addTodo(input);
// @ts-expect-error A Query has no direct `call` member; it is direct by default.
client.queries.call.findTodos({ text: 'x', cursor: null });
// @ts-expect-error A Mutation has no `enqueue` member; it is durable by default.
client.mutations.enqueue.addTodo(input);
// @ts-expect-error A direct result has no wait; it is already final.
client.queries.findTodos({ text: 'x', cursor: null }).then(result => result.wait());
// @ts-expect-error A direct Mutation result is its output, not a Call.
const directCall: Promise<Call<AddTodoOutput>> = client.mutations.call.addTodo(input);
// @ts-expect-error A durable Query resolves to a Call, not its output.
const queuedOutput: Promise<{ todos: Todo[] }> = client.queries.enqueue.findTodos({ text: 'x', cursor: null });
// @ts-expect-error A nullable Query input is still a required argument.
client.queries.findTodos({ text: 'x' });
void [directCall, queuedOutput];
declare const queryContext: QueryContext<{}>;
// @ts-expect-error A Query context has no membership writer.
queryContext.scope('todos');
// @ts-expect-error A Query context has no change declaration.
queryContext.touch.todo({ id: 'x' });
// @ts-expect-error A Query handler cannot use Mutation declarations.
const effectfulQuery: Queries<{}>['findTodos'] = async ({ ctx }) => { ctx.touch.todo({ id: 'x' }); return { todos: [], nextCursor: null }; };
declare const mutationContext: actionBackend.MutationContext<{}>;
// @ts-expect-error A composite identity names every component.
mutationContext.scope('tenant:t').add.project({ id: 'p' });
// @ts-expect-error The old publish API is gone.
mutationContext.publish({ scope: 'todos' });
// @ts-expect-error The old changes collector is gone.
mutationContext.changes.add({ model: 'Todo', identity: { id: 'x' } });
// @ts-expect-error Query Model outputs are identity objects.
const bareQueryOutput: FindTodosHandlerOutput = { todos: ['x'], nextCursor: null };
const wrongKindVersion: Queries<{}>['getTodos'] = {
  async v2() { return { todos: [] }; },
  // @ts-expect-error The Query version of GetTodos is v2; v1 is a Mutation.
  async v1() { return { todos: [] }; },
};
// @ts-expect-error A Mutation map does not register a Query-only name.
const queryInMutations: Pick<Mutations<{}>, 'findTodos'> = {};
void [effectfulQuery, bareQueryOutput, wrongKindVersion, queryInMutations];
// @ts-expect-error Transaction models cannot watch.
client.transaction(async tx => tx.models.todo.watch({}, () => {}));
// @ts-expect-error Ordinary nullable args are required-present.
const missingNullable: AddTodoInput = { todo, gone: [], tags: [] };
// @ts-expect-error Patch fields outside the restricted selection are rejected.
const badPatch: TodoUpdate<'title'> = { id: 'x', state: 'open' };
// @ts-expect-error Wrong identity fields for composite Project.
const badProject: ProjectIdentity = { id: 'p' };
// @ts-expect-error Model output requires an identity object, not a bare key.
const bare: AddTodoHandlerOutput = { relatedTodo: 'id', matches: [], count: 1, state: null };
// @ts-expect-error Excess fields in an inline identity object are rejected.
const full: AddTodoHandlerOutput = { relatedTodo: { id: 'x', title: 'extra' }, matches: [], count: 1, state: null };
// @ts-expect-error A Project identity cannot replace a Todo identity.
const wrongModel: AddTodoHandlerOutput = { relatedTodo: { tenantId: 't', id: 'x' }, matches: [], count: 1, state: null };
// @ts-expect-error Explicit handler outputs must include required count.
const missingCount: AddTodoHandlerOutput = { relatedTodo: null, matches: [], state: null };
// @ts-expect-error Model output lists require identity objects in order.
const badList: AddTodoHandlerOutput = { relatedTodo: null, matches: ['id'], count: 1, state: null };
// @ts-expect-error Composite identities need both key fields.
const badComposite: LinkHandlerOutput = { relatedProject: { id: 'x' } };
// @ts-expect-error Extra inline Model fields are not identity fields.
const fullComposite: LinkHandlerOutput = { relatedProject: { tenantId: 't', id: 'x', title: 'extra' } };
// @ts-expect-error Handler scalar outputs keep their declared type.
const wrongScalar: AddTodoHandlerOutput = { relatedTodo: null, matches: [], count: 'one', state: null };
// @ts-expect-error Retained v1 does not accept the new enum case.
const oldEnum: AddTodoV1Input = { todo: { id: 'x', title: 'Old', state: 'archived' }, gone: [], status: 'open', tags: [] };
// @ts-expect-error Retained v1 create input excludes v2 Model fields.
const oldModel: AddTodoV1Input = { todo: { id: 'x', title: 'Old', state: 'open', note: null }, gone: [], status: 'open', tags: [] };
// @ts-expect-error Retained v1 output has its original field set.
const oldOutput: AddTodoV1HandlerOutput = { relatedTodo: null, matches: [], count: 1, state: 'open' };
// @ts-expect-error A no-output client result is void.
const badPing: PingOutput = { unexpected: true };
// @ts-expect-error A no-output handler must return void.
const badPingHandler: PingHandlerOutput = { unexpected: true };
// @ts-expect-error A delete operand implies no result, so its handler returns void.
const badRemoveHandler: RemoveTodoHandlerOutput = { todo: { id: 'x' } };
// @ts-expect-error A retained v2 handler cannot be omitted.
const missingV2: Pick<Mutations<{}>, 'addTodo'> = { addTodo: { async v1() { return { relatedTodo: null, matches: [], count: 1 }; } } };
void actionBackend.createBackend;
// @ts-expect-error All versioned Mutation handlers are required.
const missingHandler: Mutations<{}> = { link: { async v1() { return { relatedProject: null }; } }, ping: { async v1() {} } };
void [missingNullable, badPatch, badProject, bare, full, wrongModel, missingCount, badList, badComposite, fullComposite, wrongScalar, oldEnum, oldModel, oldOutput, badPing, badPingHandler, badRemoveHandler, missingV2, missingHandler];

// @ts-expect-error enum-list output rejects a scalar
const stateListScalar: StateListHandlerOutput = { states: 'open' };
// @ts-expect-error enum-list output rejects an invalid member
const stateListInvalid: StateListHandlerOutput = { states: ['invalid'] };
void [stateListScalar, stateListInvalid];

// @ts-expect-error retained v1 enum-list excludes the new case
const oldStateListArchived: StateListV1HandlerOutput = { states: ['archived'] };
// @ts-expect-error retained v1 enum-list rejects a scalar
const oldStateListScalar: StateListV1HandlerOutput = { states: 'open' };
void [oldStateListArchived, oldStateListScalar];

// @ts-expect-error store maps name explicit Model outputs, not scalar outputs.
client.mutations.call.openTodo({ store: null }, { store: { count: false } });
// @ts-expect-error store maps reject unknown output names.
client.mutations.openTodo({ store: null }, { store: { missing: false } });
// @ts-expect-error store map values are booleans.
client.mutations.call.openTodo({ store: null }, { store: { suggestions: 'no' } });
// @ts-expect-error Model operands are not outputs, so they are not store keys.
client.mutations.call.addTodo(input, { store: { todo: false } });
// @ts-expect-error A delete operand has no result to store.
client.mutations.call.deleteTodo({ todo: { id: 't' } }, { store: { todo: false } });
// @ts-expect-error Operations without eligible outputs accept only a boolean store.
client.mutations.call.ping({}, { store: {} });
// @ts-expect-error Query store maps reject scalar outputs.
client.queries.findTodos({ text: 'x', cursor: null }, { store: { nextCursor: false } });
// @ts-expect-error Queued Query store maps reject unknown outputs.
client.queries.enqueue.findTodos({ text: 'x', cursor: null }, { store: { missing: true } });
// @ts-expect-error Mutations accept no once control.
client.mutations.addTodo(input, { once: true });
// @ts-expect-error Direct Mutations accept no once control.
client.mutations.call.ping({}, { once: true });
// @ts-expect-error Queued Queries accept no once control.
client.queries.enqueue.findTodos({ text: 'x', cursor: null }, { once: true });
// @ts-expect-error Queued Queries accept no refresh control.
client.queries.enqueue.getTodos({}, { refresh: true });
// @ts-expect-error refresh requires once.
client.queries.findTodos({ text: 'x', cursor: null }, { refresh: true });
// @ts-expect-error once is a boolean.
client.queries.getTodos({}, { once: 'yes' });
// @ts-expect-error Invalidation takes business args only.
client.queries.invalidate.getTodos({}, { store: false });
// @ts-expect-error Invalidation validates its args like the Query.
client.queries.invalidate.findTodos({ text: 'x' });
// @ts-expect-error Only Queries have saved results to invalidate.
client.queries.invalidate.addTodo(input);
// @ts-expect-error Invalidation resolves with no value.
const invalidatedValue: Promise<FindTodosOutput> = client.queries.invalidate.findTodos({ text: 'x', cursor: null });
void invalidatedValue;
// Creation defaults (#27): only client create inputs admit omission.
// @ts-expect-error A field without a creation default is still required.
const missingMemo: NoteCreate = {};
// @ts-expect-error An omitted field is left out, not set to undefined.
const undefinedTag: NoteCreate = { memo: null, tag: undefined };
// @ts-expect-error The full Model type stays complete.
const partialNote: Note = { memo: null };
// @ts-expect-error A handler's create argument is the complete, expanded record.
const partialHandlerArgs: AddNotesHandlerInput = { note: { memo: null }, many: [] };
// @ts-expect-error Local create takes the same create input.
client.models.note.create({ body: 'x' });
void [missingMemo, undefinedTag, partialNote, partialHandlerArgs];
// Explicit results (#140): outputs are independent of same-name inputs.
// @ts-expect-error explicit output is required even though input has the same name
const missing: EditAndReadHandlerOutput = {};
// @ts-expect-error The handler returns an identity, not the loaded record.
const recordOutput: EditAndReadHandlerOutput = { todo: { id: 'B', title: 'B', state: 'open', note: null } };
// @ts-expect-error A Mutation without outputs has no handler result.
const editResult: EditHandlerOutput = { todo: { id: 'B' } };
// @ts-expect-error A Mutation without outputs has no business result.
client.mutations.call.edit({ todo: { id: 'A' } }).then(result => result.todo);
// @ts-expect-error A delete operand has no implicit result.
client.mutations.call.removeTodo({ todo: { id: 'A' } }).then(result => result.todo);
// @ts-expect-error The client result is the loaded record, not an identity.
const identityResult: EditAndReadOutput = { todo: { id: 'B' } };
void [missing, recordOutput, editResult, identityResult];

// Loads (#173): typed backend handlers.
declare const loadContext: LoadContext<{}>;
// A Load Scope only adds: there is no remove on either handle form.
// @ts-expect-error A Load Scope's Model accessor has no remove.
loadContext.scope('todos').remove.todo({ id: 'x' });
// @ts-expect-error A Load Scope has no remove for mixed record lists.
loadContext.scope('todos').remove([actionBackend.Todo({ id: 'x' })]);
// @ts-expect-error A Load Scope has no tag selector either.
loadContext.scope('todos').where({ tags: { all: ['X'] } }).remove();
// @ts-expect-error A tagged Load add still names the Model's identity.
loadContext.scope('todos').add.todo({ id: 1 }).tag(['X']);
// @ts-expect-error Load tags are a list of strings.
loadContext.scope('todos').add.todo({ id: 'x' }).tag(3);
// @ts-expect-error A Load Scope is not a Mutation's full Scope.
const fullLoadScope: actionBackend.Scope = loadContext.scope('todos');
// @ts-expect-error A composite identity names every component.
loadContext.scope('tenant:t').add.project({ id: 'p' });
// @ts-expect-error A Todo identity is a string id, not a Project identity.
loadContext.scope('todos').add.todo({ tenantId: 't', id: 'x' });
// @ts-expect-error A Todo id is a string.
loadContext.scope('todos').add.todo({ id: 1 });
// @ts-expect-error A mixed list takes references, not raw identities.
loadContext.scope('todos').add([{ id: 'x' }]);
// @ts-expect-error Only schema Models have an accessor.
loadContext.scope('todos').add.tsak({ id: 'x' });
// @ts-expect-error A Load context has no change declaration.
loadContext.touch.todo({ id: 'x' });
// @ts-expect-error A Load handler cannot remove memberships.
const removingLoad: Loads<{}>['recentTodos'] = async ({ ctx }) => { ctx.scope('todos').remove.todo({ id: 'x' }); return { data: { todos: [] }, next: null }; };
// @ts-expect-error A Loader has no Scope: materializing a record enrolls nothing.
const scopeLoader: Loaders<{}>['project'] = async ({ ids, scope: scope }) => { scope('tenant:t').add.project(ids[0]!); return []; };
// @ts-expect-error A Load handler cannot use Mutation declarations.
const effectfulLoad: Loads<{}>['projectTodos'] = async ({ ctx }) => { ctx.touch.todo({ id: 'x' }); return { data: { todos: [], projects: [] }, next: null }; };
// @ts-expect-error Load args keep their declared types.
const wrongArgType: Loads<{}>['projectTodos'] = async ({ args }) => { const n: number = args.projectId; void n; return { data: { todos: [], projects: [] }, next: null }; };
// @ts-expect-error A Load has only its declared args.
const undeclaredArg: Loads<{}>['projectTodos'] = async ({ args }) => { void args.cursor; return { data: { todos: [], projects: [] }, next: null }; };
// @ts-expect-error A Load takes no Model operand.
const operandArgs: ProjectTodosInput = { projectId: 'p', status: null, tags: [], todo: { id: 'x', title: 'T', state: 'open', note: null } };
// @ts-expect-error A nullable Load arg is still present.
const missingNullableArg: ProjectTodosInput = { projectId: 'p', tags: [] };
// @ts-expect-error A Load page answers identities, not full Model records.
const fullPage: ProjectTodosHandlerOutput = { data: { todos: [{ id: 'x', title: 'T', state: 'open', note: null }], projects: [] }, next: null };
// @ts-expect-error Load outputs are identity objects, not bare keys.
const barePage: ProjectTodosHandlerOutput = { data: { todos: ['x'], projects: [] }, next: null };
// @ts-expect-error A Load output is a list, never a single identity.
const singlePage: ProjectTodosHandlerOutput = { data: { todos: { id: 'x' }, projects: [] }, next: null };
// @ts-expect-error Every declared output is present.
const partialPage: ProjectTodosHandlerOutput = { data: { todos: [] }, next: null };
// @ts-expect-error A Load declares no scalar output.
const scalarPage: ProjectTodosHandlerOutput = { data: { todos: [], projects: [], count: 1 }, next: null };
// @ts-expect-error A composite identity names every component.
const partialComposite: ProjectTodosHandlerOutput = { data: { todos: [], projects: [{ id: 'p' }] }, next: null };
// @ts-expect-error A Project identity cannot replace a Todo identity.
const wrongPageModel: ProjectTodosHandlerOutput = { data: { todos: [{ tenantId: 't', id: 'x' }], projects: [] }, next: null };
// @ts-expect-error next is required: null completes the Load.
const noNext: ProjectTodosHandlerOutput = { data: { todos: [], projects: [] } };
// @ts-expect-error A continuation wraps its state.
const bareState: ProjectTodosHandlerOutput = { data: { todos: [], projects: [] }, next: 'cursor' };
// @ts-expect-error The continuation wrapper has only state.
const extraWrapper: ProjectTodosHandlerOutput = { data: { todos: [], projects: [] }, next: { state: 1, extra: true } };
// @ts-expect-error Continuation state is portable JSON: no Date.
const dateState: ProjectTodosHandlerOutput = { data: { todos: [], projects: [] }, next: { state: new Date() } };
// @ts-expect-error Continuation state is portable JSON: no undefined.
const undefinedState: ProjectTodosHandlerOutput = { data: { todos: [], projects: [] }, next: { state: { cursor: undefined } } };
// @ts-expect-error Continuation state is portable JSON: no BigInt.
const bigintState: ProjectTodosHandlerOutput = { data: { todos: [], projects: [] }, next: { state: 1n } };
// @ts-expect-error Continuation state is portable JSON: no functions.
const functionState: ProjectTodosHandlerOutput = { data: { todos: [], projects: [] }, next: { state: () => 1 } };
// @ts-expect-error A contextual handler answer still types its continuation.
const dateStateHandler: Loads<{}>['projectTodos'] = async () => ({ data: { todos: [], projects: [] }, next: { state: new Date() } });
// @ts-expect-error A Load is registered under loads, not queries.
const loadInQueries: Pick<Queries<{}>, 'projectTodos'> = {};
// @ts-expect-error A Query is not registered under loads.
const queryInLoads: Pick<Loads<{}>, 'findTodos'> = {};
// @ts-expect-error A v1-only Load retains no v2.
const loadV2: Loads<{}> = { projectTodos: { async v2() { return { data: { todos: [], projects: [] }, next: null }; } } };
// @ts-expect-error Every Load handler is required.
const noLoads: Loads<{}> = {};
declare const database: Database<{}>;
declare const everyMutation: Mutations<{}>;
declare const everyQuery: Queries<{}>;
declare const everyLoader: Loaders<{}>;
// @ts-expect-error A Model that registers a Loader registers every retained version (Todo retains v1 and v2).
const partialLoader: Loaders<{}> = { todo: { async v1() { return []; } } };
// @ts-expect-error A Loader key names a Model.
const unknownLoader: Loaders<{}> = { tsak: async () => [] };
// @ts-expect-error A schema that retains Loads requires the loads map.
const withoutLoads = () => actionBackend.createBackend({ database, authenticate: () => 'alice', mutations: everyMutation, queries: everyQuery, loaders: everyLoader });
void [fullLoadScope, removingLoad, scopeLoader, partialLoader, unknownLoader, effectfulLoad, wrongArgType, undeclaredArg, operandArgs, missingNullableArg, fullPage, barePage, singlePage, partialPage, scalarPage, partialComposite, wrongPageModel, noNext, bareState, extraWrapper, dateState, undefinedState, bigintState, functionState, dateStateHandler, loadInQueries, queryInLoads, loadV2, noLoads, withoutLoads];

// Client Loads (#173): typed business args, call-site options apart from them.
declare const job: Load<'ProjectTodos'>;
// @ts-expect-error Load args keep their declared types.
client.loads.projectTodos({ projectId: 1, status: null, tags: [] });
// @ts-expect-error Every Load arg is present, a nullable one too.
client.loads.projectTodos({ projectId: 'p', tags: [] });
// @ts-expect-error A Load has only its declared args.
client.loads.projectTodos({ projectId: 'p', status: null, tags: [], cursor: 'x' });
// @ts-expect-error Options are never business args.
client.loads.projectTodos({ projectId: 'p', status: null, tags: [], once: true });
// @ts-expect-error once is a Boolean.
client.loads.projectTodos({ projectId: 'p', status: null, tags: [] }, { once: 'yes' });
// @ts-expect-error A Load stores every declared Model: there is no store option.
client.loads.projectTodos({ projectId: 'p', status: null, tags: [] }, { store: false });
// @ts-expect-error LoadOptions holds only once and refresh.
const cursorOption: LoadOptions = { once: true, cursor: 'x' };
// @ts-expect-error A no-argument Load still takes its (empty) args object.
client.loads.recentTodos();
// @ts-expect-error A Load has no aggregate business result.
job.wait().then(result => result.todos);
// @ts-expect-error A handle exposes no result or cursor.
void job.result;
// @ts-expect-error The status name is the schema operation name.
const otherName: 'RecentTodos' = job.status.name;
// @ts-expect-error Invalidation takes only business args.
client.loads.invalidate.projectTodos({ projectId: 'p', status: null, tags: [] }, { once: true });
// @ts-expect-error list takes its limit as a number.
client.loads.list({ limit: '10' });
// @ts-expect-error A Load is started under loads, not queries.
client.queries.projectTodos;
// @ts-expect-error A Mutation is not a Load.
client.loads.addTodo;
void [cursorOption, otherName];

// Transactional Mutation enqueue (#173 Loads included): the application
// transaction queues typed Mutations only, a Mutation's `local` callback has
// Models only, and onStore queues nothing. The runtime refuses the same misuse.
declare const storeTransaction: import('./client.ts').GeneratedTransaction;
client.transaction(async tx => {
  // @ts-expect-error A Load is not started inside a transaction.
  void tx.loads;
  // @ts-expect-error Fetch is unavailable inside a transaction.
  void tx.fetch;
  // @ts-expect-error A Query is not queued as a transaction Mutation.
  void tx.mutations.findTodos;
  // @ts-expect-error GetTodos is a Query in its latest version.
  void tx.mutations.getTodos;
  // @ts-expect-error A Load is not a transaction Mutation.
  void tx.mutations.projectTodos;
  // @ts-expect-error Options are never business args.
  await tx.mutations.sendEmail({ to: 'a', subject: 's', body: 'b', local: async () => {} });
  // @ts-expect-error The store option names only store-eligible outputs.
  await tx.mutations.addTodo(input, { store: { count: false } });
  // @ts-expect-error Mutations accept no once control, in a transaction too.
  await tx.mutations.ping({}, { once: true });
  return tx.mutations.addTodo(input, {
    local: async local => {
      // @ts-expect-error The callback queues no Mutation.
      void local.mutations;
      // @ts-expect-error The callback starts no Load.
      void local.loads;
      // @ts-expect-error The callback fetches nothing.
      void local.fetch;
      // @ts-expect-error The callback runs no Query.
      void local.queries;
      // @ts-expect-error The callback modifies no Scope.
      void local.scopes;
      // @ts-expect-error The callback opens no savepoint and has no raw port.
      void local.transaction;
      // @ts-expect-error The callback cannot watch.
      local.models.todo.watch({}, () => {});
    },
  });
});
// @ts-expect-error The client-level durable route takes no local callback.
client.mutations.addTodo(input, { local: async () => {} });
// @ts-expect-error The direct route takes no local callback.
client.mutations.call.addTodo(input, { local: async () => {} });
// @ts-expect-error Queued Queries take no local callback.
client.queries.enqueue.findTodos({ text: 'x', cursor: null }, { local: async () => {} });
// @ts-expect-error onStore queues no Mutation.
void storeTransaction.mutations;
// @ts-expect-error onStore starts no Load.
void storeTransaction.loads;

function invalidCanonicalScope(ctx: import('./backend.ts').MutationContext<object>, load: import('./backend.ts').LoadContext<object>, query: import('./backend.ts').QueryContext<object>) {
 // @ts-expect-error wrong scalar identity
 ctx.scope('U').add.todo(3);
 // @ts-expect-error composite requires every field
 ctx.scope('U').add.project({id:'P'});
 // @ts-expect-error mixed operands name their model
 ctx.scope('U').add({id:'A'});
 // @ts-expect-error root add needs explicit operands
 ctx.scope('U').add();
 // @ts-expect-error root remove needs explicit operands
 ctx.scope('U').remove();
 // @ts-expect-error label add needs explicit operands
 ctx.scope('U').tag('X').add();
 // @ts-expect-error existing selection cannot enroll
 ctx.scope('U').where({tags:{only:[]}}).add();
 // @ts-expect-error Load cannot remove
 load.scope('U').remove.todo('A');
 // @ts-expect-error Load cannot select
 load.scope('U').where({tags:{only:[]}});
 // @ts-expect-error Load cannot detach labels
 load.scope('U').tag('X').remove();
 // @ts-expect-error Load has no touch
 load.touch.todo('A');
 // @ts-expect-error Query has no scope
 query.scope('U');
}

function invalidViewerLoaderScope(call: import('./backend.ts').LoaderCall<object, { id: string }>) {
 // @ts-expect-error viewer Loader has no scope
 call.scope('U');
}

// Task 3A: backend exposes only the canonical Scope surface.
// @ts-expect-error backend Scope facade is retired
mutationContext.scope('U');
// @ts-expect-error Model-first membership is retired
mutationContext.scope('U').todo.add({ id: 'A' });
// @ts-expect-error tag selectors are not record references
mutationContext.scope('U').remove({ tag: 'X' });
