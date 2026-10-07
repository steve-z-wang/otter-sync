import 'generated.dart';
const created=TodoCreate(id:'t',title:'Task',state:Status.open,note:null);
const input=AddTodoInput(todo:created,gone:[],status:null,tags:[]);
Future<void> misuse(GeneratedClient client,Call<AddTodoOutput> call) async {
 call.result; // error: undefined_getter
 client.mutations.call; // error: undefined_getter
 client.queries.enqueue; // error: undefined_getter
 client.loads; // error: undefined_getter
 client.streams; // error: undefined_getter
 client.mutate; // error: undefined_getter
 await client.mutations.addTodo('bad'); // error: argument_type_not_assignable
 await client.mutations.addTodo.withTransaction((tx) async => 'bad'); // error: return_of_invalid_type_from_closure
 await client.mutations.addTodo(input,local:(_) async {}); // error: undefined_named_parameter
 await client.mutations.addTodo(input,store:false); // error: undefined_named_parameter
 await client.queries.findTodos(text:'x',cursor:null,store:{'todos':false}); // error: argument_type_not_assignable
 await client.queries.findTodos(text:'x'); // error: missing_required_argument
 await client.transaction((tx) async {
  tx.queries; // error: undefined_getter
  tx.mutations.call; // error: undefined_getter
  tx.models.todo.watch(); // error: undefined_method
 });
 await client.mutations.addTodo.withTransaction((tx) async {
  tx.mutations; // error: undefined_getter
  return input;
 });
 await client.models.note.create(const NoteCreate()); // error: missing_required_argument
 await client.models.todo.update(const TodoIdentity(id:'t'),const TodoPatch(id:'changed')); // error: undefined_named_parameter
}
const wrongComposite=ProjectIdentity(id:'p'); // error: missing_required_argument
const wrongEnum=AddTodoV1Input(todo:AddTodoV1TodoCreate(id:'t',title:'t',state:Status.archived),gone:[],status:null,tags:[]); // error: argument_type_not_assignable
const wrongList=StateListHandlerOutput(states:Status.open); // error: argument_type_not_assignable
const wrongIdentity=AddTodoHandlerOutput(relatedTodo:'t',matches:[],count:1,state:null); // error: argument_type_not_assignable

const excludedNamedSlot=EditManyTodosUpdate(id:'t',state:Present(Status.closed)); // error: undefined_named_parameter
