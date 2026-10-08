import 'generated.dart';
const todo=TodoCreate(id:'t',title:'Task',state:Status.open,note:null);
const identity=TodoIdentity(id:'t');
const composite=ProjectIdentity(tenantId:'tenant',id:'project');
const patch=AddTodoPatchUpdate(id:'t',title:Present('Renamed'));
const input=AddTodoInput(todo:todo,patch:patch,gone:[TodoDelete(id:'old')],status:null,tags:[]);
const defaulted=NoteCreate(memo:null);
const oldInput=AddTodoV1Input(todo:AddTodoV1TodoCreate(id:'old',title:'Old',state:AddTodoV1Status.closed),gone:[],status:null,tags:[]);
const oldOutput=AddTodoV1HandlerOutput(relatedTodo:TodoV1Identity(id:'old'),matches:[],count:1);
const output=AddTodoHandlerOutput(relatedTodo:identity,matches:[identity],count:1,state:null);
const states=StateListHandlerOutput(states:[Status.open,Status.archived]);
const oldStates=StateListV1HandlerOutput(states:[StateListV1OutputStatus.open]);
Future<void> clientContract(GeneratedClient client) async {
 final Call<AddTodoOutput> call=await client.mutations.addTodo(input);
 final CallOutcome<AddTodoOutput> outcome=await call.wait();
 await client.mutations.addTodo.withTransaction((tx) async {
  await tx.models.todo.update(identity,const TodoPatch(title:Present('companion')));
  return input;
 });
 await client.transaction((tx) async {
  await tx.mutations.addTodo.withTransaction((owned) async {
   await owned.models.todo.delete(identity);return input;
  });
 });
 final FindTodosOutput found=await client.queries.findTodos(text:'x',cursor:null,store:false);
 await client.queries.findTodos(text:'x',cursor:found.nextCursor,store:true);
 await client.fetch.todo(identity,store:false);
 await client.models.note.create(defaulted);
 await client.mutations.addNotes(const AddNotesInput(note:defaulted,many:[]));
 await client.queries.projectTodos(projectId:'p',status:null,tags:[]);
 await client.bootstrap();
 if(outcome is CallFailure<AddTodoOutput>) throw outcome.error;
}
void main() { [clientContract,composite,oldInput,oldOutput,output,states,oldStates]; }
