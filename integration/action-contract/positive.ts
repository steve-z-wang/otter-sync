import type {Call,CallOutcome,GeneratedClient} from './client.ts';
import type {TodoCreate,TodoUpdate,TodoDelete,TodoIdentity,ProjectIdentity,AddTodoInput,AddTodoOutput,FindTodosOutput,NoteCreate} from './generated.ts';
import type {AddTodoV1Input,AddTodoV1HandlerOutput,AddTodoHandlerOutput,StateListHandlerOutput,StateListV1HandlerOutput,MutationContext,QueryContext,TransactionCall,LoaderHooks,Loaders} from './backend.ts';
const created:TodoCreate={id:'t',title:'Task',state:'open',note:null};
const patch:TodoUpdate<'title'>={id:'t',title:'Renamed'};
const deletion:TodoDelete={id:'t'};
const input:AddTodoInput={todo:created,patch,gone:[deletion],status:null,tags:[]};
const identity:TodoIdentity={id:'t'};
const composite:ProjectIdentity={tenantId:'tenant',id:'project'};
const oldInput:AddTodoV1Input={todo:{id:'old',title:'Old',state:'closed'},gone:[],status:'open',tags:[]};
const oldOutput:AddTodoV1HandlerOutput={relatedTodo:{id:'old'},matches:[],count:1};
const handlerOutput:AddTodoHandlerOutput={relatedTodo:identity,matches:[identity],count:1,state:null};
const states:StateListHandlerOutput={states:['open','closed','archived']};
const oldStates:StateListV1HandlerOutput={states:['open','closed']};
const defaulted:NoteCreate={memo:null};
async function clientContract(client:GeneratedClient) {
 const call:Call<AddTodoOutput>=await client.mutations.addTodo(input);
 const outcome:CallOutcome<AddTodoOutput>=await call.wait();
 await client.mutations.addTodo(async tx=>{await tx.models.todo.update(identity,{title:'companion'});return input;});
 await client.transaction(async tx=>{
  const before=await tx.models.todo.get(identity);
  await tx.mutations.addTodo(async owned=>{await owned.models.todo.delete(identity);return {...input,status:before?.state??null};});
 });
 const found:FindTodosOutput=await client.queries.findTodos({text:'design',cursor:null},{store:false,once:true});
 await client.queries.findTodos({text:'design',cursor:found.nextCursor},{store:true,once:true,refresh:true});
 await client.queries.invalidate.findTodos({text:'design',cursor:null});
 await client.fetch.todo(identity,{store:false});
 await client.models.note.create(defaulted);
 await client.mutations.addNotes({note:defaulted,many:[]});
 await client.queries.projectTodos({projectId:'p',status:null,tags:[]});
 await client.bootstrap();
 void outcome;
}
function contexts(m:MutationContext<object>,q:QueryContext<object>,b:TransactionCall<object>) {
 m.stream.track.todo(identity);m.stream.invalidate.todo(identity);m.invalidate.todo(identity);
 m.streams(['User:a','User:b']).track.project(composite);
 q.stream.track.todo(identity);q.streams(['User:a']).track.todo(identity);
 b.streams(['User:a']).invalidate.todo(identity);
}
const hooks:LoaderHooks<object>={todo:{async prepareForViewer(call){call.streams(['User:a']).track.todo(call.ids);call.invalidate.todo(call.ids);}}};
const loaders:Loaders<object>={todo:{async v1({ids}){return ids.map(id=>({id:id.id,title:'old',state:'open' as const}));},async v2({ids}){return ids.map(id=>({...created,id:id.id}));}}};
void [clientContract,contexts,hooks,loaders,oldInput,oldOutput,handlerOutput,states,oldStates];
