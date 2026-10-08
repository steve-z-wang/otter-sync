import type {Call,GeneratedClient} from './client.ts';
import type {AddTodoInput,AddTodoOutput,Todo,TodoIdentity,TodoUpdate,ProjectIdentity} from './generated.ts';
import type {AddTodoV1Input,AddTodoHandlerOutput,StateListV1HandlerOutput,QueryContext,MutationContext,TransactionCall} from './backend.ts';
declare const client:GeneratedClient;declare const call:Call<AddTodoOutput>;declare const input:AddTodoInput;declare const todo:Todo;
// @ts-expect-error Call has no early result
call.result;
// @ts-expect-error status is immutable
call.status='failed';
// @ts-expect-error no direct Mutation lane
client.mutations.call.addTodo(input);
// @ts-expect-error no queued Query lane
client.queries.enqueue.findTodos({text:'x',cursor:null});
// @ts-expect-error no anonymous remote writes
client.mutate;
// @ts-expect-error no Load manager
client.loads;
// @ts-expect-error one Stream selected at open
client.streams;
// @ts-expect-error callback must return typed input
client.mutations.addTodo(async()=> 'bad');
// @ts-expect-error old local option removed
client.mutations.addTodo(input,{local:async()=>{}});
// @ts-expect-error Mutation has no store option
client.mutations.addTodo(input,{store:false});
// @ts-expect-error request store is boolean
client.queries.findTodos({text:'x',cursor:null},{store:{todos:false}});
// @ts-expect-error nullable input is still required
client.queries.findTodos({text:'x'});
// @ts-expect-error wrong kind namespace
client.mutations.findTodos({text:'x',cursor:null});
// @ts-expect-error no backend reads inside transaction
client.transaction(async tx=>tx.queries.findTodos({text:'x',cursor:null}));
client.mutations.addTodo(async tx=>{
 // @ts-expect-error owned callback has no remote Mutation capability
 tx.mutations;
 // @ts-expect-error owned callback has no watches
 tx.models.todo.watch({},()=>{});
 return input;
});
// @ts-expect-error defaulted nullable memo remains required
client.models.note.create({});
// @ts-expect-error explicit nonnullable default cannot be null
client.models.note.create({memo:null,id:null});
// @ts-expect-error named update field list excludes the existing state field
const patch:TodoUpdate<'title'>={id:'t',state:'closed'};
// @ts-expect-error composite identity requires every component
const identity:ProjectIdentity={id:'p'};
// @ts-expect-error enum frozen old input excludes archived
const old:AddTodoV1Input={todo:{id:'t',title:'t',state:'archived'},gone:[],status:null,tags:[]};
// @ts-expect-error handler returns identity not full Model
const output:AddTodoHandlerOutput={relatedTodo:{id:"t",title:"full"},matches:[],count:1,state:null};
// @ts-expect-error retained output enum excludes archived
const states:StateListV1HandlerOutput={states:['archived']};
declare const q:QueryContext<object>;declare const m:MutationContext<object>;declare const b:TransactionCall<object>;
// @ts-expect-error Query track only
q.stream.invalidate.todo('t');
// @ts-expect-error Query cannot globally invalidate
q.invalidate.todo('t');
// @ts-expect-error current Stream is a handle
m.stream('User:b');
// @ts-expect-error explicit multiple Streams require a list
m.streams('User:b');
// @ts-expect-error background has no implicit Stream
b.stream.track.todo('t');
// @ts-expect-error complete identity required
m.stream.track.project({id:'p'});
// @ts-expect-error unsupported Model field
client.models.todo.update({id:'t'},{unknown:true});
void [patch,identity,old,output,states];

// @ts-expect-error EditMany restricts its named update slot to title
client.mutations.editMany({todos:[{id:'t',state:'closed'}]});
