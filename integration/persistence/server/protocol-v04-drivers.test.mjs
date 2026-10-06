// Real ORM adapters share the same v04 authority/fence/manifest contract.
import test,{after} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {Pool} from 'pg';
import {drizzle as orm} from 'drizzle-orm/node-postgres';
import {createBackend} from '../../../packages/server/index.mts';
import {pg,prisma} from '../../../packages/postgres/index.mts';
import {drizzle} from '../../../packages/postgres/src/drizzle.mts';
const require=createRequire(import.meta.url);
const native=require('../../../bindings/node/axton-node.node');
const {PrismaClient}=require(process.env.AXTON_PRISMA_CLIENT ?? '../../bindings/node/generated/client');
const check=new Pool({connectionString:process.env.DATABASE_URL});
const closers=[];after(async()=>{for(const close of closers)await close();await check.end();});
const shims=[];
{const pool=new Pool({connectionString:process.env.DATABASE_URL});shims.push(['pg',pg(pool)]);closers.push(()=>pool.end());}
{const client=new PrismaClient();shims.push(['prisma',prisma(client)]);closers.push(()=>client.$disconnect());}
{const pool=new Pool({connectionString:process.env.DATABASE_URL});shims.push(['drizzle',drizzle(orm(pool))]);closers.push(()=>pool.end());}
let call=0;const id=()=>`01890f47-1234-7123-8123-${String(++call).padStart(12,'0')}`;
const model={name:'Task',version:1,identity:['id'],bootstrap:true,unique:[['title']],fields:['id','title'].map(name=>({name,type:{kind:'scalar',name:'string'},nullable:false}))};
const config={schema:{enums:[],models:[model],resultModels:[{...model,enums:[]}],actions:[{name:'Find',version:1,kind:'query',inputs:[],outputs:[{name:'task',kind:'model',model:'Task',modelReadVersion:1,source:'handlerIdentity',cardinality:'single',handlerType:{kind:'identity',model:'Task',fields:[{name:'id',type:{kind:'scalar',name:'string'}}]}}]},{name:'Edit',version:1,kind:'mutation',inputs:[{kind:'model',name:'task',model:'Task',operation:'update',cardinality:'single',allowedPatchFields:['title']}],outputs:[]}]},loaders:['Task'],mutations:[]};
for(const [name,base] of shims)test(`[${name}] real v04 Query, accepted Mutation and bounded unique manifest use one Serializable transaction`,async()=>{
 const namespace=`syn7_${name}`;await check.query(`CREATE SCHEMA ${namespace}`);const setup=await check.connect();
 try{await setup.query('BEGIN');await setup.query(`SET LOCAL search_path=${namespace}`);await setup.query(await readFile(new URL('../../../packages/postgres/migration.sql',import.meta.url),'utf8'));await setup.query("CREATE TABLE business_task(id text PRIMARY KEY,title text UNIQUE NOT NULL);INSERT INTO business_task VALUES('zz','X'),('aa','Y')");await setup.query('COMMIT');}catch(error){await setup.query('ROLLBACK');throw error;}finally{setup.release();}
 const query=(tx,text,params=[])=>base.driver.query(tx,text,params);
 const database={...base,transaction:body=>base.transaction(async tx=>{await query(tx,`SET LOCAL search_path=${namespace}`);return body(tx);})};
 const app=createBackend({config,native,database,protocol4:{backendId:name,contractId:'app',authorizeStream:(viewer,stream)=>stream===`User:${viewer}`},authenticate:()=> 'alice',queries:{find:async({ctx})=>{ctx.stream.track.task({id:'zz'});assert.equal((await query(ctx.tx,'SHOW transaction_isolation'))[0].transaction_isolation,'serializable');return {task:{id:'zz'}};}},mutations:{edit:async({ctx,args})=>{await query(ctx.tx,'UPDATE business_task SET title=$2 WHERE id=$1',[args.task.id,args.task.title]);ctx.invalidate.task({id:args.task.id});}},loaders:{task:async({tx,ids})=>Promise.all(ids.map(async({id})=>(await query(tx,'SELECT id,title FROM business_task WHERE id=$1',[id]))[0]??null))}});
 const context={protocol:4,binding:{backend:name,viewer:'alice',stream:'User:alice',contract:'app'},materialization:app.materializationId,incarnation:'store'};
 const read=JSON.parse(await app.action('alice',JSON.stringify({context,callId:id(),name:'Find',version:1,args:{}})));assert.equal(read.records[0].cursor,null);
 await app.transaction(async({streams})=>streams(['User:alice']).track.task({id:'aa'}));
 await app.transaction(async({tx,invalidate})=>{await query(tx,"UPDATE business_task SET title='Z' WHERE id='zz'");invalidate.task({id:'zz'});});
 await app.transaction(async({tx,invalidate})=>{await query(tx,"UPDATE business_task SET title='X' WHERE id='aa'");invalidate.task({id:'aa'});});
 const start=JSON.parse(await app.pull('alice',JSON.stringify({kind:'start',context,callId:id(),models:{Task:1},budget:10})));
 const page=JSON.parse(await app.pull('alice',JSON.stringify({kind:'page',context,callId:id(),manifestId:start.manifestId,from:0,limit:1})));assert.equal(page.items[0].change.record.identity.id,'aa');assert.ok(page.companions.some(c=>c.record.identity.id==='zz'&&c.record.state.title==='Z'));
 const request={context,callId:id(),name:'Edit',version:1,args:{task:{id:'zz',title:'W'}},models:{Task:1}};
 const receipt=JSON.parse(await app.action('alice',JSON.stringify(request)));assert.equal(receipt.completion.outcome.status,'succeeded');assert.equal(receipt.targets[0].fallback.state.title,'W');assert.equal(receipt.targets[0].cursor,5);assert.deepEqual(JSON.parse(await app.action('alice',JSON.stringify(request))),receipt);
 const delta=JSON.parse(await app.pull('alice',JSON.stringify({context,callId:id(),after:4,models:{Task:1},limit:1})));assert.equal(delta.to,5);assert.equal(delta.units[0].changes[0].record.state.title,'W');
 const persisted=await check.query(`SELECT response FROM ${namespace}.axton_call WHERE call_id=$1`,[request.callId]);assert.deepEqual(JSON.parse(persisted.rows[0].response),receipt);
});
