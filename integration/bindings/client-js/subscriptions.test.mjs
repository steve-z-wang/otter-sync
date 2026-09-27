// Subscription handles: identity, the committed status they publish, their
// observers, and what closing one means
// ([#150](https://github.com/zanminwang/axton/issues/150)).
import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { WebSocketServer } from '../../../packages/server/node_modules/ws/wrapper.mjs';
import * as runtime from '../../../packages/client-js/index.mts';
// The registry and the Bridge, for what only an exact runtime script can produce.
import { Subscriptions } from '../../../packages/client-js/subscriptions.mts';
import { Bridge } from '../../../packages/client-js/bridge.mts';

async function openClient() {
 const dir = await mkdtemp(join(tmpdir(),'axton-subscriptions-'));
 const schema = JSON.parse(await readFile(new URL('../../../fixtures/schemas/entry.json',import.meta.url),'utf8'));
 const client = await runtime.Client.open({path:join(dir,'client.sqlite'),schema});
 return {client, async close() { await client.close(); await rm(dir,{recursive:true,force:true}); }};
}
async function until(predicate,what='condition') {
 const deadline=Date.now()+5000;
 while(Date.now()<deadline) { if(await predicate()) return; await new Promise(r=>setTimeout(r,5)); }
 throw Error(`${what} timed out`);
}
const ack = (sub, heads={}) => JSON.stringify({type:'subscribed',cursors:Object.fromEntries(sub.channels.map(c=>[c,heads[c]??0]))});
const page = (text, cursor=0, stamp=cursor+1) => ({cursors:{scope:{from:cursor,to:cursor+1,head:cursor+1}},changes:[{model:'Entry',identity:{id:'live'},stamp,state:{text,note:null}}]});
/** The `bootstrap` part of a status snapshot before anything asked for a load. */
const notRequested={phase:'not-requested',error:null};
/**
 * One terminal bootstrap page covering the whole requested interval, with the
 * channel head it observed - the barrier completion then waits for
 * ([#151](https://github.com/zanminwang/axton/issues/151)).
 */
const loaded=(body,head=body.until)=>({mode:'bootstrap',channel:body.channel,from:body.after,to:body.until,until:body.until,head,records:[]});
/**
 * A fake server whose handshake acknowledges `heads` and whose pull answers when
 * `hold` resolves. A bootstrap request is answered by `load` behind its own
 * `loadHold`, and a `load` that answers `null` is refused with HTTP 400.
 */
async function fakeServer(heads={scope:0}) {
 const sockets=[];const handshakes=[];const pulls=[];let hold=Promise.resolve();let answer=(body)=>({cursors:{scope:{from:body.cursors.scope,to:heads.scope,head:heads.scope}},changes:[]});
 let loadHold=Promise.resolve();let load=(body)=>loaded(body,heads.scope);
 const http=createServer(async(req,res)=>{
  const chunks=[];for await(const c of req)chunks.push(c);const body=JSON.parse(Buffer.concat(chunks));
  pulls.push(body);
  if(body.mode==='bootstrap'){
   await loadHold;const answered=load(body);
   if(answered===null){res.statusCode=400;res.end('the server refuses this interval');return;}
   res.end(JSON.stringify(answered));return;
  }
  await hold;res.end(JSON.stringify(answer(body)));
 });
 await new Promise(r=>http.listen(0,'127.0.0.1',r));
 const ws=new WebSocketServer({server:http});
 ws.on('connection',s=>{sockets.push(s);s.on('message',m=>{const sub=JSON.parse(m);handshakes.push(sub);s.send(ack(sub,heads));});});
 return {sockets,handshakes,pulls,heads,
  get loads(){return pulls.filter(p=>p.mode==='bootstrap');},
  set hold(promise){hold=promise;},set answer(fn){answer=fn;},
  set loadHold(promise){loadHold=promise;},set load(fn){load=fn;},
  config:{url:`http://127.0.0.1:${http.address().port}`,token:'secret'},
  async close(){for(const s of ws.clients)s.terminate();await new Promise(r=>ws.close(r));await new Promise(r=>http.close(r));}};
}

test('one handle per subscription identity: concurrent and repeated calls coalesce', async()=>{
 const fixture=await openClient();const {client}=fixture;
 try {
  const [first,second]=await Promise.all([client.subscribe('scope'),client.subscribe('scope')]);
  assert.equal(first,second,'concurrent calls obtain one cached handle');
  assert.equal(await client.scopes.subscribe('scope'),first,'the scopes facade shares the cache');
  assert.equal(first.scope,'scope');
  assert.deepEqual({...first.status},{active:true,initialization:'pending',connection:'offline',bootstrap:notRequested},
   'registered offline: durable intent with no boundary and no transport');
  assert.deepEqual((await client.syncState()).channels,['scope']);
  assert.deepEqual((await client.syncState()).cursors,{},'an uninitialized subscription has no cursor at all');
  const other=await client.subscribe('other');
  assert.notEqual(other,first,'another Scope is another subscription');
  // A name no socket could subscribe is refused before a row exists, so no
  // handle is handed out and no lane is left with a Scope it cannot ask for.
  for (const blank of ['','  ','\t\n'])
   await assert.rejects(()=>client.scopes.subscribe(blank),/channel must not be empty/,
    `a blank Scope name is refused: ${JSON.stringify(blank)}`);
  assert.deepEqual((await client.syncState()).channels,['other','scope'],
   'nothing of a refused registration was written');
 } finally { await fixture.close(); }
});

test('watch delivers the current snapshot, then changes, and nothing after cancellation', async()=>{
 const fixture=await openClient();const {client}=fixture;
 try {
  const subscription=await client.subscribe('scope');
  const seen=[];
  const stop=subscription.watch(status=>seen.push(status));
  assert.deepEqual(seen.map(s=>s.connection),['offline'],'the current snapshot arrives at once');
  assert.ok(Object.isFrozen(seen[0]),'a status snapshot is immutable');
  await subscription.unsubscribe();
  assert.deepEqual(seen.map(s=>[s.active,s.connection]),[[true,'offline'],[false,'stopped']]);
  stop();
  const replacement=await client.subscribe('scope');
  assert.notEqual(replacement,subscription,'a new registration is a new handle');
  await replacement.unsubscribe();
  assert.equal(seen.length,2,'a cancelled observer hears nothing more');
  // The closed handle still reads its status, and watching it delivers the
  // stopped snapshot once.
  const late=[];subscription.watch(status=>late.push(status))();
  assert.deepEqual(late.map(s=>s.connection),['stopped']);
  assert.equal(subscription.status.active,false);
 } finally { await fixture.close(); }
});

test('an observer exception is reported after the commit and changes nothing', async()=>{
 const fixture=await openClient();const {client}=fixture;
 const previous=globalThis.reportError;const reported=[];
 globalThis.reportError=error=>reported.push(error);
 const network=await fakeServer({scope:0});
 try {
  const subscription=await client.subscribe('scope');
  subscription.watch(()=>{throw Error('observer failed');});
  assert.equal(reported.length,1,'the first snapshot already reached the failing observer');
  const connection=await client.connect(network.config);
  await until(()=>network.handshakes.length===1,'the handshake');
  await until(async()=>(await client.syncState()).cursors.scope===0,'the committed boundary');
  assert.ok(reported.length>=2,`the observer failed again after the commit: ${reported.length}`);
  assert.ok(reported.every(e=>e.message==='observer failed'));
  assert.equal((await client.syncState()).channels.length,1,'nothing was rolled back');
  // A failing observer is not a transport failure: the session it fired in is
  // still the one streaming.
  network.sockets[0].send(JSON.stringify(page('streamed')));
  await until(async()=>(await client.read('Entry',{id:'live'}))?.text==='streamed','the streamed page');
  assert.equal(network.handshakes.length,1,'no reconnect followed the observer failure');
  const again=await client.subscribe('scope');
  assert.equal(again,subscription,'subscribe resolved with the same handle, never rejected');
  await connection.close();
  // The client closes while the observer is still installed: its last snapshot
  // fails too, and that failure is reported like the others.
  await fixture.close();
  assert.ok(reported.length>=3,'the stopped snapshot reached the failing observer');
 } finally { await fixture.close();globalThis.reportError=previous;await network.close(); }
});

test('status follows the committed boundary and the lane: offline, connecting, catching up, live', async()=>{
 const fixture=await openClient();const {client}=fixture;
 const network=await fakeServer({scope:0});
 try {
  const subscription=await client.subscribe('scope');
  const seen=[];subscription.watch(status=>seen.push(status.connection));
  assert.equal(subscription.status.initialization,'pending');
  const connection=await client.connect(network.config);
  await until(()=>subscription.status.connection==='live'&&subscription.status.initialization==='ready',
   'a live subscription with a committed boundary');
  assert.ok(seen.includes('connecting'),`the lane was seen connecting: ${seen}`);
  // The server publishes while the socket is closed: the next handshake
  // acknowledges a head above the committed cursor and one HTTP catch-up runs.
  await connection.pause();
  await until(()=>subscription.status.connection==='offline','a paused lane');
  network.heads.scope=1;
  const gate=Promise.withResolvers();network.hold=gate.promise;
  network.answer=body=>({cursors:{scope:{from:body.cursors.scope,to:1,head:1}},changes:page('caught up').changes});
  await connection.resume();
  await until(()=>subscription.status.connection==='catching-up','a catching-up subscription');
  assert.equal(subscription.status.initialization,'ready','the boundary stays committed while catching up');
  gate.resolve();
  await until(async()=>(await client.read('Entry',{id:'live'}))?.text==='caught up','the catch-up page');
  await until(()=>subscription.status.connection==='live','live again after the catch-up');
  assert.deepEqual(network.pulls.map(p=>p.cursors),[{scope:0}],'one pull, from the committed cursor');
  await connection.close();
  assert.deepEqual({...subscription.status},{active:true,initialization:'ready',connection:'offline',bootstrap:notRequested},
   'a closed connection leaves the subscription registered and offline');
 } finally { await fixture.close();await network.close(); }
});

test('a recreated subscription is connecting until its own handshake acknowledges it', async()=>{
 const fixture=await openClient();const {client}=fixture;
 const network=await fakeServer({scope:0});
 try {
  const first=await client.subscribe('scope');
  const connection=await client.connect(network.config);
  await until(()=>first.status.connection==='live'&&first.status.initialization==='ready','a live subscription');
  await first.unsubscribe();
  const second=await client.subscribe('scope');
  assert.notEqual(second,first,'a new registration is a new handle');
  assert.deepEqual({...second.status},{active:true,initialization:'pending',connection:'connecting',bootstrap:notRequested},
   'the open session never subscribed this registration: it is not live and has no boundary');
  // The worker replaces the socket for the new membership; that handshake is
  // this subscription's own.
  await until(()=>network.handshakes.length>=2,'the socket the new membership needs');
  await until(()=>second.status.connection==='live'&&second.status.initialization==='ready',
   'the new subscription goes live on its own acknowledgement');
  assert.deepEqual({...first.status},{active:false,initialization:'ready',connection:'stopped',bootstrap:notRequested},
   'the handle it replaced stays stopped');
  await connection.close();
 } finally { await fixture.close();await network.close(); }
});

// A replica rebuild carries the Scope names over with fresh identities, so a
// handle from before it names a registration that no longer exists: the one
// public path where an identity-fenced removal answers "nothing went" while the
// Scope has a live registration ([#150](https://github.com/zanminwang/axton/issues/150)).
test('a stale handle from before a rebuild cannot disturb the subscription that replaced it', async()=>{
 const dir=await mkdtemp(join(tmpdir(),'axton-subscriptions-rebuild-'));
 const path=join(dir,'client.sqlite');
 const schema=JSON.parse(await readFile(new URL('../../../fixtures/schemas/entry.json',import.meta.url),'utf8'));
 const breaking=structuredClone(schema);
 breaking.models[0].fields.push({name:'due',nullable:false,type:{kind:'scalar',name:'string'}});
 const network=await fakeServer({scope:0});
 let client;
 try {
  client=await runtime.Client.open({path,schema});
  await client.subscribe('scope');
  // Unsent work keeps the incompatible file open, so the rebuild happens with
  // this client - and its handle - already alive.
  await client.mutate({name:'Create',operations:[{model:'Entry',op:'create',identity:{id:'e'},values:{text:'A',note:null}}]});
  await client.close();
  client=await runtime.Client.open({path,schema:breaking});
  const stale=await client.subscribe('scope');
  const observed=[];const stop=stale.watch(status=>observed.push({...status}));
  await client.rebuild({discardPending:true});
  assert.deepEqual({...stale.status},{active:false,initialization:'pending',connection:'stopped',bootstrap:notRequested},
   'the rebuild invalidated every handle of the replica it replaced');
  assert.deepEqual(observed.at(-1),{active:false,initialization:'pending',connection:'stopped',bootstrap:notRequested},
   'the observer was told, and the handle has no changes left');
  stop();
  const current=await client.subscribe('scope');
  assert.notEqual(current,stale,'the carried Scope is a new registration, never the same handle');
  const connection=await client.connect(network.config);
  await until(()=>current.status.connection==='live'&&current.status.initialization==='ready',
   'the carried subscription goes live');
  assert.deepEqual({...stale.status},{active:false,initialization:'pending',connection:'stopped',bootstrap:notRequested},
   'the new session acknowledges the Scope name, not the stale handle');
  // The old handle removes nothing: the Scope's current registration is another
  // identity, whose acknowledgement is not this handle's to forget.
  await stale.unsubscribe();
  assert.deepEqual((await client.syncState()).channels,['scope'],'the current registration stands');
  assert.deepEqual({...current.status},{active:true,initialization:'ready',connection:'live',bootstrap:notRequested},
   'a removal that removed nothing changes no status');
  assert.deepEqual({...stale.status},{active:false,initialization:'pending',connection:'stopped',bootstrap:notRequested});
  await connection.close();
 } finally { await client?.close();await network.close();await rm(dir,{recursive:true,force:true}); }
});

test('unsubscribe removes one registration; an old handle cannot remove its replacement', async()=>{
 const fixture=await openClient();const {client}=fixture;
 try {
  const first=await client.subscribe('scope');
  await first.unsubscribe();
  assert.deepEqual({...first.status},{active:false,initialization:'pending',connection:'stopped',bootstrap:notRequested});
  assert.deepEqual((await client.syncState()).channels,[],'the registration is gone');
  await first.unsubscribe();
  assert.deepEqual((await client.syncState()).channels,[],'repeating it on a closed handle is a no-op');
  const second=await client.subscribe('scope');
  assert.notEqual(second,first);
  await first.unsubscribe();
  assert.deepEqual((await client.syncState()).channels,['scope'],
   'an old handle must not delete the subscription that replaced it');
  assert.equal(second.status.active,true);
  // The Scope-named form removes whatever is registered and closes its handle.
  await client.unsubscribe('scope');
  assert.deepEqual((await client.syncState()).channels,[]);
  assert.deepEqual({...second.status},{active:false,initialization:'pending',connection:'stopped',bootstrap:notRequested});
 } finally { await fixture.close(); }
});

test('closing the client stops handles and deletes nothing; work through them fails closed', async()=>{
 const dir=await mkdtemp(join(tmpdir(),'axton-subscriptions-'));
 const schema=JSON.parse(await readFile(new URL('../../../fixtures/schemas/entry.json',import.meta.url),'utf8'));
 const path=join(dir,'client.sqlite');
 try {
  const client=await runtime.Client.open({path,schema});
  const subscription=await client.subscribe('scope');
  const seen=[];subscription.watch(status=>seen.push(status));
  await client.close();
  assert.deepEqual({...subscription.status},{active:false,initialization:'pending',connection:'stopped',bootstrap:notRequested});
  assert.deepEqual(seen.map(s=>s.connection),['offline','stopped'],'observers hear the stop, then are cancelled');
  const failed=await subscription.unsubscribe().then(()=>null,error=>error);
  assert.equal(failed?.code,'subscription.closed','a stopped handle cannot commit work');
  const reopened=await runtime.Client.open({path,schema});
  try {
   assert.deepEqual((await reopened.syncState()).channels,['scope'],'closing the client deleted nothing');
   const restored=await reopened.subscribe('scope');
   assert.equal(restored.status.active,true);
  } finally { await reopened.close(); }
 } finally { await rm(dir,{recursive:true,force:true}); }
});

// Whole-Scope bootstrap through the handle
// ([#151](https://github.com/zanminwang/axton/issues/151)): registration is
// eager and local, completion is a committed transition, and the status says
// which of the two the run is waiting for.
test('bootstrap is submitted eagerly, concurrent calls share one run, and the barrier completes it', async()=>{
 const fixture=await openClient();const {client}=fixture;
 const network=await fakeServer({scope:0});
 try {
  const subscription=await client.subscribe('scope');
  const phases=[];subscription.watch(status=>{if(phases.at(-1)!==status.bootstrap.phase)phases.push(status.bootstrap.phase);});
  assert.deepEqual({...subscription.status.bootstrap},notRequested,'a registration asks for no load of its own');
  const connection=await client.connect(network.config);
  await until(()=>subscription.status.initialization==='ready','the committed boundary');
  // The page is held: the calls submit their registration when they are made,
  // so the task runs and the status moves with nobody awaiting the promises.
  const gate=Promise.withResolvers();network.loadHold=gate.promise;
  network.load=body=>loaded(body,3);
  const first=subscription.bootstrap();
  const second=subscription.bootstrap();
  let settled=false;const both=Promise.all([first,second]).then(()=>{settled=true;});
  await until(()=>subscription.status.bootstrap.phase==='loading','a registered load');
  await until(()=>network.loads.length===1,'the one page the run asked for');
  assert.equal(settled,false,'no call resolved before the completion committed');
  gate.resolve();
  // The terminal page fixed the barrier at the head it saw, which delivery has
  // not reached: the run waits for the stream, not for another page.
  await until(()=>subscription.status.bootstrap.phase==='catching-up','the fixed barrier');
  assert.equal(settled,false,'a barrier delivery has not reached does not complete the run');
  assert.equal(network.loads.length,1,'two concurrent calls registered one task');
  assert.deepEqual(network.loads[0].after,0,'the page asked for the interval from committed progress');
  network.sockets[0].send(JSON.stringify({cursors:{scope:{from:0,to:3,head:3}},changes:[{model:'Entry',identity:{id:'live'},stamp:3,state:{text:'delivered',note:null}}]}));
  await both;
  assert.deepEqual({...subscription.status.bootstrap},{phase:'complete',error:null});
  assert.ok(Object.isFrozen(subscription.status.bootstrap),'the load status is immutable too');
  assert.deepEqual(phases,['not-requested','loading','catching-up','complete'],
   'the observed transitions, in the order they committed');
  assert.equal(network.loads.length,1,'completion asked for no further page');
  await connection.close();
 } finally { await fixture.close();await network.close(); }
});

test('a load registered before initialization waits for the boundary, and a closing client rejects its waiters', async()=>{
 const fixture=await openClient();const {client}=fixture;
 try {
  const subscription=await client.subscribe('scope');
  const first=subscription.bootstrap();const second=subscription.bootstrap();
  const outcome=Promise.all([first,second]).then(()=>'resolved',error=>error.code);
  await until(()=>subscription.status.bootstrap.phase==='waiting-for-initialization',
   'a registered load with no boundary yet');
  assert.equal(subscription.status.connection,'offline','waiting for connectivity is not failure');
  // Closing the client is not a failure of the durable task: it rejects the
  // waiters of this process and removes nothing.
  await fixture.close();
  assert.equal(await outcome,'client_closed');
  assert.equal((await subscription.bootstrap().then(()=>null,error=>error))?.code,'subscription.closed',
   'a stopped handle starts nothing');
 } finally { await fixture.close(); }
});

test('a completed bootstrap resolves offline, and an interrupted one resumes on the next client', async()=>{
 const dir=await mkdtemp(join(tmpdir(),'axton-bootstrap-'));
 const path=join(dir,'client.sqlite');
 const schema=JSON.parse(await readFile(new URL('../../../fixtures/schemas/entry.json',import.meta.url),'utf8'));
 const network=await fakeServer({scope:0});
 let client;
 try {
  client=await runtime.Client.open({path,schema});
  let subscription=await client.subscribe('scope');
  let connection=await client.connect(network.config);
  await until(()=>subscription.status.initialization==='ready','the committed boundary');
  const gate=Promise.withResolvers();network.loadHold=gate.promise;
  const interrupted=subscription.bootstrap().then(()=>null,error=>error);
  await until(()=>network.loads.length===1,'the page the run asked for');
  await client.close();
  assert.equal((await interrupted)?.code,'client_closed','the waiters of this process were rejected');
  gate.resolve();
  // A reopened client resumes the same run from its committed progress, with no
  // new call at all.
  client=await runtime.Client.open({path,schema});
  subscription=await client.subscribe('scope');
  const observed=[];subscription.watch(status=>{if(observed.at(-1)!==status.bootstrap.phase)observed.push(status.bootstrap.phase);});
  connection=await client.connect(network.config);
  await until(()=>subscription.status.bootstrap.phase==='complete','the resumed run completes with no new call');
  assert.deepEqual(network.loads.map(l=>[l.after,l.until]),[[0,0],[0,0]],
   'the resumed run asked for the same interval from the same progress, and registered no second run');
  assert.ok(observed.includes('loading'),`the resumed run was observable while it ran: ${observed}`);
  await connection.close();
  await client.close();
  // Completion is durable and local: a client with no network at all resolves
  // the call from what was committed.
  client=await runtime.Client.open({path,schema});
  subscription=await client.subscribe('scope');
  await subscription.bootstrap();
  assert.deepEqual({...subscription.status.bootstrap},{phase:'complete',error:null});
  assert.equal(subscription.status.connection,'offline','no transport was needed');
  assert.equal(network.loads.length,2,'a completed run asks for nothing more');
 } finally { await client?.close();await network.close();await rm(dir,{recursive:true,force:true}); }
});

test('unsubscribing rejects that handle every load it was waiting for', async()=>{
 const fixture=await openClient();const {client}=fixture;
 const network=await fakeServer({scope:0});
 try {
  const subscription=await client.subscribe('scope');
  const connection=await client.connect(network.config);
  await until(()=>subscription.status.initialization==='ready','the committed boundary');
  const gate=Promise.withResolvers();network.loadHold=gate.promise;
  const pending=subscription.bootstrap().then(()=>null,error=>error);
  await until(()=>subscription.status.bootstrap.phase==='loading','a registered load');
  // The row and its load state go together: the epoch's task is gone, so the
  // waiters of that handle cannot be kept.
  await subscription.unsubscribe();
  assert.equal((await pending)?.code,'subscription.closed');
  assert.equal((await subscription.bootstrap().then(()=>null,error=>error))?.code,'subscription.closed');
  assert.equal(subscription.status.active,false);
  gate.resolve();
  await connection.close();
 } finally { await fixture.close();await network.close(); }
});

test('a failed run stays failed for the calls it belongs to; an explicit retry is another run', async()=>{
 const fixture=await openClient();const {client}=fixture;
 const network=await fakeServer({scope:0});
 try {
  const subscription=await client.subscribe('scope');
  const connection=await client.connect(network.config,{onError(){}});
  await until(()=>subscription.status.initialization==='ready','the committed boundary');
  network.load=()=>null;
  const failure=subscription.bootstrap().then(()=>null,error=>error);
  await until(()=>subscription.status.bootstrap.phase==='failed','the refused page fails the run');
  const stored=subscription.status.bootstrap.error;
  assert.equal(stored?.code,'bootstrap.request_rejected');
  assert.match(stored.message,/refused with HTTP 400/);
  assert.deepEqual(Object.keys(stored),['code','message'],'the stored record reports are not public status');
  assert.equal((await failure)?.code,'bootstrap.request_rejected','the caller was rejected with the stored failure');
  assert.equal((await failure)?.message,stored.message);
  // The retry is a new run, and it cannot turn the call that failed into a
  // success.
  network.load=body=>loaded(body);
  await subscription.bootstrap();
  assert.deepEqual({...subscription.status.bootstrap},{phase:'complete',error:null});
  assert.equal((await failure)?.code,'bootstrap.request_rejected','the earlier call stayed failed');
  assert.equal(network.loads.length,2,'one page per run');
  await connection.close();
 } finally { await fixture.close();await network.close(); }
});

test('an observer that throws on a load transition is reported and changes nothing', async()=>{
 const fixture=await openClient();const {client}=fixture;
 const previous=globalThis.reportError;const reported=[];
 globalThis.reportError=error=>reported.push(error);
 const network=await fakeServer({scope:0});
 try {
  const subscription=await client.subscribe('scope');
  const connection=await client.connect(network.config);
  await until(()=>subscription.status.initialization==='ready','the committed boundary');
  // Installed after the boundary, so every exception below belongs to a load
  // transition and nothing else.
  const before=reported.length;
  subscription.watch(status=>{throw Error(`observer failed at ${status.bootstrap.phase}`);});
  assert.equal(reported.length,before+1,'the first snapshot already reached the failing observer');
  const gate=Promise.withResolvers();network.loadHold=gate.promise;
  network.load=body=>loaded(body,3);
  const loading=subscription.bootstrap();
  await until(()=>subscription.status.bootstrap.phase==='loading','a registered load');
  gate.resolve();
  await until(()=>subscription.status.bootstrap.phase==='catching-up','the fixed barrier');
  network.sockets[0].send(JSON.stringify({cursors:{scope:{from:0,to:3,head:3}},changes:[]}));
  // Every committed transition reached the failing observer, and none of them
  // was undone, retried or turned into a transport failure by it.
  await loading;
  assert.deepEqual({...subscription.status.bootstrap},{phase:'complete',error:null});
  const phases=reported.slice(before).map(error=>error.message.replace('observer failed at ',''));
  assert.deepEqual(phases,['not-requested','loading','catching-up','complete'],
   `each committed phase reached the observer that throws: ${phases}`);
  assert.equal(network.loads.length,1,'the exceptions asked for no further page');
  assert.equal(network.handshakes.length,1,'and reopened no socket');
  await subscription.bootstrap();
  assert.deepEqual({...subscription.status.bootstrap},{phase:'complete',error:null},
   'the committed completion is what a later call resolves from');
  await connection.close();
 } finally { await fixture.close();globalThis.reportError=previous;await network.close(); }
});

/**
 * The handles driven through a Bridge over a scripted runtime, so what the
 * runtime publishes is exact: the snapshot queued behind the task that names
 * its observer, a terminal snapshot, and every failure code a parked
 * `scopeBootstrap` can end with ([#134](https://github.com/zanminwang/axton/issues/134)).
 * The runtime decides all of it; these tests pin what the handle makes of it.
 */
const statusOf=(over={})=>({active:true,initialization:'ready',connection:'live',bootstrap:notRequested,...over});
const snapshot=(status,closed)=>({kind:'subscription',scope:'scope',subscriptionId:1,status,...(closed?{closed:true}:{})});
const changed=(status,closed)=>({type:'observerChanged',observerId:'7',snapshot:snapshot(status,closed)});
const done=(requestId,value=null)=>({type:'taskCompleted',requestId,ok:true,value});
const failed=(requestId,error,details)=>({type:'taskCompleted',requestId,ok:false,value:null,error,...(details?{details}:{})});
async function scriptedRuntime() {
 let wake;let observed=false;const outbox=[];const tasks=[];const reported=[];
 const later=()=>setImmediate(()=>wake('1'));
 const carrier={
  runtimeOpen(request,wakeRuntime){wake=wakeRuntime;outbox.push(done(JSON.parse(request).requestId,{clientId:'c',schema:{rebuilt:false,pending:null,lastRebuild:null}}));later();return '1';},
  runtimeSubmit(runtimeId,message){
   const input=JSON.parse(message);
   if(input.type==='close')outbox.push({type:'runtimeClosed'});
   if(input.type!=='task'){later();return;}
   tasks.push({...input.command,requestId:input.requestId});
   switch(input.command.kind){
    // The observer's first snapshot rides in the same batch, after the task.
    // A repeated call for a known identity publishes nothing: the status is unchanged.
    case 'scopeSubscribe':outbox.push(done(input.requestId,{state:{scope:'scope',subscriptionId:1,startingCursor:0,cursor:0},observerId:'7'}));
     if(!observed){observed=true;outbox.push(changed(statusOf()));}break;
    case 'scopeUnsubscribe':observed=false;outbox.push(changed(statusOf({active:false,connection:'stopped'}),true),done(input.requestId,{removed:true}));break;
    // A bootstrap parks until the test publishes its outcome.
   }
   later();
  },
  runtimeDrain:()=>JSON.stringify(outbox.splice(0)),
  runtimeDetach(){},
 };
 const {bridge}=await Bridge.open(carrier,{path:'unused',schema:{enums:[],models:[]}});
 const subscriptions=new Subscriptions(bridge,error=>reported.push(error));
 return {bridge,subscriptions,tasks,reported,
  publish(...events){if(events.some(e=>e.snapshot?.closed))observed=false;outbox.push(...events);wake('1');},
  bootstraps:()=>tasks.filter(t=>t.kind==='scopeBootstrap')};
}

test('a status observer attached right after subscribe resolves sees the current snapshot once', async()=>{
 const {subscriptions,publish}=await scriptedRuntime();
 const subscription=await subscriptions.subscribe('scope');
 const seen=[];subscription.watch(status=>seen.push(status));
 assert.deepEqual(seen,[statusOf()],'the snapshot published behind the task, exactly once');
 assert.ok(Object.isFrozen(seen[0])&&Object.isFrozen(seen[0].bootstrap),'the runtime snapshot is immutable');
 assert.equal(subscription.status,seen[0]);
 assert.equal(await subscriptions.subscribe('scope'),subscription,'the same identity answers the cached handle');
 assert.equal(seen.length,1,'a repeated subscribe publishes nothing new');
 const failure={code:'bootstrap.request_rejected',message:'HTTP 403'};
 publish(changed(statusOf({connection:'catching-up',bootstrap:{phase:'failed',error:failure}})));
 assert.deepEqual(seen.map(s=>s.connection),['live','catching-up']);
 assert.deepEqual(subscription.status.bootstrap.error,failure);
 assert.ok(Object.isFrozen(subscription.status.bootstrap.error));
});

test('a terminal snapshot ends the handle: observers hear it once, then nothing, and work through it is local', async()=>{
 const {subscriptions,publish,tasks}=await scriptedRuntime();
 const subscription=await subscriptions.subscribe('scope');
 const seen=[];subscription.watch(status=>seen.push(status));
 // A removal the runtime committed elsewhere: the Scope-named form, or a rebuild.
 publish(changed(statusOf({active:false,connection:'stopped'}),true));
 assert.deepEqual(seen.map(s=>[s.active,s.connection]),[[true,'live'],[false,'stopped']]);
 publish(changed(statusOf()));
 assert.equal(seen.length,2,'nothing follows a terminal snapshot');
 assert.equal(subscription.status.active,false);
 const late=[];subscription.watch(status=>late.push(status.connection))();
 assert.deepEqual(late,['stopped'],'a closed handle delivers its last snapshot once');
 const before=tasks.length;
 await subscription.unsubscribe();
 assert.equal((await subscription.bootstrap().then(()=>null,error=>error))?.code,'subscription.closed');
 assert.equal(tasks.length,before,'a closed handle submits nothing');
 const replacement=await subscriptions.subscribe('scope');
 assert.notEqual(replacement,subscription,'the forgotten identity is a new handle');
});

test('unsubscribe resolves after the terminal snapshot closed the handle', async()=>{
 const {subscriptions,tasks}=await scriptedRuntime();
 const subscription=await subscriptions.subscribe('scope');
 await subscription.unsubscribe();
 assert.deepEqual(tasks.at(-1),{kind:'scopeUnsubscribe',scope:'scope',subscriptionId:1,requestId:tasks.at(-1).requestId});
 assert.deepEqual({...subscription.status},statusOf({active:false,connection:'stopped'}));
 const before=tasks.length;
 await subscription.unsubscribe();
 assert.equal(tasks.length,before,'repeating it on a removed handle is a no-op');
});

test('a registration the engine refuses as closed rejects with subscription.closed', async()=>{
 // The removal committed between this command and the handle's own close, so
 // the engine - not the handle - is what knows the registration is gone.
 const {subscriptions,publish,bootstraps}=await scriptedRuntime();
 const subscription=await subscriptions.subscribe('scope');
 const refused=subscription.bootstrap().then(()=>null,error=>error);
 const other=subscription.bootstrap().then(()=>null,error=>error);
 assert.equal(bootstraps().length,2,'eager: submitted when called');
 assert.deepEqual(bootstraps()[0],{kind:'scopeBootstrap',scope:'scope',subscriptionId:1,requestId:bootstraps()[0].requestId});
 const text='subscription.closed: subscription 1 for scope is closed; it has no bootstrap state';
 publish(failed(bootstraps()[0].requestId,text,{code:'subscription.closed'}),
  failed(bootstraps()[1].requestId,'the database is locked'));
 const rejected=await refused;
 assert.equal(rejected?.code,'subscription.closed',`the engine's text must not reach the caller: ${rejected?.message}`);
 assert.equal(rejected.message,'subscription.closed');
 // An unrelated engine failure is still the caller's to see, unchanged.
 const unrelated=await other;
 assert.equal(unrelated?.message,'the database is locked');
 assert.equal(unrelated.code,undefined);
});

test('bootstrap maps every code the runtime fails a waiter with to its public error', async()=>{
 const {subscriptions,publish,bootstraps}=await scriptedRuntime();
 const subscription=await subscriptions.subscribe('scope');
 const outcomes=[
  ['subscription.closed',{code:'subscription.closed'}],
  ['client_closed',{code:'client_closed'}],
  ['bootstrap.superseded',{code:'bootstrap.superseded'}],
  ['HTTP 403',{code:'bootstrap.request_rejected',message:'HTTP 403'}],
  // Tasks still queued when the runtime closed carry no details.
  ['client_closed',undefined],
 ];
 const calls=outcomes.map(()=>subscription.bootstrap().then(()=>'resolved',error=>error));
 const resolved=subscription.bootstrap();
 publish(...outcomes.map(([error,details],i)=>failed(bootstraps()[i].requestId,error,details)),
  done(bootstraps().at(-1).requestId));
 const errors=await Promise.all(calls);
 assert.deepEqual(errors.map(e=>e.code),['subscription.closed','client_closed','bootstrap.superseded','bootstrap.request_rejected','client_closed']);
 assert.deepEqual(errors.map(e=>e.message),['subscription.closed','client_closed',
  'the bootstrap run of scope this call waited for was superseded','HTTP 403','client_closed']);
 assert.equal(await resolved,undefined,'a completed run resolves with nothing');
});

test('a waiter whose run was superseded is rejected, never resolved by the newer one', async()=>{
 // Run 1's waiter is failed by the runtime once run 2 is observed; the newer
 // run settles only the call that belongs to it.
 const {subscriptions,publish,bootstraps}=await scriptedRuntime();
 const subscription=await subscriptions.subscribe('scope');
 const first=subscription.bootstrap().then(()=>null,error=>error);
 publish(changed(statusOf({bootstrap:{phase:'loading',error:null}})));
 const second=subscription.bootstrap().then(()=>null,error=>error);
 publish(failed(bootstraps()[0].requestId,'bootstrap.superseded',{code:'bootstrap.superseded'}));
 assert.equal((await first)?.code,'bootstrap.superseded','run 1 must not resolve from run 2');
 publish(changed(statusOf({bootstrap:{phase:'complete',error:null}})),done(bootstraps()[1].requestId));
 assert.equal(await second,null);
 assert.deepEqual({...subscription.status.bootstrap},{phase:'complete',error:null});
});

test('a closing client marks its handles stopped; one the runtime never ended stops locally', async()=>{
 const {subscriptions,publish,bootstraps,tasks}=await scriptedRuntime();
 const subscription=await subscriptions.subscribe('scope');
 const seen=[];subscription.watch(status=>seen.push(status.connection));
 const pending=subscription.bootstrap().then(()=>null,error=>error);
 subscriptions.close();
 // The runtime's close: the waiter fails `client_closed`, then the terminal snapshot.
 publish(failed(bootstraps()[0].requestId,'client_closed',{code:'client_closed'}),changed(statusOf({active:false,connection:'stopped'}),true));
 assert.equal((await pending)?.code,'client_closed');
 assert.deepEqual(seen,['live','stopped']);
 const before=tasks.length;
 assert.equal((await subscription.unsubscribe().then(()=>null,error=>error))?.code,'subscription.closed',
  'a stopped handle cannot commit work');
 assert.equal(tasks.length,before);
 // A runtime that ended without a terminal snapshot for this one.
 const other=await scriptedRuntime();
 const orphan=await other.subscriptions.subscribe('scope');
 const heard=[];orphan.watch(status=>heard.push(status));
 other.subscriptions.close();
 await other.bridge.close();
 other.subscriptions.closed();
 assert.deepEqual(heard.map(s=>[s.active,s.connection]),[[true,'live'],[false,'stopped']]);
 assert.ok(Object.isFrozen(orphan.status));
});

test('an observer that throws is reported and hears every later snapshot', async()=>{
 const {subscriptions,publish,reported}=await scriptedRuntime();
 const subscription=await subscriptions.subscribe('scope');
 const seen=[];
 subscription.watch(status=>{seen.push(status.connection);throw Error(`observer failed at ${status.connection}`);});
 publish(changed(statusOf({connection:'offline'})),changed(statusOf({connection:'connecting'})));
 assert.deepEqual(seen,['live','offline','connecting']);
 assert.deepEqual(reported.map(e=>e.message),['observer failed at live','observer failed at offline','observer failed at connecting']);
 assert.equal(subscription.status.connection,'connecting','the exception changed nothing');
});

test('unsubscribing a Scope while a bootstrap is submitted rejects it as closed', async()=>{
 const fixture=await openClient();const {client}=fixture;
 try {
  const subscription=await client.subscribe('scope');
  // The removal and the registration are submitted in that order on the one
  // serialized command path: whether the handle or the engine sees the closed
  // registration first, the caller gets the same code.
  const removed=client.unsubscribe('scope');
  const rejected=subscription.bootstrap().then(()=>null,error=>error);
  await removed;
  assert.equal((await rejected)?.code,'subscription.closed');
  assert.equal(subscription.status.active,false);
 } finally { await fixture.close(); }
});
