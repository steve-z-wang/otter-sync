import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {createExample} from './fixtures/round-trip/server.mts';
import {Client} from '../../packages/client-js/index.mts';
import {syncProtocol,declaredModels} from './protocol-fixture.mjs';

const repoRoot=fileURLToPath(new URL('../..',import.meta.url));
/**
 * Run one of the Dart clients beside this file. It prints `READY` once its
 * subscriptions are initialized at the heads their handshake acknowledged;
 * `onReady` then publishes what it is meant to receive, because a new
 * subscription loads nothing published before its origin (#150) and #151 owns
 * the explicit whole-Scope bootstrap().
 */
const runDartClient=(script,args,onReady)=>new Promise((resolve,reject)=>{
 const child=spawn('dart',[`--packages=${join(repoRoot,'packages/dart/.dart_tool/package_config.json')}`,join(repoRoot,'integration/e2e',script),...args],{cwd:join(repoRoot,'packages/dart'),env:{...process.env,AXTON_LIBRARY:process.env.AXTON_LIBRARY??join(repoRoot,`target/debug/libaxton_dart.${process.platform==='darwin'?'dylib':'so'}`)},stdio:['ignore','pipe','inherit']});
 let output='';let waiting=onReady;
 child.stdout.on('data',chunk=>{process.stdout.write(chunk);output+=chunk;if(waiting&&output.includes('READY\n')){const publish=waiting;waiting=undefined;Promise.resolve(publish()).catch(reject);}});
 child.on('error',reject);
 child.on('exit',code=>code===0?resolve():reject(Error(`${script} exited ${code}`)));
});

test('Node SDK -> native Rust -> HTTP -> Rust backend -> Prisma -> SQLite, then Dart',async()=>{
 const app=await createExample();const directory=await mkdtemp(join(tmpdir(),'axton-e2e-'));let client;let server;
 try{
  await app.initialize();server=await app.listen(0);const url=server.url;
  const transport=async(kind,body)=>{const response=await fetch(`${url}/sync/${kind==='push'?'mutations':'pull'}`,{method:'POST',headers:{authorization:'Bearer demo-user','content-type':'application/json'},body});if(!response.ok)throw Error(`HTTP ${response.status}: ${await response.text()}`);return response.text();};
  client=await Client.open({path:join(directory,'client.sqlite'),schema:app.schema});
  // A subscription's origin is the first head a handshake acknowledges (#150),
  // so one live session establishes it and nothing published earlier is loaded.
  // The raw wire fixture below then pulls from that origin; whole-Scope loading
  // is #151's bootstrap().
  const subscription=await client.subscribe('book:demo');
  const first=await client.connect({url,token:'demo-user'});
  for(let i=0;i<1000&&subscription.status.initialization!=='ready';i++)await new Promise(r=>setTimeout(r,10));
  assert.equal(subscription.status.initialization,'ready','the handshake committed the first boundary');
  assert.equal(await client.read('Entry',{id:'entry-1'}),null,'subscribing loaded no record published before the origin');
  await first.close();
  await app.notify();
  await syncProtocol(client,transport,declaredModels(app.schema));assert.equal((await client.read('Entry',{id:'entry-1'})).text,'Hello from the server');
  await client.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:'  offline edit  '}}]});
  assert.equal((await client.read('Entry',{id:'entry-1'})).text,'  offline edit  ');const frozen=await client.freeze();await client.close();
  client=await Client.open({path:join(directory,'client.sqlite'),schema:app.schema});assert.equal(await client.freeze(),frozen);
  let dropped=false;await assert.rejects(()=>syncProtocol(client,async(kind,body)=>{const result=await transport(kind,body);if(kind==='push'&&!dropped){dropped=true;throw Error('lost ACK after COMMIT');}return result;},declaredModels(app.schema)),/lost ACK/);
  const calls=app.handlerCalls;assert.equal((await client.syncState()).pending,1);await syncProtocol(client,transport,declaredModels(app.schema));assert.equal(app.handlerCalls,calls);assert.equal((await client.read('Entry',{id:'entry-1'})).text,'offline edit');assert.equal((await client.syncState()).pending,0);assert.equal((await client.syncState()).beforeImages,0);
  await client.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:'reject'}}]});await syncProtocol(client,transport,declaredModels(app.schema));assert.equal((await client.read('Entry',{id:'entry-1'})).text,'offline edit');assert.equal((await client.syncState()).rejections[0].code,'entry.denied');
  const gate=Promise.withResolvers();const entered=Promise.withResolvers();let held=false;
  await client.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:'first'}}]});
  const syncing=syncProtocol(client,async(kind,body)=>{const result=await transport(kind,body);if(kind==='push'&&!held){held=true;entered.resolve();await gate.promise;}return result;},declaredModels(app.schema));
  await entered.promise;
  try {await Promise.race([client.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:'offline edit'}}]}),new Promise((_,reject)=>setTimeout(()=>reject(Error('local writes blocked by network')),500))]);}
  finally {gate.resolve();await syncing;}
  assert.equal((await client.read('Entry',{id:'entry-1'})).text,'offline edit');
  const background=await client.connect({url,token:'demo-user'});
  const waitSettled=async()=>{for(let i=0;i<200;i++){if((await client.syncState()).pending===0)return;await new Promise(r=>setTimeout(r,10));}throw Error('background sync did not settle');};
  try{await client.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:'  background  '}}]});await waitSettled();assert.equal((await client.read('Entry',{id:'entry-1'})).text,'background');await background.pause();await client.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:'  resumed  '}}]});await new Promise(r=>setTimeout(r,30));assert.equal((await client.syncState()).pending,1);await background.resume();await waitSettled();assert.equal((await client.read('Entry',{id:'entry-1'})).text,'resumed');}finally{await background.close();}
  await runDartClient('dart_client.dart',[url,directory],()=>app.notify());
  assert.equal((await app.db.entry.findUnique({where:{id:'entry-1'}})).text,'from Dart');
 }finally{await client?.close();await server?.close();await app.close();await rm(directory,{recursive:true,force:true});}
});

test('documented CLI keeps offline edits local and syncs them on online', { timeout: 30000 }, async () => {
 const app = await createExample();
 const directory = await mkdtemp(join(tmpdir(), 'axton-cli-'));
 let child;
 let output = '';
 let ended;
 try {
  await app.initialize();
  const server = await app.listen(0);
  const before = await app.db.entry.findUnique({ where: { id: 'entry-1' } });
  const root = fileURLToPath(new URL('../..', import.meta.url));
  child = spawn(process.execPath, ['integration/e2e/fixtures/round-trip/client.mts'], {
   cwd: root,
   env: { ...process.env, AXTON_URL: server.url, AXTON_DATABASE: join(directory, 'client.sqlite') },
   stdio: ['pipe', 'pipe', 'pipe'],
  });
  const exited = new Promise((resolve, reject) => {
   child.once('error', reject);
   child.once('exit', code => { ended = code; resolve(code); });
  });
  child.stdout.on('data', data => { output += data; });
  child.stderr.on('data', data => { output += data; });
  const waitFor = async (condition, label) => {
   const deadline = Date.now() + 10000;
   while (Date.now() < deadline) {
    if (await condition()) return;
    if (ended !== undefined) throw Error(`CLI exited ${ended}: ${output}`);
    await new Promise(resolve => setTimeout(resolve, 20));
   }
   throw Error(`Timed out waiting for ${label}: ${output}`);
  };
  const command = async (line, expected) => {
   const start = output.length;
   child.stdin.write(`${line}\n`);
   await waitFor(() => output.slice(start).includes(expected) && output.slice(start).includes('> '), line);
  };
  // The CLI's subscription starts at the head its handshake acknowledges, so the
  // record seeded before it is not loaded (#150). Once it is live the backend
  // publishes again and the same record arrives on the stream.
  await waitFor(() => output.includes('book:demo ready/live'), 'the subscription reports live');
  assert.ok(!output.includes("id: 'entry-1'"), `subscribing loaded no earlier record: ${output}`);
  await app.notify();
  await waitFor(() => output.includes("id: 'entry-1'") && output.includes('> '), 'initial record');
  await command('offline', 'Sync paused.');
  await command('edit   documented draft   ', "text: '  documented draft   '");
  await command('status', 'pending: 1');
  assert.equal((await app.db.entry.findUnique({ where: { id: 'entry-1' } })).text, before.text);
  await command('online', 'Sync resumed.');
  await waitFor(() => output.includes("text: 'documented draft'"), 'normalized remote record');
  assert.equal((await app.db.entry.findUnique({ where: { id: 'entry-1' } })).text, 'documented draft');
  child.stdin.write('quit\n');
  assert.equal(await exited, 0);
 } finally {
  if (child && ended === undefined) child.kill();
  await app.close();
  await rm(directory, { recursive: true, force: true });
 }
});

test('built-in live catch-up pages, dependent pushes, watches, offline reconnect, and Dart live client', {timeout:45000}, async()=>{
 const fetchOriginal=globalThis.fetch;const pullRequests=[];
 const app=await createExample();const directory=await mkdtemp(join(tmpdir(),'axton-live-e2e-'));let reader,writer;
 const errors=[];
 const wait=async(predicate,label)=>{const deadline=Date.now()+10000;while(Date.now()<deadline){if(await predicate())return;await new Promise(r=>setTimeout(r,5));}throw Error(`${label}: ${errors.map(String)}`);};
 try{
  await app.initialize();const server=await app.listen(0);
  reader=await Client.open({path:join(directory,'reader.sqlite'),schema:app.schema});
  writer=await Client.open({path:join(directory,'writer.sqlite'),schema:app.schema});
  const readerSubscription=await reader.subscribe('book:demo');const writerSubscription=await writer.subscribe('book:demo');
  const config={url:server.url,token:'demo-user'};
  await writer.connect(config,{onError:e=>errors.push(e)});
  const connection=await reader.connect(config,{onError:e=>errors.push(e)});
  // Each subscription's origin is the head its first handshake acknowledged
  // (#150): neither client loads the record seeded before it, and the reader's
  // catch-up below starts at that origin instead of at zero.
  await wait(async()=>writerSubscription.status.initialization==='ready'&&readerSubscription.status.initialization==='ready','first initialization');
  assert.equal(await reader.read('Entry',{id:'entry-1'}),null,'a new subscription loads no record published before it');
  const origin=(await reader.syncState()).cursors['book:demo'];
  assert.ok(origin>0,`the origin is the acknowledged head, not zero: ${origin}`);
  // The reader is offline for the burst, so its reconnect has to fetch more than
  // one page over HTTP from the cursor it committed.
  await connection.pause();
  await wait(async()=>readerSubscription.status.connection==='offline','the reader lane is offline');
  await app.backend.transaction(async({tx,channel,touch})=>{
   for(let i=0;i<55;i++){await tx.entry.upsert({where:{id:`paged-${i}`},create:{id:`paged-${i}`,text:`record ${i}`},update:{text:`record ${i}`}});touch.entry({id:`paged-${i}`});channel('book:demo').entry.add({id:`paged-${i}`});}
   // The seeded record, already a member, is touched inside the burst too: it is
   // above both origins there, so the live writer and the catching-up reader both hold it.
   touch.entry({id:'entry-1'});
  });
  await wait(async()=>(await writer.query('Entry')).length>=56,'writer receives the burst');
  const observed=[];const unwatch=reader.watch('Entry',{},rows=>observed.push(rows));
  let overlapped=false;
  // Pulls carry no client id. The writer is caught up: its cursor equals the
  // head its acknowledgement carries, so it never pulls again and every pull
  // seen from here on is the reader's.
  globalThis.fetch=async(url,init)=>{
   const response=await fetchOriginal(url,init);
   if(String(url).endsWith('/sync/pull')){
    pullRequests.push(JSON.parse(init.body));
    if(!overlapped){
     overlapped=true;
     await writer.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:'during catchup'}}]});
     await wait(async()=>(await writer.syncState()).pending===0,'commit during held HTTP catchup');
    }
   }
   return response;
  };
  await connection.resume();
  await wait(async()=>(await reader.query('Entry')).length>=56 && (await reader.read('Entry',{id:'entry-1'}))?.text==='during catchup','multi-page catchup');
  assert.ok(observed.some(rows=>rows.length>=56));
  assert.ok(pullRequests.length>=2,'more than 50 records catch up via HTTP pages');
  assert.deepEqual(pullRequests[0].cursors,{'book:demo':origin},'catch-up starts at the committed cursor');
  assert.equal((await reader.readSql('SELECT starting_cursor FROM axton_subscription WHERE channel=?',['book:demo']))[0].starting_cursor,origin,'catching up did not move the origin');
  const caughtUpPulls=pullRequests.length;
  // Queue two edits to the same record. Each batch completes from its own receipt
  // (no channel page is awaited); the second push must follow the first without
  // another application event, and the row ends at the server's normalized value.
  await reader.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:' first dependent '}}]});
  await reader.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:' second dependent '}}]});
  await wait(async()=>(await reader.syncState()).pending===0,'dependent mutation completion');
  assert.equal((await reader.read('Entry',{id:'entry-1'})).text,'second dependent');
  assert.equal((await reader.syncState()).beforeImages,0,'nothing is held once the receipts have completed both batches');
  assert.equal(pullRequests.length,caughtUpPulls,'ordinary live updates do not trigger HTTP polling');
  // A client with no subscription at all: its push's response alone corrects the
  // local row, leaves nothing pending, and the result survives a reopen. The reader,
  // subscribed to the channel, receives the same record at the same stamp.
  const stampOf=async(client,id)=>{const rows=await client.readSql('SELECT stamp FROM axton_record WHERE model = ? AND identity = ?',['Entry',JSON.stringify({id})]);assert.equal(rows.length,1,`${id} has stamp evidence`);return rows[0].stamp;};
  const lonePath=join(directory,'lone.sqlite');
  let lone=await Client.open({path:lonePath,schema:app.schema});
  let loneStamp;
  try{
   await lone.transaction(tx=>tx.direct({model:'Entry',op:'create',identity:{id:'entry-1'},values:{text:'stale local copy',note:null}}));
   await lone.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:'  lone push  '}}]});
   assert.equal((await lone.read('Entry',{id:'entry-1'})).text,'  lone push  ','the prediction is visible before the push');
   assert.deepEqual((await lone.syncState()).channels,[],'the lone client follows no channel');
   const loneConnection=await lone.connect(config,{onError:e=>errors.push(e)});
   try{
    await wait(async()=>(await lone.syncState()).pending===0,'lone push completion');
    assert.equal((await lone.read('Entry',{id:'entry-1'})).text,'lone push','the response alone corrected the local row to the server value');
    assert.equal((await lone.syncState()).beforeImages,0);
    loneStamp=await stampOf(lone,'entry-1');
    assert.ok(Number.isInteger(loneStamp)&&loneStamp>0,`the receipt stamped the record: ${loneStamp}`);
   }finally{await loneConnection.close();}
   await lone.close();
   lone=await Client.open({path:lonePath,schema:app.schema});
   assert.equal((await lone.read('Entry',{id:'entry-1'})).text,'lone push','the completed state survives a reopen');
   assert.equal((await lone.syncState()).pending,0);
   assert.equal(await stampOf(lone,'entry-1'),loneStamp,'the stamp evidence survives a reopen');
  }finally{await lone.close();}
  await wait(async()=>(await reader.read('Entry',{id:'entry-1'}))?.text==='lone push','the subscribed reader receives the lone push through the channel');
  assert.equal(await stampOf(reader,'entry-1'),loneStamp,'the channel delivers the same record at the same stamp the receipt carried');
  assert.equal(pullRequests.length,caughtUpPulls,'the channel delivery did not trigger HTTP polling');
  const saved=(await reader.syncState()).cursors['book:demo'];
  await connection.pause();
  await reader.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'entry-1'},values:{text:' offline reconciled '}}]});
  await writer.mutate({name:'Edit',operations:[{model:'Entry',op:'update',identity:{id:'paged-54'},values:{text:'missed remote'}}]});await wait(async()=>(await writer.syncState()).pending===0,'remote offline edit');
  await connection.resume();await wait(async()=>(await reader.syncState()).pending===0 && (await reader.read('Entry',{id:'paged-54'}))?.text==='missed remote','offline reconnect');
  assert.equal((await reader.read('Entry',{id:'entry-1'})).text,'offline reconciled');
  assert.equal(pullRequests[caughtUpPulls].cursors['book:demo'],saved,'reconnect HTTP starts at persisted cursor');
  assert.equal(errors.length,0);
  unwatch();await connection.close();
  // The Dart clients subscribe from scratch, so nothing published so far is
  // theirs: they print READY once initialized and the backend publishes the same
  // 56 records again for them.
  await runDartClient('dart_live_client.dart',[server.url,directory],()=>app.notify(['entry-1',...Array.from({length:55},(_,i)=>`paged-${i}`)]));
 }finally{globalThis.fetch=fetchOriginal;await reader?.close();await writer?.close();await app.close();await rm(directory,{recursive:true,force:true});}
});
