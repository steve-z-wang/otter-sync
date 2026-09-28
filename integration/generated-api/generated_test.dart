import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'package:test/test.dart';
import 'generated.dart';
import '../action-contract/generated.dart' as composite;
import 'package:axton/axton.dart' show WritePort, SubmitMutationPort;
void main(){
 test('transaction Mutations queue typed args and run local through the companion port',()async{
  final port=_ScriptedTransaction();
  final mutations=TransactionMutations(port);
  final id='123e4567-e89b-42d3-a456-426614174001';
  final entry=Entry(id:id,title:'hello',note:null,at:DateTime.utc(2026),tags:const ['x'],status:Status.active);
  CompanionContext? context;
  Composition? seen;
  final Call<PublishEntryOutput> call=await mutations.publishEntry(entry:entry,composition:id,store:const PublishEntryStore.outputs(published:false),local:(local)async{
   context=local;
   seen=await local.models.composition.get(CompositionIdentity(id:id));
   await local.models.composition.delete(CompositionIdentity(id:id));
  });
  expect(seen?.title,'draft',reason:'the callback reads through its own port');
  expect(port.companions,[{'model':'Composition','op':'delete','identity':{'id':id}}]);
  expect(port.outer,isEmpty,reason:'companion writes never use the outer transaction port');
  expect(port.submitted.single['name'],'PublishEntry');
  expect(port.submitted.single['version'],1);
  expect(port.submitted.single['args'],{'entry':entry.toRecord(),'composition':id},reason:'business args only');
  expect(port.submitted.single['store'],{'published':false});
  expect(port.submitted.single['local'],isTrue);
  expect(context,isA<CompanionContext>());
  final outcome=await call.wait();
  expect((outcome as CallSuccess<PublishEntryOutput>).result.published.at,DateTime.utc(2026));
  final Call<RenameOutput> renamed=await mutations.rename(id:id,title:'x');
  expect(port.submitted.last['local'],isFalse,reason:'no callback, no companion');
  expect(port.submitted.last['store'],isNull);
  expect(port.submitted.last['args'],{'id':id,'title':'x'});
  expect(renamed.status,CallStatus.pending);
 });
 test('store hook variants expose typed identities and rows',(){
  final hooks=StoreHooks(entry:(tx,changes)async{
   for(final change in changes){
    final String id=change.identity.id;
    if(change is StoreUpsert<EntryIdentity,Entry>){
     final DateTime at=change.row.at;
     final Status status=change.row.status;
     await tx.models.entry.get(EntryIdentity(id:id));
     await tx.channels.subscribe('entry:$id');
     expect(at,isA<DateTime>());expect(status,isA<Status>());
    }else if(change is StoreDelete<EntryIdentity,Entry>){
     await tx.channels.unsubscribe('entry:$id');
    }
   }
  });
  expect(hooks.entry,isNotNull);
 });
 test('generated Dart hook decodes incoming DateTime, enum and delete identity',()async{
  final temp=await Directory.systemTemp.createTemp('generated-store-decode-');
  final id='123e4567-e89b-42d3-a456-426614174099';
  final observed=<String>[];
  final client=await GeneratedClient.open(path:'${temp.path}/state.sqlite',libraryPath:Platform.environment['AXTON_DART_LIBRARY']!,onStore:StoreHooks(entry:(tx,changes)async{
   for(final change in changes){
    if(change is StoreUpsert<EntryIdentity,Entry>){
     expect(change.row.at,DateTime.utc(2026,1,2));
     expect(change.row.status.name,'archived');
     expect(change.row.title,'server');
     observed.add('upsert:${change.identity.id}');
    }else if(change is StoreDelete<EntryIdentity,Entry>){
     observed.add('delete:${change.identity.id}');
    }
   }
  }));
  try{
   await client.mutate.createEntry(entry:Entry(id:id,title:'local',note:null,at:DateTime.utc(2026,1,1),tags:const [],status:Status.active));
   final first=jsonDecode((await client.client.freeze())!) as Map<String,dynamic>;
   await client.client.acknowledge(first['batchSequence'] as int,{
    'clientId':first['clientId'],'batchSequence':first['batchSequence'],'rejections':[],
    'records':[{'model':'Entry','identity':{'id':id},'stamp':1,'state':{'title':'server','note':null,'at':'2026-01-02T00:00:00.000Z','tags':['server'],'status':'archived'}}],
   });
   await client.mutate.removeEntries(entries:[EntryIdentity(id:id)]);
   final second=jsonDecode((await client.client.freeze())!) as Map<String,dynamic>;
   await client.client.acknowledge(second['batchSequence'] as int,{
    'clientId':second['clientId'],'batchSequence':second['batchSequence'],'rejections':[],
    'records':[{'model':'Entry','identity':{'id':id},'stamp':2,'state':null}],
   });
   expect(observed,['upsert:$id','delete:$id']);
  }finally{await client.close();await temp.delete(recursive:true);}
 });
 test('generated Dart hook decodes a composite identity from authority',()async{
  final temp=await Directory.systemTemp.createTemp('generated-store-composite-');
  final observed=<String>[];
  final client=await composite.GeneratedClient.open(path:'${temp.path}/state.sqlite',libraryPath:Platform.environment['AXTON_DART_LIBRARY']!,onStore:composite.StoreHooks(project:(tx,changes){
   for(final change in changes){
    observed.add('${change.identity.tenantId}/${change.identity.id}');
    if(change is composite.StoreUpsert<composite.ProjectIdentity,composite.Project>) expect(change.row.title,'server');
   }
  }));
  try{
   await client.mutations.link(project:const composite.Project(tenantId:'tenant',id:'project',title:'local'));
   final frozen=jsonDecode((await client.client.freeze())!) as Map<String,dynamic>;
   final mutation=(frozen['mutations'] as List).single as Map;
   await client.client.acknowledge(frozen['batchSequence'] as int,{
    'clientId':frozen['clientId'],'batchSequence':frozen['batchSequence'],'rejections':[],
    'completions':[{'callId':mutation['callId'],'outcome':{'status':'succeeded','result':{'relatedProject':null}}}],
    'records':[{'model':'Project','identity':{'tenantId':'tenant','id':'project'},'stamp':1,'state':{'title':'server'}}],
   });
   expect(observed,['tenant/project']);
  }finally{await client.close();await temp.delete(recursive:true);}
 });
 // The child exits by itself only when nothing is left attached: no runtime
 // keeps a wake registered and no NativeCallable keeps its isolate alive.
 test('a failed open leaves no runtime attached',()async{
  final temp=await Directory.systemTemp.createTemp('generated-failed-open-');
  final child=await Process.start(Platform.resolvedExecutable,['failed_open.dart','${temp.path}/state.sqlite']);
  try{final code=await child.exitCode.timeout(const Duration(seconds:3));expect(code,0,reason:await child.stderr.transform(utf8.decoder).join());}
  finally{child.kill();await temp.delete(recursive:true);}
 });
 const id='123e4567-e89b-42d3-a456-426614174000';
 final row=Entry(id:id,title:'hello',note:null,at:DateTime.utc(2026),tags:['x'],status:Status.active);
 test('source conversion, patch absence and explicit null',(){
  expect(Entry.fromRecord(row.toRecord()).at,row.at);
  expect(const EntryPatch(note:Present(null)).toRecord(),{'note':null});
  expect(const EntryPatch().toRecord(),isEmpty);
  expect((createEntry(entry:row)['operations'] as List).single['values'].containsKey('id'),false);
  expect((removeEntries(entries:[])['operations'] as List),isEmpty);
 });
 test('generated mutations and query use real native client',()async{
  final temp=await Directory.systemTemp.createTemp('generated-api-');
  final client=await GeneratedClient.open(path:'${temp.path}/state.sqlite',libraryPath:Platform.environment['AXTON_DART_LIBRARY'] ?? '../../target/debug/libaxton_dart.dylib');
  try{
   expect(await client.mutate.createEntry(entry:row),1);
   expect((await client.models.entry.get(const EntryIdentity(id:id)))?.title,'hello');
   await client.mutate.editEntry(entry:const EditEntryEntryUpdate(identity:EntryIdentity(id:id),note:Present('changed')));
   await client.mutate.editEntry(entry:const EditEntryEntryUpdate(identity:EntryIdentity(id:id),note:Present(null)));
   expect((await client.models.entry.get(const EntryIdentity(id:id)))?.note,isNull,reason:'independent mutations apply locally in order');
   final loaded=(await client.models.entry.query()).single;
   expect(loaded.note,isNull);expect(loaded.title,'hello');expect(loaded.at,row.at);
   expect((await client.models.entry.query(where:EntryFilter(at:Present(DateTime.parse('2026-01-01T01:00:00+01:00')),note:const Present(null)),orderBy:const [EntryOrder(EntryOrderField.byTitle,descending:true)],limit:1)).length,1);
   await client.mutate.addBook(book:const Book(id:'b',title:'Book'));
   await client.mutate.addComment(comment:const Comment(id:'c',bookId:'b',text:'Comment'));
   expect((await client.models.comment.book(const CommentIdentity(id:'c')))?.id,'b');
   expect((await client.models.book.comments(const BookIdentity(id:'b'))).length,1);
   await client.transaction((tx)async{
    await tx.models.book.create(const Book(id:'local',title:'Local only'));
    await tx.models.book.update(const BookIdentity(id:'local'),const BookPatch(title:Present('Local edited')));
   });
   expect((await client.models.book.get(const BookIdentity(id:'local')))?.title,'Local edited');
   // A mutation outside a transaction is its own transaction; its record's sync state is typed.
   final ordinal=await client.mutate.editEntry(entry:const EditEntryEntryUpdate(identity:EntryIdentity(id:id),note:Present('outside')));
   final state=await client.models.entry.syncState(const EntryIdentity(id:id));
   expect(state.pending.map((p)=>p.ordinal),contains(ordinal));
   expect(state.pending.every((p)=>p.phase=='queued' && !p.diverged),isTrue);
   expect(state.rejections,isEmpty);
   expect((await client.syncState())['pending'],greaterThan(0));
   expect(client.clientId,isNotEmpty);
   expect(await client.client.freeze(),isNotNull);
  }finally{await client.close();await temp.delete(recursive:true);}
 });
 // Creation defaults ([#27](https://github.com/zanminwang/axton/issues/27)):
 // the schema string survives embedding, and the native client fills only
 // omitted fields of a fresh create, once, whether local or a mutation.
 const tricky='q \'single\' "double" \'\'\' """ \$dollar \${x} \\ back\nline';
 test('create defaults fill omitted fields and round-trip escaped strings',()async{
  final draft=(schema['models'] as List).cast<Map<String,dynamic>>().singleWhere((m)=>m['name']=='Draft');
  final body=(draft['fields'] as List).cast<Map<String,dynamic>>().singleWhere((f)=>f['name']=='body');
  expect(body['createDefault'],{'kind':'literal','value':tricky});
  expect(const DraftCreate(memo:null).toCreateRecord(),{'memo':null},reason:'omission is not encoded');
  expect(const DraftCreate(memo:null,note:Present(null)).toCreateRecord(),{'note':null,'memo':null});
  final temp=await Directory.systemTemp.createTemp('generated-api-defaults-');
  final client=await GeneratedClient.open(path:'${temp.path}/state.sqlite',libraryPath:Platform.environment['AXTON_DART_LIBRARY'] ?? '../../target/debug/libaxton_dart.dylib');
  try{
   final before=DateTime.now().toUtc().subtract(const Duration(seconds:5));
   await client.transaction((tx)async{
    await tx.models.draft.create(const DraftCreate(memo:null));
    await tx.models.draft.create(const DraftCreate(memo:'explicit',body:'mine',note:Present(null)));
   });
   await client.mutate.addDraft(draft:const DraftCreate(memo:'queued'));
   final rows=await client.models.draft.query();
   expect(rows.length,3);
   final uuid=RegExp(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$');
   expect(rows.map((r)=>r.id).toSet().length,3,reason:'each create generates its own id');
   for(final row in rows){
    expect(uuid.hasMatch(row.id),isTrue,reason:row.id);
    expect(row.created.isAfter(before),isTrue);
    expect(row.mood,Mood.busy);
   }
   final defaulted=rows.singleWhere((r)=>r.memo==null);
   expect(defaulted.body,tricky);
   expect(defaulted.note,'n');
   final explicit=rows.singleWhere((r)=>r.memo=='explicit');
   expect(explicit.body,'mine');
   expect(explicit.note,isNull,reason:'an explicit null is kept, not defaulted');
   // A complete record remains a valid create input.
   final copy=Draft(id:'123e4567-e89b-42d3-a456-426614174001',body:'full',mood:Mood.calm,created:DateTime.utc(2020),note:null,memo:null);
   await client.transaction((tx)=>tx.models.draft.create(copy));
   expect((await client.models.draft.get(DraftIdentity(id:copy.id)))?.body,'full');
  }finally{await client.close();await temp.delete(recursive:true);}
 });
 // DateTime precision ([#189](https://github.com/zanminwang/axton/issues/189)):
 // AXTON keeps a UTC instant at millisecond precision, and generated Dart
 // truncates before it writes, so every read returns exactly what was stored.
 test('a microsecond DateTime reads back as its UTC millisecond truncation',()async{
  final micro=DateTime.utc(2026,9,28,12,34,56,789,123);
  final milli=DateTime.utc(2026,9,28,12,34,56,789);
  final local=DateTime(2026,9,28,14,0,0,0,456);
  expect(micro.toAxtonPrecision(),milli,reason:'the helper is re-exported by generated code');
  expect(Placement(shelf:'s',at:micro,label:'x').toRecord()['at'],'2026-09-28T12:34:56.789Z');
  expect(PlacementIdentity(shelf:'s',at:micro).toRecord()['at'],'2026-09-28T12:34:56.789Z');
  expect(EntryPatch(at:Present(micro)).toRecord()['at'],'2026-09-28T12:34:56.789Z');
  expect(EntryFilter(at:Present(micro)).toRecord()['at'],'2026-09-28T12:34:56.789Z');
  final temp=await Directory.systemTemp.createTemp('generated-api-precision-');
  final client=await GeneratedClient.open(path:'${temp.path}/state.sqlite',libraryPath:Platform.environment['AXTON_DART_LIBRARY'] ?? '../../target/debug/libaxton_dart.dylib');
  try{
   await client.transaction((tx)async{
    await tx.models.placement.create(Placement(shelf:'s',at:micro,label:'utc'));
    await tx.models.entry.create(Entry(id:id,title:'t',note:null,at:local,tags:const [],status:Status.active));
   });
   // A DateTime identity: the microsecond value and its truncation name one record.
   final placed=await client.models.placement.get(PlacementIdentity(shelf:'s',at:micro));
   expect(placed?.at,milli);
   expect(placed!.at.isUtc,isTrue);
   expect(await client.models.placement.get(PlacementIdentity(shelf:'s',at:milli)),isNotNull);
   expect((await client.models.placement.query(where:PlacementFilter(at:Present(micro)))).single.label,'utc');
   await client.models.placement.update(PlacementIdentity(shelf:'s',at:micro),const PlacementPatch(label:Present('moved')));
   expect((await client.models.placement.query()).single.label,'moved',reason:'the update found the same record');
   // A local DateTime reads back as the same instant in UTC.
   final entry=await client.models.entry.get(const EntryIdentity(id:id));
   expect(entry!.at,local.toAxtonPrecision());
   expect(entry.at,DateTime(2026,9,28,14).toUtc());
   expect(entry.at.isUtc,isTrue);
   expect(entry.at==local,isFalse,reason:'Dart == compares isUtc and microseconds too');
   await client.models.entry.update(const EntryIdentity(id:id),EntryPatch(at:Present(micro)));
   expect((await client.models.entry.get(const EntryIdentity(id:id)))!.at,milli);
   final watched=await client.models.entry.watch(where:EntryFilter(at:Present(micro))).firstWhere((rows)=>rows.isNotEmpty).timeout(const Duration(seconds:2));
   expect(watched.single.at,milli);
  }finally{await client.close();await temp.delete(recursive:true);}
 });
 // The generated Scope facade ([#150](https://github.com/zanminwang/axton/issues/150)):
 // one handle per registration, typed handle members, and the retained
 // `channels` spelling on that same ledger path.
 test('generated scopes facade answers with one handle per registration',()async{
  final temp=await Directory.systemTemp.createTemp('generated-api-scopes-');
  final client=await GeneratedClient.open(path:'${temp.path}/state.sqlite',libraryPath:Platform.environment['AXTON_DART_LIBRARY'] ?? '../../target/debug/libaxton_dart.dylib');
  try{
   final handles=await Future.wait([client.scopes.subscribe('project:123'),client.scopes.subscribe('project:123')]);
   final Subscription a=handles.first;
   expect(identical(a,handles.last),isTrue,reason:'concurrent calls obtain one cached handle');
   expect(a.status.initialization,SubscriptionInitialization.pending);
   await a.unsubscribe();
   final c=await client.scopes.subscribe('project:123');
   await a.unsubscribe();
   expect(c.status.active,isTrue,reason:'an old handle cannot remove the registration that replaced it');
   // The handle is the runtime's: its Scope, its immutable status and its
   // observers are all named through the generated library.
   final String scope=c.scope;
   final SubscriptionStatus status=c.status;
   expect(scope,'project:123');
   expect(status.connection,SubscriptionConnection.offline);
   final seen=<SubscriptionStatus>[];
   final observer=c.watch().listen(seen.add);
   await pumpEventQueue();
   await observer.cancel();
   expect(seen.map((s)=>s.connection),[SubscriptionConnection.offline],reason:'the current snapshot arrives first');
   // The retained spelling registers through the same ledger: with no server it
   // has durable intent and no boundary.
   final Subscription retained=await client.channels.subscribe('project:456');
   expect(retained.status.initialization,SubscriptionInitialization.pending);
   await client.channels.unsubscribe('project:456');
   expect(retained.status.active,isFalse);
   await c.unsubscribe();
  }finally{await client.close();await temp.delete(recursive:true);}
 });

 // Whole-Scope bootstrap through the generated facade
 // ([#151](https://github.com/zanminwang/axton/issues/151)): the handle's
 // `bootstrap()` and the `bootstrap` part of its typed status are named through
 // the generated library, and two concurrent calls register one task.
 test('generated handle bootstraps a Scope and publishes its typed load status',()async{
  final temp=await Directory.systemTemp.createTemp('generated-api-bootstrap-');
  final loads=<Map>[];
  final held=Completer<void>();
  final server=await HttpServer.bind(InternetAddress.loopbackIPv4,0);
  server.listen((request)async{
   if(request.uri.path=='/sync/pull'){
    final body=jsonDecode(await utf8.decoder.bind(request).join()) as Map;
    // Only a bootstrap page is expected here, and the test transport holds it.
    loads.add(body);
    await held.future;
    request.response.write(jsonEncode({'mode':'bootstrap','channel':body['channel'],'from':body['after'],'to':body['until'],'until':body['until'],'head':body['until'],'records':<Object>[]}));
    await request.response.close();
    return;
   }
   final socket=await WebSocketTransformer.upgrade(request);
   socket.listen((message){
    final subscribe=jsonDecode(message as String) as Map;
    socket.add(jsonEncode({'type':'subscribed','cursors':{for(final channel in subscribe['channels'] as List) channel:0}}));
   },onError:(Object _){});
  });
  final client=await GeneratedClient.open(path:'${temp.path}/state.sqlite',libraryPath:Platform.environment['AXTON_DART_LIBRARY'] ?? '../../target/debug/libaxton_dart.dylib');
  try{
   final Subscription subscription=await client.scopes.subscribe('project:123');
   final BootstrapStatus initial=subscription.status.bootstrap;
   final BootstrapPhase phase=initial.phase;
   final BootstrapError? failure=initial.error;
   expect(phase,BootstrapPhase.notRequested);
   expect(failure,isNull);
   await client.connect(SyncServer(url:'http://127.0.0.1:${server.port}',token:()=>'secret'));
   await _until(()=>subscription.status.initialization==SubscriptionInitialization.ready,'the committed boundary');
   final Future<void> first=subscription.bootstrap();
   final Future<void> second=subscription.bootstrap();
   var settled=false;
   final both=Future.wait([first,second]).then((_)=>settled=true);
   await _until(()=>subscription.status.bootstrap.phase==BootstrapPhase.loading,'a registered load');
   await _until(()=>loads.length==1,'the one page the run asked for');
   expect(settled,isFalse,reason:'the held response keeps both calls pending');
   held.complete();
   await both;
   expect(subscription.status.bootstrap.phase,BootstrapPhase.complete);
   expect(loads,hasLength(1),reason:'two concurrent calls registered one task');
   await subscription.bootstrap();
   expect(loads,hasLength(1),reason:'a completed run completes locally and asks for nothing more');
  }finally{
   await client.close();
   await server.close(force:true);
   await temp.delete(recursive:true);
  }
 }); // Model Fetch ([#153](https://github.com/zanminwang/axton/issues/153))
 // through the generated facade, the native runtime and a real HTTP route:
 // default storage with onStore, `store: false`, a composite DateTime
 // identity, absence, a typed backend refusal and joined callers.
 test('generated Fetch reads one Model remotely and stores it by default',()async{
  final temp=await Directory.systemTemp.createTemp('generated-fetch-');
  final server=await HttpServer.bind(InternetAddress.loopbackIPv4,0);
  final requests=<Map<String,dynamic>>[];
  final paths=<String>[];
  final held=Completer<void>();
  server.listen((request)async{
   final body=jsonDecode(await utf8.decoder.bind(request).join()) as Map<String,dynamic>;
   requests.add(body);
   paths.add('${request.uri.path} ${request.headers.value('authorization')}');
   if(body['model']=='Book')await held.future;
   final identity=(body['identity'] as Map).cast<String,dynamic>();
   final Map<String,dynamic>? state=switch(body['model']){
    'Placement'=>{'label':'placed'},
    'Book'=>{'title':'remote book'},
    'Entry' when identity['id']!='00000000-0000-4000-8000-000000000000'=>{'title':'remote','note':null,'at':'2026-02-03T04:05:06.000Z','tags':['x'],'status':'archived'},
    _=>null,
   };
   final failed=body['model']=='Counter';
   request.response.write(jsonEncode({
    'completion':{'callId':body['callId'],'outcome':failed
     ?{'status':'failed','code':'loader.failed','execution':'rejected'}
     :{'status':'succeeded','result':state==null?null:{...identity,...state}}},
    'records':failed||body['store']==false?[]:[{'model':body['model'],'identity':identity,'stamp':1,'state':state}],
   }));
   await request.response.close();
  });
  final stored=<String>[];
  final client=await GeneratedClient.open(
   path:'${temp.path}/state.sqlite',
   libraryPath:Platform.environment['AXTON_DART_LIBRARY']!,
   server:SyncServer(url:'http://127.0.0.1:${server.port}',token:()=>'secret'),
   onStore:StoreHooks(entry:(tx,changes){
    for(final change in changes){
     if(change is StoreUpsert<EntryIdentity,Entry>)stored.add('${change.row.status.name}@${change.row.at.toIso8601String()}');
    }
   }),
  );
  try{
   final Entry? entry=await client.fetch.entry(const EntryIdentity(id:id));
   expect(entry!.at,DateTime.utc(2026,2,3,4,5,6));
   expect(entry.status,Status.archived);
   expect(entry.tags,['x']);
   expect(paths.first,'/sync/fetch Bearer secret');
   expect(requests.first['version'],2,reason:'the Model read version the schema declares');
   expect(requests.first.containsKey('store'),isFalse);
   expect((await client.models.entry.get(const EntryIdentity(id:id)))?.title,'remote');
   expect(stored,['archived@2026-02-03T04:05:06.000Z']);
   const other='123e4567-e89b-42d3-a456-426614174999';
   final Entry? preview=await client.fetch.entry(const EntryIdentity(id:other),store:false);
   expect(preview?.title,'remote');
   expect(requests[1]['store'],false);
   expect(await client.models.entry.get(const EntryIdentity(id:other)),isNull);
   expect(stored,hasLength(1),reason:'store false runs no onStore');
   final at=DateTime.utc(2026,3,4,5,6,7);
   final Placement? placed=await client.fetch.placement(PlacementIdentity(shelf:'s',at:at));
   expect(placed?.at,at);
   expect(placed?.label,'placed');
   expect(DateTime.parse((requests[2]['identity'] as Map)['at'] as String),at);
   expect((await client.models.placement.get(PlacementIdentity(shelf:'s',at:at)))?.label,'placed');
   expect(await client.fetch.entry(const EntryIdentity(id:'00000000-0000-4000-8000-000000000000')),isNull);
   await expectLater(client.fetch.counter(const CounterIdentity(id:'n')),throwsA(isA<CallError>().having((e)=>e.code,'code','loader.failed').having((e)=>e.execution,'execution','rejected')));
   final joined=[client.fetch.book(const BookIdentity(id:'b')),client.fetch.book(const BookIdentity(id:'b'))];
   await _until(()=>requests.any((r)=>r['model']=='Book'),'the joined request');
   held.complete();
   final books=await Future.wait(joined);
   expect(requests.where((r)=>r['model']=='Book'),hasLength(1),reason:'joined callers share one request');
   expect(books[0]!.title,books[1]!.title);
   expect(identical(books[0],books[1]),isFalse,reason:'each caller decodes its own object');
  }finally{
   await client.close();
   await server.close(force:true);
   await temp.delete(recursive:true);
  }
 });
}

Future<void> _until(bool Function() predicate,String what)async{
 final deadline=DateTime.now().add(const Duration(seconds:5));
 while(DateTime.now().isBefore(deadline)){
  if(predicate())return;
  await Future<void>.delayed(const Duration(milliseconds:5));
 }
 throw StateError('$what timed out');
}

/// Stands in for the runtime's raw transaction: records each submission and
/// runs the `local` callback against a separate companion port.
final class _ScriptedTransaction implements SubmitMutationPort {
 final submitted=<Map<String,Object?>>[];
 final outer=<Map<String,dynamic>>[];
 final companions=<Map<String,dynamic>>[];
 @override
 Future<Call<T>> submitMutation<T>(String name,int version,Map<String,dynamic> args,T Function(dynamic) decode,{CallStore? store,Future<void> Function(WritePort local)? local})async{
  submitted.add({'name':name,'version':version,'args':args,'store':store?.toWire(),'local':local!=null});
  await local?.call(_CompanionPort(companions));
  final at=DateTime.utc(2026).toIso8601String();
  return _ScriptedCall(decode(name=='PublishEntry'?{'published':{'id':args['composition'],'title':'hello','note':null,'at':at,'tags':['x'],'status':'active'}}:null));
 }
}
final class _CompanionPort implements WritePort {
 final List<Map<String,dynamic>> writes;
 _CompanionPort(this.writes);
 @override
 Future<void> direct(Map<String,dynamic> operation)async=>writes.add(operation);
 @override
 Future<Map<String,dynamic>?> read(String model,Map<String,dynamic> identity)async=>{'id':identity['id'],'title':'draft','body':'text'};
 @override
 Future<List<Map<String,dynamic>>> querySpec(String model,Map<String,dynamic> query)async=>const [];
 @override
 Future<Map<String,dynamic>?> related(String model,Map<String,dynamic> identity,String relation)async=>null;
 @override
 Future<List<Map<String,dynamic>>> referencing(String model,Map<String,dynamic> identity,String source,String relation)async=>const [];
}
final class _ScriptedCall<T> implements Call<T> {
 final T result;
 _ScriptedCall(this.result);
 @override
 CallStatus get status=>CallStatus.pending;
 @override
 Future<CallOutcome<T>> wait()async=>CallSuccess<T>(result);
}
