import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'package:test/test.dart';
import 'generated.dart';
void main(){
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
