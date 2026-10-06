import 'dart:io';
import 'generated.dart';
void check(bool condition,String message) { if(!condition) throw StateError(message); }
Future<void> main(List<String> args) async {
 final directory=await Directory.systemTemp.createTemp('axton-sdk-host-dart-');
 final library=Platform.environment['AXTON_LIBRARY']!;
 final client=await GeneratedClient.open(path:'${directory.path}/db',stream:'User:alice',connection:StoreConnection(url:args[0],token:()=>'alice',identity:const StoreIdentity(backend:'sdk',viewer:'alice',contract:'sdk-v04')),libraryPath:library);
 try {
  await client.bootstrap();
  final call=await client.mutations.publish.withTransaction((tx) async {
   await tx.models.draft.create(const DraftCreate(id:'dart-draft',text:'companion'));
   return const PublishInput(entry:EntryCreate(id:'dart-entry',text:' normalized dart '),call:'legal');
  });
  final outcome=await call.wait();
  check(outcome is CallSuccess<PublishOutput> && outcome.result.entry.text=='normalized dart','Call.wait did not settle canonical result');
  check((await client.models.entry.get(const EntryIdentity(id:'dart-entry')))?.text=='normalized dart','canonical authority not installed');
  final refused=await client.mutations.publish.withTransaction((tx) async {
   await tx.models.draft.create(const DraftCreate(id:'dart-refused-draft',text:'owned'));
   return const PublishInput(entry:EntryCreate(id:'dart-refused-entry',text:'refuse'),call:'business');
  });
  final refusal=await refused.wait();
  check(refusal is CallFailure<PublishOutput> && refusal.error.code=='publish.refused','business refusal missing');
  check(await client.models.draft.get(const DraftIdentity(id:'dart-refused-draft'))==null,'refused companion survived');
  check(await client.models.entry.get(const EntryIdentity(id:'dart-refused-entry'))==null,'refused optimism survived');
  final first=await client.queries.find(id:'read',store:false,once:true);
  check(first.entry?.text=='changed','Query snapshot wrong');
  check(await client.models.entry.get(const EntryIdentity(id:'read'))==null,'store false cached Query');
  check((await client.queries.find(id:'read',store:false,once:true)).entry?.text=='changed','once result wrong');
  check((await client.fetch.entry(const EntryIdentity(id:'read'),store:false))?.text=='changed','Fetch snapshot wrong');
  check(await client.models.entry.get(const EntryIdentity(id:'read'))==null,'store false cached Fetch');
  await client.fetch.entry(const EntryIdentity(id:'read'));
  check((await client.models.entry.get(const EntryIdentity(id:'read')))?.text=='changed','default Fetch missing');
  check((await client.queries.find(id:'missing')).entry==null,'Query missing wrong');
  check(await client.fetch.entry(const EntryIdentity(id:'missing'))==null,'Fetch missing wrong');
  stdout.writeln('Dart generated real HTTP/WebSocket/SQLite/PG: PASS');
 } finally {await client.close();await directory.delete(recursive:true);}
}
