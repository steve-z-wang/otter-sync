import 'dart:io';
import 'generated.dart';
void check(bool value,String message) { if(!value) throw StateError(message); }
Future<void> main() async {
  final directory=await Directory.systemTemp.createTemp('axton-sdk04-dart-');
  final connection=StoreConnection(url:'http://127.0.0.1:1',token:()=>'offline');
  final library=Platform.environment['AXTON_LIBRARY']!;
  final a=await GeneratedClient.open(path:'${directory.path}/a',stream:'User:alice',connection:connection,libraryPath:library);
  final b=await GeneratedClient.open(path:'${directory.path}/b',stream:'User:alice',connection:connection,libraryPath:library);
  try {
    await a.transaction((tx) async {
      await tx.models.draft.create(const DraftCreate(id:'d',text:'draft'));
      final call=await tx.mutations.publish.withTransaction((local) async {
        await local.models.draft.delete(const DraftIdentity(id:'d'));
        return const PublishInput(entry:EntryCreate(id:'e',text:'written'),call:'legal');
      });
      try { await call.wait(); throw StateError('precommit wait succeeded'); }
      on CallError catch(error) { check(error.code=='transaction_uncommitted','wrong precommit error'); }
    });
    check(await a.models.draft.get(const DraftIdentity(id:'d'))==null,'companion missing');
    check((await a.models.entry.get(const EntryIdentity(id:'e')))!.text=='written','optimism missing');
    try {
      await a.mutations.publish.withTransaction((tx) async {
        await tx.models.draft.create(const DraftCreate(id:'bad',text:'rollback'));
        throw StateError('body failed');
      });
      throw StateError('callback accepted');
    } on StateError catch(error) { check(error.message=='body failed','callback error changed'); }
    check(await a.models.draft.get(const DraftIdentity(id:'bad'))==null,'callback did not roll back');
    try {
      await a.transaction((txA) async {
        await b.transaction((txB) async {
          try { await txA.models.draft.create(const DraftCreate(id:'foreign',text:'bad')); throw StateError('foreign accepted'); }
          on StateError catch(error) { check(error.message=='foreign transaction scope','wrong foreign error'); }
        });
      });
      throw StateError('poisoned scope committed');
    } on StateError catch(error) { check(error.message=='foreign transaction scope','wrong scope terminal'); }
    check(await a.models.draft.get(const DraftIdentity(id:'foreign'))==null,'foreign row committed');
    var ran=false;
    try {
      await a.transaction((txA) async {
        await b.transaction((_) async {
          try { await txA.transaction.savepoint(() async {ran=true;return 42;}); throw StateError('foreign savepoint accepted'); }
          on StateError catch(error) {check(error.message=='foreign transaction scope','wrong savepoint refusal');}
        });
      });
      throw StateError('foreign savepoint committed');
    } on StateError catch(error) {check(error.message=='foreign transaction scope','wrong savepoint terminal');}
    check(!ran,'foreign savepoint body ran');
    stdout.writeln('Dart real native callback/rollback/precommit/foreign owner: PASS');
  } finally { await a.close();await b.close();await directory.delete(recursive:true); }
}
