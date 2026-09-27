import 'dart:io';
import 'generated.dart';
// Exits by itself only when both failed opens left nothing attached.
Future<void> main(List<String> args) async {
 final library=Platform.environment['AXTON_DART_LIBRARY'];
 try {
  await GeneratedClient.open(path:args[0],libraryPath:library,server:SyncServer(url:'http://[',token:()=>'secret'));
  throw StateError('invalid URL unexpectedly opened');
 } on FormatException {
  if (File('${args[0]}-wal').existsSync() || File('${args[0]}-shm').existsSync()) {
   throw StateError('failed open left native SQLite connection alive');
  }
 }
 // The runtime itself fails to open: its failure arrives as a failed task.
 final missing='${args[0]}.missing/state.sqlite';
 try {
  await GeneratedClient.open(path:missing,libraryPath:library);
  throw Exception('missing directory unexpectedly opened');
 } on StateError {
  if (File(missing).existsSync()) throw StateError('failed open created its file');
 }
}
