import 'dart:convert';
import 'dart:io';
import 'generated.dart';
void check(bool value,String message){if(!value)throw StateError(message);}
Future<void> main(List<String> args)async{
 String? first;
 for(var run=0;run<2;run++){
  final c=await GeneratedClient.open(path:args[0],libraryPath:Platform.environment['AXTON_DART_LIBRARY']!);
  try{
   check(c.client.clientId=='fixture-client','client ID');
   check((await c.models.todo.get(const TodoIdentity(id:'live')))?.channel=='second queued Channel','opaque application field');
   check((await c.client.readSql('SELECT count(*) AS n FROM axton_scope_member'))[0]['n']==4,'membership evidence');
   check((await c.client.readSql("SELECT count(*) AS n FROM sqlite_master WHERE name LIKE 'axton_channel%'"))[0]['n']==0,'old ownership removed');
   check((await c.client.readSql("SELECT cursor FROM axton_subscription WHERE scope='Channel:business-scope'"))[0]['cursor']==11,'ordinary cursor');
   final bytes=(await c.client.freeze())!;
   final logical=jsonDecode(bytes) as Map<String,dynamic>;logical.remove('capabilities');
   final expected=jsonDecode(await File(args[1]).readAsString());
   check(jsonEncode(logical)==jsonEncode(expected),'logical frozen bytes');
   if(run==0){first=bytes;}else{check(bytes==first,'second reopen frozen bytes');}
  }finally{await c.close();}
 }
 print('generated Dart original-v0.2 native reopen twice: holds, cursors, opaque channel and frozen work preserved');
}
