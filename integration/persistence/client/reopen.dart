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
   check((await c.client.readSql("SELECT name FROM sqlite_master WHERE name IN ('axton_stream_member','axton_stream_member_record')")).isEmpty,'local holding table and index removed');
   check((await c.client.readSql('SELECT local_authority_version FROM axton_client'))[0]['local_authority_version']==1,'local authority marker');
   check((await c.client.readSql("SELECT count(*) AS n FROM sqlite_master WHERE name LIKE 'axton_channel%'"))[0]['n']==0,'old ownership removed');
   check((await c.client.readSql("SELECT cursor FROM axton_subscription WHERE stream='Channel:business-scope'"))[0]['cursor']==11,'ordinary cursor');
   final bytes=(await c.client.freeze())!;
   final logical=jsonDecode(bytes) as Map<String,dynamic>;
   check(jsonEncode(logical['capabilities'])==jsonEncode(['stream-authority-v1']),'frozen authority capability');
   logical.remove('capabilities');
   final expected=jsonDecode(await File(args[1]).readAsString());
   check(jsonEncode(logical)==jsonEncode(expected),'logical frozen bytes');
   if(run==0){first=bytes;}else{check(bytes==first,'second reopen frozen bytes');}
  }finally{await c.close();}
 }
 print('generated Dart original-v0.2 native reopen twice: authority migration, cursors, opaque channel and frozen work preserved');
}
