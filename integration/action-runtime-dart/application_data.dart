import 'dart:io';
import 'package:axton/axton.dart';
Future<void> main() async {
  final directory = await Directory.systemTemp.createTemp('axton-application-data-');
  final library = Platform.environment['AXTON_LIBRARY']!;
  try {
    Client.configureApplicationData(directory.path, libraryPath: library);
    Client.configureApplicationData(directory.path, libraryPath: library);
    try { Client.configureApplicationData('${directory.path}/other', libraryPath: library); throw StateError('changed directory accepted'); }
    on StateError catch (error) { if (error.message == 'changed directory accepted') rethrow; }
    print('Dart process application directory: PASS');
  } finally { await directory.delete(recursive:true); }
}
