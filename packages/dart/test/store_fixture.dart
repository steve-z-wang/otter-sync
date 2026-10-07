import 'package:axton/axton.dart';

/// Explicit disconnected transport fixture. Offline open needs no connection.
StoreConnection offlineStoreConnection() => StoreConnection(
  url: 'http://127.0.0.1:1',
  token: () => 'offline',
  onError: (_) {},
);

/// Declare the named update used by generic-runtime tests; its slot carries
/// only Entry identity and explicitly mutable fields, like compiler output.
void declareEntryEdit(Map<String, dynamic> schema) {
  final entry = (schema['models'] as List).cast<Map>().firstWhere(
    (m) => m['name'] == 'Entry',
  );
  schema['actions'] = [
    {
      'kind': 'mutation',
      'name': 'Edit',
      'version': 1,
      'input': {
        'enums': [],
        'models': [entry],
      },
      'inputs': [
        {
          'name': 'entry',
          'kind': 'model',
          'model': 'Entry',
          'operation': 'update',
          'cardinality': 'single',
          'allowedPatchFields': ['text', 'note'],
        },
      ],
      'outputs': [],
      'outputEnums': [],
      'prerequisites': schema['prerequisites'] ?? [],
      'requirements': schema['requirements'] ?? [],
      'sequence': null,
    },
  ];
}
