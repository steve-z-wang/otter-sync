import 'package:axton/axton.dart';

/// Stable offline binding for tests of local Store capabilities. Transport
/// fixtures supply their own live connection; this URL never accepts writes.
StoreConnection offlineStoreConnection() => StoreConnection(
  url: 'http://127.0.0.1:1',
  token: () => 'offline',
  identity: const StoreIdentity(
    backend: 'dart-test',
    viewer: 'viewer',
    contract: 'v04',
  ),
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
