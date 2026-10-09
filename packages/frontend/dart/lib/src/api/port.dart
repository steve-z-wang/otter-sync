import 'actions.dart' show Call;

/// Reads available on both a [Client] and a [Transaction].
abstract interface class ReadPort {
  Future<Map<String, dynamic>?> read(
    String model,
    Map<String, dynamic> identity,
  );
  Future<List<Map<String, dynamic>>> querySpec(
    String model,
    Map<String, dynamic> query,
  );
  Future<Map<String, dynamic>?> related(
    String model,
    Map<String, dynamic> identity,
    String relation,
  );
  Future<List<Map<String, dynamic>>> referencing(
    String model,
    Map<String, dynamic> identity,
    String source,
    String relation,
  );
}

/// Writes available inside a [Transaction].
abstract interface class WritePort implements ReadPort {
  Future<void> direct(Map<String, dynamic> operation);
}

/// Queues a named Mutation inside an application [Transaction]. The call
/// resolves after the Mutation's optimism and its [local] callback ran in the
/// open transaction; the [Call] is sendable only after the local commit.
/// [local] receives a restricted port whose writes are that Mutation's local
/// companions; it queues no Mutation and has no Scopes, watch or
/// savepoints.
abstract interface class SubmitMutationPort {
  Future<Call<T>> submitMutation<T>(
    String name,
    int version,
    Map<String, dynamic>? args,
    T Function(dynamic) decode, {
    Future<Map<String, dynamic>> Function(WritePort tx)? input,
  });
}
