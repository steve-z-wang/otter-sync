/// One queued mutation touching a record.
class PendingMutation {
  final int ordinal;

  /// The schema mutation name.
  final String name;

  /// `queued` (not frozen) or `frozen` (request retained for sending or retry).
  final String phase;

  /// Each prerequisite key with its state: `ready`, `pending` or `failed`.
  final List<({String key, String state})> prerequisites;

  /// Set when the edit could not be replayed over newer authority.
  final bool diverged;

  const PendingMutation({
    required this.ordinal,
    required this.name,
    required this.phase,
    this.prerequisites = const [],
    this.diverged = false,
  });

  factory PendingMutation.fromRecord(Map<String, dynamic> record) =>
      PendingMutation(
        ordinal: record['ordinal'] as int,
        name: record['name'] as String,
        phase: record['phase'] as String,
        prerequisites: [
          for (final p in (record['prerequisites'] as List? ?? const []))
            (key: (p as Map)['key'] as String, state: p['state'] as String),
        ],
        diverged: record['diverged'] == true,
      );
}

/// One record's sync state: its pending mutations and retained rejections.
/// A local snapshot, not a network probe.
class SyncState {
  final List<PendingMutation> pending;

  /// Retained rejections, each with at least `ordinal` and `code`.
  final List<Map<String, dynamic>> rejections;

  const SyncState({required this.pending, required this.rejections});

  factory SyncState.fromRecord(Map<String, dynamic> record) => SyncState(
    pending: [
      for (final p in (record['pending'] as List? ?? const []))
        PendingMutation.fromRecord((p as Map).cast<String, dynamic>()),
    ],
    rejections: [
      for (final r in (record['rejections'] as List? ?? const []))
        (r as Map).cast<String, dynamic>(),
    ],
  );
}
