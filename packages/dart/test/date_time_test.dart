import 'package:axton/axton.dart';
import 'package:test/test.dart';

void main() {
  group('toAxtonPrecision', () {
    test('drops microseconds from a UTC value', () {
      final value = DateTime.utc(2026, 9, 28, 12, 34, 56, 789, 123);
      final stored = value.toAxtonPrecision();
      expect(stored, DateTime.utc(2026, 9, 28, 12, 34, 56, 789));
      expect(stored.isUtc, isTrue);
      expect(stored.toIso8601String(), '2026-09-28T12:34:56.789Z');
    });

    test('keeps a whole-millisecond UTC value equal', () {
      final value = DateTime.utc(2026, 9, 28, 12, 34, 56, 789);
      expect(value.toAxtonPrecision(), value);
    });

    test('returns a local value as the same instant in UTC', () {
      final value = DateTime(2026, 9, 28, 12, 34, 56, 789, 123);
      final stored = value.toAxtonPrecision();
      expect(stored.isUtc, isTrue);
      expect(
        stored,
        DateTime.fromMillisecondsSinceEpoch(
          value.millisecondsSinceEpoch,
          isUtc: true,
        ),
      );
      expect(stored, isNot(value), reason: 'Dart == also compares isUtc');
      expect(
        stored.isAtSameMomentAs(
          value.subtract(const Duration(microseconds: 123)),
        ),
        isTrue,
      );
    });

    test(
      'moves a pre-1970 value to the earlier millisecond, as the core does',
      () {
        // One microsecond before the epoch: 1969-12-31T23:59:59.999999Z.
        final value = DateTime.fromMicrosecondsSinceEpoch(-1, isUtc: true);
        final stored = value.toAxtonPrecision();
        expect(stored.microsecondsSinceEpoch, -1000);
        expect(stored.toIso8601String(), '1969-12-31T23:59:59.999Z');
      },
    );

    test('encodes with exactly three fractional digits', () {
      for (final value in [
        DateTime.utc(2026),
        DateTime.utc(2026, 1, 1, 0, 0, 0, 0, 1),
        DateTime.now(),
      ]) {
        expect(
          value.toAxtonPrecision().toIso8601String(),
          matches(RegExp(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$')),
        );
      }
    });
  });
}
