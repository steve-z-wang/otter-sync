/// AXTON stores a `DateTime` as a UTC instant at millisecond precision, the
/// precision a TypeScript `Date` carries, so clients and the server agree.
extension AxtonDateTime on DateTime {
  /// This instant as AXTON stores and returns it: in UTC, with the
  /// sub-millisecond part dropped.
  ///
  /// Generated code encodes every `DateTime` it writes or sends through this
  /// method, and every `DateTime` it reads is already in this form. Dart's
  /// `==` also compares [isUtc] and microseconds, so compare a value you
  /// created with one read back as `readBack == value.toAxtonPrecision()`.
  DateTime toAxtonPrecision() {
    final utc = toUtc();
    return utc.microsecond == 0
        ? utc
        : utc.subtract(Duration(microseconds: utc.microsecond));
  }
}
