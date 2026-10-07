# Rust ↔ Other Languages

- Preserve the existing protocol boundary between Rust and the language hosts.
- Rust owns synchronization, state transitions and persistence rules.
- Language bindings carry requests, results, effects and observer events; hosts execute platform I/O and return results to Rust.
- Generated language APIs remain typed facades over this boundary.
