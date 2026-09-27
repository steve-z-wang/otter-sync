//! The native carrier of the Rust-owned client runtime
//! ([#134](https://github.com/zanminwang/axton/issues/134)), shared by every
//! language binding.
//!
//! [`actor`] runs one [`axton_client::runtime::ClientRuntime`] per open client
//! on a thread of its own and exposes admission, drain and a wake; [`ffi`] is
//! its C ABI, which the Dart and mobile carriers export under their own symbol
//! names, and the Node addon calls [`actor`] directly.
pub mod actor;
pub mod ffi;
