//! C ABI of the Dart SDK: the runtime actor's open/submit/drain/detach
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
use axton_binding::ffi;
use std::ffi::{c_char, c_void};
/// Open a Rust-owned client runtime
/// ([#134](https://github.com/zanminwang/axton/issues/134)). Answers the
/// runtime id, or 0 with `*error_out` set (freed with [`axton_free`]). `wake`
/// is called from the runtime's thread with `context`; a
/// `NativeCallable.listener` schedules the drain on the owning isolate.
///
/// # Safety
/// See `axton_binding::ffi::open`: `context` stays valid for `wake` until
/// [`axton_runtime_detach`] of this id returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn axton_runtime_open(
    request: *const c_char,
    wake: Option<ffi::Wake>,
    context: *mut c_void,
    error_out: *mut *mut c_char,
) -> u64 {
    unsafe { ffi::open(request, wake, context, error_out) }
}
/// Admit one envelope: 0, or 1 with `*error_out` set.
///
/// # Safety
/// See `axton_binding::ffi::submit`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn axton_runtime_submit(
    runtime: u64,
    message: *const c_char,
    error_out: *mut *mut c_char,
) -> i32 {
    unsafe { ffi::submit(runtime, message, error_out) }
}
/// The published events as a JSON array, freed with [`axton_free`].
#[unsafe(no_mangle)]
pub extern "C" fn axton_runtime_drain(runtime: u64) -> *mut c_char {
    ffi::drain(runtime)
}
/// Stop wakes and forget the runtime; the wake context may be released once
/// this returns. Safe from any thread, repeatedly, and for an id already
/// detached; see `axton_binding::ffi::detach`.
#[unsafe(no_mangle)]
pub extern "C" fn axton_runtime_detach(runtime: u64) {
    ffi::detach(runtime)
}
/// [`axton_runtime_detach`] in the shape a Dart `NativeFinalizer` calls,
/// `void (*)(void *token)`: the token is the runtime id as an address
/// (`Pointer.fromAddress(id)`). Safe from any thread, repeatedly, and for an
/// id already detached; see `axton_binding::ffi::detach`.
#[unsafe(no_mangle)]
pub extern "C" fn axton_runtime_finalize(token: *mut c_void) {
    ffi::detach(token as usize as u64)
}
/// Free a string the `axton_runtime` functions returned.
///
/// # Safety
/// `output` must be null or a pointer returned by those functions, freed
/// exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn axton_free(output: *mut c_char) {
    unsafe { ffi::free(output) }
}
