//! The actor's C ABI, shared by the Dart and mobile carriers, which export it
//! under their own symbol names ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! Strings cross as NUL-terminated UTF-8 owned by whoever allocated them:
//! the caller's inputs are borrowed for the call only, and every string
//! returned here (a drain batch, an `error_out` message) is allocated by Rust
//! and freed exactly once with [`free`]. A panic never crosses the boundary.
use crate::actor;
use serde_json::Value;
use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};

/// The carrier's wake callback: `runtime` has events to drain. `context` is
/// the pointer given to [`open`], passed back verbatim. It is called on a
/// runtime thread and must only schedule the drain on the carrier's own
/// thread; it must not call back into this ABI synchronously.
pub type Wake = extern "C" fn(runtime: u64, context: *mut c_void);

/// The carrier's opaque context. The carrier keeps it valid until
/// [`detach`] returns, which is also when the last call through it ends.
struct Context(*mut c_void);
// SAFETY: the pointer is never dereferenced here, only handed back to the
// carrier's own callback, whose contract covers calls from any thread.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}
impl Context {
    fn get(&self) -> *mut c_void {
        self.0
    }
}

/// Open a runtime; answer its id, or 0 with `*error_out` set. The open's
/// outcome arrives as the `taskCompleted` of the request's `requestId`.
///
/// # Safety
/// `request` must be null or a valid NUL-terminated string for the call;
/// `error_out` must be null or valid for one write; `context` must stay valid
/// for `wake` until [`detach`] of the returned id has returned.
pub unsafe fn open(
    request: *const c_char,
    wake: Option<Wake>,
    context: *mut c_void,
    error_out: *mut *mut c_char,
) -> u64 {
    let context = Context(context);
    let opened = catch_unwind(AssertUnwindSafe(|| {
        let request = unsafe { read_json(request) }?;
        let wake = wake.ok_or("null wake callback")?;
        // `get()` captures the whole Send wrapper, not its raw pointer field.
        actor::open(
            request,
            Box::new(move |runtime| wake(runtime, context.get())),
        )
    }));
    match opened.unwrap_or_else(|_| Err("runtime panic".into())) {
        Ok(runtime) => runtime,
        Err(error) => {
            unsafe { write_error(error_out, &error) };
            0
        }
    }
}

/// Admit one envelope: 0 on admission, 1 with `*error_out` set otherwise.
///
/// # Safety
/// As for [`open`].
pub unsafe fn submit(runtime: u64, message: *const c_char, error_out: *mut *mut c_char) -> i32 {
    let admitted = catch_unwind(AssertUnwindSafe(|| {
        let message = unsafe { read_json(message) }?;
        actor::submit(runtime, message)
    }));
    match admitted.unwrap_or_else(|_| Err("runtime panic".into())) {
        Ok(()) => 0,
        Err(error) => {
            unsafe { write_error(error_out, &error) };
            1
        }
    }
}

/// The events published so far as a JSON array; `[]` when none or unknown.
pub fn drain(runtime: u64) -> *mut c_char {
    let batch = catch_unwind(|| Value::Array(actor::drain(runtime)).to_string())
        .unwrap_or_else(|_| "[]".into());
    owned(batch)
}

/// Stop wakes and forget the runtime; see [`actor::detach`].
///
/// Finalizer-safe: it may run on any thread, concurrently with itself, any
/// number of times, and for an id already detached, closed or never opened
/// (those return at once). It takes only short internal locks, calls no
/// carrier code and never waits for the actor, so a Dart `NativeFinalizer`
/// run by the garbage collector or at isolate shutdown may call it. Once it
/// returns the wake is never called again for this id, so the carrier may
/// then close its `NativeCallable`. It must not be called from inside that
/// runtime's own wake callback.
pub fn detach(runtime: u64) {
    let _ = catch_unwind(|| actor::detach(runtime));
}

/// Free a string this ABI returned.
///
/// # Safety
/// `output` must be null or a pointer returned by this ABI and not yet freed.
pub unsafe fn free(output: *mut c_char) {
    if !output.is_null() {
        drop(unsafe { CString::from_raw(output) });
    }
}

unsafe fn read_json(input: *const c_char) -> Result<Value, String> {
    if input.is_null() {
        return Err("null input".into());
    }
    let text = unsafe { CStr::from_ptr(input) }
        .to_str()
        .map_err(|e| e.to_string())?;
    serde_json::from_str(text).map_err(|e| e.to_string())
}

unsafe fn write_error(error_out: *mut *mut c_char, error: &str) {
    if !error_out.is_null() {
        unsafe { *error_out = owned(error.to_string()) };
    }
}

fn owned(text: String) -> *mut c_char {
    CString::new(text.replace('\0', ""))
        .unwrap_or_default()
        .into_raw()
}
