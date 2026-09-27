//! C ABI used by mobile platform modules: the runtime actor's
//! open/submit/drain/detach
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
use axton_binding::ffi;
use std::ffi::{c_char, c_void};

/// Opens a Rust-owned client runtime
/// ([#134](https://github.com/zanminwang/axton/issues/134)). Answers the
/// runtime id, or 0 with `*error_out` set (freed with [`axton_mobile_free`]).
/// `wake` is called from the runtime's thread with `context`.
///
/// # Safety
/// See `axton_binding::ffi::open`: `context` stays valid for `wake` until
/// [`axton_mobile_runtime_detach`] of this id returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn axton_mobile_runtime_open(
    request: *const c_char,
    wake: Option<ffi::Wake>,
    context: *mut c_void,
    error_out: *mut *mut c_char,
) -> u64 {
    unsafe { ffi::open(request, wake, context, error_out) }
}

/// Admits one envelope: 0, or 1 with `*error_out` set.
///
/// # Safety
/// See `axton_binding::ffi::submit`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn axton_mobile_runtime_submit(
    runtime: u64,
    message: *const c_char,
    error_out: *mut *mut c_char,
) -> i32 {
    unsafe { ffi::submit(runtime, message, error_out) }
}

/// The published events as a JSON array, freed with [`axton_mobile_free`].
#[unsafe(no_mangle)]
pub extern "C" fn axton_mobile_runtime_drain(runtime: u64) -> *mut c_char {
    ffi::drain(runtime)
}

/// Stops wakes and forgets the runtime; the wake context may be released once
/// this returns.
#[unsafe(no_mangle)]
pub extern "C" fn axton_mobile_runtime_detach(runtime: u64) {
    ffi::detach(runtime)
}

/// Frees a string the `axton_mobile_runtime` functions returned.
///
/// # Safety
/// `output` must be null or a pointer returned by one of those functions that
/// has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn axton_mobile_free(output: *mut c_char) {
    unsafe { ffi::free(output) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    /// The wake context: a channel the test waits on, never a sleep.
    extern "C" fn wake(runtime: u64, context: *mut std::ffi::c_void) {
        let sender =
            unsafe { &*(context as *const std::sync::Mutex<std::sync::mpsc::Sender<u64>>) };
        let _ = sender.lock().unwrap().send(runtime);
    }

    fn drained(runtime: u64) -> Vec<serde_json::Value> {
        let output = super::axton_mobile_runtime_drain(runtime);
        let text = unsafe { CStr::from_ptr(output) }
            .to_str()
            .unwrap()
            .to_owned();
        unsafe { super::axton_mobile_free(output) };
        serde_json::from_str(&text).unwrap()
    }

    #[test]
    fn runtime_carrier_opens_wakes_drains_and_detaches_across_the_c_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let schema: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
        let (sender, wakes) = std::sync::mpsc::channel::<u64>();
        let context = Box::into_raw(Box::new(std::sync::Mutex::new(sender)));
        let request = CString::new(
            serde_json::json!({"type":"open","requestId":"1","path":dir.path().join("db"),"schema":schema})
                .to_string(),
        )
        .unwrap();
        let mut error = std::ptr::null_mut();
        let runtime = unsafe {
            super::axton_mobile_runtime_open(
                request.as_ptr(),
                Some(wake),
                context.cast(),
                &mut error,
            )
        };
        assert!(runtime > 0 && error.is_null());
        let mut events = vec![];
        let wait = |events: &mut Vec<serde_json::Value>, kind: &str| loop {
            if let Some(i) = events.iter().position(|e| e["type"] == kind) {
                return events.remove(i);
            }
            assert_eq!(
                wakes
                    .recv_timeout(std::time::Duration::from_secs(20))
                    .unwrap(),
                runtime
            );
            events.extend(drained(runtime));
        };
        let opened = wait(&mut events, "taskCompleted");
        assert_eq!(opened["ok"], true);
        assert!(opened["value"]["clientId"].is_string());

        let close = CString::new(r#"{"type":"close"}"#).unwrap();
        assert_eq!(
            unsafe { super::axton_mobile_runtime_submit(runtime, close.as_ptr(), &mut error) },
            0
        );
        wait(&mut events, "runtimeClosed");
        super::axton_mobile_runtime_detach(runtime);
        // No wake can run after detach returned: the context can go.
        drop(unsafe { Box::from_raw(context) });
        assert_eq!(
            unsafe { super::axton_mobile_runtime_submit(runtime, close.as_ptr(), &mut error) },
            1
        );
        let message = unsafe { CStr::from_ptr(error) }
            .to_str()
            .unwrap()
            .to_owned();
        unsafe { super::axton_mobile_free(error) };
        assert_eq!(message, "client_closed");

        // Refusals at admission set the error and answer 0.
        let mut error = std::ptr::null_mut();
        let refused = unsafe {
            super::axton_mobile_runtime_open(
                std::ptr::null(),
                Some(wake),
                std::ptr::null_mut(),
                &mut error,
            )
        };
        assert_eq!(refused, 0);
        assert_eq!(
            unsafe { CStr::from_ptr(error) }.to_str().unwrap(),
            "null input"
        );
        unsafe { super::axton_mobile_free(error) };
        let mut error = std::ptr::null_mut();
        let refused = unsafe {
            super::axton_mobile_runtime_open(
                request.as_ptr(),
                None,
                std::ptr::null_mut(),
                &mut error,
            )
        };
        assert_eq!(refused, 0);
        unsafe { super::axton_mobile_free(error) };
        assert_eq!(drained(u64::MAX), Vec::<serde_json::Value>::new());
    }

    /// Take events on every wake until one of `kind` arrives; answer it.
    fn wait_for(
        wakes: &std::sync::mpsc::Receiver<u64>,
        runtime: u64,
        events: &mut Vec<serde_json::Value>,
        kind: &str,
    ) -> serde_json::Value {
        loop {
            if let Some(i) = events.iter().position(|e| e["type"] == kind) {
                return events.remove(i);
            }
            assert_eq!(
                wakes
                    .recv_timeout(std::time::Duration::from_secs(20))
                    .unwrap(),
                runtime
            );
            events.extend(drained(runtime));
        }
    }

    /// The error string an ABI call set, freed.
    fn taken(error: *mut std::ffi::c_char) -> String {
        assert!(!error.is_null());
        let text = unsafe { CStr::from_ptr(error) }
            .to_str()
            .unwrap()
            .to_owned();
        unsafe { super::axton_mobile_free(error) };
        text
    }

    /// An open that fails answers its request with the error and closes the
    /// runtime; input that is not JSON is refused at the boundary with the
    /// parser's message, and a string it returned is freed exactly once.
    #[test]
    fn a_failed_open_and_malformed_input_are_answered_across_the_c_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let schema: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
        let (sender, wakes) = std::sync::mpsc::channel::<u64>();
        let context = Box::into_raw(Box::new(std::sync::Mutex::new(sender)));
        let missing = dir.path().join("missing").join("sub").join("mobile.db");
        let request = CString::new(
            serde_json::json!({"type":"open","requestId":"1","path":missing,"schema":schema})
                .to_string(),
        )
        .unwrap();
        let mut error = std::ptr::null_mut();
        let runtime = unsafe {
            super::axton_mobile_runtime_open(
                request.as_ptr(),
                Some(wake),
                context.cast(),
                &mut error,
            )
        };
        assert!(runtime > 0 && error.is_null(), "the open is admitted");
        let mut events = vec![];
        let failed = wait_for(&wakes, runtime, &mut events, "taskCompleted");
        assert_eq!(failed["requestId"], "1");
        assert_eq!(failed["ok"], false, "open in a missing directory must fail");
        assert!(!failed["error"].as_str().unwrap().is_empty());
        wait_for(&wakes, runtime, &mut events, "runtimeClosed");
        let status =
            CString::new(r#"{"type":"task","requestId":"2","command":{"kind":"status"}}"#).unwrap();
        let mut error = std::ptr::null_mut();
        assert_eq!(
            unsafe { super::axton_mobile_runtime_submit(runtime, status.as_ptr(), &mut error) },
            1
        );
        assert_eq!(taken(error), "client_closed");
        super::axton_mobile_runtime_detach(runtime);
        drop(unsafe { Box::from_raw(context) });

        let malformed = CString::new("{").unwrap();
        let mut error = std::ptr::null_mut();
        let refused = unsafe {
            super::axton_mobile_runtime_open(
                malformed.as_ptr(),
                Some(wake),
                std::ptr::null_mut(),
                &mut error,
            )
        };
        assert_eq!(refused, 0);
        assert!(taken(error).contains("EOF"));
        let mut error = std::ptr::null_mut();
        assert_eq!(
            unsafe { super::axton_mobile_runtime_submit(runtime, malformed.as_ptr(), &mut error) },
            1
        );
        assert!(taken(error).contains("EOF"));
        let mut error = std::ptr::null_mut();
        assert_eq!(
            unsafe { super::axton_mobile_runtime_submit(runtime, std::ptr::null(), &mut error) },
            1
        );
        assert_eq!(taken(error), "null input");
        // Freeing null is allowed.
        unsafe { super::axton_mobile_free(std::ptr::null_mut()) };
    }
}
