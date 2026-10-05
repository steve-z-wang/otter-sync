use std::ffi::{CStr, CString};
#[test]
fn native_host_fixes_one_durable_application_directory_before_any_store_open() {
    let directory = tempfile::tempdir().unwrap();
    let path = CString::new(directory.path().to_str().unwrap()).unwrap();
    let mut error = std::ptr::null_mut();
    assert_eq!(
        unsafe { axton_binding::ffi::configure_application_data(path.as_ptr(), &mut error) },
        0
    );
    assert!(error.is_null());
    assert_eq!(
        unsafe { axton_binding::ffi::configure_application_data(path.as_ptr(), &mut error) },
        0
    );
    let other = tempfile::tempdir().unwrap();
    let other = CString::new(other.path().to_str().unwrap()).unwrap();
    assert_eq!(
        unsafe { axton_binding::ffi::configure_application_data(other.as_ptr(), &mut error) },
        1
    );
    assert!(
        unsafe { CStr::from_ptr(error) }
            .to_str()
            .unwrap()
            .contains("already fixed")
    );
    unsafe { axton_binding::ffi::free(error) };
    let db = directory.path().join("db");
    let first = axton_sqlite::SqliteStore::open_exclusive(&db).unwrap();
    assert!(axton_sqlite::SqliteStore::open_exclusive(&db).is_err());
    drop(first);
    let locks = directory.path().join("axton-store-locks");
    assert_eq!(std::fs::read_dir(locks).unwrap().count(), 1);
}
