//! Names the Apple dynamic library `@rpath/libaxton_dart.dylib`, the name the
//! Dart build hook bundles it under, so an application finds it in its own
//! frameworks rather than at the path it was built at.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_VENDOR").as_deref() == Ok("apple") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/libaxton_dart.dylib");
    }
}
