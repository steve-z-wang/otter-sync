//! Names the Apple dynamic library `@rpath/libaxton_dart.dylib`, the name the
//! Dart build hook bundles it under, and reserves header space so the Dart
//! and Flutter tools can rewrite that name to where they bundle it.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_VENDOR").as_deref() == Ok("apple") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/libaxton_dart.dylib");
        println!("cargo:rustc-cdylib-link-arg=-Wl,-headerpad_max_install_names");
    }
}
