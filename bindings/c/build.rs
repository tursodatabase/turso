fn main() {
    println!("cargo:rustc-link-search=native=target/debug");
    println!("cargo:rerun-if-changed=src/varargs.c");
    // varargs.c exists because some sqlite3 entry points (db_config, and
    // eventually mprintf/snprintf) are C-variadic, which stable Rust cannot
    // define. Faking them with fixed args breaks on Apple arm64, where
    // variadic arguments are passed on the stack but named arguments in
    // registers — the callee would read garbage. The exported sqlite3_*
    // symbols themselves are naked-function trampolines in lib.rs; see the
    // comment there.
    cc::Build::new()
        .file("src/varargs.c")
        .compile("turso_sqlite3_varargs");
    if let Some(profile_dir) = target_profile_dir() {
        println!("cargo:rustc-link-search=native={}", profile_dir.display());
    }

    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        configure_msvc_sqlite3();
    }
}

fn target_profile_dir() -> Option<std::path::PathBuf> {
    let out_dir = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR").expect("Cargo must set OUT_DIR for build scripts"),
    );
    out_dir
        .ancestors()
        .find(|path| path.file_name().and_then(|name| name.to_str()) == Some("build"))
        .and_then(std::path::Path::parent)
        .map(std::path::Path::to_path_buf)
}

fn configure_msvc_sqlite3() {
    for variable in [
        "VCPKG_ROOT",
        "VCPKGRS_TRIPLET",
        "VCPKGRS_DYNAMIC",
        "VCPKGRS_DISABLE",
        "VCPKGRS_NO_SQLITE3",
        "SQLITE3_NO_VCPKG",
        "NO_VCPKG",
    ] {
        println!("cargo:rerun-if-env-changed={variable}");
    }

    if std::env::var_os("CARGO_FEATURE_SQLITE3").is_none() {
        return;
    }

    let library = vcpkg::find_package("sqlite3").unwrap_or_else(|error| {
        panic!(
            "failed to find static SQLite with vcpkg: {error}; \
             the `sqlite3` feature on MSVC requires \
             `vcpkg install sqlite3:x64-windows-static-md`"
        )
    });
    assert!(
        library.is_static,
        "MSVC SQLite compatibility tests require static vcpkg linkage"
    );
}
