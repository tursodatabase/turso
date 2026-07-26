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
}
