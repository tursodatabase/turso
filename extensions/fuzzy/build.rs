fn main() {
    // Gate on the *build target* OS, not the host: cfg!() in a build script
    // evaluates against the host, so cross-compiling this crate from a
    // Windows machine to any other target injected the Windows-only
    // advapi32 link flag into the target and failed the link with
    // "unable to find library -ladvapi32".
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rustc-link-lib=advapi32");
    }
}
