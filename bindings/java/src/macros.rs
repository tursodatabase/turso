// bindings/java/src/macros.rs
#[macro_export]
macro_rules! eprint_return {
    ($log:expr, $error:expr) => {{
        eprintln!("{}", $log);
        Err($error)
    }};
}
