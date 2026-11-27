//! Writes `postgres/catalog/src/generated.rs` from
//! `postgres/catalog/upstream/postgres.bki`.
//!
//!     cargo run -p turso_pg_catalog_gen
//!
//! The two paths can be given as arguments, the input first.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let catalog_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut args = std::env::args_os().skip(1);
    let input = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| catalog_dir.join("upstream/postgres.bki"));
    let output = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| catalog_dir.join("src/generated.rs"));

    let bki = match std::fs::read_to_string(&input) {
        Ok(bki) => bki,
        Err(error) => {
            eprintln!("error: cannot read {}: {error}", input.display());
            return ExitCode::FAILURE;
        }
    };
    let code = match turso_pg_catalog_gen::generate(&bki) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {}: {error}", input.display());
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = std::fs::write(&output, code) {
        eprintln!("error: cannot write {}: {error}", output.display());
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
