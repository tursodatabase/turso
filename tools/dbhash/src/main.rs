//! turso-dbhash CLI - Compute SHA1 hash of SQLite database content.

use clap::Parser;
use turso_core::EncryptionOpts;
use turso_dbhash::{hash_database, DbHashOptions};

#[derive(Parser)]
#[command(name = "turso-dbhash")]
#[command(version, about = "Compute SHA1 hash of SQLite database content")]
struct Args {
    /// Database files to hash
    #[arg(required = true)]
    files: Vec<String>,

    /// Only hash tables matching SQL LIKE pattern
    #[arg(long, value_name = "PATTERN")]
    like: Option<String>,

    /// Only hash schema (no table content)
    #[arg(long)]
    schema_only: bool,

    /// Only hash content (no schema)
    #[arg(long)]
    without_schema: bool,

    /// Trace hash inputs to stderr
    #[arg(long)]
    debug: bool,

    /// Cipher to use when opening an encrypted database
    #[arg(long, value_name = "CIPHER", requires = "hexkey")]
    cipher: Option<String>,

    /// Hex-encoded encryption key for an encrypted database
    #[arg(long, value_name = "HEXKEY", requires = "cipher")]
    hexkey: Option<String>,
}

fn main() {
    let args = Args::parse();

    if args.schema_only && args.without_schema {
        eprintln!("Error: --schema-only and --without-schema are mutually exclusive");
        std::process::exit(1);
    }

    let options = DbHashOptions {
        table_filter: args.like,
        schema_only: args.schema_only,
        without_schema: args.without_schema,
        debug_trace: args.debug,
        encryption: args.cipher.map(|cipher| EncryptionOpts {
            cipher,
            hexkey: args.hexkey.expect("required by clap"),
        }),
    };

    let mut exit_code = 0;

    for file in &args.files {
        match hash_database(file, &options) {
            Ok(result) => {
                println!("{} {}", result.hash, file);
            }
            Err(e) => {
                eprintln!("Error hashing '{file}': {e}");
                exit_code = 1;
            }
        }
    }

    std::process::exit(exit_code);
}
