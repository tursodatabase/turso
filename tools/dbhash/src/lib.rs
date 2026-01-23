//! turso-dbhash: Compute SHA1 hash of SQLite database content.
//!
//! This tool computes a hash of a database's **logical content**, independent
//! of its physical representation. Two databases with identical data produce
//! the same hash even if they have different page sizes, encodings, or layouts.

mod encoder;

use std::sync::Arc;

use sha1::{Digest, Sha1};
use std::num::NonZero;
use turso_core::{Database, LimboError, PlatformIO, StepResult, Value, IO};

pub use encoder::encode_value;

#[derive(Debug, Clone, Default)]
pub struct DbHashOptions {
    /// Only hash tables matching this SQL LIKE pattern.
    pub table_filter: Option<String>,
    /// If true, only hash schema (no table content).
    pub schema_only: bool,
    /// If true, only hash content (no schema).
    pub without_schema: bool,
    /// If true, print each value to stderr as it's hashed.
    pub debug_trace: bool,
}

#[derive(Debug)]
pub struct DbHashResult {
    /// 40-character lowercase hex SHA1.
    pub hash: String,
    /// Number of tables hashed.
    pub tables_hashed: usize,
    /// Number of rows hashed.
    pub rows_hashed: usize,
}

/// Compute content hash of a database.
///
/// The hash is computed over the logical content of the database:
/// 1. Table content (unless `schema_only` is set)
/// 2. Schema entries (unless `without_schema` is set)
///
/// System tables (sqlite_%), virtual tables, and statistics tables are excluded.
pub fn hash_database(path: &str, options: &DbHashOptions) -> Result<DbHashResult, LimboError> {
    assert_eq!(
        !(options.schema_only && options.without_schema),
        false,
        "`schema_only` and `without_schema` cannot both be true"
    );
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
    let db = Database::open_file(io.clone(), path)?;
    let conn = db.connect()?;

    let mut hasher = Sha1::new();
    let mut tables_hashed = 0;
    let mut rows_hashed = 0;

    let filter = options.table_filter.as_deref().unwrap_or("%");

    // 1. Hash table content (unless schema_only)
    if !options.schema_only {
        let tables = get_table_names(&conn, &io, filter)?;
        for table in &tables {
            tables_hashed += 1;
            rows_hashed += hash_table(&conn, &io, table, &mut hasher, options.debug_trace)?;
        }
    }

    // 2. Hash schema (unless without_schema)
    if !options.without_schema {
        hash_schema(&conn, &io, filter, &mut hasher, options.debug_trace)?;
    }

    let hash = hex::encode(hasher.finalize());

    Ok(DbHashResult {
        hash,
        tables_hashed,
        rows_hashed,
    })
}

/// Get list of user tables (excludes sqlite_%, virtual tables).
fn get_table_names(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<dyn IO>,
    like_pattern: &str,
) -> Result<Vec<String>, LimboError> {
    let sql = r#"SELECT name FROM sqlite_schema
           WHERE type = 'table'
             AND sql NOT LIKE 'CREATE VIRTUAL%'
             AND name NOT LIKE 'sqlite_%'
             AND name LIKE ?1
           ORDER BY name COLLATE nocase"#;

    let mut stmt = conn.prepare(sql)?;
    stmt.bind_at(
        NonZero::new(1).unwrap(),
        Value::from_text(like_pattern.to_string()),
    );
    let mut names = Vec::new();

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let name = row.get_value(0).to_text().expect("table name must be text");
                names.push(name.to_string());
            }
            StepResult::IO => io.step()?,
            StepResult::Done => break,
            StepResult::Busy | StepResult::Interrupt => {
                return Err(LimboError::Busy);
            }
        }
    }

    Ok(names)
}

/// Hash all rows in a table.
fn hash_table(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<dyn IO>,
    table_name: &str,
    hasher: &mut Sha1,
    debug: bool,
) -> Result<usize, LimboError> {
    // Quote table name for safety (escape internal double quotes)
    let sql = format!("SELECT * FROM \"{}\"", table_name.replace('"', "\"\""));
    let mut stmt = conn.prepare(&sql)?;
    let mut row_count = 0;
    let mut buf = Vec::new();

    loop {
        match stmt.step()? {
            StepResult::Row => {
                row_count += 1;
                let row = stmt.row().unwrap();
                for value in row.get_values() {
                    buf.clear();
                    encode_value(value, &mut buf);
                    if debug {
                        eprintln!("{value:?}");
                    }
                    hasher.update(&buf);
                }
            }
            StepResult::IO => io.step()?,
            StepResult::Done => break,
            StepResult::Busy | StepResult::Interrupt => {
                return Err(LimboError::Busy);
            }
        }
    }

    Ok(row_count)
}

/// Hash schema entries.
fn hash_schema(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<dyn IO>,
    like_pattern: &str,
    hasher: &mut Sha1,
    debug: bool,
) -> Result<(), LimboError> {
    let sql = r#"SELECT type, name, tbl_name, sql FROM sqlite_schema
           WHERE tbl_name LIKE ?1
           ORDER BY name COLLATE nocase"#;

    let mut stmt = conn.prepare(sql)?;
    stmt.bind_at(
        NonZero::new(1).unwrap(),
        Value::from_text(like_pattern.to_string()),
    );
    let mut buf = Vec::new();

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                for value in row.get_values() {
                    buf.clear();
                    encode_value(value, &mut buf);
                    if debug {
                        eprintln!("{value:?}");
                    }
                    hasher.update(&buf);
                }
            }
            StepResult::IO => io.step()?,
            StepResult::Done => break,
            StepResult::Busy | StepResult::Interrupt => {
                return Err(LimboError::Busy);
            }
        }
    }

    Ok(())
}
