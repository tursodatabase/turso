use crate::common::maybe_setup_tracing;
use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, LimboError, OpenFlags};

#[test]
fn invalid_database_errors_on_first_query() -> anyhow::Result<()> {
    maybe_setup_tracing();
    let dir = TempDir::new()?;
    let db_path = dir.path().join("invalid.db");
    std::fs::write(&db_path, b"definitely not a database")?;

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new()?);
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::ReadOnly,
        DatabaseOpts::new(),
        None,
    )?;

    let conn = db.connect()?;
    let err = conn
        .prepare("SELECT name FROM sqlite_master LIMIT 1")
        .unwrap_err();
    assert!(matches!(err, LimboError::NotADB));

    Ok(())
}
