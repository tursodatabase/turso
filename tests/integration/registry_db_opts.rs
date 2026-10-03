use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, OpenFlags, OpenOptions, PlatformIO, SqliteDialect};

fn open_with(path: &str, opts: DatabaseOpts) -> turso_core::Result<Arc<Database>> {
    let io: Arc<dyn turso_core::IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open(
        io,
        path,
        OpenOptions::new(Arc::new(SqliteDialect))
            .flags(OpenFlags::Create)
            .db_opts(opts),
    )
}

#[test]
fn registry_hit_does_not_give_passive_checkpoint_to_caller_that_did_not_ask() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("registry-db-opts.db");
    let path = path.to_str().unwrap();

    let mut passive = DatabaseOpts::new();
    passive.enable_experimental_mvcc_passive_checkpoint = true;
    let first = open_with(path, passive).unwrap();
    first
        .connect()
        .unwrap()
        .execute("CREATE TABLE t(x)")
        .unwrap();
    assert!(first.experimental_mvcc_passive_checkpoint_enabled());

    match open_with(path, DatabaseOpts::new()) {
        Err(_) => {}
        Ok(second) => assert!(
            !second.experimental_mvcc_passive_checkpoint_enabled(),
            "second open asked for no experimental features but got passive checkpoint from the registry"
        ),
    }
}

#[test]
fn registry_hit_does_not_drop_passive_checkpoint_the_caller_asked_for() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("registry-db-opts-2.db");
    let path = path.to_str().unwrap();

    let first = open_with(path, DatabaseOpts::new()).unwrap();
    first
        .connect()
        .unwrap()
        .execute("CREATE TABLE t(x)")
        .unwrap();
    assert!(!first.experimental_mvcc_passive_checkpoint_enabled());

    let mut passive = DatabaseOpts::new();
    passive.enable_experimental_mvcc_passive_checkpoint = true;
    match open_with(path, passive) {
        Err(_) => {}
        Ok(second) => assert!(
            second.experimental_mvcc_passive_checkpoint_enabled(),
            "second open asked for passive checkpoint but the registry silently dropped it"
        ),
    }
}
