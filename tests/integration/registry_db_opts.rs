//! The process-wide database registry shares one `Database` per file. These
//! tests check that a later open whose `DatabaseOpts` the shared instance
//! cannot give it is rejected instead of silently getting the first opener's
//! options.

use std::sync::Arc;
use turso_core::storage::database::DatabaseFile;
use turso_core::{
    ConnectOpts, Database, DatabaseOpts, IOResult, LimboError, OpenDbAsyncState, OpenFlags,
    OpenOptions, PlatformIO, SqliteDialect, IO,
};

fn platform_io() -> Arc<dyn IO> {
    Arc::new(PlatformIO::new().unwrap())
}

fn open_options(flags: OpenFlags, opts: DatabaseOpts) -> OpenOptions {
    OpenOptions::new(Arc::new(SqliteDialect))
        .flags(flags)
        .db_opts(opts)
}

fn open_with_flags(
    path: &str,
    flags: OpenFlags,
    opts: DatabaseOpts,
) -> turso_core::Result<Arc<Database>> {
    Database::open(platform_io(), path, open_options(flags, opts))
}

fn open_with(path: &str, opts: DatabaseOpts) -> turso_core::Result<Arc<Database>> {
    open_with_flags(path, OpenFlags::Create, opts)
}

/// `Database::open_async` checks the registry itself instead of going
/// through the lookup `Database::open` uses, so both paths need coverage.
fn open_async_with(path: &str, opts: DatabaseOpts) -> turso_core::Result<Arc<Database>> {
    let io = platform_io();
    let file = io.open_file(path, OpenFlags::Create | OpenFlags::NoLock, false)?;
    let options = open_options(OpenFlags::Create, opts).storage(Arc::new(DatabaseFile::new(file)));
    let mut state = OpenDbAsyncState::new();
    loop {
        match Database::open_async(&mut state, io.clone(), path, &options) {
            Ok(IOResult::Done(db)) => return Ok(db),
            Ok(IOResult::IO(completion)) => completion.wait(&*io)?,
            Err(err) => return Err(*err),
        }
    }
}

fn create_table(db: &Arc<Database>) {
    db.connect(ConnectOpts::new())
        .unwrap()
        .execute("CREATE TABLE t(x)")
        .unwrap();
}

fn assert_rejected(result: turso_core::Result<Arc<Database>>, expected_message: &str) {
    match result {
        Err(LimboError::InvalidArgument(message)) => assert!(
            message.contains(expected_message),
            "expected an error mentioning {expected_message:?}, got {message:?}"
        ),
        Err(other) => panic!("expected InvalidArgument, got {other:?}"),
        Ok(_) => panic!("expected the open to be rejected because of {expected_message:?}"),
    }
}

#[test]
fn matching_opts_share_the_registered_instance() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("same.db");
    let path = path.to_str().unwrap();

    let opts = DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true);
    let first = open_with(path, opts).unwrap();
    create_table(&first);
    let second = open_with(path, opts).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn passive_checkpoint_mismatch_is_rejected_in_both_directions() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("passive.db");
    let path = path.to_str().unwrap();
    let passive = DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true);

    let first = open_with(path, passive).unwrap();
    create_table(&first);
    assert_rejected(
        open_with(path, DatabaseOpts::new()),
        "experimental MVCC passive checkpoint enabled; requested experimental MVCC passive checkpoint disabled",
    );
    drop(first);

    let first = open_with(path, DatabaseOpts::new()).unwrap();
    assert_rejected(
        open_with(path, passive),
        "experimental MVCC passive checkpoint disabled; requested experimental MVCC passive checkpoint enabled",
    );
    drop(first);
}

#[test]
fn multiprocess_wal_mismatch_is_rejected_in_both_directions() {
    if !platform_io().supports_shared_wal_coordination() {
        println!("skipping: platform IO does not support multiprocess WAL");
        return;
    }
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("multiprocess.db");
    let path = path.to_str().unwrap();
    let multiprocess = DatabaseOpts::new().with_multiprocess_wal(true);

    let first = open_with(path, multiprocess).unwrap();
    create_table(&first);
    assert_rejected(
        open_with(path, DatabaseOpts::new()),
        "experimental multiprocess WAL enabled; requested experimental multiprocess WAL disabled",
    );
    drop(first);

    let first = open_with(path, DatabaseOpts::new()).unwrap();
    assert_rejected(
        open_with(path, multiprocess),
        "experimental multiprocess WAL disabled; requested experimental multiprocess WAL enabled",
    );
    drop(first);
}

#[test]
fn autovacuum_mismatch_is_rejected_in_both_directions() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("autovacuum.db");
    let path = path.to_str().unwrap();
    let autovacuum = DatabaseOpts::new().with_autovacuum(true);

    let first = open_with(path, autovacuum).unwrap();
    create_table(&first);
    assert_rejected(
        open_with(path, DatabaseOpts::new()),
        "experimental autovacuum enabled; requested experimental autovacuum disabled",
    );
    drop(first);

    let first = open_with(path, DatabaseOpts::new()).unwrap();
    assert_rejected(
        open_with(path, autovacuum),
        "experimental autovacuum disabled; requested experimental autovacuum enabled",
    );
    drop(first);
}

#[test]
fn mvcc_request_on_an_instance_without_mvcc_is_rejected() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("wal.db");
    let path = path.to_str().unwrap();

    let first = open_with(path, DatabaseOpts::new()).unwrap();
    create_table(&first);
    assert!(!first.mvcc_enabled());
    assert_rejected(
        open_with(path, DatabaseOpts::new().with_mvcc(true)),
        "already open without MVCC",
    );

    // A read-only open never converts the database, so it accepts the
    // instance as it is.
    let read_only = open_with_flags(
        path,
        OpenFlags::ReadOnly,
        DatabaseOpts::new().with_mvcc(true),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&first, &read_only));
}

#[test]
fn plain_open_of_an_mvcc_instance_keeps_mvcc() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("mvcc.db");
    let path = path.to_str().unwrap();

    let first = open_with(path, DatabaseOpts::new().with_mvcc(true)).unwrap();
    create_table(&first);
    assert!(first.mvcc_enabled());
    let second = open_with(path, DatabaseOpts::new()).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(second.mvcc_enabled());
}

#[test]
fn open_async_checks_db_opts_on_a_registry_hit() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("async.db");
    let path = path.to_str().unwrap();
    let passive = DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true);

    let first = open_with(path, passive).unwrap();
    create_table(&first);
    assert_rejected(
        open_async_with(path, DatabaseOpts::new()),
        "experimental MVCC passive checkpoint enabled; requested experimental MVCC passive checkpoint disabled",
    );
    let same = open_async_with(path, passive).unwrap();
    assert!(Arc::ptr_eq(&first, &same));
}

/// ATTACH copies its options from the main database instead of a user
/// request, so it must accept a file that is already open with other options.
#[test]
fn attach_accepts_an_instance_opened_with_other_opts() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let passive_path = tmp_dir.path().join("passive.db");
    let passive_path = passive_path.to_str().unwrap();
    let plain_path = tmp_dir.path().join("plain.db");
    let plain_path = plain_path.to_str().unwrap();

    let passive_opts = DatabaseOpts::new()
        .with_attach(true)
        .with_experimental_mvcc_passive_checkpoint(true);
    let passive_db = open_with(passive_path, passive_opts).unwrap();
    create_table(&passive_db);
    let plain_db = open_with(plain_path, DatabaseOpts::new().with_attach(true)).unwrap();
    create_table(&plain_db);

    // A plain main database attaches a file that is open with passive
    // checkpoint; the attached instance keeps passive checkpoint.
    let plain_conn = plain_db.connect(ConnectOpts::new()).unwrap();
    plain_conn
        .execute(format!("ATTACH '{passive_path}' AS passive"))
        .unwrap();
    plain_conn
        .execute("INSERT INTO passive.t VALUES (1)")
        .unwrap();

    // A passive main database attaches a plain file, and its own file.
    let passive_conn = passive_db.connect(ConnectOpts::new()).unwrap();
    passive_conn
        .execute(format!("ATTACH '{plain_path}' AS plain"))
        .unwrap();
    passive_conn
        .execute(format!("ATTACH '{passive_path}' AS myself"))
        .unwrap();
    passive_conn
        .execute("INSERT INTO plain.t VALUES (2)")
        .unwrap();
    passive_conn
        .execute("INSERT INTO myself.t VALUES (3)")
        .unwrap();
}
