#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Barrier,
};

use turso_core::{
    io::FileSyncType, Buffer, Completion, Connection, Database, DatabaseOpts, DatabaseStorage,
    IOContext, OpenOptions, SharedPageCache, SqliteDialect,
};

use crate::common::{ExecRows, TempDatabase, TempDatabaseBuilder};

const CACHE_CAPACITY: usize = 8 * 1024 * 1024;

struct CountingDatabaseStorage {
    inner: Arc<dyn DatabaseStorage>,
    page_reads: AtomicU64,
}

impl CountingDatabaseStorage {
    fn new(inner: Arc<dyn DatabaseStorage>) -> Self {
        Self {
            inner,
            page_reads: AtomicU64::new(0),
        }
    }

    fn reset_page_reads(&self) {
        self.page_reads.store(0, Ordering::Relaxed);
    }

    fn page_reads(&self) -> u64 {
        self.page_reads.load(Ordering::Relaxed)
    }
}

impl DatabaseStorage for CountingDatabaseStorage {
    fn read_header(&self, c: Completion) -> turso_core::Result<Completion> {
        self.inner.read_header(c)
    }

    fn read_page(
        &self,
        page_idx: usize,
        io_ctx: &IOContext,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.page_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_page(page_idx, io_ctx, c)
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<Buffer>,
        io_ctx: &IOContext,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner.write_page(page_idx, buffer, io_ctx, c)
    }

    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<Arc<Buffer>>,
        io_ctx: &IOContext,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner
            .write_pages(first_page_idx, page_size, buffers, io_ctx, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        self.inner.sync(c, sync_type)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: usize, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

fn database_with_cache() -> (TempDatabase, Arc<SharedPageCache>) {
    database_with_cache_capacity(CACHE_CAPACITY)
}

fn database_with_cache_capacity(capacity_bytes: usize) -> (TempDatabase, Arc<SharedPageCache>) {
    let database = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new())
        .build();
    let cache = Arc::new(SharedPageCache::new(capacity_bytes));
    database
        .db
        .set_shared_page_cache(Some(cache.clone()))
        .unwrap();
    (database, cache)
}

fn seed_rows(connection: &Arc<Connection>, rows: usize) {
    connection
        .execute("CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        .unwrap();
    connection.execute("BEGIN").unwrap();
    for id in 1..=rows {
        connection
            .execute(format!(
                "INSERT INTO test VALUES ({id}, printf('%04d-', {id}) || hex(zeroblob(512)))"
            ))
            .unwrap();
    }
    connection.execute("COMMIT").unwrap();
}

fn table_summary(connection: &Arc<Connection>) -> (i64, i64) {
    connection
        .exec_rows("SELECT count(*), sum(length(value)) FROM test")
        .into_iter()
        .next()
        .unwrap()
}

fn value(connection: &Arc<Connection>, id: usize) -> String {
    let rows: Vec<(String,)> =
        connection.exec_rows(&format!("SELECT value FROM test WHERE id = {id}"));
    rows.into_iter().next().unwrap().0
}

#[cfg(unix)]
fn seed_single_value(connection: &Arc<Connection>, value: &str) {
    connection
        .execute("CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        .unwrap();
    connection
        .execute(format!(
            "INSERT INTO test VALUES (1, '{}')",
            value.replace('\'', "''")
        ))
        .unwrap();
}

#[cfg(unix)]
fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut path = path.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

#[test]
fn fresh_connections_reuse_clean_pages() {
    let (database, cache) = database_with_cache();
    let writer = database.connect_limbo();
    seed_rows(&writer, 256);

    let first_reader = database.connect_limbo();
    let expected = table_summary(&first_reader);
    let after_first = cache.stats();
    assert!(after_first.entries > 0);

    let second_reader = database.connect_limbo();
    assert_eq!(table_summary(&second_reader), expected);
    let after_second = cache.stats();

    assert!(after_second.hits > after_first.hits);
    assert_eq!(after_second.misses, after_first.misses);
    assert!(after_second.resident_bytes <= after_second.capacity_bytes);
}

#[test]
fn warm_scan_avoids_database_storage_page_reads() {
    let seeded = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new())
        .build();
    let writer = seeded.connect_limbo();
    seed_rows(&writer, 256);
    let expected = table_summary(&writer);
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = seeded.path.to_str().unwrap().to_owned();
    let io = seeded.io.clone();
    let storage = Arc::new(CountingDatabaseStorage::new(seeded.db.db_file.clone()));
    drop(writer);
    drop(seeded);

    let database = Database::do_open(
        io,
        &path,
        OpenOptions::new(Arc::new(SqliteDialect))
            .storage(storage.clone())
            .db_opts(DatabaseOpts::new()),
    )
    .unwrap();
    let cache = Arc::new(SharedPageCache::new(CACHE_CAPACITY));
    database.set_shared_page_cache(Some(cache.clone())).unwrap();

    storage.reset_page_reads();
    assert_eq!(table_summary(&database.connect().unwrap()), expected);
    let cold_page_reads = storage.page_reads();
    let after_cold_scan = cache.stats();

    storage.reset_page_reads();
    assert_eq!(table_summary(&database.connect().unwrap()), expected);
    let warm_page_reads = storage.page_reads();
    let after_warm_scan = cache.stats();

    assert!(
        cold_page_reads > 1,
        "cold scan should read multiple database pages, got {cold_page_reads}"
    );
    assert_eq!(
        warm_page_reads, 0,
        "warm scan should be served by the shared page cache"
    );
    assert!(after_warm_scan.hits > after_cold_scan.hits);
    assert_eq!(after_warm_scan.misses, after_cold_scan.misses);
}

#[test]
fn encryption_enabled_database_bypasses_shared_decrypted_bytes() {
    let database = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new().with_encryption(true))
        .build();
    let cache = Arc::new(SharedPageCache::new(CACHE_CAPACITY));
    database
        .db
        .set_shared_page_cache(Some(cache.clone()))
        .unwrap();

    let writer = database.connect_limbo();
    seed_rows(&writer, 64);
    assert_eq!(
        table_summary(&database.connect_limbo()),
        table_summary(&writer)
    );

    let stats = cache.stats();
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);
}

#[test]
fn old_snapshot_reuses_its_version_after_a_new_commit() {
    let (database, cache) = database_with_cache();
    let writer = database.connect_limbo();
    seed_rows(&writer, 64);

    let old_snapshot = database.connect_limbo();
    old_snapshot.execute("BEGIN").unwrap();
    let old_value = value(&old_snapshot, 1);
    let after_warm = cache.stats();

    writer
        .execute("UPDATE test SET value = 'new value' WHERE id = 1")
        .unwrap();
    assert_eq!(value(&database.connect_limbo(), 1), "new value");

    old_snapshot.get_pager().clear_page_cache(false);
    assert_eq!(value(&old_snapshot, 1), old_value);
    let after_old_snapshot_reread = cache.stats();
    assert!(after_old_snapshot_reread.hits > after_warm.hits);

    writer.execute("BEGIN").unwrap();
    writer
        .execute("UPDATE test SET value = 'rolled back' WHERE id = 1")
        .unwrap();
    writer.execute("ROLLBACK").unwrap();
    assert_eq!(value(&database.connect_limbo(), 1), "new value");
    old_snapshot.execute("COMMIT").unwrap();
}

#[test]
fn checkpoint_epoch_prevents_reusing_pre_checkpoint_bytes() {
    let (database, cache) = database_with_cache();
    let writer = database.connect_limbo();
    seed_rows(&writer, 128);

    writer
        .execute("UPDATE test SET value = 'after checkpoint' WHERE id = 1")
        .unwrap();
    assert_eq!(value(&database.connect_limbo(), 1), "after checkpoint");
    let before_checkpoint = cache.stats();

    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let first_reader = database.connect_limbo();
    assert_eq!(value(&first_reader, 1), "after checkpoint");
    let after_first_reader = cache.stats();
    assert!(after_first_reader.misses > before_checkpoint.misses);

    let second_reader = database.connect_limbo();
    assert_eq!(value(&second_reader, 1), "after checkpoint");
    let after_second_reader = cache.stats();
    assert!(after_second_reader.hits > after_first_reader.hits);
}

#[test]
fn concurrent_readers_populate_a_cold_cache_without_sharing_pager_state() {
    let database = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new())
        .build();
    let writer = database.connect_limbo();
    seed_rows(&writer, 256);
    let expected = table_summary(&writer);
    let cache = Arc::new(SharedPageCache::new(CACHE_CAPACITY));
    database
        .db
        .set_shared_page_cache(Some(cache.clone()))
        .unwrap();
    let barrier = Arc::new(Barrier::new(9));

    let readers = (0..8)
        .map(|_| {
            let database = database.db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let connection = database.connect().unwrap();
                barrier.wait();
                table_summary(&connection)
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();

    for reader in readers {
        assert_eq!(reader.join().unwrap(), expected);
    }

    let stats = cache.stats();
    assert!(stats.misses > 0);
    assert!(stats.insertions > 0);
    assert!(stats.entries > 0);
    assert!(stats.resident_bytes <= stats.capacity_bytes);
}

#[test]
fn eviction_falls_back_to_storage_and_repopulates_the_cache() {
    let (database, cache) = database_with_cache_capacity(10 * 1024);
    let writer = database.connect_limbo();
    seed_rows(&writer, 128);

    let expected = table_summary(&database.connect_limbo());
    let after_first = cache.stats();
    assert!(after_first.evictions > 0);

    assert_eq!(table_summary(&database.connect_limbo()), expected);
    let after_second = cache.stats();
    assert!(after_second.misses > after_first.misses);
    assert!(after_second.insertions > after_first.insertions);
    assert!(after_second.resident_bytes <= after_second.capacity_bytes);
}

#[test]
#[cfg(unix)]
fn external_restore_invalidates_bytes_from_the_previous_file_generation() {
    let (database, cache) = database_with_cache();
    let original_writer = database.connect_limbo();
    seed_single_value(&original_writer, "before restore");
    assert_eq!(value(&database.connect_limbo(), 1), "before restore");
    let before_restore = cache.stats();

    let replacement = TempDatabase::new_empty();
    let replacement_writer = replacement.connect_limbo();
    seed_single_value(&replacement_writer, "after restore");

    std::fs::copy(&replacement.path, &database.path).unwrap();
    std::fs::copy(
        sidecar_path(&replacement.path, "-wal"),
        sidecar_path(&database.path, "-wal"),
    )
    .unwrap();
    database.db.reload_wal_after_external_restore().unwrap();

    assert_eq!(value(&database.connect_limbo(), 1), "after restore");
    let after_restore = cache.stats();
    assert!(after_restore.misses > before_restore.misses);
}
