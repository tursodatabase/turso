use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
};

use turso_core::{
    Buffer, Completion, DatabaseStorage, LimboError, OpenDbAsyncState, OpenFlags, Value,
};

use crate::{
    database_replay_generator::DatabaseReplayGenerator,
    database_sync_engine_io::SyncEngineIo,
    database_sync_lazy_storage::LazyDatabaseStorage,
    database_sync_operations::{
        acquire_slot, apply_logical_transactions_file_without_commit_with_table_map,
        apply_transformation, bootstrap_db_file, connect_untracked, count_local_changes, has_table,
        is_logically_replayable_table, max_local_change_id, pull_updates_v1, push_logical_changes,
        read_last_change_id, read_logical_replay_table_map, read_wal_salt, reset_wal_file,
        summarize_logical_transactions_file, update_last_change_id, wait_all_results,
        wal_apply_from_file, wal_pull_to_file, PullUpdatesV1Result, SyncEngineIoStats,
        SyncOperationCtx, PAGE_SIZE, WAL_FRAME_HEADER, WAL_FRAME_SIZE,
    },
    database_tape::{
        run_stmt_once, DatabaseChangesIteratorMode, DatabaseChangesIteratorOpts,
        DatabaseReplaySession, DatabaseReplaySessionOpts, DatabaseTape, DatabaseTapeOpts,
        DatabaseWalSession, CDC_PRAGMA_NAME,
    },
    errors::Error,
    io_operations::IoOperations,
    types::{
        Coro, DatabaseMetadata, DatabasePullRevision, DatabaseRowTransformResult,
        DatabaseSavedConfiguration, DatabaseSyncEngineProtocolVersion, DatabaseTapeOperation,
        DatabaseTapeRowChange, DbChangesStatus, DbChangesStreamKind, PartialSyncOpts,
        SyncEngineIoResult, SyncEngineStats, DATABASE_METADATA_VERSION,
    },
    wal_session::WalSession,
    Result,
};

#[derive(Clone, Debug)]
pub struct DatabaseSyncEngineOpts {
    pub remote_url: Option<String>,
    pub client_name: String,
    pub tables_ignore: Vec<String>,
    pub use_transform: bool,
    pub wal_pull_batch_size: u64,
    pub long_poll_timeout: Option<std::time::Duration>,
    pub protocol_version_hint: DatabaseSyncEngineProtocolVersion,
    pub bootstrap_if_empty: bool,
    pub reserved_bytes: usize,
    pub partial_sync_opts: Option<PartialSyncOpts>,
    /// Base64-encoded encryption key for the Turso Cloud database
    pub remote_encryption_key: Option<String>,
    /// When set, [`push_changes_to_remote`] sends the local change set to the
    /// remote in multiple HTTP batches, sealing the current batch as soon as it
    /// has accumulated >= `push_operations_threshold` operations *and* the
    /// next batch boundary lines up with a transaction boundary in the local
    /// CDC log. Splits never happen mid-transaction. `None` preserves the
    /// previous behaviour of pushing the whole change set in one batch.
    pub push_operations_threshold: Option<usize>,
    /// Optional hint, in bytes, to chunk the initial bootstrap download into
    /// multiple `/pull-updates` HTTP requests using the `server_pages_selector`
    /// bitmap. Each chunk covers the smallest contiguous range of pages whose
    /// total size is >= `pull_bytes_threshold`. `None` (default) bootstraps in
    /// a single HTTP round-trip. Currently applied only to the bootstrap phase
    /// — incremental pulls are unaffected. **No-op when partial-sync uses the
    /// `Query` bootstrap strategy** — the server picks the page set, so the
    /// client can't chunk it locally.
    pub pull_bytes_threshold: Option<usize>,
    /// Opt into raw MVCC logical-log pull-updates streams when the server supports them.
    pub logical_mvcc_pull: bool,
}

pub struct DataStats {
    pub written_bytes: AtomicUsize,
    pub read_bytes: AtomicUsize,
}

impl Default for DataStats {
    fn default() -> Self {
        Self::new()
    }
}

impl DataStats {
    pub fn new() -> Self {
        Self {
            written_bytes: AtomicUsize::new(0),
            read_bytes: AtomicUsize::new(0),
        }
    }
    pub fn write(&self, size: usize) {
        self.written_bytes.fetch_add(size, Ordering::SeqCst);
    }
    pub fn read(&self, size: usize) {
        self.read_bytes.fetch_add(size, Ordering::SeqCst);
    }
}

pub struct DatabaseSyncEngine<IO: SyncEngineIo> {
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: SyncEngineIoStats<IO>,
    db_file: Arc<dyn turso_core::storage::database::DatabaseStorage>,
    main_tape: DatabaseTape,
    main_db_wal_path: String,
    revert_db_wal_path: String,
    main_db_path: String,
    meta_path: String,
    changes_file: Arc<Mutex<Option<Arc<dyn turso_core::File>>>>,
    opts: DatabaseSyncEngineOpts,
    meta: Mutex<DatabaseMetadata>,
    client_unique_id: String,
    partial_lazy_server_revision: Option<Arc<RwLock<String>>>,
}

fn db_size_from_page(page: &[u8]) -> u32 {
    u32::from_be_bytes(page[28..28 + 4].try_into().unwrap())
}
fn is_memory(main_db_path: &str) -> bool {
    main_db_path == ":memory:"
}
fn create_main_db_wal_path(main_db_path: &str) -> String {
    format!("{main_db_path}-wal")
}
fn create_revert_db_wal_path(main_db_path: &str) -> String {
    format!("{main_db_path}-wal-revert")
}
fn create_meta_path(main_db_path: &str) -> String {
    format!("{main_db_path}-info")
}
fn create_changes_path(main_db_path: &str) -> String {
    format!("{main_db_path}-changes")
}

/// Path of the MVCC logical log that belongs to the main database file.
fn create_main_db_log_path(main_db_path: &str) -> String {
    std::path::Path::new(main_db_path)
        .with_extension("db-log")
        .to_string_lossy()
        .to_string()
}

// Durable marker written while a replace-base apply is in progress. If the
// process exits before the guard is completed, startup uses this marker to
// restore the backed-up local files.
fn create_replace_base_marker_path(main_db_path: &str) -> String {
    format!("{main_db_path}-replace-base-apply")
}

// Per-file backup path for the local files that replace-base may overwrite.
fn replace_base_backup_path(main_db_path: &str, name: &str) -> String {
    format!("{main_db_path}-replace-base-apply-{name}.backup")
}

/// One local file protected by the replace-base apply guard.
///
/// `present` records whether the file existed when the guard was created. On
/// restore, missing-at-start files are removed instead of copied back.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ReplaceBaseBackupFile {
    path: String,
    backup_path: String,
    present: bool,
}

/// Crash-recovery record for a replace-base apply.
///
/// The marker contains this manifest after all backups have been copied. Its
/// presence means the local database must be restored from the listed backups
/// before normal sync engine startup can continue.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ReplaceBaseBackupManifest {
    version: u32,
    operation: String,
    main_db_path: String,
    previous_synced_revision: Option<DatabasePullRevision>,
    files: Vec<ReplaceBaseBackupFile>,
}

/// RAII-style guard for replace-base page snapshots.
///
/// Replace-base overwrites the local base database and then replays pending
/// local CDC. If any step fails, or if the process restarts while the marker is
/// present, the guard restores the original database, WAL, logical log, revert
/// WAL, and metadata files from backups.
struct ReplaceBaseApplyGuard<IO: SyncEngineIo> {
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: SyncEngineIoStats<IO>,
    main_db_path: String,
    manifest: ReplaceBaseBackupManifest,
    complete: bool,
}

#[cfg(test)]
thread_local! {
    static REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER: std::cell::Cell<usize> =
        const { std::cell::Cell::new(usize::MAX) };
}

/// Reads the test/debug fault-injection point for replace-base local replay.
#[cfg(test)]
fn replace_base_local_replay_failure_after() -> usize {
    REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER.with(|value| value.get())
}

/// Injects a deterministic failure after a selected local replay operation.
///
/// This is intentionally scoped to replace-base page applies so tests can
/// assert that the backup guard restores the original local files and does not
/// advance the synced revision on partial replay.
fn maybe_inject_replace_base_local_replay_failure(
    replace_base_pages: bool,
    replay_index: usize,
) -> Result<()> {
    #[cfg(test)]
    {
        if replace_base_pages && replay_index == replace_base_local_replay_failure_after() {
            return Err(Error::DatabaseSyncEngineError(format!(
                "injected replace-base local replay failure at change {replay_index}"
            )));
        }
    }
    let _ = (replace_base_pages, replay_index);
    Ok(())
}

/// Executes schema-sensitive SQL and retries transient schema-cookie races.
///
/// Replace-base and raw-page apply can swap database files underneath an open
/// connection. A `SchemaUpdated` error in that window usually means the SQL
/// applied or observed the new schema boundary; reparsing and retrying lets the
/// connection converge without masking persistent SQL errors.
fn execute_with_schema_retry(conn: &Arc<turso_core::Connection>, sql: &str) -> Result<()> {
    for attempt in 0..=4 {
        match conn.execute(sql) {
            Ok(()) => return Ok(()),
            Err(LimboError::SchemaUpdated) if attempt < 4 => {
                // Replace-base installs pages outside normal SQL execution, and
                // schema-changing statements can also report SQLITE_SCHEMA after
                // they apply. Refresh and retry until the connection converges.
                force_reparse_schema_with_retry(conn)?;
                conn.publish_schema_after_external_restore();
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Forces a schema reparse with a small bounded retry for restore races.
fn force_reparse_schema_with_retry(conn: &Arc<turso_core::Connection>) -> Result<()> {
    for attempt in 0..=2 {
        match conn.force_reparse_schema() {
            Ok(()) => return Ok(()),
            // Reparse itself reads page 1 and can cross a schema-cookie update
            // boundary immediately after a raw page restore. A bounded retry is
            // enough to converge without hiding persistent schema errors.
            Err(LimboError::SchemaUpdated) if attempt < 2 => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Refreshes an open connection after sync has restored files outside SQLite.
///
/// This rebuilds WAL/MVCC state, clears cached pages, reparses schema, and
/// publishes that schema so subsequent replay uses the restored database view.
fn reload_connection_after_external_restore(
    conn: &Arc<turso_core::Connection>,
    context: &str,
) -> Result<()> {
    // Replace-base and error recovery overwrite database/WAL/log files behind
    // already-open connections. Rebuild the shared WAL/MVCC view and discard
    // cached pages/schema before running any SQL against the restored files.
    match conn.reload_wal_after_external_restore() {
        Ok(()) => {}
        Err(LimboError::SchemaUpdated) => {
            force_reparse_schema_with_retry(conn).map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to refresh schema before retrying WAL reload after {context}: {error}",
                ))
            })?;
            conn.reload_wal_after_external_restore().map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to reload WAL state after {context}: {error}",
                ))
            })?;
        }
        Err(error) => {
            return Err(Error::DatabaseSyncEngineError(format!(
                "failed to reload WAL state after {context}: {error}",
            )));
        }
    }
    conn.get_pager().clear_page_cache(true);
    force_reparse_schema_with_retry(conn).map_err(|error| {
        Error::DatabaseSyncEngineError(
            format!("failed to refresh schema after {context}: {error}",),
        )
    })?;
    conn.publish_schema_after_external_restore();
    Ok(())
}

/// Publishes schema after raw WAL replay mutates database pages externally.
fn publish_schema_after_raw_wal_replay(
    conn: &Arc<turso_core::Connection>,
    context: &str,
) -> Result<()> {
    conn.get_pager().clear_page_cache(true);
    force_reparse_schema_with_retry(conn).map_err(|error| {
        Error::DatabaseSyncEngineError(
            format!("failed to refresh schema after {context}: {error}",),
        )
    })?;
    conn.publish_schema_after_external_restore();
    Ok(())
}

/// Marks all current CDC rows as already observed by `client_id`.
///
/// New clients, imported databases, and replace-base recovery may start with
/// existing CDC rows that should not be pushed back to the remote as fresh local
/// writes. This records the current high-water mark in the sync metadata table.
async fn acknowledge_existing_cdc_for_client<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
    context: &str,
) -> Result<()> {
    let Some(max_change_id) = max_local_change_id(coro, conn).await? else {
        tracing::info!("{context}: no existing local CDC rows to acknowledge");
        return Ok(());
    };
    let (pull_gen, current_change_id) = read_last_change_id(coro, conn, client_id).await?;
    if current_change_id.is_some_and(|change_id| change_id >= max_change_id) {
        tracing::info!(
            "{context}: local CDC already acknowledged through change_id={max_change_id}"
        );
        return Ok(());
    }
    tracing::info!(
        "{context}: acknowledging imported local CDC through change_id={max_change_id} for client_id={client_id}"
    );
    update_last_change_id(coro, conn, client_id, pull_gen, max_change_id).await
}

/// Recreates the local sync metadata table after page-level restore.
///
/// Raw page replay can replace the table contents along with ordinary pages.
/// This helper resets the table and writes the high-water mark that should
/// survive the restore.
async fn rebuild_local_sync_metadata_table<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
    pull_gen: i64,
    change_id: i64,
) -> Result<()> {
    execute_with_schema_retry(conn, "DROP TABLE IF EXISTS turso_sync_last_change_id").map_err(
        |error| {
            Error::DatabaseSyncEngineError(format!(
                "failed to reset local sync high-water mark table after raw page replay: {error}",
            ))
        },
    )?;
    conn.publish_schema_if_newer();
    update_last_change_id(coro, conn, client_id, pull_gen, change_id).await
}

/// Drives a core I/O completion from the sync coroutine executor.
async fn drive_core_completion<Ctx>(
    coro: &Coro<Ctx>,
    io: &Arc<dyn turso_core::IO>,
    completion: Completion,
) -> Result<()> {
    // Drive core async I/O through the sync engine coroutine scheduler. The
    // final wait_for_completion call is non-blocking here because the
    // completion is already finished; it only propagates the stored I/O error.
    while !completion.finished() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    io.wait_for_completion(completion)?;
    Ok(())
}

/// Formats a pull revision for logs without relying on debug layout stability.
fn describe_pull_revision(revision: &Option<DatabasePullRevision>) -> String {
    match revision {
        Some(DatabasePullRevision::Legacy {
            generation,
            synced_frame_no,
        }) => format!("legacy(generation={generation}, synced_frame_no={synced_frame_no:?})"),
        Some(DatabasePullRevision::V1 { revision }) => format!("v1({revision})"),
        None => "none".to_string(),
    }
}

/// Returns whether V1 pulls should request logical MVCC transactions.
fn should_use_logical_mvcc_pull(
    logical_mvcc_pull_requested: bool,
    partial_sync_active: bool,
    remote_encryption_key: Option<&str>,
) -> bool {
    logical_mvcc_pull_disable_reason(
        logical_mvcc_pull_requested,
        partial_sync_active,
        remote_encryption_key,
    )
    .is_none()
}

/// Explains why logical MVCC pull cannot be used for this configuration.
fn logical_mvcc_pull_disable_reason(
    logical_mvcc_pull_requested: bool,
    partial_sync_active: bool,
    remote_encryption_key: Option<&str>,
) -> Option<&'static str> {
    if !logical_mvcc_pull_requested {
        return Some("logical MVCC pull is disabled by configuration");
    }
    if partial_sync_active {
        return Some("partial sync is active");
    }
    if remote_encryption_key.is_some() {
        return Some(
            "remote encryption is enabled; MVCC logical sync is unsupported for encrypted remotes",
        );
    }
    None
}

/// Computes the local CDC floor to replay after remote apply.
///
/// Replace-base can use the last successful push hint to avoid replaying this
/// client's writes that are already included in the installed page snapshot.
/// Incremental logical pulls cannot use that hint because self-originated
/// transactions are filtered separately by client metadata.
fn resolve_local_replay_floor_change_id(
    use_pushed_change_hint: bool,
    local_pull_gen: i64,
    remote_pull_gen: i64,
    remote_last_change_id: Option<i64>,
    last_pushed_pull_gen_hint: i64,
    last_pushed_change_id_hint: i64,
) -> Option<i64> {
    let mut last_change_id = if remote_pull_gen == local_pull_gen {
        remote_last_change_id
    } else {
        Some(0)
    };

    if use_pushed_change_hint
        && last_pushed_pull_gen_hint == local_pull_gen
        && last_pushed_change_id_hint > 0
        && last_change_id.unwrap_or(0) < last_pushed_change_id_hint
    {
        last_change_id = Some(last_pushed_change_id_hint);
    }

    last_change_id
}

/// Enables expensive integrity diagnostics for remote apply debugging.
fn remote_apply_integrity_debug_enabled() -> bool {
    std::env::var("TURSO_SYNC_DEBUG_REMOTE_APPLY_INTEGRITY")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

/// Runs `PRAGMA integrity_check` when remote-apply diagnostics are enabled.
async fn debug_integrity_check<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    path: &str,
    label: &str,
) -> Result<()> {
    if !remote_apply_integrity_debug_enabled() {
        return Ok(());
    }

    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let mut rows = Vec::new();
    while let Some(row) = run_stmt_once(coro, &mut stmt).await? {
        let value = match row.get_value(0) {
            Value::Text(text) => text.as_str().to_string(),
            other => format!("{other:?}"),
        };
        rows.push(value);
    }
    tracing::warn!(
        "apply_changes(path={}): integrity_check after {}: {:?}",
        path,
        label,
        rows
    );

    Ok(())
}

/// Logs local CDC replay shape for remote-apply diagnostics.
fn log_local_change_summary(
    path: &str,
    label: &str,
    changes: &[crate::types::DatabaseTapeRowChange],
) {
    if !remote_apply_integrity_debug_enabled() {
        return;
    }

    let mut by_table = std::collections::BTreeMap::<&str, usize>::new();
    for change in changes {
        *by_table.entry(change.table_name.as_str()).or_default() += 1;
    }
    tracing::warn!(
        "apply_changes(path={}): {} local row changes by table: {:?}",
        path,
        label,
        by_table
    );
}

/// Returns true for pull-update streams that physically install remote pages.
fn stream_kind_applies_remote_pages(stream_kind: DbChangesStreamKind) -> bool {
    matches!(
        stream_kind,
        DbChangesStreamKind::LegacyPages
            | DbChangesStreamKind::Pages
            | DbChangesStreamKind::ReplaceBasePages
    )
}

/// Decides whether page updates should be replayed through a SQL connection.
///
/// Legacy protocol keeps the original WAL-session path. V1 page streams use the
/// SQL connection only when there is no active logical replay connection.
fn should_replay_raw_pages_on_sql_conn(
    protocol_version_hint: DatabaseSyncEngineProtocolVersion,
    logical_replay_conn_active: bool,
    stream_kind: DbChangesStreamKind,
) -> bool {
    protocol_version_hint != DatabaseSyncEngineProtocolVersion::Legacy
        && !logical_replay_conn_active
        && matches!(
            stream_kind,
            DbChangesStreamKind::Pages | DbChangesStreamKind::ReplaceBasePages
        )
}

/// caller has no access to the memory io - so we handle it here implicitly
/// ideally, we should add necessary methods to the turso_core::IO trait - but so far I am struggling with nice interface to do that
/// so, I decided to keep a little bit of mess in sync-engine for a little bit longer
async fn full_read<Ctx, IO: SyncEngineIo>(
    coro: &Coro<Ctx>,
    io: Option<Arc<dyn turso_core::IO>>,
    sync_engine_io: Arc<IO>,
    path: &str,
    is_memory: bool,
) -> Result<Option<Vec<u8>>> {
    if !is_memory {
        let completion = sync_engine_io.full_read(path)?;
        let data = wait_all_results(coro, &completion, None).await?;
        if data.is_empty() {
            return Ok(None);
        } else {
            return Ok(Some(data));
        }
    }
    let Some(io) = io else {
        return Err(Error::DatabaseSyncEngineError(
            "MemoryIO must be set".to_string(),
        ));
    };
    let Ok(file) = io.open_file(path, OpenFlags::None, false) else {
        return Ok(None);
    };
    let mut content = Vec::new();
    let mut offset = 0;
    let buffer = Arc::new(Buffer::new_temporary(4096));
    let read_len = Arc::new(Mutex::new(0));
    loop {
        let c = Completion::new_read(buffer.clone(), {
            let read_len = read_len.clone();
            move |r| {
                *read_len.lock().unwrap() = r.expect("memory io must not fail").1;
                None
            }
        });
        let read = file.pread(offset, c).expect("memory io must not fail");
        assert!(read.finished(), "memory io must complete immediately");
        let read_len = *read_len.lock().unwrap();
        if read_len == 0 {
            break;
        }
        content.extend_from_slice(&buffer.as_slice()[0..read_len as usize]);
        offset += read_len as u64;
    }
    Ok(Some(content))
}

/// caller has no access to the memory io - so we handle it here implicitly
/// ideally, we should add necessary methods to the turso_core::IO trait - but so far I am struggling with nice interface to do that
/// so, I decided to keep a little bit of mess in sync-engine for a little bit longer
async fn full_write<Ctx, IO: SyncEngineIo>(
    coro: &Coro<Ctx>,
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: Arc<IO>,
    path: &str,
    is_memory: bool,
    content: Vec<u8>,
) -> Result<()> {
    if !is_memory {
        let completion = sync_engine_io.full_write(path, content)?;
        wait_all_results(coro, &completion, None).await?;
        return Ok(());
    }
    let file = io.open_file(path, OpenFlags::Create, false)?;
    let trunc = file
        .truncate(0, Completion::new_trunc(|_| {}))
        .expect("memory io must not fail");
    assert!(trunc.finished(), "memory io must complete immediately");
    let write = file
        .pwrite(
            0,
            Arc::new(Buffer::new(content)),
            Completion::new_write(|_| {}),
        )
        .expect("memory io must nof fail");
    assert!(write.finished(), "memory io must complete immediately");
    Ok(())
}

/// Fsyncs a file if it currently exists.
async fn sync_path_if_present<Ctx>(
    coro: &Coro<Ctx>,
    io: &Arc<dyn turso_core::IO>,
    path: &str,
) -> Result<()> {
    let Some(file) = io.try_open(path)? else {
        return Ok(());
    };
    let sync = file.sync(
        Completion::new_sync(|_| {}),
        turso_core::io::FileSyncType::Fsync,
    )?;
    while !sync.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(())
}

/// Removes a file only when it exists, preserving missing-file idempotence.
fn remove_file_if_present(io: &Arc<dyn turso_core::IO>, path: &str) -> Result<()> {
    if io.try_open(path)?.is_some() {
        io.remove_file(path)?;
    }
    Ok(())
}

/// Copies one sync-engine file and reports whether the source existed.
///
/// Missing sources remove the target so backup/restore mirrors the exact
/// pre-operation file set.
async fn copy_sync_engine_file<Ctx, IO: SyncEngineIo>(
    coro: &Coro<Ctx>,
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: Arc<IO>,
    source_path: &str,
    target_path: &str,
    is_memory: bool,
) -> Result<bool> {
    let Some(content) = full_read(
        coro,
        Some(io.clone()),
        sync_engine_io.clone(),
        source_path,
        is_memory,
    )
    .await?
    else {
        remove_file_if_present(&io, target_path)?;
        return Ok(false);
    };
    full_write(
        coro,
        io.clone(),
        sync_engine_io,
        target_path,
        is_memory,
        content,
    )
    .await?;
    sync_path_if_present(coro, &io, target_path).await?;
    Ok(true)
}

impl<IO: SyncEngineIo> ReplaceBaseApplyGuard<IO> {
    /// Back up all local files that may be mutated by replace-base and write
    /// the recovery marker.
    ///
    /// The marker is written last, after backups are synced, so recovery never
    /// observes a marker without the files it needs to restore.
    async fn create<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
        previous_synced_revision: Option<DatabasePullRevision>,
    ) -> Result<Self> {
        if is_memory(main_db_path) {
            return Err(Error::DatabaseSyncEngineError(
                "replace-base local replay preservation requires durable local storage".to_string(),
            ));
        }
        let is_memory = is_memory(main_db_path);
        let file_specs = [
            (main_db_path.to_string(), "main-db"),
            (create_main_db_wal_path(main_db_path), "main-wal"),
            (create_main_db_log_path(main_db_path), "main-log"),
            (create_revert_db_wal_path(main_db_path), "revert-wal"),
            (create_meta_path(main_db_path), "metadata"),
        ];
        let mut files = Vec::with_capacity(file_specs.len());
        for (path, name) in file_specs {
            let backup_path = replace_base_backup_path(main_db_path, name);
            let present = copy_sync_engine_file(
                coro,
                io.clone(),
                sync_engine_io.io.clone(),
                &path,
                &backup_path,
                is_memory,
            )
            .await?;
            files.push(ReplaceBaseBackupFile {
                path,
                backup_path,
                present,
            });
        }
        let manifest = ReplaceBaseBackupManifest {
            version: 1,
            operation: "replace_base_apply".to_string(),
            main_db_path: main_db_path.to_string(),
            previous_synced_revision,
            files,
        };
        let marker_path = create_replace_base_marker_path(main_db_path);
        full_write(
            coro,
            io.clone(),
            sync_engine_io.io.clone(),
            &marker_path,
            is_memory,
            serde_json::to_vec(&manifest)?,
        )
        .await?;
        sync_path_if_present(coro, &io, &marker_path).await?;
        Ok(Self {
            io,
            sync_engine_io,
            main_db_path: main_db_path.to_string(),
            manifest,
            complete: false,
        })
    }

    /// Restore a replace-base operation left incomplete by a previous process.
    ///
    /// Returns `true` when a marker was found and recovery restored the backed
    /// up files. Callers must refresh open database/WAL state afterward.
    async fn recover_pending<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
    ) -> Result<bool> {
        let marker_path = create_replace_base_marker_path(main_db_path);
        let is_memory = is_memory(main_db_path);
        let Some(marker) = full_read(
            coro,
            Some(io.clone()),
            sync_engine_io.io.clone(),
            &marker_path,
            is_memory,
        )
        .await?
        else {
            return Ok(false);
        };
        let manifest: ReplaceBaseBackupManifest =
            serde_json::from_slice(&marker).map_err(|err| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to parse pending replace-base recovery marker {marker_path}: {err}"
                ))
            })?;
        tracing::warn!(
            "recover_pending_replace_base(path={}): restoring local files from pending marker",
            main_db_path
        );
        Self {
            io,
            sync_engine_io,
            main_db_path: main_db_path.to_string(),
            manifest,
            complete: false,
        }
        .restore(coro)
        .await?;
        Ok(true)
    }

    /// Restore all files from the manifest and remove the marker/backups.
    ///
    /// Files that did not exist when the guard was created are removed, which
    /// keeps recovery faithful to the pre-apply local state.
    async fn restore<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        let is_memory = is_memory(&self.main_db_path);
        for file in &self.manifest.files {
            if file.present {
                let restored = copy_sync_engine_file(
                    coro,
                    self.io.clone(),
                    self.sync_engine_io.io.clone(),
                    &file.backup_path,
                    &file.path,
                    is_memory,
                )
                .await?;
                if !restored {
                    return Err(Error::DatabaseSyncEngineError(format!(
                        "replace-base recovery backup is missing: {}",
                        file.backup_path
                    )));
                }
            } else {
                remove_file_if_present(&self.io, &file.path)?;
            }
        }
        self.cleanup(coro).await
    }

    /// Remove the marker and backup files after either successful apply or
    /// successful restore.
    async fn cleanup<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        for file in &self.manifest.files {
            remove_file_if_present(&self.io, &file.backup_path)?;
        }
        remove_file_if_present(
            &self.io,
            &create_replace_base_marker_path(&self.main_db_path),
        )?;
        sync_path_if_present(coro, &self.io, &self.main_db_path).await?;
        Ok(())
    }

    /// Mark replace-base as durable and discard the recovery backups.
    async fn mark_complete<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<()> {
        self.complete = true;
        self.cleanup(coro).await
    }

    /// Best-effort restore wrapper used on apply failure.
    ///
    /// The original apply error is preserved. If restore also fails, the returned
    /// error includes both failures so callers know the local files may need
    /// manual inspection.
    async fn restore_after_error<Ctx>(&self, coro: &Coro<Ctx>, apply_error: Error) -> Error {
        match self.restore(coro).await {
            Ok(()) => apply_error,
            Err(restore_error) => Error::DatabaseSyncEngineError(format!(
                "{apply_error}; additionally failed to restore replace-base backup: {restore_error}"
            )),
        }
    }
}

impl<IO: SyncEngineIo> DatabaseSyncEngine<IO> {
    pub async fn read_db_meta<Ctx>(
        coro: &Coro<Ctx>,
        io: Option<Arc<dyn turso_core::IO>>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
    ) -> Result<Option<DatabaseMetadata>> {
        let path = create_meta_path(main_db_path);
        let is_memory = is_memory(main_db_path);
        let meta = full_read(coro, io, sync_engine_io.io.clone(), &path, is_memory).await?;
        match meta {
            Some(meta) => Ok(Some(DatabaseMetadata::load(&meta)?)),
            None => Ok(None),
        }
    }

    pub async fn bootstrap_db<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
        opts: &DatabaseSyncEngineOpts,
        meta: Option<DatabaseMetadata>,
    ) -> Result<DatabaseMetadata> {
        tracing::info!("bootstrap_db(path={}): opts={:?}", main_db_path, opts);
        let meta_path = create_meta_path(main_db_path);
        let partial_sync_opts = opts.partial_sync_opts.clone();
        let partial = partial_sync_opts.is_some();
        if let Some(reason) = logical_mvcc_pull_disable_reason(
            opts.logical_mvcc_pull,
            partial,
            opts.remote_encryption_key.as_deref(),
        ) {
            tracing::info!(
                "bootstrap_db(path={}): disabling logical MVCC pull fallback because {}",
                main_db_path,
                reason
            );
        }

        let configuration = DatabaseSavedConfiguration {
            remote_url: opts.remote_url.clone(),
            partial_sync_prefetch: opts.partial_sync_opts.as_ref().map(|p| p.prefetch),
            partial_sync_segment_size: opts.partial_sync_opts.as_ref().map(|p| p.segment_size),
        };
        let meta = match meta {
            Some(mut meta) => {
                if meta.update_configuration(configuration) {
                    full_write(
                        coro,
                        io.clone(),
                        sync_engine_io.io.clone(),
                        &meta_path,
                        is_memory(main_db_path),
                        meta.dump()?,
                    )
                    .await?;
                }
                meta
            }
            None if opts.bootstrap_if_empty => {
                let client_unique_id = format!("{}-{}", opts.client_name, uuid::Uuid::new_v4());
                let revision = bootstrap_db_file(
                    &SyncOperationCtx::new(
                        coro,
                        &sync_engine_io,
                        opts.remote_url.clone(),
                        opts.remote_encryption_key.as_deref(),
                    ),
                    &io,
                    main_db_path,
                    opts.protocol_version_hint,
                    partial_sync_opts,
                    opts.pull_bytes_threshold,
                    false,
                )
                .await?;
                let meta = DatabaseMetadata {
                    version: DATABASE_METADATA_VERSION.to_string(),
                    client_unique_id,
                    synced_revision: Some(revision.clone()),
                    revert_since_wal_salt: None,
                    revert_since_wal_watermark: 0,
                    last_pushed_change_id_hint: 0,
                    last_pushed_pull_gen_hint: 0,
                    last_pull_unix_time: Some(io.current_time_wall_clock().secs),
                    last_push_unix_time: None,
                    partial_bootstrap_server_revision: if partial {
                        Some(revision.clone())
                    } else {
                        None
                    },
                    logical_table_names_by_stable_id: Default::default(),
                    saved_configuration: Some(configuration),
                };
                tracing::info!("write meta after successful bootstrap: meta={meta:?}");

                full_write(
                    coro,
                    io.clone(),
                    sync_engine_io.io.clone(),
                    &meta_path,
                    is_memory(main_db_path),
                    meta.dump()?,
                )
                .await?;
                // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
                meta
            }
            None => {
                if opts.protocol_version_hint == DatabaseSyncEngineProtocolVersion::Legacy {
                    return Err(Error::DatabaseSyncEngineError(
                        "deferred bootstrap is not supported for legacy protocol".to_string(),
                    ));
                }
                if partial {
                    return Err(Error::DatabaseSyncEngineError(
                        "deferred bootstrap is not supported for partial sync".to_string(),
                    ));
                }
                let client_unique_id = format!("{}-{}", opts.client_name, uuid::Uuid::new_v4());
                let meta = DatabaseMetadata {
                    version: DATABASE_METADATA_VERSION.to_string(),
                    client_unique_id,
                    synced_revision: None,
                    revert_since_wal_salt: None,
                    revert_since_wal_watermark: 0,
                    last_pushed_change_id_hint: 0,
                    last_pushed_pull_gen_hint: 0,
                    last_pull_unix_time: None,
                    last_push_unix_time: None,
                    partial_bootstrap_server_revision: None,
                    logical_table_names_by_stable_id: Default::default(),
                    saved_configuration: Some(configuration),
                };
                tracing::info!("write meta after successful bootstrap: meta={meta:?}");
                full_write(
                    coro,
                    io.clone(),
                    sync_engine_io.io.clone(),
                    &meta_path,
                    is_memory(main_db_path),
                    meta.dump()?,
                )
                .await?;
                // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
                meta
            }
        };

        if meta.version != DATABASE_METADATA_VERSION {
            return Err(Error::DatabaseSyncEngineError(format!(
                "unsupported metadata version: {}",
                meta.version
            )));
        }

        tracing::info!("check if main db file exists");

        let main_exists = io.try_open(main_db_path)?.is_some();
        if !main_exists && meta.synced_revision.is_some() {
            let error = "main DB file doesn't exists, but metadata is".to_string();
            return Err(Error::DatabaseSyncEngineError(error));
        }

        Ok(meta)
    }

    pub fn init_db_storage(
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        meta: &DatabaseMetadata,
        main_db_path: &str,
        remote_encryption_key: Option<&str>,
    ) -> Result<(Arc<dyn DatabaseStorage>, Option<Arc<RwLock<String>>>)> {
        let db_file = io.open_file(main_db_path, turso_core::OpenFlags::Create, false)?;
        let mut partial_lazy_server_revision = None;
        let db_file: Arc<dyn DatabaseStorage> = if let Some(partial_sync_opts) =
            meta.partial_sync_opts()
        {
            let Some(partial_bootstrap_server_revision) = &meta.partial_bootstrap_server_revision
            else {
                return Err(Error::DatabaseSyncEngineError(
                    "partial_bootstrap_server_revision must be set in the metadata".to_string(),
                ));
            };
            let DatabasePullRevision::V1 { revision } = &partial_bootstrap_server_revision else {
                return Err(Error::DatabaseSyncEngineError(
                    "partial sync is supported only for V1 protocol".to_string(),
                ));
            };
            tracing::info!("create LazyDatabaseStorage database storage");
            let encoded_key = remote_encryption_key.map(|k| k.to_string());
            let revision_handle = Arc::new(RwLock::new(revision.to_string()));
            partial_lazy_server_revision = Some(revision_handle.clone());
            Arc::new(LazyDatabaseStorage::new(
                db_file,
                None, // todo(sivukhin): allocate dirty file for FS IO
                sync_engine_io.clone(),
                revision_handle,
                partial_sync_opts,
                meta.saved_configuration
                    .as_ref()
                    .and_then(|x| x.remote_url.as_ref())
                    .cloned(),
                encoded_key,
            )?)
        } else {
            Arc::new(turso_core::storage::database::DatabaseFile::new(db_file))
        };

        Ok((db_file, partial_lazy_server_revision))
    }

    pub async fn open_db<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db: Arc<turso_core::Database>,
        opts: DatabaseSyncEngineOpts,
    ) -> Result<Self> {
        let main_db_path = main_db.path.to_string();
        tracing::info!("open_db(path={}): opts={:?}", main_db_path, opts);
        // A previous process may have crashed after writing the replace-base
        // marker but before completing local replay. Restore the backed-up files
        // before reading metadata or creating tape connections.
        let recovered = ReplaceBaseApplyGuard::recover_pending(
            coro,
            io.clone(),
            sync_engine_io.clone(),
            &main_db_path,
        )
        .await?;
        if recovered {
            // Recovery restored files behind this already-open Database handle,
            // so rebuild its shared WAL/MVCC view before continuing.
            main_db.reload_wal_after_external_restore()?;
        }

        let meta_path = create_meta_path(&main_db_path);

        let meta = full_read(
            coro,
            Some(io.clone()),
            sync_engine_io.io.clone(),
            &meta_path,
            is_memory(&main_db_path),
        )
        .await?;
        let Some(meta) = meta else {
            return Err(Error::DatabaseSyncEngineError(
                "meta must be initialized before open".to_string(),
            ));
        };
        let meta = DatabaseMetadata::load(&meta)?;

        // DB wasn't synced with remote but will be encrypted on remote - so we must properly set reserved bytes field in advance
        if meta.synced_revision.is_none() && opts.reserved_bytes != 0 {
            let conn = main_db.connect()?;
            conn.wal_auto_actions_disable();
            conn.set_reserved_bytes(opts.reserved_bytes as u8)?;

            // write transaction forces allocation of root DB page
            conn.execute("BEGIN IMMEDIATE")?;
            conn.execute("COMMIT")?;
        }

        let tape_opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some("full".to_string()),
            disable_auto_checkpoint: true,
        };
        tracing::info!("initialize database tape connection: path={}", main_db_path);
        let main_db_io = main_db.io.clone();
        let main_db_file = main_db.db_file.clone();
        let main_tape = DatabaseTape::new_with_opts(main_db, tape_opts);
        // Initialize CDC pragma and cache CDC version so iterate_changes() can work
        main_tape.connect(coro).await?;

        let changes_path = create_changes_path(&main_db_path);
        let changes_file = main_db_io.open_file(&changes_path, OpenFlags::Create, false)?;

        let db = Self {
            io: main_db_io,
            sync_engine_io,
            db_file: main_db_file,
            main_tape,
            main_db_path: main_db_path.to_string(),
            main_db_wal_path: create_main_db_wal_path(&main_db_path),
            revert_db_wal_path: create_revert_db_wal_path(&main_db_path),
            meta_path: create_meta_path(&main_db_path),
            changes_file: Arc::new(Mutex::new(Some(changes_file))),
            opts,
            meta: Mutex::new(meta.clone()),
            client_unique_id: meta.client_unique_id.clone(),
            partial_lazy_server_revision: None,
        };

        let synced_revision = meta.synced_revision.as_ref();
        if let Some(DatabasePullRevision::Legacy {
            synced_frame_no: None,
            ..
        }) = synced_revision
        {
            // sync WAL from the remote in case of bootstrap - all subsequent initializations will be fast
            db.pull_changes_from_remote(coro).await?;
        }

        tracing::info!("sync engine was initialized");
        Ok(db)
    }

    /// Creates new instance of SyncEngine and initialize it immediately if no consistent local data exists
    pub async fn create_db<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
        opts: DatabaseSyncEngineOpts,
    ) -> Result<Self> {
        // create_db opens the Database after this point, so recovery only needs
        // to restore files. The subsequent open will build fresh WAL/MVCC state.
        ReplaceBaseApplyGuard::recover_pending(
            coro,
            io.clone(),
            sync_engine_io.clone(),
            main_db_path,
        )
        .await?;
        let meta = Self::read_db_meta(coro, Some(io.clone()), sync_engine_io.clone(), main_db_path)
            .await?;
        let fresh_bootstrap = meta.is_none() && opts.bootstrap_if_empty;
        let meta = Self::bootstrap_db(
            coro,
            io.clone(),
            sync_engine_io.clone(),
            main_db_path,
            &opts,
            meta,
        )
        .await?;
        let (main_db_storage, partial_lazy_server_revision) = Self::init_db_storage(
            io.clone(),
            sync_engine_io.clone(),
            &meta,
            main_db_path,
            opts.remote_encryption_key.as_deref(),
        )?;

        // Use async database opening that yields on IO for large schemas
        let mut open_state = turso_core::OpenDbAsyncState::new();
        let main_db = loop {
            match turso_core::Database::open_with_flags_async(
                &mut open_state,
                io.clone(),
                main_db_path,
                main_db_storage.clone(),
                OpenFlags::Create,
                turso_core::DatabaseOpts::new(),
                None,
                None,
            )? {
                turso_core::IOResult::Done(db) => break db,
                turso_core::IOResult::IO(io_completion) => {
                    while !io_completion.finished() {
                        coro.yield_(SyncEngineIoResult::IO).await?;
                    }
                }
            }
        };

        let mut db = Self::open_db(coro, io, sync_engine_io, main_db, opts).await?;
        db.partial_lazy_server_revision = partial_lazy_server_revision;
        if fresh_bootstrap {
            let main_conn = connect_untracked(&db.main_tape)?;
            acknowledge_existing_cdc_for_client(
                coro,
                &main_conn,
                &db.client_unique_id,
                "fresh bootstrap",
            )
            .await?;
        }
        Ok(db)
    }

    async fn open_revert_db_conn<Ctx>(
        &self,
        coro: &Coro<Ctx>,
    ) -> Result<Arc<turso_core::Connection>> {
        let db = {
            let mut state = OpenDbAsyncState::new();
            loop {
                match turso_core::Database::open_with_flags_bypass_registry_async(
                    &mut state,
                    self.io.clone(),
                    &self.main_db_path,
                    Some(&self.revert_db_wal_path),
                    self.db_file.clone(),
                    OpenFlags::Create,
                    turso_core::DatabaseOpts::new(),
                    None,
                    None,
                )? {
                    turso_core::IOResult::Done(db) => break db,
                    turso_core::IOResult::IO(io_completion) => {
                        while !io_completion.finished() {
                            coro.yield_(SyncEngineIoResult::IO).await?;
                        }
                        continue;
                    }
                }
            }
        };
        let conn = db.connect()?;
        conn.wal_auto_actions_disable();
        Ok(conn)
    }

    async fn checkpoint_passive<Ctx>(&self, coro: &Coro<Ctx>) -> Result<(Option<Vec<u32>>, u64)> {
        let watermark = self.meta().revert_since_wal_watermark;
        tracing::info!(
            "checkpoint(path={:?}): revert_since_wal_watermark={}",
            self.main_db_path,
            watermark
        );
        let main_conn = connect_untracked(&self.main_tape)?;
        let main_wal = self.io.try_open(&self.main_db_wal_path)?;
        let main_wal_salt = if let Some(main_wal) = main_wal {
            read_wal_salt(coro, &main_wal).await?
        } else {
            None
        };

        tracing::info!(
            "checkpoint(path={:?}): main_wal_salt={:?}",
            self.main_db_path,
            main_wal_salt
        );

        let revert_since_wal_salt = self.meta().revert_since_wal_salt.clone();
        if revert_since_wal_salt.is_some() && main_wal_salt != revert_since_wal_salt {
            self.update_meta(coro, |meta| {
                meta.revert_since_wal_watermark = 0;
                meta.revert_since_wal_salt = main_wal_salt.clone();
            })
            .await?;
            return Ok((main_wal_salt, 0));
        }
        if watermark == 0 {
            tracing::info!(
                "checkpoint(path={:?}): no synced WAL watermark; skipping passive checkpoint",
                self.main_db_path
            );
            return Ok((main_wal_salt, 0));
        }
        // we do this Passive checkpoint in order to transfer all synced frames to the DB file and make history of revert DB valid
        // if we will not do that we will be in situation where WAL in the revert DB is not valid relative to the DB file
        let result = main_conn.checkpoint(turso_core::CheckpointMode::Passive {
            upper_bound_inclusive: Some(watermark),
        })?;
        tracing::info!(
            "checkpoint(path={:?}): checkpointed portion of WAL: {:?}",
            self.main_db_path,
            result
        );
        if watermark > 0
            && result.wal_max_frame <= watermark
            && result.wal_checkpoint_backfilled >= result.wal_max_frame
        {
            let current_wal_max_frame = connect_untracked(&self.main_tape)?.wal_state()?.max_frame;
            tracing::warn!(
                "checkpoint(path={:?}): checkpoint backfilled all frames through synced watermark {}; resetting checkpoint watermark to current WAL max frame {}",
                self.main_db_path,
                watermark,
                current_wal_max_frame
            );
            self.update_meta(coro, |meta| {
                meta.revert_since_wal_watermark = current_wal_max_frame;
                meta.revert_since_wal_salt = main_wal_salt.clone();
            })
            .await?;
            return Ok((main_wal_salt, current_wal_max_frame));
        }
        let current_wal_max_frame = connect_untracked(&self.main_tape)?.wal_state()?.max_frame;
        if current_wal_max_frame < watermark {
            tracing::warn!(
                "checkpoint(path={:?}): synced watermark {} is ahead of current WAL max frame {}; resetting checkpoint watermark",
                self.main_db_path,
                watermark,
                current_wal_max_frame
            );
            self.update_meta(coro, |meta| {
                meta.revert_since_wal_watermark = current_wal_max_frame;
                meta.revert_since_wal_salt = main_wal_salt.clone();
            })
            .await?;
            return Ok((main_wal_salt, current_wal_max_frame));
        }
        Ok((main_wal_salt, watermark))
    }

    pub async fn stats<Ctx>(&self, coro: &Coro<Ctx>) -> Result<SyncEngineStats> {
        let main_conn = connect_untracked(&self.main_tape)?;
        let change_id = self.meta().last_pushed_change_id_hint;
        let last_pull_unix_time = self.meta().last_pull_unix_time;
        let revision = self.meta().synced_revision.clone().map(|x| match x {
            DatabasePullRevision::Legacy {
                generation,
                synced_frame_no,
            } => format!("generation={generation},synced_frame_no={synced_frame_no:?}"),
            DatabasePullRevision::V1 { revision } => revision,
        });
        let last_push_unix_time = self.meta().last_push_unix_time;
        let revert_wal_path = &self.revert_db_wal_path;
        let revert_wal_file = self.io.try_open(revert_wal_path)?;
        let revert_wal_size = revert_wal_file.map(|f| f.size()).transpose()?.unwrap_or(0);
        let main_wal_frames = main_conn.wal_state()?.max_frame;
        let main_wal_size = if main_wal_frames == 0 {
            0
        } else {
            WAL_FRAME_HEADER as u64 + WAL_FRAME_SIZE as u64 * main_wal_frames
        };
        Ok(SyncEngineStats {
            cdc_operations: count_local_changes(coro, &main_conn, change_id).await?,
            main_wal_size,
            revert_wal_size,
            last_pull_unix_time,
            last_push_unix_time,
            revision,
            network_sent_bytes: self
                .sync_engine_io
                .network_stats
                .written_bytes
                .load(Ordering::SeqCst),
            network_received_bytes: self
                .sync_engine_io
                .network_stats
                .read_bytes
                .load(Ordering::SeqCst),
        })
    }

    pub async fn checkpoint<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        let (main_wal_salt, watermark) = self.checkpoint_passive(coro).await?;

        tracing::info!(
            "checkpoint(path={:?}): passive checkpoint is done",
            self.main_db_path
        );
        let main_conn = connect_untracked(&self.main_tape)?;
        let revert_conn = self.open_revert_db_conn(coro).await?;

        let mut page = [0u8; PAGE_SIZE];
        let db_size = if revert_conn.try_wal_watermark_read_page(1, &mut page, None)? {
            db_size_from_page(&page)
        } else {
            0
        };

        tracing::info!(
            "checkpoint(path={:?}): revert DB initial size: {}",
            self.main_db_path,
            db_size
        );

        let main_wal_state;
        {
            let mut revert_session = WalSession::new(revert_conn.clone());
            revert_session.begin()?;

            let mut main_session = WalSession::new(main_conn.clone());
            main_session.begin()?;

            main_wal_state = main_conn.wal_state()?;
            tracing::info!(
                "checkpoint(path={:?}): main DB WAL state: {:?}",
                self.main_db_path,
                main_wal_state
            );

            let mut revert_session = DatabaseWalSession::new(coro, revert_session).await?;

            let main_changed_pages = main_conn.wal_changed_pages_after(watermark)?;
            tracing::info!(
                "checkpoint(path={:?}): collected {} changed pages",
                self.main_db_path,
                main_changed_pages.len()
            );
            let revert_changed_pages: HashSet<u32> = revert_conn
                .wal_changed_pages_after(0)?
                .into_iter()
                .collect();
            for page_no in main_changed_pages {
                if revert_changed_pages.contains(&page_no) {
                    tracing::debug!(
                        "checkpoint(path={:?}): skip page {} as it present in revert WAL",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                if page_no > db_size {
                    tracing::debug!(
                        "checkpoint(path={:?}): skip page {} as it ahead of revert-DB size",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }

                let begin_read_result =
                    main_conn.try_wal_watermark_read_page_begin(page_no, Some(watermark))?;
                let end_read_result = match begin_read_result {
                    Some((page_ref, c)) => {
                        while !c.succeeded() {
                            let _ = coro.yield_(crate::types::SyncEngineIoResult::IO).await;
                        }
                        main_conn.try_wal_watermark_read_page_end(&mut page, page_ref)?
                    }
                    None => false,
                };
                if !end_read_result {
                    tracing::debug!(
                        "checkpoint(path={:?}): skip page {} as it was allocated in the WAL portion for revert",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                tracing::debug!(
                    "checkpoint(path={:?}): append page {} (current db_size={})",
                    self.main_db_path,
                    page_no,
                    db_size
                );
                revert_session.append_page(page_no, &page)?;
            }
            revert_session.commit(db_size)?;
            revert_session.wal_session.end(false)?;
        }
        self.update_meta(coro, |meta| {
            meta.revert_since_wal_salt = main_wal_salt;
            meta.revert_since_wal_watermark = main_wal_state.max_frame;
        })
        .await?;

        let result = main_conn.checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: Some(main_wal_state.max_frame),
        })?;
        tracing::info!(
            "checkpoint(path={:?}): main DB TRUNCATE checkpoint result: {:?}",
            self.main_db_path,
            result
        );

        Ok(())
    }

    pub async fn wait_changes_from_remote<Ctx>(&self, coro: &Coro<Ctx>) -> Result<DbChangesStatus> {
        self.wait_changes_from_remote_with_timeout(coro, self.opts.long_poll_timeout)
            .await
    }

    async fn wait_changes_from_remote_with_timeout<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        long_poll_timeout: Option<std::time::Duration>,
    ) -> Result<DbChangesStatus> {
        tracing::info!("wait_changes(path={})", self.main_db_path);

        let file = acquire_slot(&self.changes_file)?;

        let now = self.io.current_time_wall_clock();
        let (
            revision,
            saved_remote_url,
            last_pull_unix_time,
            last_push_unix_time,
            last_pushed_pull_gen_hint,
            last_pushed_change_id_hint,
        ) = {
            let meta = self.meta();
            (
                meta.synced_revision.clone(),
                meta.remote_url(),
                meta.last_pull_unix_time,
                meta.last_push_unix_time,
                meta.last_pushed_pull_gen_hint,
                meta.last_pushed_change_id_hint,
            )
        };
        let ctx = &SyncOperationCtx::new(
            coro,
            &self.sync_engine_io,
            saved_remote_url.clone(),
            self.opts.remote_encryption_key.as_deref(),
        );
        let partial = self.opts.partial_sync_opts.is_some();
        let use_logical_mvcc_pull = should_use_logical_mvcc_pull(
            self.opts.logical_mvcc_pull,
            partial,
            self.opts.remote_encryption_key.as_deref(),
        );
        if let Some(reason) = logical_mvcc_pull_disable_reason(
            self.opts.logical_mvcc_pull,
            partial,
            self.opts.remote_encryption_key.as_deref(),
        ) {
            tracing::info!(
                "wait_changes(path={}): logical MVCC pull requested but disabled because {}",
                self.main_db_path,
                reason
            );
        }
        tracing::info!(
            "wait_changes(path={}): remote_url={:?} revision={} client_id={} logical_mvcc_pull_requested={} logical_mvcc_pull_active={} long_poll_timeout_ms={} last_pull_unix_time={:?} last_push_unix_time={:?} last_pushed_pull_gen_hint={} last_pushed_change_id_hint={}",
            self.main_db_path,
            saved_remote_url,
            describe_pull_revision(&revision),
            self.client_unique_id,
            self.opts.logical_mvcc_pull,
            use_logical_mvcc_pull,
            long_poll_timeout.map(|x| x.as_millis()).unwrap_or(0),
            last_pull_unix_time,
            last_push_unix_time,
            last_pushed_pull_gen_hint,
            last_pushed_change_id_hint
        );
        let mut stream_kind = DbChangesStreamKind::Pages;
        let next_revision = if use_logical_mvcc_pull {
            let initial_logical_page_bootstrap = revision.is_none();
            let logical_pull = match &revision {
                Some(DatabasePullRevision::V1 { revision }) => {
                    pull_updates_v1(ctx, &file.value, revision, long_poll_timeout, true).await
                }
                None => {
                    // Initial MVCC sync installs a page base. Incremental logical
                    // pulls can resume from the returned MVCC revision after that.
                    pull_updates_v1(ctx, &file.value, "", long_poll_timeout, false).await
                }
                Some(DatabasePullRevision::Legacy { .. }) => {
                    stream_kind = DbChangesStreamKind::LegacyPages;
                    wal_pull_to_file(
                        ctx,
                        &file.value,
                        &revision,
                        self.opts.wal_pull_batch_size,
                        long_poll_timeout,
                    )
                    .await
                    .map(|revision| {
                        (
                            revision,
                            PullUpdatesV1Result::Pages {
                                replace_base: false,
                            },
                        )
                    })
                }
            };

            match logical_pull {
                Ok((next_revision, PullUpdatesV1Result::Pages { replace_base })) => {
                    if replace_base || initial_logical_page_bootstrap {
                        tracing::warn!(
                            "wait_changes(path={}): logical pull using replace-base page apply: server_replace_base={} initial_bootstrap={}",
                            self.main_db_path,
                            replace_base,
                            initial_logical_page_bootstrap
                        );
                        stream_kind = DbChangesStreamKind::ReplaceBasePages;
                    }
                    next_revision
                }
                Ok((next_revision, PullUpdatesV1Result::Logical { txns, ops })) => {
                    tracing::info!(
                        "wait_changes(path={}): logical pull returned {} transactions / {} ops",
                        self.main_db_path,
                        txns,
                        ops
                    );
                    stream_kind = DbChangesStreamKind::Logical;
                    next_revision
                }
                Err(error) => {
                    if let Some(DatabasePullRevision::V1 { revision }) = &revision {
                        tracing::warn!(
                            "wait_changes(path={}): logical pull failed, retrying page pull: revision={} remote_url={:?} err={:#}",
                            self.main_db_path,
                            revision,
                            saved_remote_url,
                            error
                        );
                        match pull_updates_v1(ctx, &file.value, "", long_poll_timeout, false).await
                        {
                            Ok((next_revision, PullUpdatesV1Result::Pages { .. })) => {
                                tracing::warn!(
                                    "wait_changes(path={}): logical fallback returned fresh replace-base page snapshot",
                                    self.main_db_path
                                );
                                stream_kind = DbChangesStreamKind::ReplaceBasePages;
                                next_revision
                            }
                            Ok((next_revision, PullUpdatesV1Result::Logical { txns, ops })) => {
                                tracing::info!(
                                    "wait_changes(path={}): logical fallback returned logical stream with {} transactions / {} ops",
                                    self.main_db_path,
                                    txns,
                                    ops
                                );
                                stream_kind = DbChangesStreamKind::Logical;
                                next_revision
                            }
                            Err(fallback_error) => {
                                tracing::error!(
                                    "wait_changes(path={}): logical fallback failed: revision={} remote_url={:?} err={:#}; original_err={:#}",
                                    self.main_db_path,
                                    revision,
                                    saved_remote_url,
                                    fallback_error,
                                    error
                                );
                                return Err(fallback_error);
                            }
                        }
                    } else {
                        tracing::error!(
                            "wait_changes(path={}): logical pull failed: revision={} remote_url={:?} err={:#}",
                            self.main_db_path,
                            describe_pull_revision(&revision),
                            saved_remote_url,
                            error
                        );
                        return Err(error);
                    }
                }
            }
        } else {
            match &revision {
                Some(DatabasePullRevision::Legacy { .. }) => {
                    stream_kind = DbChangesStreamKind::LegacyPages;
                    match wal_pull_to_file(
                        ctx,
                        &file.value,
                        &revision,
                        self.opts.wal_pull_batch_size,
                        long_poll_timeout,
                    )
                    .await
                    {
                        Ok(next_revision) => next_revision,
                        Err(error) => {
                            tracing::error!(
                                "wait_changes(path={}): page/WAL pull failed: revision={} remote_url={saved_remote_url:?} err={error:#}",
                                self.main_db_path,
                                describe_pull_revision(&revision),
                            );
                            return Err(error);
                        }
                    }
                }
                Some(DatabasePullRevision::V1 { revision }) => {
                    match pull_updates_v1(ctx, &file.value, revision, long_poll_timeout, false)
                        .await
                    {
                        Ok((next_revision, PullUpdatesV1Result::Pages { replace_base })) => {
                            if replace_base {
                                tracing::warn!(
                                    "wait_changes(path={}): V1 pull returned replace-base page snapshot",
                                    self.main_db_path
                                );
                                stream_kind = DbChangesStreamKind::ReplaceBasePages;
                            }
                            next_revision
                        }
                        Ok((next_revision, PullUpdatesV1Result::Logical { txns, ops })) => {
                            tracing::info!(
                                "wait_changes(path={}): fallback V1 pull returned logical stream with {} transactions / {} ops",
                                self.main_db_path,
                                txns,
                                ops
                            );
                            stream_kind = DbChangesStreamKind::Logical;
                            next_revision
                        }
                        Err(error) => {
                            tracing::error!(
                                "wait_changes(path={}): V1 pull failed: revision={} remote_url={saved_remote_url:?} err={error:#}",
                                self.main_db_path,
                                format!("v1({revision})"),
                            );
                            return Err(error);
                        }
                    }
                }
                None => {
                    match pull_updates_v1(ctx, &file.value, "", long_poll_timeout, false).await {
                        Ok((next_revision, PullUpdatesV1Result::Pages { replace_base })) => {
                            if replace_base {
                                tracing::warn!(
                                    "wait_changes(path={}): initial V1 pull returned replace-base page snapshot",
                                    self.main_db_path
                                );
                                stream_kind = DbChangesStreamKind::ReplaceBasePages;
                            }
                            next_revision
                        }
                        Ok((next_revision, PullUpdatesV1Result::Logical { txns, ops })) => {
                            tracing::info!(
                                "wait_changes(path={}): initial V1 pull returned logical stream with {} transactions / {} ops",
                                self.main_db_path,
                                txns,
                                ops
                            );
                            stream_kind = DbChangesStreamKind::Logical;
                            next_revision
                        }
                        Err(error) => {
                            tracing::error!(
                                "wait_changes(path={}): initial V1 pull failed: revision={} remote_url={saved_remote_url:?} err={error:#}",
                                self.main_db_path,
                                describe_pull_revision(&revision),
                            );
                            return Err(error);
                        }
                    }
                }
            }
        };

        let file_slot = if file.value.size()? == 0 {
            None
        } else {
            Some(file)
        };
        if file_slot.is_none() {
            tracing::info!(
                "wait_changes(path={}): no changes detected",
                self.main_db_path
            );
            self.update_meta(coro, |m| {
                m.last_pull_unix_time = Some(now.secs);
            })
            .await?;
            return Ok(DbChangesStatus {
                time: now,
                revision: next_revision,
                file_slot: None,
                stream_kind,
            });
        }

        tracing::info!(
            "wait_changes_from_remote(path={}): revision: {:?} -> {:?}",
            self.main_db_path,
            revision,
            next_revision
        );

        Ok(DbChangesStatus {
            time: now,
            revision: next_revision,
            file_slot,
            stream_kind,
        })
    }

    /// Sync all new changes from remote DB and apply them locally
    /// This method will **not** send local changed to the remote
    /// This method will block writes for the period of pull
    pub async fn apply_changes_from_remote<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        remote_changes: DbChangesStatus,
    ) -> Result<()> {
        let new_revision = remote_changes.revision.clone();
        let update_partial_lazy_revision = || {
            if let (Some(revision_handle), DatabasePullRevision::V1 { revision }) =
                (&self.partial_lazy_server_revision, &new_revision)
            {
                *revision_handle.write().unwrap() = revision.clone();
            }
        };
        if remote_changes.is_empty() {
            update_partial_lazy_revision();
            self.update_meta(coro, |m| {
                m.synced_revision = Some(new_revision.clone());
                m.last_pull_unix_time = Some(remote_changes.time.secs);
            })
            .await?;
            return Ok(());
        }
        let changes_file = remote_changes
            .file_slot
            .as_ref()
            .expect("non-empty remote changes must keep a file slot");
        let pull_result = self
            .apply_changes_internal(coro, &changes_file.value, remote_changes.stream_kind)
            .await;
        let Ok((revert_since_wal_watermark, logical_table_names_by_stable_id)) = pull_result else {
            return Err(pull_result.err().unwrap());
        };

        let revert_wal_file = self.io.open_file(
            &self.revert_db_wal_path,
            turso_core::OpenFlags::Create,
            false,
        )?;
        reset_wal_file(coro, revert_wal_file, 0).await?;

        self.update_meta(coro, |m| {
            m.revert_since_wal_watermark = revert_since_wal_watermark;
            m.synced_revision = Some(new_revision.clone());
            m.last_pushed_pull_gen_hint = 0;
            m.last_pushed_change_id_hint = 0;
            m.last_pull_unix_time = Some(remote_changes.time.secs);
            m.logical_table_names_by_stable_id = logical_table_names_by_stable_id;
        })
        .await?;
        update_partial_lazy_revision();
        Ok(())
    }

    async fn apply_changes_internal<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        changes_file: &Arc<dyn turso_core::File>,
        stream_kind: DbChangesStreamKind,
    ) -> Result<(u64, BTreeMap<u64, String>)> {
        tracing::info!("apply_changes(path={})", self.main_db_path);

        let (_, watermark) = self.checkpoint_passive(coro).await?;

        let revert_conn = self.open_revert_db_conn(coro).await?;
        let main_conn = connect_untracked(&self.main_tape)?;
        let replace_base_pages = matches!(stream_kind, DbChangesStreamKind::ReplaceBasePages);

        let mut revert_session = WalSession::new(revert_conn.clone());
        revert_session.begin()?;

        // start of the pull updates apply process
        // during this process we need to be very careful with the state of the WAL as at some points it can be not safe to read data from it
        // the reasons why this can be not safe:
        // 1. we are in the middle of rollback or apply from remote WAL - so DB now is in some weird state and no operations can be made safely
        // 2. after rollback or apply from remote WAL it's unsafe to prepare statements because schema cookie can go "back in time" and we first need to adjust it before executing any statement over DB
        let mut main_session = WalSession::new(main_conn.clone());
        main_session.begin()?;

        // we need to make sure that updates from the session will not be commited accidentally in the middle of the pull process
        // in order to achieve that we mark current session as "nested program" which eliminates possibility that data will be actually commited without our explicit command
        //
        // the reason to not use auto-commit is because it has its own rules which resets the flag in case of statement reset - which we do under the hood sometimes
        // that's why nested executed was chosen instead of auto-commit=false mode
        main_conn.start_nested();

        let had_cdc_table = has_table(coro, &main_conn, "turso_cdc").await?;
        let pre_apply_local_change_id = if had_cdc_table {
            max_local_change_id(coro, &main_conn).await?
        } else {
            None
        };
        tracing::info!(
            "apply_changes(path={}): pre-apply local CDC high-water mark: {:?}",
            self.main_db_path,
            pre_apply_local_change_id
        );
        // The original raw-WAL page replay path normalizes the schema cookie
        // after installing remote pages. Preserve that for legacy/V1 page sync
        // that does not switch to the SQL replay connection.
        let main_conn_schema_version = main_conn.read_schema_version()?;

        // read current pull generation from local table for the given client
        let (local_pull_gen, local_last_change_id) =
            read_last_change_id(coro, &main_conn, &self.client_unique_id).await?;
        tracing::info!(
            "apply_changes(path={}): local sync high-water before remote apply: client_id={} pull_gen={} change_id={:?} replace_base={}",
            self.main_db_path,
            self.client_unique_id,
            local_pull_gen,
            local_last_change_id,
            replace_base_pages,
        );
        let (last_pushed_pull_gen_hint, last_pushed_change_id_hint) = {
            let meta = self.meta();
            (
                meta.last_pushed_pull_gen_hint,
                meta.last_pushed_change_id_hint,
            )
        };
        let precollect_local_changes_before_remote_apply = replace_base_pages
            || matches!(stream_kind, DbChangesStreamKind::Logical)
            || should_replay_raw_pages_on_sql_conn(
                self.opts.protocol_version_hint,
                false,
                stream_kind,
            );
        let replace_base_precollection_floor = if replace_base_pages {
            // Replace-base snapshots are installed before local CDC replay, so
            // collect local changes from the old view. Use the same push-hint
            // aware floor that logical replay uses later; otherwise a local
            // change that was already pushed can be replayed again on top of the
            // replace-base snapshot that already contains it.
            resolve_local_replay_floor_change_id(
                true,
                local_pull_gen,
                local_pull_gen,
                local_last_change_id,
                last_pushed_pull_gen_hint,
                last_pushed_change_id_hint,
            )
        } else {
            None
        };
        let mut precollected_local_changes = Vec::with_capacity(64);
        if precollect_local_changes_before_remote_apply {
            // Pull apply rolls back local WAL frames before installing remote
            // changes. Replace-base and MVCC logical/SQL replay need the old
            // local view because a fresh iterator may no longer see those rows
            // after the remote snapshot/log is installed. The original raw WAL
            // page replay path keeps the old post-apply collection point.
            let iterate_opts = DatabaseChangesIteratorOpts {
                first_change_id: if replace_base_pages {
                    replace_base_precollection_floor.map(|change_id| change_id + 1)
                } else {
                    None
                },
                last_change_id: pre_apply_local_change_id,
                mode: DatabaseChangesIteratorMode::Apply,
                ignore_schema_changes: false,
                ..Default::default()
            };
            let mut iterator = self.main_tape.iterate_changes(iterate_opts)?;
            while let Some(operation) = iterator.next(coro).await? {
                match operation {
                    DatabaseTapeOperation::RowChange(change) => {
                        precollected_local_changes.push(change)
                    }
                    DatabaseTapeOperation::Commit => continue,
                    DatabaseTapeOperation::StmtReplay(_) => {
                        panic!("changes iterator must not use StmtReplay option")
                    }
                    DatabaseTapeOperation::SchemaReplay(_) => {
                        panic!("changes iterator must not use SchemaReplay option")
                    }
                }
            }
            tracing::info!(
                "apply_changes(path={}): precollected {} local changes for replay",
                self.main_db_path,
                precollected_local_changes.len(),
            );
        } else {
            tracing::info!(
                "apply_changes(path={}): using post-apply local change collection",
                self.main_db_path,
            );
        }
        let previous_synced_revision = self.meta().synced_revision.clone();
        let mut logical_table_names_by_stable_id =
            self.meta().logical_table_names_by_stable_id.clone();
        let mut replace_base_guard = if replace_base_pages {
            Some(
                ReplaceBaseApplyGuard::create(
                    coro,
                    self.io.clone(),
                    self.sync_engine_io.clone(),
                    &self.main_db_path,
                    previous_synced_revision,
                )
                .await?,
            )
        } else {
            None
        };

        let apply_result: Result<(u64, BTreeMap<u64, String>)> = async {
            let mut main_session = Some(DatabaseWalSession::new(coro, main_session).await?);

            // Phase 1 (start): rollback local changes from the WAL

            // Phase 1.a: rollback local changes not checkpointed to the revert-db
            tracing::info!(
            "apply_changes(path={}): rolling back frames after {} watermark, max_frame={}",
            self.main_db_path,
            watermark,
            main_conn.wal_state()?.max_frame
        );
            let local_rollback = main_session
                .as_mut()
                .expect("main WAL session must be active")
                .rollback_changes_after(coro, watermark)
                .await?;
            let mut frame = [0u8; WAL_FRAME_SIZE];

            let remote_rollback = revert_conn.wal_state()?.max_frame;
            tracing::info!(
            "apply_changes(path={}): rolling back {} frames from revert DB",
            self.main_db_path,
            remote_rollback
        );
            // Phase 1.b: rollback local changes by using frames from revert-db
            // it's important to append pages from revert-db after local revert - because pages from revert-db must overwrite rollback from main DB
            for frame_no in 1..=remote_rollback {
                let info = revert_session.read_at(frame_no, &mut frame)?;
                main_session
                    .as_mut()
                    .expect("main WAL session must be active")
                    .append_page(info.page_no, &frame[WAL_FRAME_HEADER..])?;
            }
            revert_session.end(false)?;

            // Phase 2: after revert DB has no local changes in its latest state - so its safe to apply changes from remote
            let mut logical_replay_conn = None;
            let mut applied_raw_db_size = 0;
            match stream_kind {
            stream_kind if stream_kind_applies_remote_pages(stream_kind) => {
                if matches!(stream_kind, DbChangesStreamKind::ReplaceBasePages) {
                    tracing::warn!(
                        "apply_changes(path={}): applying replace-base page snapshot from remote",
                        self.main_db_path,
                    );
                }
                let db_size = wal_apply_from_file(
                    coro,
                    changes_file,
                    main_session
                        .as_mut()
                        .expect("main WAL session must be active"),
                )
                .await?;
                tracing::info!(
                    "apply_changes(path={}): applied changes from remote: db_size={}",
                    self.main_db_path,
                    db_size,
                );
                applied_raw_db_size = db_size;
                if replace_base_pages {
                    // Finalize the replacement snapshot before replaying local CDC.
                    // Running SQL bookkeeping on the raw WAL-session connection can
                    // observe stale or transient schema state from the replaced base.
                    let mut finished_main_session = main_session
                        .take()
                        .expect("main WAL session must be active");
                    finished_main_session.commit(applied_raw_db_size)?;
                    main_conn.end_nested();
                    finished_main_session.wal_session.end(true).map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to finalize replace-base raw WAL session: {error}",
                        ))
                    })?;
                    drop(finished_main_session);
                    if let Some(mv_store) = main_conn.mv_store().as_ref() {
                        // The remote snapshot is now the durable base. Reset the
                        // local logical log so MVCC recovery cannot overlay stale
                        // local frames from the pre-replace database onto it.
                        let pager = main_conn.get_pager();
                        let reset = mv_store.reset_logical_log_after_external_restore().map_err(
                            |error| {
                                Error::DatabaseSyncEngineError(format!(
                                    "failed to reset MVCC logical log after replace-base snapshot: {error}",
                                ))
                            },
                        )?;
                        drive_core_completion(coro, &pager.io, reset)
                            .await
                            .map_err(|error| {
                                Error::DatabaseSyncEngineError(format!(
                                    "failed to complete MVCC logical log reset after replace-base snapshot: {error}",
                                ))
                            })?;
                        if let Some(sync) = mv_store
                            .sync_logical_log_after_external_restore(&main_conn)
                            .map_err(|error| {
                                Error::DatabaseSyncEngineError(format!(
                                    "failed to sync MVCC logical log after replace-base snapshot: {error}",
                                ))
                            })?
                        {
                            drive_core_completion(coro, &pager.io, sync).await.map_err(
                                |error| {
                                    Error::DatabaseSyncEngineError(format!(
                                        "failed to complete MVCC logical log sync after replace-base snapshot: {error}",
                                    ))
                                },
                            )?;
                        }
                    }
                    // Reopen the process-local WAL/MVCC state against the just
                    // installed files before creating a replay connection.
                    reload_connection_after_external_restore(
                        &main_conn,
                        "replace-base snapshot",
                    )?;

                    let mut conn = match connect_untracked(&self.main_tape) {
                        Ok(conn) => conn,
                        Err(Error::TursoError(LimboError::SchemaUpdated)) => {
                            // The first connection after an external page restore
                            // may see a schema-cookie mismatch. Refresh once and
                            // reconnect before starting replay.
                            force_reparse_schema_with_retry(&main_conn).map_err(|error| {
                                Error::DatabaseSyncEngineError(format!(
                                    "failed to refresh schema after replace-base reconnect schema update: {error}",
                                ))
                            })?;
                            connect_untracked(&self.main_tape).map_err(|error| {
                                Error::DatabaseSyncEngineError(format!(
                                    "failed to reconnect after replace-base schema refresh: {error}",
                                ))
                            })?
                        }
                        Err(error) => return Err(error),
                    };
                    force_reparse_schema_with_retry(&conn).map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to refresh replay connection schema after replace-base snapshot: {error}",
                        ))
                    })?;
                    crate::database_sync_operations::ensure_sync_last_change_id_table(
                        coro,
                        &conn,
                        &self.client_unique_id,
                    )
                    .await
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to initialize sync high-water mark table after replace-base snapshot: {error}"
                        ))
                    })?;
                    conn.publish_schema_after_external_restore();
                    conn = match connect_untracked(&self.main_tape) {
                        Ok(conn) => conn,
                        Err(Error::TursoError(LimboError::SchemaUpdated)) => {
                            force_reparse_schema_with_retry(&main_conn).map_err(|error| {
                                Error::DatabaseSyncEngineError(format!(
                                    "failed to refresh schema after replace-base sync table initialization: {error}",
                                ))
                            })?;
                            connect_untracked(&self.main_tape).map_err(|error| {
                                Error::DatabaseSyncEngineError(format!(
                                    "failed to reconnect after replace-base sync table initialization: {error}",
                                ))
                            })?
                        }
                        Err(error) => return Err(error),
                    };
                    force_reparse_schema_with_retry(&conn).map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to refresh replay connection schema after replace-base sync table initialization: {error}",
                        ))
                    })?;
                    conn.publish_schema_after_external_restore();
                    execute_with_schema_retry(&conn, "BEGIN IMMEDIATE").map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to begin replace-base replay transaction: {error}",
                        ))
                    })?;
                    // From here on, replace-base local replay uses a normal SQL
                    // transaction on a fresh connection rather than the raw WAL
                    // session used to install the remote page snapshot.
                    logical_replay_conn = Some(conn);
                }
            }
            DbChangesStreamKind::Logical => {
                // Persist the rolled-back raw WAL state first. Logical MVCC replay
                // must then run on a regular SQL transaction, not inside the raw
                // WAL-insert session.
                let mut finished_main_session = main_session
                    .take()
                    .expect("main WAL session must be active");
                finished_main_session.commit(0)?;
                main_conn.end_nested();
                finished_main_session.wal_session.end(true)?;
                drop(finished_main_session);
                main_conn.publish_schema_if_newer();

                let conn = connect_untracked(&self.main_tape)?;
                execute_with_schema_retry(&conn, "BEGIN IMMEDIATE")?;
                let mut replay = DatabaseReplaySession {
                    conn: conn.clone(),
                    cached_delete_stmt: HashMap::new(),
                    cached_insert_stmt: HashMap::new(),
                    cached_update_stmt: HashMap::new(),
                    in_txn: true,
                    generator: DatabaseReplayGenerator {
                        conn: conn.clone(),
                        opts: DatabaseReplaySessionOpts {
                            use_implicit_rowid: true,
                        },
                    },
                };
                if remote_apply_integrity_debug_enabled() {
                    let summary = summarize_logical_transactions_file(
                        coro,
                        changes_file,
                        Some(&self.client_unique_id),
                    )
                    .await?;
                    tracing::warn!(
                        "apply_changes(path={}): remote logical replay summary before apply: {:?}",
                        self.main_db_path,
                        summary
                    );
                }
                apply_logical_transactions_file_without_commit_with_table_map(
                    coro,
                    &mut replay,
                    changes_file,
                    &mut logical_table_names_by_stable_id,
                )
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to apply remote logical transactions: {error}",
                    ))
                })?;
                tracing::info!(
                    "apply_changes(path={}): applied logical transactions from remote file",
                    self.main_db_path,
                );
                logical_replay_conn = Some(conn);
            }
            DbChangesStreamKind::LegacyPages
            | DbChangesStreamKind::Pages
            | DbChangesStreamKind::ReplaceBasePages => {
                unreachable!("page stream kinds are handled by stream_kind_applies_remote_pages")
            }
        }

        let raw_page_replay_on_sql_conn = should_replay_raw_pages_on_sql_conn(
            self.opts.protocol_version_hint,
            logical_replay_conn.is_some(),
            stream_kind,
        );
        if raw_page_replay_on_sql_conn {
            // Normal page pulls install remote WAL frames through the raw WAL
            // session. Finalize that session before preparing any SQL against
            // the new schema view, then use a regular transaction for sync
            // bookkeeping and local CDC replay.
            let mut finished_main_session = main_session
                .take()
                .expect("main WAL session must be active");
            finished_main_session.commit(applied_raw_db_size)?;
            main_conn.end_nested();
            finished_main_session.wal_session.end(true).map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to finalize raw page WAL session before local replay: {error}",
                ))
            })?;
            drop(finished_main_session);
            publish_schema_after_raw_wal_replay(&main_conn, "raw WAL replay")?;

            let conn = connect_untracked(&self.main_tape).map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to open post-raw-apply replay connection: {error}",
                ))
            })?;
            execute_with_schema_retry(&conn, "BEGIN IMMEDIATE").map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to begin post-raw-apply replay transaction: {error}",
                ))
            })?;
            logical_replay_conn = Some(conn);
        }
        let phase_conn = logical_replay_conn.as_ref().unwrap_or(&main_conn);
        if logical_replay_conn.is_none() {
            main_session
                .as_mut()
                .expect("main WAL session must be active")
                .commit(applied_raw_db_size)?;
            if stream_kind_applies_remote_pages(stream_kind)
                && !replace_base_pages
                && raw_page_replay_on_sql_conn
            {
                let current_schema_version = main_conn.read_schema_version()?;
                let final_schema_version =
                    current_schema_version.max(main_conn_schema_version) + 1;
                main_conn.write_schema_version(final_schema_version)?;
                tracing::info!(
                    "apply_changes(path={}): updated schema version to {}",
                    self.main_db_path,
                    final_schema_version
                );
            }
        }
        debug_integrity_check(
            coro,
            phase_conn,
            &self.main_db_path,
            "remote apply before local CDC replay",
        )
        .await
        .map_err(|error| {
            Error::DatabaseSyncEngineError(format!(
                "failed integrity check after remote apply before local CDC replay: {error}",
            ))
        })?;
        // Pull apply rolls back the local WAL while installing remote changes,
        // so pending local CDC must be replayed locally afterward. Changes that
        // were already acknowledged by a successful push are present in the
        // remote stream/snapshot and must not be replayed from an older local
        // row image.
        let use_pushed_change_hint = true;

        // Phase 4: as now DB has all data from remote - let's read pull generation and last change id for current client
        // we will use last_change_id in order to replay local changes made strictly after that id locally
        let (remote_pull_gen, remote_last_change_id) = if replace_base_pages {
            tracing::info!(
                    "apply_changes(path={}): using local acknowledged high-water mark for replace-base replay floor: pull_gen={} change_id={:?}",
                    self.main_db_path,
                    local_pull_gen,
                    local_last_change_id,
                );
            (local_pull_gen, local_last_change_id)
        } else {
            read_last_change_id(coro, phase_conn, &self.client_unique_id)
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to read last_change_id after remote apply: {error}",
                    ))
                })?
        };

        // we update pull generation and last_change_id at remote on push, but locally its updated on pull
        // so its impossible to have remote pull generation to be greater than local one
        if remote_pull_gen > local_pull_gen {
            return Err(Error::DatabaseSyncEngineError(format!(
                "protocol error: remote_pull_gen > local_pull_gen: {remote_pull_gen} > {local_pull_gen}"
            )));
        }
        let last_change_id = resolve_local_replay_floor_change_id(
            use_pushed_change_hint,
            local_pull_gen,
            remote_pull_gen,
            remote_last_change_id,
            last_pushed_pull_gen_hint,
            last_pushed_change_id_hint,
        );
        tracing::info!(
            "apply_changes(path={}): local replay floor: use_pushed_change_hint={} local_pull_gen={} remote_pull_gen={} remote_last_change_id={:?} last_pushed_pull_gen_hint={} last_pushed_change_id_hint={} resolved_last_change_id={:?}",
            self.main_db_path,
            use_pushed_change_hint,
            local_pull_gen,
            remote_pull_gen,
            remote_last_change_id,
            last_pushed_pull_gen_hint,
            last_pushed_change_id_hint,
            last_change_id,
        );
        // Phase 5: collect/filter local changes for replay.
        let replay_floor = last_change_id.unwrap_or(0);
        let local_changes = if precollect_local_changes_before_remote_apply {
            precollected_local_changes
                .into_iter()
                .filter(|change| change.change_id > replay_floor)
                .collect::<Vec<_>>()
        } else {
            let iterate_opts = DatabaseChangesIteratorOpts {
                first_change_id: Some(replay_floor + 1),
                mode: DatabaseChangesIteratorMode::Apply,
                ignore_schema_changes: false,
                ..Default::default()
            };
            let mut local_changes = Vec::new();
            let mut iterator = self.main_tape.iterate_changes(iterate_opts)?;
            while let Some(operation) = iterator.next(coro).await? {
                match operation {
                    DatabaseTapeOperation::RowChange(change) => local_changes.push(change),
                    DatabaseTapeOperation::Commit => continue,
                    DatabaseTapeOperation::StmtReplay(_) => {
                        panic!("changes iterator must not use StmtReplay option")
                    }
                    DatabaseTapeOperation::SchemaReplay(_) => {
                        panic!("changes iterator must not use SchemaReplay option")
                    }
                }
            }
            local_changes
        };
        let replayed_local_changes = !local_changes.is_empty();
        tracing::info!(
            "apply_changes(path={}): collected {} changes",
            self.main_db_path,
            local_changes.len()
        );
        log_local_change_summary(&self.main_db_path, "collected", &local_changes);
        // Phase 6: replay local changes
        // we can skip this phase if we are sure that we had no local changes before
        if !local_changes.is_empty() || local_rollback != 0 || remote_rollback != 0 || had_cdc_table
        {
            // first, we update last_change id in the local meta table for sync
            if logical_replay_conn.is_none() {
                phase_conn.reset_main_mvcc_tx_for_wal_session();
            }
            if replace_base_pages || raw_page_replay_on_sql_conn {
                tracing::info!(
                    "apply_changes(path={}): skipping pre-replay sync high-water reset; final high-water will be persisted after replay commit",
                    self.main_db_path
                );
            } else {
                update_last_change_id(
                    coro,
                    phase_conn,
                    &self.client_unique_id,
                    local_pull_gen + 1,
                    0,
                )
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to reset sync high-water mark before local replay: {error}",
                    ))
                })
                .inspect_err(|e| tracing::error!("update_last_change_id failed: {e}"))?;
            }

            if replayed_local_changes && had_cdc_table {
                tracing::info!(
                    "apply_changes(path={}): reinitialize CDC pragma before local replay",
                    self.main_db_path,
                );
                phase_conn
                    .pragma_update(CDC_PRAGMA_NAME, "'full'")
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to reinitialize CDC pragma before local replay: {error}",
                        ))
                    })?;
                if replace_base_pages || raw_page_replay_on_sql_conn {
                    phase_conn.publish_schema_if_newer();
                }
            }

            let mut replay = DatabaseReplaySession {
                conn: phase_conn.clone(),
                cached_delete_stmt: HashMap::new(),
                cached_insert_stmt: HashMap::new(),
                cached_update_stmt: HashMap::new(),
                in_txn: true,
                generator: DatabaseReplayGenerator {
                    conn: phase_conn.clone(),
                    opts: DatabaseReplaySessionOpts {
                        use_implicit_rowid: raw_page_replay_on_sql_conn,
                    },
                },
            };

            let should_transform_local_change = |change: &DatabaseTapeRowChange| {
                is_logically_replayable_table(&change.table_name)
                    && !self
                        .opts
                        .tables_ignore
                        .iter()
                        .any(|table| table == &change.table_name)
            };
            let transform_changes = if self.opts.use_transform {
                local_changes
                    .iter()
                    .filter(|change| should_transform_local_change(change))
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let mut transformed = if self.opts.use_transform {
                let ctx = &SyncOperationCtx::new(
                    coro,
                    &self.sync_engine_io,
                    self.meta().remote_url(),
                    self.opts.remote_encryption_key.as_deref(),
                );
                Some(apply_transformation(ctx, &transform_changes, &replay.generator).await?)
            } else {
                None
            };
            let mut transform_index = 0usize;

            assert!(!replay.conn().get_auto_commit());
            // Replay local changes collected on Phase 5
            for (i, change) in local_changes.into_iter().enumerate() {
                let operation = if should_transform_local_change(&change) {
                    if let Some(transformed) = &mut transformed {
                        let transform_result = std::mem::replace(
                            &mut transformed[transform_index],
                            DatabaseRowTransformResult::Skip,
                        );
                        transform_index += 1;
                        match transform_result {
                            DatabaseRowTransformResult::Keep => {
                                DatabaseTapeOperation::RowChange(change)
                            }
                            DatabaseRowTransformResult::Skip => continue,
                            DatabaseRowTransformResult::Rewrite(replay) => {
                                DatabaseTapeOperation::StmtReplay(replay)
                            }
                        }
                    } else {
                        DatabaseTapeOperation::RowChange(change)
                    }
                } else {
                    DatabaseTapeOperation::RowChange(change)
                };
                maybe_inject_replace_base_local_replay_failure(replace_base_pages, i)?;
                replay.replay(coro, operation).await.map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to replay local change after remote apply: {error}",
                    ))
                })?;
            }
            assert!(!replay.conn().get_auto_commit());
        }
        debug_integrity_check(
            coro,
            phase_conn,
            &self.main_db_path,
            "local CDC replay before commit",
        )
        .await?;

        // Final: now we did all necessary operations as one big transaction and we are ready to commit
        if let Some(logical_conn) = logical_replay_conn {
            logical_conn.execute("COMMIT").map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to commit remote apply replay transaction: {error}",
                ))
            })?;
            logical_conn.publish_schema_if_newer();
            if had_cdc_table || replace_base_pages {
                tracing::info!(
                    "apply_changes(path={}): reinitialize CDC pragma after logical replay commit",
                    self.main_db_path,
                );
                logical_conn
                    .pragma_update(CDC_PRAGMA_NAME, "'full'")
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to reinitialize CDC pragma after remote apply: {error}",
                        ))
                    })?;
                logical_conn.publish_schema_if_newer();
                let change_id = max_local_change_id(coro, &logical_conn).await?.unwrap_or(0);
                tracing::info!(
                    "apply_changes(path={}): post-logical-replay CDC high-water mark: {}",
                    self.main_db_path,
                    change_id
                );
                let synced_change_id = if replayed_local_changes {
                    0
                } else {
                    change_id
                };
                if raw_page_replay_on_sql_conn {
                    rebuild_local_sync_metadata_table(
                        coro,
                        &logical_conn,
                        &self.client_unique_id,
                        local_pull_gen + 1,
                        synced_change_id,
                    )
                    .await?;
                } else {
                    update_last_change_id(
                        coro,
                        &logical_conn,
                        &self.client_unique_id,
                        local_pull_gen + 1,
                        synced_change_id,
                    )
                    .await?;
                }
            }
            let logical_table_names_by_stable_id =
                read_logical_replay_table_map(coro, &logical_conn).await?;
            return Ok((
                logical_conn.wal_state()?.max_frame,
                logical_table_names_by_stable_id,
            ));
        }
        if main_conn.has_main_mvcc_tx_for_wal_session() {
            main_conn
                .commit_main_mvcc_tx_for_wal_session()
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to commit main MVCC transaction after remote apply: {error}",
                    ))
                })?;
        }
        main_conn.end_nested();
        // Finalize the raw WAL session only after all follow-up SQL has run.
        // wal_insert_end() reparses and republishes schema for this connection,
        // which is the safe boundary after raw replay; forcing an extra schema
        // cookie bump before then can trip SQLITE_SCHEMA on the same connection.
        main_session
            .take()
            .expect("main WAL session must be active")
            .wal_session
            .end(true)
            .map_err(|error| {
            Error::DatabaseSyncEngineError(format!(
                "failed to finalize raw WAL session after remote apply: {error}",
                ))
        })?;
        if raw_page_replay_on_sql_conn {
            main_conn.publish_schema_if_newer();
        }
        if (had_cdc_table || replace_base_pages) && (replace_base_pages || raw_page_replay_on_sql_conn)
        {
            publish_schema_after_raw_wal_replay(&main_conn, "raw WAL replay")?;
            tracing::info!(
                "apply_changes(path={}): reinitialize CDC pragma after WAL replay commit",
                self.main_db_path,
            );
            execute_with_schema_retry(
                &main_conn,
                &format!("PRAGMA {CDC_PRAGMA_NAME} = 'full'"),
            )
            .map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to reinitialize CDC pragma after remote apply: {error}",
                ))
            })?;
            main_conn.publish_schema_if_newer();
            let change_id = max_local_change_id(coro, &main_conn).await?.unwrap_or(0);
            tracing::info!(
                "apply_changes(path={}): post-WAL-replay CDC high-water mark: {}",
                self.main_db_path,
                change_id
            );
            let synced_change_id = if replayed_local_changes {
                0
            } else {
                change_id
            };
            update_last_change_id(
                coro,
                &main_conn,
                &self.client_unique_id,
                local_pull_gen + 1,
                synced_change_id,
            )
            .await
            .map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to persist sync high-water mark after raw remote apply: {error}",
                ))
            })?;
        }

            let logical_table_names_by_stable_id =
                read_logical_replay_table_map(coro, &main_conn).await?;
            Ok((main_conn.wal_state()?.max_frame, logical_table_names_by_stable_id))
        }
        .await;

        match apply_result {
            Ok(frame) => {
                if let Some(guard) = &mut replace_base_guard {
                    guard.mark_complete(coro).await?;
                }
                Ok(frame)
            }
            Err(error) => {
                if let Some(guard) = &replace_base_guard {
                    let error = guard.restore_after_error(coro, error).await;
                    // The guard restores files on disk. Make the still-open
                    // connection forget any WAL/schema/MVCC state from the failed
                    // replace-base attempt before returning control to callers.
                    match reload_connection_after_external_restore(
                        &main_conn,
                        "replace-base error recovery",
                    ) {
                        Ok(()) => Err(error),
                        Err(reload_error) => Err(Error::DatabaseSyncEngineError(format!(
                            "{error}; additionally failed to reload restored database state: {reload_error}",
                        ))),
                    }
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Sync local changes to remote DB
    /// This method will **not** pull remote changes to the local DB
    /// This method will **not** block writes for the period of sync
    pub async fn push_changes_to_remote<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        tracing::info!("push_changes(path={})", self.main_db_path);

        let ctx = &SyncOperationCtx::new(
            coro,
            &self.sync_engine_io,
            self.meta().remote_url(),
            self.opts.remote_encryption_key.as_deref(),
        );
        let (pull_gen, change_id) =
            push_logical_changes(ctx, &self.main_tape, &self.client_unique_id, &self.opts).await?;
        let main_conn = connect_untracked(&self.main_tape)?;
        update_last_change_id(
            coro,
            &main_conn,
            &self.client_unique_id,
            pull_gen,
            change_id,
        )
        .await
        .map_err(|error| {
            Error::DatabaseSyncEngineError(format!(
                "failed to persist local sync high-water mark after push: {error}",
            ))
        })?;

        self.update_meta(coro, |m| {
            m.last_pushed_pull_gen_hint = pull_gen;
            m.last_pushed_change_id_hint = change_id;
            m.last_push_unix_time = Some(self.io.current_time_wall_clock().secs);
        })
        .await?;

        Ok(())
    }

    /// Mark CDC rows that came from a freshly bootstrapped remote image as
    /// already observed by this client. New clients must not push imported
    /// remote history back to the remote before any local writes happen.
    pub async fn acknowledge_existing_cdc_for_current_client<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        context: &str,
    ) -> Result<()> {
        let main_conn = connect_untracked(&self.main_tape)?;
        acknowledge_existing_cdc_for_client(coro, &main_conn, &self.client_unique_id, context).await
    }

    /// Create read/write database connection and appropriately configure it before use
    pub async fn connect_rw<Ctx>(&self, coro: &Coro<Ctx>) -> Result<Arc<turso_core::Connection>> {
        let conn = self.main_tape.connect(coro).await?;
        assert_eq!(
            conn.wal_auto_actions(),
            turso_core::WalAutoActions::empty(),
            "tape must be configured to have all auto-WAL actions disabled"
        );
        Ok(conn)
    }

    /// Sync local changes to remote DB and bring new changes from remote to local
    /// This method will block writes for the period of sync
    pub async fn sync<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        // todo(sivukhin): this is bit suboptimal as both 'push' and 'pull' will call pull_synced_from_remote
        // but for now - keep it simple
        self.push_changes_to_remote(coro).await?;
        self.pull_changes_from_remote(coro).await?;
        Ok(())
    }

    pub async fn pull_changes_from_remote<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        const MAX_PULL_PAGINATION_PASSES: usize = 128;

        let mut long_poll_timeout = self.opts.long_poll_timeout;
        for pass in 0..MAX_PULL_PAGINATION_PASSES {
            let previous_revision = self.meta().synced_revision.clone();
            let changes = self
                .wait_changes_from_remote_with_timeout(coro, long_poll_timeout)
                .await?;
            let is_empty = changes.is_empty();
            let next_revision = Some(changes.revision.clone());
            self.apply_changes_from_remote(coro, changes).await?;

            if is_empty || next_revision == previous_revision {
                return Ok(());
            }

            tracing::info!(
                "pull_changes(path={}): revision advanced {:?} -> {:?}; issuing immediate follow-up pull pass {}",
                self.main_db_path,
                previous_revision,
                next_revision,
                pass + 2
            );
            long_poll_timeout = None;
        }

        Err(Error::DatabaseSyncEngineError(format!(
            "pull_changes(path={}): exceeded pagination guard after {} passes",
            self.main_db_path, MAX_PULL_PAGINATION_PASSES
        )))
    }

    fn meta(&self) -> std::sync::MutexGuard<'_, DatabaseMetadata> {
        self.meta.lock().unwrap()
    }

    async fn update_meta<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        update: impl FnOnce(&mut DatabaseMetadata),
    ) -> Result<()> {
        let mut meta = self.meta().clone();
        update(&mut meta);
        tracing::info!("update_meta: {meta:?}");
        full_write(
            coro,
            self.io.clone(),
            self.sync_engine_io.io.clone(),
            &self.meta_path,
            is_memory(&self.main_db_path),
            meta.dump()?,
        )
        .await?;
        // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
        *self.meta.lock().unwrap() = meta;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        create_meta_path, create_replace_base_marker_path, logical_mvcc_pull_disable_reason,
        resolve_local_replay_floor_change_id, should_replay_raw_pages_on_sql_conn,
        should_use_logical_mvcc_pull, stream_kind_applies_remote_pages, DatabaseSyncEngine,
        DatabaseSyncEngineOpts, REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER, WAL_FRAME_HEADER,
        WAL_FRAME_SIZE,
    };
    use crate::{
        database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo},
        database_sync_operations::{
            max_local_change_id, update_last_change_id, MutexSlot, SyncEngineIoStats, PAGE_SIZE,
        },
        database_tape::{run_stmt_once, DatabaseChangesIteratorMode, DatabaseChangesIteratorOpts},
        io_operations::IoOperations,
        server_proto::{
            PullUpdatesApplyMode, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody,
            PullUpdatesStreamKind,
        },
        types::{
            Coro, DatabaseMetadata, DatabasePullRevision, DatabaseRowMutation,
            DatabaseRowTransformResult, DatabaseSavedConfiguration,
            DatabaseSyncEngineProtocolVersion, DatabaseTapeOperation, DbChangesStatus,
            DbChangesStreamKind, PartialSyncOpts, SyncEngineIoResult, DATABASE_METADATA_VERSION,
        },
        Result,
    };
    use prost::Message;
    use std::sync::{Arc, Mutex};
    use tempfile::NamedTempFile;
    use turso_core::{
        io::FileSyncType, types::WalFrameInfo, Buffer, CheckpointMode, Completion, OpenFlags,
    };

    struct EmptyPollResult<T>(Vec<T>);

    impl<T: Send + Sync + 'static> DataPollResult<T> for EmptyPollResult<T> {
        fn data(&self) -> &[T] {
            &self.0
        }
    }

    struct EmptyCompletion<T> {
        data: Mutex<Option<Vec<T>>>,
    }

    impl<T> EmptyCompletion<T> {
        fn empty() -> Self {
            Self {
                data: Mutex::new(Some(Vec::new())),
            }
        }

        fn with_data(data: Vec<T>) -> Self {
            Self {
                data: Mutex::new(Some(data)),
            }
        }
    }

    impl<T: Send + Sync + 'static> DataCompletion<T> for EmptyCompletion<T> {
        type DataPollResult = EmptyPollResult<T>;

        fn status(&self) -> Result<Option<u16>> {
            Ok(Some(200))
        }

        fn poll_data(&self) -> Result<Option<Self::DataPollResult>> {
            let data = self.data.lock().unwrap().take().unwrap_or_default();
            if data.is_empty() {
                Ok(None)
            } else {
                Ok(Some(EmptyPollResult(data)))
            }
        }

        fn is_done(&self) -> Result<bool> {
            Ok(self.data.lock().unwrap().as_ref().is_none_or(Vec::is_empty))
        }
    }

    #[derive(Default)]
    struct NoopSyncEngineIo;

    impl SyncEngineIo for NoopSyncEngineIo {
        type DataCompletionBytes = EmptyCompletion<u8>;
        type DataCompletionTransform = EmptyCompletion<DatabaseRowTransformResult>;

        fn full_read(&self, path: &str) -> Result<Self::DataCompletionBytes> {
            let data = std::fs::read(path).unwrap_or_default();
            Ok(EmptyCompletion::with_data(data))
        }

        fn full_write(&self, path: &str, content: Vec<u8>) -> Result<Self::DataCompletionBytes> {
            std::fs::write(path, content).map_err(|error| {
                crate::errors::Error::DatabaseSyncEngineError(format!(
                    "test full_write failed for {path}: {error}"
                ))
            })?;
            Ok(EmptyCompletion::empty())
        }

        fn transform(
            &self,
            _mutations: Vec<DatabaseRowMutation>,
        ) -> Result<Self::DataCompletionTransform> {
            Ok(EmptyCompletion::empty())
        }

        fn http(
            &self,
            _url: Option<&str>,
            _method: &str,
            _path: &str,
            _body: Option<Vec<u8>>,
            _headers: &[(&str, &str)],
        ) -> Result<Self::DataCompletionBytes> {
            Ok(EmptyCompletion::empty())
        }

        fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

        fn step_io_callbacks(&self) {}
    }

    struct CapturingSyncEngineIo {
        response: Mutex<Option<Vec<u8>>>,
        request: Mutex<Option<(String, String, Option<Vec<u8>>)>>,
    }

    impl SyncEngineIo for CapturingSyncEngineIo {
        type DataCompletionBytes = EmptyCompletion<u8>;
        type DataCompletionTransform = EmptyCompletion<DatabaseRowTransformResult>;

        fn full_read(&self, path: &str) -> Result<Self::DataCompletionBytes> {
            let data = std::fs::read(path).unwrap_or_default();
            Ok(EmptyCompletion::with_data(data))
        }

        fn full_write(&self, path: &str, content: Vec<u8>) -> Result<Self::DataCompletionBytes> {
            std::fs::write(path, content).map_err(|error| {
                crate::errors::Error::DatabaseSyncEngineError(format!(
                    "test full_write failed for {path}: {error}"
                ))
            })?;
            Ok(EmptyCompletion::empty())
        }

        fn transform(
            &self,
            _mutations: Vec<DatabaseRowMutation>,
        ) -> Result<Self::DataCompletionTransform> {
            Ok(EmptyCompletion::empty())
        }

        fn http(
            &self,
            _url: Option<&str>,
            method: &str,
            path: &str,
            body: Option<Vec<u8>>,
            _headers: &[(&str, &str)],
        ) -> Result<Self::DataCompletionBytes> {
            self.request
                .lock()
                .unwrap()
                .replace((method.to_string(), path.to_string(), body));
            let response = self.response.lock().unwrap().take().unwrap_or_default();
            Ok(EmptyCompletion::with_data(response))
        }

        fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

        fn step_io_callbacks(&self) {}
    }

    fn default_test_opts() -> DatabaseSyncEngineOpts {
        DatabaseSyncEngineOpts {
            remote_url: None,
            client_name: "test-client".to_string(),
            tables_ignore: vec![],
            use_transform: false,
            wal_pull_batch_size: 0,
            long_poll_timeout: None,
            protocol_version_hint: DatabaseSyncEngineProtocolVersion::V1,
            bootstrap_if_empty: false,
            reserved_bytes: 0,
            partial_sync_opts: None::<PartialSyncOpts>,
            remote_encryption_key: None,
            push_operations_threshold: None,
            pull_bytes_threshold: None,
            logical_mvcc_pull: true,
        }
    }

    async fn write_replace_base_pages_file<Ctx>(
        coro: &Coro<Ctx>,
        io: &Arc<dyn turso_core::IO>,
        source_db_path: &str,
        changes_path: &str,
    ) -> Result<Arc<dyn turso_core::File>> {
        let pages = std::fs::read(source_db_path).unwrap();
        assert_eq!(pages.len() % PAGE_SIZE, 0);
        let db_size = (pages.len() / PAGE_SIZE) as u32;
        let changes_file = io.open_file(changes_path, OpenFlags::Create, false)?;

        let truncate = changes_file.truncate(0, Completion::new_trunc(|_| {}))?;
        while !truncate.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }

        for (page_idx, page) in pages.chunks_exact(PAGE_SIZE).enumerate() {
            let mut frame = vec![0; WAL_FRAME_SIZE];
            frame[WAL_FRAME_HEADER..].copy_from_slice(page);
            let frame_info = WalFrameInfo {
                page_no: page_idx as u32 + 1,
                db_size: if page_idx + 1 == db_size as usize {
                    db_size
                } else {
                    0
                },
            };
            frame_info.put_to_frame_header(&mut frame);
            let offset = (page_idx * WAL_FRAME_SIZE) as u64;
            let len = frame.len();
            let write = changes_file.pwrite(
                offset,
                Arc::new(Buffer::new(frame)),
                Completion::new_write(move |result| {
                    let Ok(size) = result else {
                        return;
                    };
                    assert_eq!(size as usize, len);
                }),
            )?;
            while !write.succeeded() {
                coro.yield_(SyncEngineIoResult::IO).await?;
            }
        }

        let sync = changes_file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?;
        while !sync.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
        Ok(changes_file)
    }

    #[test]
    fn logical_replay_uses_last_pushed_hint_when_sync_row_is_stale() {
        let floor = resolve_local_replay_floor_change_id(true, 7, 7, Some(12), 7, 34);
        assert_eq!(floor, Some(34));
    }

    #[test]
    fn replace_base_precollection_uses_last_pushed_hint_when_sync_row_is_stale() {
        let floor = resolve_local_replay_floor_change_id(true, 7, 7, Some(12), 7, 34);
        assert_eq!(floor, Some(34));
    }

    #[test]
    fn logical_replay_ignores_last_pushed_hint_from_stale_pull_generation() {
        let floor = resolve_local_replay_floor_change_id(true, 7, 7, Some(12), 6, 34);
        assert_eq!(floor, Some(12));
    }

    #[test]
    fn logical_replay_uses_last_pushed_hint_when_remote_pull_generation_rolls_back() {
        let floor = resolve_local_replay_floor_change_id(true, 7, 6, Some(12), 7, 34);
        assert_eq!(floor, Some(34));
    }

    #[test]
    fn raw_wal_replay_does_not_override_remote_overlap_with_push_hint() {
        let floor = resolve_local_replay_floor_change_id(false, 7, 7, Some(12), 7, 34);
        assert_eq!(floor, Some(12));
    }

    #[test]
    fn replace_base_pages_use_remote_page_transport() {
        assert!(stream_kind_applies_remote_pages(
            DbChangesStreamKind::LegacyPages
        ));
        assert!(stream_kind_applies_remote_pages(DbChangesStreamKind::Pages));
        assert!(stream_kind_applies_remote_pages(
            DbChangesStreamKind::ReplaceBasePages
        ));
        assert!(!stream_kind_applies_remote_pages(
            DbChangesStreamKind::Logical
        ));
    }

    #[test]
    fn v1_page_stream_uses_sql_connection_raw_page_replay_path() {
        assert!(!should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::Legacy,
            false,
            DbChangesStreamKind::LegacyPages,
        ));
        assert!(!should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::V1,
            false,
            DbChangesStreamKind::LegacyPages,
        ));
        assert!(should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::V1,
            false,
            DbChangesStreamKind::Pages,
        ));
        assert!(!should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::V1,
            true,
            DbChangesStreamKind::Pages,
        ));
    }

    #[test]
    fn logical_mvcc_pull_disabled_by_config_falls_back_to_page_pull() {
        assert_eq!(
            logical_mvcc_pull_disable_reason(false, false, None),
            Some("logical MVCC pull is disabled by configuration")
        );
        assert!(!should_use_logical_mvcc_pull(false, false, None));
    }

    #[test]
    fn logical_mvcc_pull_with_partial_sync_falls_back_to_page_pull() {
        assert_eq!(
            logical_mvcc_pull_disable_reason(true, true, None),
            Some("partial sync is active")
        );
        assert!(!should_use_logical_mvcc_pull(true, true, None));
    }

    #[test]
    fn wait_changes_partial_sync_requests_page_pull_even_when_logical_enabled() {
        let temp_file = NamedTempFile::new().unwrap();
        let main_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let main_db = turso_core::Database::open_file(io.clone(), &main_path).unwrap();
        main_db
            .connect()
            .unwrap()
            .execute("PRAGMA journal_mode = 'mvcc'")
            .unwrap();

        let meta = DatabaseMetadata {
            version: DATABASE_METADATA_VERSION.to_string(),
            client_unique_id: "partial-client".to_string(),
            synced_revision: Some(DatabasePullRevision::V1 {
                revision: "g1:o10".to_string(),
            }),
            revert_since_wal_salt: None,
            revert_since_wal_watermark: 0,
            last_pull_unix_time: None,
            last_push_unix_time: None,
            last_pushed_pull_gen_hint: 0,
            last_pushed_change_id_hint: 0,
            partial_bootstrap_server_revision: Some(DatabasePullRevision::V1 {
                revision: "g1:o10".to_string(),
            }),
            logical_table_names_by_stable_id: Default::default(),
            saved_configuration: Some(DatabaseSavedConfiguration {
                remote_url: Some("https://example.com".to_string()),
                partial_sync_prefetch: Some(false),
                partial_sync_segment_size: Some(4096),
            }),
        };
        std::fs::write(create_meta_path(&main_path), meta.dump().unwrap()).unwrap();

        let header = PullUpdatesRespProtoBody {
            server_revision: "g1:o10".to_string(),
            db_size: 0,
            raw_encoding: None,
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
        };
        let sync_io = Arc::new(CapturingSyncEngineIo {
            response: Mutex::new(Some(header.encode_length_delimited_to_vec())),
            request: Mutex::new(None),
        });
        let sync_stats = SyncEngineIoStats::new(sync_io.clone());
        let mut opts = default_test_opts();
        opts.remote_url = Some("https://example.com".to_string());
        opts.partial_sync_opts = Some(PartialSyncOpts {
            bootstrap_strategy: None,
            segment_size: 4096,
            prefetch: false,
        });
        opts.logical_mvcc_pull = true;

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_db = main_db.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine =
                    DatabaseSyncEngine::open_db(&coro, io, sync_stats, main_db, opts).await?;
                let status = engine.wait_changes_from_remote(&coro).await?;
                assert!(matches!(status.stream_kind, DbChangesStreamKind::Pages));
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }

        let (_method, path, body) = sync_io.request.lock().unwrap().clone().unwrap();
        assert_eq!(path, "/pull-updates");
        let request = PullUpdatesReqProtoBody::decode(body.unwrap().as_slice()).unwrap();
        assert!(!request.logical_updates);
    }

    #[test]
    fn initial_logical_mvcc_pull_page_bootstrap_uses_replace_base_apply() {
        let temp_file = NamedTempFile::new().unwrap();
        let main_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let main_db = turso_core::Database::open_file(io.clone(), &main_path).unwrap();
        main_db
            .connect()
            .unwrap()
            .execute("PRAGMA journal_mode = 'mvcc'")
            .unwrap();

        let meta = DatabaseMetadata {
            version: DATABASE_METADATA_VERSION.to_string(),
            client_unique_id: "initial-client".to_string(),
            synced_revision: None,
            revert_since_wal_salt: None,
            revert_since_wal_watermark: 0,
            last_pull_unix_time: None,
            last_push_unix_time: None,
            last_pushed_pull_gen_hint: 0,
            last_pushed_change_id_hint: 0,
            partial_bootstrap_server_revision: None,
            logical_table_names_by_stable_id: Default::default(),
            saved_configuration: Some(DatabaseSavedConfiguration {
                remote_url: Some("https://example.com".to_string()),
                partial_sync_prefetch: None,
                partial_sync_segment_size: None,
            }),
        };
        std::fs::write(create_meta_path(&main_path), meta.dump().unwrap()).unwrap();

        let header = PullUpdatesRespProtoBody {
            server_revision: "g1:o10".to_string(),
            db_size: 0,
            raw_encoding: None,
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
        };
        let sync_io = Arc::new(CapturingSyncEngineIo {
            response: Mutex::new(Some(header.encode_length_delimited_to_vec())),
            request: Mutex::new(None),
        });
        let sync_stats = SyncEngineIoStats::new(sync_io.clone());
        let mut opts = default_test_opts();
        opts.remote_url = Some("https://example.com".to_string());
        opts.logical_mvcc_pull = true;

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_db = main_db.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine =
                    DatabaseSyncEngine::open_db(&coro, io, sync_stats, main_db, opts).await?;
                let status = engine.wait_changes_from_remote(&coro).await?;
                assert!(matches!(
                    status.stream_kind,
                    DbChangesStreamKind::ReplaceBasePages
                ));
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }

        let (_method, path, body) = sync_io.request.lock().unwrap().clone().unwrap();
        assert_eq!(path, "/pull-updates");
        let request = PullUpdatesReqProtoBody::decode(body.unwrap().as_slice()).unwrap();
        assert!(!request.logical_updates);
    }

    #[test]
    fn logical_mvcc_pull_with_remote_encryption_is_disabled_as_unsupported() {
        assert_eq!(
            logical_mvcc_pull_disable_reason(true, false, Some("key")),
            Some("remote encryption is enabled; MVCC logical sync is unsupported for encrypted remotes")
        );
        assert!(!should_use_logical_mvcc_pull(true, false, Some("key")));
    }

    #[test]
    fn logical_mvcc_pull_remains_enabled_for_plain_full_sync() {
        assert_eq!(logical_mvcc_pull_disable_reason(true, false, None), None);
        assert!(should_use_logical_mvcc_pull(true, false, None));
    }

    #[test]
    fn failed_replace_base_local_replay_does_not_advance_synced_revision() {
        let main_file = NamedTempFile::new().unwrap();
        let remote_file = NamedTempFile::new().unwrap();
        let changes_file = NamedTempFile::new().unwrap();
        let main_path = main_file.path().to_str().unwrap().to_string();
        let remote_path = remote_file.path().to_str().unwrap().to_string();
        let changes_path = changes_file.path().to_str().unwrap().to_string();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let remote_db = turso_core::Database::open_file(io.clone(), &remote_path).unwrap();
        let remote_conn = remote_db.connect().unwrap();
        remote_conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        remote_conn
            .execute("PRAGMA capture_data_changes_conn('full,turso_cdc')")
            .unwrap();
        remote_conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        remote_conn
            .execute("INSERT INTO items VALUES (1, 'duplicate'), (2, 'duplicate')")
            .unwrap();
        remote_conn
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();

        let sync_engine_io = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
        let old_revision = DatabasePullRevision::V1 {
            revision: "old-revision".to_string(),
        };
        let new_revision = DatabasePullRevision::V1 {
            revision: "new-revision".to_string(),
        };

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let sync_engine_io = sync_engine_io.clone();
            let main_path = main_path.clone();
            let remote_path = remote_path.clone();
            let changes_path = changes_path.clone();
            let old_revision = old_revision.clone();
            let new_revision = new_revision.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine = DatabaseSyncEngine::create_db(
                    &coro,
                    io.clone(),
                    sync_engine_io.clone(),
                    &main_path,
                    default_test_opts(),
                )
                .await?;
                engine
                    .update_meta(&coro, |meta| {
                        meta.synced_revision = Some(old_revision.clone());
                    })
                    .await?;

                let conn = engine.connect_rw(&coro).await?;
                conn.execute("PRAGMA journal_mode = 'mvcc'")?;
                conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
                conn.execute("INSERT INTO items VALUES (1, 'local-only')")?;
                let base_change_id = max_local_change_id(&coro, &conn).await?.unwrap_or(0);
                update_last_change_id(&coro, &conn, &engine.client_unique_id, 1, base_change_id)
                    .await?;
                conn.execute("CREATE UNIQUE INDEX items_value_unique ON items(value)")?;

                let changes_file =
                    write_replace_base_pages_file(&coro, &io, &remote_path, &changes_path).await?;
                let slot = Arc::new(Mutex::new(Some(changes_file)));
                let file_slot = MutexSlot {
                    value: slot.lock().unwrap().take().unwrap(),
                    slot: slot.clone(),
                };

                REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER.with(|value| value.set(0));
                let result = engine
                    .apply_changes_from_remote(
                        &coro,
                        DbChangesStatus {
                            time: turso_core::WallClockInstant {
                                secs: 10,
                                micros: 0,
                            },
                            revision: new_revision.clone(),
                            file_slot: Some(file_slot),
                            stream_kind: DbChangesStreamKind::ReplaceBasePages,
                        },
                    )
                    .await;
                REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER.with(|value| value.set(usize::MAX));

                let err = result.unwrap_err();
                assert!(
                    format!("{err:#}").contains("injected replace-base local replay failure"),
                    "{err:#}"
                );
                assert_eq!(engine.meta().synced_revision, Some(old_revision.clone()));

                let on_disk_meta = DatabaseSyncEngine::<NoopSyncEngineIo>::read_db_meta(
                    &coro,
                    Some(io.clone()),
                    sync_engine_io.clone(),
                    &main_path,
                )
                .await?
                .unwrap();
                assert_eq!(on_disk_meta.synced_revision, Some(old_revision));
                assert!(io
                    .try_open(&create_replace_base_marker_path(&main_path))?
                    .is_none());

                drop(conn);
                drop(engine);
                let recovered_engine = DatabaseSyncEngine::create_db(
                    &coro,
                    io.clone(),
                    sync_engine_io.clone(),
                    &main_path,
                    default_test_opts(),
                )
                .await?;
                let verify_conn = recovered_engine.connect_rw(&coro).await?;
                let mut value_stmt = verify_conn
                    .prepare("SELECT value FROM items WHERE id = 1")
                    .unwrap();
                let row = run_stmt_once(&coro, &mut value_stmt).await?.unwrap();
                assert_eq!(
                    row.get_values().cloned().collect::<Vec<_>>(),
                    vec![turso_core::Value::Text(turso_core::types::Text::new(
                        "local-only"
                    ))]
                );
                assert!(run_stmt_once(&coro, &mut value_stmt).await?.is_none());

                let mut index_stmt = verify_conn
                    .prepare("SELECT sql FROM sqlite_schema WHERE name = 'items_value_unique'")
                    .unwrap();
                let row = run_stmt_once(&coro, &mut index_stmt).await?.unwrap();
                assert!(row
                    .get_value(0)
                    .to_text()
                    .unwrap()
                    .contains("CREATE UNIQUE INDEX items_value_unique"));
                assert!(run_stmt_once(&coro, &mut index_stmt).await?.is_none());
                Result::Ok(())
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER.with(|value| value.set(usize::MAX));
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn replace_base_preserves_cdc_capture_for_subsequent_local_writes() {
        let main_file = NamedTempFile::new().unwrap();
        let remote_file = NamedTempFile::new().unwrap();
        let changes_file = NamedTempFile::new().unwrap();
        let main_path = main_file.path().to_str().unwrap().to_string();
        let remote_path = remote_file.path().to_str().unwrap().to_string();
        let changes_path = changes_file.path().to_str().unwrap().to_string();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let remote_db = turso_core::Database::open_file(io.clone(), &remote_path).unwrap();
        let remote_conn = remote_db.connect().unwrap();
        remote_conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        remote_conn
            .execute("PRAGMA capture_data_changes_conn('full,turso_cdc')")
            .unwrap();
        remote_conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        remote_conn
            .execute("INSERT INTO items VALUES (1, 'remote')")
            .unwrap();
        remote_conn
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();

        let sync_engine_io = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
        let new_revision = DatabasePullRevision::V1 {
            revision: "new-revision".to_string(),
        };

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let sync_engine_io = sync_engine_io.clone();
            let main_path = main_path.clone();
            let remote_path = remote_path.clone();
            let changes_path = changes_path.clone();
            let new_revision = new_revision.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine = DatabaseSyncEngine::create_db(
                    &coro,
                    io.clone(),
                    sync_engine_io.clone(),
                    &main_path,
                    default_test_opts(),
                )
                .await?;
                let conn = engine.connect_rw(&coro).await?;
                conn.execute("PRAGMA journal_mode = 'mvcc'")?;
                conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
                conn.execute("INSERT INTO items VALUES (2, 'local-before')")?;
                let local_before = max_local_change_id(&coro, &conn).await?.unwrap_or(0);
                update_last_change_id(&coro, &conn, &engine.client_unique_id, 1, local_before)
                    .await?;
                drop(conn);

                let changes_file =
                    write_replace_base_pages_file(&coro, &io, &remote_path, &changes_path).await?;
                let slot = Arc::new(Mutex::new(Some(changes_file)));
                let file_slot = MutexSlot {
                    value: slot.lock().unwrap().take().unwrap(),
                    slot: slot.clone(),
                };
                engine
                    .apply_changes_from_remote(
                        &coro,
                        DbChangesStatus {
                            time: turso_core::WallClockInstant {
                                secs: 10,
                                micros: 0,
                            },
                            revision: new_revision,
                            file_slot: Some(file_slot),
                            stream_kind: DbChangesStreamKind::ReplaceBasePages,
                        },
                    )
                    .await?;

                let conn = engine.connect_rw(&coro).await?;
                let before_insert = max_local_change_id(&coro, &conn).await?.unwrap_or(0);
                conn.execute("INSERT INTO items VALUES (3, 'local-after')")?;
                let after_insert = max_local_change_id(&coro, &conn).await?.unwrap_or(0);
                assert!(
                    after_insert > before_insert,
                    "CDC did not advance after replace-base: before={before_insert} after={after_insert}"
                );
                Result::Ok(())
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn replace_base_recaptures_replayed_local_changes_for_push() {
        let main_file = NamedTempFile::new().unwrap();
        let remote_file = NamedTempFile::new().unwrap();
        let changes_file = NamedTempFile::new().unwrap();
        let main_path = main_file.path().to_str().unwrap().to_string();
        let remote_path = remote_file.path().to_str().unwrap().to_string();
        let changes_path = changes_file.path().to_str().unwrap().to_string();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let remote_db = turso_core::Database::open_file(io.clone(), &remote_path).unwrap();
        let remote_conn = remote_db.connect().unwrap();
        remote_conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        remote_conn
            .execute("PRAGMA capture_data_changes_conn('full,turso_cdc')")
            .unwrap();
        remote_conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        remote_conn
            .execute("INSERT INTO items VALUES (1, 'remote')")
            .unwrap();
        remote_conn
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();

        let sync_engine_io = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
        let new_revision = DatabasePullRevision::V1 {
            revision: "new-revision".to_string(),
        };

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let sync_engine_io = sync_engine_io.clone();
            let main_path = main_path.clone();
            let remote_path = remote_path.clone();
            let changes_path = changes_path.clone();
            let new_revision = new_revision.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine = DatabaseSyncEngine::create_db(
                    &coro,
                    io.clone(),
                    sync_engine_io.clone(),
                    &main_path,
                    default_test_opts(),
                )
                .await?;
                let conn = engine.connect_rw(&coro).await?;
                conn.execute("PRAGMA journal_mode = 'mvcc'")?;
                conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
                let synced_change_id = max_local_change_id(&coro, &conn).await?.unwrap_or(0);
                update_last_change_id(&coro, &conn, &engine.client_unique_id, 1, synced_change_id)
                    .await?;
                conn.execute("INSERT INTO items VALUES (2, 'local-pending')")?;
                drop(conn);

                let changes_file =
                    write_replace_base_pages_file(&coro, &io, &remote_path, &changes_path).await?;
                let slot = Arc::new(Mutex::new(Some(changes_file)));
                let file_slot = MutexSlot {
                    value: slot.lock().unwrap().take().unwrap(),
                    slot: slot.clone(),
                };
                engine
                    .apply_changes_from_remote(
                        &coro,
                        DbChangesStatus {
                            time: turso_core::WallClockInstant {
                                secs: 10,
                                micros: 0,
                            },
                            revision: new_revision,
                            file_slot: Some(file_slot),
                            stream_kind: DbChangesStreamKind::ReplaceBasePages,
                        },
                    )
                    .await?;

                let conn = engine.connect_rw(&coro).await?;
                let mut stmt = conn.prepare("SELECT value FROM items WHERE id = 2")?;
                let row = run_stmt_once(&coro, &mut stmt).await?.unwrap();
                assert_eq!(row.get_value(0).to_text().unwrap(), "local-pending");

                let mut iterator =
                    engine
                        .main_tape
                        .iterate_changes(DatabaseChangesIteratorOpts {
                            first_change_id: Some(1),
                            mode: DatabaseChangesIteratorMode::Apply,
                            ignore_schema_changes: true,
                            ..Default::default()
                        })?;
                let mut saw_replayed_local_row = false;
                while let Some(operation) = iterator.next(&coro).await? {
                    match operation {
                        DatabaseTapeOperation::RowChange(change)
                            if change.table_name == "items" && change.id == 2 =>
                        {
                            saw_replayed_local_row = true;
                            break;
                        }
                        DatabaseTapeOperation::RowChange(_) | DatabaseTapeOperation::Commit => {}
                        DatabaseTapeOperation::StmtReplay(_)
                        | DatabaseTapeOperation::SchemaReplay(_) => {
                            panic!("changes iterator must not produce replay operations")
                        }
                    }
                }
                assert!(
                    saw_replayed_local_row,
                    "replayed local row was not recaptured in CDC"
                );
                Result::Ok(())
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn replace_base_does_not_replay_pushed_local_row_over_remote_backfill() {
        let main_file = NamedTempFile::new().unwrap();
        let remote_file = NamedTempFile::new().unwrap();
        let changes_file = NamedTempFile::new().unwrap();
        let main_path = main_file.path().to_str().unwrap().to_string();
        let remote_path = remote_file.path().to_str().unwrap().to_string();
        let changes_path = changes_file.path().to_str().unwrap().to_string();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let remote_db = turso_core::Database::open_file(io.clone(), &remote_path).unwrap();
        let remote_conn = remote_db.connect().unwrap();
        remote_conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        remote_conn
            .execute("PRAGMA capture_data_changes_conn('full,turso_cdc')")
            .unwrap();
        remote_conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT, note TEXT)")
            .unwrap();
        remote_conn
            .execute("INSERT INTO items VALUES (2, 'local-pushed', 'remote-backfill')")
            .unwrap();
        remote_conn
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();

        let sync_engine_io = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
        let new_revision = DatabasePullRevision::V1 {
            revision: "new-revision".to_string(),
        };

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let sync_engine_io = sync_engine_io.clone();
            let main_path = main_path.clone();
            let remote_path = remote_path.clone();
            let changes_path = changes_path.clone();
            let new_revision = new_revision.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine = DatabaseSyncEngine::create_db(
                    &coro,
                    io.clone(),
                    sync_engine_io.clone(),
                    &main_path,
                    default_test_opts(),
                )
                .await?;
                let conn = engine.connect_rw(&coro).await?;
                conn.execute("PRAGMA journal_mode = 'mvcc'")?;
                conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
                conn.execute("INSERT INTO items VALUES (2, 'local-pushed')")?;
                let pushed_change_id = max_local_change_id(&coro, &conn).await?.unwrap_or(0);
                update_last_change_id(&coro, &conn, &engine.client_unique_id, 1, pushed_change_id)
                    .await?;
                drop(conn);

                engine
                    .update_meta(&coro, |meta| {
                        meta.last_pushed_pull_gen_hint = 0;
                        meta.last_pushed_change_id_hint = 0;
                    })
                    .await?;

                let changes_file =
                    write_replace_base_pages_file(&coro, &io, &remote_path, &changes_path).await?;
                let slot = Arc::new(Mutex::new(Some(changes_file)));
                let file_slot = MutexSlot {
                    value: slot.lock().unwrap().take().unwrap(),
                    slot: slot.clone(),
                };
                engine
                    .apply_changes_from_remote(
                        &coro,
                        DbChangesStatus {
                            time: turso_core::WallClockInstant {
                                secs: 10,
                                micros: 0,
                            },
                            revision: new_revision,
                            file_slot: Some(file_slot),
                            stream_kind: DbChangesStreamKind::ReplaceBasePages,
                        },
                    )
                    .await?;

                let conn = engine.connect_rw(&coro).await?;
                let mut stmt = conn.prepare("SELECT value, note FROM items WHERE id = 2")?;
                let row = run_stmt_once(&coro, &mut stmt).await?.unwrap();
                assert_eq!(row.get_value(0).to_text().unwrap(), "local-pushed");
                assert_eq!(row.get_value(1).to_text().unwrap(), "remote-backfill");
                Result::Ok(())
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }
}
