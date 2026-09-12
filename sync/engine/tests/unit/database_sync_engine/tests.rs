use super::{
    create_main_db_log_path, create_main_db_wal_path, create_meta_path,
    create_replace_base_marker_path, create_revert_db_wal_path, ensure_logical_mvcc_pull_supported,
    ensure_stream_kind_can_use_legacy_page_apply, replace_base_backup_path,
    resolve_local_replay_floor_change_id, resolve_remote_pull_protocol,
    should_replay_raw_pages_on_sql_conn, should_request_logical_pull,
    stream_kind_applies_remote_pages, stream_kind_for_pull_updates_v1_result,
    sync_database_file_paths, synced_change_id_after_remote_apply,
    use_pushed_change_hint_for_local_replay, DatabaseSyncEngine, DatabaseSyncEngineOpts,
    ReplaceBaseApplyGuard, REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER,
};
use crate::{
    client_proto::{
        LogicalOp, LogicalOpType, LogicalSchemaAction, LogicalSchemaKind, LogicalTxnData,
    },
    database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo},
    database_sync_operations::{
        count_local_changes, max_local_change_id, read_last_change_id, update_last_change_id,
        MutexSlot, PullUpdatesV1Result, SyncEngineIoStats,
    },
    database_tape::{run_stmt_once, DatabaseTape, DatabaseTapeOpts},
    errors::Error,
    io_operations::IoOperations,
    server_proto::{
        PageData, PageSetRawEncodingProto, PullUpdatesApplyMode, PullUpdatesProtocol,
        PullUpdatesReqProtoBody, PullUpdatesRespProtoBody, PullUpdatesStreamKind,
    },
    types::{
        Coro, DatabaseMetadata, DatabasePullRevision, DatabaseSavedConfiguration,
        DatabaseSyncEngineProtocolVersion, DbChangesStatus, DbChangesStreamKind, PartialSyncOpts,
        RemotePullProtocol, SyncEngineIoResult, DATABASE_METADATA_VERSION,
    },
    Result,
};
use bytes::Bytes;
use prost::Message;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::NamedTempFile;
use turso_core::SqliteDialect;

#[test]
fn sync_database_paths_include_core_sync_and_mvcc_files() {
    assert_eq!(
        sync_database_file_paths("replica.sqlite"),
        vec![
            "replica.sqlite",
            "replica.sqlite-wal",
            "replica.sqlite-wal-revert",
            "replica.sqlite-info",
            "replica.sqlite-changes",
            "replica.db-log",
        ]
    );
}

#[test]
fn explicit_override_wins_over_persisted_protocol() {
    for persisted in [
        RemotePullProtocol::Unknown,
        RemotePullProtocol::Pages,
        RemotePullProtocol::MvccLogical,
    ] {
        assert_eq!(
            resolve_remote_pull_protocol(Some(true), persisted),
            RemotePullProtocol::MvccLogical
        );
        assert_eq!(
            resolve_remote_pull_protocol(Some(false), persisted),
            RemotePullProtocol::Pages
        );
        assert_eq!(resolve_remote_pull_protocol(None, persisted), persisted);
    }
}

#[test]
fn logical_mvcc_pull_with_partial_sync_is_a_hard_error() {
    // Silent downgrade to page pull would just defer the failure to an
    // opaque server-side protocol error on every incremental pull.
    assert!(ensure_logical_mvcc_pull_supported(true, None).is_err());
}

#[test]
fn logical_mvcc_pull_with_remote_encryption_is_a_hard_error() {
    assert!(ensure_logical_mvcc_pull_supported(false, Some("key")).is_err());
}

#[test]
fn logical_mvcc_pull_remains_enabled_for_plain_full_sync() {
    assert!(ensure_logical_mvcc_pull_supported(false, None).is_ok());
}

#[test]
fn logical_pull_is_requested_only_for_active_v1_revisions() {
    assert!(!should_request_logical_pull(false, &None));
    assert!(!should_request_logical_pull(true, &None));
    assert!(!should_request_logical_pull(
        true,
        &Some(DatabasePullRevision::Legacy {
            generation: 1,
            synced_frame_no: Some(10),
        })
    ));
    assert!(should_request_logical_pull(
        true,
        &Some(DatabasePullRevision::V1 {
            revision: "g1:o42".to_string(),
        })
    ));
}

#[test]
fn legacy_page_apply_rejects_non_page_streams() {
    assert!(ensure_stream_kind_can_use_legacy_page_apply(DbChangesStreamKind::LegacyPages).is_ok());
    assert!(ensure_stream_kind_can_use_legacy_page_apply(DbChangesStreamKind::Pages).is_ok());
    let logical_err =
        ensure_stream_kind_can_use_legacy_page_apply(DbChangesStreamKind::Logical).unwrap_err();
    assert!(
        logical_err.to_string().contains("logical MVCC apply"),
        "unexpected error: {logical_err:?}"
    );
    let replace_base_err =
        ensure_stream_kind_can_use_legacy_page_apply(DbChangesStreamKind::ReplaceBasePages)
            .unwrap_err();
    assert!(
        replace_base_err
            .to_string()
            .contains("replace-base page apply"),
        "unexpected error: {replace_base_err:?}"
    );
}

#[test]
fn logical_pull_page_fallback_preserves_replace_base_kind() {
    assert_eq!(
        stream_kind_for_pull_updates_v1_result(&PullUpdatesV1Result::Pages {
            replace_base: false
        }),
        DbChangesStreamKind::Pages
    );
    assert_eq!(
        stream_kind_for_pull_updates_v1_result(&PullUpdatesV1Result::Pages { replace_base: true }),
        DbChangesStreamKind::ReplaceBasePages
    );
    assert_eq!(
        stream_kind_for_pull_updates_v1_result(&PullUpdatesV1Result::Logical { txns: 1, ops: 2 }),
        DbChangesStreamKind::Logical
    );
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
fn sql_replay_page_routing_keeps_legacy_on_wal_session() {
    assert!(!should_replay_raw_pages_on_sql_conn(
        DatabaseSyncEngineProtocolVersion::Legacy,
        true,
        false,
        DbChangesStreamKind::LegacyPages,
        true,
    ));
    assert!(!should_replay_raw_pages_on_sql_conn(
        DatabaseSyncEngineProtocolVersion::Legacy,
        true,
        false,
        DbChangesStreamKind::ReplaceBasePages,
        true,
    ));
    assert!(!should_replay_raw_pages_on_sql_conn(
        DatabaseSyncEngineProtocolVersion::V1,
        false,
        false,
        DbChangesStreamKind::Pages,
        true,
    ));
    assert!(!should_replay_raw_pages_on_sql_conn(
        DatabaseSyncEngineProtocolVersion::V1,
        true,
        false,
        DbChangesStreamKind::Pages,
        false,
    ));
    assert!(should_replay_raw_pages_on_sql_conn(
        DatabaseSyncEngineProtocolVersion::V1,
        true,
        false,
        DbChangesStreamKind::Pages,
        true,
    ));
    assert!(should_replay_raw_pages_on_sql_conn(
        DatabaseSyncEngineProtocolVersion::V1,
        true,
        false,
        DbChangesStreamKind::ReplaceBasePages,
        true,
    ));
    assert!(!should_replay_raw_pages_on_sql_conn(
        DatabaseSyncEngineProtocolVersion::V1,
        true,
        true,
        DbChangesStreamKind::ReplaceBasePages,
        true,
    ));
    assert!(!should_replay_raw_pages_on_sql_conn(
        DatabaseSyncEngineProtocolVersion::V1,
        true,
        false,
        DbChangesStreamKind::Logical,
        true,
    ));
}

#[test]
fn v1_page_replay_uses_remote_snapshot_sync_row_not_later_push_hint() {
    assert!(!use_pushed_change_hint_for_local_replay(
        DbChangesStreamKind::Pages,
        false
    ));
    let floor = resolve_local_replay_floor_change_id(false, 7, 7, Some(12), 7, 34);
    assert_eq!(floor, Some(12));
}

#[test]
fn legacy_page_replay_can_use_last_pushed_hint_when_sync_row_is_stale() {
    assert!(use_pushed_change_hint_for_local_replay(
        DbChangesStreamKind::LegacyPages,
        false
    ));
    let floor = resolve_local_replay_floor_change_id(true, 7, 7, Some(12), 7, 34);
    assert_eq!(floor, Some(34));
}

#[test]
fn local_replay_ignores_last_pushed_hint_from_stale_pull_generation() {
    let floor = resolve_local_replay_floor_change_id(true, 7, 7, Some(12), 6, 34);
    assert_eq!(floor, Some(12));
}

#[test]
fn raw_wal_replay_preserves_existing_floor_when_hints_are_disabled() {
    let floor = resolve_local_replay_floor_change_id(false, 7, 7, Some(12), 7, 34);
    assert_eq!(floor, Some(12));
}

#[test]
fn remote_apply_acknowledges_pre_apply_cdc_when_local_changes_are_recaptured() {
    assert_eq!(
        synced_change_id_after_remote_apply(false, Some(12), 40),
        40,
        "without local replay, all CDC generated by remote apply is acknowledged"
    );
    assert_eq!(
        synced_change_id_after_remote_apply(true, Some(12), 40),
        12,
        "with local replay, preserve the replay floor so local rows remain pushable"
    );
    assert_eq!(
        synced_change_id_after_remote_apply(true, Some(11), 8),
        8,
        "the persisted sync floor must never advance beyond the local CDC high-water"
    );
    assert_eq!(
        synced_change_id_after_remote_apply(true, None, 40),
        0,
        "a database with no pre-existing CDC still starts from zero"
    );
}

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
    type DataCompletionTransform = EmptyCompletion<crate::types::DatabaseRowTransformResult>;

    fn full_read(&self, path: &str) -> Result<Self::DataCompletionBytes> {
        let data = match std::fs::read(path) {
            Ok(data) => data,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(crate::errors::Error::DatabaseSyncEngineError(format!(
                    "test full_read failed for {path}: {error}"
                )));
            }
        };
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
        _mutations: Vec<crate::types::DatabaseRowMutation>,
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
    #[allow(clippy::type_complexity)]
    request: Mutex<Option<(String, String, Option<Vec<u8>>)>>,
}

impl SyncEngineIo for CapturingSyncEngineIo {
    type DataCompletionBytes = EmptyCompletion<u8>;
    type DataCompletionTransform = EmptyCompletion<crate::types::DatabaseRowTransformResult>;

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
        _mutations: Vec<crate::types::DatabaseRowMutation>,
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

/// Serves canned HTTP responses in order; panics if the engine makes more
/// requests than responses were queued. Records every request for
/// assertions on the wire traffic.
struct QueuedSyncEngineIo {
    responses: Mutex<std::collections::VecDeque<Vec<u8>>>,
    #[allow(clippy::type_complexity)]
    requests: Mutex<Vec<(String, String, Option<Vec<u8>>)>>,
}

impl SyncEngineIo for QueuedSyncEngineIo {
    type DataCompletionBytes = EmptyCompletion<u8>;
    type DataCompletionTransform = EmptyCompletion<crate::types::DatabaseRowTransformResult>;

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
        _mutations: Vec<crate::types::DatabaseRowMutation>,
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
        self.requests
            .lock()
            .unwrap()
            .push((method.to_string(), path.to_string(), body));
        let response = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("engine made more HTTP requests than queued responses");
        Ok(EmptyCompletion::with_data(response))
    }

    fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

    fn step_io_callbacks(&self) {}
}

fn record(values: &[turso_core::Value]) -> Bytes {
    Bytes::from(
        turso_core::types::ImmutableRecord::from_values(values, values.len())
            .unwrap()
            .into_payload(),
    )
}

fn encoded_logical_txns(txns: &[LogicalTxnData]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for txn in txns {
        bytes.extend_from_slice(&txn.encode_length_delimited_to_vec());
    }
    bytes
}

fn default_test_opts() -> DatabaseSyncEngineOpts {
    DatabaseSyncEngineOpts {
        remote_url: None,
        client_name: "test-client".to_string(),
        tables_ignore: vec![],
        use_transform: false,
        wal_pull_batch_size: 0,
        long_poll_timeout: Some(Duration::from_millis(1)),
        protocol_version_hint: DatabaseSyncEngineProtocolVersion::V1,
        bootstrap_if_empty: false,
        reserved_bytes: 0,
        db_opts: turso_core::DatabaseOpts::default(),
        partial_sync_opts: None::<PartialSyncOpts>,
        remote_encryption_key: None,
        push_operations_threshold: None,
        pull_bytes_threshold: None,
        logical_mvcc_pull: Some(true),
    }
}

fn replace_base_guard_test_paths(
    main_path: &str,
) -> Vec<(&'static str, String, Option<&'static [u8]>)> {
    vec![
        ("main-db", main_path.to_string(), Some(b"main-old")),
        (
            "main-wal",
            create_main_db_wal_path(main_path),
            Some(b"wal-old"),
        ),
        (
            "main-log",
            create_main_db_log_path(main_path),
            Some(b"log-old"),
        ),
        ("revert-wal", create_revert_db_wal_path(main_path), None),
        ("metadata", create_meta_path(main_path), Some(b"meta-old")),
    ]
}

fn write_replace_base_guard_test_files(main_path: &str) {
    for (_, path, content) in replace_base_guard_test_paths(main_path) {
        if let Some(content) = content {
            std::fs::write(path, content).unwrap();
        }
    }
}

fn assert_replace_base_backups_removed(main_path: &str) {
    assert!(std::fs::read(create_replace_base_marker_path(main_path)).is_err());
    for (name, _, _) in replace_base_guard_test_paths(main_path) {
        assert!(std::fs::read(replace_base_backup_path(main_path, name)).is_err());
    }
}

async fn write_replace_base_pages_file<Ctx>(
    coro: &Coro<Ctx>,
    io: &Arc<dyn turso_core::IO>,
    source_db_path: &str,
    changes_path: &str,
) -> Result<Arc<dyn turso_core::File>> {
    let pages = std::fs::read(source_db_path).unwrap();
    assert_eq!(pages.len() % super::PAGE_SIZE, 0);
    let db_size = (pages.len() / super::PAGE_SIZE) as u32;
    let changes_file = io.open_file(changes_path, turso_core::OpenFlags::Create, false)?;

    let truncate = changes_file.truncate(0, turso_core::Completion::new_trunc(|_| {}))?;
    while !truncate.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }

    for (page_idx, page) in pages.chunks_exact(super::PAGE_SIZE).enumerate() {
        let mut frame = vec![0; super::WAL_FRAME_SIZE];
        frame[super::WAL_FRAME_HEADER..].copy_from_slice(page);
        let frame_info = turso_core::types::WalFrameInfo {
            page_no: page_idx as u32 + 1,
            db_size: if page_idx + 1 == db_size as usize {
                db_size
            } else {
                0
            },
        };
        frame_info.put_to_frame_header(&mut frame);
        let offset = (page_idx * super::WAL_FRAME_SIZE) as u64;
        let len = frame.len();
        let write = changes_file.pwrite(
            offset,
            Arc::new(turso_core::Buffer::new(frame)),
            turso_core::Completion::new_write(move |result| {
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

    let sync = changes_file.sync(
        turso_core::Completion::new_sync(|_| {}),
        turso_core::io::FileSyncType::Fsync,
    )?;
    while !sync.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(changes_file)
}

#[test]
fn replace_base_guard_restores_original_files_and_removes_created_files() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let main_path = temp_dir
        .path()
        .join("guard-restore.db")
        .to_string_lossy()
        .to_string();
    write_replace_base_guard_test_files(&main_path);

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let sync_io = Arc::new(CapturingSyncEngineIo {
        response: Mutex::new(None),
        request: Mutex::new(None),
    });
    let sync_stats = SyncEngineIoStats::new(sync_io);
    let old_revision = DatabasePullRevision::V1 {
        revision: "old-revision".to_string(),
    };

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let main_path = main_path.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let guard = ReplaceBaseApplyGuard::create(
                &coro,
                io.clone(),
                sync_stats,
                &main_path,
                Some(old_revision),
            )
            .await?;

            std::fs::write(&main_path, b"main-new").unwrap();
            std::fs::write(create_main_db_wal_path(&main_path), b"wal-new").unwrap();
            std::fs::write(create_main_db_log_path(&main_path), b"log-new").unwrap();
            std::fs::write(create_revert_db_wal_path(&main_path), b"revert-created").unwrap();
            std::fs::write(create_meta_path(&main_path), b"meta-new").unwrap();

            guard.restore(&coro).await?;
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
        }
    }

    assert_eq!(std::fs::read(&main_path).unwrap(), b"main-old");
    assert_eq!(
        std::fs::read(create_main_db_wal_path(&main_path)).unwrap(),
        b"wal-old"
    );
    assert_eq!(
        std::fs::read(create_main_db_log_path(&main_path)).unwrap(),
        b"log-old"
    );
    assert!(std::fs::read(create_revert_db_wal_path(&main_path)).is_err());
    assert_eq!(
        std::fs::read(create_meta_path(&main_path)).unwrap(),
        b"meta-old"
    );
    assert_replace_base_backups_removed(&main_path);
}

#[test]
fn replace_base_guard_recovers_pending_marker() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let main_path = temp_dir
        .path()
        .join("guard-recover.db")
        .to_string_lossy()
        .to_string();
    write_replace_base_guard_test_files(&main_path);

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let sync_io = Arc::new(CapturingSyncEngineIo {
        response: Mutex::new(None),
        request: Mutex::new(None),
    });
    let sync_stats = SyncEngineIoStats::new(sync_io);

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let main_path = main_path.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let _guard = ReplaceBaseApplyGuard::create(
                &coro,
                io.clone(),
                sync_stats.clone(),
                &main_path,
                None,
            )
            .await?;

            std::fs::write(&main_path, b"main-new").unwrap();
            std::fs::write(create_meta_path(&main_path), b"meta-new").unwrap();

            let recovered =
                ReplaceBaseApplyGuard::recover_pending(&coro, io.clone(), sync_stats, &main_path)
                    .await?;
            assert!(recovered);
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
        }
    }

    assert_eq!(std::fs::read(&main_path).unwrap(), b"main-old");
    assert_eq!(
        std::fs::read(create_meta_path(&main_path)).unwrap(),
        b"meta-old"
    );
    assert_replace_base_backups_removed(&main_path);
}

#[test]
fn replace_base_guard_mark_complete_removes_marker_without_restoring() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let main_path = temp_dir
        .path()
        .join("guard-complete.db")
        .to_string_lossy()
        .to_string();
    write_replace_base_guard_test_files(&main_path);

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let sync_io = Arc::new(CapturingSyncEngineIo {
        response: Mutex::new(None),
        request: Mutex::new(None),
    });
    let sync_stats = SyncEngineIoStats::new(sync_io);

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let main_path = main_path.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let mut guard =
                ReplaceBaseApplyGuard::create(&coro, io.clone(), sync_stats, &main_path, None)
                    .await?;
            std::fs::write(&main_path, b"main-new").unwrap();
            guard.mark_complete(&coro).await?;
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
        }
    }

    assert_eq!(std::fs::read(&main_path).unwrap(), b"main-new");
    assert_replace_base_backups_removed(&main_path);
}

#[test]
fn initial_logical_mvcc_pull_page_bootstrap_uses_replace_base_apply() {
    let temp_file = NamedTempFile::new().unwrap();
    let main_path = temp_file.path().to_str().unwrap().to_string();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let main_db =
        turso_core::Database::open_file(io.clone(), &main_path, Arc::new(SqliteDialect)).unwrap();

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
        last_pushed_replay_floor_change_id_hint: 0,
        partial_bootstrap_server_revision: None,
        fresh_bootstrap_pending_cdc_ack: false,
        remote_pull_protocol: RemotePullProtocol::MvccLogical,
        logical_table_names_by_stable_id: Default::default(),
        saved_configuration: Some(DatabaseSavedConfiguration {
            remote_url: Some("https://example.com".to_string()),
            partial_sync_prefetch: None,
            partial_sync_segment_size: None,
        }),
    };
    std::fs::write(create_meta_path(&main_path), meta.dump().unwrap()).unwrap();

    let header = PullUpdatesRespProtoBody {
        protocol: 0,
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
    opts.logical_mvcc_pull = Some(true);

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let main_db = main_db.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let engine = DatabaseSyncEngine::open_db(&coro, io, sync_stats, main_db, opts).await?;
            let status = engine.wait_changes_from_remote(&coro).await?;
            assert!(status.file_slot.is_none());
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
    assert_eq!(request.stream_kind, PullUpdatesStreamKind::Pages as i32);
    assert_eq!(request.client_revision, "");
    assert_eq!(request.server_revision, "");
}

#[test]
fn apply_changes_from_remote_applies_logical_stream_without_local_replay() {
    let db_temp = NamedTempFile::new().unwrap();
    let meta_temp = NamedTempFile::new().unwrap();
    let changes_temp = NamedTempFile::new().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let txns = vec![
        LogicalTxnData {
            end_offset: 1,
            commit_ts: 1,
            origin_client_id: "remote".to_string(),
            ops: vec![LogicalOp {
                op_type: LogicalOpType::Schema as i32,
                table_name: String::new(),
                rowid: 0,
                record: Bytes::new(),
                sql: "CREATE TABLE items(x TEXT)".to_string(),
                user_version: None,
                application_id: None,
                schema_action: Some(LogicalSchemaAction::Create as i32),
                schema_kind: Some(LogicalSchemaKind::Table as i32),
                schema_name: "items".to_string(),
                stable_table_id: 7,
            }],
        },
        LogicalTxnData {
            end_offset: 2,
            commit_ts: 2,
            origin_client_id: "remote".to_string(),
            ops: vec![LogicalOp {
                op_type: LogicalOpType::UpsertRow as i32,
                table_name: String::new(),
                rowid: 2,
                record: record(&[turso_core::Value::Text(turso_core::types::Text::new(
                    "remote".to_string(),
                ))]),
                sql: String::new(),
                user_version: None,
                application_id: None,
                schema_action: None,
                schema_kind: None,
                schema_name: String::new(),
                stable_table_id: 7,
            }],
        },
    ];
    std::fs::write(changes_temp.path(), encoded_logical_txns(&txns)).unwrap();

    let db = turso_core::Database::open_file(
        io.clone(),
        db_temp.path().to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let db_file = db.db_file.clone();
    let db_io = db.io.clone();
    let main_tape = DatabaseTape::new_with_opts(
        db,
        DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some("full".to_string()),
            disable_auto_checkpoint: true,
        },
    );
    let sync_io = Arc::new(NoopSyncEngineIo);
    let sync_engine_io = SyncEngineIoStats::new(sync_io);
    let changes_file = io
        .open_file(
            changes_temp.path().to_str().unwrap(),
            turso_core::OpenFlags::None,
            false,
        )
        .unwrap();
    let slot = Arc::new(Mutex::new(None));
    let meta = DatabaseMetadata {
        version: DATABASE_METADATA_VERSION.to_string(),
        client_unique_id: "client-a".to_string(),
        synced_revision: Some(DatabasePullRevision::V1 {
            revision: "g1:o1".to_string(),
        }),
        revert_since_wal_salt: None,
        revert_since_wal_watermark: 0,
        last_pull_unix_time: None,
        last_push_unix_time: None,
        last_pushed_pull_gen_hint: 0,
        last_pushed_change_id_hint: 0,
        last_pushed_replay_floor_change_id_hint: 0,
        partial_bootstrap_server_revision: None,
        fresh_bootstrap_pending_cdc_ack: false,
        remote_pull_protocol: RemotePullProtocol::MvccLogical,
        logical_table_names_by_stable_id: Default::default(),
        saved_configuration: Some(DatabaseSavedConfiguration {
            remote_url: None,
            partial_sync_prefetch: None,
            partial_sync_segment_size: None,
        }),
    };
    let engine = DatabaseSyncEngine {
        io: db_io,
        sync_engine_io,
        db_file,
        main_tape,
        main_db_path: db_temp.path().to_str().unwrap().to_string(),
        main_db_wal_path: super::create_main_db_wal_path(db_temp.path().to_str().unwrap()),
        revert_db_wal_path: super::create_revert_db_wal_path(db_temp.path().to_str().unwrap()),
        meta_path: meta_temp.path().to_str().unwrap().to_string(),
        changes_file: Arc::new(Mutex::new(None)),
        opts: default_test_opts(),
        meta: Mutex::new(meta),
        client_unique_id: "client-a".to_string(),
    };
    let remote_changes = DbChangesStatus {
        time: io.current_time_wall_clock(),
        revision: DatabasePullRevision::V1 {
            revision: "g1:o2".to_string(),
        },
        file_slot: Some(crate::database_sync_operations::MutexSlot {
            value: changes_file,
            slot,
        }),
        stream_kind: DbChangesStreamKind::Logical,
    };

    let mut gen = genawaiter::sync::Gen::new({
        let engine = engine;
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            engine
                .apply_changes_from_remote(&coro, remote_changes)
                .await
                .unwrap();
            let conn = engine.main_tape.connect(&coro).await.unwrap();
            let mut stmt = conn.prepare("SELECT rowid, x FROM items").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            let (pull_gen, change_id) = read_last_change_id(&coro, &conn, &engine.client_unique_id)
                .await
                .unwrap();
            let pending_local_changes =
                count_local_changes(&coro, &conn, &engine.opts, change_id.unwrap())
                    .await
                    .unwrap();
            let meta = engine.meta.lock().unwrap().clone();
            (rows, meta, pull_gen, change_id, pending_local_changes)
        }
    });
    let (rows, meta, pull_gen, change_id, pending_local_changes) = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };

    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::from_i64(2),
            turso_core::Value::Text(turso_core::types::Text::new("remote".to_string())),
        ]]
    );
    assert_eq!(
        meta.synced_revision,
        Some(DatabasePullRevision::V1 {
            revision: "g1:o2".to_string(),
        })
    );
    assert_eq!(
        meta.logical_table_names_by_stable_id.get(&7).unwrap(),
        "items"
    );
    assert_eq!(meta.revert_since_wal_watermark, 0);
    assert_eq!(pull_gen, 0);
    assert!(change_id.is_some());
    assert_eq!(pending_local_changes, 0);
}

#[test]
fn apply_changes_from_remote_replays_pending_local_changes() {
    let db_temp = NamedTempFile::new().unwrap();
    let meta_temp = NamedTempFile::new().unwrap();
    let changes_temp = NamedTempFile::new().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let txns = vec![LogicalTxnData {
        end_offset: 1,
        commit_ts: 1,
        origin_client_id: "remote".to_string(),
        ops: vec![
            LogicalOp {
                op_type: LogicalOpType::Schema as i32,
                table_name: String::new(),
                rowid: 0,
                record: Bytes::new(),
                sql: "CREATE TABLE remote_items(id INTEGER PRIMARY KEY, x TEXT)".to_string(),
                user_version: None,
                application_id: None,
                schema_action: Some(LogicalSchemaAction::Create as i32),
                schema_kind: Some(LogicalSchemaKind::Table as i32),
                schema_name: "remote_items".to_string(),
                stable_table_id: 9,
            },
            LogicalOp {
                op_type: LogicalOpType::UpsertRow as i32,
                table_name: String::new(),
                rowid: 2,
                record: record(&[
                    turso_core::Value::from_i64(2),
                    turso_core::Value::Text(turso_core::types::Text::new("remote".to_string())),
                ]),
                sql: String::new(),
                user_version: None,
                application_id: None,
                schema_action: None,
                schema_kind: None,
                schema_name: String::new(),
                stable_table_id: 9,
            },
        ],
    }];
    std::fs::write(changes_temp.path(), encoded_logical_txns(&txns)).unwrap();

    let db = turso_core::Database::open_file(
        io.clone(),
        db_temp.path().to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let db_file = db.db_file.clone();
    let db_io = db.io.clone();
    let main_tape = DatabaseTape::new_with_opts(
        db,
        DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some("full".to_string()),
            disable_auto_checkpoint: true,
        },
    );
    let sync_engine_io = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
    let changes_file = io
        .open_file(
            changes_temp.path().to_str().unwrap(),
            turso_core::OpenFlags::None,
            false,
        )
        .unwrap();
    let old_revision = DatabasePullRevision::V1 {
        revision: "g1:o1".to_string(),
    };
    let meta = DatabaseMetadata {
        version: DATABASE_METADATA_VERSION.to_string(),
        client_unique_id: "client-a".to_string(),
        synced_revision: Some(old_revision.clone()),
        revert_since_wal_salt: None,
        revert_since_wal_watermark: 0,
        last_pull_unix_time: None,
        last_push_unix_time: None,
        last_pushed_pull_gen_hint: 0,
        last_pushed_change_id_hint: 0,
        last_pushed_replay_floor_change_id_hint: 0,
        partial_bootstrap_server_revision: None,
        fresh_bootstrap_pending_cdc_ack: false,
        remote_pull_protocol: RemotePullProtocol::MvccLogical,
        logical_table_names_by_stable_id: Default::default(),
        saved_configuration: Some(DatabaseSavedConfiguration {
            remote_url: None,
            partial_sync_prefetch: None,
            partial_sync_segment_size: None,
        }),
    };
    let engine = DatabaseSyncEngine {
        io: db_io,
        sync_engine_io,
        db_file,
        main_tape,
        main_db_path: db_temp.path().to_str().unwrap().to_string(),
        main_db_wal_path: super::create_main_db_wal_path(db_temp.path().to_str().unwrap()),
        revert_db_wal_path: super::create_revert_db_wal_path(db_temp.path().to_str().unwrap()),
        meta_path: meta_temp.path().to_str().unwrap().to_string(),
        changes_file: Arc::new(Mutex::new(None)),
        opts: default_test_opts(),
        meta: Mutex::new(meta),
        client_unique_id: "client-a".to_string(),
    };
    let remote_changes = DbChangesStatus {
        time: io.current_time_wall_clock(),
        revision: DatabasePullRevision::V1 {
            revision: "g1:o2".to_string(),
        },
        file_slot: Some(crate::database_sync_operations::MutexSlot {
            value: changes_file,
            slot: Arc::new(Mutex::new(None)),
        }),
        stream_kind: DbChangesStreamKind::Logical,
    };

    let mut gen = genawaiter::sync::Gen::new({
        let engine = engine;
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = engine.main_tape.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE local_items(id INTEGER PRIMARY KEY, x TEXT)")
                .unwrap();
            conn.execute("INSERT INTO local_items(id, x) VALUES (1, 'local')")
                .unwrap();

            engine
                .apply_changes_from_remote(&coro, remote_changes)
                .await
                .unwrap();

            let mut stmt = conn
                .prepare("SELECT id, x FROM remote_items ORDER BY id")
                .unwrap();
            let remote_row = run_stmt_once(&coro, &mut stmt)
                .await
                .unwrap()
                .unwrap()
                .get_values()
                .cloned()
                .collect::<Vec<_>>();
            assert!(run_stmt_once(&coro, &mut stmt).await.unwrap().is_none());

            let mut stmt = conn
                .prepare("SELECT id, x FROM local_items ORDER BY id")
                .unwrap();
            let local_row = run_stmt_once(&coro, &mut stmt)
                .await
                .unwrap()
                .unwrap()
                .get_values()
                .cloned()
                .collect::<Vec<_>>();
            assert!(run_stmt_once(&coro, &mut stmt).await.unwrap().is_none());

            let (_, synced_change_id) = read_last_change_id(&coro, &conn, &engine.client_unique_id)
                .await
                .unwrap();
            let pending_local_changes =
                count_local_changes(&coro, &conn, &engine.opts, synced_change_id.unwrap())
                    .await
                    .unwrap();
            let meta = engine.meta.lock().unwrap().clone();
            (remote_row, local_row, pending_local_changes, meta)
        }
    });
    let (remote_row, local_row, pending_local_changes, meta) = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };
    assert_eq!(
        remote_row,
        vec![
            turso_core::Value::from_i64(2),
            turso_core::Value::Text(turso_core::types::Text::new("remote".to_string())),
        ]
    );
    assert_eq!(
        local_row,
        vec![
            turso_core::Value::from_i64(1),
            turso_core::Value::Text(turso_core::types::Text::new("local".to_string())),
        ]
    );
    assert!(
        pending_local_changes > 0,
        "recaptured local CDC should remain pending for push"
    );
    assert_eq!(
        meta.synced_revision,
        Some(DatabasePullRevision::V1 {
            revision: "g1:o2".to_string(),
        })
    );
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
    let remote_db =
        turso_core::Database::open_file(io.clone(), &remote_path, Arc::new(SqliteDialect)).unwrap();
    let remote_conn = remote_db.connect().unwrap();
    remote_conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    remote_conn
        .execute("INSERT INTO items VALUES (1, 'duplicate'), (2, 'duplicate')")
        .unwrap();
    remote_conn
        .checkpoint(turso_core::CheckpointMode::Truncate {
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
            .await
            .map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test create_db failed: {error}"))
            })?;
            engine
                .update_meta(&coro, |meta| {
                    meta.synced_revision = Some(old_revision.clone());
                    meta.remote_pull_protocol = RemotePullProtocol::Pages;
                })
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test update_meta failed: {error}"))
                })?;

            let conn = engine.connect_rw(&coro).await.map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test connect_rw failed: {error}"))
            })?;
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
            .await
            .map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test read_db_meta failed: {error}"))
            })?
            .unwrap();
            assert_eq!(on_disk_meta.synced_revision, Some(old_revision));
            assert!(io
                .try_open(&create_replace_base_marker_path(&main_path))?
                .is_none());

            drop(conn);
            drop(engine);
            let verify_db =
                turso_core::Database::open_file(io.clone(), &main_path, Arc::new(SqliteDialect))
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!("test verify open failed: {error}"))
                    })?;
            let verify_conn = verify_db.connect().map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test verify connect failed: {error}"))
            })?;
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

/// Full-fidelity page-stream response: header (with the given protocol
/// hint) followed by one PageData message per page of `db_bytes`.
fn encoded_page_stream_response(
    db_bytes: &[u8],
    server_revision: &str,
    protocol: PullUpdatesProtocol,
) -> Vec<u8> {
    assert_eq!(db_bytes.len() % super::PAGE_SIZE, 0);
    let header = PullUpdatesRespProtoBody {
        server_revision: server_revision.to_string(),
        db_size: (db_bytes.len() / super::PAGE_SIZE) as u64,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: None,
        protocol: protocol as i32,
    };
    let mut bytes = header.encode_length_delimited_to_vec();
    for (page_idx, page) in db_bytes.chunks_exact(super::PAGE_SIZE).enumerate() {
        let page_data = PageData {
            page_id: page_idx as u64,
            encoded_page: Bytes::copy_from_slice(page),
        };
        bytes.extend_from_slice(&page_data.encode_length_delimited_to_vec());
    }
    bytes
}

/// Deferred-bootstrap replica ("converted to cloud sync later"): a local
/// WAL-mode database with local writes meets an MVCC-protocol remote on
/// first contact. The engine must detect the protocol, convert the local
/// database to MVCC journal mode, apply the remote base as replace-base,
/// replay the local changes on top, and stay MVCC across a reopen.
#[test]
fn deferred_first_contact_converts_wal_replica_to_mvcc_and_applies_base() {
    let main_file = NamedTempFile::new().unwrap();
    let remote_file = NamedTempFile::new().unwrap();
    let main_path = main_file.path().to_str().unwrap().to_string();
    let remote_path = remote_file.path().to_str().unwrap().to_string();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let remote_db =
        turso_core::Database::open_file(io.clone(), &remote_path, Arc::new(SqliteDialect)).unwrap();
    let remote_conn = remote_db.connect().unwrap();
    remote_conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    assert!(remote_conn.mvcc_enabled());
    remote_conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    remote_conn
        .execute("INSERT INTO items VALUES (1, 'remote-a'), (2, 'remote-b')")
        .unwrap();
    let remote_wal_state = remote_conn.wal_state().unwrap();
    remote_conn
        .checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: Some(remote_wal_state.max_frame),
        })
        .unwrap();
    drop(remote_conn);
    drop(remote_db);
    let remote_bytes = std::fs::read(&remote_path).unwrap();
    assert!(!remote_bytes.is_empty());

    let server_revision = "g1:o0";
    let first_contact_response = encoded_page_stream_response(
        &remote_bytes,
        server_revision,
        PullUpdatesProtocol::MvccLogical,
    );
    // The replace-base apply issues one follow-up logical pull from the
    // new revision; serve it an empty logical stream.
    let followup_logical_response = PullUpdatesRespProtoBody {
        server_revision: server_revision.to_string(),
        db_size: (remote_bytes.len() / super::PAGE_SIZE) as u64,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: None,
        protocol: PullUpdatesProtocol::MvccLogical as i32,
    }
    .encode_length_delimited_to_vec();
    let sync_io = Arc::new(QueuedSyncEngineIo {
        responses: Mutex::new(
            vec![first_contact_response, followup_logical_response]
                .into_iter()
                .collect(),
        ),
        requests: Mutex::new(Vec::new()),
    });
    let sync_engine_io = SyncEngineIoStats::new(sync_io);

    let mut opts = default_test_opts();
    opts.remote_url = Some("https://example.com".to_string());
    opts.bootstrap_if_empty = false;
    opts.logical_mvcc_pull = None; // auto-detect

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let main_path = main_path.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let engine = DatabaseSyncEngine::create_db(
                &coro,
                io.clone(),
                sync_engine_io.clone(),
                &main_path,
                opts,
            )
            .await
            .map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test create_db failed: {error}"))
            })?;
            assert_eq!(
                engine.meta().remote_pull_protocol,
                RemotePullProtocol::Unknown
            );

            // Local writes before ever contacting the server.
            let conn = engine.connect_rw(&coro).await.map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test connect_rw failed: {error}"))
            })?;
            assert!(!conn.mvcc_enabled());
            conn.execute("CREATE TABLE local_notes(id INTEGER PRIMARY KEY, note TEXT)")?;
            conn.execute("INSERT INTO local_notes VALUES (1, 'kept-across-conversion')")?;
            let base_change_id = max_local_change_id(&coro, &conn).await?.unwrap_or(0);
            update_last_change_id(&coro, &conn, &engine.client_unique_id, 1, base_change_id)
                .await?;
            drop(conn);

            // First contact: detection, conversion, replace-base pull.
            let status = engine.wait_changes_from_remote(&coro).await?;
            assert!(matches!(
                status.stream_kind,
                DbChangesStreamKind::ReplaceBasePages
            ));
            assert!(status.file_slot.is_some());
            assert_eq!(
                engine.meta().remote_pull_protocol,
                RemotePullProtocol::MvccLogical
            );
            assert!(engine.meta().logical_mvcc_pull_active());

            engine.apply_changes_from_remote(&coro, status).await?;
            assert_eq!(
                engine.meta().synced_revision,
                Some(DatabasePullRevision::V1 {
                    revision: server_revision.to_string(),
                })
            );

            // Remote base and replayed local data both present, on an
            // MVCC-mode connection.
            let conn = engine.connect_rw(&coro).await.map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test reconnect failed: {error}"))
            })?;
            assert!(conn.mvcc_enabled());
            let mut stmt = conn.prepare("SELECT value FROM items ORDER BY id")?;
            let mut values = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await? {
                values.push(row.get_value(0).to_text().unwrap().to_string());
            }
            assert_eq!(values, vec!["remote-a".to_string(), "remote-b".to_string()]);
            let mut stmt = conn.prepare("SELECT note FROM local_notes")?;
            let row = run_stmt_once(&coro, &mut stmt).await?.unwrap();
            assert_eq!(
                row.get_value(0).to_text().unwrap(),
                "kept-across-conversion"
            );
            assert!(run_stmt_once(&coro, &mut stmt).await?.is_none());
            drop(stmt);
            drop(conn);
            drop(engine);

            // The conversion must survive a reopen: header version and
            // logical log agree, so a fresh open comes up in MVCC mode
            // with the same data.
            let verify_db =
                turso_core::Database::open_file(io.clone(), &main_path, Arc::new(SqliteDialect))
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!("test verify open failed: {error}"))
                    })?;
            let verify_conn = verify_db.connect().map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test verify connect failed: {error}"))
            })?;
            assert!(verify_conn.mvcc_enabled());
            let mut stmt = verify_conn.prepare("SELECT COUNT(*) FROM items")?;
            let row = run_stmt_once(&coro, &mut stmt).await?.unwrap();
            assert_eq!(row.get_value(0).as_int(), Some(2));
            Result::Ok(())
        }
    });

    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
        }
    }
}

/// A fresh bootstrap of an MVCC database serves the last durable
/// generation base only. `create_db` must follow it with exactly one
/// non-long-polling incremental logical pull so `connect()` hands out a
/// current database instead of one missing every commit since the last
/// natural checkpoint.
#[test]
fn fresh_mvcc_bootstrap_catches_up_with_one_logical_pull() {
    let main_file = NamedTempFile::new().unwrap();
    let remote_file = NamedTempFile::new().unwrap();
    let main_path = main_file.path().to_str().unwrap().to_string();
    let remote_path = remote_file.path().to_str().unwrap().to_string();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let remote_db =
        turso_core::Database::open_file(io.clone(), &remote_path, Arc::new(SqliteDialect)).unwrap();
    let remote_conn = remote_db.connect().unwrap();
    remote_conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    remote_conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")
        .unwrap();
    let remote_wal_state = remote_conn.wal_state().unwrap();
    remote_conn
        .checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: Some(remote_wal_state.max_frame),
        })
        .unwrap();
    drop(remote_conn);
    drop(remote_db);
    let remote_bytes = std::fs::read(&remote_path).unwrap();

    let server_revision = "g1:o64";
    let bootstrap_response = encoded_page_stream_response(
        &remote_bytes,
        server_revision,
        PullUpdatesProtocol::MvccLogical,
    );
    // The catch-up pull gets an empty logical stream: already current.
    let catch_up_response = PullUpdatesRespProtoBody {
        server_revision: server_revision.to_string(),
        db_size: (remote_bytes.len() / super::PAGE_SIZE) as u64,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: None,
        protocol: PullUpdatesProtocol::MvccLogical as i32,
    }
    .encode_length_delimited_to_vec();
    let sync_io = Arc::new(QueuedSyncEngineIo {
        responses: Mutex::new(
            vec![bootstrap_response, catch_up_response]
                .into_iter()
                .collect(),
        ),
        requests: Mutex::new(Vec::new()),
    });
    let sync_engine_io = SyncEngineIoStats::new(sync_io.clone());

    let mut opts = default_test_opts();
    opts.remote_url = Some("https://example.com".to_string());
    opts.bootstrap_if_empty = true;
    opts.logical_mvcc_pull = None; // auto-detect

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let main_path = main_path.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let engine = DatabaseSyncEngine::create_db(
                &coro,
                io.clone(),
                sync_engine_io.clone(),
                &main_path,
                opts,
            )
            .await
            .map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test create_db failed: {error}"))
            })?;
            assert_eq!(
                engine.meta().remote_pull_protocol,
                RemotePullProtocol::MvccLogical
            );
            assert_eq!(
                engine.meta().synced_revision,
                Some(DatabasePullRevision::V1 {
                    revision: server_revision.to_string(),
                })
            );
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
        }
    }

    // Both queued responses were consumed: the bootstrap plus exactly one
    // catch-up pull, and the catch-up was a non-long-polling logical pull
    // from the bootstrap revision.
    assert!(sync_io.responses.lock().unwrap().is_empty());
    let requests = sync_io.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let catch_up =
        PullUpdatesReqProtoBody::decode(requests[1].2.as_ref().unwrap().as_slice()).unwrap();
    assert_eq!(
        catch_up.stream_kind,
        PullUpdatesStreamKind::MvccLogicalLog as i32
    );
    assert_eq!(catch_up.client_revision, server_revision);
    assert_eq!(catch_up.long_poll_timeout_ms, 0);
}

/// Forcing `logical_mvcc_pull: Some(true)` on a replica whose revision
/// came from the legacy wire protocol is a misconfiguration: the server
/// cannot resume an MVCC logical stream from a legacy revision.
#[test]
fn forced_logical_pull_on_legacy_revision_replica_is_rejected() {
    let temp_file = NamedTempFile::new().unwrap();
    let main_path = temp_file.path().to_str().unwrap().to_string();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let main_db =
        turso_core::Database::open_file(io.clone(), &main_path, Arc::new(SqliteDialect)).unwrap();

    let meta = DatabaseMetadata {
        version: DATABASE_METADATA_VERSION.to_string(),
        client_unique_id: "legacy-client".to_string(),
        synced_revision: Some(DatabasePullRevision::Legacy {
            generation: 3,
            synced_frame_no: Some(17),
        }),
        revert_since_wal_salt: None,
        revert_since_wal_watermark: 0,
        last_pull_unix_time: None,
        last_push_unix_time: None,
        last_pushed_pull_gen_hint: 0,
        last_pushed_change_id_hint: 0,
        last_pushed_replay_floor_change_id_hint: 0,
        partial_bootstrap_server_revision: None,
        fresh_bootstrap_pending_cdc_ack: false,
        remote_pull_protocol: RemotePullProtocol::Pages,
        logical_table_names_by_stable_id: Default::default(),
        saved_configuration: Some(DatabaseSavedConfiguration {
            remote_url: Some("https://example.com".to_string()),
            partial_sync_prefetch: None,
            partial_sync_segment_size: None,
        }),
    };
    std::fs::write(create_meta_path(&main_path), meta.dump().unwrap()).unwrap();

    let sync_stats = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
    let mut opts = default_test_opts();
    opts.remote_url = Some("https://example.com".to_string());
    opts.logical_mvcc_pull = Some(true);

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let main_db = main_db.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let engine = DatabaseSyncEngine::open_db(&coro, io, sync_stats, main_db, opts).await?;
            let err = engine.wait_changes_from_remote(&coro).await.unwrap_err();
            assert!(format!("{err:#}").contains("legacy protocol"), "{err:#}");
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
        }
    }
}

/// A database file that lived outside the sync engine has rows without
/// CDC provenance; replace-base replay would silently drop them, so the
/// MVCC conversion must refuse instead.
#[test]
fn first_contact_mvcc_conversion_rejects_local_data_without_cdc_history() {
    let main_file = NamedTempFile::new().unwrap();
    let main_path = main_file.path().to_str().unwrap().to_string();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let foreign_db =
        turso_core::Database::open_file(io.clone(), &main_path, Arc::new(SqliteDialect)).unwrap();
    let foreign_conn = foreign_db.connect().unwrap();
    foreign_conn
        .execute("CREATE TABLE orphaned(id INTEGER PRIMARY KEY)")
        .unwrap();
    foreign_conn
        .execute("INSERT INTO orphaned VALUES (1)")
        .unwrap();
    drop(foreign_conn);
    drop(foreign_db);

    let sync_engine_io = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
    let mut opts = default_test_opts();
    opts.remote_url = Some("https://example.com".to_string());
    opts.bootstrap_if_empty = false;
    opts.logical_mvcc_pull = None;

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let main_path = main_path.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let engine = DatabaseSyncEngine::create_db(
                &coro,
                io.clone(),
                sync_engine_io.clone(),
                &main_path,
                opts,
            )
            .await
            .map_err(|error| {
                Error::DatabaseSyncEngineError(format!("test create_db failed: {error}"))
            })?;
            let err = engine
                .ensure_local_mvcc_journal_mode(&coro)
                .await
                .unwrap_err();
            assert!(
                format!("{err:#}").contains("without CDC history"),
                "{err:#}"
            );
            Result::Ok(())
        }
    });

    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
        }
    }
}
/// Bootstraps a replica from a canned page stream, makes one local write,
/// then checkpoints twice: the first checkpoint folds the WAL frames and
/// truncates the WAL file to zero, the second one runs with an empty WAL.
fn bootstrap_and_checkpoint_twice(
    io: Arc<dyn turso_core::IO>,
    partial_sync_opts: Option<PartialSyncOpts>,
    db_name: &str,
) -> Result<()> {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let main_path = temp_dir.path().join(db_name).to_string_lossy().to_string();
    let remote_path = temp_dir
        .path()
        .join("remote.db")
        .to_string_lossy()
        .to_string();

    let platform_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let remote_db =
        turso_core::Database::open_file(platform_io.clone(), &remote_path, Arc::new(SqliteDialect))
            .unwrap();
    let remote_conn = remote_db.connect().unwrap();
    remote_conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    remote_conn
        .execute("INSERT INTO items VALUES (1, 'remote-a')")
        .unwrap();
    let remote_wal_state = remote_conn.wal_state().unwrap();
    remote_conn
        .checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: Some(remote_wal_state.max_frame),
        })
        .unwrap();
    drop(remote_conn);
    drop(remote_db);
    let remote_bytes = std::fs::read(&remote_path).unwrap();
    assert!(!remote_bytes.is_empty());

    let bootstrap_response =
        encoded_page_stream_response(&remote_bytes, "g1:o0", PullUpdatesProtocol::Pages);
    let sync_io = Arc::new(QueuedSyncEngineIo {
        responses: Mutex::new(vec![bootstrap_response].into_iter().collect()),
        requests: Mutex::new(Vec::new()),
    });
    let sync_engine_io = SyncEngineIoStats::new(sync_io);

    let mut opts = default_test_opts();
    opts.remote_url = Some("https://example.com".to_string());
    opts.bootstrap_if_empty = true;
    opts.logical_mvcc_pull = Some(false);
    opts.partial_sync_opts = partial_sync_opts;

    let main_wal_path = create_main_db_wal_path(&main_path);
    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let engine = DatabaseSyncEngine::create_db(
                &coro,
                io.clone(),
                sync_engine_io.clone(),
                &main_path,
                opts,
            )
            .await?;

            let conn = engine.connect_rw(&coro).await?;
            conn.execute("INSERT INTO items VALUES (2, 'local-b')")?;
            drop(conn);

            assert!(
                std::fs::metadata(&main_wal_path).unwrap().len() > 0,
                "local write must leave frames in the main WAL"
            );
            engine.checkpoint(&coro).await?;
            assert_eq!(
                std::fs::metadata(&main_wal_path).unwrap().len(),
                0,
                "TRUNCATE checkpoint must leave an empty main WAL file"
            );

            engine.checkpoint(&coro).await
        }
    });

    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    }
}

/// Reported bug: on a partial-sync database `checkpoint()` succeeds while
/// the main WAL holds frames, and then fails with
/// `I/O error (pread): unexpected end of file` on every checkpoint that
/// runs while the WAL is empty. Linux-only, because `SparseLinuxIo` is.
#[test]
#[cfg(target_os = "linux")]
fn partial_sync_checkpoint_succeeds_when_main_wal_is_empty() {
    let io: Arc<dyn turso_core::IO> = Arc::new(crate::sparse_io::SparseLinuxIo::new().unwrap());
    // A prefix covering the whole remote database leaves no holes, so the
    // failure does not depend on any page being unmaterialized.
    let result = bootstrap_and_checkpoint_twice(
        io,
        Some(PartialSyncOpts {
            bootstrap_strategy: Some(crate::types::PartialBootstrapStrategy::Prefix {
                length: usize::MAX / 2,
            }),
            segment_size: 128 * 1024,
            prefetch: false,
        }),
        "partial.db",
    );
    assert!(
        result.is_ok(),
        "checkpoint on an empty WAL must not fail: {:?}",
        result.err()
    );
}

/// Control for [`partial_sync_checkpoint_succeeds_when_main_wal_is_empty`]:
/// the very same sequence over a full-sync replica, which always worked
/// because `PlatformIO` reports a short read at end of file.
#[test]
fn full_sync_checkpoint_succeeds_when_main_wal_is_empty() {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let result = bootstrap_and_checkpoint_twice(io, None, "full.db");
    assert!(result.is_ok(), "{:?}", result.err());
}

/// `read_wal_salt` is the first read `checkpoint()` performs, and it is
/// written for a WAL file that may be shorter than one header: a short
/// read means "no salt". Every IO backend must report that short read
/// rather than an error, otherwise partial sync (the only user of
/// `SparseLinuxIo`) cannot checkpoint an empty WAL. Linux-only, because
/// `SparseLinuxIo` is.
#[test]
#[cfg(target_os = "linux")]
fn read_wal_salt_of_empty_wal_file_is_none_on_every_io() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let wal_path = temp_dir
        .path()
        .join("empty.db-wal")
        .to_string_lossy()
        .to_string();
    std::fs::write(&wal_path, []).unwrap();

    let platform_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let sparse_io: Arc<dyn turso_core::IO> =
        Arc::new(crate::sparse_io::SparseLinuxIo::new().unwrap());

    for (name, io) in [("PlatformIO", platform_io), ("SparseLinuxIo", sparse_io)] {
        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let wal_path = wal_path.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let wal = io.try_open(&wal_path)?.expect("wal file exists");
                super::read_wal_salt(&coro, &wal).await
            }
        });
        let result = loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result,
            }
        };
        assert!(matches!(result, Ok(None)), "{name}: {result:?}");
    }
}
