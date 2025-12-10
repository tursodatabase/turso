use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use bytes::BytesMut;
use prost::Message;
use roaring::RoaringBitmap;
use turso_core::{
    types::{Text, WalFrameInfo},
    Buffer, Completion, LimboError, OpenFlags, Value,
};

use crate::{
    database_replay_generator::DatabaseReplayGenerator,
    database_sync_engine::{DataStats, DatabaseSyncEngineOpts},
    database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo},
    database_tape::{
        run_stmt_expect_one_row, run_stmt_ignore_rows, DatabaseChangesIteratorMode,
        DatabaseChangesIteratorOpts, DatabaseReplaySessionOpts, DatabaseTape, DatabaseWalSession,
    },
    errors::Error,
    io_operations::IoOperations,
    server_proto::{
        self, Batch, BatchCond, BatchStep, BatchStreamReq, ExecuteStreamReq, PageData,
        PageUpdatesEncodingReq, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody, Stmt,
        StmtResult, StreamRequest,
    },
    types::{
        Coro, DatabasePullRevision, DatabaseRowTransformResult, DatabaseSyncEngineProtocolVersion,
        DatabaseTapeOperation, DatabaseTapeRowChange, DatabaseTapeRowChangeType, DbSyncInfo,
        DbSyncStatus, PartialBootstrapStrategy, PartialSyncOpts, SyncEngineIoResult,
    },
    wal_session::WalSession,
    Result,
};

pub const WAL_HEADER: usize = 32;
pub const WAL_FRAME_HEADER: usize = 24;
pub const PAGE_SIZE: usize = 4096;
pub const WAL_FRAME_SIZE: usize = WAL_FRAME_HEADER + PAGE_SIZE;

pub struct MutexSlot<T: Clone> {
    pub value: T,
    pub slot: Arc<Mutex<Option<T>>>,
}

impl<T: Clone> Drop for MutexSlot<T> {
    fn drop(&mut self) {
        self.slot.lock().unwrap().replace(self.value.clone());
    }
}

pub(crate) fn acquire_slot<T: Clone>(slot: &Arc<Mutex<Option<T>>>) -> Result<MutexSlot<T>> {
    let Some(value) = slot.lock().unwrap().take() else {
        return Err(Error::DatabaseSyncEngineError(
            "changes file already acquired by another operation".to_string(),
        ));
    };
    Ok(MutexSlot {
        value,
        slot: slot.clone(),
    })
}

pub struct SyncEngineIoStats<IO: SyncEngineIo> {
    pub io: Arc<IO>,
    pub network_stats: Arc<DataStats>,
}

impl<IO: SyncEngineIo> SyncEngineIoStats<IO> {
    pub fn new(io: Arc<IO>) -> Self {
        Self {
            io,
            network_stats: Arc::new(DataStats::new()),
        }
    }
}

impl<IO: SyncEngineIo> Clone for SyncEngineIoStats<IO> {
    fn clone(&self) -> Self {
        Self {
            io: self.io.clone(),
            network_stats: self.network_stats.clone(),
        }
    }
}

impl<IO: SyncEngineIo> Deref for SyncEngineIoStats<IO> {
    type Target = IO;

    fn deref(&self) -> &Self::Target {
        &self.io
    }
}

enum WalHttpPullResult<C: DataCompletion<u8>> {
    Frames(C),
    NeedCheckpoint(DbSyncStatus),
}

pub enum WalPushResult {
    Ok { baton: Option<String> },
    NeedCheckpoint,
}

pub fn connect_untracked(tape: &DatabaseTape) -> Result<Arc<turso_core::Connection>> {
    let conn = tape.connect_untracked()?;
    conn.wal_auto_checkpoint_disable();
    Ok(conn)
}

/// Bootstrap multiple DB files from latest generation from remote
pub async fn db_bootstrap<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    db: Arc<dyn turso_core::File>,
) -> Result<DbSyncInfo> {
    tracing::info!("db_bootstrap");
    let start_time = std::time::Instant::now();
    let db_info = db_info_http(coro, client).await?;
    tracing::info!("db_bootstrap: fetched db_info={db_info:?}");
    let content = db_bootstrap_http(coro, client, db_info.current_generation).await?;
    let mut pos = 0;
    loop {
        while let Some(chunk) = content.poll_data()? {
            client.network_stats.read(chunk.data().len());
            let chunk = chunk.data();
            let content_len = chunk.len();

            // todo(sivukhin): optimize allocations here
            #[allow(clippy::arc_with_non_send_sync)]
            let buffer = Arc::new(Buffer::new_temporary(chunk.len()));
            buffer.as_mut_slice().copy_from_slice(chunk);
            let c = Completion::new_write(move |result| {
                // todo(sivukhin): we need to error out in case of partial read
                let Ok(size) = result else {
                    return;
                };
                assert!(size as usize == content_len);
            });
            let c = db.pwrite(pos, buffer.clone(), c)?;
            while !c.succeeded() {
                coro.yield_(SyncEngineIoResult::IO).await?;
            }
            pos += content_len as u64;
        }
        if content.is_done()? {
            break;
        }
        coro.yield_(SyncEngineIoResult::IO).await?;
    }

    // sync files in the end
    let c = Completion::new_sync(move |_| {
        // todo(sivukhin): we need to error out in case of failed sync
    });
    let c = db.sync(c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }

    let elapsed = std::time::Instant::now().duration_since(start_time);
    tracing::info!("db_bootstrap: finished: bytes={pos}, elapsed={:?}", elapsed);

    Ok(db_info)
}

pub async fn wal_apply_from_file<Ctx>(
    coro: &Coro<Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    session: &mut DatabaseWalSession,
) -> Result<u32> {
    let size = frames_file.size()?;
    assert!(size % WAL_FRAME_SIZE as u64 == 0);
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));
    tracing::info!("wal_apply_from_file: size={}", size);
    let mut db_size = 0;
    for offset in (0..size).step_by(WAL_FRAME_SIZE) {
        let c = Completion::new_read(buffer.clone(), move |result| {
            let Ok((_, size)) = result else {
                return;
            };
            // todo(sivukhin): we need to error out in case of partial read
            assert!(size as usize == WAL_FRAME_SIZE);
        });
        let c = frames_file.pread(offset, c)?;
        while !c.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
        let info = WalFrameInfo::from_frame_header(buffer.as_slice());
        tracing::info!("got frame: {:?}", info);
        db_size = info.db_size;
        session.append_page(info.page_no, &buffer.as_slice()[WAL_FRAME_HEADER..])?;
    }
    assert!(db_size > 0);
    Ok(db_size)
}

pub async fn wal_pull_to_file<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    frames_file: &Arc<dyn turso_core::File>,
    revision: &Option<DatabasePullRevision>,
    wal_pull_batch_size: u64,
    long_poll_timeout: Option<std::time::Duration>,
) -> Result<DatabasePullRevision> {
    // truncate file before pulling new data
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = frames_file.truncate(0, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    match revision {
        Some(DatabasePullRevision::Legacy {
            generation,
            synced_frame_no,
        }) => {
            let start_frame = synced_frame_no.unwrap_or(0) + 1;
            wal_pull_to_file_legacy(
                coro,
                client,
                frames_file,
                *generation,
                start_frame,
                wal_pull_batch_size,
            )
            .await
        }
        Some(DatabasePullRevision::V1 { revision }) => {
            wal_pull_to_file_v1(coro, client, frames_file, revision, long_poll_timeout).await
        }
        None => wal_pull_to_file_v1(coro, client, frames_file, "", long_poll_timeout).await,
    }
}

/// Pull updates from remote to the separate file
pub async fn wal_pull_to_file_v1<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    frames_file: &Arc<dyn turso_core::File>,
    revision: &str,
    long_poll_timeout: Option<std::time::Duration>,
) -> Result<DatabasePullRevision> {
    tracing::info!("wal_pull: revision={revision}");
    let mut bytes = BytesMut::new();

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        server_revision: String::new(),
        client_revision: revision.to_string(),
        long_poll_timeout_ms: long_poll_timeout.map(|x| x.as_millis() as u32).unwrap_or(0),
        server_pages_selector: BytesMut::new().into(),
        server_query_selector: String::new(),
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    client.network_stats.write(request.len());
    let completion = client.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        coro,
        &completion,
        &client.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::info!("wal_pull_to_file: got header={:?}", header);

    let mut offset = 0;
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));

    let mut page_data_opt =
        wait_proto_message::<Ctx, PageData>(coro, &completion, &client.network_stats, &mut bytes)
            .await?;
    while let Some(page_data) = page_data_opt.take() {
        let page_id = page_data.page_id;
        tracing::info!("received page {}", page_id);
        let page = decode_page(&header, page_data)?;
        if page.len() != PAGE_SIZE {
            return Err(Error::DatabaseSyncEngineError(format!(
                "page has unexpected size: {} != {}",
                page.len(),
                PAGE_SIZE
            )));
        }
        buffer.as_mut_slice()[WAL_FRAME_HEADER..].copy_from_slice(&page);
        page_data_opt =
            wait_proto_message(coro, &completion, &client.network_stats, &mut bytes).await?;
        let mut frame_info = WalFrameInfo {
            db_size: 0,
            page_no: page_id as u32 + 1,
        };
        if page_data_opt.is_none() {
            frame_info.db_size = header.db_size as u32;
        }
        tracing::info!("page_data_opt: {}", page_data_opt.is_some());
        frame_info.put_to_frame_header(buffer.as_mut_slice());

        let c = Completion::new_write(move |result| {
            // todo(sivukhin): we need to error out in case of partial read
            let Ok(size) = result else {
                return;
            };
            assert!(size as usize == WAL_FRAME_SIZE);
        });

        let c = frames_file.pwrite(offset, buffer.clone(), c)?;
        while !c.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
        offset += WAL_FRAME_SIZE as u64;
    }

    let c = Completion::new_sync(move |_| {
        // todo(sivukhin): we need to error out in case of failed sync
    });
    let c = frames_file.sync(c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }

    Ok(DatabasePullRevision::V1 {
        revision: header.server_revision,
    })
}

/// Pull pages from remote
pub async fn pull_pages_v1<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    server_revision: &str,
    pages: &[u32],
) -> Result<Vec<(u64, Vec<u8>)>> {
    tracing::info!("pull_pages_v1: revision={server_revision}, pages={pages:?}");

    if pages.is_empty() {
        return Ok(Vec::new());
    }

    let mut bytes = BytesMut::new();

    let mut bitmap = RoaringBitmap::new();
    bitmap.extend(pages);

    let mut bitmap_bytes = Vec::with_capacity(bitmap.serialized_size());
    bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
        Error::DatabaseSyncEngineError(format!("unable to serialize pull page request: {e}"))
    })?;

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        server_revision: server_revision.to_string(),
        client_revision: String::new(),
        long_poll_timeout_ms: 0,
        server_pages_selector: bitmap_bytes.into(),
        server_query_selector: String::new(),
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    client.network_stats.write(request.len());
    let completion = client.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        coro,
        &completion,
        &client.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::info!("pull_pages_v1: got header={:?}", header);

    let mut pages = Vec::with_capacity(pages.len());

    let mut page_data_opt =
        wait_proto_message::<Ctx, PageData>(coro, &completion, &client.network_stats, &mut bytes)
            .await?;
    while let Some(page_data) = page_data_opt.take() {
        let page_id = page_data.page_id;
        tracing::info!("received page {}", page_id);
        let page = decode_page(&header, page_data)?;
        if page.len() != PAGE_SIZE {
            return Err(Error::DatabaseSyncEngineError(format!(
                "page has unexpected size: {} != {}",
                page.len(),
                PAGE_SIZE
            )));
        }
        pages.push((page_id, page));
        page_data_opt =
            wait_proto_message(coro, &completion, &client.network_stats, &mut bytes).await?;
        tracing::info!("page_data_opt: {}", page_data_opt.is_some());
    }

    Ok(pages)
}

/// Pull updates from remote to the separate file
pub async fn wal_pull_to_file_legacy<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    frames_file: &Arc<dyn turso_core::File>,
    mut generation: u64,
    mut start_frame: u64,
    wal_pull_batch_size: u64,
) -> Result<DatabasePullRevision> {
    tracing::info!(
        "wal_pull: generation={generation}, start_frame={start_frame}, wal_pull_batch_size={wal_pull_batch_size}"
    );

    // todo(sivukhin): optimize allocation by using buffer pool in the DatabaseSyncOperations
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));
    let mut buffer_len = 0;
    let mut last_offset = 0;
    let mut committed_len = 0;
    let revision = loop {
        let end_frame = start_frame + wal_pull_batch_size;
        let result = wal_pull_http(coro, client, generation, start_frame, end_frame).await?;
        let data = match result {
            WalHttpPullResult::NeedCheckpoint(status) => {
                assert!(status.status == "checkpoint_needed");
                tracing::info!("wal_pull: need checkpoint: status={status:?}");
                if status.generation == generation && status.max_frame_no < start_frame {
                    tracing::info!("wal_pull: end of history: status={:?}", status);
                    break DatabasePullRevision::Legacy {
                        generation: status.generation,
                        synced_frame_no: Some(status.max_frame_no),
                    };
                }
                generation += 1;
                start_frame = 1;
                continue;
            }
            WalHttpPullResult::Frames(content) => content,
        };
        loop {
            while let Some(chunk) = data.poll_data()? {
                client.network_stats.read(chunk.data().len());
                let mut chunk = chunk.data();

                while !chunk.is_empty() {
                    let to_fill = (WAL_FRAME_SIZE - buffer_len).min(chunk.len());
                    buffer.as_mut_slice()[buffer_len..buffer_len + to_fill]
                        .copy_from_slice(&chunk[0..to_fill]);
                    buffer_len += to_fill;
                    chunk = &chunk[to_fill..];

                    if buffer_len < WAL_FRAME_SIZE {
                        continue;
                    }
                    let c = Completion::new_write(move |result| {
                        // todo(sivukhin): we need to error out in case of partial read
                        let Ok(size) = result else {
                            return;
                        };
                        assert!(size as usize == WAL_FRAME_SIZE);
                    });
                    let c = frames_file.pwrite(last_offset, buffer.clone(), c)?;
                    while !c.succeeded() {
                        coro.yield_(SyncEngineIoResult::IO).await?;
                    }

                    last_offset += WAL_FRAME_SIZE as u64;
                    buffer_len = 0;
                    start_frame += 1;

                    let info = WalFrameInfo::from_frame_header(buffer.as_slice());
                    if info.is_commit_frame() {
                        committed_len = last_offset;
                    }
                }
            }
            if data.is_done()? {
                break;
            }
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
        if start_frame < end_frame {
            // chunk which was sent from the server has ended early - so there is nothing left on server-side for pull
            break DatabasePullRevision::Legacy {
                generation,
                synced_frame_no: Some(start_frame - 1),
            };
        }
        if buffer_len != 0 {
            return Err(Error::DatabaseSyncEngineError(format!(
                "wal_pull: response has unexpected trailing data: buffer_len={buffer_len}"
            )));
        }
    };

    tracing::info!(
        "wal_pull: generation={generation}, frame={start_frame}, last_offset={last_offset}, commited_len={committed_len}"
    );
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = frames_file.truncate(committed_len, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }

    let c = Completion::new_sync(move |_| {
        // todo(sivukhin): we need to error out in case of failed sync
    });
    let c = frames_file.sync(c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }

    Ok(revision)
}

/// Push frame range [start_frame..end_frame) to the remote
/// Returns baton for WAL remote-session in case of success
/// Returns [Error::DatabaseSyncEngineConflict] in case of frame conflict at remote side
///
/// Guarantees:
/// 1. If there is a single client which calls wal_push, then this operation is idempotent for fixed generation
///    and can be called multiple times with same frame range
pub async fn wal_push<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    wal_session: &mut WalSession,
    baton: Option<String>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
) -> Result<WalPushResult> {
    assert!(wal_session.in_txn());
    tracing::info!("wal_push: baton={baton:?}, generation={generation}, start_frame={start_frame}, end_frame={end_frame}");

    if start_frame == end_frame {
        return Ok(WalPushResult::Ok { baton: None });
    }

    let mut frames_data = Vec::with_capacity((end_frame - start_frame) as usize * WAL_FRAME_SIZE);
    let mut buffer = [0u8; WAL_FRAME_SIZE];
    for frame_no in start_frame..end_frame {
        let frame_info = wal_session.read_at(frame_no, &mut buffer)?;
        tracing::trace!(
            "wal_push: collect frame {} ({:?}) for push",
            frame_no,
            frame_info
        );
        frames_data.extend_from_slice(&buffer);
    }

    let status = wal_push_http(
        coro,
        client,
        None,
        generation,
        start_frame,
        end_frame,
        frames_data,
    )
    .await?;
    if status.status == "ok" {
        Ok(WalPushResult::Ok {
            baton: status.baton,
        })
    } else if status.status == "checkpoint_needed" {
        Ok(WalPushResult::NeedCheckpoint)
    } else if status.status == "conflict" {
        Err(Error::DatabaseSyncEngineConflict(format!(
            "wal_push conflict: {status:?}"
        )))
    } else {
        Err(Error::DatabaseSyncEngineError(format!(
            "wal_push unexpected status: {status:?}"
        )))
    }
}

pub const TURSO_SYNC_TABLE_NAME: &str = "turso_sync_last_change_id";
pub const TURSO_SYNC_CREATE_TABLE: &str =
    "CREATE TABLE IF NOT EXISTS turso_sync_last_change_id (client_id TEXT PRIMARY KEY, pull_gen INTEGER, change_id INTEGER)";
pub const TURSO_SYNC_INSERT_LAST_CHANGE_ID: &str =
    "INSERT INTO turso_sync_last_change_id(client_id, pull_gen, change_id) VALUES (?, ?, ?)";
pub const TURSO_SYNC_UPSERT_LAST_CHANGE_ID: &str =
    "INSERT INTO turso_sync_last_change_id(client_id, pull_gen, change_id) VALUES (?, ?, ?) ON CONFLICT(client_id) DO UPDATE SET pull_gen=excluded.pull_gen, change_id=excluded.change_id";
pub const TURSO_SYNC_UPDATE_LAST_CHANGE_ID: &str =
    "UPDATE turso_sync_last_change_id SET pull_gen = ?, change_id = ? WHERE client_id = ?";
const TURSO_SYNC_SELECT_LAST_CHANGE_ID: &str =
    "SELECT pull_gen, change_id FROM turso_sync_last_change_id WHERE client_id = ?";

fn convert_to_args(values: Vec<turso_core::Value>) -> Vec<server_proto::Value> {
    values
        .into_iter()
        .map(|value| match value {
            Value::Null => server_proto::Value::Null,
            Value::Integer(value) => server_proto::Value::Integer { value },
            Value::Float(value) => server_proto::Value::Float { value },
            Value::Text(value) => server_proto::Value::Text {
                value: value.as_str().to_string(),
            },
            Value::Blob(value) => server_proto::Value::Blob {
                value: value.into(),
            },
        })
        .collect()
}

pub async fn has_table<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    table_name: &str,
) -> Result<bool> {
    let mut stmt =
        conn.prepare("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?")?;
    stmt.bind_at(
        1.try_into().unwrap(),
        Value::Text(Text::new(table_name.to_string())),
    );

    let count = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0]
            .as_int()
            .ok_or_else(|| Error::DatabaseSyncEngineError("unexpected column type".to_string()))?,
        _ => panic!("expected single row"),
    };
    Ok(count > 0)
}

pub async fn count_local_changes<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    change_id: i64,
) -> Result<i64> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM turso_cdc WHERE change_id > ?")?;
    stmt.bind_at(1.try_into().unwrap(), Value::Integer(change_id));

    let count = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0]
            .as_int()
            .ok_or_else(|| Error::DatabaseSyncEngineError("unexpected column type".to_string()))?,
        _ => panic!("expected single row"),
    };
    Ok(count)
}

pub async fn update_last_change_id<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
    pull_gen: i64,
    change_id: i64,
) -> Result<()> {
    tracing::info!(
        "update_last_change_id(client_id={client_id}): pull_gen={pull_gen}, change_id={change_id}"
    );
    conn.execute(TURSO_SYNC_CREATE_TABLE)?;
    tracing::info!("update_last_change_id(client_id={client_id}): initialized table");
    let mut select_stmt = conn.prepare(TURSO_SYNC_SELECT_LAST_CHANGE_ID)?;
    select_stmt.bind_at(
        1.try_into().unwrap(),
        turso_core::Value::Text(turso_core::types::Text::new(client_id.to_string())),
    );
    let row = run_stmt_expect_one_row(coro, &mut select_stmt).await?;
    tracing::info!("update_last_change_id(client_id={client_id}): selected client row if any");

    if row.is_some() {
        let mut update_stmt = conn.prepare(TURSO_SYNC_UPDATE_LAST_CHANGE_ID)?;
        update_stmt.bind_at(1.try_into().unwrap(), turso_core::Value::Integer(pull_gen));
        update_stmt.bind_at(2.try_into().unwrap(), turso_core::Value::Integer(change_id));
        update_stmt.bind_at(
            3.try_into().unwrap(),
            turso_core::Value::Text(turso_core::types::Text::new(client_id.to_string())),
        );
        run_stmt_ignore_rows(coro, &mut update_stmt).await?;
        tracing::info!("update_last_change_id(client_id={client_id}): updated row for the client");
    } else {
        let mut update_stmt = conn.prepare(TURSO_SYNC_INSERT_LAST_CHANGE_ID)?;
        update_stmt.bind_at(
            1.try_into().unwrap(),
            turso_core::Value::Text(turso_core::types::Text::new(client_id.to_string())),
        );
        update_stmt.bind_at(2.try_into().unwrap(), turso_core::Value::Integer(pull_gen));
        update_stmt.bind_at(3.try_into().unwrap(), turso_core::Value::Integer(change_id));
        run_stmt_ignore_rows(coro, &mut update_stmt).await?;
        tracing::info!(
            "update_last_change_id(client_id={client_id}): inserted new row for the client"
        );
    }

    Ok(())
}

pub async fn read_last_change_id<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
) -> Result<(i64, Option<i64>)> {
    tracing::info!("read_last_change_id: client_id={client_id}");

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let mut select_last_change_id_stmt = match conn.prepare(TURSO_SYNC_SELECT_LAST_CHANGE_ID) {
        Ok(stmt) => stmt,
        Err(LimboError::ParseError(..)) => return Ok((0, None)),
        Err(err) => return Err(err.into()),
    };

    select_last_change_id_stmt.bind_at(
        1.try_into().unwrap(),
        Value::Text(Text::new(client_id.to_string())),
    );

    match run_stmt_expect_one_row(coro, &mut select_last_change_id_stmt).await? {
        Some(row) => {
            let pull_gen = row[0].as_int().ok_or_else(|| {
                Error::DatabaseSyncEngineError("unexpected source pull_gen type".to_string())
            })?;
            let change_id = row[1].as_int().ok_or_else(|| {
                Error::DatabaseSyncEngineError("unexpected source change_id type".to_string())
            })?;
            Ok((pull_gen, Some(change_id)))
        }
        None => {
            tracing::info!("read_last_change_id: client_id={client_id}, turso_sync_last_change_id client id is not found");
            Ok((0, None))
        }
    }
}

pub async fn fetch_last_change_id<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    source_conn: &Arc<turso_core::Connection>,
    client_id: &str,
) -> Result<(i64, Option<i64>)> {
    tracing::info!("fetch_last_change_id: client_id={client_id}");

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let (source_pull_gen, _) = read_last_change_id(coro, source_conn, client_id).await?;
    tracing::info!(
        "fetch_last_change_id: client_id={client_id}, source_pull_gen={source_pull_gen}"
    );

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let init_hrana_request = server_proto::PipelineReqBody {
        baton: None,
        requests: vec![
            // read pull_gen, change_id values for current client if they were set before
            StreamRequest::Execute(ExecuteStreamReq {
                stmt: Stmt {
                    sql: Some(TURSO_SYNC_SELECT_LAST_CHANGE_ID.to_string()),
                    sql_id: None,
                    args: vec![server_proto::Value::Text {
                        value: client_id.to_string(),
                    }],
                    named_args: Vec::new(),
                    want_rows: Some(true),
                    replication_index: None,
                },
            }),
        ]
        .into(),
    };

    let response = match sql_execute_http(coro, client, init_hrana_request).await {
        Ok(response) => response,
        Err(Error::DatabaseSyncEngineError(err)) if err.contains("no such table") => {
            return Ok((source_pull_gen, None));
        }
        Err(err) => return Err(err),
    };
    assert!(response.len() == 1);
    let last_change_id_response = &response[0];
    tracing::debug!("fetch_last_change_id: response={:?}", response);
    assert!(last_change_id_response.rows.len() <= 1);
    if last_change_id_response.rows.is_empty() {
        return Ok((source_pull_gen, None));
    }
    let row = &last_change_id_response.rows[0].values;
    let server_proto::Value::Integer {
        value: target_pull_gen,
    } = row[0]
    else {
        return Err(Error::DatabaseSyncEngineError(
            "unexpected target pull_gen type".to_string(),
        ));
    };
    let server_proto::Value::Integer {
        value: target_change_id,
    } = row[1]
    else {
        return Err(Error::DatabaseSyncEngineError(
            "unexpected target change_id type".to_string(),
        ));
    };
    tracing::debug!(
        "fetch_last_change_id: client_id={client_id}, target_pull_gen={target_pull_gen}, target_change_id={target_change_id}"
    );
    if target_pull_gen > source_pull_gen {
        return Err(Error::DatabaseSyncEngineError(format!("protocol error: target_pull_gen > source_pull_gen: {target_pull_gen} > {source_pull_gen}")));
    }
    let last_change_id = if target_pull_gen == source_pull_gen {
        Some(target_change_id)
    } else {
        Some(0)
    };
    Ok((source_pull_gen, last_change_id))
}

pub async fn push_logical_changes<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    source: &DatabaseTape,
    client_id: &str,
    opts: &DatabaseSyncEngineOpts,
) -> Result<(i64, i64)> {
    tracing::info!("push_logical_changes: client_id={client_id}");
    let source_conn = connect_untracked(source)?;

    let (source_pull_gen, mut last_change_id) =
        fetch_last_change_id(coro, client, &source_conn, client_id).await?;

    tracing::debug!("push_logical_changes: last_change_id={:?}", last_change_id);
    let replay_opts = DatabaseReplaySessionOpts {
        use_implicit_rowid: false,
    };

    let generator = DatabaseReplayGenerator::new(source_conn, replay_opts);

    let iterate_opts = DatabaseChangesIteratorOpts {
        first_change_id: last_change_id.map(|x| x + 1),
        mode: DatabaseChangesIteratorMode::Apply,
        ignore_schema_changes: false,
        ..Default::default()
    };
    let step = |query, args| BatchStep {
        stmt: Stmt {
            sql: Some(query),
            sql_id: None,
            args,
            named_args: Vec::new(),
            want_rows: Some(false),
            replication_index: None,
        },
        condition: Some(BatchCond::Not {
            cond: Box::new(BatchCond::IsAutocommit {}),
        }),
    };
    let mut sql_over_http_requests = vec![
        BatchStep {
            stmt: Stmt {
                sql: Some("BEGIN IMMEDIATE".to_string()),
                sql_id: None,
                args: Vec::new(),
                named_args: Vec::new(),
                want_rows: Some(false),
                replication_index: None,
            },
            condition: None,
        },
        step(TURSO_SYNC_CREATE_TABLE.to_string(), Vec::new()),
    ];
    let mut rows_changed = 0;
    let mut changes = source.iterate_changes(iterate_opts)?;
    let mut local_changes = Vec::new();
    while let Some(operation) = changes.next(coro).await? {
        match operation {
            DatabaseTapeOperation::StmtReplay(_) => {
                panic!("changes iterator must not use StmtReplay option")
            }
            DatabaseTapeOperation::RowChange(change) => {
                if change.table_name == TURSO_SYNC_TABLE_NAME {
                    continue;
                }
                let ignore = &opts.tables_ignore;
                if ignore.iter().any(|x| &change.table_name == x) {
                    continue;
                }
                local_changes.push(change);
            }
            DatabaseTapeOperation::Commit => continue,
        }
    }

    let mut transformed = if opts.use_transform {
        Some(apply_transformation(coro, client, &local_changes, &generator).await?)
    } else {
        None
    };

    tracing::info!("local_changes: {:?}", local_changes);

    for (i, change) in local_changes.into_iter().enumerate() {
        let change_id = change.change_id;
        let operation = if let Some(transformed) = &mut transformed {
            match std::mem::replace(&mut transformed[i], DatabaseRowTransformResult::Skip) {
                DatabaseRowTransformResult::Keep => DatabaseTapeOperation::RowChange(change),
                DatabaseRowTransformResult::Skip => continue,
                DatabaseRowTransformResult::Rewrite(replay) => {
                    DatabaseTapeOperation::StmtReplay(replay)
                }
            }
        } else {
            DatabaseTapeOperation::RowChange(change)
        };
        tracing::info!(
            "change_id: {}, last_change_id: {:?}",
            change_id,
            last_change_id
        );
        assert!(
            last_change_id.is_none() || last_change_id.unwrap() < change_id,
            "change id must be strictly increasing: last_change_id={last_change_id:?}, change.change_id={change_id}"
        );
        rows_changed += 1;
        // we give user full control over CDC table - so let's not emit assert here for now
        if last_change_id.is_some() && last_change_id.unwrap() + 1 != change_id {
            tracing::warn!(
                "out of order change sequence: {} -> {}",
                last_change_id.unwrap(),
                change_id
            );
        }
        last_change_id = Some(change_id);
        match operation {
            DatabaseTapeOperation::Commit => {
                panic!("Commit operation must not be emited at this stage")
            }
            DatabaseTapeOperation::StmtReplay(replay) => {
                sql_over_http_requests.push(step(replay.sql, convert_to_args(replay.values)))
            }
            DatabaseTapeOperation::RowChange(change) => {
                let replay_info = generator.replay_info(coro, &change).await?;
                match change.change {
                    DatabaseTapeRowChangeType::Delete { before } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            before,
                            None,
                        );
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)))
                    }
                    DatabaseTapeRowChangeType::Insert { after } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            after,
                            None,
                        );
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)));
                    }
                    DatabaseTapeRowChangeType::Update {
                        after,
                        updates: Some(updates),
                        ..
                    } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            after,
                            Some(updates),
                        );
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)));
                    }
                    DatabaseTapeRowChangeType::Update {
                        after,
                        updates: None,
                        ..
                    } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            after,
                            None,
                        );
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)));
                    }
                }
            }
        }
    }

    if rows_changed > 0 {
        tracing::info!("prepare update stmt for turso_sync_last_change_id table with client_id={} and last_change_id={:?}", client_id, last_change_id);
        // update turso_sync_last_change_id table with new value before commit
        let next_change_id = last_change_id.unwrap_or(0);
        tracing::info!("push_logical_changes: client_id={client_id}, set pull_gen={source_pull_gen}, change_id={next_change_id}, rows_changed={rows_changed}");
        sql_over_http_requests.push(step(
            TURSO_SYNC_UPSERT_LAST_CHANGE_ID.to_string(),
            vec![
                server_proto::Value::Text {
                    value: client_id.to_string(),
                },
                server_proto::Value::Integer {
                    value: source_pull_gen,
                },
                server_proto::Value::Integer {
                    value: next_change_id,
                },
            ],
        ));
    }
    sql_over_http_requests.push(step("COMMIT".to_string(), Vec::new()));

    tracing::debug!("hrana request: {:?}", sql_over_http_requests);
    let replay_hrana_request = server_proto::PipelineReqBody {
        baton: None,
        requests: vec![StreamRequest::Batch(BatchStreamReq {
            batch: Batch {
                steps: sql_over_http_requests.into(),
                replication_index: None,
            },
        })]
        .into(),
    };

    let _ = sql_execute_http(coro, client, replay_hrana_request).await?;
    tracing::info!("push_logical_changes: rows_changed={:?}", rows_changed);
    Ok((source_pull_gen, last_change_id.unwrap_or(0)))
}

pub async fn apply_transformation<C: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<C>,
    changes: &Vec<DatabaseTapeRowChange>,
    generator: &DatabaseReplayGenerator,
) -> Result<Vec<DatabaseRowTransformResult>> {
    let mut mutations = Vec::new();
    for change in changes {
        let replay_info = generator.replay_info(coro, change).await?;
        mutations.push(generator.create_mutation(&replay_info, change)?);
    }
    let completion = client.transform(mutations)?;
    let transformed = wait_all_results(coro, &completion, None).await?;
    if transformed.len() != changes.len() {
        return Err(Error::DatabaseSyncEngineError(format!(
            "unexpected result from custom transformation: mismatch in shapes: {} != {}",
            transformed.len(),
            changes.len()
        )));
    }
    tracing::info!("apply_transformation: got {:?}", transformed);
    Ok(transformed)
}

pub async fn read_wal_salt<Ctx>(
    coro: &Coro<Ctx>,
    wal: &Arc<dyn turso_core::File>,
) -> Result<Option<Vec<u32>>> {
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_HEADER));
    let c = Completion::new_read(buffer.clone(), |result| {
        let Ok((buffer, len)) = result else {
            return;
        };
        if (len as usize) < WAL_HEADER {
            buffer.as_mut_slice().fill(0);
        }
    });
    let c = wal.pread(0, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    if buffer.as_mut_slice() == [0u8; WAL_HEADER] {
        return Ok(None);
    }
    let salt1 = u32::from_be_bytes(buffer.as_slice()[16..20].try_into().unwrap());
    let salt2 = u32::from_be_bytes(buffer.as_slice()[20..24].try_into().unwrap());
    Ok(Some(vec![salt1, salt2]))
}

pub async fn checkpoint_wal_file<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
) -> Result<()> {
    let mut checkpoint_stmt = conn.prepare("PRAGMA wal_checkpoint(TRUNCATE)")?;
    loop {
        match checkpoint_stmt.step()? {
            turso_core::StepResult::IO => coro.yield_(SyncEngineIoResult::IO).await?,
            turso_core::StepResult::Done => break,
            turso_core::StepResult::Row => continue,
            r => {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "unexepcted checkpoint result: {r:?}"
                )))
            }
        }
    }
    Ok(())
}

pub async fn bootstrap_db_file<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    io: &Arc<dyn turso_core::IO>,
    main_db_path: &str,
    protocol: DatabaseSyncEngineProtocolVersion,
    partial_sync: Option<PartialSyncOpts>,
) -> Result<DatabasePullRevision> {
    match protocol {
        DatabaseSyncEngineProtocolVersion::Legacy => {
            if partial_sync.is_some() {
                return Err(Error::DatabaseSyncEngineError(
                    "can't bootstrap prefix of database with legacy protocol".to_string(),
                ));
            }
            bootstrap_db_file_legacy(coro, client, io, main_db_path).await
        }
        DatabaseSyncEngineProtocolVersion::V1 => {
            bootstrap_db_file_v1(coro, client, io, main_db_path, partial_sync).await
        }
    }
}

pub async fn bootstrap_db_file_v1<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    io: &Arc<dyn turso_core::IO>,
    main_db_path: &str,
    partial_sync: Option<PartialSyncOpts>,
) -> Result<DatabasePullRevision> {
    let server_pages_selector = if let Some(PartialSyncOpts {
        bootstrap_strategy: PartialBootstrapStrategy::Prefix { length },
        ..
    }) = &partial_sync
    {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..(*length / PAGE_SIZE) as u32);
        let mut bitmap_bytes = Vec::with_capacity(bitmap.serialized_size());
        bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
            Error::DatabaseSyncEngineError(format!("unable to serialize bootstrap request: {e}"))
        })?;
        bitmap_bytes
    } else {
        Vec::new()
    };
    let server_query_selector = if let Some(PartialSyncOpts {
        bootstrap_strategy: PartialBootstrapStrategy::Query { query },
        ..
    }) = partial_sync
    {
        query
    } else {
        String::new()
    };

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        server_revision: String::new(),
        client_revision: String::new(),
        long_poll_timeout_ms: 0,
        server_pages_selector: server_pages_selector.into(),
        server_query_selector,
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    client.network_stats.write(request.len());
    let completion = client.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let mut bytes = BytesMut::new();
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        coro,
        &completion,
        &client.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::info!(
        "bootstrap_db_file(path={}): got header={:?}",
        main_db_path,
        header
    );
    let file = io.open_file(main_db_path, OpenFlags::Create, false)?;
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = file.truncate(header.db_size * PAGE_SIZE as u64, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }

    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(PAGE_SIZE));
    while let Some(page_data) =
        wait_proto_message::<Ctx, PageData>(coro, &completion, &client.network_stats, &mut bytes)
            .await?
    {
        tracing::info!(
            "bootstrap_db_file: received page page_id={}",
            page_data.page_id
        );
        let offset = page_data.page_id * PAGE_SIZE as u64;
        let page = decode_page(&header, page_data)?;
        if page.len() != PAGE_SIZE {
            return Err(Error::DatabaseSyncEngineError(format!(
                "page has unexpected size: {} != {}",
                page.len(),
                PAGE_SIZE
            )));
        }
        buffer.as_mut_slice().copy_from_slice(&page);
        let c = Completion::new_write(move |result| {
            // todo(sivukhin): we need to error out in case of partial read
            let Ok(size) = result else {
                return;
            };
            assert!(size as usize == PAGE_SIZE);
        });
        let c = file.pwrite(offset, buffer.clone(), c)?;
        while !c.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
    }
    Ok(DatabasePullRevision::V1 {
        revision: header.server_revision,
    })
}

fn decode_page(header: &PullUpdatesRespProtoBody, page_data: PageData) -> Result<Vec<u8>> {
    if header.raw_encoding.is_some() && header.zstd_encoding.is_some() {
        return Err(Error::DatabaseSyncEngineError(
            "both of raw_encoding and zstd_encoding are set".to_string(),
        ));
    }
    if header.raw_encoding.is_none() && header.zstd_encoding.is_none() {
        return Err(Error::DatabaseSyncEngineError(
            "none from raw_encoding and zstd_encoding are set".to_string(),
        ));
    }

    if header.raw_encoding.is_some() {
        return Ok(page_data.encoded_page.to_vec());
    }
    Err(Error::DatabaseSyncEngineError(
        "zstd encoding is not supported".to_string(),
    ))
}

pub async fn bootstrap_db_file_legacy<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    io: &Arc<dyn turso_core::IO>,
    main_db_path: &str,
) -> Result<DatabasePullRevision> {
    tracing::info!("bootstrap_db_file(path={})", main_db_path);

    let start_time = std::time::Instant::now();
    // cleanup all files left from previous attempt to bootstrap
    // we shouldn't write any WAL files - but let's truncate them too for safety
    if let Some(file) = io.try_open(main_db_path)? {
        io.truncate(coro, file, 0).await?;
    }
    if let Some(file) = io.try_open(&format!("{main_db_path}-wal"))? {
        io.truncate(coro, file, 0).await?;
    }

    let file = io.create(main_db_path)?;
    let db_info = db_bootstrap(coro, client, file).await?;

    let elapsed = std::time::Instant::now().duration_since(start_time);
    tracing::info!(
        "bootstrap_db_files(path={}): finished: elapsed={:?}",
        main_db_path,
        elapsed
    );

    Ok(DatabasePullRevision::Legacy {
        generation: db_info.current_generation,
        synced_frame_no: None,
    })
}

pub async fn reset_wal_file<Ctx>(
    coro: &Coro<Ctx>,
    wal: Arc<dyn turso_core::File>,
    frames_count: u64,
) -> Result<()> {
    let wal_size = if frames_count == 0 {
        // let's truncate WAL file completely in order for this operation to safely execute on empty WAL in case of initial bootstrap phase
        0
    } else {
        WAL_HEADER as u64 + WAL_FRAME_SIZE as u64 * frames_count
    };
    tracing::debug!("reset db wal to the size of {} frames", frames_count);
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = wal.truncate(wal_size, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(())
}

async fn sql_execute_http<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    request: server_proto::PipelineReqBody,
) -> Result<Vec<StmtResult>> {
    let body = serde_json::to_vec(&request)?;

    client.network_stats.write(body.len());
    let completion = client.http("POST", "/v2/pipeline", Some(body), &[])?;

    wait_ok_status(coro, &completion, "sql_execute_http").await?;

    let response = wait_all_results(coro, &completion, Some(&client.network_stats)).await?;
    let response: server_proto::PipelineRespBody = serde_json::from_slice(&response)?;
    tracing::debug!("hrana response: {:?}", response);
    let mut results = Vec::new();
    for result in response.results {
        match result {
            server_proto::StreamResult::Error { error } => {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "failed to execute sql: {error:?}"
                )))
            }
            server_proto::StreamResult::None => {
                return Err(Error::DatabaseSyncEngineError(
                    "unexpected None result".to_string(),
                ));
            }
            server_proto::StreamResult::Ok { response } => match response {
                server_proto::StreamResponse::Execute(execute) => {
                    results.push(execute.result);
                }
                server_proto::StreamResponse::Batch(batch) => {
                    if let Some(error) = batch.result.step_errors.into_iter().flatten().next() {
                        return Err(Error::DatabaseSyncEngineError(format!(
                            "failed to execute sql: {error:?}"
                        )));
                    }
                    for result in batch.result.step_results.into_iter().flatten() {
                        results.push(result);
                    }
                }
            },
        }
    }
    Ok(results)
}

async fn wal_pull_http<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
) -> Result<WalHttpPullResult<IO::DataCompletionBytes>> {
    let completion = client.http(
        "GET",
        &format!("/sync/{generation}/{start_frame}/{end_frame}"),
        None,
        &[],
    )?;
    let status = wait_status(coro, &completion).await?;
    if status == http::StatusCode::BAD_REQUEST {
        let status_body = wait_all_results(coro, &completion, Some(&client.network_stats)).await?;
        let status: DbSyncStatus = serde_json::from_slice(&status_body)?;
        if status.status == "checkpoint_needed" {
            return Ok(WalHttpPullResult::NeedCheckpoint(status));
        } else {
            let error = format!("wal_pull: unexpected sync status: {status:?}");
            return Err(Error::DatabaseSyncEngineError(error));
        }
    }
    if status != http::StatusCode::OK {
        let error = format!("wal_pull: unexpected status code: {status}");
        return Err(Error::DatabaseSyncEngineError(error));
    }
    Ok(WalHttpPullResult::Frames(completion))
}

async fn wal_push_http<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
    baton: Option<String>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
    frames: Vec<u8>,
) -> Result<DbSyncStatus> {
    let baton = baton
        .map(|baton| format!("/{baton}"))
        .unwrap_or("".to_string());

    client.network_stats.write(frames.len());
    let completion = client.http(
        "POST",
        &format!("/sync/{generation}/{start_frame}/{end_frame}{baton}"),
        Some(frames),
        &[],
    )?;
    wait_ok_status(coro, &completion, "wal_push").await?;
    let status_body = wait_all_results(coro, &completion, Some(&client.network_stats)).await?;
    Ok(serde_json::from_slice(&status_body)?)
}

async fn db_info_http<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<IO>,
) -> Result<DbSyncInfo> {
    let completion = client.http("GET", "/info", None, &[])?;
    wait_ok_status(coro, &completion, "db_info").await?;
    let status_body = wait_all_results(coro, &completion, Some(&client.network_stats)).await?;
    Ok(serde_json::from_slice(&status_body)?)
}

async fn db_bootstrap_http<C: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    client: &SyncEngineIoStats<C>,
    generation: u64,
) -> Result<C::DataCompletionBytes> {
    let completion = client.http("GET", &format!("/export/{generation}"), None, &[])?;
    wait_ok_status(coro, &completion, "db_bootstrap").await?;
    Ok(completion)
}

pub async fn wait_ok_status<Ctx>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<u8>,
    operation: &'static str,
) -> Result<()> {
    let status = wait_status(coro, completion).await?;
    if status == http::StatusCode::OK {
        return Ok(());
    }
    let body = wait_all_results(coro, completion, None).await?;
    match std::str::from_utf8(body.as_slice()) {
        Ok(body) => Err(Error::DatabaseSyncEngineError(format!(
            "{operation}: unexpected http response: status={status}, body={body}"
        ))),
        Err(_) => Err(Error::DatabaseSyncEngineError(format!(
            "{operation}: unexpected http response: status={status}"
        ))),
    }
}

pub async fn wait_status<Ctx, T>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<T>,
) -> Result<u16> {
    while completion.status()?.is_none() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(completion.status()?.unwrap())
}

#[inline(always)]
pub fn read_varint(buf: &[u8]) -> Result<Option<(usize, usize)>> {
    let mut v: u64 = 0;
    for i in 0..9 {
        match buf.get(i) {
            Some(c) => {
                v |= ((c & 0x7f) as u64) << (i * 7);
                if (c & 0x80) == 0 {
                    return Ok(Some((v as usize, i + 1)));
                }
            }
            None => return Ok(None),
        }
    }
    Err(Error::DatabaseSyncEngineError(format!(
        "invalid variant byte: {:?}",
        &buf[0..=8]
    )))
}

pub async fn wait_proto_message<Ctx, T: prost::Message + Default>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<u8>,
    network_stats: &DataStats,
    bytes: &mut BytesMut,
) -> Result<Option<T>> {
    let start_time = std::time::Instant::now();
    while completion.status()?.is_none() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    let status = completion.status()?.expect("status must be set");
    if status != 200 {
        let body = wait_all_results(coro, completion, Some(network_stats)).await?;
        return match std::str::from_utf8(body.as_slice()) {
            Ok(body) => Err(Error::DatabaseSyncEngineError(format!(
                "remote server returned an error: status={status}, body={body}"
            ))),
            Err(_) => Err(Error::DatabaseSyncEngineError(format!(
                "remote server returned an error: status={status}"
            ))),
        };
    }
    loop {
        let length = read_varint(bytes)?;
        let not_enough_bytes = match length {
            None => true,
            Some((message_length, prefix_length)) => message_length + prefix_length > bytes.len(),
        };
        if not_enough_bytes {
            if let Some(poll) = completion.poll_data()? {
                network_stats.read(poll.data().len());
                bytes.extend_from_slice(poll.data());
            } else if !completion.is_done()? {
                coro.yield_(SyncEngineIoResult::IO).await?;
            } else if bytes.is_empty() {
                return Ok(None);
            } else {
                return Err(Error::DatabaseSyncEngineError(
                    "unexpected end of protobuf message".to_string(),
                ));
            }
            continue;
        }
        let (message_length, prefix_length) = length.unwrap();
        let message = T::decode_length_delimited(&**bytes).map_err(|e| {
            Error::DatabaseSyncEngineError(format!("unable to deserialize protobuf message: {e}"))
        })?;
        let _ = bytes.split_to(message_length + prefix_length);
        tracing::trace!(
            "wait_proto_message: elapsed={:?}",
            std::time::Instant::now().duration_since(start_time)
        );
        return Ok(Some(message));
    }
}

pub async fn wait_all_results<Ctx, T: Clone>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<T>,
    stats: Option<&DataStats>,
) -> Result<Vec<T>> {
    let mut results = Vec::new();
    loop {
        while let Some(poll) = completion.poll_data()? {
            stats.inspect(|s| s.read(poll.data().len()));
            results.extend_from_slice(poll.data());
        }
        if completion.is_done()? {
            break;
        }
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use bytes::{Bytes, BytesMut};
    use prost::Message;

    use crate::{
        database_sync_engine::DataStats,
        database_sync_engine_io::{DataCompletion, DataPollResult},
        database_sync_operations::wait_proto_message,
        server_proto::PageData,
        types::Coro,
        Result,
    };

    struct TestPollResult(Vec<u8>);

    impl DataPollResult<u8> for TestPollResult {
        fn data(&self) -> &[u8] {
            &self.0
        }
    }

    struct TestCompletion {
        data: RefCell<Bytes>,
        chunk: usize,
    }

    unsafe impl Sync for TestCompletion {}

    impl DataCompletion<u8> for TestCompletion {
        type DataPollResult = TestPollResult;
        fn status(&self) -> crate::Result<Option<u16>> {
            Ok(Some(200))
        }

        fn poll_data(&self) -> crate::Result<Option<Self::DataPollResult>> {
            let mut data = self.data.borrow_mut();
            let len = data.len();
            let chunk = data.split_to(len.min(self.chunk));
            if chunk.is_empty() {
                Ok(None)
            } else {
                Ok(Some(TestPollResult(chunk.to_vec())))
            }
        }

        fn is_done(&self) -> crate::Result<bool> {
            Ok(self.data.borrow().is_empty())
        }
    }

    #[test]
    pub fn wait_proto_message_test() {
        let mut data = Vec::new();
        for i in 0..1024 {
            let page = PageData {
                page_id: i as u64,
                encoded_page: vec![0u8; 16 * 1024].into(),
            };
            data.extend_from_slice(&page.encode_length_delimited_to_vec());
        }
        let completion = TestCompletion {
            data: RefCell::new(data.into()),
            chunk: 128,
        };
        let mut gen = genawaiter::sync::Gen::new({
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let mut bytes = BytesMut::new();
                let mut count = 0;
                let network_stats = DataStats::new();
                while wait_proto_message::<(), PageData>(
                    &coro,
                    &completion,
                    &network_stats,
                    &mut bytes,
                )
                .await?
                .is_some()
                {
                    assert!(bytes.capacity() <= 16 * 1024 + 1024);
                    count += 1;
                }
                assert_eq!(count, 1024);
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => {}
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }
}
