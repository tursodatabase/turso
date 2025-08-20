use std::sync::Arc;

use turso_core::{
    types::{Text, WalFrameInfo},
    Buffer, Completion, LimboError, Value,
};

use crate::{
    database_replay_generator::DatabaseReplayGenerator,
    database_tape::{
        run_stmt_expect_one_row, run_stmt_ignore_rows, DatabaseChangesIteratorMode,
        DatabaseChangesIteratorOpts, DatabaseReplaySessionOpts, DatabaseTape, DatabaseWalSession,
    },
    errors::Error,
    protocol_io::{DataCompletion, DataPollResult, ProtocolIO},
    server_proto::{self, ExecuteStreamReq, Stmt, StmtResult, StreamRequest},
    types::{
        Coro, DatabaseTapeOperation, DatabaseTapeRowChangeType, DbSyncInfo, DbSyncStatus,
        ProtocolCommand,
    },
    wal_session::WalSession,
    Result,
};

pub const WAL_HEADER: usize = 32;
pub const WAL_FRAME_HEADER: usize = 24;
pub const PAGE_SIZE: usize = 4096;
pub const WAL_FRAME_SIZE: usize = WAL_FRAME_HEADER + PAGE_SIZE;

enum WalHttpPullResult<C: DataCompletion> {
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
pub async fn db_bootstrap<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
    db: Arc<dyn turso_core::File>,
) -> Result<DbSyncInfo> {
    tracing::debug!("db_bootstrap");
    let start_time = std::time::Instant::now();
    let db_info = db_info_http(coro, client).await?;
    tracing::debug!("db_bootstrap: fetched db_info={db_info:?}");
    let content = db_bootstrap_http(coro, client, db_info.current_generation).await?;
    let mut pos = 0;
    loop {
        while let Some(chunk) = content.poll_data()? {
            let chunk = chunk.data();
            let content_len = chunk.len();
            // todo(sivukhin): optimize allocations here
            #[allow(clippy::arc_with_non_send_sync)]
            let buffer = Arc::new(Buffer::new_temporary(chunk.len()));
            buffer.as_mut_slice().copy_from_slice(chunk);
            let c = Completion::new_write(move |size| {
                // todo(sivukhin): we need to error out in case of partial read
                assert!(size as usize == content_len);
            });
            let c = db.pwrite(pos, buffer.clone(), c)?;
            while !c.is_completed() {
                coro.yield_(ProtocolCommand::IO).await?;
            }
            pos += content_len;
        }
        if content.is_done()? {
            break;
        }
        coro.yield_(ProtocolCommand::IO).await?;
    }

    // sync files in the end
    let c = Completion::new_sync(move |_| {
        // todo(sivukhin): we need to error out in case of failed sync
    });
    let c = db.sync(c)?;
    while !c.is_completed() {
        coro.yield_(ProtocolCommand::IO).await?;
    }

    let elapsed = std::time::Instant::now().duration_since(start_time);
    tracing::debug!("db_bootstrap: finished: bytes={pos}, elapsed={:?}", elapsed);

    Ok(db_info)
}

pub async fn wal_apply_from_file(
    coro: &Coro,
    frames_file: Arc<dyn turso_core::File>,
    session: &mut DatabaseWalSession,
) -> Result<u32> {
    let size = frames_file.size()?;
    assert!(size % WAL_FRAME_SIZE as u64 == 0);
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));
    let mut db_size = 0;
    for offset in (0..size).step_by(WAL_FRAME_SIZE) {
        let c = Completion::new_read(buffer.clone(), move |_, size| {
            // todo(sivukhin): we need to error out in case of partial read
            assert!(size as usize == WAL_FRAME_SIZE);
        });
        let c = frames_file.pread(offset as usize, c)?;
        while !c.is_completed() {
            coro.yield_(ProtocolCommand::IO).await?;
        }
        let info = WalFrameInfo::from_frame_header(buffer.as_slice());
        tracing::debug!("got frame: {:?}", info);
        db_size = info.db_size;
        session.append_page(info.page_no, &buffer.as_slice()[WAL_FRAME_HEADER..])?;
    }
    assert!(db_size > 0);
    Ok(db_size)
}

/// Pull updates from remote to the separate file
pub async fn wal_pull_to_file<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
    frames_file: Arc<dyn turso_core::File>,
    mut generation: u64,
    mut start_frame: u64,
    wal_pull_batch_size: u64,
) -> Result<DbSyncStatus> {
    tracing::info!(
        "wal_pull: generation={generation}, start_frame={start_frame}, wal_pull_batch_size={wal_pull_batch_size}"
    );

    // todo(sivukhin): optimize allocation by using buffer pool in the DatabaseSyncOperations
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));
    let mut buffer_len = 0;
    let mut last_offset = 0;
    let mut committed_len = 0;
    let status = loop {
        let end_frame = start_frame + wal_pull_batch_size;
        let result = wal_pull_http(coro, client, generation, start_frame, end_frame).await?;
        let data = match result {
            WalHttpPullResult::NeedCheckpoint(status) => {
                assert!(status.status == "checkpoint_needed");
                tracing::debug!("wal_pull: need checkpoint: status={status:?}");
                if status.generation == generation && status.max_frame_no < start_frame {
                    tracing::debug!("wal_pull: end of history: status={:?}", status);
                    break status;
                }
                generation += 1;
                start_frame = 1;
                continue;
            }
            WalHttpPullResult::Frames(content) => content,
        };
        loop {
            while let Some(chunk) = data.poll_data()? {
                let mut chunk = chunk.data();
                while !chunk.is_empty() {
                    let to_fill = (WAL_FRAME_SIZE - buffer_len).min(chunk.len());
                    buffer.as_mut_slice()[buffer_len..].copy_from_slice(&chunk[0..to_fill]);
                    buffer_len += to_fill;
                    chunk = &chunk[to_fill..];

                    if buffer_len < WAL_FRAME_SIZE {
                        continue;
                    }
                    let c = Completion::new_write(move |size| {
                        // todo(sivukhin): we need to error out in case of partial read
                        assert!(size as usize == WAL_FRAME_SIZE);
                    });
                    let c = frames_file.pwrite(last_offset, buffer.clone(), c)?;
                    while !c.is_completed() {
                        coro.yield_(ProtocolCommand::IO).await?;
                    }

                    last_offset += WAL_FRAME_SIZE;
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
            coro.yield_(ProtocolCommand::IO).await?;
        }
        if start_frame < end_frame {
            // chunk which was sent from the server has ended early - so there is nothing left on server-side for pull
            break DbSyncStatus {
                baton: None,
                status: "".to_string(),
                generation,
                max_frame_no: start_frame - 1,
            };
        }
        if buffer_len != 0 {
            return Err(Error::DatabaseSyncEngineError(format!(
                "wal_pull: response has unexpected trailing data: buffer_len={}",
                buffer_len
            )));
        }
    };

    tracing::info!(
        "wal_pull: generation={generation}, frame={start_frame}, last_offset={last_offset}, commited_len={committed_len}"
    );
    let c = Completion::new_trunc(move |rc| {
        assert!(rc as usize == 0);
    });
    let c = frames_file.truncate(committed_len, c)?;
    while !c.is_completed() {
        coro.yield_(ProtocolCommand::IO).await?;
    }

    let c = Completion::new_sync(move |_| {
        // todo(sivukhin): we need to error out in case of failed sync
    });
    let c = frames_file.sync(c)?;
    while !c.is_completed() {
        coro.yield_(ProtocolCommand::IO).await?;
    }

    Ok(status)
}

/// Push frame range [start_frame..end_frame) to the remote
/// Returns baton for WAL remote-session in case of success
/// Returns [Error::DatabaseSyncEngineConflict] in case of frame conflict at remote side
///
/// Guarantees:
/// 1. If there is a single client which calls wal_push, then this operation is idempotent for fixed generation
///    and can be called multiple times with same frame range
pub async fn wal_push<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
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

pub async fn has_table(
    coro: &Coro,
    conn: &Arc<turso_core::Connection>,
    table_name: &str,
) -> Result<bool> {
    let mut stmt =
        conn.prepare("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?")?;
    stmt.bind_at(1.try_into().unwrap(), Value::Text(Text::new(table_name)));

    let count = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0]
            .as_int()
            .ok_or_else(|| Error::DatabaseSyncEngineError("unexpected column type".to_string()))?,
        _ => panic!("expected single row"),
    };
    Ok(count > 0)
}

pub async fn count_local_changes(
    coro: &Coro,
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

pub async fn update_last_change_id(
    coro: &Coro,
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
        turso_core::Value::Text(turso_core::types::Text::new(client_id)),
    );
    let row = run_stmt_expect_one_row(coro, &mut select_stmt).await?;
    tracing::info!("update_last_change_id(client_id={client_id}): selected client row if any");

    if let Some(_) = row {
        let mut update_stmt = conn.prepare(TURSO_SYNC_UPDATE_LAST_CHANGE_ID)?;
        update_stmt.bind_at(1.try_into().unwrap(), turso_core::Value::Integer(pull_gen));
        update_stmt.bind_at(2.try_into().unwrap(), turso_core::Value::Integer(change_id));
        update_stmt.bind_at(
            3.try_into().unwrap(),
            turso_core::Value::Text(turso_core::types::Text::new(client_id)),
        );
        run_stmt_ignore_rows(coro, &mut update_stmt).await?;
        tracing::info!("update_last_change_id(client_id={client_id}): updated row for the client");
    } else {
        let mut update_stmt = conn.prepare(TURSO_SYNC_INSERT_LAST_CHANGE_ID)?;
        update_stmt.bind_at(
            1.try_into().unwrap(),
            turso_core::Value::Text(turso_core::types::Text::new(client_id)),
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

pub async fn fetch_last_change_id<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
    source_conn: &Arc<turso_core::Connection>,
    client_id: &str,
) -> Result<(i64, Option<i64>)> {
    tracing::info!("fetch_last_change_id: client_id={client_id}");

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let source_pull_gen = 'source_pull_gen: {
        let mut select_last_change_id_stmt =
            match source_conn.prepare(TURSO_SYNC_SELECT_LAST_CHANGE_ID) {
                Ok(stmt) => stmt,
                Err(LimboError::ParseError(..)) => break 'source_pull_gen 0,
                Err(err) => return Err(err.into()),
            };

        select_last_change_id_stmt
            .bind_at(1.try_into().unwrap(), Value::Text(Text::new(client_id)));

        match run_stmt_expect_one_row(coro, &mut select_last_change_id_stmt).await? {
            Some(row) => row[0].as_int().ok_or_else(|| {
                Error::DatabaseSyncEngineError("unexpected source pull_gen type".to_string())
            })?,
            None => {
                tracing::info!("fetch_last_change_id: client_id={client_id}, turso_sync_last_change_id table is not found");
                0
            }
        }
    };
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

pub async fn push_logical_changes<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
    source: &DatabaseTape,
    client_id: &str,
    tables_to_ignore: &[String],
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
    let mut sql_over_http_requests = vec![
        Stmt {
            sql: Some("BEGIN IMMEDIATE".to_string()),
            sql_id: None,
            args: Vec::new(),
            named_args: Vec::new(),
            want_rows: Some(false),
            replication_index: None,
        },
        Stmt {
            sql: Some(TURSO_SYNC_CREATE_TABLE.to_string()),
            sql_id: None,
            args: Vec::new(),
            named_args: Vec::new(),
            want_rows: Some(false),
            replication_index: None,
        },
    ];
    let mut rows_changed = 0;
    let mut changes = source.iterate_changes(iterate_opts)?;
    while let Some(operation) = changes.next(coro).await? {
        match operation {
            DatabaseTapeOperation::RowChange(change) => {
                assert!(
                    last_change_id.is_none() || last_change_id.unwrap() < change.change_id,
                    "change id must be strictly increasing: last_change_id={:?}, change.change_id={}",
                    last_change_id,
                    change.change_id
                );
                if change.table_name == TURSO_SYNC_TABLE_NAME {
                    continue;
                }
                if tables_to_ignore.iter().any(|x| &change.table_name == x) {
                    continue;
                }
                rows_changed += 1;
                // we give user full control over CDC table - so let's not emit assert here for now
                if last_change_id.is_some() && last_change_id.unwrap() + 1 != change.change_id {
                    tracing::warn!(
                        "out of order change sequence: {} -> {}",
                        last_change_id.unwrap(),
                        change.change_id
                    );
                }
                last_change_id = Some(change.change_id);
                let replay_info = generator.replay_info(coro, &change).await?;
                let change_type = (&change.change).into();
                match change.change {
                    DatabaseTapeRowChangeType::Delete { before } => {
                        assert!(replay_info.len() == 1);
                        let values = generator.replay_values(
                            &replay_info[0],
                            change_type,
                            change.id,
                            before,
                            None,
                        );
                        sql_over_http_requests.push(Stmt {
                            sql: Some(replay_info[0].query.clone()),
                            sql_id: None,
                            args: convert_to_args(values),
                            named_args: Vec::new(),
                            want_rows: Some(false),
                            replication_index: None,
                        })
                    }
                    DatabaseTapeRowChangeType::Insert { after } => {
                        assert!(replay_info.len() == 1);
                        let values = generator.replay_values(
                            &replay_info[0],
                            change_type,
                            change.id,
                            after,
                            None,
                        );
                        sql_over_http_requests.push(Stmt {
                            sql: Some(replay_info[0].query.clone()),
                            sql_id: None,
                            args: convert_to_args(values),
                            named_args: Vec::new(),
                            want_rows: Some(false),
                            replication_index: None,
                        })
                    }
                    DatabaseTapeRowChangeType::Update {
                        after,
                        updates: Some(updates),
                        ..
                    } => {
                        assert!(replay_info.len() == 1);
                        let values = generator.replay_values(
                            &replay_info[0],
                            change_type,
                            change.id,
                            after,
                            Some(updates),
                        );
                        sql_over_http_requests.push(Stmt {
                            sql: Some(replay_info[0].query.clone()),
                            sql_id: None,
                            args: convert_to_args(values),
                            named_args: Vec::new(),
                            want_rows: Some(false),
                            replication_index: None,
                        })
                    }
                    DatabaseTapeRowChangeType::Update {
                        before,
                        after,
                        updates: None,
                    } => {
                        assert!(replay_info.len() == 2);
                        let values = generator.replay_values(
                            &replay_info[0],
                            change_type,
                            change.id,
                            before,
                            None,
                        );
                        sql_over_http_requests.push(Stmt {
                            sql: Some(replay_info[0].query.clone()),
                            sql_id: None,
                            args: convert_to_args(values),
                            named_args: Vec::new(),
                            want_rows: Some(false),
                            replication_index: None,
                        });
                        let values = generator.replay_values(
                            &replay_info[1],
                            change_type,
                            change.id,
                            after,
                            None,
                        );
                        sql_over_http_requests.push(Stmt {
                            sql: Some(replay_info[1].query.clone()),
                            sql_id: None,
                            args: convert_to_args(values),
                            named_args: Vec::new(),
                            want_rows: Some(false),
                            replication_index: None,
                        });
                    }
                }
            }
            DatabaseTapeOperation::Commit => {
                if rows_changed > 0 {
                    tracing::info!("prepare update stmt for turso_sync_last_change_id table with client_id={} and last_change_id={:?}", client_id, last_change_id);
                    // update turso_sync_last_change_id table with new value before commit
                    let next_change_id = last_change_id.unwrap_or(0);
                    tracing::info!("push_logical_changes: client_id={client_id}, set pull_gen={source_pull_gen}, change_id={next_change_id}, rows_changed={rows_changed}");
                    sql_over_http_requests.push(Stmt {
                        sql: Some(TURSO_SYNC_UPSERT_LAST_CHANGE_ID.to_string()),
                        sql_id: None,
                        args: vec![
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
                        named_args: Vec::new(),
                        want_rows: Some(false),
                        replication_index: None,
                    });
                }
                sql_over_http_requests.push(Stmt {
                    sql: Some("COMMIT".to_string()),
                    sql_id: None,
                    args: Vec::new(),
                    named_args: Vec::new(),
                    want_rows: Some(false),
                    replication_index: None,
                });
            }
        }
    }

    tracing::debug!("hrana request: {:?}", sql_over_http_requests);
    let replay_hrana_request = server_proto::PipelineReqBody {
        baton: None,
        requests: sql_over_http_requests
            .into_iter()
            .map(|stmt| StreamRequest::Execute(ExecuteStreamReq { stmt }))
            .collect(),
    };

    let _ = sql_execute_http(coro, client, replay_hrana_request).await?;
    tracing::info!("push_logical_changes: rows_changed={:?}", rows_changed);
    Ok((source_pull_gen, last_change_id.unwrap_or(0)))
}

pub async fn checkpoint_wal_file(coro: &Coro, conn: &Arc<turso_core::Connection>) -> Result<()> {
    let mut checkpoint_stmt = conn.prepare("PRAGMA wal_checkpoint(TRUNCATE)")?;
    loop {
        match checkpoint_stmt.step()? {
            turso_core::StepResult::IO => coro.yield_(ProtocolCommand::IO).await?,
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

pub async fn reset_wal_file(
    coro: &Coro,
    wal: Arc<dyn turso_core::File>,
    frames_count: u64,
) -> Result<()> {
    let wal_size = if frames_count == 0 {
        // let's truncate WAL file completely in order for this operation to safely execute on empty WAL in case of initial bootstrap phase
        0
    } else {
        WAL_HEADER + WAL_FRAME_SIZE * (frames_count as usize)
    };
    tracing::debug!("reset db wal to the size of {} frames", frames_count);
    let c = Completion::new_trunc(move |rc| {
        assert!(rc as usize == 0);
    });
    let c = wal.truncate(wal_size, c)?;
    while !c.is_completed() {
        coro.yield_(ProtocolCommand::IO).await?;
    }
    Ok(())
}

async fn sql_execute_http<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
    request: server_proto::PipelineReqBody,
) -> Result<Vec<StmtResult>> {
    let body = serde_json::to_vec(&request)?;
    let completion = client.http("POST", "/v2/pipeline", Some(body))?;
    let status = wait_status(coro, &completion).await?;
    if status != http::StatusCode::OK {
        let error = format!("sql_execute_http: unexpected status code: {status}");
        return Err(Error::DatabaseSyncEngineError(error));
    }
    let response = wait_full_body(coro, &completion).await?;
    let response: server_proto::PipelineRespBody = serde_json::from_slice(&response)?;
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
            },
        }
    }
    Ok(results)
}

async fn wal_pull_http<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
) -> Result<WalHttpPullResult<C::DataCompletion>> {
    let completion = client.http(
        "GET",
        &format!("/sync/{generation}/{start_frame}/{end_frame}"),
        None,
    )?;
    let status = wait_status(coro, &completion).await?;
    if status == http::StatusCode::BAD_REQUEST {
        let status_body = wait_full_body(coro, &completion).await?;
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

async fn wal_push_http<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
    baton: Option<String>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
    frames: Vec<u8>,
) -> Result<DbSyncStatus> {
    let baton = baton
        .map(|baton| format!("/{baton}"))
        .unwrap_or("".to_string());
    let completion = client.http(
        "POST",
        &format!("/sync/{generation}/{start_frame}/{end_frame}{baton}"),
        Some(frames),
    )?;
    let status = wait_status(coro, &completion).await?;
    let status_body = wait_full_body(coro, &completion).await?;
    if status != http::StatusCode::OK {
        let error = std::str::from_utf8(&status_body).ok().unwrap_or("");
        return Err(Error::DatabaseSyncEngineError(format!(
            "wal_push go unexpected status: {status} (error={error})"
        )));
    }
    Ok(serde_json::from_slice(&status_body)?)
}

async fn db_info_http<C: ProtocolIO>(coro: &Coro, client: &C) -> Result<DbSyncInfo> {
    let completion = client.http("GET", "/info", None)?;
    let status = wait_status(coro, &completion).await?;
    let status_body = wait_full_body(coro, &completion).await?;
    if status != http::StatusCode::OK {
        return Err(Error::DatabaseSyncEngineError(format!(
            "db_info go unexpected status: {status}"
        )));
    }
    Ok(serde_json::from_slice(&status_body)?)
}

async fn db_bootstrap_http<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
    generation: u64,
) -> Result<C::DataCompletion> {
    let completion = client.http("GET", &format!("/export/{generation}"), None)?;
    let status = wait_status(coro, &completion).await?;
    if status != http::StatusCode::OK.as_u16() {
        return Err(Error::DatabaseSyncEngineError(format!(
            "db_bootstrap go unexpected status: {status}"
        )));
    }
    Ok(completion)
}

pub async fn wait_status(coro: &Coro, completion: &impl DataCompletion) -> Result<u16> {
    while completion.status()?.is_none() {
        coro.yield_(ProtocolCommand::IO).await?;
    }
    Ok(completion.status()?.unwrap())
}

pub async fn wait_full_body(coro: &Coro, completion: &impl DataCompletion) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    loop {
        while let Some(poll) = completion.poll_data()? {
            bytes.extend_from_slice(poll.data());
        }
        if completion.is_done()? {
            break;
        }
        coro.yield_(ProtocolCommand::IO).await?;
    }
    Ok(bytes)
}
