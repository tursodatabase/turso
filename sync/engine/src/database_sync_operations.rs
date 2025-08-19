use std::sync::Arc;

use turso_core::{types::Text, Buffer, Completion, LimboError, Value};

use crate::{
    database_replay_generator::DatabaseReplayGenerator,
    database_tape::{
        exec_stmt, run_stmt_expect_one_row, DatabaseChangesIteratorMode,
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
const PAGE_SIZE: usize = 4096;
const WAL_FRAME_SIZE: usize = WAL_FRAME_HEADER + PAGE_SIZE;

enum WalHttpPullResult<C: DataCompletion> {
    Frames(C),
    NeedCheckpoint(DbSyncStatus),
}

pub enum WalPullResult {
    Done,
    PullMore,
    NeedCheckpoint,
}

pub enum WalPushResult {
    Ok { baton: Option<String> },
    NeedCheckpoint,
}

pub async fn connect(coro: &Coro, tape: &DatabaseTape) -> Result<Arc<turso_core::Connection>> {
    let conn = tape.connect(coro).await?;
    conn.wal_auto_checkpoint_disable();
    Ok(conn)
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
    dbs: &[Arc<dyn turso_core::File>],
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
            let mut completions = Vec::with_capacity(dbs.len());
            for db in dbs {
                let c = Completion::new_write(move |size| {
                    // todo(sivukhin): we need to error out in case of partial read
                    assert!(size as usize == content_len);
                });
                completions.push(db.pwrite(pos, buffer.clone(), c)?);
            }
            while !completions.iter().all(|x| x.is_completed()) {
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
    let mut completions = Vec::with_capacity(dbs.len());
    for db in dbs {
        let c = Completion::new_sync(move |_| {
            // todo(sivukhin): we need to error out in case of failed sync
        });
        completions.push(db.sync(c)?);
    }
    while !completions.iter().all(|x| x.is_completed()) {
        coro.yield_(ProtocolCommand::IO).await?;
    }

    let elapsed = std::time::Instant::now().duration_since(start_time);
    tracing::debug!("db_bootstrap: finished: bytes={pos}, elapsed={:?}", elapsed);

    Ok(db_info)
}

/// Pull updates from remote to the database file
///
/// Returns [WalPullResult::Done] if pull reached the end of database history
/// Returns [WalPullResult::PullMore] if all frames from [start_frame..end_frame) range were pulled, but remote have more
/// Returns [WalPullResult::NeedCheckpoint] if remote generation increased and local version must be checkpointed
///
/// Guarantees:
/// 1. Frames are commited to the WAL (i.e. db_size is not zero 0) only at transaction boundaries from remote
/// 2. wal_pull is idempotent for fixed generation and can be called multiple times with same frame range
pub async fn wal_pull<'a, C: ProtocolIO, U: AsyncFnMut(&'a Coro, u64) -> Result<()>>(
    coro: &'a Coro,
    client: &C,
    wal_session: &mut WalSession,
    generation: u64,
    mut start_frame: u64,
    end_frame: u64,
    mut update: U,
) -> Result<WalPullResult> {
    tracing::info!(
        "wal_pull: generation={}, start_frame={}, end_frame={}",
        generation,
        start_frame,
        end_frame
    );

    // todo(sivukhin): optimize allocation by using buffer pool in the DatabaseSyncOperations
    let mut buffer = Vec::with_capacity(WAL_FRAME_SIZE);

    let result = wal_pull_http(coro, client, generation, start_frame, end_frame).await?;
    let data = match result {
        WalHttpPullResult::NeedCheckpoint(status) => {
            assert!(status.status == "checkpoint_needed");
            tracing::debug!("wal_pull: need checkpoint: status={status:?}");
            if status.generation == generation && status.max_frame_no < start_frame {
                tracing::debug!("wal_pull: end of history: status={:?}", status);
                update(coro, status.max_frame_no).await?;
                return Ok(WalPullResult::Done);
            }
            return Ok(WalPullResult::NeedCheckpoint);
        }
        WalHttpPullResult::Frames(content) => content,
    };
    loop {
        while let Some(chunk) = data.poll_data()? {
            let mut chunk = chunk.data();
            while !chunk.is_empty() {
                let to_fill = (WAL_FRAME_SIZE - buffer.len()).min(chunk.len());
                buffer.extend_from_slice(&chunk[0..to_fill]);
                chunk = &chunk[to_fill..];

                assert!(
                    buffer.capacity() == WAL_FRAME_SIZE,
                    "buffer should not extend its capacity"
                );
                if buffer.len() < WAL_FRAME_SIZE {
                    continue;
                }
                if !wal_session.in_txn() {
                    wal_session.begin()?;
                }
                let frame_info = wal_session.insert_at(start_frame, &buffer)?;
                if frame_info.is_commit_frame() {
                    wal_session.end()?;
                    // transaction boundary reached - safe to commit progress
                    update(coro, start_frame).await?;
                }
                buffer.clear();
                start_frame += 1;
            }
        }
        if data.is_done()? {
            break;
        }
        coro.yield_(ProtocolCommand::IO).await?;
    }
    if start_frame < end_frame {
        // chunk which was sent from the server has ended early - so there is nothing left on server-side for pull
        return Ok(WalPullResult::Done);
    }
    if !buffer.is_empty() {
        return Err(Error::DatabaseSyncEngineError(format!(
            "wal_pull: response has unexpected trailing data: buffer.len()={}",
            buffer.len()
        )));
    }
    Ok(WalPullResult::PullMore)
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

const TURSO_SYNC_TABLE_NAME: &str = "turso_sync_last_change_id";
const TURSO_SYNC_CREATE_TABLE: &str =
    "CREATE TABLE IF NOT EXISTS turso_sync_last_change_id (client_id TEXT PRIMARY KEY, pull_gen INTEGER, change_id INTEGER)";
const TURSO_SYNC_SELECT_LAST_CHANGE_ID: &str =
    "SELECT pull_gen, change_id FROM turso_sync_last_change_id WHERE client_id = ?";
const TURSO_SYNC_INSERT_LAST_CHANGE_ID: &str =
    "INSERT INTO turso_sync_last_change_id(client_id, pull_gen, change_id) VALUES (?, 0, 0) ON CONFLICT(client_id) DO NOTHING";
const TURSO_SYNC_UPDATE_LAST_CHANGE_ID: &str =
    "UPDATE turso_sync_last_change_id SET pull_gen = ?, change_id = ? WHERE client_id = ?";

/// Transfers row changes from source DB to target DB
/// In order to guarantee atomicity and avoid conflicts - method maintain last_change_id counter in the target db table turso_sync_last_change_id
pub async fn transfer_logical_changes(
    coro: &Coro,
    source: &DatabaseTape,
    target: &DatabaseTape,
    client_id: &str,
    bump_pull_gen: bool,
) -> Result<()> {
    tracing::info!("transfer_logical_changes: client_id={client_id}");
    let source_conn = connect_untracked(source)?;
    let target_conn = connect_untracked(target)?;

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
                tracing::info!("transfer_logical_changes: client_id={client_id}, turso_sync_last_change_id table is not found");
                0
            }
        }
    };
    tracing::info!(
        "transfer_logical_changes: client_id={client_id}, source_pull_gen={source_pull_gen}"
    );

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let mut schema_stmt = target_conn.prepare(TURSO_SYNC_CREATE_TABLE)?;
    exec_stmt(coro, &mut schema_stmt).await?;

    let mut select_last_change_id_stmt = target_conn.prepare(TURSO_SYNC_SELECT_LAST_CHANGE_ID)?;
    select_last_change_id_stmt.bind_at(1.try_into().unwrap(), Value::Text(Text::new(client_id)));

    let mut last_change_id = match run_stmt_expect_one_row(coro, &mut select_last_change_id_stmt)
        .await?
    {
        Some(row) => {
            let target_pull_gen = row[0].as_int().ok_or_else(|| {
                Error::DatabaseSyncEngineError("unexpected target pull_gen type".to_string())
            })?;
            let target_change_id = row[1].as_int().ok_or_else(|| {
                Error::DatabaseSyncEngineError("unexpected target change_id type".to_string())
            })?;
            tracing::debug!(
                "transfer_logical_changes: client_id={client_id}, target_pull_gen={target_pull_gen}, target_change_id={target_change_id}"
            );
            if target_pull_gen > source_pull_gen {
                return Err(Error::DatabaseSyncEngineError(format!("protocol error: target_pull_gen > source_pull_gen: {target_pull_gen} > {source_pull_gen}")));
            }
            if target_pull_gen == source_pull_gen {
                Some(target_change_id)
            } else {
                Some(0)
            }
        }
        None => {
            let mut insert_last_change_id_stmt =
                target_conn.prepare(TURSO_SYNC_INSERT_LAST_CHANGE_ID)?;
            insert_last_change_id_stmt
                .bind_at(1.try_into().unwrap(), Value::Text(Text::new(client_id)));
            exec_stmt(coro, &mut insert_last_change_id_stmt).await?;
            None
        }
    };

    tracing::debug!(
        "transfer_logical_changes: last_change_id={:?}",
        last_change_id
    );
    let replay_opts = DatabaseReplaySessionOpts {
        use_implicit_rowid: false,
    };

    let source_schema_cookie = connect_untracked(source)?.read_schema_version()?;

    let mut session = target.start_replay_session(coro, replay_opts).await?;

    let iterate_opts = DatabaseChangesIteratorOpts {
        first_change_id: last_change_id.map(|x| x + 1),
        mode: DatabaseChangesIteratorMode::Apply,
        ignore_schema_changes: false,
        ..Default::default()
    };
    let mut rows_changed = 0;
    let mut changes = source.iterate_changes(iterate_opts)?;
    while let Some(operation) = changes.next(coro).await? {
        match &operation {
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
            }
            DatabaseTapeOperation::Commit if rows_changed > 0 || bump_pull_gen => {
                tracing::info!("prepare update stmt for turso_sync_last_change_id table with client_id={} and last_change_id={:?}", client_id, last_change_id);
                // update turso_sync_last_change_id table with new value before commit
                let mut set_last_change_id_stmt =
                    session.conn().prepare(TURSO_SYNC_UPDATE_LAST_CHANGE_ID)?;
                let (next_pull_gen, next_change_id) = if bump_pull_gen {
                    (source_pull_gen + 1, 0)
                } else {
                    (source_pull_gen, last_change_id.unwrap_or(0))
                };
                tracing::info!("transfer_logical_changes: client_id={client_id}, set pull_gen={next_pull_gen}, change_id={next_change_id}, rows_changed={rows_changed}");
                set_last_change_id_stmt
                    .bind_at(1.try_into().unwrap(), Value::Integer(next_pull_gen));
                set_last_change_id_stmt
                    .bind_at(2.try_into().unwrap(), Value::Integer(next_change_id));
                set_last_change_id_stmt
                    .bind_at(3.try_into().unwrap(), Value::Text(Text::new(client_id)));
                exec_stmt(coro, &mut set_last_change_id_stmt).await?;
                let session_schema_cookie = session.conn().read_schema_version()?;
                if session_schema_cookie <= source_schema_cookie {
                    session
                        .conn()
                        .write_schema_version(source_schema_cookie + 1)?;
                }
            }
            _ => {}
        }
        session.replay(coro, operation).await?;
    }

    tracing::info!("transfer_logical_changes: rows_changed={:?}", rows_changed);
    Ok(())
}

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

pub async fn push_logical_changes<C: ProtocolIO>(
    coro: &Coro,
    client: &C,
    source: &DatabaseTape,
    client_id: &str,
) -> Result<()> {
    tracing::info!("push_logical_changes: client_id={client_id}");
    let source_conn = connect_untracked(source)?;

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
                tracing::info!("push_logical_changes: client_id={client_id}, turso_sync_last_change_id table is not found");
                0
            }
        }
    };
    tracing::info!(
        "push_logical_changes: client_id={client_id}, source_pull_gen={source_pull_gen}"
    );

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let init_hrana_request = server_proto::PipelineReqBody {
        baton: None,
        requests: vec![
            // create 'turso_sync_last_change_id' table if not exists
            StreamRequest::Execute(ExecuteStreamReq {
                stmt: Stmt {
                    sql: Some(TURSO_SYNC_CREATE_TABLE.to_string()),
                    sql_id: None,
                    args: Vec::new(),
                    named_args: Vec::new(),
                    want_rows: Some(false),
                    replication_index: None,
                },
            }),
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
            // initialize pull_gen, change_id for current client if the entry was absent
            StreamRequest::Execute(ExecuteStreamReq {
                stmt: Stmt {
                    sql: Some(TURSO_SYNC_INSERT_LAST_CHANGE_ID.to_string()),
                    sql_id: None,
                    args: vec![server_proto::Value::Text {
                        value: client_id.to_string(),
                    }],
                    named_args: Vec::new(),
                    want_rows: Some(false),
                    replication_index: None,
                },
            }),
        ]
        .into(),
    };

    let response = sql_execute_http(coro, client, init_hrana_request).await?;
    tracing::info!("response: {:?}", response);
    assert!(response.len() == 3);
    let last_change_id_response = &response[1];
    assert!(last_change_id_response.rows.len() <= 1);
    let mut last_change_id = if !last_change_id_response.rows.is_empty() {
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
            "push_logical_changes: client_id={client_id}, target_pull_gen={target_pull_gen}, target_change_id={target_change_id}"
        );
        if target_pull_gen > source_pull_gen {
            return Err(Error::DatabaseSyncEngineError(format!("protocol error: target_pull_gen > source_pull_gen: {target_pull_gen} > {source_pull_gen}")));
        }
        if target_pull_gen == source_pull_gen {
            Some(target_change_id)
        } else {
            Some(0)
        }
    } else {
        None
    };

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
                    let (next_pull_gen, next_change_id) =
                        (source_pull_gen, last_change_id.unwrap_or(0));
                    tracing::info!("push_logical_changes: client_id={client_id}, set pull_gen={next_pull_gen}, change_id={next_change_id}, rows_changed={rows_changed}");
                    sql_over_http_requests.push(Stmt {
                        sql: Some(TURSO_SYNC_UPDATE_LAST_CHANGE_ID.to_string()),
                        sql_id: None,
                        args: vec![
                            server_proto::Value::Integer {
                                value: next_pull_gen,
                            },
                            server_proto::Value::Integer {
                                value: next_change_id,
                            },
                            server_proto::Value::Text {
                                value: client_id.to_string(),
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
    Ok(())
}

/// Replace WAL frames [target_wal_match_watermark..) in the target DB with frames [source_wal_match_watermark..) from source DB
/// Return the position in target DB wal which logically equivalent to the source_sync_watermark in the source DB WAL
pub async fn transfer_physical_changes(
    coro: &Coro,
    source: &DatabaseTape,
    target_session: WalSession,
    source_wal_match_watermark: u64,
    source_sync_watermark: u64,
    target_wal_match_watermark: u64,
) -> Result<u64> {
    tracing::info!("transfer_physical_changes: source_wal_match_watermark={source_wal_match_watermark}, source_sync_watermark={source_sync_watermark}, target_wal_match_watermark={target_wal_match_watermark}");

    let source_conn = connect(coro, source).await?;
    let mut source_session = WalSession::new(source_conn.clone());
    source_session.begin()?;

    let source_frames_count = source_conn.wal_frame_count()?;
    assert!(
        source_frames_count >= source_wal_match_watermark,
        "watermark can't be greater than current frames count: {source_frames_count} vs {source_wal_match_watermark}",
    );
    if source_frames_count == source_wal_match_watermark {
        assert!(source_sync_watermark == source_wal_match_watermark);
        return Ok(target_wal_match_watermark);
    }
    assert!(
        (source_wal_match_watermark..=source_frames_count).contains(&source_sync_watermark),
        "source_sync_watermark={source_sync_watermark} must be in range: {source_wal_match_watermark}..={source_frames_count}",
    );

    let target_sync_watermark = {
        let mut target_session = DatabaseWalSession::new(coro, target_session).await?;
        tracing::info!("rollback_changes_after: {target_wal_match_watermark}");

        target_session.rollback_changes_after(target_wal_match_watermark)?;
        let mut last_frame_info = None;
        let mut frame = vec![0u8; WAL_FRAME_SIZE];
        let mut target_sync_watermark = target_session.frames_count()?;
        tracing::info!(
            "transfer_physical_changes: start={}, end={}",
            source_wal_match_watermark + 1,
            source_frames_count
        );
        for source_frame_no in source_wal_match_watermark + 1..=source_frames_count {
            let frame_info = source_conn.wal_get_frame(source_frame_no, &mut frame)?;
            tracing::debug!("append page {} to target DB", frame_info.page_no);
            target_session.append_page(frame_info.page_no, &frame[WAL_FRAME_HEADER..])?;
            if source_frame_no == source_sync_watermark {
                target_sync_watermark = target_session.frames_count()? + 1; // +1 because page will be actually commited on next iteration
                tracing::info!("set target_sync_watermark to {}", target_sync_watermark);
            }
            last_frame_info = Some(frame_info);
        }
        let db_size = last_frame_info.unwrap().db_size;
        tracing::debug!("commit WAL session to target with db_size={db_size}");
        target_session.commit(db_size)?;
        assert!(target_sync_watermark != 0);
        target_sync_watermark
    };
    Ok(target_sync_watermark)
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

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use tempfile::NamedTempFile;
    use turso_core::Value;

    use crate::{
        database_sync_operations::{transfer_logical_changes, transfer_physical_changes},
        database_tape::{run_stmt_once, DatabaseTape, DatabaseTapeOpts},
        wal_session::WalSession,
        Result,
    };

    #[test]
    pub fn test_transfer_logical_changes() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false, true).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false, true).unwrap();
        let db2 = Arc::new(DatabaseTape::new(db2));

        let mut gen = genawaiter::sync::Gen::new(|coro| async move {
            let conn1 = db1.connect(&coro).await?;
            conn1.execute("CREATE TABLE t(x, y)").unwrap();
            conn1
                .execute("INSERT INTO t VALUES (1, 2), (3, 4), (5, 6)")
                .unwrap();

            let conn2 = db2.connect(&coro).await.unwrap();

            transfer_logical_changes(&coro, &db1, &db2, "id-1", false)
                .await
                .unwrap();

            let mut rows = Vec::new();
            let mut stmt = conn2.prepare("SELECT x, y FROM t").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![Value::Integer(1), Value::Integer(2)],
                    vec![Value::Integer(3), Value::Integer(4)],
                    vec![Value::Integer(5), Value::Integer(6)],
                ]
            );

            conn1.execute("INSERT INTO t VALUES (7, 8)").unwrap();
            transfer_logical_changes(&coro, &db1, &db2, "id-1", false)
                .await
                .unwrap();

            let mut rows = Vec::new();
            let mut stmt = conn2.prepare("SELECT x, y FROM t").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![Value::Integer(1), Value::Integer(2)],
                    vec![Value::Integer(3), Value::Integer(4)],
                    vec![Value::Integer(5), Value::Integer(6)],
                    vec![Value::Integer(7), Value::Integer(8)],
                ]
            );

            Result::Ok(())
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.run_once().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    pub fn test_transfer_physical_changes() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();

        let opts = DatabaseTapeOpts {
            cdc_mode: Some("off".to_string()),
            cdc_table: None,
        };
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false, true).unwrap();
        let db1 = Arc::new(DatabaseTape::new_with_opts(db1, opts.clone()));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false, true).unwrap();
        let db2 = Arc::new(DatabaseTape::new_with_opts(db2, opts.clone()));

        let mut gen = genawaiter::sync::Gen::new(|coro| async move {
            let conn1 = db1.connect(&coro).await?;
            conn1.execute("CREATE TABLE t(x, y)")?;
            conn1.execute("INSERT INTO t VALUES (1, 2)")?;
            let conn1_match_watermark = conn1.wal_frame_count().unwrap();
            conn1.execute("INSERT INTO t VALUES (3, 4)")?;
            let conn1_sync_watermark = conn1.wal_frame_count().unwrap();
            conn1.execute("INSERT INTO t VALUES (5, 6)")?;

            let conn2 = db2.connect(&coro).await?;
            conn2.execute("CREATE TABLE t(x, y)")?;
            conn2.execute("INSERT INTO t VALUES (1, 2)")?;
            let conn2_match_watermark = conn2.wal_frame_count().unwrap();
            conn2.execute("INSERT INTO t VALUES (5, 6)")?;

            // db1 WAL frames: [A1 A2] [A3] [A4] (sync_watermark) [A5]
            // db2 WAL frames: [B1 B2] [B3] [B4]

            let session = WalSession::new(conn2);
            let conn2_sync_watermark = transfer_physical_changes(
                &coro,
                &db1,
                session,
                conn1_match_watermark,
                conn1_sync_watermark,
                conn2_match_watermark,
            )
            .await?;

            // db2 WAL frames: [B1 B2] [B3] [B4] [B4^-1] [A4] (sync_watermark) [A5]
            assert_eq!(conn2_sync_watermark, 6);

            let conn2 = db2.connect(&coro).await.unwrap();
            let mut rows = Vec::new();
            let mut stmt = conn2.prepare("SELECT x, y FROM t").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![Value::Integer(1), Value::Integer(2)],
                    vec![Value::Integer(3), Value::Integer(4)],
                    vec![Value::Integer(5), Value::Integer(6)],
                ]
            );

            conn2.execute("INSERT INTO t VALUES (7, 8)")?;
            let mut rows = Vec::new();
            let mut stmt = conn2.prepare("SELECT x, y FROM t").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![Value::Integer(1), Value::Integer(2)],
                    vec![Value::Integer(3), Value::Integer(4)],
                    vec![Value::Integer(5), Value::Integer(6)],
                    vec![Value::Integer(7), Value::Integer(8)],
                ]
            );

            Result::Ok(())
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.run_once().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }
}
