use std::{collections::HashMap, path::Path, sync::Arc};

use tokio::sync::Mutex;
use turso::{IntoParams, Value};

use crate::{
    errors::Error,
    sync_server::{DbSyncInfo, DbSyncStatus, Stream, SyncServer},
    test_context::TestContext,
    Result,
};

struct Generation {
    snapshot: Vec<u8>,
    frames: Vec<Vec<u8>>,
}

#[derive(Clone)]
struct SyncSession {
    baton: String,
    conn: turso::Connection,
    in_txn: bool,
}

struct TestSyncServerState {
    generation: usize,
    generations: HashMap<usize, Generation>,
    sessions: HashMap<String, SyncSession>,
}

#[derive(Debug, Clone)]
pub struct TestSyncServerOpts {
    pub pull_batch_size: usize,
}

#[derive(Clone)]
pub struct TestSyncServer {
    ctx: Arc<TestContext>,
    db: turso::Database,
    opts: Arc<TestSyncServerOpts>,
    state: Arc<Mutex<TestSyncServerState>>,
}

pub struct TestStream {
    ctx: Arc<TestContext>,
    data: Vec<u8>,
    position: usize,
}

impl TestStream {
    pub fn new(ctx: Arc<TestContext>, data: Vec<u8>) -> Self {
        Self {
            ctx,
            data,
            position: 0,
        }
    }
}

impl Stream for TestStream {
    async fn read_chunk(&mut self) -> Result<Option<hyper::body::Bytes>> {
        self.ctx
            .faulty_call(if self.position == 0 {
                "read_chunk_first"
            } else {
                "read_chunk_next"
            })
            .await?;
        let size = (self.data.len() - self.position).min(FRAME_SIZE);
        if size == 0 {
            Ok(None)
        } else {
            let chunk = &self.data[self.position..self.position + size];
            self.position += size;
            Ok(Some(hyper::body::Bytes::copy_from_slice(chunk)))
        }
    }
}

const PAGE_SIZE: usize = 4096;
const FRAME_SIZE: usize = 24 + PAGE_SIZE;

impl SyncServer for TestSyncServer {
    type Stream = TestStream;
    async fn db_info(&self) -> Result<DbSyncInfo> {
        self.ctx.faulty_call("db_info").await?;

        let state = self.state.lock().await;
        Ok(DbSyncInfo {
            current_generation: state.generation,
        })
    }

    async fn db_export(&self, generation_id: usize) -> Result<TestStream> {
        self.ctx.faulty_call("db_export").await?;

        let state = self.state.lock().await;
        let Some(generation) = state.generations.get(&generation_id) else {
            return Err(Error::DatabaseSyncError("generation not found".to_string()));
        };
        Ok(TestStream::new(
            self.ctx.clone(),
            generation.snapshot.clone(),
        ))
    }

    async fn wal_pull(&self, generation_id: usize, start_frame: usize) -> Result<TestStream> {
        tracing::debug!("wal_pull: {}/{}", generation_id, start_frame);
        self.ctx.faulty_call("wal_pull").await?;

        let state = self.state.lock().await;
        let Some(generation) = state.generations.get(&generation_id) else {
            return Err(Error::DatabaseSyncError("generation not found".to_string()));
        };
        let mut data = Vec::new();
        for frame_no in start_frame..start_frame + self.opts.pull_batch_size {
            let frame_idx = frame_no - 1;
            let Some(frame) = generation.frames.get(frame_idx) else {
                break;
            };
            data.extend_from_slice(frame);
        }
        if data.is_empty() {
            let last_generation = state.generations.get(&state.generation).unwrap();
            return Err(Error::PullNeedCheckpoint(DbSyncStatus {
                baton: None,
                status: "checkpoint_needed".to_string(),
                generation: state.generation,
                max_frame_no: last_generation.frames.len(),
            }));
        }
        Ok(TestStream::new(self.ctx.clone(), data))
    }

    async fn wal_push(
        &self,
        mut baton: Option<String>,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
        frames: Vec<u8>,
    ) -> Result<super::DbSyncStatus> {
        tracing::debug!(
            "wal_push: {}/{}/{}/{:?}",
            generation_id,
            start_frame,
            end_frame,
            baton
        );
        self.ctx.faulty_call("wal_push").await?;

        let mut session = {
            let mut state = self.state.lock().await;
            if state.generation != generation_id {
                return Err(Error::DatabaseSyncError(
                    "generation id mismatch".to_string(),
                ));
            }
            let baton_str = baton.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let session = match state.sessions.get(&baton_str) {
                Some(session) => session.clone(),
                None => {
                    let session = SyncSession {
                        baton: baton_str.clone(),
                        conn: self.db.connect()?,
                        in_txn: false,
                    };
                    state.sessions.insert(baton_str.clone(), session.clone());
                    session
                }
            };
            baton = Some(baton_str.clone());
            session
        };

        let mut offset = 0;
        for frame_no in start_frame..end_frame {
            if offset + FRAME_SIZE > frames.len() {
                return Err(Error::DatabaseSyncError(
                    "unexpected length of frames data".to_string(),
                ));
            }
            if !session.in_txn {
                session.conn.wal_insert_begin()?;
                session.in_txn = true;
            }
            let frame = &frames[offset..offset + FRAME_SIZE];
            match session.conn.wal_insert_frame(frame_no as u32, frame) {
                Ok(info) => {
                    if info.is_commit_frame() {
                        if session.in_txn {
                            session.conn.wal_insert_end()?;
                            session.in_txn = false;
                        }
                        self.sync_frames_from_conn(&session.conn).await?;
                    }
                }
                Err(turso::Error::WalOperationError(err)) if err.contains("Conflict") => {
                    session.conn.wal_insert_end()?;
                    return Err(Error::PushConflict);
                }
                Err(err) => {
                    session.conn.wal_insert_end()?;
                    return Err(err.into());
                }
            }
            offset += FRAME_SIZE;
        }
        let mut state = self.state.lock().await;
        state
            .sessions
            .insert(baton.clone().unwrap(), session.clone());
        Ok(DbSyncStatus {
            baton: Some(session.baton.clone()),
            status: "ok".into(),
            generation: state.generation,
            max_frame_no: session.conn.wal_frame_count()? as usize,
        })
    }
}

// empty DB with single 4096-byte page and WAL mode (PRAGMA journal_mode=WAL)
// see test test_empty_wal_mode_db_content which validates asset content
pub const EMPTY_WAL_MODE_DB: &[u8] = include_bytes!("empty_wal_mode.db");

pub async fn convert_rows(rows: &mut turso::Rows) -> Result<Vec<Vec<Value>>> {
    let mut rows_values = vec![];
    while let Some(row) = rows.next().await? {
        let mut row_values = vec![];
        for i in 0..row.column_count() {
            row_values.push(row.get_value(i)?);
        }
        rows_values.push(row_values);
    }
    Ok(rows_values)
}

impl TestSyncServer {
    pub async fn new(ctx: Arc<TestContext>, path: &Path, opts: TestSyncServerOpts) -> Result<Self> {
        let mut generations = HashMap::new();
        generations.insert(
            1,
            Generation {
                snapshot: EMPTY_WAL_MODE_DB.to_vec(),
                frames: Vec::new(),
            },
        );
        Ok(Self {
            ctx,
            db: turso::Builder::new_local(path.to_str().unwrap())
                .build()
                .await?,
            opts: Arc::new(opts),
            state: Arc::new(Mutex::new(TestSyncServerState {
                generation: 1,
                generations,
                sessions: HashMap::new(),
            })),
        })
    }
    pub fn db(&self) -> turso::Database {
        self.db.clone()
    }
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<()> {
        let conn = self.db.connect()?;
        conn.execute(sql, params).await?;
        self.sync_frames_from_conn(&conn).await?;
        Ok(())
    }
    async fn sync_frames_from_conn(&self, conn: &turso::Connection) -> Result<()> {
        let mut state = self.state.lock().await;
        let generation = state.generation;
        let generation = state.generations.get_mut(&generation).unwrap();
        let last_frame = generation.frames.len() + 1;
        let mut frame = [0u8; FRAME_SIZE];
        let wal_frame_count = conn.wal_frame_count()?;
        tracing::debug!("conn frames count: {}", wal_frame_count);
        for frame_no in last_frame..=wal_frame_count as usize {
            conn.wal_get_frame(frame_no as u32, &mut frame)?;
            tracing::debug!("push local frame {}", frame_no);
            generation.frames.push(frame.to_vec());
        }
        Ok(())
    }
}
