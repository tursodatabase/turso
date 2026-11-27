use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Result};
use base64::Engine;
use bytes::Bytes;
use itertools::Either;
use prost::Message;
use roaring::RoaringBitmap;
use tracing::{debug, error, info};

use turso_core::mvcc::persistent_storage::logical_log::frame_codec;
use turso_core::{
    CipherMode, ConnectOpts, Connection, Database, DatabaseOpts, EncryptionContext, OpenFlags,
    SessionRole, Value as CoreValue,
};
use turso_sqlite::SqliteDialect;
use turso_sync_engine::server_proto::{
    BatchCond, BatchResult, BatchStep, BatchStreamReq, BatchStreamResp, Col, Error,
    ExecuteStreamReq, ExecuteStreamResp, MvccLogicalLogMetadataProto, MvccLogicalLogRangeProto,
    PageData, PageSetRawEncodingProto, PageUpdatesEncodingReq, PipelineReqBody, PipelineRespBody,
    PullUpdatesApplyMode, PullUpdatesProtocol, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody,
    PullUpdatesStreamKind, Row, StmtResult, StreamRequest, StreamResponse, StreamResult, Value,
};
use turso_sync_engine::sync_encryption::SyncEncryption;

const WAL_FRAME_HEADER_SIZE: usize = 24;
const PAGE_SIZE: usize = 4096;
const MVCC_LOG_VERSION: u8 = 3;
const MVCC_LOG_HEADER_SIZE: usize = 56;
const MAX_HEADER_BYTES: usize = 32 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const QUEUED_CONNECTIONS_PER_WORKER: usize = 4;

pub struct OpenConfig {
    pub vfs: Option<String>,
    pub flags: OpenFlags,
    pub db_opts: DatabaseOpts,
    pub max_open: usize,
    pub encryption: Option<SyncEncryption>,
}

struct DbHandle {
    conn: Mutex<Arc<Connection>>,
    path: String,
}

struct OpenHandles {
    state: Mutex<HandleState>,
    opened: Condvar,
    capacity: usize,
    #[cfg(test)]
    opens: AtomicUsize,
}

struct HandleState {
    handles: HashMap<String, Arc<DbHandle>>,
    opening: HashSet<String>,
}

enum DbSource {
    Single(Arc<DbHandle>),
    Dir {
        base: PathBuf,
        config: Box<OpenConfig>,
        open_handles: OpenHandles,
    },
}

pub struct TursoSyncServer {
    address: String,
    source: DbSource,
    interrupt_count: Arc<AtomicUsize>,
    workers: usize,
    encryption: Option<SyncEncryption>,
}

struct ConnectionQueue {
    state: Mutex<QueueState>,
    ready: Condvar,
    capacity: usize,
}

#[derive(Default)]
struct QueueState {
    streams: VecDeque<TcpStream>,
    closed: bool,
}

impl TursoSyncServer {
    pub fn new(
        address: String,
        db_path: String,
        conn: Arc<Connection>,
        interrupt_count: Arc<AtomicUsize>,
        workers: usize,
        encryption: Option<SyncEncryption>,
    ) -> Result<Self> {
        require_server_key(encryption.as_ref())?;
        anyhow::ensure!(conn.get_encryption_cipher_mode() == encryption.as_ref().map(|e| e.cipher()),
            "encrypted sync requires matching --sync-encryption-cipher and --sync-encryption-key-file");
        conn.wal_auto_actions_disable();
        conn.set_portable_logical_changes_enabled(true);

        Ok(Self {
            address,
            source: DbSource::Single(Arc::new(DbHandle {
                conn: Mutex::new(conn),
                path: db_path,
            })),
            interrupt_count,
            workers,
            encryption,
        })
    }

    pub fn new_dir(
        address: String,
        base: PathBuf,
        interrupt_count: Arc<AtomicUsize>,
        config: OpenConfig,
        workers: usize,
    ) -> Result<Self> {
        require_server_key(config.encryption.as_ref())?;
        if !base.is_dir() {
            return Err(anyhow!(
                "sync dir path does not exist or is not a directory: {}",
                base.display()
            ));
        }
        let open_handles = OpenHandles::new(config.max_open);
        Ok(Self {
            address,
            encryption: config.encryption.clone(),
            source: DbSource::Dir {
                base: base.canonicalize()?,
                config: Box::new(config),
                open_handles,
            },
            interrupt_count,
            workers,
        })
    }

    fn resolve_db(
        &self,
        requested: Option<&str>,
    ) -> std::result::Result<Arc<DbHandle>, HttpResponse> {
        match (&self.source, requested) {
            (DbSource::Single(h), None) => Ok(h.clone()),
            (DbSource::Single(_), Some(_)) => Err(text_response(404, "Not Found")),
            (DbSource::Dir { .. }, None) => Err(text_response(404, "Not Found")),
            (
                DbSource::Dir {
                    base,
                    config,
                    open_handles,
                },
                Some(name),
            ) => {
                if !validate_db_name(name) {
                    return Err(text_response(400, "Invalid database name"));
                }
                let slot = match open_handles.reserve(name) {
                    SlotRequest::AlreadyOpen(handle) => return Ok(handle),
                    SlotRequest::Reserved(slot) => slot,
                    SlotRequest::Full => {
                        error!(
                            "refusing to open {name}: {} databases are open or opening",
                            config.max_open
                        );
                        return Err(text_response(503, "Too many open databases"));
                    }
                };
                let path = db_path_for(base, name);
                let dir = path.parent().expect("database path has a parent directory");
                if !config.flags.contains(OpenFlags::ReadOnly) {
                    if let Err(err) = std::fs::create_dir_all(dir) {
                        error!("failed to create directory for database {name}: {err}");
                        return Err(text_response(500, &format!("Internal Server Error: {err}")));
                    }
                }
                if !dir.canonicalize().is_ok_and(|dir| dir.starts_with(base)) {
                    return Err(text_response(404, "Not Found"));
                }
                #[cfg(test)]
                open_handles.opens.fetch_add(1, Ordering::SeqCst);
                match open_db_handle(&path, config) {
                    Ok(handle) => Ok(slot.fill(handle)),
                    // Sync clients retry a 500 forever but can act on a 404.
                    Err(err) if path.exists() => {
                        error!("failed to open database {name}: {err}");
                        Err(text_response(500, &format!("Internal Server Error: {err}")))
                    }
                    Err(err) => {
                        debug!("no database named {name}: {err}");
                        Err(text_response(404, "Not Found"))
                    }
                }
            }
        }
    }

    pub fn run(self) -> Result<()> {
        let listener = self.bind()?;
        Arc::new(self).serve(listener)
    }

    fn bind(&self) -> Result<TcpListener> {
        info!("Starting TursoSyncServer on {}", self.address);

        let listener = TcpListener::bind(&self.address)?;
        listener.set_nonblocking(true)?;
        Ok(listener)
    }

    fn serve(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        let interrupt_count = self.interrupt_count.clone();
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();

        let monitor_handle = thread::spawn(move || loop {
            if interrupt_count.load(Ordering::SeqCst) > 0 {
                debug!("Interrupt detected, signaling shutdown");
                shutdown_flag_clone.store(true, Ordering::SeqCst);
                break;
            }
            thread::sleep(Duration::from_millis(100));
        });

        let queue = Arc::new(ConnectionQueue::new(
            self.workers * QUEUED_CONNECTIONS_PER_WORKER,
        ));
        let mut workers = Vec::with_capacity(self.workers);
        for _ in 0..self.workers {
            let server = self.clone();
            let queue = queue.clone();
            workers.push(thread::spawn(move || server.work(&queue)));
        }

        loop {
            if shutdown_flag.load(Ordering::SeqCst) {
                info!("Shutdown signal received, stopping server");
                break;
            }
            if queue.is_full() {
                thread::sleep(Duration::from_millis(10));
                continue;
            }

            match listener.accept() {
                Ok((stream, addr)) => {
                    info!("Accepted connection from {}", addr);
                    queue.push(stream);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }

        queue.close();
        for worker in workers {
            let _ = worker.join();
        }
        let _ = monitor_handle.join();
        info!("TursoSyncServer stopped");
        Ok(())
    }

    fn work(self: Arc<Self>, queue: &ConnectionQueue) {
        while let Some(stream) = queue.next() {
            let handled =
                std::panic::catch_unwind(AssertUnwindSafe(|| self.handle_connection(stream)));
            match handled {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("Error handling connection: {}", e),
                Err(_) => {
                    error!("a request panicked, aborting rather than reusing a database connection left mid-statement");
                    std::process::abort();
                }
            }
        }
    }

    fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(REQUEST_TIMEOUT))?;
        stream.set_write_timeout(Some(REQUEST_TIMEOUT))?;

        let mut buffer = [0u8; 8192];
        let mut request_data = Vec::new();

        loop {
            let n = stream.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            // Bytes before this offset hold no terminator, and one can still
            // straddle the last three of them.
            let unscanned = request_data.len().saturating_sub(3);
            request_data.extend_from_slice(&buffer[..n]);

            let Some(header_end) = find_header_end(&request_data, unscanned) else {
                if request_data.len() > MAX_HEADER_BYTES {
                    return Err(anyhow!(
                        "HTTP request headers exceed {MAX_HEADER_BYTES} bytes"
                    ));
                }
                continue;
            };
            let headers = String::from_utf8_lossy(&request_data[..header_end]);
            if let Some(content_length) = parse_content_length(&headers) {
                let total_expected = request_end(header_end, content_length)?;
                while request_data.len() < total_expected {
                    let n = stream.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    request_data.extend_from_slice(&buffer[..n]);
                }
            }
            break;
        }

        let response = self.handle_request(&request_data);
        let http_response = match response {
            Ok(resp) => resp,
            Err(e) => {
                error!("Request error: {}", e);
                text_response(500, &format!("Internal Server Error: {e}"))
            }
        };
        stream.write_all(&format_http_response(&http_response))?;
        stream.flush()?;
        Ok(())
    }

    fn handle_request(&self, request_data: &[u8]) -> Result<HttpResponse> {
        let (method, path, body) = parse_http_request(request_data)?;
        info!("Request: {} {}", method, path);
        let route = parse_route(&method, &path);
        let invalid_credentials = match route {
            Route::Pipeline { .. } => {
                validate_encryption_headers(request_data, self.encryption.as_ref(), false).is_err()
            }
            Route::PullUpdates { .. } => {
                validate_encryption_headers(request_data, self.encryption.as_ref(), true).is_err()
            }
            Route::Options | Route::NotFound => false,
        };
        if invalid_credentials {
            return Ok(text_response(
                400,
                "Invalid or missing sync encryption credentials",
            ));
        }
        match route {
            Route::Options => Ok(text_response(204, "")),
            Route::Pipeline { db } => match self.resolve_db(db) {
                Ok(handle) => {
                    debug!("Handling /v2/pipeline request");
                    self.handle_pipeline(&handle, &body)
                }
                Err(resp) => Ok(resp),
            },
            Route::PullUpdates { db } => match self.resolve_db(db) {
                Ok(handle) => {
                    debug!("Handling /pull-updates request");
                    self.handle_pull_updates(&handle, &body)
                }
                Err(resp) => Ok(resp),
            },
            Route::NotFound => {
                info!("Unknown endpoint: {} {}", method, path);
                Ok(text_response(404, "Not Found"))
            }
        }
    }

    fn handle_pipeline(&self, db: &DbHandle, body: &[u8]) -> Result<HttpResponse> {
        let req: PipelineReqBody = serde_json::from_slice(body)
            .map_err(|e| anyhow!("Failed to parse pipeline request: {}", e))?;

        debug!("Pipeline request: {:?}", req);

        let conn = db.conn.lock().expect("database connection lock poisoned");

        let mut results = Vec::new();

        for request in req.requests {
            let result = match request {
                StreamRequest::Execute(exec_req) => self.execute_statement(&conn, &exec_req),
                StreamRequest::Batch(batch_req) => self.execute_batch(&conn, &batch_req),
                StreamRequest::None => StreamResult::Error {
                    error: Error {
                        message: "Unknown request type".to_string(),
                        code: "UNKNOWN".to_string(),
                    },
                },
            };
            results.push(result);
        }

        let resp = PipelineRespBody {
            baton: req.baton,
            base_url: None,
            results,
        };

        let body = serde_json::to_vec(&resp)?;

        Ok(HttpResponse {
            status: 200,
            content_type: "application/json".to_string(),
            body,
        })
    }

    fn execute_statement(&self, conn: &Arc<Connection>, req: &ExecuteStreamReq) -> StreamResult {
        let sql = match &req.stmt.sql {
            Some(s) => s.clone(),
            None => {
                return StreamResult::Error {
                    error: Error {
                        message: "No SQL provided".to_string(),
                        code: "NO_SQL".to_string(),
                    },
                }
            }
        };

        debug!("Executing SQL: {}", sql);

        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to prepare statement: {}", e);
                return StreamResult::Error {
                    error: Error {
                        message: e.to_string(),
                        code: "PREPARE_ERROR".to_string(),
                    },
                };
            }
        };

        for (i, arg) in req.stmt.args.iter().enumerate() {
            let core_value = convert_value_to_core(arg);
            if let Err(err) = stmt.bind_at(std::num::NonZero::new(i + 1).unwrap(), core_value) {
                error!("Failed to bind statement argument: {}", err);
                return StreamResult::Error {
                    error: Error {
                        message: err.to_string(),
                        code: "BIND_ERROR".to_string(),
                    },
                };
            }
        }

        let want_rows = req.stmt.want_rows.unwrap_or(true);

        if want_rows {
            match stmt.run_collect_rows() {
                Ok(rows) => {
                    let cols: Vec<Col> = (0..stmt.num_columns())
                        .map(|i| Col {
                            name: Some(stmt.get_column_name(i).to_string()),
                            decltype: stmt.get_column_decltype(i),
                        })
                        .collect();

                    let result_rows: Vec<Row> = rows
                        .into_iter()
                        .map(|row| Row {
                            values: row.into_iter().map(convert_core_to_value).collect(),
                        })
                        .collect();

                    StreamResult::Ok {
                        response: StreamResponse::Execute(ExecuteStreamResp {
                            result: StmtResult {
                                cols,
                                rows: result_rows,
                                affected_row_count: 0,
                                last_insert_rowid: None,
                                replication_index: None,
                                rows_read: 0,
                                rows_written: 0,
                                query_duration_ms: 0.0,
                            },
                        }),
                    }
                }
                Err(e) => {
                    error!("Failed to execute statement: {}", e);
                    StreamResult::Error {
                        error: Error {
                            message: e.to_string(),
                            code: "EXECUTE_ERROR".to_string(),
                        },
                    }
                }
            }
        } else {
            match stmt.run_ignore_rows() {
                Ok(()) => StreamResult::Ok {
                    response: StreamResponse::Execute(ExecuteStreamResp {
                        result: StmtResult {
                            cols: vec![],
                            rows: vec![],
                            affected_row_count: 0,
                            last_insert_rowid: None,
                            replication_index: None,
                            rows_read: 0,
                            rows_written: 0,
                            query_duration_ms: 0.0,
                        },
                    }),
                },
                Err(e) => {
                    error!("Failed to execute statement: {}", e);
                    StreamResult::Error {
                        error: Error {
                            message: e.to_string(),
                            code: "EXECUTE_ERROR".to_string(),
                        },
                    }
                }
            }
        }
    }

    fn execute_batch(&self, conn: &Arc<Connection>, req: &BatchStreamReq) -> StreamResult {
        let batch = &req.batch;
        let mut step_results: Vec<Option<StmtResult>> = Vec::with_capacity(batch.steps.len());
        let mut step_errors: Vec<Option<Error>> = Vec::with_capacity(batch.steps.len());

        for (step_idx, step) in batch.steps.iter().enumerate() {
            let should_execute = match &step.condition {
                None => true,
                Some(cond) => Self::evaluate_condition(cond, &step_results, &step_errors, conn),
            };

            if should_execute {
                let result = self.execute_batch_step(conn, step);
                match result {
                    Ok(stmt_result) => {
                        step_results.push(Some(stmt_result));
                        step_errors.push(None);
                    }
                    Err(e) => {
                        error!("Batch step {} failed: {}", step_idx, e);
                        step_results.push(None);
                        step_errors.push(Some(Error {
                            message: e.to_string(),
                            code: "BATCH_STEP_ERROR".to_string(),
                        }));
                    }
                }
            } else {
                step_results.push(None);
                step_errors.push(None);
            }
        }

        StreamResult::Ok {
            response: StreamResponse::Batch(BatchStreamResp {
                result: BatchResult {
                    step_results,
                    step_errors,
                    replication_index: None,
                },
            }),
        }
    }

    fn evaluate_condition(
        cond: &BatchCond,
        step_results: &[Option<StmtResult>],
        step_errors: &[Option<Error>],
        conn: &Arc<Connection>,
    ) -> bool {
        match cond {
            BatchCond::None => true,
            BatchCond::Ok { step } => {
                let idx = *step as usize;
                idx < step_results.len() && step_results[idx].is_some()
            }
            BatchCond::Error { step } => {
                let idx = *step as usize;
                idx < step_errors.len() && step_errors[idx].is_some()
            }
            BatchCond::Not { cond } => {
                !Self::evaluate_condition(cond, step_results, step_errors, conn)
            }
            BatchCond::And(list) => list
                .conds
                .iter()
                .all(|c| Self::evaluate_condition(c, step_results, step_errors, conn)),
            BatchCond::Or(list) => list
                .conds
                .iter()
                .any(|c| Self::evaluate_condition(c, step_results, step_errors, conn)),
            BatchCond::IsAutocommit {} => conn.get_auto_commit(),
        }
    }

    fn execute_batch_step(&self, conn: &Arc<Connection>, step: &BatchStep) -> Result<StmtResult> {
        let sql = step
            .stmt
            .sql
            .as_ref()
            .ok_or_else(|| anyhow!("No SQL in batch step"))?;

        debug!("Executing batch step SQL: {}", sql);

        let mut stmt = conn.prepare(sql)?;

        for (i, arg) in step.stmt.args.iter().enumerate() {
            let core_value = convert_value_to_core(arg);
            stmt.bind_at(std::num::NonZero::new(i + 1).unwrap(), core_value)?;
        }

        let want_rows = step.stmt.want_rows.unwrap_or(true);

        if want_rows {
            let rows = stmt.run_collect_rows()?;

            let cols: Vec<Col> = (0..stmt.num_columns())
                .map(|i| Col {
                    name: Some(stmt.get_column_name(i).to_string()),
                    decltype: stmt.get_column_decltype(i),
                })
                .collect();

            let result_rows: Vec<Row> = rows
                .into_iter()
                .map(|row| Row {
                    values: row.into_iter().map(convert_core_to_value).collect(),
                })
                .collect();

            Ok(StmtResult {
                cols,
                rows: result_rows,
                affected_row_count: 0,
                last_insert_rowid: None,
                replication_index: None,
                rows_read: 0,
                rows_written: 0,
                query_duration_ms: 0.0,
            })
        } else {
            stmt.run_ignore_rows()?;
            Ok(StmtResult {
                cols: vec![],
                rows: vec![],
                affected_row_count: 0,
                last_insert_rowid: None,
                replication_index: None,
                rows_read: 0,
                rows_written: 0,
                query_duration_ms: 0.0,
            })
        }
    }

    fn handle_pull_updates(&self, db: &DbHandle, body: &[u8]) -> Result<HttpResponse> {
        let req = <PullUpdatesReqProtoBody as Message>::decode(body)
            .map_err(|e| anyhow!("Failed to decode PullUpdatesRequest: {}", e))?;
        if self.encryption.is_some()
            && (!req.server_pages_selector.is_empty() || !req.server_query_selector.is_empty())
        {
            return Ok(text_response(
                400,
                "Encrypted sync requires an unfiltered bootstrap",
            ));
        }

        debug!(
            "Pull updates request: server_revision={}, client_revision={}",
            req.server_revision, req.client_revision
        );

        let encoding =
            PageUpdatesEncodingReq::try_from(req.encoding).unwrap_or(PageUpdatesEncodingReq::Raw);

        if encoding == PageUpdatesEncodingReq::Zstd {
            return Err(anyhow!("Zstd encoding is not supported"));
        }

        if PullUpdatesStreamKind::try_from(req.stream_kind).unwrap_or(PullUpdatesStreamKind::Pages)
            == PullUpdatesStreamKind::MvccLogicalLog
        {
            return self.handle_logical_pull_updates(db, &req);
        }

        self.handle_page_pull_updates(db, &req, PullUpdatesApplyMode::Incremental)
    }

    fn handle_page_pull_updates(
        &self,
        db: &DbHandle,
        req: &PullUpdatesReqProtoBody,
        apply_mode: PullUpdatesApplyMode,
    ) -> Result<HttpResponse> {
        let conn = db.conn.lock().expect("database connection lock poisoned");

        let wal_state = conn.wal_state()?;
        let server_revision: u64 = if req.server_revision.is_empty() {
            wal_state.max_frame
        } else {
            req.server_revision.parse().unwrap_or(wal_state.max_frame)
        };
        let pages_selector: Option<RoaringBitmap> = if !req.server_pages_selector.is_empty() {
            Some(
                RoaringBitmap::deserialize_from(&req.server_pages_selector[..])
                    .map_err(|e| anyhow!("Failed to parse server_pages_selector: {}", e))?,
            )
        } else {
            None
        };
        if req.client_revision.is_empty() && req.server_query_selector.is_empty() {
            let protocol = if conn.mvcc_enabled() {
                PullUpdatesProtocol::MvccLogical
            } else {
                PullUpdatesProtocol::Pages
            };
            let revision = if conn.mvcc_enabled() {
                "g1:o0".to_string()
            } else {
                server_revision.to_string()
            };
            drop(conn);
            return self.handle_snapshot_pages(
                db,
                revision,
                apply_mode,
                protocol,
                Some(server_revision),
                pages_selector.as_ref(),
            );
        }
        debug!("WAL state: max_frame={}", wal_state.max_frame);

        let client_revision: u64 = if req.client_revision.is_empty() {
            0
        } else {
            req.client_revision.parse().unwrap_or(0)
        };

        debug!(
            "Using server_revision={}, client_revision={}",
            server_revision, client_revision
        );

        let mut seen_pages: HashSet<u32> = HashSet::new();
        let mut pages_to_send: Vec<(u32, Vec<u8>)> = Vec::new();

        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let mut frame_buffer = vec![0u8; frame_size];

        debug!(
            "pull-updates: scanning WAL frames {}..={} (client_revision={}, server_revision={})",
            client_revision + 1,
            server_revision,
            client_revision,
            server_revision
        );

        if server_revision > client_revision {
            for frame_no in (client_revision + 1..=server_revision).rev() {
                let frame_info = conn.wal_get_frame(frame_no, &mut frame_buffer)?;

                let page_no = frame_info.page_no;
                // WAL uses 1-based page numbers, sync protocol uses 0-based
                let page_id = page_no - 1;

                if seen_pages.contains(&page_no) {
                    continue;
                }

                if let Some(ref selector) = pages_selector {
                    if !selector.contains(page_id) {
                        continue;
                    }
                }

                seen_pages.insert(page_no);

                let type_byte = frame_buffer[WAL_FRAME_HEADER_SIZE];
                debug!(
                    "pull-updates: including page_no={}, frame_no={}, type_byte={}, db_size={}",
                    page_no, frame_no, type_byte, frame_info.db_size
                );

                // Core's raw-frame API already authenticates and decodes the page.
                let page_data = frame_buffer[WAL_FRAME_HEADER_SIZE..].to_vec();
                pages_to_send.push((page_id, page_data));
            }
        }

        debug!(
            "pull-updates: sending {} pages, seen_pages={:?}",
            pages_to_send.len(),
            seen_pages
        );
        pages_to_send.reverse();

        let db_size = current_snapshot_db_size_pages(&conn, wal_state.max_frame)?;

        let header = PullUpdatesRespProtoBody {
            server_revision: server_revision.to_string(),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: apply_mode as i32,
            mvcc_log: None,
            // The protocol hint reflects the database, not the response shape:
            // page bootstraps of an MVCC database advertise MvccLogical so
            // auto-detecting clients switch to logical pulls, mirroring the
            // production server.
            protocol: if conn.mvcc_enabled() {
                PullUpdatesProtocol::MvccLogical as i32
            } else {
                PullUpdatesProtocol::Pages as i32
            },
        };

        let mut response_body = Vec::new();

        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);

        for (page_id, page_data) in pages_to_send {
            let page_msg = PageData {
                page_id: page_id as u64,
                encoded_page: Bytes::from(page_data),
            };
            let page_bytes = page_msg.encode_to_vec();
            encode_length_delimited(&mut response_body, &page_bytes);
        }

        debug!(
            "Sending {} bytes in pull-updates response",
            response_body.len()
        );

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
        })
    }

    fn handle_logical_pull_updates(
        &self,
        db: &DbHandle,
        req: &PullUpdatesReqProtoBody,
    ) -> Result<HttpResponse> {
        let default_log_path = match logical_log_path(&db.path) {
            Ok(path) => path,
            Err(_) if is_in_memory_db_path(&db.path) => {
                info!(
                    "logical pull requested for in-memory sync server database; returning incremental pages"
                );
                return self.handle_page_pull_updates(db, req, PullUpdatesApplyMode::Incremental);
            }
            Err(err) => return Err(err),
        };
        let (db_size, fallback_revision, legacy_current_revision, log_path, log) = {
            let conn = db.conn.lock().expect("database connection lock poisoned");
            let wal_state = conn.wal_state()?;
            // A database that an older version opened in MVCC mode can use a
            // log with the old name, so read the file that core writes.
            let log_path = conn
                .mv_store()
                .as_ref()
                .and_then(|mv_store| mv_store.logical_log_path().map(Path::to_path_buf))
                .unwrap_or(default_log_path);
            // The log is read under the connection lock so that no commit or
            // checkpoint changes it between the snapshot and the read.
            let log = std::fs::read(&log_path).map(|mut log| {
                if let Some(mv_store) = conn.mv_store().as_ref() {
                    // Recovery ends the log at the last valid frame without
                    // truncating the file, and the next commit overwrites
                    // only part of the bytes after it. Serve only the
                    // frames up to the write offset, as core reads them.
                    // Core reports offset 0 until its first commit, while the
                    // file already holds the header, so keep the header.
                    let offset = mv_store.logical_log_offset() as usize;
                    log.truncate(offset.max(MVCC_LOG_HEADER_SIZE));
                }
                log
            });
            (
                current_snapshot_db_size_pages(&conn, wal_state.max_frame)?,
                format!("page:{}", wal_state.max_frame),
                wal_state.max_frame.to_string(),
                log_path,
                log,
            )
        };
        let log = match log {
            Ok(log) => log,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    "logical pull requested but no MVCC log exists at {}; returning replace-base fallback",
                    log_path.display()
                );
                return self.handle_logical_fallback(
                    db,
                    req,
                    fallback_revision,
                    &legacy_current_revision,
                    db_size,
                );
            }
            Err(err) => return Err(err.into()),
        };
        if log.len() == MVCC_LOG_HEADER_SIZE {
            frame_codec::validate_log_header(&log)?;
            let start = parse_mvcc_revision_offset(&req.client_revision, log.len() as u64)?;
            anyhow::ensure!(
                start == 0 || start == log.len() as u64,
                "MVCC logical pull offset is not a transaction boundary"
            );
            // A header-only log has no transactions to normalize, including
            // an older LML2 log awaiting its first portable transaction.
            return self.handle_empty_logical_pull(format!("g1:o{}", log.len()), db_size);
        }
        let crypto = self
            .encryption
            .as_ref()
            .map(SyncEncryption::local_context)
            .transpose()?
            .flatten();
        let normalized = match normalize_mvcc_log(&log, crypto.as_ref()) {
            Ok(normalized) => normalized,
            Err(err) if is_nonportable_mvcc_log_error(&err) => {
                return self.handle_logical_fallback(
                    db,
                    req,
                    fallback_revision,
                    &legacy_current_revision,
                    db_size,
                );
            }
            Err(err) => return Err(err),
        };
        let snapshot = &normalized.snapshot;
        let start_offset = parse_mvcc_revision_offset(&req.client_revision, snapshot.end_offset)?;
        if start_offset > snapshot.end_offset {
            return Err(anyhow!(
                "MVCC logical pull revision is from the future: client_offset={} server_offset={}",
                start_offset,
                snapshot.end_offset
            ));
        }
        let start = *normalized
            .wire_offsets
            .get(&start_offset)
            .ok_or_else(|| anyhow!("MVCC offset is not a transaction boundary"))?;
        let log = normalized.body.as_ref();
        let end = log.len();

        let mut response_body = Vec::new();
        let mvcc_log = if start == end {
            None
        } else {
            let crc_seed = if start_offset == 0 {
                None
            } else {
                let seed = snapshot.crc_seed_at(start_offset)?;
                Some(seed.to_le_bytes().to_vec())
            };
            Some(MvccLogicalLogMetadataProto {
                format: "lml3".to_string(),
                checkpoint_transition: false,
                ranges: vec![MvccLogicalLogRangeProto {
                    generation: 1,
                    start_offset,
                    end_offset: snapshot.end_offset,
                    starts_with_header: start_offset == 0,
                    crc_seed,
                }],
            })
        };

        let header = PullUpdatesRespProtoBody {
            server_revision: format!("g1:o{}", snapshot.end_offset),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };

        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);
        response_body.extend_from_slice(&log[start..end]);

        debug!(
            "pull-updates logical: path={} client_revision={} end_offset={} body_bytes={}",
            log_path.display(),
            req.client_revision,
            snapshot.end_offset,
            end - start
        );

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
        })
    }

    fn handle_logical_fallback(
        &self,
        db: &DbHandle,
        req: &PullUpdatesReqProtoBody,
        server_revision: String,
        legacy_current_revision: &str,
        db_size: u64,
    ) -> Result<HttpResponse> {
        if req.client_revision == server_revision {
            return self.handle_empty_logical_pull(server_revision, db_size);
        }
        if req.client_revision == legacy_current_revision {
            return self.handle_empty_logical_pull(req.client_revision.clone(), db_size);
        }

        self.handle_replace_base_pages(db, server_revision)
    }

    fn handle_empty_logical_pull(
        &self,
        server_revision: String,
        db_size: u64,
    ) -> Result<HttpResponse> {
        let header = PullUpdatesRespProtoBody {
            server_revision,
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };

        let mut response_body = Vec::new();
        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
        })
    }

    fn handle_replace_base_pages(
        &self,
        db: &DbHandle,
        server_revision: String,
    ) -> Result<HttpResponse> {
        self.handle_snapshot_pages(
            db,
            server_revision,
            PullUpdatesApplyMode::ReplaceBase,
            PullUpdatesProtocol::MvccLogical,
            None,
            None,
        )
    }

    fn handle_snapshot_pages(
        &self,
        db: &DbHandle,
        server_revision: String,
        apply_mode: PullUpdatesApplyMode,
        protocol: PullUpdatesProtocol,
        frame_watermark: Option<u64>,
        pages_selector: Option<&RoaringBitmap>,
    ) -> Result<HttpResponse> {
        let (db_size, pages) = self.read_snapshot_pages(db, frame_watermark, pages_selector)?;

        let header = PullUpdatesRespProtoBody {
            server_revision,
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: apply_mode as i32,
            mvcc_log: None,
            protocol: protocol as i32,
        };

        let mut response_body = Vec::new();
        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);

        for (page_id, page) in pages {
            let page_msg = PageData {
                page_id,
                encoded_page: Bytes::from(page),
            };
            let page_bytes = page_msg.encode_to_vec();
            encode_length_delimited(&mut response_body, &page_bytes);
        }

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
        })
    }

    #[allow(clippy::type_complexity)]
    fn read_snapshot_pages(
        &self,
        db: &DbHandle,
        frame_watermark: Option<u64>,
        pages_selector: Option<&RoaringBitmap>,
    ) -> Result<(u64, Vec<(u64, Vec<u8>)>)> {
        let conn = db.conn.lock().expect("database connection lock poisoned");
        let wal_state = conn.wal_state()?;
        let max_frame = frame_watermark.unwrap_or(wal_state.max_frame);
        anyhow::ensure!(
            max_frame <= wal_state.max_frame,
            "requested WAL revision is no longer available"
        );
        let db_size = current_snapshot_db_size_pages(&conn, max_frame)?;
        let pages_capacity = pages_selector.map_or(db_size, |pages| pages.len().min(db_size));
        let pages_capacity = usize::try_from(pages_capacity)
            .map_err(|_| anyhow!("database page count does not fit usize: {db_size}"))?;
        let mut pages = Vec::with_capacity(pages_capacity);
        let mut historical_pages = HashMap::new();
        if conn
            .get_pager()
            .wal_backfill_frame()
            .is_some_and(|backfill| max_frame < backfill)
            && pages_capacity > 0
        {
            // A checkpoint can advance the database file past this revision
            // while its older page versions are still retained in the WAL.
            let mut frame = vec![0; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
            for frame_no in (1..=max_frame).rev() {
                let info = conn.wal_get_frame(frame_no, &mut frame)?;
                let page_id = info
                    .page_no
                    .checked_sub(1)
                    .ok_or_else(|| anyhow!("WAL frame has an invalid page number"))?;
                if u64::from(page_id) >= db_size
                    || pages_selector.is_some_and(|selected| !selected.contains(page_id))
                {
                    continue;
                }
                historical_pages
                    .entry(u64::from(page_id))
                    .or_insert_with(|| frame[WAL_FRAME_HEADER_SIZE..].to_vec());
                if historical_pages.len() == pages_capacity {
                    break;
                }
            }
        }

        let page_ids = match pages_selector {
            Some(selected) => Either::Left(selected.iter().map(u64::from)),
            None => Either::Right(0..db_size),
        };
        for page_id in page_ids.take_while(|&id| id < db_size) {
            if let Some(page) = historical_pages.remove(&page_id) {
                pages.push((page_id, page));
                continue;
            }
            let page_no = page_id + 1;
            let page_no_u32 = u32::try_from(page_no)
                .map_err(|_| anyhow!("database page number does not fit u32: {page_no}"))?;
            let mut page = vec![0; PAGE_SIZE];
            let found =
                conn.try_wal_watermark_read_page(page_no_u32, &mut page, Some(max_frame))?;
            if !found {
                return Err(anyhow!(
                    "database page {} is missing from replace-base snapshot",
                    page_no
                ));
            }
            pages.push((page_id, page));
        }

        Ok((db_size, pages))
    }
}

struct HttpResponse {
    status: u16,
    content_type: String,
    body: Vec<u8>,
}

fn require_server_key(encryption: Option<&SyncEncryption>) -> Result<()> {
    anyhow::ensure!(
        encryption.is_none_or(|encryption| encryption.local_key().is_some()),
        "sync server encryption requires a key"
    );
    Ok(())
}

fn validate_encryption_headers(
    request: &[u8],
    encryption: Option<&SyncEncryption>,
    require_cipher: bool,
) -> Result<()> {
    let end = find_header_end(request, 0).ok_or_else(|| anyhow!("missing HTTP headers"))?;
    let headers = String::from_utf8_lossy(&request[..end]);
    let mut key = None;
    let mut cipher = None;
    for line in headers.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let slot = if name.eq_ignore_ascii_case("x-turso-encryption-key") {
            &mut key
        } else if name.eq_ignore_ascii_case("x-turso-encryption-cipher") {
            &mut cipher
        } else {
            continue;
        };
        anyhow::ensure!(
            slot.replace(value.trim()).is_none(),
            "duplicate encryption header"
        );
    }
    let Some(encryption) = encryption else {
        anyhow::ensure!(
            key.is_none() && cipher.is_none(),
            "database is not encrypted"
        );
        return Ok(());
    };
    if let Some(cipher) = cipher {
        anyhow::ensure!(
            CipherMode::try_from(cipher)? == encryption.cipher(),
            "cipher mismatch"
        );
    } else {
        anyhow::ensure!(!require_cipher, "missing encryption cipher");
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(key.ok_or_else(|| anyhow!("missing encryption key"))?)?;
    anyhow::ensure!(
        bytes.as_slice()
            == encryption
                .local_key()
                .ok_or_else(|| anyhow!("sync server encryption requires a key"))?
                .as_slice(),
        "encryption key mismatch"
    );
    Ok(())
}

struct NormalizedMvccLog<'a> {
    snapshot: MvccLogSnapshot,
    body: Cow<'a, [u8]>,
    wire_offsets: HashMap<u64, usize>,
}

fn normalize_mvcc_log<'a>(
    log: &'a [u8],
    crypto: Option<&EncryptionContext>,
) -> Result<NormalizedMvccLog<'a>> {
    if log.is_empty() {
        return Ok(NormalizedMvccLog {
            snapshot: MvccLogSnapshot {
                end_offset: 0,
                crc_by_offset: vec![(0, 0)],
            },
            body: Cow::Borrowed(log),
            wire_offsets: HashMap::from([(0, 0)]),
        });
    }
    anyhow::ensure!(
        log.len() >= MVCC_LOG_HEADER_SIZE,
        "truncated MVCC log header"
    );
    let header = frame_codec::validate_log_header(&log[..MVCC_LOG_HEADER_SIZE])?;
    // Only a valid older header may trigger replacement; corruption must fail.
    anyhow::ensure!(
        header.version() == MVCC_LOG_VERSION,
        "unsupported MVCC logical log version {}",
        header.version()
    );
    let mut source_crc = header.initial_crc();
    let mut wire_crc = source_crc;
    let mut offset = MVCC_LOG_HEADER_SIZE;
    let mut body = crypto.map(|_| header.bytes().to_vec());
    let mut wire_offsets = HashMap::from([(0, 0), (offset as u64, offset)]);
    let mut crc_by_offset = vec![(offset as u64, wire_crc)];
    let mut plaintext = Vec::new();
    // A crash may leave an incomplete frame after the last committed boundary.
    while offset < log.len() {
        let Some(layout) = frame_codec::inspect_frame_prefix(&log[offset..], crypto)? else {
            break;
        };
        let end = offset
            .checked_add(layout.stored_size())
            .ok_or_else(|| anyhow!("MVCC frame end overflows usize"))?;
        if end > log.len() {
            break;
        }
        let frame = frame_codec::validate_frame_into(
            &header,
            offset as u64,
            &log[offset..end],
            source_crc,
            crypto,
            &mut plaintext,
        )?;
        source_crc = frame.source_crc();
        if let Some(body) = &mut body {
            let wire = frame_codec::plaintext_wire_parts(&frame, wire_crc);
            body.extend_from_slice(wire.header);
            body.extend_from_slice(wire.body);
            body.extend_from_slice(&wire.trailer);
            wire_crc = wire.next_crc;
        } else {
            wire_crc = source_crc;
        }
        offset = end;
        wire_offsets.insert(offset as u64, body.as_ref().map_or(offset, Vec::len));
        crc_by_offset.push((offset as u64, wire_crc));
    }
    Ok(NormalizedMvccLog {
        snapshot: MvccLogSnapshot {
            end_offset: offset as u64,
            crc_by_offset,
        },
        // Plaintext frames are already in wire format; borrow the complete prefix.
        body: body.map_or_else(|| Cow::Borrowed(&log[..offset]), Cow::Owned),
        wire_offsets,
    })
}

struct MvccLogSnapshot {
    end_offset: u64,
    crc_by_offset: Vec<(u64, u32)>,
}

impl MvccLogSnapshot {
    fn crc_seed_at(&self, offset: u64) -> Result<u32> {
        self.crc_by_offset
            .iter()
            .find_map(|(boundary, crc)| (*boundary == offset).then_some(*crc))
            .ok_or_else(|| {
                anyhow!("MVCC logical pull offset is not a transaction boundary: {offset}")
            })
    }
}

fn logical_log_path(db_path: &str) -> Result<PathBuf> {
    Ok(turso_core::logical_log_path(db_file_path(db_path)?))
}

fn is_in_memory_db_path(db_path: &str) -> bool {
    db_path == ":memory:"
}

fn db_file_path(db_path: &str) -> Result<PathBuf> {
    if is_in_memory_db_path(db_path) {
        return Err(anyhow!(
            "MVCC logical pull is not supported for in-memory sync server databases"
        ));
    }
    let path = if let Some(rest) = db_path.strip_prefix("file:") {
        rest.split_once('?').map_or(rest, |(path, _)| path)
    } else {
        db_path
    };
    Ok(PathBuf::from(path))
}

fn parse_mvcc_revision_offset(revision: &str, legacy_default: u64) -> Result<u64> {
    if revision.is_empty() {
        return Ok(0);
    }
    if let Some((generation, offset)) = revision.split_once(":o") {
        let generation = generation
            .strip_prefix('g')
            .ok_or_else(|| anyhow!("invalid MVCC pull revision generation: {revision}"))?
            .parse::<u64>()
            .map_err(|err| anyhow!("invalid MVCC pull revision generation: {revision}: {err}"))?;
        if generation != 1 {
            return Err(anyhow!(
                "sync_server supports only single-generation MVCC logical pulls: {revision}"
            ));
        }
        return offset
            .parse::<u64>()
            .map_err(|err| anyhow!("invalid MVCC pull revision offset: {revision}: {err}"));
    }
    // Older page bootstrap responses from this test server used WAL frame
    // numbers. Treat them as "the page snapshot already includes the current
    // logical log" so the required follow-up logical pull becomes a no-op.
    Ok(legacy_default)
}

fn is_nonportable_mvcc_log_error(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    message.starts_with("unsupported MVCC logical log version ")
}

fn current_db_size_pages(conn: &Connection, max_frame: u64) -> Result<u64> {
    if max_frame > 0 {
        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let mut last_frame = vec![0u8; frame_size];
        let last_info = conn.wal_get_frame(max_frame, &mut last_frame)?;
        Ok(last_info.db_size as u64)
    } else {
        Ok(0)
    }
}

fn current_snapshot_db_size_pages(conn: &Connection, max_frame: u64) -> Result<u64> {
    if max_frame > 0 {
        return current_db_size_pages(conn, max_frame);
    }

    let mut page = vec![0u8; PAGE_SIZE];
    if conn.try_wal_watermark_read_page(1, &mut page, Some(max_frame))? {
        Ok(db_size_from_page(&page) as u64)
    } else {
        Ok(0)
    }
}

fn db_size_from_page(page: &[u8]) -> u32 {
    u32::from_be_bytes(page[28..32].try_into().unwrap())
}

/// A client controls Content-Length, so the end of the body has to be
/// computed without trusting it to fit.
fn request_end(header_end: usize, content_length: usize) -> Result<usize> {
    (header_end + 4)
        .checked_add(content_length)
        .ok_or_else(|| anyhow!("HTTP request length overflows: {content_length}"))
}

fn find_header_end(data: &[u8], start: usize) -> Option<usize> {
    (start..data.len().saturating_sub(3)).find(|&i| &data[i..i + 4] == b"\r\n\r\n")
}

fn parse_content_length(headers: &str) -> Option<usize> {
    for line in headers.lines() {
        let lower = line.to_lowercase();
        if lower.starts_with("content-length:") {
            let value = line.split(':').nth(1)?.trim();
            return value.parse().ok();
        }
    }
    None
}

fn parse_http_request(data: &[u8]) -> Result<(String, String, Vec<u8>)> {
    let header_end = find_header_end(data, 0).ok_or_else(|| anyhow!("Invalid HTTP request"))?;
    let headers = String::from_utf8_lossy(&data[..header_end]);

    let first_line = headers
        .lines()
        .next()
        .ok_or_else(|| anyhow!("Empty request"))?;
    let parts: Vec<&str> = first_line.split_whitespace().collect();

    if parts.len() < 2 {
        return Err(anyhow!("Invalid request line"));
    }

    let method = parts[0].to_string();
    let path = parts[1].to_string();
    let body = data[header_end + 4..].to_vec();

    Ok((method, path, body))
}

fn text_response(status: u16, body: &str) -> HttpResponse {
    HttpResponse {
        status,
        content_type: "text/plain".to_string(),
        body: body.as_bytes().to_vec(),
    }
}

fn format_http_response(resp: &HttpResponse) -> Vec<u8> {
    let status_text = match resp.status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    };

    let header = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: *\r\n\
         Access-Control-Expose-Headers: *\r\n\
         \r\n",
        resp.status,
        status_text,
        resp.content_type,
        resp.body.len()
    );

    let mut result = header.into_bytes();
    result.extend_from_slice(&resp.body);
    result
}

#[derive(Debug, PartialEq, Eq)]
enum Route<'a> {
    Pipeline { db: Option<&'a str> },
    PullUpdates { db: Option<&'a str> },
    Options,
    NotFound,
}

fn parse_route<'a>(method: &str, path: &'a str) -> Route<'a> {
    if method == "OPTIONS" {
        return Route::Options;
    }
    if method != "POST" {
        return Route::NotFound;
    }
    match path {
        "/v2/pipeline" => return Route::Pipeline { db: None },
        "/pull-updates" => return Route::PullUpdates { db: None },
        _ => {}
    }
    let Some(rest) = path.strip_prefix("/db/") else {
        return Route::NotFound;
    };
    let Some((name, tail)) = rest.split_once('/') else {
        return Route::NotFound;
    };
    match tail {
        "v2/pipeline" => Route::Pipeline { db: Some(name) },
        "pull-updates" => Route::PullUpdates { db: Some(name) },
        _ => Route::NotFound,
    }
}

fn validate_db_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

fn db_path_for(base: &Path, name: &str) -> PathBuf {
    // Extensionless like sqld's layout: the files appear as data, data-wal,
    // data-shm (and data.db-log under MVCC) inside the database's directory.
    base.join(name).join("data")
}

impl OpenHandles {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(HandleState {
                handles: HashMap::new(),
                opening: HashSet::new(),
            }),
            opened: Condvar::new(),
            capacity,
            #[cfg(test)]
            opens: AtomicUsize::new(0),
        }
    }

    fn reserve<'a>(&'a self, name: &'a str) -> SlotRequest<'a> {
        let mut state = self.state.lock().expect("open handles lock poisoned");
        loop {
            if let Some(handle) = state.handles.get(name) {
                return SlotRequest::AlreadyOpen(handle.clone());
            }
            if !state.opening.contains(name) {
                break;
            }
            state = self.opened.wait(state).expect("open handles lock poisoned");
        }
        if state.handles.len() + state.opening.len() >= self.capacity {
            return SlotRequest::Full;
        }
        state.opening.insert(name.to_string());
        SlotRequest::Reserved(OpenSlot {
            open_handles: self,
            name,
            filled: false,
        })
    }
}

enum SlotRequest<'a> {
    AlreadyOpen(Arc<DbHandle>),
    Reserved(OpenSlot<'a>),
    Full,
}

struct OpenSlot<'a> {
    open_handles: &'a OpenHandles,
    name: &'a str,
    filled: bool,
}

impl OpenSlot<'_> {
    fn fill(mut self, handle: Arc<DbHandle>) -> Arc<DbHandle> {
        let mut state = self
            .open_handles
            .state
            .lock()
            .expect("open handles lock poisoned");
        let replaced = state.handles.insert(self.name.to_string(), handle.clone());
        assert!(
            replaced.is_none(),
            "database {} was opened twice",
            self.name
        );
        state.opening.remove(self.name);
        self.filled = true;
        self.open_handles.opened.notify_all();
        handle
    }
}

impl Drop for OpenSlot<'_> {
    fn drop(&mut self) {
        if self.filled {
            return;
        }
        let mut state = self
            .open_handles
            .state
            .lock()
            .expect("open handles lock poisoned");
        state.opening.remove(self.name);
        self.open_handles.opened.notify_all();
    }
}

impl ConnectionQueue {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(QueueState::default()),
            ready: Condvar::new(),
            capacity,
        }
    }

    fn is_full(&self) -> bool {
        self.locked().streams.len() >= self.capacity
    }

    fn push(&self, stream: TcpStream) {
        let mut state = self.locked();
        assert!(
            state.streams.len() < self.capacity,
            "connection queue accepted past its capacity of {}",
            self.capacity
        );
        state.streams.push_back(stream);
        self.ready.notify_one();
    }

    fn close(&self) {
        self.locked().closed = true;
        self.ready.notify_all();
    }

    fn next(&self) -> Option<TcpStream> {
        let mut state = self.locked();
        loop {
            if let Some(stream) = state.streams.pop_front() {
                return Some(stream);
            }
            if state.closed {
                return None;
            }
            state = self
                .ready
                .wait(state)
                .expect("connection queue lock poisoned");
        }
    }

    fn locked(&self) -> MutexGuard<'_, QueueState> {
        self.state.lock().expect("connection queue lock poisoned")
    }
}

fn open_db_handle(path: &Path, config: &OpenConfig) -> Result<Arc<DbHandle>> {
    require_server_key(config.encryption.as_ref())?;
    let path_str = path.to_string_lossy().to_string();
    let (_io, db) = Database::open_new(
        &path_str,
        config.vfs.as_deref(),
        config.flags,
        config
            .db_opts
            .with_encryption(config.encryption.is_some() || config.db_opts.enable_encryption),
        config
            .encryption
            .as_ref()
            .and_then(SyncEncryption::local_options),
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(config.encryption.as_ref().map_or_else(
        || ConnectOpts::new(SessionRole::Superuser),
        SyncEncryption::connect_options,
    ))?;
    anyhow::ensure!(
        conn.get_encryption_cipher_mode() == config.encryption.as_ref().map(|e| e.cipher()),
        "database cipher does not match the sync server configuration"
    );
    conn.set_portable_logical_changes_enabled(true);
    conn.wal_auto_actions_disable();
    Ok(Arc::new(DbHandle {
        conn: Mutex::new(conn),
        path: path_str,
    }))
}

fn encode_length_delimited(output: &mut Vec<u8>, data: &[u8]) {
    let mut len = data.len();
    while len >= 0x80 {
        output.push((len as u8) | 0x80);
        len >>= 7;
    }
    output.push(len as u8);
    output.extend_from_slice(data);
}

fn convert_value_to_core(value: &Value) -> CoreValue {
    match value {
        Value::None | Value::Null => CoreValue::Null,
        Value::Integer { value } => CoreValue::from_i64(*value),
        Value::Float { value } => CoreValue::from_f64(*value),
        Value::Text { value } => CoreValue::Text(turso_core::types::Text {
            value: std::borrow::Cow::Owned(value.clone()),
            subtype: turso_core::types::TextSubtype::Text,
        }),
        Value::Blob { value } => CoreValue::Blob(value.to_vec()),
    }
}

fn convert_core_to_value(value: CoreValue) -> Value {
    match value {
        CoreValue::Null => Value::Null,
        CoreValue::Numeric(turso_core::Numeric::Integer(v)) => Value::Integer { value: v },
        CoreValue::Numeric(turso_core::Numeric::Float(v)) => Value::Float {
            value: f64::from(v),
        },
        CoreValue::Text(t) => Value::Text {
            value: t.value.to_string(),
        },
        CoreValue::Blob(b) => Value::Blob {
            value: Bytes::from(b),
        },
    }
}

#[cfg(test)]
mod tests {
    use turso_core::EncryptionKey;
    const MVCC_LOG_HEADER_CRC_START: usize = 52;
    const MVCC_TX_EXT_HEADER_SIZE: usize = 40;
    const MVCC_TX_TRAILER_SIZE: usize = 8;
    use super::*;
    use std::path::Path;

    #[test]
    fn server_rejects_encryption_without_a_key_before_opening() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data");
        let config = OpenConfig {
            vfs: None,
            flags: OpenFlags::Create,
            db_opts: DatabaseOpts::new(),
            max_open: 1,
            encryption: Some(SyncEncryption::new(CipherMode::Aes256Gcm, None, None).unwrap()),
        };
        let error = open_db_handle(&path, &config).err().expect("missing key");
        assert!(error
            .to_string()
            .contains("sync server encryption requires a key"));
        assert!(!path.exists());
        let error = TursoSyncServer::new_dir(
            "127.0.0.1:0".into(),
            dir.path().into(),
            Arc::new(AtomicUsize::new(0)),
            config,
            1,
        )
        .err()
        .expect("missing key");
        assert!(error
            .to_string()
            .contains("sync server encryption requires a key"));
    }

    #[test]
    fn plaintext_requests_accept_non_utf8_unrelated_headers() {
        let dir = tempfile::tempdir().unwrap();
        let server = dir_server(dir.path());
        for (endpoint, body) in [("v2/pipeline", SELECT_ONE_PIPELINE), ("pull-updates", "")] {
            let mut request =
                format!("POST /db/plaintext/{endpoint} HTTP/1.1\r\nUser-Agent: caf").into_bytes();
            request.extend_from_slice(b"\xe9\r\n");
            request.extend_from_slice(
                format!("Content-Length: {}\r\n\r\n{body}", body.len()).as_bytes(),
            );
            let response = server.handle_request(&request).unwrap();
            assert_eq!(response.status, 200, "{endpoint}");
            if endpoint == "v2/pipeline" {
                let response: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
                assert_eq!(response["results"][0]["type"], "ok");
            }
        }
    }

    #[test]
    fn encrypted_pipeline_accepts_key_only_but_pull_requires_cipher() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = dir_server(dir.path());
        let encryption = SyncEncryption::new(
            CipherMode::Aes256Gcm,
            Some(EncryptionKey::new_256([11; 32])),
            None,
        )
        .unwrap();
        server.encryption = Some(encryption.clone());
        if let DbSource::Dir { config, .. } = &mut server.source {
            config.encryption = Some(encryption);
        }
        let key = base64::engine::general_purpose::STANDARD.encode([11; 32]);
        let key_header = format!("x-turso-encryption-key: {key}\r\n");
        let request = |endpoint: &str, headers: &str, body: &str| {
            format!(
                "POST /db/key-only/{endpoint} HTTP/1.1\r\n{headers}Content-Length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        let pull = request("pull-updates", &key_header, "");
        assert_eq!(server.handle_request(pull.as_bytes()).unwrap().status, 400);
        assert!(!dir.path().join("key-only").exists());

        // RemoteWriter's SQL requests carry the cloud key without a cipher.
        let body =
            r#"{"requests":[{"type":"execute","stmt":{"sql":"CREATE TABLE secret(value TEXT)"}}]}"#;
        let pipeline = request("v2/pipeline", &key_header, body);
        let response = server.handle_request(pipeline.as_bytes()).unwrap();
        assert_eq!(response.status, 200);
        let response: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(response["results"][0]["type"], "ok");

        let wrong_key = base64::engine::general_purpose::STANDARD.encode([12; 32]);
        for headers in [
            String::new(),
            key_header.replace(&key, &wrong_key),
            format!("{key_header}x-turso-encryption-cipher: aegis256\r\n"),
            format!("{key_header}x-turso-encryption-cipher: invalid\r\n"),
            format!("{key_header}{key_header}"),
        ] {
            let pipeline = request("v2/pipeline", &headers, body);
            assert_eq!(
                server.handle_request(pipeline.as_bytes()).unwrap().status,
                400
            );
        }
        // Cached connections must also accept the key-only form.
        let body = r#"{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO secret VALUES ('remote write')"}}]}"#;
        let pipeline = request("v2/pipeline", &key_header, body);
        let response = server.handle_request(pipeline.as_bytes()).unwrap();
        assert_eq!(response.status, 200);
        let response: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(response["results"][0]["type"], "ok");
    }

    #[test]
    fn encrypted_requests_validate_credentials_before_opening_or_writing() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = dir_server(dir.path());
        let encryption = SyncEncryption::new(
            CipherMode::Aes256Gcm,
            Some(EncryptionKey::new_256([11; 32])),
            None,
        )
        .unwrap();
        server.encryption = Some(encryption.clone());
        if let DbSource::Dir { config, .. } = &mut server.source {
            config.encryption = Some(encryption);
        }
        let key = base64::engine::general_purpose::STANDARD.encode([11; 32]);
        let valid =
            format!("X-Turso-Encryption-Key: {key}\r\nx-turso-encryption-cipher: aes256gcm\r\n");
        for endpoint in ["pull-updates", "v2/pipeline"] {
            for headers in [
                String::new(),
                valid.replace(
                    &key,
                    &base64::engine::general_purpose::STANDARD.encode([12; 32]),
                ),
                valid.replace("aes256gcm", "aegis256"),
                format!("{valid}x-turso-encryption-key: {key}\r\n"),
                valid.replace(&key, "invalid"),
            ] {
                let request = format!(
                    "POST /db/rejected/{endpoint} HTTP/1.1\r\n{headers}Content-Length: 0\r\n\r\n"
                );
                assert_eq!(
                    server.handle_request(request.as_bytes()).unwrap().status,
                    400
                );
                assert!(!dir.path().join("rejected").exists());
            }
        }
        let body =
            r#"{"requests":[{"type":"execute","stmt":{"sql":"CREATE TABLE secret(value TEXT)"}}]}"#;
        let request = format!(
            "POST /db/accepted/v2/pipeline HTTP/1.1\r\n{valid}Content-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert_eq!(
            server.handle_request(request.as_bytes()).unwrap().status,
            200
        );
        assert!(dir.path().join("accepted/data").exists());
        // Validation also runs when the database is already cached.
        let request = request.replace(&valid, "");
        assert_eq!(
            server.handle_request(request.as_bytes()).unwrap().status,
            400
        );
        assert!(validate_encryption_headers(
            format!("POST / HTTP/1.1\r\n{valid}\r\n").as_bytes(),
            None,
            false
        )
        .is_err());
    }

    #[test]
    fn encrypted_pull_rejects_corrupt_header_instead_of_replacing_base() {
        for cipher in [CipherMode::Aes256Gcm, CipherMode::Aegis256] {
            let dir = tempfile::tempdir().unwrap();
            let encryption =
                SyncEncryption::new(cipher, Some(EncryptionKey::new_256([11; 32])), None).unwrap();
            let path = dir.path().join("data");
            let handle = open_db_handle(
                &path,
                &OpenConfig {
                    vfs: None,
                    flags: OpenFlags::Create,
                    db_opts: DatabaseOpts::new(),
                    max_open: 1,
                    encryption: Some(encryption.clone()),
                },
            )
            .unwrap();
            {
                let conn = handle.conn.lock().unwrap();
                conn.execute("PRAGMA journal_mode=mvcc").unwrap();
                conn.execute("CREATE TABLE secrets(value TEXT)").unwrap();
                conn.execute("INSERT INTO secrets VALUES ('committed secret')")
                    .unwrap();
            }
            let mut server = dir_server(dir.path());
            server.encryption = Some(encryption);
            let request = PullUpdatesReqProtoBody {
                stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
                client_revision: "g1:o0".into(),
                ..Default::default()
            }
            .encode_to_vec();
            let log_path = logical_log_path(path.to_str().unwrap()).unwrap();
            let original = std::fs::read(&log_path).unwrap();
            assert_eq!(original[4], MVCC_LOG_VERSION);
            assert!(original.len() > MVCC_LOG_HEADER_SIZE);
            let crypto = server.encryption.as_ref().unwrap().local_context().unwrap();
            let normalized = normalize_mvcc_log(&original, crypto.as_ref()).unwrap();
            let boundaries = &normalized.snapshot.crc_by_offset;
            let committed = boundaries[boundaries.len() - 2].0;
            let start = committed as usize;
            let layout = frame_codec::inspect_frame_prefix(&original[start..], crypto.as_ref())
                .unwrap()
                .unwrap();
            for end in [
                start + 1,
                start + 3,
                start + layout.header_size() - 1,
                start + layout.header_size(),
                original.len() - 1,
            ] {
                std::fs::write(&log_path, &original[..end]).unwrap();
                let response = server.handle_pull_updates(&handle, &request).unwrap();
                assert_eq!(response.status, 200);
                let mut bytes = response.body.as_slice();
                let header = PullUpdatesRespProtoBody::decode_length_delimited(&mut bytes).unwrap();
                assert_eq!(header.server_revision, format!("g1:o{committed}"));
                assert_eq!(
                    bytes,
                    &normalized.body[..normalized.wire_offsets[&committed]]
                );
            }
            for version in [2, 4] {
                let mut corrupt = original.clone();
                corrupt[4] = version;
                std::fs::write(&log_path, &corrupt).unwrap();
                let result = server.handle_pull_updates(&handle, &request);
                assert!(
                    result.is_err(),
                    "corrupt version {version} must not return replacement pages"
                );
                assert!(!is_nonportable_mvcc_log_error(&result.err().unwrap()));
            }
            for version in [2, MVCC_LOG_VERSION] {
                let mut header_only = original[..MVCC_LOG_HEADER_SIZE].to_vec();
                header_only[4] = version;
                header_only[MVCC_LOG_HEADER_CRC_START..].fill(0);
                let crc = crc32c::crc32c(&header_only);
                header_only[MVCC_LOG_HEADER_CRC_START..].copy_from_slice(&crc.to_le_bytes());
                if version == 2 {
                    let error = normalize_mvcc_log(
                        &header_only,
                        server
                            .encryption
                            .as_ref()
                            .unwrap()
                            .local_context()
                            .unwrap()
                            .as_ref(),
                    )
                    .err()
                    .expect("valid older log must request replacement");
                    assert!(is_nonportable_mvcc_log_error(&error));
                }
                std::fs::write(&log_path, header_only).unwrap();
                let response = server.handle_pull_updates(&handle, &request).unwrap();
                let mut bytes = response.body.as_slice();
                let header = PullUpdatesRespProtoBody::decode_length_delimited(&mut bytes).unwrap();
                assert_eq!(
                    header.server_revision,
                    format!("g1:o{MVCC_LOG_HEADER_SIZE}")
                );
                assert_eq!(header.apply_mode, PullUpdatesApplyMode::Incremental as i32);
                assert_eq!(
                    header.stream_kind,
                    PullUpdatesStreamKind::MvccLogicalLog as i32
                );
                assert!(bytes.is_empty());
            }
            std::fs::write(log_path, original).unwrap();
        }
    }

    #[test]
    fn encrypted_log_normalization_preserves_source_boundaries_and_authenticates() {
        for cipher in [
            CipherMode::Aes128Gcm,
            CipherMode::Aes256Gcm,
            CipherMode::Aegis128L,
            CipherMode::Aegis128X2,
            CipherMode::Aegis128X4,
            CipherMode::Aegis256,
            CipherMode::Aegis256X2,
            CipherMode::Aegis256X4,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let encryption = SyncEncryption::new(
                cipher,
                Some(if cipher.required_key_size() == 16 {
                    EncryptionKey::new_128([11; 16])
                } else {
                    EncryptionKey::new_256([11; 32])
                }),
                None,
            )
            .unwrap();
            let path = dir.path().join("data");
            let handle = open_db_handle(
                &path,
                &OpenConfig {
                    vfs: None,
                    flags: OpenFlags::Create,
                    db_opts: DatabaseOpts::new(),
                    max_open: 1,
                    encryption: Some(encryption.clone()),
                },
            )
            .unwrap();
            let conn = handle.conn.lock().unwrap();
            conn.execute("PRAGMA journal_mode=mvcc").unwrap();
            conn.execute("CREATE TABLE secrets(value TEXT)").unwrap();
            conn.execute(format!(
                "INSERT INTO secrets VALUES ('{}')",
                "private payload ".repeat(8000)
            ))
            .unwrap();
            let log = std::fs::read(logical_log_path(path.to_str().unwrap()).unwrap()).unwrap();
            let normalized =
                normalize_mvcc_log(&log, encryption.local_context().unwrap().as_ref()).unwrap();
            assert_eq!(normalized.snapshot.end_offset, log.len() as u64);
            assert!(normalized.body.len() < log.len());
            let wire = normalize_mvcc_log(&normalized.body, None).unwrap();
            assert_eq!(wire.body, normalized.body);
            let wire_snapshot = wire.snapshot;
            assert_eq!(wire_snapshot.end_offset, normalized.body.len() as u64);
            let mut corrupt_wire = normalized.body.to_vec();
            corrupt_wire[MVCC_LOG_HEADER_SIZE + MVCC_TX_EXT_HEADER_SIZE] ^= 1;
            assert!(normalize_mvcc_log(&corrupt_wire, None).is_err());
            let truncated_wire =
                normalize_mvcc_log(&normalized.body[..normalized.body.len() - 1], None).unwrap();
            let boundaries = &wire_snapshot.crc_by_offset;
            assert_eq!(
                truncated_wire.snapshot.end_offset,
                boundaries[boundaries.len() - 2].0
            );
            for (source_offset, crc) in &normalized.snapshot.crc_by_offset {
                let wire_offset = normalized.wire_offsets[source_offset];
                assert_eq!(wire_snapshot.crc_seed_at(wire_offset as u64).unwrap(), *crc);
            }
            assert!(normalized.body.windows(15).any(|w| w == b"private payload"));
            assert!(!log.windows(15).any(|w| w == b"private payload"));
            let truncated = normalize_mvcc_log(
                &log[..log.len() - 1],
                encryption.local_context().unwrap().as_ref(),
            )
            .unwrap();
            let boundaries = &normalized.snapshot.crc_by_offset;
            assert_eq!(
                truncated.snapshot.end_offset,
                boundaries[boundaries.len() - 2].0
            );
            let wrong_key = EncryptionContext::new(
                cipher,
                &if cipher.required_key_size() == 16 {
                    EncryptionKey::new_128([12; 16])
                } else {
                    EncryptionKey::new_256([12; 32])
                },
                PAGE_SIZE,
            )
            .unwrap();
            assert!(normalize_mvcc_log(&log, Some(&wrong_key)).is_err());
            let mut corrupt = log.clone();
            corrupt[MVCC_LOG_HEADER_SIZE + MVCC_TX_EXT_HEADER_SIZE + 5] ^= 1;
            assert!(
                normalize_mvcc_log(&corrupt, encryption.local_context().unwrap().as_ref()).is_err()
            );
            // Recomputing the unkeyed frame CRC must not bypass AEAD authentication.
            let header = frame_codec::validate_log_header(&log[..MVCC_LOG_HEADER_SIZE]).unwrap();
            let layout = frame_codec::inspect_frame_prefix(
                &log[MVCC_LOG_HEADER_SIZE..],
                encryption.local_context().unwrap().as_ref(),
            )
            .unwrap()
            .unwrap();
            let trailer = MVCC_LOG_HEADER_SIZE + layout.stored_size() - MVCC_TX_TRAILER_SIZE;
            let crc = crc32c::crc32c_append(
                header.initial_crc(),
                &corrupt[MVCC_LOG_HEADER_SIZE..trailer],
            );
            corrupt[trailer..trailer + 4].copy_from_slice(&crc.to_le_bytes());
            assert!(
                normalize_mvcc_log(&corrupt, encryption.local_context().unwrap().as_ref())
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("authentication failed")
            );
        }
    }

    #[test]
    fn bootstrap_pages_advertise_sync_support() {
        for (mvcc, encrypted) in [(false, false), (true, false), (false, true), (true, true)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.db");
            let encryption = encrypted.then(|| {
                SyncEncryption::new(
                    CipherMode::Aes256Gcm,
                    Some(EncryptionKey::new_256([11; 32])),
                    None,
                )
                .unwrap()
            });
            let handle = open_db_handle(
                &path,
                &OpenConfig {
                    vfs: None,
                    flags: OpenFlags::Create,
                    db_opts: DatabaseOpts::new(),
                    max_open: 1,
                    encryption: encryption.clone(),
                },
            )
            .unwrap();
            {
                let conn = handle.conn.lock().unwrap();
                if mvcc {
                    conn.execute("PRAGMA journal_mode=mvcc").unwrap();
                }
                conn.execute("CREATE TABLE secrets(value TEXT)").unwrap();
                conn.execute("INSERT INTO secrets VALUES ('sample private value')")
                    .unwrap();
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            }
            let mut server = dir_server(dir.path());
            server.encryption = encryption;
            let request = PullUpdatesReqProtoBody::default().encode_to_vec();
            let response = server.handle_pull_updates(&handle, &request).unwrap();
            let mut bytes = response.body.as_slice();
            let header = PullUpdatesRespProtoBody::decode_length_delimited(&mut bytes).unwrap();
            assert!(header.db_size > 0);
            assert_eq!(header.apply_mode, PullUpdatesApplyMode::Incremental as i32);
            assert_eq!(
                header.protocol,
                if mvcc {
                    PullUpdatesProtocol::MvccLogical
                } else {
                    PullUpdatesProtocol::Pages
                } as i32,
            );
            let first = PageData::decode_length_delimited(&mut bytes).unwrap();
            assert!(first.encoded_page.starts_with(b"SQLite format 3\0"));

            let request = PullUpdatesReqProtoBody {
                client_revision: header.server_revision,
                stream_kind: if mvcc {
                    PullUpdatesStreamKind::MvccLogicalLog
                } else {
                    PullUpdatesStreamKind::Pages
                } as i32,
                ..Default::default()
            }
            .encode_to_vec();
            let response = server.handle_pull_updates(&handle, &request).unwrap();
            let mut bytes = response.body.as_slice();
            let incremental =
                PullUpdatesRespProtoBody::decode_length_delimited(&mut bytes).unwrap();
            assert_eq!(incremental.db_size, header.db_size);
            assert!(bytes.is_empty());
        }
    }

    #[test]
    fn plaintext_server_commits_have_portable_changes() {
        for directory in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let server = plaintext_server(dir.path(), directory);
            let handle = server
                .resolve_db(directory.then_some("source"))
                .unwrap_or_else(|_| panic!("open source"));
            let conn = handle.conn.lock().unwrap();
            conn.execute("PRAGMA journal_mode=mvcc").unwrap();
            conn.execute("CREATE TABLE items(value TEXT)").unwrap();
            conn.execute("INSERT INTO items VALUES ('portable row')")
                .unwrap();
            let log = std::fs::read(logical_log_path(&handle.path).unwrap()).unwrap();
            let normalized = normalize_mvcc_log(&log, None).unwrap();
            let mut offset = MVCC_LOG_HEADER_SIZE;
            while offset < normalized.body.len() {
                let frame = frame_codec::inspect_frame_prefix(&normalized.body[offset..], None)
                    .unwrap()
                    .unwrap();
                assert!(
                    frame.extension_size() > 0,
                    "recovery-only frame in directory={directory}"
                );
                offset += frame.stored_size();
            }
            assert!(offset > MVCC_LOG_HEADER_SIZE);
        }
    }

    #[test]
    fn plaintext_bootstrap_honors_page_selector_and_revision() {
        let dir = tempfile::tempdir().unwrap();
        let server = plaintext_server(dir.path(), false);
        let handle = server
            .resolve_db(None)
            .unwrap_or_else(|_| panic!("open source"));
        let mut selector = RoaringBitmap::new();
        selector.insert(1);
        selector.insert(u32::MAX);
        let mut selector_bytes = Vec::new();
        selector.serialize_into(&mut selector_bytes).unwrap();
        let mut request = PullUpdatesReqProtoBody {
            server_pages_selector: selector_bytes.into(),
            ..Default::default()
        };
        {
            let conn = handle.conn.lock().unwrap();
            conn.execute("CREATE TABLE items(value TEXT)").unwrap();
            conn.execute("INSERT INTO items VALUES ('old value')")
                .unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        for pinned in [false, true] {
            if pinned {
                let conn = handle.conn.lock().unwrap();
                conn.execute("INSERT INTO items VALUES ('another old value')")
                    .unwrap();
                request.server_revision = conn.wal_state().unwrap().max_frame.to_string();
                conn.execute("UPDATE items SET value = 'new value'")
                    .unwrap();
            }
            let response = server
                .handle_pull_updates(&handle, &request.encode_to_vec())
                .unwrap();
            let mut bytes = response.body.as_slice();
            let header = PullUpdatesRespProtoBody::decode_length_delimited(&mut bytes).unwrap();
            assert_eq!(header.db_size, 2);
            if pinned {
                assert_eq!(header.server_revision, request.server_revision);
            }
            let page = PageData::decode_length_delimited(&mut bytes).unwrap();
            assert_eq!(page.page_id, 1);
            assert!(bytes.is_empty(), "only the selected page may be returned");
            assert!(page.encoded_page.windows(9).any(|w| w == b"old value"));
            assert!(!page.encoded_page.windows(9).any(|w| w == b"new value"));
        }
    }

    #[test]
    fn plaintext_pinned_pages_survive_nontruncating_checkpoints() {
        for mode in ["PASSIVE", "FULL"] {
            let dir = tempfile::tempdir().unwrap();
            let server = plaintext_server(dir.path(), false);
            let handle = server
                .resolve_db(None)
                .unwrap_or_else(|_| panic!("open source"));
            let mut selector = Vec::new();
            RoaringBitmap::from_iter([1, u32::MAX])
                .serialize_into(&mut selector)
                .unwrap();
            let revision = {
                let conn = handle.conn.lock().unwrap();
                conn.execute("CREATE TABLE items(value TEXT)").unwrap();
                conn.execute("INSERT INTO items VALUES ('old value')")
                    .unwrap();
                conn.wal_state().unwrap().max_frame.to_string()
            };
            let request = PullUpdatesReqProtoBody {
                server_revision: revision,
                server_pages_selector: selector.into(),
                ..Default::default()
            }
            .encode_to_vec();
            let original = server.handle_pull_updates(&handle, &request).unwrap();
            {
                let conn = handle.conn.lock().unwrap();
                conn.execute("UPDATE items SET value = 'new value'")
                    .unwrap();
                conn.execute(
                    "INSERT INTO items SELECT printf('%0500d', value) FROM generate_series(1, 200)",
                )
                .unwrap();
                conn.execute(format!("PRAGMA wal_checkpoint({mode})"))
                    .unwrap();
            }
            let response = server.handle_pull_updates(&handle, &request).unwrap();
            assert_eq!(response.body, original.body, "{mode}");
        }
    }

    #[test]
    fn pinned_pages_missing_from_retained_wal_do_not_read_newer_main() {
        let dir = tempfile::tempdir().unwrap();
        let server = plaintext_server(dir.path(), false);
        let handle = server
            .resolve_db(None)
            .unwrap_or_else(|_| panic!("open source"));
        let revision = {
            let conn = handle.conn.lock().unwrap();
            conn.execute("CREATE TABLE items(value TEXT)").unwrap();
            conn.execute("INSERT INTO items VALUES ('old value')")
                .unwrap();
            conn.execute("CREATE TABLE other(value TEXT)").unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            conn.execute("INSERT INTO other VALUES ('pin')").unwrap();
            let revision = conn.wal_state().unwrap().max_frame;
            conn.execute("UPDATE items SET value = 'new value'")
                .unwrap();
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
            revision
        };
        let selector = RoaringBitmap::from_iter([1]);
        assert!(server
            .read_snapshot_pages(&handle, Some(revision), Some(&selector))
            .is_err());
    }

    fn pull_logical(
        server: &TursoSyncServer,
        handle: &DbHandle,
        revision: &str,
    ) -> Result<(PullUpdatesRespProtoBody, Vec<u8>)> {
        let request = PullUpdatesReqProtoBody {
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            client_revision: revision.into(),
            ..Default::default()
        }
        .encode_to_vec();
        let response = server.handle_pull_updates(handle, &request)?;
        assert_eq!(response.status, 200);
        let mut bytes = response.body.as_slice();
        let header = PullUpdatesRespProtoBody::decode_length_delimited(&mut bytes)?;
        Ok((header, bytes.to_vec()))
    }

    #[test]
    fn bytes_after_the_last_valid_frame_do_not_fail_logical_pulls() {
        let dir = tempfile::tempdir().unwrap();
        let log_path;
        let torn_frame;
        {
            let server = plaintext_server(dir.path(), false);
            let handle = server
                .resolve_db(None)
                .unwrap_or_else(|_| panic!("open source"));
            log_path = logical_log_path(&handle.path).unwrap();
            let conn = handle.conn.lock().unwrap();
            conn.execute("PRAGMA journal_mode=mvcc").unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'small')").unwrap();
            let start = std::fs::metadata(&log_path).unwrap().len();
            conn.execute("INSERT INTO t VALUES (2, printf('%020000d', 2))")
                .unwrap();
            torn_frame = start..std::fs::metadata(&log_path).unwrap().len();
            conn.close().unwrap();
        }
        // A crash during the write of the last frame leaves half of it on disk.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&log_path)
            .unwrap();
        file.set_len(torn_frame.start + (torn_frame.end - torn_frame.start) / 2)
            .unwrap();
        drop(file);

        let log_end = {
            let server = plaintext_server(dir.path(), false);
            let handle = server
                .resolve_db(None)
                .unwrap_or_else(|_| panic!("open source"));
            let (header, _) =
                pull_logical(&server, &handle, "g1:o0").expect("a torn last frame is skipped");
            assert_eq!(header.server_revision, format!("g1:o{}", torn_frame.start));
            // Recovery ended the log before the torn frame, so this commit
            // overwrites the start of it and the rest of it stays after the
            // new frame.
            let conn = handle.conn.lock().unwrap().clone();
            conn.execute("INSERT INTO t VALUES (3, 'small')").unwrap();
            let log_end = conn.mv_store().as_ref().unwrap().logical_log_offset();
            assert!(std::fs::metadata(&log_path).unwrap().len() > log_end);
            let (header, body) = pull_logical(&server, &handle, "g1:o0")
                .expect("bytes after the last valid frame are skipped");
            assert_eq!(header.server_revision, format!("g1:o{log_end}"));
            assert_eq!(body, std::fs::read(&log_path).unwrap()[..log_end as usize]);
            conn.close().unwrap();
            log_end
        };

        // A power loss can leave zero bytes after the last valid frame.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&log_path)
            .unwrap();
        file.set_len(log_end + 4096).unwrap();
        drop(file);
        let server = plaintext_server(dir.path(), false);
        let handle = server
            .resolve_db(None)
            .unwrap_or_else(|_| panic!("open source"));
        let (header, _) = pull_logical(&server, &handle, "g1:o0")
            .expect("zero bytes after the last valid frame are skipped");
        assert_eq!(header.server_revision, format!("g1:o{log_end}"));
    }

    #[test]
    fn header_only_logical_log_keeps_its_revision() {
        let dir = tempfile::tempdir().unwrap();
        {
            let server = plaintext_server(dir.path(), false);
            let handle = server
                .resolve_db(None)
                .unwrap_or_else(|_| panic!("open source"));
            let conn = handle.conn.lock().unwrap().clone();
            conn.execute("PRAGMA journal_mode=mvcc").unwrap();
            let (header, _) = pull_logical(&server, &handle, "g1:o0").unwrap();
            assert_eq!(
                header.server_revision,
                format!("g1:o{MVCC_LOG_HEADER_SIZE}")
            );
            conn.close().unwrap();
        }
        let server = plaintext_server(dir.path(), false);
        let handle = server
            .resolve_db(None)
            .unwrap_or_else(|_| panic!("open source"));
        let revision = format!("g1:o{MVCC_LOG_HEADER_SIZE}");
        let (header, _) = pull_logical(&server, &handle, &revision).unwrap();
        assert_eq!(header.server_revision, revision);
    }

    #[test]
    fn plaintext_incremental_pull_reports_checkpointed_size() {
        let dir = tempfile::tempdir().unwrap();
        let server = plaintext_server(dir.path(), false);
        let handle = server
            .resolve_db(None)
            .unwrap_or_else(|_| panic!("open source"));
        {
            let conn = handle.conn.lock().unwrap();
            conn.execute("CREATE TABLE items(value TEXT)").unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        let request = PullUpdatesReqProtoBody {
            client_revision: "0".into(),
            ..Default::default()
        }
        .encode_to_vec();
        let response = server.handle_pull_updates(&handle, &request).unwrap();
        let mut bytes = response.body.as_slice();
        let header = PullUpdatesRespProtoBody::decode_length_delimited(&mut bytes).unwrap();
        assert_eq!(header.db_size, 2);
        assert!(bytes.is_empty());
    }

    #[test]
    fn validates_database_names() {
        for ok in ["db1", "a-b_c", "x", "nul", "con", &"n".repeat(128)] {
            assert!(validate_db_name(ok), "expected {ok:?} to be valid");
        }
        for bad in [
            "",
            "..",
            "../x",
            "a/b",
            "a\\b",
            ".hidden",
            "a.b",
            "a%2fb",
            "a b",
            "A1",
            "Db1",
            "DB1",
            &"n".repeat(129),
        ] {
            assert!(!validate_db_name(bad), "expected {bad:?} to be rejected");
        }
    }

    /// Mirrors the read loop: the terminator must be found whatever the chunk
    /// boundaries, including when it straddles two reads.
    #[test]
    fn finds_header_end_across_read_boundaries() {
        let request = b"POST / HTTP/1.1\r\nHost: x\r\n\r\nbody".to_vec();
        let expected = find_header_end(&request, 0).expect("terminator is present");

        for chunk in 1..=request.len() {
            let mut data = Vec::new();
            let mut found = None;
            for piece in request.chunks(chunk) {
                let unscanned = data.len().saturating_sub(3);
                data.extend_from_slice(piece);
                if let Some(end) = find_header_end(&data, unscanned) {
                    found = Some(end);
                    break;
                }
            }
            assert_eq!(found, Some(expected), "missed terminator at chunk {chunk}");
        }
    }

    #[test]
    fn rejects_content_length_that_overflows() {
        assert!(request_end(0, usize::MAX).is_err());
        assert_eq!(request_end(10, 5).unwrap(), 19);
    }

    #[test]
    fn parses_single_and_multi_routes() {
        assert_eq!(
            parse_route("POST", "/v2/pipeline"),
            Route::Pipeline { db: None }
        );
        assert_eq!(
            parse_route("POST", "/pull-updates"),
            Route::PullUpdates { db: None }
        );
        assert_eq!(
            parse_route("POST", "/db/db1/v2/pipeline"),
            Route::Pipeline { db: Some("db1") }
        );
        assert_eq!(
            parse_route("POST", "/db/db1/pull-updates"),
            Route::PullUpdates { db: Some("db1") }
        );
        assert_eq!(parse_route("OPTIONS", "/anything"), Route::Options);
        assert_eq!(parse_route("GET", "/v2/pipeline"), Route::NotFound);
        assert_eq!(parse_route("POST", "/nope"), Route::NotFound);
        assert_eq!(parse_route("POST", "/db/a/b/v2/pipeline"), Route::NotFound);
        assert_eq!(
            parse_route("POST", "/db//v2/pipeline"),
            Route::Pipeline { db: Some("") }
        );
    }

    #[test]
    fn each_database_resolves_its_own_files() {
        let base = Path::new("/tmp/dbs");
        let db1 = db_path_for(base, "db1");
        let db2 = db_path_for(base, "db2");

        assert_eq!(db1, Path::new("/tmp/dbs/db1/data"));
        assert_ne!(db1, db2);

        let log1 = logical_log_path(&db1.to_string_lossy()).unwrap();
        let log2 = logical_log_path(&db2.to_string_lossy()).unwrap();
        assert_ne!(log1, log2, "databases must not share a logical log");
        assert_eq!(log1, Path::new("/tmp/dbs/db1/data-log"));
    }

    const TEST_MAX_OPEN: usize = 4;
    const TEST_WORKERS: usize = 4;

    fn plaintext_server(base: &Path, directory: bool) -> TursoSyncServer {
        if directory {
            return dir_server(base);
        }
        let path = base.join("source.db");
        let db = Database::open_file_with_flags(
            Arc::new(turso_core::PlatformIO::new().unwrap()),
            path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        TursoSyncServer::new(
            "127.0.0.1:0".into(),
            path.to_string_lossy().into_owned(),
            conn,
            Arc::new(AtomicUsize::new(0)),
            TEST_WORKERS,
            None,
        )
        .unwrap()
    }

    fn dir_server(base: &Path) -> TursoSyncServer {
        TursoSyncServer::new_dir(
            "127.0.0.1:0".to_string(),
            base.to_path_buf(),
            Arc::new(AtomicUsize::new(0)),
            OpenConfig {
                vfs: None,
                flags: OpenFlags::default(),
                db_opts: DatabaseOpts::new(),
                max_open: TEST_MAX_OPEN,
                encryption: None,
            },
            TEST_WORKERS,
        )
        .unwrap()
    }

    #[test]
    fn refuses_new_databases_once_the_open_map_is_full() {
        let base = tempfile::TempDir::new().unwrap();
        let server = dir_server(base.path());

        for i in 0..TEST_MAX_OPEN {
            let name = format!("db{i}");
            if let Err(resp) = server.resolve_db(Some(&name)) {
                panic!(
                    "opening {name} under the cap must succeed, got {}",
                    resp.status
                );
            }
        }

        assert!(
            server.resolve_db(Some("db0")).is_ok(),
            "an already open database stays reachable at capacity"
        );
        let Err(refused) = server.resolve_db(Some("overflow")) else {
            panic!("a full open map must refuse an unknown database");
        };
        assert_eq!(refused.status, 503);
        assert!(
            !base.path().join("overflow").exists(),
            "a refused database must not reach the filesystem"
        );
    }

    #[test]
    fn refuses_a_database_directory_that_escapes_the_served_tree() {
        let base = tempfile::TempDir::new().unwrap();
        let outside = tempfile::TempDir::new().unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), base.path().join("escaped")).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(outside.path(), base.path().join("escaped")).unwrap();

        let server = dir_server(base.path());
        let Err(refused) = server.resolve_db(Some("escaped")) else {
            panic!("a symlinked database directory must be refused");
        };
        assert_eq!(refused.status, 404);
        assert!(
            !outside.path().join("data").exists(),
            "a refused name must not create files outside the served tree"
        );
    }

    fn open_db(server: &TursoSyncServer, name: &str) -> Arc<DbHandle> {
        match server.resolve_db(Some(name)) {
            Ok(handle) => handle,
            Err(refused) => panic!("{name} must open, got status {}", refused.status),
        }
    }

    const RESPONSE_WAIT: Duration = Duration::from_secs(20);
    const SELECT_ONE_PIPELINE: &str =
        r#"{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}"#;

    #[test]
    fn a_request_to_one_database_does_not_wait_for_a_busy_one() {
        let base = tempfile::TempDir::new().unwrap();
        let server = Arc::new(dir_server(base.path()));
        let listener = server.bind().unwrap();
        let address = listener.local_addr().unwrap();

        let busy = open_db(&server, "busy");
        open_db(&server, "free");

        let serving = {
            let server = server.clone();
            thread::spawn(move || server.serve(listener).unwrap())
        };

        let busy_conn = busy.conn.lock().expect("database connection lock poisoned");
        let mut busy_request = send_pipeline_request(address, "busy");
        let mut free_request = send_pipeline_request(address, "free");

        assert_eq!(
            read_status(&mut free_request),
            200,
            "a request to an idle database must not wait for a busy one"
        );

        drop(busy_conn);
        assert_eq!(read_status(&mut busy_request), 200);

        server.interrupt_count.fetch_add(1, Ordering::SeqCst);
        serving.join().unwrap();
    }

    fn send_pipeline_request(address: std::net::SocketAddr, db: &str) -> TcpStream {
        let head = format!(
            "POST /db/{db}/v2/pipeline HTTP/1.1\r\nHost: localhost\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n",
            SELECT_ONE_PIPELINE.len()
        );
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(SELECT_ONE_PIPELINE.as_bytes()).unwrap();
        stream.flush().unwrap();
        stream
    }

    fn read_status(stream: &mut TcpStream) -> u16 {
        stream.set_read_timeout(Some(RESPONSE_WAIT)).unwrap();
        let mut response = Vec::new();
        if let Err(err) = stream.read_to_end(&mut response) {
            panic!("no response within {RESPONSE_WAIT:?}: {err}");
        }
        String::from_utf8_lossy(&response[..response.len().min(64)])
            .split_whitespace()
            .nth(1)
            .and_then(|status| status.parse().ok())
            .unwrap_or_else(|| panic!("response has no status line: {response:?}"))
    }

    #[test]
    fn threads_racing_to_open_one_new_database_open_it_once() {
        let base = tempfile::TempDir::new().unwrap();
        let server = Arc::new(dir_server(base.path()));

        let racers = 8;
        let start = Arc::new(std::sync::Barrier::new(racers));
        let opened: Vec<Arc<DbHandle>> = (0..racers)
            .map(|_| {
                let server = server.clone();
                let start = start.clone();
                thread::spawn(move || {
                    start.wait();
                    open_db(&server, "shared")
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|racer| racer.join().unwrap())
            .collect();

        for handle in &opened {
            assert!(
                Arc::ptr_eq(handle, &opened[0]),
                "every thread must get the same handle for one name"
            );
        }
        assert_eq!(
            databases_opened(&server),
            1,
            "opening one database twice would take a second file lock that the loser releases for both"
        );

        for i in 1..TEST_MAX_OPEN {
            let name = format!("db{i}");
            assert!(
                server.resolve_db(Some(&name)).is_ok(),
                "the race must leave {} free slots, {name} failed",
                TEST_MAX_OPEN - 1
            );
        }
        let Err(refused) = server.resolve_db(Some("overflow")) else {
            panic!("a full open map must refuse an unknown database");
        };
        assert_eq!(refused.status, 503);
    }

    fn databases_opened(server: &TursoSyncServer) -> usize {
        match &server.source {
            DbSource::Dir { open_handles, .. } => open_handles.opens.load(Ordering::SeqCst),
            DbSource::Single(_) => panic!("only a directory server opens databases by name"),
        }
    }

    #[test]
    fn the_accept_loop_stops_taking_connections_when_the_queue_is_full() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let queue = ConnectionQueue::new(2);

        for _ in 0..2 {
            let _client = TcpStream::connect(address).unwrap();
            let (stream, _) = listener.accept().unwrap();
            assert!(!queue.is_full());
            queue.push(stream);
        }
        assert!(
            queue.is_full(),
            "two queued connections fill a queue of two"
        );

        assert!(queue.next().is_some());
        assert!(
            !queue.is_full(),
            "a worker taking a connection makes room for the accept loop"
        );
    }
}
