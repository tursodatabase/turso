use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use prost::Message;
use roaring::RoaringBitmap;
use tracing::{debug, error, info};

use turso_core::{Connection, Value as CoreValue};
use turso_sync_engine::server_proto::{
    BatchCond, BatchResult, BatchStep, BatchStreamReq, BatchStreamResp, Col, Error,
    ExecuteStreamReq, ExecuteStreamResp, MvccLogicalLogMetadataProto, MvccLogicalLogRangeProto,
    PageData, PageSetRawEncodingProto, PageUpdatesEncodingReq, PipelineReqBody, PipelineRespBody,
    PullUpdatesApplyMode, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody, PullUpdatesStreamKind,
    Row, StmtResult, StreamRequest, StreamResponse, StreamResult, Value,
};

const WAL_FRAME_HEADER_SIZE: usize = 24;
const PAGE_SIZE: usize = 4096;
const MVCC_LOG_MAGIC: u32 = 0x4C4D4C32;
const MVCC_LOG_VERSION: u8 = 3;
const MVCC_LOG_HEADER_SIZE: usize = 56;
const MVCC_LOG_HEADER_SALT_START: usize = 8;
const MVCC_LOG_HEADER_SALT_END: usize = 16;
const MVCC_LOG_HEADER_RESERVED_START: usize = 16;
const MVCC_LOG_HEADER_CRC_START: usize = 52;
const MVCC_TX_FRAME_MAGIC: u32 = 0x5854564D;
const MVCC_TX_EXT_FRAME_MAGIC: u32 = 0x5845564D;
const MVCC_TX_END_MAGIC: u32 = 0x4554564D;
const MVCC_TX_HEADER_SIZE: usize = 24;
const MVCC_TX_EXT_HEADER_SIZE: usize = 40;
const MVCC_TX_TRAILER_SIZE: usize = 8;
const MVCC_TX_FRAME_FLAG_HAS_EXTENSION_BLOCK: u32 = 1 << 0;

type SnapshotPage = (u64, Vec<u8>);
type SnapshotPages = (u64, Vec<SnapshotPage>);

pub struct TursoSyncServer {
    address: String,
    db_path: String,
    conn: Arc<Mutex<Arc<Connection>>>,
    interrupt_count: Arc<AtomicUsize>,
}

impl TursoSyncServer {
    pub fn new(
        address: String,
        db_path: String,
        conn: Arc<Connection>,
        interrupt_count: Arc<AtomicUsize>,
    ) -> Result<Self> {
        conn.wal_auto_actions_disable();

        Ok(Self {
            address,
            db_path,
            conn: Arc::new(Mutex::new(conn)),
            interrupt_count,
        })
    }

    pub fn run(&self) -> Result<()> {
        info!("Starting TursoSyncServer on {}", self.address);

        let listener = TcpListener::bind(&self.address)?;
        listener.set_nonblocking(true)?;

        let interrupt_count = self.interrupt_count.clone();
        let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();

        let monitor_handle = thread::spawn(move || loop {
            if interrupt_count.load(Ordering::SeqCst) > 0 {
                debug!("Interrupt detected, signaling shutdown");
                shutdown_flag_clone.store(true, Ordering::SeqCst);
                break;
            }
            thread::sleep(std::time::Duration::from_millis(100));
        });

        loop {
            if shutdown_flag.load(Ordering::SeqCst) {
                info!("Shutdown signal received, stopping server");
                break;
            }

            match listener.accept() {
                Ok((stream, addr)) => {
                    info!("Accepted connection from {}", addr);
                    if let Err(e) = self.handle_connection(stream) {
                        error!("Error handling connection: {}", e);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }

        let _ = monitor_handle.join();
        info!("TursoSyncServer stopped");
        Ok(())
    }

    fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;

        let mut buffer = [0u8; 8192];
        let mut request_data = Vec::new();

        loop {
            let n = stream.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            request_data.extend_from_slice(&buffer[..n]);

            if let Some(header_end) = find_header_end(&request_data) {
                let headers = String::from_utf8_lossy(&request_data[..header_end]);
                if let Some(content_length) = parse_content_length(&headers) {
                    let body_start = header_end + 4;
                    let total_expected = body_start + content_length;
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
        }

        let (method, path, body) = parse_http_request(&request_data)?;
        info!("Request: {} {}", method, path);

        let response = match (method.as_str(), path.as_str()) {
            ("OPTIONS", _) => Ok(HttpResponse {
                status: 204,
                content_type: "text/plain".to_string(),
                body: Vec::new(),
            }),
            ("POST", "/v2/pipeline") => {
                debug!("Handling /v2/pipeline request");
                self.handle_pipeline(&body)
            }
            ("POST", "/pull-updates") => {
                debug!("Handling /pull-updates request");
                self.handle_pull_updates(&body)
            }
            _ => {
                info!("Unknown endpoint: {} {}", method, path);
                Ok(HttpResponse {
                    status: 404,
                    content_type: "text/plain".to_string(),
                    body: b"Not Found".to_vec(),
                })
            }
        };

        let http_response = match response {
            Ok(resp) => resp,
            Err(e) => {
                error!("Request error: {}", e);
                HttpResponse {
                    status: 500,
                    content_type: "text/plain".to_string(),
                    body: format!("Internal Server Error: {e}").into_bytes(),
                }
            }
        };

        let response_bytes = format_http_response(&http_response);
        stream.write_all(&response_bytes)?;
        stream.flush()?;

        Ok(())
    }

    fn handle_pipeline(&self, body: &[u8]) -> Result<HttpResponse> {
        let req: PipelineReqBody = serde_json::from_slice(body)
            .map_err(|e| anyhow!("Failed to parse pipeline request: {}", e))?;

        debug!("Pipeline request: {:?}", req);

        let conn = self.conn.lock().unwrap();

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

    fn handle_pull_updates(&self, body: &[u8]) -> Result<HttpResponse> {
        let req = <PullUpdatesReqProtoBody as Message>::decode(body)
            .map_err(|e| anyhow!("Failed to decode PullUpdatesRequest: {}", e))?;

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
            return self.handle_logical_pull_updates(&req);
        }

        self.handle_page_pull_updates(&req, PullUpdatesApplyMode::Incremental)
    }

    fn handle_page_pull_updates(
        &self,
        req: &PullUpdatesReqProtoBody,
        apply_mode: PullUpdatesApplyMode,
    ) -> Result<HttpResponse> {
        let conn = self.conn.lock().unwrap();

        let wal_state = conn.wal_state()?;
        debug!("WAL state: max_frame={}", wal_state.max_frame);

        let server_revision: u64 = if req.server_revision.is_empty() {
            wal_state.max_frame
        } else {
            req.server_revision.parse().unwrap_or(wal_state.max_frame)
        };

        let client_revision: u64 = if req.client_revision.is_empty() {
            0
        } else {
            req.client_revision.parse().unwrap_or(0)
        };

        debug!(
            "Using server_revision={}, client_revision={}",
            server_revision, client_revision
        );

        let pages_selector: Option<RoaringBitmap> = if !req.server_pages_selector.is_empty() {
            Some(
                RoaringBitmap::deserialize_from(&req.server_pages_selector[..])
                    .map_err(|e| anyhow!("Failed to parse server_pages_selector: {}", e))?,
            )
        } else {
            None
        };

        // A missing client revision denotes bootstrap, not "changes after frame 0".
        // The base database may already contain checkpointed pages which do not
        // exist in the current WAL, so a WAL-only response would create a sparse
        // or entirely zero-filled client database.
        if req.client_revision.is_empty() {
            let (db_size, pages) =
                read_snapshot_pages(&conn, server_revision, pages_selector.as_ref())?;
            return Ok(page_stream_response(
                server_revision.to_string(),
                db_size,
                pages,
                PullUpdatesApplyMode::ReplaceBase,
            ));
        }

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

        let db_size = current_db_size_pages(&conn, wal_state.max_frame)?;

        let header = PullUpdatesRespProtoBody {
            server_revision: server_revision.to_string(),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: apply_mode as i32,
            mvcc_log: None,
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

    fn handle_logical_pull_updates(&self, req: &PullUpdatesReqProtoBody) -> Result<HttpResponse> {
        let (db_size, fallback_revision, legacy_current_revision) = {
            let conn = self.conn.lock().unwrap();
            let wal_state = conn.wal_state()?;
            (
                current_db_size_pages(&conn, wal_state.max_frame)?,
                format!("page:{}", wal_state.max_frame),
                wal_state.max_frame.to_string(),
            )
        };
        let log_path = match logical_log_path(&self.db_path) {
            Ok(path) => path,
            Err(_) if is_in_memory_db_path(&self.db_path) => {
                info!(
                    "logical pull requested for in-memory sync server database; returning incremental pages"
                );
                return self.handle_page_pull_updates(req, PullUpdatesApplyMode::Incremental);
            }
            Err(err) => return Err(err),
        };
        let log = match std::fs::read(&log_path) {
            Ok(log) => log,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    "logical pull requested but no MVCC log exists at {}; returning replace-base fallback",
                    log_path.display()
                );
                return self.handle_logical_fallback(
                    req,
                    fallback_revision,
                    &legacy_current_revision,
                    db_size,
                );
            }
            Err(err) => return Err(err.into()),
        };
        let snapshot = match scan_mvcc_log(&log) {
            Ok(snapshot) => snapshot,
            Err(err) if is_nonportable_mvcc_log_error(&err) => {
                info!(
                    "logical pull requested but MVCC log is not portable; returning replace-base pages: {err}"
                );
                return self.handle_logical_fallback(
                    req,
                    fallback_revision,
                    &legacy_current_revision,
                    db_size,
                );
            }
            Err(err) => return Err(err),
        };
        let start_offset = parse_mvcc_revision_offset(&req.client_revision, snapshot.end_offset)?;
        if start_offset > snapshot.end_offset {
            return Err(anyhow!(
                "MVCC logical pull revision is from the future: client_offset={} server_offset={}",
                start_offset,
                snapshot.end_offset
            ));
        }
        let start = usize::try_from(start_offset)
            .map_err(|_| anyhow!("MVCC logical pull start offset overflows usize"))?;
        let end = usize::try_from(snapshot.end_offset)
            .map_err(|_| anyhow!("MVCC logical pull end offset overflows usize"))?;

        let mut response_body = Vec::new();
        let (mvcc_log, body) = if start == end {
            (None, Vec::new())
        } else {
            let crc_seed = if start_offset == 0 {
                None
            } else {
                let seed = snapshot.crc_seed_at(start_offset)?;
                Some(seed.to_le_bytes().to_vec())
            };
            (
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
                }),
                log[start..end].to_vec(),
            )
        };

        let header = PullUpdatesRespProtoBody {
            server_revision: format!("g1:o{}", snapshot.end_offset),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log,
        };

        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);
        response_body.extend_from_slice(&body);

        debug!(
            "pull-updates logical: path={} client_revision={} end_offset={} body_bytes={}",
            log_path.display(),
            req.client_revision,
            snapshot.end_offset,
            body.len()
        );

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
        })
    }

    fn handle_logical_fallback(
        &self,
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

        self.handle_replace_base_pages(server_revision)
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

    fn handle_replace_base_pages(&self, server_revision: String) -> Result<HttpResponse> {
        let (db_size, pages) = self.read_replace_base_pages()?;
        Ok(page_stream_response(
            server_revision,
            db_size,
            pages,
            PullUpdatesApplyMode::ReplaceBase,
        ))
    }

    fn read_replace_base_pages(&self) -> Result<SnapshotPages> {
        let conn = self.conn.lock().unwrap();
        let wal_state = conn.wal_state()?;
        read_snapshot_pages(&conn, wal_state.max_frame, None)
    }
}

struct HttpResponse {
    status: u16,
    content_type: String,
    body: Vec<u8>,
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
    Ok(db_file_path(db_path)?.with_extension("db-log"))
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

fn scan_mvcc_log(log: &[u8]) -> Result<MvccLogSnapshot> {
    if log.is_empty() {
        return Ok(MvccLogSnapshot {
            end_offset: 0,
            crc_by_offset: vec![(0, 0)],
        });
    }
    if log.len() < MVCC_LOG_HEADER_SIZE {
        return Err(anyhow!(
            "truncated MVCC logical log header: len={} header_size={}",
            log.len(),
            MVCC_LOG_HEADER_SIZE
        ));
    }
    validate_mvcc_log_header(log)?;
    let mut running_crc = initial_mvcc_log_crc(log)?;
    let mut offset = MVCC_LOG_HEADER_SIZE;
    let mut crc_by_offset = vec![(MVCC_LOG_HEADER_SIZE as u64, running_crc)];

    while offset < log.len() {
        let Some((frame_end, frame_crc)) = read_mvcc_frame_boundary(log, offset, running_crc)?
        else {
            break;
        };
        running_crc = frame_crc;
        offset = frame_end;
        crc_by_offset.push((offset as u64, running_crc));
    }

    Ok(MvccLogSnapshot {
        end_offset: offset as u64,
        crc_by_offset,
    })
}

fn is_nonportable_mvcc_log_error(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    message.starts_with("unsupported MVCC logical log version ")
}

fn validate_mvcc_log_header(log: &[u8]) -> Result<()> {
    if read_u32_le(log, 0)? != MVCC_LOG_MAGIC {
        return Err(anyhow!("invalid MVCC logical log magic"));
    }
    if log[4] != MVCC_LOG_VERSION {
        return Err(anyhow!("unsupported MVCC logical log version {}", log[4]));
    }
    if log[5] & 0b1111_1110 != 0 {
        return Err(anyhow!("invalid MVCC logical log header flags"));
    }
    let header_len = u16::from_le_bytes([log[6], log[7]]) as usize;
    if header_len != MVCC_LOG_HEADER_SIZE {
        return Err(anyhow!(
            "invalid MVCC logical log header length: {header_len}"
        ));
    }
    if log[MVCC_LOG_HEADER_RESERVED_START..MVCC_LOG_HEADER_CRC_START]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(anyhow!(
            "MVCC logical log header reserved bytes must be zero"
        ));
    }
    let stored_crc = read_u32_le(log, MVCC_LOG_HEADER_CRC_START)?;
    let mut crc_buf = [0u8; MVCC_LOG_HEADER_SIZE];
    crc_buf.copy_from_slice(&log[..MVCC_LOG_HEADER_SIZE]);
    crc_buf[MVCC_LOG_HEADER_CRC_START..MVCC_LOG_HEADER_SIZE].fill(0);
    let expected_crc = crc32c::crc32c(&crc_buf);
    if stored_crc != expected_crc {
        return Err(anyhow!("MVCC logical log header checksum mismatch"));
    }
    Ok(())
}

fn initial_mvcc_log_crc(log: &[u8]) -> Result<u32> {
    let salt = u64::from_le_bytes(
        log[MVCC_LOG_HEADER_SALT_START..MVCC_LOG_HEADER_SALT_END]
            .try_into()
            .expect("fixed-size salt slice"),
    );
    Ok(crc32c::crc32c(&salt.to_le_bytes()))
}

fn read_mvcc_frame_boundary(
    log: &[u8],
    offset: usize,
    running_crc: u32,
) -> Result<Option<(usize, u32)>> {
    if log.len() - offset < MVCC_TX_HEADER_SIZE + MVCC_TX_TRAILER_SIZE {
        return Ok(None);
    }
    let frame_magic = read_u32_le(log, offset)?;
    let has_extension_header = frame_magic == MVCC_TX_EXT_FRAME_MAGIC;
    if frame_magic != MVCC_TX_FRAME_MAGIC && !has_extension_header {
        return Err(anyhow!(
            "invalid MVCC logical log frame magic at offset {offset}: {frame_magic:#x}"
        ));
    }
    let header_size = if has_extension_header {
        MVCC_TX_EXT_HEADER_SIZE
    } else {
        MVCC_TX_HEADER_SIZE
    };
    if log.len() - offset < header_size + MVCC_TX_TRAILER_SIZE {
        return Ok(None);
    }
    let payload_size = usize::try_from(read_u64_le(log, offset + 4)?)
        .map_err(|_| anyhow!("MVCC logical log payload size overflows usize"))?;
    let extension_size = if has_extension_header {
        let extension_size = usize::try_from(read_u64_le(log, offset + 24)?)
            .map_err(|_| anyhow!("MVCC logical log extension size overflows usize"))?;
        let extension_record_count = read_u32_le(log, offset + 32)?;
        let frame_flags = read_u32_le(log, offset + 36)?;
        if frame_flags & !MVCC_TX_FRAME_FLAG_HAS_EXTENSION_BLOCK != 0 {
            return Err(anyhow!(
                "unsupported MVCC logical log frame flags at offset {offset}: {frame_flags:#x}"
            ));
        }
        if extension_size == 0 && extension_record_count != 0 {
            return Err(anyhow!(
                "MVCC logical log extension record count without extension block at offset {offset}"
            ));
        }
        if extension_size > 0 && frame_flags & MVCC_TX_FRAME_FLAG_HAS_EXTENSION_BLOCK == 0 {
            return Err(anyhow!(
                "MVCC logical log extension block missing flag at offset {offset}"
            ));
        }
        extension_size
    } else {
        0
    };
    let trailer_start = offset
        .checked_add(header_size)
        .and_then(|value| value.checked_add(payload_size))
        .and_then(|value| value.checked_add(extension_size))
        .ok_or_else(|| anyhow!("MVCC logical log frame offset overflow"))?;
    let frame_end = trailer_start
        .checked_add(MVCC_TX_TRAILER_SIZE)
        .ok_or_else(|| anyhow!("MVCC logical log frame end overflow"))?;
    if frame_end > log.len() {
        return Ok(None);
    }
    let expected_crc = crc32c::crc32c_append(running_crc, &log[offset..trailer_start]);
    let stored_crc = read_u32_le(log, trailer_start)?;
    if stored_crc != expected_crc {
        return Err(anyhow!(
            "MVCC logical log frame checksum mismatch at offset {offset}"
        ));
    }
    let end_magic = read_u32_le(log, trailer_start + 4)?;
    if end_magic != MVCC_TX_END_MAGIC {
        return Err(anyhow!(
            "invalid MVCC logical log frame end magic at offset {offset}"
        ));
    }
    Ok(Some((frame_end, stored_crc)))
}

fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
    let bytes = buf
        .get(offset..offset + 4)
        .ok_or_else(|| anyhow!("buffer too short for u32 at offset {offset}"))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
    let bytes = buf
        .get(offset..offset + 8)
        .ok_or_else(|| anyhow!("buffer too short for u64 at offset {offset}"))?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
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

fn read_snapshot_pages(
    conn: &Connection,
    frame_watermark: u64,
    pages_selector: Option<&RoaringBitmap>,
) -> Result<SnapshotPages> {
    let db_size = current_snapshot_db_size_pages(conn, frame_watermark)?;
    let selected_count = pages_selector
        .map(|selector| selector.len().min(db_size))
        .unwrap_or(db_size);
    let pages_capacity = usize::try_from(selected_count)
        .map_err(|_| anyhow!("database page count does not fit usize: {selected_count}"))?;
    let mut pages = Vec::with_capacity(pages_capacity);

    let mut read_page = |page_id: u64| -> Result<()> {
        if page_id >= db_size {
            return Ok(());
        }
        let page_no = page_id + 1;
        let page_no_u32 = u32::try_from(page_no)
            .map_err(|_| anyhow!("database page number does not fit u32: {page_no}"))?;
        let mut page = vec![0; PAGE_SIZE];
        if !conn.try_wal_watermark_read_page(page_no_u32, &mut page, Some(frame_watermark))? {
            return Err(anyhow!(
                "database page {page_no} is missing from snapshot at frame {frame_watermark}"
            ));
        }
        pages.push((page_id, page));
        Ok(())
    };

    if let Some(selector) = pages_selector {
        for page_id in selector.iter() {
            read_page(u64::from(page_id))?;
        }
    } else {
        for page_id in 0..db_size {
            read_page(page_id)?;
        }
    }

    Ok((db_size, pages))
}

fn page_stream_response(
    server_revision: String,
    db_size: u64,
    pages: Vec<SnapshotPage>,
    apply_mode: PullUpdatesApplyMode,
) -> HttpResponse {
    let header = PullUpdatesRespProtoBody {
        server_revision,
        db_size,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        apply_mode: apply_mode as i32,
        mvcc_log: None,
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

    HttpResponse {
        status: 200,
        content_type: "application/protobuf".to_string(),
        body: response_body,
    }
}

fn db_size_from_page(page: &[u8]) -> u32 {
    u32::from_be_bytes(page[28..32].try_into().unwrap())
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    (0..data.len().saturating_sub(3)).find(|&i| &data[i..i + 4] == b"\r\n\r\n")
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
    let header_end = find_header_end(data).ok_or_else(|| anyhow!("Invalid HTTP request"))?;
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

fn format_http_response(resp: &HttpResponse) -> Vec<u8> {
    let status_text = match resp.status {
        200 => "OK",
        204 => "No Content",
        404 => "Not Found",
        500 => "Internal Server Error",
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
    use super::*;
    use tempfile::TempDir;
    use turso_core::{Database, PlatformIO, SqliteDialect};

    fn preexisting_checkpointed_server() -> (TempDir, TursoSyncServer) {
        let dir = tempfile::tempdir().unwrap();
        let server_path = dir.path().join("server.db");

        {
            let sqlite = rusqlite::Connection::open(&server_path).unwrap();
            sqlite.pragma_update(None, "journal_mode", "wal").unwrap();
            sqlite
                .execute_batch(
                    "
                    CREATE TABLE notes(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        content TEXT NOT NULL
                    );
                    INSERT INTO notes(content) VALUES
                        ('first'),
                        ('second'),
                        ('third');
                    PRAGMA wal_checkpoint(TRUNCATE);
                    ",
                )
                .unwrap();
        }

        let db_path = server_path.to_str().unwrap();
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, db_path, Arc::new(SqliteDialect))
            .expect("open server database");
        let conn = db.connect().unwrap();
        assert_eq!(
            conn.wal_state().unwrap().max_frame,
            0,
            "the regression requires all committed pages to exist only in the base database"
        );

        (
            dir,
            TursoSyncServer::new(
                "unused".to_string(),
                db_path.to_string(),
                conn,
                Arc::new(AtomicUsize::new(0)),
            )
            .unwrap(),
        )
    }

    fn page_request(
        server_revision: &str,
        client_revision: &str,
        server_pages_selector: Bytes,
    ) -> PullUpdatesReqProtoBody {
        PullUpdatesReqProtoBody {
            encoding: PageUpdatesEncodingReq::Raw as i32,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            server_revision: server_revision.to_string(),
            client_revision: client_revision.to_string(),
            long_poll_timeout_ms: 0,
            server_pages_selector,
            server_query_selector: String::new(),
            client_pages: Bytes::new(),
        }
    }

    fn decode_page_response(
        server: &TursoSyncServer,
        request: PullUpdatesReqProtoBody,
    ) -> (PullUpdatesRespProtoBody, Vec<PageData>) {
        let response = server
            .handle_pull_updates(&request.encode_to_vec())
            .unwrap();
        let mut body = response.body.as_slice();
        let header = PullUpdatesRespProtoBody::decode_length_delimited(&mut body).unwrap();
        let mut pages = Vec::new();
        while !body.is_empty() {
            pages.push(PageData::decode_length_delimited(&mut body).unwrap());
        }
        (header, pages)
    }

    #[test]
    fn bootstrap_preexisting_checkpointed_database_sends_complete_snapshot() {
        let (dir, server) = preexisting_checkpointed_server();
        let (header, pages) = decode_page_response(&server, page_request("", "", Bytes::new()));
        assert!(
            header.db_size > 0,
            "bootstrap must report the preexisting base database size"
        );
        assert_eq!(header.apply_mode, PullUpdatesApplyMode::ReplaceBase as i32);
        assert_eq!(
            pages.len() as u64,
            header.db_size,
            "a full bootstrap must include every page in the snapshot"
        );

        let mut client_image = vec![0; header.db_size as usize * PAGE_SIZE];
        for page in pages {
            let start = page.page_id as usize * PAGE_SIZE;
            let end = start + PAGE_SIZE;
            client_image[start..end].copy_from_slice(&page.encoded_page);
        }

        let client_path = dir.path().join("client.db");
        std::fs::write(&client_path, client_image).unwrap();
        let client = rusqlite::Connection::open(client_path).unwrap();
        let row_count: i64 = client
            .query_row("SELECT count(*) FROM notes", [], |row| row.get(0))
            .unwrap();
        assert_eq!(row_count, 3);
    }

    #[test]
    fn bootstrap_preexisting_database_respects_page_selector() {
        let (_dir, server) = preexisting_checkpointed_server();
        let mut selector = RoaringBitmap::new();
        selector.insert(0);
        let mut selector_bytes = Vec::new();
        selector.serialize_into(&mut selector_bytes).unwrap();

        let (header, pages) =
            decode_page_response(&server, page_request("", "", Bytes::from(selector_bytes)));
        assert!(header.db_size > 1);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].page_id, 0);
        assert_eq!(&pages[0].encoded_page[..16], b"SQLite format 3\0");
    }

    #[test]
    fn nonempty_zero_revision_still_receives_incremental_wal_pages() {
        let (_dir, server) = preexisting_checkpointed_server();
        {
            let conn = server.conn.lock().unwrap();
            conn.execute("INSERT INTO notes(content) VALUES ('after bootstrap')")
                .unwrap();
            assert!(conn.wal_state().unwrap().max_frame > 0);
        }

        let (header, pages) = decode_page_response(&server, page_request("", "0", Bytes::new()));
        assert_eq!(header.apply_mode, PullUpdatesApplyMode::Incremental as i32);
        assert!(
            !pages.is_empty(),
            "an established client at revision 0 must receive later WAL changes"
        );
    }
}
