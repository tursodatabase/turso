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
    MvccLogicalRevision, PageData, PageSetRawEncodingProto, PageUpdatesEncodingReq,
    PipelineReqBody, PipelineRespBody, PullUpdatesApplyMode, PullUpdatesProtocol,
    PullUpdatesReqProtoBody, PullUpdatesRespProtoBody, PullUpdatesStreamKind, Row, StmtResult,
    StreamRequest, StreamResponse, StreamResult, Value,
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
const MAX_HEADER_BYTES: usize = 32 * 1024;

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
        if conn.mvcc_enabled() {
            conn.prepare_mvcc_for_portable_sync()?;
        }

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

        let apply_mode =
            if self.conn.lock().unwrap().mvcc_enabled() && !req.client_revision.is_empty() {
                PullUpdatesApplyMode::ReplaceBase
            } else {
                PullUpdatesApplyMode::Incremental
            };
        self.handle_page_pull_updates(&req, apply_mode)
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

        let db_size = if conn.mvcc_enabled() {
            let db_size = current_snapshot_db_size_pages(&conn, wal_state.max_frame)?;
            pages_to_send.clear();
            for page_no in 1..=db_size {
                let page_no_u32 = u32::try_from(page_no)
                    .map_err(|_| anyhow!("database page number does not fit u32: {page_no}"))?;
                let page_id = page_no_u32 - 1;
                if pages_selector
                    .as_ref()
                    .is_some_and(|selector| !selector.contains(page_id))
                {
                    continue;
                }
                let mut page = vec![0; PAGE_SIZE];
                if !conn.try_wal_watermark_read_page(
                    page_no_u32,
                    &mut page,
                    Some(server_revision),
                )? {
                    return Err(anyhow!(
                        "database page {page_no} is missing from MVCC page snapshot"
                    ));
                }
                pages_to_send.push((page_id, page));
            }
            db_size
        } else {
            current_db_size_pages(&conn, wal_state.max_frame)?
        };
        let logical_resume_revision = if conn.mvcc_enabled() {
            mvcc_logical_resume_revision(&conn, &self.db_path)?
        } else {
            String::new()
        };

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
            logical_resume_revision,
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
        let db_size = {
            let conn = self.conn.lock().unwrap();
            let wal_state = conn.wal_state()?;
            current_db_size_pages(&conn, wal_state.max_frame)?
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
                return self.handle_page_pull_updates(req, PullUpdatesApplyMode::ReplaceBase);
            }
            Err(err) => return Err(err.into()),
        };
        let snapshot = match scan_mvcc_log(&log) {
            Ok(snapshot) => snapshot,
            Err(err) if is_nonportable_mvcc_log_error(&err) => {
                info!(
                    "logical pull requested but MVCC log is not portable; returning replace-base pages: {err}"
                );
                return self.handle_page_pull_updates(req, PullUpdatesApplyMode::ReplaceBase);
            }
            Err(err) => return Err(err),
        };
        let revision = if req.client_revision.is_empty() {
            MvccLogicalRevision {
                generation: snapshot.generation,
                offset: MVCC_LOG_HEADER_SIZE as u64,
            }
        } else {
            match req.client_revision.parse::<MvccLogicalRevision>() {
                Ok(revision)
                    if revision.generation == snapshot.generation
                        && revision.offset <= snapshot.end_offset
                        && snapshot.is_frame_boundary(revision.offset) =>
                {
                    revision
                }
                _ => return self.handle_page_pull_updates(req, PullUpdatesApplyMode::ReplaceBase),
            }
        };
        let start_offset = revision.offset;
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
                        generation: snapshot.generation,
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
            server_revision: MvccLogicalRevision {
                generation: snapshot.generation,
                offset: snapshot.end_offset,
            }
            .to_string(),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
            logical_resume_revision: String::new(),
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
}

struct HttpResponse {
    status: u16,
    content_type: String,
    body: Vec<u8>,
}

struct MvccLogSnapshot {
    generation: u64,
    end_offset: u64,
    crc_by_offset: Vec<(u64, u32)>,
    frames: Vec<MvccLogFrameBoundary>,
}

struct MvccLogFrameBoundary {
    commit_ts: u64,
    end_offset: u64,
}

impl MvccLogSnapshot {
    fn is_frame_boundary(&self, offset: u64) -> bool {
        self.crc_by_offset
            .iter()
            .any(|(boundary, _)| *boundary == offset)
    }

    fn crc_seed_at(&self, offset: u64) -> Result<u32> {
        self.crc_by_offset
            .iter()
            .find_map(|(boundary, crc)| (*boundary == offset).then_some(*crc))
            .ok_or_else(|| {
                anyhow!("MVCC logical pull offset is not a transaction boundary: {offset}")
            })
    }

    fn resume_offset_after(&self, durable_txid_max: u64) -> Result<u64> {
        let mut resume_offset = MVCC_LOG_HEADER_SIZE as u64;
        let mut tail_started = false;
        for frame in &self.frames {
            if frame.commit_ts <= durable_txid_max {
                if tail_started {
                    return Err(anyhow!(
                        "MVCC logical log timestamps cross the durable boundary out of order"
                    ));
                }
                resume_offset = frame.end_offset;
            } else {
                tail_started = true;
            }
        }
        Ok(resume_offset)
    }
}

fn mvcc_logical_resume_revision(conn: &Connection, db_path: &str) -> Result<String> {
    let mv_store = conn
        .mv_store()
        .as_ref()
        .cloned()
        .ok_or_else(|| anyhow!("MVCC database has no open MVCC store"))?;
    let log_path = logical_log_path(db_path)?;
    let log = match std::fs::read(&log_path) {
        Ok(log) => log,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => return Err(err.into()),
    };
    let snapshot = scan_mvcc_log(&log)?;
    let offset = snapshot.resume_offset_after(mv_store.durable_txid_max())?;
    Ok(MvccLogicalRevision {
        generation: snapshot.generation,
        offset,
    }
    .to_string())
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

fn scan_mvcc_log(log: &[u8]) -> Result<MvccLogSnapshot> {
    if log.is_empty() {
        return Err(anyhow!("MVCC logical log is missing its header"));
    }
    if log.len() < MVCC_LOG_HEADER_SIZE {
        return Err(anyhow!(
            "truncated MVCC logical log header: len={} header_size={}",
            log.len(),
            MVCC_LOG_HEADER_SIZE
        ));
    }
    validate_mvcc_log_header(log)?;
    let generation = u64::from_le_bytes(
        log[MVCC_LOG_HEADER_SALT_START..MVCC_LOG_HEADER_SALT_END]
            .try_into()
            .expect("fixed-size salt slice"),
    );
    let mut running_crc = initial_mvcc_log_crc(log)?;
    let mut offset = MVCC_LOG_HEADER_SIZE;
    let mut crc_by_offset = vec![(MVCC_LOG_HEADER_SIZE as u64, running_crc)];
    let mut frames = Vec::new();

    while offset < log.len() {
        let Some((frame_end, frame_crc, commit_ts)) =
            read_mvcc_frame_boundary(log, offset, running_crc)?
        else {
            break;
        };
        if frames
            .last()
            .is_some_and(|previous: &MvccLogFrameBoundary| previous.commit_ts >= commit_ts)
        {
            return Err(anyhow!(
                "MVCC logical log commit timestamps are not strictly increasing at offset {offset}"
            ));
        }
        running_crc = frame_crc;
        offset = frame_end;
        crc_by_offset.push((offset as u64, running_crc));
        frames.push(MvccLogFrameBoundary {
            commit_ts,
            end_offset: offset as u64,
        });
    }

    Ok(MvccLogSnapshot {
        generation,
        end_offset: offset as u64,
        crc_by_offset,
        frames,
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
) -> Result<Option<(usize, u32, u64)>> {
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
    let commit_ts = read_u64_le(log, offset + 16)?;
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
    Ok(Some((frame_end, stored_crc, commit_ts)))
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
    use tempfile::NamedTempFile;
    use turso_core::{Database, PlatformIO, SqliteDialect};

    fn pull_updates_request(stream_kind: PullUpdatesStreamKind) -> PullUpdatesReqProtoBody {
        PullUpdatesReqProtoBody {
            encoding: PageUpdatesEncodingReq::Raw as i32,
            stream_kind: stream_kind as i32,
            server_revision: String::new(),
            client_revision: String::new(),
            long_poll_timeout_ms: 0,
            server_pages_selector: Bytes::new(),
            server_query_selector: String::new(),
            client_pages: Bytes::new(),
        }
    }

    fn decode_response_header(response: &HttpResponse) -> (PullUpdatesRespProtoBody, &[u8]) {
        let mut body = response.body.as_slice();
        let header = PullUpdatesRespProtoBody::decode_length_delimited(&mut body).unwrap();
        (header, body)
    }

    fn open_file_database(db_path: &str) -> (Arc<Database>, Arc<Connection>) {
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, db_path, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        (db, conn)
    }

    /// WAL-to-MVCC conversion can leave a complete database beside an empty LML2 log. There are
    /// no old transactions to translate, so sync startup must begin a portable log generation
    /// without rejecting or rewriting the already-durable application pages.
    #[test]
    fn sync_server_accepts_header_only_lml2_after_wal_to_mvcc_conversion() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let (_db, conn) = open_file_database(db_path);
        conn.execute("CREATE TABLE account_preference(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO account_preference VALUES (1, 'preserved')")
            .unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

        let log_path = logical_log_path(db_path).unwrap();
        let legacy_log = std::fs::read(&log_path).unwrap();
        assert_eq!(legacy_log.len(), MVCC_LOG_HEADER_SIZE);
        assert_eq!(legacy_log[4], 2);

        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn,
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();
        let response = server
            .handle_pull_updates(
                &pull_updates_request(PullUpdatesStreamKind::Pages).encode_to_vec(),
            )
            .unwrap();
        let (header, body) = decode_response_header(&response);

        assert_eq!(response.status, 200);
        assert_eq!(
            PullUpdatesProtocol::try_from(header.protocol).unwrap(),
            PullUpdatesProtocol::MvccLogical
        );
        assert!(header
            .logical_resume_revision
            .parse::<MvccLogicalRevision>()
            .is_ok());
        assert!(!body.is_empty(), "the complete page base must be returned");

        let portable_log = std::fs::read(log_path).unwrap();
        assert_eq!(portable_log.len(), MVCC_LOG_HEADER_SIZE);
        assert_eq!(portable_log[4], MVCC_LOG_VERSION);
    }

    /// A recovered LML2 tail can contain schema and rows absent from the main database. Startup
    /// must checkpoint those commits before replacing the old log, otherwise a replacement page
    /// response would permanently omit committed data.
    #[test]
    fn sync_server_checkpoints_nonempty_lml2_before_starting_portable_log() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let (db, conn) = open_file_database(db_path);
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("CREATE TABLE account_preference(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO account_preference VALUES (1, 'recovered')")
            .unwrap();

        let log_path = logical_log_path(db_path).unwrap();
        let legacy_log = std::fs::read(&log_path).unwrap();
        assert!(legacy_log.len() > MVCC_LOG_HEADER_SIZE);
        assert_eq!(legacy_log[4], 2);

        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn,
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();

        let portable_log = std::fs::read(log_path).unwrap();
        assert_eq!(portable_log.len(), MVCC_LOG_HEADER_SIZE);
        assert_eq!(portable_log[4], MVCC_LOG_VERSION);
        drop(server);
        drop(db);

        let (_reopened_db, reopened_conn) = open_file_database(db_path);
        let mut recovered_value = None;
        let mut rows = reopened_conn
            .query("SELECT value FROM account_preference WHERE id = 1")
            .unwrap()
            .unwrap();
        rows.run_with_row_callback(|row| {
            recovered_value = Some(row.get::<&str>(0)?.to_string());
            Ok(())
        })
        .unwrap();
        assert_eq!(recovered_value.as_deref(), Some("recovered"));
    }

    /// Numeric revisions belong to the old page protocol. Even when the number happens to equal
    /// the current WAL frame count, it cannot prove that an MVCC replica contains the current
    /// schema, so the server must replace its page base and provide a logical resume revision.
    #[test]
    fn legacy_numeric_revision_zero_receives_replace_base_pages() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let (_db, conn) = open_file_database(db_path);
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("CREATE TABLE account_preference(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();

        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn,
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();
        let mut request = pull_updates_request(PullUpdatesStreamKind::MvccLogicalLog);
        request.client_revision = "0".to_string();
        let response = server
            .handle_pull_updates(&request.encode_to_vec())
            .unwrap();
        let (header, body) = decode_response_header(&response);

        assert_eq!(response.status, 200);
        assert_eq!(
            PullUpdatesStreamKind::try_from(header.stream_kind).unwrap(),
            PullUpdatesStreamKind::Pages
        );
        assert_eq!(
            PullUpdatesApplyMode::try_from(header.apply_mode).unwrap(),
            PullUpdatesApplyMode::ReplaceBase
        );
        assert!(header
            .logical_resume_revision
            .parse::<MvccLogicalRevision>()
            .is_ok());
        assert!(
            !body.is_empty(),
            "replacement pages must accompany the response"
        );
    }

    /// A page revision proves continuity only with a WAL page stream. Once the same remote moves
    /// to MVCC, applying its page image incrementally would combine incompatible journal states;
    /// the server must explicitly require one complete replacement instead.
    #[test]
    fn existing_page_replica_receives_replace_base_after_remote_moves_to_mvcc() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let (_db, conn) = open_file_database(db_path);
        conn.execute("CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        conn.execute("INSERT INTO messages VALUES (1, 'preserved')")
            .unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn,
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();
        let mut request = pull_updates_request(PullUpdatesStreamKind::Pages);
        request.client_revision = "17".to_string();
        let response = server
            .handle_pull_updates(&request.encode_to_vec())
            .unwrap();
        let (header, body) = decode_response_header(&response);

        assert_eq!(
            PullUpdatesProtocol::try_from(header.protocol).unwrap(),
            PullUpdatesProtocol::MvccLogical
        );
        assert_eq!(
            PullUpdatesApplyMode::try_from(header.apply_mode).unwrap(),
            PullUpdatesApplyMode::ReplaceBase
        );
        assert!(header
            .logical_resume_revision
            .parse::<MvccLogicalRevision>()
            .is_ok());
        assert!(
            !body.is_empty(),
            "the replacement must include the page base"
        );
    }

    /// Only a revision from the current logical-log generation at a validated frame boundary can
    /// describe replica contents. Every other revision must replace the page base instead of
    /// returning an error or claiming that the replica is current.
    #[test]
    fn foreign_or_invalid_logical_revisions_receive_replace_base_pages() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let (_db, conn) = open_file_database(db_path);
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn.clone(),
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();
        conn.execute("CREATE TABLE current_generation(id INTEGER PRIMARY KEY)")
            .unwrap();

        let current_response = server
            .handle_pull_updates(
                &pull_updates_request(PullUpdatesStreamKind::MvccLogicalLog).encode_to_vec(),
            )
            .unwrap();
        let (current_header, _) = decode_response_header(&current_response);
        let current = current_header
            .server_revision
            .parse::<MvccLogicalRevision>()
            .unwrap();
        assert!(current.offset > MVCC_LOG_HEADER_SIZE as u64);

        let invalid_revisions = [
            "0".to_string(),
            "page:0".to_string(),
            format!("g{}:o0", current.generation.saturating_add(1)),
            MvccLogicalRevision {
                generation: current.generation,
                offset: current.offset + 1,
            }
            .to_string(),
            MvccLogicalRevision {
                generation: current.generation,
                offset: current.offset - 1,
            }
            .to_string(),
        ];

        for revision in invalid_revisions {
            let mut request = pull_updates_request(PullUpdatesStreamKind::MvccLogicalLog);
            request.client_revision = revision.clone();
            let response = server
                .handle_pull_updates(&request.encode_to_vec())
                .unwrap_or_else(|error| panic!("revision {revision} returned an error: {error}"));
            let (header, body) = decode_response_header(&response);

            assert_eq!(
                PullUpdatesStreamKind::try_from(header.stream_kind).unwrap(),
                PullUpdatesStreamKind::Pages,
                "revision {revision}"
            );
            assert_eq!(
                PullUpdatesApplyMode::try_from(header.apply_mode).unwrap(),
                PullUpdatesApplyMode::ReplaceBase,
                "revision {revision}"
            );
            assert!(
                header
                    .logical_resume_revision
                    .parse::<MvccLogicalRevision>()
                    .is_ok(),
                "revision {revision}"
            );
            assert!(!body.is_empty(), "revision {revision}");
        }
    }

    /// A revision at the exact end of the current portable log is the one case where an empty
    /// logical response is valid. Keeping this path narrow prevents replacement loops after a
    /// replica has genuinely converged.
    #[test]
    fn current_logical_revision_receives_empty_incremental_response() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let (_db, conn) = open_file_database(db_path);
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn.clone(),
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();
        conn.execute("CREATE TABLE current_generation(id INTEGER PRIMARY KEY)")
            .unwrap();

        let first_response = server
            .handle_pull_updates(
                &pull_updates_request(PullUpdatesStreamKind::MvccLogicalLog).encode_to_vec(),
            )
            .unwrap();
        let (first_header, _) = decode_response_header(&first_response);
        let mut request = pull_updates_request(PullUpdatesStreamKind::MvccLogicalLog);
        request.client_revision = first_header.server_revision.clone();
        let response = server
            .handle_pull_updates(&request.encode_to_vec())
            .unwrap();
        let (header, body) = decode_response_header(&response);

        assert_eq!(
            PullUpdatesStreamKind::try_from(header.stream_kind).unwrap(),
            PullUpdatesStreamKind::MvccLogicalLog
        );
        assert_eq!(
            PullUpdatesApplyMode::try_from(header.apply_mode).unwrap(),
            PullUpdatesApplyMode::Incremental
        );
        assert_eq!(header.server_revision, request.client_revision);
        assert!(header.mvcc_log.is_none());
        assert!(body.is_empty());
    }

    /// Restarting an already-portable server must preserve its retained logical tail. Treating a
    /// valid LML3 log as legacy would checkpoint unnecessarily and invalidate active replicas.
    #[test]
    fn sync_server_restart_preserves_valid_lml3_tail() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let (db, conn) = open_file_database(db_path);
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn.clone(),
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();
        conn.execute("CREATE TABLE retained_tail(id INTEGER PRIMARY KEY)")
            .unwrap();
        let log_path = logical_log_path(db_path).unwrap();
        let before_restart = std::fs::read(&log_path).unwrap();
        assert!(before_restart.len() > MVCC_LOG_HEADER_SIZE);
        assert_eq!(before_restart[4], MVCC_LOG_VERSION);
        drop(server);
        drop(conn);
        drop(db);

        let (_reopened_db, reopened_conn) = open_file_database(db_path);
        let _reopened_server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            reopened_conn,
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();

        assert_eq!(std::fs::read(log_path).unwrap(), before_restart);
    }

    /// Enabling portable writes on a nonempty LML2 log upgrades only the header; it cannot turn
    /// the older recovery frames into portable changes. Startup must detect that mixed history,
    /// checkpoint every recovered row, and begin a clean generation.
    #[test]
    fn sync_server_checkpoints_lml3_header_with_legacy_frames() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let (db, conn) = open_file_database(db_path);
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("CREATE TABLE mixed_history(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO mixed_history VALUES (1, 'legacy')")
            .unwrap();
        conn.set_portable_logical_changes_enabled(true);
        conn.execute("INSERT INTO mixed_history VALUES (2, 'portable')")
            .unwrap();

        let log_path = logical_log_path(db_path).unwrap();
        let mixed_log = std::fs::read(&log_path).unwrap();
        assert!(mixed_log.len() > MVCC_LOG_HEADER_SIZE);
        assert_eq!(mixed_log[4], MVCC_LOG_VERSION);

        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn,
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();
        let normalized_log = std::fs::read(&log_path).unwrap();
        assert_eq!(normalized_log.len(), MVCC_LOG_HEADER_SIZE);
        assert_eq!(normalized_log[4], MVCC_LOG_VERSION);
        drop(server);
        drop(db);

        let (_reopened_db, reopened_conn) = open_file_database(db_path);
        let mut values = Vec::new();
        let mut rows = reopened_conn
            .query("SELECT value FROM mixed_history ORDER BY id")
            .unwrap()
            .unwrap();
        rows.run_with_row_callback(|row| {
            values.push(row.get::<&str>(0)?.to_string());
            Ok(())
        })
        .unwrap();
        assert_eq!(values, ["legacy", "portable"]);
    }

    /// MVCC compatibility work must not alter the ordinary WAL page protocol. WAL databases keep
    /// numeric page revisions and never advertise a logical resume token.
    #[test]
    fn wal_page_pull_does_not_advertise_logical_revision() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let (_db, conn) = open_file_database(db_path);
        conn.execute("CREATE TABLE wal_only(id INTEGER PRIMARY KEY)")
            .unwrap();
        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn,
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();
        let response = server
            .handle_pull_updates(
                &pull_updates_request(PullUpdatesStreamKind::Pages).encode_to_vec(),
            )
            .unwrap();
        let (header, _) = decode_response_header(&response);

        assert_eq!(
            PullUpdatesProtocol::try_from(header.protocol).unwrap(),
            PullUpdatesProtocol::Pages
        );
        assert!(header.server_revision.parse::<u64>().is_ok());
        assert!(header.logical_resume_revision.is_empty());
    }

    /// A fresh replica receives the durable page base before switching to logical pulls.
    /// Commits made after the server starts can exist only in the retained MVCC log, so the
    /// revision handed across that boundary must cause the first logical pull to return them.
    #[test]
    fn fresh_mvcc_page_bootstrap_revision_does_not_skip_logical_tail() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap();
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, db_path, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

        let server = TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            db_path.to_string(),
            conn.clone(),
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();

        conn.execute("CREATE TABLE post_start(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO post_start VALUES (1, 'retained')")
            .unwrap();

        let log = std::fs::read(logical_log_path(db_path).unwrap()).unwrap();
        assert!(log.len() > MVCC_LOG_HEADER_SIZE);

        let page_request = pull_updates_request(PullUpdatesStreamKind::Pages);
        let page_response = server
            .handle_pull_updates(&page_request.encode_to_vec())
            .unwrap();
        let (page_header, _) = decode_response_header(&page_response);
        assert_eq!(
            PullUpdatesProtocol::try_from(page_header.protocol).unwrap(),
            PullUpdatesProtocol::MvccLogical
        );
        assert!(page_header.server_revision.parse::<u64>().is_ok());
        assert!(page_header
            .logical_resume_revision
            .parse::<MvccLogicalRevision>()
            .is_ok());

        let mut logical_request = pull_updates_request(PullUpdatesStreamKind::MvccLogicalLog);
        logical_request.client_revision = page_header.logical_resume_revision;
        let logical_response = server
            .handle_pull_updates(&logical_request.encode_to_vec())
            .unwrap();
        let (logical_header, logical_body) = decode_response_header(&logical_response);

        assert!(
            logical_header.mvcc_log.is_some(),
            "fresh bootstrap must receive the retained MVCC log tail"
        );
        assert!(!logical_body.is_empty());
    }

    /// Page revisions and logical-log revisions name different histories. Guessing a logical
    /// offset from a page revision can silently mark missing transactions as synchronized.
    #[test]
    fn mvcc_logical_pull_rejects_page_revisions() {
        assert!("7".parse::<MvccLogicalRevision>().is_err());
    }

    /// A retained log may contain a prefix already materialized by a passive checkpoint. The
    /// bootstrap must resume after that prefix while preserving every newer transaction.
    #[test]
    fn logical_resume_offset_follows_the_durable_transaction_boundary() {
        let snapshot = MvccLogSnapshot {
            generation: 1,
            end_offset: 350,
            crc_by_offset: Vec::new(),
            frames: vec![
                MvccLogFrameBoundary {
                    commit_ts: 10,
                    end_offset: 150,
                },
                MvccLogFrameBoundary {
                    commit_ts: 20,
                    end_offset: 250,
                },
                MvccLogFrameBoundary {
                    commit_ts: 30,
                    end_offset: 350,
                },
            ],
        };

        assert_eq!(
            snapshot.resume_offset_after(0).unwrap(),
            MVCC_LOG_HEADER_SIZE as u64
        );
        assert_eq!(snapshot.resume_offset_after(10).unwrap(), 150);
        assert_eq!(snapshot.resume_offset_after(25).unwrap(), 250);
        assert_eq!(snapshot.resume_offset_after(30).unwrap(), 350);
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
}
