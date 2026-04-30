use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
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
    ExecuteStreamReq, ExecuteStreamResp, PageData, PageSetRawEncodingProto, PageUpdatesEncodingReq,
    PipelineReqBody, PipelineRespBody, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody, Row,
    StmtResult, StreamRequest, StreamResponse, StreamResult, Value,
};

const WAL_FRAME_HEADER_SIZE: usize = 24;
const PAGE_SIZE: usize = 4096;

pub struct TursoSyncServer {
    address: String,
    conn: Arc<Mutex<Arc<Connection>>>,
    interrupt_count: Arc<AtomicUsize>,
}

impl TursoSyncServer {
    pub fn new(address: String, conn: Arc<Connection>, interrupt_count: Arc<AtomicUsize>) -> Self {
        conn.wal_auto_checkpoint_disable();
        Self {
            address,
            conn: Arc::new(Mutex::new(conn)),
            interrupt_count,
        }
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
            stmt.bind_at(std::num::NonZero::new(i + 1).unwrap(), core_value);
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
            stmt.bind_at(std::num::NonZero::new(i + 1).unwrap(), core_value);
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

        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;

        let mut pages_to_send: Vec<(u32, Vec<u8>)> = Vec::new();

        let db_size: u64;

        if client_revision == 0 {
            // Bootstrap (or pull-specific-pages) request: the client has no prior
            // state at server_revision and expects the actual page contents, not
            // just WAL deltas. Walking WAL frames alone is unsafe because pages
            // that were never modified through the current WAL (e.g. page 1 in a
            // mostly-checkpointed DB) would silently be omitted, leaving the
            // client with a zero-filled file that fails to open
            // ("invalid page size in database header: 0"). Read each requested
            // page from the current DB state (WAL ∪ main DB) at a fixed
            // server_revision watermark instead.
            let frame_watermark = if wal_state.max_frame > 0 {
                Some(server_revision)
            } else {
                None
            };

            // Derive db_size from page 1 of the current snapshot (WAL ∪ main
            // DB) rather than from the last WAL frame: a fully-checkpointed DB
            // has max_frame == 0 yet still has a non-empty main.db, and a WAL
            // frame's db_size field is only populated on commit frames.
            let mut page1 = vec![0u8; PAGE_SIZE];
            db_size = if conn.try_wal_watermark_read_page(1, &mut page1, frame_watermark)? {
                u32::from_be_bytes(page1[28..32].try_into().unwrap()) as u64
            } else {
                0
            };

            let max_page_id = u32::try_from(db_size).unwrap_or(u32::MAX);
            let mut page_buf = vec![0u8; PAGE_SIZE];

            let emit_page =
                |page_id: u32, buf: &mut [u8], out: &mut Vec<(u32, Vec<u8>)>| -> Result<()> {
                    let page_no = page_id + 1;
                    if !conn.try_wal_watermark_read_page(page_no, buf, frame_watermark)? {
                        // Sparse page within [0, db_size): nothing on disk for this
                        // offset (e.g. an unallocated free-list slot). Skip it —
                        // the client zero-fills via the initial truncate, matching
                        // what the server's own filesystem holds.
                        debug!("pull-updates: skipping sparse page page_no={}", page_no);
                        return Ok(());
                    }
                    out.push((page_id, buf.to_vec()));
                    Ok(())
                };

            if let Some(ref selector) = pages_selector {
                for page_id in selector.iter() {
                    if page_id >= max_page_id {
                        continue;
                    }
                    if page_id == 0 {
                        // Page 1 was already read above; reuse it.
                        pages_to_send.push((0, page1.clone()));
                        continue;
                    }
                    emit_page(page_id, &mut page_buf, &mut pages_to_send)?;
                }
            } else {
                if max_page_id > 0 {
                    pages_to_send.push((0, page1));
                }
                for page_id in 1..max_page_id {
                    emit_page(page_id, &mut page_buf, &mut pages_to_send)?;
                }
            }

            debug!(
                "pull-updates: bootstrap mode, sending {} pages (db_size={})",
                pages_to_send.len(),
                db_size,
            );
        } else {
            // Incremental sync: the client already has a consistent snapshot at
            // client_revision and only needs the pages that changed in WAL up to
            // server_revision. Walk frames newest-to-oldest and emit each page's
            // most recent version once.
            db_size = if wal_state.max_frame > 0 {
                let mut last_frame = vec![0u8; frame_size];
                let last_info = conn.wal_get_frame(wal_state.max_frame, &mut last_frame)?;
                last_info.db_size as u64
            } else {
                0
            };

            let mut seen_pages: HashSet<u32> = HashSet::new();
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
                "pull-updates: incremental mode, sending {} pages, seen_pages={:?}",
                pages_to_send.len(),
                seen_pages
            );
            pages_to_send.reverse();
        }

        let header = PullUpdatesRespProtoBody {
            server_revision: server_revision.to_string(),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
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
}

struct HttpResponse {
    status: u16,
    content_type: String,
    body: Vec<u8>,
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
    use std::sync::atomic::AtomicUsize;
    use turso_core::{CheckpointMode, Database, OpenFlags, PlatformIO, IO};

    fn open_db(dir: &std::path::Path) -> (Arc<dyn IO>, Arc<turso_core::Connection>) {
        let path = dir.join("test.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            path.to_str().unwrap(),
            OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();
        (io, conn)
    }

    fn read_varint(buf: &[u8], pos: &mut usize) -> Option<usize> {
        let mut result: usize = 0;
        let mut shift = 0;
        loop {
            if *pos >= buf.len() {
                return None;
            }
            let byte = buf[*pos];
            *pos += 1;
            result |= ((byte & 0x7f) as usize) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        Some(result)
    }

    fn decode_pull_response(body: &[u8]) -> (PullUpdatesRespProtoBody, Vec<PageData>) {
        let mut pos = 0;
        let header_len = read_varint(body, &mut pos).expect("header length");
        let header =
            <PullUpdatesRespProtoBody as Message>::decode(&body[pos..pos + header_len]).unwrap();
        pos += header_len;
        let mut pages = Vec::new();
        while pos < body.len() {
            let page_len = read_varint(body, &mut pos).expect("page length");
            let page = <PageData as Message>::decode(&body[pos..pos + page_len]).unwrap();
            pos += page_len;
            pages.push(page);
        }
        (header, pages)
    }

    fn make_server(conn: Arc<turso_core::Connection>) -> TursoSyncServer {
        TursoSyncServer::new(
            "127.0.0.1:0".to_string(),
            conn,
            Arc::new(AtomicUsize::new(0)),
        )
    }

    fn bootstrap_request_body() -> Vec<u8> {
        let req = PullUpdatesReqProtoBody {
            encoding: PageUpdatesEncodingReq::Raw as i32,
            server_revision: String::new(),
            client_revision: String::new(),
            long_poll_timeout_ms: 0,
            server_pages_selector: Bytes::new(),
            server_query_selector: String::new(),
            client_pages: Bytes::new(),
        };
        req.encode_to_vec()
    }

    /// Bootstrap must return every page in [0, db_size) with valid contents,
    /// even when some pages are NOT in WAL because they were checkpointed
    /// out before later writes added new frames. Regression test for the
    /// "page 1 zero-filled" bug (#5971).
    #[test]
    fn bootstrap_includes_pages_not_in_wal_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let (_io, conn) = open_db(dir.path());

        conn.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        for i in 0..100 {
            conn.execute(format!(
                "INSERT INTO foo VALUES ({i}, 'value-padding-padding-padding-{i}')"
            ))
            .unwrap();
        }

        // Flush WAL into the main DB. After this max_frame may be 0 or small,
        // and the pages we just wrote live in main.db rather than WAL.
        conn.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();

        // A few more writes after checkpoint produce WAL frames that DO NOT
        // necessarily cover every page (e.g. unchanged b-tree interior pages
        // or page 1) — exactly the scenario that left bootstrap clients with
        // a zero-filled page 1 on v0.5.1.
        for i in 100..110 {
            conn.execute(format!("INSERT INTO foo VALUES ({i}, 'extra-{i}')"))
                .unwrap();
        }

        let server = make_server(conn);
        let resp = server
            .handle_pull_updates(&bootstrap_request_body())
            .unwrap();
        assert_eq!(resp.status, 200);

        let (header, pages) = decode_pull_response(&resp.body);
        assert!(header.db_size > 0, "bootstrap header must report db_size");
        assert_eq!(
            pages.len() as u64,
            header.db_size,
            "bootstrap must return every page in [0, db_size)"
        );

        let mut page_ids: Vec<u64> = pages.iter().map(|p| p.page_id).collect();
        page_ids.sort_unstable();
        let expected: Vec<u64> = (0..header.db_size).collect();
        assert_eq!(page_ids, expected, "page ids must cover [0, db_size)");

        let page1 = pages.iter().find(|p| p.page_id == 0).expect("page 1");
        assert_eq!(page1.encoded_page.len(), PAGE_SIZE);
        let page_size_field = u16::from_be_bytes(page1.encoded_page[16..18].try_into().unwrap());
        assert!(
            page_size_field != 0,
            "page 1 page_size header field must be non-zero (regression: zero-filled page 1)"
        );
        assert!(
            page1.encoded_page.iter().any(|&b| b != 0),
            "page 1 must not be zero-filled"
        );
    }

    /// The selector path must also serve actual page contents, not just WAL
    /// deltas, when the client's revision is 0 (e.g. pull_pages_v1).
    #[test]
    fn bootstrap_with_selector_returns_requested_pages_from_main_db() {
        let dir = tempfile::tempdir().unwrap();
        let (_io, conn) = open_db(dir.path());

        conn.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        for i in 0..50 {
            conn.execute(format!(
                "INSERT INTO foo VALUES ({i}, 'padding-padding-padding-{i}')"
            ))
            .unwrap();
        }
        conn.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
        conn.execute("INSERT INTO foo VALUES (1000, 'after-ckpt')")
            .unwrap();

        let server = make_server(conn);

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0u32);
        bitmap.insert(1u32);
        let mut bitmap_bytes = Vec::with_capacity(bitmap.serialized_size());
        bitmap.serialize_into(&mut bitmap_bytes).unwrap();

        let req = PullUpdatesReqProtoBody {
            encoding: PageUpdatesEncodingReq::Raw as i32,
            server_revision: String::new(),
            client_revision: String::new(),
            long_poll_timeout_ms: 0,
            server_pages_selector: Bytes::from(bitmap_bytes),
            server_query_selector: String::new(),
            client_pages: Bytes::new(),
        };
        let resp = server.handle_pull_updates(&req.encode_to_vec()).unwrap();
        assert_eq!(resp.status, 200);

        let (_, pages) = decode_pull_response(&resp.body);
        let mut page_ids: Vec<u64> = pages.iter().map(|p| p.page_id).collect();
        page_ids.sort_unstable();
        assert_eq!(page_ids, vec![0u64, 1u64]);
        let page1 = pages.iter().find(|p| p.page_id == 0).unwrap();
        assert!(page1.encoded_page.iter().any(|&b| b != 0));
    }

    /// Incremental sync (client_revision > 0) must keep the existing
    /// behaviour of returning only the pages that changed in WAL between
    /// client_revision and server_revision.
    #[test]
    fn incremental_sync_returns_only_wal_changed_pages() {
        let dir = tempfile::tempdir().unwrap();
        let (_io, conn) = open_db(dir.path());

        conn.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        for i in 0..20 {
            conn.execute(format!("INSERT INTO foo VALUES ({i}, 'v-{i}')"))
                .unwrap();
        }
        let baseline = conn.wal_state().unwrap().max_frame;

        conn.execute("INSERT INTO foo VALUES (999, 'incremental')")
            .unwrap();
        let after = conn.wal_state().unwrap().max_frame;
        assert!(after > baseline, "follow-up insert must add WAL frames");

        let server = make_server(conn);
        let req = PullUpdatesReqProtoBody {
            encoding: PageUpdatesEncodingReq::Raw as i32,
            server_revision: String::new(),
            client_revision: baseline.to_string(),
            long_poll_timeout_ms: 0,
            server_pages_selector: Bytes::new(),
            server_query_selector: String::new(),
            client_pages: Bytes::new(),
        };
        let resp = server.handle_pull_updates(&req.encode_to_vec()).unwrap();
        assert_eq!(resp.status, 200);

        let (header, pages) = decode_pull_response(&resp.body);
        assert!(!pages.is_empty(), "incremental sync must return changes");
        // Incremental response must NOT be a full snapshot: it should be
        // strictly smaller than the on-server db_size for this workload.
        assert!(
            (pages.len() as u64) < header.db_size,
            "incremental response should only carry WAL deltas"
        );
    }

    /// After a TRUNCATE checkpoint with no subsequent writes the WAL is empty
    /// (max_frame == 0) but main.db still holds the database. Bootstrap must
    /// derive db_size from page 1's header rather than from the (absent) last
    /// WAL frame, otherwise the client receives a header with db_size == 0
    /// and ends up with a 0-byte replica even though data exists.
    #[test]
    fn bootstrap_after_truncate_with_no_writes_returns_full_db() {
        let dir = tempfile::tempdir().unwrap();
        let (_io, conn) = open_db(dir.path());

        conn.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        for i in 0..50 {
            conn.execute(format!("INSERT INTO foo VALUES ({i}, 'pad-pad-pad-{i}')"))
                .unwrap();
        }
        conn.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
        // No writes after the checkpoint: WAL is fully drained.
        assert_eq!(conn.wal_state().unwrap().max_frame, 0);

        let server = make_server(conn);
        let resp = server
            .handle_pull_updates(&bootstrap_request_body())
            .unwrap();
        assert_eq!(resp.status, 200);

        let (header, pages) = decode_pull_response(&resp.body);
        assert!(
            header.db_size > 0,
            "bootstrap must report a non-zero db_size when main.db is non-empty"
        );
        assert_eq!(pages.len() as u64, header.db_size);
        let page1 = pages.iter().find(|p| p.page_id == 0).expect("page 1");
        let page_size_field = u16::from_be_bytes(page1.encoded_page[16..18].try_into().unwrap());
        assert!(page_size_field != 0);
    }
}
