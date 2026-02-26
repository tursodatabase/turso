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

        let mut seen_pages: HashSet<u32> = HashSet::new();
        let mut pages_to_send: Vec<(u32, Vec<u8>)> = Vec::new();

        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let mut frame_buffer = vec![0u8; frame_size];

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

                let page_data = frame_buffer[WAL_FRAME_HEADER_SIZE..].to_vec();
                pages_to_send.push((page_id, page_data));
            }
        }

        pages_to_send.reverse();

        let db_size = if wal_state.max_frame > 0 {
            let mut last_frame = vec![0u8; frame_size];
            let last_info = conn.wal_get_frame(wal_state.max_frame, &mut last_frame)?;
            last_info.db_size as u64
        } else {
            0
        };

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
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    };

    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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
