use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use roaring::RoaringBitmap;
use serde_json::json;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::num::NonZero;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use turso_core::types::Value as DbValue;
use turso_core::Connection;
use turso_sync_engine::server_proto::{
    Batch, BatchCond, BatchCondList, BatchResult, BatchStreamResp, Col, Error as HranaError,
    ExecuteStreamResp, NamedArg, PageData, PageSetRawEncodingProto, PageSetZstdEncodingProto,
    PageUpdatesEncodingReq, PipelineReqBody, PipelineRespBody, PullUpdatesReqProtoBody,
    PullUpdatesRespProtoBody, Row, Stmt, StmtResult, StreamRequest, StreamResponse, StreamResult,
    Value,
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
        // Spawn monitor thread to unblock accept() when interrupt_count > 0 by making a loopback connection
        let addr_for_monitor = self.address.clone();
        let interrupt_flag = Arc::clone(&self.interrupt_count);
        thread::spawn(move || {
            loop {
                if interrupt_flag.load(Ordering::SeqCst) > 0 {
                    // Try to connect to unblock accept
                    let _ = TcpStream::connect(&addr_for_monitor);
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
        });

        let listener = TcpListener::bind(&self.address)
            .with_context(|| format!("Failed to bind to address {}", &self.address))?;
        listener
            .set_nonblocking(false)
            .context("Failed to set blocking mode")?;

        info!(%self.address, "TursoSyncServer listening");

        // Main accept loop
        for stream in listener.incoming() {
            if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                info!("Shutdown requested via interrupt_count, stopping server accept loop");
                break;
            }

            match stream {
                Ok(stream) => {
                    let peer = stream.peer_addr().ok();
                    info!(?peer, "Incoming connection");
                    let conn = Arc::clone(&self.conn);
                    thread::spawn(move || {
                        if let Err(e) = handle_client(stream, conn) {
                            error!(error=?e, "Client handling failed");
                        }
                    });
                }
                Err(e) => {
                    error!(error=?e, "Accept failed");
                    // Respect shutdown flag
                    if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                        break;
                    }
                }
            }
        }

        info!("TursoSyncServer stopped");
        Ok(())
    }
}

fn handle_client(mut stream: TcpStream, conn: Arc<Mutex<Arc<Connection>>>) -> Result<()> {
    // Read HTTP request headers
    let mut buf = Vec::with_capacity(8192);
    let mut temp = [0u8; 4096];
    let mut headers_end = None;

    let start = Instant::now();
    loop {
        let n = stream.read(&mut temp)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&temp[..n]);
        if let Some(pos) = find_headers_end(&buf) {
            headers_end = Some(pos);
            break;
        }
        if buf.len() > 1024 * 1024 {
            bail!("Request header too large");
        }
    }

    let headers_end =
        headers_end.ok_or_else(|| anyhow!("Malformed HTTP request: no header terminator"))?;
    let (header_bytes, body_rest) = buf.split_at(headers_end);

    let header_str = std::str::from_utf8(header_bytes).context("Invalid HTTP header encoding")?;
    let (method, path, version, headers) = parse_http_headers(header_str)?;

    // We only support HTTP/1.1
    if version != "HTTP/1.1" {
        let response = http_response(
            505,
            "text/plain",
            b"HTTP Version Not Supported".to_vec(),
            None,
        );
        let _ = stream.write_all(&response);
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(());
    }

    let content_length = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);

    // Read body (may have partially read some bytes into body_rest)
    let mut body = Vec::with_capacity(content_length);
    body.extend_from_slice(body_rest);
    while body.len() < content_length {
        let n = stream.read(&mut temp)?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&temp[..n]);
    }

    info!(%method, %path, content_length=body.len(), "HTTP request received");

    let response = match (method.as_str(), path.as_str()) {
        ("POST", "/v2/pipeline") => {
            let res = sql_over_http(&conn, &body);
            match res {
                Ok(resp_body) => http_response(200, "application/json", resp_body, None),
                Err(e) => {
                    error!(error=?e, "SQL-over-HTTP pipeline failed");
                    let err_body = json!({
                        "baton": null,
                        "base_url": null,
                        "results": [{
                            "type": "error",
                            "error": {
                                "message": e.to_string(),
                                "code": "generic_error"
                            }
                        }]
                    });
                    http_response(
                        400,
                        "application/json",
                        serde_json::to_vec(&err_body).unwrap(),
                        None,
                    )
                }
            }
        }
        ("POST", "/pull-updates") => {
            let res = pull_updates(&conn, &body);
            match res {
                Ok(bytes) => http_response(200, "application/protobuf", bytes, None),
                Err(e) => {
                    error!(error=?e, "pull-updates failed");
                    http_response(
                        400,
                        "text/plain",
                        format!("pull-updates error: {e}").into_bytes(),
                        None,
                    )
                }
            }
        }
        _ => http_response(404, "text/plain", b"Not Found".to_vec(), None),
    };

    stream.write_all(&response)?;
    stream.flush()?;
    let dur_ms = start.elapsed().as_millis();
    info!(duration_ms = dur_ms as u64, "Request processed");
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    // look for \r\n\r\n
    memchr::memmem::find(buf, b"\r\n\r\n").map(|pos| pos + 4)
}

fn parse_http_headers(header: &str) -> Result<(String, String, String, HashMap<String, String>)> {
    let mut lines = header.split("\r\n").filter(|l| !l.is_empty());
    let request_line = lines
        .next()
        .ok_or_else(|| anyhow!("missing request line"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow!("missing method"))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| anyhow!("missing path"))?
        .to_string();
    let version = parts
        .next()
        .ok_or_else(|| anyhow!("missing version"))?
        .to_string();

    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(":") {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    Ok((method, path, version, headers))
}

fn http_response(
    status: u16,
    content_type: &str,
    body: Vec<u8>,
    extra_headers: Option<Vec<(String, String)>>,
) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        500 => "Internal Server Error",
        505 => "HTTP Version Not Supported",
        _ => "OK",
    };
    let mut headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n",
        status,
        reason,
        body.len(),
        content_type
    );
    if let Some(extra) = extra_headers {
        for (k, v) in extra {
            headers.push_str(&format!("{k}: {v}\r\n"));
        }
    }
    headers.push_str("\r\n");
    let mut resp = headers.into_bytes();
    resp.extend_from_slice(&body);
    resp
}

// -------- SQL over HTTP (hrana-like) --------

fn sql_over_http(conn_guard: &Arc<Mutex<Arc<Connection>>>, body: &[u8]) -> Result<Vec<u8>> {
    let req: PipelineReqBody = serde_json::from_slice(body).context("invalid pipeline JSON")?;
    debug!("Pipeline request parsed");

    let conn = conn_guard.lock().unwrap().clone();

    let mut results = Vec::with_capacity(req.requests.len());
    for request in req.requests {
        let res = match request {
            StreamRequest::Execute(exe) => match execute_stmt(&conn, &exe.stmt) {
                Ok(stmt_res) => StreamResult::Ok {
                    response: StreamResponse::Execute(ExecuteStreamResp { result: stmt_res }),
                },
                Err(e) => {
                    error!(error=?e, "Execute request failed");
                    StreamResult::Error {
                        error: HranaError {
                            message: e.to_string(),
                            code: "execute_error".to_string(),
                        },
                    }
                }
            },
            StreamRequest::Batch(b) => match execute_batch(&conn, &b.batch) {
                Ok(batch_res) => StreamResult::Ok {
                    response: StreamResponse::Batch(BatchStreamResp { result: batch_res }),
                },
                Err(e) => {
                    error!(error=?e, "Batch request failed");
                    StreamResult::Error {
                        error: HranaError {
                            message: e.to_string(),
                            code: "batch_error".to_string(),
                        },
                    }
                }
            },
            StreamRequest::None => StreamResult::None,
        };
        results.push(res);
    }

    let resp = PipelineRespBody {
        baton: req.baton,
        base_url: None,
        results,
    };

    let out = serde_json::to_vec(&resp)?;
    Ok(out)
}

fn execute_batch(conn: &Arc<Connection>, batch: &Batch) -> Result<BatchResult> {
    let mut step_results: Vec<Option<StmtResult>> = Vec::with_capacity(batch.steps.len());
    let mut step_errors: Vec<Option<HranaError>> = Vec::with_capacity(batch.steps.len());

    for (idx, step) in batch.steps.iter().enumerate() {
        let cond_ok = match &step.condition {
            Some(cond) => eval_batch_cond(cond, &step_results, &step_errors, conn),
            None => true,
        };
        if cond_ok {
            match execute_stmt(conn, &step.stmt) {
                Ok(sr) => {
                    step_results.push(Some(sr));
                    step_errors.push(None);
                }
                Err(e) => {
                    error!(error=?e, step=idx, "Batch step failed");
                    step_results.push(None);
                    step_errors.push(Some(HranaError {
                        message: e.to_string(),
                        code: "step_error".to_string(),
                    }));
                }
            }
        } else {
            debug!(step = idx, "Batch step condition not satisfied");
            step_results.push(None);
            step_errors.push(None);
        }
    }

    let result = BatchResult {
        step_results,
        step_errors,
        replication_index: batch.replication_index,
    };
    Ok(result)
}

fn eval_batch_cond(
    cond: &BatchCond,
    step_results: &Vec<Option<StmtResult>>,
    step_errors: &Vec<Option<HranaError>>,
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
        BatchCond::Not { cond } => !eval_batch_cond(cond, step_results, step_errors, conn),
        BatchCond::And(BatchCondList { conds }) => conds
            .iter()
            .all(|c| eval_batch_cond(c, step_results, step_errors, conn)),
        BatchCond::Or(BatchCondList { conds }) => conds
            .iter()
            .any(|c| eval_batch_cond(c, step_results, step_errors, conn)),
        BatchCond::IsAutocommit {} => conn.get_auto_commit(),
    }
}

fn execute_stmt(conn: &Arc<Connection>, stmt: &Stmt) -> Result<StmtResult> {
    let sql = stmt
        .sql
        .as_deref()
        .ok_or_else(|| anyhow!("stmt.sql is required"))?;
    info!(sql=%sql, "Executing SQL");
    let mut st = conn.prepare(sql)?;

    // Positional arguments
    for (i, val) in stmt.args.iter().enumerate() {
        let idx = NonZero::new(i + 1).unwrap();
        st.bind_at(idx, map_value_to_db(val)?);
    }

    // Named args
    for NamedArg { name, value } in &stmt.named_args {
        let mut bound = false;
        // Try provided name
        if let Some(idx) = st.parameter_index(name) {
            st.bind_at(idx, map_value_to_db(value)?);
            bound = true;
        } else {
            // Try with leading ':'
            let candidate = format!(":{name}");
            if let Some(idx) = st.parameter_index(&candidate) {
                st.bind_at(idx, map_value_to_db(value)?);
                bound = true;
            } else {
                // Try with '@'
                let candidate = format!("@{name}");
                if let Some(idx) = st.parameter_index(&candidate) {
                    st.bind_at(idx, map_value_to_db(value)?);
                    bound = true;
                } else {
                    // Try with '$'
                    let candidate = format!("${name}");
                    if let Some(idx) = st.parameter_index(&candidate) {
                        st.bind_at(idx, map_value_to_db(value)?);
                        bound = true;
                    }
                }
            }
        }
        if !bound {
            warn!(param=%name, "Named parameter not found in statement");
        }
    }

    let want_rows = stmt.want_rows.unwrap_or(true);
    let mut cols: Vec<Col> = Vec::new();
    let mut rows: Vec<Row> = Vec::new();
    let affected_row_count = if want_rows {
        let values = st.run_collect_rows()?;
        let ncols = st.num_columns();
        for i in 0..ncols {
            let name = Some(st.get_column_name(i).to_string());
            let decltype = st.get_column_type(i);
            cols.push(Col { name, decltype });
        }
        for r in values {
            let mapped: Vec<Value> = r
                .into_iter()
                .map(map_db_to_value)
                .collect::<Result<Vec<_>>>()?;
            rows.push(Row { values: mapped });
        }
        // For SELECT, affected rows is 0
        0
    } else {
        st.run_ignore_rows()?;
        conn.changes() as u64
    };

    let last_insert_rowid = if !want_rows {
        Some(conn.last_insert_rowid())
    } else {
        None
    };

    let result = StmtResult {
        cols,
        rows,
        affected_row_count,
        last_insert_rowid,
        replication_index: stmt.replication_index,
        rows_read: 0,
        rows_written: 0,
        query_duration_ms: 0.0,
    };
    Ok(result)
}

fn map_value_to_db(v: &Value) -> Result<DbValue> {
    match v {
        Value::None => Ok(DbValue::Null),
        Value::Null => Ok(DbValue::Null),
        Value::Integer { value } => Ok(DbValue::Integer(*value)),
        Value::Float { value } => Ok(DbValue::Float(*value)),
        Value::Text { value } => Ok(DbValue::Text(turso_core::types::Text::new(value.clone()))),
        Value::Blob { value } => Ok(DbValue::Blob(value.clone().to_vec())),
    }
}

fn map_db_to_value(v: DbValue) -> Result<Value> {
    match v {
        DbValue::Null => Ok(Value::Null),
        DbValue::Integer(i) => Ok(Value::Integer { value: i }),
        DbValue::Float(f) => Ok(Value::Float { value: f }),
        DbValue::Text(t) => Ok(Value::Text {
            value: t.as_str().to_string(),
        }),
        DbValue::Blob(b) => Ok(Value::Blob {
            value: Bytes::from(b),
        }),
    }
}

// -------- Pull updates (protobuf) --------

fn pull_updates(conn_guard: &Arc<Mutex<Arc<Connection>>>, body: &[u8]) -> Result<Vec<u8>> {
    // Decode request using prost
    let req = <PullUpdatesReqProtoBody as prost::Message>::decode(body)
        .context("Failed to decode PullUpdatesReqProtoBody")?;
    debug!("PullUpdates request decoded");

    // Encoding
    let enc =
        PageUpdatesEncodingReq::try_from(req.encoding).map_err(|_| anyhow!("Unknown encoding"))?;
    match enc {
        PageUpdatesEncodingReq::Raw => { /* ok */ }
        PageUpdatesEncodingReq::Zstd => {
            bail!("Zstd encoding is not supported by this server");
        }
    }

    let conn = conn_guard.lock().unwrap().clone();

    // Check page size
    let pg_size = conn.get_page_size().get();
    if pg_size as usize != PAGE_SIZE {
        bail!("Unsupported page size {}, only 4096 is supported", pg_size);
    }

    // Determine revisions
    let server_rev_opt = if req.server_revision.trim().is_empty() {
        None
    } else {
        Some(u64::from_str(req.server_revision.trim()).context("Invalid server_revision")?)
    };
    let client_rev = if req.client_revision.trim().is_empty() {
        0u64
    } else {
        u64::from_str(req.client_revision.trim()).context("Invalid client_revision")?
    };

    let wal_state = conn.wal_state()?;
    let server_rev = server_rev_opt.unwrap_or(wal_state.max_frame);

    // Decode server_pages_selector (RoaringBitmap), if present
    let page_selector = if !req.server_pages_selector.is_empty() {
        let mut cur = std::io::Cursor::new(req.server_pages_selector.as_ref());
        let rbm = RoaringBitmap::deserialize_from(&mut cur)
            .context("Invalid server_pages_selector bitmap")?;
        Some(rbm)
    } else {
        None
    };
    if !req.server_query_selector.is_empty() {
        // Ignored per requirement
        debug!("Ignoring server_query_selector");
    }
    if !req.client_pages.is_empty() {
        // Ignored per requirement
        debug!("Ignoring client_pages");
    }

    // Iterate frames from server_rev down to client_rev+1 and collect the latest versions of pages
    let mut seen_pages = std::collections::HashSet::<u32>::new();
    let mut pages_data = Vec::<(u32, Vec<u8>)>::new();
    let mut buf = vec![0u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];

    let mut header_db_size: Option<u64> = None;

    if server_rev > 0 && server_rev > client_rev {
        let mut frame = server_rev;
        while frame > client_rev {
            let info = conn.wal_get_frame(frame, &mut buf)?;
            // record db_size from the topmost frame once
            if header_db_size.is_none() {
                header_db_size = Some(info.db_size as u64);
            }
            // SQLite WAL page numbers are 1-based; protocol uses 0-based
            let page_no_1based = info.page_no;
            if page_no_1based == 0 {
                frame -= 1;
                continue;
            }
            let page_no_0based = page_no_1based - 1;
            if seen_pages.contains(&page_no_1based) {
                frame -= 1;
                continue;
            }
            // Filter by selector if present
            if let Some(sel) = &page_selector {
                if !sel.contains(page_no_0based) {
                    frame -= 1;
                    continue;
                }
            }
            seen_pages.insert(page_no_1based);
            let page_bytes = &buf[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
            pages_data.push((page_no_0based, page_bytes.to_vec()));
            frame -= 1;
        }
    } else {
        // No frames or client already up to date
        header_db_size = Some(0);
    }

    // Prepare header and body
    let header = PullUpdatesRespProtoBody {
        server_revision: server_rev.to_string(),
        db_size: header_db_size.unwrap_or(0),
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None::<PageSetZstdEncodingProto>,
    };

    // Encode into length-delimited protobuf sequence
    let mut out = Vec::with_capacity(1024 + pages_data.len() * (PAGE_SIZE + 32));
    {
        let mut buf = Vec::new();
        prost::Message::encode_length_delimited(&header, &mut buf)?;
        out.extend_from_slice(&buf);
    }
    // Pages order can be arbitrary; produce in any order (current order is by latest-first in frames)
    for (page_id, data) in pages_data {
        let pd = PageData {
            page_id: page_id as u64,
            encoded_page: Bytes::from(data),
        };
        let mut buf = Vec::with_capacity(PAGE_SIZE + 32);
        prost::Message::encode_length_delimited(&pd, &mut buf)?;
        out.extend_from_slice(&buf);
    }

    Ok(out)
}

// ------------- External helper crate for memmem -------------
mod memchr {
    pub mod memmem {
        // Simple and sufficient for our needs
        pub fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
            if needle.is_empty() {
                return Some(0);
            }
            haystack
                .windows(needle.len())
                .position(|window| window == needle)
        }
    }
}
