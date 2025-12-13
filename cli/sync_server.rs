use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use prost::Message as _;
use roaring::RoaringBitmap;
use serde_json::json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, error, info};
use turso_core::types::Value as CoreValue;
use turso_core::Connection;
use turso_sync_engine::server_proto::{
    Batch, BatchCond, BatchCondList, BatchResult, BatchStreamReq, BatchStreamResp, Col,
    Error as HranaError, ExecuteStreamReq, ExecuteStreamResp, NamedArg, PageData,
    PageSetRawEncodingProto, PageUpdatesEncodingReq, PipelineReqBody, PipelineRespBody,
    PullUpdatesReqProtoBody, PullUpdatesRespProtoBody, Row as HranaRow, Stmt as HranaStmt,
    StmtResult, StreamRequest, StreamResponse, StreamResult, Value as HranaValue,
};

const CRLF: &[u8] = b"\r\n";
const WAL_FRAME_HEADER_SIZE: usize = 24;
const PAGE_SIZE: usize = 4096;

pub struct TursoSyncServer {
    // listen address (e.g. 0.0.0.0:8080)
    address: String,
    conn: Arc<Mutex<Arc<Connection>>>,
    // stop server if interrupt_count > 0 (do this check in the main server event loop)
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
        info!(address = %self.address, "Starting Turso sync server");
        // Disable automatic checkpointing if possible (best effort)
        // Note: We keep WAL checkpoints disabled by not invoking checkpoint APIs and attempting a PRAGMA commonly used in SQLite.
        // If unsupported, it will be ignored.
        {
            let guard = self.conn.lock().expect("poisoned lock");
            if let Err(e) = guard.pragma_update("wal_autocheckpoint", 0) {
                debug!("PRAGMA wal_autocheckpoint=0 not supported or failed: {e:?}");
            } else {
                debug!("Disabled wal_autocheckpoint via PRAGMA");
            }
            // keep guard in scope to hold lock briefly here; we drop before starting listener
        }

        let listener = TcpListener::bind(&self.address)?;
        listener.set_nonblocking(false)?;

        // Monitor interrupt_count in separate thread and poke the listener to break accept()
        let addr_clone = self.address.clone();
        let interrupt_clone = self.interrupt_count.clone();
        std::thread::spawn(move || {
            loop {
                if interrupt_clone.load(Ordering::SeqCst) > 0 {
                    // poke the listener via a dummy connection
                    if let Ok(mut s) = TcpStream::connect(&addr_clone) {
                        let _ = s.write_all(
                            b"POST /__shutdown HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
                        );
                        let _ = s.shutdown(Shutdown::Both);
                    }
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        });

        // Main accept loop
        loop {
            if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                info!("Interrupt requested, shutting down server");
                break;
            }

            match listener.accept() {
                Ok((mut stream, peer)) => {
                    info!(remote=%peer, "Accepted new connection");
                    if let Err(e) = self.handle_connection(&mut stream) {
                        error!("Connection handling error: {e:?}");
                        // Try to send a 500 in case nothing was written yet
                        let _ = write_http_response(
                            &mut stream,
                            500,
                            "Internal Server Error",
                            "text/plain",
                            b"internal error".to_vec(),
                            &[],
                        );
                    }
                    let _ = stream.shutdown(Shutdown::Both);
                }
                Err(e) => {
                    // Likely interrupted by shutdown poke or other transient issue
                    debug!("Accept error: {e:?}");
                    if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                        break;
                    }
                    // minor backoff
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }

        info!("Server stopped");
        Ok(())
    }

    fn handle_connection(&self, stream: &mut TcpStream) -> Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(15))).ok();

        let (method, path, headers, body) = read_http_request(stream)?;
        info!(%method, %path, headers=format!("{headers:?}"), content_length = body.len(), "Incoming request");

        if path == "/__shutdown" {
            // Just acknowledge and return quickly
            write_http_response(stream, 200, "OK", "text/plain", b"bye".to_vec(), &[])?;
            return Ok(());
        }

        match (method.as_str(), path.as_str()) {
            ("POST", "/v2/pipeline") => {
                // Acquire the DB lock for the full request processing duration
                let guard = self.conn.lock().expect("poisoned lock");
                let start = Instant::now();
                let res = self.sql_over_http(&guard, &body);
                debug!("Pipeline handled in {:?}\n", start.elapsed());

                match res {
                    Ok(resp_body) => {
                        write_http_response(stream, 200, "OK", "application/json", resp_body, &[])
                    }
                    Err(err) => {
                        error!("SQL over HTTP pipeline failed: {err:?}");
                        // Return JSON error with message
                        let err_body = json!({
                            "error": {"message": format!("{err}"), "code": "SQL_ERROR"}
                        })
                        .to_string()
                        .into_bytes();

                        write_http_response(
                            stream,
                            400,
                            "Bad Request",
                            "application/json",
                            err_body,
                            &[],
                        )
                    }
                }?;
            }
            ("POST", "/pull-updates") => {
                let content_type = headers
                    .get("content-type")
                    .cloned()
                    .unwrap_or_else(|| "".to_string());
                if !content_type.contains("application/protobuf") {
                    info!("Bad content-type for /pull-updates: {}", content_type);
                    write_http_response(
                        stream,
                        415,
                        "Unsupported Media Type",
                        "text/plain",
                        b"expecting application/protobuf".to_vec(),
                        &[],
                    )?;
                    return Ok(());
                }

                // Acquire lock for full duration
                let guard = self.conn.lock().expect("poisoned lock");
                let start = Instant::now();
                let res = self.pull_updates(&guard, &body);
                debug!("Pull-updates handled in {:?}", start.elapsed());

                match res {
                    Ok(resp_body) => write_http_response(
                        stream,
                        200,
                        "OK",
                        "application/protobuf",
                        resp_body,
                        &[],
                    ),
                    Err(err) => {
                        error!("Pull-updates failed: {err:?}");
                        write_http_response(
                            stream,
                            400,
                            "Bad Request",
                            "text/plain",
                            format!("pull-updates error: {err}").into_bytes(),
                            &[],
                        )
                    }
                }?;
            }
            _ => {
                write_http_response(
                    stream,
                    404,
                    "Not Found",
                    "text/plain",
                    b"not found".to_vec(),
                    &[],
                )?;
            }
        }
        Ok(())
    }

    fn sql_over_http(&self, conn: &Arc<Connection>, body: &[u8]) -> Result<Vec<u8>> {
        let req: PipelineReqBody = serde_json::from_slice(body)?;
        debug!(
            "Parsed pipeline request: baton={:?}, #requests={}",
            req.baton,
            req.requests.len()
        );

        let mut results = Vec::with_capacity(req.requests.len());

        for (idx, request) in req.requests.into_iter().enumerate() {
            match request {
                StreamRequest::Execute(exec_req) => {
                    info!(step = idx, "Executing single statement");
                    match self.execute_stmt(conn, &exec_req.stmt) {
                        Ok(stmt_result) => {
                            results.push(StreamResult::Ok {
                                response: StreamResponse::Execute(ExecuteStreamResp {
                                    result: stmt_result,
                                }),
                            });
                        }
                        Err(e) => {
                            error!("Execute failed: {e:?}");
                            results.push(StreamResult::Error {
                                error: HranaError {
                                    message: format!("{e}"),
                                    code: "SQL_ERROR".to_string(),
                                },
                            });
                        }
                    }
                }
                StreamRequest::Batch(BatchStreamReq { batch }) => {
                    info!(step = idx, "Executing batch");
                    let (batch_res, batch_errs) = self.execute_batch(conn, &batch);
                    let batch_result = BatchResult {
                        step_results: batch_res,
                        step_errors: batch_errs,
                        replication_index: None,
                    };
                    results.push(StreamResult::Ok {
                        response: StreamResponse::Batch(BatchStreamResp {
                            result: batch_result,
                        }),
                    });
                }
                StreamRequest::None => {
                    debug!("StreamRequest::None received");
                    results.push(StreamResult::None);
                }
            }
        }

        let resp = PipelineRespBody {
            baton: req.baton,
            base_url: None,
            results,
        };
        let json = serde_json::to_vec(&resp)?;
        Ok(json)
    }

    fn execute_batch(
        &self,
        conn: &Arc<Connection>,
        batch: &Batch,
    ) -> (Vec<Option<StmtResult>>, Vec<Option<HranaError>>) {
        let mut step_results: Vec<Option<StmtResult>> = Vec::with_capacity(batch.steps.len());
        let mut step_errors: Vec<Option<HranaError>> = Vec::with_capacity(batch.steps.len());

        for (i, step) in batch.steps.iter().enumerate() {
            let cond = step.condition.as_ref();
            if let Some(cond) = cond {
                if !self.eval_batch_cond(conn, cond, &step_results, &step_errors) {
                    debug!("Batch step {} skipped due to condition", i);
                    step_results.push(None);
                    step_errors.push(None);
                    continue;
                }
            }
            debug!("Executing batch step {}", i);
            match self.execute_stmt(conn, &step.stmt) {
                Ok(r) => {
                    step_results.push(Some(r));
                    step_errors.push(None);
                }
                Err(e) => {
                    error!("Batch step {} failed: {e:?}", i);
                    step_results.push(None);
                    step_errors.push(Some(HranaError {
                        message: format!("{e}"),
                        code: "SQL_ERROR".to_string(),
                    }));
                }
            }
        }

        (step_results, step_errors)
    }

    fn eval_batch_cond(
        &self,
        conn: &Arc<Connection>,
        cond: &BatchCond,
        step_results: &Vec<Option<StmtResult>>,
        step_errors: &Vec<Option<HranaError>>,
    ) -> bool {
        match cond {
            BatchCond::None => true,
            BatchCond::Ok { step } => {
                let idx = *step as usize;
                step_results.get(idx).map(|o| o.is_some()).unwrap_or(false)
                    && step_errors.get(idx).map(|o| o.is_none()).unwrap_or(false)
            }
            BatchCond::Error { step } => {
                let idx = *step as usize;
                step_errors.get(idx).map(|o| o.is_some()).unwrap_or(false)
            }
            BatchCond::Not { cond } => {
                !self.eval_batch_cond(conn, cond.as_ref(), step_results, step_errors)
            }
            BatchCond::And(BatchCondList { conds }) => conds
                .iter()
                .all(|c| self.eval_batch_cond(conn, c, step_results, step_errors)),
            BatchCond::Or(BatchCondList { conds }) => conds
                .iter()
                .any(|c| self.eval_batch_cond(conn, c, step_results, step_errors)),
            BatchCond::IsAutocommit {} => conn.get_auto_commit(),
        }
    }

    fn execute_stmt(&self, conn: &Arc<Connection>, stmt: &HranaStmt) -> Result<StmtResult> {
        let sql = stmt.sql.clone().unwrap_or_default();
        info!("Executing SQL: {}", sql);
        let mut s = conn.prepare(&sql)?;
        // positional args only
        for (i, v) in stmt.args.iter().enumerate() {
            let idx = NonZeroUsize::new(i + 1).unwrap();
            s.bind_at(idx, hrana_to_core_value(v)?);
        }
        // ignore named_args per protocol
        if !stmt.named_args.is_empty() {
            debug!("Ignoring named args in statement execution");
        }

        let start = Instant::now();
        let want_rows = stmt.want_rows.unwrap_or(true);

        let mut cols_meta: Vec<Col> = Vec::new();
        let num_cols = s.num_columns();
        for ci in 0..num_cols {
            let name = Some(s.get_column_name(ci).to_string());
            let decltype = s.get_column_type(ci);
            cols_meta.push(Col { name, decltype });
        }

        let (rows, affected) = if want_rows {
            debug!("Collecting rows for SQL");
            let rows_core = s.run_collect_rows()?;
            let rows = rows_core
                .into_iter()
                .map(|row_vals| HranaRow {
                    values: row_vals.into_iter().map(core_to_hrana_value).collect(),
                })
                .collect::<Vec<_>>();
            (rows, conn.changes() as u64)
        } else {
            debug!("Ignoring rows for SQL");
            s.run_ignore_rows()?;
            (Vec::new(), conn.changes() as u64)
        };

        let dur_ms = start.elapsed().as_secs_f64() * 1000.0;

        let result = StmtResult {
            cols: cols_meta,
            rows,
            affected_row_count: affected,
            last_insert_rowid: Some(conn.last_insert_rowid()),
            replication_index: None,
            rows_read: 0,
            rows_written: 0,
            query_duration_ms: dur_ms,
        };
        Ok(result)
    }

    fn pull_updates(&self, conn: &Arc<Connection>, body: &[u8]) -> Result<Vec<u8>> {
        // Note: Use of deprecated from_i32 is disallowed; use TryFrom<i32>.
        let req = PullUpdatesReqProtoBody::decode(body)?;
        debug!("Pull-updates request: enc={}, server_rev='{}', client_rev='{}', long_poll={}ms, selector_sz={} bytes",
            req.encoding, req.server_revision, req.client_revision, req.long_poll_timeout_ms, req.server_pages_selector.len());

        let enc: PageUpdatesEncodingReq = PageUpdatesEncodingReq::try_from(req.encoding)
            .map_err(|_| anyhow!("invalid encoding value"))?;

        match enc {
            PageUpdatesEncodingReq::Zstd => {
                bail!("ZSTD encoding is not supported by server")
            }
            PageUpdatesEncodingReq::Raw => {
                // ok
            }
        }

        // Decode optional server pages selector bitmap (zero-based page ids)
        let selector_bitmap: Option<RoaringBitmap> = if !req.server_pages_selector.is_empty() {
            let mut cursor = std::io::Cursor::new(req.server_pages_selector.to_vec());
            match RoaringBitmap::deserialize_from(&mut cursor) {
                Ok(bm) => Some(bm),
                Err(e) => {
                    debug!("Failed to deserialize server pages selector, ignoring: {e:?}");
                    None
                }
            }
        } else {
            None
        };

        // Determine revisions
        let mut server_rev = if req.server_revision.trim().is_empty() {
            // find latest committed frame
            let state = conn.wal_state()?;
            let mut rev = state.max_frame;
            let mut found = false;
            let mut frame = vec![0u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
            while rev > 0 {
                let info = conn.wal_get_frame(rev, &mut frame)?;
                if info.is_commit_frame() {
                    found = true;
                    break;
                }
                rev -= 1;
            }
            if !found {
                debug!(
                    "No commit frame found, using max_frame as server_rev={}",
                    state.max_frame
                );
                state.max_frame
            } else {
                rev
            }
        } else {
            u64::from_str(req.server_revision.trim())?
        };

        let client_rev = if req.client_revision.trim().is_empty() {
            0u64
        } else {
            u64::from_str(req.client_revision.trim())?
        };

        if PAGE_SIZE != 4096 {
            bail!("Server compiled with unexpected page size");
        }

        // Collect pages changed in (client_rev..=server_rev], newest first, unique pages only
        let mut seen_pages_one_based: HashSet<u32> = HashSet::new();
        let mut page_datas: Vec<PageData> = Vec::new();
        let mut header_db_size: u64 = 0;

        // If server_rev is 0, nothing to send
        if server_rev > 0 && server_rev > client_rev {
            let mut frame_buf = vec![0u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];

            let mut f = server_rev;
            while f > client_rev {
                let info = conn.wal_get_frame(f, &mut frame_buf)?;
                if header_db_size == 0 {
                    header_db_size = info.db_size as u64;
                }
                let page_no_one_based = info.page_no;
                if !seen_pages_one_based.contains(&page_no_one_based) {
                    let page_id_zero_based = page_no_one_based.saturating_sub(1) as u64;
                    // filter by selector if set
                    let include_page = match &selector_bitmap {
                        Some(bm) => bm.contains(page_id_zero_based as u32),
                        None => true,
                    };
                    if include_page {
                        let page_bytes =
                            &frame_buf[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
                        page_datas.push(PageData {
                            page_id: page_id_zero_based,
                            encoded_page: Bytes::copy_from_slice(page_bytes),
                        });
                    }
                    seen_pages_one_based.insert(page_no_one_based);
                }
                f -= 1;
            }
        } else {
            // No changes, use current wal state for db size if possible
            let state = conn.wal_state()?;
            header_db_size = state.max_frame as u64; // best effort; exact DB size unknown without reading a frame
        }

        // Prepare header
        let header = PullUpdatesRespProtoBody {
            server_revision: server_rev.to_string(),
            db_size: header_db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
        };

        // Encode as length-delimited sequence: first header, then each page
        let mut resp_body: Vec<u8> = Vec::new();
        {
            header.encode_length_delimited(&mut resp_body)?;
            for p in page_datas.into_iter() {
                p.encode_length_delimited(&mut resp_body)?;
            }
        }

        Ok(resp_body)
    }
}

// --- Helpers ---

fn read_http_request(
    stream: &mut TcpStream,
) -> Result<(String, String, HashMap<String, String>, Vec<u8>)> {
    // Read until CRLFCRLF
    let mut buf = Vec::with_capacity(8192);
    let mut tmp = [0u8; 1024];
    let mut header_end = None;

    loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            // client closed
            info!(
                header = std::str::from_utf8(&buf).unwrap_or("malformed"),
                "n == 0"
            );
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_header_end(&buf) {
            header_end = Some(pos);
            info!(
                header = std::str::from_utf8(&buf[..pos]).unwrap_or("malformed"),
                "read header"
            );
            break;
        }
        if buf.len() > 1024 * 1024 {
            info!(
                header = std::str::from_utf8(&buf).unwrap_or("malformed"),
                "read big header"
            );
            bail!("Header too large");
        }
    }

    let header_end =
        header_end.ok_or_else(|| anyhow!("Incomplete HTTP request header: {buf:?}"))?;
    let header_bytes = &buf[..header_end];
    let mut body_remaining = buf[header_end + 4..].to_vec();

    // Parse start line and headers
    let header_str = String::from_utf8_lossy(header_bytes);
    let mut lines = header_str.split("\r\n");
    let start_line = lines
        .next()
        .ok_or_else(|| anyhow!("Missing request line"))?;
    let mut parts = start_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow!("Missing method"))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| anyhow!("Missing path"))?
        .to_string();
    let _version = parts.next().unwrap_or("HTTP/1.1");

    let mut headers = HashMap::new();
    for l in lines {
        if l.trim().is_empty() {
            continue;
        }
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }

    // Read body by Content-Length
    let content_len = headers
        .get("content-length")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0usize);

    if body_remaining.len() < content_len {
        let mut to_read = content_len - body_remaining.len();
        while to_read > 0 {
            let n = stream.read(&mut tmp)?;
            if n == 0 {
                break;
            }
            body_remaining.extend_from_slice(&tmp[..n]);
            if body_remaining.len() > content_len {
                break;
            }
            to_read = content_len - body_remaining.len();
        }
    }

    if body_remaining.len() != content_len {
        bail!(
            "Unexpected body size: expected {}, got {}",
            content_len,
            body_remaining.len()
        );
    }

    info!(%method, %path, content_length = content_len, "Parsed HTTP request");
    Ok((method, path, headers, body_remaining))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn write_http_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: Vec<u8>,
    extra_headers: &[(&str, &str)],
) -> Result<()> {
    let mut headers = Vec::new();
    headers.push(format!("HTTP/1.1 {} {}", status, reason));
    headers.push(format!("Content-Length: {}", body.len()));
    headers.push(format!("Content-Type: {}", content_type));
    for (k, v) in extra_headers {
        headers.push(format!("{}: {}", k, v));
    }
    headers.push(String::new()); // empty line

    let mut resp = headers.join("\r\n").into_bytes();
    resp.extend_from_slice(CRLF);
    resp.extend_from_slice(&body);

    stream.write_all(&resp)?;
    stream.flush()?;
    Ok(())
}

fn hrana_to_core_value(v: &HranaValue) -> Result<CoreValue> {
    Ok(match v {
        HranaValue::None => CoreValue::Null,
        HranaValue::Null => CoreValue::Null,
        HranaValue::Integer { value } => CoreValue::Integer(*value),
        HranaValue::Float { value } => CoreValue::Float(*value),
        HranaValue::Text { value } => CoreValue::Text(value.clone().into()),
        HranaValue::Blob { value } => CoreValue::Blob(value.to_vec()),
    })
}

fn core_to_hrana_value(v: CoreValue) -> HranaValue {
    match v {
        CoreValue::Null => HranaValue::Null,
        CoreValue::Integer(i) => HranaValue::Integer { value: i },
        CoreValue::Float(f) => HranaValue::Float { value: f },
        CoreValue::Text(t) => HranaValue::Text {
            value: t.as_str().to_string(),
        },
        CoreValue::Blob(b) => HranaValue::Blob {
            value: Bytes::from(b),
        },
    }
}

// The following implementations ensure compatibility with turso_core::Statement::bind_at signature
trait BindAtExt {
    fn bind_at(&mut self, idx: NonZeroUsize, val: CoreValue) -> Result<()>;
}

impl BindAtExt for turso_core::Statement {
    fn bind_at(&mut self, idx: NonZeroUsize, val: CoreValue) -> Result<()> {
        self.bind_at(idx, val);
        Ok(())
    }
}
