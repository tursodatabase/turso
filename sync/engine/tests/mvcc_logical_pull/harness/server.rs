use std::borrow::Cow;
use std::num::NonZero;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use turso_core::types::{Text, TextSubtype};
use turso_core::{Connection, Numeric, Value as CoreValue};
use turso_sync_engine::database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo};
use turso_sync_engine::database_sync_operations::PAGE_SIZE;
use turso_sync_engine::errors::Error;
use turso_sync_engine::server_proto::{
    self, BatchCond, BatchResult, BatchStep, BatchStreamReq, BatchStreamResp, Col,
    ExecuteStreamReq, ExecuteStreamResp, MvccLogicalLogMetadataProto, MvccLogicalLogRangeProto,
    PageData, PageSetRawEncodingProto, PipelineReqBody, PipelineRespBody, PullUpdatesApplyMode,
    PullUpdatesProtocol, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody, PullUpdatesStreamKind,
    Row, StmtResult, StreamRequest, StreamResponse, StreamResult, Value as ProtoValue,
};
use turso_sync_engine::types::{DatabaseRowMutation, DatabaseRowTransformResult};
use turso_sync_engine::Result;

use super::remote::Remote;

const FRAME_TRAILER_SIZE: usize = 8;
const FRAME_END_MAGIC: u32 = 0x4554564D;

pub struct InProcessServer {
    remote: Arc<Remote>,
    bootstrap_base: Arc<Remote>,
    fail_next_commit_pipeline: AtomicBool,
}

impl InProcessServer {
    pub fn new(remote: Arc<Remote>) -> Self {
        Self {
            bootstrap_base: remote.clone(),
            remote,
            fail_next_commit_pipeline: AtomicBool::new(false),
        }
    }

    pub fn with_bootstrap_base(remote: Arc<Remote>, bootstrap_base: Arc<Remote>) -> Self {
        Self {
            remote,
            bootstrap_base,
            fail_next_commit_pipeline: AtomicBool::new(false),
        }
    }

    pub fn remote(&self) -> &Arc<Remote> {
        &self.remote
    }

    pub fn fail_next_push_after_remote_commit(&self) {
        self.fail_next_commit_pipeline.store(true, Ordering::SeqCst);
    }
}

impl SyncEngineIo for InProcessServer {
    type DataCompletionBytes = Response;
    type DataCompletionTransform = NoTransform;

    fn full_read(&self, path: &str) -> Result<Response> {
        match std::fs::read(path) {
            Ok(content) => Ok(Response::new(content)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Response::new(Vec::new()))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn full_write(&self, path: &str, content: Vec<u8>) -> Result<Response> {
        std::fs::write(path, content)?;
        Ok(Response::new(Vec::new()))
    }

    fn transform(&self, _mutations: Vec<DatabaseRowMutation>) -> Result<NoTransform> {
        Err(Error::DatabaseSyncEngineError(
            "the in-process server does not transform rows".to_string(),
        ))
    }

    fn http(
        &self,
        _url: Option<&str>,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        _headers: &[(&str, &str)],
    ) -> Result<Response> {
        match (method, path) {
            ("POST", "/pull-updates") => {
                Ok(Response::new(self.pull_updates(&body.unwrap_or_default())?))
            }
            ("POST", "/v2/pipeline") => {
                let raw = body.unwrap_or_default();
                let is_commit_batch = String::from_utf8_lossy(&raw).contains("BEGIN IMMEDIATE");
                let resp = self.pipeline(&raw)?;
                if is_commit_batch && self.fail_next_commit_pipeline.swap(false, Ordering::SeqCst) {
                    return Err(Error::DatabaseSyncEngineError(
                        "simulated network drop after remote push commit".to_string(),
                    ));
                }
                Ok(Response::new(resp))
            }
            _ => Err(Error::DatabaseSyncEngineError(format!(
                "the in-process server does not handle {method} {path}"
            ))),
        }
    }

    fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

    fn step_io_callbacks(&self) {}
}

impl InProcessServer {
    fn pipeline(&self, body: &[u8]) -> Result<Vec<u8>> {
        let req: PipelineReqBody = serde_json::from_slice(body)?;
        let conn = self.remote.connection();
        let mut results = Vec::with_capacity(req.requests.len());
        for request in req.requests {
            let result = match request {
                StreamRequest::Execute(exec_req) => execute_stmt(conn, &exec_req),
                StreamRequest::Batch(batch_req) => execute_batch(conn, &batch_req),
                StreamRequest::None => StreamResult::Error {
                    error: server_proto::Error {
                        message: "unknown stream request".to_string(),
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
        Ok(serde_json::to_vec(&resp)?)
    }

    fn pull_updates(&self, body: &[u8]) -> Result<Vec<u8>> {
        let request = PullUpdatesReqProtoBody::decode(body)
            .map_err(|error| Error::DatabaseSyncEngineError(error.to_string()))?;
        if request.client_revision.is_empty() {
            self.page_bootstrap()
        } else {
            self.logical_log_since(log_offset_of(&request.client_revision)?)
        }
    }

    fn page_bootstrap(&self) -> Result<Vec<u8>> {
        let database = self.bootstrap_base.checkpoint_and_read_database_file()?;
        let log_end = self.bootstrap_base.read_logical_log()?.len();
        let header = PullUpdatesRespProtoBody {
            server_revision: revision_at(log_end),
            db_size: (database.len() / PAGE_SIZE) as u64,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };
        let mut response = header.encode_length_delimited_to_vec();
        for (page_id, page) in database.chunks_exact(PAGE_SIZE).enumerate() {
            let page = PageData {
                page_id: page_id as u64,
                encoded_page: Bytes::copy_from_slice(page),
            };
            response.extend(page.encode_length_delimited_to_vec());
        }
        Ok(response)
    }

    fn logical_log_since(&self, offset: usize) -> Result<Vec<u8>> {
        let log = self.remote.read_logical_log()?;
        let new_frames = log.get(offset..).ok_or_else(|| {
            Error::DatabaseSyncEngineError(format!(
                "client revision offset {offset} is past the end of the {}-byte log",
                log.len()
            ))
        })?;
        let mvcc_log = if new_frames.is_empty() {
            None
        } else {
            Some(MvccLogicalLogMetadataProto {
                format: "lml3".to_string(),
                checkpoint_transition: false,
                ranges: vec![MvccLogicalLogRangeProto {
                    generation: 1,
                    start_offset: offset as u64,
                    end_offset: log.len() as u64,
                    starts_with_header: offset == 0,
                    crc_seed: crc_of_frame_ending_at(&log, offset)?
                        .map(|crc| crc.to_le_bytes().to_vec()),
                }],
            })
        };
        let header = PullUpdatesRespProtoBody {
            server_revision: revision_at(log.len()),
            db_size: 0,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };
        let mut response = header.encode_length_delimited_to_vec();
        response.extend_from_slice(new_frames);
        Ok(response)
    }
}

fn execute_stmt(conn: &Arc<Connection>, req: &ExecuteStreamReq) -> StreamResult {
    match run_stmt(conn, &req.stmt) {
        Ok(result) => StreamResult::Ok {
            response: StreamResponse::Execute(ExecuteStreamResp { result }),
        },
        Err(error) => StreamResult::Error { error },
    }
}

fn execute_batch(conn: &Arc<Connection>, req: &BatchStreamReq) -> StreamResult {
    let mut step_results = Vec::with_capacity(req.batch.steps.len());
    let mut step_errors = Vec::with_capacity(req.batch.steps.len());
    for step in &req.batch.steps {
        let should_run = step
            .condition
            .as_ref()
            .is_none_or(|cond| evaluate_cond(cond, &step_results, &step_errors, conn));
        if should_run {
            match run_batch_step(conn, step) {
                Ok(res) => {
                    step_results.push(Some(res));
                    step_errors.push(None);
                }
                Err(err) => {
                    step_results.push(None);
                    step_errors.push(Some(err));
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

fn evaluate_cond(
    cond: &BatchCond,
    results: &[Option<StmtResult>],
    errors: &[Option<server_proto::Error>],
    conn: &Arc<Connection>,
) -> bool {
    match cond {
        BatchCond::None => true,
        BatchCond::Ok { step } => results.get(*step as usize).is_some_and(Option::is_some),
        BatchCond::Error { step } => errors.get(*step as usize).is_some_and(Option::is_some),
        BatchCond::Not { cond } => !evaluate_cond(cond, results, errors, conn),
        BatchCond::And(list) => list
            .conds
            .iter()
            .all(|c| evaluate_cond(c, results, errors, conn)),
        BatchCond::Or(list) => list
            .conds
            .iter()
            .any(|c| evaluate_cond(c, results, errors, conn)),
        BatchCond::IsAutocommit {} => conn.get_auto_commit(),
    }
}

fn run_batch_step(
    conn: &Arc<Connection>,
    step: &BatchStep,
) -> std::result::Result<StmtResult, server_proto::Error> {
    run_stmt(conn, &step.stmt)
}

fn run_stmt(
    conn: &Arc<Connection>,
    stmt_req: &server_proto::Stmt,
) -> std::result::Result<StmtResult, server_proto::Error> {
    let sql = stmt_req
        .sql
        .as_deref()
        .ok_or_else(|| proto_error("NO_SQL", "missing sql"))?;
    let mut stmt = conn
        .prepare(sql)
        .map_err(|err| proto_error("PREPARE_ERROR", err))?;
    for (idx, arg) in stmt_req.args.iter().enumerate() {
        stmt.bind_at(NonZero::new(idx + 1).unwrap(), to_core_value(arg))
            .map_err(|err| proto_error("BIND_ERROR", err))?;
    }
    if stmt_req.want_rows.unwrap_or(true) {
        let rows = stmt
            .run_collect_rows()
            .map_err(|err| proto_error("EXEC_ERROR", err))?;
        let cols = (0..stmt.num_columns())
            .map(|i| Col {
                name: Some(stmt.get_column_name(i).to_string()),
                decltype: stmt.get_column_decltype(i),
            })
            .collect();
        let rows = rows
            .into_iter()
            .map(|row| Row {
                values: row.into_iter().map(from_core_value).collect(),
            })
            .collect();
        Ok(StmtResult {
            cols,
            rows,
            ..Default::default()
        })
    } else {
        stmt.run_ignore_rows()
            .map_err(|err| proto_error("EXEC_ERROR", err))?;
        Ok(StmtResult::default())
    }
}

fn proto_error(code: &str, message: impl ToString) -> server_proto::Error {
    server_proto::Error {
        message: message.to_string(),
        code: code.to_string(),
    }
}

fn to_core_value(value: &ProtoValue) -> CoreValue {
    match value {
        ProtoValue::None | ProtoValue::Null => CoreValue::Null,
        ProtoValue::Integer { value } => CoreValue::from_i64(*value),
        ProtoValue::Float { value } => CoreValue::from_f64(*value),
        ProtoValue::Text { value } => CoreValue::Text(Text {
            value: Cow::Owned(value.clone()),
            subtype: TextSubtype::Text,
        }),
        ProtoValue::Blob { value } => CoreValue::Blob(value.to_vec()),
    }
}

fn from_core_value(value: CoreValue) -> ProtoValue {
    match value {
        CoreValue::Null => ProtoValue::Null,
        CoreValue::Numeric(Numeric::Integer(v)) => ProtoValue::Integer { value: v },
        CoreValue::Numeric(Numeric::Float(v)) => ProtoValue::Float {
            value: f64::from(v),
        },
        CoreValue::Text(t) => ProtoValue::Text {
            value: t.value.to_string(),
        },
        CoreValue::Blob(b) => ProtoValue::Blob {
            value: Bytes::from(b),
        },
    }
}

fn log_offset_of(revision: &str) -> Result<usize> {
    revision
        .strip_prefix("g1:o")
        .and_then(|offset| offset.parse().ok())
        .ok_or_else(|| {
            Error::DatabaseSyncEngineError(format!("unexpected client revision {revision}"))
        })
}

fn revision_at(log_offset: usize) -> String {
    format!("g1:o{log_offset}")
}

fn crc_of_frame_ending_at(log: &[u8], offset: usize) -> Result<Option<u32>> {
    if offset == 0 {
        return Ok(None);
    }
    let trailer = offset
        .checked_sub(FRAME_TRAILER_SIZE)
        .and_then(|start| log.get(start..offset));
    match trailer {
        Some(&[c0, c1, c2, c3, m0, m1, m2, m3])
            if u32::from_le_bytes([m0, m1, m2, m3]) == FRAME_END_MAGIC =>
        {
            Ok(Some(u32::from_le_bytes([c0, c1, c2, c3])))
        }
        _ => Err(Error::DatabaseSyncEngineError(format!(
            "log offset {offset} is not the end of a transaction frame"
        ))),
    }
}

pub struct Response {
    body: Vec<u8>,
    sent: AtomicBool,
}

impl Response {
    fn new(body: Vec<u8>) -> Self {
        Self {
            body,
            sent: AtomicBool::new(false),
        }
    }
}

impl DataCompletion<u8> for Response {
    type DataPollResult = Chunk;

    fn status(&self) -> Result<Option<u16>> {
        Ok(Some(200))
    }

    fn poll_data(&self) -> Result<Option<Chunk>> {
        let already_sent = self.sent.swap(true, Ordering::AcqRel);
        Ok((!already_sent && !self.body.is_empty()).then(|| Chunk(self.body.clone())))
    }

    fn is_done(&self) -> Result<bool> {
        Ok(self.body.is_empty() || self.sent.load(Ordering::Acquire))
    }
}

pub struct Chunk(Vec<u8>);

impl DataPollResult<u8> for Chunk {
    fn data(&self) -> &[u8] {
        &self.0
    }
}

pub enum NoTransform {}

impl DataCompletion<DatabaseRowTransformResult> for NoTransform {
    type DataPollResult = NoTransform;

    fn status(&self) -> Result<Option<u16>> {
        match *self {}
    }

    fn poll_data(&self) -> Result<Option<NoTransform>> {
        match *self {}
    }

    fn is_done(&self) -> Result<bool> {
        match *self {}
    }
}

impl DataPollResult<DatabaseRowTransformResult> for NoTransform {
    fn data(&self) -> &[DatabaseRowTransformResult] {
        match *self {}
    }
}
