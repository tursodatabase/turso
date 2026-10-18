use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use turso_sync_engine::database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo};
use turso_sync_engine::database_sync_operations::PAGE_SIZE;
use turso_sync_engine::errors::Error;
use turso_sync_engine::server_proto::{
    MvccLogicalLogMetadataProto, MvccLogicalLogRangeProto, PageData, PageSetRawEncodingProto,
    PullUpdatesApplyMode, PullUpdatesProtocol, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody,
    PullUpdatesStreamKind,
};
use turso_sync_engine::types::{DatabaseRowMutation, DatabaseRowTransformResult};
use turso_sync_engine::Result;

use super::remote::Remote;

const FRAME_TRAILER_SIZE: usize = 8;
const FRAME_END_MAGIC: u32 = 0x4554564D;

pub struct InProcessServer {
    remote: Arc<Remote>,
}

impl InProcessServer {
    pub fn new(remote: Arc<Remote>) -> Self {
        Self { remote }
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
            _ => Err(Error::DatabaseSyncEngineError(format!(
                "the in-process server does not handle {method} {path}"
            ))),
        }
    }

    fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

    fn step_io_callbacks(&self) {}
}

impl InProcessServer {
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
        let database = self.remote.checkpoint_and_read_database_file()?;
        let log_end = self.remote.read_logical_log()?.len();
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
