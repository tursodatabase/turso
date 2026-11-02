use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

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
    bootstrap_base: Arc<Remote>,
    replace_once: AtomicBool,
    empty_base: bool,
    pub metadata_write_failure: Arc<AtomicU8>,
    pub host_files: Option<Arc<Mutex<HashMap<String, Vec<u8>>>>>,
}

impl InProcessServer {
    pub fn new(remote: Arc<Remote>) -> Self {
        Self {
            bootstrap_base: remote.clone(),
            remote,
            replace_once: AtomicBool::new(false),
            empty_base: false,
            metadata_write_failure: Arc::new(AtomicU8::new(0)),
            host_files: None,
        }
    }

    pub fn with_empty_base(remote: Arc<Remote>) -> Self {
        let mut server = Self::new(remote);
        server.empty_base = true;
        server
    }

    pub fn with_replacement(remote: Arc<Remote>) -> Self {
        let server = Self::new(remote);
        server.replace_once.store(true, Ordering::SeqCst);
        server
    }

    pub fn with_bootstrap_base(remote: Arc<Remote>, bootstrap_base: Arc<Remote>) -> Self {
        Self {
            remote,
            bootstrap_base,
            replace_once: AtomicBool::new(false),
            empty_base: false,
            metadata_write_failure: Arc::new(AtomicU8::new(0)),
            host_files: None,
        }
    }
}

impl SyncEngineIo for InProcessServer {
    type DataCompletionBytes = Response;
    type DataCompletionTransform = NoTransform;

    fn full_read(&self, path: &str) -> Result<Response> {
        if let Some(files) = &self.host_files {
            let data = files.lock().unwrap().get(path).cloned();
            let status = match &data {
                None => 404,
                Some(data) if data.is_empty() => 204,
                _ => 200,
            };
            let mut response = Response::new(data.unwrap_or_default());
            response.status = status;
            return Ok(response);
        }
        match std::fs::read(path) {
            Ok(content) => Ok(Response::new(content)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Response::new(Vec::new()))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn full_write(&self, path: &str, content: Vec<u8>) -> Result<Response> {
        let failure = if path.ends_with("-info") {
            self.metadata_write_failure.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if failure == 1 {
            return Err(Error::DatabaseSyncEngineError(
                "injected before metadata publication".into(),
            ));
        }
        if let Some(files) = &self.host_files {
            files.lock().unwrap().insert(path.into(), content);
        } else {
            std::fs::write(path, content)?;
        }
        if failure == 2 {
            return Err(Error::DatabaseSyncEngineError(
                "injected after metadata publication".into(),
            ));
        }
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
            self.page_bootstrap(false)
        } else if self.replace_once.swap(false, Ordering::SeqCst) {
            self.page_bootstrap(true)
        } else {
            self.logical_log_since(log_offset_of(&request.client_revision)?)
        }
    }

    fn page_bootstrap(&self, replace: bool) -> Result<Vec<u8>> {
        let database = if self.empty_base {
            Vec::new()
        } else {
            self.bootstrap_base.checkpoint_and_read_database_file()?
        };
        let log_end = if self.empty_base {
            0
        } else {
            self.bootstrap_base.read_logical_log()?.len()
        };
        let header = PullUpdatesRespProtoBody {
            encrypted_sync: self.remote.crypto.is_some(),
            server_revision: revision_at(log_end),
            db_size: (database.len() / PAGE_SIZE) as u64,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: if replace {
                PullUpdatesApplyMode::ReplaceBase
            } else {
                PullUpdatesApplyMode::Incremental
            } as i32,
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
        let mut normalized = Vec::new();
        let mut wire_seed = None;
        if let Some(crypto) = &self.remote.crypto {
            if offset < log.len() {
                use turso_core::mvcc::persistent_storage::logical_log::frame_codec::*;
                let header = validate_log_header(&log[..56])?;
                let mut source_crc = header.initial_crc();
                let mut wire_crc = header.initial_crc();
                if offset == 0 {
                    normalized.extend_from_slice(header.bytes());
                }
                let mut pos = 56;
                while pos < log.len() {
                    let layout = inspect_frame_prefix(&log[pos..], Some(crypto))?.unwrap();
                    let mut plaintext = Vec::new();
                    let frame = validate_frame_into(
                        &header,
                        pos as u64,
                        &log[pos..pos + layout.stored_size()],
                        source_crc,
                        Some(crypto),
                        &mut plaintext,
                    )?;
                    let parts = plaintext_wire_parts(&frame, wire_crc);
                    if pos >= offset {
                        if wire_seed.is_none() && offset != 0 {
                            wire_seed = Some(wire_crc);
                        }
                        normalized.extend_from_slice(parts.header);
                        normalized.extend_from_slice(parts.body);
                        normalized.extend_from_slice(&parts.trailer);
                    }
                    source_crc = frame.source_crc();
                    wire_crc = parts.next_crc;
                    pos = frame.source_end() as usize;
                }
            }
        }
        let raw_frames = log.get(offset..).ok_or_else(|| {
            Error::DatabaseSyncEngineError(format!(
                "client revision offset {offset} is past the end of the {}-byte log",
                log.len()
            ))
        })?;
        let new_frames = if self.remote.crypto.is_some() {
            normalized.as_slice()
        } else {
            raw_frames
        };
        let mvcc_log = if new_frames.is_empty() {
            None
        } else {
            Some(MvccLogicalLogMetadataProto {
                format: "lml3".to_string(),
                checkpoint_transition: false,
                ranges: vec![MvccLogicalLogRangeProto {
                    transmitted_length: self
                        .remote
                        .crypto
                        .as_ref()
                        .map(|_| new_frames.len() as u64),
                    generation: 1,
                    start_offset: offset as u64,
                    end_offset: log.len() as u64,
                    starts_with_header: offset == 0,
                    crc_seed: if self.remote.crypto.is_some() {
                        wire_seed
                    } else {
                        crc_of_frame_ending_at(&log, offset)?
                    }
                    .map(|crc| crc.to_le_bytes().to_vec()),
                }],
            })
        };
        let header = PullUpdatesRespProtoBody {
            encrypted_sync: self.remote.crypto.is_some(),
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
    status: u16,
    sent: AtomicBool,
}

impl Response {
    fn new(body: Vec<u8>) -> Self {
        Self {
            body,
            status: 200,
            sent: AtomicBool::new(false),
        }
    }
}

impl DataCompletion<u8> for Response {
    type DataPollResult = Chunk;

    fn status(&self) -> Result<Option<u16>> {
        Ok(Some(self.status))
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
