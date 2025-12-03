use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
};

use turso_sdk_kit::rsapi::{turso_slice_from_bytes, TursoError, TursoStatusCode};

use crate::capi::c::{self};

/// sync engine extended IO request
pub enum SyncEngineIoRequest {
    Http {
        method: String,
        path: String,
        body: Option<Vec<u8>>,
        headers: Vec<(String, String)>,
    },
    /// atomic full read of the file content
    FullRead { path: String },
    /// atomic write of the file content (on most FS, this will be temp file write followed by rename and fsync)
    FullWrite { path: String, content: Vec<u8> },
}

impl SyncEngineIoRequest {
    pub fn kind_to_capi(&self) -> c::turso_sync_io_request_type_t {
        match self {
            SyncEngineIoRequest::Http { .. } => c::turso_sync_io_request_type_t::TURSO_SYNC_IO_HTTP,
            SyncEngineIoRequest::FullRead { .. } => {
                c::turso_sync_io_request_type_t::TURSO_SYNC_IO_FULL_READ
            }
            SyncEngineIoRequest::FullWrite { .. } => {
                c::turso_sync_io_request_type_t::TURSO_SYNC_IO_FULL_WRITE
            }
        }
    }
    /// extract header key-value pair from the HTTP IO request
    pub fn header_to_capi(
        &self,
        index: usize,
    ) -> Result<c::turso_sync_io_http_header_t, TursoError> {
        match self {
            SyncEngineIoRequest::Http { headers, .. } if index < headers.len() => {
                Ok(c::turso_sync_io_http_header_t {
                    key: turso_slice_from_bytes(headers[index].0.as_bytes()),
                    value: turso_slice_from_bytes(headers[index].1.as_bytes()),
                })
            }
            SyncEngineIoRequest::Http { headers, .. } if index >= headers.len() => {
                Err(TursoError {
                    code: TursoStatusCode::Misuse,
                    message: Some("header index out of boudns".to_string()),
                })
            }
            _ => Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("unexpected io request type".to_string()),
            }),
        }
    }
    pub fn http_to_capi(&self) -> Result<c::turso_sync_io_http_request_t, TursoError> {
        match self {
            SyncEngineIoRequest::Http {
                method,
                path,
                body,
                headers,
            } => Ok(c::turso_sync_io_http_request_t {
                method: turso_slice_from_bytes(method.as_bytes()),
                path: turso_slice_from_bytes(path.as_bytes()),
                body: if let Some(body) = body {
                    turso_slice_from_bytes(body.as_ref())
                } else {
                    Default::default()
                },
                headers: headers.len() as i32,
            }),
            _ => Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("unexpected io request type".to_string()),
            }),
        }
    }
    pub fn full_read_to_capi(&self) -> Result<c::turso_sync_io_full_read_request_t, TursoError> {
        match self {
            SyncEngineIoRequest::FullRead { path } => Ok(c::turso_sync_io_full_read_request_t {
                path: turso_slice_from_bytes(path.as_bytes()),
            }),
            _ => Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("unexpected io request type".to_string()),
            }),
        }
    }
    pub fn full_write_to_capi(&self) -> Result<c::turso_sync_io_full_write_request_t, TursoError> {
        match self {
            SyncEngineIoRequest::FullWrite { path, content } => {
                Ok(c::turso_sync_io_full_write_request_t {
                    path: turso_slice_from_bytes(path.as_bytes()),
                    content: turso_slice_from_bytes(content.as_ref()),
                })
            }
            _ => Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("unexpected io request type".to_string()),
            }),
        }
    }
}

struct SyncEngineIoCompletionInner<TBytes: AsRef<[u8]>> {
    status: Option<u16>,
    chunks: VecDeque<TBytes>,
    finished: bool,
    err: Option<String>,
}

pub struct SyncEngineIoBytesPollResult<TBytes: AsRef<[u8]>>(TBytes);

impl<TBytes: AsRef<[u8]> + Send + Sync + 'static>
    turso_sync_engine::database_sync_engine_io::DataPollResult<u8>
    for SyncEngineIoBytesPollResult<TBytes>
{
    fn data(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<TBytes: AsRef<[u8]> + Send + Sync + 'static>
    turso_sync_engine::database_sync_engine_io::DataCompletion<u8>
    for SyncEngineIoCompletion<TBytes>
{
    type DataPollResult = SyncEngineIoBytesPollResult<TBytes>;

    fn status(&self) -> turso_sync_engine::Result<Option<u16>> {
        let inner = self.inner()?;
        Ok(inner.status)
    }

    fn poll_data(&self) -> turso_sync_engine::Result<Option<Self::DataPollResult>> {
        let mut inner = self.inner()?;
        let chunk = inner.chunks.pop_front();
        Ok(chunk.map(SyncEngineIoBytesPollResult))
    }

    fn is_done(&self) -> turso_sync_engine::Result<bool> {
        let inner = self.inner()?;
        Ok(inner.finished)
    }
}

// todo(sivukhin): implement mutation callback
pub struct SyncEngineIoTransformPollResult;

impl
    turso_sync_engine::database_sync_engine_io::DataPollResult<
        turso_sync_engine::types::DatabaseRowTransformResult,
    > for SyncEngineIoTransformPollResult
{
    fn data(&self) -> &[turso_sync_engine::types::DatabaseRowTransformResult] {
        todo!()
    }
}

impl<TBytes: AsRef<[u8]> + Send + Sync + 'static>
    turso_sync_engine::database_sync_engine_io::DataCompletion<
        turso_sync_engine::types::DatabaseRowTransformResult,
    > for SyncEngineIoCompletion<TBytes>
{
    type DataPollResult = SyncEngineIoTransformPollResult;

    fn status(&self) -> turso_sync_engine::Result<Option<u16>> {
        todo!()
    }

    fn poll_data(&self) -> turso_sync_engine::Result<Option<Self::DataPollResult>> {
        todo!()
    }

    fn is_done(&self) -> turso_sync_engine::Result<bool> {
        todo!()
    }
}

/// IO completion which is manipulated by the caller
pub struct SyncEngineIoCompletion<TBytes: AsRef<[u8]>> {
    inner: Arc<Mutex<SyncEngineIoCompletionInner<TBytes>>>,
}

impl<TBytes: AsRef<[u8]>> Clone for SyncEngineIoCompletion<TBytes> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<TBytes: AsRef<[u8]>> SyncEngineIoCompletion<TBytes> {
    /// set error to the IO completion which will trigger appropriate error path on the sync-engine side
    pub fn poison(&self, err: String) {
        let mut completion = self.inner.lock().unwrap();
        completion.err = Some(err);
    }
    /// set HTTP status code for HTTP IO request
    pub fn status(&self, value: u32) {
        let mut completion = self.inner.lock().unwrap();
        completion.status = Some(value as u16);
    }
    /// push raw data to the IO request (FullRead or HTTP)
    pub fn push_buffer(&self, value: TBytes) {
        let mut completion = self.inner.lock().unwrap();
        completion.chunks.push_back(value);
    }
    /// mark completion as done (sync-engine will stop waiting for more data to arrive)
    pub fn done(&self) {
        let mut completion = self.inner.lock().unwrap();
        completion.finished = true;
    }

    fn inner(&self) -> turso_sync_engine::Result<MutexGuard<SyncEngineIoCompletionInner<TBytes>>> {
        let inner = self.inner.lock().unwrap();
        if let Some(err) = &inner.err {
            return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                err.clone(),
            ));
        }
        Ok(inner)
    }
}

/// sync engine queued IO request with its completion
pub struct SyncEngineIoQueueItem<TBytes: AsRef<[u8]>> {
    request: SyncEngineIoRequest,
    completion: SyncEngineIoCompletion<TBytes>,
}

impl<TBytes: AsRef<[u8]>> SyncEngineIoQueueItem<TBytes> {
    pub fn get_request(&self) -> &SyncEngineIoRequest {
        &self.request
    }
    pub fn get_completion(&self) -> &SyncEngineIoCompletion<TBytes> {
        &self.completion
    }
    pub fn to_capi(self: Box<Self>) -> *mut c::turso_sync_io_item_t {
        Box::into_raw(self) as *mut c::turso_sync_io_item_t
    }
    /// helper method to restore [SyncEngineIoQueueItem] ref from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn ref_from_capi<'a>(
        value: *const c::turso_sync_io_item_t,
    ) -> Result<&'a Self, TursoError> {
        if value.is_null() {
            Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("got null pointer".to_string()),
            })
        } else {
            Ok(&*(value as *const Self))
        }
    }
    /// helper method to restore [SyncEngineIoQueueItem] instance from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn box_from_capi(value: *const c::turso_sync_io_item_t) -> Box<Self> {
        Box::from_raw(value as *mut Self)
    }
}

pub struct SyncEngineIoQueue<TBytes: AsRef<[u8]>> {
    io_queue: Mutex<VecDeque<SyncEngineIoQueueItem<TBytes>>>,
    io_callbacks: Mutex<VecDeque<Box<dyn FnMut() -> bool + Send>>>,
}

impl<TBytes: AsRef<[u8]> + Send + Sync + 'static>
    turso_sync_engine::database_sync_engine_io::SyncEngineIo for SyncEngineIoQueue<TBytes>
{
    type DataCompletionBytes = SyncEngineIoCompletion<TBytes>;
    type DataCompletionTransform = SyncEngineIoCompletion<TBytes>;

    fn http(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        headers: &[(&str, &str)],
    ) -> turso_sync_engine::Result<Self::DataCompletionBytes> {
        Ok(self.push_back(SyncEngineIoRequest::Http {
            method: method.to_string(),
            path: path.to_string(),
            body,
            headers: headers
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect(),
        }))
    }

    fn full_read(&self, path: &str) -> turso_sync_engine::Result<Self::DataCompletionBytes> {
        Ok(self.push_back(SyncEngineIoRequest::FullRead {
            path: path.to_string(),
        }))
    }

    fn full_write(
        &self,
        path: &str,
        content: Vec<u8>,
    ) -> turso_sync_engine::Result<Self::DataCompletionBytes> {
        Ok(self.push_back(SyncEngineIoRequest::FullWrite {
            path: path.to_string(),
            content,
        }))
    }

    fn transform(
        &self,
        _mutations: Vec<turso_sync_engine::types::DatabaseRowMutation>,
    ) -> turso_sync_engine::Result<Self::DataCompletionTransform> {
        // todo(sivukhin): add mutation callbacks to the sdk-kit
        todo!()
    }

    fn add_io_callback(&self, callback: Box<dyn FnMut() -> bool + Send>) {
        let mut io_callbacks = self.io_callbacks.lock().unwrap();
        io_callbacks.push_back(callback);
    }

    fn step_io_callbacks(&self) {
        let mut items = {
            let mut io_callbacks = self.io_callbacks.lock().unwrap();
            io_callbacks.drain(..).collect::<VecDeque<_>>()
        };
        let length = items.len();
        for _ in 0..length {
            let mut item = items.pop_front().unwrap();
            if item() {
                continue;
            }
            items.push_back(item);
        }
        {
            let mut io_callbacks = self.io_callbacks.lock().unwrap();
            io_callbacks.extend(items);
        }
    }
}
impl<TBytes: AsRef<[u8]>> SyncEngineIoQueue<TBytes> {
    pub fn new() -> Arc<SyncEngineIoQueue<TBytes>> {
        Arc::new(Self {
            io_queue: Mutex::new(VecDeque::new()),
            io_callbacks: Mutex::new(VecDeque::new()),
        })
    }
    pub fn pop_front(&self) -> Option<Box<SyncEngineIoQueueItem<TBytes>>> {
        self.io_queue.lock().unwrap().pop_front().map(Box::new)
    }
    fn push_back(&self, request: SyncEngineIoRequest) -> SyncEngineIoCompletion<TBytes> {
        let completion = SyncEngineIoCompletionInner {
            chunks: VecDeque::new(),
            finished: false,
            err: None,
            status: None,
        };
        let completion = SyncEngineIoCompletion {
            inner: Arc::new(Mutex::new(completion)),
        };

        let mut queue = self.io_queue.lock().unwrap();
        queue.push_back(SyncEngineIoQueueItem {
            request,
            completion: completion.clone(),
        });
        completion
    }
}
