#![deny(clippy::all)]

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use turso_sync_engine::protocol_io::{DataCompletion, DataPollResult, ProtocolIO};

#[napi]
pub enum JsProtocolRequest {
    Http {
        method: String,
        path: String,
        body: Option<Vec<u8>>,
        headers: Vec<(String, String)>,
    },
    FullRead {
        path: String,
    },
    FullWrite {
        path: String,
        content: Vec<u8>,
    },
}

#[derive(Clone)]
#[napi]
pub struct JsDataCompletion(Arc<Mutex<JsDataCompletionInner>>);

struct JsDataCompletionInner {
    status: Option<u16>,
    chunks: VecDeque<Buffer>,
    finished: bool,
    err: Option<String>,
}

impl JsDataCompletion {
    fn inner(&self) -> turso_sync_engine::Result<MutexGuard<JsDataCompletionInner>> {
        let inner = self.0.lock().unwrap();
        if let Some(err) = &inner.err {
            return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                err.clone(),
            ));
        }
        Ok(inner)
    }
}

impl DataCompletion for JsDataCompletion {
    type DataPollResult = JsDataPollResult;

    fn status(&self) -> turso_sync_engine::Result<Option<u16>> {
        let inner = self.inner()?;
        Ok(inner.status)
    }

    fn poll_data(&self) -> turso_sync_engine::Result<Option<Self::DataPollResult>> {
        let mut inner = self.inner()?;
        let chunk = inner.chunks.pop_front();
        Ok(chunk.map(JsDataPollResult))
    }

    fn is_done(&self) -> turso_sync_engine::Result<bool> {
        let inner = self.inner()?;
        Ok(inner.finished)
    }
}

#[napi]
impl JsDataCompletion {
    #[napi]
    pub fn poison(&self, err: String) {
        let mut completion = self.0.lock().unwrap();
        completion.err = Some(err);
    }

    #[napi]
    pub fn status(&self, value: u32) {
        let mut completion = self.0.lock().unwrap();
        completion.status = Some(value as u16);
    }

    #[napi]
    pub fn push(&self, value: Buffer) {
        let mut completion = self.0.lock().unwrap();
        completion.chunks.push_back(value);
    }

    #[napi]
    pub fn done(&self) {
        let mut completion = self.0.lock().unwrap();
        completion.finished = true;
    }
}

#[napi]
pub struct JsDataPollResult(Buffer);

impl DataPollResult for JsDataPollResult {
    fn data(&self) -> &[u8] {
        &self.0
    }
}

#[napi]
pub struct JsProtocolRequestData {
    request: Arc<Mutex<Option<JsProtocolRequest>>>,
    completion: JsDataCompletion,
}

#[napi]
impl JsProtocolRequestData {
    #[napi]
    pub fn request(&self) -> JsProtocolRequest {
        let mut request = self.request.lock().unwrap();
        request.take().unwrap()
    }
    #[napi]
    pub fn completion(&self) -> JsDataCompletion {
        self.completion.clone()
    }
}

impl ProtocolIO for JsProtocolIo {
    type DataCompletion = JsDataCompletion;
    fn http(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        headers: &[(&str, &str)],
    ) -> turso_sync_engine::Result<JsDataCompletion> {
        Ok(self.add_request(JsProtocolRequest::Http {
            method: method.to_string(),
            path: path.to_string(),
            body,
            headers: headers
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect(),
        }))
    }

    fn full_read(&self, path: &str) -> turso_sync_engine::Result<Self::DataCompletion> {
        Ok(self.add_request(JsProtocolRequest::FullRead {
            path: path.to_string(),
        }))
    }

    fn full_write(
        &self,
        path: &str,
        content: Vec<u8>,
    ) -> turso_sync_engine::Result<Self::DataCompletion> {
        Ok(self.add_request(JsProtocolRequest::FullWrite {
            path: path.to_string(),
            content,
        }))
    }
}

#[napi]
pub struct JsProtocolIo {
    requests: Mutex<Vec<JsProtocolRequestData>>,
}

impl Default for JsProtocolIo {
    fn default() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
        }
    }
}

#[napi]
impl JsProtocolIo {
    #[napi]
    pub fn take_request(&self) -> Option<JsProtocolRequestData> {
        self.requests.lock().unwrap().pop()
    }

    fn add_request(&self, request: JsProtocolRequest) -> JsDataCompletion {
        let completion = JsDataCompletionInner {
            chunks: VecDeque::new(),
            finished: false,
            err: None,
            status: None,
        };
        let completion = JsDataCompletion(Arc::new(Mutex::new(completion)));

        let mut requests = self.requests.lock().unwrap();
        requests.push(JsProtocolRequestData {
            request: Arc::new(Mutex::new(Some(request))),
            completion: completion.clone(),
        });
        completion
    }
}
