use napi::{bindgen_prelude::AsyncTask, Env, Task};
use napi_derive::napi;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use turso_sync_engine::types::{DbChangesStatus, ProtocolCommand};

pub trait Generator {
    fn resume(&mut self, result: Option<String>) -> napi::Result<GeneratorResponse>;
}

impl<F: Future<Output = turso_sync_engine::Result<()>>> Generator
    for genawaiter::sync::Gen<ProtocolCommand, turso_sync_engine::Result<()>, F>
{
    fn resume(&mut self, error: Option<String>) -> napi::Result<GeneratorResponse> {
        let result = match error {
            Some(err) => Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                format!("JsProtocolIo error: {err}"),
            )),
            None => Ok(()),
        };
        match self.resume_with(result) {
            genawaiter::GeneratorState::Yielded(ProtocolCommand::IO) => Ok(GeneratorResponse::IO),
            genawaiter::GeneratorState::Complete(Ok(())) => Ok(GeneratorResponse::Done),
            genawaiter::GeneratorState::Complete(Err(err)) => Err(napi::Error::new(
                napi::Status::GenericFailure,
                format!("sync engine operation failed: {err}"),
            )),
        }
    }
}

#[napi]
pub struct SyncEngineChanges {
    pub(crate) status: Box<Option<DbChangesStatus>>,
}

#[napi(discriminant = "type", object_from_js = false)]
pub enum GeneratorResponse {
    IO,
    Done,
    SyncEngineStats {
        operations: i64,
        main_wal: i64,
        revert_wal: i64,
        last_pull_unix_time: i64,
        last_push_unix_time: Option<i64>,
        revision: Option<String>,
    },
    SyncEngineChanges {
        changes: SyncEngineChanges,
    },
}

#[napi]
impl SyncEngineChanges {
    #[napi]
    pub fn empty(&self) -> bool {
        let Some(changes) = self.status.as_ref() else {
            return true;
        };
        changes.file_slot.is_none()
    }
}

#[napi]
#[derive(Clone)]
pub struct GeneratorHolder {
    pub(crate) generator: Arc<Mutex<dyn Generator>>,
    pub(crate) response: Arc<Mutex<Option<GeneratorResponse>>>,
}

pub struct ResumeTask {
    holder: GeneratorHolder,
    error: Option<String>,
}

unsafe impl Send for ResumeTask {}

impl Task for ResumeTask {
    type Output = GeneratorResponse;
    type JsValue = GeneratorResponse;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        resume_sync(&self.holder, self.error.take())
    }

    fn resolve(&mut self, _: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(output)
    }
}

fn resume_sync(holder: &GeneratorHolder, error: Option<String>) -> napi::Result<GeneratorResponse> {
    let result = holder.generator.lock().unwrap().resume(error)?;
    if let GeneratorResponse::Done = result {
        let response = holder.response.lock().unwrap().take();
        Ok(response.unwrap_or(GeneratorResponse::Done))
    } else {
        Ok(result)
    }
}

#[napi]
impl GeneratorHolder {
    #[napi]
    pub fn resume_sync(&self, error: Option<String>) -> napi::Result<GeneratorResponse> {
        resume_sync(self, error)
    }

    #[napi]
    pub fn resume_async(&self, error: Option<String>) -> napi::Result<AsyncTask<ResumeTask>> {
        Ok(AsyncTask::new(ResumeTask {
            holder: self.clone(),
            error,
        }))
    }
}
