use napi::Env;
use napi_derive::napi;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use turso_sync_engine::types::ProtocolCommand;

pub const GENERATOR_RESUME_IO: u32 = 0;
pub const GENERATOR_RESUME_DONE: u32 = 1;

pub trait Generator {
    fn resume(&mut self, env: Env, result: Option<String>) -> napi::Result<u32>;
}

impl<F: Future<Output = turso_sync_engine::Result<()>>> Generator
    for genawaiter::sync::Gen<ProtocolCommand, turso_sync_engine::Result<Env>, F>
{
    fn resume(&mut self, env: Env, error: Option<String>) -> napi::Result<u32> {
        let result = match error {
            Some(err) => Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                format!("JsProtocolIo error: {err}"),
            )),
            None => Ok(env),
        };
        match self.resume_with(result) {
            genawaiter::GeneratorState::Yielded(ProtocolCommand::IO) => Ok(GENERATOR_RESUME_IO),
            genawaiter::GeneratorState::Complete(Ok(())) => Ok(GENERATOR_RESUME_DONE),
            genawaiter::GeneratorState::Complete(Err(err)) => Err(napi::Error::new(
                napi::Status::GenericFailure,
                format!("sync engine operation failed: {err}"),
            )),
        }
    }
}

#[napi(discriminant = "type")]
pub enum GeneratorResponse {
    SyncEngineStats { operations: i64, wal: i64 },
}

#[napi]
pub struct GeneratorHolder {
    pub(crate) inner: Box<Mutex<dyn Generator>>,
    pub(crate) response: Arc<Mutex<Option<GeneratorResponse>>>,
}

#[napi]
impl GeneratorHolder {
    #[napi]
    pub fn resume(&self, env: Env, error: Option<String>) -> napi::Result<u32> {
        self.inner.lock().unwrap().resume(env, error)
    }
    #[napi]
    pub fn take(&self) -> Option<GeneratorResponse> {
        self.response.lock().unwrap().take()
    }
}
