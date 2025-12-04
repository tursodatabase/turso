use std::sync::{Arc, Mutex};

use turso_sdk_kit::{
    capi::c::turso_slice_ref_t,
    rsapi::{self, turso_slice_from_bytes, TursoError, TursoStatusCode},
};

use crate::{
    capi::c::{self},
    rsapi::TursoDatabaseSyncChanges,
};

pub struct TursoAsyncOperationStatus {
    pub status: rsapi::TursoStatusCode,
    pub result: Option<TursoAsyncOperationResult>,
}

pub enum TursoAsyncOperationResult {
    Connection {
        connection: Arc<turso_sdk_kit::rsapi::TursoConnection>,
    },
    Stats {
        stats: turso_sync_engine::types::SyncEngineStats,
    },
    Changes {
        changes: Box<TursoDatabaseSyncChanges>,
    },
}

pub trait TursoAsyncOperation {
    fn resume(&mut self) -> Result<TursoAsyncOperationStatus, rsapi::TursoError>;
}

pub struct TursoDatabaseAsyncOperation {
    pub generator: Arc<Mutex<dyn TursoAsyncOperation + Send + 'static>>,
    pub response: Arc<Mutex<Option<TursoAsyncOperationResult>>>,
}

// todo(sivukhin): unsafe - get rid of this
unsafe impl Send for TursoDatabaseAsyncOperation {}

type Generator = Box<
    dyn FnOnce(
            turso_sync_engine::types::Coro<()>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = turso_sync_engine::Result<Option<TursoAsyncOperationResult>>,
                    > + Send,
            >,
        > + Send,
>;

impl TursoDatabaseAsyncOperation {
    pub fn new(f: Generator) -> Self {
        let response = Arc::new(Mutex::new(None));
        let generator = genawaiter::sync::Gen::new({
            let response = response.clone();
            |coro| async move {
                let coro = turso_sync_engine::types::Coro::new((), coro);
                *response.lock().unwrap() = f(coro).await?;
                Ok(())
            }
        });
        Self {
            generator: Arc::new(Mutex::new(generator)),
            response,
        }
    }
    pub fn resume(&self) -> Result<rsapi::TursoStatusCode, rsapi::TursoError> {
        let result = self.generator.lock().unwrap().resume()?;
        Ok(result.status)
    }
    pub fn take_result(&self) -> Result<TursoAsyncOperationResult, TursoError> {
        match self.response.lock().unwrap().take() {
            Some(response) => Ok(response),
            None => Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("operation has no result".to_string()),
            }),
        }
    }
    pub fn take_connection_to_capi(
        &self,
    ) -> Result<*const turso_sdk_kit::capi::c::turso_connection_t, TursoError> {
        match self.take_result()? {
            TursoAsyncOperationResult::Connection { connection } => Ok(connection.to_capi()),
            _ => Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("unexpected async operation result".to_string()),
            }),
        }
    }
    pub fn take_changes_to_capi(&self) -> Result<*const c::turso_sync_changes_t, TursoError> {
        match self.take_result()? {
            TursoAsyncOperationResult::Changes { changes } => {
                if changes.empty() {
                    Ok(std::ptr::null())
                } else {
                    Ok(changes.to_capi())
                }
            }
            _ => Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("unexpected async operation result".to_string()),
            }),
        }
    }
    pub fn get_stats_to_capi(&self) -> Result<c::turso_sync_stats_t, TursoError> {
        match self.response.lock().unwrap().as_ref() {
            Some(TursoAsyncOperationResult::Stats { stats }) => Ok(c::turso_sync_stats_t {
                cdc_operations: stats.cdc_operations,
                main_wal_size: stats.main_wal_size as i64,
                revert_wal_size: stats.revert_wal_size as i64,
                last_pull_unix_time: stats.last_pull_unix_time.unwrap_or(0),
                last_push_unix_time: stats.last_push_unix_time.unwrap_or(0),
                network_sent_bytes: stats.network_sent_bytes as i64,
                network_received_bytes: stats.network_received_bytes as i64,
                revision: if let Some(revision) = &stats.revision {
                    turso_slice_from_bytes(revision.as_bytes())
                } else {
                    turso_slice_ref_t::default()
                },
            }),
            _ => Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("unexpected async operation result".to_string()),
            }),
        }
    }
    pub fn result_kind_to_capi(&self) -> c::turso_sync_operation_result_type_t {
        match &*self.response.lock().unwrap() {
            Some(TursoAsyncOperationResult::Connection { .. }) => {
                c::turso_sync_operation_result_type_t::TURSO_ASYNC_RESULT_CONNECTION
            }
            Some(TursoAsyncOperationResult::Changes { .. }) => {
                c::turso_sync_operation_result_type_t::TURSO_ASYNC_RESULT_CHANGES
            }
            Some(TursoAsyncOperationResult::Stats { .. }) => {
                c::turso_sync_operation_result_type_t::TURSO_ASYNC_RESULT_STATS
            }
            None => c::turso_sync_operation_result_type_t::TURSO_ASYNC_RESULT_NONE,
        }
    }
    pub fn to_capi(self: Box<Self>) -> *mut c::turso_sync_operation_t {
        Box::into_raw(self) as *mut c::turso_sync_operation_t
    }
    /// helper method to restore [TursoDatabaseAsyncOperation] ref from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn ref_from_capi<'a>(
        value: *const c::turso_sync_operation_t,
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
    /// helper method to restore [TursoDatabaseAsyncOperation] instance from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn box_from_capi(value: *const c::turso_sync_operation_t) -> Box<Self> {
        Box::from_raw(value as *mut Self)
    }
}

type OperationGen<F> = genawaiter::sync::Gen<
    turso_sync_engine::types::SyncEngineIoResult,
    turso_sync_engine::Result<()>,
    F,
>;

impl<F: std::future::Future<Output = turso_sync_engine::Result<()>>> TursoAsyncOperation
    for OperationGen<F>
{
    fn resume(&mut self) -> Result<TursoAsyncOperationStatus, rsapi::TursoError> {
        match self.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(
                turso_sync_engine::types::SyncEngineIoResult::IO,
            ) => Ok(TursoAsyncOperationStatus {
                status: rsapi::TursoStatusCode::Io,
                result: None,
            }),
            genawaiter::GeneratorState::Complete(Ok(())) => Ok(TursoAsyncOperationStatus {
                status: rsapi::TursoStatusCode::Done,
                result: None,
            }),
            genawaiter::GeneratorState::Complete(Err(err)) => Err(rsapi::TursoError {
                code: turso_sdk_kit::rsapi::TursoStatusCode::Error,
                message: Some(format!("sync engine operation failed: {err}")),
            }),
        }
    }
}
