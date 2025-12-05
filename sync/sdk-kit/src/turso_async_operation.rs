use std::{
    mem::ManuallyDrop,
    sync::{Arc, Mutex},
};

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

impl TursoAsyncOperationStatus {
    pub fn to_capi(self) -> c::turso_sync_operation_resume_result_t {
        c::turso_sync_operation_resume_result_t {
            status: self.status.to_capi(),
            result: match self.result {
                Some(TursoAsyncOperationResult::Stats { stats }) => {
                    c::turso_sync_operation_result_t {
                        type_: c::turso_sync_operation_result_type_t::TURSO_ASYNC_RESULT_STATS,
                        result: c::turso_sync_operation_result_union_t {
                            stats: ManuallyDrop::new(c::turso_sync_stats_t {
                                cdc_operations: stats.cdc_operations,
                                main_wal_size: stats.main_wal_size as i64,
                                revert_wal_size: stats.revert_wal_size as i64,
                                last_pull_unix_time: stats.last_pull_unix_time.unwrap_or(0),
                                last_push_unix_time: stats.last_push_unix_time.unwrap_or(0),
                                network_sent_bytes: stats.network_sent_bytes as i64,
                                network_received_bytes: stats.network_received_bytes as i64,
                                revision: if let Some(revision) = stats.revision {
                                    turso_slice_from_bytes(revision.as_bytes())
                                } else {
                                    turso_slice_ref_t::default()
                                },
                            }),
                        },
                    }
                }
                Some(TursoAsyncOperationResult::Connection { connection }) => {
                    c::turso_sync_operation_result_t {
                        type_: c::turso_sync_operation_result_type_t::TURSO_ASYNC_RESULT_CONNECTION,
                        result: c::turso_sync_operation_result_union_t {
                            connection: ManuallyDrop::new(connection.to_capi()),
                        },
                    }
                }
                Some(TursoAsyncOperationResult::Changes { changes }) => {
                    c::turso_sync_operation_result_t {
                        type_: c::turso_sync_operation_result_type_t::TURSO_ASYNC_RESULT_CHANGES,
                        result: c::turso_sync_operation_result_union_t {
                            changes: ManuallyDrop::new(changes.to_capi()),
                        },
                    }
                }
                None => c::turso_sync_operation_result_t {
                    type_: c::turso_sync_operation_result_type_t::TURSO_ASYNC_RESULT_NONE,
                    ..Default::default()
                },
            },
        }
    }
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
    pub fn resume(&self) -> Result<TursoAsyncOperationStatus, rsapi::TursoError> {
        let result = self.generator.lock().unwrap().resume()?;
        if result.status == rsapi::TursoStatusCode::Done {
            let response = self.response.lock().unwrap().take();
            Ok(TursoAsyncOperationStatus {
                status: result.status,
                result: response,
            })
        } else {
            Ok(result)
        }
    }
    pub fn to_capi(self: Box<Self>) -> c::turso_sync_operation_t {
        c::turso_sync_operation_t {
            inner: Box::into_raw(self) as *mut std::ffi::c_void,
        }
    }
    /// helper method to restore [TursoDatabaseAsyncOperation] ref from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn ref_from_capi<'a>(
        value: c::turso_sync_operation_t,
    ) -> Result<&'a Self, TursoError> {
        if value.inner.is_null() {
            Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("got null pointer".to_string()),
            })
        } else {
            Ok(&*(value.inner as *const Self))
        }
    }
    /// helper method to restore [TursoDatabaseAsyncOperation] instance from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn box_from_capi(value: c::turso_sync_operation_t) -> Box<Self> {
        Box::from_raw(value.inner as *mut Self)
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
