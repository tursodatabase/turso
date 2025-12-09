use std::sync::Arc;

use turso_core::{Completion, LimboError, OpenFlags};

use crate::{
    database_tape::{DatabaseTape, DatabaseTapeOpts},
    types::{Coro, SyncEngineIoResult},
    Result,
};

pub trait IoOperations {
    fn open_tape(&self, path: &str, capture: bool) -> Result<DatabaseTape>;
    fn try_open(&self, path: &str) -> Result<Option<Arc<dyn turso_core::File>>>;
    fn create(&self, path: &str) -> Result<Arc<dyn turso_core::File>>;
    fn truncate<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        file: Arc<dyn turso_core::File>,
        len: usize,
    ) -> impl std::future::Future<Output = Result<()>>;
}

impl IoOperations for Arc<dyn turso_core::IO> {
    fn open_tape(&self, path: &str, capture: bool) -> Result<DatabaseTape> {
        let io = self.clone();
        let clean = turso_core::Database::open_file(io, path, false).unwrap();
        let opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some(if capture { "full" } else { "off" }.to_string()),
        };
        tracing::debug!("initialize database tape connection: path={}", path);
        Ok(DatabaseTape::new_with_opts(clean, opts))
    }
    fn try_open(&self, path: &str) -> Result<Option<Arc<dyn turso_core::File>>> {
        match self.open_file(path, OpenFlags::None, false) {
            Ok(file) => Ok(Some(file)),
            Err(LimboError::CompletionError(turso_core::CompletionError::IOError(
                std::io::ErrorKind::NotFound,
            ))) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
    fn create(&self, path: &str) -> Result<Arc<dyn turso_core::File>> {
        match self.open_file(path, OpenFlags::Create, false) {
            Ok(file) => Ok(file),
            Err(err) => Err(err.into()),
        }
    }

    async fn truncate<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        file: Arc<dyn turso_core::File>,
        len: usize,
    ) -> Result<()> {
        let c = Completion::new_trunc(move |rc| {
            let Ok(rc) = rc else {
                return;
            };
            tracing::debug!("file truncated: rc={}", rc);
        });
        let c = file.truncate(len as u64, c)?;
        while !c.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
        Ok(())
    }
}
