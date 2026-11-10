use crate::{
    types::{DatabaseRowMutation, DatabaseRowTransformResult},
    Result,
};

pub trait DataPollResult<T>: Send + Sync + 'static {
    fn data(&self) -> &[T];
}

pub trait DataCompletion<T>: Send + Sync + 'static {
    type DataPollResult: DataPollResult<T>;
    fn status(&self) -> Result<Option<u16>>;
    fn poll_data(&self) -> Result<Option<Self::DataPollResult>>;
    fn is_done(&self) -> Result<bool>;
}

pub trait SyncEngineIo: Send + Sync + 'static {
    type DataCompletionBytes: DataCompletion<u8>;
    type DataCompletionTransform: DataCompletion<DatabaseRowTransformResult>;
    /// Read a complete host file. Hosts whose storage differs from core IO
    /// should report status 404 for a missing file, 204 for an existing empty
    /// file, and 200 for nonempty data. This lets encrypted metadata reject an
    /// empty/corrupt file without confusing it with a fresh bootstrap. Legacy
    /// hosts may omit status when their files are also visible through core IO.
    fn full_read(&self, path: &str) -> Result<Self::DataCompletionBytes>;
    fn full_write(&self, path: &str, content: Vec<u8>) -> Result<Self::DataCompletionBytes>;
    fn transform(
        &self,
        mutations: Vec<DatabaseRowMutation>,
    ) -> Result<Self::DataCompletionTransform>;
    fn http(
        &self,
        url: Option<&str>,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        headers: &[(&str, &str)],
    ) -> Result<Self::DataCompletionBytes>;
    fn add_io_callback(&self, callback: Box<dyn FnMut() -> bool + Send>);
    fn step_io_callbacks(&self);
}
