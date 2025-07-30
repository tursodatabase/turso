use turso_sync_protocol::{
    types::{DbSyncInfo, DbSyncStatus},
    Result,
};

#[cfg(test)]
pub mod test;
pub mod turso;

pub trait Stream {
    fn read_chunk(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<bytes::Bytes>>> + Send;
}

pub trait SyncServer {
    type Stream: Stream;
    fn db_info(&self) -> impl std::future::Future<Output = Result<DbSyncInfo>> + Send;
    fn db_export(
        &self,
        generation_id: usize,
    ) -> impl std::future::Future<Output = Result<Self::Stream>> + Send;
    fn wal_pull(
        &self,
        generation_id: usize,
        start_frame: usize,
    ) -> impl std::future::Future<Output = Result<Self::Stream>> + Send;
    fn wal_push(
        &self,
        baton: Option<String>,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
        frames: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<DbSyncStatus>> + Send;
}
