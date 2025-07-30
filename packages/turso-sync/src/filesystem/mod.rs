#[cfg(test)]
pub mod test;
pub mod tokio;

use std::path::Path;
use turso_sync_protocol::Result;

pub trait Filesystem {
    type File;
    fn exists_file(&self, path: &Path) -> impl std::future::Future<Output = Result<bool>> + Send;
    fn remove_file(&self, path: &Path) -> impl std::future::Future<Output = Result<()>> + Send;
    fn create_file(
        &self,
        path: &Path,
    ) -> impl std::future::Future<Output = Result<Self::File>> + Send;
    fn open_file(
        &self,
        path: &Path,
    ) -> impl std::future::Future<Output = Result<Self::File>> + Send;
    fn copy_file(
        &self,
        src: &Path,
        dst: &Path,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn rename_file(
        &self,
        src: &Path,
        dst: &Path,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn truncate_file(
        &self,
        file: &Self::File,
        size: usize,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn write_file(
        &self,
        file: &mut Self::File,
        buf: &[u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn sync_file(&self, file: &Self::File) -> impl std::future::Future<Output = Result<()>> + Send;
    fn read_file(&self, path: &Path) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send;
}
