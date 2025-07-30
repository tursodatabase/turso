use std::path::Path;

use tokio::io::AsyncWriteExt;
use turso_sync_protocol::Result;

use crate::filesystem::Filesystem;

pub struct TokioFilesystem();

impl Filesystem for TokioFilesystem {
    type File = tokio::fs::File;

    async fn exists_file(&self, path: &Path) -> Result<bool> {
        tracing::debug!("check file exists at {:?}", path);
        Ok(tokio::fs::try_exists(&path).await?)
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        tracing::debug!("remove file at {:?}", path);
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn create_file(&self, path: &Path) -> Result<Self::File> {
        tracing::debug!("create file at {:?}", path);
        Ok(tokio::fs::File::create_new(path)
            .await
            .inspect_err(|e| tracing::error!("failed to create file at {:?}: {}", path, e))?)
    }

    async fn open_file(&self, path: &Path) -> Result<Self::File> {
        tracing::debug!("open file at {:?}", path);
        Ok(tokio::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(path)
            .await?)
    }

    async fn copy_file(&self, src: &Path, dst: &Path) -> Result<()> {
        tracing::debug!("copy file from {:?} to {:?}", src, dst);
        tokio::fs::copy(&src, &dst).await?;
        Ok(())
    }

    async fn rename_file(&self, src: &Path, dst: &Path) -> Result<()> {
        tracing::debug!("rename file from {:?} to {:?}", src, dst);
        tokio::fs::rename(&src, &dst)
            .await
            .inspect_err(|e| tracing::error!("failed to rename {:?} to {:?}: {}", src, dst, e))?;
        Ok(())
    }

    async fn truncate_file(&self, file: &Self::File, size: usize) -> Result<()> {
        tracing::debug!("truncate file to size {}", size);
        file.set_len(size as u64).await?;
        Ok(())
    }

    async fn write_file(&self, file: &mut Self::File, buf: &[u8]) -> Result<()> {
        tracing::debug!("write buffer of size {} to file", buf.len());
        file.write_all(buf).await?;
        Ok(())
    }

    async fn sync_file(&self, file: &Self::File) -> Result<()> {
        tracing::debug!("sync file");
        file.sync_all().await?;
        Ok(())
    }

    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        tracing::debug!("read file {:?}", path);
        Ok(tokio::fs::read(path).await?)
    }
}
