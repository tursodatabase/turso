use std::{io::Write, sync::Arc};

use turso_sync_protocol::Result;

use crate::{filesystem::Filesystem, test_context::TestContext};

pub struct TestFilesystem {
    ctx: Arc<TestContext>,
}

impl TestFilesystem {
    pub fn new(ctx: Arc<TestContext>) -> Self {
        Self { ctx }
    }
}

impl Filesystem for TestFilesystem {
    type File = std::fs::File;

    async fn exists_file(&self, path: &std::path::Path) -> Result<bool> {
        self.ctx.faulty_call("exists_file_start").await?;
        let result = std::fs::exists(path)?;
        self.ctx.faulty_call("exists_file_end").await?;
        Ok(result)
    }

    async fn remove_file(&self, path: &std::path::Path) -> Result<()> {
        self.ctx.faulty_call("remove_file_start").await?;
        match std::fs::remove_file(path) {
            Ok(()) => Result::Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Result::Ok(()),
            Err(e) => Err(e.into()),
        }?;
        self.ctx.faulty_call("remove_file_end").await?;
        Ok(())
    }

    async fn create_file(&self, path: &std::path::Path) -> Result<Self::File> {
        self.ctx.faulty_call("create_file_start").await?;
        let result = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        self.ctx.faulty_call("create_file_end").await?;
        Ok(result)
    }

    async fn open_file(&self, path: &std::path::Path) -> Result<Self::File> {
        self.ctx.faulty_call("open_file_start").await?;
        let result = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        self.ctx.faulty_call("open_file_end").await?;
        Ok(result)
    }

    async fn copy_file(&self, src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
        self.ctx.faulty_call("copy_file_start").await?;
        std::fs::copy(src, dst)?;
        self.ctx.faulty_call("copy_file_end").await?;
        Ok(())
    }

    async fn rename_file(&self, src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
        self.ctx.faulty_call("rename_file_start").await?;
        std::fs::rename(src, dst)?;
        self.ctx.faulty_call("rename_file_end").await?;
        Ok(())
    }

    async fn truncate_file(&self, file: &Self::File, size: usize) -> Result<()> {
        self.ctx.faulty_call("truncate_file_start").await?;
        file.set_len(size as u64)?;
        self.ctx.faulty_call("truncate_file_end").await?;
        Ok(())
    }

    async fn write_file(&self, file: &mut Self::File, buf: &[u8]) -> Result<()> {
        self.ctx.faulty_call("write_file_start").await?;
        file.write_all(buf)?;
        self.ctx.faulty_call("write_file_end").await?;
        Ok(())
    }

    async fn sync_file(&self, file: &Self::File) -> Result<()> {
        self.ctx.faulty_call("sync_file_start").await?;
        file.sync_all()?;
        self.ctx.faulty_call("sync_file_end").await?;
        Ok(())
    }

    async fn read_file(&self, path: &std::path::Path) -> Result<Vec<u8>> {
        self.ctx.faulty_call("read_file_start").await?;
        let data = std::fs::read(path)?;
        self.ctx.faulty_call("read_file_end").await?;
        Ok(data)
    }
}
