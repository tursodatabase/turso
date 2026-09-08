use std::io;
use std::path::PathBuf;

use common::HasLen;

use super::{AsyncWrite, AsyncWritePtr, Directory};

/// Transaction-private scratch for formats that place an index before its body.
/// The directory owns cleanup after cancellation; successful copies retire the
/// name explicitly. No synchronous reads or writes are used.
pub(crate) struct AsyncSpool {
    path: PathBuf,
    write: AsyncWritePtr,
    len: u64,
}

impl AsyncSpool {
    pub async fn open(directory: &dyn Directory) -> io::Result<Self> {
        let path = PathBuf::from(format!(
            ".scratch-{}",
            crate::index::SegmentId::generate_random()
        ));
        let write = directory
            .open_write_async(&path)
            .await
            .map_err(io::Error::other)?;
        Ok(Self {
            path,
            write,
            len: 0,
        })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub async fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write.write_all(bytes).await?;
        self.len += bytes.len() as u64;
        Ok(())
    }

    pub async fn copy_to(
        self,
        directory: &dyn Directory,
        output: &mut dyn AsyncWrite,
    ) -> io::Result<u64> {
        self.write.finish().await?;
        let file = directory
            .open_read_async(&self.path)
            .await
            .map_err(io::Error::other)?;
        if file.len() as u64 != self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Scratch length changed",
            ));
        }
        const COPY_BYTES: usize = 64 * 1024;
        for start in (0..file.len()).step_by(COPY_BYTES) {
            let bytes = file
                .slice(start..(start + COPY_BYTES).min(file.len()))
                .read_bytes_async()
                .await?;
            output.write_all(&bytes).await?;
        }
        directory
            .delete_async(&self.path)
            .await
            .map_err(io::Error::other)?;
        Ok(self.len)
    }
}
