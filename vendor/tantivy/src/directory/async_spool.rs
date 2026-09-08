use std::io;
use std::path::PathBuf;

use common::HasLen;

use super::{AsyncWrite, AsyncWritePtr, Directory};

/// Transaction-private scratch for formats that place an index before its body.
/// The directory owns cleanup after cancellation; successful copies retire the
/// name explicitly. No synchronous reads or writes are used.
pub(crate) struct AsyncSpool {
    directory: Box<dyn Directory>,
    data: Option<SpoolData>,
    len: u64,
}

enum SpoolData {
    Resident(Vec<u8>),
    Stored { path: PathBuf, write: AsyncWritePtr },
}

const BUFFER_BYTES: usize = 64 * 1024;

impl AsyncSpool {
    pub fn new(directory: &dyn Directory) -> Self {
        Self {
            directory: directory.box_clone(),
            data: Some(SpoolData::Resident(Vec::new())),
            len: 0,
        }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub async fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let data = self.data.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scratch append invalidated by failure or cancellation",
            )
        })?;
        self.data = Some(match data {
            SpoolData::Resident(mut buffer) if bytes.len() <= BUFFER_BYTES - buffer.len() => {
                // Tiny posting lists must not create several B-tree files per term.
                buffer.reserve_exact(bytes.len());
                buffer.extend_from_slice(bytes);
                SpoolData::Resident(buffer)
            }
            SpoolData::Resident(buffer) => {
                let path = PathBuf::from(format!(
                    ".scratch-{}",
                    crate::index::SegmentId::generate_random()
                ));
                let mut write = self
                    .directory
                    .open_write_async(&path)
                    .await
                    .map_err(io::Error::other)?;
                write.write_all(&buffer).await?;
                drop(buffer);
                write.write_all(bytes).await?;
                SpoolData::Stored { path, write }
            }
            SpoolData::Stored { path, mut write } => {
                write.write_all(bytes).await?;
                SpoolData::Stored { path, write }
            }
        });
        self.len += bytes.len() as u64;
        Ok(())
    }

    pub async fn copy_to(
        self,
        directory: &dyn Directory,
        output: &mut dyn AsyncWrite,
    ) -> io::Result<u64> {
        let data = self.data.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scratch append invalidated by failure or cancellation",
            )
        })?;
        let (path, write) = match data {
            SpoolData::Resident(buffer) => {
                output.write_all(&buffer).await?;
                return Ok(self.len);
            }
            SpoolData::Stored { path, write } => (path, write),
        };
        write.finish().await?;
        let file = directory
            .open_read_async(&path)
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
            .delete_async(&path)
            .await
            .map_err(io::Error::other)?;
        Ok(self.len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::tests::AsyncOutputDirectory;

    #[test]
    fn scratch_spills_only_above_buffer_limit() -> io::Result<()> {
        for size in [0, 17, BUFFER_BYTES, BUFFER_BYTES + 1, BUFFER_BYTES * 3] {
            let directory = AsyncOutputDirectory::default();
            directory.run(async {
                let mut spool = AsyncSpool::new(&directory);
                let bytes = vec![91; size];
                for part in bytes.chunks(127) {
                    spool.append(part).await?;
                }
                match spool.data.as_ref().unwrap() {
                    SpoolData::Resident(buffer) => {
                        assert!(size <= BUFFER_BYTES);
                        assert!(buffer.capacity() <= BUFFER_BYTES);
                    }
                    SpoolData::Stored { .. } => assert!(size > BUFFER_BYTES),
                }
                let path = std::path::Path::new("output");
                let mut output = directory
                    .open_write_async(path)
                    .await
                    .map_err(io::Error::other)?;
                assert_eq!(
                    spool.copy_to(&directory, output.as_mut()).await?,
                    size as u64
                );
                output.finish().await?;
                let file = directory
                    .open_read_async(path)
                    .await
                    .map_err(io::Error::other)?;
                assert_eq!(file.read_bytes_async().await?.as_slice(), bytes);
                io::Result::Ok(())
            })?;
        }
        Ok(())
    }
}
