//! Modified code from Gag to accomodate my use case of having stdout and stderr be redirected to the same file

use std::fs::File;
use std::io::{self, Read};
use std::sync::Arc;

use gag::Redirect;
use tempfile::NamedTempFile;

/// Buffer output in an in-memory buffer.
pub struct BufferRedirect {
    #[allow(dead_code)]
    stdout: Redirect<Arc<File>>,
    #[allow(dead_code)]
    stderr: Redirect<Arc<File>>,
    outer: File,
}

/// An in-memory read-only buffer into which BufferRedirect buffers output.
pub struct Buffer(File);

impl Read for Buffer {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl BufferRedirect {
    pub fn stdout_stderr() -> io::Result<BufferRedirect> {
        let tempfile = NamedTempFile::new()?;
        let inner = Arc::new(tempfile.reopen()?);
        let outer = tempfile.reopen()?;
        let stdout = Redirect::stdout(inner.clone())?;
        let stderr = Redirect::stderr(inner)?;
        Ok(BufferRedirect {
            stdout,
            stderr,
            outer,
        })
    }
}

impl Read for BufferRedirect {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.outer.read(buf)
    }
}
