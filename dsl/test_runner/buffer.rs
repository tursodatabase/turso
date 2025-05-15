//! Modified code from Gag to accomodate my use case of having stdout and stderr be redirected to the same file

use std::fs::File;
use std::io::{self, Read};

use gag::Redirect;
use tempfile::NamedTempFile;

/// Buffer output in an in-memory buffer.
pub struct BufferRedirect {
    #[allow(dead_code)]
    stdout: Redirect<File>,
    #[allow(dead_code)]
    stderr: Redirect<File>,
    outer: File,
}

impl BufferRedirect {
    pub fn stdout_stderr() -> io::Result<BufferRedirect> {
        let tempfile = NamedTempFile::new()?;
        let outer = tempfile.reopen()?;
        let stdout = Redirect::stdout(tempfile.reopen()?)?;
        let stderr = Redirect::stderr(tempfile.reopen()?)?;
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
