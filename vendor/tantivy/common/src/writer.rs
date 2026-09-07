use std::io::{self, BufWriter, Write};

pub struct CountingWriter<W> {
    underlying: W,
    written_bytes: u64,
}

impl<W: Write> CountingWriter<W> {
    pub fn wrap(underlying: W) -> CountingWriter<W> {
        CountingWriter {
            underlying,
            written_bytes: 0,
        }
    }

    #[inline]
    pub fn written_bytes(&self) -> u64 {
        self.written_bytes
    }

    /// Returns the underlying write object.
    /// Note that this method does not trigger any flushing.
    #[inline]
    pub fn finish(self) -> W {
        self.underlying
    }
}

impl<W: Write> Write for CountingWriter<W> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written_size = self.underlying.write(buf)?;
        self.written_bytes += written_size as u64;
        Ok(written_size)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.underlying.write_all(buf)?;
        self.written_bytes += buf.len() as u64;
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.underlying.flush()
    }
}

impl<W: TerminatingWrite> TerminatingWrite for CountingWriter<W> {
    #[inline]
    fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        self.underlying.terminate_ref(token)
    }
}

/// Struct used to prevent from calling
/// [`terminate_ref`](TerminatingWrite::terminate_ref) directly
///
/// The point is that while the type is public, it cannot be built by anyone
/// outside of this module.
pub struct AntiCallToken(());

/// Trait used to indicate when no more write need to be done on a writer
///
/// Thread-safety is enforced at the call sites that require it.
pub trait TerminatingWrite: Write {
    /// Indicate that the writer will no longer be used. Internally call terminate_ref.
    fn terminate(mut self) -> io::Result<()>
    where Self: Sized {
        self.terminate_ref(AntiCallToken(()))
    }

    /// You should implement this function to define custom behavior.
    /// This function should flush any buffer it may hold.
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()>;
}

impl<W: TerminatingWrite + ?Sized> TerminatingWrite for Box<W> {
    fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        self.as_mut().terminate_ref(token)
    }
}

impl<W: TerminatingWrite> TerminatingWrite for BufWriter<W> {
    fn terminate_ref(&mut self, a: AntiCallToken) -> io::Result<()> {
        self.flush()?;
        self.get_mut().terminate_ref(a)
    }
}

impl TerminatingWrite for &mut Vec<u8> {
    fn terminate_ref(&mut self, _a: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

#[cfg(test)]
mod test {

    use std::io::Write;

    use super::CountingWriter;

    #[test]
    fn test_counting_writer() {
        let buffer: Vec<u8> = vec![];
        let mut counting_writer = CountingWriter::wrap(buffer);
        let bytes = (0u8..10u8).collect::<Vec<u8>>();
        counting_writer.write_all(&bytes).unwrap();
        let len = counting_writer.written_bytes();
        let buffer_restituted: Vec<u8> = counting_writer.finish();
        assert_eq!(len, 10u64);
        assert_eq!(buffer_restituted.len(), 10);
    }
}
