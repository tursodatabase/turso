use std::fs::File;
use std::ops::{Deref, Range, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use ownedbytes::{OwnedBytes, StableDeref};

use crate::{ByteCount, HasLen};

/// Objects that represents files sections in tantivy.
///
/// By contract, whatever happens to the directory file, as long as a FileHandle
/// is alive, the data associated with it cannot be altered or destroyed.
///
/// The underlying behavior is therefore specific to the `Directory` that
/// created it. Despite its name, a [`FileSlice`] may or may not directly map to an actual file
/// on the filesystem.

#[async_trait]
pub trait FileHandle: 'static + Send + Sync + HasLen + fmt::Debug {
    /// Reads a slice of bytes.
    ///
    /// This method may panic if the range requested is invalid.
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes>;

    #[doc(hidden)]
    async fn read_bytes_async(&self, _byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Async read is not supported.",
        ))
    }
}

#[derive(Debug)]
/// A File with it's length included.
pub struct WrapFile {
    file: File,
    len: usize,
}
impl WrapFile {
    /// Creates a new WrapFile and stores its length.
    pub fn new(file: File) -> io::Result<Self> {
        let len = file.metadata()?.len() as usize;
        Ok(WrapFile { file, len })
    }
}

#[async_trait]
impl FileHandle for WrapFile {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let file_len = self.len();

        // Calculate the actual range to read, ensuring it stays within file boundaries
        let start = range.start;
        let end = range.end.min(file_len);

        // Ensure the start is before the end of the range
        if start >= end {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid range"));
        }

        let mut buffer = vec![0; end - start];

        #[cfg(unix)]
        {
            use std::os::unix::prelude::FileExt;
            self.file.read_exact_at(&mut buffer, start as u64)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek};
            let mut file = self.file.try_clone()?; // Clone the file to read from it separately
            // Seek to the start position in the file
            file.seek(io::SeekFrom::Start(start as u64))?;
            // Read the data into the buffer
            file.read_exact(&mut buffer)?;
        }

        Ok(OwnedBytes::new(buffer))
    }
    // todo implement async
}
impl HasLen for WrapFile {
    fn len(&self) -> usize {
        self.len
    }
}

#[async_trait]
impl FileHandle for &'static [u8] {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let bytes = &self[range];
        Ok(OwnedBytes::new(bytes))
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        Ok(self.read_bytes(byte_range)?)
    }
}

impl<B> From<B> for FileSlice
where B: StableDeref + Deref<Target = [u8]> + 'static + Send + Sync
{
    fn from(bytes: B) -> FileSlice {
        FileSlice::new(Arc::new(OwnedBytes::new(bytes)))
    }
}

/// Logical slice of read only file in tantivy.
///
/// It can be cloned and sliced cheaply.
#[derive(Clone)]
pub struct FileSlice {
    data: Arc<dyn FileHandle>,
    range: Range<usize>,
}

impl fmt::Debug for FileSlice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSlice({:?}, {:?})", &self.data, self.range)
    }
}

impl FileSlice {
    pub fn stream_file_chunks(&self) -> impl Iterator<Item = io::Result<OwnedBytes>> + '_ {
        let len = self.range.end;
        let mut start = self.range.start;
        std::iter::from_fn(move || {
            /// Returns chunks of 1MB of data from the FileHandle.
            const CHUNK_SIZE: usize = 1024 * 1024; // 1MB

            if start < len {
                let end = (start + CHUNK_SIZE).min(len);
                let range = start..end;
                let chunk = self.data.read_bytes(range);
                start += CHUNK_SIZE;
                match chunk {
                    Ok(chunk) => Some(Ok(chunk)),
                    Err(e) => Some(Err(e)),
                }
            } else {
                None
            }
        })
    }
}

/// Takes a range, a `RangeBounds` object, and returns
/// a `Range` that corresponds to the relative application of the
/// `RangeBounds` object to the original `Range`.
///
/// For instance, combine_ranges(`[2..11)`, `[5..7]`) returns `[7..10]`
/// as it reads, what is the sub-range that starts at the 5 element of
/// `[2..11)` and ends at the 9th element included.
///
/// This function panics, if the result would suggest something outside
/// of the bounds of the original range.
fn combine_ranges<R: RangeBounds<usize>>(orig_range: Range<usize>, rel_range: R) -> Range<usize> {
    let start: usize = orig_range.start
        + match rel_range.start_bound().cloned() {
            std::ops::Bound::Included(rel_start) => rel_start,
            std::ops::Bound::Excluded(rel_start) => rel_start + 1,
            std::ops::Bound::Unbounded => 0,
        };
    assert!(start <= orig_range.end);
    let end: usize = match rel_range.end_bound().cloned() {
        std::ops::Bound::Included(rel_end) => orig_range.start + rel_end + 1,
        std::ops::Bound::Excluded(rel_end) => orig_range.start + rel_end,
        std::ops::Bound::Unbounded => orig_range.end,
    };
    assert!(end >= start);
    assert!(end <= orig_range.end);
    start..end
}

impl FileSlice {
    /// Creates a FileSlice from a path.
    pub fn open(path: &Path) -> io::Result<FileSlice> {
        let wrap_file = WrapFile::new(File::open(path)?)?;
        Ok(FileSlice::new(Arc::new(wrap_file)))
    }

    /// Wraps a FileHandle.
    pub fn new(file_handle: Arc<dyn FileHandle>) -> Self {
        let num_bytes = file_handle.len();
        FileSlice::new_with_num_bytes(file_handle, num_bytes)
    }

    /// Wraps a FileHandle.
    #[doc(hidden)]
    #[must_use]
    pub fn new_with_num_bytes(file_handle: Arc<dyn FileHandle>, num_bytes: usize) -> Self {
        FileSlice {
            data: file_handle,
            range: 0..num_bytes,
        }
    }

    /// Creates a fileslice that is just a view over a slice of the data.
    ///
    /// # Panics
    ///
    /// Panics if `byte_range.end` exceeds the filesize.
    #[must_use]
    #[inline]
    pub fn slice<R: RangeBounds<usize>>(&self, byte_range: R) -> FileSlice {
        FileSlice {
            data: self.data.clone(),
            range: combine_ranges(self.range.clone(), byte_range),
        }
    }

    /// Creates an empty FileSlice
    pub fn empty() -> FileSlice {
        const EMPTY_SLICE: &[u8] = &[];
        FileSlice::from(EMPTY_SLICE)
    }

    /// Returns a `OwnedBytes` with all of the data in the `FileSlice`.
    ///
    /// The behavior is strongly dependent on the implementation of the underlying
    /// `Directory` and the `FileSliceTrait` it creates.
    /// In particular, it is  up to the `Directory` implementation
    /// to handle caching if needed.
    pub fn read_bytes(&self) -> io::Result<OwnedBytes> {
        self.data.read_bytes(self.range.clone())
    }

    #[doc(hidden)]
    pub async fn read_bytes_async(&self) -> io::Result<OwnedBytes> {
        self.data.read_bytes_async(self.range.clone()).await
    }

    /// Reads a specific slice of data.
    ///
    /// This is equivalent to running `file_slice.slice(from, to).read_bytes()`.
    pub fn read_bytes_slice(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        assert!(
            range.end <= self.len(),
            "end of requested range exceeds the fileslice length ({} > {})",
            range.end,
            self.len()
        );
        self.data
            .read_bytes(self.range.start + range.start..self.range.start + range.end)
    }

    #[doc(hidden)]
    pub async fn read_bytes_slice_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        assert!(
            self.range.start + byte_range.end <= self.range.end,
            "`to` exceeds the fileslice length"
        );
        self.data
            .read_bytes_async(
                self.range.start + byte_range.start..self.range.start + byte_range.end,
            )
            .await
    }

    /// Splits the FileSlice at the given offset and return two file slices.
    /// `file_slice[..split_offset]` and `file_slice[split_offset..]`.
    ///
    /// This operation is cheap and must not copy any underlying data.
    pub fn split(self, left_len: usize) -> (FileSlice, FileSlice) {
        let left = self.slice_to(left_len);
        let right = self.slice_from(left_len);
        (left, right)
    }

    /// Splits the file slice at the given offset and return two file slices.
    /// `file_slice[..split_offset]` and `file_slice[split_offset..]`.
    pub fn split_from_end(self, right_len: usize) -> (FileSlice, FileSlice) {
        let left_len = self.len() - right_len;
        self.split(left_len)
    }

    /// Like `.slice(...)` but enforcing only the `from`
    /// boundary.
    ///
    /// Equivalent to `.slice(from_offset, self.len())`
    #[must_use]
    pub fn slice_from(&self, from_offset: usize) -> FileSlice {
        self.slice(from_offset..self.len())
    }

    /// Returns a slice from the end.
    ///
    /// Equivalent to `.slice(self.len() - from_offset, self.len())`
    #[must_use]
    pub fn slice_from_end(&self, from_offset: usize) -> FileSlice {
        self.slice(self.len() - from_offset..self.len())
    }

    /// Like `.slice(...)` but enforcing only the `to`
    /// boundary.
    ///
    /// Equivalent to `.slice(0, to_offset)`
    #[must_use]
    pub fn slice_to(&self, to_offset: usize) -> FileSlice {
        self.slice(0..to_offset)
    }

    /// Returns the byte count of the FileSlice.
    pub fn num_bytes(&self) -> ByteCount {
        self.range.len().into()
    }
}

#[async_trait]
impl FileHandle for FileSlice {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.read_bytes_slice(range)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        self.read_bytes_slice_async(byte_range).await
    }
}

impl HasLen for FileSlice {
    fn len(&self) -> usize {
        self.range.len()
    }
}

#[async_trait]
impl FileHandle for OwnedBytes {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        Ok(self.slice(range))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.read_bytes(range)
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::ops::Bound;
    use std::sync::Arc;

    use super::{FileHandle, FileSlice};
    use crate::HasLen;
    use crate::file_slice::combine_ranges;

    #[test]
    fn test_file_slice() -> io::Result<()> {
        let file_slice = FileSlice::new(Arc::new(b"abcdef".as_ref()));
        assert_eq!(file_slice.len(), 6);
        assert_eq!(file_slice.slice_from(2).read_bytes()?.as_slice(), b"cdef");
        assert_eq!(file_slice.slice_to(2).read_bytes()?.as_slice(), b"ab");
        assert_eq!(
            file_slice
                .slice_from(1)
                .slice_to(2)
                .read_bytes()?
                .as_slice(),
            b"bc"
        );
        {
            let (left, right) = file_slice.clone().split(0);
            assert_eq!(left.read_bytes()?.as_slice(), b"");
            assert_eq!(right.read_bytes()?.as_slice(), b"abcdef");
        }
        {
            let (left, right) = file_slice.clone().split(2);
            assert_eq!(left.read_bytes()?.as_slice(), b"ab");
            assert_eq!(right.read_bytes()?.as_slice(), b"cdef");
        }
        {
            let (left, right) = file_slice.clone().split_from_end(0);
            assert_eq!(left.read_bytes()?.as_slice(), b"abcdef");
            assert_eq!(right.read_bytes()?.as_slice(), b"");
        }
        {
            let (left, right) = file_slice.split_from_end(2);
            assert_eq!(left.read_bytes()?.as_slice(), b"abcd");
            assert_eq!(right.read_bytes()?.as_slice(), b"ef");
        }
        Ok(())
    }

    #[test]
    fn test_file_slice_trait_slice_len() {
        let blop: &'static [u8] = b"abc";
        let owned_bytes: Box<dyn FileHandle> = Box::new(blop);
        assert_eq!(owned_bytes.len(), 3);
    }

    #[test]
    fn test_slice_simple_read() -> io::Result<()> {
        let slice = FileSlice::new(Arc::new(&b"abcdef"[..]));
        assert_eq!(slice.len(), 6);
        assert_eq!(slice.read_bytes()?.as_ref(), b"abcdef");
        assert_eq!(slice.slice(1..4).read_bytes()?.as_ref(), b"bcd");
        Ok(())
    }

    #[test]
    fn test_slice_read_slice() -> io::Result<()> {
        let slice_deref = FileSlice::new(Arc::new(&b"abcdef"[..]));
        assert_eq!(slice_deref.read_bytes_slice(1..4)?.as_ref(), b"bcd");
        Ok(())
    }

    #[test]
    #[should_panic(expected = "end of requested range exceeds the fileslice length (10 > 6)")]
    fn test_slice_read_slice_invalid_range_exceeds() {
        let slice_deref = FileSlice::new(Arc::new(&b"abcdef"[..]));
        assert_eq!(
            slice_deref.read_bytes_slice(0..10).unwrap().as_ref(),
            b"bcd"
        );
    }

    #[test]
    fn test_combine_range() {
        assert_eq!(combine_ranges(1..3, 0..1), 1..2);
        assert_eq!(combine_ranges(1..3, 1..), 2..3);
        assert_eq!(combine_ranges(1..4, ..2), 1..3);
        assert_eq!(combine_ranges(3..10, 2..5), 5..8);
        assert_eq!(combine_ranges(2..11, 5..=7), 7..10);
        assert_eq!(
            combine_ranges(2..11, (Bound::Excluded(5), Bound::Unbounded)),
            8..11
        );
    }

    #[test]
    #[should_panic]
    fn test_combine_range_panics() {
        let _ = combine_ranges(3..5, 1..4);
    }
}
