use std::ops::{Deref, Range};
use std::sync::Arc;
use std::{fmt, io};

pub use stable_deref_trait::StableDeref;

/// An OwnedBytes simply wraps an object that owns a slice of data and exposes
/// this data as a slice.
///
/// The backing object is required to be `StableDeref`.
#[derive(Clone)]
pub struct OwnedBytes {
    data: &'static [u8],
    box_stable_deref: Arc<dyn Deref<Target = [u8]> + Sync + Send>,
}

impl OwnedBytes {
    /// Creates an empty `OwnedBytes`.
    pub fn empty() -> OwnedBytes {
        OwnedBytes::new(&[][..])
    }

    /// Creates an `OwnedBytes` instance given a `StableDeref` object.
    pub fn new<T: StableDeref + Deref<Target = [u8]> + 'static + Send + Sync>(
        data_holder: T,
    ) -> OwnedBytes {
        let box_stable_deref = Arc::new(data_holder);
        let bytes: &[u8] = box_stable_deref.deref();
        let data = unsafe { &*(bytes as *const [u8]) };
        OwnedBytes {
            data,
            box_stable_deref,
        }
    }

    /// creates a fileslice that is just a view over a slice of the data.
    #[must_use]
    #[inline]
    pub fn slice(&self, range: Range<usize>) -> Self {
        OwnedBytes {
            data: &self.data[range],
            box_stable_deref: self.box_stable_deref.clone(),
        }
    }

    /// Returns the underlying slice of data.
    /// `Deref` and `AsRef` are also available.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.data
    }

    /// Returns the len of the slice.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true iff this `OwnedBytes` is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Splits the OwnedBytes into two OwnedBytes `(left, right)`.
    ///
    /// Left will hold `split_len` bytes.
    ///
    /// This operation is cheap and does not require to copy any memory.
    /// On the other hand, both `left` and `right` retain a handle over
    /// the entire slice of memory. In other words, the memory will only
    /// be released when both left and right are dropped.
    #[inline]
    #[must_use]
    pub fn split(self, split_len: usize) -> (OwnedBytes, OwnedBytes) {
        let (left_data, right_data) = self.data.split_at(split_len);
        let right_box_stable_deref = self.box_stable_deref.clone();
        let left = OwnedBytes {
            data: left_data,
            box_stable_deref: self.box_stable_deref,
        };
        let right = OwnedBytes {
            data: right_data,
            box_stable_deref: right_box_stable_deref,
        };
        (left, right)
    }

    /// Splits the OwnedBytes into two OwnedBytes `(left, right)`.
    ///
    /// Right will hold `split_len` bytes.
    ///
    /// This operation is cheap and does not require to copy any memory.
    /// On the other hand, both `left` and `right` retain a handle over
    /// the entire slice of memory. In other words, the memory will only
    /// be released when both left and right are dropped.
    #[inline]
    #[must_use]
    pub fn rsplit(self, split_len: usize) -> (OwnedBytes, OwnedBytes) {
        let data_len = self.data.len();
        self.split(data_len - split_len)
    }

    /// Splits the right part of the `OwnedBytes` at the given offset.
    ///
    /// `self` is truncated to `split_len`, left with the remaining bytes.
    pub fn split_off(&mut self, split_len: usize) -> OwnedBytes {
        let (left, right) = self.data.split_at(split_len);
        let right_box_stable_deref = self.box_stable_deref.clone();
        let right_piece = OwnedBytes {
            data: right,
            box_stable_deref: right_box_stable_deref,
        };
        self.data = left;
        right_piece
    }

    /// Drops the left most `advance_len` bytes.
    #[inline]
    pub fn advance(&mut self, advance_len: usize) -> &[u8] {
        let (data, rest) = self.data.split_at(advance_len);
        self.data = rest;
        data
    }

    /// Reads an `u8` from the `OwnedBytes` and advance by one byte.
    #[inline]
    pub fn read_u8(&mut self) -> u8 {
        self.advance(1)[0]
    }

    #[inline]
    fn read_n<const N: usize>(&mut self) -> [u8; N] {
        self.advance(N).try_into().unwrap()
    }

    /// Reads an `u32` encoded as little-endian from the `OwnedBytes` and advance by 4 bytes.
    #[inline]
    pub fn read_u32(&mut self) -> u32 {
        u32::from_le_bytes(self.read_n())
    }

    /// Reads an `u64` encoded as little-endian from the `OwnedBytes` and advance by 8 bytes.
    #[inline]
    pub fn read_u64(&mut self) -> u64 {
        u64::from_le_bytes(self.read_n())
    }
}

impl fmt::Debug for OwnedBytes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // We truncate the bytes in order to make sure the debug string
        // is not too long.
        let bytes_truncated: &[u8] = if self.len() > 10 {
            &self.as_slice()[..10]
        } else {
            self.as_slice()
        };
        write!(f, "OwnedBytes({bytes_truncated:?}, len={})", self.len())
    }
}

impl PartialEq for OwnedBytes {
    fn eq(&self, other: &OwnedBytes) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for OwnedBytes {}

impl PartialEq<[u8]> for OwnedBytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<str> for OwnedBytes {
    fn eq(&self, other: &str) -> bool {
        self.as_slice() == other.as_bytes()
    }
}

impl<'a, T: ?Sized> PartialEq<&'a T> for OwnedBytes
where OwnedBytes: PartialEq<T>
{
    fn eq(&self, other: &&'a T) -> bool {
        *self == **other
    }
}

impl Deref for OwnedBytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u8]> for OwnedBytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl io::Read for OwnedBytes {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let data_len = self.data.len();
        let buf_len = buf.len();
        if data_len >= buf_len {
            let data = self.advance(buf_len);
            buf.copy_from_slice(data);
            Ok(buf_len)
        } else {
            buf[..data_len].copy_from_slice(self.data);
            self.data = &[];
            Ok(data_len)
        }
    }
    #[inline]
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        buf.extend(self.data);
        let read_len = self.data.len();
        self.data = &[];
        Ok(read_len)
    }
    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let read_len = self.read(buf)?;
        if read_len != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};

    use super::OwnedBytes;

    #[test]
    fn test_owned_bytes_debug() {
        let short_bytes = OwnedBytes::new(b"abcd".as_ref());
        assert_eq!(
            format!("{short_bytes:?}"),
            "OwnedBytes([97, 98, 99, 100], len=4)"
        );
        let medium_bytes = OwnedBytes::new(b"abcdefghi".as_ref());
        assert_eq!(
            format!("{medium_bytes:?}"),
            "OwnedBytes([97, 98, 99, 100, 101, 102, 103, 104, 105], len=9)"
        );
        let long_bytes = OwnedBytes::new(b"abcdefghijklmnopq".as_ref());
        assert_eq!(
            format!("{long_bytes:?}"),
            "OwnedBytes([97, 98, 99, 100, 101, 102, 103, 104, 105, 106], len=17)"
        );
    }

    #[test]
    fn test_owned_bytes_read() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"abcdefghiklmnopqrstuvwxyz".as_ref());
        {
            let mut buf = [0u8; 5];
            bytes.read_exact(&mut buf[..]).unwrap();
            assert_eq!(&buf, b"abcde");
            assert_eq!(bytes.as_slice(), b"fghiklmnopqrstuvwxyz")
        }
        {
            let mut buf = [0u8; 2];
            bytes.read_exact(&mut buf[..]).unwrap();
            assert_eq!(&buf, b"fg");
            assert_eq!(bytes.as_slice(), b"hiklmnopqrstuvwxyz")
        }
        Ok(())
    }

    #[test]
    fn test_owned_bytes_read_right_at_the_end() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"abcde".as_ref());
        let mut buf = [0u8; 5];
        assert_eq!(bytes.read(&mut buf[..]).unwrap(), 5);
        assert_eq!(&buf, b"abcde");
        assert_eq!(bytes.as_slice(), b"");
        assert_eq!(bytes.read(&mut buf[..]).unwrap(), 0);
        assert_eq!(&buf, b"abcde");
        Ok(())
    }
    #[test]
    fn test_owned_bytes_read_incomplete() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"abcde".as_ref());
        let mut buf = [0u8; 7];
        assert_eq!(bytes.read(&mut buf[..]).unwrap(), 5);
        assert_eq!(&buf[..5], b"abcde");
        assert_eq!(bytes.read(&mut buf[..]).unwrap(), 0);
        Ok(())
    }

    #[test]
    fn test_owned_bytes_read_to_end() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"abcde".as_ref());
        let mut buf = Vec::new();
        bytes.read_to_end(&mut buf)?;
        assert_eq!(buf.as_slice(), b"abcde".as_ref());
        Ok(())
    }

    #[test]
    fn test_owned_bytes_read_u8() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"\xFF".as_ref());
        assert_eq!(bytes.read_u8(), 255);
        assert_eq!(bytes.len(), 0);
        Ok(())
    }

    #[test]
    fn test_owned_bytes_read_u64() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"\0\xFF\xFF\xFF\xFF\xFF\xFF\xFF".as_ref());
        assert_eq!(bytes.read_u64(), u64::MAX - 255);
        assert_eq!(bytes.len(), 0);
        Ok(())
    }

    #[test]
    fn test_owned_bytes_split() {
        let bytes = OwnedBytes::new(b"abcdefghi".as_ref());
        let (left, right) = bytes.split(3);
        assert_eq!(left.as_slice(), b"abc");
        assert_eq!(right.as_slice(), b"defghi");
    }

    #[test]
    fn test_owned_bytes_split_boundary() {
        let bytes = OwnedBytes::new(b"abcdefghi".as_ref());
        {
            let (left, right) = bytes.clone().split(0);
            assert_eq!(left.as_slice(), b"");
            assert_eq!(right.as_slice(), b"abcdefghi");
        }
        {
            let (left, right) = bytes.split(9);
            assert_eq!(left.as_slice(), b"abcdefghi");
            assert_eq!(right.as_slice(), b"");
        }
    }

    #[test]
    fn test_split_off() {
        let mut data = OwnedBytes::new(b"abcdef".as_ref());
        assert_eq!(data, "abcdef");
        assert_eq!(data.split_off(2), "cdef");
        assert_eq!(data, "ab");
        assert_eq!(data.split_off(1), "b");
        assert_eq!(data, "a");
    }
}
