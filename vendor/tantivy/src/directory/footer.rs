//! The footer is a small metadata structure that is appended at the end of every file.
//!
//! The footer is used to store a checksum of the file content.
//! The footer also stores the version of the index format.
//! This version is used to detect incompatibility between the index and the library version.

use std::io;
use std::io::Write;

use common::{BinarySerializable, CountingWriter, DeserializeFrom, FixedSize, HasLen};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::directory::error::Incompatibility;
use crate::directory::{AntiCallToken, FileSlice, TerminatingWrite};
use crate::{Version, INDEX_FORMAT_OLDEST_SUPPORTED_VERSION, INDEX_FORMAT_VERSION};

const FOOTER_MAX_LEN: u32 = 50_000;

/// The magic byte of the footer to identify corruption
/// or an old version of the footer.
const FOOTER_MAGIC_NUMBER: u32 = 1337;

type CrcHashU32 = u32;

/// A Footer is appended to every file
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Footer {
    /// The version of the index format
    pub version: Version,
    /// The crc32 hash of the body
    pub crc: CrcHashU32,
}

impl Footer {
    pub(crate) fn new(crc: CrcHashU32) -> Self {
        let version = crate::VERSION.clone();
        Footer { version, crc }
    }

    pub(crate) fn crc(&self) -> CrcHashU32 {
        self.crc
    }
    pub(crate) fn append_footer<W: io::Write>(&self, mut write: &mut W) -> io::Result<()> {
        let mut counting_write = CountingWriter::wrap(&mut write);
        counting_write.write_all(serde_json::to_string(&self)?.as_ref())?;
        let footer_payload_len = counting_write.written_bytes();
        BinarySerializable::serialize(&(footer_payload_len as u32), write)?;
        BinarySerializable::serialize(&FOOTER_MAGIC_NUMBER, write)?;
        Ok(())
    }

    /// Extracts the tantivy Footer from the file and returns the footer and the rest of the file
    pub fn extract_footer(file: FileSlice) -> io::Result<(Footer, FileSlice)> {
        if file.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "File corrupted. The file is smaller than 4 bytes (len={}).",
                    file.len()
                ),
            ));
        }

        let footer_metadata_len = <(u32, u32)>::SIZE_IN_BYTES;
        let (footer_len, footer_magic_byte): (u32, u32) = file
            .slice_from_end(footer_metadata_len)
            .read_bytes()?
            .as_ref()
            .deserialize()?;

        if footer_magic_byte != FOOTER_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Footer magic byte mismatch. File corrupted or index was created using old an \
                 tantivy version which is not supported anymore. Please use tantivy 0.15 or above \
                 to recreate the index.",
            ));
        }

        if footer_len > FOOTER_MAX_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Footer seems invalid as it suggests a footer len of {footer_len}. File is \
                     corrupted, or the index was created with a different & old version of \
                     tantivy."
                ),
            ));
        }
        let total_footer_size = footer_len as usize + footer_metadata_len;
        if file.len() < total_footer_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "File corrupted. The file is smaller than it's footer bytes \
                     (len={total_footer_size})."
                ),
            ));
        }

        let footer: Footer =
            serde_json::from_slice(&file.read_bytes_slice(
                file.len() - total_footer_size..file.len() - footer_metadata_len,
            )?)?;

        let body = file.slice_to(file.len() - total_footer_size);
        Ok((footer, body))
    }

    /// Confirms that the index will be read correctly by this version of tantivy
    /// Has to be called after `extract_footer` to make sure it's not accessing uninitialised memory
    pub fn is_compatible(&self) -> Result<(), Incompatibility> {
        const SUPPORTED_INDEX_FORMAT_VERSION_RANGE: std::ops::RangeInclusive<u32> =
            INDEX_FORMAT_OLDEST_SUPPORTED_VERSION..=INDEX_FORMAT_VERSION;

        let library_version = crate::version();
        if !SUPPORTED_INDEX_FORMAT_VERSION_RANGE.contains(&self.version.index_format_version) {
            return Err(Incompatibility::IndexMismatch {
                library_version: library_version.clone(),
                index_version: self.version.clone(),
            });
        }
        Ok(())
    }
}

pub(crate) struct FooterProxy<W: TerminatingWrite> {
    /// always Some except after terminate call
    hasher: Option<Hasher>,
    /// always Some except after terminate call
    writer: Option<W>,
}

impl<W: TerminatingWrite> FooterProxy<W> {
    pub fn new(writer: W) -> Self {
        FooterProxy {
            hasher: Some(Hasher::new()),
            writer: Some(writer),
        }
    }
}

impl<W: TerminatingWrite> Write for FooterProxy<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.writer.as_mut().unwrap().write(buf)?;
        self.hasher.as_mut().unwrap().update(&buf[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.as_mut().unwrap().flush()
    }
}

impl<W: TerminatingWrite> TerminatingWrite for FooterProxy<W> {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        let crc32 = self.hasher.take().unwrap().finalize();
        let footer = Footer::new(crc32);
        let mut writer = self.writer.take().unwrap();
        footer.append_footer(&mut writer)?;
        writer.terminate()
    }
}

#[cfg(test)]
mod tests {

    use std::io;
    use std::sync::Arc;

    use common::BinarySerializable;

    use crate::directory::footer::{Footer, FOOTER_MAGIC_NUMBER};
    use crate::directory::{FileSlice, OwnedBytes};

    #[test]
    fn test_deserialize_footer() {
        let mut buf: Vec<u8> = vec![];
        let footer = Footer::new(123);
        footer.append_footer(&mut buf).unwrap();
        let owned_bytes = OwnedBytes::new(buf);
        let fileslice = FileSlice::new(Arc::new(owned_bytes));
        let (footer_deser, _body) = Footer::extract_footer(fileslice).unwrap();
        assert_eq!(footer_deser.crc(), footer.crc());
    }
    #[test]
    fn test_deserialize_footer_missing_magic_byte() {
        let mut buf: Vec<u8> = vec![];
        BinarySerializable::serialize(&0_u32, &mut buf).unwrap();
        let wrong_magic_byte: u32 = 5555;
        BinarySerializable::serialize(&wrong_magic_byte, &mut buf).unwrap();

        let owned_bytes = OwnedBytes::new(buf);

        let fileslice = FileSlice::new(Arc::new(owned_bytes));
        let err = Footer::extract_footer(fileslice).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Footer magic byte mismatch. File corrupted or index was created using old an tantivy \
             version which is not supported anymore. Please use tantivy 0.15 or above to recreate \
             the index."
        );
    }
    #[test]
    fn test_deserialize_footer_wrong_filesize() {
        let mut buf: Vec<u8> = vec![];
        BinarySerializable::serialize(&100_u32, &mut buf).unwrap();
        BinarySerializable::serialize(&FOOTER_MAGIC_NUMBER, &mut buf).unwrap();

        let owned_bytes = OwnedBytes::new(buf);

        let fileslice = FileSlice::new(Arc::new(owned_bytes));
        let err = Footer::extract_footer(fileslice).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(
            err.to_string(),
            "File corrupted. The file is smaller than it\'s footer bytes (len=108)."
        );
    }

    #[test]
    fn test_deserialize_too_large_footer() {
        let mut buf: Vec<u8> = vec![];

        let footer_length = super::FOOTER_MAX_LEN + 1;
        BinarySerializable::serialize(&footer_length, &mut buf).unwrap();
        BinarySerializable::serialize(&FOOTER_MAGIC_NUMBER, &mut buf).unwrap();

        let owned_bytes = OwnedBytes::new(buf);

        let fileslice = FileSlice::new(Arc::new(owned_bytes));
        let err = Footer::extract_footer(fileslice).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            err.to_string(),
            "Footer seems invalid as it suggests a footer len of 50001. File is corrupted, or the \
             index was created with a different & old version of tantivy."
        );
    }
}
