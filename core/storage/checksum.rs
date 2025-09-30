#![allow(unused_variables, dead_code)]
use crate::{CompletionError, Result};

const CHECKSUM_PAGE_SIZE: usize = 4096;
const CHECKSUM_SIZE: usize = 8;
pub(crate) const CHECKSUM_REQUIRED_RESERVED_BYTES: u8 = CHECKSUM_SIZE as u8;

#[derive(Clone)]
pub struct ChecksumContext {}

impl ChecksumContext {
    pub fn new() -> Self {
        ChecksumContext {}
    }

    #[cfg(not(feature = "checksum"))]
    pub fn add_checksum_to_page(&self, _page: &mut [u8], _page_id: usize) -> Result<()> {
        Ok(())
    }

    #[cfg(not(feature = "checksum"))]
    pub fn verify_checksum(
        &self,
        _page: &mut [u8],
        _page_id: usize,
    ) -> std::result::Result<(), CompletionError> {
        Ok(())
    }

    #[cfg(feature = "checksum")]
    pub fn add_checksum_to_page(&self, page: &mut [u8], _page_id: usize) -> Result<()> {
        if page.len() != CHECKSUM_PAGE_SIZE {
            return Ok(());
        }

        // compute checksum on the actual page data (excluding the reserved checksum area)
        let actual_page = &page[..CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE];
        let checksum = self.compute_checksum(actual_page);

        let checksum_bytes = checksum.to_le_bytes();
        assert_eq!(checksum_bytes.len(), CHECKSUM_SIZE);
        page[CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE..].copy_from_slice(&checksum_bytes);
        Ok(())
    }

    #[cfg(feature = "checksum")]
    pub fn verify_checksum(
        &self,
        page: &mut [u8],
        page_id: usize,
    ) -> std::result::Result<(), CompletionError> {
        if page.len() != CHECKSUM_PAGE_SIZE {
            return Ok(());
        }

        let actual_page = &page[..CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE];
        let stored_checksum_bytes = &page[CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE..];
        let stored_checksum = u64::from_le_bytes(stored_checksum_bytes.try_into().unwrap());

        let computed_checksum = self.compute_checksum(actual_page);
        if stored_checksum != computed_checksum {
            tracing::error!(
                "Checksum mismatch on page {}: expected {:x}, got {:x}",
                page_id,
                stored_checksum,
                computed_checksum
            );
            return Err(CompletionError::ChecksumMismatch {
                page_id,
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }
        Ok(())
    }

    fn compute_checksum(&self, data: &[u8]) -> u64 {
        twox_hash::XxHash3_64::oneshot(data)
    }

    pub fn required_reserved_bytes(&self) -> u8 {
        CHECKSUM_REQUIRED_RESERVED_BYTES
    }
}

impl Default for ChecksumContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_random_page() -> [u8; CHECKSUM_PAGE_SIZE] {
        let mut page = [0u8; CHECKSUM_PAGE_SIZE];
        for (i, byte) in page
            .iter_mut()
            .enumerate()
            .take(CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE)
        {
            *byte = (i % 256) as u8;
        }
        page
    }

    #[test]
    #[cfg(feature = "checksum")]
    fn test_add_checksum_to_page() {
        let ctx = ChecksumContext::new();
        let mut page = get_random_page();

        let result = ctx.add_checksum_to_page(&mut page, 2);
        assert!(result.is_ok());

        let checksum_bytes = &page[CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE..];
        let stored_checksum = u64::from_le_bytes(checksum_bytes.try_into().unwrap());

        let actual_page = &page[..CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE];
        let expected_checksum = ctx.compute_checksum(actual_page);

        assert_eq!(stored_checksum, expected_checksum);
    }

    #[test]
    fn test_verify_and_strip_checksum_valid() {
        let ctx = ChecksumContext::new();
        let mut page = get_random_page();

        ctx.add_checksum_to_page(&mut page, 2).unwrap();

        let result = ctx.verify_checksum(&mut page, 2);
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "checksum")]
    fn test_verify_and_strip_checksum_mismatch() {
        let ctx = ChecksumContext::new();
        let mut page = get_random_page();

        ctx.add_checksum_to_page(&mut page, 2).unwrap();

        // corrupt the data to cause checksum mismatch
        page[0] = 255;

        let result = ctx.verify_checksum(&mut page, 2);
        assert!(result.is_err());
        match result.unwrap_err() {
            CompletionError::ChecksumMismatch {
                page_id,
                expected,
                actual,
            } => {
                assert_eq!(page_id, 2);
                assert_ne!(expected, actual);
            }
            _ => panic!("Expected ChecksumMismatch error"),
        }
    }

    #[test]
    #[cfg(feature = "checksum")]
    fn test_verify_and_strip_checksum_corrupted_checksum() {
        let ctx = ChecksumContext::new();
        let mut page = get_random_page();

        ctx.add_checksum_to_page(&mut page, 2).unwrap();

        // corrupt the checksum itself
        page[CHECKSUM_PAGE_SIZE - 1] = 255;

        let result = ctx.verify_checksum(&mut page, 2);
        assert!(result.is_err());

        match result.unwrap_err() {
            CompletionError::ChecksumMismatch {
                page_id,
                expected,
                actual,
            } => {
                assert_eq!(page_id, 2);
                assert_ne!(expected, actual);
            }
            _ => panic!("Expected ChecksumMismatch error"),
        }
    }
}
