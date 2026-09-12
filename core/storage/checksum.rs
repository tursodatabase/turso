#![allow(unused_variables, dead_code)]
use crate::{CompletionError, Result};

const CHECKSUM_PAGE_SIZE: usize = 4096;
const CHECKSUM_SIZE: usize = 8;
pub(crate) const CHECKSUM_REQUIRED_RESERVED_BYTES: u8 = CHECKSUM_SIZE as u8;

#[derive(Debug, Clone)]
pub struct ChecksumContext {}

impl ChecksumContext {
    pub fn new() -> Self {
        ChecksumContext {}
    }

    #[cfg(not(feature = "checksum"))]
    pub fn add_checksum_to_page(&self, _page: &mut [u8], _page_id: usize) -> Result<()> {
        use crate::LimboError;
        Err(LimboError::InternalError(
            "tursodb must be recompiled with checksum feature in order to use checksums"
                .to_string(),
        ))
    }

    #[cfg(not(feature = "checksum"))]
    pub fn verify_checksum(
        &self,
        _page: &mut [u8],
        _page_id: usize,
    ) -> std::result::Result<(), CompletionError> {
        Err(CompletionError::ChecksumNotEnabled)
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
#[cfg(feature = "checksum")]
#[path = "../tests/unit/storage/checksum/tests.rs"]
mod tests;
