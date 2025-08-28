use crate::{CompletionError, Result};

const CHECKSUM_PAGE_SIZE: usize = 4096;
const CHECKSUM_SIZE: usize = 8;

#[derive(Clone)]
pub struct ChecksumContext {}

impl ChecksumContext {
    pub fn new() -> Self {
        ChecksumContext {}
    }

    pub fn add_checksum_to_page(&self, page: &mut [u8], page_id: usize) -> Result<()> {
        assert_eq!(
            page.len(),
            CHECKSUM_PAGE_SIZE,
            "page size must be 4096 bytes"
        );

        if page_id == 1 {
            // lets skip checksum verification for the first page (header page)
            let reserved_bytes = &page[CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE..];
            let reserved_bytes_zeroed = reserved_bytes.iter().all(|&b| b == 0);
            assert!(
                reserved_bytes_zeroed,
                "last reserved bytes must be empty/zero, but found non-zero bytes on page {page_id}"
            );
            return Ok(());
        }

        // compute checksum on the actual page data (excluding the reserved checksum area)
        let actual_page = &page[..CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE];
        let checksum = self.compute_checksum(actual_page);

        tracing::trace!("computed checksum {checksum} for page_id={page_id}");

        // write checksum directly to the reserved area at the end of the page
        let checksum_bytes = checksum.to_le_bytes();
        assert_eq!(checksum_bytes.len(), CHECKSUM_SIZE);
        page[CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE..].copy_from_slice(&checksum_bytes);
        Ok(())
    }

    pub fn verify_and_strip_checksum(
        &self,
        page: &mut [u8],
        page_id: usize,
    ) -> std::result::Result<(), CompletionError> {
        assert_eq!(
            page.len(),
            CHECKSUM_PAGE_SIZE,
            "page size must be 4096 bytes"
        );

        if page_id == 1 {
            // lets skip checksum verification for the first page (header page)
            return Ok(());
        }

        // extract data and checksum portions
        let actual_page = &page[..CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE];
        let stored_checksum_bytes = &page[CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE..];
        let stored_checksum = u64::from_le_bytes(stored_checksum_bytes.try_into().unwrap());

        // verify checksum
        let computed_checksum = self.compute_checksum(actual_page);
        if stored_checksum != computed_checksum {
            return Err(CompletionError::ChecksumMismatch {
                page_id,
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }
        tracing::trace!("checksum verified (page_id={page_id})");
        // zero out the checksum area in-place
        // page[CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE..].fill(0);
        Ok(())
    }

    fn compute_checksum(&self, data: &[u8]) -> u64 {
        twox_hash::XxHash3_64::oneshot(data)
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
    use crate::CompletionError;

    #[test]
    fn test_add_checksum_to_page_normal_page() {
        let ctx = ChecksumContext::new();
        let mut page = [0u8; CHECKSUM_PAGE_SIZE];

        // Fill page with some test data (excluding checksum area)
        for (i, byte) in page
            .iter_mut()
            .enumerate()
            .take(CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE)
        {
            *byte = (i % 256) as u8;
        }

        let result = ctx.add_checksum_to_page(&mut page, 2);
        assert!(result.is_ok());

        // Verify checksum was written to the last 8 bytes
        let checksum_bytes = &page[CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE..];
        let stored_checksum = u64::from_le_bytes(checksum_bytes.try_into().unwrap());

        // Compute expected checksum
        let data_portion = &page[..CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE];
        let expected_checksum = ctx.compute_checksum(data_portion);

        assert_eq!(stored_checksum, expected_checksum);
    }

    #[test]
    fn test_verify_and_strip_checksum_valid() {
        let ctx = ChecksumContext::new();
        let mut page = [0u8; CHECKSUM_PAGE_SIZE];

        // Fill with test data
        for (i, byte) in page
            .iter_mut()
            .enumerate()
            .take(CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE)
        {
            *byte = (i % 256) as u8;
        }

        // Add checksum first
        ctx.add_checksum_to_page(&mut page, 2).unwrap();

        // Now verify it
        let result = ctx.verify_and_strip_checksum(&mut page, 2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_and_strip_checksum_mismatch() {
        let ctx = ChecksumContext::new();
        let mut page = [0u8; CHECKSUM_PAGE_SIZE];

        // Fill with test data
        for (i, byte) in page
            .iter_mut()
            .enumerate()
            .take(CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE)
        {
            *byte = (i % 256) as u8;
        }

        // Add valid checksum
        ctx.add_checksum_to_page(&mut page, 2).unwrap();

        // Corrupt the data to cause checksum mismatch
        page[0] = 255;

        let result = ctx.verify_and_strip_checksum(&mut page, 2);
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
    fn test_verify_and_strip_checksum_corrupted_checksum() {
        let ctx = ChecksumContext::new();
        let mut page = [0u8; CHECKSUM_PAGE_SIZE];

        // Fill with test data
        for (i, byte) in page
            .iter_mut()
            .enumerate()
            .take(CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE)
        {
            *byte = (i % 256) as u8;
        }

        // Add valid checksum
        ctx.add_checksum_to_page(&mut page, 2).unwrap();

        // Corrupt the checksum itself
        page[CHECKSUM_PAGE_SIZE - 1] = 255;

        let result = ctx.verify_and_strip_checksum(&mut page, 2);
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
