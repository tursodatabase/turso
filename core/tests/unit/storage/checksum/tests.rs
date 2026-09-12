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
fn test_verify_checksum_valid() {
    let ctx = ChecksumContext::new();
    let mut page = get_random_page();

    ctx.add_checksum_to_page(&mut page, 2).unwrap();

    let result = ctx.verify_checksum(&mut page, 2);
    assert!(result.is_ok());
}

#[test]
fn test_verify_checksum_mismatch() {
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
fn test_verify_checksum_corrupted_checksum() {
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
