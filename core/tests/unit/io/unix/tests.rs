use super::*;
use std::io::Write;

#[test]
fn test_multiple_processes_cannot_open_file() {
    common::tests::test_multiple_processes_cannot_open_file(UnixIO::new);
}

#[test]
fn test_shared_wal_map_supports_unaligned_logical_offset() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let backing_len = 128 * 1024;
    let bytes: Vec<u8> = (0..backing_len).map(|i| (i % 251) as u8).collect();
    file.as_file().write_all(&bytes).unwrap();
    file.as_file().sync_all().unwrap();

    let mapped = unix_shared_wal_map(4096, 81920, file.as_file().as_raw_fd()).unwrap();
    assert_eq!(mapped.len(), 81920);
    let slice = unsafe { std::slice::from_raw_parts(mapped.ptr().as_ptr(), mapped.len()) };
    assert_eq!(&slice[..128], &bytes[4096..4096 + 128]);
    assert_eq!(&slice[mapped.len() - 128..], &bytes[4096 + 81920 - 128..4096 + 81920]);
}
