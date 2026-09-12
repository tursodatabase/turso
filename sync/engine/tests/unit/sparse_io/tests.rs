use std::sync::Arc;

use turso_core::{Buffer, Completion, OpenFlags, IO};

use crate::sparse_io::SparseLinuxIo;

#[test]
pub fn sparse_io_test() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let tmp_path = tmp.into_temp_path();
    let tmp_path = tmp_path.as_os_str().to_str().unwrap();
    let io = SparseLinuxIo::new().unwrap();
    let file = io.open_file(tmp_path, OpenFlags::default(), false).unwrap();
    #[expect(clippy::let_underscore_future)]
    let _ = file
        .truncate(1024 * 1024, Completion::new_trunc(|_| {}))
        .unwrap();
    assert!(file.has_hole(0, 4096).unwrap());

    let buffer = Arc::new(Buffer::new_temporary(4096));
    buffer.as_mut_slice().fill(1);
    #[expect(clippy::let_underscore_future)]
    let _ = file
        .pwrite(0, buffer.clone(), Completion::new_write(|_| {}))
        .unwrap();
    assert!(!file.has_hole(0, 4096).unwrap());

    assert!(file.has_hole(4096, 4096).unwrap());
    assert!(file.has_hole(4096 * 2, 4096).unwrap());

    #[expect(clippy::let_underscore_future)]
    let _ = file
        .pwrite(4096 * 2, buffer.clone(), Completion::new_write(|_| {}))
        .unwrap();
    assert!(file.has_hole(4096, 4096).unwrap());
    assert!(!file.has_hole(4096 * 2, 4096).unwrap());

    assert!(!file.has_hole(4096, 4097).unwrap());

    file.punch_hole(2 * 4096, 4096).unwrap();
    assert!(file.has_hole(4096 * 2, 4096).unwrap());
    assert!(file.has_hole(4096, 4097).unwrap());
}
#[test]
pub fn pread_reports_short_read_at_end_of_file() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let tmp_path = tmp.into_temp_path();
    let tmp_path = tmp_path.as_os_str().to_str().unwrap();
    let io = SparseLinuxIo::new().unwrap();
    let file = io.open_file(tmp_path, OpenFlags::default(), false).unwrap();

    for (file_len, expected_read) in [(0, 0), (10, 10), (32, 32)] {
        #[expect(clippy::let_underscore_future)]
        let _ = file
            .truncate(file_len, Completion::new_trunc(|_| {}))
            .unwrap();

        let buffer = Arc::new(Buffer::new_temporary(32));
        let read = Arc::new(std::sync::atomic::AtomicI32::new(-1));
        let c = file
            .pread(
                0,
                Completion::new_read(buffer.clone(), {
                    let read = read.clone();
                    move |result| {
                        read.store(
                            result.expect("read past end of file must not fail").1,
                            std::sync::atomic::Ordering::SeqCst,
                        );
                        None
                    }
                }),
            )
            .unwrap();
        assert!(c.succeeded());
        assert_eq!(
            read.load(std::sync::atomic::Ordering::SeqCst),
            expected_read,
            "32 byte read of a {file_len} byte file"
        );
    }
}
