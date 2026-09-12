use super::*;
use crate::File;
use crate::{io::IO, MemoryIO};

struct MockFile {
    read_result: std::result::Result<i32, CompletionError>,
}

impl File for MockFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, _pos: u64, c: Completion) -> Result<Completion> {
        match self.read_result {
            Ok(bytes_read) => c.complete(bytes_read),
            Err(err) => c.error(err),
        }
        Ok(c)
    }

    fn pwrite(&self, _pos: u64, _buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        c.complete(0);
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        Ok(0)
    }

    fn truncate(&self, _len: u64, c: Completion) -> Result<Completion> {
        c.complete(0);
        Ok(c)
    }
}

#[test]
fn checksum_read_wrapper_propagates_callback_errors() {
    let db_file = DatabaseFile {
        file: Arc::new(MockFile { read_result: Ok(0) }),
    };
    let io_ctx = IOContext::default();
    let page_idx = 1usize;
    let expected = 4096usize;
    let buf = Arc::new(Buffer::new_temporary(expected));
    let original = Completion::new_read(buf, move |res| {
        let (_, bytes_read) = res.expect("mock read should complete");
        if bytes_read == 0 {
            Some(CompletionError::ShortRead {
                page_idx,
                expected,
                actual: 0,
            })
        } else {
            None
        }
    });

    let wrapped = db_file
        .read_page(page_idx, &io_ctx, original.clone())
        .unwrap();
    let io = MemoryIO::new();
    let err = io
        .wait_for_completion(wrapped)
        .expect_err("wrapped completion must fail");
    assert!(matches!(
        err,
        LimboError::CompletionError(CompletionError::ShortRead { .. })
    ));
    assert!(matches!(
        original.get_error(),
        Some(CompletionError::ShortRead { .. })
    ));
}

#[test]
fn checksum_read_wrapper_propagates_transport_errors_to_original_completion() {
    let db_file = DatabaseFile {
        file: Arc::new(MockFile {
            read_result: Err(CompletionError::Aborted),
        }),
    };
    let io_ctx = IOContext::default();
    let page_idx = 1usize;
    let buf = Arc::new(Buffer::new_temporary(4096));
    let original = Completion::new_read(buf, |_res| None);

    let wrapped = db_file
        .read_page(page_idx, &io_ctx, original.clone())
        .unwrap();
    let io = MemoryIO::new();
    let err = io
        .wait_for_completion(wrapped)
        .expect_err("wrapped completion must fail");
    assert!(matches!(
        err,
        LimboError::CompletionError(CompletionError::Aborted)
    ));
    assert_eq!(original.get_error(), Some(CompletionError::Aborted));
}
