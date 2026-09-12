use super::*;
use crate::storage::page_transform::PageCodecId;
use crate::File;
use crate::{io::IO, MemoryIO};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Debug)]
struct XorPageCodec(u8);

impl PageCodec for XorPageCodec {
    fn codec_id(&self) -> PageCodecId {
        let mut id = *b"xor-page-codec--";
        id[15] = self.0;
        PageCodecId::new(id)
    }

    fn required_reserved_bytes(&self) -> u8 {
        0
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        let _ = context;
        for (input, output) in input.iter().zip(output) {
            *output = input ^ self.0;
        }
        Ok(())
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        self.encode_page(context, input, output)
    }
}

#[derive(Debug)]
struct FailOnEncodePageCodec {
    fail_page_no: u32,
    contexts: Arc<Mutex<Vec<PageCodecContext>>>,
}

impl PageCodec for FailOnEncodePageCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(*b"fail-on-page----")
    }

    fn required_reserved_bytes(&self) -> u8 {
        0
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        self.contexts.lock().unwrap().push(context);
        if context.page_no == self.fail_page_no {
            return Err(LimboError::InternalError("codec encode failed".into()));
        }
        output.copy_from_slice(input);
        Ok(())
    }

    fn decode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        output.copy_from_slice(input);
        Ok(())
    }
}

#[derive(Debug)]
enum FailingPageCodec {
    Encode,
    Decode,
}

impl PageCodec for FailingPageCodec {
    fn codec_id(&self) -> PageCodecId {
        let mut id = *b"failing-page-cod";
        id[15] = match self {
            Self::Encode => 1,
            Self::Decode => 2,
        };
        PageCodecId::new(id)
    }

    fn required_reserved_bytes(&self) -> u8 {
        0
    }

    fn encode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        match self {
            Self::Encode => Err(LimboError::InternalError("codec encode failed".into())),
            Self::Decode => {
                output.copy_from_slice(input);
                Ok(())
            }
        }
    }

    fn decode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        match self {
            Self::Encode => {
                output.copy_from_slice(input);
                Ok(())
            }
            Self::Decode => Err(LimboError::InternalError("codec decode failed".into())),
        }
    }
}

struct MockFile {
    read_result: std::result::Result<i32, CompletionError>,
    writes_submitted: Arc<AtomicUsize>,
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
        self.writes_submitted.fetch_add(1, Ordering::Relaxed);
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
fn page_codec_encodes_into_fixed_size_database_buffer() {
    let buffer = Arc::new(Buffer::new(vec![1, 2, 3, 4]));
    let encoded = encode_buffer(7, buffer, &XorPageCodec(0xa5), PageLocation::Database).unwrap();
    assert_eq!(encoded.as_slice(), &[0xa4, 0xa7, 0xa6, 0xa1]);
}

#[test]
fn page_codec_read_decodes_into_original_database_buffer() {
    let db_file = DatabaseFile {
        file: Arc::new(MockFile {
            read_result: Ok(4096),
            writes_submitted: Arc::new(AtomicUsize::new(0)),
        }),
    };
    let mut io_ctx = IOContext::default();
    io_ctx.set_page_codec(Arc::new(XorPageCodec(0xa5)));
    let page_idx = 9usize;
    let original = Completion::new_read(Arc::new(Buffer::new_temporary(4096)), |_res| None);

    let wrapped = db_file
        .read_page(page_idx, &io_ctx, original.clone())
        .unwrap();
    MemoryIO::new().wait_for_completion(wrapped).unwrap();
    assert!(original.succeeded());
    assert!(original
        .as_read()
        .buf()
        .as_slice()
        .iter()
        .all(|byte| *byte == 0xa5));
}

#[test]
fn page_codec_zero_byte_read_reaches_original_completion() {
    let db_file = DatabaseFile {
        file: Arc::new(MockFile {
            read_result: Ok(0),
            writes_submitted: Arc::new(AtomicUsize::new(0)),
        }),
    };
    let mut io_ctx = IOContext::default();
    io_ctx.set_page_codec(Arc::new(XorPageCodec(0xa5)));
    let page_idx = 9usize;
    let bytes_seen = Arc::new(AtomicUsize::new(usize::MAX));
    let bytes_seen_callback = bytes_seen.clone();
    let original = Completion::new_read(Arc::new(Buffer::new_temporary(4096)), move |result| {
        let (_, bytes_read) = result.expect("zero-byte read should reach the callback");
        bytes_seen_callback.store(bytes_read as usize, Ordering::Relaxed);
        None
    });

    let wrapped = db_file
        .read_page(page_idx, &io_ctx, original.clone())
        .unwrap();
    MemoryIO::new().wait_for_completion(wrapped).unwrap();

    assert!(original.succeeded());
    assert_eq!(bytes_seen.load(Ordering::Relaxed), 0);
    assert!(
        original
            .as_read()
            .buf()
            .as_slice()
            .iter()
            .all(|byte| *byte == 0),
        "an absent page must not be decoded"
    );
}

#[test]
fn page_codec_database_read_forwards_transport_error() {
    let db_file = DatabaseFile {
        file: Arc::new(MockFile {
            read_result: Err(CompletionError::Aborted),
            writes_submitted: Arc::new(AtomicUsize::new(0)),
        }),
    };
    let mut io_ctx = IOContext::default();
    io_ctx.set_page_codec(Arc::new(XorPageCodec(0xa5)));
    let original = Completion::new_read(Arc::new(Buffer::new_temporary(512)), |_| None);

    let wrapped = db_file.read_page(2, &io_ctx, original.clone()).unwrap();
    let err = MemoryIO::new().wait_for_completion(wrapped).unwrap_err();

    assert!(matches!(
        err,
        LimboError::CompletionError(CompletionError::Aborted)
    ));
    assert!(matches!(
        original.get_error(),
        Some(CompletionError::Aborted)
    ));
    assert!(original
        .as_read()
        .buf()
        .as_slice()
        .iter()
        .all(|byte| *byte == 0));
}

#[test]
fn page_codec_partial_database_read_fails_before_decode() {
    let db_file = DatabaseFile {
        file: Arc::new(MockFile {
            read_result: Ok(128),
            writes_submitted: Arc::new(AtomicUsize::new(0)),
        }),
    };
    let mut io_ctx = IOContext::default();
    io_ctx.set_page_codec(Arc::new(XorPageCodec(0xa5)));
    let original = Completion::new_read(Arc::new(Buffer::new_temporary(512)), |_| None);

    let wrapped = db_file.read_page(1, &io_ctx, original.clone()).unwrap();
    let err = MemoryIO::new().wait_for_completion(wrapped).unwrap_err();

    assert!(matches!(
        err,
        LimboError::CompletionError(CompletionError::ShortRead {
            page_idx: 1,
            expected: 512,
            actual: 128,
        })
    ));
    assert!(matches!(
        original.get_error(),
        Some(CompletionError::ShortRead {
            page_idx: 1,
            expected: 512,
            actual: 128,
        })
    ));
}

#[test]
fn page_codec_database_write_error_does_not_submit_io() {
    let writes_submitted = Arc::new(AtomicUsize::new(0));
    let db_file = DatabaseFile {
        file: Arc::new(MockFile {
            read_result: Ok(0),
            writes_submitted: writes_submitted.clone(),
        }),
    };
    let mut io_ctx = IOContext::default();
    io_ctx.set_page_codec(Arc::new(FailingPageCodec::Encode));

    let err = db_file
        .write_page(
            1,
            Arc::new(Buffer::new_temporary(512)),
            &io_ctx,
            Completion::new_write(|_| {}),
        )
        .unwrap_err();

    assert!(err.to_string().contains("codec encode failed"));
    assert_eq!(
        writes_submitted.load(Ordering::Relaxed),
        0,
        "a codec error must prevent the database write from being submitted"
    );
}

#[test]
fn page_codec_vectored_encode_error_does_not_submit_io() {
    let writes_submitted = Arc::new(AtomicUsize::new(0));
    let db_file = DatabaseFile {
        file: Arc::new(MockFile {
            read_result: Ok(0),
            writes_submitted: writes_submitted.clone(),
        }),
    };
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let mut io_ctx = IOContext::default();
    io_ctx.set_page_codec(Arc::new(FailOnEncodePageCodec {
        fail_page_no: 3,
        contexts: contexts.clone(),
    }));
    let buffers = (0..3)
        .map(|_| Arc::new(Buffer::new_temporary(512)))
        .collect();

    let err = db_file
        .write_pages(2, 512, buffers, &io_ctx, Completion::new_write(|_| {}))
        .unwrap_err();

    assert!(err.to_string().contains("codec encode failed"));
    assert_eq!(writes_submitted.load(Ordering::Relaxed), 0);
    assert_eq!(
        *contexts.lock().unwrap(),
        vec![
            PageCodecContext::new(2, PageLocation::Database),
            PageCodecContext::new(3, PageLocation::Database),
        ]
    );
}

#[test]
fn page_codec_database_read_reports_decode_error() {
    let db_file = DatabaseFile {
        file: Arc::new(MockFile {
            read_result: Ok(512),
            writes_submitted: Arc::new(AtomicUsize::new(0)),
        }),
    };
    let mut io_ctx = IOContext::default();
    io_ctx.set_page_codec(Arc::new(FailingPageCodec::Decode));
    let original = Completion::new_read(Arc::new(Buffer::new_temporary(512)), |_| None);

    let wrapped = db_file.read_page(1, &io_ctx, original.clone()).unwrap();
    let err = MemoryIO::new().wait_for_completion(wrapped).unwrap_err();

    assert!(matches!(
        err,
        LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 1 })
    ));
    assert!(matches!(
        original.get_error(),
        Some(CompletionError::PageCodecError { page_idx: 1 })
    ));
}
