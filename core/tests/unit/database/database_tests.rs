use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use super::{is_memory_like, Database, InitState};
use crate::storage::encryption::EncryptionKey;
use crate::storage::page_transform::{
    PageCodec, PageCodecContext, PageCodecHeaderInfo, PageCodecId, PageLocation,
};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::{
    storage::database::DatabaseFile, CompletionError, Connection, DatabaseOpts, DatabaseStorage,
    EncryptionOpts, IOResult, LimboError, OpenDbAsyncState, OpenFlags, OpenOptions, PlatformIO,
    SqliteDialect, IO,
};

#[test]
fn memory_path_classifies_named_memory_databases() {
    assert!(is_memory_like(":memory:"));
    assert!(is_memory_like(":memory:sync-draft"));
    assert!(is_memory_like("file::memory:?cache=shared"));
    assert!(is_memory_like(""));
    assert!(!is_memory_like("memory.db"));
    assert!(!is_memory_like("file:memory.db"));
}

#[cfg(feature = "fs")]
#[test]
fn io_for_path_uses_memory_io_for_named_memory_database() {
    let path = format!(":memory:named-io-selection-{}", std::process::id());
    assert!(std::fs::metadata(&path).is_err());

    let io = Database::io_for_path(&path).unwrap();

    assert!(io.file_id(&path).is_ok());
    assert!(std::fs::metadata(&path).is_err());
}

#[derive(Debug)]
struct IdentityPageCodec;

impl PageCodec for IdentityPageCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(*b"identity-codec--")
    }

    fn required_reserved_bytes(&self) -> u8 {
        0
    }

    fn encode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        output.copy_from_slice(input);
        Ok(())
    }

    fn decode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        output.copy_from_slice(input);
        Ok(())
    }
}

#[derive(Debug)]
struct InvalidHeaderPageCodec {
    page_size: usize,
    reserved_space: u8,
}

impl PageCodec for InvalidHeaderPageCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(*b"invalid-header-c")
    }

    fn bootstrap_page_info(&self, _raw_page1_prefix: &[u8]) -> crate::Result<PageCodecHeaderInfo> {
        Ok(PageCodecHeaderInfo {
            page_size: self.page_size,
            reserved_space: self.reserved_space,
        })
    }

    fn required_reserved_bytes(&self) -> u8 {
        0
    }

    fn encode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        output.copy_from_slice(input);
        Ok(())
    }

    fn decode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        output.copy_from_slice(input);
        Ok(())
    }
}

#[derive(Debug)]
struct XorPageCodec {
    mask: u8,
    reserved_bytes: u8,
}

impl XorPageCodec {
    fn transform(&self, page: &[u8], output: &mut [u8]) {
        for (input, output) in page.iter().zip(output) {
            *output = input ^ self.mask;
        }
    }
}

impl PageCodec for XorPageCodec {
    fn codec_id(&self) -> PageCodecId {
        let mut id = *b"xor-page-codec--";
        id[15] = self.mask;
        id[14] = self.reserved_bytes;
        PageCodecId::new(id)
    }

    fn bootstrap_page_info(&self, raw_page1_prefix: &[u8]) -> crate::Result<PageCodecHeaderInfo> {
        if raw_page1_prefix.len() < 21 {
            return Err(LimboError::NotADB);
        }

        let decoded_magic = raw_page1_prefix[..16]
            .iter()
            .map(|byte| byte ^ self.mask)
            .collect::<Vec<_>>();
        if decoded_magic.as_slice() != b"SQLite format 3\0" {
            return Err(LimboError::NotADB);
        }

        let ps_raw = u16::from_be_bytes([
            raw_page1_prefix[16] ^ self.mask,
            raw_page1_prefix[17] ^ self.mask,
        ]);
        let page_size = if ps_raw == 1 { 65536 } else { ps_raw as usize };
        Ok(PageCodecHeaderInfo {
            page_size,
            reserved_space: raw_page1_prefix[20] ^ self.mask,
        })
    }

    fn required_reserved_bytes(&self) -> u8 {
        self.reserved_bytes
    }

    fn encode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        self.transform(input, output);
        Ok(())
    }

    fn decode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        self.transform(input, output);
        Ok(())
    }
}

#[derive(Debug)]
struct CountingPageCodec {
    inner: XorPageCodec,
    database_page1_decodes: Arc<AtomicUsize>,
}

impl PageCodec for CountingPageCodec {
    fn codec_id(&self) -> PageCodecId {
        self.inner.codec_id()
    }

    fn bootstrap_page_info(&self, raw_page1_prefix: &[u8]) -> crate::Result<PageCodecHeaderInfo> {
        self.inner.bootstrap_page_info(raw_page1_prefix)
    }

    fn required_reserved_bytes(&self) -> u8 {
        self.inner.required_reserved_bytes()
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        self.inner.encode_page(context, input, output)
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        if context.page_no == DatabaseHeader::PAGE_ID as u32
            && context.location == PageLocation::Database
        {
            self.database_page1_decodes.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.decode_page(context, input, output)
    }
}

#[derive(Debug)]
struct FailOncePageCodec {
    inner: XorPageCodec,
    fail_decode: Arc<AtomicBool>,
    fail_context: PageCodecContext,
}

impl PageCodec for FailOncePageCodec {
    fn codec_id(&self) -> PageCodecId {
        self.inner.codec_id()
    }

    fn bootstrap_page_info(&self, raw_page1_prefix: &[u8]) -> crate::Result<PageCodecHeaderInfo> {
        self.inner.bootstrap_page_info(raw_page1_prefix)
    }

    fn required_reserved_bytes(&self) -> u8 {
        self.inner.required_reserved_bytes()
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        self.inner.encode_page(context, input, output)
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        if context == self.fail_context && self.fail_decode.swap(false, Ordering::Relaxed) {
            return Err(LimboError::InternalError(
                "injected page decode failure".into(),
            ));
        }
        self.inner.decode_page(context, input, output)
    }
}

#[derive(Debug, Default)]
struct PageCodecFailureSwitches {
    wal_encode: AtomicBool,
    wal_decode: AtomicBool,
    database_encode: AtomicBool,
    database_decode_page: AtomicUsize,
}

#[derive(Debug)]
struct FailOnceTransformPageCodec {
    inner: XorPageCodec,
    failures: Arc<PageCodecFailureSwitches>,
}

impl PageCodec for FailOnceTransformPageCodec {
    fn codec_id(&self) -> PageCodecId {
        self.inner.codec_id()
    }

    fn bootstrap_page_info(&self, raw_page1_prefix: &[u8]) -> crate::Result<PageCodecHeaderInfo> {
        self.inner.bootstrap_page_info(raw_page1_prefix)
    }

    fn required_reserved_bytes(&self) -> u8 {
        self.inner.required_reserved_bytes()
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        let failure = match context.location {
            PageLocation::Database => &self.failures.database_encode,
            PageLocation::Wal => &self.failures.wal_encode,
        };
        if failure.swap(false, Ordering::Relaxed) {
            return Err(LimboError::InternalError(format!(
                "injected {:?} encode failure",
                context.location
            )));
        }
        self.inner.encode_page(context, input, output)
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        let fail = match context.location {
            PageLocation::Database => self
                .failures
                .database_decode_page
                .compare_exchange(
                    context.page_no as usize,
                    0,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok(),
            PageLocation::Wal => self.failures.wal_decode.swap(false, Ordering::Relaxed),
        };
        if fail {
            if let (Some(input), Some(output)) = (input.first(), output.first_mut()) {
                *output = *input;
            }
            return Err(LimboError::InternalError(format!(
                "injected {:?} decode failure",
                context.location
            )));
        }
        self.inner.decode_page(context, input, output)
    }
}

#[derive(Debug)]
struct TaggedPageCodec;

impl TaggedPageCodec {
    const TAG_BYTES: usize = std::mem::size_of::<u64>();

    // This is deliberately a small test-only integrity tag, not a
    // cryptographic authenticator.
    fn tag(context: PageCodecContext, data: &[u8]) -> u64 {
        let location = match context.location {
            PageLocation::Database => 0x4442_5041_4745_0000,
            PageLocation::Wal => 0x5741_4c50_4147_4500,
        };
        data.iter()
            .fold(location ^ u64::from(context.page_no), |tag, byte| {
                tag.rotate_left(5) ^ u64::from(*byte)
            })
    }
}

impl PageCodec for TaggedPageCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(*b"tagged-page-----")
    }

    fn required_reserved_bytes(&self) -> u8 {
        Self::TAG_BYTES as u8
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        if input.len() != output.len() || input.len() < Self::TAG_BYTES {
            return Err(LimboError::InvalidArgument(
                "tagged codec requires equal page-sized buffers".into(),
            ));
        }
        let data_len = input.len() - Self::TAG_BYTES;
        output[..data_len].copy_from_slice(&input[..data_len]);
        output[data_len..].copy_from_slice(&Self::tag(context, &input[..data_len]).to_le_bytes());
        Ok(())
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        if input.len() != output.len() || input.len() < Self::TAG_BYTES {
            return Err(LimboError::InvalidArgument(
                "tagged codec requires equal page-sized buffers".into(),
            ));
        }
        let data_len = input.len() - Self::TAG_BYTES;
        let stored = u64::from_le_bytes(
            input[data_len..]
                .try_into()
                .expect("tag length was checked above"),
        );
        let expected = Self::tag(context, &input[..data_len]);
        if stored != expected {
            return Err(LimboError::Corrupt(format!(
                "invalid page tag for page {} at {:?}",
                context.page_no, context.location
            )));
        }
        output[..data_len].copy_from_slice(&input[..data_len]);
        output[data_len..].fill(0);
        Ok(())
    }
}

#[derive(Debug)]
struct LocationPageCodec;

impl LocationPageCodec {
    const MASK: u8 = 0x6d;

    fn byte_key(page_no: u32, location: PageLocation, offset: usize) -> u8 {
        // Persisted transforms must not depend on the host pointer width.
        let page_id_byte = (page_no as u64).to_le_bytes()[offset % std::mem::size_of::<u64>()];
        let page_id_rotation = ((offset / std::mem::size_of::<u64>()) % (u8::BITS as usize)) as u32;
        Self::MASK
            ^ page_id_byte.rotate_left(page_id_rotation)
            ^ match location {
                PageLocation::Database => 0,
                PageLocation::Wal => 0xa5,
            }
    }

    fn transform(input: &[u8], output: &mut [u8], context: PageCodecContext) {
        for (offset, (input, output)) in input.iter().zip(output).enumerate() {
            *output = input ^ Self::byte_key(context.page_no, context.location, offset);
        }
    }
}

impl PageCodec for LocationPageCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(*b"location-page-co")
    }

    fn bootstrap_page_info(&self, raw_page1_prefix: &[u8]) -> crate::Result<PageCodecHeaderInfo> {
        if raw_page1_prefix.len() < 21 {
            return Err(LimboError::NotADB);
        }
        if !raw_page1_prefix[..16]
            .iter()
            .enumerate()
            .zip(b"SQLite format 3\0")
            .all(|((offset, encoded), expected)| {
                *encoded
                    ^ Self::byte_key(
                        DatabaseHeader::PAGE_ID as u32,
                        PageLocation::Database,
                        offset,
                    )
                    == *expected
            })
        {
            return Err(LimboError::NotADB);
        }

        let page_size = u16::from_be_bytes([
            raw_page1_prefix[16]
                ^ Self::byte_key(DatabaseHeader::PAGE_ID as u32, PageLocation::Database, 16),
            raw_page1_prefix[17]
                ^ Self::byte_key(DatabaseHeader::PAGE_ID as u32, PageLocation::Database, 17),
        ]);
        Ok(PageCodecHeaderInfo {
            page_size: if page_size == 1 {
                65_536
            } else {
                page_size as usize
            },
            reserved_space: raw_page1_prefix[20]
                ^ Self::byte_key(DatabaseHeader::PAGE_ID as u32, PageLocation::Database, 20),
        })
    }

    fn required_reserved_bytes(&self) -> u8 {
        1
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        Self::transform(input, output, context);
        Ok(())
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        Self::transform(input, output, context);
        Ok(())
    }
}

#[test]
fn location_page_codec_uses_full_page_id() {
    let input = vec![0; 512];
    let mut page_one = vec![0; input.len()];
    let mut page_two_fifty_seven = vec![0; input.len()];
    LocationPageCodec::transform(
        &input,
        &mut page_one,
        PageCodecContext::new(1, PageLocation::Database),
    );
    LocationPageCodec::transform(
        &input,
        &mut page_two_fifty_seven,
        PageCodecContext::new(257, PageLocation::Database),
    );

    assert_ne!(page_one, page_two_fifty_seven);

    let mut decoded = vec![0; input.len()];
    LocationPageCodec::transform(
        &page_two_fifty_seven,
        &mut decoded,
        PageCodecContext::new(257, PageLocation::Database),
    );
    assert_eq!(decoded, input);
}

#[derive(Debug)]
struct XorDefaultBootstrapPageCodec {
    mask: u8,
    reserved_bytes: u8,
}

impl XorDefaultBootstrapPageCodec {
    fn transform(&self, page: &[u8], output: &mut [u8]) {
        for (input, output) in page.iter().zip(output) {
            *output = input ^ self.mask;
        }
    }
}

impl PageCodec for XorDefaultBootstrapPageCodec {
    fn codec_id(&self) -> PageCodecId {
        let mut id = *b"xor-no-probe----";
        id[15] = self.mask;
        id[14] = self.reserved_bytes;
        PageCodecId::new(id)
    }

    fn required_reserved_bytes(&self) -> u8 {
        self.reserved_bytes
    }

    fn encode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        self.transform(input, output);
        Ok(())
    }

    fn decode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> crate::Result<()> {
        self.transform(input, output);
        Ok(())
    }
}

#[cfg(feature = "fs")]
fn open_with_page_codec_result(
    io: Arc<dyn IO>,
    path: &str,
    codec: Arc<dyn PageCodec>,
) -> crate::Result<Arc<Database>> {
    open_with_page_codec_with_opts_result(io, path, codec, DatabaseOpts::new())
}

#[cfg(feature = "fs")]
fn open_with_page_codec_with_opts_result(
    io: Arc<dyn IO>,
    path: &str,
    codec: Arc<dyn PageCodec>,
    opts: DatabaseOpts,
) -> crate::Result<Arc<Database>> {
    let file = io.open_file(path, OpenFlags::Create, true).unwrap();
    let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(file));
    let mut state = OpenDbAsyncState::new();
    let options = OpenOptions::new(Arc::new(SqliteDialect))
        .storage(db_file)
        .flags(OpenFlags::Create)
        .db_opts(opts)
        .page_codec(codec);

    loop {
        match Database::open_async(&mut state, io.clone(), path, &options)? {
            IOResult::Done(db) => return Ok(db),
            IOResult::IO(completion) => completion.wait(&*io)?,
        }
    }
}

#[cfg(feature = "fs")]
fn open_with_page_codec(io: Arc<dyn IO>, path: &str, codec: Arc<dyn PageCodec>) -> Arc<Database> {
    open_with_page_codec_result(io, path, codec).unwrap()
}

#[cfg(feature = "fs")]
fn count_test_rows(conn: &Arc<Connection>) -> i64 {
    let mut stmt = conn.prepare("select count(*) from test").unwrap();
    let mut count = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    count
}

fn try_count_test_rows(conn: &Arc<Connection>) -> crate::Result<i64> {
    let mut stmt = conn.prepare("select count(*) from test")?;
    let mut count = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0)?;
        Ok(())
    })?;
    Ok(count)
}

#[cfg(feature = "fs")]
fn passive_checkpoint_busy(conn: &Arc<Connection>) -> crate::Result<i64> {
    let mut stmt = conn.prepare("PRAGMA wal_checkpoint(PASSIVE)")?;
    let mut busy = None;
    stmt.run_with_row_callback(|row| {
        busy = Some(row.get(0)?);
        Ok(())
    })?;
    busy.ok_or_else(|| {
        LimboError::InternalError("wal_checkpoint did not return a result row".to_owned())
    })
}

#[cfg(feature = "fs")]
#[test]
fn registry_reuses_cached_database_without_retaining_page_codec() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-registry.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let first_codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
    let first_codec_weak = Arc::downgrade(&first_codec);
    let second_codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);

    let first = open_with_page_codec(io.clone(), path, first_codec);
    assert!(
        first_codec_weak.upgrade().is_none(),
        "cached Database must not retain the page codec"
    );
    let second = open_with_page_codec(io, path, second_codec);

    assert!(Arc::ptr_eq(&first, &second));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_identity_must_match_cached_database() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-identity.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let first_codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0xa5,
        reserved_bytes: 1,
    });
    let different_codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x5a,
        reserved_bytes: 1,
    });

    let db = open_with_page_codec(io.clone(), path, first_codec);

    let err = match db.connect_with_page_codec(different_codec.clone()) {
        Ok(_) => panic!("a cached codec-backed database must reject a different codec"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("page codec identity does not match the existing database"));

    let err = open_with_page_codec_result(io, path, different_codec).unwrap_err();
    assert!(err
        .to_string()
        .contains("page codec identity does not match the existing database"));
}

#[cfg(feature = "fs")]
#[test]
fn cached_database_rejects_encryption_and_page_codec() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-encryption-cached.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
    let _db = open_with_page_codec(io.clone(), path, codec.clone());

    let err = Database::open(
        io,
        path,
        OpenOptions::new(Arc::new(SqliteDialect))
            .flags(OpenFlags::Create)
            .encryption(EncryptionOpts {
                cipher: "aes256gcm".to_string(),
                hexkey: "00".repeat(32),
            })
            .page_codec(codec),
    )
    .unwrap_err();
    assert!(err
        .to_string()
        .contains("built-in encryption cannot be combined with an external page codec"));
}

#[cfg(feature = "fs")]
#[test]
fn external_page_codec_rejects_multiprocess_wal_before_opening_file() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-multiprocess-wal.db");
    let path_str = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    let err = Database::open(
        io,
        path_str,
        OpenOptions::new(Arc::new(SqliteDialect))
            .flags(OpenFlags::Create)
            .db_opts(DatabaseOpts::new().with_multiprocess_wal(true))
            .page_codec(Arc::new(IdentityPageCodec) as Arc<dyn PageCodec>),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        LimboError::InvalidArgument(ref message)
            if message
                == "external page codecs are not supported with experimental multiprocess WAL"
    ));
    assert!(!path.exists());
}

#[cfg(feature = "fs")]
#[test]
fn bypass_registry_page_codec_open_rejects_multiprocess_wal() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-bypass-multiprocess-wal.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let file = io.open_file(path, OpenFlags::Create, true).unwrap();
    let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(file));
    let mut state = OpenDbAsyncState::new();

    let options = OpenOptions::new(Arc::new(SqliteDialect))
        .storage(db_file)
        .flags(OpenFlags::Create)
        .db_opts(DatabaseOpts::new().with_multiprocess_wal(true))
        .page_codec(Arc::new(IdentityPageCodec) as Arc<dyn PageCodec>);
    let err = match Database::do_open_async(&mut state, io, path, &options) {
        Err(err) => err,
        Ok(_) => panic!("multiprocess WAL must reject an external page codec"),
    };

    assert!(matches!(
        *err,
        LimboError::InvalidArgument(ref message)
            if message
                == "external page codecs are not supported with experimental multiprocess WAL"
    ));
}

#[cfg(feature = "fs")]
#[test]
fn cached_page_codec_database_requires_codec_at_connection_time() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-registry-no-codec.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0xa5,
        reserved_bytes: 1,
    });
    let _db = open_with_page_codec(io.clone(), path, codec.clone());
    let db = open_with_page_codec(io.clone(), path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();
    conn.execute("create table test(id integer primary key)")
        .unwrap();
    drop(conn);

    assert!(
        Database::open_file_with_flags(
            io,
            path,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .is_err(),
        "opening without the codec must not reuse a codec-required database"
    );
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_checkpoint_decodes_database_page1_for_identity() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-checkpoint-identity.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let database_page1_decodes = Arc::new(AtomicUsize::new(0));
    let codec: Arc<dyn PageCodec> = Arc::new(CountingPageCodec {
        inner: XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        },
        database_page1_decodes: database_page1_decodes.clone(),
    });
    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();
    conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
    conn.execute(
        "create table test(id integer primary key, value text);
             insert into test(value) values ('alpha');",
    )
    .unwrap();
    conn.set_sync_mode(crate::SyncMode::Full);

    database_page1_decodes.store(0, Ordering::Relaxed);
    let checkpoint = conn
        .checkpoint(crate::storage::wal::CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();

    assert!(checkpoint.wal_checkpoint_backfilled > 0);
    assert_eq!(
        database_page1_decodes.load(Ordering::Relaxed),
        1,
        "checkpoint identity must decode database page 1 exactly once"
    );
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_checkpoint_recovers_after_page1_decode_failure() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-checkpoint-retry.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let fail_database_page1_decode = Arc::new(AtomicBool::new(false));
    let codec: Arc<dyn PageCodec> = Arc::new(FailOncePageCodec {
        inner: XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        },
        fail_decode: fail_database_page1_decode.clone(),
        fail_context: PageCodecContext::new(DatabaseHeader::PAGE_ID as u32, PageLocation::Database),
    });
    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();
    conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
    conn.execute(
        "create table test(id integer primary key, value text);
             insert into test(value) values ('alpha');",
    )
    .unwrap();
    conn.set_sync_mode(crate::SyncMode::Full);

    fail_database_page1_decode.store(true, Ordering::Relaxed);
    let err = conn
        .checkpoint(crate::storage::wal::CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 1 })
    ));

    let checkpoint = conn
        .checkpoint(crate::storage::wal::CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();
    assert!(checkpoint.wal_checkpoint_backfilled > 0);
    assert_eq!(count_test_rows(&conn), 1);
}

#[cfg(feature = "fs")]
#[test]
fn pragma_checkpoint_codec_error_releases_checkpoint_lock() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-pragma-checkpoint-retry.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let fail_wal_page2_decode = Arc::new(AtomicBool::new(false));
    let codec: Arc<dyn PageCodec> = Arc::new(FailOncePageCodec {
        inner: XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        },
        fail_decode: fail_wal_page2_decode.clone(),
        fail_context: PageCodecContext::new(2, PageLocation::Wal),
    });
    let db = open_with_page_codec(io, path, codec.clone());
    let writer = db.connect_with_page_codec(codec.clone()).unwrap();
    writer.wal_auto_actions_disable();
    writer.execute("PRAGMA journal_mode = 'wal'").unwrap();
    writer
        .execute(
            "create table test(id integer primary key, value text);
                 insert into test(value) values ('alpha');",
        )
        .unwrap();

    let pager = writer.get_pager();
    assert!(
        pager.wal_pos().1 > 0,
        "test setup must leave committed WAL frames"
    );
    assert_eq!(
        pager.wal_backfill_frame(),
        Some(0),
        "test setup must not checkpoint the WAL before injecting the codec error"
    );
    assert!(
        pager.wal_changed_pages_after(0).unwrap().contains(&2),
        "test setup must leave page 2 in the WAL"
    );

    let first = db.connect_with_page_codec(codec.clone()).unwrap();
    assert!(first.get_pager().has_wal());
    fail_wal_page2_decode.store(true, Ordering::Relaxed);
    let first_error = passive_checkpoint_busy(&first).unwrap_err();
    assert!(matches!(first_error, LimboError::CheckpointFailed(_)));
    assert!(!fail_wal_page2_decode.load(Ordering::Relaxed));

    let second = db.connect_with_page_codec(codec).unwrap();
    assert!(second.get_pager().has_wal());
    let other_connection_retry = passive_checkpoint_busy(&second);
    let same_connection_retry = passive_checkpoint_busy(&first);
    assert!(
        matches!(same_connection_retry, Ok(0)) && matches!(other_connection_retry, Ok(0)),
        "checkpoint failure must not leave stale state or retain its guard: same connection \
             returned {same_connection_retry:?}, other connection returned {other_connection_retry:?}"
    );
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_transaction_recovers_after_wal_encode_failure() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-commit-retry.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let failures = Arc::new(PageCodecFailureSwitches::default());
    let codec: Arc<dyn PageCodec> = Arc::new(FailOnceTransformPageCodec {
        inner: XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        },
        failures: failures.clone(),
    });
    let db = open_with_page_codec(io.clone(), path, codec.clone());
    let conn = db.connect_with_page_codec(codec.clone()).unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();

    failures.wal_encode.store(true, Ordering::Relaxed);
    let err = conn
        .execute("insert into test(value) values ('not-committed')")
        .unwrap_err();
    assert!(err.to_string().contains("injected Wal encode failure"));
    assert_eq!(count_test_rows(&conn), 0);

    conn.execute("insert into test(value) values ('committed')")
        .unwrap();
    assert_eq!(count_test_rows(&conn), 1);
    conn.checkpoint(crate::CheckpointMode::Full).unwrap();
    drop(conn);
    drop(db);

    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&conn), 1);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_pager_read_recovers_after_decode_failure() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-read-retry.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let failures = Arc::new(PageCodecFailureSwitches::default());
    let codec: Arc<dyn PageCodec> = Arc::new(FailOnceTransformPageCodec {
        inner: XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        },
        failures: failures.clone(),
    });

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
                 insert into test(value) values ('persisted');",
        )
        .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec.clone()).unwrap();
    failures.database_decode_page.store(2, Ordering::Relaxed);
    let err = try_count_test_rows(&conn).unwrap_err();
    assert!(matches!(
        err,
        LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 2 })
    ));

    assert_eq!(try_count_test_rows(&conn).unwrap(), 1);
    let second = db.connect_with_page_codec(codec).unwrap();
    assert_eq!(try_count_test_rows(&second).unwrap(), 1);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_checkpoint_recovers_after_backfill_transform_failures() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-backfill-retry.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let failures = Arc::new(PageCodecFailureSwitches::default());
    let codec: Arc<dyn PageCodec> = Arc::new(FailOnceTransformPageCodec {
        inner: XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        },
        failures: failures.clone(),
    });
    let db = open_with_page_codec(io.clone(), path, codec.clone());
    let conn = db.connect_with_page_codec(codec.clone()).unwrap();
    conn.execute(
        "create table test(id integer primary key, value text);
             insert into test(value) values ('first');",
    )
    .unwrap();

    conn.pager.load().clear_page_cache(false);
    failures.wal_decode.store(true, Ordering::Relaxed);
    let err = conn.checkpoint(crate::CheckpointMode::Full).unwrap_err();
    assert!(matches!(
        err,
        LimboError::CompletionError(CompletionError::PageCodecError { .. })
    ));
    assert_eq!(count_test_rows(&conn), 1);
    conn.checkpoint(crate::CheckpointMode::Full).unwrap();

    conn.execute("insert into test(value) values ('second')")
        .unwrap();
    failures.database_encode.store(true, Ordering::Relaxed);
    let err = conn.checkpoint(crate::CheckpointMode::Full).unwrap_err();
    assert!(err.to_string().contains("injected Database encode failure"));
    assert_eq!(count_test_rows(&conn), 2);
    conn.checkpoint(crate::CheckpointMode::Full).unwrap();
    drop(conn);
    drop(db);

    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&conn), 2);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_round_trips_wal_and_checkpointed_database_with_bootstrap_header() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-roundtrip.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x5a,
        reserved_bytes: 1,
    });

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
                 insert into test(value) values ('alpha'), ('bravo');",
        )
        .unwrap();
        assert_eq!(count_test_rows(&conn), 2);
        conn.set_sync_mode(crate::SyncMode::Full);
        let passive = conn
            .checkpoint(crate::storage::wal::CheckpointMode::Passive {
                upper_bound_inclusive: None,
            })
            .unwrap();
        assert!(
            passive.wal_checkpoint_backfilled > 0,
            "PASSIVE checkpoint must backfill codec-transformed pages"
        );
        conn.execute("insert into test(value) values ('charlie')")
            .unwrap();
        let full = conn
            .checkpoint(crate::storage::wal::CheckpointMode::Full)
            .unwrap();
        assert!(
            full.wal_checkpoint_backfilled > 0,
            "FULL checkpoint must backfill codec-transformed pages"
        );
        assert_eq!(count_test_rows(&conn), 3);
    }

    let raw_database = std::fs::read(path).unwrap();
    assert_ne!(&raw_database[..16], b"SQLite format 3\0");

    let reopened = open_with_page_codec(io, path, codec.clone());
    let conn = reopened.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&conn), 3);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_reopens_live_wal_then_checkpointed_database() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-live-wal-recovery.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(LocationPageCodec);

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
                 insert into test(value) values ('alpha'), ('bravo'), ('charlie');",
        )
        .unwrap();
        assert_eq!(count_test_rows(&conn), 3);
    }
    let wal_path = format!("{path}-wal");
    assert!(
        std::fs::metadata(&wal_path).unwrap().len() > 0,
        "the first reopen must recover committed codec-transformed WAL frames"
    );

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        assert_eq!(count_test_rows(&conn), 3);
        let checkpoint = conn
            .checkpoint(crate::storage::wal::CheckpointMode::Full)
            .unwrap();
        assert!(
            checkpoint.wal_checkpoint_backfilled > 0,
            "the recovery checkpoint must backfill codec-transformed WAL frames"
        );
    }
    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&conn), 3);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_reopen_requires_recoverable_bootstrap_header() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-no-probe-roundtrip.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorDefaultBootstrapPageCodec {
        mask: 0x3c,
        reserved_bytes: 1,
    });

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
                 insert into test(value) values ('alpha'), ('bravo');",
        )
        .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    let err = open_with_page_codec_result(io, path, codec).unwrap_err();
    assert!(err
        .to_string()
        .contains("page codec reported invalid page size"));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_rejects_mvcc_mode() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-mvcc.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();

    let err = conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap_err();
    assert!(err
        .to_string()
        .contains("external page codecs are not supported with MVCC"));
}

#[cfg(feature = "fs")]
#[test]
fn plaintext_database_rejects_page_codec_connection() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("plaintext-codec-connection.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();

    let err = match db.connect_with_page_codec(Arc::new(IdentityPageCodec)) {
        Ok(_) => panic!("plaintext databases must reject external page codecs"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("database was opened without an external page codec"));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_database_rejects_codecless_connection() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-requires-connection-codec.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = open_with_page_codec(io, path, Arc::new(IdentityPageCodec));

    let err = match db.connect() {
        Ok(_) => panic!("codec-backed databases must reject codec-less connections"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("database requires an external page codec"));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_reopen_rejects_mismatched_reserved_space() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-reserved-space.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let initial_codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x5a,
        reserved_bytes: 1,
    });

    {
        let db = open_with_page_codec(io.clone(), path, initial_codec.clone());
        let conn = db.connect_with_page_codec(initial_codec).unwrap();
        conn.execute("create table test(id integer primary key)")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    for required_reserved_bytes in [0, 2] {
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x5a,
            reserved_bytes: required_reserved_bytes,
        });
        let err = open_with_page_codec_result(io.clone(), path, codec).unwrap_err();
        assert!(err.to_string().contains(&format!(
            "page codec requires exactly {required_reserved_bytes} reserved bytes, but database provides 1"
        )));
    }
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_reopen_rejects_invalid_header_layout() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-invalid-header-layout.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    {
        let db = open_with_page_codec(io.clone(), path, Arc::new(IdentityPageCodec));
        let conn = db
            .connect_with_page_codec(Arc::new(IdentityPageCodec))
            .unwrap();
        conn.execute("create table test(id integer primary key)")
            .unwrap();
        conn.execute("pragma wal_checkpoint(truncate)").unwrap();
    }

    for (codec, expected_error) in [
        (
            InvalidHeaderPageCodec {
                page_size: 513,
                reserved_space: 0,
            },
            "page codec reported invalid page size 513",
        ),
        (
            InvalidHeaderPageCodec {
                page_size: 512,
                reserved_space: 33,
            },
            "page codec reported invalid reserved space 33 for page size 512",
        ),
        (
            InvalidHeaderPageCodec {
                page_size: 8192,
                reserved_space: 0,
            },
            "page codec bootstrap page size 8192 does not match decoded page-1 size 4096",
        ),
    ] {
        let err = open_with_page_codec_result(io.clone(), path, Arc::new(codec)).unwrap_err();
        assert!(err.to_string().contains(expected_error));
    }
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_reopen_reports_short_header_read() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-short-header.db");
    std::fs::write(&path, [0u8]).unwrap();
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    let err = open_with_page_codec_result(io, path, Arc::new(IdentityPageCodec)).unwrap_err();
    assert!(err.to_string().contains("short read on page 1"));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_and_encryption_cannot_share_pager() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-encryption-conflict.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
    let db = open_with_page_codec(io, path, codec.clone());
    let encryption_key = EncryptionKey::from_hex_string(&"00".repeat(32)).unwrap();
    let mut state = InitState::default();

    let err = match db._init_nonblock(&mut state, Some(&encryption_key), Some(&codec)) {
        Ok(_) => panic!("encryption and an external page codec must not share a pager"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("built-in encryption cannot be combined with an external page codec"));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_connection_rejects_builtin_encryption_settings() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-encryption-settings.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();

    let err = conn
        .set_encryption_key(EncryptionKey::from_hex_string(&"00".repeat(32)).unwrap())
        .unwrap_err();
    assert!(matches!(
        err,
        LimboError::InvalidArgument(message)
            if message
                == "cannot configure built-in encryption while an external page codec is installed"
    ));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_vacuum_preserves_encoded_database() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-vacuum.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x4d,
        reserved_bytes: 1,
    });
    let db = open_with_page_codec_with_opts_result(
        io.clone(),
        path,
        codec.clone(),
        DatabaseOpts::new().with_vacuum(true),
    )
    .unwrap();
    let conn = db.connect_with_page_codec(codec.clone()).unwrap();
    conn.execute(
        "create table test(id integer primary key, value text);
             insert into test(value) values ('alpha'), ('bravo');",
    )
    .unwrap();
    conn.execute("VACUUM").unwrap();
    assert_eq!(count_test_rows(&conn), 2);
    drop(conn);
    drop(db);

    assert_ne!(&std::fs::read(path).unwrap()[..16], b"SQLite format 3\0");
    let reopened = open_with_page_codec(io, path, codec.clone());
    let conn = reopened.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&conn), 2);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_vacuum_into_preserves_encoded_database() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-vacuum-into.db");
    let output_path = temp_dir.path().join("codec-vacuum-into-copy.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x4d,
        reserved_bytes: 1,
    });
    let db = open_with_page_codec_with_opts_result(
        io.clone(),
        path,
        codec.clone(),
        DatabaseOpts::new().with_vacuum(true),
    )
    .unwrap();
    let conn = db.connect_with_page_codec(codec.clone()).unwrap();
    conn.execute(
        "create table test(id integer primary key, value text);
             insert into test(value) values ('secret-value');",
    )
    .unwrap();
    conn.execute(format!("VACUUM INTO '{}'", output_path.display()))
        .unwrap();

    let raw_output = std::fs::read(&output_path).unwrap();
    assert_ne!(&raw_output[..16], b"SQLite format 3\0");
    let output = open_with_page_codec(io, output_path.to_str().unwrap(), codec.clone());
    let output_conn = output.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&output_conn), 1);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_connection_uses_plain_internal_temp_database() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-temp.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x4d,
        reserved_bytes: 1,
    });
    let db = open_with_page_codec(io.clone(), path, codec.clone());
    let conn = db.connect_with_page_codec(codec.clone()).unwrap();
    conn.execute(
        "create table test(id integer primary key, value text);
             insert into test(value) values ('main');
             create temp table temp_values(value integer);
             insert into temp_values values (3), (1), (2);",
    )
    .unwrap();

    let mut values = Vec::new();
    conn.prepare("select value from temp_values order by value")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push(row.get::<i64>(0).unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(values, vec![1, 2, 3]);
    assert_eq!(count_test_rows(&conn), 1);
    drop(conn);
    drop(db);

    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&conn), 1);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_connections_preserve_old_reader_snapshot_during_checkpoint() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-connections.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec = || -> Arc<dyn PageCodec> {
        Arc::new(XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        })
    };
    let db = open_with_page_codec(io.clone(), path, codec());
    let writer = db.connect_with_page_codec(codec()).unwrap();
    let old_reader = db.connect_with_page_codec(codec()).unwrap();
    writer
        .execute(
            "create table test(id integer primary key, value text);
                 insert into test(value) values ('first');",
        )
        .unwrap();

    old_reader.execute("BEGIN").unwrap();
    assert_eq!(count_test_rows(&old_reader), 1);
    writer
        .execute("insert into test(value) values ('second')")
        .unwrap();
    assert_eq!(count_test_rows(&old_reader), 1);

    let new_reader = db.connect_with_page_codec(codec()).unwrap();
    assert_eq!(count_test_rows(&new_reader), 2);
    let passive = writer
        .checkpoint(crate::CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();
    assert!(passive.wal_total_backfilled < passive.wal_max_frame);
    assert_eq!(count_test_rows(&old_reader), 1);
    assert_eq!(count_test_rows(&new_reader), 2);

    old_reader.execute("COMMIT").unwrap();
    writer.checkpoint(crate::CheckpointMode::Full).unwrap();
    drop(new_reader);
    drop(old_reader);
    drop(writer);
    drop(db);

    let db = open_with_page_codec(io, path, codec());
    let conn = db.connect_with_page_codec(codec()).unwrap();
    assert_eq!(count_test_rows(&conn), 2);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_accepts_minimum_usable_space_boundary() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-minimum-usable-space.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x5a,
        reserved_bytes: 32,
    });

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute("PRAGMA page_size = 512").unwrap();
        assert_eq!(conn.get_page_size().get(), 512);
        conn.execute(
            "create table test(id integer primary key, value blob);
                 insert into test(value) values (zeroblob(2000));",
        )
        .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();
    assert_eq!(conn.get_page_size().get(), 512);
    assert_eq!(count_test_rows(&conn), 1);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_reserved_tail_detects_persistent_page_corruption() {
    use std::fs::OpenOptions as StdOpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};

    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-tagged-page.db");
    let path_str = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(TaggedPageCodec);

    {
        let db = open_with_page_codec(io.clone(), path_str, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute("PRAGMA page_size = 512").unwrap();
        conn.execute(
            "create table test(id integer primary key, value blob);
                 insert into test(value) values (zeroblob(2000));",
        )
        .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        assert_eq!(count_test_rows(&conn), 1);
    }

    let tag_offset = 2 * 512 - 1;
    let mut file = StdOpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(tag_offset)).unwrap();
    let mut original = [0];
    file.read_exact(&mut original).unwrap();
    file.seek(SeekFrom::Start(tag_offset)).unwrap();
    file.write_all(&[original[0] ^ 1]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    {
        let db = open_with_page_codec(io.clone(), path_str, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        let err = try_count_test_rows(&conn).unwrap_err();
        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 2 })
        ));
    }

    let mut file = StdOpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(tag_offset)).unwrap();
    file.write_all(&original).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let db = open_with_page_codec(io, path_str, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&conn), 1);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_reopens_checkpointed_database_read_only() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-read-only.db");
    let path_str = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x5a,
        reserved_bytes: 1,
    });

    {
        let db = open_with_page_codec(io.clone(), path_str, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
                 insert into test(value) values ('read-only');",
        )
        .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    let before = std::fs::metadata(&path).unwrap().modified().unwrap();

    let db = Database::open(
        io,
        path_str,
        OpenOptions::new(Arc::new(SqliteDialect))
            .flags(OpenFlags::ReadOnly)
            .page_codec(codec.clone()),
    )
    .unwrap();
    let conn = db.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&conn), 1);
    drop(conn);
    drop(db);
    assert_eq!(std::fs::metadata(path).unwrap().modified().unwrap(), before);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_rejects_database_already_in_mvcc_mode() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-existing-mvcc.db");
    let path_str = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            path_str,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }

    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0,
        reserved_bytes: if cfg!(feature = "checksum") {
            crate::storage::checksum::CHECKSUM_REQUIRED_RESERVED_BYTES
        } else {
            0
        },
    });
    let err = open_with_page_codec_result(io.clone(), path_str, codec).unwrap_err();
    assert!(
        err.to_string()
            .contains("external page codecs are not supported with MVCC databases"),
        "unexpected error: {err}"
    );

    let db = Database::open_file_with_flags(
        io,
        path_str,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    db.connect().unwrap();
}

#[cfg(feature = "fs")]
#[test]
fn location_page_codec_round_trips_wal_checkpoint_and_overflow_pages() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("location-codec.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(LocationPageCodec);

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute(
            "create table test(id integer primary key, value blob);
                 insert into test(value) values (zeroblob(12000));
                 delete from test where id = 1;
                 insert into test(value)
                 values (cast(replace(printf('%0*d', 16000, 0), '0', 'x') as blob));",
        )
        .unwrap();
        let checkpoint = conn
            .checkpoint(crate::storage::wal::CheckpointMode::Full)
            .unwrap();
        assert!(checkpoint.wal_checkpoint_backfilled > 0);
    }

    let raw_database = std::fs::read(path).unwrap();
    assert_ne!(&raw_database[..16], b"SQLite format 3\0");
    let reopened = open_with_page_codec(io, path, codec.clone());
    let conn = reopened.connect_with_page_codec(codec).unwrap();
    let mut values: Vec<(i64, String)> = Vec::new();
    conn.prepare("select length(value), hex(substr(value, 1, 8)) from test order by id")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push((row.get(0).unwrap(), row.get(1).unwrap()));
            Ok(())
        })
        .unwrap();
    assert_eq!(values, vec![(16_000, "7878787878787878".to_string())]);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_attach_is_rejected() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let main_path = temp_dir.path().join("codec-main.db");
    let aux_path = temp_dir.path().join("codec-aux.db");
    let main_path = main_path.to_str().unwrap();
    let aux_path = aux_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x91,
        reserved_bytes: 1,
    });
    let db = open_with_page_codec_with_opts_result(
        io.clone(),
        main_path,
        codec.clone(),
        DatabaseOpts::new().with_attach(true),
    )
    .unwrap();
    let conn = db.connect_with_page_codec(codec.clone()).unwrap();
    let err = conn
        .execute(format!("ATTACH '{aux_path}' AS aux"))
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("ATTACH is unsupported for connections using an external page codec"));
    assert!(!std::path::Path::new(aux_path).exists());
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_rejects_reserved_space_mutation() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-reserved-space.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x91,
        reserved_bytes: 1,
    });
    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db.connect_with_page_codec(codec).unwrap();

    let err = conn.set_reserved_bytes(0).unwrap_err();
    assert!(err
        .to_string()
        .contains("page codec requires exactly 1 reserved bytes"));
    assert_eq!(conn.get_reserved_bytes(), Some(1));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_rejects_page_size_incompatible_with_reserved_space() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-page-size.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x5a,
        reserved_bytes: 33,
    });

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        // 512 - 33 = 479 usable bytes < 480: must be rejected up front,
        // otherwise the engine creates a database it refuses to reopen.
        let err = conn.execute("PRAGMA page_size = 512").unwrap_err();
        assert!(err.to_string().contains("usable bytes"));
        assert_eq!(conn.get_page_size().get(), 4096);
        // The database must remain usable at a compatible page size.
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        conn.execute("insert into test(value) values ('alpha')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    let reopened = open_with_page_codec(io, path, codec.clone());
    let conn = reopened.connect_with_page_codec(codec).unwrap();
    assert_eq!(count_test_rows(&conn), 1);
}
