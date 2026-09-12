use super::{c, CApiPageCodec};
use crate::{
    rsapi::{
        OpenFlags, TursoDatabase, TursoDatabaseConfig, TursoError, TursoStatusCode, FINALIZED_ERR,
    },
    IoBackend,
};
use std::{
    ffi::{c_char, c_void},
    mem::MaybeUninit,
    sync::Arc,
};
use turso_core::{
    LimboError, PageCodec, PageCodecContext, PageCodecHeaderInfo, PageCodecId, Value,
};

fn config_with_features(features: Option<&str>) -> TursoDatabaseConfig {
    TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: features.map(str::to_string),
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    }
}

#[test]
fn capi_page_codec_does_not_read_c_struct_padding() {
    unsafe extern "C" fn transform(
        _ctx: *mut c_void,
        _page_no: u32,
        _location: c::turso_codec_location_t,
        _input: *const u8,
        _input_len: usize,
        _output: *mut u8,
        _output_len: usize,
        _error: *mut *const c_char,
    ) -> i32 {
        0
    }

    let mut codec = MaybeUninit::<c::turso_page_codec_v1_t>::uninit();
    let codec = codec.as_mut_ptr();
    unsafe {
        std::ptr::addr_of_mut!((*codec).abi_version).write(1);
        std::ptr::addr_of_mut!((*codec).ctx).write(std::ptr::null_mut());
        std::ptr::addr_of_mut!((*codec).reserved_space).write(16);
        std::ptr::addr_of_mut!((*codec).codec_id).write([1; 16]);
        std::ptr::addr_of_mut!((*codec).destroy).write(None);
        std::ptr::addr_of_mut!((*codec).probe_header).write(None);
        std::ptr::addr_of_mut!((*codec).decode_page).write(Some(transform));
        std::ptr::addr_of_mut!((*codec).encode_page).write(Some(transform));

        let codec = CApiPageCodec::from_capi(codec).unwrap();
        assert_eq!(codec.inner.raw.codec_id, [1; 16]);
        assert_eq!(codec.inner.raw.reserved_space, 16);
    }
}

#[derive(Debug)]
struct XorPageCodec {
    mask: u8,
    reserved_bytes: u8,
}

impl XorPageCodec {
    fn transform(&self, page: &[u8], output: &mut [u8]) {
        for (input, output) in page.iter().zip(output.iter_mut()) {
            *output = *input ^ self.mask;
        }
    }

    fn stable_config_fingerprint(&self) -> [u8; 16] {
        fn mix(mut hash: u64, byte: u8) -> u64 {
            hash ^= byte as u64;
            hash.wrapping_mul(0x0000_0100_0000_01b3)
        }

        let mut hash1 = 0xcbf2_9ce4_8422_2325;
        for byte in b"turso-sdk-xor-page-codec-v1" {
            hash1 = mix(hash1, *byte);
        }
        hash1 = mix(hash1, b'm');
        hash1 = mix(hash1, self.mask);
        hash1 = mix(hash1, b'r');
        hash1 = mix(hash1, self.reserved_bytes);

        let mut hash2 = 0x9ae1_6a3b_2f90_404f;
        for byte in b"turso-sdk-xor-page-codec-v1".iter().rev() {
            hash2 = mix(hash2, *byte);
        }
        hash2 = mix(hash2, b'r');
        hash2 = mix(hash2, self.reserved_bytes);
        hash2 = mix(hash2, b'm');
        hash2 = mix(hash2, self.mask);

        let mut id = [0; 16];
        id[..8].copy_from_slice(&hash1.to_le_bytes());
        id[8..].copy_from_slice(&hash2.to_le_bytes());
        id
    }
}

impl PageCodec for XorPageCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(self.stable_config_fingerprint())
    }

    fn bootstrap_page_info(
        &self,
        raw_page1_prefix: &[u8],
    ) -> turso_core::Result<PageCodecHeaderInfo> {
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
        page: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        self.transform(page, output);
        Ok(())
    }

    fn decode_page(
        &self,
        _context: PageCodecContext,
        page: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        self.transform(page, output);
        Ok(())
    }
}

#[test]
pub fn page_codec_id_tracks_full_test_codec_configuration() {
    let base = XorPageCodec {
        mask: 0xa5,
        reserved_bytes: 1,
    }
    .codec_id();
    let different_mask = XorPageCodec {
        mask: 0x5a,
        reserved_bytes: 1,
    }
    .codec_id();
    let different_reserved_bytes = XorPageCodec {
        mask: 0xa5,
        reserved_bytes: 2,
    }
    .codec_id();

    assert_ne!(base, different_mask);
    assert_ne!(base, different_reserved_bytes);
}

fn open_database_with_page_codec(path: &str, codec: Arc<dyn PageCodec>) -> Arc<TursoDatabase> {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: path.to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: Some(codec),
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());
    db
}

#[test]
pub fn database_opts_maps_experimental_features() {
    // No features -> all defaults.
    assert_eq!(
        config_with_features(None).database_opts(),
        turso_core::DatabaseOpts::new()
    );

    // Each token toggles its corresponding flag.
    let opts = config_with_features(Some(
        "views,index_method,custom_types,autovacuum,vacuum,encryption,attach,generated_columns,multiprocess_wal,without_rowid",
    ))
    .database_opts();
    assert!(opts.enable_views);
    assert!(opts.enable_index_method);
    assert!(opts.enable_custom_types);
    assert!(opts.enable_autovacuum);
    assert!(opts.enable_vacuum);
    assert!(opts.enable_encryption);
    assert!(opts.enable_attach);
    assert!(opts.enable_generated_columns);
    assert!(opts.enable_multiprocess_wal);
    assert!(opts.enable_without_rowid);

    // Whitespace is trimmed; `strict` and unknown names are ignored.
    let opts = config_with_features(Some(" views , strict , unknown_one ")).database_opts();
    assert!(opts.enable_views);
    assert_eq!(
        config_with_features(Some("strict,unknown")).database_opts(),
        turso_core::DatabaseOpts::new()
    );
}

#[test]
pub fn page_codec_is_applied_to_sdk_connections() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0xa5,
        reserved_bytes: 1,
    });

    {
        let db = open_database_with_page_codec(db_path, codec.clone());
        let conn = db.connect().unwrap();

        let mut stmt = conn
            .prepare_single("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        assert_eq!(stmt.execute(None).unwrap().status, TursoStatusCode::Done);

        let mut stmt = conn
            .prepare_single("INSERT INTO test (id, value) VALUES (1, 'secret_data')")
            .unwrap();
        assert_eq!(stmt.execute(None).unwrap().status, TursoStatusCode::Done);

        let mut stmt = conn
            .prepare_single("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        assert_eq!(stmt.execute(None).unwrap().status, TursoStatusCode::Done);
    }

    let raw_database = std::fs::read(db_path).unwrap();
    assert_ne!(&raw_database[..16], b"SQLite format 3\0");

    {
        let db = open_database_with_page_codec(db_path, codec);
        let conn = db.connect().unwrap();

        let mut stmt = conn
            .prepare_single("SELECT value FROM test WHERE id = 1")
            .unwrap();
        assert_eq!(stmt.step(None).unwrap(), TursoStatusCode::Row);
        assert_eq!(stmt.row_value(0).unwrap().to_text(), Some("secret_data"));
    }
}

#[test]
pub fn page_codec_rejects_multiprocess_wal_through_sdk_open() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: Some("multiprocess_wal".to_string()),
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: Some(Arc::new(XorPageCodec {
            mask: 0xa5,
            reserved_bytes: 1,
        })),
        open_flags: OpenFlags::default(),
    });

    let error = db.open().unwrap_err();
    match error {
        TursoError::Error(message) => assert_eq!(
            message,
            "external page codecs are not supported with experimental multiprocess WAL"
        ),
        error => panic!("expected multiprocess WAL rejection, got {error:?}"),
    }
}

#[test]
pub fn test_db_concurrent_use() {
    use std::sync::{Arc, Barrier};

    let mut errors = Vec::new();
    for _ in 0..16 {
        let db = TursoDatabase::new(TursoDatabaseConfig {
            path: ":memory:".to_string(),
            experimental_features: None,
            async_io: false,
            encryption: None,
            vfs: IoBackend::Default,
            io: None,
            db_file: None,
            page_codec: None,
            open_flags: OpenFlags::default(),
        });
        let result = db.open().unwrap();
        assert!(!result.is_io());
        let conn = db.connect().unwrap();
        let stmt1 = conn
            .prepare_single("SELECT * FROM generate_series(1, 100000)")
            .unwrap();
        let stmt2 = conn
            .prepare_single("SELECT * FROM generate_series(1, 100000)")
            .unwrap();

        // Use a barrier to ensure both threads start executing at the same time
        let barrier = Arc::new(Barrier::new(2));
        let mut threads = Vec::new();
        for mut stmt in [stmt1, stmt2] {
            let barrier_clone = Arc::clone(&barrier);
            let thread = std::thread::spawn(move || {
                barrier_clone.wait();
                stmt.execute(None)
            });
            threads.push(thread);
        }
        let mut results = Vec::new();
        for thread in threads {
            results.push(thread.join().unwrap());
        }
        assert!(
            !(results[0].is_err() && results[1].is_err()),
            "results: {results:?}",
        );
        if results[0].is_err() || results[1].is_err() {
            errors.push(
                results[0]
                    .clone()
                    .err()
                    .or(results[1].clone().err())
                    .unwrap(),
            );
        }
    }
    println!("{errors:?}");
    assert!(
        !errors.is_empty(),
        "misuse errors should be very likely with the test setup: {errors:?}"
    );
    assert!(
        errors.iter().all(|e| matches!(e, TursoError::Misuse(_))),
        "all errors must have Misuse code: {errors:?}"
    );
}

#[test]
pub fn test_db_rsapi_use() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());
    let conn = db.connect().unwrap();
    let mut stmt = conn
        .prepare_single("SELECT * FROM generate_series(1, 10000)")
        .unwrap();
    assert_eq!(stmt.execute(None).unwrap().status, TursoStatusCode::Done);
}

#[test]
pub fn test_named_position_requires_prefixed_name() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();
    let mut stmt = conn
        .prepare_single("SELECT :new_name, @other_name, $third_name")
        .unwrap();

    assert_eq!(stmt.named_position(":new_name").unwrap(), 1);
    assert!(stmt.named_position("new_name").is_err());
    assert!(stmt.named_position("?1").is_err());

    assert_eq!(stmt.named_position("@other_name").unwrap(), 2);
    assert!(stmt.named_position("other_name").is_err());

    assert_eq!(stmt.named_position("$third_name").unwrap(), 3);
    assert!(stmt.named_position("third_name").is_err());
}

#[test]
pub fn test_bind_positional_rejects_out_of_bounds_index() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare_single("SELECT ?1").unwrap();

    stmt.bind_positional(1, Value::from_i64(42)).unwrap();

    let err = stmt.bind_positional(2, Value::from_i64(7)).unwrap_err();
    assert!(matches!(err, TursoError::Misuse(_)));
}

#[test]
pub fn test_execute_update_with_prefixed_named_parameters() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();

    let mut create_stmt = conn
        .prepare_single("CREATE TABLE simple (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    assert_eq!(
        create_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut insert_stmt = conn
        .prepare_single("INSERT INTO simple (name) VALUES ('original_name')")
        .unwrap();
    assert_eq!(
        insert_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut update_stmt = conn
        .prepare_single("UPDATE simple SET name = :new_name WHERE name = :old_name")
        .unwrap();

    let new_name_position = update_stmt.named_position(":new_name").unwrap();
    update_stmt
        .bind_positional(new_name_position, Value::build_text("updated_name"))
        .unwrap();
    let old_name_position = update_stmt.named_position(":old_name").unwrap();
    update_stmt
        .bind_positional(old_name_position, Value::build_text("original_name"))
        .unwrap();

    let update_result = update_stmt.execute(None).unwrap();
    assert_eq!(update_result.status, TursoStatusCode::Done);
    assert_eq!(update_result.rows_changed, 1);
}

#[test]
pub fn test_execute_update_with_mixed_placeholders() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();

    let mut create_stmt = conn
        .prepare_single("CREATE TABLE mixed (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, age INTEGER)")
        .unwrap();
    assert_eq!(
        create_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut insert_stmt = conn
        .prepare_single(
            "INSERT INTO mixed (name, email, age) VALUES ('alice', 'alice@old.com', 25)",
        )
        .unwrap();
    assert_eq!(
        insert_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut update_stmt = conn
        .prepare_single("UPDATE mixed SET email = ?, age = :new_age WHERE name = ?")
        .unwrap();

    assert_eq!(update_stmt.named_position("?1").unwrap(), 1);
    assert_eq!(update_stmt.named_position(":new_age").unwrap(), 2);
    assert!(update_stmt.named_position("new_age").is_err());
    assert_eq!(update_stmt.named_position("?3").unwrap(), 3);

    update_stmt
        .bind_positional(1, Value::build_text("alice@new.com"))
        .unwrap();
    let age_position = update_stmt.named_position(":new_age").unwrap();
    update_stmt
        .bind_positional(age_position, Value::from_i64(30))
        .unwrap();
    update_stmt
        .bind_positional(3, Value::build_text("alice"))
        .unwrap();

    let update_result = update_stmt.execute(None).unwrap();
    assert_eq!(update_result.status, TursoStatusCode::Done);
    assert_eq!(update_result.rows_changed, 1);
}

#[test]
pub fn test_select_named_and_positional_mapping_stays_sql_order() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();
    let mut create_stmt = conn
        .prepare_single("CREATE TABLE simple (name TEXT NOT NULL)")
        .unwrap();
    assert_eq!(
        create_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut stmt = conn
        .prepare_single("SELECT :named FROM simple WHERE name = ?")
        .unwrap();

    assert_eq!(stmt.named_position(":named").unwrap(), 1);
    assert!(stmt.named_position("named").is_err());
    assert_eq!(stmt.named_position("?2").unwrap(), 2);
}

#[test]
pub fn test_named_and_indexed_alias_share_slot() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();
    let mut stmt = conn
        .prepare_single("SELECT :v AS named_slot, ?1 AS pos_slot")
        .unwrap();

    assert_eq!(stmt.named_position(":v").unwrap(), 1);
    assert!(stmt.named_position("v").is_err());
    assert!(stmt.named_position("?1").is_err());

    stmt.bind_positional(1, Value::from_i64(7)).unwrap();
    assert_eq!(stmt.step(None).unwrap(), TursoStatusCode::Row);
    assert_eq!(stmt.row_value(0).unwrap().as_int(), Some(7));
    assert_eq!(stmt.row_value(1).unwrap().as_int(), Some(7));
}

#[test]
pub fn test_sparse_positional_index_uses_declared_slot() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare_single("SELECT ?3").unwrap();

    assert_eq!(stmt.parameters_count(), 3);
    stmt.bind_positional(1, Value::from_i64(1)).unwrap();

    stmt.bind_positional(3, Value::from_i64(9)).unwrap();
    assert_eq!(stmt.step(None).unwrap(), TursoStatusCode::Row);
    assert_eq!(stmt.row_value(0).unwrap().as_int(), Some(9));
}

#[test]
pub fn test_sparse_positional_index_count_matches_sqlite() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare_single("SELECT ?3").unwrap();

    assert_eq!(stmt.parameters_count(), 3);
    assert!(stmt.named_position("?1").is_err());
    assert_eq!(stmt.named_position("?3").unwrap(), 3);

    stmt.bind_positional(3, Value::from_i64(11)).unwrap();
    assert_eq!(stmt.step(None).unwrap(), TursoStatusCode::Row);
    assert_eq!(stmt.row_value(0).unwrap().as_int(), Some(11));
}

#[test]
pub fn test_insert_with_mixed_placeholders() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();
    let mut create_stmt = conn
        .prepare_single("CREATE TABLE users (name TEXT NOT NULL, age INTEGER NOT NULL)")
        .unwrap();
    assert_eq!(
        create_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut insert_stmt = conn
        .prepare_single("INSERT INTO users (name, age) VALUES (?, :age)")
        .unwrap();

    assert_eq!(insert_stmt.named_position("?1").unwrap(), 1);
    assert_eq!(insert_stmt.named_position(":age").unwrap(), 2);
    assert!(insert_stmt.named_position("age").is_err());

    insert_stmt
        .bind_positional(1, Value::build_text("alice"))
        .unwrap();
    let age_position = insert_stmt.named_position(":age").unwrap();
    insert_stmt
        .bind_positional(age_position, Value::from_i64(30))
        .unwrap();

    let insert_result = insert_stmt.execute(None).unwrap();
    assert_eq!(insert_result.status, TursoStatusCode::Done);
    assert_eq!(insert_result.rows_changed, 1);

    let mut verify_stmt = conn
        .prepare_single("SELECT age FROM users WHERE name = 'alice'")
        .unwrap();
    assert_eq!(verify_stmt.step(None).unwrap(), TursoStatusCode::Row);
    assert_eq!(verify_stmt.row_value(0).unwrap().as_int(), Some(30));
}

#[test]
pub fn test_delete_with_mixed_placeholders() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();
    let mut create_stmt = conn
        .prepare_single("CREATE TABLE users (name TEXT NOT NULL, age INTEGER NOT NULL)")
        .unwrap();
    assert_eq!(
        create_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut seed_stmt = conn
        .prepare_single("INSERT INTO users (name, age) VALUES ('alice', 30), ('bob', 40)")
        .unwrap();
    assert_eq!(
        seed_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut delete_stmt = conn
        .prepare_single("DELETE FROM users WHERE name = ? AND age = :age")
        .unwrap();

    assert_eq!(delete_stmt.named_position("?1").unwrap(), 1);
    assert_eq!(delete_stmt.named_position(":age").unwrap(), 2);
    assert!(delete_stmt.named_position("age").is_err());

    delete_stmt
        .bind_positional(1, Value::build_text("alice"))
        .unwrap();
    let age_position = delete_stmt.named_position(":age").unwrap();
    delete_stmt
        .bind_positional(age_position, Value::from_i64(30))
        .unwrap();

    let delete_result = delete_stmt.execute(None).unwrap();
    assert_eq!(delete_result.status, TursoStatusCode::Done);
    assert_eq!(delete_result.rows_changed, 1);

    let mut verify_stmt = conn.prepare_single("SELECT count(*) FROM users").unwrap();
    assert_eq!(verify_stmt.step(None).unwrap(), TursoStatusCode::Row);
    assert_eq!(verify_stmt.row_value(0).unwrap().as_int(), Some(1));
}

#[cfg(feature = "encryption")]
mod encryption_tests {
    use super::*;
    use tempfile::NamedTempFile;

    const TEST_CIPHER: &str = "aes256gcm";
    const TEST_HEXKEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const WRONG_HEXKEY: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    fn create_encryption_opts() -> crate::rsapi::EncryptionOpts {
        crate::rsapi::EncryptionOpts {
            cipher: TEST_CIPHER.to_string(),
            hexkey: TEST_HEXKEY.to_string(),
        }
    }

    fn assert_integer(value: turso_core::Value, expected: i64) {
        match value {
            turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => {
                assert_eq!(i, expected)
            }
            _ => panic!("Expected integer {expected}, got {value:?}"),
        }
    }

    #[test]
    fn test_encryption() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        // 1. Create encrypted database and insert data
        {
            let db = TursoDatabase::new(TursoDatabaseConfig {
                path: db_path.to_string(),
                experimental_features: Some("encryption".to_string()),
                async_io: false,
                encryption: Some(create_encryption_opts()),
                vfs: IoBackend::Default,
                io: None,
                db_file: None,
                page_codec: None,
                open_flags: OpenFlags::default(),
            });
            let result = db.open().unwrap();
            assert!(!result.is_io());
            let conn = db.connect().unwrap();

            let mut stmt = conn
                .prepare_single("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
                .unwrap();
            stmt.execute(None).unwrap();

            let mut stmt = conn
                .prepare_single("INSERT INTO test (id, value) VALUES (1, 'secret_data')")
                .unwrap();
            stmt.execute(None).unwrap();

            // Checkpoint to ensure data is written to main db file
            let mut stmt = conn
                .prepare_single("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
            stmt.execute(None).unwrap();
        }

        // 2. Verify data is encrypted on disk
        let content = std::fs::read(db_path).unwrap();
        assert!(content.len() > 1024);
        assert!(
            !content.windows(11).any(|w| w == b"secret_data"),
            "Plaintext should not appear in encrypted database file"
        );

        // 3. Reopen with correct key and verify data
        {
            let db = TursoDatabase::new(TursoDatabaseConfig {
                path: db_path.to_string(),
                experimental_features: Some("encryption".to_string()),
                async_io: false,
                encryption: Some(create_encryption_opts()),
                vfs: IoBackend::Default,
                io: None,
                db_file: None,
                page_codec: None,
                open_flags: OpenFlags::default(),
            });
            let result = db.open().unwrap();
            assert!(!result.is_io());
            let conn = db.connect().unwrap();

            let mut stmt = conn
                .prepare_single("SELECT id, value FROM test WHERE id = 1")
                .unwrap();
            assert_eq!(stmt.step(None).unwrap(), TursoStatusCode::Row);
            assert_integer(stmt.row_value(0).unwrap(), 1);
            assert_eq!(stmt.row_value(1).unwrap().to_text(), Some("secret_data"));
        }

        // 4. Verify opening with wrong key fails
        {
            let db = TursoDatabase::new(TursoDatabaseConfig {
                path: db_path.to_string(),
                experimental_features: Some("encryption".to_string()),
                async_io: false,
                encryption: Some(crate::rsapi::EncryptionOpts {
                    cipher: TEST_CIPHER.to_string(),
                    hexkey: WRONG_HEXKEY.to_string(),
                }),
                vfs: IoBackend::Default,
                io: None,
                db_file: None,
                page_codec: None,
                open_flags: OpenFlags::default(),
            });
            assert!(db.open().is_err(), "Opening with wrong key should fail");
        }

        // 5. Verify opening without encryption fails
        {
            let db = TursoDatabase::new(TursoDatabaseConfig {
                path: db_path.to_string(),
                experimental_features: Some("encryption".to_string()),
                async_io: false,
                encryption: None,
                vfs: IoBackend::Default,
                io: None,
                db_file: None,
                page_codec: None,
                open_flags: OpenFlags::default(),
            });
            let result = db.open();
            println!("result: {result:?}");
            assert!(
                result.is_err(),
                "Opening encrypted database without key should fail"
            );
        }
    }
}

/// Reproducer: stale DATABASE_MANAGER entry when old TursoDatabase/TursoConnection
/// haven't been GC'd (dropped) before reopening at the same path.
///
/// Steps (mirrors the React Native bug report):
///   1. Open database A via SDK, create table "cache", close connection
///   2. Copy A.db → B.db, delete A.db
///   3. Open a *new* database at path A.db — while old db_a/conn_a still alive
///   4. CREATE TABLE cache should succeed (A.db is fresh) but fails with
///      "table cache already exists" because the registry returned the stale Database
#[test]
pub fn test_stale_registry_with_live_sdk_handles() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path_a = tmp_dir.path().join("A.db");
    let path_b = tmp_dir.path().join("B.db");

    // 1. Open database A via SDK and create a table.
    let db_a = TursoDatabase::new(TursoDatabaseConfig {
        path: path_a.to_str().unwrap().to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let _ = db_a.open().unwrap();
    let conn_a = db_a.connect().unwrap();

    let mut stmt = conn_a
        .prepare_single("CREATE TABLE cache(x INTEGER)")
        .unwrap();
    assert_eq!(stmt.execute(None).unwrap().status, TursoStatusCode::Done);
    drop(stmt);

    // Close the connection but do NOT drop conn_a or db_a — simulates
    // the JS GC not having collected them yet.
    conn_a.close().unwrap();

    // 2. Copy A.db → B.db, then delete A.db (and WAL/SHM files).
    std::fs::copy(&path_a, &path_b).unwrap();
    std::fs::remove_file(&path_a).unwrap();
    for ext in &["-wal", "-shm"] {
        let src = tmp_dir.path().join(format!("A.db{ext}"));
        let dst = tmp_dir.path().join(format!("B.db{ext}"));
        if src.exists() {
            std::fs::copy(&src, &dst).unwrap();
            std::fs::remove_file(&src).unwrap();
        }
    }

    // 3. Open a new database at the same path A.db.
    //    The old db_a and conn_a are still alive — this is the key difference
    //    from test_sdk_close_finalizes_leaked_statements which drops everything.
    let db_a2 = TursoDatabase::new(TursoDatabaseConfig {
        path: path_a.to_str().unwrap().to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let _ = db_a2.open().unwrap();
    let conn_a2 = db_a2.connect().unwrap();

    // 4. A.db should be a fresh empty database — CREATE TABLE cache must succeed.
    let mut stmt2 = conn_a2
        .prepare_single("CREATE TABLE cache(x INTEGER)")
        .expect("prepare should succeed on fresh database");
    let result = stmt2.execute(None);
    assert_eq!(
        result.unwrap().status,
        TursoStatusCode::Done,
        "CREATE TABLE cache on a fresh A.db should succeed — \
             stale DATABASE_MANAGER entry returned the old Database"
    );

    // Cleanup: drop old handles (simulates eventual GC).
    drop(conn_a);
    drop(db_a);
}

/// Regression test: connection.close() must finalize all outstanding statements
/// to break the Statement → Arc<Connection> → Arc<Database> chain that keeps the
/// database alive in DATABASE_MANAGER after a file rename.
#[test]
pub fn test_close_finalizes_outstanding_statements() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();

    // Create a statement but do NOT finalize or drop it
    let mut stmt = conn.prepare_single("SELECT 1").unwrap();
    assert_eq!(stmt.step(None).unwrap(), TursoStatusCode::Row);

    // close() should finalize the outstanding statement
    conn.close().unwrap();

    // The statement should now be finalized — using it returns an error
    let result = stmt.step(None);
    assert!(result.is_err());
    match result.unwrap_err() {
        TursoError::Misuse(msg) => assert_eq!(msg, FINALIZED_ERR),
        other => panic!("expected Misuse error, got: {other:?}"),
    }
}

/// Test that finalize() sets the statement handle to None, making subsequent
/// operations return "statement has been finalized".
#[test]
pub fn test_finalize_disposes_statement() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());

    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare_single("SELECT 1").unwrap();

    // Finalize the statement
    assert_eq!(stmt.finalize(None).unwrap(), TursoStatusCode::Done);

    // All operations should now return "statement has been finalized"
    assert!(stmt.step(None).is_err());
    assert!(stmt.execute(None).is_err());
    assert!(stmt.reset().is_err());
    assert!(stmt.run_io().is_err());
    assert!(stmt.bind_positional(1, Value::Null).is_err());
    assert_eq!(stmt.n_change(), 0);
    assert_eq!(stmt.column_count(), 0);
    assert_eq!(stmt.parameters_count(), 0);
}
