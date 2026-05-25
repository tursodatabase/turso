use std::sync::Arc;

use tempfile::NamedTempFile;
use turso_core::{
    Database, DatabaseOpts, EncryptionKey, EncryptionOpts, OpenFlags, PlatformIO, IO,
};
use turso_dbhash::{hash_database, DbHashOptions};

const CIPHER: &str = "aegis256";
const HEXKEY: &str = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

fn encryption_opts() -> EncryptionOpts {
    EncryptionOpts {
        cipher: CIPHER.to_string(),
        hexkey: HEXKEY.to_string(),
    }
}

fn create_test_db(encryption: Option<EncryptionOpts>) -> NamedTempFile {
    let file = NamedTempFile::new().expect("create temp db");
    let path = file.path().to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().expect("create platform io"));
    let database_opts = if encryption.is_some() {
        DatabaseOpts::new().with_encryption(true)
    } else {
        DatabaseOpts::new()
    };
    let encryption_key = encryption
        .as_ref()
        .map(|_| EncryptionKey::from_hex_string(HEXKEY).expect("parse hexkey"));
    let db = Database::open_file_with_flags(
        io.clone(),
        path,
        OpenFlags::Create,
        database_opts,
        encryption,
    )
    .expect("open db");
    let conn = db
        .connect_with_encryption(encryption_key)
        .expect("connect db");

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t(value) VALUES ('alpha'), ('beta')")
        .expect("insert rows");
    for completion in conn.cacheflush().expect("flush db") {
        io.wait_for_completion(completion).expect("wait for flush");
    }

    file
}

#[test]
fn hashes_encrypted_database_with_cipher_and_hexkey() {
    let plain_db = create_test_db(None);
    let encrypted_db = create_test_db(Some(encryption_opts()));
    let options = DbHashOptions {
        encryption: Some(encryption_opts()),
        ..Default::default()
    };

    let plain_result =
        hash_database(plain_db.path().to_str().unwrap(), &Default::default()).expect("hash plain");
    let encrypted_result = hash_database(encrypted_db.path().to_str().unwrap(), &options)
        .expect("hash encrypted database");

    assert_eq!(encrypted_result.hash, plain_result.hash);
    assert_eq!(encrypted_result.tables_hashed, 1);
    assert_eq!(encrypted_result.rows_hashed, 2);
}
