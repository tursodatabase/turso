use crate::helpers::wrapper::Wrapper;

pub use super::connection::RustConnection;
use std::sync::Arc;

pub enum OpenFlags {
    ReadOnly,
    ReadWrite,
    Create,
}

/// Supported encryption ciphers for local database encryption.
pub enum EncryptionCipher {
    Aes128Gcm,
    Aes256Gcm,
    Aegis256,
    Aegis256x2,
    Aegis128l,
    Aegis128x2,
    Aegis128x4,
}

impl EncryptionCipher {
    fn as_str(&self) -> &'static str {
        match self {
            EncryptionCipher::Aes128Gcm => "aes128gcm",
            EncryptionCipher::Aes256Gcm => "aes256gcm",
            EncryptionCipher::Aegis256 => "aegis256",
            EncryptionCipher::Aegis256x2 => "aegis256x2",
            EncryptionCipher::Aegis128l => "aegis128l",
            EncryptionCipher::Aegis128x2 => "aegis128x2",
            EncryptionCipher::Aegis128x4 => "aegis128x4",
        }
    }
}

/// Encryption options for local database encryption.
pub struct EncryptionOpts {
    /// The cipher to use for encryption
    pub cipher: EncryptionCipher,
    /// The hex-encoded encryption key
    pub hexkey: String,
}

pub struct ConnectArgs {
    pub url: String,
    pub auth_token: Option<String>,
    pub sync_url: Option<String>,
    pub sync_interval_seconds: Option<u64>,
    pub encryption_key: Option<String>,
    pub read_your_writes: Option<bool>,
    pub open_flags: Option<OpenFlags>,
    pub offline: Option<bool>,
    /// Optional encryption options for local database encryption
    pub encryption: Option<EncryptionOpts>,
}

pub async fn connect(args: ConnectArgs) -> RustConnection {
    let database = if args.url == ":memory:" {
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
        turso_core::Database::open_file(io, args.url.as_str())
    } else {
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

        // Handle encryption options
        let encryption_opts = args.encryption.map(|enc| turso_core::EncryptionOpts {
            cipher: enc.cipher.as_str().to_string(),
            hexkey: enc.hexkey,
        });

        let db_opts = if encryption_opts.is_some() {
            turso_core::DatabaseOpts::new().with_encryption(true)
        } else {
            turso_core::DatabaseOpts::new()
        };

        turso_core::Database::open_file_with_flags(
            io,
            args.url.as_str(),
            turso_core::OpenFlags::Create,
            db_opts,
            encryption_opts,
        )
    }
    .unwrap();
    let connection = database.connect().unwrap();
    RustConnection::new(Wrapper { inner: connection })
}
