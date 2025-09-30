#![allow(unused_variables, dead_code)]
use crate::{LimboError, Result};
use aegis::aegis128l::Aegis128L;
use aegis::aegis128x2::Aegis128X2;
use aegis::aegis128x4::Aegis128X4;
use aegis::aegis256::Aegis256;
use aegis::aegis256x2::Aegis256X2;
use aegis::aegis256x4::Aegis256X4;
use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes128Gcm, Aes256Gcm, Key, Nonce,
};
use turso_macros::match_ignore_ascii_case;

/// Encryption Scheme
/// We support two major algorithms: AEGIS, AES GCM. These algorithms picked so that they also do
/// verification of the ciphertext, so we don't need to implement. That is if the page is corrupted
/// (or tampered), then we will know if we got garbage bytes post decryption.
///
/// We perform encryption at the page level, i.e., each page is encrypted and decrypted individually.
/// We store the nonce and tag (or the verification bits) in the page itself.  We also generate a
/// random nonce every time we encrypt a page.
///
/// Example: Assume the page size is 4096 bytes and we use AEGIS 256. So we reserve the last 48 bytes
/// for the nonce (32 bytes) and tag (16 bytes).
///
/// ```ignore
///             Unencrypted Page              Encrypted Page
///             ┌───────────────┐            ┌───────────────┐
///             │               │            │               │
///             │ Page Content  │            │   Encrypted   │
///             │ (4048 bytes)  │  ────────► │    Content    │
///             │               │            │ (4048 bytes)  │
///             ├───────────────┤            ├───────────────┤
///             │   Reserved    │            │    Tag (32)   │
///             │  (48 bytes)   │            ├───────────────┤
///             │   [empty]     │            │   Nonce (12)  │
///             └───────────────┘            └───────────────┘
///                4096 bytes                   4096 bytes
/// ```
///
/// The above applies to all the pages except Page 1. The page 1 contains the SQLite header (the
/// first 100 bytes). Specifically, the bytes 16 to 24 contain metadata which is required to
/// initialise the connection, which happens before we can setup the encryption context. So, we
/// don't encrypt the header but instead use the header data as additional data (AD) for the
/// encryption of the rest of the page. This provides us protection against tampering and
/// corruption for the unencrypted portion.
///
/// On disk, the encrypted page 1 contains special bytes replacing the SQLite's magic bytes (the
/// first 16 bytes):
///
/// ```ignore
///                    Turso Header (16 bytes)
///        ┌─────────┬───────┬────────┬──────────────────┐
///        │         │       │        │                  │
///        │  Turso  │Version│ Cipher │     Unused       │
///        │  (5)    │ (1)   │  (1)   │    (9 bytes)     │
///        │         │       │        │                  │
///        └─────────┴───────┴────────┴──────────────────┘
///         0-4      5       6        7-15
///
///        Standard SQLite Header: "SQLite format 3\0" (16 bytes)
///                            ↓
///        Turso Encrypted Header: "Turso" + Version + Cipher ID + Unused
/// ```
///
/// constants used for the Turso page header in the encrypted dbs.
const TURSO_HEADER_PREFIX: &[u8] = b"Turso";
const TURSO_VERSION: u8 = 0x00;
const VERSION_OFFSET: usize = 5;
const CIPHER_OFFSET: usize = 6;
const TURSO_HEADER_SIZE: usize = 16;
const SQLITE_HEADER: &[u8] = b"SQLite format 3\0";

#[derive(Clone)]
pub enum EncryptionKey {
    Key128([u8; 16]),
    Key256([u8; 32]),
}

impl EncryptionKey {
    pub fn new_256(key: [u8; 32]) -> Self {
        Self::Key256(key)
    }

    pub fn new_128(key: [u8; 16]) -> Self {
        Self::Key128(key)
    }

    pub fn from_hex_string(s: &str) -> Result<Self> {
        let hex_str = s.trim();
        let bytes = hex::decode(hex_str)
            .map_err(|e| LimboError::InvalidArgument(format!("Invalid hex string: {e}")))?;

        match bytes.len() {
            16 => {
                let key: [u8; 16] = bytes.try_into().unwrap();
                Ok(Self::Key128(key))
            }
            32 => {
                let key: [u8; 32] = bytes.try_into().unwrap();
                Ok(Self::Key256(key))
            }
            _ => Err(LimboError::InvalidArgument(format!(
                "Hex string must decode to exactly 16 or 32 bytes, got {}",
                bytes.len()
            ))),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Key128(key) => key,
            Self::Key256(key) => key,
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        match self {
            Self::Key128(_) => 16,
            Self::Key256(_) => 32,
        }
    }

    pub fn as_128(&self) -> Option<&[u8; 16]> {
        match self {
            Self::Key128(key) => Some(key),
            _ => None,
        }
    }

    pub fn as_256(&self) -> Option<&[u8; 32]> {
        match self {
            Self::Key256(key) => Some(key),
            _ => None,
        }
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKey")
            .field("key", &"<encryption key redacted>")
            .finish()
    }
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        // securely zero out the key bytes before dropping
        match self {
            Self::Key128(key) => {
                for byte in key.iter_mut() {
                    unsafe {
                        std::ptr::write_volatile(byte, 0);
                    }
                }
            }
            Self::Key256(key) => {
                for byte in key.iter_mut() {
                    unsafe {
                        std::ptr::write_volatile(byte, 0);
                    }
                }
            }
        }
    }
}

macro_rules! define_aegis_cipher {
    ($struct_name:ident, $cipher_type:ty, key128, $nonce_size:literal, $name:literal) => {
        define_aegis_cipher!(@impl $struct_name, $cipher_type, $nonce_size, $name, 16, as_128);
    };
    ($struct_name:ident, $cipher_type:ty, key256, $nonce_size:literal, $name:literal) => {
        define_aegis_cipher!(@impl $struct_name, $cipher_type, $nonce_size, $name, 32, as_256);
    };
    (@impl $struct_name:ident, $cipher_type:ty, $nonce_size:literal, $name:literal, $key_size:literal, $key_method:ident) => {
        #[derive(Clone)]
        pub struct $struct_name {
            key: EncryptionKey,
        }

        impl $struct_name {
            const TAG_SIZE: usize = 16;

            fn new(key: &EncryptionKey) -> Self {
                Self { key: key.clone() }
            }

            fn encrypt(&self, plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, [u8; $nonce_size])> {
                let nonce = generate_secure_nonce::<$nonce_size>();
                let key_bytes = self.key.$key_method()
                    .ok_or_else(|| -> LimboError { CipherError::InvalidKeySize { cipher: $name, expected: $key_size }.into() })?;
                let (ciphertext, tag) = <$cipher_type>::new(key_bytes, &nonce).encrypt(plaintext, ad);
                let mut result = ciphertext;
                result.extend_from_slice(&tag);
                Ok((result, nonce))
            }

            fn decrypt(&self, ciphertext: &[u8], nonce: &[u8; $nonce_size], ad: &[u8]) -> Result<Vec<u8>> {
                if ciphertext.len() < Self::TAG_SIZE {
                    return Err(LimboError::from(CipherError::CiphertextTooShort { cipher: $name }));
                }
                let (ct, tag) = ciphertext.split_at(ciphertext.len() - Self::TAG_SIZE);
                let tag_array: [u8; 16] = tag.try_into().map_err(|_| -> LimboError { CipherError::InvalidTagSize { cipher: $name }.into() })?;

                let key_bytes = self.key.$key_method()
                    .ok_or_else(|| -> LimboError { CipherError::InvalidKeySize { cipher: $name, expected: $key_size }.into() })?;
                <$cipher_type>::new(key_bytes, nonce)
                    .decrypt(ct, &tag_array, ad)
                    .map_err(|_| -> LimboError { CipherError::DecryptionFailed { cipher: $name }.into() })
            }
        }

        impl std::fmt::Debug for $struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($struct_name))
                    .field("key", &"<redacted>")
                    .finish()
            }
        }
    };
}

macro_rules! define_aes_gcm_cipher {
    ($struct_name:ident, $cipher_type:ty, key128, $name:literal) => {
        define_aes_gcm_cipher!(@impl $struct_name, $cipher_type, $name, 16, as_128);
    };
    ($struct_name:ident, $cipher_type:ty, key256, $name:literal) => {
        define_aes_gcm_cipher!(@impl $struct_name, $cipher_type, $name, 32, as_256);
    };
    (@impl $struct_name:ident, $cipher_type:ty, $name:literal, $key_size:literal, $key_method:ident) => {
        #[derive(Clone)]
        pub struct $struct_name {
            cipher: $cipher_type,
        }

        impl $struct_name {
            const TAG_SIZE: usize = 16;
            const NONCE_SIZE: usize = 12;

            fn new(key: &EncryptionKey) -> Result<Self> {
                let key_bytes = key.$key_method()
                    .ok_or_else(|| -> LimboError { CipherError::InvalidKeySize { cipher: $name, expected: $key_size }.into() })?;
                let cipher_key: &Key<$cipher_type> = key_bytes.into();
                Ok(Self {
                    cipher: <$cipher_type>::new(cipher_key),
                })
            }

            fn encrypt(&self, plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, [u8; 12])> {
                let nonce = <$cipher_type>::generate_nonce(&mut OsRng);
                let ciphertext = self.cipher.encrypt(&nonce, aes_gcm::aead::Payload {
                    msg: plaintext,
                    aad: ad,
                }).map_err(|e| {
                    LimboError::InternalError(format!("{} encryption failed: {e:?}", $name))
                })?;
                let mut nonce_array = [0u8; 12];
                nonce_array.copy_from_slice(&nonce);
                Ok((ciphertext, nonce_array))
            }

            fn decrypt(&self, ciphertext: &[u8], nonce: &[u8; 12], ad: &[u8]) -> Result<Vec<u8>> {
                let nonce = Nonce::from_slice(nonce);
                self.cipher
                    .decrypt(nonce, aes_gcm::aead::Payload {
                        msg: ciphertext,
                        aad: ad,
                    })
                    .map_err(|_| -> LimboError { CipherError::DecryptionFailed { cipher: $name }.into() })
            }
        }

        impl std::fmt::Debug for $struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($struct_name))
                    .field("key", &"<redacted>")
                    .finish()
            }
        }
    };
}

// AES-GCM ciphers
define_aes_gcm_cipher!(Aes128GcmCipher, Aes128Gcm, key128, "AES-128-GCM");
define_aes_gcm_cipher!(Aes256GcmCipher, Aes256Gcm, key256, "AES-256-GCM");

// AEGIS ciphers
define_aegis_cipher!(Aegis256Cipher, Aegis256::<16>, key256, 32, "AEGIS-256");
define_aegis_cipher!(
    Aegis256X2Cipher,
    Aegis256X2::<16>,
    key256,
    32,
    "AEGIS-256X2"
);
define_aegis_cipher!(
    Aegis256X4Cipher,
    Aegis256X4::<16>,
    key256,
    32,
    "AEGIS-256X4"
);
define_aegis_cipher!(
    Aegis128X2Cipher,
    Aegis128X2::<16>,
    key128,
    16,
    "AEGIS-128X2"
);
define_aegis_cipher!(Aegis128LCipher, Aegis128L::<16>, key128, 16, "AEGIS-128L");
define_aegis_cipher!(
    Aegis128X4Cipher,
    Aegis128X4::<16>,
    key128,
    16,
    "AEGIS-128X4"
);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CipherMode {
    Aes128Gcm,
    Aes256Gcm,
    Aegis256,
    Aegis128L,
    Aegis128X2,
    Aegis128X4,
    Aegis256X2,
    Aegis256X4,
}

impl TryFrom<&str> for CipherMode {
    type Error = LimboError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let s_bytes = s.as_bytes();
        match_ignore_ascii_case!(match s_bytes {
            b"aes128gcm" | b"aes-128-gcm" | b"aes_128_gcm" => Ok(CipherMode::Aes128Gcm),
            b"aes256gcm" | b"aes-256-gcm" | b"aes_256_gcm" => Ok(CipherMode::Aes256Gcm),
            b"aegis256" | b"aegis-256" | b"aegis_256" => Ok(CipherMode::Aegis256),
            b"aegis128l" | b"aegis-128l" | b"aegis_128l" => Ok(CipherMode::Aegis128L),
            b"aegis128x2" | b"aegis-128x2" | b"aegis_128x2" => Ok(CipherMode::Aegis128X2),
            b"aegis128x4" | b"aegis-128x4" | b"aegis_128x4" => Ok(CipherMode::Aegis128X4),
            b"aegis256x2" | b"aegis-256x2" | b"aegis_256x2" => Ok(CipherMode::Aegis256X2),
            b"aegis256x4" | b"aegis-256x4" | b"aegis_256x4" => Ok(CipherMode::Aegis256X4),
            _ => Err(LimboError::InvalidArgument(format!(
                "Unknown cipher name: {s}"
            ))),
        })
    }
}

impl std::fmt::Display for CipherMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CipherMode::Aes128Gcm => write!(f, "aes128gcm"),
            CipherMode::Aes256Gcm => write!(f, "aes256gcm"),
            CipherMode::Aegis256 => write!(f, "aegis256"),
            CipherMode::Aegis128L => write!(f, "aegis128l"),
            CipherMode::Aegis128X2 => write!(f, "aegis128x2"),
            CipherMode::Aegis128X4 => write!(f, "aegis128x4"),
            CipherMode::Aegis256X2 => write!(f, "aegis256x2"),
            CipherMode::Aegis256X4 => write!(f, "aegis256x4"),
        }
    }
}

impl CipherMode {
    /// Every cipher requires a specific key size. For 256-bit algorithms, this is 32 bytes.
    /// For 128-bit algorithms, it would be 16 bytes, etc.
    pub fn required_key_size(&self) -> usize {
        match self {
            CipherMode::Aes128Gcm => 16,
            CipherMode::Aes256Gcm => 32,
            CipherMode::Aegis256 => 32,
            CipherMode::Aegis256X2 => 32,
            CipherMode::Aegis256X4 => 32,
            CipherMode::Aegis128L => 16,
            CipherMode::Aegis128X2 => 16,
            CipherMode::Aegis128X4 => 16,
        }
    }

    /// Returns the nonce size for this cipher mode.
    pub fn nonce_size(&self) -> usize {
        match self {
            CipherMode::Aes128Gcm => 12,
            CipherMode::Aes256Gcm => 12,
            CipherMode::Aegis256 => 32,
            CipherMode::Aegis256X2 => 32,
            CipherMode::Aegis256X4 => 32,
            CipherMode::Aegis128L => 16,
            CipherMode::Aegis128X2 => 16,
            CipherMode::Aegis128X4 => 16,
        }
    }

    /// Returns the authentication tag size for this cipher mode.
    pub fn tag_size(&self) -> usize {
        match self {
            CipherMode::Aes128Gcm => 16,
            CipherMode::Aes256Gcm => 16,
            CipherMode::Aegis256 => 16,
            CipherMode::Aegis256X2 => 16,
            CipherMode::Aegis256X4 => 16,
            CipherMode::Aegis128L => 16,
            CipherMode::Aegis128X2 => 16,
            CipherMode::Aegis128X4 => 16,
        }
    }

    /// Returns the total metadata size (nonce + tag) for this cipher mode.
    pub fn metadata_size(&self) -> usize {
        self.nonce_size() + self.tag_size()
    }

    /// Returns the cipher identifier byte for Turso header
    pub fn cipher_id(&self) -> u8 {
        match self {
            CipherMode::Aes128Gcm => 1,
            CipherMode::Aes256Gcm => 2,
            CipherMode::Aegis256 => 3,
            CipherMode::Aegis256X2 => 4,
            CipherMode::Aegis256X4 => 5,
            CipherMode::Aegis128L => 6,
            CipherMode::Aegis128X2 => 7,
            CipherMode::Aegis128X4 => 8,
        }
    }

    /// Creates a CipherMode from cipher identifier byte. This is used when read from Turso header.
    pub fn from_cipher_id(id: u8) -> Result<Self> {
        match id {
            1 => Ok(CipherMode::Aes128Gcm),
            2 => Ok(CipherMode::Aes256Gcm),
            3 => Ok(CipherMode::Aegis256),
            4 => Ok(CipherMode::Aegis256X2),
            5 => Ok(CipherMode::Aegis256X4),
            6 => Ok(CipherMode::Aegis128L),
            7 => Ok(CipherMode::Aegis128X2),
            8 => Ok(CipherMode::Aegis128X4),
            _ => Err(LimboError::InvalidArgument(format!(
                "Unknown cipher ID: {id}"
            ))),
        }
    }
}

#[derive(Clone)]
pub enum Cipher {
    Aes128Gcm(Box<Aes128GcmCipher>),
    Aes256Gcm(Box<Aes256GcmCipher>),
    Aegis256(Box<Aegis256Cipher>),
    Aegis256X2(Box<Aegis256X2Cipher>),
    Aegis256X4(Box<Aegis256X4Cipher>),
    Aegis128L(Box<Aegis128LCipher>),
    Aegis128X2(Box<Aegis128X2Cipher>),
    Aegis128X4(Box<Aegis128X4Cipher>),
}

impl std::fmt::Debug for Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cipher::Aes128Gcm(_) => write!(f, "Cipher::Aes128Gcm"),
            Cipher::Aes256Gcm(_) => write!(f, "Cipher::Aes256Gcm"),
            Cipher::Aegis256(_) => write!(f, "Cipher::Aegis256"),
            Cipher::Aegis256X2(_) => write!(f, "Cipher::Aegis256X2"),
            Cipher::Aegis256X4(_) => write!(f, "Cipher::Aegis256X4"),
            Cipher::Aegis128L(_) => write!(f, "Cipher::Aegis128L"),
            Cipher::Aegis128X2(_) => write!(f, "Cipher::Aegis128X2"),
            Cipher::Aegis128X4(_) => write!(f, "Cipher::Aegis128X4"),
        }
    }
}

#[derive(Clone)]
pub struct EncryptionContext {
    cipher_mode: CipherMode,
    cipher: Cipher,
    page_size: usize,
}

impl EncryptionContext {
    pub fn new(cipher_mode: CipherMode, key: &EncryptionKey, page_size: usize) -> Result<Self> {
        let required_size = cipher_mode.required_key_size();
        if key.len() != required_size {
            return Err(crate::LimboError::InvalidArgument(format!(
                "Invalid key size for {:?}: expected {} bytes, got {}",
                cipher_mode,
                required_size,
                key.len()
            )));
        }

        let cipher = match cipher_mode {
            CipherMode::Aes128Gcm => Cipher::Aes128Gcm(Box::new(Aes128GcmCipher::new(key)?)),
            CipherMode::Aes256Gcm => Cipher::Aes256Gcm(Box::new(Aes256GcmCipher::new(key)?)),
            CipherMode::Aegis256 => Cipher::Aegis256(Box::new(Aegis256Cipher::new(key))),
            CipherMode::Aegis256X2 => Cipher::Aegis256X2(Box::new(Aegis256X2Cipher::new(key))),
            CipherMode::Aegis256X4 => Cipher::Aegis256X4(Box::new(Aegis256X4Cipher::new(key))),
            CipherMode::Aegis128L => Cipher::Aegis128L(Box::new(Aegis128LCipher::new(key))),
            CipherMode::Aegis128X2 => Cipher::Aegis128X2(Box::new(Aegis128X2Cipher::new(key))),
            CipherMode::Aegis128X4 => Cipher::Aegis128X4(Box::new(Aegis128X4Cipher::new(key))),
        };
        Ok(Self {
            cipher_mode,
            cipher,
            page_size,
        })
    }

    pub fn cipher_mode(&self) -> CipherMode {
        self.cipher_mode
    }

    /// Returns the number of reserved bytes required at the end of each page for encryption metadata.
    pub fn required_reserved_bytes(&self) -> u8 {
        self.cipher_mode.metadata_size() as u8
    }

    /// Creates Turso header for encrypted page 1
    fn create_turso_header(&self) -> [u8; TURSO_HEADER_SIZE] {
        let mut header = [0u8; TURSO_HEADER_SIZE];

        // "Turso" prefix (5 bytes)
        header[..TURSO_HEADER_PREFIX.len()].copy_from_slice(TURSO_HEADER_PREFIX);

        // version byte (1 byte)
        header[VERSION_OFFSET] = TURSO_VERSION;

        // cipher identifier (1 byte)
        header[CIPHER_OFFSET] = self.cipher_mode.cipher_id();

        // remaining unused 9 bytes
        header
    }

    /// Validates and extracts cipher mode from Turso header
    fn validate_turso_header(&self, header: &[u8]) -> Result<()> {
        if header.len() < TURSO_HEADER_SIZE {
            return Err(LimboError::InternalError(
                "Header too short for encrypted Turso db".into(),
            ));
        }

        if &header[..TURSO_HEADER_PREFIX.len()] != TURSO_HEADER_PREFIX {
            return Err(LimboError::InternalError(
                "Invalid Turso header: prefix mismatch".into(),
            ));
        }

        let version = header[VERSION_OFFSET];
        if version != TURSO_VERSION {
            return Err(LimboError::InternalError(format!(
                "Unsupported Turso header version: expected {TURSO_VERSION}, got {version}"
            )));
        }

        let cipher_id = header[CIPHER_OFFSET];
        let header_cipher = CipherMode::from_cipher_id(cipher_id)?;
        if header_cipher != self.cipher_mode {
            return Err(LimboError::InternalError(format!(
                "Cipher mode mismatch: expected {:?} (ID {}), got {:?} (ID {})",
                self.cipher_mode,
                self.cipher_mode.cipher_id(),
                header_cipher,
                cipher_id
            )));
        }

        if header[CIPHER_OFFSET + 1..TURSO_HEADER_SIZE]
            .iter()
            .any(|&b| b != 0)
        {
            return Err(LimboError::InternalError(
                "Invalid Turso header: unused bytes must be zero".into(),
            ));
        }
        Ok(())
    }

    #[cfg(feature = "encryption")]
    pub fn encrypt_page(&self, page: &[u8], page_id: usize) -> Result<Vec<u8>> {
        use crate::storage::sqlite3_ondisk::DatabaseHeader;
        if page_id == DatabaseHeader::PAGE_ID {
            return self.encrypt_page_1(page);
        }
        tracing::debug!("encrypting page {}", page_id);
        assert_eq!(
            page.len(),
            self.page_size,
            "Page data must be exactly {} bytes",
            self.page_size
        );

        let metadata_size = self.cipher_mode.metadata_size();
        let reserved_bytes = &page[self.page_size - metadata_size..];

        #[cfg(debug_assertions)]
        {
            use crate::turso_assert;
            let reserved_bytes_zeroed = reserved_bytes.iter().all(|&b| b == 0);
            turso_assert!(
                reserved_bytes_zeroed,
                "last reserved bytes must be empty/zero, but found non-zero bytes"
            );
        }

        let payload = &page[..self.page_size - metadata_size];
        let (encrypted, nonce) = self.encrypt_raw(payload)?;

        let nonce_size = self.cipher_mode.nonce_size();
        assert_eq!(
            encrypted.len(),
            self.page_size - nonce_size,
            "Encrypted page must be exactly {} bytes",
            self.page_size - nonce_size
        );

        let mut result = Vec::with_capacity(self.page_size);
        result.extend_from_slice(&encrypted);
        result.extend_from_slice(&nonce);
        assert_eq!(
            result.len(),
            self.page_size,
            "Encrypted page must be exactly {} bytes",
            self.page_size
        );
        Ok(result)
    }

    #[cfg(feature = "encryption")]
    pub fn decrypt_page(&self, encrypted_page: &[u8], page_id: usize) -> Result<Vec<u8>> {
        use crate::storage::sqlite3_ondisk::DatabaseHeader;
        if page_id == DatabaseHeader::PAGE_ID {
            return self.decrypt_page_1(encrypted_page);
        }
        tracing::debug!("decrypting page {}", page_id);
        assert_eq!(
            encrypted_page.len(),
            self.page_size,
            "Encrypted page data must be exactly {} bytes",
            self.page_size
        );

        let nonce_size = self.cipher_mode.nonce_size();
        let nonce_offset = encrypted_page.len() - nonce_size;
        let payload = &encrypted_page[..nonce_offset];
        let nonce = &encrypted_page[nonce_offset..];

        let decrypted_data = self.decrypt_raw(payload, nonce)?;
        let metadata_size = self.cipher_mode.metadata_size();
        assert_eq!(
            decrypted_data.len(),
            self.page_size - metadata_size,
            "Decrypted page data must be exactly {} bytes",
            self.page_size - metadata_size
        );

        let mut result = Vec::with_capacity(self.page_size);
        result.extend_from_slice(&decrypted_data);
        result.resize(self.page_size, 0);

        assert_eq!(
            result.len(),
            self.page_size,
            "Decrypted page data must be exactly {} bytes",
            self.page_size
        );
        Ok(result)
    }

    #[cfg(feature = "encryption")]
    fn encrypt_page_1(&self, page: &[u8]) -> Result<Vec<u8>> {
        use crate::storage::sqlite3_ondisk::DatabaseHeader;

        tracing::debug!("encrypting page 1");
        assert_eq!(
            page.len(),
            self.page_size,
            "Page data must be exactly {} bytes",
            self.page_size
        );

        // since this is page 1, this must have header
        crate::turso_assert!(
            page.starts_with(SQLITE_HEADER),
            "Page 1 must start with SQLite header"
        );

        let metadata_size = self.cipher_mode.metadata_size();
        let reserved_bytes = &page[self.page_size - metadata_size..];

        #[cfg(debug_assertions)]
        {
            use crate::turso_assert;
            // In debug builds, ensure that the reserved bytes are zeroed out. So even when we are
            // reusing a page from buffer pool, we zero out in debug build so that we can be
            // sure that b tree layer is not writing any data into the reserved space.
            // We avoid calling `memset` in release builds for performance reasons.
            let reserved_bytes_zeroed = reserved_bytes.iter().all(|&b| b == 0);
            turso_assert!(
                reserved_bytes_zeroed,
                "last reserved bytes must be empty/zero, but found non-zero bytes"
            );
        }

        // page 1 encryption:
        // 1. First 16 bytes are replaced with Turso magic bytes
        // 2. Next 84 bytes (16-100) are kept as-is (not encrypted)
        // 3. Remaining bytes (100-end) are encrypted
        // 4. The header (the first 100 bytes) as associated data
        let turso_header = self.create_turso_header();
        let mut new_header = Vec::with_capacity(DatabaseHeader::SIZE);
        new_header.extend_from_slice(&turso_header);
        new_header.extend_from_slice(&page[TURSO_HEADER_SIZE..DatabaseHeader::SIZE]);

        let payload = &page[DatabaseHeader::SIZE..self.page_size - metadata_size];
        let (encrypted, nonce) = self.encrypt_raw_with_ad(payload, &new_header)?;

        let nonce_size = self.cipher_mode.nonce_size();
        assert_eq!(
            encrypted.len(),
            self.page_size - nonce_size - DatabaseHeader::SIZE,
            "Encrypted page must be exactly {} bytes",
            self.page_size - nonce_size - DatabaseHeader::SIZE
        );

        let mut result = Vec::with_capacity(self.page_size);

        // 1. copy the header
        result.extend_from_slice(&new_header);
        // 2. copy the encrypted payload
        result.extend_from_slice(&encrypted);
        // 3. now add the nonce
        result.extend_from_slice(&nonce);

        assert_eq!(
            result.len(),
            self.page_size,
            "Encrypted page must be exactly {} bytes",
            self.page_size
        );
        Ok(result)
    }

    #[cfg(feature = "encryption")]
    fn decrypt_page_1(&self, encrypted_page: &[u8]) -> Result<Vec<u8>> {
        use crate::storage::sqlite3_ondisk::DatabaseHeader;

        tracing::debug!("decrypting page 1");
        assert_eq!(
            encrypted_page.len(),
            self.page_size,
            "Encrypted page data must be exactly {} bytes",
            self.page_size
        );

        self.validate_turso_header(&encrypted_page[..TURSO_HEADER_SIZE])?;

        let nonce_size = self.cipher_mode.nonce_size();
        let nonce_offset = encrypted_page.len() - nonce_size;
        let payload = &encrypted_page[DatabaseHeader::SIZE..nonce_offset];
        let nonce = &encrypted_page[nonce_offset..];

        // it's important to use the header on disk (with Turso magic bytes) as associated data
        // for protection against tampering the header
        let header = &encrypted_page[..DatabaseHeader::SIZE];
        let decrypted_data = self.decrypt_raw_with_ad(payload, nonce, header)?;

        let metadata_size = self.cipher_mode.metadata_size();
        assert_eq!(
            decrypted_data.len(),
            self.page_size - metadata_size - DatabaseHeader::SIZE,
            "Decrypted page data must be exactly {} bytes",
            self.page_size - metadata_size - DatabaseHeader::SIZE
        );

        // reconstruct the page with the appropriate SQLite header
        let mut result = Vec::with_capacity(self.page_size);
        result.extend_from_slice(SQLITE_HEADER);
        result.extend_from_slice(&encrypted_page[TURSO_HEADER_SIZE..DatabaseHeader::SIZE]);
        result.extend_from_slice(&decrypted_data);
        result.resize(self.page_size, 0);

        assert_eq!(
            result.len(),
            self.page_size,
            "Decrypted page data must be exactly {} bytes",
            self.page_size
        );
        Ok(result)
    }

    /// encrypts raw data using the configured cipher, returns ciphertext and nonce
    fn encrypt_raw(&self, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        const AD: &[u8] = b"";
        self.encrypt_raw_with_ad(plaintext, AD)
    }

    /// encrypts raw data with associated data using the configured cipher
    fn encrypt_raw_with_ad(&self, plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        macro_rules! encrypt_cipher {
            ($cipher:expr) => {{
                let (ciphertext, nonce) = $cipher.encrypt(plaintext, ad)?;
                Ok((ciphertext, nonce.to_vec()))
            }};
        }

        match &self.cipher {
            Cipher::Aes128Gcm(cipher) => encrypt_cipher!(cipher),
            Cipher::Aes256Gcm(cipher) => encrypt_cipher!(cipher),
            Cipher::Aegis256(cipher) => encrypt_cipher!(cipher),
            Cipher::Aegis256X2(cipher) => encrypt_cipher!(cipher),
            Cipher::Aegis256X4(cipher) => encrypt_cipher!(cipher),
            Cipher::Aegis128L(cipher) => encrypt_cipher!(cipher),
            Cipher::Aegis128X2(cipher) => encrypt_cipher!(cipher),
            Cipher::Aegis128X4(cipher) => encrypt_cipher!(cipher),
        }
    }

    fn decrypt_raw(&self, ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        const AD: &[u8] = b"";
        self.decrypt_raw_with_ad(ciphertext, nonce, AD)
    }

    fn decrypt_raw_with_ad(&self, ciphertext: &[u8], nonce: &[u8], ad: &[u8]) -> Result<Vec<u8>> {
        macro_rules! decrypt_with_nonce {
            ($cipher:expr, $nonce_size:literal, $name:literal) => {{
                let nonce_array: [u8; $nonce_size] = nonce.try_into().map_err(|_| {
                    LimboError::InternalError(format!(
                        "Invalid nonce size for {}: expected {}, got {}",
                        $name,
                        $nonce_size,
                        nonce.len()
                    ))
                })?;
                $cipher.decrypt(ciphertext, &nonce_array, ad)
            }};
        }

        match &self.cipher {
            Cipher::Aes128Gcm(cipher) => decrypt_with_nonce!(cipher, 12, "AES-128-GCM"),
            Cipher::Aes256Gcm(cipher) => decrypt_with_nonce!(cipher, 12, "AES-256-GCM"),
            Cipher::Aegis256(cipher) => decrypt_with_nonce!(cipher, 32, "AEGIS-256"),
            Cipher::Aegis256X2(cipher) => decrypt_with_nonce!(cipher, 32, "AEGIS-256X2"),
            Cipher::Aegis256X4(cipher) => decrypt_with_nonce!(cipher, 32, "AEGIS-256X4"),
            Cipher::Aegis128L(cipher) => decrypt_with_nonce!(cipher, 16, "AEGIS-128L"),
            Cipher::Aegis128X2(cipher) => decrypt_with_nonce!(cipher, 16, "AEGIS-128X2"),
            Cipher::Aegis128X4(cipher) => decrypt_with_nonce!(cipher, 16, "AEGIS-128X4"),
        }
    }

    #[cfg(not(feature = "encryption"))]
    pub fn encrypt_page(&self, _page: &[u8], _page_id: usize) -> Result<Vec<u8>> {
        Err(LimboError::InvalidArgument(
            "encryption is not enabled, cannot encrypt page. enable via passing `--features encryption`".into(),
        ))
    }

    #[cfg(not(feature = "encryption"))]
    pub fn decrypt_page(&self, _encrypted_page: &[u8], _page_id: usize) -> Result<Vec<u8>> {
        Err(LimboError::InvalidArgument(
            "encryption is not enabled, cannot decrypt page. enable via passing `--features encryption`".into(),
        ))
    }
}

fn generate_secure_nonce<const N: usize>() -> [u8; N] {
    // use OsRng directly to fill bytes, generic over nonce size
    use aes_gcm::aead::rand_core::RngCore;
    let mut nonce = [0u8; N];
    OsRng.fill_bytes(&mut nonce);
    nonce
}

// Helper functions for consistent error messages
enum CipherError {
    InvalidKeySize {
        cipher: &'static str,
        expected: usize,
    },
    InvalidTagSize {
        cipher: &'static str,
    },
    DecryptionFailed {
        cipher: &'static str,
    },
    CiphertextTooShort {
        cipher: &'static str,
    },
}

impl From<CipherError> for LimboError {
    fn from(err: CipherError) -> Self {
        let msg = match err {
            CipherError::InvalidKeySize { cipher, expected } => {
                format!("{cipher} requires {expected}-byte key")
            }
            CipherError::InvalidTagSize { cipher } => format!("Invalid tag size for {cipher}"),
            CipherError::DecryptionFailed { cipher } => {
                format!("{cipher} decryption failed: invalid tag")
            }
            CipherError::CiphertextTooShort { cipher } => {
                format!("Ciphertext too short for {cipher}")
            }
        };
        LimboError::InternalError(msg)
    }
}

#[cfg(test)]
#[cfg(feature = "encryption")]
mod tests {
    use super::*;
    use rand::Rng;
    const DEFAULT_ENCRYPTED_PAGE_SIZE: usize = 4096;

    macro_rules! test_cipher_wrapper {
        ($test_name:ident, $cipher_type:ty, $key_gen:expr, $nonce_size:literal, $message:literal) => {
            #[test]
            fn $test_name() {
                let key = EncryptionKey::from_hex_string(&$key_gen()).unwrap();
                let cipher = <$cipher_type>::new(&key);

                let plaintext = $message.as_bytes();
                let ad = b"additional data";

                let (ciphertext, nonce) = cipher.encrypt(plaintext, ad).unwrap();
                assert_eq!(nonce.len(), $nonce_size);
                assert_ne!(ciphertext[..plaintext.len()], plaintext[..]);

                let decrypted = cipher.decrypt(&ciphertext, &nonce, ad).unwrap();
                assert_eq!(decrypted, plaintext);
            }
        };
    }

    macro_rules! test_aes_cipher_wrapper {
        ($test_name:ident, $cipher_type:ty, $key_gen:expr, $nonce_size:literal, $message:literal) => {
            #[test]
            fn $test_name() {
                let key = EncryptionKey::from_hex_string(&$key_gen()).unwrap();
                let cipher = <$cipher_type>::new(&key).unwrap();

                let plaintext = $message.as_bytes();
                let ad = b"additional data";

                let (ciphertext, nonce) = cipher.encrypt(plaintext, ad).unwrap();
                assert_eq!(nonce.len(), $nonce_size);
                assert_ne!(ciphertext[..plaintext.len()], plaintext[..]);

                let decrypted = cipher.decrypt(&ciphertext, &nonce, ad).unwrap();
                assert_eq!(decrypted, plaintext);
            }
        };
    }

    macro_rules! test_raw_encryption {
        ($test_name:ident, $cipher_mode:expr, $key_gen:expr, $nonce_size:literal, $message:literal) => {
            #[test]
            fn $test_name() {
                let key = EncryptionKey::from_hex_string(&$key_gen()).unwrap();
                let ctx = EncryptionContext::new($cipher_mode, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
                    .unwrap();

                let plaintext = $message.as_bytes();
                let (ciphertext, nonce) = ctx.encrypt_raw(plaintext).unwrap();

                assert_eq!(nonce.len(), $nonce_size);
                assert_ne!(ciphertext[..plaintext.len()], plaintext[..]);

                let decrypted = ctx.decrypt_raw(&ciphertext, &nonce).unwrap();
                assert_eq!(decrypted, plaintext);
            }
        };
    }

    fn generate_random_hex_key() -> String {
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        hex::encode(bytes)
    }

    fn generate_random_hex_key_128() -> String {
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 16];
        rng.fill(&mut bytes);
        hex::encode(bytes)
    }

    fn create_test_page_1() -> Vec<u8> {
        let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
        page[..SQLITE_HEADER.len()].copy_from_slice(SQLITE_HEADER);
        let mut rng = rand::thread_rng();
        // 48 is the max reserved bytes we might need for metadata with any cipher
        rng.fill(&mut page[SQLITE_HEADER.len()..DEFAULT_ENCRYPTED_PAGE_SIZE - 48]);
        page
    }

    test_aes_cipher_wrapper!(
        test_aes128gcm_cipher_wrapper,
        Aes128GcmCipher,
        generate_random_hex_key_128,
        12,
        "Hello, AES-128-GCM!"
    );

    test_raw_encryption!(
        test_aes128gcm_raw_encryption,
        CipherMode::Aes128Gcm,
        generate_random_hex_key_128,
        12,
        "Hello, AES-128-GCM!"
    );

    #[test]
    fn test_page_1_encrypt_decrypt_round_trip_with_ad() {
        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_data = create_test_page_1();
        let encrypted = ctx.encrypt_page(&page_data, 1).unwrap();
        assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);

        // check that header is readable directly from disk (not encrypted)
        assert_eq!(&encrypted[..5], b"Turso");
        assert_eq!(encrypted[5], TURSO_VERSION);
        assert_eq!(encrypted[6], CipherMode::Aegis256.cipher_id());

        // header should be unencrypted, but data after DatabaseHeader::SIZE should be different
        assert_eq!(&encrypted[16..100], &page_data[16..100]); // header portion
        assert_ne!(&encrypted[100..200], &page_data[100..200]); // some encrypted portion

        // decrypt page 1
        let decrypted = ctx.decrypt_page(&encrypted, 1).unwrap();
        assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);

        // check that SQLite header was restored
        assert_eq!(&decrypted[..SQLITE_HEADER.len()], SQLITE_HEADER);
        assert_eq!(decrypted, page_data);
    }

    #[test]
    fn test_turso_header_validation() {
        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        // test cipher_id conversion
        assert_eq!(CipherMode::Aes128Gcm.cipher_id(), 1);
        assert_eq!(CipherMode::Aes256Gcm.cipher_id(), 2);
        assert_eq!(CipherMode::Aegis256.cipher_id(), 3);
        assert_eq!(CipherMode::Aegis128L.cipher_id(), 6);

        // test from_cipher_id conversion
        assert_eq!(
            CipherMode::from_cipher_id(1).unwrap(),
            CipherMode::Aes128Gcm
        );
        assert_eq!(CipherMode::from_cipher_id(3).unwrap(), CipherMode::Aegis256);
        assert!(CipherMode::from_cipher_id(99).is_err());

        // test header creation
        let header = ctx.create_turso_header();
        assert_eq!(&header[..5], b"Turso");
        assert_eq!(header[5], TURSO_VERSION);
        assert_eq!(header[6], 3); // AEGIS-256
        assert_eq!(&header[7..], &[0u8; 9]); // unused bytes are zero
    }

    #[test]
    fn test_invalid_turso_header_fails_decrypt() {
        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_data = create_test_page_1();
        let encrypted = ctx.encrypt_page(&page_data, 1).unwrap();

        // corrupt the header prefix
        let mut corrupted = encrypted.clone();
        corrupted[0] = b'V'; // make `Turso` to `Vurso`
        assert!(ctx.decrypt_page(&corrupted, 1).is_err());

        // test with wrong cipher ID
        let mut wrong_cipher = encrypted.clone();
        wrong_cipher[6] = 99; // invalid cipher ID
        assert!(ctx.decrypt_page(&wrong_cipher, 1).is_err());
    }

    #[test]
    fn test_associated_data_validation() {
        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_data = create_test_page_1();
        let encrypted = ctx.encrypt_page(&page_data, 1).unwrap();

        // modify a byte in the preserved header portion (bytes 16-100)
        let mut corrupted_ad = encrypted.clone();
        corrupted_ad[50] ^= 1; // flip one bit in the associated data portion

        // this should fail decryption because associated data doesn't match
        let decrypt_result = ctx.decrypt_page(&corrupted_ad, 1);
        assert!(
            decrypt_result.is_err(),
            "Decryption should fail with corrupted associated data"
        );
    }

    #[test]
    fn test_turso_header_corruption_detection() {
        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_data = create_test_page_1();
        let encrypted = ctx.encrypt_page(&page_data, 1).unwrap();

        let mut corrupted_turso_header = encrypted.clone();
        corrupted_turso_header[7] ^= 1;

        let decrypt_result = ctx.decrypt_page(&corrupted_turso_header, 1);
        assert!(
            decrypt_result.is_err(),
            "Decryption should fail with corrupted Turso header"
        );
    }

    #[test]
    fn test_aes128gcm_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let cipher_mode = CipherMode::Aes128Gcm;
        let metadata_size = cipher_mode.metadata_size();
        let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

        let page_data = {
            let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_hex_string(&generate_random_hex_key_128()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aes128Gcm, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_id = 42;
        let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
        assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);
        assert_ne!(&encrypted[..], &page_data[..]);

        let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
        assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }

    #[test]
    fn test_aes_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let cipher_mode = CipherMode::Aes256Gcm;
        let metadata_size = cipher_mode.metadata_size();
        let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

        let page_data = {
            let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aes256Gcm, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_id = 42;
        let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
        assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);
        assert_ne!(&encrypted[..], &page_data[..]);

        let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
        assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }

    test_cipher_wrapper!(
        test_aegis256_cipher_wrapper,
        Aegis256Cipher,
        generate_random_hex_key,
        32,
        "Hello, AEGIS-256!"
    );

    test_raw_encryption!(
        test_aegis256_raw_encryption,
        CipherMode::Aegis256,
        generate_random_hex_key,
        32,
        "Hello, AEGIS-256!"
    );

    #[test]
    fn test_aegis256_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let cipher_mode = CipherMode::Aegis256;
        let metadata_size = cipher_mode.metadata_size();
        let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

        let page_data = {
            let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_id = 42;
        let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
        assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

        let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
        assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }

    test_cipher_wrapper!(
        test_aegis128x2_cipher_wrapper,
        Aegis128X2Cipher,
        generate_random_hex_key_128,
        16,
        "Hello, AEGIS-128X2!"
    );

    test_raw_encryption!(
        test_aegis128x2_raw_encryption,
        CipherMode::Aegis128X2,
        generate_random_hex_key_128,
        16,
        "Hello, AEGIS-128X2!"
    );

    #[test]
    fn test_aegis128x2_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let cipher_mode = CipherMode::Aegis128X2;
        let metadata_size = cipher_mode.metadata_size();
        let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

        let page_data = {
            let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_hex_string(&generate_random_hex_key_128()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis128X2, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_id = 42;
        let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
        assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

        let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
        assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }

    test_cipher_wrapper!(
        test_aegis128l_cipher_wrapper,
        Aegis128LCipher,
        generate_random_hex_key_128,
        16,
        "Hello, AEGIS-128L!"
    );

    test_raw_encryption!(
        test_aegis128l_raw_encryption,
        CipherMode::Aegis128L,
        generate_random_hex_key_128,
        16,
        "Hello, AEGIS-128L!"
    );

    #[test]
    fn test_aegis128l_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let cipher_mode = CipherMode::Aegis128L;
        let metadata_size = cipher_mode.metadata_size();
        let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

        let page_data = {
            let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_hex_string(&generate_random_hex_key_128()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis128L, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_id = 42;
        let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
        assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

        let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
        assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }

    test_cipher_wrapper!(
        test_aegis128x4_cipher_wrapper,
        Aegis128X4Cipher,
        generate_random_hex_key_128,
        16,
        "Hello, AEGIS-128X4!"
    );

    test_raw_encryption!(
        test_aegis128x4_raw_encryption,
        CipherMode::Aegis128X4,
        generate_random_hex_key_128,
        16,
        "Hello, AEGIS-128X4!"
    );

    #[test]
    fn test_aegis128x4_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let cipher_mode = CipherMode::Aegis128X4;
        let metadata_size = cipher_mode.metadata_size();
        let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

        let page_data = {
            let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_hex_string(&generate_random_hex_key_128()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis128X4, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_id = 42;
        let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
        assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

        let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
        assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }

    test_cipher_wrapper!(
        test_aegis256x2_cipher_wrapper,
        Aegis256X2Cipher,
        generate_random_hex_key,
        32,
        "Hello, AEGIS-256X2!"
    );

    test_raw_encryption!(
        test_aegis256x2_raw_encryption,
        CipherMode::Aegis256X2,
        generate_random_hex_key,
        32,
        "Hello, AEGIS-256X2!"
    );

    #[test]
    fn test_aegis256x2_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let cipher_mode = CipherMode::Aegis256X2;
        let metadata_size = cipher_mode.metadata_size();
        let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

        let page_data = {
            let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis256X2, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_id = 42;
        let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
        assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

        let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
        assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }

    test_cipher_wrapper!(
        test_aegis256x4_cipher_wrapper,
        Aegis256X4Cipher,
        generate_random_hex_key,
        32,
        "Hello, AEGIS-256X4!"
    );

    test_raw_encryption!(
        test_aegis256x4_raw_encryption,
        CipherMode::Aegis256X4,
        generate_random_hex_key,
        32,
        "Hello, AEGIS-256X4!"
    );

    #[test]
    fn test_aegis256x4_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let cipher_mode = CipherMode::Aegis256X4;
        let metadata_size = cipher_mode.metadata_size();
        let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

        let page_data = {
            let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis256X4, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let page_id = 42;
        let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
        assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

        let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
        assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }

    #[test]
    fn test_cipher_mode_string_parsing() {
        // Test AES-128-GCM
        let mode = CipherMode::try_from("aes128gcm").unwrap();
        assert_eq!(mode, CipherMode::Aes128Gcm);
        assert_eq!(mode.to_string(), "aes128gcm");
        assert_eq!(mode.required_key_size(), 16);
        assert_eq!(mode.nonce_size(), 12);
        assert_eq!(mode.tag_size(), 16);

        let mode = CipherMode::try_from("aes-128-gcm").unwrap();
        assert_eq!(mode, CipherMode::Aes128Gcm);

        let mode = CipherMode::try_from("aes_128_gcm").unwrap();
        assert_eq!(mode, CipherMode::Aes128Gcm);

        // Test AES-256-GCM
        let mode = CipherMode::try_from("aes256gcm").unwrap();
        assert_eq!(mode, CipherMode::Aes256Gcm);
        assert_eq!(mode.to_string(), "aes256gcm");
        assert_eq!(mode.required_key_size(), 32);
        assert_eq!(mode.nonce_size(), 12);

        // Test that all AEGIS variants can be parsed from strings
        let mode = CipherMode::try_from("aegis128x2").unwrap();
        assert_eq!(mode, CipherMode::Aegis128X2);
        assert_eq!(mode.to_string(), "aegis128x2");
        assert_eq!(mode.required_key_size(), 16);
        assert_eq!(mode.nonce_size(), 16);
        assert_eq!(mode.tag_size(), 16);

        let mode = CipherMode::try_from("aegis-128x2").unwrap();
        assert_eq!(mode, CipherMode::Aegis128X2);

        let mode = CipherMode::try_from("aegis_128x2").unwrap();
        assert_eq!(mode, CipherMode::Aegis128X2);

        // Test AEGIS-128L
        let mode = CipherMode::try_from("aegis128l").unwrap();
        assert_eq!(mode, CipherMode::Aegis128L);
        assert_eq!(mode.to_string(), "aegis128l");
        assert_eq!(mode.required_key_size(), 16);
        assert_eq!(mode.nonce_size(), 16);

        // Test AEGIS-128X4
        let mode = CipherMode::try_from("aegis128x4").unwrap();
        assert_eq!(mode, CipherMode::Aegis128X4);
        assert_eq!(mode.to_string(), "aegis128x4");
        assert_eq!(mode.required_key_size(), 16);
        assert_eq!(mode.nonce_size(), 16);

        // Test AEGIS-256X2
        let mode = CipherMode::try_from("aegis256x2").unwrap();
        assert_eq!(mode, CipherMode::Aegis256X2);
        assert_eq!(mode.to_string(), "aegis256x2");
        assert_eq!(mode.required_key_size(), 32);
        assert_eq!(mode.nonce_size(), 32);

        // Test AEGIS-256X4
        let mode = CipherMode::try_from("aegis256x4").unwrap();
        assert_eq!(mode, CipherMode::Aegis256X4);
        assert_eq!(mode.to_string(), "aegis256x4");
        assert_eq!(mode.required_key_size(), 32);
        assert_eq!(mode.nonce_size(), 32);
    }
}
