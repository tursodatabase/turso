#![allow(unused_variables, dead_code)]
use crate::{Result, TursoError};
use aegis::aegis256::Aegis256;
use aes_gcm::aead::{AeadCore, OsRng};
use std::ops::Deref;
use turso_macros::match_ignore_ascii_case;
// AEGIS-256 supports both 16 and 32 byte tags, we use the 16 byte variant, it is faster
// and provides sufficient security for our use case.
const AEGIS_TAG_SIZE: usize = 16;
const AES256GCM_TAG_SIZE: usize = 16;

#[repr(transparent)]
#[derive(Clone)]
pub struct EncryptionKey([u8; 32]);

impl EncryptionKey {
    pub fn new(key: [u8; 32]) -> Self {
        Self(key)
    }

    pub fn from_hex_string(s: &str) -> Result<Self> {
        let hex_str = s.trim();
        let bytes = hex::decode(hex_str)
            .map_err(|e| TursoError::InvalidArgument(format!("Invalid hex string: {e}")))?;
        let key: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
            TursoError::InvalidArgument(format!(
                "Hex string must decode to exactly 32 bytes, got {}",
                v.len()
            ))
        })?;
        Ok(Self(key))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for EncryptionKey {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8; 32]> for EncryptionKey {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
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
        for byte in self.0.iter_mut() {
            unsafe {
                std::ptr::write_volatile(byte, 0);
            }
        }
    }
}

pub trait AeadCipher {
    fn encrypt(&self, plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, Vec<u8>)>;
    fn decrypt(&self, ciphertext: &[u8], nonce: &[u8], ad: &[u8]) -> Result<Vec<u8>>;

    fn encrypt_detached(&self, plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)>;

    fn decrypt_detached(
        &self,
        ciphertext: &[u8],
        nonce: &[u8],
        tag: &[u8],
        ad: &[u8],
    ) -> Result<Vec<u8>>;
}

// wrapper struct for AEGIS-256 cipher, because the crate we use is a bit low-level and we add
// some nice abstractions here
// note, the AEGIS has many variants and support for hardware acceleration. Here we just use the
// vanilla version, which is still order of magnitudes faster than AES-GCM in software. Hardware
// based compilation is left for future work.
#[derive(Clone)]
pub struct Aegis256Cipher {
    key: EncryptionKey,
}

impl Aegis256Cipher {
    fn new(key: &EncryptionKey) -> Self {
        Self { key: key.clone() }
    }
}

impl AeadCipher for Aegis256Cipher {
    fn encrypt(&self, plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        let nonce = generate_secure_nonce();
        let (ciphertext, tag) =
            Aegis256::<AEGIS_TAG_SIZE>::new(self.key.as_bytes(), &nonce).encrypt(plaintext, ad);

        let mut result = ciphertext;
        result.extend_from_slice(&tag);
        Ok((result, nonce.to_vec()))
    }

    fn decrypt(&self, ciphertext: &[u8], nonce: &[u8], ad: &[u8]) -> Result<Vec<u8>> {
        if ciphertext.len() < AEGIS_TAG_SIZE {
            return Err(TursoError::InternalError("Ciphertext too short".into()));
        }
        let (ct, tag) = ciphertext.split_at(ciphertext.len() - AEGIS_TAG_SIZE);
        let tag_array: [u8; AEGIS_TAG_SIZE] = tag.try_into().map_err(|_| {
            TursoError::InternalError(format!("Invalid tag size for AEGIS-256 {AEGIS_TAG_SIZE}"))
        })?;

        let nonce_array: [u8; 32] = nonce
            .try_into()
            .map_err(|_| TursoError::InternalError("Invalid nonce size for AEGIS-256".into()))?;

        Aegis256::<AEGIS_TAG_SIZE>::new(self.key.as_bytes(), &nonce_array)
            .decrypt(ct, &tag_array, ad)
            .map_err(|_| TursoError::InternalError("AEGIS-256 decryption failed".into()))
    }

    fn encrypt_detached(&self, plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let nonce = generate_secure_nonce();
        let (ciphertext, tag) =
            Aegis256::<AEGIS_TAG_SIZE>::new(self.key.as_bytes(), &nonce).encrypt(plaintext, ad);

        Ok((ciphertext, tag.to_vec(), nonce.to_vec()))
    }

    fn decrypt_detached(
        &self,
        ciphertext: &[u8],
        nonce: &[u8],
        tag: &[u8],
        ad: &[u8],
    ) -> Result<Vec<u8>> {
        let tag_array: [u8; AEGIS_TAG_SIZE] = tag.try_into().map_err(|_| {
            TursoError::InternalError(format!("Invalid tag size for AEGIS-256 {AEGIS_TAG_SIZE}"))
        })?;
        let nonce_array: [u8; 32] = nonce
            .try_into()
            .map_err(|_| TursoError::InternalError("Invalid nonce size for AEGIS-256".into()))?;

        Aegis256::<AEGIS_TAG_SIZE>::new(self.key.as_bytes(), &nonce_array)
            .decrypt(ciphertext, &tag_array, ad)
            .map_err(|_| TursoError::InternalError("AEGIS-256 decrypt_detached failed".into()))
    }
}

#[derive(Clone)]
pub struct Aes256GcmCipher {
    key: EncryptionKey,
}

impl Aes256GcmCipher {
    fn new(key: &EncryptionKey) -> Self {
        Self { key: key.clone() }
    }
}

impl AeadCipher for Aes256GcmCipher {
    fn encrypt(&self, plaintext: &[u8], _ad: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        use aes_gcm::aead::{AeadInPlace, KeyInit};
        use aes_gcm::Aes256Gcm;

        let cipher = Aes256Gcm::new_from_slice(self.key.as_bytes())
            .map_err(|_| TursoError::InternalError("Bad AES key".into()))?;
        let nonce = Aes256Gcm::generate_nonce(&mut rand::thread_rng());
        let mut buffer = plaintext.to_vec();

        let tag = cipher
            .encrypt_in_place_detached(&nonce, b"", &mut buffer)
            .map_err(|_| TursoError::InternalError("AES-GCM encrypt failed".into()))?;

        buffer.extend_from_slice(&tag[..AES256GCM_TAG_SIZE]);
        Ok((buffer, nonce.to_vec()))
    }

    fn decrypt(&self, ciphertext: &[u8], nonce: &[u8], ad: &[u8]) -> Result<Vec<u8>> {
        use aes_gcm::aead::{AeadInPlace, KeyInit};
        use aes_gcm::{Aes256Gcm, Nonce};

        if ciphertext.len() < AES256GCM_TAG_SIZE {
            return Err(TursoError::InternalError("Ciphertext too short".into()));
        }
        let (ct, tag) = ciphertext.split_at(ciphertext.len() - AES256GCM_TAG_SIZE);

        let cipher = Aes256Gcm::new_from_slice(self.key.as_bytes())
            .map_err(|_| TursoError::InternalError("Bad AES key".into()))?;
        let nonce = Nonce::from_slice(nonce);

        let mut buffer = ct.to_vec();
        cipher
            .decrypt_in_place_detached(nonce, ad, &mut buffer, tag.into())
            .map_err(|_| TursoError::InternalError("AES-GCM decrypt failed".into()))?;

        Ok(buffer)
    }

    fn encrypt_detached(&self, plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        use aes_gcm::aead::{AeadInPlace, KeyInit};
        use aes_gcm::Aes256Gcm;

        let cipher = Aes256Gcm::new_from_slice(self.key.as_bytes())
            .map_err(|_| TursoError::InternalError("Bad AES key".into()))?;
        let nonce = Aes256Gcm::generate_nonce(&mut rand::thread_rng());

        let mut buffer = plaintext.to_vec();
        let tag = cipher
            .encrypt_in_place_detached(&nonce, ad, &mut buffer)
            .map_err(|_| TursoError::InternalError("AES-GCM encrypt_detached failed".into()))?;

        Ok((buffer, nonce.to_vec(), tag.to_vec()))
    }

    fn decrypt_detached(
        &self,
        ciphertext: &[u8],
        nonce: &[u8],
        tag: &[u8],
        ad: &[u8],
    ) -> Result<Vec<u8>> {
        use aes_gcm::aead::{AeadInPlace, KeyInit};
        use aes_gcm::{Aes256Gcm, Nonce};

        let cipher = Aes256Gcm::new_from_slice(self.key.as_bytes())
            .map_err(|_| TursoError::InternalError("Bad AES key".into()))?;
        let nonce = Nonce::from_slice(nonce);

        let mut buffer = ciphertext.to_vec();
        cipher
            .decrypt_in_place_detached(nonce, ad, &mut buffer, tag.into())
            .map_err(|_| TursoError::InternalError("AES-GCM decrypt_detached failed".into()))?;

        Ok(buffer)
    }
}

impl std::fmt::Debug for Aegis256Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aegis256Cipher")
            .field("key", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CipherMode {
    Aes256Gcm,
    Aegis256,
}

impl TryFrom<&str> for CipherMode {
    type Error = TursoError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let s_bytes = s.as_bytes();
        match_ignore_ascii_case!(match s_bytes {
            b"aes256gcm" | b"aes-256-gcm" | b"aes_256_gcm" => Ok(CipherMode::Aes256Gcm),
            b"aegis256" | b"aegis-256" | b"aegis_256" => Ok(CipherMode::Aegis256),
            _ => Err(TursoError::InvalidArgument(format!(
                "Unknown cipher name: {s}"
            ))),
        })
    }
}

impl std::fmt::Display for CipherMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CipherMode::Aes256Gcm => write!(f, "aes256gcm"),
            CipherMode::Aegis256 => write!(f, "aegis256"),
        }
    }
}

impl CipherMode {
    /// Every cipher requires a specific key size. For 256-bit algorithms, this is 32 bytes.
    /// For 128-bit algorithms, it would be 16 bytes, etc.
    pub fn required_key_size(&self) -> usize {
        match self {
            CipherMode::Aes256Gcm => 32,
            CipherMode::Aegis256 => 32,
        }
    }

    /// Returns the nonce size for this cipher mode.
    pub fn nonce_size(&self) -> usize {
        match self {
            CipherMode::Aes256Gcm => 12,
            CipherMode::Aegis256 => 32,
        }
    }

    /// Returns the authentication tag size for this cipher mode.
    pub fn tag_size(&self) -> usize {
        match self {
            CipherMode::Aes256Gcm => AES256GCM_TAG_SIZE,
            CipherMode::Aegis256 => AEGIS_TAG_SIZE,
        }
    }

    /// Returns the total metadata size (nonce + tag) for this cipher mode.
    pub fn metadata_size(&self) -> usize {
        self.nonce_size() + self.tag_size()
    }
}

#[derive(Clone)]
pub enum Cipher {
    Aes256Gcm(Aes256GcmCipher),
    Aegis256(Aegis256Cipher),
}

impl Cipher {
    fn as_aead(&self) -> &dyn AeadCipher {
        match self {
            Cipher::Aes256Gcm(c) => c,
            Cipher::Aegis256(c) => c,
        }
    }
}

impl std::fmt::Debug for Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cipher::Aes256Gcm(_) => write!(f, "Cipher::Aes256Gcm"),
            Cipher::Aegis256(_) => write!(f, "Cipher::Aegis256"),
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
        if key.as_slice().len() != required_size {
            return Err(crate::TursoError::InvalidArgument(format!(
                "Invalid key size for {:?}: expected {} bytes, got {}",
                cipher_mode,
                required_size,
                key.as_slice().len()
            )));
        }

        let cipher = match cipher_mode {
            CipherMode::Aes256Gcm => Cipher::Aes256Gcm(Aes256GcmCipher::new(key)),
            CipherMode::Aegis256 => Cipher::Aegis256(Aegis256Cipher::new(key)),
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

    #[cfg(feature = "encryption")]
    pub fn encrypt_page(&self, page: &[u8], page_id: usize) -> Result<Vec<u8>> {
        if page_id == 1 {
            tracing::debug!("skipping encryption for page 1 (database header)");
            return Ok(page.to_vec());
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
        let reserved_bytes_zeroed = reserved_bytes.iter().all(|&b| b == 0);
        assert!(
            reserved_bytes_zeroed,
            "last reserved bytes must be empty/zero, but found non-zero bytes"
        );

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
        if page_id == 1 {
            tracing::debug!("skipping decryption for page 1 (database header)");
            return Ok(encrypted_page.to_vec());
        }
        tracing::debug!("decrypting page {}", page_id);
        assert_eq!(
            encrypted_page.len(),
            self.page_size,
            "Encrypted page data must be exactly {} bytes",
            self.page_size
        );

        let nonce_size = self.cipher_mode.nonce_size();
        let nonce_start = encrypted_page.len() - nonce_size;
        let payload = &encrypted_page[..nonce_start];
        let nonce = &encrypted_page[nonce_start..];

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

    /// encrypts raw data using the configured cipher, returns ciphertext and nonce
    fn encrypt_raw(&self, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        self.cipher.as_aead().encrypt(plaintext, b"")
    }

    fn decrypt_raw(&self, ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        self.cipher.as_aead().decrypt(ciphertext, nonce, b"")
    }

    fn encrypt_raw_detached(&self, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        self.cipher.as_aead().encrypt_detached(plaintext, b"")
    }

    fn decrypt_raw_detached(&self, ciphertext: &[u8], nonce: &[u8], tag: &[u8]) -> Result<Vec<u8>> {
        self.cipher
            .as_aead()
            .decrypt_detached(ciphertext, nonce, tag, b"")
    }

    #[cfg(not(feature = "encryption"))]
    pub fn encrypt_page(&self, _page: &[u8], _page_id: usize) -> Result<Vec<u8>> {
        Err(TursoError::InvalidArgument(
            "encryption is not enabled, cannot encrypt page. enable via passing `--features encryption`".into(),
        ))
    }

    #[cfg(not(feature = "encryption"))]
    pub fn decrypt_page(&self, _encrypted_page: &[u8], _page_id: usize) -> Result<Vec<u8>> {
        Err(TursoError::InvalidArgument(
            "encryption is not enabled, cannot decrypt page. enable via passing `--features encryption`".into(),
        ))
    }
}

fn generate_secure_nonce() -> [u8; 32] {
    // use OsRng directly to fill bytes, similar to how AeadCore does it
    use aes_gcm::aead::rand_core::RngCore;
    let mut nonce = [0u8; 32];
    OsRng.fill_bytes(&mut nonce);
    nonce
}

#[cfg(feature = "encryption")]
#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    const DEFAULT_ENCRYPTED_PAGE_SIZE: usize = 4096;

    fn generate_random_hex_key() -> String {
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        hex::encode(bytes)
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

    #[test]
    fn test_aegis256_cipher_wrapper() {
        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let cipher = Aegis256Cipher::new(&key);

        let plaintext = b"Hello, AEGIS-256!";
        let ad = b"additional data";

        let (ciphertext, nonce) = cipher.encrypt(plaintext, ad).unwrap();
        assert_eq!(nonce.len(), 32);
        assert_ne!(ciphertext[..plaintext.len()], plaintext[..]);

        let decrypted = cipher.decrypt(&ciphertext, &nonce, ad).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_aegis256_raw_encryption() {
        let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
        let ctx = EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE)
            .unwrap();

        let plaintext = b"Hello, AEGIS-256!";
        let (ciphertext, nonce) = ctx.encrypt_raw(plaintext).unwrap();

        assert_eq!(nonce.len(), 32); // AEGIS-256 uses 32-byte nonces
        assert_ne!(ciphertext[..plaintext.len()], plaintext[..]);

        let decrypted = ctx.decrypt_raw(&ciphertext, &nonce).unwrap();
        assert_eq!(decrypted, plaintext);
    }

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
}
