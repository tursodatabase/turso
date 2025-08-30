#![allow(unused_variables, dead_code)]
use crate::{LimboError, Result};
use aegis::aegis256::Aegis256;
use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use std::ops::Deref;

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
            .map_err(|e| LimboError::InvalidArgument(format!("Invalid hex string: {e}")))?;
        let key: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
            LimboError::InvalidArgument(format!(
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

// wrapper struct for AEGIS-256 cipher, because the crate we use is a bit low-level and we add
// some nice abstractions here
// note, the AEGIS has many variants and support for hardware acceleration. Here we just use the
// vanilla version, which is still order of maginitudes faster than AES-GCM in software. Hardware
// based compilation is left for future work.
#[derive(Clone)]
pub struct Aegis256Cipher {
    key: EncryptionKey,
}

impl Aegis256Cipher {
    // AEGIS-256 supports both 16 and 32 byte tags, we use the 16 byte variant, it is faster
    // and provides sufficient security for our use case.
    const TAG_SIZE: usize = 16;
    fn new(key: &EncryptionKey) -> Self {
        Self { key: key.clone() }
    }

    fn encrypt(&self, plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, [u8; 32])> {
        let nonce = generate_secure_nonce();
        let (ciphertext, tag) =
            Aegis256::<16>::new(self.key.as_bytes(), &nonce).encrypt(plaintext, ad);
        let mut result = ciphertext;
        result.extend_from_slice(&tag);
        Ok((result, nonce))
    }

    fn decrypt(&self, ciphertext: &[u8], nonce: &[u8; 32], ad: &[u8]) -> Result<Vec<u8>> {
        if ciphertext.len() < Self::TAG_SIZE {
            return Err(LimboError::InternalError(
                "Ciphertext too short for AEGIS-256".into(),
            ));
        }
        let (ct, tag) = ciphertext.split_at(ciphertext.len() - Self::TAG_SIZE);
        let tag_array: [u8; 16] = tag
            .try_into()
            .map_err(|_| LimboError::InternalError("Invalid tag size for AEGIS-256".into()))?;

        let plaintext = Aegis256::<16>::new(self.key.as_bytes(), nonce)
            .decrypt(ct, &tag_array, ad)
            .map_err(|_| {
                LimboError::InternalError("AEGIS-256 decryption failed: invalid tag".into())
            })?;
        Ok(plaintext)
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
    type Error = LimboError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "aes256gcm" | "aes-256-gcm" | "aes_256_gcm" => Ok(CipherMode::Aes256Gcm),
            "aegis256" | "aegis-256" | "aegis_256" => Ok(CipherMode::Aegis256),
            _ => Err(LimboError::InvalidArgument(format!(
                "Unknown cipher name: {s}"
            ))),
        }
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
            CipherMode::Aes256Gcm => 16,
            CipherMode::Aegis256 => 16,
        }
    }

    /// Returns the total metadata size (nonce + tag) for this cipher mode.
    pub fn metadata_size(&self) -> usize {
        self.nonce_size() + self.tag_size()
    }
}

#[derive(Clone)]
pub enum Cipher {
    Aes256Gcm(Box<Aes256Gcm>),
    Aegis256(Box<Aegis256Cipher>),
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
            return Err(crate::LimboError::InvalidArgument(format!(
                "Invalid key size for {:?}: expected {} bytes, got {}",
                cipher_mode,
                required_size,
                key.as_slice().len()
            )));
        }

        let cipher = match cipher_mode {
            CipherMode::Aes256Gcm => {
                let cipher_key: &Key<Aes256Gcm> = key.as_ref().into();
                Cipher::Aes256Gcm(Box::new(Aes256Gcm::new(cipher_key)))
            }
            CipherMode::Aegis256 => Cipher::Aegis256(Box::new(Aegis256Cipher::new(key))),
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
        match &self.cipher {
            Cipher::Aes256Gcm(cipher) => {
                let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
                let ciphertext = cipher
                    .encrypt(&nonce, plaintext)
                    .map_err(|e| LimboError::InternalError(format!("Encryption failed: {e:?}")))?;
                Ok((ciphertext, nonce.to_vec()))
            }
            Cipher::Aegis256(cipher) => {
                let ad = b"";
                let (ciphertext, nonce) = cipher.encrypt(plaintext, ad)?;
                Ok((ciphertext, nonce.to_vec()))
            }
        }
    }

    fn decrypt_raw(&self, ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        match &self.cipher {
            Cipher::Aes256Gcm(cipher) => {
                let nonce = Nonce::from_slice(nonce);
                let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|e| {
                    crate::LimboError::InternalError(format!("Decryption failed: {e:?}"))
                })?;
                Ok(plaintext)
            }
            Cipher::Aegis256(cipher) => {
                let nonce_array: [u8; 32] = nonce.try_into().map_err(|_| {
                    LimboError::InternalError(format!(
                        "Invalid nonce size for AEGIS-256: expected 32, got {}",
                        nonce.len()
                    ))
                })?;
                let ad = b"";
                cipher.decrypt(ciphertext, &nonce_array, ad)
            }
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

fn generate_secure_nonce() -> [u8; 32] {
    // use OsRng directly to fill bytes, similar to how AeadCore does it
    use aes_gcm::aead::rand_core::RngCore;
    let mut nonce = [0u8; 32];
    OsRng.fill_bytes(&mut nonce);
    nonce
}

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
    #[cfg(feature = "encryption")]
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
    #[cfg(feature = "encryption")]
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
    #[cfg(feature = "encryption")]
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
    #[cfg(feature = "encryption")]
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
