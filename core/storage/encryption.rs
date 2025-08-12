use crate::Result;
use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use std::ops::Deref;

pub const ENCRYPTION_METADATA_SIZE: usize = 28;
pub const ENCRYPTED_PAGE_SIZE: usize = 4096;
pub const ENCRYPTION_NONCE_SIZE: usize = 12;

#[repr(transparent)]
#[derive(Clone)]
pub struct EncryptionKey([u8; 32]);

impl EncryptionKey {
    pub fn new(key: [u8; 32]) -> Self {
        Self(key)
    }

    pub fn from_string(s: &str) -> Self {
        let mut key = [0u8; 32];
        let bytes = s.as_bytes();
        let len = bytes.len().min(32);
        key[..len].copy_from_slice(&bytes[..len]);
        Self(key)
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

pub fn encrypt_page(page: &[u8], page_id: usize, key: &EncryptionKey) -> Result<Vec<u8>> {
    if page_id == 1 {
        tracing::debug!("skipping encryption for page 1 (database header)");
        return Ok(page.to_vec());
    }
    tracing::debug!("encrypting page {}", page_id);
    assert_eq!(
        page.len(),
        ENCRYPTED_PAGE_SIZE,
        "Page data must be exactly {ENCRYPTED_PAGE_SIZE} bytes"
    );
    let reserved_bytes = &page[ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE..];
    let reserved_bytes_zeroed = reserved_bytes.iter().all(|&b| b == 0);
    assert!(
        reserved_bytes_zeroed,
        "last reserved bytes must be empty/zero, but found non-zero bytes"
    );
    let payload = &page[..ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE];
    let (encrypted, nonce) = encrypt(payload, key)?;
    assert_eq!(
        encrypted.len(),
        ENCRYPTED_PAGE_SIZE - nonce.len(),
        "Encrypted page must be exactly {} bytes",
        ENCRYPTED_PAGE_SIZE - nonce.len()
    );
    let mut result = Vec::with_capacity(ENCRYPTED_PAGE_SIZE);
    result.extend_from_slice(&encrypted);
    result.extend_from_slice(&nonce);
    assert_eq!(
        result.len(),
        ENCRYPTED_PAGE_SIZE,
        "Encrypted page must be exactly {ENCRYPTED_PAGE_SIZE} bytes"
    );
    Ok(result)
}

pub fn decrypt_page(encrypted_page: &[u8], page_id: usize, key: &EncryptionKey) -> Result<Vec<u8>> {
    if page_id == 1 {
        tracing::debug!("skipping decryption for page 1 (database header)");
        return Ok(encrypted_page.to_vec());
    }
    tracing::debug!("decrypting page {}", page_id);
    assert_eq!(
        encrypted_page.len(),
        ENCRYPTED_PAGE_SIZE,
        "Encrypted page data must be exactly {ENCRYPTED_PAGE_SIZE} bytes"
    );

    let nonce_start = encrypted_page.len() - ENCRYPTION_NONCE_SIZE;
    let payload = &encrypted_page[..nonce_start];
    let nonce = &encrypted_page[nonce_start..];

    let decrypted_data = decrypt(payload, nonce, key)?;
    assert_eq!(
        decrypted_data.len(),
        ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE,
        "Decrypted page data must be exactly {} bytes",
        ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE
    );
    let mut result = Vec::with_capacity(ENCRYPTED_PAGE_SIZE);
    result.extend_from_slice(&decrypted_data);
    result.resize(ENCRYPTED_PAGE_SIZE, 0);
    assert_eq!(
        result.len(),
        ENCRYPTED_PAGE_SIZE,
        "Decrypted page data must be exactly {ENCRYPTED_PAGE_SIZE} bytes"
    );
    Ok(result)
}

fn encrypt(plaintext: &[u8], key: &EncryptionKey) -> Result<(Vec<u8>, Vec<u8>)> {
    let key: &Key<Aes256Gcm> = key.as_ref().into();
    let cipher = Aes256Gcm::new(key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher.encrypt(&nonce, plaintext).unwrap();
    Ok((ciphertext, nonce.to_vec()))
}

fn decrypt(ciphertext: &[u8], nonce: &[u8], key: &EncryptionKey) -> Result<Vec<u8>> {
    let key: &Key<Aes256Gcm> = key.as_ref().into();
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(nonce);
    let plaintext = cipher.decrypt(nonce, ciphertext).unwrap();
    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let data_size = ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE;

        let page_data = {
            let mut page = vec![0u8; ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_string("alice and bob use encryption on database");

        let page_id = 42;
        let encrypted = encrypt_page(&page_data, page_id, &key).unwrap();
        assert_eq!(encrypted.len(), ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);
        assert_ne!(&encrypted[..], &page_data[..]);

        let decrypted = decrypt_page(&encrypted, page_id, &key).unwrap();
        assert_eq!(decrypted.len(), ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }
}
