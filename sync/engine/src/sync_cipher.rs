use turso_core::EncryptionContext;

use crate::{errors::Error, Result};

const INFO_HEADER: &[u8; 10] = b"TSYNMETA\x01\x00";
const INFO_DOMAIN: &[u8] = b"turso-sync/info/v1\0";
const CHANGES_DOMAIN: &[u8] = b"turso-sync/changes/chunk/v2\0";
pub(crate) const CHUNK_SIZE: usize = 32 * 1024;

pub(crate) fn seal_metadata(crypto: &EncryptionContext, plaintext: &[u8]) -> Result<Vec<u8>> {
    let aad = [INFO_DOMAIN, INFO_HEADER].concat();
    let mut bytes = INFO_HEADER.to_vec();
    bytes.extend(seal(crypto, plaintext, &aad)?);
    Ok(bytes)
}

pub(crate) fn open_metadata(crypto: &EncryptionContext, bytes: &[u8]) -> Result<Vec<u8>> {
    if !bytes.starts_with(INFO_HEADER) {
        return Err(invalid("invalid encrypted metadata header"));
    }
    open(
        crypto,
        &bytes[INFO_HEADER.len()..],
        &[INFO_DOMAIN, INFO_HEADER].concat(),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChangeKind {
    Logical = 0,
    Pages = 1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChangeIdentity {
    id: [u8; 16],
    kind: ChangeKind,
}

impl ChangeIdentity {
    pub(crate) fn new(kind: ChangeKind) -> Result<Self> {
        let mut id = [0; 16];
        getrandom::fill(&mut id)
            .map_err(|e| invalid(&format!("cannot generate change download identity: {e}")))?;
        Ok(Self { id, kind })
    }

    pub(crate) fn validate_record_length(&self, length: u64) -> Result<()> {
        if length == 0 || (self.kind == ChangeKind::Pages && length != 4120) {
            return Err(invalid("invalid change record length"));
        }
        Ok(())
    }

    pub(crate) fn seal_chunk(
        &self,
        crypto: &EncryptionContext,
        record_index: u64,
        length: u64,
        chunk_index: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        let size = self.chunk_plaintext_size(length, chunk_index)?;
        if plaintext.len() != size {
            return Err(invalid("incorrect change chunk length"));
        }
        seal(
            crypto,
            plaintext,
            &self.chunk_aad(record_index, length, chunk_index),
        )
    }

    pub(crate) fn open_chunk(
        &self,
        crypto: &EncryptionContext,
        record_index: u64,
        length: u64,
        chunk_index: u64,
        stored: &[u8],
    ) -> Result<Vec<u8>> {
        let size = self.chunk_plaintext_size(length, chunk_index)?;
        if stored.len() != size + crypto.tag_size() + crypto.nonce_size() {
            return Err(invalid("truncated or oversized change chunk"));
        }
        open(
            crypto,
            stored,
            &self.chunk_aad(record_index, length, chunk_index),
        )
    }

    pub(crate) fn chunk_plaintext_size(&self, length: u64, chunk_index: u64) -> Result<usize> {
        self.validate_record_length(length)?;
        let offset = chunk_index
            .checked_mul(CHUNK_SIZE as u64)
            .ok_or_else(|| invalid("change chunk offset overflow"))?;
        let remaining = length
            .checked_sub(offset)
            .filter(|&n| n > 0)
            .ok_or_else(|| invalid("change chunk is outside its record"))?;
        Ok(remaining.min(CHUNK_SIZE as u64) as usize)
    }

    fn chunk_aad(&self, record_index: u64, length: u64, chunk_index: u64) -> Vec<u8> {
        [
            CHANGES_DOMAIN,
            self.id.as_slice(),
            &[self.kind as u8],
            &record_index.to_le_bytes(),
            &length.to_le_bytes(),
            &chunk_index.to_le_bytes(),
        ]
        .concat()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ChangeCounts {
    pub(crate) records: u64,
    pub(crate) bytes: u64,
}

impl ChangeCounts {
    pub(crate) fn append(&mut self, length: u64) -> Result<()> {
        let records = self
            .records
            .checked_add(1)
            .ok_or_else(|| invalid("change record count overflow"))?;
        let bytes = self
            .bytes
            .checked_add(length)
            .ok_or_else(|| invalid("change byte count overflow"))?;
        *self = Self { records, bytes };
        Ok(())
    }
}

fn seal(crypto: &EncryptionContext, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    let (mut bytes, nonce) = crypto.encrypt_chunk(plaintext, aad)?;
    bytes.extend(nonce);
    Ok(bytes)
}

fn open(crypto: &EncryptionContext, bytes: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() < crypto.tag_size() + crypto.nonce_size() {
        return Err(invalid("truncated encrypted sync data"));
    }
    let (ciphertext, nonce) = bytes.split_at(bytes.len() - crypto.nonce_size());
    crypto
        .decrypt_chunk(ciphertext, nonce, aad)
        .map_err(|_| Error::AuthenticationFailed)
}

fn invalid(message: &str) -> Error {
    Error::DatabaseSyncEngineError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::{CipherMode, EncryptionKey};

    fn contexts() -> impl Iterator<Item = EncryptionContext> {
        [
            CipherMode::Aes128Gcm,
            CipherMode::Aes256Gcm,
            CipherMode::Aegis128L,
            CipherMode::Aegis128X2,
            CipherMode::Aegis128X4,
            CipherMode::Aegis256,
            CipherMode::Aegis256X2,
            CipherMode::Aegis256X4,
        ]
        .into_iter()
        .map(|cipher| {
            let key = if cipher.required_key_size() == 16 {
                EncryptionKey::new_128([7; 16])
            } else {
                EncryptionKey::new_256([7; 32])
            };
            EncryptionContext::new(cipher, &key, 4096).expect("valid cipher")
        })
    }

    #[test]
    fn metadata_roundtrip_and_fresh_nonce() {
        for crypto in contexts() {
            let plaintext = br#"{"revision":"secret"}"#;
            let bytes = seal_metadata(&crypto, plaintext).expect("seal");
            assert_eq!(open_metadata(&crypto, &bytes).expect("open"), plaintext);
            assert_ne!(
                bytes,
                seal_metadata(&crypto, plaintext).expect("fresh seal")
            );
            for i in 0..bytes.len() {
                let mut corrupt = bytes.clone();
                corrupt[i] ^= 1;
                assert!(open_metadata(&crypto, &corrupt).is_err(), "byte {i}");
                assert!(open_metadata(&crypto, &bytes[..i]).is_err(), "prefix {i}");
            }
            let mut trailing = bytes;
            trailing.push(0);
            assert!(open_metadata(&crypto, &trailing).is_err());
        }
    }

    #[test]
    fn changes_roundtrip_boundaries_and_identity_binding() {
        for crypto in contexts() {
            let identity = ChangeIdentity::new(ChangeKind::Logical).unwrap();
            for length in [
                1,
                CHUNK_SIZE - 1,
                CHUNK_SIZE,
                CHUNK_SIZE + 1,
                CHUNK_SIZE * 3 + 5,
            ] {
                for (i, chunk) in vec![42; length].chunks(CHUNK_SIZE).enumerate() {
                    let stored = identity
                        .seal_chunk(&crypto, 0, length as u64, i as u64, chunk)
                        .unwrap();
                    assert_eq!(
                        identity
                            .open_chunk(&crypto, 0, length as u64, i as u64, &stored)
                            .unwrap(),
                        chunk
                    );
                    for (record, len, index) in [
                        (1, length as u64, i as u64),
                        (0, length as u64 + 1, i as u64),
                        (0, length as u64, i as u64 + 1),
                    ] {
                        assert!(identity
                            .open_chunk(&crypto, record, len, index, &stored)
                            .is_err());
                    }
                    let other = ChangeIdentity::new(ChangeKind::Logical).unwrap();
                    assert!(other
                        .open_chunk(&crypto, 0, length as u64, i as u64, &stored)
                        .is_err());
                }
            }
            let pages = ChangeIdentity::new(ChangeKind::Pages).unwrap();
            assert!(pages.validate_record_length(0).is_err());
            assert!(pages.validate_record_length(4096).is_err());
            assert!(pages.chunk_plaintext_size(4120, u64::MAX).is_err());
            let stored = pages.seal_chunk(&crypto, 0, 4120, 0, &[13; 4120]).unwrap();
            assert_eq!(
                pages.open_chunk(&crypto, 0, 4120, 0, &stored).unwrap(),
                vec![13; 4120]
            );
            for i in [0, 4119, 4120, stored.len() - 1] {
                let mut corrupt = stored.clone();
                corrupt[i] ^= 1;
                assert!(matches!(
                    pages.open_chunk(&crypto, 0, 4120, 0, &corrupt),
                    Err(Error::AuthenticationFailed)
                ));
                assert!(pages.open_chunk(&crypto, 0, 4120, 0, &stored[..i]).is_err());
            }
        }
    }

    #[test]
    fn wrong_key_and_counts_overflow() {
        let crypto = contexts().next().expect("AES128");
        let wrong = EncryptionContext::new(
            CipherMode::Aes128Gcm,
            &EncryptionKey::new_128([8; 16]),
            4096,
        )
        .expect("context");
        let bytes = seal_metadata(&crypto, b"metadata").expect("seal");
        assert!(matches!(
            open_metadata(&wrong, &bytes),
            Err(Error::AuthenticationFailed)
        ));
        let mut counts = ChangeCounts {
            records: 1,
            bytes: u64::MAX,
        };
        let original = counts;
        assert!(counts.append(1).is_err());
        assert_eq!(counts, original);
    }
}
