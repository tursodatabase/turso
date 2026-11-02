use turso_core::EncryptionContext;

use crate::{errors::Error, Result};

const INFO_HEADER: &[u8; 10] = b"TSYNMETA\x01\x00";
const INFO_DOMAIN: &[u8] = b"turso-sync/info/v1\0";
const CHANGES_DOMAIN: &[u8] = b"turso-sync/changes/chunk/v1\0";
const END_DOMAIN: &[u8] = b"turso-sync/changes/end/v1\0";
pub(crate) const CHUNK_SIZE: usize = 32 * 1024;
pub(crate) const FILE_HEADER_SIZE: usize = 28;
pub(crate) const RECORD_HEADER_SIZE: usize = 9;
const RECORD: u8 = 1;
const END: u8 = 2;

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
    header: [u8; FILE_HEADER_SIZE],
}

impl ChangeIdentity {
    pub(crate) fn new(kind: ChangeKind) -> Result<Self> {
        let mut header = [0; FILE_HEADER_SIZE];
        header[..8].copy_from_slice(b"TSYNCHNG");
        header[8] = 1;
        header[10] = kind as u8;
        getrandom::fill(&mut header[12..])
            .map_err(|e| invalid(&format!("cannot generate change download identity: {e}")))?;
        Ok(Self { header })
    }

    pub(crate) fn header(&self) -> &[u8; FILE_HEADER_SIZE] {
        &self.header
    }

    pub(crate) fn validate_header(&self, bytes: &[u8]) -> Result<()> {
        if bytes != self.header {
            return Err(invalid("change file does not match the active download"));
        }
        Ok(())
    }

    pub(crate) fn record_header(&self, length: u64) -> Result<[u8; RECORD_HEADER_SIZE]> {
        self.validate_record_length(length)?;
        let mut header = [0; RECORD_HEADER_SIZE];
        header[0] = RECORD;
        header[1..].copy_from_slice(&length.to_le_bytes());
        Ok(header)
    }

    pub(crate) fn record_length(&self, header: &[u8]) -> Result<u64> {
        if header.len() != RECORD_HEADER_SIZE || header[0] != RECORD {
            return Err(invalid("invalid change record header"));
        }
        let length = u64::from_le_bytes(header[1..].try_into().expect("checked record header"));
        self.validate_record_length(length)?;
        Ok(length)
    }

    fn validate_record_length(&self, length: u64) -> Result<()> {
        if length == 0 || (self.header[10] == ChangeKind::Pages as u8 && length != 4120) {
            return Err(invalid("invalid change record length"));
        }
        Ok(())
    }

    pub(crate) fn seal_chunk(
        &self,
        crypto: &EncryptionContext,
        record_index: u64,
        record_header: &[u8; RECORD_HEADER_SIZE],
        chunk_index: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        let size = self.chunk_plaintext_size(record_header, chunk_index)?;
        if plaintext.len() != size {
            return Err(invalid("incorrect change chunk length"));
        }
        seal(
            crypto,
            plaintext,
            &self.chunk_aad(record_index, record_header, chunk_index),
        )
    }

    pub(crate) fn open_chunk(
        &self,
        crypto: &EncryptionContext,
        record_index: u64,
        record_header: &[u8; RECORD_HEADER_SIZE],
        chunk_index: u64,
        stored: &[u8],
    ) -> Result<Vec<u8>> {
        let size = self.chunk_plaintext_size(record_header, chunk_index)?;
        if stored.len() != size + crypto.tag_size() + crypto.nonce_size() {
            return Err(invalid("truncated or oversized change chunk"));
        }
        open(
            crypto,
            stored,
            &self.chunk_aad(record_index, record_header, chunk_index),
        )
    }

    pub(crate) fn chunk_plaintext_size(
        &self,
        record_header: &[u8; RECORD_HEADER_SIZE],
        chunk_index: u64,
    ) -> Result<usize> {
        let length = self.record_length(record_header)?;
        let offset = chunk_index
            .checked_mul(CHUNK_SIZE as u64)
            .ok_or_else(|| invalid("change chunk offset overflow"))?;
        let remaining = length
            .checked_sub(offset)
            .filter(|&n| n > 0)
            .ok_or_else(|| invalid("change chunk is outside its record"))?;
        Ok(remaining.min(CHUNK_SIZE as u64) as usize)
    }

    fn chunk_aad(&self, record_index: u64, record_header: &[u8], chunk_index: u64) -> Vec<u8> {
        [
            CHANGES_DOMAIN,
            self.header.as_slice(),
            &record_index.to_le_bytes(),
            record_header,
            &chunk_index.to_le_bytes(),
        ]
        .concat()
    }

    pub(crate) fn seal_end(
        &self,
        crypto: &EncryptionContext,
        counts: ChangeCounts,
    ) -> Result<Vec<u8>> {
        let plaintext = [counts.records.to_le_bytes(), counts.bytes.to_le_bytes()].concat();
        let mut bytes = vec![END];
        bytes.extend(seal(
            crypto,
            &plaintext,
            &[END_DOMAIN, &self.header, &[END]].concat(),
        )?);
        Ok(bytes)
    }

    pub(crate) fn validate_end(
        &self,
        crypto: &EncryptionContext,
        bytes: &[u8],
        counts: ChangeCounts,
    ) -> Result<()> {
        if bytes.len() != Self::end_size(crypto) || bytes[0] != END {
            return Err(invalid(
                "missing, truncated, or oversized change end record",
            ));
        }
        let plaintext = open(
            crypto,
            &bytes[1..],
            &[END_DOMAIN, &self.header, &[END]].concat(),
        )?;
        let expected = [counts.records.to_le_bytes(), counts.bytes.to_le_bytes()].concat();
        if plaintext != expected {
            return Err(invalid("change end record counts do not match"));
        }
        Ok(())
    }

    pub(crate) fn end_size(crypto: &EncryptionContext) -> usize {
        1 + 16 + crypto.tag_size() + crypto.nonce_size()
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
    fn changes_roundtrip_boundaries_and_empty_end() {
        for crypto in contexts() {
            let identity = ChangeIdentity::new(ChangeKind::Logical).expect("identity");
            identity.validate_header(identity.header()).expect("header");
            let mut counts = ChangeCounts::default();
            let end = identity.seal_end(&crypto, counts).expect("empty end");
            identity
                .validate_end(&crypto, &end, counts)
                .expect("empty end");
            for length in [
                1,
                CHUNK_SIZE - 1,
                CHUNK_SIZE,
                CHUNK_SIZE + 1,
                CHUNK_SIZE * 3 + 5,
            ] {
                let header = identity.record_header(length as u64).expect("header");
                for (i, chunk) in vec![42; length].chunks(CHUNK_SIZE).enumerate() {
                    let stored = identity
                        .seal_chunk(&crypto, counts.records, &header, i as u64, chunk)
                        .expect("seal");
                    assert_eq!(
                        identity
                            .open_chunk(&crypto, counts.records, &header, i as u64, &stored)
                            .expect("open"),
                        chunk
                    );
                    assert!(identity
                        .open_chunk(&crypto, counts.records + 1, &header, i as u64, &stored)
                        .is_err());
                    assert!(identity
                        .open_chunk(&crypto, counts.records, &header, i as u64 + 1, &stored)
                        .is_err());
                    let other = ChangeIdentity::new(ChangeKind::Logical).expect("other identity");
                    assert!(other.validate_header(identity.header()).is_err());
                    assert!(other
                        .open_chunk(&crypto, counts.records, &header, i as u64, &stored)
                        .is_err());
                }
                counts.append(length as u64).expect("count");
            }
            let end = identity.seal_end(&crypto, counts).expect("end");
            identity.validate_end(&crypto, &end, counts).expect("end");
            assert!(identity
                .validate_end(&crypto, &end, ChangeCounts::default())
                .is_err());
            for i in 0..end.len() {
                let mut corrupt = end.clone();
                corrupt[i] ^= 1;
                assert!(identity.validate_end(&crypto, &corrupt, counts).is_err());
                assert!(identity.validate_end(&crypto, &end[..i], counts).is_err());
            }
            let mut trailing = end;
            trailing.push(0);
            assert!(identity.validate_end(&crypto, &trailing, counts).is_err());
        }
    }

    #[test]
    fn pages_and_malformed_records() {
        let identity = ChangeIdentity::new(ChangeKind::Pages).expect("identity");
        assert!(identity.record_header(0).is_err());
        assert!(identity.record_header(4096).is_err());
        let header = identity.record_header(4120).expect("page header");
        assert_eq!(identity.record_length(&header).expect("length"), 4120);
        assert!(identity.chunk_plaintext_size(&header, u64::MAX).is_err());
        for crypto in contexts() {
            let bytes = identity
                .seal_chunk(&crypto, 0, &header, 0, &[13; 4120])
                .expect("seal");
            assert_eq!(
                identity
                    .open_chunk(&crypto, 0, &header, 0, &bytes)
                    .expect("open"),
                vec![13; 4120]
            );
            for i in [0, 4119, 4120, bytes.len() - 1] {
                let mut corrupt = bytes.clone();
                corrupt[i] ^= 1;
                assert!(matches!(
                    identity.open_chunk(&crypto, 0, &header, 0, &corrupt),
                    Err(Error::AuthenticationFailed)
                ));
                assert!(identity
                    .open_chunk(&crypto, 0, &header, 0, &bytes[..i])
                    .is_err());
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
