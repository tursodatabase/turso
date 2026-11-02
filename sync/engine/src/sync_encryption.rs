use turso_core::{CipherMode, ConnectOpts, EncryptionContext, EncryptionKey, EncryptionOpts};

use crate::{errors::Error, Result};

#[derive(Clone, Debug)]
pub struct SyncEncryption {
    cipher: CipherMode,
    local_key: Option<EncryptionKey>,
}

impl SyncEncryption {
    pub fn new(cipher: CipherMode, local_key: Option<EncryptionKey>) -> Result<Self> {
        if cipher == CipherMode::None {
            return Err(Error::DatabaseSyncEngineError(
                "sync encryption requires a cipher".into(),
            ));
        }
        let config = Self { cipher, local_key };
        config.local_context()?;
        Ok(config)
    }

    pub fn from_local_options(options: &EncryptionOpts) -> Result<Self> {
        Self::new(
            CipherMode::try_from(options.cipher.as_str())?,
            Some(EncryptionKey::from_hex_string(&options.hexkey)?),
        )
    }

    pub fn cipher(&self) -> CipherMode {
        self.cipher
    }

    pub fn local_key(&self) -> Option<&EncryptionKey> {
        self.local_key.as_ref()
    }

    pub(crate) fn local_context(&self) -> Result<Option<EncryptionContext>> {
        self.local_key
            .as_ref()
            .map(|key| EncryptionContext::new(self.cipher, key, 4096).map_err(Error::from))
            .transpose()
    }

    pub(crate) fn connect_options(&self) -> ConnectOpts {
        ConnectOpts::new().with_encryption_key(self.local_key.clone())
    }

    pub(crate) fn local_options(&self) -> Option<EncryptionOpts> {
        self.local_key.as_ref().map(|key| EncryptionOpts {
            cipher: self.cipher.to_string(),
            hexkey: key
                .as_slice()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_keys_and_redacts_debug() {
        assert!(SyncEncryption::new(CipherMode::None, None).is_err());
        assert!(
            SyncEncryption::new(CipherMode::Aes256Gcm, Some(EncryptionKey::new_128([1; 16])))
                .is_err()
        );
        let config = SyncEncryption::new(
            CipherMode::Aes256Gcm,
            Some(EncryptionKey::new_256([0xab; 32])),
        )
        .expect("config");
        assert!(!format!("{config:?}").contains(&"ab".repeat(32)));
        assert_eq!(
            config.local_options().expect("local options").hexkey,
            "ab".repeat(32)
        );
        let plaintext =
            SyncEncryption::new(CipherMode::Aes256Gcm, None).expect("plaintext replica");
        assert!(plaintext.local_context().expect("local context").is_none());
    }
}
