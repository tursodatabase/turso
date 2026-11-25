use turso_core::{
    CipherMode, ConnectOpts, EncryptionContext, EncryptionFormat, EncryptionKey, EncryptionOpts,
};

use crate::{errors::Error, types::Secret, Result};

#[derive(Clone, Debug)]
pub struct SyncEncryption {
    cipher: CipherMode,
    local_key: Option<EncryptionKey>,
    cloud_key: Option<Secret>,
}

impl SyncEncryption {
    pub fn new(
        cipher: CipherMode,
        local_key: Option<EncryptionKey>,
        cloud_key: Option<Secret>,
    ) -> Result<Self> {
        if cipher == CipherMode::None {
            return Err(Error::DatabaseSyncEngineError(
                "sync encryption requires a cipher".into(),
            ));
        }
        let config = Self {
            cipher,
            local_key,
            cloud_key,
        };
        config.local_context()?;
        Ok(config)
    }

    pub fn from_options(
        remote_cipher: Option<&str>,
        local_options: Option<&EncryptionOpts>,
        cloud_key: Option<Secret>,
    ) -> Result<Option<Self>> {
        let local = local_options.map(Self::from_local_options).transpose()?;
        let cipher = remote_cipher.map(CipherMode::try_from).transpose()?;
        let mut config = match (local, cipher) {
            (Some(local), Some(cipher)) if local.cipher != cipher => {
                Err(Error::DatabaseSyncEngineError(
                    "sync uses a single cipher for both local and cloud encryption".into(),
                ))
            }
            (Some(local), _) => Ok(Some(local)),
            (None, Some(cipher)) => Self::new(cipher, None, None).map(Some),
            (None, None) if cloud_key.is_some() => Err(Error::DatabaseSyncEngineError(
                "encrypted sync requires an explicit cipher".into(),
            )),
            (None, None) => Ok(None),
        }?;
        if let Some(config) = &mut config {
            config.cloud_key = cloud_key;
        }
        Ok(config)
    }

    pub fn from_local_options(options: &EncryptionOpts) -> Result<Self> {
        Self::new(
            CipherMode::try_from(options.cipher.as_str())?,
            Some(EncryptionKey::from_hex_string(&options.hexkey)?),
            None,
        )
    }

    pub fn cipher(&self) -> CipherMode {
        self.cipher
    }

    pub fn local_key(&self) -> Option<&EncryptionKey> {
        self.local_key.as_ref()
    }

    pub fn cloud_key(&self) -> Option<&Secret> {
        self.cloud_key.as_ref()
    }

    /// Encryption context for the local database, absent when there is no local key.
    pub fn local_context(&self) -> Result<Option<EncryptionContext>> {
        // The sync engine encrypts pages with this context only when a bootstrap writes a new
        // database, so the pages use the format of new databases. Core opens the database later
        // and reads that format from page 1.
        self.local_key
            .as_ref()
            .map(|key| {
                EncryptionContext::new(self.cipher, key, 4096, EncryptionFormat::CURRENT)
                    .map_err(Error::from)
            })
            .transpose()
    }

    /// Connection options using the local key and a superuser session.
    pub fn connect_options(&self) -> ConnectOpts {
        ConnectOpts::new(turso_core::SessionRole::Superuser)
            .with_encryption_key(self.local_key.clone())
    }

    /// Database-open encryption options, absent when there is no local key.
    pub fn local_options(&self) -> Option<EncryptionOpts> {
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

/// Check the public feature list before converting it to core database options.
pub fn validate_experimental_opt_in<'a>(
    has_local_key: bool,
    experimental_features: impl IntoIterator<Item = &'a str>,
) -> std::result::Result<(), &'static str> {
    if has_local_key
        && !experimental_features
            .into_iter()
            .any(|feature| feature == "sync_encryption")
    {
        return Err("local sync encryption requires experimental feature 'sync_encryption'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_key_requires_a_supported_cipher() {
        for cipher in [None, Some("chacha20poly1305"), Some("future-cloud-cipher")] {
            assert!(
                SyncEncryption::from_options(cipher, None, Some(Secret::new("cloud-key"))).is_err()
            );
        }
    }

    #[test]
    fn local_encryption_requires_exact_experimental_feature() {
        assert!(validate_experimental_opt_in(true, ["sync_encryption"]).is_ok());
        for features in [
            vec![],
            vec!["encryption"],
            vec!["sync_encryption_extra"],
            vec!["prefix_sync_encryption"],
            vec!["sync-encryption"],
        ] {
            let error = validate_experimental_opt_in(true, features.iter().copied()).unwrap_err();
            assert_eq!(
                error,
                "local sync encryption requires experimental feature 'sync_encryption'"
            );
            assert!(validate_experimental_opt_in(false, features.iter().copied()).is_ok());
        }
        assert!(
            validate_experimental_opt_in(true, ["views", "encryption", "sync_encryption"]).is_ok()
        );
    }

    #[test]
    fn validates_keys_and_redacts_debug() {
        assert!(SyncEncryption::new(CipherMode::None, None, None).is_err());
        assert!(SyncEncryption::new(
            CipherMode::Aes256Gcm,
            Some(EncryptionKey::new_128([1; 16])),
            None
        )
        .is_err());
        let config = SyncEncryption::new(
            CipherMode::Aes256Gcm,
            Some(EncryptionKey::new_256([0xab; 32])),
            Some(Secret::new("cloud-secret")),
        )
        .expect("config");
        assert!(!format!("{config:?}").contains(&"ab".repeat(32)));
        assert!(!format!("{config:?}").contains("cloud-secret"));
        assert_eq!(
            config.local_options().expect("local options").hexkey,
            "ab".repeat(32)
        );
        let plaintext =
            SyncEncryption::new(CipherMode::Aes256Gcm, None, None).expect("plaintext replica");
        assert!(plaintext.local_context().expect("local context").is_none());
    }
}
