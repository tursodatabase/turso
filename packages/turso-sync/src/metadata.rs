use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{errors::Error, Result};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum DatabasePrimary {
    Draft,
    Clean,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DatabaseSyncMetadata {
    pub(crate) clean_generation: usize,
    pub(crate) clean_frame_no: usize,
    pub(crate) draft_change_id: Option<i64>,
    pub(crate) primary_db: DatabasePrimary,
}

impl DatabaseSyncMetadata {
    pub async fn read_from(path: &Path) -> Result<Option<Self>> {
        tracing::debug!("try read from: {:?}", path);
        let exists = Path::new(&path)
            .try_exists()
            .map_err(|e| Error::MetadataError(format!("existance check error: {}", e)))?;
        if !exists {
            tracing::debug!("no metadata found at {:?}", path);
            return Ok(None);
        }
        let contents = tokio::fs::read(&path)
            .await
            .map_err(|e| Error::MetadataError(format!("read error: {}", e)))?;
        let meta = serde_json::from_slice::<DatabaseSyncMetadata>(&contents[..])
            .map_err(|e| Error::MetadataError(format!("parse error: {}", e)))?;
        tracing::debug!("read from {:?}: {:?}", path, meta);
        Ok(Some(meta))
    }
    pub async fn write_to(&self, path: &Path) -> Result<()> {
        tracing::debug!("write to {:?}: {:?}", path, self);
        let directory = path.parent().ok_or_else(|| {
            Error::MetadataError(format!(
                "unable to get parent of the provided path: {:?}",
                path
            ))
        })?;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                Error::MetadataError(format!("failed to get current time for temp file: {}", e))
            })?;
        let temp_name = format!(".tmp.{}", timestamp.as_micros());
        let temp_path = directory.join(temp_name);

        let mut temp_file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(|e| Error::MetadataError(format!("temp file create error: {}", e)))?;

        let data = serde_json::to_string(self)
            .map_err(|e| Error::MetadataError(format!("failed to serialize metadata: {}", e)))?;

        temp_file
            .write_all(data.as_bytes())
            .await
            .map_err(|e| Error::MetadataError(format!("failed to write metadata: {}", e)))?;

        temp_file
            .sync_all()
            .await
            .map_err(|e| Error::MetadataError(format!("failed to sync metadata: {}", e)))?;

        drop(temp_file);

        tokio::fs::rename(&temp_path, &path)
            .await
            .map_err(|e| Error::MetadataError(format!("failed to move metadata: {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata::{DatabaseSyncMetadata, DatabasePrimary};

    #[tokio::test]
    pub async fn metadata_simple_test() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db-info");
        let meta = DatabaseSyncMetadata {
            clean_generation: 1,
            clean_frame_no: 2,
            draft_change_id: Some(3),
            primary_db: DatabasePrimary::Draft,
        };
        meta.write_to(&path).await.unwrap();

        let read = DatabaseSyncMetadata::read_from(&path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(meta, read);
    }
}
