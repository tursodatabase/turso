use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{errors::Error, filesystem::Filesystem, Result};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DatabaseMetadata {
    /// Latest generation from remote which was pulled locally to the Synced DB
    pub synced_generation: usize,
    /// Latest frame number from remote which was pulled locally to the Synced DB
    pub synced_frame_no: usize,
    /// Latest change_id from CDC table in Draft DB which was successfully pushed to the remote through Synced DB
    pub synced_change_id: Option<i64>,
    /// Optional field which will store change_id from CDC table in Draft DB which was successfully transferred to the SyncedDB
    /// but not durably pushed to the remote yet
    ///
    /// This can happen if WAL push will abort in the middle due to network partition, application crash, etc
    pub transferred_change_id: Option<i64>,
    /// pair of frame_no for Draft and Synced DB such that content of the database file up to these frames is identical
    pub draft_wal_match_watermark: u64,
    pub synced_wal_match_watermark: u64,
}

impl DatabaseMetadata {
    pub async fn read_from(fs: &impl Filesystem, path: &Path) -> Result<Option<Self>> {
        tracing::debug!("try read metadata from: {:?}", path);
        if !fs.exists_file(path).await? {
            tracing::debug!("no metadata found at {:?}", path);
            return Ok(None);
        }
        let contents = fs.read_file(path).await?;
        let meta = serde_json::from_slice::<DatabaseMetadata>(&contents[..])?;
        tracing::debug!("read metadata from {:?}: {:?}", path, meta);
        Ok(Some(meta))
    }
    pub async fn write_to(&self, fs: &impl Filesystem, path: &Path) -> Result<()> {
        tracing::debug!("write metadata to {:?}: {:?}", path, self);
        let directory = path.parent().ok_or_else(|| {
            Error::MetadataError(format!(
                "unable to get parent of the provided path: {path:?}",
            ))
        })?;
        let filename = path
            .file_name()
            .and_then(|x| x.to_str())
            .ok_or_else(|| Error::MetadataError(format!("unable to get filename: {path:?}")))?;

        let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
        let timestamp = timestamp.map_err(|e| {
            Error::MetadataError(format!("failed to get current time for temp file: {e}"))
        })?;
        let temp_name = format!("{}.tmp.{}", filename, timestamp.as_nanos());
        let temp_path = directory.join(temp_name);

        let data = serde_json::to_string(self)?;

        let mut temp_file = fs.create_file(&temp_path).await?;
        let mut result = fs.write_file(&mut temp_file, data.as_bytes()).await;
        if result.is_ok() {
            result = fs.sync_file(&temp_file).await;
        }
        drop(temp_file);
        if result.is_ok() {
            result = fs.rename_file(&temp_path, path).await;
        }
        if result.is_err() {
            let _ = fs.remove_file(&temp_path).await.inspect_err(|e| {
                tracing::warn!("failed to remove temp file at {:?}: {}", temp_path, e)
            });
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::{filesystem::tokio::TokioFilesystem, metadata::DatabaseMetadata};

    #[tokio::test]
    pub async fn metadata_simple_test() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db-info");
        let meta = DatabaseMetadata {
            synced_generation: 1,
            synced_frame_no: 2,
            synced_change_id: Some(3),
            transferred_change_id: Some(4),
            draft_wal_match_watermark: 5,
            synced_wal_match_watermark: 6,
        };
        let fs = TokioFilesystem();
        meta.write_to(&fs, &path).await.unwrap();

        let read = DatabaseMetadata::read_from(&fs, &path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(meta, read);
    }
}
