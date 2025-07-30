use std::path::Path;

use turso_sync_protocol::{errors::Error, Result};

use crate::filesystem::Filesystem;

pub async fn read_from(
    fs: &impl Filesystem,
    path: &Path,
) -> Result<Option<turso_sync_protocol::types::DatabaseMetadata>> {
    tracing::debug!("try read metadata from: {:?}", path);
    if !fs.exists_file(path).await? {
        tracing::debug!("no metadata found at {:?}", path);
        return Ok(None);
    }
    let contents = fs.read_file(path).await?;
    let meta = turso_sync_protocol::types::DatabaseMetadata::load(&contents)?;
    tracing::debug!("read metadata from {:?}: {:?}", path, meta);
    Ok(Some(meta))
}

pub async fn write_to(
    metadata: &turso_sync_protocol::types::DatabaseMetadata,
    fs: &impl Filesystem,
    path: &Path,
) -> Result<()> {
    tracing::debug!("write metadata to {:?}: {:?}", path, metadata);
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

    let data = metadata.dump()?;

    let mut temp_file = fs.create_file(&temp_path).await?;
    let mut result = fs.write_file(&mut temp_file, &data).await;
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

#[cfg(test)]
mod tests {
    use turso_sync_protocol::types::DatabaseMetadata;

    use crate::{
        filesystem::tokio::TokioFilesystem,
        metadata::{read_from, write_to},
    };

    #[tokio::test]
    pub async fn metadata_simple_test() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db-info");
        let meta = DatabaseMetadata {
            synced_generation: 1,
            synced_frame_no: Some(2),
            synced_change_id: Some(3),
            transferred_change_id: Some(4),
            draft_wal_match_watermark: 5,
            synced_wal_match_watermark: 6,
        };
        let fs = TokioFilesystem();
        write_to(&meta, &fs, &path).await.unwrap();

        let read = read_from(&fs, &path).await.unwrap().unwrap();
        assert_eq!(meta, read);
    }
}
