use crate::io::{File, FileId};
use crate::sync::{atomic::AtomicUsize, atomic::Ordering, Arc, Mutex};
use crate::{Buffer, Completion, CompletionError, LimboError, OpenFlags, Result, IO};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

pub(super) struct ReadonlyStorage {
    io: Arc<dyn IO>,
    file: Arc<dyn File>,
    path: String,
    wal_path: String,
    log_path: String,
    snapshot: StorageSnapshot,
    lock: Mutex<Option<FileLock>>,
    bootstrapping: Mutex<bool>,
}

#[derive(PartialEq, Eq)]
struct StorageSnapshot {
    main_id: FileId,
    main_size: u64,
    main_header: Vec<u8>,
    main_fingerprint: u64,
    wal: Option<FileSnapshot>,
    log: Option<FileSnapshot>,
}

#[derive(PartialEq, Eq)]
struct FileSnapshot {
    id: FileId,
    size: u64,
    fingerprint: u64,
}

pub(super) struct FileLock {
    file: Arc<dyn File>,
}

impl FileLock {
    fn acquire(file: Arc<dyn File>) -> Result<Self> {
        file.lock_file(true)?;
        Ok(Self { file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock_file();
    }
}

impl ReadonlyStorage {
    pub(super) fn try_new(
        io: Arc<dyn IO>,
        file: Arc<dyn File>,
        path: &str,
        wal_path: &str,
    ) -> Result<Option<Arc<Self>>> {
        if file.file_id()?.is_none() {
            return Ok(None);
        }
        // WAL writers and checkpoints are not excluded by a shared main-file lock.
        let lock = FileLock::acquire(file.clone())?;
        let canonical_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.into());
        let log_path = canonical_path
            .with_extension("db-log")
            .to_string_lossy()
            .into_owned();
        let snapshot = StorageSnapshot::read(&io, &file, path, wal_path, &log_path)?;
        Ok(Some(Arc::new(Self {
            io,
            file,
            path: path.into(),
            wal_path: wal_path.into(),
            log_path,
            snapshot,
            lock: Mutex::new(Some(lock)),
            bootstrapping: Mutex::new(true),
        })))
    }

    pub(super) fn logical_log_path(&self) -> &str {
        &self.log_path
    }

    pub(super) fn verify_log_file(&self, file: &Arc<dyn File>) -> Result<()> {
        let expected = self.snapshot.log.as_ref().ok_or_else(changed_storage)?;
        if file.file_id()? != Some(expected.id) {
            return Err(changed_storage());
        }
        Ok(())
    }

    pub(super) fn finish_bootstrap(&self) -> Result<()> {
        let mut bootstrapping = self.bootstrapping.lock();
        if *bootstrapping {
            let result = self.verify();
            self.lock.lock().take();
            *bootstrapping = false;
            result?;
        }
        Ok(())
    }

    pub(super) fn abort_bootstrap(&self) {
        let mut bootstrapping = self.bootstrapping.lock();
        if *bootstrapping {
            self.lock.lock().take();
            *bootstrapping = false;
        }
    }

    pub(super) fn lock_writer(&self) -> Result<FileLock> {
        if *self.bootstrapping.lock() {
            return Err(LimboError::InvalidArgument(
                "read-only database initialization has not completed".into(),
            ));
        }
        let lock = FileLock::acquire(self.file.clone())?;
        self.verify()?;
        Ok(lock)
    }

    pub(super) fn retain_writer_lock(&self, lock: FileLock) {
        *self.lock.lock() = Some(lock);
    }

    fn verify(&self) -> Result<()> {
        let current = StorageSnapshot::read(
            &self.io,
            &self.file,
            &self.path,
            &self.wal_path,
            &self.log_path,
        )?;
        if current != self.snapshot {
            return Err(changed_storage());
        }
        Ok(())
    }
}

fn changed_storage() -> LimboError {
    LimboError::InvalidArgument(
        "database storage changed while open read-only; close all handles and reopen before writing"
            .into(),
    )
}

impl StorageSnapshot {
    fn read(
        io: &Arc<dyn IO>,
        file: &Arc<dyn File>,
        path: &str,
        wal_path: &str,
        log_path: &str,
    ) -> Result<Self> {
        let main_id = file.file_id()?.ok_or_else(changed_storage)?;
        if io.file_id(path)? != main_id {
            return Err(changed_storage());
        }
        let main_size = file.size()?;
        let main_header = read_bytes(io, file, 0, 512)?;
        let main_header = main_header[..main_header.len().min(100)].to_vec();
        // A WAL checkpoint can change table pages without changing the database header.
        let main_fingerprint = fingerprint_file(io, file, main_size)?;
        if file.size()? != main_size || io.file_id(path)? != main_id {
            return Err(changed_storage());
        }
        let wal = FileSnapshot::read(io, wal_path, &[main_id])?;
        let mut excluded_ids = vec![main_id];
        if let Some(wal) = &wal {
            excluded_ids.push(wal.id);
        }
        let log = if main_header.get(18) == Some(&255) {
            FileSnapshot::read(io, log_path, &excluded_ids)?
        } else {
            None
        };
        Ok(Self {
            main_id,
            main_size,
            main_header,
            main_fingerprint,
            wal,
            log,
        })
    }
}

impl FileSnapshot {
    fn read(io: &Arc<dyn IO>, path: &str, excluded_ids: &[FileId]) -> Result<Option<Self>> {
        let path_id = io.file_id(path).ok();
        // Closing any descriptor of a locked inode releases POSIX process-scoped locks.
        if path_id.is_some_and(|id| excluded_ids.contains(&id)) {
            return Err(changed_storage());
        }
        let file = match io.open_file(path, OpenFlags::ReadOnly, false) {
            Ok(file) => file,
            Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::NotFound,
                _,
            ))) => return Ok(None),
            Err(error) => return Err(error),
        };
        let id = file.file_id()?.ok_or_else(changed_storage)?;
        if path_id != Some(id) || excluded_ids.contains(&id) || io.file_id(path)? != id {
            return Err(changed_storage());
        }
        let size = file.size()?;
        let fingerprint = fingerprint_file(io, &file, size)?;
        if file.size()? != size || io.file_id(path)? != id {
            return Err(changed_storage());
        }
        Ok(Some(Self {
            id,
            size,
            fingerprint,
        }))
    }
}

fn fingerprint_file(io: &Arc<dyn IO>, file: &Arc<dyn File>, size: u64) -> Result<u64> {
    let mut fingerprint = DefaultHasher::new();
    let mut offset = 0;
    while offset < size {
        let bytes = read_bytes(io, file, offset, 64 * 1024)?;
        if bytes.is_empty() {
            return Err(changed_storage());
        }
        let count = (size - offset).min(bytes.len() as u64) as usize;
        fingerprint.write(&bytes[..count]);
        offset += count as u64;
    }
    Ok(fingerprint.finish())
}

fn read_bytes(io: &Arc<dyn IO>, file: &Arc<dyn File>, offset: u64, size: usize) -> Result<Vec<u8>> {
    let buffer = Arc::new(Buffer::new_temporary(size));
    let count = Arc::new(AtomicUsize::new(0));
    let completion = file.pread(
        offset,
        Completion::new_read(buffer.clone(), {
            let count = count.clone();
            move |result| {
                if let Ok((_, bytes)) = result {
                    count.store(bytes as usize, Ordering::Release);
                }
                None
            }
        }),
    )?;
    io.wait_for_completion(completion)?;
    Ok(buffer.as_slice()[..count.load(Ordering::Acquire)].to_vec())
}
