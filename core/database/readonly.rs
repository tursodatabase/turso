use crate::io::{
    Clock, File, FileId, FileSyncType, MemoryIO, SharedWalLockKind, SharedWalMappedRegion,
};
use crate::sync::{Arc, Mutex};
use crate::{
    Buffer, Completion, CompletionError, LimboError, MonotonicInstant, OpenFlags, Result,
    WallClockInstant, IO,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ptr::NonNull;

pub(super) struct ReadonlyStorage {
    io: Arc<dyn IO>,
    file: Arc<dyn File>,
    path: String,
    main_id: FileId,
    wal_path: String,
    log_path: String,
    lock: Mutex<Option<FileLock>>,
    bootstrapping: Mutex<bool>,
    observations: Mutex<Observations>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Observing,
    Frozen,
    Active,
    Poisoned,
}

struct Observations {
    phase: Phase,
    mutating: bool,
    inflight: usize,
    contradicted: bool,
    paths: HashMap<String, Option<FileId>>,
    missing: HashSet<String>,
    files: HashMap<FileId, ObservedContents>,
    // An alias close can release POSIX locks owned by another descriptor.
    retained: Vec<Arc<dyn File>>,
}

#[derive(Clone)]
struct ObservedContents {
    file: Arc<dyn File>,
    size: Option<u64>,
    reads: BTreeMap<(u64, usize), ReadObservation>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct ReadObservation {
    count: usize,
    checksum: [u8; 16],
}

impl ReadObservation {
    fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            count: bytes.len(),
            checksum: twox_hash::XxHash3_128::oneshot(bytes).to_le_bytes(),
        }
    }
}

struct FrozenObservations {
    paths: HashMap<String, Option<FileId>>,
    missing: HashSet<String>,
    files: HashMap<FileId, ObservedContents>,
}

pub(super) struct FileLock {
    file: Arc<dyn File>,
}

pub(super) struct WriterGuard {
    storage: Arc<ReadonlyStorage>,
    lock: Option<FileLock>,
    published: bool,
    opened_paths: HashMap<String, FileId>,
}

struct Admission {
    storage: Arc<ReadonlyStorage>,
    recording: bool,
}

struct ObservedFile {
    storage: Arc<ReadonlyStorage>,
    file: Arc<dyn File>,
    id: FileId,
}

struct ObservedIO {
    storage: Arc<ReadonlyStorage>,
}

impl ReadonlyStorage {
    pub(super) fn try_new(
        io: Arc<dyn IO>,
        file: Arc<dyn File>,
        path: &str,
        wal_path: &str,
    ) -> Result<Option<Arc<Self>>> {
        let Some(main_id) = file.file_id()? else {
            return Ok(None);
        };
        if io.file_id(path)? != main_id {
            return Err(changed_storage());
        }
        let lock = match FileLock::acquire(file.clone()) {
            Ok(lock) => Some(lock),
            Err(LimboError::LockingError(_)) => None,
            Err(error) => return Err(error),
        };
        let canonical_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.into());
        let log_path = canonical_path
            .with_extension("db-log")
            .to_string_lossy()
            .into_owned();
        let mut paths = HashMap::new();
        paths.insert(path.to_string(), Some(main_id));
        paths.insert(canonical_path.to_string_lossy().into_owned(), Some(main_id));
        let mut files = HashMap::new();
        files.insert(
            main_id,
            ObservedContents {
                file: file.clone(),
                size: None,
                reads: BTreeMap::new(),
            },
        );
        Ok(Some(Arc::new(Self {
            io,
            file,
            path: path.into(),
            main_id,
            wal_path: wal_path.into(),
            log_path,
            lock: Mutex::new(lock),
            bootstrapping: Mutex::new(true),
            observations: Mutex::new(Observations {
                phase: Phase::Observing,
                mutating: false,
                inflight: 0,
                contradicted: false,
                paths,
                missing: HashSet::new(),
                files,
                retained: Vec::new(),
            }),
        })))
    }

    pub(super) fn observed_file(self: &Arc<Self>) -> Arc<dyn File> {
        Arc::new(ObservedFile {
            storage: self.clone(),
            file: self.file.clone(),
            id: self.main_id,
        })
    }

    pub(super) fn observed_io(self: &Arc<Self>) -> Arc<dyn IO> {
        Arc::new(ObservedIO {
            storage: self.clone(),
        })
    }

    pub(super) fn raw_io(&self) -> Arc<dyn IO> {
        self.io.clone()
    }

    pub(super) fn logical_log_path(&self) -> &str {
        &self.log_path
    }

    pub(super) fn verify_log_file(&self, file: &Arc<dyn File>) -> Result<()> {
        let id = file.file_id()?.ok_or_else(changed_storage)?;
        if self.io.file_id(&self.log_path)? != id {
            return Err(changed_storage());
        }
        self.record_identity(&self.log_path, Some(id));
        Ok(())
    }

    pub(super) fn check_health(&self) -> Result<()> {
        match self.observations.lock().phase {
            Phase::Poisoned => Err(poisoned_storage()),
            Phase::Frozen => Err(LimboError::Busy),
            Phase::Observing | Phase::Active => Ok(()),
        }
    }

    pub(super) fn is_active(&self) -> bool {
        self.observations.lock().phase == Phase::Active
    }

    pub(super) fn finish_bootstrap(&self) -> Result<()> {
        let mut bootstrapping = self.bootstrapping.lock();
        if !*bootstrapping {
            return Ok(());
        }
        let result = if self.lock.lock().is_some() {
            self.verify()
        } else {
            Ok(())
        };
        self.lock.lock().take();
        *bootstrapping = false;
        result
    }

    pub(super) fn abort_bootstrap(&self) {
        let mut bootstrapping = self.bootstrapping.lock();
        if *bootstrapping {
            self.lock.lock().take();
            *bootstrapping = false;
        }
    }

    pub(super) fn freeze_validate(self: &Arc<Self>) -> Result<WriterGuard> {
        if *self.bootstrapping.lock() {
            return Err(LimboError::InvalidArgument(
                "read-only database initialization has not completed".into(),
            ));
        }
        let snapshot = self.freeze()?;
        let lock = match FileLock::acquire(self.file.clone()) {
            Ok(lock) => lock,
            Err(error) => {
                self.observations.lock().phase = Phase::Observing;
                return Err(error);
            }
        };
        let guard = WriterGuard {
            storage: self.clone(),
            lock: Some(lock),
            published: false,
            opened_paths: HashMap::new(),
        };
        self.validate(snapshot)?;
        Ok(guard)
    }

    pub(super) fn retain_writer_lock(&self, lock: FileLock) {
        *self.lock.lock() = Some(lock);
        let mut observations = self.observations.lock();
        observations.phase = Phase::Active;
        observations.mutating = false;
        observations.paths.clear();
        observations.missing.clear();
        observations.files.clear();
    }

    fn verify(&self) -> Result<()> {
        let snapshot = self.freeze()?;
        let result = self.validate(snapshot);
        let mut observations = self.observations.lock();
        observations.phase = Phase::Observing;
        result
    }

    fn freeze(&self) -> Result<FrozenObservations> {
        let mut observations = self.observations.lock();
        match observations.phase {
            Phase::Poisoned => return Err(poisoned_storage()),
            Phase::Frozen => return Err(LimboError::Busy),
            Phase::Active => {
                return Err(LimboError::InvalidArgument(
                    "writer storage is already active".into(),
                ))
            }
            Phase::Observing => {}
        }
        if observations.inflight != 0 {
            return Err(LimboError::Busy);
        }
        if observations.contradicted {
            return Err(changed_storage());
        }
        observations.phase = Phase::Frozen;
        Ok(FrozenObservations {
            paths: observations.paths.clone(),
            missing: observations.missing.clone(),
            files: observations.files.clone(),
        })
    }

    fn validate(&self, snapshot: FrozenObservations) -> Result<()> {
        for (path, expected) in snapshot.paths {
            let current = self.path_identity(&path)?;
            if current != expected
                && !self.observed_empty_wal_was_removed(&path, expected, &snapshot.files)?
            {
                return Err(self.changed_observations());
            }
        }
        for path in snapshot.missing {
            match self
                .io
                .open_file(&path, OpenFlags::ReadOnly | OpenFlags::NoLock, false)
            {
                Ok(file) => {
                    self.observations.lock().retained.push(file);
                    return Err(self.changed_observations());
                }
                Err(LimboError::CompletionError(CompletionError::IOError(
                    std::io::ErrorKind::NotFound,
                    _,
                ))) => {}
                Err(error) => return Err(error),
            }
        }
        for (id, contents) in snapshot.files {
            if contents.file.file_id()? != Some(id) {
                return Err(self.changed_observations());
            }
            if let Some(size) = contents.size {
                if contents.file.size()? != size {
                    return Err(self.changed_observations());
                }
            }
            for ((offset, requested), expected) in contents.reads {
                let current = read_bytes(&self.io, &contents.file, offset, requested)?;
                if ReadObservation::from_bytes(&current) != expected {
                    return Err(self.changed_observations());
                }
            }
        }
        Ok(())
    }

    fn path_identity(&self, path: &str) -> Result<Option<FileId>> {
        match self.io.file_id(path) {
            Ok(id) => Ok(Some(id)),
            Err(identity_error) => {
                match self
                    .io
                    .open_file(path, OpenFlags::ReadOnly | OpenFlags::NoLock, false)
                {
                    Err(LimboError::CompletionError(CompletionError::IOError(
                        std::io::ErrorKind::NotFound,
                        _,
                    ))) => Ok(None),
                    Ok(file) => {
                        self.observations.lock().retained.push(file);
                        Err(identity_error)
                    }
                    Err(_) => Err(identity_error),
                }
            }
        }
    }

    fn changed_observations(&self) -> LimboError {
        self.observations.lock().contradicted = true;
        changed_storage()
    }

    fn observed_empty_wal_was_removed(
        &self,
        path: &str,
        expected: Option<FileId>,
        files: &HashMap<FileId, ObservedContents>,
    ) -> Result<bool> {
        if path != self.wal_path {
            return Ok(false);
        }
        let Some(contents) = expected.and_then(|id| files.get(&id)) else {
            return Ok(false);
        };
        if contents.size != Some(0) || contents.reads.values().any(|read| read.count != 0) {
            return Ok(false);
        }
        match self
            .io
            .open_file(path, OpenFlags::ReadOnly | OpenFlags::NoLock, false)
        {
            Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::NotFound,
                _,
            ))) => Ok(true),
            Ok(file) => {
                self.observations.lock().retained.push(file);
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    fn admit(self: &Arc<Self>) -> Result<Admission> {
        let mut observations = self.observations.lock();
        let recording = match observations.phase {
            Phase::Observing => true,
            Phase::Active => false,
            Phase::Frozen => return Err(LimboError::Busy),
            Phase::Poisoned => return Err(poisoned_storage()),
        };
        if recording {
            observations.inflight += 1;
        }
        Ok(Admission {
            storage: self.clone(),
            recording,
        })
    }

    fn record_identity(&self, path: &str, id: Option<FileId>) {
        let mut observations = self.observations.lock();
        if observations.phase != Phase::Observing {
            return;
        }
        if let Some(previous) = observations.paths.get(path) {
            if *previous != id {
                observations.contradicted = true;
            }
        } else {
            observations.paths.insert(path.to_string(), id);
        }
    }

    fn record_file(&self, id: FileId, file: Arc<dyn File>) {
        let mut observations = self.observations.lock();
        if observations.phase == Phase::Observing {
            observations
                .files
                .entry(id)
                .or_insert_with(|| ObservedContents {
                    file: file.clone(),
                    size: None,
                    reads: BTreeMap::new(),
                });
            observations.retained.push(file);
        }
    }

    fn record_size(&self, id: FileId, size: u64) {
        let mut observations = self.observations.lock();
        if observations.phase != Phase::Observing {
            return;
        }
        if let Some(contents) = observations.files.get_mut(&id) {
            if contents.size.is_some_and(|previous| previous != size) {
                observations.contradicted = true;
            } else {
                contents.size = Some(size);
            }
        } else {
            observations.contradicted = true;
        }
    }

    fn record_read(&self, id: FileId, offset: u64, requested: usize, bytes: &[u8]) {
        let mut observations = self.observations.lock();
        if observations.phase != Phase::Observing {
            observations.contradicted = true;
            return;
        }
        if let Some(contents) = observations.files.get_mut(&id) {
            let current = ReadObservation::from_bytes(bytes);
            match contents.reads.get(&(offset, requested)) {
                Some(previous) if *previous != current => observations.contradicted = true,
                Some(_) => {}
                None => {
                    contents.reads.insert((offset, requested), current);
                }
            }
        } else {
            observations.contradicted = true;
        }
    }

    fn invalidate(&self) {
        self.observations.lock().contradicted = true;
    }

    fn tracked_path(&self, path: &str) -> bool {
        path == self.path
            || path == self.wal_path
            || path == self.log_path
            || self.observations.lock().paths.contains_key(path)
    }

    fn sidecar_alias(&self, path: &str, id: Option<FileId>) -> bool {
        if path != self.wal_path && path != self.log_path {
            return false;
        }
        let Some(id) = id else {
            return false;
        };
        if id == self.main_id {
            return true;
        }
        let other = if path == self.wal_path {
            &self.log_path
        } else {
            &self.wal_path
        };
        self.observations
            .lock()
            .paths
            .get(other)
            .is_some_and(|previous| *previous == Some(id))
    }

    fn may_write(&self) -> Result<()> {
        let observations = self.observations.lock();
        match observations.phase {
            Phase::Active => Ok(()),
            Phase::Frozen if observations.mutating => Ok(()),
            Phase::Frozen => Err(LimboError::Busy),
            Phase::Poisoned => Err(poisoned_storage()),
            Phase::Observing => Err(LimboError::ReadOnly),
        }
    }
}

impl WriterGuard {
    pub(super) fn mark_mutating(&mut self) {
        self.storage.observations.lock().mutating = true;
    }

    pub(super) fn open_file(
        &mut self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        if flags.contains(OpenFlags::Create) && !flags.contains(OpenFlags::ReadOnly) {
            self.mark_mutating();
        }
        let file = self.storage.io.open_file(path, flags, direct)?;
        self.storage.observations.lock().retained.push(file.clone());
        let id = file.file_id()?.ok_or_else(changed_storage)?;
        if id == self.storage.main_id
            || self.storage.sidecar_alias(path, Some(id))
            || self
                .opened_paths
                .iter()
                .any(|(previous_path, previous_id)| previous_path != path && *previous_id == id)
            || self.storage.io.file_id(path)? != id
        {
            return Err(changed_storage());
        }
        self.opened_paths.insert(path.to_string(), id);
        Ok(file)
    }

    pub(super) fn commit(mut self) {
        let lock = self.lock.take().expect("writer guard owns the file lock");
        self.storage.retain_writer_lock(lock);
        self.published = true;
    }
}

impl Drop for WriterGuard {
    fn drop(&mut self) {
        if !self.published {
            drop(self.lock.take());
            let mut observations = self.storage.observations.lock();
            observations.phase = if observations.mutating {
                Phase::Poisoned
            } else {
                Phase::Observing
            };
            observations.mutating = false;
        }
    }
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

impl Drop for Admission {
    fn drop(&mut self) {
        if self.recording {
            let mut observations = self.storage.observations.lock();
            observations.inflight -= 1;
        }
    }
}

impl File for ObservedFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        if self.storage.observations.lock().phase == Phase::Frozen {
            return Ok(Some(self.id));
        }
        let admission = self.storage.admit()?;
        let result = self.file.file_id();
        if admission.recording && result.as_ref().ok().copied() != Some(Some(self.id)) {
            self.storage.invalidate();
        }
        result
    }

    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let admission = self.storage.admit()?;
        if !admission.recording {
            return self.file.pread(pos, c);
        }
        let requested = c.as_read().buf().len();
        let buffer = c.as_read().buf_arc();
        let storage = self.storage.clone();
        let id = self.id;
        let pending = Arc::new(Mutex::new(Some(admission)));
        let pending_callback = pending.clone();
        let completion = Completion::new_read(buffer, move |result| {
            let admission = pending_callback.lock().take();
            match result {
                Ok((buffer, bytes)) if bytes >= 0 && bytes as usize <= requested => {
                    storage.record_read(id, pos, requested, &buffer.as_slice()[..bytes as usize]);
                    c.complete(bytes);
                }
                Ok(_) => {
                    storage.invalidate();
                    c.error(CompletionError::IOError(
                        std::io::ErrorKind::InvalidData,
                        "invalid observed read length",
                    ));
                }
                Err(error) => {
                    storage.invalidate();
                    c.error(error);
                }
            }
            let error = c.get_error();
            if error == Some(CompletionError::Aborted) {
                storage.invalidate();
            }
            drop(admission);
            error
        });
        let result = self.file.pread(pos, completion);
        if result.is_err() {
            self.storage.invalidate();
            pending.lock().take();
        }
        result
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.storage.may_write()?;
        self.file.pwrite(pos, buffer, c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        self.storage.may_write()?;
        self.file.pwritev(pos, buffers, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.storage.may_write()?;
        self.file.sync(c, sync_type)
    }
    fn size(&self) -> Result<u64> {
        let admission = self.storage.admit()?;
        let size = match self.file.size() {
            Ok(size) => size,
            Err(error) => {
                if admission.recording {
                    self.storage.invalidate();
                }
                return Err(error);
            }
        };
        if admission.recording {
            self.storage.record_size(self.id, size);
        }
        Ok(size)
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.storage.may_write()?;
        self.file.truncate(len, c)
    }
    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        let admission = self.storage.admit()?;
        if admission.recording {
            self.storage.invalidate();
        }
        self.file.has_hole(pos, len)
    }
    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        self.storage.may_write()?;
        self.file.punch_hole(pos, len)
    }
    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        self.file.shared_wal_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.file.shared_wal_try_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_probe_exclusive_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.file.shared_wal_probe_exclusive_byte(offset, kind)
    }
    fn shared_wal_probe_exclusive_while_shared_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.file
            .shared_wal_probe_exclusive_while_shared_byte(offset, kind)
    }
    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        self.file.shared_wal_unlock_byte(offset, kind)
    }
    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.storage.may_write()?;
        self.file.shared_wal_set_len(len)
    }
    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        let admission = self.storage.admit()?;
        if admission.recording {
            self.storage.invalidate();
        }
        self.file.shared_wal_map(offset, len)
    }
}

impl Clock for ObservedIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.storage.io.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.storage.io.current_time_wall_clock()
    }
}

impl IO for ObservedIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let admission = self.storage.admit()?;
        if !admission.recording {
            return self.storage.io.open_file(path, flags, direct);
        }
        let tracked_path = self.storage.tracked_path(path);
        if self
            .storage
            .sidecar_alias(path, self.storage.io.file_id(path).ok())
        {
            self.storage.invalidate();
            return Err(changed_storage());
        }
        if tracked_path && flags.contains(OpenFlags::Create) && !flags.contains(OpenFlags::ReadOnly)
        {
            return Err(LimboError::ReadOnly);
        }
        match self.storage.io.open_file(path, flags, direct) {
            Ok(file) => {
                let id = match file.file_id()? {
                    Some(id) => id,
                    None if tracked_path => {
                        self.storage.invalidate();
                        return Ok(file);
                    }
                    None => return Ok(file),
                };
                let tracked_id = self.storage.observations.lock().files.contains_key(&id);
                if !tracked_path && !tracked_id {
                    return Ok(file);
                }
                self.storage.record_identity(path, Some(id));
                self.storage.record_file(id, file.clone());
                if self.storage.sidecar_alias(path, Some(id)) {
                    self.storage.invalidate();
                    return Err(changed_storage());
                }
                Ok(Arc::new(ObservedFile {
                    storage: self.storage.clone(),
                    file,
                    id,
                }))
            }
            Err(error) => {
                if tracked_path {
                    match error {
                        LimboError::CompletionError(CompletionError::IOError(
                            std::io::ErrorKind::NotFound,
                            _,
                        )) => {
                            self.storage.record_identity(path, None);
                            self.storage
                                .observations
                                .lock()
                                .missing
                                .insert(path.to_string());
                        }
                        _ => self.storage.invalidate(),
                    }
                }
                Err(error)
            }
        }
    }
    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        self.open_file(path, OpenFlags::Create | OpenFlags::NoLock, false)
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        if self.storage.tracked_path(path) {
            self.storage.may_write()?;
        }
        self.storage.io.remove_file(path)
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        self.storage.io.supports_shared_wal_coordination()
    }
    fn step(&self) -> Result<()> {
        self.storage.io.step()
    }
    fn cancel(&self, c: &[Completion]) -> Result<()> {
        self.storage.io.cancel(c)
    }
    fn drain_completions(&self, c: &[Completion]) -> Result<()> {
        self.storage.io.drain_completions(c)
    }
    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        self.storage.io.wait_for_completion(c)
    }
    fn generate_random_number(&self) -> i64 {
        self.storage.io.generate_random_number()
    }
    fn fill_bytes(&self, dest: &mut [u8]) {
        self.storage.io.fill_bytes(dest)
    }
    fn get_memory_io(&self) -> Arc<MemoryIO> {
        self.storage.io.get_memory_io()
    }
    fn register_fixed_buffer(&self, ptr: NonNull<u8>, len: usize) -> Result<u32> {
        self.storage.io.register_fixed_buffer(ptr, len)
    }
    fn yield_now(&self) {
        self.storage.io.yield_now();
    }
    fn sleep(&self, duration: std::time::Duration) {
        self.storage.io.sleep(duration);
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        let admission = self.storage.admit()?;
        let result = self.storage.io.file_id(path);
        if admission.recording && self.storage.tracked_path(path) {
            self.storage
                .record_identity(path, result.as_ref().ok().copied());
        }
        result
    }
}

fn changed_storage() -> LimboError {
    LimboError::InvalidArgument("database storage changed while open read-only; close all handles and reopen before writing".into())
}

fn poisoned_storage() -> LimboError {
    LimboError::InvalidArgument(
        "writer activation failed after changing storage; close all handles and reopen".into(),
    )
}

fn read_bytes(io: &Arc<dyn IO>, file: &Arc<dyn File>, offset: u64, size: usize) -> Result<Vec<u8>> {
    let buffer = Arc::new(Buffer::new_temporary(size));
    let count = Arc::new(Mutex::new(None));
    let completion = file.pread(
        offset,
        Completion::new_read(buffer.clone(), {
            let count = count.clone();
            move |result| {
                if let Ok((_, bytes)) = result {
                    *count.lock() = Some(bytes);
                }
                None
            }
        }),
    )?;
    io.wait_for_completion(completion)?;
    let bytes = (*count.lock()).ok_or_else(changed_storage)?;
    if bytes < 0 || bytes as usize > size {
        return Err(changed_storage());
    }
    Ok(buffer.as_slice()[..bytes as usize].to_vec())
}
#[cfg(all(test, feature = "fs", not(miri), not(shuttle)))]
mod tests {
    use super::*;
    use crate::io::FileSyncType;
    use crate::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn writer_guard_keeps_reads_frozen_until_file_unlock_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unlock.db");
        std::fs::write(&path, [3; 100]).unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::PlatformIO::new().unwrap());
        let slot = Arc::new(Mutex::new(None));
        let file: Arc<dyn File> = Arc::new(UnlockProbe {
            inner: io
                .open_file(path.to_str().unwrap(), OpenFlags::NoLock, false)
                .unwrap(),
            storage: slot.clone(),
        });
        let storage = ReadonlyStorage::try_new(
            io,
            file,
            path.to_str().unwrap(),
            &format!("{}-wal", path.display()),
        )
        .unwrap()
        .unwrap();
        storage.finish_bootstrap().unwrap();
        *slot.lock() = Some(Arc::downgrade(&storage));
        drop(storage.freeze_validate().unwrap());
        *slot.lock() = None;
    }

    struct UnlockProbe {
        inner: Arc<dyn File>,
        storage: Arc<Mutex<Option<crate::sync::Weak<ReadonlyStorage>>>>,
    }
    impl File for UnlockProbe {
        fn file_id(&self) -> Result<Option<FileId>> {
            self.inner.file_id()
        }
        fn lock_file(&self, exclusive: bool) -> Result<()> {
            self.inner.lock_file(exclusive)
        }
        fn unlock_file(&self) -> Result<()> {
            let storage = self
                .storage
                .lock()
                .as_ref()
                .and_then(crate::sync::Weak::upgrade);
            if let Some(storage) = storage {
                let file = storage.observed_file();
                let c = Completion::new_read(Arc::new(Buffer::new_temporary(1)), |_| None);
                assert!(matches!(file.pread(0, c), Err(LimboError::Busy)));
            }
            self.inner.unlock_file()
        }
        fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
            self.inner.pread(pos, c)
        }
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.inner.pwrite(pos, buffer, c)
        }
        fn sync(&self, c: Completion, kind: FileSyncType) -> Result<Completion> {
            self.inner.sync(c, kind)
        }
        fn size(&self) -> Result<u64> {
            self.inner.size()
        }
        fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
            self.inner.truncate(len, c)
        }
    }

    #[test]
    fn verified_log_descriptor_does_not_keep_writer_authority_alive() {
        let fixture = Fixture::new(vec![3; 100]);
        std::fs::write(fixture.path.with_extension("db-log"), [9; 32]).unwrap();
        let observed_io = fixture.storage.observed_io();
        let log = observed_io
            .open_file(
                fixture.storage.logical_log_path(),
                OpenFlags::ReadOnly | OpenFlags::NoLock,
                false,
            )
            .unwrap();
        fixture.storage.verify_log_file(&log).unwrap();
        let weak = Arc::downgrade(&fixture.storage);
        fixture.storage.freeze_validate().unwrap().commit();
        drop(log);
        drop(observed_io);
        drop(fixture);
        assert!(
            weak.upgrade().is_none(),
            "observed log retained its own storage authority"
        );
    }

    #[test]
    fn disappearance_of_observed_empty_wal_does_not_invalidate_read_data() {
        let fixture = Fixture::new(vec![3; 100]);
        let wal_path = format!("{}-wal", fixture.path.display());
        std::fs::write(&wal_path, []).unwrap();
        let io = fixture.storage.observed_io();
        let wal = io
            .open_file(&wal_path, OpenFlags::ReadOnly | OpenFlags::NoLock, false)
            .unwrap();
        assert_eq!(wal.size().unwrap(), 0);
        assert!(read_bytes(&io, &wal, 0, 32).unwrap().is_empty());
        std::fs::remove_file(&wal_path).unwrap();
        fixture.storage.freeze_validate().unwrap().commit();
    }

    #[test]
    fn readonly_storage_setup_does_not_read_unused_database_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("main.db");
        {
            let seed = std::fs::File::create(&path).unwrap();
            seed.set_len(32 * 1024 * 1024).unwrap();
        }
        let io: Arc<dyn IO> = Arc::new(crate::PlatformIO::new().unwrap());
        let bytes_read = Arc::new(AtomicUsize::new(0));
        let file: Arc<dyn File> = Arc::new(CountingFile {
            inner: io
                .open_file(path.to_str().unwrap(), OpenFlags::NoLock, false)
                .unwrap(),
            bytes_read: bytes_read.clone(),
        });
        let storage = ReadonlyStorage::try_new(
            io,
            file,
            path.to_str().unwrap(),
            &format!("{}-wal", path.display()),
        )
        .unwrap()
        .unwrap();
        storage.finish_bootstrap().unwrap();
        assert_eq!(bytes_read.load(Ordering::Acquire), 0);
    }

    #[test]
    fn transient_validation_failures_allow_retry_without_discarding_read_history() {
        for fault in 1..=5 {
            let fixture = Fixture::new(vec![3; 100]);
            let pending = Arc::new(AtomicUsize::new(0));
            let file: Arc<dyn File> = Arc::new(ValidationFaultFile {
                inner: fixture.storage.file.clone(),
                pending: pending.clone(),
            });
            let io: Arc<dyn IO> = Arc::new(ValidationFaultIO {
                inner: fixture.io.clone(),
                pending: pending.clone(),
                path: fixture.path.to_string_lossy().into_owned(),
            });
            let storage = ReadonlyStorage::try_new(
                io.clone(),
                file,
                fixture.path.to_str().unwrap(),
                &fixture.storage.wal_path,
            )
            .unwrap()
            .unwrap();
            storage.finish_bootstrap().unwrap();
            let observed = storage.observed_file();
            assert!(storage
                .observed_io()
                .open_file(&storage.wal_path, OpenFlags::ReadOnly, false)
                .is_err());
            assert_eq!(observed.size().unwrap(), 100);
            assert_eq!(read_bytes(&io, &observed, 8, 16).unwrap(), vec![3; 16]);
            pending.store(fault, Ordering::Release);
            assert!(storage.freeze_validate().is_err(), "fault {fault}");
            assert_eq!(pending.load(Ordering::Acquire), 0, "fault {fault}");
            drop(storage.freeze_validate().unwrap());
            fixture.write(8, &[7; 16]);
            assert!(
                storage.freeze_validate().is_err(),
                "lost history after fault {fault}"
            );
            fixture.write(8, &[3; 16]);
            assert!(
                storage.freeze_validate().is_err(),
                "forgot mismatch after fault {fault}"
            );
        }
    }

    fn fail_validation(pending: &AtomicUsize, fault: usize) -> Result<()> {
        if pending
            .compare_exchange(fault, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Err(CompletionError::IOError(
                std::io::ErrorKind::Interrupted,
                "injected transient validation failure",
            )
            .into());
        }
        Ok(())
    }

    struct ValidationFaultFile {
        inner: Arc<dyn File>,
        pending: Arc<AtomicUsize>,
    }

    impl File for ValidationFaultFile {
        fn file_id(&self) -> Result<Option<FileId>> {
            fail_validation(&self.pending, 3)?;
            self.inner.file_id()
        }
        fn lock_file(&self, exclusive: bool) -> Result<()> {
            self.inner.lock_file(exclusive)
        }
        fn unlock_file(&self) -> Result<()> {
            self.inner.unlock_file()
        }
        fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
            fail_validation(&self.pending, 1)?;
            self.inner.pread(pos, c)
        }
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.inner.pwrite(pos, buffer, c)
        }
        fn sync(&self, c: Completion, kind: FileSyncType) -> Result<Completion> {
            self.inner.sync(c, kind)
        }
        fn size(&self) -> Result<u64> {
            fail_validation(&self.pending, 2)?;
            self.inner.size()
        }
        fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
            self.inner.truncate(len, c)
        }
    }

    struct ValidationFaultIO {
        inner: Arc<dyn IO>,
        pending: Arc<AtomicUsize>,
        path: String,
    }

    impl Clock for ValidationFaultIO {
        fn current_time_monotonic(&self) -> MonotonicInstant {
            self.inner.current_time_monotonic()
        }
        fn current_time_wall_clock(&self) -> WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }

    impl IO for ValidationFaultIO {
        fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
            fail_validation(&self.pending, 5)?;
            self.inner.open_file(path, flags, direct)
        }
        fn remove_file(&self, path: &str) -> Result<()> {
            self.inner.remove_file(path)
        }
        fn file_id(&self, path: &str) -> Result<FileId> {
            if path == self.path {
                fail_validation(&self.pending, 4)?;
            }
            self.inner.file_id(path)
        }
        fn step(&self) -> Result<()> {
            self.inner.step()
        }
    }

    #[test]
    fn records_reads_after_bootstrap_and_rejects_changed_table_bytes() {
        let fixture = Fixture::new(vec![3; 8192]);
        let file = fixture.storage.observed_file();
        assert_eq!(
            read_bytes(&fixture.io, &file, 4096, 32).unwrap(),
            vec![3; 32]
        );
        fixture.write(4096, &[7; 32]);
        assert!(fixture.storage.freeze_validate().is_err());
        assert_eq!(std::fs::read(&fixture.path).unwrap()[..100], [3; 100]);
    }

    #[test]
    fn records_eof_and_missing_sidecar_observations() {
        let fixture = Fixture::new(vec![3; 100]);
        let file = fixture.storage.observed_file();
        assert!(read_bytes(&fixture.io, &file, 100, 32).unwrap().is_empty());
        fixture.write(100, &[7; 16]);
        assert!(fixture.storage.freeze_validate().is_err());

        let fixture = Fixture::new(vec![3; 100]);
        let io = fixture.storage.observed_io();
        assert!(io
            .open_file(&fixture.storage.wal_path, OpenFlags::ReadOnly, false)
            .is_err());
        std::fs::write(&fixture.storage.wal_path, b"a new WAL").unwrap();
        assert!(fixture.storage.freeze_validate().is_err());
    }

    #[test]
    fn deduplicates_equal_reads_and_preserves_overlapping_history() {
        let fixture = Fixture::new(vec![3; 100]);
        let file = fixture.storage.observed_file();
        for _ in 0..10 {
            assert_eq!(read_bytes(&fixture.io, &file, 8, 16).unwrap(), vec![3; 16]);
        }
        assert_eq!(
            fixture.storage.observations.lock().files[&fixture.storage.main_id]
                .reads
                .len(),
            1
        );
        fixture.write(12, &[7; 4]);
        assert_eq!(read_bytes(&fixture.io, &file, 12, 4).unwrap(), vec![7; 4]);
        assert!(fixture.storage.freeze_validate().is_err());
    }

    #[test]
    fn changed_repeated_read_cannot_replace_the_original_observation() {
        let fixture = Fixture::new(vec![3; 100]);
        let file = fixture.storage.observed_file();
        assert_eq!(read_bytes(&fixture.io, &file, 8, 16).unwrap(), vec![3; 16]);
        fixture.write(8, &[7; 16]);
        assert_eq!(read_bytes(&fixture.io, &file, 8, 16).unwrap(), vec![7; 16]);
        fixture.write(8, &[3; 16]);
        assert!(fixture.storage.freeze_validate().is_err());
    }

    #[test]
    fn freeze_rejects_new_reads_and_pre_mutation_drop_allows_retry() {
        let fixture = Fixture::new(vec![3; 100]);
        let file = fixture.storage.observed_file();
        let guard = fixture.storage.freeze_validate().unwrap();
        assert!(matches!(file.size(), Err(LimboError::Busy)));
        assert!(matches!(
            file.pread(
                0,
                Completion::new_read(Arc::new(Buffer::new_temporary(8)), |_| None)
            ),
            Err(LimboError::Busy)
        ));
        assert!(matches!(
            fixture.storage.freeze_validate(),
            Err(LimboError::Busy)
        ));
        drop(guard);
        assert_eq!(file.size().unwrap(), 100);
        drop(fixture.storage.freeze_validate().unwrap());
    }

    #[test]
    fn mutation_failure_poison_and_successful_publication_are_distinct() {
        let fixture = Fixture::new(vec![3; 100]);
        let file = fixture.storage.observed_file();
        let mut guard = fixture.storage.freeze_validate().unwrap();
        guard.mark_mutating();
        drop(guard);
        assert!(fixture.storage.check_health().is_err());
        assert!(file.size().is_err());
        assert!(!fixture.storage.is_active());

        let fixture = Fixture::new(vec![3; 100]);
        let file = fixture.storage.observed_file();
        read_bytes(&fixture.io, &file, 0, 32).unwrap();
        let mut guard = fixture.storage.freeze_validate().unwrap();
        guard.mark_mutating();
        guard.commit();
        assert!(fixture.storage.is_active());
        assert!(fixture.storage.observations.lock().files.is_empty());
        fixture.storage.check_health().unwrap();
        assert_eq!(read_bytes(&fixture.io, &file, 0, 32).unwrap(), vec![3; 32]);
    }

    #[test]
    fn read_callback_runs_before_admission_is_released() {
        let fixture = Fixture::new(vec![3; 100]);
        let file = fixture.storage.observed_file();
        let storage = fixture.storage.clone();
        let completion = file
            .pread(
                0,
                Completion::new_read(Arc::new(Buffer::new_temporary(8)), move |_| {
                    assert!(matches!(storage.freeze_validate(), Err(LimboError::Busy)));
                    None
                }),
            )
            .unwrap();
        fixture.io.wait_for_completion(completion).unwrap();
        drop(fixture.storage.freeze_validate().unwrap());
    }

    #[test]
    fn callback_errors_reach_both_completions_without_discarding_valid_read_evidence() {
        let fixture = Fixture::new(vec![3; 100]);
        let file = fixture.storage.observed_file();
        let expected = CompletionError::ShortRead {
            page_idx: 1,
            expected: 8,
            actual: 4,
        };
        let original =
            Completion::new_read(Arc::new(Buffer::new_temporary(8)), move |_| Some(expected));
        let completion = file.pread(0, original.clone()).unwrap();
        assert!(fixture.io.wait_for_completion(completion.clone()).is_err());
        assert_eq!(original.get_error(), Some(expected));
        assert_eq!(completion.get_error(), Some(expected));
        drop(fixture.storage.freeze_validate().unwrap());
    }

    #[test]
    fn queued_reads_prevent_freeze_until_the_actual_completion() {
        let fixture = Fixture::with_delayed_reads(vec![3; 100]);
        let file = fixture.storage.observed_file();
        let completion = file
            .pread(
                0,
                Completion::new_read(Arc::new(Buffer::new_temporary(8)), |_| None),
            )
            .unwrap();
        assert!(!completion.finished());
        assert!(matches!(
            fixture.storage.freeze_validate(),
            Err(LimboError::Busy)
        ));
        fixture.io.step().unwrap();
        assert!(completion.succeeded());
        drop(fixture.storage.freeze_validate().unwrap());
    }

    struct Fixture {
        storage: Arc<ReadonlyStorage>,
        io: Arc<dyn IO>,
        path: std::path::PathBuf,
        _dir: tempfile::TempDir,
    }

    impl Fixture {
        fn new(bytes: Vec<u8>) -> Self {
            Self::create(bytes, false)
        }
        fn with_delayed_reads(bytes: Vec<u8>) -> Self {
            Self::create(bytes, true)
        }
        fn create(bytes: Vec<u8>, delayed: bool) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("main.db");
            std::fs::write(&path, bytes).unwrap();
            let platform: Arc<dyn IO> = Arc::new(crate::PlatformIO::new().unwrap());
            let file = platform
                .open_file(path.to_str().unwrap(), OpenFlags::NoLock, false)
                .unwrap();
            let (io, file): (Arc<dyn IO>, Arc<dyn File>) = if delayed {
                let queue = Arc::new(Mutex::new(Vec::new()));
                (
                    Arc::new(DelayedIO {
                        inner: platform,
                        queue: queue.clone(),
                    }),
                    Arc::new(DelayedFile { inner: file, queue }),
                )
            } else {
                (platform, file)
            };
            let storage = ReadonlyStorage::try_new(
                io.clone(),
                file,
                path.to_str().unwrap(),
                &format!("{}-wal", path.display()),
            )
            .unwrap()
            .unwrap();
            storage.finish_bootstrap().unwrap();
            Self {
                storage,
                io,
                path,
                _dir: dir,
            }
        }
        fn write(&self, offset: u64, bytes: &[u8]) {
            use std::io::{Seek, Write};
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&self.path)
                .unwrap();
            file.seek(std::io::SeekFrom::Start(offset)).unwrap();
            file.write_all(bytes).unwrap();
        }
    }

    struct ScheduledRead {
        file: Arc<dyn File>,
        offset: u64,
        completion: Completion,
    }
    struct DelayedFile {
        inner: Arc<dyn File>,
        queue: Arc<Mutex<Vec<ScheduledRead>>>,
    }
    impl File for DelayedFile {
        fn file_id(&self) -> Result<Option<FileId>> {
            self.inner.file_id()
        }
        fn lock_file(&self, exclusive: bool) -> Result<()> {
            self.inner.lock_file(exclusive)
        }
        fn unlock_file(&self) -> Result<()> {
            self.inner.unlock_file()
        }
        fn pread(&self, offset: u64, c: Completion) -> Result<Completion> {
            self.queue.lock().push(ScheduledRead {
                file: self.inner.clone(),
                offset,
                completion: c.clone(),
            });
            Ok(c)
        }
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.inner.pwrite(pos, buffer, c)
        }
        fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
            self.inner.sync(c, sync_type)
        }
        fn size(&self) -> Result<u64> {
            self.inner.size()
        }
        fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
            self.inner.truncate(len, c)
        }
    }
    struct DelayedIO {
        inner: Arc<dyn IO>,
        queue: Arc<Mutex<Vec<ScheduledRead>>>,
    }
    impl Clock for DelayedIO {
        fn current_time_monotonic(&self) -> MonotonicInstant {
            self.inner.current_time_monotonic()
        }
        fn current_time_wall_clock(&self) -> WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }
    impl IO for DelayedIO {
        fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
            self.inner.open_file(path, flags, direct)
        }
        fn remove_file(&self, path: &str) -> Result<()> {
            self.inner.remove_file(path)
        }
        fn file_id(&self, path: &str) -> Result<FileId> {
            self.inner.file_id(path)
        }
        fn step(&self) -> Result<()> {
            let read = self.queue.lock().pop();
            if let Some(read) = read {
                drop(read.file.pread(read.offset, read.completion)?);
            }
            Ok(())
        }
    }

    struct CountingFile {
        inner: Arc<dyn File>,
        bytes_read: Arc<AtomicUsize>,
    }

    impl File for CountingFile {
        fn file_id(&self) -> Result<Option<FileId>> {
            self.inner.file_id()
        }
        fn lock_file(&self, exclusive: bool) -> Result<()> {
            self.inner.lock_file(exclusive)
        }
        fn unlock_file(&self) -> Result<()> {
            self.inner.unlock_file()
        }
        fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
            self.bytes_read
                .fetch_add(c.as_read().buf().len(), Ordering::AcqRel);
            self.inner.pread(pos, c)
        }
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.inner.pwrite(pos, buffer, c)
        }
        fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
            self.inner.sync(c, sync_type)
        }
        fn size(&self) -> Result<u64> {
            self.inner.size()
        }
        fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
            self.inner.truncate(len, c)
        }
    }
}
