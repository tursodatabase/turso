use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use turso_core::{
    io::{FileId, FileSyncType},
    Buffer, Clock, Completion, File, MonotonicInstant, OpenFlags, WallClockInstant, IO,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueuedIoOpKind {
    Pread,
    Pwrite,
    Pwritev,
    Sync,
    Truncate,
}

#[derive(Clone, Debug)]
pub(crate) struct QueuedIoEvent {
    pub(crate) path: String,
    pub(crate) kind: QueuedIoOpKind,
}

struct QueuedIoOp {
    event: QueuedIoEvent,
    action: Box<dyn FnOnce() -> turso_core::Result<()> + Send>,
}

#[derive(Debug)]
struct QueuedIoFault {
    path_suffix: String,
    kind: QueuedIoOpKind,
    allowed_successes: usize,
    seen: usize,
}

struct QueuedIoState {
    pending: Mutex<VecDeque<QueuedIoOp>>,
    history: Mutex<Vec<QueuedIoEvent>>,
    fault: Mutex<Option<QueuedIoFault>>,
}

impl QueuedIoState {
    fn new() -> Self {
        Self {
            pending: Mutex::new(VecDeque::new()),
            history: Mutex::new(Vec::new()),
            fault: Mutex::new(None),
        }
    }
}

pub(crate) struct QueuedIo {
    inner: Arc<dyn IO>,
    state: Arc<QueuedIoState>,
}

impl QueuedIo {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(turso_core::MemoryIO::new()),
            state: Arc::new(QueuedIoState::new()),
        }
    }

    pub(crate) fn step_one(&self) -> turso_core::Result<Option<QueuedIoEvent>> {
        let Some(op) = self.state.pending.lock().unwrap().pop_front() else {
            return Ok(None);
        };
        let event = op.event.clone();
        (op.action)()?;
        self.state.history.lock().unwrap().push(event.clone());
        Ok(Some(event))
    }
}

impl Clock for QueuedIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for QueuedIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(QueuedFile {
            path: path.to_string(),
            inner,
            state: self.state.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.step_one().map(|_| ())
    }

    fn drain(&self) -> turso_core::Result<()> {
        while self.step_one()?.is_some() {}
        Ok(())
    }

    fn cancel(&self, completions: &[Completion]) -> turso_core::Result<()> {
        for completion in completions {
            completion.abort();
        }
        Ok(())
    }

    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        self.inner.file_id(path)
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest);
    }

    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }
}

struct QueuedFile {
    path: String,
    inner: Arc<dyn File>,
    state: Arc<QueuedIoState>,
}

impl QueuedFile {
    fn enqueue(
        &self,
        kind: QueuedIoOpKind,
        completion: Completion,
        action: impl FnOnce() -> turso_core::Result<()> + Send + 'static,
    ) -> turso_core::Result<Completion> {
        let event = QueuedIoEvent {
            path: self.path.clone(),
            kind,
        };
        let fault_this_op = {
            let mut fault = self.state.fault.lock().unwrap();
            if let Some(fault) = fault.as_mut() {
                if self.path.ends_with(&fault.path_suffix) && kind == fault.kind {
                    fault.seen += 1;
                    fault.seen > fault.allowed_successes
                } else {
                    false
                }
            } else {
                false
            }
        };

        let queued_completion = completion.clone();
        let queued_action: Box<dyn FnOnce() -> turso_core::Result<()> + Send> = if fault_this_op {
            Box::new(move || {
                queued_completion.abort();
                Ok(())
            })
        } else {
            Box::new(action)
        };

        self.state.pending.lock().unwrap().push_back(QueuedIoOp {
            event,
            action: queued_action,
        });
        Ok(completion)
    }
}

impl File for QueuedFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, completion: Completion) -> turso_core::Result<Completion> {
        let inner = self.inner.clone();
        let c = completion.clone();
        self.enqueue(QueuedIoOpKind::Pread, completion, move || {
            drop(inner.pread(pos, c)?);
            Ok(())
        })
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        let inner = self.inner.clone();
        let c = completion.clone();
        self.enqueue(QueuedIoOpKind::Pwrite, completion, move || {
            drop(inner.pwrite(pos, buffer, c)?);
            Ok(())
        })
    }

    fn sync(
        &self,
        completion: Completion,
        sync_type: FileSyncType,
    ) -> turso_core::Result<Completion> {
        let inner = self.inner.clone();
        let c = completion.clone();
        self.enqueue(QueuedIoOpKind::Sync, completion, move || {
            drop(inner.sync(c, sync_type)?);
            Ok(())
        })
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        let inner = self.inner.clone();
        let c = completion.clone();
        self.enqueue(QueuedIoOpKind::Pwritev, completion, move || {
            drop(inner.pwritev(pos, buffers, c)?);
            Ok(())
        })
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, completion: Completion) -> turso_core::Result<Completion> {
        let inner = self.inner.clone();
        let c = completion.clone();
        self.enqueue(QueuedIoOpKind::Truncate, completion, move || {
            drop(inner.truncate(len, c)?);
            Ok(())
        })
    }
}
