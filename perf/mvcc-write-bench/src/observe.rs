use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LogTrace {
    pub peak_bytes: u64,
    pub checkpoints_observed: u32,
    pub sampled: bool,
}

impl LogTrace {
    pub(crate) fn merge(self, other: Self) -> Self {
        Self {
            peak_bytes: self.peak_bytes.max(other.peak_bytes),
            checkpoints_observed: self.checkpoints_observed.max(other.checkpoints_observed),
            sampled: self.sampled || other.sampled,
        }
    }
}

/// File-size checkpoint observation. Truncate of `.db-log` is the signal
/// we can see without private `MvStore` APIs.
pub(crate) struct LogWatch {
    log_path: PathBuf,
    wal_path: PathBuf,
    last_log: Option<u64>,
    log_bytes: u64,
    wal_bytes: u64,
    peak_bytes: u64,
    checkpoints_observed: u32,
    sampled: bool,
}

impl LogWatch {
    pub(crate) fn open(db_path: &Path) -> Self {
        let log_path = db_path.with_extension("db-log");
        let wal_path = wal_path_for(db_path);
        Self {
            log_path,
            wal_path,
            last_log: None,
            log_bytes: 0,
            wal_bytes: 0,
            peak_bytes: 0,
            checkpoints_observed: 0,
            sampled: false,
        }
    }

    pub(crate) fn silent() -> Self {
        Self::open(Path::new(""))
    }

    pub(crate) fn sample(&mut self) {
        let log_bytes = file_len(&self.log_path);
        let wal_bytes = file_len(&self.wal_path);
        self.sampled = true;
        self.log_bytes = log_bytes;
        self.wal_bytes = wal_bytes;
        self.peak_bytes = self.peak_bytes.max(log_bytes);
        if let Some(last) = self.last_log {
            if log_bytes < last {
                self.checkpoints_observed = self.checkpoints_observed.saturating_add(1);
            }
        }
        self.last_log = Some(log_bytes);
    }

    pub(crate) fn finish(self) -> (u64, u64, LogTrace) {
        (
            self.log_bytes,
            self.wal_bytes,
            LogTrace {
                peak_bytes: self.peak_bytes,
                checkpoints_observed: self.checkpoints_observed,
                sampled: self.sampled,
            },
        )
    }
}

pub(crate) fn wal_path_for(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push("-wal");
    PathBuf::from(s)
}

pub(crate) fn shm_path_for(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push("-shm");
    PathBuf::from(s)
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}
