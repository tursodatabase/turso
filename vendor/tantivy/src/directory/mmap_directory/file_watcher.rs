use std::io::BufRead;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, io, thread};

use crc32fast::Hasher;

use crate::directory::{WatchCallback, WatchCallbackList, WatchHandle};

const POLLING_INTERVAL: Duration = Duration::from_millis(if cfg!(test) { 1 } else { 500 });

// Watches a file and executes registered callbacks when the file is modified.
pub struct FileWatcher {
    path: Arc<Path>,
    callbacks: Arc<WatchCallbackList>,
    state: Arc<AtomicUsize>, // 0: new, 1: runnable, 2: terminated
}

impl FileWatcher {
    pub fn new(path: &Path) -> FileWatcher {
        FileWatcher {
            path: Arc::from(path),
            callbacks: Default::default(),
            state: Default::default(),
        }
    }

    pub fn spawn(&self) {
        if self
            .state
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let path = self.path.clone();
        let callbacks = self.callbacks.clone();
        let state = self.state.clone();

        thread::Builder::new()
            .name("thread-tantivy-meta-file-watcher".to_string())
            .spawn(move || {
                let mut current_checksum_opt = None;

                while state.load(Ordering::SeqCst) == 1 {
                    if let Ok(checksum) = FileWatcher::compute_checksum(&path) {
                        let metafile_has_changed = current_checksum_opt
                            .map(|current_checksum| current_checksum != checksum)
                            .unwrap_or(true);
                        if metafile_has_changed {
                            info!("Meta file {path:?} was modified");
                            current_checksum_opt = Some(checksum);
                            // We actually ignore callbacks failing here.
                            // We just wait for the end of their execution.
                            let _ = callbacks.broadcast().wait();
                        }
                    }

                    thread::sleep(POLLING_INTERVAL);
                }
            })
            .expect("Failed to spawn meta file watcher thread");
    }

    pub fn watch(&self, callback: WatchCallback) -> WatchHandle {
        let handle = self.callbacks.subscribe(callback);
        self.spawn();
        handle
    }

    fn compute_checksum(path: &Path) -> Result<u32, io::Error> {
        let reader = match fs::File::open(path) {
            Ok(f) => io::BufReader::new(f),
            Err(e) => {
                warn!("Failed to open meta file {path:?}: {e:?}");
                return Err(e);
            }
        };

        let mut hasher = Hasher::new();

        for line in reader.lines() {
            hasher.update(line?.as_bytes())
        }

        Ok(hasher.finalize())
    }
}

impl Drop for FileWatcher {
    fn drop(&mut self) {
        self.state.store(2, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {

    use std::mem;

    use super::*;
    use crate::directory::mmap_directory::atomic_write;

    #[test]
    fn test_file_watcher_drop_watcher() -> crate::Result<()> {
        let tmp_dir = tempfile::TempDir::new()?;
        let tmp_file = tmp_dir.path().join("watched.txt");

        let counter: Arc<AtomicUsize> = Default::default();
        let (tx, rx) = crossbeam_channel::unbounded();
        let timeout = Duration::from_millis(100);

        let watcher = FileWatcher::new(&tmp_file);

        let state = watcher.state.clone();
        assert_eq!(state.load(Ordering::SeqCst), 0);

        let counter_clone = counter.clone();

        let _handle = watcher.watch(WatchCallback::new(move || {
            let val = counter_clone.fetch_add(1, Ordering::SeqCst);
            tx.send(val + 1).unwrap();
        }));

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(state.load(Ordering::SeqCst), 1);

        atomic_write(&tmp_file, b"foo")?;
        assert_eq!(rx.recv_timeout(timeout), Ok(1));

        atomic_write(&tmp_file, b"foo")?;
        assert!(rx.recv_timeout(timeout).is_err());

        atomic_write(&tmp_file, b"bar")?;
        assert_eq!(rx.recv_timeout(timeout), Ok(2));

        mem::drop(watcher);

        atomic_write(&tmp_file, b"qux")?;
        thread::sleep(Duration::from_millis(10));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert_eq!(state.load(Ordering::SeqCst), 2);

        Ok(())
    }

    #[test]
    fn test_file_watcher_drop_handle() -> crate::Result<()> {
        let tmp_dir = tempfile::TempDir::new()?;
        let tmp_file = tmp_dir.path().join("watched.txt");

        let counter: Arc<AtomicUsize> = Default::default();
        let (tx, rx) = crossbeam_channel::unbounded();
        let timeout = Duration::from_millis(100);

        let watcher = FileWatcher::new(&tmp_file);

        let state = watcher.state.clone();
        assert_eq!(state.load(Ordering::SeqCst), 0);

        let counter_clone = counter.clone();

        let handle = watcher.watch(WatchCallback::new(move || {
            let val = counter_clone.fetch_add(1, Ordering::SeqCst);
            tx.send(val + 1).unwrap();
        }));

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(state.load(Ordering::SeqCst), 1);

        atomic_write(&tmp_file, b"foo")?;
        assert_eq!(rx.recv_timeout(timeout), Ok(1));

        mem::drop(handle);

        atomic_write(&tmp_file, b"qux")?;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(state.load(Ordering::SeqCst), 1);

        Ok(())
    }
}
