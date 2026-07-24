use rand::{Rng, RngCore};
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::fs::{File as StdFile, OpenOptions};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::debug;
use turso_core::{
    Clock, Completion, File, IO, MonotonicInstant, OpenFlags, Result, WallClockInstant,
};

#[derive(Debug, Clone)]
pub struct IOFaultConfig {
    /// Probability of a cosmic ray bit flip on write (0.0-1.0)
    pub cosmic_ray_probability: f64,
}

impl Default for IOFaultConfig {
    fn default() -> Self {
        Self {
            cosmic_ray_probability: 0.0,
        }
    }
}

fn canonical_key(path: &str) -> String {
    std::fs::canonicalize(path)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string())
}

pub struct SimulatorIO {
    files: Mutex<Vec<(String, Arc<SimulatorFile>)>>,
    file_sizes: Arc<Mutex<HashMap<String, u64>>>,
    keep_files: bool,
    rng: Mutex<ChaCha8Rng>,
    fault_config: IOFaultConfig,
    /// Simulated time in microseconds, incremented on each step
    time: AtomicU64,
    pending: PendingQueue,
}

impl SimulatorIO {
    pub fn new(keep_files: bool, rng: ChaCha8Rng, fault_config: IOFaultConfig) -> Self {
        debug!("SimulatorIO fault config: {:?}", fault_config);
        Self {
            files: Mutex::new(Vec::new()),
            file_sizes: Arc::new(Mutex::new(HashMap::new())),
            keep_files,
            rng: Mutex::new(rng),
            fault_config,
            time: AtomicU64::new(0),
            pending: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn file_sizes(&self) -> Arc<Mutex<HashMap<String, u64>>> {
        self.file_sizes.clone()
    }

    /// Number of submitted I/O operations whose completion callbacks have not run.
    pub fn pending_completion_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// Abort queued completion notifications after a simulated process crash.
    ///
    /// `SimulatorFile` applies submitted bytes before queuing the completion, so
    /// this models losing process state after storage accepted an I/O request but
    /// before the issuing state machine observed completion. Completing the
    /// callbacks as aborted releases their process-local waiters before the
    /// test drops the simulated process.
    pub fn abort_pending_completions(&self) -> usize {
        let pending = {
            let mut pending = self.pending.lock().unwrap();
            std::mem::take(&mut *pending)
        };
        let discarded = pending.len();
        for completion in pending {
            completion.completion.abort();
        }
        discarded
    }

    /// Dump all database files to the specified output directory.
    /// Only copies the actual file content, not the full mmap size.
    pub fn dump_files(&self, out_dir: &std::path::Path) -> anyhow::Result<()> {
        let files = self.files.lock().unwrap();
        let sizes = self.file_sizes.lock().unwrap();

        for (path, file) in files.iter() {
            // Only dump database-related files
            if path.ends_with(".db") || path.ends_with("-wal") || path.ends_with("-log") {
                let actual_size = sizes.get(path).copied().unwrap_or(0) as usize;

                // Extract just the filename from the path
                let filename = std::path::Path::new(path)
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| path.clone());

                let dest_path = out_dir.join(&filename);
                let contents = file.read_to_vec(0, actual_size);
                std::fs::write(&dest_path, &contents)?;
                println!(
                    "Dumped {} ({} bytes) to {}",
                    path,
                    actual_size,
                    dest_path.display()
                );
            }
        }
        Ok(())
    }
}

impl Drop for SimulatorIO {
    fn drop(&mut self) {
        let files = self.files.lock().unwrap();
        let paths: HashSet<String> = files.iter().map(|(path, _)| path.clone()).collect();
        if !self.keep_files {
            for path in paths.iter() {
                let _ = std::fs::remove_file(path);
                {
                    let mut sizes = self.file_sizes.lock().unwrap();
                    sizes.remove(path);
                }
            }
        } else {
            for path in paths.iter() {
                println!("Keeping file: {path}");
            }
        }
    }
}

impl Clock for SimulatorIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        MonotonicInstant::now()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        let micros = self.time.load(Ordering::Relaxed);
        WallClockInstant {
            secs: (micros / 1_000_000) as i64,
            micros: (micros % 1_000_000) as u32,
        }
    }
}

impl IO for SimulatorIO {
    fn sleep(&self, duration: std::time::Duration) {
        self.time
            .fetch_add(duration.as_micros() as u64, Ordering::SeqCst);
    }
    fn open_file(&self, path: &str, _flags: OpenFlags, _create_new: bool) -> Result<Arc<dyn File>> {
        let lookup_key = canonical_key(path);
        {
            let files = self.files.lock().unwrap();
            if let Some((_, file)) = files.iter().find(|f| f.0 == lookup_key) {
                return Ok(file.clone());
            }
        }

        let file = Arc::new(SimulatorFile::new(
            path,
            self.file_sizes.clone(),
            self.pending.clone(),
        ));
        let insert_key = canonical_key(path);

        let mut files = self.files.lock().unwrap();
        files.push((insert_key, file.clone()));

        Ok(file as Arc<dyn File>)
    }

    fn file_id(&self, path: &str) -> Result<turso_core::io::FileId> {
        Ok(turso_core::io::FileId::from_path_hash(path))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        let key = canonical_key(path);
        let mut files = self.files.lock().unwrap();
        files.retain(|(p, _)| p != &key);

        if !self.keep_files {
            let _ = std::fs::remove_file(path);
        }
        Ok(())
    }

    fn step(&self) -> Result<()> {
        // Complete any pending IO operations
        let mut pending = self.pending.lock().unwrap();
        for pc in pending.drain(..) {
            pc.completion.complete(pc.result);
        }
        drop(pending);

        // Advance simulated time by 1ms per step
        self.time.fetch_add(1000, Ordering::Relaxed);

        // Inject cosmic ray faults with configured probability
        if self.fault_config.cosmic_ray_probability > 0.0 {
            let mut rng = self.rng.lock().unwrap();
            if rng.random::<f64>() < self.fault_config.cosmic_ray_probability {
                // Collect files that are still alive
                let open_files: Vec<_> = {
                    let files = self.files.lock().unwrap();
                    files
                        .iter()
                        .map(|(path, file)| (path.clone(), file.clone()))
                        .collect()
                };

                if !open_files.is_empty() {
                    let file_idx = rng.random_range(0..open_files.len());
                    let (path, file) = &open_files[file_idx];

                    // Get the actual file size (not the mmap size)
                    let file_size = *file.size.lock().unwrap();
                    if file_size > 0 {
                        // Pick a random offset within the actual file size
                        let byte_offset = rng.random_range(0..file_size);
                        let bit_idx = rng.random_range(0..8);

                        let mut byte = [0];
                        let result = file.read_at(byte_offset, &mut byte);
                        assert_eq!(
                            result, 1,
                            "cosmic ray offset is selected from a valid simulator file size"
                        );
                        let old_byte = byte[0];
                        byte[0] ^= 1 << bit_idx;
                        let result = file.write_at(byte_offset, &byte);
                        assert_eq!(
                            result, 1,
                            "cosmic ray offset is selected from a valid simulator file size"
                        );
                        println!(
                            "Cosmic ray! File: {} - Flipped bit {} at offset {} (0x{:02x} -> 0x{:02x})",
                            path, bit_idx, byte_offset, old_byte, byte[0]
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut rng = self.rng.lock().unwrap();
        rng.next_u64() as i64
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        let mut rng = self.rng.lock().unwrap();
        rng.fill_bytes(dest);
    }
}

struct PendingCompletion {
    completion: Completion,
    result: i32,
}
type PendingQueue = Arc<Mutex<Vec<PendingCompletion>>>;

const MAX_FILE_SIZE: usize = 1 << 33; // 8 GiB
pub(crate) const FILE_SIZE_SOFT_LIMIT: u64 = 6 * (1 << 30); // 6 GiB (75% of MAX_FILE_SIZE)
const SIMULATOR_FILE_BLOCK_SIZE: usize = 4096;
type SimulatorFileBlock = Box<[u8; SIMULATOR_FILE_BLOCK_SIZE]>;

struct SimulatorFile {
    blocks: Mutex<BTreeMap<usize, SimulatorFileBlock>>,
    size: Mutex<usize>,
    file_sizes: Arc<Mutex<HashMap<String, u64>>>,
    path: String,
    _file: StdFile,
    pending: PendingQueue,
}

impl SimulatorFile {
    fn new(
        file_path: &str,
        file_sizes: Arc<Mutex<HashMap<String, u64>>>,
        pending: PendingQueue,
    ) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)
            .unwrap_or_else(|e| panic!("Failed to create file {file_path}: {e}"));

        {
            let mut sizes = file_sizes.lock().unwrap();
            sizes.insert(file_path.to_string(), 0);
        }

        Self {
            blocks: Mutex::new(BTreeMap::new()),
            size: Mutex::new(0),
            file_sizes,
            path: file_path.to_string(),
            _file: file,
            pending,
        }
    }

    fn read_at(&self, pos: usize, dest: &mut [u8]) -> i32 {
        let Some(end) = pos.checked_add(dest.len()) else {
            return 0;
        };
        if end > MAX_FILE_SIZE {
            return 0;
        }

        dest.fill(0);
        let blocks = self.blocks.lock().unwrap();
        let mut copied = 0;
        while copied < dest.len() {
            let absolute = pos + copied;
            let block_idx = absolute / SIMULATOR_FILE_BLOCK_SIZE;
            let block_offset = absolute % SIMULATOR_FILE_BLOCK_SIZE;
            let chunk_len = (SIMULATOR_FILE_BLOCK_SIZE - block_offset).min(dest.len() - copied);
            if let Some(block) = blocks.get(&block_idx) {
                dest[copied..copied + chunk_len]
                    .copy_from_slice(&block[block_offset..block_offset + chunk_len]);
            }
            copied += chunk_len;
        }
        dest.len() as i32
    }

    fn read_to_vec(&self, pos: usize, len: usize) -> Vec<u8> {
        let mut contents = vec![0; len];
        let result = self.read_at(pos, &mut contents);
        assert_eq!(
            result as usize, len,
            "dumped simulator files must stay within MAX_FILE_SIZE"
        );
        contents
    }

    fn write_at(&self, pos: usize, src: &[u8]) -> i32 {
        let Some(end) = pos.checked_add(src.len()) else {
            return 0;
        };
        if end > MAX_FILE_SIZE {
            return 0;
        }

        let mut blocks = self.blocks.lock().unwrap();
        let mut written = 0;
        while written < src.len() {
            let absolute = pos + written;
            let block_idx = absolute / SIMULATOR_FILE_BLOCK_SIZE;
            let block_offset = absolute % SIMULATOR_FILE_BLOCK_SIZE;
            let chunk_len = (SIMULATOR_FILE_BLOCK_SIZE - block_offset).min(src.len() - written);
            let block = blocks
                .entry(block_idx)
                .or_insert_with(|| Box::new([0; SIMULATOR_FILE_BLOCK_SIZE]));
            block[block_offset..block_offset + chunk_len]
                .copy_from_slice(&src[written..written + chunk_len]);
            written += chunk_len;
        }
        drop(blocks);

        let mut size = self.size.lock().unwrap();
        if end > *size {
            *size = end;
            let mut sizes = self.file_sizes.lock().unwrap();
            sizes.insert(self.path.clone(), *size as u64);
        }
        src.len() as i32
    }
}

impl Drop for SimulatorFile {
    fn drop(&mut self) {}
}

unsafe impl Send for SimulatorFile {}
unsafe impl Sync for SimulatorFile {}

impl File for SimulatorFile {
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let pos = pos as usize;
        let read_completion = c.as_read();
        let buffer = read_completion.buf_arc();

        let result = self.read_at(pos, buffer.as_mut_slice());
        self.pending.lock().unwrap().push(PendingCompletion {
            completion: c.clone(),
            result,
        });
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<turso_core::Buffer>,
        c: Completion,
    ) -> Result<Completion> {
        let pos = pos as usize;
        let result = self.write_at(pos, buffer.as_slice());
        self.pending.lock().unwrap().push(PendingCompletion {
            completion: c.clone(),
            result,
        });
        Ok(c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<turso_core::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        let mut offset = pos as usize;
        let mut total_written = 0;

        for buffer in buffers {
            let len = buffer.len();
            let result = self.write_at(offset, buffer.as_slice());
            if result != len as i32 {
                break;
            }
            offset += len;
            total_written += len;
        }

        self.pending.lock().unwrap().push(PendingCompletion {
            completion: c.clone(),
            result: total_written as i32,
        });
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: turso_core::io::FileSyncType) -> Result<Completion> {
        // No-op for memory files
        self.pending.lock().unwrap().push(PendingCompletion {
            completion: c.clone(),
            result: 0,
        });
        Ok(c)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let mut size = self.size.lock().unwrap();
        *size = len as usize;
        let mut sizes = self.file_sizes.lock().unwrap();
        sizes.insert(self.path.clone(), len);
        self.pending.lock().unwrap().push(PendingCompletion {
            completion: c.clone(),
            result: 0,
        });
        Ok(c)
    }

    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        // No-op for memory files
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        // No-op for memory files
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(*self.size.lock().unwrap() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempFile {
        path: String,
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn simulator_file_does_not_preallocate_max_size() {
        let path =
            std::env::temp_dir().join(format!("turso-whopper-blocks-{}.db", std::process::id()));
        let path = path.to_string_lossy().into_owned();
        let _cleanup = TempFile { path: path.clone() };
        let file = SimulatorFile::new(
            &path,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(Vec::new())),
        );

        assert_eq!(file._file.metadata().unwrap().len(), 0);

        let pos = MAX_FILE_SIZE - 2;
        assert_eq!(file.write_at(pos, &[1, 2]), 2);
        assert_eq!(file._file.metadata().unwrap().len(), 0);
        assert_eq!(file.size().unwrap(), MAX_FILE_SIZE as u64);

        let mut read = [0; 3];
        assert_eq!(file.read_at(pos - 1, &mut read), 3);
        assert_eq!(read, [0, 1, 2]);
    }
}
