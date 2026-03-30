use memmap2::{MmapMut, MmapOptions};
use rand::{Rng, RngCore};
use rand_chacha::ChaCha8Rng;
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
                let mmap = file.mmap.lock().unwrap();
                std::fs::write(&dest_path, &mmap[..actual_size])?;
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
        {
            let files = self.files.lock().unwrap();
            if let Some((_, file)) = files.iter().find(|f| f.0 == path) {
                return Ok(file.clone());
            }
        }

        let file = Arc::new(SimulatorFile::new(
            path,
            self.file_sizes.clone(),
            self.pending.clone(),
        ));

        let mut files = self.files.lock().unwrap();
        files.push((path.to_string(), file.clone()));

        Ok(file as Arc<dyn File>)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        files.retain(|(p, _)| p != path);

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

                        let mut mmap = file.mmap.lock().unwrap();
                        let old_byte = mmap[byte_offset];
                        mmap[byte_offset] ^= 1 << bit_idx;
                        println!(
                            "Cosmic ray! File: {} - Flipped bit {} at offset {} (0x{:02x} -> 0x{:02x})",
                            path, bit_idx, byte_offset, old_byte, mmap[byte_offset]
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

struct SimulatorFile {
    mmap: Mutex<MmapMut>,
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

        file.set_len(MAX_FILE_SIZE as u64)
            .unwrap_or_else(|e| panic!("Failed to truncate file {file_path}: {e}"));

        let mmap = unsafe {
            MmapOptions::new()
                .len(MAX_FILE_SIZE)
                .map_mut(&file)
                .unwrap_or_else(|e| panic!("mmap failed for file {file_path}: {e}"))
        };

        {
            let mut sizes = file_sizes.lock().unwrap();
            sizes.insert(file_path.to_string(), 0);
        }

        Self {
            mmap: Mutex::new(mmap),
            size: Mutex::new(0),
            file_sizes,
            path: file_path.to_string(),
            _file: file,
            pending,
        }
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
        let len = buffer.len();

        let result = if pos + len <= MAX_FILE_SIZE {
            let mmap = self.mmap.lock().unwrap();
            buffer.as_mut_slice().copy_from_slice(&mmap[pos..pos + len]);
            len as i32
        } else {
            0
        };
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
        let len = buffer.len();

        let result = if pos + len <= MAX_FILE_SIZE {
            let mut mmap = self.mmap.lock().unwrap();
            mmap[pos..pos + len].copy_from_slice(buffer.as_slice());
            let mut size = self.size.lock().unwrap();
            if pos + len > *size {
                *size = pos + len;
                {
                    let mut sizes = self.file_sizes.lock().unwrap();
                    sizes.insert(self.path.clone(), *size as u64);
                }
            }
            len as i32
        } else {
            0
        };
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

        {
            let mut mmap = self.mmap.lock().unwrap();
            for buffer in buffers {
                let len = buffer.len();
                if offset + len <= MAX_FILE_SIZE {
                    mmap[offset..offset + len].copy_from_slice(buffer.as_slice());
                    offset += len;
                    total_written += len;
                } else {
                    break;
                }
            }
        }

        // Update the file size if we wrote beyond the current size
        if total_written > 0 {
            let mut size = self.size.lock().unwrap();
            let end_pos = (pos as usize) + total_written;
            if end_pos > *size {
                *size = end_pos;
                {
                    let mut sizes = self.file_sizes.lock().unwrap();
                    sizes.insert(self.path.clone(), *size as u64);
                }
            }
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
