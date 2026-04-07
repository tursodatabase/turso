use memmap2::{MmapMut, MmapOptions};
use rand::{Rng, RngCore};
use rand_chacha::ChaCha8Rng;
use std::collections::{HashMap, HashSet};
use std::fs::{File as StdFile, OpenOptions};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::debug;
use turso_core::{
    Clock, Completion, File, IO, MonotonicInstant, OpenFlags, Result, WallClockInstant,
};

/// Restricts cosmic ray bit flips to a specific byte range in a specific file.
///
/// Used to focus the existing fault model on specific subsystems (e.g., a
/// particular B-tree page or freelist trunk page) without changing the
/// underlying fault class. The same random bit-flip mechanism still applies;
/// only the universe of target bytes is narrowed.
///
/// This is useful when a fault class is reachable in principle but
/// astronomically rare with whole-file random injection — focusing the scope
/// makes discovery deterministic on a tractable seed budget while preserving
/// the legitimacy of the fault model.
#[derive(Debug, Clone)]
pub struct CosmicRayTarget {
    /// File path suffix to match (e.g. ".db" for the main database file).
    pub file_suffix: String,
    /// Byte range within the file where cosmic rays are allowed to fire.
    pub byte_range: Range<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct IOFaultConfig {
    /// Probability of a cosmic ray bit flip on write (0.0-1.0)
    pub cosmic_ray_probability: f64,
    /// When non-empty, cosmic ray bit flips are restricted to these target
    /// ranges. The `cosmic_ray_probability` still gates whether a flip fires;
    /// the targets only constrain *where* it can fire. When empty, cosmic rays
    /// fire uniformly across the whole file (default behavior).
    pub cosmic_ray_targets: Vec<CosmicRayTarget>,
}

pub struct SimulatorIO {
    files: Mutex<Vec<(String, Arc<SimulatorFile>)>>,
    file_sizes: Arc<Mutex<HashMap<String, u64>>>,
    keep_files: bool,
    rng: Mutex<ChaCha8Rng>,
    fault_config: Mutex<IOFaultConfig>,
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
            fault_config: Mutex::new(fault_config),
            time: AtomicU64::new(0),
            pending: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Replace the active fault injection configuration. Used by tests that
    /// need to build a clean database first and then enable focused fault
    /// injection for a specific operation (e.g. populate a freelist, then
    /// turn on cosmic rays focused on the freelist trunk page).
    pub fn set_fault_config(&self, fault_config: IOFaultConfig) {
        debug!("SimulatorIO fault config updated: {:?}", fault_config);
        *self.fault_config.lock().unwrap() = fault_config;
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

    /// Read a contiguous slice of a file's current contents.
    /// Used by tests to inspect database structure (e.g. read the freelist
    /// trunk pointer from the header) so they can compute byte ranges to
    /// pass to `cosmic_ray_targets`.
    pub fn read_file_bytes(&self, path: &str, range: Range<usize>) -> Vec<u8> {
        let files = self.files.lock().unwrap();
        let (_, file) = files
            .iter()
            .find(|(p, _)| p == path)
            .unwrap_or_else(|| panic!("read_file_bytes: file '{path}' not found"));
        let mmap = file.mmap.lock().unwrap();
        mmap[range].to_vec()
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

    fn file_id(&self, path: &str) -> Result<turso_core::io::FileId> {
        Ok(turso_core::io::FileId::from_path_hash(path))
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
        let fault_config = self.fault_config.lock().unwrap().clone();
        if fault_config.cosmic_ray_probability > 0.0 {
            let mut rng = self.rng.lock().unwrap();
            if rng.random::<f64>() < fault_config.cosmic_ray_probability {
                // Collect files that are still alive
                let open_files: Vec<_> = {
                    let files = self.files.lock().unwrap();
                    files
                        .iter()
                        .map(|(path, file)| (path.clone(), file.clone()))
                        .collect()
                };

                if !open_files.is_empty() {
                    // Build the candidate set: either restricted to configured
                    // targets, or unrestricted (whole file) when no targets set.
                    let targets = &fault_config.cosmic_ray_targets;
                    let candidates: Vec<(String, Arc<SimulatorFile>, usize)> = if targets.is_empty()
                    {
                        open_files
                            .into_iter()
                            .filter_map(|(path, file)| {
                                let size = *file.size.lock().unwrap();
                                if size > 0 {
                                    let byte_offset = rng.random_range(0..size);
                                    Some((path, file, byte_offset))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    } else {
                        open_files
                            .into_iter()
                            .flat_map(|(path, file)| {
                                let size = *file.size.lock().unwrap();
                                let matching_ranges: Vec<Range<usize>> = targets
                                    .iter()
                                    .filter(|t| path.ends_with(&t.file_suffix))
                                    .map(|t| t.byte_range.clone())
                                    .filter(|r| r.start < size && r.start < r.end)
                                    .map(|r| r.start..r.end.min(size))
                                    .collect();
                                matching_ranges
                                    .into_iter()
                                    .map(|range| {
                                        let byte_offset = rng.random_range(range);
                                        (path.clone(), file.clone(), byte_offset)
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .collect()
                    };

                    if !candidates.is_empty() {
                        let pick = rng.random_range(0..candidates.len());
                        let (path, file, byte_offset) = &candidates[pick];
                        let bit_idx = rng.random_range(0..8);
                        let mut mmap = file.mmap.lock().unwrap();
                        let old_byte = mmap[*byte_offset];
                        mmap[*byte_offset] ^= 1 << bit_idx;
                        println!(
                            "Cosmic ray! File: {} - Flipped bit {} at offset {} (0x{:02x} -> 0x{:02x})",
                            path, bit_idx, byte_offset, old_byte, mmap[*byte_offset]
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
