//! Turso Shared Memory (TSHM) for multi-process coordination.
//!
//! The TSHM file (`db-tshm`) is a single mmap'd page (4096 bytes) laid out as:
//!
//! ```text
//! Offset 0:   writer_state   [monotonic counter]                (AtomicU64)
//! Offset 8:   locking_mode   [0=unset, 1=SharedReads, 2=SharedWrites]
//! Offset 16:  write_lock     [pid:32][instance:32]              (AtomicU64)
//! Offset 24:  reader slot 0  [pid:32][max_frame:32]             (AtomicU64)
//! Offset 32:  reader slot 1
//! ...
//! Offset 4088: reader slot 508
//! ```
//!
//! **Writer state** (slot 0): Updated by the writer after committing frames
//! or restarting the WAL. Readers compare this to a cached value to decide
//! whether a full WAL rescan is needed — skip if unchanged (~10ns per read tx).
//!
//! **Write lock** (slot 2): Atomic CAS-based write lock that stores the
//! owner's identity as `[pid:32][instance:32]`. Provides exclusion both
//! across separate OS processes AND within the same process (multiple
//! Database instances). Complements flock which on macOS doesn't provide
//! per-fd exclusion within one process.
//!
//! **Reader slots** (slots 3..511): Each slot is an AtomicU64 packed as
//! [pid:32][max_frame:32]. Readers claim a slot at transaction start (CAS)
//! and release it at transaction end (store 0). The writer scans slots at
//! checkpoint time to determine the safe checkpoint boundary.
//!
//! This module is only available on 64-bit Unix platforms where AtomicU64
//! operations on mmap'd memory are guaranteed to be hardware-atomic across
//! processes.

#[cfg(all(unix, target_pointer_width = "64"))]
#[allow(dead_code)]
mod imp {
    use std::sync::atomic::{AtomicU64, Ordering};

    const PAGE_SIZE: usize = 4096;
    const SLOT_SIZE: usize = std::mem::size_of::<AtomicU64>();
    const NUM_SLOTS: usize = PAGE_SIZE / SLOT_SIZE; // 512

    /// Number of slots reserved at the start for header metadata.
    const HEADER_SLOTS: usize = 3;
    /// Number of reader slots available for transaction registration.
    pub const NUM_READER_SLOTS: usize = NUM_SLOTS - HEADER_SLOTS; // 509

    /// Header slot 0: writer state — monotonic counter incremented on each commit.
    const HDR_WRITER_STATE: usize = 0;
    /// Header slot 1: locking mode — 0 = unset, 1 = SharedReads, 2 = SharedWrites.
    const HDR_LOCKING_MODE: usize = 1;
    /// Header slot 2: write lock owner — packed [pid:32][instance:32].
    /// 0 means unlocked. Non-zero identifies the Tshm instance that holds the lock.
    const HDR_WRITE_LOCK: usize = 2;

    /// Global counter for generating unique per-Tshm instance IDs within a process.
    static INSTANCE_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

    /// Pack a PID and max_frame into a single u64 slot value.
    #[inline]
    pub fn pack_slot(pid: u32, max_frame: u32) -> u64 {
        ((pid as u64) << 32) | (max_frame as u64)
    }

    /// Unpack a slot value into (pid, max_frame).
    #[inline]
    pub fn unpack_slot(val: u64) -> (u32, u32) {
        let pid = (val >> 32) as u32;
        let max_frame = val as u32;
        (pid, max_frame)
    }

    /// Pack checkpoint_seq and max_frame into a single u64 writer state value.
    #[inline]
    pub fn pack_writer_state(checkpoint_seq: u32, max_frame: u32) -> u64 {
        ((checkpoint_seq as u64) << 32) | (max_frame as u64)
    }

    /// Handle to an acquired reader slot. Stores the absolute slot index
    /// (including header offset) so it can be released later.
    pub struct SlotHandle {
        index: usize,
    }

    impl SlotHandle {
        /// Returns the absolute slot index.
        pub fn index(&self) -> usize {
            self.index
        }
    }

    /// Turso Shared Memory file for multi-process coordination.
    ///
    /// Handles reader slot management (via mmap'd atomics), writer state
    /// publishing, and inter-process write locking (via shared-memory CAS
    /// + flock on the same fd).
    pub struct Tshm {
        /// Pointer to the mmap'd region. The region is PAGE_SIZE bytes,
        /// interpreted as an array of NUM_SLOTS AtomicU64 values.
        ptr: *mut u8,
        /// File descriptor for the tshm file (kept open for the lifetime of the mmap).
        /// Also used for flock-based write locking.
        fd: std::os::unix::io::RawFd,
        /// MVCC write lock refcount. Only the 0→1 transition acquires the
        /// shared-memory + flock lock, only the 1→0 transition releases it.
        write_refcount: std::sync::atomic::AtomicU32,
        /// Unique identity for this Tshm instance, packed as [pid:32][counter:32].
        /// Used for the shared-memory write lock (HDR_WRITE_LOCK slot).
        instance_id: u64,
    }

    // SAFETY: The mmap'd region is shared across processes and accessed only
    // through atomic operations. The pointer itself is stable for the lifetime
    // of the Tshm (the mmap is not remapped or resized).
    unsafe impl Send for Tshm {}
    unsafe impl Sync for Tshm {}

    impl Tshm {
        /// Open or create a TSHM file at the given path and mmap it.
        pub fn open(path: &str) -> crate::Result<Self> {
            use std::os::unix::io::AsRawFd;

            fn io_err(path: &str, e: std::io::Error) -> crate::LimboError {
                crate::LimboError::InternalError(format!("tshm file '{path}': {e}"))
            }

            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(|e| io_err(path, e))?;

            let fd = file.as_raw_fd();

            // Ensure the file is exactly PAGE_SIZE bytes.
            let metadata = file.metadata().map_err(|e| io_err(path, e))?;
            if metadata.len() < PAGE_SIZE as u64 {
                file.set_len(PAGE_SIZE as u64)
                    .map_err(|e| io_err(path, e))?;
            }

            // SAFETY: We've ensured the file is at least PAGE_SIZE bytes.
            // MAP_SHARED ensures changes are visible across processes.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    PAGE_SIZE,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd,
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                return Err(crate::LimboError::InternalError(format!(
                    "mmap failed for tshm file '{path}': {}",
                    std::io::Error::last_os_error()
                )));
            }

            // Leak the File so the fd stays open (we manage it via _fd and Drop).
            let fd = file.into_raw_fd();

            // Generate a unique instance ID: [pid:32][counter:32].
            // Non-zero by construction (PID is never 0, counter starts at 1).
            let pid = std::process::id();
            let counter = INSTANCE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let instance_id = ((pid as u64) << 32) | (counter as u64);

            Ok(Self {
                ptr: ptr as *mut u8,
                fd,
                write_refcount: std::sync::atomic::AtomicU32::new(0),
                instance_id,
            })
        }

        /// Get a reference to the entire slot array (header + reader slots).
        #[inline]
        fn all_slots(&self) -> &[AtomicU64; NUM_SLOTS] {
            // SAFETY: The mmap'd region is PAGE_SIZE bytes = NUM_SLOTS * 8 bytes,
            // properly aligned for AtomicU64 (mmap returns page-aligned memory).
            unsafe { &*(self.ptr as *const [AtomicU64; NUM_SLOTS]) }
        }

        /// Get a slice of only the reader slots (excluding header).
        #[inline]
        fn reader_slots(&self) -> &[AtomicU64] {
            &self.all_slots()[HEADER_SLOTS..]
        }

        // --- Writer state (header slot 0) ---

        /// Load the writer state from shared memory.
        /// Returns a packed u64: [checkpoint_seq:32][max_frame:32].
        pub fn writer_state(&self) -> u64 {
            self.all_slots()[HDR_WRITER_STATE].load(Ordering::Acquire)
        }

        /// Store the writer state to shared memory.
        /// `val` should be packed via `pack_writer_state(checkpoint_seq, max_frame)`.
        pub fn set_writer_state(&self, val: u64) {
            self.all_slots()[HDR_WRITER_STATE].store(val, Ordering::Release)
        }

        /// Atomically increment the writer state counter and return the new value.
        /// Used by MultiProcessWal to signal readers that the WAL has changed.
        pub fn increment_writer_state(&self) -> u64 {
            self.all_slots()[HDR_WRITER_STATE].fetch_add(1, Ordering::AcqRel) + 1
        }

        // --- Locking mode (header slot 1) ---

        /// Load the locking mode from shared memory.
        /// Returns 0 (unset), 1 (SharedReads), or 2 (SharedWrites).
        pub fn locking_mode(&self) -> u64 {
            self.all_slots()[HDR_LOCKING_MODE].load(Ordering::Acquire)
        }

        /// Try to set the locking mode in shared memory.
        /// If the slot is unset (0), atomically sets it to `mode` via CAS.
        /// If the slot is already set, returns the existing value.
        /// Returns Ok(()) if the mode was set (or already matches),
        /// or Err(existing_mode) if a different mode is already set.
        pub fn try_set_locking_mode(&self, mode: u64) -> Result<(), u64> {
            let slot = &self.all_slots()[HDR_LOCKING_MODE];
            match slot.compare_exchange(0, mode, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => Ok(()),
                Err(existing) if existing == mode => Ok(()),
                Err(existing) => Err(existing),
            }
        }

        /// Force-set the locking mode (for when we know TSHM is stale).
        pub fn set_locking_mode(&self, mode: u64) {
            self.all_slots()[HDR_LOCKING_MODE].store(mode, Ordering::Release);
        }

        /// Clear the locking mode (set to 0 / unset).
        pub fn clear_locking_mode(&self) {
            self.all_slots()[HDR_LOCKING_MODE].store(0, Ordering::Release);
        }

        // --- Reader slot management ---

        /// Try to claim a reader slot with the given PID and max_frame.
        /// Returns a SlotHandle on success, or None if all slots are full.
        pub fn claim_reader_slot(&self, pid: u32, max_frame: u32) -> Option<SlotHandle> {
            let val = pack_slot(pid, max_frame);
            for (i, slot) in self.reader_slots().iter().enumerate() {
                if slot
                    .compare_exchange(0, val, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return Some(SlotHandle {
                        index: i + HEADER_SLOTS,
                    });
                }
            }
            None
        }

        /// Release a previously claimed reader slot.
        pub fn release_reader_slot(&self, handle: SlotHandle) {
            self.all_slots()[handle.index].store(0, Ordering::Release);
        }

        /// Release a reader slot by absolute index.
        pub fn release_slot_by_index(&self, index: usize) {
            debug_assert!(
                (HEADER_SLOTS..NUM_SLOTS).contains(&index),
                "release_slot_by_index: index {index} out of reader slot range [{HEADER_SLOTS}..{NUM_SLOTS})"
            );
            self.all_slots()[index].store(0, Ordering::Release);
        }

        /// Scan all reader slots and return the minimum max_frame across slots
        /// with live PIDs. Returns None if no reader slots are occupied.
        ///
        /// Also reclaims slots with dead PIDs as a side effect.
        pub fn min_reader_frame(&self) -> Option<u32> {
            self.min_reader_frame_excluding(0)
        }

        /// Return the minimum `max_frame` across all active reader slots,
        /// excluding any slots owned by `exclude_pid`. When the writer
        /// process wants to know about *external* readers only (to decide
        /// whether WAL restart is safe), it passes its own PID here so
        /// that its own reader slot doesn't block restart.
        pub fn min_reader_frame_excluding(&self, exclude_pid: u32) -> Option<u32> {
            let mut min_frame: Option<u32> = None;

            for slot in self.reader_slots().iter() {
                let val = slot.load(Ordering::Acquire);
                if val == 0 {
                    continue;
                }
                let (pid, max_frame) = unpack_slot(val);
                if exclude_pid != 0 && pid == exclude_pid {
                    continue;
                }
                if pid_is_alive(pid) {
                    min_frame = Some(match min_frame {
                        Some(current) => current.min(max_frame),
                        None => max_frame,
                    });
                } else {
                    // Reclaim dead slot. Use CAS to avoid clearing a slot that
                    // was reclaimed and re-claimed by another process between
                    // our load and this CAS.
                    let _ = slot.compare_exchange(val, 0, Ordering::AcqRel, Ordering::Relaxed);
                }
            }
            min_frame
        }

        /// Reclaim all reader slots belonging to dead PIDs.
        pub fn reclaim_dead_slots(&self) {
            for slot in self.reader_slots().iter() {
                let val = slot.load(Ordering::Acquire);
                if val == 0 {
                    continue;
                }
                let (pid, _) = unpack_slot(val);
                if !pid_is_alive(pid) {
                    let _ = slot.compare_exchange(val, 0, Ordering::AcqRel, Ordering::Relaxed);
                }
            }
        }

        /// Check if any reader slot is occupied (with a live PID).
        pub fn has_active_readers(&self) -> bool {
            self.min_reader_frame().is_some()
        }

        // --- Inter-process write lock (shared-memory CAS + flock) ---

        /// Acquire the inter-process write lock (non-blocking).
        /// Uses a two-level mechanism:
        /// 1. Shared-memory CAS on HDR_WRITE_LOCK (works across processes
        ///    AND within one process on macOS where flock is per-inode).
        /// 2. flock for defense-in-depth (handles stale mmap after crashes).
        ///
        /// MVCC refcount: only the 0→1 transition acquires the lock;
        /// subsequent writers in the same Database just bump the refcount.
        pub fn write_lock(&self) -> crate::Result<()> {
            loop {
                let prev = self
                    .write_refcount
                    .load(std::sync::atomic::Ordering::Acquire);
                if prev == 0 {
                    // Try the 0→1 transition atomically. If another thread
                    // beats us, retry the loop.
                    if self
                        .write_refcount
                        .compare_exchange(
                            0,
                            1,
                            std::sync::atomic::Ordering::AcqRel,
                            std::sync::atomic::Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    // We own the 0→1 transition — acquire the real locks.
                    if !self.try_acquire_shm_write_lock() {
                        self.write_refcount
                            .store(0, std::sync::atomic::Ordering::Release);
                        return Err(crate::LimboError::Busy);
                    }
                    // Also acquire flock for cross-process safety on Linux
                    // (where flock is per-fd and provides real exclusion).
                    let ret = unsafe { libc::flock(self.fd, libc::LOCK_EX | libc::LOCK_NB) };
                    if ret != 0 {
                        self.release_shm_write_lock();
                        self.write_refcount
                            .store(0, std::sync::atomic::Ordering::Release);
                        return Err(crate::LimboError::Busy);
                    }
                    return Ok(());
                } else {
                    // Lock already held by this process — bump the refcount.
                    // Use CAS to avoid racing with another thread's 0→1 or
                    // N→(N-1) transition.
                    if self
                        .write_refcount
                        .compare_exchange(
                            prev,
                            prev + 1,
                            std::sync::atomic::Ordering::AcqRel,
                            std::sync::atomic::Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    return Ok(());
                }
            }
        }

        /// Release the inter-process write lock.
        /// Only the last writer in this Database calls the actual release.
        pub fn write_unlock(&self) {
            let prev = self
                .write_refcount
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            debug_assert!(prev > 0, "write_unlock called without matching write_lock");
            if prev == 1 {
                self.release_shm_write_lock();
                unsafe {
                    libc::flock(self.fd, libc::LOCK_UN);
                }
            }
        }

        /// Returns the current number of active writers in this Database.
        pub fn active_writers(&self) -> u32 {
            self.write_refcount
                .load(std::sync::atomic::Ordering::Acquire)
        }

        /// Try to acquire the shared-memory write lock via CAS.
        /// Returns true if acquired, false if another live instance holds it.
        fn try_acquire_shm_write_lock(&self) -> bool {
            let slot = &self.all_slots()[HDR_WRITE_LOCK];
            let current = slot.load(Ordering::Acquire);
            if current == 0 {
                // Unlocked — try to claim it.
                return slot
                    .compare_exchange(0, self.instance_id, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
            }
            if current == self.instance_id {
                // We already hold it (shouldn't happen with refcount, but safe).
                return true;
            }
            // Someone else holds it. Check if they're still alive.
            let owner_pid = (current >> 32) as u32;
            if !pid_is_alive(owner_pid) {
                // Dead owner — steal the lock.
                return slot
                    .compare_exchange(
                        current,
                        self.instance_id,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok();
            }
            // Live owner, different instance — lock is held.
            false
        }

        /// Release the shared-memory write lock.
        fn release_shm_write_lock(&self) {
            let slot = &self.all_slots()[HDR_WRITE_LOCK];
            // Only clear if we're the owner (CAS to avoid clearing someone else's lock).
            let _ = slot.compare_exchange(self.instance_id, 0, Ordering::AcqRel, Ordering::Relaxed);
        }
    }

    impl Drop for Tshm {
        fn drop(&mut self) {
            if *self.write_refcount.get_mut() > 0 {
                // Release shared-memory write lock before closing the fd.
                self.release_shm_write_lock();
                unsafe {
                    libc::flock(self.fd, libc::LOCK_UN);
                }
            }
            unsafe {
                libc::munmap(self.ptr as *mut libc::c_void, PAGE_SIZE);
                libc::close(self.fd);
            }
        }
    }

    /// Check if a process with the given PID is alive.
    fn pid_is_alive(pid: u32) -> bool {
        // kill with signal 0 checks if the process exists without sending a signal.
        // Returns 0 if the process exists (and we have permission to signal it).
        // Returns -1 with ESRCH if the process does not exist.
        // Returns -1 with EPERM if we don't have permission (but the process exists).
        let ret = unsafe { libc::kill(pid as i32, 0) };
        if ret == 0 {
            return true;
        }
        // EPERM means the process exists but we can't signal it
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    use std::os::unix::io::IntoRawFd;

    #[cfg(test)]
    mod tests {
        use super::*;

        fn temp_tshm_path() -> (tempfile::TempDir, String) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test-tshm");
            (dir, path.to_str().unwrap().to_string())
        }

        #[test]
        fn test_open_creates_file() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();
            let metadata = std::fs::metadata(&path).unwrap();
            assert_eq!(metadata.len(), PAGE_SIZE as u64);
            drop(tshm);
        }

        #[test]
        fn test_claim_and_release_slot() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            let pid = std::process::id();
            let handle = tshm.claim_reader_slot(pid, 100).unwrap();

            // Verify slot is occupied (at absolute index in the mmap)
            let val = tshm.all_slots()[handle.index()].load(Ordering::Acquire);
            assert_eq!(val, pack_slot(pid, 100));

            // Release
            let idx = handle.index();
            tshm.release_reader_slot(handle);
            let val = tshm.all_slots()[idx].load(Ordering::Acquire);
            assert_eq!(val, 0);
        }

        #[test]
        fn test_min_reader_frame() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();
            let pid = std::process::id();

            // No readers → None
            assert_eq!(tshm.min_reader_frame(), None);

            let h1 = tshm.claim_reader_slot(pid, 200).unwrap();
            let h2 = tshm.claim_reader_slot(pid, 50).unwrap();
            let h3 = tshm.claim_reader_slot(pid, 150).unwrap();

            // Min should be 50
            assert_eq!(tshm.min_reader_frame(), Some(50));

            tshm.release_reader_slot(h2);
            // Min should now be 150
            assert_eq!(tshm.min_reader_frame(), Some(150));

            tshm.release_reader_slot(h3);
            assert_eq!(tshm.min_reader_frame(), Some(200));

            tshm.release_reader_slot(h1);
            assert_eq!(tshm.min_reader_frame(), None);
        }

        #[test]
        fn test_claim_all_reader_slots_returns_none() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();
            let pid = std::process::id();

            let mut handles = Vec::new();
            for i in 0..NUM_READER_SLOTS {
                let h = tshm.claim_reader_slot(pid, i as u32);
                assert!(h.is_some(), "failed to claim slot {i}");
                handles.push(h.unwrap());
            }

            // Next claim should fail — all reader slots are full.
            assert!(tshm.claim_reader_slot(pid, 999).is_none());

            // Release all
            for h in handles {
                tshm.release_reader_slot(h);
            }

            // Should be claimable again
            assert!(tshm.claim_reader_slot(pid, 0).is_some());
        }

        #[test]
        fn test_reader_slots_dont_touch_header() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();
            let pid = std::process::id();

            // Set a writer state in header slot
            let state = pack_writer_state(5, 42);
            tshm.set_writer_state(state);

            // Claim and release all reader slots
            let mut handles = Vec::new();
            for i in 0..NUM_READER_SLOTS {
                handles.push(tshm.claim_reader_slot(pid, i as u32).unwrap());
            }
            for h in handles {
                tshm.release_reader_slot(h);
            }

            // Header should be untouched
            assert_eq!(tshm.writer_state(), state);
        }

        #[test]
        fn test_reclaim_dead_slots() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            // Use a PID that (almost certainly) doesn't exist.
            // PID 1 is init/launchd and always exists, so use a very high PID.
            let dead_pid: u32 = 4_000_000;
            let val = pack_slot(dead_pid, 42);
            // Write to the first reader slot (absolute index HEADER_SLOTS)
            tshm.all_slots()[HEADER_SLOTS].store(val, Ordering::Release);

            // Before reclaim, slot is occupied
            assert_ne!(tshm.all_slots()[HEADER_SLOTS].load(Ordering::Acquire), 0);

            tshm.reclaim_dead_slots();

            // After reclaim, slot should be cleared (assuming PID 4000000 doesn't exist)
            if !pid_is_alive(dead_pid) {
                assert_eq!(tshm.all_slots()[HEADER_SLOTS].load(Ordering::Acquire), 0);
            }
        }

        #[test]
        fn test_has_active_readers() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();
            let pid = std::process::id();

            assert!(!tshm.has_active_readers());

            let h = tshm.claim_reader_slot(pid, 100).unwrap();
            assert!(tshm.has_active_readers());

            tshm.release_reader_slot(h);
            assert!(!tshm.has_active_readers());
        }

        #[test]
        fn test_concurrent_slot_claims() {
            let (_dir, path) = temp_tshm_path();
            let tshm = std::sync::Arc::new(Tshm::open(&path).unwrap());
            let pid = std::process::id();
            // Barrier ensures all threads finish claiming before any release,
            // so we get an accurate count with no slot recycling.
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));

            let mut handles = Vec::new();
            for t in 0..8 {
                let tshm = tshm.clone();
                let barrier = barrier.clone();
                handles.push(std::thread::spawn(move || {
                    let mut slot_handles = Vec::new();
                    for i in 0..64 {
                        let frame = (t * 64 + i) as u32;
                        if let Some(h) = tshm.claim_reader_slot(pid, frame) {
                            slot_handles.push(h);
                        }
                    }
                    let count = slot_handles.len();
                    // Wait for all threads to finish claiming before releasing.
                    barrier.wait();
                    for h in slot_handles {
                        tshm.release_reader_slot(h);
                    }
                    count
                }));
            }

            let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
            // 8 threads * 64 attempts = 512, but only 509 reader slots available.
            // 3 claims fail; total should be exactly NUM_READER_SLOTS.
            assert_eq!(total, NUM_READER_SLOTS);
        }

        #[test]
        fn test_pack_unpack_roundtrip() {
            let pid = 12345u32;
            let max_frame = 67890u32;
            let packed = pack_slot(pid, max_frame);
            let (unpacked_pid, unpacked_frame) = unpack_slot(packed);
            assert_eq!(unpacked_pid, pid);
            assert_eq!(unpacked_frame, max_frame);
        }

        #[test]
        fn test_pack_unpack_max_values() {
            let pid = u32::MAX;
            let max_frame = u32::MAX;
            let packed = pack_slot(pid, max_frame);
            let (unpacked_pid, unpacked_frame) = unpack_slot(packed);
            assert_eq!(unpacked_pid, pid);
            assert_eq!(unpacked_frame, max_frame);
        }

        #[test]
        fn test_shared_across_opens() {
            let (_dir, path) = temp_tshm_path();
            let pid = std::process::id();

            // First open: claim a slot
            let tshm1 = Tshm::open(&path).unwrap();
            let _h = tshm1.claim_reader_slot(pid, 42).unwrap();

            // Second open of same file: should see the slot
            let tshm2 = Tshm::open(&path).unwrap();
            assert_eq!(tshm2.min_reader_frame(), Some(42));
        }

        // --- Writer state tests ---

        #[test]
        fn test_writer_state_set_get() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            // Initially zero
            assert_eq!(tshm.writer_state(), 0);

            let state = pack_writer_state(3, 100);
            tshm.set_writer_state(state);
            assert_eq!(tshm.writer_state(), state);

            // Update
            let state2 = pack_writer_state(4, 200);
            tshm.set_writer_state(state2);
            assert_eq!(tshm.writer_state(), state2);
        }

        #[test]
        fn test_writer_state_shared_across_opens() {
            let (_dir, path) = temp_tshm_path();

            let tshm1 = Tshm::open(&path).unwrap();
            let state = pack_writer_state(7, 42);
            tshm1.set_writer_state(state);

            // Second open should see the writer state
            let tshm2 = Tshm::open(&path).unwrap();
            assert_eq!(tshm2.writer_state(), state);
        }

        #[test]
        fn test_writer_state_pack_roundtrip() {
            let ckpt = 12345u32;
            let frame = 67890u32;
            let packed = pack_writer_state(ckpt, frame);
            // Upper 32 = checkpoint_seq, lower 32 = max_frame
            assert_eq!((packed >> 32) as u32, ckpt);
            assert_eq!(packed as u32, frame);
        }

        #[test]
        fn test_increment_writer_state_monotonic() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            assert_eq!(tshm.writer_state(), 0);

            let s1 = tshm.increment_writer_state();
            assert_eq!(s1, 1);
            assert_eq!(tshm.writer_state(), 1);

            let s2 = tshm.increment_writer_state();
            assert_eq!(s2, 2);

            let s3 = tshm.increment_writer_state();
            assert_eq!(s3, 3);

            // Every call produces a unique value
            assert_ne!(s1, s2);
            assert_ne!(s2, s3);
        }

        /// Regression test for the ABA problem: when a WAL restart resets
        /// max_frame to 0 and the next write lands at the same max_frame as
        /// before, the old pack_writer_state(ckpt_seq, max_frame) encoding
        /// produced an identical value, causing readers to skip the rescan.
        /// The monotonic counter avoids this.
        #[test]
        fn test_increment_writer_state_no_aba() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            // Simulate: initial state after first write tx (some frames committed)
            let state_before = tshm.increment_writer_state();

            // Simulate: second write tx commits (WAL may restart internally,
            // but the counter always advances)
            let state_after = tshm.increment_writer_state();

            // Reader cached state_before. After the second commit, the reader
            // must see a different value to trigger a WAL rescan.
            assert_ne!(
                state_before, state_after,
                "writer state must differ after every commit to avoid ABA"
            );
        }

        #[test]
        fn test_increment_writer_state_visible_cross_open() {
            let (_dir, path) = temp_tshm_path();

            let tshm1 = Tshm::open(&path).unwrap();
            let s1 = tshm1.increment_writer_state();

            // Second open (simulating another process) should see the updated state
            let tshm2 = Tshm::open(&path).unwrap();
            assert_eq!(tshm2.writer_state(), s1);

            // Writer bumps again
            let s2 = tshm1.increment_writer_state();
            assert_ne!(s1, s2);

            // Reader sees the new value
            assert_eq!(tshm2.writer_state(), s2);
        }

        // --- Write lock tests (flock on tshm fd) ---

        #[test]
        fn test_write_lock_acquire_release() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            assert_eq!(tshm.active_writers(), 0);

            tshm.write_lock().unwrap();
            assert_eq!(tshm.active_writers(), 1);

            tshm.write_unlock();
            assert_eq!(tshm.active_writers(), 0);
        }

        #[test]
        fn test_write_lock_mvcc_refcount() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            // Simulate MVCC: multiple concurrent writers in the same process
            tshm.write_lock().unwrap();
            assert_eq!(tshm.active_writers(), 1);

            tshm.write_lock().unwrap();
            assert_eq!(tshm.active_writers(), 2);

            tshm.write_lock().unwrap();
            assert_eq!(tshm.active_writers(), 3);

            tshm.write_unlock();
            assert_eq!(tshm.active_writers(), 2);

            tshm.write_unlock();
            assert_eq!(tshm.active_writers(), 1);

            tshm.write_unlock();
            assert_eq!(tshm.active_writers(), 0);
        }

        #[test]
        fn test_write_lock_reacquire_after_full_release() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            tshm.write_lock().unwrap();
            tshm.write_unlock();
            assert_eq!(tshm.active_writers(), 0);

            // Re-acquire — should call flock again
            tshm.write_lock().unwrap();
            assert_eq!(tshm.active_writers(), 1);
            tshm.write_unlock();
        }

        #[test]
        fn test_write_lock_cross_process_exclusion() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            tshm.write_lock().unwrap();

            // Fork a child that tries to acquire the same lock
            let child = unsafe { libc::fork() };
            if child == 0 {
                let tshm2 = Tshm::open(&path).unwrap();
                let result = tshm2.write_lock();
                let code = if result.is_err() { 0 } else { 1 };
                unsafe { libc::_exit(code) };
            }

            assert!(child > 0, "fork failed");

            let mut status: libc::c_int = 0;
            unsafe { libc::waitpid(child, &mut status, 0) };
            assert!(
                libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
                "child should have failed to acquire lock (exit status: {})",
                libc::WEXITSTATUS(status)
            );

            tshm.write_unlock();
        }

        #[test]
        fn test_write_lock_concurrent_threads() {
            let (_dir, path) = temp_tshm_path();
            let tshm = std::sync::Arc::new(Tshm::open(&path).unwrap());

            let mut handles = Vec::new();
            for _ in 0..8 {
                let tshm = tshm.clone();
                handles.push(std::thread::spawn(move || {
                    for _ in 0..100 {
                        tshm.write_lock().unwrap();
                        std::thread::yield_now();
                        tshm.write_unlock();
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(tshm.active_writers(), 0);
        }

        #[test]
        fn test_write_lock_drop_releases() {
            let (_dir, path) = temp_tshm_path();

            {
                let tshm = Tshm::open(&path).unwrap();
                tshm.write_lock().unwrap();
                tshm.write_lock().unwrap();
                // Drop without explicit unlock
            }

            // After drop, a new instance should be able to acquire
            let tshm2 = Tshm::open(&path).unwrap();
            tshm2.write_lock().unwrap();
            tshm2.write_unlock();
        }

        #[test]
        fn test_locking_mode_slot_default_is_zero() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();
            assert_eq!(
                tshm.locking_mode(),
                0,
                "fresh TSHM should have mode 0 (unset)"
            );
        }

        #[test]
        fn test_locking_mode_set_and_read() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            // Set SharedWrites (2).
            assert!(tshm.try_set_locking_mode(2).is_ok());
            assert_eq!(tshm.locking_mode(), 2);

            // Second instance sees the same value via mmap.
            let tshm2 = Tshm::open(&path).unwrap();
            assert_eq!(tshm2.locking_mode(), 2);
        }

        #[test]
        fn test_locking_mode_cas_same_mode_ok() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            // First set succeeds (CAS 0 → 1).
            assert!(tshm.try_set_locking_mode(1).is_ok());

            // Same mode again succeeds (already 1, requested 1).
            assert!(tshm.try_set_locking_mode(1).is_ok());
        }

        #[test]
        fn test_locking_mode_cas_conflicting_mode_rejected() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            // First set: SharedReads (1).
            assert!(tshm.try_set_locking_mode(1).is_ok());

            // Conflicting: SharedWrites (2) — should fail with Err(1).
            let result = tshm.try_set_locking_mode(2);
            assert_eq!(result, Err(1), "conflicting mode should be rejected");

            // Second instance also rejected.
            let tshm2 = Tshm::open(&path).unwrap();
            let result = tshm2.try_set_locking_mode(2);
            assert_eq!(result, Err(1), "second instance conflicting mode rejected");
        }

        #[test]
        fn test_locking_mode_clear_and_reset() {
            let (_dir, path) = temp_tshm_path();
            let tshm = Tshm::open(&path).unwrap();

            tshm.try_set_locking_mode(2).unwrap();
            assert_eq!(tshm.locking_mode(), 2);

            // Clear resets to 0.
            tshm.clear_locking_mode();
            assert_eq!(tshm.locking_mode(), 0);

            // Can now set a different mode.
            assert!(tshm.try_set_locking_mode(1).is_ok());
            assert_eq!(tshm.locking_mode(), 1);
        }
    }
}

#[cfg(all(unix, target_pointer_width = "64"))]
#[allow(unused_imports)]
pub use imp::*;
