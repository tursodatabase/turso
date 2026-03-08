//! Shared WAL Index for multi-process coordination.
//!
//! The WAL index provides a shared `page_id → frame_id` mapping in mmap'd
//! shared memory, eliminating file I/O from the reader hot path.
//!
//! ## File layout (embedded in `{db}-tshm` starting at byte 16384)
//!
//! The WAL index lives in the tshm file immediately after the 16 KB
//! coordination page. The coordination page is 16 KB to fill one Apple
//! Silicon page and ensure mmap alignment on all platforms.
//! Segments are 32 KB each, separately mmap'd:
//!
//! ```text
//! Segment 0 (32768 bytes, file offset 16384):
//!   [0..128):       WalIndexHeader
//!   [128..16384):   aPgno[4064]  — u32: frame_offset → page_id
//!   [16384..32768): aHash[8192]  — u16: hash table for reverse lookup
//!
//! Segments 1+ (32768 bytes each, file offset 16384 + N*32768):
//!   [0..16384):     aPgno[4096]
//!   [16384..32768): aHash[8192]
//! ```
//!
//! ## Hash table
//!
//! Same design as SQLite's WAL index:
//! - Hash function: `(page_id * 383) % nslot`
//! - Linear probing with wraparound
//! - `aHash[i] == 0` → empty slot
//! - `aHash[i] == k` (k ≥ 1) → `aPgno[k-1]` holds the page_id
//!
//! ## Memory ordering
//!
//! Writer stores aPgno/aHash with Relaxed, then stores `max_frame` with
//! Release. Reader loads `max_frame` with Acquire, then reads aPgno/aHash
//! with Relaxed. The Release/Acquire pair on `max_frame` creates the
//! happens-before edge.

#[cfg(all(unix, target_pointer_width = "64"))]
mod imp {
    #[cfg(test)]
    use std::os::unix::io::IntoRawFd;
    use std::os::unix::io::RawFd;
    use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};

    const SEGMENT_SIZE: usize = 32_768;
    const HEADER_SIZE: usize = 128;

    /// Byte offset where WAL index segments begin within the tshm file.
    /// Must be a multiple of the system page size for mmap alignment (16 KB on
    /// Apple Silicon, 4 KB on x86_64). Matches the 16 KB coordination page size,
    /// so segments start immediately after with no wasted space.
    const BASE_OFFSET: usize = 16_384;

    /// Number of aPgno entries in segment 0 (after header).
    const APGNO_COUNT_SEG0: usize = (SEGMENT_SIZE - HEADER_SIZE - AHASH_SIZE) / 4;
    /// Number of aPgno entries in segments 1+.
    const APGNO_COUNT: usize = (SEGMENT_SIZE - AHASH_SIZE) / 4;
    /// Number of hash table slots per segment.
    const AHASH_NSLOT: usize = 8192;
    /// Size of hash table in bytes.
    const AHASH_SIZE: usize = AHASH_NSLOT * 2;

    /// Magic number for the WAL index header ("WALI").
    const WAL_INDEX_MAGIC: u32 = 0x5741_4C49;

    /// A single mmap'd segment of the WAL index.
    struct WalIndexSegment {
        ptr: *mut u8,
    }

    // SAFETY: mmap'd MAP_SHARED region accessed through atomics only.
    unsafe impl Send for WalIndexSegment {}
    unsafe impl Sync for WalIndexSegment {}

    impl WalIndexSegment {
        /// Get pointer to the aPgno array as a slice of AtomicU32.
        fn apgno(&self, seg_idx: usize) -> &[AtomicU32] {
            let offset = if seg_idx == 0 { HEADER_SIZE } else { 0 };
            let count = if seg_idx == 0 {
                APGNO_COUNT_SEG0
            } else {
                APGNO_COUNT
            };
            unsafe {
                let ptr = self.ptr.add(offset) as *const AtomicU32;
                std::slice::from_raw_parts(ptr, count)
            }
        }

        /// Get pointer to the aHash array as a slice of AtomicU16.
        fn ahash(&self, seg_idx: usize) -> &[AtomicU16] {
            let offset = if seg_idx == 0 {
                HEADER_SIZE + APGNO_COUNT_SEG0 * 4
            } else {
                APGNO_COUNT * 4
            };
            unsafe {
                let ptr = self.ptr.add(offset) as *const AtomicU16;
                std::slice::from_raw_parts(ptr, AHASH_NSLOT)
            }
        }
    }

    /// Compute the base frame number for a segment.
    /// Frame IDs are 1-based. Segment 0 holds frames 1..=APGNO_COUNT_SEG0,
    /// segment 1 holds frames (APGNO_COUNT_SEG0+1)..=(APGNO_COUNT_SEG0+APGNO_COUNT), etc.
    #[inline]
    fn seg_base_frame(seg_idx: usize) -> u32 {
        if seg_idx == 0 {
            0
        } else {
            APGNO_COUNT_SEG0 as u32 + (seg_idx as u32 - 1) * APGNO_COUNT as u32
        }
    }

    /// How many aPgno entries fit in this segment.
    #[inline]
    fn seg_capacity(seg_idx: usize) -> usize {
        if seg_idx == 0 {
            APGNO_COUNT_SEG0
        } else {
            APGNO_COUNT
        }
    }

    /// Which segment and offset within that segment a frame_id maps to.
    /// frame_id is 1-based.
    #[inline]
    fn frame_to_seg_offset(frame_id: u32) -> (usize, usize) {
        debug_assert!(frame_id > 0, "frame_id must be 1-based");
        let idx = frame_id as usize - 1; // 0-based index
        if idx < APGNO_COUNT_SEG0 {
            (0, idx)
        } else {
            let remaining = idx - APGNO_COUNT_SEG0;
            let seg = 1 + remaining / APGNO_COUNT;
            let offset = remaining % APGNO_COUNT;
            (seg, offset)
        }
    }

    /// Hash function for the WAL index (same as SQLite).
    #[inline]
    fn wal_hash(page_id: u32) -> usize {
        ((page_id as u64 * 383) % AHASH_NSLOT as u64) as usize
    }

    /// Search a single segment's hash table for the latest frame of `page_id`
    /// within `[min_frame, max_frame]`. Extracted to avoid duplicating the
    /// probe loop between the lock-free segment-0 fast path and the
    /// multi-segment slow path.
    #[inline]
    fn find_in_segment(
        seg: &WalIndexSegment,
        seg_idx: usize,
        page_id: u32,
        min_frame: u32,
        max_frame: u32,
    ) -> Option<u32> {
        let base = seg_base_frame(seg_idx);
        let cap = seg_capacity(seg_idx);
        let apgno = seg.apgno(seg_idx);
        let ahash = seg.ahash(seg_idx);

        let mut h = wal_hash(page_id);
        let mut best: Option<u32> = None;

        for _ in 0..AHASH_NSLOT {
            let slot_val = ahash[h].load(Ordering::Relaxed);
            if slot_val == 0 {
                break;
            }
            let frame_offset = (slot_val as usize) - 1;
            if frame_offset < cap {
                let stored_page = apgno[frame_offset].load(Ordering::Relaxed);
                if stored_page == page_id {
                    let global_frame = base + frame_offset as u32 + 1;
                    if global_frame >= min_frame && global_frame <= max_frame {
                        best = Some(match best {
                            Some(b) => b.max(global_frame),
                            None => global_frame,
                        });
                    }
                }
            }
            h = (h + 1) % AHASH_NSLOT;
        }
        best
    }

    /// Insert a page_id → offset mapping into the hash table via linear probing.
    fn hash_insert(ahash: &[AtomicU16], page_id: u32, offset: usize) -> crate::Result<()> {
        let nslot = ahash.len();
        let mut h = wal_hash(page_id) % nslot;
        for _ in 0..nslot {
            if ahash[h].load(Ordering::Relaxed) == 0 {
                ahash[h].store((offset + 1) as u16, Ordering::Relaxed);
                return Ok(());
            }
            h = (h + 1) % nslot;
        }
        Err(crate::LimboError::InternalError(
            "wal_index hash table full or corrupt (no empty slot found)".to_string(),
        ))
    }

    /// Ensure file is at least `needed_size` bytes, extending with ftruncate if needed.
    fn ensure_file_size(fd: RawFd, needed_size: u64) -> crate::Result<()> {
        let current_size = unsafe {
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstat(fd, &mut stat) != 0 {
                return Err(crate::LimboError::InternalError(format!(
                    "wal_index fstat: {}",
                    std::io::Error::last_os_error()
                )));
            }
            stat.st_size as u64
        };
        if current_size < needed_size {
            let ret = unsafe { libc::ftruncate(fd, needed_size as libc::off_t) };
            if ret != 0 {
                return Err(crate::LimboError::InternalError(format!(
                    "wal_index ftruncate: {}",
                    std::io::Error::last_os_error()
                )));
            }
        }
        Ok(())
    }

    /// Shared WAL index providing page_id → frame_id lookup via mmap'd shared memory.
    pub struct WalIndex {
        fd: RawFd,
        /// Whether this instance owns the fd and should close it on drop.
        /// False when borrowing the fd from Tshm (production path).
        /// True when opened standalone (unit tests).
        owns_fd: bool,
        /// RwLock protects the Vec (segment count), not the mmap'd data.
        segments: crate::sync::RwLock<Vec<WalIndexSegment>>,
        /// Direct pointer to the header in segment 0's mmap region.
        /// Valid for the entire lifetime of WalIndex (segment 0 is never unmapped).
        header_ptr: *const WalIndexHeader,
        /// Test-only: when true, `append_frame` returns an error to simulate
        /// disk failures (ftruncate/mmap) during segment growth.
        #[cfg(test)]
        inject_failure: std::sync::atomic::AtomicBool,
    }

    // SAFETY: mmap'd MAP_SHARED, accessed through atomics. header_ptr points
    // into segment 0 which lives for the lifetime of WalIndex.
    unsafe impl Send for WalIndex {}
    unsafe impl Sync for WalIndex {}

    impl WalIndex {
        /// Open the WAL index using a borrowed fd from Tshm.
        ///
        /// The fd is NOT owned — WalIndex will not close it on drop.
        /// This is critical: POSIX fcntl locks are per-process-per-inode,
        /// so closing any fd to the same file would release Tshm's write lock.
        pub fn open(fd: RawFd) -> crate::Result<Self> {
            Self::open_inner(fd, false)
        }

        /// Open a standalone WAL index for unit tests.
        /// Creates its own file (with BASE_OFFSET padding) and owns the fd.
        #[cfg(test)]
        pub fn open_standalone(path: &str) -> crate::Result<Self> {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(|e| {
                    crate::LimboError::InternalError(format!("wal_index file '{path}': {e}"))
                })?;
            let fd = file.into_raw_fd();
            Self::open_inner(fd, true)
        }

        fn open_inner(fd: RawFd, owns_fd: bool) -> crate::Result<Self> {
            // Ensure file is large enough for BASE_OFFSET + one segment.
            ensure_file_size(fd, (BASE_OFFSET + SEGMENT_SIZE) as u64)?;

            // mmap segment 0.
            let seg0 = mmap_segment(fd, 0)?;
            let header_ptr = seg0.ptr as *const WalIndexHeader;

            let wal_index = Self {
                fd,
                owns_fd,
                segments: crate::sync::RwLock::new(vec![seg0]),
                header_ptr,
                #[cfg(test)]
                inject_failure: std::sync::atomic::AtomicBool::new(false),
            };

            // Initialize header if fresh file.
            let header = wal_index.header();
            if header.magic.load(Ordering::Acquire) != WAL_INDEX_MAGIC {
                header.magic.store(WAL_INDEX_MAGIC, Ordering::Release);
                header.max_frame.store(0, Ordering::Release);
                header.nbackfills.store(0, Ordering::Release);
                header.num_segments.store(1, Ordering::Release);
                header.generation.store(0, Ordering::Release);
                header.salt_1.store(0, Ordering::Release);
                header.salt_2.store(0, Ordering::Release);
                header.pgno_checksum.store(0, Ordering::Release);
            } else {
                // Existing file — verify hash table integrity.
                let stored_cksum = header.pgno_checksum.load(Ordering::Acquire);
                let max_frame = header.max_frame.load(Ordering::Acquire);
                let num_segments = header.num_segments.load(Ordering::Acquire);

                // Ensure all segments are mapped.
                if num_segments > 1 {
                    wal_index.ensure_segments_mapped_inner(num_segments)?;
                }

                if max_frame > 0 {
                    let segments = wal_index.segments.read();
                    let computed = Self::compute_pgno_checksum_with(&segments, max_frame);
                    if computed != stored_cksum {
                        return Err(crate::LimboError::Corrupt(format!(
                            "WalIndex hash table corruption detected at open \
                             (stored_checksum={stored_cksum:#x}, computed={computed:#x}, \
                             max_frame={max_frame}). \
                             Delete the -tshm file and reopen the database to recover."
                        )));
                    }
                    drop(segments);
                    // Clean up stale hash entries left by a crashed writer
                    // (appended via append_frame but never committed via
                    // commit_max_frame). Without this, stale entries
                    // accumulate across crash cycles, lengthening probe
                    // chains and eventually exhausting hash slots.
                    wal_index.cleanup_uncommitted();
                }
            }

            Ok(wal_index)
        }

        /// Get a reference to the header (at the start of segment 0).
        /// Lock-free: uses a pointer cached at construction time.
        #[inline]
        fn header(&self) -> &WalIndexHeader {
            // SAFETY: segment 0 is always mapped for the lifetime of WalIndex,
            // and the header is at offset 0, properly aligned (mmap returns
            // page-aligned memory, all fields are naturally aligned u32).
            unsafe { &*self.header_ptr }
        }

        /// Get current max_frame from header.
        pub fn max_frame(&self) -> u32 {
            self.header().max_frame.load(Ordering::Acquire)
        }

        /// Get current generation from header.
        pub fn generation(&self) -> u32 {
            self.header().generation.load(Ordering::Acquire)
        }

        /// Get salt values from header.
        pub fn salt(&self) -> (u32, u32) {
            let h = self.header();
            (
                h.salt_1.load(Ordering::Acquire),
                h.salt_2.load(Ordering::Acquire),
            )
        }

        /// Set salt values in header (called by writer after WAL restart).
        pub fn set_salt(&self, salt_1: u32, salt_2: u32) {
            let h = self.header();
            h.salt_1.store(salt_1, Ordering::Release);
            h.salt_2.store(salt_2, Ordering::Release);
        }

        /// Get the pgno_checksum from header. Used by readers to detect
        /// hash table corruption mid-transaction.
        pub fn pgno_checksum(&self) -> u32 {
            self.header().pgno_checksum.load(Ordering::Acquire)
        }

        /// Get page_size from header.
        pub fn page_size(&self) -> u32 {
            self.header().page_size.load(Ordering::Acquire)
        }

        /// Set page_size in header (called by writer).
        pub fn set_page_size(&self, page_size: u32) {
            self.header().page_size.store(page_size, Ordering::Release);
        }

        /// Get current nbackfills from header.
        pub fn nbackfills(&self) -> u32 {
            self.header().nbackfills.load(Ordering::Acquire)
        }

        /// Set nbackfills after checkpoint.
        pub fn set_nbackfills(&self, val: u32) {
            self.header().nbackfills.store(val, Ordering::Release);
        }

        /// Get rolling checksum of the last WAL frame.
        pub fn last_checksum(&self) -> (u32, u32) {
            let h = self.header();
            (
                h.last_checksum_1.load(Ordering::Acquire),
                h.last_checksum_2.load(Ordering::Acquire),
            )
        }

        /// Set rolling checksum of the last WAL frame (called by writer on commit).
        pub fn set_last_checksum(&self, c1: u32, c2: u32) {
            let h = self.header();
            h.last_checksum_1.store(c1, Ordering::Release);
            h.last_checksum_2.store(c2, Ordering::Release);
        }

        /// Get checkpoint_seq from header.
        pub fn checkpoint_seq(&self) -> u32 {
            self.header().checkpoint_seq.load(Ordering::Acquire)
        }

        /// Set checkpoint_seq in header (called by writer after checkpoint).
        pub fn set_checkpoint_seq(&self, seq: u32) {
            self.header().checkpoint_seq.store(seq, Ordering::Release);
        }

        /// Look up the latest frame for `page_id` within [min_frame, max_frame].
        /// Reads only from mmap'd shared memory — zero file I/O.
        pub fn find_frame(&self, page_id: u32, min_frame: u32, max_frame: u32) -> Option<u32> {
            if max_frame == 0 || page_id == 0 || max_frame < min_frame {
                return None;
            }

            // Fast path: all frames fit in segment 0 (99%+ of workloads).
            // SAFETY: header_ptr is derived from segment 0's mmap, which
            // is valid for the entire lifetime of WalIndex.
            if max_frame <= APGNO_COUNT_SEG0 as u32 {
                let seg0 = WalIndexSegment {
                    ptr: self.header_ptr as *mut u8,
                };
                return find_in_segment(&seg0, 0, page_id, min_frame, max_frame);
            }

            // Multi-segment slow path: acquire RwLock.
            let segments = self.segments.read();
            for seg_idx in (0..segments.len()).rev() {
                if let Some(frame) =
                    find_in_segment(&segments[seg_idx], seg_idx, page_id, min_frame, max_frame)
                {
                    return Some(frame);
                }
            }
            None
        }

        /// Test-only: brute-force scan of aPgno arrays to find ALL frames
        /// for a page_id, ignoring hash tables. Used to debug hash lookup issues.
        #[cfg(test)]
        pub fn brute_force_find(&self, page_id: u32, min_frame: u32, max_frame: u32) -> Vec<u32> {
            let mut results = Vec::new();
            let segments = self.segments.read();
            for (seg_idx, seg) in segments.iter().enumerate() {
                let base = seg_base_frame(seg_idx);
                let apgno = seg.apgno(seg_idx);
                for (offset, entry) in apgno.iter().enumerate() {
                    let global_frame = base + offset as u32 + 1;
                    if global_frame < min_frame || global_frame > max_frame {
                        continue;
                    }
                    if entry.load(Ordering::Relaxed) == page_id {
                        results.push(global_frame);
                    }
                }
            }
            results
        }

        /// Test-only: dump hash table probe chain for a page_id.
        #[cfg(test)]
        pub fn debug_hash_chain(&self, page_id: u32) -> String {
            let segments = self.segments.read();
            let mut out = String::new();
            for (seg_idx, seg) in segments.iter().enumerate() {
                let ahash = seg.ahash(seg_idx);
                let apgno = seg.apgno(seg_idx);
                let cap = seg_capacity(seg_idx);
                let base = seg_base_frame(seg_idx);
                let mut h = wal_hash(page_id);
                use std::fmt::Write;
                write!(out, "seg{seg_idx}[hash={h}]: ").unwrap();
                for i in 0..20 {
                    let val = ahash[h].load(Ordering::Relaxed);
                    if val == 0 {
                        write!(out, "slot[{h}]=0(empty) ").unwrap();
                        break;
                    }
                    let offset = (val as usize) - 1;
                    let pg = if offset < cap {
                        apgno[offset].load(Ordering::Relaxed)
                    } else {
                        u32::MAX
                    };
                    let gf = base + offset as u32 + 1;
                    write!(out, "slot[{h}]={val}(pg={pg},gf={gf}) ").unwrap();
                    h = (h + 1) % AHASH_NSLOT;
                    if i == 19 {
                        write!(out, "...").unwrap();
                    }
                }
            }
            out
        }

        /// Test-only: return the segment 0 mmap pointer for diagnostics.
        #[cfg(test)]
        pub fn debug_seg0_ptr(&self) -> usize {
            let segments = self.segments.read();
            segments[0].ptr as usize
        }

        /// Test-only: read a specific ahash slot value.
        #[cfg(test)]
        pub fn debug_ahash_slot(&self, slot: usize) -> u16 {
            let segments = self.segments.read();
            segments[0].ahash(0)[slot].load(Ordering::SeqCst)
        }

        /// Test-only: return the fd used by this WalIndex.
        #[cfg(test)]
        pub fn debug_fd(&self) -> i32 {
            self.fd
        }

        /// Test-only: dump raw memory at the ahash location for a page_id.
        /// Bypasses AtomicU16::load to check if the issue is in atomic ops.
        #[cfg(test)]
        pub fn debug_raw_ahash(&self, page_id: u32) -> String {
            let segments = self.segments.read();
            let mut out = String::new();
            use std::fmt::Write;
            for (seg_idx, seg) in segments.iter().enumerate() {
                let h = wal_hash(page_id);
                let ahash = seg.ahash(seg_idx);
                let apgno = seg.apgno(seg_idx);

                // Read via AtomicU16::load
                let atomic_val = ahash[h].load(Ordering::SeqCst);

                // Read via raw pointer (volatile)
                let raw_val = unsafe {
                    let ptr = &ahash[h] as *const AtomicU16 as *const u16;
                    std::ptr::read_volatile(ptr)
                };

                // Read apgno[0] for comparison
                let apgno0_atomic = apgno[0].load(Ordering::SeqCst);
                let apgno0_raw = unsafe {
                    let ptr = &apgno[0] as *const AtomicU32 as *const u32;
                    std::ptr::read_volatile(ptr)
                };

                // Also read a few surrounding ahash slots
                let mut surrounding = String::new();
                for i in 0..5 {
                    let idx = (h + i) % AHASH_NSLOT;
                    let v = ahash[idx].load(Ordering::SeqCst);
                    write!(surrounding, "[{idx}]={v} ").unwrap();
                }

                // Segment pointer info
                let seg_ptr = seg.ptr as usize;
                let ahash_offset = if seg_idx == 0 {
                    HEADER_SIZE + APGNO_COUNT_SEG0 * 4
                } else {
                    APGNO_COUNT * 4
                };
                let ahash_ptr = seg_ptr + ahash_offset + h * 2;

                write!(
                    out,
                    "seg{seg_idx}: ahash[{h}] atomic={atomic_val} raw={raw_val}, \
                     apgno[0] atomic={apgno0_atomic} raw={apgno0_raw}, \
                     seg_ptr=0x{seg_ptr:x} ahash_ptr=0x{ahash_ptr:x}, \
                     nearby: {surrounding}"
                )
                .unwrap();
            }
            out
        }

        /// Test-only: make all subsequent `append_frame` calls fail.
        #[cfg(test)]
        pub fn inject_append_failure(&self, fail: bool) {
            self.inject_failure
                .store(fail, std::sync::atomic::Ordering::Relaxed);
        }

        /// Test-only: zero all hash tables while keeping aPgno and max_frame
        /// intact. Simulates hash table corruption — find_frame returns None
        /// for every page even though the data is in the index.
        #[cfg(test)]
        pub fn corrupt_hash_tables(&self) {
            let segments = self.segments.read();
            for (seg_idx, seg) in segments.iter().enumerate() {
                let ahash = seg.ahash(seg_idx);
                for entry in ahash {
                    entry.store(0, Ordering::Relaxed);
                }
            }
            // Also corrupt the pgno_checksum so the integrity check detects
            // the corruption and triggers a rebuild. In a real crash, both
            // ahash and pgno_checksum would be stale.
            self.header()
                .pgno_checksum
                .store(0xDEAD_BEEF, Ordering::Release);
        }

        /// Append a frame entry. Called by writer after committing a frame.
        /// May grow the file and mmap new segments.
        pub fn append_frame(&self, page_id: u32, frame_id: u32) -> crate::Result<()> {
            #[cfg(test)]
            if self.inject_failure.load(Ordering::Relaxed) {
                return Err(crate::LimboError::InternalError(
                    "injected append_frame failure".to_string(),
                ));
            }

            assert!(page_id > 0, "page_id must be > 0");
            assert!(frame_id > 0, "frame_id must be 1-based");

            let (seg_idx, offset) = frame_to_seg_offset(frame_id);

            // Grow if needed.
            self.ensure_segment_exists(seg_idx)?;

            let segments = self.segments.read();
            let seg = &segments[seg_idx];
            let apgno = seg.apgno(seg_idx);
            let ahash = seg.ahash(seg_idx);

            // Store page_id in aPgno (source of truth).
            apgno[offset].store(page_id, Ordering::Relaxed);

            // Insert into hash table via linear probing.
            hash_insert(ahash, page_id, offset)?;

            Ok(())
        }

        /// Make appended frames visible to readers by updating header.max_frame.
        /// Called after all frames in a commit batch have been appended.
        /// Incrementally updates pgno_checksum — only processes newly committed
        /// frames [old_max+1..=frame_id] instead of recomputing from frame 1.
        pub fn commit_max_frame(&self, frame_id: u32) {
            let header = self.header();
            let old_max = header.max_frame.load(Ordering::Acquire);
            assert!(
                frame_id >= old_max,
                "commit_max_frame: frame_id ({frame_id}) must not decrease max_frame ({old_max})"
            );
            let old_checksum = header.pgno_checksum.load(Ordering::Acquire);

            // Incrementally extend the checksum from old_max+1 to frame_id.
            let segments = self.segments.read();
            let mut checksum = old_checksum;
            for fid in (old_max + 1)..=frame_id {
                let (seg_idx, offset) = frame_to_seg_offset(fid);
                if seg_idx >= segments.len() {
                    break;
                }
                let page_id = segments[seg_idx].apgno(seg_idx)[offset].load(Ordering::Relaxed);
                checksum = checksum.wrapping_add(page_id).wrapping_mul(17);
            }
            drop(segments);

            header.pgno_checksum.store(checksum, Ordering::Relaxed);
            // Release barrier: ensures all aPgno/aHash stores above are visible
            // to any reader that loads this max_frame with Acquire.
            header.max_frame.store(frame_id, Ordering::Release);
        }

        /// Clear the entire index. Called on WAL restart/truncate.
        /// Auto-increments the generation counter so readers with stale
        /// snapshots detect the change and fall back to inner find_frame.
        pub fn clear(&self, salt_1: u32, salt_2: u32) {
            #[cfg(test)]
            {
                let self_addr = self as *const Self as usize;
                let bt = std::backtrace::Backtrace::force_capture();
                eprintln!(
                    "WalIndex::clear called on 0x{self_addr:x}, max_frame={}\n{bt}",
                    self.max_frame()
                );
            }
            let header = self.header();
            // Bump generation and zero max_frame BEFORE zeroing data —
            // concurrent readers that load the new generation will see
            // the mismatch against cached_gen and fall back to
            // inner.find_frame(), avoiding reads from zeroed hash tables.
            header.bump_generation();
            header.max_frame.store(0, Ordering::Release);

            let segments = self.segments.read();
            for (seg_idx, seg) in segments.iter().enumerate() {
                let apgno = seg.apgno(seg_idx);
                let ahash = seg.ahash(seg_idx);
                for entry in apgno {
                    entry.store(0, Ordering::Relaxed);
                }
                for entry in ahash {
                    entry.store(0, Ordering::Relaxed);
                }
            }
            drop(segments);

            header.nbackfills.store(0, Ordering::Release);
            header.salt_1.store(salt_1, Ordering::Release);
            header.salt_2.store(salt_2, Ordering::Release);
            header.pgno_checksum.store(0, Ordering::Release);
            // Keep num_segments as-is; segments are reusable.
        }

        /// Zero aPgno entries beyond `max_frame` and rebuild hash tables
        /// for affected segments. Does NOT update the header (max_frame,
        /// pgno_checksum) — callers handle that. Returns true if any
        /// entries were actually trimmed.
        fn trim_beyond(segments: &[WalIndexSegment], max_frame: u32) -> bool {
            let mut any_trimmed = false;
            for (seg_idx, seg) in segments.iter().enumerate() {
                let base = seg_base_frame(seg_idx);
                let apgno = seg.apgno(seg_idx);
                for (offset, entry) in apgno.iter().enumerate() {
                    let global_frame = base + offset as u32 + 1;
                    if global_frame > max_frame && entry.load(Ordering::Relaxed) != 0 {
                        entry.store(0, Ordering::Relaxed);
                        any_trimmed = true;
                    }
                }
            }
            if any_trimmed {
                for (seg_idx, seg) in segments.iter().enumerate() {
                    let base = seg_base_frame(seg_idx);
                    let cap = seg_capacity(seg_idx);
                    let seg_last_frame = base + cap as u32;
                    if seg_last_frame > max_frame {
                        Self::rebuild_segment_hash(seg, seg_idx, max_frame);
                    }
                }
            }
            any_trimmed
        }

        /// Clean up any uncommitted entries beyond the committed max_frame.
        /// Called after rollback(None) to remove hash table entries that
        /// were added by `append_frame` but never made visible via
        /// `commit_max_frame`. Without this, stale hash entries pollute
        /// the table and can cause collisions on subsequent writes.
        pub fn cleanup_uncommitted(&self) {
            let header = self.header();
            let current_max = header.max_frame.load(Ordering::Acquire);
            let segments = self.segments.read();
            // Check if there are any uncommitted entries to clean up.
            let has_uncommitted = segments.iter().enumerate().any(|(seg_idx, seg)| {
                let base = seg_base_frame(seg_idx);
                seg.apgno(seg_idx)
                    .iter()
                    .enumerate()
                    .any(|(offset, entry)| {
                        let global_frame = base + offset as u32 + 1;
                        global_frame > current_max && entry.load(Ordering::Relaxed) != 0
                    })
            });
            if !has_uncommitted {
                return;
            }
            // Bump generation BEFORE rebuilding hash tables so concurrent
            // readers detect the mismatch and fall back to inner.find_frame,
            // avoiding reads from partially-rebuilt hash tables.
            header.bump_generation();
            Self::trim_beyond(&segments, current_max);
        }

        /// Trim frames > max_frame. Called on rollback.
        /// Rebuilds hash tables for affected segments.
        pub fn rollback_to(&self, max_frame: u32) {
            let header = self.header();
            let current_max = header.max_frame.load(Ordering::Acquire);
            if max_frame >= current_max {
                return; // Nothing to trim.
            }

            // Bump generation BEFORE rebuilding hash tables so concurrent
            // readers detect the mismatch and fall back to inner.find_frame,
            // avoiding reads from partially-rebuilt hash tables.
            header.bump_generation();
            let segments = self.segments.read();
            Self::trim_beyond(&segments, max_frame);
            let checksum = Self::compute_pgno_checksum_with(&segments, max_frame);
            drop(segments);
            self.header()
                .pgno_checksum
                .store(checksum, Ordering::Relaxed);
            self.header().max_frame.store(max_frame, Ordering::Release);
        }

        /// Iterate all (page_id, frame_id) pairs in [min_frame, max_frame].
        /// Used by checkpoint to determine which pages to back up.
        /// For each page, returns only the latest frame in the range.
        pub fn iter_latest_frames(&self, min_frame: u32, max_frame: u32) -> Vec<(u32, u32)> {
            use rustc_hash::FxHashMap;
            let mut latest: FxHashMap<u32, u32> = FxHashMap::default();
            let segments = self.segments.read();

            for (seg_idx, seg) in segments.iter().enumerate() {
                let base = seg_base_frame(seg_idx);
                let apgno = seg.apgno(seg_idx);

                for (offset, entry) in apgno.iter().enumerate() {
                    let global_frame = base + offset as u32 + 1;
                    if global_frame < min_frame || global_frame > max_frame {
                        continue;
                    }
                    let page_id = entry.load(Ordering::Relaxed);
                    if page_id != 0 {
                        latest
                            .entry(page_id)
                            .and_modify(|f| *f = (*f).max(global_frame))
                            .or_insert(global_frame);
                    }
                }
            }

            let mut result: Vec<(u32, u32)> = latest.into_iter().collect();
            result.sort_unstable_by_key(|&(_, frame)| frame);
            result
        }

        /// Verify WalIndex hash table integrity. Returns `Ok(())` if intact,
        /// or an error if corruption is detected. The caller should fail the
        /// operation — recovery is done by deleting the tshm file and reopening.
        ///
        /// On ARM (Apple Silicon), `commit_max_frame` stores pgno_checksum
        /// (Relaxed) and max_frame (Release) as two separate atomics. A reader
        /// can transiently observe the new pgno_checksum with the old max_frame,
        /// causing a false mismatch. We retry once on mismatch to distinguish
        /// transient races from real corruption.
        pub fn verify_hash_integrity(&self) -> crate::Result<()> {
            let header = self.header();
            let segments = self.segments.read();
            let mut last_max = 0u32;
            let mut last_stored = 0u32;
            let mut last_computed = 0u32;

            // Retry once: a mismatch on the first attempt may be a transient
            // race between pgno_checksum and max_frame stores on ARM.
            for _ in 0..2 {
                let max_frame = header.max_frame.load(Ordering::Acquire);
                if max_frame == 0 {
                    return Ok(());
                }
                let stored = header.pgno_checksum.load(Ordering::Acquire);
                let computed = Self::compute_pgno_checksum_with(&segments, max_frame);
                if stored == computed {
                    return Ok(());
                }
                last_max = max_frame;
                last_stored = stored;
                last_computed = computed;
            }
            drop(segments);
            Err(crate::LimboError::Corrupt(format!(
                "WalIndex hash table corruption detected \
                 (stored_checksum={last_stored:#x}, computed={last_computed:#x}, \
                 max_frame={last_max}). \
                 Delete the -tshm file and reopen the database to recover."
            )))
        }

        /// Ensure all segments up to the current num_segments are mmap'd.
        /// Called by readers when they detect the writer added segments.
        pub fn ensure_segments_mapped(&self) -> crate::Result<()> {
            let num_segments = self.header().num_segments.load(Ordering::Acquire);
            self.ensure_segments_mapped_inner(num_segments)
        }

        fn ensure_segments_mapped_inner(&self, num_segments: u32) -> crate::Result<()> {
            let current_count = self.segments.read().len();
            if current_count >= num_segments as usize {
                return Ok(());
            }

            let mut segments = self.segments.write();
            // Re-check under write lock.
            if segments.len() >= num_segments as usize {
                return Ok(());
            }

            for i in segments.len()..num_segments as usize {
                let seg = mmap_segment(self.fd, i)?;
                segments.push(seg);
            }
            Ok(())
        }

        /// Ensure a specific segment exists (grow file + mmap if needed).
        fn ensure_segment_exists(&self, seg_idx: usize) -> crate::Result<()> {
            let current_count = self.segments.read().len();
            if seg_idx < current_count {
                return Ok(());
            }

            let mut segments = self.segments.write();
            // Re-check under write lock.
            if seg_idx < segments.len() {
                return Ok(());
            }

            // Grow file to accommodate the new segment (after BASE_OFFSET).
            ensure_file_size(self.fd, (BASE_OFFSET + (seg_idx + 1) * SEGMENT_SIZE) as u64)?;

            // mmap new segments.
            for i in segments.len()..=seg_idx {
                let seg = mmap_segment(self.fd, i)?;
                segments.push(seg);
            }

            // Update header directly (no lock needed — header_ptr is always valid).
            self.header()
                .num_segments
                .store(segments.len() as u32, Ordering::Release);

            Ok(())
        }

        /// Compute checksum over aPgno entries [1..=max_frame] using an already-locked segments slice.
        fn compute_pgno_checksum_with(segments: &[WalIndexSegment], max_frame: u32) -> u32 {
            let mut checksum: u32 = 0;
            for frame_id in 1..=max_frame {
                let (seg_idx, offset) = frame_to_seg_offset(frame_id);
                if seg_idx >= segments.len() {
                    break;
                }
                let page_id = segments[seg_idx].apgno(seg_idx)[offset].load(Ordering::Relaxed);
                checksum = checksum.wrapping_add(page_id).wrapping_mul(17);
            }
            checksum
        }

        /// Rebuild the hash table for a single segment from its aPgno.
        /// Used by `rollback_to` after trimming aPgno entries beyond max_frame.
        fn rebuild_segment_hash(seg: &WalIndexSegment, seg_idx: usize, max_frame: u32) {
            let base = seg_base_frame(seg_idx);
            let apgno = seg.apgno(seg_idx);
            let ahash = seg.ahash(seg_idx);

            // Clear the entire hash table.
            for entry in ahash {
                entry.store(0, Ordering::Relaxed);
            }

            // Re-insert valid entries.
            for (offset, entry) in apgno.iter().enumerate() {
                let global_frame = base + offset as u32 + 1;
                if global_frame > max_frame {
                    break;
                }
                let page_id = entry.load(Ordering::Relaxed);
                if page_id == 0 {
                    continue;
                }

                // AHASH_NSLOT (8192) > max aPgno entries per segment (4064/4096),
                // so the hash table should never be full during a rebuild.
                let result = hash_insert(ahash, page_id, offset);
                debug_assert!(
                    result.is_ok(),
                    "rebuild_segment_hash: hash table overflow at seg={seg_idx} offset={offset}"
                );
                if let Err(e) = result {
                    tracing::error!(
                        "rebuild_segment_hash: {e} at seg={seg_idx} \
                         offset={offset} page_id={page_id} max_frame={max_frame}"
                    );
                }
            }
        }
    }

    impl Drop for WalIndex {
        fn drop(&mut self) {
            let segments = self.segments.get_mut();
            for seg in segments.drain(..) {
                unsafe {
                    libc::munmap(seg.ptr as *mut libc::c_void, SEGMENT_SIZE);
                }
            }
            if self.owns_fd {
                unsafe {
                    libc::close(self.fd);
                }
            }
        }
    }

    /// mmap a single segment at the given index.
    /// Segments start at BASE_OFFSET within the file (after the tshm coordination page).
    fn mmap_segment(fd: RawFd, seg_idx: usize) -> crate::Result<WalIndexSegment> {
        let offset = BASE_OFFSET + seg_idx * SEGMENT_SIZE;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                SEGMENT_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                offset as libc::off_t,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(crate::LimboError::InternalError(format!(
                "wal_index mmap segment {seg_idx}: {}",
                std::io::Error::last_os_error()
            )));
        }
        #[cfg(test)]
        {
            let inode = unsafe {
                let mut stat: libc::stat = std::mem::zeroed();
                libc::fstat(fd, &mut stat);
                stat.st_ino
            };
            // Read ahash[383] immediately after mapping
            let ahash_val = if seg_idx == 0 {
                let ahash_offset = HEADER_SIZE + APGNO_COUNT_SEG0 * 4 + 383 * 2;
                unsafe {
                    let ahash_ptr = (ptr as *const u8).add(ahash_offset) as *const u16;
                    std::ptr::read_volatile(ahash_ptr)
                }
            } else {
                0
            };
            eprintln!(
                "MMAP_SEGMENT: seg_idx={seg_idx} fd={fd} inode={inode} offset=0x{offset:x} \
                 ptr=0x{:x} ahash[383]={ahash_val}",
                ptr as usize
            );
        }
        Ok(WalIndexSegment {
            ptr: ptr as *mut u8,
        })
    }

    /// WAL index header at the start of segment 0.
    #[repr(C)]
    struct WalIndexHeader {
        /// Number of valid frames (1-based count: frames 1..=max_frame).
        max_frame: AtomicU32,
        /// Number of frames checkpointed to DB file.
        nbackfills: AtomicU32,
        /// Number of 32KB segments in use.
        num_segments: AtomicU32,
        /// WAL generation (matches checkpoint_seq from WalHeader).
        generation: AtomicU32,
        /// Salt values from WAL header (for crash detection).
        salt_1: AtomicU32,
        salt_2: AtomicU32,
        /// Checksum over aPgno entries for corruption detection.
        pgno_checksum: AtomicU32,
        /// Magic number (WAL_INDEX_MAGIC).
        magic: AtomicU32,
        /// Database page size (set by writer, read by reader for frame_offset).
        page_size: AtomicU32,
        /// Rolling checksum of the last frame in the WAL (component 1).
        /// Updated by writer on commit so a new writer can chain checksums
        /// without reading the WAL file.
        last_checksum_1: AtomicU32,
        /// Rolling checksum of the last frame in the WAL (component 2).
        last_checksum_2: AtomicU32,
        /// WAL header checkpoint_seq, synced so readers can detect WAL restart
        /// without reading the WAL file from disk.
        checkpoint_seq: AtomicU32,
        // Remaining bytes up to HEADER_SIZE are padding.
    }

    impl WalIndexHeader {
        /// Atomically bump the generation counter with Release ordering.
        /// Skips `u32::MAX` to avoid collision with the sentinel value
        /// used by `cached_wal_index_generation` in `MultiProcessWal`.
        fn bump_generation(&self) {
            let mut new_gen = self.generation.load(Ordering::Relaxed).wrapping_add(1);
            if new_gen == u32::MAX {
                new_gen = 0;
            }
            self.generation.store(new_gen, Ordering::Release);
        }
    }

    // Verify the header fits.
    const _: () = assert!(std::mem::size_of::<WalIndexHeader>() <= HEADER_SIZE);

    #[cfg(test)]
    mod tests {
        use super::*;

        fn temp_wal_index() -> (tempfile::TempDir, String) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test-twal");
            (dir, path.to_str().unwrap().to_string())
        }

        #[test]
        fn test_open_creates_file() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();
            let metadata = std::fs::metadata(&path).unwrap();
            assert_eq!(metadata.len(), (BASE_OFFSET + SEGMENT_SIZE) as u64);
            assert_eq!(idx.max_frame(), 0);
            assert_eq!(idx.generation(), 0);
            drop(idx);
        }

        #[test]
        fn test_basic_append_and_find() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            idx.append_frame(10, 1).unwrap();
            idx.append_frame(20, 2).unwrap();
            idx.append_frame(30, 3).unwrap();
            idx.commit_max_frame(3);

            assert_eq!(idx.find_frame(10, 1, 3), Some(1));
            assert_eq!(idx.find_frame(20, 1, 3), Some(2));
            assert_eq!(idx.find_frame(30, 1, 3), Some(3));
            assert_eq!(idx.find_frame(99, 1, 3), None);
        }

        #[test]
        fn test_multiple_frames_per_page() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            idx.append_frame(42, 1).unwrap();
            idx.append_frame(99, 2).unwrap();
            idx.append_frame(42, 3).unwrap();
            idx.append_frame(99, 4).unwrap();
            idx.append_frame(42, 5).unwrap();
            idx.commit_max_frame(5);

            assert_eq!(idx.find_frame(42, 1, 5), Some(5));
            assert_eq!(idx.find_frame(42, 1, 4), Some(3));
            assert_eq!(idx.find_frame(42, 1, 2), Some(1));
            assert_eq!(idx.find_frame(42, 2, 4), Some(3));
            assert_eq!(idx.find_frame(42, 4, 5), Some(5));
            assert_eq!(idx.find_frame(42, 6, 10), None);
        }

        #[test]
        fn test_watermark_respect() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            idx.append_frame(1, 1).unwrap();
            idx.append_frame(2, 2).unwrap();
            idx.append_frame(3, 3).unwrap();
            idx.commit_max_frame(3);

            assert_eq!(idx.find_frame(1, 2, 3), None);
            assert_eq!(idx.find_frame(3, 1, 2), None);
            assert_eq!(idx.find_frame(2, 1, 3), Some(2));
        }

        #[test]
        fn test_hash_collision_handling() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            let target_hash = wal_hash(1);
            let mut colliding_pages = vec![1u32];
            let mut candidate = 2u32;
            while colliding_pages.len() < 5 {
                if wal_hash(candidate) == target_hash {
                    colliding_pages.push(candidate);
                }
                candidate += 1;
            }

            for (i, &page_id) in colliding_pages.iter().enumerate() {
                idx.append_frame(page_id, (i + 1) as u32).unwrap();
            }
            idx.commit_max_frame(colliding_pages.len() as u32);

            for (i, &page_id) in colliding_pages.iter().enumerate() {
                assert_eq!(
                    idx.find_frame(page_id, 1, colliding_pages.len() as u32),
                    Some((i + 1) as u32),
                    "failed to find page_id {page_id}"
                );
            }
        }

        #[test]
        fn test_segment_growth() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            let total_frames = APGNO_COUNT_SEG0 as u32 + 100;
            for frame_id in 1..=total_frames {
                idx.append_frame(frame_id * 10, frame_id).unwrap();
            }
            idx.commit_max_frame(total_frames);

            assert_eq!(idx.find_frame(10, 1, total_frames), Some(1));
            let last_seg0_frame = APGNO_COUNT_SEG0 as u32;
            assert_eq!(
                idx.find_frame(last_seg0_frame * 10, 1, total_frames),
                Some(last_seg0_frame)
            );
            assert_eq!(
                idx.find_frame((last_seg0_frame + 1) * 10, 1, total_frames),
                Some(last_seg0_frame + 1)
            );
            assert_eq!(
                idx.find_frame(total_frames * 10, 1, total_frames),
                Some(total_frames)
            );

            let metadata = std::fs::metadata(&path).unwrap();
            assert_eq!(metadata.len(), (BASE_OFFSET + 2 * SEGMENT_SIZE) as u64);
        }

        #[test]
        fn test_clear() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            idx.append_frame(42, 1).unwrap();
            idx.append_frame(43, 2).unwrap();
            idx.commit_max_frame(2);
            assert_eq!(idx.find_frame(42, 1, 2), Some(1));

            idx.clear(100, 200);

            assert_eq!(idx.max_frame(), 0);
            assert_eq!(idx.generation(), 1);
            assert_eq!(idx.find_frame(42, 1, 2), None);
            assert_eq!(idx.find_frame(43, 1, 2), None);
        }

        #[test]
        fn test_rollback() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            idx.append_frame(10, 1).unwrap();
            idx.append_frame(20, 2).unwrap();
            idx.append_frame(30, 3).unwrap();
            idx.append_frame(40, 4).unwrap();
            idx.append_frame(50, 5).unwrap();
            idx.commit_max_frame(5);

            idx.rollback_to(3);

            assert_eq!(idx.max_frame(), 3);
            assert_eq!(idx.find_frame(10, 1, 3), Some(1));
            assert_eq!(idx.find_frame(20, 1, 3), Some(2));
            assert_eq!(idx.find_frame(30, 1, 3), Some(3));
            assert_eq!(idx.find_frame(40, 1, 5), None);
            assert_eq!(idx.find_frame(50, 1, 5), None);
        }

        #[test]
        fn test_empty_index() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            assert_eq!(idx.find_frame(1, 1, 100), None);
            assert_eq!(idx.max_frame(), 0);
        }

        #[test]
        fn test_crash_recovery_returns_corrupt_error() {
            let (_dir, path) = temp_wal_index();

            {
                let idx = WalIndex::open_standalone(&path).unwrap();
                idx.append_frame(10, 1).unwrap();
                idx.append_frame(20, 2).unwrap();
                idx.append_frame(30, 3).unwrap();
                idx.commit_max_frame(3);

                // Corrupt the pgno_checksum.
                idx.header().pgno_checksum.store(0xDEAD, Ordering::Release);
            }

            // Reopen — should detect mismatch and return Corrupt error.
            let result = WalIndex::open_standalone(&path);
            assert!(result.is_err(), "open with corrupt checksum should fail");
            let err = result.err().unwrap();
            assert!(
                err.to_string().contains("corruption") || err.to_string().contains("Corrupt"),
                "error should indicate corruption, got: {err}"
            );

            // Recovery path: delete the file and reopen (gets a fresh index).
            std::fs::remove_file(&path).unwrap();
            let idx = WalIndex::open_standalone(&path).unwrap();
            assert_eq!(idx.max_frame(), 0);
        }

        #[test]
        fn test_iter_latest_frames() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            idx.append_frame(10, 1).unwrap();
            idx.append_frame(20, 2).unwrap();
            idx.append_frame(10, 3).unwrap();
            idx.append_frame(30, 4).unwrap();
            idx.commit_max_frame(4);

            let frames = idx.iter_latest_frames(1, 4);
            assert_eq!(frames.len(), 3);
            assert!(frames.contains(&(20, 2)));
            assert!(frames.contains(&(10, 3)));
            assert!(frames.contains(&(30, 4)));
            assert!(!frames.contains(&(10, 1)));
        }

        #[test]
        fn test_segment_boundary_crossing() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            let seg0_cap = APGNO_COUNT_SEG0 as u32;
            for frame_id in 1..=seg0_cap {
                idx.append_frame(frame_id, frame_id).unwrap();
            }
            idx.commit_max_frame(seg0_cap);

            assert_eq!(idx.find_frame(seg0_cap, 1, seg0_cap), Some(seg0_cap));

            let first_seg1 = seg0_cap + 1;
            idx.append_frame(first_seg1, first_seg1).unwrap();
            idx.commit_max_frame(first_seg1);

            assert_eq!(idx.find_frame(seg0_cap, 1, first_seg1), Some(seg0_cap));
            assert_eq!(idx.find_frame(first_seg1, 1, first_seg1), Some(first_seg1));
        }

        #[test]
        fn test_many_segments() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            // Force 5 segments.
            let target = APGNO_COUNT_SEG0 as u32 + 4 * APGNO_COUNT as u32;
            for frame_id in 1..=target {
                idx.append_frame(frame_id, frame_id).unwrap();
            }
            idx.commit_max_frame(target);

            assert_eq!(idx.find_frame(1, 1, target), Some(1));
            assert_eq!(idx.find_frame(target, 1, target), Some(target));

            let metadata = std::fs::metadata(&path).unwrap();
            assert_eq!(metadata.len(), (BASE_OFFSET + 5 * SEGMENT_SIZE) as u64);
        }

        #[test]
        fn test_cross_open_visibility() {
            let (_dir, path) = temp_wal_index();

            let writer = WalIndex::open_standalone(&path).unwrap();
            writer.append_frame(42, 1).unwrap();
            writer.append_frame(43, 2).unwrap();
            writer.commit_max_frame(2);

            let reader = WalIndex::open_standalone(&path).unwrap();
            assert_eq!(reader.max_frame(), 2);
            assert_eq!(reader.find_frame(42, 1, 2), Some(1));
            assert_eq!(reader.find_frame(43, 1, 2), Some(2));
        }

        /// Reproduce the clear+rewrite visibility bug.
        /// Writer writes, clears (checkpoint), writes again.
        /// Reader (opened early) must see the new hash table entries.
        #[test]
        fn test_cross_mmap_clear_rewrite_visibility() {
            let (_dir, path) = temp_wal_index();

            let writer = WalIndex::open_standalone(&path).unwrap();
            let reader = WalIndex::open_standalone(&path).unwrap();

            // Phase 1: Writer writes frames 1-3
            writer.append_frame(1, 1).unwrap(); // page 1 → frame 1
            writer.append_frame(2, 2).unwrap(); // page 2 → frame 2
            writer.append_frame(3, 3).unwrap(); // page 3 → frame 3
            writer.commit_max_frame(3);

            // Reader sees them via hash table
            assert_eq!(reader.max_frame(), 3);
            assert_eq!(reader.find_frame(1, 1, 3), Some(1));
            assert_eq!(reader.find_frame(2, 1, 3), Some(2));
            assert_eq!(reader.find_frame(3, 1, 3), Some(3));

            // Phase 2: clear (simulates checkpoint + WAL restart)
            writer.clear(0, 0);
            assert_eq!(reader.max_frame(), 0);

            // Phase 3: Writer writes again (same page IDs, new frames)
            writer.append_frame(1, 1).unwrap();
            writer.append_frame(2, 2).unwrap();
            writer.append_frame(3, 3).unwrap();
            writer.append_frame(1, 4).unwrap(); // page 1 updated again
            writer.commit_max_frame(4);

            // Reader MUST see the new hash table entries
            assert_eq!(reader.max_frame(), 4);

            // Brute-force confirms aPgno has data
            let brute1 = reader.brute_force_find(1, 1, 4);
            assert!(!brute1.is_empty(), "aPgno missing page 1: brute={brute1:?}");

            // Hash table lookup must also work
            let hash_result = reader.find_frame(1, 1, 4);
            assert_eq!(
                hash_result,
                Some(4),
                "Hash lookup failed for page 1! brute={brute1:?}, chain={}",
                reader.debug_hash_chain(1)
            );
            assert_eq!(reader.find_frame(2, 1, 4), Some(2));
            assert_eq!(reader.find_frame(3, 1, 4), Some(3));
        }

        #[test]
        fn test_rollback_across_segments() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            let total = APGNO_COUNT_SEG0 as u32 + 50;
            for frame_id in 1..=total {
                idx.append_frame(frame_id * 10, frame_id).unwrap();
            }
            idx.commit_max_frame(total);

            let rollback_point = APGNO_COUNT_SEG0 as u32 - 10;
            idx.rollback_to(rollback_point);

            assert_eq!(idx.max_frame(), rollback_point);
            assert_eq!(
                idx.find_frame(rollback_point * 10, 1, rollback_point),
                Some(rollback_point)
            );
            assert_eq!(idx.find_frame((rollback_point + 1) * 10, 1, total), None);
        }

        #[test]
        fn test_frame_to_seg_offset() {
            assert_eq!(frame_to_seg_offset(1), (0, 0));
            let seg0_cap = APGNO_COUNT_SEG0 as u32;
            assert_eq!(frame_to_seg_offset(seg0_cap), (0, APGNO_COUNT_SEG0 - 1));
            assert_eq!(frame_to_seg_offset(seg0_cap + 1), (1, 0));
            assert_eq!(
                frame_to_seg_offset(seg0_cap + APGNO_COUNT as u32),
                (1, APGNO_COUNT - 1)
            );
            assert_eq!(
                frame_to_seg_offset(seg0_cap + APGNO_COUNT as u32 + 1),
                (2, 0)
            );
        }

        #[test]
        fn test_reopen_preserves_state() {
            let (_dir, path) = temp_wal_index();

            {
                let idx = WalIndex::open_standalone(&path).unwrap();
                idx.append_frame(10, 1).unwrap();
                idx.append_frame(20, 2).unwrap();
                idx.commit_max_frame(2);
            }

            let idx = WalIndex::open_standalone(&path).unwrap();
            assert_eq!(idx.max_frame(), 2);
            assert_eq!(idx.find_frame(10, 1, 2), Some(1));
            assert_eq!(idx.find_frame(20, 1, 2), Some(2));
        }

        #[test]
        fn test_clear_then_reuse() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            idx.append_frame(10, 1).unwrap();
            idx.commit_max_frame(1);
            assert_eq!(idx.find_frame(10, 1, 1), Some(1));

            idx.clear(0, 0);

            idx.append_frame(20, 1).unwrap();
            idx.append_frame(30, 2).unwrap();
            idx.commit_max_frame(2);

            assert_eq!(idx.find_frame(10, 1, 2), None);
            assert_eq!(idx.find_frame(20, 1, 2), Some(1));
            assert_eq!(idx.find_frame(30, 1, 2), Some(2));
        }

        #[test]
        fn test_rollback_noop_when_at_or_beyond_max() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            idx.append_frame(10, 1).unwrap();
            idx.append_frame(20, 2).unwrap();
            idx.commit_max_frame(2);

            idx.rollback_to(2);
            assert_eq!(idx.max_frame(), 2);
            assert_eq!(idx.find_frame(10, 1, 2), Some(1));
            assert_eq!(idx.find_frame(20, 1, 2), Some(2));

            idx.rollback_to(5);
            assert_eq!(idx.max_frame(), 2);
        }

        /// Stress test: writer thread appends frames while reader thread
        /// probes for known pages. Validates Release/Acquire ordering on
        /// max_frame ensures aPgno/aHash stores are visible to readers.
        #[test]
        fn test_concurrent_append_and_find() {
            let (_dir, path) = temp_wal_index();
            let idx = std::sync::Arc::new(WalIndex::open_standalone(&path).unwrap());

            let total_frames = 2000u32;

            // Writer thread: append frames and commit in batches.
            let writer_idx = idx.clone();
            let writer = std::thread::spawn(move || {
                let batch_size = 10;
                for frame_id in 1..=total_frames {
                    // Use page_id = frame_id * 7 to avoid trivial patterns.
                    writer_idx.append_frame(frame_id * 7, frame_id).unwrap();
                    if frame_id % batch_size == 0 {
                        writer_idx.commit_max_frame(frame_id);
                    }
                }
                writer_idx.commit_max_frame(total_frames);
            });

            // Reader thread: continuously probe for pages that should exist.
            let reader_idx = idx.clone();
            let reader = std::thread::spawn(move || {
                let mut found_count = 0u64;
                for _ in 0..50_000 {
                    let max = reader_idx.max_frame();
                    if max == 0 {
                        std::thread::yield_now();
                        continue;
                    }
                    // Pick a frame we know should be committed.
                    let target_frame = 1 + (found_count as u32 % max);
                    let target_page = target_frame * 7;
                    if let Some(frame) = reader_idx.find_frame(target_page, 1, max) {
                        assert_eq!(
                            frame, target_frame,
                            "find_frame returned wrong frame for page {target_page}"
                        );
                        found_count += 1;
                    }
                }
                assert!(
                    found_count > 0,
                    "reader never found any frames during concurrent test"
                );
            });

            writer.join().unwrap();
            reader.join().unwrap();

            // Final verification: all frames findable.
            for frame_id in 1..=total_frames {
                assert_eq!(
                    idx.find_frame(frame_id * 7, 1, total_frames),
                    Some(frame_id),
                    "missing frame {frame_id} after concurrent test"
                );
            }
        }

        /// Fill segment 0 to maximum capacity (APGNO_COUNT_SEG0 entries)
        /// and verify every page is findable. Stresses long probe chains
        /// at high load factor (~49.6% of 8192 hash slots).
        #[test]
        fn test_near_full_hash_table() {
            let (_dir, path) = temp_wal_index();
            let idx = WalIndex::open_standalone(&path).unwrap();

            let cap = APGNO_COUNT_SEG0 as u32;
            // Use distinct page IDs that will create hash collisions.
            for frame_id in 1..=cap {
                idx.append_frame(frame_id * 3, frame_id).unwrap();
            }
            idx.commit_max_frame(cap);

            // Verify every single page is findable.
            for frame_id in 1..=cap {
                assert_eq!(
                    idx.find_frame(frame_id * 3, 1, cap),
                    Some(frame_id),
                    "missing page_id {} at frame {frame_id} in near-full table",
                    frame_id * 3
                );
            }

            // Verify a page not in the table returns None (must traverse
            // long probe chains to determine this).
            assert_eq!(idx.find_frame(999_999, 1, cap), None);
        }

        /// Simulate crash between append_frame and commit_max_frame.
        /// On reopen, frames beyond max_frame should be invisible and
        /// the aPgno slots should be safely overwritable.
        #[test]
        fn test_crash_after_append_before_commit() {
            let (_dir, path) = temp_wal_index();

            // Phase 1: append and commit 3 frames.
            {
                let idx = WalIndex::open_standalone(&path).unwrap();
                idx.append_frame(10, 1).unwrap();
                idx.append_frame(20, 2).unwrap();
                idx.append_frame(30, 3).unwrap();
                idx.commit_max_frame(3);
            }

            // Phase 2: append 2 more frames but DON'T commit (simulate crash).
            {
                let idx = WalIndex::open_standalone(&path).unwrap();
                idx.append_frame(40, 4).unwrap();
                idx.append_frame(50, 5).unwrap();
                // Crash! No commit_max_frame called.
                // max_frame stays at 3.
            }

            // Phase 3: reopen and verify.
            {
                let idx = WalIndex::open_standalone(&path).unwrap();
                assert_eq!(
                    idx.max_frame(),
                    3,
                    "max_frame should still be 3 after crash"
                );

                // Committed frames should be visible.
                assert_eq!(idx.find_frame(10, 1, 3), Some(1));
                assert_eq!(idx.find_frame(20, 1, 3), Some(2));
                assert_eq!(idx.find_frame(30, 1, 3), Some(3));

                // Uncommitted frames should NOT be visible when using committed max_frame.
                // Hash entries for frames 4,5 still exist but find_frame respects the
                // max_frame bound, so they are invisible.
                let committed_max = idx.max_frame();
                assert_eq!(idx.find_frame(40, 1, committed_max), None);
                assert_eq!(idx.find_frame(50, 1, committed_max), None);

                // New writes should overwrite the stale aPgno entries.
                idx.append_frame(60, 4).unwrap();
                idx.append_frame(70, 5).unwrap();
                idx.commit_max_frame(5);

                assert_eq!(idx.find_frame(60, 1, 5), Some(4));
                assert_eq!(idx.find_frame(70, 1, 5), Some(5));
                // Old uncommitted pages should not be findable.
                assert_eq!(idx.find_frame(40, 1, 5), None);
                assert_eq!(idx.find_frame(50, 1, 5), None);
            }
        }
    }
}

#[cfg(all(unix, target_pointer_width = "64"))]
pub use imp::*;
