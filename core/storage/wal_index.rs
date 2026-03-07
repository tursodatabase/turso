//! Shared WAL Index for multi-process coordination.
//!
//! The WAL index provides a shared `page_id → frame_id` mapping in mmap'd
//! shared memory, eliminating file I/O from the reader hot path.
//!
//! ## File layout (embedded in `{db}-tshm` starting at byte 32768)
//!
//! The WAL index lives in the tshm file after a 32 KB reserved region
//! (the first 4 KB of which is the tshm coordination page; the rest is
//! padding to ensure mmap alignment on platforms with 16 KB pages like
//! Apple Silicon). Segments are 32 KB each, separately mmap'd:
//!
//! ```text
//! Segment 0 (32768 bytes, file offset 32768):
//!   [0..128):       WalIndexHeader
//!   [128..16384):   aPgno[4064]  — u32: frame_offset → page_id
//!   [16384..32768): aHash[8192]  — u16: hash table for reverse lookup
//!
//! Segments 1+ (32768 bytes each, file offset 32768 + N*32768):
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
    /// Apple Silicon, 4 KB on x86_64). Using SEGMENT_SIZE (32 KB) guarantees
    /// alignment on all platforms. The first 4096 bytes of this region are the
    /// tshm coordination page; the remaining bytes are unused padding.
    const BASE_OFFSET: usize = SEGMENT_SIZE;

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
            let min_size = (BASE_OFFSET + SEGMENT_SIZE) as u64;
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
            if current_size < min_size {
                let ret = unsafe { libc::ftruncate(fd, min_size as libc::off_t) };
                if ret != 0 {
                    return Err(crate::LimboError::InternalError(format!(
                        "wal_index ftruncate: {}",
                        std::io::Error::last_os_error()
                    )));
                }
            }

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
                        tracing::warn!(
                            "WAL index pgno_checksum mismatch (stored={stored_cksum}, computed={computed}), rebuilding hash tables"
                        );
                        Self::rebuild_hash_tables_with(&segments, max_frame);
                        header.pgno_checksum.store(computed, Ordering::Relaxed);
                    }
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

        /// Get current nbackfills from header.
        pub fn nbackfills(&self) -> u32 {
            self.header().nbackfills.load(Ordering::Acquire)
        }

        /// Set nbackfills after checkpoint.
        pub fn set_nbackfills(&self, val: u32) {
            self.header().nbackfills.store(val, Ordering::Release);
        }

        /// Look up the latest frame for `page_id` within [min_frame, max_frame].
        /// Reads only from mmap'd shared memory — zero file I/O.
        pub fn find_frame(&self, page_id: u32, min_frame: u32, max_frame: u32) -> Option<u32> {
            if max_frame == 0 || page_id == 0 || max_frame < min_frame {
                return None;
            }

            let segments = self.segments.read();
            let num_segments = segments.len();

            // Walk segments newest → oldest.
            for seg_idx in (0..num_segments).rev() {
                let base = seg_base_frame(seg_idx);
                let cap = seg_capacity(seg_idx);
                let seg = &segments[seg_idx];
                let apgno = seg.apgno(seg_idx);
                let ahash = seg.ahash(seg_idx);

                let mut h = wal_hash(page_id);
                let mut best: Option<u32> = None;
                let mut iterations = 0;

                loop {
                    let slot_val = ahash[h].load(Ordering::Relaxed);
                    if slot_val == 0 {
                        break; // empty slot — page not in this chain
                    }
                    iterations += 1;
                    if iterations > AHASH_NSLOT {
                        break; // safety: corrupt hash table
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

                if best.is_some() {
                    return best;
                }
            }
            None
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

            debug_assert!(page_id > 0, "page_id must be > 0");
            debug_assert!(frame_id > 0, "frame_id must be 1-based");

            let (seg_idx, offset) = frame_to_seg_offset(frame_id);

            // Grow if needed.
            self.ensure_segment_exists(seg_idx)?;

            let segments = self.segments.read();
            let seg = &segments[seg_idx];
            let apgno = seg.apgno(seg_idx);
            let ahash = seg.ahash(seg_idx);

            // Store page_id in aPgno (source of truth).
            apgno[offset].store(page_id, Ordering::Relaxed);

            // Insert into hash table.
            let mut h = wal_hash(page_id);
            let mut iterations = 0;
            loop {
                let current = ahash[h].load(Ordering::Relaxed);
                if current == 0 {
                    // Empty slot — claim it.
                    ahash[h].store((offset + 1) as u16, Ordering::Relaxed);
                    break;
                }
                iterations += 1;
                if iterations >= AHASH_NSLOT {
                    return Err(crate::LimboError::InternalError(
                        "wal_index hash table full or corrupt (no empty slot found)".to_string(),
                    ));
                }
                // Slot occupied — linear probe.
                h = (h + 1) % AHASH_NSLOT;
            }

            Ok(())
        }

        /// Make appended frames visible to readers by updating header.max_frame.
        /// Called after all frames in a commit batch have been appended.
        pub fn commit_max_frame(&self, frame_id: u32) {
            // Update checksum before publishing max_frame.
            let segments = self.segments.read();
            let checksum = Self::compute_pgno_checksum_with(&segments, frame_id);
            drop(segments);
            self.header()
                .pgno_checksum
                .store(checksum, Ordering::Relaxed);
            // Release barrier: ensures all aPgno/aHash stores above are visible
            // to any reader that loads this max_frame with Acquire.
            self.header().max_frame.store(frame_id, Ordering::Release);
        }

        /// Clear the entire index. Called on WAL restart/truncate.
        /// Auto-increments the generation counter so readers with stale
        /// snapshots detect the change and fall back to inner find_frame.
        pub fn clear(&self, salt_1: u32, salt_2: u32) {
            let header = self.header();
            let new_gen = header.generation.load(Ordering::Relaxed).wrapping_add(1);

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

            header.max_frame.store(0, Ordering::Release);
            header.nbackfills.store(0, Ordering::Release);
            header.generation.store(new_gen, Ordering::Release);
            header.salt_1.store(salt_1, Ordering::Release);
            header.salt_2.store(salt_2, Ordering::Release);
            header.pgno_checksum.store(0, Ordering::Release);
            // Keep num_segments as-is; segments are reusable.
        }

        /// Trim frames > max_frame. Called on rollback.
        /// Rebuilds hash tables for affected segments.
        pub fn rollback_to(&self, max_frame: u32) {
            let current_max = self.header().max_frame.load(Ordering::Acquire);
            if max_frame >= current_max {
                return; // Nothing to trim.
            }

            let segments = self.segments.read();

            // Zero aPgno entries beyond max_frame in all affected segments.
            for (seg_idx, seg) in segments.iter().enumerate() {
                let base = seg_base_frame(seg_idx);
                let apgno = seg.apgno(seg_idx);

                for (offset, entry) in apgno.iter().enumerate() {
                    let global_frame = base + offset as u32 + 1;
                    if global_frame > max_frame {
                        entry.store(0, Ordering::Relaxed);
                    }
                }
            }

            // Rebuild hash tables for all segments that had entries trimmed.
            for (seg_idx, seg) in segments.iter().enumerate() {
                let base = seg_base_frame(seg_idx);
                let cap = seg_capacity(seg_idx);
                let seg_last_frame = base + cap as u32;
                if seg_last_frame > max_frame {
                    Self::rebuild_segment_hash(seg, seg_idx, max_frame);
                }
            }

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
            let required_size = BASE_OFFSET + (seg_idx + 1) * SEGMENT_SIZE;
            let current_size = unsafe {
                let mut stat: libc::stat = std::mem::zeroed();
                if libc::fstat(self.fd, &mut stat) != 0 {
                    return Err(crate::LimboError::InternalError(format!(
                        "wal_index fstat: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                stat.st_size as usize
            };

            if current_size < required_size {
                let ret = unsafe { libc::ftruncate(self.fd, required_size as libc::off_t) };
                if ret != 0 {
                    return Err(crate::LimboError::InternalError(format!(
                        "wal_index ftruncate: {}",
                        std::io::Error::last_os_error()
                    )));
                }
            }

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

        /// Rebuild all hash tables from aPgno using an already-locked segments slice.
        fn rebuild_hash_tables_with(segments: &[WalIndexSegment], max_frame: u32) {
            for (seg_idx, seg) in segments.iter().enumerate() {
                Self::rebuild_segment_hash(seg, seg_idx, max_frame);
            }
        }

        /// Rebuild the hash table for a single segment from its aPgno.
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

                let mut h = wal_hash(page_id);
                let mut iterations = 0;
                loop {
                    let current = ahash[h].load(Ordering::Relaxed);
                    if current == 0 {
                        ahash[h].store((offset + 1) as u16, Ordering::Relaxed);
                        break;
                    }
                    iterations += 1;
                    if iterations >= AHASH_NSLOT {
                        break; // corrupt hash table — skip this entry
                    }
                    h = (h + 1) % AHASH_NSLOT;
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
        // Remaining bytes up to HEADER_SIZE are padding.
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
        fn test_crash_recovery_rebuild() {
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

            // Reopen — should detect mismatch and rebuild.
            let idx = WalIndex::open_standalone(&path).unwrap();
            assert_eq!(idx.find_frame(10, 1, 3), Some(1));
            assert_eq!(idx.find_frame(20, 1, 3), Some(2));
            assert_eq!(idx.find_frame(30, 1, 3), Some(3));
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
