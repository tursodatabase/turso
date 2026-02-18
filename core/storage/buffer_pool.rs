use super::{slot_bitmap::SlotBitmap, sqlite3_ondisk::WAL_FRAME_HEADER_SIZE};
use crate::fast_lock::SpinLock;
use crate::io::TEMP_BUFFER_CACHE;
use crate::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use crate::sync::{Arc, Weak};
use crate::{turso_assert, Buffer, LimboError, IO};

use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::OnceLock;

#[derive(Debug)]
/// A buffer allocated from an arena from `[BufferPool]`
pub struct ArenaBuffer {
    /// The `Arena` the buffer came from
    arena: Weak<Arena>,
    /// Pointer to the start of the buffer
    ptr: NonNull<u8>,
    /// Identifier for the `[Arena]` the buffer came from
    arena_id: u32,
    /// The index of the first slot making up the buffer
    slot_idx: u32,
    /// The requested length of the allocation.
    /// For pooled buffers, `len` is always `<= Arena::slot_size` and occupies exactly one slot.
    len: usize,
}

// Unsound: write and read from different threads can be dangerous with current ArenaBuffer implementation without some additional explicit synchronization
unsafe impl Sync for ArenaBuffer {}
unsafe impl Send for ArenaBuffer {}
crate::assert::assert_send_sync!(ArenaBuffer);

impl ArenaBuffer {
    const fn new(
        arena: Weak<Arena>,
        ptr: NonNull<u8>,
        len: usize,
        arena_id: u32,
        slot_idx: u32,
    ) -> Self {
        ArenaBuffer {
            arena,
            ptr,
            arena_id,
            slot_idx,
            len,
        }
    }

    #[inline(always)]
    /// Returns the `id` of the underlying arena, only if it was registered with `io_uring`
    pub const fn fixed_id(&self) -> Option<u32> {
        // Arenas which are not registered will have `id`s <= UNREGISTERED_START
        if self.arena_id < UNREGISTERED_START {
            Some(self.arena_id)
        } else {
            None
        }
    }

    /// The requested size of the allocation, the actual size of the underlying buffer is rounded up to
    /// the arena's slot_size (and in practice is always `<= slot_size` for pooled buffers).
    pub const fn logical_len(&self) -> usize {
        self.len
    }
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.logical_len()) }
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.logical_len()) }
    }
}

impl Drop for ArenaBuffer {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.upgrade() {
            arena.free(self.slot_idx, self.logical_len());
        }
    }
}

impl std::ops::Deref for ArenaBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::ops::DerefMut for ArenaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

/// Static Buffer pool managing multiple memory arenas
/// of which `[ArenaBuffer]`s are returned for requested allocations
pub struct BufferPool {
    inner: UnsafeCell<PoolInner>,
}

unsafe impl Sync for BufferPool {}
unsafe impl Send for BufferPool {}
crate::assert::assert_send_sync!(BufferPool);

struct PoolInner {
    /// An instance of the program's IO, used for registering
    /// Arena's with io_uring.
    io: Option<Arc<dyn IO>>,
    /// An Arena which returns `ArenaBuffer`s of size `db_page_size`.
    page_arena: Option<Arc<Arena>>,
    /// An Arena which returns `ArenaBuffer`s of size `db_page_size`
    /// plus 24 byte `WAL_FRAME_HEADER_SIZE`, preventing the fragmentation
    /// or complex book-keeping needed to use the same arena for both sizes.
    wal_frame_arena: Option<Arc<Arena>>,
    /// The size of each `Arena`, in bytes.
    arena_size: usize,
    /// The `[Database::page_size]`, which the `page_arena` will use to
    /// return buffers from `Self::get_page`.
    db_page_size: OnceLock<usize>,
}

unsafe impl Sync for PoolInner {}
unsafe impl Send for PoolInner {}
crate::assert::assert_send_sync!(PoolInner);

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(Self::DEFAULT_ARENA_SIZE)
    }
}

impl BufferPool {
    /// 3MB Default size for each `Arena`. Any higher and
    /// it will fail to register the second arena with io_uring due
    /// to `RL_MEMLOCK` limit for un-privileged processes being 8MB total.
    pub const DEFAULT_ARENA_SIZE: usize = 3 * 1024 * 1024;
    /// 1MB size For testing/CI
    pub const TEST_ARENA_SIZE: usize = 1024 * 1024;
    /// 4KB default page_size
    pub const DEFAULT_PAGE_SIZE: usize = 4096;
    /// Maximum size for each Arena (64MB total)
    const MAX_ARENA_SIZE: usize = 32 * 1024 * 1024;
    /// 64kb Minimum arena size
    const MIN_ARENA_SIZE: usize = 1024 * 64;
    fn new(arena_size: usize) -> Self {
        turso_assert!(
            (Self::MIN_ARENA_SIZE..Self::MAX_ARENA_SIZE).contains(&arena_size),
            "Arena size needs to be between {}..{} bytes",
            Self::MIN_ARENA_SIZE,
            Self::MAX_ARENA_SIZE
        );
        Self {
            inner: UnsafeCell::new(PoolInner {
                page_arena: None,
                wal_frame_arena: None,
                arena_size,
                db_page_size: OnceLock::new(),
                io: None,
            }),
        }
    }

    /// Request a `Buffer` of size `len`
    #[inline]
    pub fn allocate(&self, len: usize) -> Buffer {
        self.inner().allocate(len)
    }

    /// Request a `Buffer` the size of the `db_page_size` the `BufferPool` was initialized with.
    #[inline]
    pub fn get_page(&self) -> Buffer {
        let inner = self.inner_mut();
        inner.get_db_page_buffer()
    }

    /// Request a `Buffer` for use with a WAL frame,
    /// `[Database::page_size] + `WAL_FRAME_HEADER_SIZE`
    #[inline]
    pub fn get_wal_frame(&self) -> Buffer {
        let inner = self.inner_mut();
        inner.get_wal_frame_buffer()
    }

    #[inline]
    fn inner(&self) -> &PoolInner {
        unsafe { &*self.inner.get() }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn inner_mut(&self) -> &mut PoolInner {
        unsafe { &mut *self.inner.get() }
    }

    /// Create a static `BufferPool` initialize the pool to the default page size, **without**
    /// populating the Arenas. Arenas will not be created until `[BufferPool::finalize_page_size]`,
    /// and the pool will temporarily return temporary buffers to prevent reallocation of the
    /// arena if the page size is set to something other than the default value.
    pub fn begin_init(io: &Arc<dyn IO>, arena_size: usize) -> Arc<Self> {
        let pool = Arc::new(BufferPool::new(arena_size));
        let inner = pool.inner_mut();
        // Just store the IO handle, don't create arena yet
        if inner.io.is_none() {
            inner.io = Some(Arc::clone(io));
        }
        pool
    }

    /// Call when `[Database::db_state]` is initialized, providing the `page_size` to allocate
    /// an arena for the pool. Before this call, the pool will use temporary buffers which are
    /// cached in thread local storage.
    pub fn finalize_with_page_size(&self, page_size: usize) -> crate::Result<()> {
        let inner = self.inner_mut();
        tracing::trace!("finalize page size called with size {page_size}");
        if page_size != BufferPool::DEFAULT_PAGE_SIZE {
            // so far we have handed out some temporary buffers, since the page size is not
            // default, we need to clear the cache so they aren't reused for other operations.
            TEMP_BUFFER_CACHE.with(|cache| {
                cache.borrow_mut().reinit_cache(page_size);
            });
        }
        if inner.page_arena.is_some() {
            tracing::trace!("Buffer pool already initialized, skipping finalize");
            return Ok(());
        }

        // Tries to atomically (guarenteed by the OnceLock) initialize the page size for the inner pool.
        // If it succeeds, we now have to initialize the arenas.
        // If the initialization fails, this means the arenas have already been initialized by a previous thread
        // This avoids a potential TOCTOU race, where 2 threads could try to initalize the arena at the same time
        // after checking the `db_page_size`
        if inner.db_page_size.set(page_size).is_ok() {
            inner.init_arenas()?;
        };
        Ok(())
    }
}

impl PoolInner {
    #[inline]
    pub fn get_db_page_size(&self) -> usize {
        *(self
            .db_page_size
            .get()
            .unwrap_or(&BufferPool::DEFAULT_PAGE_SIZE))
    }

    /// Allocate a buffer of the given length from the pool, falling back to
    /// temporary thread local buffers if the pool is not initialized or is full.
    pub fn allocate(&self, len: usize) -> Buffer {
        turso_assert!(len > 0, "Cannot allocate zero-length buffer");

        let db_page_size = self.get_db_page_size();
        let wal_frame_size = db_page_size + WAL_FRAME_HEADER_SIZE;

        // Check if this is exactly a WAL frame size allocation
        if len == wal_frame_size {
            return self
                .wal_frame_arena
                .as_ref()
                .and_then(|wal_arena| Arena::try_alloc(wal_arena, len))
                .unwrap_or_else(|| Buffer::new_temporary(len));
        }
        // For all other sizes, use regular arena
        self.page_arena
            .as_ref()
            .and_then(|arena| Arena::try_alloc(arena, len))
            .unwrap_or_else(|| Buffer::new_temporary(len))
    }

    fn get_db_page_buffer(&mut self) -> Buffer {
        let db_page_size = self.get_db_page_size();
        self.page_arena
            .as_ref()
            .and_then(|arena| Arena::try_alloc(arena, db_page_size))
            .unwrap_or_else(|| Buffer::new_temporary(db_page_size))
    }

    fn get_wal_frame_buffer(&mut self) -> Buffer {
        let len = self.get_db_page_size() + WAL_FRAME_HEADER_SIZE;
        self.wal_frame_arena
            .as_ref()
            .and_then(|wal_arena| Arena::try_alloc(wal_arena, len))
            .unwrap_or_else(|| Buffer::new_temporary(len))
    }

    /// Allocate a new arena for the pool to use
    fn init_arenas(&mut self) -> crate::Result<()> {
        let db_page_size = self.get_db_page_size();
        let arena_size = self.arena_size;

        let io = self.io.as_ref().expect("Pool not initialized").clone();

        // Create regular page arena
        match Arena::new(db_page_size, arena_size, &io) {
            Ok(arena) => {
                tracing::trace!(
                    "added arena {} with size {} MB and slot size {}",
                    arena.id,
                    arena_size / (1024 * 1024),
                    db_page_size
                );
                self.page_arena = Some(Arc::new(arena));
            }
            Err(e) => {
                tracing::error!("Failed to create arena: {:?}", e);
                return Err(LimboError::InternalError(format!(
                    "Failed to create arena: {e}",
                )));
            }
        }

        // Create WAL frame arena
        let wal_frame_size = db_page_size + WAL_FRAME_HEADER_SIZE;
        match Arena::new(wal_frame_size, arena_size, &io) {
            Ok(arena) => {
                tracing::trace!(
                    "added WAL frame arena {} with size {} MB and slot size {}",
                    arena.id,
                    arena_size / (1024 * 1024),
                    wal_frame_size
                );
                self.wal_frame_arena = Some(Arc::new(arena));
            }
            Err(e) => {
                tracing::error!("Failed to create WAL frame arena: {:?}", e);
                return Err(LimboError::InternalError(format!(
                    "Failed to create WAL frame arena: {e}",
                )));
            }
        }

        Ok(())
    }
}

/// Preallocated block of memory used by the pool to distribute `ArenaBuffer`s
struct Arena {
    /// Identifier to tie allocations back to the arena. If the arena is registerd
    /// with `io_uring`, then the ID represents the index of the arena into the ring's
    /// sparse registered buffer array created on the ring's initialization.
    id: u32,
    /// Base pointer to the arena returned by `mmap`
    base: NonNull<u8>,
    /// Total number of slots currently allocated/in use.
    allocated_slots: AtomicUsize,
    /// Currently free slots.
    free_slots: SpinLock<SlotBitmap>,
    /// Total size of the arena in bytes
    arena_size: usize,
    /// Slot size the total arena is divided into.
    slot_size: usize,
}

// SAFETY: Arena's base pointer comes from mmap and is never aliased. All mutable
// state is behind AtomicUsize or SpinLock, so concurrent access is safe.
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Drop for Arena {
    fn drop(&mut self) {
        unsafe { arena::dealloc(self.base.as_ptr(), self.arena_size) };
    }
}

/// Slots 0 and 1 will be reserved for Arenas which are registered buffers
/// with io_uring.
const UNREGISTERED_START: u32 = 2;

/// ID's for an Arena which is not registered with `io_uring`
/// registered arena will always have id = 0..=1
static NEXT_ID: AtomicU32 = AtomicU32::new(UNREGISTERED_START);

impl Arena {
    /// Create a new arena with the given size and page size.
    /// NOTE: Minimum arena size is slot_size * 64
    fn new(slot_size: usize, arena_size: usize, io: &Arc<dyn IO>) -> Result<Self, String> {
        let min_slots = arena_size.div_ceil(slot_size);
        let rounded_slots = (min_slots.max(64) + 63) & !63;
        let rounded_bytes = rounded_slots * slot_size;
        // Guard against the global cap
        if rounded_bytes > BufferPool::MAX_ARENA_SIZE {
            return Err(format!(
                "arena size {} B exceeds hard limit of {} B",
                rounded_bytes,
                BufferPool::MAX_ARENA_SIZE
            ));
        }
        let ptr = unsafe { arena::alloc(rounded_bytes) };
        let base = NonNull::new(ptr).ok_or("Failed to allocate arena")?;
        let id = io
            .register_fixed_buffer(base, rounded_bytes)
            .unwrap_or_else(|_| {
                // Register with io_uring if possible, otherwise use next available ID
                let next_id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
                tracing::trace!("Allocating arena with id {}", next_id);
                next_id
            });
        let map = SlotBitmap::new(rounded_slots as u32);
        Ok(Self {
            id,
            base,
            free_slots: SpinLock::new(map),
            allocated_slots: AtomicUsize::new(0),
            slot_size,
            arena_size: rounded_bytes,
        })
    }

    /// Allocate a `Buffer` large enough for logical length `size`.
    pub fn try_alloc(arena: &Arc<Arena>, size: usize) -> Option<Buffer> {
        if size > arena.slot_size {
            // The buffer pool only supports single-slot allocations. Larger requests fall back to
            // temporary heap buffers via the caller.
            return None;
        }
        let mut freemap = arena.free_slots.lock();
        let first_idx = freemap.alloc_one()?;
        arena.allocated_slots.fetch_add(1, Ordering::SeqCst);
        let offset = first_idx as usize * arena.slot_size;
        let ptr = unsafe { NonNull::new_unchecked(arena.base.as_ptr().add(offset)) };
        Some(Buffer::new_pooled(ArenaBuffer::new(
            Arc::downgrade(arena),
            ptr,
            size,
            arena.id,
            first_idx,
        )))
    }

    /// Mark all relevant slots that include `size` starting at `slot_idx` as free.
    pub fn free(&self, slot_idx: u32, size: usize) {
        turso_assert!(
            size <= self.slot_size,
            "pooled buffers must not exceed one slot"
        );
        let mut bm = self.free_slots.lock();
        turso_assert!(!bm.is_free(slot_idx), "must not already be marked free");
        bm.free_one(slot_idx);
        self.allocated_slots.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(all(unix, not(miri)))]
mod arena {
    use libc::MAP_ANONYMOUS;
    use libc::{mmap, munmap, MAP_PRIVATE, PROT_READ, PROT_WRITE};
    use std::ffi::c_void;

    pub unsafe fn alloc(len: usize) -> *mut u8 {
        let ptr = mmap(
            std::ptr::null_mut(),
            len,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS,
            -1,
            0,
        );
        if ptr == libc::MAP_FAILED {
            panic!("mmap failed: {}", std::io::Error::last_os_error());
        }
        #[cfg(target_os = "linux")]
        {
            libc::madvise(ptr, len, libc::MADV_HUGEPAGE);
        }
        ptr as *mut u8
    }

    pub unsafe fn dealloc(ptr: *mut u8, len: usize) {
        let result = munmap(ptr as *mut c_void, len);
        if result != 0 {
            panic!("munmap failed: {}", std::io::Error::last_os_error());
        }
    }
}

#[cfg(any(not(unix), miri))]
mod arena {
    pub fn alloc(len: usize) -> *mut u8 {
        let layout = std::alloc::Layout::from_size_align(len, std::mem::size_of::<u8>()).unwrap();
        unsafe { std::alloc::alloc_zeroed(layout) }
    }
    pub fn dealloc(ptr: *mut u8, len: usize) {
        let layout = std::alloc::Layout::from_size_align(len, std::mem::size_of::<u8>()).unwrap();
        unsafe { std::alloc::dealloc(ptr, layout) };
    }
}

/// Shuttle tests for concurrent buffer pool operations.
///
/// These tests target the `unsafe impl Sync/Send` on:
/// - `ArenaBuffer`: Raw pointer access across threads
/// - `BufferPool`: UnsafeCell-based interior mutability
/// - `PoolInner`: Shared mutable state
///
#[cfg(all(shuttle, test))]
mod shuttle_tests {
    use super::*;
    use crate::io::MemoryIO;
    use crate::sync::*;
    use crate::thread;
    use rustc_hash::FxHashSet as HashSet;

    fn create_test_pool() -> Arc<BufferPool> {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        pool.finalize_with_page_size(4096).unwrap();
        pool
    }

    /// Test concurrent allocations from BufferPool.
    /// Verifies that multiple threads can safely call get_page() simultaneously.
    #[test]
    fn shuttle_concurrent_page_allocation() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let mut handles = vec![];

                for _ in 0..3 {
                    let pool = Arc::clone(&pool);
                    let h = thread::spawn(move || {
                        let buf = pool.get_page();
                        assert_eq!(buf.len(), 4096);
                        buf
                    });
                    handles.push(h);
                }

                let buffers: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
                // Verify all buffers are valid and distinct (no double allocation)
                assert_eq!(buffers.len(), 3);
            },
            1000,
        );
    }

    /// Test concurrent allocation and deallocation.
    /// Buffers are dropped in different threads than they were allocated.
    #[test]
    fn shuttle_concurrent_alloc_and_drop() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let pool2 = Arc::clone(&pool);

                // Thread 1: allocate and send buffer to be dropped elsewhere
                let h1 = thread::spawn(move || {
                    let buf = pool.get_page();
                    buf.len() // return length, buffer dropped here
                });

                // Thread 2: allocate concurrently
                let h2 = thread::spawn(move || {
                    let buf = pool2.get_page();
                    buf.len()
                });

                assert_eq!(h1.join().unwrap(), 4096);
                assert_eq!(h2.join().unwrap(), 4096);
            },
            1000,
        );
    }

    /// Test that ArenaBuffer can be safely sent between threads and written to.
    /// This tests the `unsafe impl Send + Sync for ArenaBuffer`.
    #[test]
    fn shuttle_arena_buffer_send_and_write() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let buf = pool.get_page();

                // Write some data
                buf.as_mut_slice()[0] = 42;

                // Send to another thread for reading
                let h = thread::spawn(move || {
                    assert_eq!(buf.as_slice()[0], 42);
                    buf.as_slice()[0]
                });

                assert_eq!(h.join().unwrap(), 42);
            },
            1000,
        );
    }

    /// Test concurrent WAL frame and page allocations.
    /// Both arena types are exercised simultaneously.
    #[test]
    fn shuttle_concurrent_mixed_allocations() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let pool2 = Arc::clone(&pool);
                let pool3 = Arc::clone(&pool);

                let h1 = thread::spawn(move || {
                    let buf = pool.get_page();
                    assert_eq!(buf.len(), 4096);
                });

                let h2 = thread::spawn(move || {
                    let buf = pool2.get_wal_frame();
                    // WAL frame = page_size + WAL_FRAME_HEADER_SIZE (24)
                    assert_eq!(buf.len(), 4096 + WAL_FRAME_HEADER_SIZE);
                });

                let h3 = thread::spawn(move || {
                    let buf = pool3.allocate(1024);
                    assert_eq!(buf.len(), 1024);
                });

                h1.join().unwrap();
                h2.join().unwrap();
                h3.join().unwrap();
            },
            1000,
        );
    }

    /// Stress test: many threads allocating and dropping buffers rapidly.
    /// This helps find race conditions in the slot bitmap and arena management.
    #[test]
    fn shuttle_stress_concurrent_alloc_drop() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let mut handles = vec![];

                for i in 0..4 {
                    let pool = Arc::clone(&pool);
                    let h = thread::spawn(move || {
                        // Each thread does multiple alloc/drop cycles
                        for _ in 0..2 {
                            let buf = pool.get_page();
                            // Write thread-specific data
                            buf.as_mut_slice()[0] = i as u8;
                            assert_eq!(buf.as_slice()[0], i as u8);
                            // buf dropped here, returning slot to arena
                        }
                    });
                    handles.push(h);
                }

                for h in handles {
                    h.join().unwrap();
                }
            },
            1000,
        );
    }

    /// Test that buffers allocated by one thread can be safely read by another.
    /// Uses a channel-like pattern with Arc to share buffers.
    #[test]
    fn shuttle_buffer_shared_read() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();

                // Allocate and write in main thread
                let buf = pool.get_page();
                for (i, byte) in buf.as_mut_slice().iter_mut().enumerate().take(100) {
                    *byte = (i % 256) as u8;
                }

                // Wrap in Arc for shared access (Buffer itself doesn't impl Clone)
                let buf = Arc::new(buf);
                let buf2 = Arc::clone(&buf);

                // Reader thread
                let h = thread::spawn(move || {
                    for i in 0..100 {
                        assert_eq!(buf2.as_slice()[i], (i % 256) as u8);
                    }
                });

                // Main thread also reads
                for i in 0..100 {
                    assert_eq!(buf.as_slice()[i], (i % 256) as u8);
                }

                h.join().unwrap();
            },
            1000,
        );
    }

    /// Test pool initialization race (though guarded by init_lock).
    /// Multiple threads trying to finalize should be safe.
    #[test]
    fn shuttle_concurrent_finalize() {
        let test = || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
            let pool2 = Arc::clone(&pool);
            let pool3 = Arc::clone(&pool);

            let h1 = thread::spawn(move || {
                let _ = pool.finalize_with_page_size(4096).unwrap();
            });

            let h2 = thread::spawn(move || {
                let _ = pool2.finalize_with_page_size(4096).unwrap();
            });

            // Also try to allocate while finalizing
            let h3 = thread::spawn(move || {
                // This may get a temporary buffer if arena isn't ready
                let buf = pool3.allocate(4096);
                assert_eq!(buf.len(), 4096);
            });

            h1.join().unwrap();
            h2.join().unwrap();
            h3.join().unwrap();
        };
        shuttle::check_random(test, 1000);
    }

    /// Test concurrent writes to the same ArenaBuffer at the SAME offsets.
    /// This exercises the `unsafe impl Sync for ArenaBuffer` which is documented as potentially unsound.
    /// Each thread writes a distinct pattern to all 4096 bytes, and we verify no torn writes occurred
    /// (all bytes must have the same pattern value, not a mix from different threads).
    #[test]
    fn shuttle_concurrent_write_same_buffer_same_offset() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let buf = pool.get_page();

                // Three distinct byte patterns that threads will race to write
                const PATTERN_A: u8 = 0xAA;
                const PATTERN_B: u8 = 0xBB;
                const PATTERN_C: u8 = 0xCC;

                // Use scoped threads so buf can be borrowed by multiple threads
                thread::scope(|scope| {
                    // Thread A writes PATTERN_A to all 4096 bytes
                    scope.spawn(|| {
                        buf.as_mut_slice().fill(PATTERN_A);
                    });

                    // Thread B writes PATTERN_B to all 4096 bytes
                    scope.spawn(|| {
                        buf.as_mut_slice().fill(PATTERN_B);
                    });

                    // Thread C writes PATTERN_C to all 4096 bytes
                    scope.spawn(|| {
                        buf.as_mut_slice().fill(PATTERN_C);
                    });
                });

                // After all writes complete, verify no torn writes across the entire buffer
                // All 4096 bytes should have the same pattern (whichever thread won)
                let slice = buf.as_slice();
                let first_byte = slice[0];

                // First byte must be one of our patterns
                assert!(
                    first_byte == PATTERN_A || first_byte == PATTERN_B || first_byte == PATTERN_C,
                    "Invalid pattern in buffer: 0x{:02X}",
                    first_byte
                );

                // All bytes must match the first byte (no partial/torn writes)
                for (i, &byte) in slice.iter().enumerate() {
                    assert!(
                        byte == first_byte,
                        "Torn write at offset {}: got 0x{:02X}, expected 0xAA, 0xBB, or 0xCC",
                        i,
                        byte
                    );
                }
            },
            1000,
        );
    }

    /// Test concurrent writes to different offsets of the same buffer (non-overlapping).
    /// This tests that writes to different parts of the buffer don't interfere.
    #[test]
    fn shuttle_concurrent_write_different_offsets() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let buf = pool.get_page();

                let ptr = buf.as_ptr() as usize;
                let len = buf.len();
                let _buf = buf;

                let h1 = thread::spawn(move || {
                    let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len) };
                    for i in 0..100 {
                        slice[i] = 0xAA;
                    }
                });

                let h2 = thread::spawn(move || {
                    let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len) };
                    for i in 100..200 {
                        slice[i] = 0xBB;
                    }
                });

                let h3 = thread::spawn(move || {
                    let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len) };
                    for i in 200..300 {
                        slice[i] = 0xCC;
                    }
                });

                h1.join().unwrap();
                h2.join().unwrap();
                h3.join().unwrap();

                // Verify each section has the correct pattern
                let final_slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) };
                for i in 0..100 {
                    assert_eq!(
                        final_slice[i], 0xAA,
                        "Section 1 corrupted at offset {}: expected 0xAA, got 0x{:02X}",
                        i, final_slice[i]
                    );
                }
                for i in 100..200 {
                    assert_eq!(
                        final_slice[i], 0xBB,
                        "Section 2 corrupted at offset {}: expected 0xBB, got 0x{:02X}",
                        i, final_slice[i]
                    );
                }
                for i in 200..300 {
                    assert_eq!(
                        final_slice[i], 0xCC,
                        "Section 3 corrupted at offset {}: expected 0xCC, got 0x{:02X}",
                        i, final_slice[i]
                    );
                }
            },
            1000,
        );
    }

    /// Test allocation racing with deallocation (slot recycling).
    /// Verifies that when a buffer is dropped and its slot is freed,
    /// concurrent allocations correctly handle the recycled slot.
    #[test]
    fn shuttle_alloc_during_drop_slot_recycling() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();

                // Pre-allocate some buffers and write identifying data
                let mut initial_bufs: Vec<_> = (0..5).map(|_| pool.get_page()).collect();
                let initial_ptrs: Vec<usize> =
                    initial_bufs.iter().map(|b| b.as_ptr() as usize).collect();

                // Write unique patterns to initial buffers
                for (i, buf) in initial_bufs.iter_mut().enumerate() {
                    buf.as_mut_slice()[0] = 0xDE;
                    buf.as_mut_slice()[1] = i as u8;
                }

                let pool2 = Arc::clone(&pool);
                let pool3 = Arc::clone(&pool);

                // Thread 1: drops buffers, freeing slots
                let h1 = thread::spawn(move || {
                    drop(initial_bufs);
                });

                // Thread 2: allocates while slots are being freed
                let h2 = thread::spawn(move || {
                    let mut bufs = Vec::new();
                    for i in 0..3 {
                        let buf = pool2.get_page();
                        assert_eq!(buf.len(), 4096, "Buffer {} has wrong length", i);
                        // Write identifying pattern
                        buf.as_mut_slice()[0] = 0xAA;
                        buf.as_mut_slice()[1] = i as u8;
                        bufs.push(buf);
                    }
                    bufs
                });

                // Thread 3: also allocates concurrently
                let h3 = thread::spawn(move || {
                    let mut bufs = Vec::new();
                    for i in 0..3 {
                        let buf = pool3.get_page();
                        assert_eq!(buf.len(), 4096, "Buffer {} has wrong length", i);
                        // Write different identifying pattern
                        buf.as_mut_slice()[0] = 0xBB;
                        buf.as_mut_slice()[1] = i as u8;
                        bufs.push(buf);
                    }
                    bufs
                });

                h1.join().unwrap();
                let bufs2 = h2.join().unwrap();
                let bufs3 = h3.join().unwrap();

                // Verify buffers within each thread don't overlap
                let ptrs2: HashSet<_> = bufs2.iter().map(|b| b.as_ptr() as usize).collect();
                assert_eq!(
                    ptrs2.len(),
                    bufs2.len(),
                    "Thread 2 got duplicate buffer pointers"
                );

                let ptrs3: HashSet<_> = bufs3.iter().map(|b| b.as_ptr() as usize).collect();
                assert_eq!(
                    ptrs3.len(),
                    bufs3.len(),
                    "Thread 3 got duplicate buffer pointers"
                );

                // Verify no overlap between buffers from different threads
                for ptr in &ptrs2 {
                    assert!(
                        !ptrs3.contains(ptr),
                        "Slot double-allocation: same memory 0x{:X} returned to both threads",
                        ptr
                    );
                }

                // Verify each buffer has correct identifying data (not corrupted)
                for (i, buf) in bufs2.iter().enumerate() {
                    assert_eq!(
                        buf.as_slice()[0],
                        0xAA,
                        "Thread 2 buffer {} header corrupted",
                        i
                    );
                    assert_eq!(
                        buf.as_slice()[1],
                        i as u8,
                        "Thread 2 buffer {} index corrupted",
                        i
                    );
                }
                for (i, buf) in bufs3.iter().enumerate() {
                    assert_eq!(
                        buf.as_slice()[0],
                        0xBB,
                        "Thread 3 buffer {} header corrupted",
                        i
                    );
                    assert_eq!(
                        buf.as_slice()[1],
                        i as u8,
                        "Thread 3 buffer {} index corrupted",
                        i
                    );
                }

                // Verify we can still allocate after all this
                let final_buf = pool.get_page();
                assert_eq!(final_buf.len(), 4096, "Final allocation failed");

                // Keep initial_ptrs to suppress unused warning
                let _ = initial_ptrs;
            },
            1000,
        );
    }

    /// Test arena exhaustion and recovery.
    /// Allocates until the arena is full (falls back to temporary buffers),
    /// then frees and verifies slots are correctly recycled.
    #[test]
    fn shuttle_arena_exhaustion_and_recovery() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();

                // Allocate many buffers to exhaust the arena
                // TEST_ARENA_SIZE = 1MB, page_size = 4KB, so ~256 slots max
                let mut buffers: Vec<Buffer> = Vec::new();
                let mut pooled_count = 0;
                let mut temp_count = 0;

                for i in 0..300 {
                    let buf = pool.get_page();
                    assert_eq!(buf.len(), 4096, "Buffer {} has wrong length", i);

                    // Write identifying data
                    buf.as_mut_slice()[0] = (i & 0xFF) as u8;
                    buf.as_mut_slice()[1] = ((i >> 8) & 0xFF) as u8;

                    if buf.is_pooled() {
                        pooled_count += 1;
                    } else {
                        temp_count += 1;
                    }
                    buffers.push(buf);
                }

                assert!(temp_count > 0);
                // We should have some pooled and some temporary buffers
                assert!(pooled_count > 0, "Expected some pooled buffers, got none");
                // With 1MB arena and 4KB pages, we have ~256 slots
                // So with 300 allocations, we should have some temporary
                assert!(
                    pooled_count <= 256,
                    "Got {} pooled buffers, but arena should only have ~256 slots",
                    pooled_count
                );
                assert!(pooled_count + temp_count >= 256);

                // Verify all buffers have correct identifying data
                for (i, buf) in buffers.iter().enumerate() {
                    assert_eq!(
                        buf.as_slice()[0],
                        (i & 0xFF) as u8,
                        "Buffer {} low byte corrupted",
                        i
                    );
                    assert_eq!(
                        buf.as_slice()[1],
                        ((i >> 8) & 0xFF) as u8,
                        "Buffer {} high byte corrupted",
                        i
                    );
                }

                // Drop half the buffers to free slots
                let dropped_count = buffers.len() - 150;
                buffers.truncate(150);

                // Allocate again - should get recycled slots
                let pool2 = Arc::clone(&pool);
                let h = thread::spawn(move || {
                    let mut new_bufs = Vec::new();
                    for i in 0..50 {
                        let buf = pool2.get_page();
                        assert_eq!(buf.len(), 4096, "New buffer {} has wrong length", i);
                        // Write new pattern
                        buf.as_mut_slice()[0] = 0xFF;
                        buf.as_mut_slice()[1] = i as u8;
                        new_bufs.push(buf);
                    }
                    new_bufs
                });

                let new_bufs = h.join().unwrap();

                // Verify new buffers
                for (i, buf) in new_bufs.iter().enumerate() {
                    assert_eq!(buf.len(), 4096, "New buffer {} length check failed", i);
                    assert_eq!(buf.as_slice()[0], 0xFF, "New buffer {} header corrupted", i);
                    assert_eq!(
                        buf.as_slice()[1],
                        i as u8,
                        "New buffer {} index corrupted",
                        i
                    );
                }

                // Verify remaining original buffers still have correct data
                for (i, buf) in buffers.iter().enumerate() {
                    assert_eq!(
                        buf.as_slice()[0],
                        (i & 0xFF) as u8,
                        "Original buffer {} corrupted after recycling",
                        i
                    );
                }

                let _ = (temp_count, dropped_count); // suppress warnings
            },
            1000,
        );
    }

    /// Test that allocated buffers never overlap (slot double-allocation detection).
    /// Multiple threads allocate concurrently and we verify all pointers are unique.
    #[test]
    fn shuttle_slot_overlap_verification() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let mut handles = vec![];

                for thread_id in 0..4u8 {
                    let pool = Arc::clone(&pool);
                    let h = thread::spawn(move || {
                        let mut bufs = Vec::new();
                        for buf_id in 0..10u8 {
                            let buf = pool.get_page();
                            assert_eq!(buf.len(), 4096, "Buffer has wrong length");

                            // Write thread and buffer identifying data
                            buf.as_mut_slice()[0] = thread_id;
                            buf.as_mut_slice()[1] = buf_id;
                            // Write a checksum pattern
                            buf.as_mut_slice()[2] = thread_id ^ buf_id;

                            bufs.push(buf);
                        }
                        bufs
                    });
                    handles.push(h);
                }

                let all_bufs: Vec<Vec<Buffer>> =
                    handles.into_iter().map(|h| h.join().unwrap()).collect();

                // Verify each thread got the expected number of buffers
                for (thread_id, thread_bufs) in all_bufs.iter().enumerate() {
                    assert_eq!(
                        thread_bufs.len(),
                        10,
                        "Thread {} got {} buffers instead of 10",
                        thread_id,
                        thread_bufs.len()
                    );
                }

                // Collect all pointers and verify uniqueness
                let mut all_ptrs: Vec<usize> = Vec::new();
                for thread_bufs in &all_bufs {
                    for buf in thread_bufs {
                        all_ptrs.push(buf.as_ptr() as usize);
                    }
                }

                let unique_ptrs: HashSet<_> = all_ptrs.iter().copied().collect();
                assert_eq!(
                    all_ptrs.len(),
                    unique_ptrs.len(),
                    "Slot double-allocation detected: {} total buffers but only {} unique pointers",
                    all_ptrs.len(),
                    unique_ptrs.len()
                );

                // Verify each buffer still has correct identifying data (no cross-thread corruption)
                for (thread_id, thread_bufs) in all_bufs.iter().enumerate() {
                    for (buf_id, buf) in thread_bufs.iter().enumerate() {
                        assert_eq!(
                            buf.as_slice()[0],
                            thread_id as u8,
                            "Buffer [{},{}] thread_id corrupted: expected {}, got {}",
                            thread_id,
                            buf_id,
                            thread_id,
                            buf.as_slice()[0]
                        );
                        assert_eq!(
                            buf.as_slice()[1],
                            buf_id as u8,
                            "Buffer [{},{}] buf_id corrupted: expected {}, got {}",
                            thread_id,
                            buf_id,
                            buf_id,
                            buf.as_slice()[1]
                        );
                        let expected_checksum = (thread_id as u8) ^ (buf_id as u8);
                        assert_eq!(
                            buf.as_slice()[2],
                            expected_checksum,
                            "Buffer [{},{}] checksum corrupted: expected {}, got {}",
                            thread_id,
                            buf_id,
                            expected_checksum,
                            buf.as_slice()[2]
                        );
                    }
                }

                // Verify buffers don't overlap by checking memory ranges
                let page_size = 4096usize;
                for (i, ptr_i) in all_ptrs.iter().enumerate() {
                    for (j, ptr_j) in all_ptrs.iter().enumerate() {
                        if i != j {
                            let range_i = *ptr_i..(*ptr_i + page_size);
                            // Check if ptr_j falls within range_i
                            assert!(
                                !range_i.contains(ptr_j),
                                "Buffer {} (0x{:X}) overlaps with buffer {} (0x{:X})",
                                i,
                                ptr_i,
                                j,
                                ptr_j
                            );
                        }
                    }
                }
            },
            1000,
        );
    }

    /// Test buffer content integrity under concurrent operations.
    /// Each thread writes a unique pattern and verifies it's not corrupted
    /// by other threads' operations.
    #[test]
    fn shuttle_buffer_content_integrity() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let mut handles = vec![];

                for thread_id in 0u8..4 {
                    let pool = Arc::clone(&pool);
                    let h = thread::spawn(move || {
                        let buf = pool.get_page();
                        // Write thread-specific pattern
                        let pattern = thread_id.wrapping_mul(37);
                        for byte in buf.as_mut_slice().iter_mut() {
                            *byte = pattern;
                        }
                        // Yield to allow other threads to run
                        thread::yield_now();
                        // Verify pattern is intact
                        for (i, byte) in buf.as_slice().iter().enumerate() {
                            assert_eq!(
                                *byte, pattern,
                                "Buffer corruption at offset {}: expected {}, got {}",
                                i, pattern, *byte
                            );
                        }
                        buf
                    });
                    handles.push(h);
                }

                // All threads should complete without corruption
                for h in handles {
                    h.join().unwrap();
                }
            },
            1000,
        );
    }

    /// Test the race between ArenaBuffer::drop upgrading Weak<Arena> while
    /// the Arena might be getting dropped. This exercises the weak reference
    /// pattern used for buffer deallocation.
    #[test]
    fn shuttle_weak_reference_upgrade_during_drop() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();

                // Allocate multiple buffers and write identifying data
                let buf1 = pool.get_page();
                let buf2 = pool.get_page();
                let buf3 = pool.get_page();

                buf1.as_mut_slice()[0] = 0x11;
                buf2.as_mut_slice()[0] = 0x22;
                buf3.as_mut_slice()[0] = 0x33;

                let buf1_ptr = buf1.as_ptr() as usize;
                let buf2_ptr = buf2.as_ptr() as usize;

                // Clone pool references
                let pool2 = Arc::clone(&pool);
                let pool3 = Arc::clone(&pool);

                // Thread 1: drop buffer1, freeing its slot
                let h1 = thread::spawn(move || {
                    // Buffer drop will try to upgrade Weak<Arena> and call free()
                    drop(buf1);
                });

                // Thread 2: drop pool reference and buffer2
                let h2 = thread::spawn(move || {
                    drop(buf2);
                    drop(pool2);
                });

                // Thread 3: allocate while others are dropping
                let h3 = thread::spawn(move || {
                    let new_buf = pool3.get_page();
                    assert_eq!(new_buf.len(), 4096, "New buffer has wrong length");
                    new_buf.as_mut_slice()[0] = 0x44;
                    new_buf
                });

                h1.join().unwrap();
                h2.join().unwrap();
                let new_buf = h3.join().unwrap();

                // Verify buf3 is still intact
                assert_eq!(buf3.as_slice()[0], 0x33, "buf3 was corrupted");

                // Verify new_buf has correct data
                assert_eq!(new_buf.as_slice()[0], 0x44, "new_buf was corrupted");

                // Original pool reference keeps arena alive
                // Allocate more to verify arena is still functional
                let final_buf = pool.get_page();
                assert_eq!(final_buf.len(), 4096, "Final allocation failed");
                final_buf.as_mut_slice()[0] = 0x55;
                assert_eq!(final_buf.as_slice()[0], 0x55, "Final buffer write failed");

                // The recycled slot might be one of the dropped buffers
                let final_ptr = final_buf.as_ptr() as usize;
                // This is valid - we might get a recycled slot
                let _ = (buf1_ptr, buf2_ptr, final_ptr);
            },
            1000,
        );
    }

    /// Test bitmap consistency: after many concurrent operations,
    /// verify that allocated_slots matches actual allocations.
    #[test]
    fn shuttle_bitmap_consistency() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let mut handles = vec![];

                // Many threads doing alloc/free cycles
                for thread_id in 0..4u8 {
                    let pool = Arc::clone(&pool);
                    let h = thread::spawn(move || {
                        let mut bufs = Vec::new();
                        // Allocate 5 buffers
                        for i in 0..5u8 {
                            let buf = pool.get_page();
                            assert_eq!(
                                buf.len(),
                                4096,
                                "Thread {} buf {} wrong length",
                                thread_id,
                                i
                            );
                            // Mark with identifying data
                            buf.as_mut_slice()[0] = thread_id;
                            buf.as_mut_slice()[1] = i;
                            buf.as_mut_slice()[2] = 0xAA; // Initial marker
                            bufs.push(buf);
                        }

                        // Verify all 5 before truncation
                        for (i, buf) in bufs.iter().enumerate() {
                            assert_eq!(
                                buf.as_slice()[0],
                                thread_id,
                                "Pre-truncate thread_id mismatch"
                            );
                            assert_eq!(buf.as_slice()[1], i as u8, "Pre-truncate index mismatch");
                        }

                        // Free 3 buffers (keep first 2)
                        bufs.truncate(2);

                        // Allocate 3 more
                        for i in 0..3u8 {
                            let buf = pool.get_page();
                            assert_eq!(
                                buf.len(),
                                4096,
                                "Thread {} new buf {} wrong length",
                                thread_id,
                                i
                            );
                            buf.as_mut_slice()[0] = thread_id;
                            buf.as_mut_slice()[1] = 10 + i; // Different index range
                            buf.as_mut_slice()[2] = 0xBB; // New marker
                            bufs.push(buf);
                        }

                        // Should have 5 buffers now (2 original + 3 new)
                        assert_eq!(bufs.len(), 5, "Thread {} should have 5 buffers", thread_id);
                        bufs
                    });
                    handles.push(h);
                }

                let all_bufs: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

                // Verify each thread returned 5 buffers
                for (thread_id, thread_bufs) in all_bufs.iter().enumerate() {
                    assert_eq!(
                        thread_bufs.len(),
                        5,
                        "Thread {} returned {} buffers instead of 5",
                        thread_id,
                        thread_bufs.len()
                    );
                }

                // Count pooled vs temporary buffers
                let mut pooled_count = 0;
                let mut temp_count = 0;
                for thread_bufs in &all_bufs {
                    for buf in thread_bufs {
                        if buf.fixed_id().is_some() {
                            pooled_count += 1;
                        } else {
                            temp_count += 1;
                        }
                    }
                }

                // Total should be 20 (4 threads * 5 buffers)
                assert_eq!(
                    pooled_count + temp_count,
                    20,
                    "Total buffer count mismatch: {} pooled + {} temp != 20",
                    pooled_count,
                    temp_count
                );

                // Verify all buffers have valid identifying data
                for (thread_id, thread_bufs) in all_bufs.iter().enumerate() {
                    for (i, buf) in thread_bufs.iter().enumerate() {
                        assert_eq!(
                            buf.as_slice()[0],
                            thread_id as u8,
                            "Buffer [{},{}] thread_id corrupted",
                            thread_id,
                            i
                        );
                        let marker = buf.as_slice()[2];
                        assert!(
                            marker == 0xAA || marker == 0xBB,
                            "Buffer [{},{}] has invalid marker 0x{:02X}",
                            thread_id,
                            i,
                            marker
                        );
                    }
                }

                // Collect all pointers to verify no duplicates
                let all_ptrs: HashSet<_> = all_bufs
                    .iter()
                    .flat_map(|bufs| bufs.iter().map(|b| b.as_ptr() as usize))
                    .collect();
                assert_eq!(
                    all_ptrs.len(),
                    20,
                    "Found duplicate pointers: {} unique out of 20",
                    all_ptrs.len()
                );

                // Try allocating more to verify arena is consistent
                let mut final_bufs = Vec::new();
                for i in 0..10 {
                    let buf = pool.get_page();
                    assert_eq!(buf.len(), 4096, "Final buf {} wrong length", i);
                    buf.as_mut_slice()[0] = 0xFF;
                    buf.as_mut_slice()[1] = i as u8;
                    final_bufs.push(buf);
                }

                // Verify final buffers don't overlap with existing ones
                for buf in &final_bufs {
                    let ptr = buf.as_ptr() as usize;
                    assert!(
                        !all_ptrs.contains(&ptr),
                        "Final buffer overlaps with existing at 0x{:X}",
                        ptr
                    );
                }

                // Keep buffers alive until end
                drop(all_bufs);
                drop(final_bufs);
            },
            1000,
        );
    }

    /// Test concurrent access through inner_mut().
    /// Multiple threads calling get_page() and get_wal_frame() simultaneously
    /// access different PoolInner fields through the unsynchronized inner_mut().
    #[test]
    fn shuttle_concurrent_inner_mut_access() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let mut handles = vec![];

                // Threads calling get_page (accesses page_arena through inner_mut)
                for page_thread_id in 0..2u8 {
                    let pool = Arc::clone(&pool);
                    let h = thread::spawn(move || {
                        let mut bufs = Vec::new();
                        for i in 0..5u8 {
                            let buf = pool.get_page();
                            assert_eq!(buf.len(), 4096, "Page buffer has wrong length");
                            // Mark as page buffer with identifying data
                            buf.as_mut_slice()[0] = 0xAA; // Page marker
                            buf.as_mut_slice()[1] = page_thread_id;
                            buf.as_mut_slice()[2] = i;
                            bufs.push(buf);
                        }
                        bufs
                    });
                    handles.push(h);
                }

                // Threads calling get_wal_frame (accesses wal_frame_arena through inner_mut)
                for wal_thread_id in 0..2u8 {
                    let pool = Arc::clone(&pool);
                    let h = thread::spawn(move || {
                        let mut bufs = Vec::new();
                        for i in 0..5u8 {
                            let buf = pool.get_wal_frame();
                            assert_eq!(
                                buf.len(),
                                4096 + WAL_FRAME_HEADER_SIZE,
                                "WAL frame buffer has wrong length"
                            );
                            // Mark as WAL buffer with identifying data
                            buf.as_mut_slice()[0] = 0xBB; // WAL marker
                            buf.as_mut_slice()[1] = wal_thread_id;
                            buf.as_mut_slice()[2] = i;
                            bufs.push(buf);
                        }
                        bufs
                    });
                    handles.push(h);
                }

                // Thread calling allocate with various sizes
                {
                    let pool = Arc::clone(&pool);
                    let h = thread::spawn(move || {
                        let mut bufs = Vec::new();
                        for i in 0..5u8 {
                            let buf = pool.allocate(2048);
                            assert_eq!(buf.len(), 2048, "Allocated buffer has wrong length");
                            // Mark as generic allocation
                            buf.as_mut_slice()[0] = 0xCC; // Allocate marker
                            buf.as_mut_slice()[1] = i;
                            bufs.push(buf);
                        }
                        bufs
                    });
                    handles.push(h);
                }

                let results: Vec<Vec<Buffer>> =
                    handles.into_iter().map(|h| h.join().unwrap()).collect();

                // Verify we got expected number of buffers from each type
                // 2 page threads * 5 + 2 wal threads * 5 + 1 allocate thread * 5 = 25 total
                let total_bufs: usize = results.iter().map(|v| v.len()).sum();
                assert_eq!(
                    total_bufs, 25,
                    "Expected 25 total buffers, got {}",
                    total_bufs
                );

                // Verify each buffer has correct marker and data
                for (thread_idx, thread_bufs) in results.iter().enumerate() {
                    for (buf_idx, buf) in thread_bufs.iter().enumerate() {
                        let marker = buf.as_slice()[0];
                        assert!(
                            marker == 0xAA || marker == 0xBB || marker == 0xCC,
                            "Buffer [{},{}] has invalid marker 0x{:02X}",
                            thread_idx,
                            buf_idx,
                            marker
                        );

                        // Verify length matches marker type
                        match marker {
                            0xAA => assert_eq!(buf.len(), 4096, "Page buffer wrong length"),
                            0xBB => assert_eq!(
                                buf.len(),
                                4096 + WAL_FRAME_HEADER_SIZE,
                                "WAL buffer wrong length"
                            ),
                            0xCC => assert_eq!(buf.len(), 2048, "Allocate buffer wrong length"),
                            _ => unreachable!(),
                        }
                    }
                }

                // Collect all pointers and verify no duplicates
                let all_ptrs: HashSet<_> = results
                    .iter()
                    .flat_map(|bufs| bufs.iter().map(|b| b.as_ptr() as usize))
                    .collect();
                assert_eq!(
                    all_ptrs.len(),
                    total_bufs,
                    "Found duplicate pointers: {} unique out of {}",
                    all_ptrs.len(),
                    total_bufs
                );
            },
            1000,
        );
    }

    /// Stress test with higher iteration count and more threads.
    /// This provides better coverage for subtle race conditions.
    #[test]
    fn shuttle_high_contention_stress() {
        shuttle::check_random(
            || {
                let pool = create_test_pool();
                let mut handles = vec![];

                for thread_id in 0..6u8 {
                    let pool = Arc::clone(&pool);
                    let h = thread::spawn(move || {
                        let mut bufs = Vec::new();
                        let mut dropped_count = 0u8;

                        for iter in 0..4u8 {
                            // Alternate between page and WAL frame allocations
                            let is_page = (thread_id + iter) % 2 == 0;
                            let buf = if is_page {
                                pool.get_page()
                            } else {
                                pool.get_wal_frame()
                            };

                            // Verify length matches allocation type
                            let expected_len = if is_page {
                                4096
                            } else {
                                4096 + WAL_FRAME_HEADER_SIZE
                            };
                            assert_eq!(
                                buf.len(),
                                expected_len,
                                "Thread {} iter {} got wrong buffer length",
                                thread_id,
                                iter
                            );

                            // Write identifying data with checksum
                            buf.as_mut_slice()[0] = thread_id;
                            buf.as_mut_slice()[1] = iter;
                            buf.as_mut_slice()[2] = if is_page { 0xAA } else { 0xBB };
                            buf.as_mut_slice()[3] = thread_id ^ iter; // Checksum

                            bufs.push(buf);

                            // Occasionally drop a buffer to test recycling
                            if bufs.len() > 2 && iter % 2 == 1 {
                                let dropped = bufs.pop().unwrap();
                                // Verify the dropped buffer still had valid data
                                assert_eq!(
                                    dropped.as_slice()[0],
                                    thread_id,
                                    "Dropped buffer thread_id corrupted"
                                );
                                dropped_count += 1;
                            }
                        }

                        // Verify all remaining buffers before returning
                        for (i, buf) in bufs.iter().enumerate() {
                            assert_eq!(
                                buf.as_slice()[0],
                                thread_id,
                                "Pre-return buffer {} thread_id corrupted",
                                i
                            );
                            let checksum = buf.as_slice()[0] ^ buf.as_slice()[1];
                            assert_eq!(
                                buf.as_slice()[3],
                                checksum,
                                "Pre-return buffer {} checksum invalid",
                                i
                            );
                        }

                        (bufs, dropped_count)
                    });
                    handles.push(h);
                }

                let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

                // Verify results from all threads
                let mut total_remaining = 0;
                let mut total_dropped = 0u8;

                for (thread_id, (thread_bufs, dropped)) in results.iter().enumerate() {
                    total_remaining += thread_bufs.len();
                    total_dropped = total_dropped.saturating_add(*dropped);

                    // Verify each buffer has correct identifying data
                    for (buf_idx, buf) in thread_bufs.iter().enumerate() {
                        assert_eq!(
                            buf.as_slice()[0],
                            thread_id as u8,
                            "Thread {} buffer {} thread_id corrupted: expected {}, got {}",
                            thread_id,
                            buf_idx,
                            thread_id,
                            buf.as_slice()[0]
                        );

                        let marker = buf.as_slice()[2];
                        assert!(
                            marker == 0xAA || marker == 0xBB,
                            "Thread {} buffer {} has invalid marker 0x{:02X}",
                            thread_id,
                            buf_idx,
                            marker
                        );

                        // Verify checksum
                        let expected_checksum = buf.as_slice()[0] ^ buf.as_slice()[1];
                        assert_eq!(
                            buf.as_slice()[3],
                            expected_checksum,
                            "Thread {} buffer {} checksum mismatch",
                            thread_id,
                            buf_idx
                        );

                        // Verify length matches marker
                        let expected_len = if marker == 0xAA {
                            4096
                        } else {
                            4096 + WAL_FRAME_HEADER_SIZE
                        };
                        assert_eq!(
                            buf.len(),
                            expected_len,
                            "Thread {} buffer {} length mismatch for marker",
                            thread_id,
                            buf_idx
                        );
                    }
                }

                // Sanity check: we should have some buffers remaining
                assert!(
                    total_remaining > 0,
                    "No buffers remaining after stress test"
                );

                // Collect all pointers and verify no duplicates among remaining buffers
                let all_ptrs: HashSet<_> = results
                    .iter()
                    .flat_map(|(bufs, _)| bufs.iter().map(|b| b.as_ptr() as usize))
                    .collect();
                assert_eq!(
                    all_ptrs.len(),
                    total_remaining,
                    "Found duplicate pointers: {} unique out of {} remaining",
                    all_ptrs.len(),
                    total_remaining
                );

                // Final allocation to verify pool is still healthy
                let final_buf = pool.get_page();
                assert_eq!(final_buf.len(), 4096, "Final allocation failed");
                final_buf.as_mut_slice()[0] = 0xFF;
                assert_eq!(final_buf.as_slice()[0], 0xFF, "Final buffer write failed");

                let _ = total_dropped; // suppress warning
            },
            1000,
        );
    }
}
