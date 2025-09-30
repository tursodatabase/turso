use super::{slot_bitmap::SlotBitmap, sqlite3_ondisk::WAL_FRAME_HEADER_SIZE};
use crate::fast_lock::SpinLock;
use crate::io::TEMP_BUFFER_CACHE;
use crate::{turso_assert, Buffer, LimboError, IO};
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

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
    /// The actual size of what is allocated for the
    /// buffer is `len` rounded up to the next multiple of
    /// `[Arena::slot_size]`
    len: usize,
}

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
    /// the next multiple of the arena's slot_size
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
    /// A lock preventing concurrent initialization.
    init_lock: Mutex<()>,
    /// The size of each `Arena`, in bytes.
    arena_size: AtomicUsize,
    /// The `[Database::page_size]`, which the `page_arena` will use to
    /// return buffers from `Self::get_page`.
    db_page_size: AtomicUsize,
}

unsafe impl Sync for PoolInner {}
unsafe impl Send for PoolInner {}

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
                arena_size: arena_size.into(),
                db_page_size: Self::DEFAULT_PAGE_SIZE.into(),
                init_lock: Mutex::new(()),
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
            return Ok(());
        }
        inner.db_page_size.store(page_size, Ordering::SeqCst);
        inner.init_arenas()?;
        Ok(())
    }
}

impl PoolInner {
    /// Allocate a buffer of the given length from the pool, falling back to
    /// temporary thread local buffers if the pool is not initialized or is full.
    pub fn allocate(&self, len: usize) -> Buffer {
        turso_assert!(len > 0, "Cannot allocate zero-length buffer");

        let db_page_size = self.db_page_size.load(Ordering::SeqCst);
        let wal_frame_size = db_page_size + WAL_FRAME_HEADER_SIZE;

        // Check if this is exactly a WAL frame size allocation
        if len == wal_frame_size {
            return self
                .wal_frame_arena
                .as_ref()
                .and_then(|wal_arena| Arena::try_alloc(wal_arena, len))
                .unwrap_or(Buffer::new_temporary(len));
        }
        // For all other sizes, use regular arena
        self.page_arena
            .as_ref()
            .and_then(|arena| Arena::try_alloc(arena, len))
            .unwrap_or(Buffer::new_temporary(len))
    }

    fn get_db_page_buffer(&mut self) -> Buffer {
        let db_page_size = self.db_page_size.load(Ordering::SeqCst);
        self.page_arena
            .as_ref()
            .and_then(|arena| Arena::try_alloc(arena, db_page_size))
            .unwrap_or(Buffer::new_temporary(db_page_size))
    }

    fn get_wal_frame_buffer(&mut self) -> Buffer {
        let len = self.db_page_size.load(Ordering::SeqCst) + WAL_FRAME_HEADER_SIZE;
        self.wal_frame_arena
            .as_ref()
            .and_then(|wal_arena| Arena::try_alloc(wal_arena, len))
            .unwrap_or(Buffer::new_temporary(len))
    }

    /// Allocate a new arena for the pool to use
    fn init_arenas(&mut self) -> crate::Result<()> {
        // Prevent concurrent growth
        let Some(_guard) = self.init_lock.try_lock() else {
            tracing::debug!("Buffer pool is already growing, skipping initialization");
            return Ok(()); // Already in progress
        };
        let arena_size = self.arena_size.load(Ordering::SeqCst);
        let db_page_size = self.db_page_size.load(Ordering::SeqCst);
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
    /// May span multiple slots
    pub fn try_alloc(arena: &Arc<Arena>, size: usize) -> Option<Buffer> {
        let slots = size.div_ceil(arena.slot_size) as u32;
        let mut freemap = arena.free_slots.lock();

        let first_idx = if slots == 1 {
            // use the optimized method for individual pages which attempts
            // to leave large contiguous areas free of fragmentation for
            // larger `runs`.
            freemap.alloc_one()?
        } else {
            freemap.alloc_run(slots)?
        };
        arena
            .allocated_slots
            .fetch_add(slots as usize, Ordering::SeqCst);
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
        let mut bm = self.free_slots.lock();
        let count = size.div_ceil(self.slot_size);
        turso_assert!(
            !bm.check_run_free(slot_idx, count as u32),
            "must not already be marked free"
        );
        bm.free_run(slot_idx, count as u32);
        self.allocated_slots.fetch_sub(count, Ordering::SeqCst);
    }
}

#[cfg(unix)]
mod arena {
    #[cfg(target_os = "macos")]
    use libc::MAP_ANON as MAP_ANONYMOUS;
    #[cfg(target_os = "linux")]
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

#[cfg(not(unix))]
mod arena {
    pub fn alloc(len: usize) -> *mut u8 {
        let layout = std::alloc::Layout::from_size_align(len, std::mem::size_of::<u8>()).unwrap();
        unsafe { std::alloc::alloc(layout) }
    }
    pub fn dealloc(ptr: *mut u8, len: usize) {
        let layout = std::alloc::Layout::from_size_align(len, std::mem::size_of::<u8>()).unwrap();
        unsafe { std::alloc::dealloc(ptr, layout) };
    }
}
