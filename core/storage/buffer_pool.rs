use crate::{Buffer, IO};
use crossbeam::queue::SegQueue;
use parking_lot::RwLock;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

pub const MAX_ARENA_PAGES: u32 = 256; // 512MB total max buffer pool size
pub const DEFAULT_ARENA_SIZE: usize = 2 * 1024 * 1024; // 2MB arenas

#[derive(Debug, Clone)]
pub struct ArenaBuffer {
    ptr: NonNull<u8>,
    len: usize,
    id: u32, // packed (arena, slot)
    arena: Arc<Arena>,
}

// Buffer pool is responsible for making sure two buffers
// are not allocated from the same arena slot or overlapping.
// Only one owner can exist.
unsafe impl Send for ArenaBuffer {}
unsafe impl Sync for ArenaBuffer {}

impl ArenaBuffer {
    pub fn new(ptr: NonNull<u8>, len: usize, id: u32, arena: Arc<Arena>) -> Arc<Self> {
        Arc::new(Self {
            ptr,
            len,
            id,
            arena,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The arena ID, only useful for representing the index into
    /// the kernel's array of fixed iovecs registered with io_uring.
    pub fn io_id(&self) -> u32 {
        split_id(self.id).0
    }

    /// Mark the buffer as free in the arena.
    pub fn mark_free(&self) {
        self.arena.mark_free(self.id);
    }
}

impl Deref for ArenaBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for ArenaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

/// Each arena represents a 2MB anonymous `mmap` memory region that is split into
/// logical pages and given out to the BufferPool. Each arena is the same fixed size but
/// can be created with it's own `page_size`.
/// This is to support the WAL requesting buffers
/// of size: page_size + WAL_FRAME_HEADER_SIZE without having to use an ephemeral heap buffer
/// and providing a fast path for writing wal frames (which will likely never exceed 1 arena)
#[derive(Debug)]
struct ArenaInner {
    base: NonNull<u8>,
    page_size: usize,
    page_count: u32,
    freelist: SegQueue<u32>,
}

#[derive(Debug)]
pub struct Arena {
    id: u32,
    inner: Arc<ArenaInner>,
}

impl Arena {
    pub fn new(id: u32, page_size: usize) -> Arc<Self> {
        assert!(page_size <= DEFAULT_ARENA_SIZE);
        let inner = ArenaInner::new(page_size);
        #[allow(clippy::arc_with_non_send_sync)]
        Arc::new(Self { id, inner })
    }

    pub fn mark_free(self: &Arc<Self>, id: u32) {
        assert!(self.id == split_id(id).0); // mark correct arena
        assert!(self.inner.page_count > split_id(id).1); // mark correct slot
        tracing::trace!("{} mark_free: id: {}", self.id, id);
        self.inner.freelist.push(split_id(id).1);
    }

    pub fn try_alloc(self: &Arc<Self>, len: usize) -> Option<ArenaBuffer> {
        if len > self.inner.page_size {
            return None;
        }
        if let Some(slot) = self.inner.freelist.pop() {
            let addr = unsafe {
                self.inner
                    .base
                    .as_ptr()
                    .add(slot as usize * self.inner.page_size)
            };
            return Some(ArenaBuffer {
                ptr: NonNull::new(addr).unwrap(),
                len,
                id: make_id(self.id, slot),
                arena: Arc::clone(self),
            });
        };
        None
    }
}

impl ArenaInner {
    fn new(page_size: usize) -> Arc<Self> {
        let base = unsafe { arena::alloc(DEFAULT_ARENA_SIZE) };
        let base = NonNull::new(base).unwrap();
        let freelist = SegQueue::new();
        let page_count = DEFAULT_ARENA_SIZE / page_size;
        tracing::trace!(
            "new arena: page_size: {}, page_count: {}",
            page_size,
            page_count
        );
        for i in 0..page_count {
            freelist.push(i as u32);
        }
        #[allow(clippy::arc_with_non_send_sync)]
        Arc::new(Self {
            base,
            freelist,
            page_count: page_count as u32,
            page_size,
        })
    }
}

impl Drop for ArenaInner {
    fn drop(&mut self) {
        if cfg!(debug_assertions) || self.page_count as usize != self.freelist.len() {
            eprintln!(
                "buffer pool leak: {}/{} pages still in use when Arena dropped",
                self.freelist.len(),
                self.page_count
            );
            std::process::exit(1);
        }
        unsafe { arena::dealloc(self.base.as_ptr(), DEFAULT_ARENA_SIZE) };
    }
}

const SLOT_BITS: u32 = 16; // 65,535 slots / arena
const ARENA_BITS: u32 = 8; // 256 arenas per process
const MAX_ARENAS: u32 = 1 << ARENA_BITS;

#[inline]
fn make_id(a: u32, s: u32) -> u32 {
    (a << SLOT_BITS) | s
}
#[inline]
fn split_id(id: u32) -> (u32, u32) {
    (id >> SLOT_BITS, id & ((1 << SLOT_BITS) - 1))
}

#[cfg(unix)]
mod arena {
    use rustix::mm::{mmap_anonymous, munmap, MapFlags, ProtFlags};
    pub unsafe fn alloc(len: usize) -> *mut u8 {
        // Attempt to mmap backed by 2MB hugepages
        let ptr = unsafe {
            mmap_anonymous(
                std::ptr::null_mut(),
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE | MapFlags::HUGE_2MB,
            )
        };
        if let Ok(ptr) = ptr {
            if ptr != libc::MAP_FAILED {
                return ptr.cast();
            }
        }
        // Fallback to normal anonymous mapping.
        let ptr = unsafe {
            mmap_anonymous(
                std::ptr::null_mut(),
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE,
            )
        }
        .expect("mmap failed");
        ptr.cast()
    }

    pub unsafe fn dealloc(ptr: *mut u8, len: usize) {
        munmap(ptr.cast(), len).expect("munmap failed");
    }
}

#[cfg(windows)]
mod arena {
    use windows_sys::Win32::System::Memory::{
        VirtualAlloc, VirtualFree, MEM_COMMIT, MEM_RELEASE, MEM_RESERVE, PAGE_READWRITE,
    };

    pub unsafe fn alloc(len: usize) -> *mut u8 {
        let ptr = VirtualAlloc(
            std::ptr::null_mut(),
            len,
            MEM_RESERVE | MEM_COMMIT,
            PAGE_READWRITE,
        );
        assert!(!ptr.is_null(), "VirtualAlloc failed");
        ptr.cast()
    }
    pub unsafe fn dealloc(ptr: *mut u8, _len: usize) {
        let ok = VirtualFree(ptr.cast(), 0, MEM_RELEASE);
        assert!(ok != 0, "VirtualFree failed");
    }
}

/// BufferPool manages a set of arenas which are devided into pages
/// and allocated to the pager/IO layer. Can return an ephemeral Heap
/// buffer if no arena is available for the requested size.
pub struct BufferPool {
    io: Arc<dyn IO>,
    default_page_size: usize, // e.g. 4096 (data pages)
    next_arena: AtomicU32,
    arenas: RwLock<Vec<Arc<Arena>>>, // mixed sizes
    expand_guard: Mutex<()>,
}

impl BufferPool {
    pub fn new(io: Arc<dyn IO>, default_page: usize) -> Self {
        tracing::trace!("creating buffer pool with default page size: {default_page}");
        Self {
            io,
            default_page_size: default_page,
            next_arena: AtomicU32::new(0),
            arenas: RwLock::new(Vec::new()),
            expand_guard: Mutex::new(()),
        }
    }

    /// Returns a Buffer for use in IO operations.
    /// Takes an optional length parameter, returning a page of the pool's
    /// default page size for the connection if None is provided.
    pub fn get_page(&self, len: Option<usize>) -> Arc<Buffer> {
        let size = len.unwrap_or(self.default_page_size);
        match self.get(size) {
            Ok(buf) => Buffer::new_pooled(buf),
            Err(_) => Buffer::new_heap(size),
        }
    }

    /// internal: try arena then maybe grow
    fn get(&self, len: usize) -> Result<ArenaBuffer, ()> {
        // fast path: existing arena with arena.page_size >= len
        for a in self
            .arenas
            .read()
            .iter()
            .filter(|a| a.inner.page_size >= len)
        {
            if let Some(b) = a.try_alloc(len) {
                return Ok(b);
            }
        }
        // need a new arena whose page >= len but <= 2 MiB
        let psize = len.min(self.default_page_size.max(len));
        let a = self.add_arena(psize)?;
        a.try_alloc(len).ok_or(())
    }

    // Add an arena to the pool with a default page size.
    fn add_arena(&self, page_size: usize) -> Result<Arc<Arena>, ()> {
        // hold guard to prevent expansion while we are adding a new arena
        let _g = self.expand_guard.lock();
        let id = self.next_arena.fetch_add(1, Ordering::Relaxed);
        if id >= MAX_ARENAS {
            return Err(());
        }
        tracing::trace!("add_arena: id: {} with page_size: {}", id, page_size);
        let arena = Arena::new(id, page_size);

        // map length is always fixed 2 MiB so O_DIRECT will always be aligned
        self.io
            .register_buffer(id, (arena.inner.base.as_ptr(), DEFAULT_ARENA_SIZE))
            .map_err(|_| ())?;

        self.arenas.write().push(arena.clone());
        Ok(arena)
    }
}
