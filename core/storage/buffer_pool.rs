use crate::IO;
use crossbeam::queue::SegQueue;
use parking_lot::RwLock;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, Weak};

const MAX_ARENA_PAGES: u32 = 256;
pub const DEFAULT_ARENA_SIZE: usize = 2 * 1024 * 1024; // 2MB

pub struct Buffer {
    ptr: NonNull<u8>,
    len: usize,
    id: u32, // encoded (arena, slot)
    arena: Weak<Arena>,
}

impl Buffer {
    pub fn new(ptr: NonNull<u8>, len: usize, id: u32, arena: Weak<Arena>) -> Arc<Self> {
        Arc::new(Self {
            ptr,
            len,
            id,
            arena,
        })
    }

    pub fn io_id(&self) -> u32 {
        split_id(self.id).0
    }
}

impl Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.arena.upgrade().map(|arena| {
            arena.mark_free(self.id);
        });
    }
}

struct ArenaInner {
    base: NonNull<u8>,
    id: u32,
    page_size: usize,
    page_count: u32,
    freelist: SegQueue<u32>,
}

pub struct Arena {
    id: u32,
    inner: Arc<ArenaInner>,
}

impl Arena {
    pub fn new(id: u32, page_size: usize) -> Arc<Self> {
        assert!(page_size.is_power_of_two());
        assert!(page_size <= DEFAULT_ARENA_SIZE);
        let page_count = (DEFAULT_ARENA_SIZE / page_size) as u32;
        let inner = ArenaInner::new(id, page_size, page_count);
        Arc::new(Self { id, inner })
    }

    pub fn mark_free(self: Arc<Self>, id: u32) {
        self.inner.freelist.push(split_id(id).1);
    }

    pub fn try_alloc(self: &Arc<Self>, len: usize) -> Option<Buffer> {
        debug_assert!(len <= self.inner.page_size);
        let slot = self.inner.freelist.pop()?;
        let addr = unsafe {
            self.inner
                .base
                .as_ptr()
                .add(slot as usize * self.inner.page_size)
        };
        Some(Buffer {
            ptr: NonNull::new(addr).unwrap(),
            len,
            id: make_id(self.id, slot),
            arena: Arc::downgrade(&self),
        })
    }
}

impl ArenaInner {
    fn new(id: u32, page_size: usize, page_count: u32) -> Arc<Self> {
        let bytes = page_size * page_count as usize;
        let base = unsafe { arena::alloc(bytes) };
        let base = NonNull::new(base).unwrap();
        let freelist = SegQueue::new();
        for i in 0..page_count {
            freelist.push(i);
        }
        Arc::new(Self {
            id,
            base,
            freelist,
            page_count,
            page_size,
        })
    }
}

impl Drop for ArenaInner {
    fn drop(&mut self) {
        unsafe {
            arena::dealloc(
                self.base.as_ptr(),
                self.page_size * self.page_count as usize,
            )
        };
    }
}
const SLOT_BITS: u32 = 16;
const ARENA_BITS: u32 = 8;

#[inline]
fn make_id(arena: u32, slot: u32) -> u32 {
    debug_assert!(arena < (1 << ARENA_BITS) && slot < (1 << SLOT_BITS));
    (arena << SLOT_BITS) | slot
}
#[inline]
fn split_id(id: u32) -> (u32, u32) {
    (id >> SLOT_BITS, id & ((1 << SLOT_BITS) - 1))
}

#[cfg(unix)]
mod arena {
    use rustix::mm::{mmap_anonymous, munmap, MapFlags, ProtFlags};
    pub unsafe fn alloc(len: usize) -> *mut u8 {
        let ptr = mmap_anonymous(
            std::ptr::null_mut(),
            len,
            ProtFlags::READ | ProtFlags::WRITE,
            MapFlags::PRIVATE | MapFlags::HUGE_2MB,
        )
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

pub struct BufferPool {
    io: Arc<dyn IO>,
    page_size: usize,
    arena_pages: u32,
    arenas: RwLock<Vec<Arc<Arena>>>,
    expand_guard: Mutex<()>,
}

impl BufferPool {
    pub fn new(io: Arc<dyn IO>, page_size: usize, arena_pages: u32) -> Self {
        Self {
            io,
            page_size,
            arena_pages,
            arenas: RwLock::new(Vec::new()),
            expand_guard: Mutex::new(()),
        }
    }

    fn add_arena(&self) -> Arc<Arena> {
        let arena_id = self.arena_pages.wrapping_add(1);
        assert!(arena_id < 1 << ARENA_BITS, "arena id overflow");

        let arena = Arena::new(arena_id, self.page_size);
        // one‑time kernel registration
        let iovec = libc::iovec {
            iov_base: arena.inner.base.as_ptr() as *mut _,
            iov_len: DEFAULT_ARENA_SIZE,
        };
        self.io
            .register_buffers(arena_id, &[iovec])
            .expect("register_buffers_update failed");
        self.arenas.write().push(arena.clone());
        arena
    }

    /// Get a buffer at least `len` bytes (<= page_size → one slot).
    pub fn get(&self, len: usize) -> Buffer {
        assert!(
            len <= self.page_size,
            "variable‑sized > page_size not yet supported"
        );
        // fast path: try existing arenas
        for arena in self.arenas.read().iter() {
            if let Some(pg) = arena.try_alloc(len) {
                return pg;
            }
        }
        // slow path: allocate new arena
        let arena = Arena::new(self.arena_pages.wrapping_add(1), self.page_size);
        let pg = arena.try_alloc(len).expect("fresh arena must have slots");
        self.arenas.write().push(arena);
        pg
    }
}
