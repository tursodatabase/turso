//! 32-bits Memory arena for types implementing `Copy`.
//! This Memory arena has been implemented to fit the use of tantivy's indexer
//! and has *twisted specifications*.
//!
//! - It works on stable rust.
//! - One can get an accurate figure of the memory usage of the arena.
//! - Allocation are very cheap.
//! - Allocation happening consecutively are very likely to have great locality.
//! - Addresses (`Addr`) are 32bits.
//! - Dropping the whole `MemoryArena` is cheap.
//!
//! # Limitations
//!
//! - Your object shall not implement `Drop`.
//! - `Addr` to the `Arena` are 32-bits. The maximum capacity of the arena is 4GB. *(Tantivy's
//!   indexer uses one arena per indexing thread.)*
//! - The arena only works for objects much smaller than  `1MB`. Allocating more than `1MB` at a
//!   time will result in a panic, and allocating a lot of large object (> 500KB) will result in a
//!   fragmentation.
//! - Your objects are store in an unaligned fashion. For this reason, the API does not let you
//!   access them as references.
//!
//! Instead, you store and access your data via `.write(...)` and `.read(...)`, which under the hood
//! stores your object using `ptr::write_unaligned` and `ptr::read_unaligned`.
use std::{mem, ptr};

const NUM_BITS_PAGE_ADDR: usize = 20;
const PAGE_SIZE: usize = 1 << NUM_BITS_PAGE_ADDR; // pages are 1 MB large

/// Represents a pointer into the `MemoryArena`
/// .
/// Pointer are 32-bits and are split into
/// two parts.
///
/// The first 12 bits represent the id of a
/// page of memory.
///
/// The last 20 bits are an address within this page of memory.
#[derive(Copy, Clone, Debug)]
pub struct Addr(u32);

impl Addr {
    /// Creates a null pointer.
    #[inline]
    pub fn null_pointer() -> Addr {
        Addr(u32::MAX)
    }

    /// Returns the `Addr` object for `addr + offset`
    #[inline]
    pub fn offset(self, offset: u32) -> Addr {
        Addr(self.0.wrapping_add(offset))
    }

    #[inline]
    fn new(page_id: usize, local_addr: usize) -> Addr {
        Addr(((page_id << NUM_BITS_PAGE_ADDR) | local_addr) as u32)
    }

    #[inline]
    fn page_id(self) -> usize {
        (self.0 as usize) >> NUM_BITS_PAGE_ADDR
    }

    #[inline]
    fn page_local_addr(self) -> usize {
        (self.0 as usize) & (PAGE_SIZE - 1)
    }

    /// Returns true if and only if the `Addr` is null.
    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == u32::MAX
    }
}

#[inline(always)]
pub fn store<Item: Copy + 'static>(dest: &mut [u8], val: Item) {
    debug_assert_eq!(dest.len(), std::mem::size_of::<Item>());
    unsafe {
        ptr::write_unaligned(dest.as_mut_ptr() as *mut Item, val);
    }
}

#[inline]
pub fn load<Item: Copy + 'static>(data: &[u8]) -> Item {
    debug_assert_eq!(data.len(), std::mem::size_of::<Item>());
    unsafe { ptr::read_unaligned(data.as_ptr() as *const Item) }
}

/// The `MemoryArena`
pub struct MemoryArena {
    pages: Vec<Page>,
}

impl Default for MemoryArena {
    fn default() -> MemoryArena {
        let first_page = Page::new(0);
        MemoryArena {
            pages: vec![first_page],
        }
    }
}

impl MemoryArena {
    /// Returns an estimate in number of bytes
    /// of resident memory consumed by the `MemoryArena`.
    ///
    /// Internally, it counts a number of `1MB` pages
    /// and therefore delivers an upperbound.
    pub fn mem_usage(&self) -> usize {
        self.pages.len() * PAGE_SIZE
    }

    /// Returns the number of bytes allocated in the arena.
    pub fn len(&self) -> usize {
        self.pages.len().saturating_sub(1) * PAGE_SIZE + self.pages.last().unwrap().len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn write_at<Item: Copy + 'static>(&mut self, addr: Addr, val: Item) {
        let dest = self.slice_mut(addr, std::mem::size_of::<Item>());
        store(dest, val);
    }

    /// Read an item in the memory arena at the given `address`.
    ///
    /// # Panics
    ///
    /// If the address is erroneous
    #[inline]
    pub fn read<Item: Copy + 'static>(&self, addr: Addr) -> Item {
        load(self.slice(addr, mem::size_of::<Item>()))
    }
    #[inline]
    fn get_page(&self, page_id: usize) -> &Page {
        unsafe { self.pages.get_unchecked(page_id) }
    }
    #[inline]
    fn get_page_mut(&mut self, page_id: usize) -> &mut Page {
        unsafe { self.pages.get_unchecked_mut(page_id) }
    }

    #[inline]
    pub fn slice(&self, addr: Addr, len: usize) -> &[u8] {
        self.get_page(addr.page_id())
            .slice(addr.page_local_addr(), len)
    }

    #[inline]
    pub fn slice_from(&self, addr: Addr) -> &[u8] {
        self.get_page(addr.page_id())
            .slice_from(addr.page_local_addr())
    }
    #[inline]
    pub fn slice_from_mut(&mut self, addr: Addr) -> &mut [u8] {
        self.get_page_mut(addr.page_id())
            .slice_from_mut(addr.page_local_addr())
    }

    #[inline]
    pub fn slice_mut(&mut self, addr: Addr, len: usize) -> &mut [u8] {
        self.get_page_mut(addr.page_id())
            .slice_mut(addr.page_local_addr(), len)
    }

    /// Add a page and allocate len on it.
    /// Return the address
    fn add_page(&mut self, len: usize) -> Addr {
        let new_page_id = self.pages.len();
        let mut page = Page::new(new_page_id);
        page.len = len;
        self.pages.push(page);
        Addr::new(new_page_id, 0)
    }

    /// Allocates `len` bytes and returns the allocated address.
    #[inline]
    pub fn allocate_space(&mut self, len: usize) -> Addr {
        let page_id = self.pages.len() - 1;
        if let Some(addr) = self.get_page_mut(page_id).allocate_space(len) {
            return addr;
        }
        self.add_page(len)
    }
}

struct Page {
    page_id: usize,
    len: usize,
    data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    fn new(page_id: usize) -> Page {
        // We use 32-bits addresses.
        // - 20 bits for the in-page addressing
        // - 12 bits for the page id.
        // This limits us to 2^12 - 1=4095 for the page id.
        assert!(page_id < 4096);
        Page {
            page_id,
            len: 0,
            data: vec![0u8; PAGE_SIZE].into_boxed_slice().try_into().unwrap(),
        }
    }

    #[inline]
    fn is_available(&self, len: usize) -> bool {
        len + self.len <= PAGE_SIZE
    }

    #[inline]
    fn slice(&self, local_addr: usize, len: usize) -> &[u8] {
        let data = &self.slice_from(local_addr);
        unsafe { data.get_unchecked(..len) }
    }

    #[inline]
    fn slice_from(&self, local_addr: usize) -> &[u8] {
        &self.data[local_addr..]
    }
    #[inline]
    fn slice_from_mut(&mut self, local_addr: usize) -> &mut [u8] {
        &mut self.data[local_addr..]
    }

    #[inline]
    fn slice_mut(&mut self, local_addr: usize, len: usize) -> &mut [u8] {
        let data = &mut self.data[local_addr..];
        unsafe { data.get_unchecked_mut(..len) }
    }

    #[inline]
    fn allocate_space(&mut self, len: usize) -> Option<Addr> {
        if self.is_available(len) {
            let addr = Addr::new(self.page_id, self.len);
            self.len += len;
            Some(addr)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use super::MemoryArena;
    use crate::memory_arena::PAGE_SIZE;

    #[test]
    fn test_arena_allocate_slice() {
        let mut arena = MemoryArena::default();
        let a = b"hello";
        let b = b"happy tax payer";

        let addr_a = arena.allocate_space(a.len());
        arena.slice_mut(addr_a, a.len()).copy_from_slice(a);

        let addr_b = arena.allocate_space(b.len());
        arena.slice_mut(addr_b, b.len()).copy_from_slice(b);

        assert_eq!(arena.slice(addr_a, a.len()), a);
        assert_eq!(arena.slice(addr_b, b.len()), b);
    }

    #[test]
    fn test_arena_allocate_end_of_page() {
        let mut arena = MemoryArena::default();

        // A big block
        let len_a = PAGE_SIZE - 2;
        let addr_a = arena.allocate_space(len_a);
        *arena.slice_mut(addr_a, len_a).last_mut().unwrap() = 1;

        // Single bytes
        let addr_b = arena.allocate_space(1);
        arena.slice_mut(addr_b, 1)[0] = 2;

        let addr_c = arena.allocate_space(1);
        arena.slice_mut(addr_c, 1)[0] = 3;

        let addr_d = arena.allocate_space(1);
        arena.slice_mut(addr_d, 1)[0] = 4;

        assert_eq!(arena.slice(addr_a, len_a)[len_a - 1], 1);
        assert_eq!(arena.slice(addr_b, 1)[0], 2);
        assert_eq!(arena.slice(addr_c, 1)[0], 3);
        assert_eq!(arena.slice(addr_d, 1)[0], 4);
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct MyTest {
        pub a: usize,
        pub b: u8,
        pub c: u32,
    }

    #[test]
    fn test_store_object() {
        let mut arena = MemoryArena::default();
        let a = MyTest {
            a: 143,
            b: 21,
            c: 32,
        };
        let b = MyTest {
            a: 113,
            b: 221,
            c: 12,
        };

        let num_bytes = std::mem::size_of::<MyTest>();
        let addr_a = arena.allocate_space(num_bytes);
        arena.write_at(addr_a, a);

        let addr_b = arena.allocate_space(num_bytes);
        arena.write_at(addr_b, b);

        assert_eq!(arena.read::<MyTest>(addr_a), a);
        assert_eq!(arena.read::<MyTest>(addr_b), b);
    }
}
