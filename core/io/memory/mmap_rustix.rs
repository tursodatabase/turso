//! Memory pages using Mmap with rustix

use crate::Result;
use core::ffi::c_void;
use core::ops::{Deref, DerefMut};
use core::{ptr, slice};
#[cfg(target_family = "unix")]
use rustix::mm::{mmap_anonymous, munmap, MapFlags, ProtFlags};
#[cfg(target_os = "linux")]
use rustix::mm::{mremap, MremapFlags};

#[derive(Debug)]
struct MmapInner {
    ptr: *mut c_void,
    len: usize,
}

impl MmapInner {
    fn new(len: usize) -> Result<Self> {
        assert!(len > 0, "length of MMap must be greater than 0");

        // If addr is NULL, then the kernel chooses the (page-aligned)
        //    address at which to create the mapping; this is the most portable
        //    method of creating a new mapping.
        let addr = ptr::null_mut();
        let ptr = unsafe {
            mmap_anonymous(
                addr,
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE,
            )?
        };

        // TODO for reviewers: I believe rustix already checks if ptr is valid at this point as the mmap_anonymous
        // call returns a Result type.
        Ok(MmapInner { ptr, len })
    }

    fn ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    fn mut_ptr(&mut self) -> *mut u8 {
        self.ptr as *mut u8
    }

    fn len(&self) -> usize {
        self.len
    }

    /// Linux mremap which may move the memory
    #[cfg(target_os = "linux")]
    fn remap(&mut self, new_len: usize) -> Result<()> {
        self.ptr = unsafe { mremap(self.ptr, self.len, new_len, MremapFlags::MAYMOVE)? };
        self.len = new_len;
        Ok(())
    }
}

impl Drop for MmapInner {
    fn drop(&mut self) {
        let ptr = self.ptr;
        let len = self.len;

        // Error ignored here
        unsafe {
            if let Err(e) = munmap(ptr, len) {
                tracing::error!("Munmap error: {}", e)
            }
        };
    }
}

#[derive(Debug)]
pub struct MemoryPages {
    inner: MmapInner,
}

impl MemoryPages {
    pub fn new(len: usize) -> Result<Self> {
        Ok(MemoryPages {
            inner: MmapInner::new(len)?,
        })
    }

    /// Linux mremap which may move the memory location
    /// # Safety
    ///
    /// If resizing to smaller size, you should not hold any references
    /// to data that is located beyond the new size
    ///
    /// [`mremap(2)`]: https://man7.org/linux/man-pages/man2/mremap.2.html
    #[cfg(target_os = "linux")]
    pub fn remap(&mut self, new_len: usize) -> Result<()> {
        self.inner.remap(new_len)
    }
}

impl Deref for MemoryPages {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.inner.ptr(), self.inner.len()) }
    }
}

impl DerefMut for MemoryPages {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.inner.mut_ptr(), self.inner.len()) }
    }
}

impl AsRef<[u8]> for MemoryPages {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl AsMut<[u8]> for MemoryPages {
    fn as_mut(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}
