use std::{fmt, ptr::NonNull, sync::OnceLock};

use super::{api, AllocError, Layout, TursoAllocator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetAllocatorError {
    AlreadyInitialized,
}

impl fmt::Display for SetAllocatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyInitialized => f.write_str("Turso allocator is already initialized"),
        }
    }
}

impl std::error::Error for SetAllocatorError {}

/// Backend for Turso heap allocations.
///
/// # Safety
///
/// Implementations must uphold the `Allocator` contract for every allocation
/// returned from `allocate`, including zero-sized layouts.
pub unsafe trait TursoAllocBackend: Sync {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError>;

    /// # Safety
    ///
    /// `ptr` and `layout` must describe a live block previously returned by
    /// this backend.
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout);
}

struct DefaultBackend;

unsafe impl TursoAllocBackend for DefaultBackend {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        <api::Global as api::ApiAllocator>::allocate(&api::Global, layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            <api::Global as api::ApiAllocator>::deallocate(&api::Global, ptr, layout);
        }
    }
}

static DEFAULT_BACKEND: DefaultBackend = DefaultBackend;
static BACKEND: OnceLock<&'static dyn TursoAllocBackend> = OnceLock::new();

pub fn set_allocator(backend: &'static dyn TursoAllocBackend) -> Result<(), SetAllocatorError> {
    BACKEND
        .set(backend)
        .map_err(|_| SetAllocatorError::AlreadyInitialized)
}

fn backend() -> &'static dyn TursoAllocBackend {
    BACKEND.get().copied().unwrap_or(&DEFAULT_BACKEND)
}

unsafe impl api::ApiAllocator for TursoAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        backend().allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            backend().deallocate(ptr, layout);
        }
    }
}
