use std::{
    alloc::{self, Layout},
    ptr::{self, NonNull},
};

use crate::{mvcc::cursor, storage::btree::BTreeCursor};

/// Continuation is a utility to store a closure without allocating memory if not needed.
#[derive(Clone)]
pub struct Continuation {
    /// Store the closure as raw bytes
    ptr: NonNull<u8>,
    /// Store the vtable for calling and dropping the closure
    call_fn: Option<unsafe fn(&mut Continuation, NonNull<u8>, &mut BTreeCursor)>,
    /// Mark if the closure is initialized.
    initialized: bool,
    /// Store layout of the last closure that requested more memory.
    /// Since we might reuse same memory region, we don't store the layout of
    /// the last closure because it might have a smaller size than a previous one
    /// that could've allocated more memory. Therefore, we need this in order to deallocate properly.
    layout: Layout,
    cursor: *mut BTreeCursor,
}

impl Continuation {
    pub fn new() -> Self {
        let default_layout = Layout::from_size_align(1024, size_of::<usize>()).unwrap();
        let ptr = unsafe { alloc::alloc(default_layout) } as *mut u8;
        if ptr.is_null() {
            alloc::handle_alloc_error(default_layout);
        }
        let ptr = unsafe { NonNull::new_unchecked(ptr as *mut u8) };
        Continuation {
            ptr,
            call_fn: None,
            layout: default_layout,
            initialized: false,
            cursor: std::ptr::null_mut(),
        }
    }

    pub fn replace<F>(&mut self, f: F, cursor: &mut BTreeCursor)
    where
        F: FnOnce(&mut Self, &mut BTreeCursor) + 'static,
    {
        let cursor = cursor as *mut BTreeCursor;
        unsafe fn call_impl<F: FnOnce(&mut Continuation, &mut BTreeCursor)>(
            this: &mut Continuation,
            ptr: NonNull<u8>,
            cursor: &mut BTreeCursor,
        ) {
            let closure = ptr.as_ptr() as *mut F;
            let closure = ptr::read(closure);
            closure(this, cursor);
        }

        let layout = Layout::new::<F>();

        let ptr = unsafe {
            // Replace current allocation with the new closure and allocate more if needed
            if self.layout.size() < layout.size() {
                // Increase size of the allocated memory to hold new closure
                alloc::dealloc(self.ptr.as_ptr() as *mut u8, self.layout);
                let ptr = alloc::alloc(layout) as *mut F;
                if ptr.is_null() {
                    alloc::handle_alloc_error(layout);
                }
                self.layout = layout;
                ptr::write(ptr, f);
            }
            self.ptr
        };
        self.call_fn = Some(call_impl::<F>);
        self.ptr = ptr;
        self.cursor = cursor;
    }

    pub fn call(&mut self) {
        unsafe {
            // first make as unitilized so to mark we have already called it
            self.set_unitialized();
            assert!(!self.cursor.is_null());
            let cursor = &mut *self.cursor;
            (self.call_fn.as_mut().unwrap())(self, self.ptr, cursor);
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub fn set_unitialized(&mut self) {
        self.initialized = false;
    }
}

impl Drop for Continuation {
    fn drop(&mut self) {
        unsafe {
            // Call the destructor of the closure
            alloc::dealloc(self.ptr.as_ptr() as *mut u8, self.layout);
        }
        self.initialized = false;
    }
}
