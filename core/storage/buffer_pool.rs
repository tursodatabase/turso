use crate::io::BufferData;
use std::cell::{Cell, RefCell};
use std::pin::Pin;

pub struct BufferPool {
    pub free_buffers: RefCell<Vec<BufferData>>,
    page_size: Cell<usize>,
}

const DEFAULT_PAGE_SIZE: usize = 4096;

impl BufferPool {
    pub fn new(page_size: Option<usize>) -> Self {
        Self {
            free_buffers: RefCell::new(Vec::new()),
            page_size: Cell::new(page_size.unwrap_or(DEFAULT_PAGE_SIZE)),
        }
    }

    pub fn set_page_size(&self, page_size: usize) {
        self.page_size.set(page_size);
    }

    pub fn get(&self) -> BufferData {
        let mut free_buffers = self.free_buffers.borrow_mut();
        if let Some(buffer) = free_buffers.pop() {
            buffer
        } else {
            Pin::new(vec![0; self.page_size.get()])
        }
    }

    pub fn put(&self, buffer: BufferData) {
        let mut free_buffers = self.free_buffers.borrow_mut();
        free_buffers.push(buffer);
    }
}
