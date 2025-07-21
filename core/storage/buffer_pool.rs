use crate::io::BufferData;
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::sqlite3_ondisk::PageSize;

pub struct BufferPool {
    pub free_buffers: Mutex<Vec<BufferData>>,
    page_size: AtomicUsize,
}

impl BufferPool {
    pub fn new(page_size: Option<usize>) -> Self {
        Self {
            free_buffers: Mutex::new(Vec::new()),
            page_size: AtomicUsize::new(page_size.unwrap_or(PageSize::DEFAULT as usize)),
        }
    }

    pub fn set_page_size(&self, page_size: usize) {
        self.page_size.store(page_size, Ordering::Relaxed);
    }

    pub fn get(&self) -> BufferData {
        let buffer = self.free_buffers.lock().pop();
        buffer.unwrap_or_else(|| Pin::new(vec![0; self.page_size.load(Ordering::Relaxed)]))
    }

    pub fn put(&self, buffer: BufferData) {
        self.free_buffers.lock().push(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn test_send_sync() {
        is_send_sync_static::<BufferPool>();
    }
}
