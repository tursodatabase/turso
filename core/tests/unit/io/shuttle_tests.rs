use std::path::PathBuf;

use super::*;
use crate::io::{Buffer, Completion, OpenFlags, IO};
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::Arc;
use crate::thread;

/// Factory trait for creating IO implementations in tests.
/// Allows the same test logic to run against different IO backends.
trait IOFactory: Send + Sync + 'static {
    fn create(&self) -> Arc<dyn IO>;
    /// Returns a unique temp directory path for this factory instance.
    fn temp_dir(&self) -> PathBuf;
}

struct MemoryIOFactory {
    id: u64,
}

impl MemoryIOFactory {
    fn new() -> Self {
        use crate::sync::atomic::AtomicU64;
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        Self {
            id: COUNTER.fetch_add(1, Ordering::SeqCst),
        }
    }
}

impl IOFactory for MemoryIOFactory {
    fn create(&self) -> Arc<dyn IO> {
        Arc::new(MemoryIO::new())
    }
    fn temp_dir(&self) -> PathBuf {
        format!("mem_{}", self.id).into()
    }
}

#[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
struct PlatformIOFactory {
    temp_dir: tempfile::TempDir,
}

#[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
impl PlatformIOFactory {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().unwrap(),
        }
    }
}

#[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
impl IOFactory for PlatformIOFactory {
    fn create(&self) -> Arc<dyn IO> {
        Arc::new(PlatformIO::new().unwrap())
    }
    fn temp_dir(&self) -> PathBuf {
        self.temp_dir.path().to_path_buf()
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring", feature = "fs", not(miri)))]
struct UringIOFactory {
    temp_dir: tempfile::TempDir,
}

#[cfg(all(target_os = "linux", feature = "io_uring", feature = "fs", not(miri)))]
impl UringIOFactory {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().unwrap(),
        }
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring", feature = "fs", not(miri)))]
impl IOFactory for UringIOFactory {
    fn create(&self) -> Arc<dyn IO> {
        Arc::new(UringIO::new().unwrap())
    }
    fn temp_dir(&self) -> PathBuf {
        self.temp_dir.path().to_path_buf()
    }
}

#[cfg(all(
    target_os = "windows",
    feature = "experimental_win_iocp",
    feature = "fs",
    not(miri)
))]
struct WinIOCPFactory {
    temp_dir: tempfile::TempDir,
}

#[cfg(all(
    target_os = "windows",
    feature = "experimental_win_iocp",
    feature = "fs",
    not(miri)
))]
impl WinIOCPFactory {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().unwrap(),
        }
    }
}

#[cfg(all(
    target_os = "windows",
    feature = "experimental_win_iocp",
    feature = "fs",
    not(miri)
))]
impl IOFactory for WinIOCPFactory {
    fn create(&self) -> Arc<dyn IO> {
        Arc::new(WindowsIOCP::new().unwrap())
    }
    fn temp_dir(&self) -> PathBuf {
        self.temp_dir.path().to_path_buf()
    }
}

/// Macro to generate shuttle tests for all IO implementations.
/// Creates a test for MemoryIO, and conditionally for PlatformIO and UringIO.
macro_rules! shuttle_io_test {
    ($test_name:ident, $test_impl:ident) => {
        pastey::paste! {
            #[test]
            fn [<shuttle_ $test_name _memory>]() {
                shuttle::check_random(|| $test_impl(MemoryIOFactory::new()), 1000);
            }

            #[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
            #[test]
            fn [<shuttle_ $test_name _platform>]() {
                shuttle::check_random(|| $test_impl(PlatformIOFactory::new()), 1000);
            }

            #[cfg(all(target_os = "linux", feature = "io_uring", feature = "fs", not(miri)))]
            #[test]
            fn [<shuttle_ $test_name _uring>]() {
                shuttle::check_random(|| $test_impl(UringIOFactory::new()), 1000);
            }

            #[cfg(all(target_os = "windows", feature = "experimental_win_iocp", feature = "fs", not(miri)))]
            #[test]
            fn [<shuttle_ $test_name _win_iocp>]() {
                shuttle::check_random(|| $test_impl(WinIOCPFactory::new()), 1000);
            }

        }
    };
}

/// Helper to wait for a completion synchronously and assert it succeeded.
fn wait_completion_ok(io: &dyn IO, c: &Completion) {
    io.wait_for_completion(c.clone()).unwrap();
    assert!(c.succeeded(), "completion failed: {:?}", c.get_error());
    assert!(!c.failed());
    assert!(c.finished());
    assert!(c.get_error().is_none());
}

/// Helper to wait for a completion synchronously without asserting success.
#[allow(dead_code)]
fn wait_completion(io: &dyn IO, c: &Completion) {
    io.wait_for_completion(c.clone()).unwrap();
    assert!(c.finished());
}

/// Test concurrent file creation from multiple threads.
fn test_concurrent_file_creation_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let base = factory.temp_dir();
    let mut handles = vec![];
    const NUM_THREADS: usize = 3;

    for i in 0..NUM_THREADS {
        let io = io.clone();
        let base = base.clone();
        handles.push(thread::spawn(move || {
            let path = base.join(format!("test_file_{}.db", i));
            let file = io
                .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
                .unwrap();
            assert!(file.size().unwrap() == 0);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(concurrent_file_creation, test_concurrent_file_creation_impl);

/// Test concurrent writes to different offsets in the same file.
fn test_concurrent_writes_different_offsets_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    let mut handles = vec![];
    const NUM_THREADS: usize = 3;

    for i in 0..NUM_THREADS {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let data = vec![i as u8; 100];
            let buf = Arc::new(Buffer::new(data));
            let pos = (i * 100) as u64;

            let c = Completion::new_write(|_| {});
            let c = file.pwrite(pos, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify file size accounts for all writes
    let expected_size = (NUM_THREADS * 100) as u64;
    assert_eq!(file.size().unwrap(), expected_size);

    // Read back and verify each segment contains correct data
    for i in 0..NUM_THREADS {
        let read_buf = Arc::new(Buffer::new_temporary(100));
        let pos = (i * 100) as u64;
        let c = Completion::new_read(read_buf.clone(), |_| None);
        let c = file.pread(pos, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let expected = vec![i as u8; 100];
        assert_eq!(
            read_buf.as_slice(),
            expected.as_slice(),
            "data mismatch at offset {}",
            pos
        );
    }
}

shuttle_io_test!(
    concurrent_writes_different_offsets,
    test_concurrent_writes_different_offsets_impl
);

/// Test concurrent reads and writes to the same file.
fn test_concurrent_read_write_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    // First write some initial data
    let initial_data = vec![0xAA; 1000];
    let buf = Arc::new(Buffer::new(initial_data));
    let c = Completion::new_write(|_| {});
    let c = file.pwrite(0, buf, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);

    let mut handles = vec![];

    // Spawn readers
    for _ in 0..2 {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let read_buf = Arc::new(Buffer::new_temporary(100));
            let c = Completion::new_read(read_buf.clone(), |_| None);
            let c = file.pread(0, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);

            // All bytes read should be 0xAA (initial data at offset 0)
            assert!(
                read_buf.as_slice().iter().all(|&b| b == 0xAA),
                "read buffer should contain initial data 0xAA"
            );
        }));
    }

    // Spawn a writer
    {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let data = vec![0xBB; 100];
            let buf = Arc::new(Buffer::new(data));
            let c = Completion::new_write(|_| {});
            let c = file.pwrite(500, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify the write at offset 500 succeeded
    let read_buf = Arc::new(Buffer::new_temporary(100));
    let c = Completion::new_read(read_buf.clone(), |_| None);
    let c = file.pread(500, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);
    assert!(
        read_buf.as_slice().iter().all(|&b| b == 0xBB),
        "data at offset 500 should be 0xBB"
    );
}

shuttle_io_test!(concurrent_read_write, test_concurrent_read_write_impl);

/// Test that completion callbacks are invoked correctly under concurrency.
fn test_completion_callbacks_concurrent_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    let callback_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    const NUM_WRITES: usize = 3;

    for i in 0..NUM_WRITES {
        let file = file.clone();
        let io = io.clone();
        let count = callback_count.clone();
        handles.push(thread::spawn(move || {
            let data = vec![i as u8; 50];
            let buf = Arc::new(Buffer::new(data));
            let count_clone = count.clone();
            let c = Completion::new_write(move |res| {
                assert!(res.is_ok());
                count_clone.fetch_add(1, Ordering::SeqCst);
            });
            let c = file.pwrite((i * 50) as u64, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(callback_count.load(Ordering::SeqCst), NUM_WRITES);
}

shuttle_io_test!(
    completion_callbacks_concurrent,
    test_completion_callbacks_concurrent_impl
);

/// Test concurrent truncate operations.
fn test_concurrent_truncate_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    // Write initial data
    let initial = vec![0xFF; 5000];
    let buf = Arc::new(Buffer::new(initial));
    let c = Completion::new_write(|_| {});
    let c = file.pwrite(0, buf, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);

    let mut handles = vec![];

    // Spawn threads that truncate to different sizes
    for i in 0..3 {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let truncate_size = ((i + 1) * 1000) as u64;
            let c = Completion::new_trunc(|_| {});
            let c = file.truncate(truncate_size, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Size should be one of the truncate values
    let final_size = file.size().unwrap();
    assert!(final_size == 1000 || final_size == 2000 || final_size == 3000);
}

shuttle_io_test!(concurrent_truncate, test_concurrent_truncate_impl);

/// Test pwritev with concurrent reads.
fn test_pwritev_with_concurrent_reads_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    // Write initial data so reads have something to return
    let initial = vec![0x11; 2000];
    let buf = Arc::new(Buffer::new(initial));
    let c = Completion::new_write(|_| {});
    let c = file.pwrite(0, buf, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);

    let mut handles = vec![];

    // Spawn a pwritev thread
    {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let bufs = vec![
                Arc::new(Buffer::new(vec![0x22; 100])),
                Arc::new(Buffer::new(vec![0x33; 100])),
                Arc::new(Buffer::new(vec![0x44; 100])),
            ];
            let c = Completion::new_write(|_| {});
            let c = file.pwritev(0, bufs, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    // Spawn reader threads
    for _ in 0..2 {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let buf = Arc::new(Buffer::new_temporary(100));
            let c = Completion::new_read(buf.clone(), |_| None);
            let c = file.pread(0, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);

            // Data should be either initial (0x11) or from pwritev (0x22)
            // depending on race ordering
            let first_byte = buf.as_slice()[0];
            assert!(
                first_byte == 0x11 || first_byte == 0x22,
                "first byte should be 0x11 or 0x22, got {:#x}",
                first_byte
            );
            // All 100 bytes should be consistent
            assert!(
                buf.as_slice().iter().all(|&b| b == first_byte),
                "all bytes should be the same value"
            );
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // After all threads complete, verify pwritev data is present
    let read_buf = Arc::new(Buffer::new_temporary(300));
    let c = Completion::new_read(read_buf.clone(), |_| None);
    let c = file.pread(0, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);

    // Should have 0x22 for first 100, 0x33 for next 100, 0x44 for last 100
    assert!(
        read_buf.as_slice()[..100].iter().all(|&b| b == 0x22),
        "bytes 0-99 should be 0x22"
    );
    assert!(
        read_buf.as_slice()[100..200].iter().all(|&b| b == 0x33),
        "bytes 100-199 should be 0x33"
    );
    assert!(
        read_buf.as_slice()[200..300].iter().all(|&b| b == 0x44),
        "bytes 200-299 should be 0x44"
    );
}

shuttle_io_test!(
    pwritev_with_concurrent_reads,
    test_pwritev_with_concurrent_reads_impl
);

/// Test concurrent access to multiple files.
fn test_concurrent_multifile_access_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let base = factory.temp_dir();

    let mut handles = vec![];
    const NUM_FILES: usize = 3;

    for i in 0..NUM_FILES {
        let io = io.clone();
        let base = base.clone();
        handles.push(thread::spawn(move || {
            let path = base.join(format!("file_{}.db", i));
            let file = io
                .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
                .unwrap();

            // Write to file
            let data = vec![i as u8; 200];
            let buf = Arc::new(Buffer::new(data.clone()));
            let c = Completion::new_write(|_| {});
            let c = file.pwrite(0, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);

            // Read back and verify
            let read_buf = Arc::new(Buffer::new_temporary(200));
            let c = Completion::new_read(read_buf.clone(), |_| None);
            let c = file.pread(0, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);

            assert_eq!(read_buf.as_slice(), data.as_slice());
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(
    concurrent_multifile_access,
    test_concurrent_multifile_access_impl
);

/// Test file locking under concurrent access.
fn test_file_locking_concurrent_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    let mut handles = vec![];

    // Multiple threads try to lock/unlock
    for _ in 0..3 {
        let file = file.clone();
        handles.push(thread::spawn(move || {
            // Exclusive lock
            file.lock_file(true).unwrap();
            thread::yield_now();
            file.unlock_file().unwrap();

            // Shared lock
            file.lock_file(false).unwrap();
            thread::yield_now();
            file.unlock_file().unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(file_locking_concurrent, test_file_locking_concurrent_impl);

/// Test reading past end of file returns zero bytes.
fn test_read_past_eof_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    // Write 100 bytes
    let data = vec![0xAA; 100];
    let buf = Arc::new(Buffer::new(data));
    let c = Completion::new_write(|_| {});
    let c = file.pwrite(0, buf, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);

    let mut handles = vec![];

    // Multiple threads try to read past EOF
    for _ in 0..3 {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let read_buf = Arc::new(Buffer::new_temporary(100));
            let bytes_read = Arc::new(AtomicUsize::new(999));
            let bytes_read_clone = bytes_read.clone();
            let c = Completion::new_read(read_buf, move |res| {
                if let Ok((_, n)) = res {
                    bytes_read_clone.store(n as usize, Ordering::SeqCst);
                }
                None
            });
            let c = file.pread(200, c).unwrap(); // Past EOF
                                                 // Reading past EOF succeeds with 0 bytes read
            wait_completion_ok(io.as_ref(), &c);
            assert_eq!(bytes_read.load(Ordering::SeqCst), 0);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(read_past_eof, test_read_past_eof_impl);

/// Test empty write operations.
fn test_empty_write_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    let mut handles = vec![];

    for _ in 0..3 {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            // Empty buffer write
            let buf = Arc::new(Buffer::new(vec![]));
            let c = Completion::new_write(|_| {});
            let c = file.pwrite(0, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(file.size().unwrap(), 0);
}

shuttle_io_test!(empty_write, test_empty_write_impl);

/// Test sync operations under concurrency.
fn test_concurrent_sync_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    // Write some data first
    let data = vec![0xFF; 1000];
    let buf = Arc::new(Buffer::new(data));
    let c = Completion::new_write(|_| {});
    let c = file.pwrite(0, buf, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);

    let mut handles = vec![];

    // Multiple sync calls concurrently
    for _ in 0..3 {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let c = Completion::new_sync(|_| {});
            let c = file.sync(c, FileSyncType::Fsync).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(concurrent_sync, test_concurrent_sync_impl);

/// Test concurrent open of the same file returns same file instance.
fn test_concurrent_open_same_file_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("shared.db");

    // Create file first
    let _ = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    let mut handles = vec![];

    for _ in 0..3 {
        let io = io.clone();
        let path = path.clone();
        handles.push(thread::spawn(move || {
            let file = io
                .open_file(path.to_str().unwrap(), OpenFlags::None, false)
                .unwrap();
            thread::yield_now();
            // Write a byte to prove we got a valid file
            let buf = Arc::new(Buffer::new(vec![0xAA]));
            let c = Completion::new_write(|_| {});
            let c = file.pwrite(0, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(
    concurrent_open_same_file,
    test_concurrent_open_same_file_impl
);

/// Test file removal while concurrent access.
fn test_file_remove_concurrent_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let base = factory.temp_dir();

    // Create multiple files
    for i in 0..3 {
        let path = base.join(format!("remove_{}.db", i));
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();
        let buf = Arc::new(Buffer::new(vec![0xFF; 100]));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);
    }

    let mut handles = vec![];

    // Remove files concurrently
    for i in 0..3 {
        let io = io.clone();
        let base = base.clone();
        handles.push(thread::spawn(move || {
            let path = base.join(format!("remove_{}.db", i));
            io.remove_file(path.to_str().unwrap()).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(file_remove_concurrent, test_file_remove_concurrent_impl);

/// Test write spanning multiple internal pages.
fn test_large_write_concurrent_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    let mut handles = vec![];

    // Multiple threads write large buffers that span multiple pages
    for i in 0..2 {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            // Write 10000 bytes (spans multiple 4096-byte pages)
            let data = vec![(i + 1) as u8; 10000];
            let buf = Arc::new(Buffer::new(data));
            let c = Completion::new_write(|_| {});
            let c = file.pwrite((i * 10000) as u64, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(file.size().unwrap(), 20000);

    // Read back and verify each segment contains correct data
    for i in 0..2 {
        let read_buf = Arc::new(Buffer::new_temporary(10000));
        let pos = (i * 10000) as u64;
        let c = Completion::new_read(read_buf.clone(), |_| None);
        let c = file.pread(pos, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let expected_byte = (i + 1) as u8;
        assert!(
            read_buf.as_slice().iter().all(|&b| b == expected_byte),
            "all bytes at offset {} should be {:#x}",
            pos,
            expected_byte
        );
    }
}

shuttle_io_test!(large_write_concurrent, test_large_write_concurrent_impl);

/// Test has_hole and punch_hole under concurrency.
/// Note: Only runs on MemoryIO as hole operations are not supported on all backends.
fn test_hole_operations_concurrent_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    // Write data spanning multiple pages (at least 3 pages = 12288 bytes)
    let data = vec![0xFF; 16384];
    let buf = Arc::new(Buffer::new(data));
    let c = Completion::new_write(|_| {});
    let c = file.pwrite(0, buf, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);

    let mut handles = vec![];

    // Thread 1: punch holes
    {
        let file = file.clone();
        handles.push(thread::spawn(move || {
            // Punch hole in middle page (page-aligned)
            file.punch_hole(4096, 4096).unwrap();
        }));
    }

    // Thread 2: check for holes
    {
        let file = file.clone();
        handles.push(thread::spawn(move || {
            // Check various regions
            let has_hole = file.has_hole(0, 4096).unwrap();
            assert!(!has_hole);
            let _ = file.has_hole(4096, 4096).unwrap();
            let has_hole = file.has_hole(8192, 4096).unwrap();
            assert!(!has_hole);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// hole_operations only runs on MemoryIO since not all backends support holes
#[test]
fn shuttle_hole_operations_concurrent_memory() {
    shuttle::check_random(
        || test_hole_operations_concurrent_impl(MemoryIOFactory::new()),
        1000,
    );
}

/// Test that partial reads work correctly at EOF boundary.
fn test_partial_read_at_eof_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    // Write exactly 150 bytes
    let data = vec![0xAB; 150];
    let buf = Arc::new(Buffer::new(data));
    let c = Completion::new_write(|_| {});
    let c = file.pwrite(0, buf, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);

    let mut handles = vec![];

    // Multiple threads try to read 100 bytes starting at offset 100
    // Should only get 50 bytes back
    for _ in 0..3 {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let read_buf = Arc::new(Buffer::new_temporary(100));
            let bytes_read = Arc::new(AtomicUsize::new(999));
            let bytes_read_clone = bytes_read.clone();
            let c = Completion::new_read(read_buf.clone(), move |res| {
                if let Ok((_, n)) = res {
                    bytes_read_clone.store(n as usize, Ordering::SeqCst);
                }
                None
            });
            let c = file.pread(100, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);

            // Should read exactly 50 bytes (150 - 100)
            assert_eq!(bytes_read.load(Ordering::SeqCst), 50);
            // Verify the bytes read are correct
            assert_eq!(&read_buf.as_slice()[..50], &[0xAB; 50]);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(partial_read_at_eof, test_partial_read_at_eof_impl);

/// Test empty pwritev.
fn test_empty_pwritev_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    let mut handles = vec![];

    for _ in 0..3 {
        let file = file.clone();
        let io = io.clone();
        handles.push(thread::spawn(move || {
            let bufs: Vec<Arc<Buffer>> = vec![];
            let c = Completion::new_write(|_| {});
            let c = file.pwritev(0, bufs, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(empty_pwritev, test_empty_pwritev_impl);

/// Test error case: opening non-existent file without Create flag.
fn test_open_nonexistent_without_create_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let base = factory.temp_dir();

    let mut handles = vec![];

    for i in 0..3 {
        let io = io.clone();
        let base = base.clone();
        handles.push(thread::spawn(move || {
            let path = base.join(format!("nonexistent_{}.db", i));
            let result = io.open_file(path.to_str().unwrap(), OpenFlags::None, false);
            assert!(result.is_err());
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

shuttle_io_test!(
    open_nonexistent_without_create,
    test_open_nonexistent_without_create_impl
);

/// Test concurrent writes to overlapping regions.
/// This tests that the final state is consistent (one of the writes wins).
fn test_concurrent_overlapping_writes_impl<F: IOFactory>(factory: F) {
    let io = factory.create();
    let path = factory.temp_dir().join("test.db");
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();

    let write_complete = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Multiple threads write to the same offset
    for i in 0..3 {
        let file = file.clone();
        let io = io.clone();
        let write_complete = write_complete.clone();
        handles.push(thread::spawn(move || {
            let data = vec![(i + 1) as u8; 100];
            let buf = Arc::new(Buffer::new(data));
            let write_complete_clone = write_complete.clone();
            let c = Completion::new_write(move |_| {
                write_complete_clone.fetch_add(1, Ordering::SeqCst);
            });
            let c = file.pwrite(0, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // All writes should have completed
    assert_eq!(write_complete.load(Ordering::SeqCst), 3);

    // Read back and verify we got one of the written values
    let read_buf = Arc::new(Buffer::new_temporary(100));
    let c = Completion::new_read(read_buf.clone(), |_| None);
    let c = file.pread(0, c).unwrap();
    wait_completion_ok(io.as_ref(), &c);

    let first_byte = read_buf.as_slice()[0];
    assert!(first_byte == 1 || first_byte == 2 || first_byte == 3);

    // All 100 bytes should be the same value
    assert!(read_buf.as_slice().iter().all(|&b| b == first_byte));
}

shuttle_io_test!(
    concurrent_overlapping_writes,
    test_concurrent_overlapping_writes_impl
);
