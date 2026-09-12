use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::task::{Wake, Waker};

use crate::{
    io::{common, win_iocp::get_generic_limboerror_from_os_err, OpenFlags, TempFile},
    Buffer, Completion, IO,
};

use super::WindowsIOCP;

#[derive(Default)]
struct FlagWaker(AtomicBool);

impl Wake for FlagWaker {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A single `step()` must leave nothing outstanding and must have fired
/// every registered waker. Futures in the bindings call `step()` exactly
/// once and then return `Poll::Pending`, so a `step()` that reaps only one
/// packet (or none) loses the wakeup and hangs the caller forever.
#[test]
fn test_step_completes_all_outstanding_io_and_wakes_wakers() {
    const WRITE: &[u8] = b"wake";
    // More than one operation, so a `step()` that reaps a single packet
    // fails deterministically rather than depending on IO timing.
    const OPERATIONS: u64 = 64;

    let iocp: Arc<dyn IO> = Arc::new(WindowsIOCP::new().unwrap());
    let file = TempFile::new(&iocp).unwrap();

    let mut completions = Vec::new();
    let mut wakers = Vec::new();
    for n in 0..OPERATIONS {
        let buffer = Arc::new(Buffer::new_temporary(WRITE.len()));
        buffer.as_mut_slice().copy_from_slice(WRITE);
        let completion = file
            .pwrite(
                n * WRITE.len() as u64,
                buffer,
                Completion::new_write(|res| assert_eq!(res, Ok(4))),
            )
            .unwrap();

        let flag = Arc::new(FlagWaker::default());
        completion.set_waker(&Waker::from(flag.clone()));
        completions.push(completion);
        wakers.push(flag);
    }

    iocp.step().unwrap();

    for (n, completion) in completions.iter().enumerate() {
        assert!(
            completion.finished(),
            "completion {n} still outstanding after step()"
        );
        assert!(completion.succeeded(), "completion {n} failed");
    }
    for (n, flag) in wakers.iter().enumerate() {
        assert!(
            flag.0.load(Ordering::SeqCst),
            "waker for completion {n} was never fired"
        );
    }
}

/// `step()` must not block when the backend has no work queued.
#[test]
fn test_step_returns_immediately_when_idle() {
    let iocp: Arc<dyn IO> = Arc::new(WindowsIOCP::new().unwrap());
    iocp.step().unwrap();
    iocp.step().unwrap();
}

#[test]
fn test_file_read_write() {
    let iocp: Arc<dyn IO> = Arc::new(WindowsIOCP::new().unwrap());
    let file = TempFile::new(&iocp).unwrap();

    const WRITE: &[u8] = b"ABCD";

    let mut vec = vec![];
    for n in 0..150 {
        let comp = Completion::new_write(|res| {
            assert_eq!(res, Ok(4));
        });
        let buffer = Arc::new(Buffer::new_temporary(WRITE.len()));

        buffer.as_mut_slice().copy_from_slice(WRITE);

        let ret = file.pwrite(n * WRITE.len() as u64, buffer, comp).unwrap();
        vec.push(ret);
    }
    vec.into_iter().for_each(|c| {
        iocp.wait_for_completion(c.clone()).unwrap();
        if c.failed() {
            panic!();
        }
    });
    let mut vec = vec![];

    for n in 0..150 {
        let buffer = Arc::new(Buffer::new_temporary(WRITE.len()));

        let comp = Completion::new_read(buffer, |res| {
            assert_eq!(res.clone().unwrap().1, 4);
            res.err()
        });

        let ret = file.pread(n * WRITE.len() as u64, comp).unwrap();
        vec.push(ret);
    }
    vec.iter().for_each(|c| {
        iocp.wait_for_completion(c.clone()).unwrap();
    });
    vec.iter().any(|c| c.failed()).then(|| panic!());

    assert_eq!(file.size().unwrap(), 150 * WRITE.iter().len() as u64);
}

#[test]
fn test_error_functions() {
    assert_eq!(
        get_generic_limboerror_from_os_err(5).to_string(),
        String::from("Internal error: Windows Error: [5]Access is denied.\r\n")
    );
}

#[test]
fn test_proper_drop() {
    let write = b"Abcd";
    let iocp: Arc<dyn IO> = Arc::new(WindowsIOCP::new().unwrap());
    let file = TempFile::new(&iocp).unwrap();
    let comp = Completion::new_write(|_| {});
    let buffer = Arc::new(Buffer::new_temporary(write.len()));

    buffer.as_mut_slice().copy_from_slice(write);

    drop(file.pwrite(0, buffer, comp).unwrap());
    drop(iocp);
    drop(file);
}

#[test]
fn test_duplicate_opens_share_process_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("same.db");
    let path = path.to_str().unwrap();
    let io = WindowsIOCP::new().unwrap();

    let first = io.open_file(path, OpenFlags::Create, false).unwrap();
    let second = io.open_file(path, OpenFlags::Create, false).unwrap();
    drop(first);
    drop(second);

    io.open_file(path, OpenFlags::Create, false).unwrap();
}

#[test]
fn test_multiple_processes_cannot_open_file() {
    common::tests::test_multiple_processes_cannot_open_file(WindowsIOCP::new);
}
