use std::io::Write;
use std::mem;
use std::path::Path;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

use super::*;

#[cfg(feature = "mmap")]
mod mmap_directory_tests {
    use crate::directory::MmapDirectory;

    type DirectoryImpl = MmapDirectory;

    fn make_directory() -> DirectoryImpl {
        MmapDirectory::create_from_tempdir().unwrap()
    }

    #[test]
    fn test_simple() -> crate::Result<()> {
        let directory = make_directory();
        super::test_simple(&directory)
    }

    #[test]
    fn test_write_create_the_file() {
        let directory = make_directory();
        super::test_write_create_the_file(&directory);
    }

    #[test]
    fn test_rewrite_forbidden() -> crate::Result<()> {
        let directory = make_directory();
        super::test_rewrite_forbidden(&directory)?;
        Ok(())
    }

    #[test]
    fn test_directory_delete() -> crate::Result<()> {
        let directory = make_directory();
        super::test_directory_delete(&directory)?;
        Ok(())
    }

    #[test]
    fn test_lock_non_blocking() {
        let directory = make_directory();
        super::test_lock_non_blocking(&directory);
    }

    #[test]
    fn test_lock_blocking() {
        let directory = make_directory();
        super::test_lock_blocking(&directory);
    }

    #[test]
    fn test_watch() {
        let directory = make_directory();
        super::test_watch(&directory);
    }
}

mod ram_directory_tests {
    use crate::directory::RamDirectory;

    type DirectoryImpl = RamDirectory;

    fn make_directory() -> DirectoryImpl {
        RamDirectory::default()
    }

    #[test]
    fn test_simple() -> crate::Result<()> {
        let directory = make_directory();
        super::test_simple(&directory)
    }

    #[test]
    fn test_write_create_the_file() {
        let directory = make_directory();
        super::test_write_create_the_file(&directory);
    }

    #[test]
    fn test_rewrite_forbidden() -> crate::Result<()> {
        let directory = make_directory();
        super::test_rewrite_forbidden(&directory)?;
        Ok(())
    }

    #[test]
    fn test_directory_delete() -> crate::Result<()> {
        let directory = make_directory();
        super::test_directory_delete(&directory)?;
        Ok(())
    }

    #[test]
    fn test_lock_non_blocking() {
        let directory = make_directory();
        super::test_lock_non_blocking(&directory);
    }

    #[test]
    fn test_lock_blocking() {
        let directory = make_directory();
        super::test_lock_blocking(&directory);
    }

    #[test]
    fn test_watch() {
        let directory = make_directory();
        super::test_watch(&directory);
    }
}

fn test_simple(directory: &dyn Directory) -> crate::Result<()> {
    let test_path: &'static Path = Path::new("some_path_for_test");
    let mut write_file = directory.open_write(test_path)?;
    assert!(directory.exists(test_path).unwrap());
    write_file.write_all(&[4])?;
    write_file.write_all(&[3])?;
    write_file.write_all(&[7, 3, 5])?;
    write_file.flush()?;
    let read_file = directory.open_read(test_path)?.read_bytes()?;
    assert_eq!(read_file.as_slice(), &[4u8, 3u8, 7u8, 3u8, 5u8]);
    mem::drop(read_file);
    assert!(directory.delete(test_path).is_ok());
    assert!(!directory.exists(test_path).unwrap());
    Ok(())
}

fn test_rewrite_forbidden(directory: &dyn Directory) -> crate::Result<()> {
    let test_path: &'static Path = Path::new("some_path_for_test");
    directory.open_write(test_path)?;
    assert!(directory.exists(test_path).unwrap());
    assert!(directory.open_write(test_path).is_err());
    assert!(directory.delete(test_path).is_ok());
    Ok(())
}

fn test_write_create_the_file(directory: &dyn Directory) {
    let test_path: &'static Path = Path::new("some_path_for_test");
    {
        assert!(directory.open_read(test_path).is_err());
        let _w = directory.open_write(test_path).unwrap();
        assert!(directory.exists(test_path).unwrap());
        assert!(directory.open_read(test_path).is_ok());
        assert!(directory.delete(test_path).is_ok());
    }
}

fn test_directory_delete(directory: &dyn Directory) -> crate::Result<()> {
    let test_path: &'static Path = Path::new("some_path_for_test");
    assert!(directory.open_read(test_path).is_err());
    let mut write_file = directory.open_write(test_path)?;
    write_file.write_all(&[1, 2, 3, 4])?;
    write_file.flush()?;
    {
        let read_handle = directory.open_read(test_path)?.read_bytes()?;
        assert_eq!(read_handle.as_slice(), &[1u8, 2u8, 3u8, 4u8]);
        // Mapped files can't be deleted on Windows
        if !cfg!(windows) {
            assert!(directory.delete(test_path).is_ok());
            assert_eq!(read_handle.as_slice(), &[1u8, 2u8, 3u8, 4u8]);
        }
        assert!(directory.delete(Path::new("SomeOtherPath")).is_err());
    }

    if cfg!(windows) {
        assert!(directory.delete(test_path).is_ok());
    }

    assert!(directory.open_read(test_path).is_err());
    assert!(directory.delete(test_path).is_err());
    Ok(())
}

fn test_watch(directory: &dyn Directory) {
    let counter: Arc<AtomicUsize> = Default::default();
    let (tx, rx) = crossbeam_channel::unbounded();
    let timeout = Duration::from_millis(500);

    let handle = directory
        .watch(WatchCallback::new(move || {
            let val = counter.fetch_add(1, SeqCst);
            tx.send(val + 1).unwrap();
        }))
        .unwrap();

    assert!(directory
        .atomic_write(Path::new("meta.json"), b"foo")
        .is_ok());
    assert_eq!(rx.recv_timeout(timeout), Ok(1));

    assert!(directory
        .atomic_write(Path::new("meta.json"), b"bar")
        .is_ok());
    assert_eq!(rx.recv_timeout(timeout), Ok(2));

    mem::drop(handle);

    assert!(directory
        .atomic_write(Path::new("meta.json"), b"qux")
        .is_ok());
    assert!(rx.recv_timeout(timeout).is_err());
}

fn test_lock_non_blocking(directory: &dyn Directory) {
    {
        let lock_a_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: false,
        });
        assert!(lock_a_res.is_ok());
        let lock_b_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("b.lock"),
            is_blocking: false,
        });
        assert!(lock_b_res.is_ok());
        let lock_a_res2 = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: false,
        });
        assert!(lock_a_res2.is_err());
    }
    let lock_a_res = directory.acquire_lock(&Lock {
        filepath: PathBuf::from("a.lock"),
        is_blocking: false,
    });
    assert!(lock_a_res.is_ok());
}

fn test_lock_blocking(directory: &dyn Directory) {
    let lock_a_res = directory.acquire_lock(&Lock {
        filepath: PathBuf::from("a.lock"),
        is_blocking: true,
    });
    assert!(lock_a_res.is_ok());
    let in_thread = Arc::new(AtomicBool::default());
    let in_thread_clone = in_thread.clone();
    let (sender, receiver) = oneshot::channel();
    std::thread::spawn(move || {
        //< lock_a_res is sent to the thread.
        in_thread_clone.store(true, SeqCst);
        let _just_sync = receiver.recv();
        // explicitly dropping lock_a_res. It would have been sufficient to just force it
        // to be part of the move, but the intent seems clearer that way.
        drop(lock_a_res);
    });
    {
        // A non-blocking call should fail, as the thread is running and holding the lock.
        let lock_a_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: false,
        });
        assert!(lock_a_res.is_err());
    }
    let directory_clone = directory.box_clone();
    let (sender2, receiver2) = oneshot::channel();
    let join_handle = std::thread::spawn(move || {
        assert!(sender2.send(()).is_ok());
        let lock_a_res = directory_clone.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: true,
        });
        assert!(in_thread.load(SeqCst));
        assert!(lock_a_res.is_ok());
    });
    assert!(receiver2.recv().is_ok());
    assert!(sender.send(()).is_ok());
    assert!(join_handle.join().is_ok());
}

mod async_mutations {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::io;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use futures::channel::oneshot;
    use futures::FutureExt;

    use super::*;
    use crate::directory::error::{DeleteError, OpenReadError, OpenWriteError};
    use crate::{Index, IndexSettings};

    #[test]
    fn ram_metadata_mutations_work_through_a_boxed_directory() {
        let directory: Box<dyn Directory> = Box::new(RamDirectory::default());
        async {
            directory
                .atomic_write_async(Path::new("meta"), b"old")
                .await
                .unwrap();
            directory
                .atomic_write_async(Path::new("meta"), b"new")
                .await
                .unwrap();
            directory.sync_directory_async().await.unwrap();
            assert_eq!(
                directory
                    .atomic_read_async(Path::new("meta"))
                    .await
                    .unwrap(),
                b"new"
            );
            directory.delete_async(Path::new("meta")).await.unwrap();
            assert!(matches!(
                directory.delete_async(Path::new("meta")).await,
                Err(DeleteError::FileDoesNotExist(_))
            ));
        }
        .now_or_never()
        .unwrap();
        let unsupported = DelayedDirectory::default();
        assert!(
            matches!(unsupported.delete_async(Path::new("meta")).now_or_never().unwrap(), Err(DeleteError::IoError { io_error, .. }) if io_error.kind() == io::ErrorKind::Unsupported)
        );
    }

    #[test]
    fn index_creation_awaits_each_metadata_operation() {
        for fail_at in 0..=5 {
            let directory = DelayedDirectory::default();
            let schema = crate::schema::Schema::builder().build();
            let mut create = Box::pin(Index::create_async(
                directory.clone(),
                schema.clone(),
                IndexSettings::default(),
            ));
            for (step, expected) in ["sync", ".managed.json", "sync", "meta.json", "sync"]
                .iter()
                .enumerate()
            {
                for _ in 0..3 {
                    assert!(poll(create.as_mut()).is_pending());
                    assert_eq!(directory.pending.lock().unwrap().len(), 1);
                }
                assert_eq!(directory.complete(step == fail_at), *expected);
                if step == fail_at {
                    assert!(matches!(poll(create.as_mut()), Poll::Ready(Err(_))));
                    assert!(directory.pending.lock().unwrap().is_empty());
                    break;
                }
            }
            if fail_at == 5 {
                let Poll::Ready(Ok(index)) = poll(create.as_mut()) else {
                    panic!("creation did not complete")
                };
                assert_eq!(index.schema(), schema);
                let opened = Index::open_async(directory.clone())
                    .now_or_never()
                    .unwrap()
                    .unwrap();
                assert_eq!(opened.schema(), schema);
            }
        }
    }

    #[test]
    fn cancelled_metadata_writes_cannot_race_a_new_write() {
        for cancel_at in 0..3 {
            let directory = DelayedDirectory::default();
            let managed = ManagedDirectory::wrap_async(Box::new(directory.clone()))
                .now_or_never()
                .unwrap()
                .unwrap();
            let mut write = managed.atomic_write_async(Path::new("first"), b"one");
            for _ in 0..cancel_at {
                assert!(poll(write.as_mut()).is_pending());
                directory.complete(false);
            }
            assert!(poll(write.as_mut()).is_pending());
            drop(write);
            let clone = managed.clone();
            let error = clone
                .atomic_write_async(Path::new("second"), b"two")
                .now_or_never()
                .unwrap()
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(
                clone
                    .atomic_write(Path::new(".managed.json"), b"[]")
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            // The driver still owns the cancelled write and its bytes.
            assert_eq!(directory.pending.lock().unwrap().len(), 1);
            directory.complete(false);
            assert!(!directory.ram.exists(Path::new("second")).unwrap());
        }
    }

    #[test]
    fn concurrent_metadata_registration_waits_without_losing_paths() {
        let directory = DelayedDirectory::default();
        let managed = ManagedDirectory::wrap_async(Box::new(directory.clone()))
            .now_or_never()
            .unwrap()
            .unwrap();
        let clone = managed.clone();
        let mut first = managed.atomic_write_async(Path::new("first"), b"one");
        let mut second = clone.atomic_write_async(Path::new("second"), b"two");
        for _ in 0..3 {
            assert!(poll(first.as_mut()).is_pending());
            assert!(poll(second.as_mut()).is_pending());
            assert_eq!(directory.pending.lock().unwrap().len(), 1);
            directory.complete(false);
        }
        assert!(matches!(poll(first.as_mut()), Poll::Ready(Ok(()))));
        for _ in 0..2 {
            assert!(poll(second.as_mut()).is_pending());
            directory.complete(false);
        }
        assert!(matches!(poll(second.as_mut()), Poll::Ready(Ok(()))));
        let bytes = directory
            .ram
            .atomic_read(Path::new(".managed.json"))
            .unwrap();
        let paths: std::collections::HashSet<String> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(paths, ["first".to_owned(), "second".to_owned()].into());
    }

    fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
    }

    #[derive(Debug)]
    struct Mutation {
        path: Option<PathBuf>,
        bytes: Vec<u8>,
        response: oneshot::Sender<io::Result<()>>,
    }

    #[derive(Clone, Debug, Default)]
    struct DelayedDirectory {
        ram: RamDirectory,
        pending: Arc<Mutex<VecDeque<Mutation>>>,
    }

    impl DelayedDirectory {
        fn complete(&self, fail: bool) -> String {
            let mutation = self.pending.lock().unwrap().pop_front().unwrap();
            let name = mutation
                .path
                .as_ref()
                .map(|p| p.to_str().unwrap())
                .unwrap_or("sync")
                .to_owned();
            let result = if fail {
                Err(io::Error::other("injected metadata error"))
            } else if let Some(path) = mutation.path {
                self.ram.atomic_write(&path, &mutation.bytes)
            } else {
                Ok(())
            };
            let _ = mutation.response.send(result);
            name
        }

        fn submit(
            &self,
            path: Option<PathBuf>,
            bytes: Vec<u8>,
        ) -> DirectoryFuture<'_, io::Result<()>> {
            Box::pin(async move {
                let (response, receiver) = oneshot::channel();
                self.pending.lock().unwrap().push_back(Mutation {
                    path,
                    bytes,
                    response,
                });
                receiver
                    .await
                    .map_err(|_| io::Error::other("driver dropped request"))?
            })
        }
    }

    impl Directory for DelayedDirectory {
        fn get_file_handle(&self, _: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
            panic!("sync lookup")
        }
        fn delete(&self, _: &Path) -> Result<(), DeleteError> {
            panic!("sync delete")
        }
        fn exists(&self, _: &Path) -> Result<bool, OpenReadError> {
            panic!("sync exists")
        }
        fn open_write(&self, _: &Path) -> Result<WritePtr, OpenWriteError> {
            panic!("sync open write")
        }
        fn atomic_read(&self, _: &Path) -> Result<Vec<u8>, OpenReadError> {
            panic!("sync metadata read")
        }
        fn atomic_write(&self, _: &Path, _: &[u8]) -> io::Result<()> {
            panic!("sync metadata write")
        }
        fn sync_directory(&self) -> io::Result<()> {
            panic!("sync durability")
        }
        fn watch(&self, _: WatchCallback) -> crate::Result<WatchHandle> {
            Ok(WatchHandle::empty())
        }

        fn atomic_read_async<'a>(
            &'a self,
            path: &'a Path,
        ) -> DirectoryFuture<'a, Result<Vec<u8>, OpenReadError>> {
            self.ram.atomic_read_async(path)
        }
        fn atomic_write_async<'a>(
            &'a self,
            path: &'a Path,
            bytes: &'a [u8],
        ) -> DirectoryFuture<'a, io::Result<()>> {
            self.submit(Some(path.to_owned()), bytes.to_vec())
        }
        fn sync_directory_async(&self) -> DirectoryFuture<'_, io::Result<()>> {
            self.submit(None, Vec::new())
        }
    }
}
