use crate::sync::Arc;

use super::SpinLock;

#[test]
fn test_fast_lock_multiple_thread_sum() {
    let lock = Arc::new(SpinLock::new(0));
    let mut threads = vec![];
    const NTHREADS: usize = 1000;
    for _ in 0..NTHREADS {
        let lock = lock.clone();
        threads.push(std::thread::spawn(move || {
            let mut guard = lock.lock();
            *guard += 1;
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(*lock.lock(), NTHREADS);
}
