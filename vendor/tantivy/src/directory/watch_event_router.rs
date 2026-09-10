use std::sync::{Arc, RwLock, Weak};

use crate::FutureResult;

/// Cloneable wrapper for callbacks registered when watching files of a `Directory`.
#[derive(Clone)]
pub struct WatchCallback(Arc<dyn Fn() + Sync + Send>);

impl WatchCallback {
    /// Wraps a `Fn()` to create a WatchCallback.
    pub fn new<F: Fn() + Sync + Send + 'static>(op: F) -> Self {
        WatchCallback(Arc::new(op))
    }

    fn call(&self) {
        self.0()
    }
}

/// Helper struct to implement the watch method in `Directory` implementations.
///
/// It registers callbacks (See `.subscribe(...)`) and
/// calls them upon calls to `.broadcast(...)`.
#[derive(Default)]
pub struct WatchCallbackList {
    router: RwLock<Vec<Weak<WatchCallback>>>,
}

/// Controls how long a directory should watch for a file change.
///
/// After all the clones of `WatchHandle` are dropped, the associated will not be called when a
/// file change is detected.
#[must_use = "This `WatchHandle` controls the lifetime of the watch and should therefore be used."]
#[derive(Clone)]
#[expect(dead_code)]
pub struct WatchHandle(Arc<WatchCallback>);

impl WatchHandle {
    /// Create a WatchHandle handle.
    pub fn new(watch_callback: Arc<WatchCallback>) -> WatchHandle {
        WatchHandle(watch_callback)
    }

    /// Returns an empty watch handle.
    ///
    /// This function is only useful when implementing a readonly directory.
    pub fn empty() -> WatchHandle {
        WatchHandle::new(Arc::new(WatchCallback::new(|| {})))
    }
}

impl WatchCallbackList {
    /// Subscribes a new callback and returns a handle that controls the lifetime of the callback.
    pub fn subscribe(&self, watch_callback: WatchCallback) -> WatchHandle {
        let watch_callback_arc = Arc::new(watch_callback);
        let watch_callback_weak = Arc::downgrade(&watch_callback_arc);
        self.router.write().unwrap().push(watch_callback_weak);
        WatchHandle::new(watch_callback_arc)
    }

    fn list_callback(&self) -> Vec<WatchCallback> {
        let mut callbacks: Vec<WatchCallback> = vec![];
        let mut router_wlock = self.router.write().unwrap();
        let mut i = 0;
        while i < router_wlock.len() {
            if let Some(watch) = router_wlock[i].upgrade() {
                callbacks.push(watch.as_ref().clone());
                i += 1;
            } else {
                router_wlock.swap_remove(i);
            }
        }
        callbacks
    }

    /// Triggers all callbacks
    pub fn broadcast(&self) -> FutureResult<()> {
        let callbacks = self.list_callback();
        let (result, sender) = FutureResult::create("One of the callback panicked.");
        if callbacks.is_empty() {
            let _ = sender.send(Ok(()));
            return result;
        }
        let spawn_res = std::thread::Builder::new()
            .name("watch-callbacks".to_string())
            .spawn(move || {
                for callback in callbacks {
                    callback.call();
                }
                let _ = sender.send(Ok(()));
            });
        if let Err(err) = spawn_res {
            error!("Failed to spawn thread to call watch callbacks. Cause: {err:?}");
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::directory::{WatchCallback, WatchCallbackList};

    #[test]
    fn test_watch_event_router_simple() {
        let watch_event_router = WatchCallbackList::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let inc_callback = WatchCallback::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });
        watch_event_router.broadcast().wait().unwrap();
        assert_eq!(0, counter.load(Ordering::SeqCst));
        let handle_a = watch_event_router.subscribe(inc_callback);
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast().wait().unwrap();
        assert_eq!(1, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast().wait().unwrap();
        watch_event_router.broadcast().wait().unwrap();
        watch_event_router.broadcast().wait().unwrap();
        assert_eq!(4, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        watch_event_router.broadcast().wait().unwrap();
        assert_eq!(4, counter.load(Ordering::SeqCst));
    }

    #[test]
    fn test_watch_event_router_multiple_callback_same_key() {
        let watch_event_router = WatchCallbackList::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let inc_callback = |inc: usize| {
            let counter_clone = counter.clone();
            WatchCallback::new(move || {
                counter_clone.fetch_add(inc, Ordering::SeqCst);
            })
        };
        let handle_a = watch_event_router.subscribe(inc_callback(1));
        let handle_a2 = watch_event_router.subscribe(inc_callback(10));
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast().wait().unwrap();
        watch_event_router.broadcast().wait().unwrap();
        assert_eq!(22, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        watch_event_router.broadcast().wait().unwrap();
        assert_eq!(32, counter.load(Ordering::SeqCst));
        mem::drop(handle_a2);
        watch_event_router.broadcast().wait().unwrap();
        watch_event_router.broadcast().wait().unwrap();
        assert_eq!(32, counter.load(Ordering::SeqCst));
    }

    #[test]
    fn test_watch_event_router_multiple_callback_different_key() {
        let watch_event_router = WatchCallbackList::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let inc_callback = WatchCallback::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });
        let handle_a = watch_event_router.subscribe(inc_callback);
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast().wait().unwrap();
        watch_event_router.broadcast().wait().unwrap();
        assert_eq!(2, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        drop(watch_event_router.broadcast());
        watch_event_router.broadcast().wait().unwrap();
        assert_eq!(2, counter.load(Ordering::SeqCst));
    }
}
