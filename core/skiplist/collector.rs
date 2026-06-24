//! Collector selection and pinning for skiplist epoch reclamation.

use std::cell::RefCell;

use crossbeam_epoch::{Collector, Guard, LocalHandle};

// TODO: to avoid allocations in the future we can use an intrusive linked list
thread_local! {
    static LOCAL_HANDLES: RefCell<Vec<CachedHandle>> = const { RefCell::new(Vec::new()) };
}

struct CachedHandle {
    collector: Collector,
    handle: LocalHandle,
}

#[inline]
pub(crate) fn pin(collector: &Collector) -> Guard {
    if collector == crossbeam_epoch::default_collector() {
        crossbeam_epoch::pin()
    } else {
        with_handle(collector, |handle| handle.pin())
    }
}

#[inline]
fn with_handle<F, R>(collector: &Collector, mut f: F) -> R
where
    F: FnMut(&LocalHandle) -> R,
{
    LOCAL_HANDLES
        .try_with(|handles| {
            let Ok(mut handles) = handles.try_borrow_mut() else {
                return f(&collector.register());
            };

            let index = handles
                .iter()
                .position(|handle| handle.collector == *collector)
                .unwrap_or_else(|| {
                    handles.push(CachedHandle {
                        collector: collector.clone(),
                        handle: collector.register(),
                    });
                    handles.len() - 1
                });
            f(&handles[index].handle)
        })
        .unwrap_or_else(|_| f(&collector.register()))
}
