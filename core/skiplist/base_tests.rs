#![allow(clippy::redundant_clone)]

use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::skiplist::{base, SkipList};
use crossbeam_epoch as epoch;

fn ref_entry<'a, K, V>(e: impl Into<Option<base::RefEntry<'a, K, V>>>) -> Entry<'a, K, V> {
    Entry(e.into())
}
struct Entry<'a, K, V>(Option<base::RefEntry<'a, K, V>>);
impl<K, V> Entry<'_, K, V> {
    fn value(&self) -> &V {
        self.0.as_ref().unwrap().value()
    }
}
impl<K, V> Drop for Entry<'_, K, V> {
    fn drop(&mut self) {
        if let Some(e) = self.0.take() {
            e.release_with_pin(epoch::pin)
        }
    }
}

#[test]
fn new() {
    SkipList::<i32, i32>::new(epoch::default_collector().clone());
    SkipList::<String, Box<i32>>::new(epoch::default_collector().clone());
}

#[test]
fn is_empty() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    assert!(s.is_empty());

    s.insert(1, 10, guard).release(guard);
    assert!(!s.is_empty());
    s.insert(2, 20, guard).release(guard);
    s.insert(3, 30, guard).release(guard);
    assert!(!s.is_empty());

    s.remove(&2, guard).unwrap().release(guard);
    assert!(!s.is_empty());

    s.remove(&1, guard).unwrap().release(guard);
    assert!(!s.is_empty());

    s.remove(&3, guard).unwrap().release(guard);
    assert!(s.is_empty());
}

#[test]
fn insert() {
    let guard = &epoch::pin();
    let insert = [0, 4, 2, 12, 8, 7, 11, 5];
    let not_present = [1, 3, 6, 9, 10];
    let s = SkipList::new(epoch::default_collector().clone());

    for &x in &insert {
        s.insert(x, x * 10, guard).release(guard);
        assert_eq!(*s.get(&x, guard).unwrap().value(), x * 10);
    }

    for &x in &not_present {
        assert!(s.get(&x, guard).is_none());
    }
}

#[test]
fn remove() {
    let guard = &epoch::pin();
    let insert = [0, 4, 2, 12, 8, 7, 11, 5];
    let not_present = [1, 3, 6, 9, 10];
    let remove = [2, 12, 8];
    let remaining = [0, 4, 5, 7, 11];

    let s = SkipList::new(epoch::default_collector().clone());

    for &x in &insert {
        s.insert(x, x * 10, guard).release(guard);
    }
    for x in &not_present {
        assert!(s.remove(x, guard).is_none());
    }
    for x in &remove {
        s.remove(x, guard).unwrap().release(guard);
    }

    let mut v = vec![];
    let mut e = s.front(guard).unwrap();
    loop {
        v.push(*e.key());
        if !e.move_next() {
            break;
        }
    }

    assert_eq!(v, remaining);
    for x in &insert {
        ref_entry(s.remove(x, guard));
    }
    assert!(s.is_empty());
}

#[test]
fn entry() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());

    assert!(s.front(guard).is_none());
    assert!(s.back(guard).is_none());

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10, guard).release(guard);
    }

    let mut e = s.front(guard).unwrap();
    assert_eq!(*e.key(), 2);
    assert!(!e.move_prev());
    assert!(e.move_next());
    assert_eq!(*e.key(), 4);

    e = s.back(guard).unwrap();
    assert_eq!(*e.key(), 12);
    assert!(!e.move_next());
    assert!(e.move_prev());
    assert_eq!(*e.key(), 11);
}

#[test]
fn entry_remove() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10, guard).release(guard);
    }

    let mut e = s.get(&7, guard).unwrap();
    assert!(!e.is_removed());
    assert!(e.remove());
    assert!(e.is_removed());

    e.move_prev();
    e.move_next();
    assert_ne!(*e.key(), 7);

    for e in s.iter(guard) {
        assert!(!s.is_empty());
        assert_ne!(s.len(), 0);
        e.remove();
    }
    assert!(s.is_empty());
    assert_eq!(s.len(), 0);
}

#[test]
fn entry_reposition() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10, guard).release(guard);
    }

    let mut e = s.get(&7, guard).unwrap();
    assert!(!e.is_removed());
    assert!(e.remove());
    assert!(e.is_removed());

    s.insert(7, 700, guard).release(guard);
    e.move_prev();
    e.move_next();
    assert_eq!(*e.key(), 7);
}

#[test]
fn len() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    assert_eq!(s.len(), 0);

    for (i, &x) in [4, 2, 12, 8, 7, 11, 5].iter().enumerate() {
        s.insert(x, x * 10, guard).release(guard);
        assert_eq!(s.len(), i + 1);
    }

    s.insert(5, 0, guard).release(guard);
    assert_eq!(s.len(), 7);
    s.insert(5, 0, guard).release(guard);
    assert_eq!(s.len(), 7);

    assert!(s.remove(&6, guard).is_none());
    assert_eq!(s.len(), 7);
    s.remove(&5, guard).unwrap().release(guard);
    assert_eq!(s.len(), 6);
    s.remove(&12, guard).unwrap().release(guard);
    assert_eq!(s.len(), 5);
}

#[test]
fn insert_and_remove() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    let keys = || s.iter(guard).map(|e| *e.key()).collect::<Vec<_>>();

    s.insert(3, 0, guard).release(guard);
    s.insert(5, 0, guard).release(guard);
    s.insert(1, 0, guard).release(guard);
    s.insert(4, 0, guard).release(guard);
    s.insert(2, 0, guard).release(guard);
    assert_eq!(keys(), [1, 2, 3, 4, 5]);

    s.remove(&4, guard).unwrap().release(guard);
    assert_eq!(keys(), [1, 2, 3, 5]);
    s.remove(&3, guard).unwrap().release(guard);
    assert_eq!(keys(), [1, 2, 5]);
    s.remove(&1, guard).unwrap().release(guard);
    assert_eq!(keys(), [2, 5]);

    assert!(s.remove(&1, guard).is_none());
    assert_eq!(keys(), [2, 5]);
    assert!(s.remove(&3, guard).is_none());
    assert_eq!(keys(), [2, 5]);

    s.remove(&2, guard).unwrap().release(guard);
    assert_eq!(keys(), [5]);
    s.remove(&5, guard).unwrap().release(guard);
    assert!(keys().is_empty());

    s.insert(3, 0, guard).release(guard);
    assert_eq!(keys(), [3]);
    s.insert(1, 0, guard).release(guard);
    assert_eq!(keys(), [1, 3]);
    s.insert(3, 0, guard).release(guard);
    assert_eq!(keys(), [1, 3]);
    s.insert(5, 0, guard).release(guard);
    assert_eq!(keys(), [1, 3, 5]);

    s.remove(&3, guard).unwrap().release(guard);
    assert_eq!(keys(), [1, 5]);
    s.remove(&1, guard).unwrap().release(guard);
    assert_eq!(keys(), [5]);
    assert!(s.remove(&3, guard).is_none());
    assert_eq!(keys(), [5]);
    s.remove(&5, guard).unwrap().release(guard);
    assert!(keys().is_empty());
}

#[test]
fn get() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    s.insert(30, 3, guard).release(guard);
    s.insert(50, 5, guard).release(guard);
    s.insert(10, 1, guard).release(guard);
    s.insert(40, 4, guard).release(guard);
    s.insert(20, 2, guard).release(guard);

    assert_eq!(*s.get(&10, guard).unwrap().value(), 1);
    assert_eq!(*s.get(&20, guard).unwrap().value(), 2);
    assert_eq!(*s.get(&30, guard).unwrap().value(), 3);
    assert_eq!(*s.get(&40, guard).unwrap().value(), 4);
    assert_eq!(*s.get(&50, guard).unwrap().value(), 5);

    assert!(s.get(&7, guard).is_none());
    assert!(s.get(&27, guard).is_none());
    assert!(s.get(&31, guard).is_none());
    assert!(s.get(&97, guard).is_none());
}

#[test]
fn lower_bound() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    s.insert(30, 3, guard).release(guard);
    s.insert(50, 5, guard).release(guard);
    s.insert(10, 1, guard).release(guard);
    s.insert(40, 4, guard).release(guard);
    s.insert(20, 2, guard).release(guard);

    assert_eq!(*s.lower_bound(Bound::Unbounded, guard).unwrap().value(), 1);

    assert_eq!(
        *s.lower_bound(Bound::Included(&10), guard).unwrap().value(),
        1
    );
    assert_eq!(
        *s.lower_bound(Bound::Included(&20), guard).unwrap().value(),
        2
    );
    assert_eq!(
        *s.lower_bound(Bound::Included(&30), guard).unwrap().value(),
        3
    );
    assert_eq!(
        *s.lower_bound(Bound::Included(&40), guard).unwrap().value(),
        4
    );
    assert_eq!(
        *s.lower_bound(Bound::Included(&50), guard).unwrap().value(),
        5
    );

    assert_eq!(
        *s.lower_bound(Bound::Included(&7), guard).unwrap().value(),
        1
    );
    assert_eq!(
        *s.lower_bound(Bound::Included(&27), guard).unwrap().value(),
        3
    );
    assert_eq!(
        *s.lower_bound(Bound::Included(&31), guard).unwrap().value(),
        4
    );
    assert!(s.lower_bound(Bound::Included(&97), guard).is_none());

    assert_eq!(
        *s.lower_bound(Bound::Excluded(&10), guard).unwrap().value(),
        2
    );
    assert_eq!(
        *s.lower_bound(Bound::Excluded(&20), guard).unwrap().value(),
        3
    );
    assert_eq!(
        *s.lower_bound(Bound::Excluded(&30), guard).unwrap().value(),
        4
    );
    assert_eq!(
        *s.lower_bound(Bound::Excluded(&40), guard).unwrap().value(),
        5
    );
    assert!(s.lower_bound(Bound::Excluded(&50), guard).is_none());

    assert_eq!(
        *s.lower_bound(Bound::Excluded(&7), guard).unwrap().value(),
        1
    );
    assert_eq!(
        *s.lower_bound(Bound::Excluded(&27), guard).unwrap().value(),
        3
    );
    assert_eq!(
        *s.lower_bound(Bound::Excluded(&31), guard).unwrap().value(),
        4
    );
    assert!(s.lower_bound(Bound::Excluded(&97), guard).is_none());
}

#[test]
fn upper_bound() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    s.insert(30, 3, guard).release(guard);
    s.insert(50, 5, guard).release(guard);
    s.insert(10, 1, guard).release(guard);
    s.insert(40, 4, guard).release(guard);
    s.insert(20, 2, guard).release(guard);

    assert_eq!(*s.upper_bound(Bound::Unbounded, guard).unwrap().value(), 5);

    assert_eq!(
        *s.upper_bound(Bound::Included(&10), guard).unwrap().value(),
        1
    );
    assert_eq!(
        *s.upper_bound(Bound::Included(&20), guard).unwrap().value(),
        2
    );
    assert_eq!(
        *s.upper_bound(Bound::Included(&30), guard).unwrap().value(),
        3
    );
    assert_eq!(
        *s.upper_bound(Bound::Included(&40), guard).unwrap().value(),
        4
    );
    assert_eq!(
        *s.upper_bound(Bound::Included(&50), guard).unwrap().value(),
        5
    );

    assert!(s.upper_bound(Bound::Included(&7), guard).is_none());
    assert_eq!(
        *s.upper_bound(Bound::Included(&27), guard).unwrap().value(),
        2
    );
    assert_eq!(
        *s.upper_bound(Bound::Included(&31), guard).unwrap().value(),
        3
    );
    assert_eq!(
        *s.upper_bound(Bound::Included(&97), guard).unwrap().value(),
        5
    );

    assert!(s.upper_bound(Bound::Excluded(&10), guard).is_none());
    assert_eq!(
        *s.upper_bound(Bound::Excluded(&20), guard).unwrap().value(),
        1
    );
    assert_eq!(
        *s.upper_bound(Bound::Excluded(&30), guard).unwrap().value(),
        2
    );
    assert_eq!(
        *s.upper_bound(Bound::Excluded(&40), guard).unwrap().value(),
        3
    );
    assert_eq!(
        *s.upper_bound(Bound::Excluded(&50), guard).unwrap().value(),
        4
    );

    assert!(s.upper_bound(Bound::Excluded(&7), guard).is_none());
    assert_eq!(
        *s.upper_bound(Bound::Excluded(&27), guard).unwrap().value(),
        2
    );
    assert_eq!(
        *s.upper_bound(Bound::Excluded(&31), guard).unwrap().value(),
        3
    );
    assert_eq!(
        *s.upper_bound(Bound::Excluded(&97), guard).unwrap().value(),
        5
    );
}

#[test]
fn get_or_insert() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    s.insert(3, 3, guard).release(guard);
    s.insert(5, 5, guard).release(guard);
    s.insert(1, 1, guard).release(guard);
    s.insert(4, 4, guard).release(guard);
    s.insert(2, 2, guard).release(guard);

    assert_eq!(*s.get(&4, guard).unwrap().value(), 4);
    assert_eq!(*ref_entry(s.insert(4, 40, guard)).value(), 40);
    assert_eq!(*s.get(&4, guard).unwrap().value(), 40);

    assert_eq!(*s.get_or_insert(4, 400, guard).value(), 40);
    assert_eq!(*s.get(&4, guard).unwrap().value(), 40);
    assert_eq!(*s.get_or_insert(6, 600, guard).value(), 600);
}

#[test]
fn get_or_insert_with() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    s.insert(3, 3, guard).release(guard);
    s.insert(5, 5, guard).release(guard);
    s.insert(1, 1, guard).release(guard);
    s.insert(4, 4, guard).release(guard);
    s.insert(2, 2, guard).release(guard);

    assert_eq!(*s.get(&4, guard).unwrap().value(), 4);
    assert_eq!(*ref_entry(s.insert(4, 40, guard)).value(), 40);
    assert_eq!(*s.get(&4, guard).unwrap().value(), 40);

    assert_eq!(*s.get_or_insert_with(4, || 400, guard).value(), 40);
    assert_eq!(*s.get(&4, guard).unwrap().value(), 40);
    assert_eq!(*s.get_or_insert_with(6, || 600, guard).value(), 600);
}

#[test]
fn get_or_insert_with_panic() {
    use std::panic;

    let s = SkipList::new(epoch::default_collector().clone());
    let res = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        let guard = &epoch::pin();
        s.get_or_insert_with(4, || panic!(), guard);
    }));
    assert!(res.is_err());
    assert!(s.is_empty());
    let guard = &epoch::pin();
    assert_eq!(*s.get_or_insert_with(4, || 40, guard).value(), 40);
    assert_eq!(s.len(), 1);
}

#[test]
fn get_or_insert_with_parallel_run() {
    use std::sync::{Arc, Mutex};

    let s = Arc::new(SkipList::new(epoch::default_collector().clone()));
    let s2 = s.clone();
    let called = Arc::new(Mutex::new(false));
    let called2 = called.clone();
    let handle = std::thread::spawn(move || {
        let guard = &epoch::pin();
        assert_eq!(
            *s2.get_or_insert_with(
                7,
                || {
                    *called2.lock().unwrap() = true;

                    // allow main thread to run before we return result
                    std::thread::sleep(std::time::Duration::from_secs(4));
                    70
                },
                guard,
            )
            .value(),
            700
        );
    });
    std::thread::sleep(std::time::Duration::from_secs(2));
    let guard = &epoch::pin();

    // main thread writes the value first
    assert_eq!(*s.get_or_insert(7, 700, guard).value(), 700);
    handle.join().unwrap();
    assert!(*called.lock().unwrap());
}

#[test]
fn get_next_prev() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    s.insert(3, 3, guard).release(guard);
    s.insert(5, 5, guard).release(guard);
    s.insert(1, 1, guard).release(guard);
    s.insert(4, 4, guard).release(guard);
    s.insert(2, 2, guard).release(guard);

    let mut e = s.get(&3, guard).unwrap();
    assert_eq!(*e.next().unwrap().value(), 4);
    assert_eq!(*e.prev().unwrap().value(), 2);
    assert_eq!(*e.value(), 3);

    e.move_prev();
    assert_eq!(*e.next().unwrap().value(), 3);
    assert_eq!(*e.prev().unwrap().value(), 1);
    assert_eq!(*e.value(), 2);

    e.move_prev();
    assert_eq!(*e.next().unwrap().value(), 2);
    assert!(e.prev().is_none());
    assert_eq!(*e.value(), 1);

    e.move_next();
    e.move_next();
    e.move_next();
    e.move_next();
    assert!(e.next().is_none());
    assert_eq!(*e.prev().unwrap().value(), 4);
    assert_eq!(*e.value(), 5);
}

#[test]
fn front_and_back() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    assert!(s.front(guard).is_none());
    assert!(s.back(guard).is_none());

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10, guard);
    }

    assert_eq!(*s.front(guard).unwrap().key(), 2);
    assert_eq!(*s.back(guard).unwrap().key(), 12);
}

#[test]
fn iter() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10, guard).release(guard);
    }

    assert_eq!(
        s.iter(guard).map(|e| *e.key()).collect::<Vec<_>>(),
        &[2, 4, 5, 7, 8, 11, 12]
    );

    let mut it = s.iter(guard);
    s.remove(&2, guard).unwrap().release(guard);
    assert_eq!(*it.next().unwrap().key(), 4);
    s.remove(&7, guard).unwrap().release(guard);
    assert_eq!(*it.next().unwrap().key(), 5);
    s.remove(&5, guard).unwrap().release(guard);
    assert_eq!(*it.next().unwrap().key(), 8);
    s.remove(&12, guard).unwrap().release(guard);
    assert_eq!(*it.next().unwrap().key(), 11);
    assert!(it.next().is_none());
}

#[test]
fn iter_range() {
    use std::ops::Bound::*;
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    let v = (0..10).map(|x| x * 10).collect::<Vec<_>>();
    for &x in v.iter() {
        s.insert(x, x, guard).release(guard);
    }

    assert_eq!(
        s.iter(guard).map(|x| *x.value()).collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.iter(guard).rev().map(|x| *x.value()).collect::<Vec<_>>(),
        vec![90, 80, 70, 60, 50, 40, 30, 20, 10, 0]
    );
    assert_eq!(
        s.range(.., guard).map(|x| *x.value()).collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );

    assert_eq!(
        s.range((Included(&0), Unbounded), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&0), Unbounded), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Included(&25), Unbounded), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&25), Unbounded), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Included(&70), Unbounded), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&70), Unbounded), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![80, 90]
    );
    assert_eq!(
        s.range((Included(&100), Unbounded), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&100), Unbounded), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Unbounded, Included(&90)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&90)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Unbounded, Included(&25)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&25)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20]
    );
    assert_eq!(
        s.range((Unbounded, Included(&70)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&70)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60]
    );
    assert_eq!(
        s.range((Unbounded, Included(&-1)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&-1)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Included(&25), Included(&80)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Included(&25), Excluded(&80)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70]
    );
    assert_eq!(
        s.range((Excluded(&25), Included(&80)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Excluded(&25), Excluded(&80)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70]
    );

    assert_eq!(
        s.range((Included(&25), Included(&25)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Included(&25), Excluded(&25)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&25), Included(&25)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&25), Excluded(&25)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Included(&50), Included(&50)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![50]
    );
    assert_eq!(
        s.range((Included(&50), Excluded(&50)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&50), Included(&50)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&50), Excluded(&50)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Included(&100), Included(&-2)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Included(&100), Excluded(&-2)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&100), Included(&-2)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&100), Excluded(&-2)), guard)
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
}

#[test]
fn into_iter() {
    let guard = &epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10, guard).release(guard);
    }

    assert_eq!(
        s.into_iter().collect::<Vec<_>>(),
        &[
            (2, 20),
            (4, 40),
            (5, 50),
            (7, 70),
            (8, 80),
            (11, 110),
            (12, 120),
        ]
    );
}

#[test]
fn clear() {
    let guard = &mut epoch::pin();
    let s = SkipList::new(epoch::default_collector().clone());
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10, guard).release(guard);
    }

    assert!(!s.is_empty());
    assert_ne!(s.len(), 0);
    s.clear(guard);
    assert!(s.is_empty());
    assert_eq!(s.len(), 0);
}

#[test]
fn drops() {
    static KEYS: AtomicUsize = AtomicUsize::new(0);
    static VALUES: AtomicUsize = AtomicUsize::new(0);

    let collector = epoch::Collector::new();
    let handle = collector.register();
    {
        let guard = &handle.pin();

        #[derive(Eq, PartialEq, Ord, PartialOrd)]
        struct Key(i32);

        impl Drop for Key {
            fn drop(&mut self) {
                KEYS.fetch_add(1, Ordering::SeqCst);
            }
        }

        struct Value;

        impl Drop for Value {
            fn drop(&mut self) {
                VALUES.fetch_add(1, Ordering::SeqCst);
            }
        }

        let s = SkipList::new(collector.clone());
        for &x in &[4, 2, 12, 8, 7, 11, 5] {
            s.insert(Key(x), Value, guard).release(guard);
        }
        assert_eq!(KEYS.load(Ordering::SeqCst), 0);
        assert_eq!(VALUES.load(Ordering::SeqCst), 0);

        let key7 = Key(7);
        s.remove(&key7, guard).unwrap().release(guard);
        assert_eq!(KEYS.load(Ordering::SeqCst), 0);
        assert_eq!(VALUES.load(Ordering::SeqCst), 0);

        drop(s);
    }

    handle.pin().flush();
    handle.pin().flush();
    assert_eq!(KEYS.load(Ordering::SeqCst), 8);
    assert_eq!(VALUES.load(Ordering::SeqCst), 7);
}
