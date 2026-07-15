use std::{
    borrow::Borrow,
    iter,
    ops::Bound,
    sync::{Arc, Barrier},
};

use crate::skiplist::SkipMap;
use crossbeam_utils::thread;

#[test]
fn smoke() {
    let m = SkipMap::new();
    m.insert(1, 10);
    m.insert(5, 50);
    m.insert(7, 70);
}

#[test]
fn is_empty() {
    let s = SkipMap::new();
    assert!(s.is_empty());

    s.insert(1, 10);
    assert!(!s.is_empty());
    s.insert(2, 20);
    s.insert(3, 30);
    assert!(!s.is_empty());

    s.remove(&2);
    assert!(!s.is_empty());

    s.remove(&1);
    assert!(!s.is_empty());

    s.remove(&3);
    assert!(s.is_empty());
}

#[test]
fn insert() {
    let insert = [0, 4, 2, 12, 8, 7, 11, 5];
    let not_present = [1, 3, 6, 9, 10];
    let s = SkipMap::new();

    for &x in &insert {
        s.insert(x, x * 10);
        assert_eq!(*s.get(&x).unwrap().value(), x * 10);
    }

    for &x in &not_present {
        assert!(s.get(&x).is_none());
    }
}

#[test]
fn compare_and_insert() {
    let insert = [0, 4, 2, 12, 8, 7, 11, 5];
    let not_present = [1, 3, 6, 9, 10];
    let s = SkipMap::new();

    for &x in &insert {
        s.insert(x, x * 10);
    }

    for &x in &insert {
        let value = x * 5;
        let old_value = *s.get(&x).unwrap().value();
        s.compare_insert(x, value, |x| x < &value);
        assert_eq!(*s.get(&x).unwrap().value(), old_value);
    }

    for &x in &insert {
        let value = x * 15;
        s.compare_insert(x, value, |x| x < &value);
        assert_eq!(*s.get(&x).unwrap().value(), value);
    }

    for &x in &not_present {
        assert!(s.get(&x).is_none());
    }
}

#[test]
fn compare_insert_with_absent_key() {
    let insert = [0, 4, 2, 12, 8, 7, 11, 5];
    let s = SkipMap::new();

    // The closure will not be called if the key is not present,
    // so the key-value will be inserted into the map
    for &x in &insert {
        let value = x * 15;
        s.compare_insert(x, value, |_| false);
        assert_eq!(*s.get(&x).unwrap().value(), value);
    }
}

#[test]
fn remove() {
    let insert = [0, 4, 2, 12, 8, 7, 11, 5];
    let not_present = [1, 3, 6, 9, 10];
    let remove = [2, 12, 8];
    let remaining = [0, 4, 5, 7, 11];

    let s = SkipMap::new();

    for &x in &insert {
        s.insert(x, x * 10);
    }
    for x in &not_present {
        assert!(s.remove(x).is_none());
    }
    for x in &remove {
        assert!(s.remove(x).is_some());
    }

    let mut v = vec![];
    let mut e = s.front().unwrap();
    loop {
        v.push(*e.key());
        if !e.move_next() {
            break;
        }
    }

    assert_eq!(v, remaining);
    for x in &insert {
        s.remove(x);
    }
    assert!(s.is_empty());
}

// https://github.com/crossbeam-rs/crossbeam/issues/672
#[test]
fn concurrent_insert() {
    for _ in 0..100 {
        let set: SkipMap<i32, i32> = iter::once((1, 1)).collect();
        let barrier = Barrier::new(2);
        thread::scope(|s| {
            s.spawn(|_| {
                barrier.wait();
                set.insert(1, 1);
            });
            s.spawn(|_| {
                barrier.wait();
                set.insert(1, 1);
            });
        })
        .unwrap();
    }
}

#[test]
fn concurrent_compare_and_insert() {
    let set: SkipMap<i32, i32> = SkipMap::new();
    let set = std::sync::Arc::new(set);
    let len = 100;
    let mut handlers = Vec::with_capacity(len as usize);
    for i in 0..len {
        let set = set.clone();
        let handler = std::thread::spawn(move || {
            set.compare_insert(1, i, |j| j < &i);
        });
        handlers.push(handler);
    }
    for handler in handlers {
        handler.join().unwrap();
    }
    assert_eq!(*set.get(&1).unwrap().value(), len - 1);
}

// https://github.com/crossbeam-rs/crossbeam/issues/672
#[test]
fn concurrent_remove() {
    for _ in 0..100 {
        let set: SkipMap<i32, i32> = iter::once((1, 1)).collect();
        let barrier = Barrier::new(2);
        thread::scope(|s| {
            s.spawn(|_| {
                barrier.wait();
                set.remove(&1);
            });
            s.spawn(|_| {
                barrier.wait();
                set.remove(&1);
            });
        })
        .unwrap();
    }
}

#[test]
fn next_memory_leak() {
    let map: SkipMap<i32, i32> = iter::once((1, 1)).collect();
    let mut iter = map.iter();
    let e = iter.next_back();
    assert!(e.is_some());
    let e = iter.next();
    assert!(e.is_none());
    map.remove(&1);
}

#[test]
fn next_back_memory_leak() {
    let map: SkipMap<i32, i32> = iter::once((1, 1)).collect();
    map.insert(1, 1);
    let mut iter = map.iter();
    let e = iter.next();
    assert!(e.is_some());
    let e = iter.next_back();
    assert!(e.is_none());
    map.remove(&1);
}

#[test]
fn range_next_memory_leak() {
    let map: SkipMap<i32, i32> = iter::once((1, 1)).collect();
    let mut iter = map.range(0..);
    let e = iter.next();
    assert!(e.is_some());
    let e = iter.next_back();
    assert!(e.is_none());
    map.remove(&1);
}

#[test]
fn entry() {
    let s = SkipMap::new();

    assert!(s.front().is_none());
    assert!(s.back().is_none());

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10);
    }

    let mut e = s.front().unwrap();
    assert_eq!(*e.key(), 2);
    assert!(!e.move_prev());
    assert!(e.move_next());
    assert_eq!(*e.key(), 4);

    e = s.back().unwrap();
    assert_eq!(*e.key(), 12);
    assert!(!e.move_next());
    assert!(e.move_prev());
    assert_eq!(*e.key(), 11);
}

#[test]
fn ordered_iter() {
    let s: SkipMap<i32, i32> = SkipMap::new();
    s.insert(1, 1);

    let mut iter = s.iter();
    assert!(iter.next().is_some());
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());

    s.insert(2, 2);
    assert!(iter.next().is_some());
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());

    s.insert(3, 3);
    s.insert(4, 4);
    s.insert(5, 5);
    assert_eq!(*iter.next_back().unwrap().key(), 5);
    assert_eq!(*iter.next().unwrap().key(), 3);
    assert_eq!(*iter.next_back().unwrap().key(), 4);
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
}

#[test]
fn ordered_range() {
    let s: SkipMap<i32, i32> = SkipMap::new();
    s.insert(1, 1);

    let mut iter = s.range(0..);
    assert!(iter.next().is_some());
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());

    s.insert(2, 2);
    assert!(iter.next().is_some());
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());

    s.insert(3, 3);
    s.insert(4, 4);
    s.insert(5, 5);
    assert_eq!(*iter.next_back().unwrap().key(), 5);
    assert_eq!(*iter.next().unwrap().key(), 3);
    assert_eq!(*iter.next_back().unwrap().key(), 4);
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
}
#[test]
fn entry_remove() {
    let s = SkipMap::new();

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10);
    }

    let mut e = s.get(&7).unwrap();
    assert!(!e.is_removed());
    assert!(e.remove());
    assert!(e.is_removed());

    e.move_prev();
    e.move_next();
    assert_ne!(*e.key(), 7);

    for e in s.iter() {
        assert!(!s.is_empty());
        assert_ne!(s.len(), 0);
        e.remove();
    }
    assert!(s.is_empty());
    assert_eq!(s.len(), 0);
}

#[test]
fn entry_reposition() {
    let s = SkipMap::new();

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10);
    }

    let mut e = s.get(&7).unwrap();
    assert!(!e.is_removed());
    assert!(e.remove());
    assert!(e.is_removed());

    s.insert(7, 700);
    e.move_prev();
    e.move_next();
    assert_eq!(*e.key(), 7);
}

#[test]
fn len() {
    let s = SkipMap::new();
    assert_eq!(s.len(), 0);

    for (i, &x) in [4, 2, 12, 8, 7, 11, 5].iter().enumerate() {
        s.insert(x, x * 10);
        assert_eq!(s.len(), i + 1);
    }

    s.insert(5, 0);
    assert_eq!(s.len(), 7);
    s.insert(5, 0);
    assert_eq!(s.len(), 7);

    s.remove(&6);
    assert_eq!(s.len(), 7);
    s.remove(&5);
    assert_eq!(s.len(), 6);
    s.remove(&12);
    assert_eq!(s.len(), 5);
}

#[test]
fn insert_and_remove() {
    let s = SkipMap::new();
    let keys = || s.iter().map(|e| *e.key()).collect::<Vec<_>>();

    s.insert(3, 0);
    s.insert(5, 0);
    s.insert(1, 0);
    s.insert(4, 0);
    s.insert(2, 0);
    assert_eq!(keys(), [1, 2, 3, 4, 5]);

    assert!(s.remove(&4).is_some());
    assert_eq!(keys(), [1, 2, 3, 5]);
    assert!(s.remove(&3).is_some());
    assert_eq!(keys(), [1, 2, 5]);
    assert!(s.remove(&1).is_some());
    assert_eq!(keys(), [2, 5]);

    assert!(s.remove(&1).is_none());
    assert_eq!(keys(), [2, 5]);
    assert!(s.remove(&3).is_none());
    assert_eq!(keys(), [2, 5]);

    assert!(s.remove(&2).is_some());
    assert_eq!(keys(), [5]);
    assert!(s.remove(&5).is_some());
    assert!(keys().is_empty());

    s.insert(3, 0);
    assert_eq!(keys(), [3]);
    s.insert(1, 0);
    assert_eq!(keys(), [1, 3]);
    s.insert(3, 0);
    assert_eq!(keys(), [1, 3]);
    s.insert(5, 0);
    assert_eq!(keys(), [1, 3, 5]);

    assert!(s.remove(&3).is_some());
    assert_eq!(keys(), [1, 5]);
    assert!(s.remove(&1).is_some());
    assert_eq!(keys(), [5]);
    assert!(s.remove(&3).is_none());
    assert_eq!(keys(), [5]);
    assert!(s.remove(&5).is_some());
    assert!(keys().is_empty());
}

#[test]
fn get() {
    let s = SkipMap::new();
    s.insert(30, 3);
    s.insert(50, 5);
    s.insert(10, 1);
    s.insert(40, 4);
    s.insert(20, 2);

    assert_eq!(*s.get(&10).unwrap().value(), 1);
    assert_eq!(*s.get(&20).unwrap().value(), 2);
    assert_eq!(*s.get(&30).unwrap().value(), 3);
    assert_eq!(*s.get(&40).unwrap().value(), 4);
    assert_eq!(*s.get(&50).unwrap().value(), 5);

    assert!(s.get(&7).is_none());
    assert!(s.get(&27).is_none());
    assert!(s.get(&31).is_none());
    assert!(s.get(&97).is_none());
}

#[test]
fn lower_bound() {
    let s = SkipMap::new();
    s.insert(30, 3);
    s.insert(50, 5);
    s.insert(10, 1);
    s.insert(40, 4);
    s.insert(20, 2);

    assert_eq!(*s.lower_bound(Bound::Unbounded).unwrap().value(), 1);

    assert_eq!(*s.lower_bound(Bound::Included(&10)).unwrap().value(), 1);
    assert_eq!(*s.lower_bound(Bound::Included(&20)).unwrap().value(), 2);
    assert_eq!(*s.lower_bound(Bound::Included(&30)).unwrap().value(), 3);
    assert_eq!(*s.lower_bound(Bound::Included(&40)).unwrap().value(), 4);
    assert_eq!(*s.lower_bound(Bound::Included(&50)).unwrap().value(), 5);

    assert_eq!(*s.lower_bound(Bound::Included(&7)).unwrap().value(), 1);
    assert_eq!(*s.lower_bound(Bound::Included(&27)).unwrap().value(), 3);
    assert_eq!(*s.lower_bound(Bound::Included(&31)).unwrap().value(), 4);
    assert!(s.lower_bound(Bound::Included(&97)).is_none());

    assert_eq!(*s.lower_bound(Bound::Excluded(&10)).unwrap().value(), 2);
    assert_eq!(*s.lower_bound(Bound::Excluded(&20)).unwrap().value(), 3);
    assert_eq!(*s.lower_bound(Bound::Excluded(&30)).unwrap().value(), 4);
    assert_eq!(*s.lower_bound(Bound::Excluded(&40)).unwrap().value(), 5);
    assert!(s.lower_bound(Bound::Excluded(&50)).is_none());

    assert_eq!(*s.lower_bound(Bound::Excluded(&7)).unwrap().value(), 1);
    assert_eq!(*s.lower_bound(Bound::Excluded(&27)).unwrap().value(), 3);
    assert_eq!(*s.lower_bound(Bound::Excluded(&31)).unwrap().value(), 4);
    assert!(s.lower_bound(Bound::Excluded(&97)).is_none());
}

#[test]
fn upper_bound() {
    let s = SkipMap::new();
    s.insert(30, 3);
    s.insert(50, 5);
    s.insert(10, 1);
    s.insert(40, 4);
    s.insert(20, 2);

    assert_eq!(*s.upper_bound(Bound::Unbounded).unwrap().value(), 5);

    assert_eq!(*s.upper_bound(Bound::Included(&10)).unwrap().value(), 1);
    assert_eq!(*s.upper_bound(Bound::Included(&20)).unwrap().value(), 2);
    assert_eq!(*s.upper_bound(Bound::Included(&30)).unwrap().value(), 3);
    assert_eq!(*s.upper_bound(Bound::Included(&40)).unwrap().value(), 4);
    assert_eq!(*s.upper_bound(Bound::Included(&50)).unwrap().value(), 5);

    assert!(s.upper_bound(Bound::Included(&7)).is_none());
    assert_eq!(*s.upper_bound(Bound::Included(&27)).unwrap().value(), 2);
    assert_eq!(*s.upper_bound(Bound::Included(&31)).unwrap().value(), 3);
    assert_eq!(*s.upper_bound(Bound::Included(&97)).unwrap().value(), 5);

    assert!(s.upper_bound(Bound::Excluded(&10)).is_none());
    assert_eq!(*s.upper_bound(Bound::Excluded(&20)).unwrap().value(), 1);
    assert_eq!(*s.upper_bound(Bound::Excluded(&30)).unwrap().value(), 2);
    assert_eq!(*s.upper_bound(Bound::Excluded(&40)).unwrap().value(), 3);
    assert_eq!(*s.upper_bound(Bound::Excluded(&50)).unwrap().value(), 4);

    assert!(s.upper_bound(Bound::Excluded(&7)).is_none());
    assert_eq!(*s.upper_bound(Bound::Excluded(&27)).unwrap().value(), 2);
    assert_eq!(*s.upper_bound(Bound::Excluded(&31)).unwrap().value(), 3);
    assert_eq!(*s.upper_bound(Bound::Excluded(&97)).unwrap().value(), 5);
}

#[test]
fn get_or_insert() {
    let s = SkipMap::new();
    s.insert(3, 3);
    s.insert(5, 5);
    s.insert(1, 1);
    s.insert(4, 4);
    s.insert(2, 2);

    assert_eq!(*s.get(&4).unwrap().value(), 4);
    assert_eq!(*s.insert(4, 40).value(), 40);
    assert_eq!(*s.get(&4).unwrap().value(), 40);

    assert_eq!(*s.get_or_insert(4, 400).value(), 40);
    assert_eq!(*s.get(&4).unwrap().value(), 40);
    assert_eq!(*s.get_or_insert(6, 600).value(), 600);
}

#[test]
fn get_or_insert_with() {
    let s = SkipMap::new();
    s.insert(3, 3);
    s.insert(5, 5);
    s.insert(1, 1);
    s.insert(4, 4);
    s.insert(2, 2);

    assert_eq!(*s.get(&4).unwrap().value(), 4);
    assert_eq!(*s.insert(4, 40).value(), 40);
    assert_eq!(*s.get(&4).unwrap().value(), 40);

    assert_eq!(*s.get_or_insert_with(4, || 400).value(), 40);
    assert_eq!(*s.get(&4).unwrap().value(), 40);
    assert_eq!(*s.get_or_insert_with(6, || 600).value(), 600);
}

#[test]
fn get_or_insert_with_panic() {
    use std::panic;

    let s = SkipMap::new();
    let res = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        s.get_or_insert_with(4, || panic!());
    }));
    assert!(res.is_err());
    assert!(s.is_empty());
    assert_eq!(*s.get_or_insert_with(4, || 40).value(), 40);
    assert_eq!(s.len(), 1);
}

#[test]
fn get_or_insert_with_parallel_run() {
    use std::sync::{Arc, Mutex};

    let s = Arc::new(SkipMap::new());
    let s2 = s.clone();
    let called = Arc::new(Mutex::new(false));
    let called2 = called.clone();
    let handle = std::thread::spawn(move || {
        assert_eq!(
            *s2.get_or_insert_with(7, || {
                *called2.lock().unwrap() = true;

                // allow main thread to run before we return result
                std::thread::sleep(std::time::Duration::from_secs(4));
                70
            })
            .value(),
            700
        );
    });
    std::thread::sleep(std::time::Duration::from_secs(2));

    // main thread writes the value first
    assert_eq!(*s.get_or_insert(7, 700).value(), 700);
    handle.join().unwrap();
    assert!(*called.lock().unwrap());
}

#[test]
fn get_next_prev() {
    let s = SkipMap::new();
    s.insert(3, 3);
    s.insert(5, 5);
    s.insert(1, 1);
    s.insert(4, 4);
    s.insert(2, 2);

    let mut e = s.get(&3).unwrap();
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
    let s = SkipMap::new();
    assert!(s.front().is_none());
    assert!(s.back().is_none());

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10);
    }

    assert_eq!(*s.front().unwrap().key(), 2);
    assert_eq!(*s.back().unwrap().key(), 12);
}

#[test]
fn iter() {
    let s = SkipMap::new();
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10);
    }

    assert_eq!(
        s.iter().map(|e| *e.key()).collect::<Vec<_>>(),
        &[2, 4, 5, 7, 8, 11, 12]
    );

    let mut it = s.iter();
    s.remove(&2);
    assert_eq!(*it.next().unwrap().key(), 4);
    s.remove(&7);
    assert_eq!(*it.next().unwrap().key(), 5);
    s.remove(&5);
    assert_eq!(*it.next().unwrap().key(), 8);
    s.remove(&12);
    assert_eq!(*it.next().unwrap().key(), 11);
    assert!(it.next().is_none());
}

#[test]
fn iter_range() {
    use std::ops::Bound::*;
    let s = SkipMap::new();
    let v = (0..10).map(|x| x * 10).collect::<Vec<_>>();
    for &x in v.iter() {
        s.insert(x, x);
    }

    assert_eq!(
        s.iter().map(|x| *x.value()).collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.iter().rev().map(|x| *x.value()).collect::<Vec<_>>(),
        vec![90, 80, 70, 60, 50, 40, 30, 20, 10, 0]
    );
    assert_eq!(
        s.range(..).map(|x| *x.value()).collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );

    assert_eq!(
        s.range((Included(&0), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Included(&0), Included(&60)))
            .rev()
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![60, 50, 40, 30, 20, 10, 0]
    );
    assert_eq!(
        s.range((Excluded(&0), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Included(&25), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&25), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Included(&70), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&70), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![80, 90]
    );
    assert_eq!(
        s.range((Included(&100), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&100), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Unbounded, Included(&90)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&90)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Unbounded, Included(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20]
    );
    assert_eq!(
        s.range((Unbounded, Included(&70)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&70)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60]
    );
    assert_eq!(
        s.range((Unbounded, Included(&-1)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&-1)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Included(&25), Included(&80)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Included(&25), Excluded(&80)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70]
    );
    assert_eq!(
        s.range((Excluded(&25), Included(&80)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Excluded(&25), Excluded(&80)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70]
    );

    assert_eq!(
        s.range((Included(&25), Included(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Included(&25), Excluded(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&25), Included(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&25), Excluded(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Included(&50), Included(&50)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![50]
    );
    assert_eq!(
        s.range((Included(&50), Excluded(&50)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&50), Included(&50)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&50), Excluded(&50)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Included(&100), Included(&-2)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Included(&100), Excluded(&-2)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&100), Included(&-2)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&100), Excluded(&-2)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
}

// https://github.com/crossbeam-rs/crossbeam/issues/671
#[test]
fn iter_range2() {
    let set: SkipMap<_, _> = [1, 3, 5].iter().map(|x| (*x, *x)).collect();
    assert_eq!(set.range(2..4).count(), 1);
    set.insert(3, 3);
}

#[test]
fn into_iter() {
    let s = SkipMap::new();
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10);
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
    let s = SkipMap::new();
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10);
    }

    assert!(!s.is_empty());
    assert_ne!(s.len(), 0);
    s.clear();
    assert!(s.is_empty());
    assert_eq!(s.len(), 0);
}

// https://github.com/crossbeam-rs/crossbeam/issues/1023
#[test]
fn concurrent_insert_get_same_key() {
    let map: Arc<SkipMap<u32, ()>> = Arc::new(SkipMap::new());
    let len = if cfg!(miri) { 100 } else { 10_000 };
    let key = 0;
    map.insert(0, ());

    let getter = map.clone();
    let handle = std::thread::spawn(move || {
        for _ in 0..len {
            map.insert(0, ());
        }
    });
    for _ in 0..len {
        assert!(getter.get(&key).is_some());
    }
    handle.join().unwrap()
}

// https://github.com/crossbeam-rs/crossbeam/issues/1178
#[test]
fn issue_1178() {
    let map = SkipMap::new();
    let _ = map.insert(vec![1u8], vec![1u8]);
    let _ = map.insert(vec![2], vec![2]);
    let _ = map.insert(vec![3], vec![3]);

    let kvs: Vec<(Vec<u8>, Vec<u8>)> = map
        .range(vec![1]..vec![3])
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();

    for (k, v) in kvs {
        map.insert(k, v);
    }
}

#[test]
fn comparator() {
    use std::cmp::Ordering;

    use crate::skiplist::comparator::{Comparator, Equivalator};

    #[derive(Debug)]
    struct CaseComparator {
        case_sensitive: bool,
    }

    impl<L: Borrow<str> + ?Sized, R: Borrow<str> + ?Sized> Equivalator<L, R> for CaseComparator {
        fn equivalent(&self, lhs: &L, rhs: &R) -> bool {
            if self.case_sensitive {
                lhs.borrow() == rhs.borrow()
            } else {
                lhs.borrow()
                    .chars()
                    .flat_map(|c| c.to_lowercase())
                    .eq(rhs.borrow().chars().flat_map(|c| c.to_lowercase()))
            }
        }
    }

    impl<L: Borrow<str> + ?Sized, R: Borrow<str> + ?Sized> Comparator<L, R> for CaseComparator {
        fn compare(&self, lhs: &L, rhs: &R) -> Ordering {
            if self.case_sensitive {
                lhs.borrow().cmp(rhs.borrow())
            } else {
                lhs.borrow()
                    .chars()
                    .flat_map(|c| c.to_lowercase())
                    .cmp(rhs.borrow().chars().flat_map(|c| c.to_lowercase()))
            }
        }
    }

    let s: SkipMap<String, u32, _> = SkipMap::with_comparator(CaseComparator {
        case_sensitive: true,
    });
    s.insert("abc".to_owned(), 1);
    s.insert("ABC".to_owned(), 2);
    assert_eq!(s.len(), 2);
    assert_eq!(*s.get("abc").unwrap().value(), 1);
    assert_eq!(*s.get("ABC").unwrap().value(), 2);
    assert!(!s.contains_key("aBc"));
    s.remove("abc");
    assert!(!s.contains_key("abc"));
    assert!(s.contains_key("ABC"));

    let s: SkipMap<String, u32, _> = SkipMap::with_comparator(CaseComparator {
        case_sensitive: false,
    });
    s.insert("abc".to_owned(), 1);
    s.insert("ABC".to_owned(), 2);
    assert_eq!(s.len(), 1);
    assert_eq!(*s.get("abc").unwrap().value(), 2);
    assert_eq!(*s.get("ABC").unwrap().value(), 2);
    assert_eq!(*s.get("aBc").unwrap().value(), 2);
    assert_eq!(s.remove("AbC").unwrap().key(), "ABC");
    assert!(s.is_empty());
}

/// An allocator that fails every allocation while its flag is set.
///
/// Delegates to [`TursoAllocator`] otherwise, so nodes are still backed by the
/// regular Turso heap.
#[derive(Clone, Default)]
pub(crate) struct FailOnDemandAlloc {
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl FailOnDemandAlloc {
    pub(crate) fn fail_allocations(&self, fail: bool) {
        self.fail.store(fail, std::sync::atomic::Ordering::Relaxed);
    }
}

unsafe impl crate::alloc::ApiAllocator for FailOnDemandAlloc {
    fn allocate(
        &self,
        layout: crate::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, crate::alloc::AllocError> {
        if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::alloc::AllocError);
        }
        crate::alloc::TursoAllocator.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: crate::alloc::Layout) {
        unsafe { crate::alloc::TursoAllocator.deallocate(ptr, layout) }
    }
}

#[test]
fn try_insert_surfaces_allocation_failure() {
    let alloc = FailOnDemandAlloc::default();
    let map: SkipMap<i32, String, _, _> = SkipMap::new_in(alloc.clone());
    map.try_insert(1, "one".to_string()).unwrap();
    assert_eq!(map.len(), 1);

    alloc.fail_allocations(true);

    // Inserting a new key needs a node allocation, which now fails. The map
    // must be left unchanged.
    assert!(map.try_insert(2, "two".to_string()).is_err());
    assert!(map
        .try_compare_insert(2, "two".to_string(), |_| true)
        .is_err());
    assert_eq!(map.len(), 1);
    assert!(map.get(&2).is_none());

    // Finding an existing key takes the no-allocation fast path and still
    // succeeds while allocations fail.
    let entry = map.try_get_or_insert(1, "uno".to_string()).unwrap();
    assert_eq!(*entry.value(), "one");
    let entry = map.try_get_or_insert_with(1, || "uno".to_string()).unwrap();
    assert_eq!(*entry.value(), "one");

    alloc.fail_allocations(false);

    // The map stays fully usable after a failed insert.
    map.try_insert(2, "two".to_string()).unwrap();
    assert_eq!(map.len(), 2);
    assert_eq!(*map.get(&2).unwrap().value(), "two");
}

#[test]
fn try_insert_failure_drops_value_exactly_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountsDrops(Arc<AtomicUsize>);
    impl Drop for CountsDrops {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let alloc = FailOnDemandAlloc::default();
    let map: SkipMap<i32, CountsDrops, _, _> = SkipMap::new_in(alloc.clone());

    // Failed insert into an empty map: the value is dropped exactly once.
    alloc.fail_allocations(true);
    assert!(map.try_insert(1, CountsDrops(drops.clone())).is_err());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert!(map.is_empty());

    // Successful insert; the stored value must not be dropped.
    alloc.fail_allocations(false);
    map.try_insert(1, CountsDrops(drops.clone())).unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 1);

    // Failed insert over an existing key (replace path): only the new value is
    // dropped, the stored entry stays untouched.
    alloc.fail_allocations(true);
    assert!(map.try_insert(1, CountsDrops(drops.clone())).is_err());
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    assert_eq!(map.len(), 1);

    // Dropping the map finalizes the stored value exactly once.
    drop(map);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
}

#[test]
fn try_get_or_insert_family_fails_on_missing_key() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let alloc = FailOnDemandAlloc::default();
    let map: SkipMap<i32, i32, _, _> = SkipMap::new_in(alloc.clone());

    alloc.fail_allocations(true);
    // A missing key forces a node allocation in every fallible variant.
    assert!(map.try_get_or_insert(1, 10).is_err());
    let calls = AtomicUsize::new(0);
    assert!(map
        .try_get_or_insert_with(1, || {
            calls.fetch_add(1, Ordering::Relaxed);
            10
        })
        .is_err());
    // The value closure runs before the allocation is attempted; the value it
    // built is dropped on failure.
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(map.try_compare_insert(1, 10, |_| false).is_err());
    assert!(map.is_empty());

    alloc.fail_allocations(false);
    map.try_get_or_insert(1, 10).unwrap();
    assert_eq!(map.len(), 1);

    alloc.fail_allocations(true);
    // Existing key: the fast path returns the entry without allocating, and
    // the value closure is never invoked.
    assert_eq!(*map.try_get_or_insert(1, 20).unwrap().value(), 10);
    assert_eq!(
        *map.try_get_or_insert_with(1, || {
            calls.fetch_add(1, Ordering::Relaxed);
            20
        })
        .unwrap()
        .value(),
        10
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    // A false predicate keeps the existing entry without allocating; a true
    // predicate must allocate a replacement node and fails.
    assert_eq!(
        *map.try_compare_insert(1, 20, |_| false).unwrap().value(),
        10
    );
    assert!(map.try_compare_insert(1, 20, |_| true).is_err());
    assert_eq!(*map.get(&1).unwrap().value(), 10);
}

#[test]
fn insert_reserved_performs_no_allocation() {
    let alloc = FailOnDemandAlloc::default();
    let map: SkipMap<i32, String, _, _> = SkipMap::new_in(alloc.clone());
    let reservation = map.try_reserve_node().unwrap();

    alloc.fail_allocations(true);

    // Reserving needs an allocation and fails while faults are on...
    assert!(map.try_reserve_node().is_err());

    // ...but consuming an existing reservation must succeed without allocating.
    let entry = map.insert_reserved(1, "one".to_string(), reservation);
    assert_eq!(*entry.value(), "one");
    assert_eq!(map.len(), 1);
    assert_eq!(*map.get(&1).unwrap().value(), "one");
}

#[test]
fn insert_reserved_replaces_existing_entry() {
    let map: SkipMap<i32, String> = SkipMap::new();
    map.insert(1, "one".to_string());

    // Same replace semantics as `insert`.
    let reservation = map.try_reserve_node().unwrap();
    let entry = map.insert_reserved(1, "uno".to_string(), reservation);
    assert_eq!(*entry.value(), "uno");
    assert_eq!(map.len(), 1);
    assert_eq!(*map.get(&1).unwrap().value(), "uno");
}

#[test]
fn unused_reservation_drop_is_dealloc_only() {
    let alloc = FailOnDemandAlloc::default();
    let map: SkipMap<i32, String, _, _> = SkipMap::new_in(alloc.clone());
    let reservation = map.try_reserve_node().unwrap();
    let outlives_map = map.try_reserve_node().unwrap();

    // Dropping an unused reservation only deallocates; it must work while
    // allocations fail and must leave the map untouched.
    alloc.fail_allocations(true);
    drop(reservation);
    assert!(map.is_empty());

    // A reservation may even outlive its map: it carries its own allocator
    // clone for the deallocation.
    drop(map);
    drop(outlives_map);
}

#[test]
fn insert_reserved_drops_values_exactly_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountsDrops(Arc<AtomicUsize>);
    impl Drop for CountsDrops {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let map: SkipMap<i32, CountsDrops> = SkipMap::new();

    // Reserved insert of a fresh key: the stored value is not dropped.
    let reservation = map.try_reserve_node().unwrap();
    map.insert_reserved(1, CountsDrops(drops.clone()), reservation);
    assert_eq!(drops.load(Ordering::Relaxed), 0);

    // Reserved insert over an existing key: the replaced value is destroyed
    // (via the epoch collector), the new value stays stored.
    let reservation = map.try_reserve_node().unwrap();
    map.insert_reserved(1, CountsDrops(drops.clone()), reservation);
    assert_eq!(map.len(), 1);

    // Dropping the map finalizes the stored value; flush the collector until
    // the deferred destruction of the replaced value has run too. The total
    // must be exactly 2 — anything more is a double drop.
    drop(map);
    for _ in 0..128 {
        if drops.load(Ordering::Relaxed) == 2 {
            break;
        }
        crossbeam_epoch::pin().flush();
    }
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
#[should_panic(expected = "different skip list")]
fn insert_reserved_rejects_foreign_reservation() {
    let map_a: SkipMap<i32, i32> = SkipMap::new();
    let map_b: SkipMap<i32, i32> = SkipMap::new();
    let reservation = map_a.try_reserve_node().unwrap();
    map_b.insert_reserved(1, 10, reservation);
}
