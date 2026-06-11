use std::{
    iter,
    ops::Bound,
    sync::{Arc, Barrier},
};

use crate::skiplist::SkipSet;
use crossbeam_utils::thread;

#[test]
fn smoke() {
    let m = SkipSet::new();
    m.insert(1);
    m.insert(5);
    m.insert(7);
}

#[test]
fn is_empty() {
    let s = SkipSet::new();
    assert!(s.is_empty());

    s.insert(1);
    assert!(!s.is_empty());
    s.insert(2);
    s.insert(3);
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
    let s = SkipSet::new();

    for &x in &insert {
        s.insert(x);
        assert_eq!(*s.get(&x).unwrap(), x);
    }

    for &x in &not_present {
        assert!(s.get(&x).is_none());
    }
}

#[test]
fn remove() {
    let insert = [0, 4, 2, 12, 8, 7, 11, 5];
    let not_present = [1, 3, 6, 9, 10];
    let remove = [2, 12, 8];
    let remaining = [0, 4, 5, 7, 11];

    let s = SkipSet::new();

    for &x in &insert {
        s.insert(x);
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
        v.push(*e);
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
        let set: SkipSet<i32> = iter::once(1).collect();
        let barrier = Barrier::new(2);
        thread::scope(|s| {
            s.spawn(|_| {
                barrier.wait();
                set.insert(1);
            });
            s.spawn(|_| {
                barrier.wait();
                set.insert(1);
            });
        })
        .unwrap();
    }
}

// https://github.com/crossbeam-rs/crossbeam/issues/672
#[test]
fn concurrent_remove() {
    for _ in 0..100 {
        let set: SkipSet<i32> = iter::once(1).collect();
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
fn entry() {
    let s = SkipSet::new();

    assert!(s.front().is_none());
    assert!(s.back().is_none());

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x);
    }

    let mut e = s.front().unwrap();
    assert_eq!(*e, 2);
    assert!(!e.move_prev());
    assert!(e.move_next());
    assert_eq!(*e, 4);

    e = s.back().unwrap();
    assert_eq!(*e, 12);
    assert!(!e.move_next());
    assert!(e.move_prev());
    assert_eq!(*e, 11);
}

#[test]
fn entry_remove() {
    let s = SkipSet::new();

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x);
    }

    let mut e = s.get(&7).unwrap();
    assert!(!e.is_removed());
    assert!(e.remove());
    assert!(e.is_removed());

    e.move_prev();
    e.move_next();
    assert_ne!(*e, 7);

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
    let s = SkipSet::new();

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x);
    }

    let mut e = s.get(&7).unwrap();
    assert!(!e.is_removed());
    assert!(e.remove());
    assert!(e.is_removed());

    s.insert(7);
    e.move_prev();
    e.move_next();
    assert_eq!(*e, 7);
}

#[test]
fn len() {
    let s = SkipSet::new();
    assert_eq!(s.len(), 0);

    for (i, &x) in [4, 2, 12, 8, 7, 11, 5].iter().enumerate() {
        s.insert(x);
        assert_eq!(s.len(), i + 1);
    }

    s.insert(5);
    assert_eq!(s.len(), 7);
    s.insert(5);
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
    let s = SkipSet::new();
    let keys = || s.iter().map(|e| *e).collect::<Vec<_>>();

    s.insert(3);
    s.insert(5);
    s.insert(1);
    s.insert(4);
    s.insert(2);
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

    s.insert(3);
    assert_eq!(keys(), [3]);
    s.insert(1);
    assert_eq!(keys(), [1, 3]);
    s.insert(3);
    assert_eq!(keys(), [1, 3]);
    s.insert(5);
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
    let s = SkipSet::new();
    s.insert(30);
    s.insert(50);
    s.insert(10);
    s.insert(40);
    s.insert(20);

    assert_eq!(*s.get(&10).unwrap(), 10);
    assert_eq!(*s.get(&20).unwrap(), 20);
    assert_eq!(*s.get(&30).unwrap(), 30);
    assert_eq!(*s.get(&40).unwrap(), 40);
    assert_eq!(*s.get(&50).unwrap(), 50);

    assert!(s.get(&7).is_none());
    assert!(s.get(&27).is_none());
    assert!(s.get(&31).is_none());
    assert!(s.get(&97).is_none());
}

#[test]
fn lower_bound() {
    let s = SkipSet::new();
    s.insert(30);
    s.insert(50);
    s.insert(10);
    s.insert(40);
    s.insert(20);

    assert_eq!(*s.lower_bound(Bound::Unbounded).unwrap(), 10);

    assert_eq!(*s.lower_bound(Bound::Included(&10)).unwrap(), 10);
    assert_eq!(*s.lower_bound(Bound::Included(&20)).unwrap(), 20);
    assert_eq!(*s.lower_bound(Bound::Included(&30)).unwrap(), 30);
    assert_eq!(*s.lower_bound(Bound::Included(&40)).unwrap(), 40);
    assert_eq!(*s.lower_bound(Bound::Included(&50)).unwrap(), 50);

    assert_eq!(*s.lower_bound(Bound::Included(&7)).unwrap(), 10);
    assert_eq!(*s.lower_bound(Bound::Included(&27)).unwrap(), 30);
    assert_eq!(*s.lower_bound(Bound::Included(&31)).unwrap(), 40);
    assert!(s.lower_bound(Bound::Included(&97)).is_none());

    assert_eq!(*s.lower_bound(Bound::Excluded(&10)).unwrap(), 20);
    assert_eq!(*s.lower_bound(Bound::Excluded(&20)).unwrap(), 30);
    assert_eq!(*s.lower_bound(Bound::Excluded(&30)).unwrap(), 40);
    assert_eq!(*s.lower_bound(Bound::Excluded(&40)).unwrap(), 50);
    assert!(s.lower_bound(Bound::Excluded(&50)).is_none());

    assert_eq!(*s.lower_bound(Bound::Excluded(&7)).unwrap(), 10);
    assert_eq!(*s.lower_bound(Bound::Excluded(&27)).unwrap(), 30);
    assert_eq!(*s.lower_bound(Bound::Excluded(&31)).unwrap(), 40);
    assert!(s.lower_bound(Bound::Excluded(&97)).is_none());
}

#[test]
fn upper_bound() {
    let s = SkipSet::new();
    s.insert(30);
    s.insert(50);
    s.insert(10);
    s.insert(40);
    s.insert(20);

    assert_eq!(*s.upper_bound(Bound::Unbounded).unwrap(), 50);

    assert_eq!(*s.upper_bound(Bound::Included(&10)).unwrap(), 10);
    assert_eq!(*s.upper_bound(Bound::Included(&20)).unwrap(), 20);
    assert_eq!(*s.upper_bound(Bound::Included(&30)).unwrap(), 30);
    assert_eq!(*s.upper_bound(Bound::Included(&40)).unwrap(), 40);
    assert_eq!(*s.upper_bound(Bound::Included(&50)).unwrap(), 50);

    assert!(s.upper_bound(Bound::Included(&7)).is_none());
    assert_eq!(*s.upper_bound(Bound::Included(&27)).unwrap(), 20);
    assert_eq!(*s.upper_bound(Bound::Included(&31)).unwrap(), 30);
    assert_eq!(*s.upper_bound(Bound::Included(&97)).unwrap(), 50);

    assert!(s.upper_bound(Bound::Excluded(&10)).is_none());
    assert_eq!(*s.upper_bound(Bound::Excluded(&20)).unwrap(), 10);
    assert_eq!(*s.upper_bound(Bound::Excluded(&30)).unwrap(), 20);
    assert_eq!(*s.upper_bound(Bound::Excluded(&40)).unwrap(), 30);
    assert_eq!(*s.upper_bound(Bound::Excluded(&50)).unwrap(), 40);

    assert!(s.upper_bound(Bound::Excluded(&7)).is_none());
    assert_eq!(*s.upper_bound(Bound::Excluded(&27)).unwrap(), 20);
    assert_eq!(*s.upper_bound(Bound::Excluded(&31)).unwrap(), 30);
    assert_eq!(*s.upper_bound(Bound::Excluded(&97)).unwrap(), 50);
}

#[test]
fn get_or_insert() {
    let s = SkipSet::new();
    s.insert(3);
    s.insert(5);
    s.insert(1);
    s.insert(4);
    s.insert(2);

    assert_eq!(*s.get(&4).unwrap(), 4);
    assert_eq!(*s.insert(4), 4);
    assert_eq!(*s.get(&4).unwrap(), 4);

    assert_eq!(*s.get_or_insert(4), 4);
    assert_eq!(*s.get(&4).unwrap(), 4);
    assert_eq!(*s.get_or_insert(6), 6);
}

#[test]
fn get_next_prev() {
    let s = SkipSet::new();
    s.insert(3);
    s.insert(5);
    s.insert(1);
    s.insert(4);
    s.insert(2);

    let mut e = s.get(&3).unwrap();
    assert_eq!(*e.next().unwrap(), 4);
    assert_eq!(*e.prev().unwrap(), 2);
    assert_eq!(*e, 3);

    e.move_prev();
    assert_eq!(*e.next().unwrap(), 3);
    assert_eq!(*e.prev().unwrap(), 1);
    assert_eq!(*e, 2);

    e.move_prev();
    assert_eq!(*e.next().unwrap(), 2);
    assert!(e.prev().is_none());
    assert_eq!(*e, 1);

    e.move_next();
    e.move_next();
    e.move_next();
    e.move_next();
    assert!(e.next().is_none());
    assert_eq!(*e.prev().unwrap(), 4);
    assert_eq!(*e, 5);
}

#[test]
fn front_and_back() {
    let s = SkipSet::new();
    assert!(s.front().is_none());
    assert!(s.back().is_none());

    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x);
    }

    assert_eq!(*s.front().unwrap(), 2);
    assert_eq!(*s.back().unwrap(), 12);
}

#[test]
fn iter() {
    let s = SkipSet::new();
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x);
    }

    assert_eq!(
        s.iter().map(|e| *e).collect::<Vec<_>>(),
        &[2, 4, 5, 7, 8, 11, 12]
    );

    let mut it = s.iter();
    s.remove(&2);
    assert_eq!(*it.next().unwrap(), 4);
    s.remove(&7);
    assert_eq!(*it.next().unwrap(), 5);
    s.remove(&5);
    assert_eq!(*it.next().unwrap(), 8);
    s.remove(&12);
    assert_eq!(*it.next().unwrap(), 11);
    assert!(it.next().is_none());
}

#[test]
fn iter_range() {
    use std::ops::Bound::*;
    let s = SkipSet::new();
    let v = (0..10).map(|x| x * 10).collect::<Vec<_>>();
    for &x in v.iter() {
        s.insert(x);
    }

    assert_eq!(
        s.iter().map(|x| *x).collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.iter().rev().map(|x| *x).collect::<Vec<_>>(),
        vec![90, 80, 70, 60, 50, 40, 30, 20, 10, 0]
    );
    assert_eq!(
        s.range(..).map(|x| *x).collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );

    assert_eq!(
        s.range((Included(&0), Unbounded))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&0), Unbounded))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Included(&25), Unbounded))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&25), Unbounded))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Included(&70), Unbounded))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&70), Unbounded))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![80, 90]
    );
    assert_eq!(
        s.range((Included(&100), Unbounded))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&100), Unbounded))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Unbounded, Included(&90)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&90)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Unbounded, Included(&25)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![0, 10, 20]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&25)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![0, 10, 20]
    );
    assert_eq!(
        s.range((Unbounded, Included(&70)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&70)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60]
    );
    assert_eq!(
        s.range((Unbounded, Included(&-1)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&-1)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Included(&25), Included(&80)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Included(&25), Excluded(&80)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70]
    );
    assert_eq!(
        s.range((Excluded(&25), Included(&80)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Excluded(&25), Excluded(&80)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70]
    );

    assert_eq!(
        s.range((Included(&25), Included(&25)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Included(&25), Excluded(&25)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&25), Included(&25)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&25), Excluded(&25)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Included(&50), Included(&50)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        vec![50]
    );
    assert_eq!(
        s.range((Included(&50), Excluded(&50)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&50), Included(&50)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&50), Excluded(&50)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );

    assert_eq!(
        s.range((Included(&100), Included(&-2)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Included(&100), Excluded(&-2)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&100), Included(&-2)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
    assert_eq!(
        s.range((Excluded(&100), Excluded(&-2)))
            .map(|x| *x)
            .collect::<Vec<_>>(),
        Vec::<i32>::new()
    );
}

// https://github.com/crossbeam-rs/crossbeam/issues/671
#[test]
fn iter_range2() {
    let set: SkipSet<_> = [1, 3, 5].iter().cloned().collect();
    assert_eq!(set.range(2..4).count(), 1);
    set.insert(3);
}

#[test]
fn into_iter() {
    let s = SkipSet::new();
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x);
    }

    assert_eq!(s.into_iter().collect::<Vec<_>>(), &[2, 4, 5, 7, 8, 11, 12]);
}

#[test]
fn clear() {
    let s = SkipSet::new();
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x);
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
    let set: Arc<SkipSet<u32>> = Arc::new(SkipSet::new());
    let len = if cfg!(miri) { 100 } else { 10_000 };
    let key = 0;
    set.insert(0);

    let getter = set.clone();
    let handle = std::thread::spawn(move || {
        for _ in 0..len {
            set.insert(0);
        }
    });
    for _ in 0..len {
        assert!(getter.get(&key).is_some());
    }
    handle.join().unwrap()
}

#[test]
fn comparator() {
    use std::cmp::Ordering;

    use crate::skiplist::comparator::{Comparator, Equivalator};

    struct DynComparator {
        #[allow(clippy::type_complexity)]
        inner: Box<dyn Fn(&[u8], &[u8]) -> Ordering>,
    }

    impl<L: ?Sized, R: ?Sized> Equivalator<L, R> for DynComparator
    where
        L: std::borrow::Borrow<[u8]>,
        R: std::borrow::Borrow<[u8]>,
    {
        fn equivalent(&self, lhs: &L, rhs: &R) -> bool {
            (self.inner)(lhs.borrow(), rhs.borrow()).is_eq()
        }
    }

    impl<L: ?Sized, R: ?Sized> Comparator<L, R> for DynComparator
    where
        L: std::borrow::Borrow<[u8]>,
        R: std::borrow::Borrow<[u8]>,
    {
        fn compare(&self, lhs: &L, rhs: &R) -> Ordering {
            (self.inner)(lhs.borrow(), rhs.borrow())
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct OrderedF32(f32);

    impl PartialEq for OrderedF32 {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(other).is_eq()
        }
    }

    impl Eq for OrderedF32 {}

    impl PartialOrd for OrderedF32 {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for OrderedF32 {
        fn cmp(&self, other: &Self) -> Ordering {
            match self.0.partial_cmp(&other.0) {
                Some(o) => o,
                None => Ordering::Equal,
            }
        }
    }

    fn encode(slice: &[f32]) -> Vec<u8> {
        let mut bytes = vec![];
        for &f in slice {
            bytes.extend_from_slice(&f.to_le_bytes());
        }
        bytes
    }

    fn decode(slice: &[u8]) -> impl Iterator<Item = OrderedF32> + '_ {
        slice
            .chunks_exact(4)
            .map(|b| OrderedF32(f32::from_le_bytes(b.try_into().unwrap())))
    }

    fn compare(lhs: &[u8], rhs: &[u8]) -> Ordering {
        decode(lhs).cmp(decode(rhs))
    }

    let s = SkipSet::with_comparator(DynComparator {
        inner: Box::new(compare),
    });
    s.insert(encode(&[0f32]));
    s.insert(encode(&[1f32]));
    s.insert(encode(&[0f32, 1f32]));
    s.insert(encode(&[-0f32]));

    assert_eq!(s.len(), 3);
    assert!(s.contains(&encode(&[0f32])));
    assert!(s.contains(&encode(&[-0f32])));
    assert!(s.contains(&encode(&[1f32])));
    assert!(s.contains(&encode(&[0f32, 1f32])));

    let elems: Vec<_> = s.iter().map(|x| &x.value()[..]).collect();
    assert_eq!(
        elems,
        [&encode(&[-0f32]), &encode(&[0f32, 1f32]), &encode(&[1f32])],
    );

    s.remove(&encode(&[0f32]));
    assert!(!s.contains(&encode(&[-0f32])));
}
