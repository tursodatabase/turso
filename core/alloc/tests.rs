use super::*;

struct LowerBoundOnly {
    next: usize,
    end: usize,
}

impl Iterator for LowerBoundOnly {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.end {
            return None;
        }
        let value = self.next;
        self.next += 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[test]
fn try_extend_accepts_exact_size_iterators() {
    let mut values = Vec::new();

    values.try_extend([1, 2, 3]).unwrap();

    assert_eq!(values.as_slice(), &[1, 2, 3]);
}

#[test]
fn try_extend_accepts_iterators_without_upper_bounds() {
    let mut values = Vec::new();

    values
        .try_extend(LowerBoundOnly { next: 0, end: 3 })
        .unwrap();

    assert_eq!(values.as_slice(), &[0, 1, 2]);
}

#[test]
fn hash_map_try_insert_and_extend_reserve_before_mutation() {
    let mut values: HashMap<&str, usize> = HashMap::default();

    assert_eq!(
        TursoHashMapExt::try_insert(&mut values, "one", 1).unwrap(),
        None
    );
    assert_eq!(
        TursoHashMapExt::try_insert(&mut values, "one", 11).unwrap(),
        Some(1)
    );
    values.try_extend([("two", 2), ("three", 3)]).unwrap();

    assert_eq!(values.get("one"), Some(&11));
    assert_eq!(values.get("two"), Some(&2));
    assert_eq!(values.get("three"), Some(&3));
}

#[test]
fn hash_set_try_insert_and_extend_reserve_before_mutation() {
    let mut values: HashSet<usize> = HashSet::default();

    assert!(TursoHashSetExt::try_insert(&mut values, 1).unwrap());
    assert!(!TursoHashSetExt::try_insert(&mut values, 1).unwrap());
    values.try_extend([2, 3]).unwrap();

    assert!(values.contains(&1));
    assert!(values.contains(&2));
    assert!(values.contains(&3));
}

#[test]
fn vec_deque_try_push_and_extend_reserve_before_mutation() {
    let mut values: VecDeque<usize> = TursoAllocExt::new();

    values.try_push_back(2).unwrap();
    values.try_push_front(1).unwrap();
    values.try_extend([3, 4]).unwrap();

    assert_eq!(values.pop_front(), Some(1));
    assert_eq!(values.pop_front(), Some(2));
    assert_eq!(values.pop_front(), Some(3));
    assert_eq!(values.pop_front(), Some(4));
    assert_eq!(values.pop_front(), None);
}

#[test]
fn binary_heap_try_push_and_extend_reserve_before_mutation() {
    let mut values: BinaryHeap<usize> = TursoAllocExt::new();

    values.try_push(2).unwrap();
    values.try_extend([1, 3]).unwrap();

    assert_eq!(values.pop(), Some(3));
    assert_eq!(values.pop(), Some(2));
    assert_eq!(values.pop(), Some(1));
    assert_eq!(values.pop(), None);
}

#[test]
fn iterator_try_collect_builds_turso_vec() {
    let values: Vec<_> = [1, 2, 3].into_iter().try_collect().unwrap();

    assert_eq!(values.as_slice(), &[1, 2, 3]);
}

#[test]
fn try_extend_extends_existing_collection() {
    let mut values = try_vec![1].unwrap();

    values.try_extend([2, 3]).unwrap();

    assert_eq!(values.as_slice(), &[1, 2, 3]);
}

#[test]
fn try_vec_macro_builds_turso_vecs() {
    let empty: Vec<usize> = try_vec![].unwrap();
    let repeated: Vec<_> = try_vec![7; 3].unwrap();
    let listed: Vec<_> = try_vec![1, 2, 3].unwrap();

    assert!(empty.is_empty());
    assert_eq!(repeated.as_slice(), &[7, 7, 7]);
    assert_eq!(listed.as_slice(), &[1, 2, 3]);
}

#[test]
fn iterator_try_collect_builds_turso_collections() {
    let map: HashMap<_, _> = [("one", 1), ("two", 2)].into_iter().try_collect().unwrap();
    let set: HashSet<_> = [1, 2, 3].into_iter().try_collect().unwrap();
    let heap: BinaryHeap<_> = [1, 3, 2].into_iter().try_collect().unwrap();

    assert_eq!(map.get("one"), Some(&1));
    assert!(set.contains(&3));
    assert_eq!(heap.peek(), Some(&3));
}

#[test]
fn iterator_try_collect_builds_option_collection() {
    let values: Option<Vec<_>> = [Some(1), Some(2), Some(3)]
        .into_iter()
        .try_collect()
        .unwrap();
    let none: Option<Vec<_>> = [Some(1), None, Some(3)].into_iter().try_collect().unwrap();

    assert_eq!(values.unwrap().as_slice(), &[1, 2, 3]);
    assert!(none.is_none());
}

#[test]
fn iterator_try_collect_builds_result_collection() {
    let values: Result<Vec<_>, &str> = [Ok::<_, &str>(1), Ok(2), Ok(3)]
        .into_iter()
        .try_collect()
        .unwrap();
    let error: Result<Vec<_>, &str> = [Ok(1), Err("bad"), Ok(3)]
        .into_iter()
        .try_collect()
        .unwrap();

    assert_eq!(values.unwrap().as_slice(), &[1, 2, 3]);
    assert_eq!(error, Err("bad"));
}

#[test]
fn iterator_try_collect_converts_result_error() {
    #[derive(Debug, PartialEq)]
    struct Converted(&'static str);

    impl From<&'static str> for Converted {
        fn from(value: &'static str) -> Self {
            Self(value)
        }
    }

    let values: Result<Vec<_>, Converted> = [
        Ok::<_, &'static str>(1),
        Ok::<_, &'static str>(2),
        Ok::<_, &'static str>(3),
    ]
    .into_iter()
    .try_collect::<Result<Vec<_>, Converted>>()
    .unwrap();
    let error: Result<Vec<_>, Converted> = [Ok(1), Err("bad"), Ok(3)]
        .into_iter()
        .try_collect::<Result<Vec<_>, Converted>>()
        .unwrap();

    assert_eq!(values.unwrap().as_slice(), &[1, 2, 3]);
    assert_eq!(error, Err(Converted("bad")));
}

#[test]
fn iterator_try_collect_accepts_try_vec_results() {
    let values: crate::Result<Vec<_>> = [1usize, 2]
        .into_iter()
        .map(|count| try_vec![false; count])
        .try_collect::<crate::Result<Vec<_>>>()
        .unwrap();
    let values = values.unwrap();

    assert_eq!(values[0].as_slice(), &[false]);
    assert_eq!(values[1].as_slice(), &[false, false]);
}

#[test]
fn tuple_try_extend_extends_both_collections() {
    let mut values: (Vec<_>, VecDeque<_>) = (Vec::new(), VecDeque::new());

    values
        .try_extend([(1, "one"), (2, "two"), (3, "three")])
        .unwrap();

    assert_eq!(values.0.as_slice(), &[1, 2, 3]);
    assert_eq!(
        values.1.into_iter().collect::<std::vec::Vec<_>>(),
        ["one", "two", "three"]
    );
}

#[test]
fn tuple_try_collect_builds_three_collections() {
    let (numbers, words, flags): (Vec<_>, VecDeque<_>, Vec<_>) =
        [(1, "one", true), (2, "two", false), (3, "three", true)]
            .into_iter()
            .try_collect()
            .unwrap();

    assert_eq!(numbers.as_slice(), &[1, 2, 3]);
    assert_eq!(
        words.into_iter().collect::<std::vec::Vec<_>>(),
        ["one", "two", "three"]
    );
    assert_eq!(flags.as_slice(), &[true, false, true]);
}

#[test]
fn iterator_try_unzip_builds_turso_collections() {
    let (numbers, words): (Vec<_>, VecDeque<_>) = [(1, "one"), (2, "two"), (3, "three")]
        .into_iter()
        .try_unzip()
        .unwrap();

    assert_eq!(numbers.as_slice(), &[1, 2, 3]);
    assert_eq!(
        words.into_iter().collect::<std::vec::Vec<_>>(),
        ["one", "two", "three"]
    );
}

#[test]
fn try_with_capacity_builds_turso_collections() {
    let values: Vec<usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();
    let map: HashMap<usize, usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();
    let set: HashSet<usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();
    let queue: VecDeque<usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();
    let heap: BinaryHeap<usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();

    assert!(values.capacity() >= 3);
    assert!(map.capacity() >= 3);
    assert!(set.capacity() >= 3);
    assert!(queue.capacity() >= 3);
    assert!(heap.capacity() >= 3);
}

#[test]
fn try_clone_builds_independent_alloc_collections() {
    let values: Vec<_> = try_vec![1, 2, 3].unwrap();
    let cloned = values.try_clone().unwrap();
    assert_eq!(cloned.as_slice(), values.as_slice());
    assert_ne!(cloned.as_ptr(), values.as_ptr());

    let boxed = Box::try_new(String::from("turso")).unwrap();
    let cloned = boxed.try_clone().unwrap();
    assert_eq!(&*cloned, &*boxed);

    let queue: VecDeque<_> = [1, 2, 3].into_iter().try_collect().unwrap();
    let mut cloned = queue.try_clone().unwrap();
    assert_eq!(cloned.pop_front(), Some(1));
    assert_eq!(cloned.pop_front(), Some(2));
    assert_eq!(cloned.pop_front(), Some(3));

    let heap: BinaryHeap<_> = [1, 3, 2].into_iter().try_collect().unwrap();
    let mut cloned = heap.try_clone().unwrap();
    assert_eq!(cloned.pop(), Some(3));
    assert_eq!(cloned.pop(), Some(2));
    assert_eq!(cloned.pop(), Some(1));
}

#[test]
fn try_clone_builds_independent_hash_collections() {
    let map: HashMap<_, _> = [
        ("one".to_string(), try_vec![1].unwrap()),
        ("two".to_string(), try_vec![2].unwrap()),
    ]
    .into_iter()
    .try_collect()
    .unwrap();
    let set: HashSet<_> = ["one".to_string(), "two".to_string()]
        .into_iter()
        .try_collect()
        .unwrap();

    let cloned_map = map.try_clone().unwrap();
    let cloned_set = set.try_clone().unwrap();

    assert_eq!(cloned_map.get("one").unwrap().as_slice(), &[1]);
    assert_eq!(cloned_map.get("two").unwrap().as_slice(), &[2]);
    assert!(cloned_set.contains("one"));
    assert!(cloned_set.contains("two"));
}
