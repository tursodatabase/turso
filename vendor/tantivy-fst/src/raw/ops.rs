use std::cmp;
use std::collections::BinaryHeap;
use std::iter::FromIterator;

use crate::raw::Output;
use crate::stream::{IntoStreamer, Streamer};

/// Permits stream operations to be hetergeneous with respect to streams.
type BoxedStream<'f> = Box<dyn for<'a> Streamer<'a, Item = (&'a [u8], Output)> + 'f>;

/// A value indexed by a stream.
///
/// Indexed values are used to indicate the presence of a key in multiple
/// streams during a set operation. Namely, the index corresponds to the stream
/// (by the order in which it was added to the operation, starting at `0`)
/// and the value corresponds to the value associated with a particular key
/// in that stream.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexedValue {
    /// The index of the stream that produced this value (starting at `0`).
    pub index: usize,
    /// The value.
    pub value: u64,
}

/// A builder for collecting fst streams on which to perform set operations
/// on the keys of fsts.
///
/// Set operations include intersection, union, difference and symmetric
/// difference. The result of each set operation is itself a stream that emits
/// pairs of keys and a sequence of each occurrence of that key in the
/// participating streams. This information allows one to perform set
/// operations on fsts and customize how conflicting output values are handled.
///
/// All set operations work efficiently on an arbitrary number of
/// streams with memory proportional to the number of streams.
///
/// The algorithmic complexity of all set operations is `O(n1 + n2 + n3 + ...)`
/// where `n1, n2, n3, ...` correspond to the number of elements in each
/// stream.
///
/// The `'f` lifetime parameter refers to the lifetime of the underlying set.
pub struct OpBuilder<'f> {
    streams: Vec<BoxedStream<'f>>,
}

impl<'f> Default for OpBuilder<'f> {
    fn default() -> Self {
        OpBuilder { streams: vec![] }
    }
}

impl<'f> OpBuilder<'f> {
    /// Add a stream to this set operation.
    ///
    /// This is useful for a chaining style pattern, e.g.,
    /// `builder.add(stream1).add(stream2).union()`.
    ///
    /// The stream must emit a lexicographically ordered sequence of key-value
    /// pairs.
    pub fn add<I, S>(mut self, stream: I) -> Self
    where
        I: for<'a> IntoStreamer<'a, Into = S, Item = (&'a [u8], Output)>,
        S: 'f + for<'a> Streamer<'a, Item = (&'a [u8], Output)>,
    {
        self.push(stream);
        self
    }

    /// Add a stream to this set operation.
    ///
    /// The stream must emit a lexicographically ordered sequence of key-value
    /// pairs.
    pub fn push<I, S>(&mut self, stream: I)
    where
        I: for<'a> IntoStreamer<'a, Into = S, Item = (&'a [u8], Output)>,
        S: 'f + for<'a> Streamer<'a, Item = (&'a [u8], Output)>,
    {
        self.streams.push(Box::new(stream.into_stream()));
    }

    /// Performs a chain operation on all streams that have been added.
    ///
    /// Panics when no streams have been added.
    #[inline]
    pub fn chain(self) -> Chain<'f> {
        Chain::new(self.streams)
    }
    /// Performs a union operation on all streams that have been added.
    ///
    /// Note that this returns a stream of `(&[u8], &[IndexedValue])`. The
    /// first element of the tuple is the byte string key. The second element
    /// of the tuple is a list of all occurrences of that key in participating
    /// streams. The `IndexedValue` contains an index and the value associated
    /// with that key in that stream. The index uniquely identifies each
    /// stream, which is an integer that is auto-incremented when a stream
    /// is added to this operation (starting at `0`).
    #[inline]
    pub fn union(self) -> Union<'f> {
        Union {
            heap: StreamHeap::new(self.streams),
            outs: vec![],
            cur_slot: None,
        }
    }

    /// Performs an intersection operation on all streams that have been added.
    ///
    /// Note that this returns a stream of `(&[u8], &[IndexedValue])`. The
    /// first element of the tuple is the byte string key. The second element
    /// of the tuple is a list of all occurrences of that key in participating
    /// streams. The `IndexedValue` contains an index and the value associated
    /// with that key in that stream. The index uniquely identifies each
    /// stream, which is an integer that is auto-incremented when a stream
    /// is added to this operation (starting at `0`).
    #[inline]
    pub fn intersection(self) -> Intersection<'f> {
        Intersection {
            heap: StreamHeap::new(self.streams),
            outs: vec![],
            cur_slot: None,
        }
    }

    /// Performs a difference operation with respect to the first stream added.
    /// That is, this returns a stream of all elements in the first stream
    /// that don't exist in any other stream that has been added.
    ///
    /// Note that this returns a stream of `(&[u8], &[IndexedValue])`. The
    /// first element of the tuple is the byte string key. The second element
    /// of the tuple is a list of all occurrences of that key in participating
    /// streams. The `IndexedValue` contains an index and the value associated
    /// with that key in that stream. The index uniquely identifies each
    /// stream, which is an integer that is auto-incremented when a stream
    /// is added to this operation (starting at `0`).
    #[inline]
    pub fn difference(mut self) -> Difference<'f> {
        let first = self.streams.swap_remove(0);
        Difference {
            set: first,
            key: vec![],
            heap: StreamHeap::new(self.streams),
            outs: vec![],
        }
    }

    /// Performs a symmetric difference operation on all of the streams that
    /// have been added.
    ///
    /// When there are only two streams, then the keys returned correspond to
    /// keys that are in either stream but *not* in both streams.
    ///
    /// More generally, for any number of streams, keys that occur in an odd
    /// number of streams are returned.
    ///
    /// Note that this returns a stream of `(&[u8], &[IndexedValue])`. The
    /// first element of the tuple is the byte string key. The second element
    /// of the tuple is a list of all occurrences of that key in participating
    /// streams. The `IndexedValue` contains an index and the value associated
    /// with that key in that stream. The index uniquely identifies each
    /// stream, which is an integer that is auto-incremented when a stream
    /// is added to this operation (starting at `0`).
    #[inline]
    pub fn symmetric_difference(self) -> SymmetricDifference<'f> {
        SymmetricDifference {
            heap: StreamHeap::new(self.streams),
            outs: vec![],
            cur_slot: None,
        }
    }
}

impl<'f, I, S> Extend<I> for OpBuilder<'f>
where
    I: for<'a> IntoStreamer<'a, Into = S, Item = (&'a [u8], Output)>,
    S: 'f + for<'a> Streamer<'a, Item = (&'a [u8], Output)>,
{
    fn extend<T>(&mut self, it: T)
    where
        T: IntoIterator<Item = I>,
    {
        for stream in it {
            self.push(stream);
        }
    }
}

impl<'f, I, S> FromIterator<I> for OpBuilder<'f>
where
    I: for<'a> IntoStreamer<'a, Into = S, Item = (&'a [u8], Output)>,
    S: 'f + for<'a> Streamer<'a, Item = (&'a [u8], Output)>,
{
    fn from_iter<T>(it: T) -> Self
    where
        T: IntoIterator<Item = I>,
    {
        let mut op = OpBuilder::default();
        op.extend(it);
        op
    }
}

/// A stream of chaining multiple fst streams in added order.
///
/// The `'f` lifetime parameter refers to the lifetime of the underlying map.
pub struct Chain<'f> {
    streams: Vec<BoxedStream<'f>>,
    current_stream: BoxedStream<'f>,
    key: Vec<u8>,
}

impl<'f> Chain<'f> {
    /// Creates a new Chain. panics when streams is empty
    fn new(mut streams: Vec<BoxedStream<'f>>) -> Self {
        streams.reverse();

        let current_stream = streams.pop().unwrap();
        Chain {
            streams,
            current_stream,
            key: vec![],
        }
    }
}

impl<'a, 'f> Streamer<'a> for Chain<'f> {
    type Item = (&'a [u8], Output);

    fn next(&'a mut self) -> Option<Self::Item> {
        loop {
            if let Some((key, val)) = self.current_stream.next() {
                self.key.clear();
                self.key.extend_from_slice(&key);
                return Some((&self.key, val));
            } else {
                if let Some(next_stream) = self.streams.pop() {
                    self.current_stream = next_stream;
                } else {
                    return None;
                }
            }
        }
    }
}

/// A stream of set union over multiple fst streams in lexicographic order.
///
/// The `'f` lifetime parameter refers to the lifetime of the underlying map.
pub struct Union<'f> {
    heap: StreamHeap<'f>,
    outs: Vec<IndexedValue>,
    cur_slot: Option<Slot>,
}

impl<'a, 'f> Streamer<'a> for Union<'f> {
    type Item = (&'a [u8], &'a [IndexedValue]);

    fn next(&'a mut self) -> Option<Self::Item> {
        if let Some(slot) = self.cur_slot.take() {
            self.heap.refill(slot);
        }
        let slot = match self.heap.pop() {
            None => return None,
            Some(slot) => {
                self.cur_slot = Some(slot);
                self.cur_slot.as_ref().unwrap()
            }
        };
        self.outs.clear();
        self.outs.push(slot.indexed_value());
        while let Some(slot2) = self.heap.pop_if_equal(slot.input()) {
            self.outs.push(slot2.indexed_value());
            self.heap.refill(slot2);
        }
        Some((slot.input(), &self.outs))
    }
}

/// A stream of set intersection over multiple fst streams in lexicographic
/// order.
///
/// The `'f` lifetime parameter refers to the lifetime of the underlying fst.
pub struct Intersection<'f> {
    heap: StreamHeap<'f>,
    outs: Vec<IndexedValue>,
    cur_slot: Option<Slot>,
}

impl<'a, 'f> Streamer<'a> for Intersection<'f> {
    type Item = (&'a [u8], &'a [IndexedValue]);

    fn next(&'a mut self) -> Option<Self::Item> {
        if let Some(slot) = self.cur_slot.take() {
            self.heap.refill(slot);
        }
        loop {
            let slot = match self.heap.pop() {
                None => return None,
                Some(slot) => slot,
            };
            self.outs.clear();
            self.outs.push(slot.indexed_value());
            let mut popped: usize = 1;
            while let Some(slot2) = self.heap.pop_if_equal(slot.input()) {
                self.outs.push(slot2.indexed_value());
                self.heap.refill(slot2);
                popped += 1;
            }
            if popped < self.heap.num_slots() {
                self.heap.refill(slot);
            } else {
                self.cur_slot = Some(slot);
                let key = self.cur_slot.as_ref().unwrap().input();
                return Some((key, &self.outs));
            }
        }
    }
}

/// A stream of set difference over multiple fst streams in lexicographic
/// order.
///
/// The difference operation is taken with respect to the first stream and the
/// rest of the streams. i.e., All elements in the first stream that do not
/// appear in any other streams.
///
/// The `'f` lifetime parameter refers to the lifetime of the underlying fst.
pub struct Difference<'f> {
    set: BoxedStream<'f>,
    key: Vec<u8>,
    heap: StreamHeap<'f>,
    outs: Vec<IndexedValue>,
}

impl<'a, 'f> Streamer<'a> for Difference<'f> {
    type Item = (&'a [u8], &'a [IndexedValue]);

    fn next(&'a mut self) -> Option<Self::Item> {
        loop {
            match self.set.next() {
                None => return None,
                Some((key, out)) => {
                    self.key.clear();
                    self.key.extend(key);
                    self.outs.clear();
                    self.outs.push(IndexedValue {
                        index: 0,
                        value: out.value(),
                    });
                }
            };
            let mut unique = true;
            while let Some(slot) = self.heap.pop_if_le(&self.key) {
                if slot.input() == &*self.key {
                    unique = false;
                }
                self.heap.refill(slot);
            }
            if unique {
                return Some((&self.key, &self.outs));
            }
        }
    }
}

/// A stream of set symmetric difference over multiple fst streams in
/// lexicographic order.
///
/// The `'f` lifetime parameter refers to the lifetime of the underlying fst.
pub struct SymmetricDifference<'f> {
    heap: StreamHeap<'f>,
    outs: Vec<IndexedValue>,
    cur_slot: Option<Slot>,
}

impl<'a, 'f> Streamer<'a> for SymmetricDifference<'f> {
    type Item = (&'a [u8], &'a [IndexedValue]);

    fn next(&'a mut self) -> Option<Self::Item> {
        if let Some(slot) = self.cur_slot.take() {
            self.heap.refill(slot);
        }
        loop {
            let slot = match self.heap.pop() {
                None => return None,
                Some(slot) => slot,
            };
            self.outs.clear();
            self.outs.push(slot.indexed_value());
            let mut popped: usize = 1;
            while let Some(slot2) = self.heap.pop_if_equal(slot.input()) {
                self.outs.push(slot2.indexed_value());
                self.heap.refill(slot2);
                popped += 1;
            }
            // This key is in the symmetric difference if and only if it
            // appears in an odd number of sets.
            if popped % 2 == 0 {
                self.heap.refill(slot);
            } else {
                self.cur_slot = Some(slot);
                let key = self.cur_slot.as_ref().unwrap().input();
                return Some((key, &self.outs));
            }
        }
    }
}

struct StreamHeap<'f> {
    rdrs: Vec<BoxedStream<'f>>,
    heap: BinaryHeap<Slot>,
}

impl<'f> StreamHeap<'f> {
    fn new(streams: Vec<BoxedStream<'f>>) -> StreamHeap<'f> {
        let mut u = StreamHeap {
            rdrs: streams,
            heap: BinaryHeap::new(),
        };
        for i in 0..u.rdrs.len() {
            u.refill(Slot::new(i));
        }
        u
    }

    fn pop(&mut self) -> Option<Slot> {
        self.heap.pop()
    }

    fn peek_is_duplicate(&self, key: &[u8]) -> bool {
        self.heap.peek().map(|s| s.input() == key).unwrap_or(false)
    }

    fn pop_if_equal(&mut self, key: &[u8]) -> Option<Slot> {
        if self.peek_is_duplicate(key) {
            self.pop()
        } else {
            None
        }
    }

    fn pop_if_le(&mut self, key: &[u8]) -> Option<Slot> {
        if self.heap.peek().map(|s| s.input() <= key).unwrap_or(false) {
            self.pop()
        } else {
            None
        }
    }

    fn num_slots(&self) -> usize {
        self.rdrs.len()
    }

    fn refill(&mut self, mut slot: Slot) {
        if let Some((input, output)) = self.rdrs[slot.idx].next() {
            slot.set_input(input);
            slot.set_output(output);
            self.heap.push(slot);
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Slot {
    idx: usize,
    input: Vec<u8>,
    output: Output,
}

impl Slot {
    fn new(rdr_idx: usize) -> Slot {
        Slot {
            idx: rdr_idx,
            input: Vec::with_capacity(64),
            output: Output::zero(),
        }
    }

    fn indexed_value(&self) -> IndexedValue {
        IndexedValue {
            index: self.idx,
            value: self.output.value(),
        }
    }

    fn input(&self) -> &[u8] {
        &self.input
    }

    fn set_input(&mut self, input: &[u8]) {
        self.input.clear();
        self.input.extend(input);
    }

    fn set_output(&mut self, output: Output) {
        self.output = output;
    }
}

impl PartialOrd for Slot {
    fn partial_cmp(&self, other: &Slot) -> Option<cmp::Ordering> {
        (&self.input, self.output)
            .partial_cmp(&(&other.input, other.output))
            .map(|ord| ord.reverse())
    }
}

impl Ord for Slot {
    fn cmp(&self, other: &Slot) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::raw::tests::{fst_map, fst_set};
    use crate::raw::Fst;
    use crate::raw::Output;
    use crate::stream::{IntoStreamer, Streamer};

    use super::OpBuilder;

    fn s(string: &str) -> String {
        string.to_owned()
    }

    macro_rules! create_set_op {
        ($name:ident, $op:ident) => {
            fn $name(sets: Vec<Vec<&str>>) -> Vec<String> {
                let fsts: Vec<Fst> = sets.into_iter().map(fst_set).collect();
                let op: OpBuilder = fsts.iter().collect();
                let mut stream = op.$op().into_stream();
                let mut keys = vec![];
                while let Some((key, _)) = stream.next() {
                    keys.push(String::from_utf8(key.to_vec()).unwrap());
                }
                keys
            }
        };
    }

    macro_rules! create_map_op {
        ($name:ident, $op:ident) => {
            fn $name(sets: Vec<Vec<(&str, u64)>>) -> Vec<(String, u64)> {
                let fsts: Vec<Fst> = sets.into_iter().map(fst_map).collect();
                let op: OpBuilder = fsts.iter().collect();
                let mut stream = op.$op().into_stream();
                let mut keys = vec![];
                while let Some((key, outs)) = stream.next() {
                    let merged = outs.iter().fold(0, |a, b| a + b.value);
                    let s = String::from_utf8(key.to_vec()).unwrap();
                    keys.push((s, merged));
                }
                keys
            }
        };
    }

    macro_rules! create_map_op_chain {
        ($name:ident, $op:ident) => {
            fn $name(sets: Vec<Vec<(&str, u64)>>) -> Vec<(String, Output)> {
                let fsts: Vec<Fst> = sets.into_iter().map(fst_map).collect();
                let op: OpBuilder = fsts.iter().collect();
                let mut stream = op.$op().into_stream();
                let mut keys = vec![];
                while let Some((key, outs)) = stream.next() {
                    let s = String::from_utf8(key.to_vec()).unwrap();
                    keys.push((s, outs));
                }
                keys
            }
        };
    }
    create_set_op!(fst_union, union);
    create_set_op!(fst_intersection, intersection);
    create_set_op!(fst_symmetric_difference, symmetric_difference);
    create_set_op!(fst_difference, difference);
    create_set_op!(fst_chain, chain);
    create_map_op!(fst_union_map, union);
    create_map_op!(fst_intersection_map, intersection);
    create_map_op!(fst_symmetric_difference_map, symmetric_difference);
    create_map_op!(fst_difference_map, difference);
    create_map_op_chain!(fst_chain_map, chain);

    #[test]
    fn chain_set() {
        let v = fst_chain(vec![vec!["a", "b", "c"], vec!["x", "y", "z"]]);
        assert_eq!(v, vec!["a", "b", "c", "x", "y", "z"]);
    }
    #[test]
    fn chain_set_wrong_order() {
        // Chaining doesn't care about order
        let v = fst_chain(vec![vec!["a", "b", "c", "z"], vec!["x", "y", "z"]]);
        assert_eq!(v, vec!["a", "b", "c", "z", "x", "y", "z"]);
    }
    #[test]
    fn chain_map() {
        let v = fst_chain_map(vec![
            vec![("a", 1), ("b", 2), ("c", 3)],
            vec![("x", 1), ("y", 2), ("z", 3)],
        ]);
        assert_eq!(
            v,
            vec![
                (s("a"), Output::new(1)),
                (s("b"), Output::new(2)),
                (s("c"), Output::new(3)),
                (s("x"), Output::new(1)),
                (s("y"), Output::new(2)),
                (s("z"), Output::new(3)),
            ]
        );
    }
    #[test]
    fn union_set() {
        let v = fst_union(vec![vec!["a", "b", "c"], vec!["x", "y", "z"]]);
        assert_eq!(v, vec!["a", "b", "c", "x", "y", "z"]);
    }

    #[test]
    fn union_set_dupes() {
        let v = fst_union(vec![vec!["aa", "b", "cc"], vec!["b", "cc", "z"]]);
        assert_eq!(v, vec!["aa", "b", "cc", "z"]);
    }

    #[test]
    fn union_map() {
        let v = fst_union_map(vec![
            vec![("a", 1), ("b", 2), ("c", 3)],
            vec![("x", 1), ("y", 2), ("z", 3)],
        ]);
        assert_eq!(
            v,
            vec![
                (s("a"), 1),
                (s("b"), 2),
                (s("c"), 3),
                (s("x"), 1),
                (s("y"), 2),
                (s("z"), 3),
            ]
        );
    }

    #[test]
    fn union_map_dupes() {
        let v = fst_union_map(vec![
            vec![("aa", 1), ("b", 2), ("cc", 3)],
            vec![("b", 1), ("cc", 2), ("z", 3)],
            vec![("b", 1)],
        ]);
        assert_eq!(
            v,
            vec![(s("aa"), 1), (s("b"), 4), (s("cc"), 5), (s("z"), 3),]
        );
    }

    #[test]
    fn intersect_set() {
        let v = fst_intersection(vec![vec!["a", "b", "c"], vec!["x", "y", "z"]]);
        assert_eq!(v, Vec::<String>::new());
    }

    #[test]
    fn intersect_set_dupes() {
        let v = fst_intersection(vec![vec!["aa", "b", "cc"], vec!["b", "cc", "z"]]);
        assert_eq!(v, vec!["b", "cc"]);
    }

    #[test]
    fn intersect_map() {
        let v = fst_intersection_map(vec![
            vec![("a", 1), ("b", 2), ("c", 3)],
            vec![("x", 1), ("y", 2), ("z", 3)],
        ]);
        assert_eq!(v, Vec::<(String, u64)>::new());
    }

    #[test]
    fn intersect_map_dupes() {
        let v = fst_intersection_map(vec![
            vec![("aa", 1), ("b", 2), ("cc", 3)],
            vec![("b", 1), ("cc", 2), ("z", 3)],
            vec![("b", 1)],
        ]);
        assert_eq!(v, vec![(s("b"), 4)]);
    }

    #[test]
    fn symmetric_difference() {
        let v = fst_symmetric_difference(vec![vec!["a", "b", "c"], vec!["a", "b"], vec!["a"]]);
        assert_eq!(v, vec!["a", "c"]);
    }

    #[test]
    fn symmetric_difference_map() {
        let v = fst_symmetric_difference_map(vec![
            vec![("a", 1), ("b", 2), ("c", 3)],
            vec![("a", 1), ("b", 2)],
            vec![("a", 1)],
        ]);
        assert_eq!(v, vec![(s("a"), 3), (s("c"), 3)]);
    }

    #[test]
    fn difference() {
        let v = fst_difference(vec![vec!["a", "b", "c"], vec!["a", "b"], vec!["a"]]);
        assert_eq!(v, vec!["c"]);
    }

    #[test]
    fn difference2() {
        // Regression test: https://github.com/BurntSushi/fst/issues/19
        let v = fst_difference(vec![vec!["a", "c"], vec!["b", "c"]]);
        assert_eq!(v, vec!["a"]);
        let v = fst_difference(vec![vec!["bar", "foo"], vec!["baz", "foo"]]);
        assert_eq!(v, vec!["bar"]);
    }

    #[test]
    fn difference_map() {
        let v = fst_difference_map(vec![
            vec![("a", 1), ("b", 2), ("c", 3)],
            vec![("a", 1), ("b", 2)],
            vec![("a", 1)],
        ]);
        assert_eq!(v, vec![(s("c"), 3)]);
    }
}
