mod heap_merge;

pub use self::heap_merge::merge_sstable;

pub trait SingleValueMerger<V> {
    fn add(&mut self, v: &V);
    fn finish(self) -> V;
}

pub trait ValueMerger<V> {
    type TSingleValueMerger: SingleValueMerger<V>;
    fn new_value(&mut self, v: &V) -> Self::TSingleValueMerger;
}

#[derive(Default)]
pub struct KeepFirst;

pub struct FirstVal<V>(V);

impl<V: Clone> ValueMerger<V> for KeepFirst {
    type TSingleValueMerger = FirstVal<V>;

    fn new_value(&mut self, v: &V) -> FirstVal<V> {
        FirstVal(v.clone())
    }
}

impl<V> SingleValueMerger<V> for FirstVal<V> {
    fn add(&mut self, _: &V) {}

    fn finish(self) -> V {
        self.0
    }
}

pub struct VoidMerge;
impl ValueMerger<()> for VoidMerge {
    type TSingleValueMerger = ();

    fn new_value(&mut self, _: &()) {}
}

pub struct U64Merge;
impl ValueMerger<u64> for U64Merge {
    type TSingleValueMerger = u64;

    fn new_value(&mut self, val: &u64) -> u64 {
        *val
    }
}

impl SingleValueMerger<u64> for u64 {
    fn add(&mut self, val: &u64) {
        *self += *val;
    }

    fn finish(self) -> u64 {
        self
    }
}

impl SingleValueMerger<()> for () {
    fn add(&mut self, _: &()) {}

    fn finish(self) {}
}

#[cfg(test)]
mod tests {

    use std::collections::{BTreeMap, BTreeSet};
    use std::str;

    use common::OwnedBytes;

    use super::super::{MonotonicU64SSTable, SSTable, VoidSSTable};
    use super::{U64Merge, VoidMerge};

    fn write_sstable(keys: &[&'static str]) -> OwnedBytes {
        let mut buffer: Vec<u8> = vec![];
        {
            let mut sstable_writer = VoidSSTable::writer(&mut buffer);
            for &key in keys {
                assert!(sstable_writer.insert(key.as_bytes(), &()).is_ok());
            }
            assert!(sstable_writer.finish().is_ok());
        }
        OwnedBytes::new(buffer)
    }

    fn write_sstable_u64(keys: &[(&'static str, u64)]) -> OwnedBytes {
        let mut buffer: Vec<u8> = vec![];
        {
            let mut sstable_writer = MonotonicU64SSTable::writer(&mut buffer);
            for (key, val) in keys {
                assert!(sstable_writer.insert(key.as_bytes(), val).is_ok());
            }
            assert!(sstable_writer.finish().is_ok());
        }
        OwnedBytes::new(buffer)
    }

    fn merge_test_aux(arrs: &[&[&'static str]]) {
        let sstables = arrs.iter().cloned().map(write_sstable).collect::<Vec<_>>();
        let mut merged = BTreeSet::new();
        for &arr in arrs.iter() {
            for &s in arr {
                merged.insert(s.to_string());
            }
        }
        let mut w = Vec::new();
        assert!(VoidSSTable::merge(sstables, &mut w, VoidMerge).is_ok());
        let w = OwnedBytes::new(w);
        let mut reader = VoidSSTable::reader(w);
        for k in merged {
            assert!(reader.advance().unwrap());
            assert_eq!(reader.key(), k.as_bytes());
        }
        assert!(!reader.advance().unwrap());
    }

    fn merge_test_u64_monotonic_aux(arrs: &[&[(&'static str, u64)]]) {
        let sstables = arrs
            .iter()
            .cloned()
            .map(write_sstable_u64)
            .collect::<Vec<_>>();
        let mut merged = BTreeMap::new();
        for &arr in arrs.iter() {
            for (key, val) in arr {
                let entry = merged.entry(key.to_string()).or_insert(0u64);
                *entry += val;
            }
        }
        let mut w = Vec::new();
        assert!(MonotonicU64SSTable::merge(sstables, &mut w, U64Merge).is_ok());
        let w = OwnedBytes::new(w);
        let mut reader = MonotonicU64SSTable::reader(w);
        for (k, v) in merged {
            assert!(reader.advance().unwrap());
            assert_eq!(reader.key(), k.as_bytes());
            assert_eq!(reader.value(), &v);
        }
        assert!(!reader.advance().unwrap());
    }

    #[test]
    fn test_merge_simple_reproduce() {
        let sstable_data = write_sstable(&["a"]);
        let mut reader = VoidSSTable::reader(sstable_data);
        assert!(reader.advance().unwrap());
        assert_eq!(reader.key(), b"a");
        assert!(!reader.advance().unwrap());
    }

    #[test]
    fn test_merge() {
        merge_test_aux(&[]);
        merge_test_aux(&[&["a"]]);
        merge_test_aux(&[&["a", "b"], &["ab"]]); // a, ab, b
        merge_test_aux(&[&["a", "b"], &["a", "b"]]);
        merge_test_aux(&[
            &["happy", "hello", "payer", "tax"],
            &["habitat", "hello", "zoo"],
            &[],
            &["a"],
        ]);
        merge_test_aux(&[&["a"]]);
        merge_test_aux(&[&["a", "b"], &["ab"]]);
        merge_test_aux(&[&["a", "b"], &["a", "b"]]);
    }

    #[test]
    fn test_merge_u64() {
        merge_test_u64_monotonic_aux(&[]);
        merge_test_u64_monotonic_aux(&[&[("a", 1u64)]]);
        merge_test_u64_monotonic_aux(&[&[("a", 1u64), ("b", 3u64)], &[("ab", 2u64)]]); // a, ab, b
        merge_test_u64_monotonic_aux(&[&[("a", 1u64), ("b", 2u64)], &[("a", 16u64), ("b", 23u64)]]);
    }
}
