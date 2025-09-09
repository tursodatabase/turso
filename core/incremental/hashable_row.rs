use crate::types::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// The DBSP paper uses as a key the whole record, with both the row key and the values.  This is a
// bit confuses for us in databases, because when you say "key", it is easy to understand that as
// being the row key.
//
// Empirically speaking, using row keys as the ZSet keys will waste a competent but not brilliant
// engineer around 82 and 88 hours, depending on how you count. Hours that are never coming back.
//
// One of the situations in which using row keys completely breaks are table updates. If the "key"
// is the row key, let's say "5", then an update is a delete + insert. Imagine a table that had k =
// 5, v = 5, and a view that filters v > 2.
//
// Now we will do an update that changes v => 1. If the "key" is 5, then inside the Delta set, we
// will have (5, weight = -1), (5, weight = +1), and the whole thing just disappears. The Delta
// set, therefore, has to contain ((5, 5), weight = -1), ((5, 1), weight = +1).
//
// It is theoretically possible to use the rowkey in the ZSet and then use a hash of key ->
// Vec(changes) in the Delta set. But deviating from the paper here is just asking for trouble, as
// I am sure it would break somewhere else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashableRow {
    pub rowid: i64,
    pub values: Vec<Value>,
    // Pre-computed hash: DBSP rows are immutable and frequently hashed during joins,
    // making caching worthwhile despite the memory overhead
    cached_hash: u64,
}

impl HashableRow {
    pub fn new(rowid: i64, values: Vec<Value>) -> Self {
        let cached_hash = Self::compute_hash(rowid, &values);
        Self {
            rowid,
            values,
            cached_hash,
        }
    }

    fn compute_hash(rowid: i64, values: &[Value]) -> u64 {
        let mut hasher = DefaultHasher::new();

        rowid.hash(&mut hasher);

        for value in values {
            match value {
                Value::Null => {
                    0u8.hash(&mut hasher);
                }
                Value::Integer(i) => {
                    1u8.hash(&mut hasher);
                    i.hash(&mut hasher);
                }
                Value::Float(f) => {
                    2u8.hash(&mut hasher);
                    f.to_bits().hash(&mut hasher);
                }
                Value::Text(s) => {
                    3u8.hash(&mut hasher);
                    s.value.hash(&mut hasher);
                    (s.subtype as u8).hash(&mut hasher);
                }
                Value::Blob(b) => {
                    4u8.hash(&mut hasher);
                    b.hash(&mut hasher);
                }
            }
        }

        hasher.finish()
    }
}

impl Hash for HashableRow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cached_hash.hash(state);
    }
}

impl PartialOrd for HashableRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HashableRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare by rowid, then by values if rowids are equal
        // This ensures Ord is consistent with Eq (which compares all fields)
        match self.rowid.cmp(&other.rowid) {
            std::cmp::Ordering::Equal => {
                // If rowids are equal, compare values to maintain consistency with Eq
                self.values.cmp(&other.values)
            }
            other => other,
        }
    }
}
