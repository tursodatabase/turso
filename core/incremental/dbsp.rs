// Simplified DBSP integration for incremental view maintenance
// For now, we'll use a basic approach and can expand to full DBSP later

use crate::Value;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

/// A 128-bit hash value implemented as a UUID
/// We use UUID because it's a standard 128-bit type we already depend on
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hash128 {
    // Store as UUID internally for efficient 128-bit representation
    uuid: uuid::Uuid,
}

impl Hash128 {
    /// Create a new 128-bit hash from high and low 64-bit parts
    pub fn new(high: u64, low: u64) -> Self {
        // Convert two u64 values to UUID bytes (big-endian)
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&high.to_be_bytes());
        bytes[8..16].copy_from_slice(&low.to_be_bytes());
        Self {
            uuid: uuid::Uuid::from_bytes(bytes),
        }
    }

    /// Get the low 64 bits as i64 (for when we need a rowid)
    pub fn as_i64(&self) -> i64 {
        let bytes = self.uuid.as_bytes();
        let low = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        low as i64
    }

    /// Compute a 128-bit hash of the given values
    /// We serialize values to a string representation and use UUID v5 (SHA-1 based)
    /// to get a deterministic 128-bit hash
    pub fn hash_values(values: &[Value]) -> Self {
        // Build a string representation of all values
        // Use a delimiter that won't appear in normal values
        let mut s = String::new();
        for (i, value) in values.iter().enumerate() {
            if i > 0 {
                s.push('\x00'); // null byte as delimiter
            }
            // Add type prefix to distinguish between types
            match value {
                Value::Null => s.push_str("N:"),
                Value::Integer(n) => {
                    s.push_str("I:");
                    s.push_str(&n.to_string());
                }
                Value::Float(f) => {
                    s.push_str("F:");
                    // Use to_bits to ensure consistent representation
                    s.push_str(&f.to_bits().to_string());
                }
                Value::Text(t) => {
                    s.push_str("T:");
                    s.push_str(t.as_str());
                }
                Value::Blob(b) => {
                    s.push_str("B:");
                    s.push_str(&hex::encode(b));
                }
            }
        }

        Self::hash_str(&s)
    }

    /// Hash a string value to 128 bits using UUID v5
    pub fn hash_str(s: &str) -> Self {
        // Use UUID v5 with a fixed namespace to get deterministic 128-bit hashes
        // We use the DNS namespace as it's a standard choice
        let uuid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, s.as_bytes());
        Self { uuid }
    }

    /// Convert to a big-endian byte array for storage
    pub fn to_blob(self) -> Vec<u8> {
        self.uuid.as_bytes().to_vec()
    }

    /// Create from a big-endian byte array
    pub fn from_blob(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 16 {
            return None;
        }

        let mut uuid_bytes = [0u8; 16];
        uuid_bytes.copy_from_slice(bytes);
        Some(Self {
            uuid: uuid::Uuid::from_bytes(uuid_bytes),
        })
    }

    /// Convert to a Value::Blob for storage
    pub fn to_value(self) -> Value {
        Value::Blob(self.to_blob())
    }

    /// Try to extract a Hash128 from a Value
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Blob(b) => Self::from_blob(b),
            _ => None,
        }
    }
}

impl std::fmt::Display for Hash128 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.uuid)
    }
}

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
    cached_hash: Hash128,
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

    fn compute_hash(rowid: i64, values: &[Value]) -> Hash128 {
        // Include rowid in the hash by prepending it to values
        let mut all_values = Vec::with_capacity(values.len() + 1);
        all_values.push(Value::Integer(rowid));
        all_values.extend_from_slice(values);
        Hash128::hash_values(&all_values)
    }

    pub fn cached_hash(&self) -> Hash128 {
        self.cached_hash
    }
}

impl Hash for HashableRow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the 128-bit value by hashing both parts
        self.cached_hash.to_blob().hash(state);
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

type DeltaEntry = (HashableRow, isize);
/// A delta represents ordered changes to data
#[derive(Debug, Clone, Default)]
pub struct Delta {
    /// Ordered list of changes: (row, weight) where weight is +1 for insert, -1 for delete
    /// It is crucial that this is ordered. Imagine the case of an update, which becomes a delete +
    /// insert. If this is not ordered, it would be applied in arbitrary order and break the view.
    pub changes: Vec<DeltaEntry>,
}

impl Delta {
    pub fn new() -> Self {
        Self {
            changes: Vec::new(),
        }
    }

    pub fn insert(&mut self, row_key: i64, values: Vec<Value>) {
        let row = HashableRow::new(row_key, values);
        self.changes.push((row, 1));
    }

    pub fn delete(&mut self, row_key: i64, values: Vec<Value>) {
        let row = HashableRow::new(row_key, values);
        self.changes.push((row, -1));
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// Merge another delta into this one
    /// This preserves the order of operations - no consolidation is done
    /// to maintain the full history of changes
    pub fn merge(&mut self, other: &Delta) {
        // Simply append all changes from other, preserving order
        self.changes.extend(other.changes.iter().cloned());
    }

    /// Consolidate changes by combining entries with the same HashableRow
    pub fn consolidate(&mut self) {
        if self.changes.is_empty() {
            return;
        }

        // Use a HashMap to accumulate weights
        let mut consolidated: HashMap<HashableRow, isize> = HashMap::new();

        for (row, weight) in self.changes.drain(..) {
            *consolidated.entry(row).or_insert(0) += weight;
        }

        // Convert back to vec, filtering out zero weights
        self.changes = consolidated
            .into_iter()
            .filter(|(_, weight)| *weight != 0)
            .collect();
    }
}

/// A pair of deltas for operators that process two inputs
#[derive(Debug, Clone, Default)]
pub struct DeltaPair {
    pub left: Delta,
    pub right: Delta,
}

impl DeltaPair {
    /// Create a new delta pair
    pub fn new(left: Delta, right: Delta) -> Self {
        Self { left, right }
    }
}

impl From<Delta> for DeltaPair {
    /// Convert a single delta into a delta pair with empty right delta
    fn from(delta: Delta) -> Self {
        Self {
            left: delta,
            right: Delta::new(),
        }
    }
}

impl From<&Delta> for DeltaPair {
    /// Convert a delta reference into a delta pair with empty right delta
    fn from(delta: &Delta) -> Self {
        Self {
            left: delta.clone(),
            right: Delta::new(),
        }
    }
}

/// A simplified ZSet for incremental computation
/// Each element has a weight: positive for additions, negative for deletions
#[derive(Clone, Debug, Default)]
pub struct SimpleZSet<T> {
    data: BTreeMap<T, isize>,
}

#[allow(dead_code)]
impl<T: std::hash::Hash + Eq + Ord + Clone> SimpleZSet<T> {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, item: T, weight: isize) {
        let current = self.data.get(&item).copied().unwrap_or(0);
        let new_weight = current + weight;
        if new_weight == 0 {
            self.data.remove(&item);
        } else {
            self.data.insert(item, new_weight);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&T, isize)> {
        self.data.iter().map(|(k, &v)| (k, v))
    }

    /// Get all items with positive weights
    pub fn to_vec(&self) -> Vec<T> {
        self.data
            .iter()
            .filter(|(_, &weight)| weight > 0)
            .map(|(item, _)| item.clone())
            .collect()
    }

    pub fn merge(&mut self, other: &SimpleZSet<T>) {
        for (item, weight) in other.iter() {
            self.insert(item.clone(), weight);
        }
    }

    /// Get the weight for a specific item (0 if not present)
    pub fn get(&self, item: &T) -> isize {
        self.data.get(item).copied().unwrap_or(0)
    }

    /// Get the first element (smallest key) in the Z-set
    pub fn first(&self) -> Option<(&T, isize)> {
        self.data.iter().next().map(|(k, &v)| (k, v))
    }

    /// Get the last element (largest key) in the Z-set
    pub fn last(&self) -> Option<(&T, isize)> {
        self.data.iter().next_back().map(|(k, &v)| (k, v))
    }

    /// Get a range of elements
    pub fn range<R>(&self, range: R) -> impl Iterator<Item = (&T, isize)> + '_
    where
        R: std::ops::RangeBounds<T>,
    {
        self.data.range(range).map(|(k, &v)| (k, v))
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

// Type aliases for convenience
pub type RowKey = HashableRow;
pub type RowKeyZSet = SimpleZSet<RowKey>;

impl RowKeyZSet {
    /// Create a Z-set from a Delta by consolidating all changes
    pub fn from_delta(delta: &Delta) -> Self {
        let mut zset = Self::new();

        // Add all changes from the delta, consolidating as we go
        for (row, weight) in &delta.changes {
            zset.insert(row.clone(), *weight);
        }

        zset
    }

    /// Seek to find ALL entries for the best matching rowid
    /// For GT/GE: returns all entries for the smallest rowid that satisfies the condition
    /// For LT/LE: returns all entries for the largest rowid that satisfies the condition
    /// Returns empty vec if no match found
    pub fn seek(&self, target: i64, op: crate::types::SeekOp) -> Vec<(HashableRow, isize)> {
        use crate::types::SeekOp;

        // First find the best matching rowid
        let best_rowid = match op {
            SeekOp::GT => {
                // Find smallest rowid > target
                self.data
                    .iter()
                    .filter(|(row, _)| row.rowid > target)
                    .map(|(row, _)| row.rowid)
                    .min()
            }
            SeekOp::GE { eq_only: false } => {
                // Find smallest rowid >= target
                self.data
                    .iter()
                    .filter(|(row, _)| row.rowid >= target)
                    .map(|(row, _)| row.rowid)
                    .min()
            }
            SeekOp::GE { eq_only: true } | SeekOp::LE { eq_only: true } => {
                // Need exact match
                if self.data.iter().any(|(row, _)| row.rowid == target) {
                    Some(target)
                } else {
                    None
                }
            }
            SeekOp::LT => {
                // Find largest rowid < target
                self.data
                    .iter()
                    .filter(|(row, _)| row.rowid < target)
                    .map(|(row, _)| row.rowid)
                    .max()
            }
            SeekOp::LE { eq_only: false } => {
                // Find largest rowid <= target
                self.data
                    .iter()
                    .filter(|(row, _)| row.rowid <= target)
                    .map(|(row, _)| row.rowid)
                    .max()
            }
        };

        // Now get ALL entries with that rowid
        match best_rowid {
            Some(rowid) => self
                .data
                .iter()
                .filter(|(row, _)| row.rowid == rowid)
                .map(|(k, &v)| (k.clone(), v))
                .collect(),
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zset_merge_with_weights() {
        let mut zset1 = SimpleZSet::new();
        zset1.insert(1, 1); // Row 1 with weight +1
        zset1.insert(2, 1); // Row 2 with weight +1

        let mut zset2 = SimpleZSet::new();
        zset2.insert(2, -1); // Row 2 with weight -1 (delete)
        zset2.insert(3, 1); // Row 3 with weight +1 (insert)

        zset1.merge(&zset2);

        // Row 1: weight 1 (unchanged)
        // Row 2: weight 0 (deleted)
        // Row 3: weight 1 (inserted)
        assert_eq!(zset1.iter().count(), 2); // Only rows 1 and 3
        assert!(zset1.iter().any(|(k, _)| *k == 1));
        assert!(zset1.iter().any(|(k, _)| *k == 3));
        assert!(!zset1.iter().any(|(k, _)| *k == 2)); // Row 2 removed
    }

    #[test]
    fn test_zset_represents_updates_as_delete_plus_insert() {
        let mut zset = SimpleZSet::new();

        // Initial state
        zset.insert(1, 1);

        // Update row 1: delete old + insert new
        zset.insert(1, -1); // Delete old version
        zset.insert(1, 1); // Insert new version

        // Weight should be 1 (not 2)
        let weight = zset.iter().find(|(k, _)| **k == 1).map(|(_, w)| w);
        assert_eq!(weight, Some(1));
    }

    #[test]
    fn test_hashable_row_delta_operations() {
        let mut delta = Delta::new();

        // Test INSERT
        delta.insert(1, vec![Value::Integer(1), Value::Integer(100)]);
        assert_eq!(delta.len(), 1);

        // Test UPDATE (DELETE + INSERT) - order matters!
        delta.delete(1, vec![Value::Integer(1), Value::Integer(100)]);
        delta.insert(1, vec![Value::Integer(1), Value::Integer(200)]);
        assert_eq!(delta.len(), 3); // Should have 3 operations before consolidation

        // Verify order is preserved
        let ops: Vec<_> = delta.changes.iter().collect();
        assert_eq!(ops[0].1, 1); // First insert
        assert_eq!(ops[1].1, -1); // Delete
        assert_eq!(ops[2].1, 1); // Second insert

        // Test consolidation
        delta.consolidate();
        // After consolidation, the first insert and delete should cancel out
        // leaving only the second insert
        assert_eq!(delta.len(), 1);

        let final_row = &delta.changes[0];
        assert_eq!(final_row.0.rowid, 1);
        assert_eq!(
            final_row.0.values,
            vec![Value::Integer(1), Value::Integer(200)]
        );
        assert_eq!(final_row.1, 1);
    }

    #[test]
    fn test_duplicate_row_consolidation() {
        let mut delta = Delta::new();

        // Insert same row twice
        delta.insert(2, vec![Value::Integer(2), Value::Integer(300)]);
        delta.insert(2, vec![Value::Integer(2), Value::Integer(300)]);

        assert_eq!(delta.len(), 2);

        delta.consolidate();
        assert_eq!(delta.len(), 1);

        // Weight should be 2 (sum of both inserts)
        let final_row = &delta.changes[0];
        assert_eq!(final_row.0.rowid, 2);
        assert_eq!(final_row.1, 2);
    }
}
