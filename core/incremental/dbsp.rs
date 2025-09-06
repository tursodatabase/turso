// Simplified DBSP integration for incremental view maintenance
// For now, we'll use a basic approach and can expand to full DBSP later

use super::hashable_row::HashableRow;
use crate::Value;
use std::collections::{BTreeMap, HashMap};

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
}
