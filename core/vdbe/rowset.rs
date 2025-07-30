// RowSet implementation - a data structure for efficiently storing and testing sets of integer rowids
//
// Note: SQLite uses a complex two-tiered approach with different data structures for different set sizes.
// For simplicity, we use only a sorted array approach and don't implement the complex forest structure.
#[derive(Debug, Clone)]
pub struct RowSet {
    values: Vec<i64>,   // Sorted array of unique values
    batch: i32, // Batch number - used to optimize when to test vs insert (interface compatibility)
    next_index: usize, // Current position for next() operations
    next_started: bool, // Whether next() has been called
}

impl RowSet {
    pub fn new() -> Self {
        RowSet {
            values: Vec::new(),
            batch: 0,
            next_index: 0,
            next_started: false,
        }
    }

    pub fn insert(&mut self, value: i64) {
        assert!(
            !self.next_started,
            "Cannot insert after next() has been called"
        );

        match self.values.binary_search(&value) {
            Ok(_) => {} // Value already exists, do nothing
            Err(pos) => self.values.insert(pos, value),
        }
    }

    pub fn test(&mut self, batch: i32, value: i64) -> bool {
        assert!(
            !self.next_started,
            "Cannot test after next() has been called"
        );

        self.batch = batch;

        // If batch is 0, don't test - just return false (SQLite behavior)
        if batch == 0 {
            return false;
        }

        self.values.binary_search(&value).is_ok()
    }

    pub fn next(&mut self) -> Option<i64> {
        if !self.next_started {
            self.next_started = true;
            self.next_index = 0;
        }

        if self.next_index < self.values.len() {
            let value = self.values[self.next_index];
            self.next_index += 1;
            Some(value)
        } else {
            None
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(b"ROWSET\x00\x01");
        bytes.extend_from_slice(&(self.batch as u32).to_le_bytes());
        bytes.extend_from_slice(&[if self.next_started { 1 } else { 0 }]);
        bytes.extend_from_slice(&(self.next_index as u32).to_le_bytes());

        bytes.extend_from_slice(&(self.values.len() as u32).to_le_bytes());
        for &value in &self.values {
            bytes.extend_from_slice(&value.to_le_bytes());
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 21 || &bytes[0..8] != b"ROWSET\x00\x01" {
            return None;
        }

        let mut pos = 8;
        let batch =
            i32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]);
        pos += 4;

        let next_started = bytes[pos] != 0;
        pos += 1;

        let next_index =
            u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        pos += 4;

        let values_count =
            u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        pos += 4;

        if pos + values_count * 8 > bytes.len() {
            return None;
        }

        let mut values = Vec::with_capacity(values_count);
        for _ in 0..values_count {
            let value = i64::from_le_bytes([
                bytes[pos],
                bytes[pos + 1],
                bytes[pos + 2],
                bytes[pos + 3],
                bytes[pos + 4],
                bytes[pos + 5],
                bytes[pos + 6],
                bytes[pos + 7],
            ]);
            values.push(value);
            pos += 8;
        }

        Some(RowSet {
            values,
            batch,
            next_index,
            next_started,
        })
    }

    // Helper methods for testing
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[cfg(test)]
    pub fn contains(&self, value: i64) -> bool {
        self.values.binary_search(&value).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rowset_new() {
        let rowset = RowSet::new();
        assert!(rowset.is_empty());
        assert_eq!(rowset.len(), 0);
        assert_eq!(rowset.batch, 0);
        assert!(!rowset.next_started);
    }

    #[test]
    fn test_rowset_insert() {
        let mut rowset = RowSet::new();

        // Insert values in non-sorted order
        rowset.insert(5);
        rowset.insert(1);
        rowset.insert(3);
        rowset.insert(1); // Duplicate should be ignored

        assert_eq!(rowset.len(), 3);
        assert!(rowset.contains(1));
        assert!(rowset.contains(3));
        assert!(rowset.contains(5));
        assert!(!rowset.contains(2));

        // Values should be stored in sorted order
        assert_eq!(rowset.values, vec![1, 3, 5]);
    }

    #[test]
    fn test_rowset_test() {
        let mut rowset = RowSet::new();
        rowset.insert(10);
        rowset.insert(20);
        rowset.insert(30);

        // Test existing values
        assert!(rowset.test(1, 20));
        assert!(rowset.test(2, 10));
        assert!(rowset.test(3, 30));

        // Test non-existing values
        assert!(!rowset.test(1, 15));
        assert!(!rowset.test(2, 25));

        // Batch number should be updated
        assert_eq!(rowset.batch, 2);
    }

    #[test]
    fn test_rowset_next() {
        let mut rowset = RowSet::new();
        rowset.insert(30);
        rowset.insert(10);
        rowset.insert(20);

        // Values should be returned in sorted order
        assert_eq!(rowset.next(), Some(10));
        assert_eq!(rowset.next(), Some(20));
        assert_eq!(rowset.next(), Some(30));
        assert_eq!(rowset.next(), None);
        assert_eq!(rowset.next(), None); // Should continue returning None
    }

    #[test]
    fn test_rowset_next_empty() {
        let mut rowset = RowSet::new();
        assert_eq!(rowset.next(), None);
        assert_eq!(rowset.next(), None);
    }

    #[test]
    #[should_panic(expected = "Cannot insert after next() has been called")]
    fn test_rowset_insert_after_next() {
        let mut rowset = RowSet::new();
        rowset.insert(1);
        rowset.next();
        rowset.insert(2); // Should panic
    }

    #[test]
    #[should_panic(expected = "Cannot test after next() has been called")]
    fn test_rowset_test_after_next() {
        let mut rowset = RowSet::new();
        rowset.insert(1);
        rowset.next();
        rowset.test(1, 1); // Should panic
    }

    #[test]
    fn test_rowset_serialization() {
        let mut rowset = RowSet::new();
        rowset.insert(100);
        rowset.insert(200);
        rowset.insert(50);
        rowset.batch = 42;

        let bytes = rowset.to_bytes();
        let deserialized = RowSet::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.values, vec![50, 100, 200]);
        assert_eq!(deserialized.batch, 42);
        assert!(!deserialized.next_started);
        assert_eq!(deserialized.next_index, 0);
    }

    #[test]
    fn test_rowset_serialization_with_next_state() {
        let mut rowset = RowSet::new();
        rowset.insert(1);
        rowset.insert(2);
        rowset.insert(3);

        // Advance next state
        assert_eq!(rowset.next(), Some(1));
        assert_eq!(rowset.next(), Some(2));

        let bytes = rowset.to_bytes();
        let mut deserialized = RowSet::from_bytes(&bytes).unwrap();

        assert!(deserialized.next_started);
        assert_eq!(deserialized.next_index, 2);

        // Should continue from where we left off
        assert_eq!(deserialized.next(), Some(3));
        assert_eq!(deserialized.next(), None);
    }

    #[test]
    fn test_rowset_serialization_invalid_data() {
        // Too short
        assert!(RowSet::from_bytes(&[1, 2, 3]).is_none());

        // Wrong magic
        assert!(RowSet::from_bytes(b"INVALID\x00\x01abcdefghijk").is_none());

        // Incomplete data
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"ROWSET\x00\x01");
        bytes.extend_from_slice(&(0i32).to_le_bytes()); // batch
        bytes.extend_from_slice(&[0]); // next_started
        bytes.extend_from_slice(&(0u32).to_le_bytes()); // next_index
        bytes.extend_from_slice(&(1u32).to_le_bytes()); // values_count = 1
                                                        // Missing the actual value data
        assert!(RowSet::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_rowset_large_values() {
        let mut rowset = RowSet::new();
        let large_values = vec![i64::MIN, -1000000, 0, 1000000, i64::MAX];

        for &val in &large_values {
            rowset.insert(val);
        }

        // Should handle large values correctly
        for &val in &large_values {
            assert!(rowset.contains(val));
        }

        // Test serialization with large values
        let bytes = rowset.to_bytes();
        let deserialized = RowSet::from_bytes(&bytes).unwrap();

        for &val in &large_values {
            assert!(deserialized.contains(val));
        }
    }

    #[test]
    fn test_rowset_batch_behavior() {
        let mut rowset = RowSet::new();
        rowset.insert(10);
        rowset.insert(20);

        // Batch 0 should not test, just return false
        assert!(!rowset.test(0, 10));
        assert!(!rowset.test(0, 30));

        // Non-zero batch should test normally
        assert!(rowset.test(1, 10));
        assert!(!rowset.test(1, 30));

        // Batch should be updated
        assert_eq!(rowset.batch, 1);
    }
}
