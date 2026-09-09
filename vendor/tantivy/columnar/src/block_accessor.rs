use std::cmp::Ordering;

use crate::{Column, DocId, RowId};

#[derive(Debug, Default, Clone)]
pub struct ColumnBlockAccessor<T> {
    val_cache: Vec<T>,
    docid_cache: Vec<DocId>,
    missing_docids_cache: Vec<DocId>,
    row_id_cache: Vec<RowId>,
}

impl<T: PartialOrd + Copy + std::fmt::Debug + Send + Sync + 'static + Default>
    ColumnBlockAccessor<T>
{
    #[inline]
    pub fn fetch_block<'a>(&'a mut self, docs: &'a [u32], accessor: &Column<T>) {
        if accessor.index.get_cardinality().is_full() {
            self.val_cache.resize(docs.len(), T::default());
            accessor.values.get_vals(docs, &mut self.val_cache);
        } else {
            self.docid_cache.clear();
            self.row_id_cache.clear();
            accessor.row_ids_for_docs(docs, &mut self.docid_cache, &mut self.row_id_cache);
            self.val_cache.resize(self.row_id_cache.len(), T::default());
            accessor
                .values
                .get_vals(&self.row_id_cache, &mut self.val_cache);
        }
    }
    #[inline]
    pub fn fetch_block_with_missing(
        &mut self,
        docs: &[u32],
        accessor: &Column<T>,
        missing: Option<T>,
    ) {
        self.fetch_block(docs, accessor);
        // no missing values
        if accessor.index.get_cardinality().is_full() {
            return;
        }
        let Some(missing) = missing else {
            return;
        };

        // We can compare docid_cache length with docs to find missing docs
        // For multi value columns we can't rely on the length and always need to scan
        if accessor.index.get_cardinality().is_multivalue() || docs.len() != self.docid_cache.len()
        {
            self.missing_docids_cache.clear();
            find_missing_docs(docs, &self.docid_cache, |doc| {
                self.missing_docids_cache.push(doc);
                self.val_cache.push(missing);
            });
            self.docid_cache
                .extend_from_slice(&self.missing_docids_cache);
        }
    }

    /// Like `fetch_block_with_missing`, but deduplicates (doc_id, value) pairs
    /// so that each unique value per document is returned only once.
    ///
    /// This is necessary for correct document counting in aggregations,
    /// where multi-valued fields can produce duplicate entries that inflate counts.
    #[inline]
    pub fn fetch_block_with_missing_unique_per_doc(
        &mut self,
        docs: &[u32],
        accessor: &Column<T>,
        missing: Option<T>,
    ) where
        T: Ord,
    {
        self.fetch_block_with_missing(docs, accessor, missing);
        if accessor.index.get_cardinality().is_multivalue() {
            self.dedup_docid_val_pairs();
        }
    }

    /// Removes duplicate (doc_id, value) pairs from the caches.
    ///
    /// After `fetch_block`, entries are sorted by doc_id, but values within
    /// the same doc may not be sorted (e.g. `(0,1), (0,2), (0,1)`).
    /// We group consecutive entries by doc_id, sort values within each group
    /// if it has more than 2 elements, then deduplicate adjacent pairs.
    ///
    /// Skips entirely if no doc_id appears more than once in the block.
    fn dedup_docid_val_pairs(&mut self)
    where T: Ord {
        if self.docid_cache.len() <= 1 {
            return;
        }

        // Quick check: if no consecutive doc_ids are equal, no dedup needed.
        let has_multivalue = self.docid_cache.windows(2).any(|w| w[0] == w[1]);
        if !has_multivalue {
            return;
        }

        // Sort values within each doc_id group so duplicates become adjacent.
        let mut start = 0;
        while start < self.docid_cache.len() {
            let doc = self.docid_cache[start];
            let mut end = start + 1;
            while end < self.docid_cache.len() && self.docid_cache[end] == doc {
                end += 1;
            }
            if end - start > 2 {
                self.val_cache[start..end].sort();
            }
            start = end;
        }

        // Now duplicates are adjacent — deduplicate in place.
        let mut write = 0;
        for read in 1..self.docid_cache.len() {
            if self.docid_cache[read] != self.docid_cache[write]
                || self.val_cache[read] != self.val_cache[write]
            {
                write += 1;
                if write != read {
                    self.docid_cache[write] = self.docid_cache[read];
                    self.val_cache[write] = self.val_cache[read];
                }
            }
        }
        let new_len = write + 1;
        self.docid_cache.truncate(new_len);
        self.val_cache.truncate(new_len);
    }

    #[inline]
    pub fn iter_vals(&self) -> impl Iterator<Item = T> + '_ {
        self.val_cache.iter().cloned()
    }

    #[inline]
    /// Returns an iterator over the docids and values
    /// The passed in `docs` slice needs to be the same slice that was passed to `fetch_block` or
    /// `fetch_block_with_missing`.
    ///
    /// The docs is used if the column is full (each docs has exactly one value), otherwise the
    /// internal docid vec is used for the iterator, which e.g. may contain duplicate docs.
    pub fn iter_docid_vals<'a>(
        &'a self,
        docs: &'a [u32],
        accessor: &Column<T>,
    ) -> impl Iterator<Item = (DocId, T)> + 'a + use<'a, T> {
        if accessor.index.get_cardinality().is_full() {
            docs.iter().cloned().zip(self.val_cache.iter().cloned())
        } else {
            self.docid_cache
                .iter()
                .cloned()
                .zip(self.val_cache.iter().cloned())
        }
    }
}

/// Given two sorted lists of docids `docs` and `hits`, hits is a subset of `docs`.
/// Return all docs that are not in `hits`.
fn find_missing_docs<F>(docs: &[u32], hits: &[u32], mut callback: F)
where F: FnMut(u32) {
    let mut docs_iter = docs.iter();
    let mut hits_iter = hits.iter();

    let mut doc = docs_iter.next();
    let mut hit = hits_iter.next();

    while let (Some(&current_doc), Some(&current_hit)) = (doc, hit) {
        match current_doc.cmp(&current_hit) {
            Ordering::Less => {
                callback(current_doc);
                doc = docs_iter.next();
            }
            Ordering::Equal => {
                doc = docs_iter.next();
                hit = hits_iter.next();
            }
            Ordering::Greater => {
                hit = hits_iter.next();
            }
        }
    }

    while let Some(&current_doc) = doc {
        callback(current_doc);
        doc = docs_iter.next();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_missing_docs() {
        let docs: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let hits: Vec<u32> = vec![2, 4, 6, 8, 10];

        let mut missing_docs: Vec<u32> = Vec::new();

        find_missing_docs(&docs, &hits, |missing_doc| {
            missing_docs.push(missing_doc);
        });

        assert_eq!(missing_docs, vec![1, 3, 5, 7, 9]);
    }

    #[test]
    fn test_find_missing_docs_empty() {
        let docs: Vec<u32> = Vec::new();
        let hits: Vec<u32> = vec![2, 4, 6, 8, 10];

        let mut missing_docs: Vec<u32> = Vec::new();

        find_missing_docs(&docs, &hits, |missing_doc| {
            missing_docs.push(missing_doc);
        });

        assert_eq!(missing_docs, Vec::<u32>::new());
    }

    #[test]
    fn test_find_missing_docs_all_missing() {
        let docs: Vec<u32> = vec![1, 2, 3, 4, 5];
        let hits: Vec<u32> = Vec::new();

        let mut missing_docs: Vec<u32> = Vec::new();

        find_missing_docs(&docs, &hits, |missing_doc| {
            missing_docs.push(missing_doc);
        });

        assert_eq!(missing_docs, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_consecutive() {
        let mut accessor = ColumnBlockAccessor::<u64>::default();
        accessor.docid_cache = vec![0, 0, 2, 3];
        accessor.val_cache = vec![10, 10, 10, 10];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, vec![0, 2, 3]);
        assert_eq!(accessor.val_cache, vec![10, 10, 10]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_non_consecutive() {
        // (0,1), (0,2), (0,1) — duplicate value not adjacent
        let mut accessor = ColumnBlockAccessor::<u64>::default();
        accessor.docid_cache = vec![0, 0, 0];
        accessor.val_cache = vec![1, 2, 1];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, vec![0, 0]);
        assert_eq!(accessor.val_cache, vec![1, 2]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_multi_doc() {
        // doc 0: values [3, 1, 3], doc 1: values [5, 5]
        let mut accessor = ColumnBlockAccessor::<u64>::default();
        accessor.docid_cache = vec![0, 0, 0, 1, 1];
        accessor.val_cache = vec![3, 1, 3, 5, 5];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, vec![0, 0, 1]);
        assert_eq!(accessor.val_cache, vec![1, 3, 5]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_no_duplicates() {
        let mut accessor = ColumnBlockAccessor::<u64>::default();
        accessor.docid_cache = vec![0, 0, 1];
        accessor.val_cache = vec![1, 2, 3];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, vec![0, 0, 1]);
        assert_eq!(accessor.val_cache, vec![1, 2, 3]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_single_element() {
        let mut accessor = ColumnBlockAccessor::<u64>::default();
        accessor.docid_cache = vec![0];
        accessor.val_cache = vec![1];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, vec![0]);
        assert_eq!(accessor.val_cache, vec![1]);
    }
}
