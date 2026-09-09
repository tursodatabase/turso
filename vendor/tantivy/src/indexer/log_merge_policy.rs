use std::cmp;

use itertools::Itertools;

use super::merge_policy::{MergeCandidate, MergePolicy};
use crate::index::SegmentMeta;

const DEFAULT_LEVEL_LOG_SIZE: f64 = 0.75;
const DEFAULT_MIN_LAYER_SIZE: u32 = 10_000;
const DEFAULT_MIN_NUM_SEGMENTS_IN_MERGE: usize = 8;
const DEFAULT_MAX_DOCS_BEFORE_MERGE: usize = 10_000_000;
// The default value of 1 means that deletes are not taken in account when
// identifying merge candidates. This is not a very sensible default: it was
// set like that for backward compatibility and might change in the near future.
const DEFAULT_DEL_DOCS_RATIO_BEFORE_MERGE: f32 = 1.0f32;

/// `LogMergePolicy` tries to merge segments that have a similar number of
/// documents.
#[derive(Debug, Clone)]
pub struct LogMergePolicy {
    min_num_segments: usize,
    max_docs_before_merge: usize,
    min_layer_size: u32,
    level_log_size: f64,
    del_docs_ratio_before_merge: f32,
}

impl LogMergePolicy {
    fn clip_min_size(&self, size: u32) -> u32 {
        cmp::max(self.min_layer_size, size)
    }

    /// Set the minimum number of segments that may be merged together.
    pub fn set_min_num_segments(&mut self, min_num_segments: usize) {
        self.min_num_segments = min_num_segments;
    }

    /// Set the maximum number docs in a segment for it to be considered for
    /// merging. A segment can still reach more than max_docs, by merging many
    /// smaller ones.
    pub fn set_max_docs_before_merge(&mut self, max_docs_merge_size: usize) {
        self.max_docs_before_merge = max_docs_merge_size;
    }

    /// Set the minimum segment size under which all segment belong
    /// to the same level.
    pub fn set_min_layer_size(&mut self, min_layer_size: u32) {
        self.min_layer_size = min_layer_size;
    }

    /// Set the ratio between two consecutive levels.
    ///
    /// Segments are grouped in levels according to their sizes.
    /// These levels are defined as intervals of exponentially growing sizes.
    /// level_log_size define the factor by which one should multiply the limit
    /// to reach a level, in order to get the limit to reach the following
    /// level.
    pub fn set_level_log_size(&mut self, level_log_size: f64) {
        self.level_log_size = level_log_size;
    }

    /// Set the ratio of deleted documents in a segment to tolerate.
    ///
    /// If it is exceeded by any segment at a log level, a merge
    /// will be triggered for that level.
    ///
    /// If there is a single segment at a level, we effectively end up expunging
    /// deleted documents from it.
    ///
    /// # Panics
    ///
    /// Panics if del_docs_ratio_before_merge is not within (0..1].
    pub fn set_del_docs_ratio_before_merge(&mut self, del_docs_ratio_before_merge: f32) {
        assert!(del_docs_ratio_before_merge <= 1.0f32);
        assert!(del_docs_ratio_before_merge > 0f32);
        self.del_docs_ratio_before_merge = del_docs_ratio_before_merge;
    }

    fn has_segment_above_deletes_threshold(&self, level: &[&SegmentMeta]) -> bool {
        level
            .iter()
            .any(|segment| deletes_ratio(segment) > self.del_docs_ratio_before_merge)
    }
}

fn deletes_ratio(segment: &SegmentMeta) -> f32 {
    if segment.max_doc() == 0 {
        return 0f32;
    }
    segment.num_deleted_docs() as f32 / segment.max_doc() as f32
}

impl MergePolicy for LogMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        let size_sorted_segments = segments
            .iter()
            .filter(|seg| (seg.num_docs() as usize) <= self.max_docs_before_merge)
            .sorted_by_key(|seg| std::cmp::Reverse(seg.max_doc()))
            .collect::<Vec<&SegmentMeta>>();

        if size_sorted_segments.is_empty() {
            return vec![];
        }

        let mut current_max_log_size = f64::MAX;
        let mut levels = vec![];
        for (_, merge_group) in &size_sorted_segments.into_iter().chunk_by(|segment| {
            let segment_log_size = f64::from(self.clip_min_size(segment.num_docs())).log2();
            if segment_log_size < (current_max_log_size - self.level_log_size) {
                // update current_max_log_size to create a new group
                current_max_log_size = segment_log_size;
            }
            // return current_max_log_size to be grouped to the current group
            current_max_log_size
        }) {
            levels.push(merge_group.collect::<Vec<&SegmentMeta>>());
        }

        levels
            .iter()
            .filter(|level| {
                level.len() >= self.min_num_segments
                    || self.has_segment_above_deletes_threshold(level)
            })
            .map(|segments| MergeCandidate(segments.iter().map(|&seg| seg.id()).collect()))
            .collect()
    }
}

impl Default for LogMergePolicy {
    fn default() -> LogMergePolicy {
        LogMergePolicy {
            min_num_segments: DEFAULT_MIN_NUM_SEGMENTS_IN_MERGE,
            max_docs_before_merge: DEFAULT_MAX_DOCS_BEFORE_MERGE,
            min_layer_size: DEFAULT_MIN_LAYER_SIZE,
            level_log_size: DEFAULT_LEVEL_LOG_SIZE,
            del_docs_ratio_before_merge: DEFAULT_DEL_DOCS_RATIO_BEFORE_MERGE,
        }
    }
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;

    use super::*;
    use crate::index::{SegmentId, SegmentMetaInventory};
    use crate::schema;
    use crate::schema::INDEXED;

    static INVENTORY: Lazy<SegmentMetaInventory> = Lazy::new(SegmentMetaInventory::default);

    use crate::Index;

    #[test]
    fn create_index_test_max_merge_issue_1035() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let int_field = schema_builder.add_u64_field("intval", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);

        {
            let mut log_merge_policy = LogMergePolicy::default();
            log_merge_policy.set_min_num_segments(1);
            log_merge_policy.set_max_docs_before_merge(1);
            log_merge_policy.set_min_layer_size(0);

            let mut index_writer = index.writer_for_tests()?;
            index_writer.set_merge_policy(Box::new(log_merge_policy));

            // after every commit the merge checker is started, it will merge only segments with 1
            // element in it because of the max_docs_before_merge.
            index_writer.add_document(doc!(int_field=>1_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>2_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>3_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>4_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>5_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>6_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>7_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>8_u64))?;
            index_writer.commit()?;
        }

        let _segment_ids = index
            .searchable_segment_ids()
            .expect("Searchable segments failed.");

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_readers = searcher.segment_readers();
        for segment in segment_readers {
            if segment.num_docs() > 2 {
                panic!("segment can't have more than two segments");
            } // don't know how to wait for the merge, then it could be a simple eq
        }
        Ok(())
    }

    fn test_merge_policy() -> LogMergePolicy {
        let mut log_merge_policy = LogMergePolicy::default();
        log_merge_policy.set_min_num_segments(3);
        log_merge_policy.set_max_docs_before_merge(100_000);
        log_merge_policy.set_min_layer_size(2);
        log_merge_policy
    }

    #[test]
    fn test_log_merge_policy_empty() {
        let y = Vec::new();
        let result_list = test_merge_policy().compute_merge_candidates(&y);
        assert!(result_list.is_empty());
    }

    fn create_random_segment_meta(num_docs: u32) -> SegmentMeta {
        INVENTORY.new_segment_meta(SegmentId::generate_random(), num_docs)
    }

    #[test]
    fn test_log_merge_policy_pair() {
        let test_input = vec![
            create_random_segment_meta(10),
            create_random_segment_meta(10),
            create_random_segment_meta(10),
        ];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 1);
    }

    #[test]
    fn test_log_merge_policy_levels() {
        // multiple levels all get merged correctly
        // 2 MergeCandidates expected:
        // * one with the 6 * 10-docs segments
        // * one with the 3 * 1000-docs segments
        // no MergeCandidate expected for the 2 * 10_000-docs segments as min_merge_size=3
        let test_input = vec![
            create_random_segment_meta(10),
            create_random_segment_meta(10),
            create_random_segment_meta(10),
            create_random_segment_meta(1_000),
            create_random_segment_meta(1_000),
            create_random_segment_meta(1_000),
            create_random_segment_meta(10_000),
            create_random_segment_meta(10_000),
            create_random_segment_meta(10),
            create_random_segment_meta(10),
            create_random_segment_meta(10),
        ];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 2);
    }

    #[test]
    fn test_log_merge_policy_within_levels() {
        // multiple levels all get merged correctly
        let test_input = vec![
            create_random_segment_meta(10),   // log2(10) = ~3.32 (> 3.58 - 0.75)
            create_random_segment_meta(11),   // log2(11) = ~3.46
            create_random_segment_meta(12),   // log2(12) = ~3.58
            create_random_segment_meta(800),  // log2(800) = ~9.64 (> 9.97 - 0.75)
            create_random_segment_meta(1000), // log2(1000) = ~9.97
            create_random_segment_meta(1000),
        ]; // log2(1000) = ~9.97
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 2);
    }

    #[test]
    fn test_log_merge_policy_small_segments() {
        // segments under min_layer_size are merged together
        let test_input = vec![
            create_random_segment_meta(1),
            create_random_segment_meta(1),
            create_random_segment_meta(1),
            create_random_segment_meta(2),
            create_random_segment_meta(2),
            create_random_segment_meta(2),
        ];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 1);
    }

    #[test]
    fn test_log_merge_policy_all_segments_too_large_to_merge() {
        let eight_large_segments: Vec<SegmentMeta> =
            std::iter::repeat_with(|| create_random_segment_meta(100_001))
                .take(8)
                .collect();
        assert!(test_merge_policy()
            .compute_merge_candidates(&eight_large_segments)
            .is_empty());
    }

    #[test]
    fn test_large_merge_segments() {
        let test_input = vec![
            create_random_segment_meta(1_000_000),
            create_random_segment_meta(100_001),
            create_random_segment_meta(100_000),
            create_random_segment_meta(1_000_001),
            create_random_segment_meta(100_000),
            create_random_segment_meta(100_000),
            create_random_segment_meta(1_500_000),
        ];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        // Do not include large segments
        assert_eq!(result_list.len(), 1);
        assert_eq!(result_list[0].0.len(), 3);

        // Making sure merge policy points to the correct index of the original input
        assert_eq!(result_list[0].0[0], test_input[2].id());
        assert_eq!(result_list[0].0[1], test_input[4].id());
        assert_eq!(result_list[0].0[2], test_input[5].id());
    }

    #[test]
    fn test_merge_single_segment_with_deletes_below_threshold() {
        let mut test_merge_policy = test_merge_policy();
        test_merge_policy.set_del_docs_ratio_before_merge(0.25f32);
        let test_input = vec![create_random_segment_meta(40_000).with_delete_meta(10_000, 1)];
        let merge_candidates = test_merge_policy.compute_merge_candidates(&test_input);
        assert!(merge_candidates.is_empty());
    }

    #[test]
    fn test_merge_single_segment_with_deletes_above_threshold() {
        let mut test_merge_policy = test_merge_policy();
        test_merge_policy.set_del_docs_ratio_before_merge(0.25f32);
        let test_input = vec![create_random_segment_meta(40_000).with_delete_meta(10_001, 1)];
        let merge_candidates = test_merge_policy.compute_merge_candidates(&test_input);
        assert_eq!(merge_candidates.len(), 1);
    }

    #[test]
    fn test_merge_segments_with_deletes_above_threshold_all_in_level() {
        let mut test_merge_policy = test_merge_policy();
        test_merge_policy.set_del_docs_ratio_before_merge(0.25f32);
        let test_input = vec![
            create_random_segment_meta(40_000).with_delete_meta(10_001, 1),
            create_random_segment_meta(40_000),
        ];
        let merge_candidates = test_merge_policy.compute_merge_candidates(&test_input);
        assert_eq!(merge_candidates.len(), 1);
        assert_eq!(merge_candidates[0].0.len(), 2);
    }

    #[test]
    fn test_merge_segments_with_deletes_above_threshold_different_level_not_involved() {
        let mut test_merge_policy = test_merge_policy();
        test_merge_policy.set_del_docs_ratio_before_merge(0.25f32);
        let test_input = vec![
            create_random_segment_meta(100),
            create_random_segment_meta(40_000).with_delete_meta(10_001, 1),
        ];
        let merge_candidates = test_merge_policy.compute_merge_candidates(&test_input);
        assert_eq!(merge_candidates.len(), 1);
        assert_eq!(merge_candidates[0].0.len(), 1);
        assert_eq!(merge_candidates[0].0[0], test_input[1].id());
    }

    #[test]
    fn test_max_docs_before_merge_large_value() {
        // Regression test: (max_docs_before_merge as u32) truncates values > u32::MAX.
        // Casting num_docs() to usize instead avoids the truncation.
        let mut policy = LogMergePolicy::default();
        policy.set_min_num_segments(2);
        policy.set_max_docs_before_merge(5_000_000_000usize);
        let test_input = vec![
            create_random_segment_meta(100_000),
            create_random_segment_meta(100_000),
        ];
        let result = policy.compute_merge_candidates(&test_input);
        // Both segments should be eligible (100_000 < 5_000_000_000)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.len(), 2);
    }
}
