use std::cmp::Ordering;

use crate::docset::{DocSet, SeekDangerResult, TERMINATED};
use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::bm25::Bm25Weight;
use crate::query::{Intersection, Scorer};
use crate::{DocId, Score};

struct PostingsWithOffset<TPostings> {
    offset: u32,
    postings: TPostings,
}

impl<TPostings: Postings> PostingsWithOffset<TPostings> {
    pub fn new(segment_postings: TPostings, offset: u32) -> PostingsWithOffset<TPostings> {
        PostingsWithOffset {
            offset,
            postings: segment_postings,
        }
    }

    pub fn positions(&mut self, output: &mut Vec<u32>) {
        self.postings.positions_with_offset(self.offset, output)
    }
}

impl<TPostings: Postings> DocSet for PostingsWithOffset<TPostings> {
    fn advance(&mut self) -> DocId {
        self.postings.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.postings.seek(target)
    }

    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}

pub struct PhraseScorer<TPostings: Postings> {
    intersection_docset: Intersection<PostingsWithOffset<TPostings>, PostingsWithOffset<TPostings>>,
    num_terms: usize,
    left_positions: Vec<u32>,
    right_positions: Vec<u32>,
    phrase_count: u32,
    fieldnorm_reader: FieldNormReader,
    similarity_weight_opt: Option<Bm25Weight>,
    slop: u32,
    left_slops: Vec<u8>,
    positions_buffer: Vec<u32>,
    slops_buffer: Vec<u8>,
}

/// Returns true if and only if the two sorted arrays contain a common element
fn intersection_exists(left: &[u32], right: &[u32]) -> bool {
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() && right_index < right.len() {
        let left_val = left[left_index];
        let right_val = right[right_index];
        match left_val.cmp(&right_val) {
            Ordering::Less => {
                left_index += 1;
            }
            Ordering::Equal => {
                return true;
            }
            Ordering::Greater => {
                right_index += 1;
            }
        }
    }
    false
}

pub(crate) fn intersection_count(left: &[u32], right: &[u32]) -> usize {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;
    while left_index < left.len() && right_index < right.len() {
        let left_val = left[left_index];
        let right_val = right[right_index];
        match left_val.cmp(&right_val) {
            Ordering::Less => {
                left_index += 1;
            }
            Ordering::Equal => {
                count += 1;
                left_index += 1;
                right_index += 1;
            }
            Ordering::Greater => {
                right_index += 1;
            }
        }
    }
    count
}

/// Intersect twos sorted arrays `left` and `right` and outputs the
/// resulting array in left.
///
/// Returns the length of the intersection
#[inline]
fn intersection(left: &mut Vec<u32>, right: &[u32]) {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;
    let left_len = left.len();
    let right_len = right.len();
    while left_index < left_len && right_index < right_len {
        let left_val = left[left_index];
        let right_val = right[right_index];
        match left_val.cmp(&right_val) {
            Ordering::Less => {
                left_index += 1;
            }
            Ordering::Equal => {
                left[count] = left_val;
                count += 1;
                left_index += 1;
                right_index += 1;
            }
            Ordering::Greater => {
                right_index += 1;
            }
        }
    }
    left.truncate(count);
}

/// Intersect twos sorted arrays `left` and `right` and outputs the
/// resulting array in left_positions if update_left is true.
///
/// Condition for match is that the distance between left and right is less than or equal to `slop`.
///
/// Returns the length of the intersection
#[inline]
fn intersection_count_with_slop(
    left_positions: &mut Vec<u32>,
    right_positions: &[u32],
    slop: u32,
    update_left: bool,
) -> usize {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;
    let left_len = left_positions.len();
    let right_len = right_positions.len();
    while left_index < left_len && right_index < right_len {
        let left_val = left_positions[left_index];
        let right_val = right_positions[right_index];

        let distance = left_val.abs_diff(right_val);
        if distance <= slop {
            while left_index + 1 < left_len {
                // there could be a better match
                let next_left_val = left_positions[left_index + 1];
                if next_left_val > right_val {
                    // the next value is outside the range, so current one is the best.
                    break;
                }
                // the next value is better.
                left_index += 1;
            }

            // store the match in left.
            if update_left {
                left_positions[count] = right_val;
            }
            count += 1;
            left_index += 1;
            right_index += 1;
        } else if left_val < right_val {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }
    if update_left {
        left_positions.truncate(count);
    }

    count
}

fn intersection_exists_with_slop(
    left_positions: &[u32],
    right_positions: &[u32],
    slop: u32,
) -> bool {
    let mut left_index = 0;
    let mut right_index = 0;
    let left_len = left_positions.len();
    let right_len = right_positions.len();
    while left_index < left_len && right_index < right_len {
        let left_val = left_positions[left_index];
        let right_val = right_positions[right_index];
        let distance = left_val.abs_diff(right_val);
        if distance <= slop {
            return true;
        } else if left_val < right_val {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }
    false
}

/// Intersection variant for multi term searches that keeps track of slop so far.
///
/// In contrast to the regular algorithm this solves some issues:
/// - Keep track of the slop so far. Slop is a budget that is spent on the distance between terms.
/// - When encountering a match between two positions, which position is the best match is unclear
///   and depends on intersections afterwards, therefore this algorithm keeps left and right as
///   matches, but only counts one.
///
/// This algorithm may return an incorrect count in some cases (e.g. left, right expansion and is
/// then matches both on the following term.)
/// I think to fix this we would need to iterate all positions simultaneously,
/// but not sure if that's worth it. (It may be considerable slower - untested)
///
/// left_slops is allowed to be empty, which equals to a slop of 0 so far.
#[inline]
fn intersection_count_with_carrying_slop(
    left_positions: &mut Vec<u32>,
    left_slops: &mut Vec<u8>,
    right_positions: &[u32],
    max_slop: u32,
    update_left: bool,
    positions_buffer: &mut Vec<u32>,
    slops_buffer: &mut Vec<u8>,
) -> u32 {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;

    if left_positions.is_empty() || right_positions.is_empty() {
        if update_left {
            left_positions.clear();
            left_slops.clear();
        }
        return 0;
    }

    let add_val = |val: (u8, u32), new_left: &mut Vec<u32>, new_slops: &mut Vec<u8>| {
        if update_left {
            let pos_exists = new_left.last().map(|v| *v == val.1).unwrap_or(false);
            if pos_exists {
                let last_slop = new_slops.last_mut().unwrap();
                *last_slop = (*last_slop).min(val.0);
            } else {
                new_left.push(val.1);
                new_slops.push(val.0);
            }
        }
    };
    loop {
        let left_val = left_positions[left_index];
        let slop_so_far = left_slops.get(left_index).cloned().unwrap_or(0);
        let right_val = right_positions[right_index];

        let distance = slop_so_far as u32 + left_val.abs_diff(right_val);
        if distance <= max_slop {
            let (smaller_val, larger_val, mut smaller_val_idx, smaller_val_positions) =
                if left_val < right_val {
                    (left_val, right_val, left_index, left_positions.as_slice())
                } else {
                    (right_val, left_val, right_index, right_positions)
                };

            let mut new_slop = distance;
            add_val(
                (new_slop as u8, smaller_val),
                positions_buffer,
                slops_buffer,
            );
            while smaller_val_idx + 1 < smaller_val_positions.len() {
                // there could be a better match
                let next_val = smaller_val_positions[smaller_val_idx + 1];
                if next_val > larger_val {
                    // the next value is outside the range, so current one is the best.
                    break;
                }
                let distance = next_val.abs_diff(larger_val);

                // the next value is better.
                smaller_val_idx += 1;
                // better slop
                new_slop = slop_so_far as u32 + distance;
                add_val((new_slop as u8, next_val), positions_buffer, slops_buffer);
            }

            add_val((new_slop as u8, larger_val), positions_buffer, slops_buffer);
            count += 1;
            left_index += 1;
            right_index += 1;
        } else if left_val < right_val {
            left_index += 1;
        } else {
            right_index += 1;
        }

        if left_index >= left_positions.len() || right_index >= right_positions.len() {
            // finish rest
            if left_index >= left_positions.len() {
                let left_val = *left_positions.last().unwrap();
                let slop_so_far: u8 = *left_slops.last().unwrap_or(&0);
                for right_val in &right_positions[right_index..] {
                    let new_slop = left_val.abs_diff(*right_val) + slop_so_far as u32;
                    if new_slop <= max_slop {
                        add_val((new_slop as u8, *right_val), positions_buffer, slops_buffer);
                    }
                }
            } else {
                let right_val = *right_positions.last().unwrap();
                for left_idx in left_index..left_positions.len() {
                    let left_val = left_positions[left_idx];
                    let slop_so_far = *left_slops.get(left_idx).unwrap_or(&0);
                    let new_slop = left_val.abs_diff(right_val) + slop_so_far as u32;
                    if new_slop <= max_slop {
                        add_val((new_slop as u8, left_val), positions_buffer, slops_buffer);
                    }
                }
            };

            break;
        }
    }
    if update_left {
        std::mem::swap(left_positions, positions_buffer);
        std::mem::swap(left_slops, slops_buffer);
        positions_buffer.clear();
        slops_buffer.clear();
    }

    count
}

impl<TPostings: Postings> PhraseScorer<TPostings> {
    // If similarity_weight is None, then scoring is disabled.
    pub fn new(
        term_postings: Vec<(usize, TPostings)>,
        similarity_weight_opt: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
    ) -> PhraseScorer<TPostings> {
        Self::new_with_offset(
            term_postings,
            similarity_weight_opt,
            fieldnorm_reader,
            slop,
            0,
        )
    }

    pub(crate) fn new_with_offset(
        term_postings_with_offset: Vec<(usize, TPostings)>,
        similarity_weight_opt: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
        offset: usize,
    ) -> PhraseScorer<TPostings> {
        let num_docs = fieldnorm_reader.num_docs();
        let max_offset = term_postings_with_offset
            .iter()
            .map(|&(offset, _)| offset)
            .max()
            .unwrap_or(0)
            + offset;
        let num_docsets = term_postings_with_offset.len();
        let postings_with_offsets = term_postings_with_offset
            .into_iter()
            .map(|(offset, postings)| {
                PostingsWithOffset::new(postings, (max_offset - offset) as u32)
            })
            .collect::<Vec<_>>();
        let intersection_docset = Intersection::new(postings_with_offsets, num_docs);
        let mut scorer = PhraseScorer {
            intersection_docset,
            num_terms: num_docsets,
            left_positions: Vec::with_capacity(100),
            right_positions: Vec::with_capacity(100),
            phrase_count: 0u32,
            similarity_weight_opt,
            fieldnorm_reader,
            slop,
            left_slops: Vec::with_capacity(100),
            slops_buffer: Vec::with_capacity(100),
            positions_buffer: Vec::with_capacity(100),
        };
        if scorer.doc() != TERMINATED && !scorer.phrase_match() {
            scorer.advance();
        }
        scorer
    }

    pub fn phrase_count(&self) -> u32 {
        self.phrase_count
    }

    pub(crate) fn get_intersection(&mut self) -> &[u32] {
        intersection(&mut self.left_positions, &self.right_positions);
        &self.left_positions
    }

    fn phrase_match(&mut self) -> bool {
        if self.similarity_weight_opt.is_some() {
            let count = self.compute_phrase_count();
            self.phrase_count = count;
            count > 0u32
        } else {
            self.phrase_exists()
        }
    }

    fn phrase_exists(&mut self) -> bool {
        self.compute_phrase_match();
        if self.has_slop() {
            intersection_exists_with_slop(
                &self.left_positions,
                &self.right_positions[..],
                self.slop,
            )
        } else {
            intersection_exists(&self.left_positions, &self.right_positions[..])
        }
    }

    fn compute_phrase_count(&mut self) -> u32 {
        self.compute_phrase_match();
        if self.has_slop() {
            if self.num_terms > 2 {
                intersection_count_with_carrying_slop(
                    &mut self.left_positions,
                    &mut self.left_slops,
                    &self.right_positions[..],
                    self.slop,
                    false,
                    &mut self.positions_buffer,
                    &mut self.slops_buffer,
                )
            } else {
                intersection_count_with_slop(
                    &mut self.left_positions,
                    &self.right_positions[..],
                    self.slop,
                    false,
                ) as u32
            }
        } else {
            intersection_count(&self.left_positions, &self.right_positions[..]) as u32
        }
    }

    fn compute_phrase_match(&mut self) {
        {
            self.intersection_docset
                .docset_mut_specialized(0)
                .positions(&mut self.left_positions);
            if self.has_slop() {
                self.left_slops.clear();
            }
        }
        for i in 1..self.num_terms - 1 {
            {
                self.intersection_docset
                    .docset_mut_specialized(i)
                    .positions(&mut self.right_positions);
            }
            if self.has_slop() {
                if self.num_terms > 2 {
                    intersection_count_with_carrying_slop(
                        &mut self.left_positions,
                        &mut self.left_slops,
                        &self.right_positions[..],
                        self.slop,
                        true,
                        &mut self.positions_buffer,
                        &mut self.slops_buffer,
                    );
                } else {
                    intersection_count_with_slop(
                        &mut self.left_positions,
                        &self.right_positions[..],
                        self.slop,
                        true,
                    );
                }
            } else {
                intersection(&mut self.left_positions, &self.right_positions);
            };
            if self.left_positions.is_empty() {
                return;
            }
        }
        self.intersection_docset
            .docset_mut_specialized(self.num_terms - 1)
            .positions(&mut self.right_positions);
    }

    fn has_slop(&self) -> bool {
        self.slop > 0
    }
}

impl<TPostings: Postings> DocSet for PhraseScorer<TPostings> {
    fn advance(&mut self) -> DocId {
        loop {
            let doc = self.intersection_docset.advance();
            if doc == TERMINATED || self.phrase_match() {
                return doc;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc());
        let doc = self.intersection_docset.seek(target);
        if doc == TERMINATED || self.phrase_match() {
            return doc;
        }
        self.advance()
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        debug_assert!(
            target >= self.doc(),
            "target ({}) should be greater than or equal to doc ({})",
            target,
            self.doc()
        );
        let seek_res = self.intersection_docset.seek_danger(target);
        if seek_res != SeekDangerResult::Found {
            return seek_res;
        }
        // The intersection matched. Now let's see if we match the phrase.
        if self.phrase_match() {
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(target + 1)
        }
    }

    fn doc(&self) -> DocId {
        self.intersection_docset.doc()
    }

    fn size_hint(&self) -> u32 {
        // We adjust the intersection estimate, since actual phrase hits are much lower than where
        // the all appear.
        // The estimate should depend on average field length, e.g. if the field is really short
        // a phrase hit is more likely
        self.intersection_docset.size_hint() / (10 * self.num_terms as u32)
    }

    /// Returns a best-effort hint of the
    /// cost to drive the docset.
    fn cost(&self) -> u64 {
        // While determing a potential hit is cheap for phrases, evaluating an actual hit is
        // expensive since it requires to load positions for a doc and check if they are next to
        // each other.
        // So the cost estimation would be the number of times we need to check if a doc is a hit *
        // 10 * self.num_terms.
        self.intersection_docset.size_hint() as u64 * 10 * self.num_terms as u64
    }
}

impl<TPostings: Postings> Scorer for PhraseScorer<TPostings> {
    #[inline]
    fn score(&mut self) -> Score {
        let doc = self.doc();
        let fieldnorm_id = self.fieldnorm_reader.fieldnorm_id(doc);
        if let Some(similarity_weight) = self.similarity_weight_opt.as_ref() {
            similarity_weight.score(fieldnorm_id, self.phrase_count)
        } else {
            1.0f32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_intersection_sym(left: &[u32], right: &[u32], expected: &[u32]) {
        test_intersection_aux(left, right, expected, 0);
        test_intersection_aux(right, left, expected, 0);
    }

    fn test_intersection_aux(left: &[u32], right: &[u32], expected: &[u32], slop: u32) {
        let mut left_vec = Vec::from(left);
        if slop == 0 {
            assert_eq!(intersection_count(&left_vec, right), expected.len());
            intersection(&mut left_vec, right);
            assert_eq!(&left_vec, expected);
        } else {
            let mut right_vec = Vec::from(right);
            let right_mut = &mut right_vec[..];
            intersection_count_with_slop(&mut left_vec, right_mut, slop, true);
            assert_eq!(&left_vec, expected);
        }
    }

    #[test]
    fn test_intersection() {
        test_intersection_sym(&[1], &[1], &[1]);
        test_intersection_sym(&[1], &[2], &[]);
        test_intersection_sym(&[], &[2], &[]);
        test_intersection_sym(&[5, 7], &[1, 5, 10, 12], &[5]);
        test_intersection_sym(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 9, 12]);
    }
    #[test]
    fn test_slop() {
        // The slop is not symmetric. It does not allow for the phrase to be out of order.
        test_intersection_aux(&[1], &[2], &[2], 1);
        test_intersection_aux(&[1], &[3], &[], 1);
        test_intersection_aux(&[1], &[3], &[3], 2);
        test_intersection_aux(&[], &[2], &[], 100000);
        test_intersection_aux(&[5, 7, 11], &[1, 5, 10, 12], &[5, 10], 1);
        test_intersection_aux(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 8, 9, 12], 1);
        test_intersection_aux(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 8, 9, 12], 10);
        test_intersection_aux(&[1, 3, 5], &[2, 4, 6], &[2, 4, 6], 1);
        test_intersection_aux(&[1, 3, 5], &[2, 4, 6], &[], 0);
    }

    fn test_merge(left: &[u32], right: &[u32], expected_left: &[u32], slop: u32) {
        let mut left_vec = Vec::from(left);
        let mut right_vec = Vec::from(right);
        let right_mut = &mut right_vec[..];
        intersection_count_with_slop(&mut left_vec, right_mut, slop, true);
        assert_eq!(&left_vec, expected_left);
    }

    #[test]
    fn test_merge_slop() {
        test_merge(&[1, 2], &[1], &[1], 1);
        test_merge(&[3], &[4], &[4], 2);
        test_merge(&[3], &[4], &[4], 2);
        test_merge(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 8, 9, 12], 10);
    }

    fn test_carry_slop_intersection_aux(
        right: &[&[u32]],
        expected: &[(u8, u32)],
        slop: u32,
        expected_count: u32,
    ) {
        let mut left_vec = right[0].to_vec();
        let mut slops = vec![0; left_vec.len()];
        let mut count = 0;
        for right in &right[1..] {
            count = intersection_count_with_carrying_slop(
                &mut left_vec,
                &mut slops,
                right,
                slop,
                true,
                &mut Vec::new(),
                &mut Vec::new(),
            );
        }
        let out: Vec<(u8, u32)> = slops
            .iter()
            .cloned()
            .zip(left_vec.iter().cloned())
            .collect();
        assert_eq!(&out, expected);
        assert_eq!(count, expected_count);
    }

    #[test]
    fn test_carry_slop_intersection() {
        test_carry_slop_intersection_aux(&[&[1], &[]], &[], 1, 0);
        test_carry_slop_intersection_aux(&[&[1], &[2]], &[(1, 1), (1, 2)], 1, 1);
        test_carry_slop_intersection_aux(&[&[1], &[3]], &[], 1, 0);
        test_carry_slop_intersection_aux(&[&[1], &[2]], &[(1, 1), (1, 2)], 1, 1);

        // The order may still matter
        test_carry_slop_intersection_aux(&[&[1], &[2], &[2]], &[(1, 2)], 1, 1);
        test_carry_slop_intersection_aux(&[&[2], &[1], &[2]], &[(1, 2)], 1, 1);
        test_carry_slop_intersection_aux(&[&[2], &[2], &[1]], &[(1, 1), (1, 2)], 1, 1);

        test_carry_slop_intersection_aux(&[&[2], &[2], &[1], &[2]], &[(1, 2)], 1, 1);
        test_carry_slop_intersection_aux(&[&[1], &[2], &[2], &[2]], &[(1, 2)], 1, 1);

        test_carry_slop_intersection_aux(&[&[1], &[2], &[1]], &[(1, 1)], 1, 1);

        test_carry_slop_intersection_aux(&[&[11], &[10, 12]], &[(1, 10), (1, 11), (1, 12)], 1, 1);
        test_carry_slop_intersection_aux(&[&[10, 12], &[11]], &[(1, 10), (1, 11), (1, 12)], 1, 1);

        test_carry_slop_intersection_aux(
            &[&[5, 7, 11], &[1, 5, 10, 12]],
            &[(0, 5), (1, 10), (1, 11), (1, 12)],
            1,
            2,
        );
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use test::Bencher;

    use super::*;

    #[bench]
    fn bench_intersection_short_slop_carrying(b: &mut Bencher) {
        let mut left = Vec::new();
        let mut left_slops = Vec::new();
        let mut buffer = Vec::new();
        let mut slop_buffer = Vec::new();
        b.iter(|| {
            left.clear();
            left.extend_from_slice(&[1, 5, 10, 12]);
            left_slops.extend_from_slice(&[0, 0, 0, 0]);
            let right = [5, 7];
            intersection(&mut left, &right);

            intersection_count_with_carrying_slop(
                &mut left,
                &mut left_slops,
                &right,
                2,
                true,
                &mut buffer,
                &mut slop_buffer,
            )
        });
    }

    #[bench]
    fn bench_intersection_short(b: &mut Bencher) {
        let mut left = Vec::new();
        b.iter(|| {
            left.clear();
            left.extend_from_slice(&[1, 5, 10, 12]);
            let right = [5, 7];
            intersection(&mut left, &right);
        });
    }

    #[bench]
    fn bench_intersection_medium_slop_carrying(b: &mut Bencher) {
        let mut left = Vec::new();
        let mut left_slops: Vec<u8> = Vec::new();
        let mut buffer = Vec::new();
        let mut slop_buffer = Vec::new();
        let left_data: Vec<u32> = (0..100).collect();
        let left_slop_data: Vec<u8> = (0..100).map(|_| 0).collect();

        b.iter(|| {
            left.clear();
            left.extend_from_slice(&left_data);
            left_slops.clear();
            left_slops.extend_from_slice(&left_slop_data);
            let right = [5, 7, 55, 200];

            intersection_count_with_carrying_slop(
                &mut left,
                &mut left_slops,
                &right,
                2,
                true,
                &mut buffer,
                &mut slop_buffer,
            )
        });
    }

    #[bench]
    fn bench_intersection_medium_slop(b: &mut Bencher) {
        let mut left = Vec::new();
        let left_data: Vec<u32> = (0..100).collect();

        b.iter(|| {
            left.clear();
            left.extend_from_slice(&left_data);
            let right = [5, 7, 55, 200];
            intersection_count_with_slop(&mut left, &right[..], 2, true) as u32
        });
    }

    #[bench]
    fn bench_intersection_medium(b: &mut Bencher) {
        let mut left = Vec::new();
        let left_data: Vec<u32> = (0..100).collect();
        b.iter(|| {
            left.clear();
            left.extend_from_slice(&left_data);
            let right = [5, 7, 55, 200];
            intersection(&mut left, &right);
        });
    }

    #[bench]
    fn bench_intersection_count_short(b: &mut Bencher) {
        b.iter(|| {
            let left = [1, 5, 10, 12];
            let right = [5, 7];
            intersection_count(&left, &right);
        });
    }
}
