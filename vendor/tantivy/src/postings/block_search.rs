use crate::postings::compression::COMPRESSION_BLOCK_SIZE;

/// Search the first index containing an element greater or equal to
/// the target.
///
/// The results should be equivalent to
/// ```compile_fail
/// block[..]
//       .iter()
//       .take_while(|&&val| val < target)
//       .count()
/// ```
/// 
/// the `start` argument is just used to hint that the response is
/// greater than beyond `start`. The implementation may or may not use
/// it for optimization.
///
/// # Assumption
///
/// - The block is sorted. Some elements may appear several times. This is the case at the
///   end of the last block for instance.
/// - The target is assumed smaller or equal to the last element of the block.
pub fn branchless_binary_search(arr: &[u32; COMPRESSION_BLOCK_SIZE], target: u32) -> usize {
    let mut start = 0;
    let mut len = arr.len();
    for _ in 0..7 {
        len /= 2;
        let pivot = unsafe { *arr.get_unchecked(start + len - 1) };
        if pivot < target {
            start += len;
        }
    }
    start
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use proptest::prelude::*;

    use super::branchless_binary_search;
    use crate::docset::TERMINATED;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;

    fn search_in_block_trivial_but_slow(block: &[u32], target: u32) -> usize {
        block.iter().take_while(|&&val| val < target).count()
    }

    fn util_test_search_in_block(block: &[u32], target: u32) {
        let cursor = search_in_block_trivial_but_slow(block, target);
        assert!(cursor < COMPRESSION_BLOCK_SIZE);
        assert!(block[cursor] >= target);
        if cursor > 0 {
            assert!(block[cursor - 1] < target);
        }
        assert_eq!(block.len(), COMPRESSION_BLOCK_SIZE);
        let mut output_buffer = [TERMINATED; COMPRESSION_BLOCK_SIZE];
        output_buffer[..block.len()].copy_from_slice(block);
        assert_eq!(branchless_binary_search(&output_buffer, target), cursor);
    }

    fn util_test_search_in_block_all(block: &[u32]) {
        let mut targets = HashSet::new();
        targets.insert(0);
        for &val in block {
            if val > 0 {
                targets.insert(val - 1);
            }
            targets.insert(val);
        }
        for target in targets {
            util_test_search_in_block(block, target);
        }
    }

    #[test]
    fn test_search_in_branchless_binary_search() {
        let v: Vec<u32> = (0..COMPRESSION_BLOCK_SIZE).map(|i| i as u32 * 2).collect();
        util_test_search_in_block_all(&v[..]);
    }

    fn monotonous_block() -> impl Strategy<Value = Vec<u32>> {
        prop::collection::vec(0u32..5u32, COMPRESSION_BLOCK_SIZE).prop_map(|mut deltas| {
            let mut el = 0;
            for i in 0..COMPRESSION_BLOCK_SIZE {
                el += deltas[i];
                deltas[i] = el;
            }
            deltas
        })
    }

    proptest! {
        #[test]
        fn test_proptest_branchless_binary_search(block in monotonous_block()) {
            util_test_search_in_block_all(&block[..]);
        }
    }
}
