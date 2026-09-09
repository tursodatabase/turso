//! SIMD filtering of a vector as described in the following blog post.
//! <https://quickwit.io/blog/filtering%20a%20vector%20with%20simd%20instructions%20avx-2%20and%20avx-512>
use std::arch::x86_64::{
    __m256i as DataType, _mm256_add_epi32 as op_add, _mm256_cmpgt_epi32 as op_greater,
    _mm256_lddqu_si256 as load_unaligned, _mm256_or_si256 as op_or, _mm256_set1_epi32 as set1,
    _mm256_storeu_si256 as store_unaligned, _mm256_xor_si256 as op_xor, *,
};
use std::ops::RangeInclusive;

const NUM_LANES: usize = 8;

const HIGHEST_BIT: u32 = 1 << 31;

#[inline]
fn u32_to_i32(val: u32) -> i32 {
    (val ^ HIGHEST_BIT) as i32
}

#[inline]
unsafe fn u32_to_i32_avx2(vals_u32x8s: DataType) -> DataType {
    const HIGHEST_BIT_MASK: DataType = from_u32x8([HIGHEST_BIT; NUM_LANES]);
    unsafe { op_xor(vals_u32x8s, HIGHEST_BIT_MASK) }
}

pub fn filter_vec_in_place(range: RangeInclusive<u32>, offset: u32, output: &mut Vec<u32>) {
    // We use a monotonic mapping from u32 to i32 to make the comparison possible in AVX2.
    let range_i32: RangeInclusive<i32> = u32_to_i32(*range.start())..=u32_to_i32(*range.end());
    let num_words = output.len() / NUM_LANES;
    let mut output_len = unsafe {
        filter_vec_avx2_aux(
            output.as_ptr() as *const __m256i,
            range_i32,
            output.as_mut_ptr(),
            offset,
            num_words,
        )
    };
    let reminder_start = num_words * NUM_LANES;
    for i in reminder_start..output.len() {
        let val = output[i];
        output[output_len] = offset + i as u32;
        output_len += if range.contains(&val) { 1 } else { 0 };
    }
    output.truncate(output_len);
}

#[target_feature(enable = "avx2")]
unsafe fn filter_vec_avx2_aux(
    mut input: *const __m256i,
    range: RangeInclusive<i32>,
    output: *mut u32,
    offset: u32,
    num_words: usize,
) -> usize {
    let mut output_tail = output;
    let range_simd = set1(*range.start())..=set1(*range.end());
    let mut ids = from_u32x8([
        offset,
        offset + 1,
        offset + 2,
        offset + 3,
        offset + 4,
        offset + 5,
        offset + 6,
        offset + 7,
    ]);
    const SHIFT: __m256i = from_u32x8([NUM_LANES as u32; NUM_LANES]);
    for _ in 0..num_words {
        unsafe {
            let word = load_unaligned(input);
            let word = u32_to_i32_avx2(word);
            let keeper_bitset = compute_filter_bitset(word, range_simd.clone());
            let added_len = keeper_bitset.count_ones();
            let filtered_doc_ids = compact(ids, keeper_bitset);
            store_unaligned(output_tail as *mut __m256i, filtered_doc_ids);
            output_tail = output_tail.offset(added_len as isize);
            ids = op_add(ids, SHIFT);
            input = input.offset(1);
        }
    }
    unsafe { output_tail.offset_from(output) as usize }
}

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn compact(data: DataType, mask: u8) -> DataType {
    let vperm_mask = MASK_TO_PERMUTATION[mask as usize];
    _mm256_permutevar8x32_epi32(data, vperm_mask)
}

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn compute_filter_bitset(val: __m256i, range: std::ops::RangeInclusive<__m256i>) -> u8 {
    let too_low = op_greater(*range.start(), val);
    let too_high = op_greater(val, *range.end());
    let inside = op_or(too_low, too_high);
    255 - std::arch::x86_64::_mm256_movemask_ps(_mm256_castsi256_ps(inside)) as u8
}

union U8x32 {
    vector: DataType,
    vals: [u32; NUM_LANES],
}

const fn from_u32x8(vals: [u32; NUM_LANES]) -> DataType {
    unsafe { U8x32 { vals }.vector }
}

const MASK_TO_PERMUTATION: [DataType; 256] = [
    from_u32x8([0, 0, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 0, 0, 0, 0, 0, 0, 0]),
    from_u32x8([1, 0, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 0, 0, 0, 0, 0, 0]),
    from_u32x8([2, 0, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 0, 0, 0, 0, 0, 0]),
    from_u32x8([1, 2, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 0, 0, 0, 0, 0]),
    from_u32x8([3, 0, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 0, 0, 0, 0, 0, 0]),
    from_u32x8([1, 3, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 0, 0, 0, 0, 0]),
    from_u32x8([2, 3, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 0, 0, 0, 0, 0]),
    from_u32x8([1, 2, 3, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 0, 0, 0, 0]),
    from_u32x8([4, 0, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 4, 0, 0, 0, 0, 0, 0]),
    from_u32x8([1, 4, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 4, 0, 0, 0, 0, 0]),
    from_u32x8([2, 4, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 4, 0, 0, 0, 0, 0]),
    from_u32x8([1, 2, 4, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 4, 0, 0, 0, 0]),
    from_u32x8([3, 4, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 4, 0, 0, 0, 0, 0]),
    from_u32x8([1, 3, 4, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 4, 0, 0, 0, 0]),
    from_u32x8([2, 3, 4, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 4, 0, 0, 0, 0]),
    from_u32x8([1, 2, 3, 4, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 4, 0, 0, 0]),
    from_u32x8([5, 0, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 5, 0, 0, 0, 0, 0, 0]),
    from_u32x8([1, 5, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 5, 0, 0, 0, 0, 0]),
    from_u32x8([2, 5, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 5, 0, 0, 0, 0, 0]),
    from_u32x8([1, 2, 5, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 5, 0, 0, 0, 0]),
    from_u32x8([3, 5, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 5, 0, 0, 0, 0, 0]),
    from_u32x8([1, 3, 5, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 5, 0, 0, 0, 0]),
    from_u32x8([2, 3, 5, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 5, 0, 0, 0, 0]),
    from_u32x8([1, 2, 3, 5, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 5, 0, 0, 0]),
    from_u32x8([4, 5, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 4, 5, 0, 0, 0, 0, 0]),
    from_u32x8([1, 4, 5, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 4, 5, 0, 0, 0, 0]),
    from_u32x8([2, 4, 5, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 4, 5, 0, 0, 0, 0]),
    from_u32x8([1, 2, 4, 5, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 4, 5, 0, 0, 0]),
    from_u32x8([3, 4, 5, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 4, 5, 0, 0, 0, 0]),
    from_u32x8([1, 3, 4, 5, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 4, 5, 0, 0, 0]),
    from_u32x8([2, 3, 4, 5, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 4, 5, 0, 0, 0]),
    from_u32x8([1, 2, 3, 4, 5, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 4, 5, 0, 0]),
    from_u32x8([6, 0, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 6, 0, 0, 0, 0, 0, 0]),
    from_u32x8([1, 6, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 6, 0, 0, 0, 0, 0]),
    from_u32x8([2, 6, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 6, 0, 0, 0, 0, 0]),
    from_u32x8([1, 2, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 6, 0, 0, 0, 0]),
    from_u32x8([3, 6, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 6, 0, 0, 0, 0, 0]),
    from_u32x8([1, 3, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 6, 0, 0, 0, 0]),
    from_u32x8([2, 3, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 6, 0, 0, 0, 0]),
    from_u32x8([1, 2, 3, 6, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 6, 0, 0, 0]),
    from_u32x8([4, 6, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 4, 6, 0, 0, 0, 0, 0]),
    from_u32x8([1, 4, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 4, 6, 0, 0, 0, 0]),
    from_u32x8([2, 4, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 4, 6, 0, 0, 0, 0]),
    from_u32x8([1, 2, 4, 6, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 4, 6, 0, 0, 0]),
    from_u32x8([3, 4, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 4, 6, 0, 0, 0, 0]),
    from_u32x8([1, 3, 4, 6, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 4, 6, 0, 0, 0]),
    from_u32x8([2, 3, 4, 6, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 4, 6, 0, 0, 0]),
    from_u32x8([1, 2, 3, 4, 6, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 4, 6, 0, 0]),
    from_u32x8([5, 6, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 5, 6, 0, 0, 0, 0, 0]),
    from_u32x8([1, 5, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 5, 6, 0, 0, 0, 0]),
    from_u32x8([2, 5, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 5, 6, 0, 0, 0, 0]),
    from_u32x8([1, 2, 5, 6, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 5, 6, 0, 0, 0]),
    from_u32x8([3, 5, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 5, 6, 0, 0, 0, 0]),
    from_u32x8([1, 3, 5, 6, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 5, 6, 0, 0, 0]),
    from_u32x8([2, 3, 5, 6, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 5, 6, 0, 0, 0]),
    from_u32x8([1, 2, 3, 5, 6, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 5, 6, 0, 0]),
    from_u32x8([4, 5, 6, 0, 0, 0, 0, 0]),
    from_u32x8([0, 4, 5, 6, 0, 0, 0, 0]),
    from_u32x8([1, 4, 5, 6, 0, 0, 0, 0]),
    from_u32x8([0, 1, 4, 5, 6, 0, 0, 0]),
    from_u32x8([2, 4, 5, 6, 0, 0, 0, 0]),
    from_u32x8([0, 2, 4, 5, 6, 0, 0, 0]),
    from_u32x8([1, 2, 4, 5, 6, 0, 0, 0]),
    from_u32x8([0, 1, 2, 4, 5, 6, 0, 0]),
    from_u32x8([3, 4, 5, 6, 0, 0, 0, 0]),
    from_u32x8([0, 3, 4, 5, 6, 0, 0, 0]),
    from_u32x8([1, 3, 4, 5, 6, 0, 0, 0]),
    from_u32x8([0, 1, 3, 4, 5, 6, 0, 0]),
    from_u32x8([2, 3, 4, 5, 6, 0, 0, 0]),
    from_u32x8([0, 2, 3, 4, 5, 6, 0, 0]),
    from_u32x8([1, 2, 3, 4, 5, 6, 0, 0]),
    from_u32x8([0, 1, 2, 3, 4, 5, 6, 0]),
    from_u32x8([7, 0, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 7, 0, 0, 0, 0, 0, 0]),
    from_u32x8([1, 7, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 7, 0, 0, 0, 0, 0]),
    from_u32x8([2, 7, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 7, 0, 0, 0, 0, 0]),
    from_u32x8([1, 2, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 7, 0, 0, 0, 0]),
    from_u32x8([3, 7, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 7, 0, 0, 0, 0, 0]),
    from_u32x8([1, 3, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 7, 0, 0, 0, 0]),
    from_u32x8([2, 3, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 7, 0, 0, 0, 0]),
    from_u32x8([1, 2, 3, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 7, 0, 0, 0]),
    from_u32x8([4, 7, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 4, 7, 0, 0, 0, 0, 0]),
    from_u32x8([1, 4, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 4, 7, 0, 0, 0, 0]),
    from_u32x8([2, 4, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 4, 7, 0, 0, 0, 0]),
    from_u32x8([1, 2, 4, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 4, 7, 0, 0, 0]),
    from_u32x8([3, 4, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 4, 7, 0, 0, 0, 0]),
    from_u32x8([1, 3, 4, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 4, 7, 0, 0, 0]),
    from_u32x8([2, 3, 4, 7, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 4, 7, 0, 0, 0]),
    from_u32x8([1, 2, 3, 4, 7, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 4, 7, 0, 0]),
    from_u32x8([5, 7, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 5, 7, 0, 0, 0, 0, 0]),
    from_u32x8([1, 5, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 5, 7, 0, 0, 0, 0]),
    from_u32x8([2, 5, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 5, 7, 0, 0, 0, 0]),
    from_u32x8([1, 2, 5, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 5, 7, 0, 0, 0]),
    from_u32x8([3, 5, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 5, 7, 0, 0, 0, 0]),
    from_u32x8([1, 3, 5, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 5, 7, 0, 0, 0]),
    from_u32x8([2, 3, 5, 7, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 5, 7, 0, 0, 0]),
    from_u32x8([1, 2, 3, 5, 7, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 5, 7, 0, 0]),
    from_u32x8([4, 5, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 4, 5, 7, 0, 0, 0, 0]),
    from_u32x8([1, 4, 5, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 4, 5, 7, 0, 0, 0]),
    from_u32x8([2, 4, 5, 7, 0, 0, 0, 0]),
    from_u32x8([0, 2, 4, 5, 7, 0, 0, 0]),
    from_u32x8([1, 2, 4, 5, 7, 0, 0, 0]),
    from_u32x8([0, 1, 2, 4, 5, 7, 0, 0]),
    from_u32x8([3, 4, 5, 7, 0, 0, 0, 0]),
    from_u32x8([0, 3, 4, 5, 7, 0, 0, 0]),
    from_u32x8([1, 3, 4, 5, 7, 0, 0, 0]),
    from_u32x8([0, 1, 3, 4, 5, 7, 0, 0]),
    from_u32x8([2, 3, 4, 5, 7, 0, 0, 0]),
    from_u32x8([0, 2, 3, 4, 5, 7, 0, 0]),
    from_u32x8([1, 2, 3, 4, 5, 7, 0, 0]),
    from_u32x8([0, 1, 2, 3, 4, 5, 7, 0]),
    from_u32x8([6, 7, 0, 0, 0, 0, 0, 0]),
    from_u32x8([0, 6, 7, 0, 0, 0, 0, 0]),
    from_u32x8([1, 6, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 1, 6, 7, 0, 0, 0, 0]),
    from_u32x8([2, 6, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 2, 6, 7, 0, 0, 0, 0]),
    from_u32x8([1, 2, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 2, 6, 7, 0, 0, 0]),
    from_u32x8([3, 6, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 3, 6, 7, 0, 0, 0, 0]),
    from_u32x8([1, 3, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 3, 6, 7, 0, 0, 0]),
    from_u32x8([2, 3, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 2, 3, 6, 7, 0, 0, 0]),
    from_u32x8([1, 2, 3, 6, 7, 0, 0, 0]),
    from_u32x8([0, 1, 2, 3, 6, 7, 0, 0]),
    from_u32x8([4, 6, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 4, 6, 7, 0, 0, 0, 0]),
    from_u32x8([1, 4, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 4, 6, 7, 0, 0, 0]),
    from_u32x8([2, 4, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 2, 4, 6, 7, 0, 0, 0]),
    from_u32x8([1, 2, 4, 6, 7, 0, 0, 0]),
    from_u32x8([0, 1, 2, 4, 6, 7, 0, 0]),
    from_u32x8([3, 4, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 3, 4, 6, 7, 0, 0, 0]),
    from_u32x8([1, 3, 4, 6, 7, 0, 0, 0]),
    from_u32x8([0, 1, 3, 4, 6, 7, 0, 0]),
    from_u32x8([2, 3, 4, 6, 7, 0, 0, 0]),
    from_u32x8([0, 2, 3, 4, 6, 7, 0, 0]),
    from_u32x8([1, 2, 3, 4, 6, 7, 0, 0]),
    from_u32x8([0, 1, 2, 3, 4, 6, 7, 0]),
    from_u32x8([5, 6, 7, 0, 0, 0, 0, 0]),
    from_u32x8([0, 5, 6, 7, 0, 0, 0, 0]),
    from_u32x8([1, 5, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 1, 5, 6, 7, 0, 0, 0]),
    from_u32x8([2, 5, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 2, 5, 6, 7, 0, 0, 0]),
    from_u32x8([1, 2, 5, 6, 7, 0, 0, 0]),
    from_u32x8([0, 1, 2, 5, 6, 7, 0, 0]),
    from_u32x8([3, 5, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 3, 5, 6, 7, 0, 0, 0]),
    from_u32x8([1, 3, 5, 6, 7, 0, 0, 0]),
    from_u32x8([0, 1, 3, 5, 6, 7, 0, 0]),
    from_u32x8([2, 3, 5, 6, 7, 0, 0, 0]),
    from_u32x8([0, 2, 3, 5, 6, 7, 0, 0]),
    from_u32x8([1, 2, 3, 5, 6, 7, 0, 0]),
    from_u32x8([0, 1, 2, 3, 5, 6, 7, 0]),
    from_u32x8([4, 5, 6, 7, 0, 0, 0, 0]),
    from_u32x8([0, 4, 5, 6, 7, 0, 0, 0]),
    from_u32x8([1, 4, 5, 6, 7, 0, 0, 0]),
    from_u32x8([0, 1, 4, 5, 6, 7, 0, 0]),
    from_u32x8([2, 4, 5, 6, 7, 0, 0, 0]),
    from_u32x8([0, 2, 4, 5, 6, 7, 0, 0]),
    from_u32x8([1, 2, 4, 5, 6, 7, 0, 0]),
    from_u32x8([0, 1, 2, 4, 5, 6, 7, 0]),
    from_u32x8([3, 4, 5, 6, 7, 0, 0, 0]),
    from_u32x8([0, 3, 4, 5, 6, 7, 0, 0]),
    from_u32x8([1, 3, 4, 5, 6, 7, 0, 0]),
    from_u32x8([0, 1, 3, 4, 5, 6, 7, 0]),
    from_u32x8([2, 3, 4, 5, 6, 7, 0, 0]),
    from_u32x8([0, 2, 3, 4, 5, 6, 7, 0]),
    from_u32x8([1, 2, 3, 4, 5, 6, 7, 0]),
    from_u32x8([0, 1, 2, 3, 4, 5, 6, 7]),
];
