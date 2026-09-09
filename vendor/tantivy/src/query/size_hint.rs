/// Computes the estimated number of documents in the intersection of multiple docsets
/// given their sizes.
///
/// # Arguments
/// * `docset_sizes` - An iterator over the sizes of the docsets (number of documents in each set).
/// * `max_docs` - The maximum number of docs that can hit, usually number of documents in the
///   segment.
///
/// # Returns
/// The estimated number of documents in the intersection.
pub fn estimate_intersection<I>(mut docset_sizes: I, max_docs: u32) -> u32
where I: Iterator<Item = u32> {
    if max_docs == 0u32 {
        return 0u32;
    }
    // Terms tend to be not really randomly distributed.
    // This factor is used to adjust the estimate.
    let mut co_loc_factor: f64 = 1.3;

    let mut intersection_estimate = match docset_sizes.next() {
        Some(first_size) => first_size as f64,
        None => return 0, // No docsets provided, so return 0.
    };

    let mut smallest_docset_size = intersection_estimate;
    // Assuming random distribution of terms, the probability of a document being in the
    // intersection
    for size in docset_sizes {
        // Diminish the co-location factor for each additional set, or we will overestimate.
        co_loc_factor = (co_loc_factor - 0.1).max(1.0);
        intersection_estimate *= (size as f64 / max_docs as f64) * co_loc_factor;
        smallest_docset_size = smallest_docset_size.min(size as f64);
    }

    intersection_estimate.round().min(smallest_docset_size) as u32
}

/// Computes the estimated number of documents in the union of multiple docsets
/// given their sizes.
///
/// # Arguments
/// * `docset_sizes` - An iterator over the sizes of the docsets (number of documents in each set).
/// * `max_docs` - The maximum number of docs that can hit, usually number of documents in the
///   segment.
///
/// # Returns
/// The estimated number of documents in the union.
pub fn estimate_union<I>(docset_sizes: I, max_docs: u32) -> u32
where I: Iterator<Item = u32> {
    // Terms tend to be not really randomly distributed.
    // This factor is used to adjust the estimate.
    // Unlike intersection, the co-location reduces the estimate.
    let co_loc_factor = 0.8;

    // The approach for union is to compute the probability of a document not being in any of the
    // sets
    let mut not_in_any_set_prob = 1.0;

    // Assuming random distribution of terms, the probability of a document being in the
    // union is the complement of the probability of it not being in any of the sets.
    for size in docset_sizes {
        let prob_in_set = (size as f64 / max_docs as f64) * co_loc_factor;
        not_in_any_set_prob *= 1.0 - prob_in_set;
    }

    let union_estimate = (max_docs as f64 * (1.0 - not_in_any_set_prob)).round();

    union_estimate.min(max_docs as f64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_intersection_small1() {
        let docset_sizes = &[500, 1000];
        let n = 10_000;
        let result = estimate_intersection(docset_sizes.iter().copied(), n);
        assert_eq!(result, 60);
    }

    #[test]
    fn test_estimate_intersection_small2() {
        let docset_sizes = &[500, 1000, 1500];
        let n = 10_000;
        let result = estimate_intersection(docset_sizes.iter().copied(), n);
        assert_eq!(result, 10);
    }

    #[test]
    fn test_estimate_intersection_large_values() {
        let docset_sizes = &[100_000, 50_000, 30_000];
        let n = 1_000_000;
        let result = estimate_intersection(docset_sizes.iter().copied(), n);
        assert_eq!(result, 198);
    }

    #[test]
    fn test_estimate_union_small() {
        let docset_sizes = &[500, 1000, 1500];
        let n = 10000;
        let result = estimate_union(docset_sizes.iter().copied(), n);
        assert_eq!(result, 2228);
    }

    #[test]
    fn test_estimate_union_large_values() {
        let docset_sizes = &[100000, 50000, 30000];
        let n = 1000000;
        let result = estimate_union(docset_sizes.iter().copied(), n);
        assert_eq!(result, 137997);
    }

    #[test]
    fn test_estimate_intersection_large() {
        let docset_sizes: Vec<_> = (0..10).map(|_| 4_000_000).collect();
        let n = 5_000_000;
        let result = estimate_intersection(docset_sizes.iter().copied(), n);
        // Check that it doesn't overflow and returns a reasonable result
        assert_eq!(result, 708_670);
    }

    #[test]
    fn test_estimate_intersection_overflow_safety() {
        let docset_sizes: Vec<_> = (0..100).map(|_| 4_000_000).collect();
        let n = 5_000_000;
        let result = estimate_intersection(docset_sizes.iter().copied(), n);
        // Check that it doesn't overflow and returns a reasonable result
        assert_eq!(result, 0);
    }

    #[test]
    fn test_estimate_union_overflow_safety() {
        let docset_sizes: Vec<_> = (0..100).map(|_| 1_000_000).collect();
        let n = 20_000_000;
        let result = estimate_union(docset_sizes.iter().copied(), n);
        // Check that it doesn't overflow and returns a reasonable result
        assert_eq!(result, 19_662_594);
    }
}
