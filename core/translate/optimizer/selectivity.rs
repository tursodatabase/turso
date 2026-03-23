use smallvec::SmallVec;

/// Combine selectivities using exponential backoff, as introduced in the
/// SQL Server 2014 cardinality estimator (compatibility level 120+).
///
/// Pure independence (`s1 × s2 × s3`) systematically underestimates when
/// predicates are correlated. Exponential backoff dampens each subsequent
/// predicate: `s1 × s2^(1/2) × s3^(1/4) × ...`, sitting between full
/// independence and minimum-selectivity.
///
/// Reference: <https://sqlperformance.com/2014/01/sql-plan/cardinality-estimation-for-multiple-predicates>
///
/// Example with 3 predicates at sel=0.1 each:
/// - Pure independence: 0.001
/// - Dampened:          0.1 × 0.316 × 0.562 ≈ 0.018
/// - Min-selectivity:   0.1
pub(crate) fn dampened_product(selectivities: &mut SmallVec<[f64; 4]>) -> f64 {
    if selectivities.is_empty() {
        return 1.0;
    }
    // Most selective first → gets full weight.
    selectivities.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    let mut result = 1.0;
    let mut exponent = 1.0;
    for &sel in selectivities.iter() {
        result *= sel.powf(1.0 / exponent);
        exponent *= 2.0;
    }
    result
}

/// Combine upper and lower range bounds on the same column into a single
/// effective selectivity.
///
/// Two bounds on the same column (e.g. `x > 5 AND x < 10`) define a single
/// interval, not two independent predicates. When both bounds are present,
/// `closed_range_sel` is used directly as the combined selectivity estimate
///
/// Returns `None` when no bounds are present.
pub(crate) fn closed_range_selectivity(
    lower_sel: Option<f64>,
    upper_sel: Option<f64>,
    closed_range_sel: f64,
) -> Option<f64> {
    match (lower_sel, upper_sel) {
        (Some(_), Some(_)) => Some(closed_range_sel),
        (Some(s), None) | (None, Some(s)) => Some(s),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_selectivity_unchanged() {
        let mut sels = SmallVec::from_elem(0.1, 1);
        assert!((dampened_product(&mut sels) - 0.1).abs() < 1e-10);
    }

    #[test]
    fn dampening_is_less_aggressive_than_independence() {
        let mut sels = SmallVec::from_slice(&[0.1, 0.1, 0.1]);
        let dampened = dampened_product(&mut sels);
        let independent = 0.1 * 0.1 * 0.1;
        assert!(dampened > independent);
        assert!(dampened < 0.1); // But still less than min-selectivity
    }

    #[test]
    fn empty_selectivities_return_one() {
        let mut sels = SmallVec::new();
        assert!((dampened_product(&mut sels) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn closed_range_both_bounds() {
        // When both bounds present, uses the closed_range_sel directly
        let sel = closed_range_selectivity(Some(0.4), Some(0.4), 0.11);
        assert!((sel.unwrap() - 0.11).abs() < 1e-10);
    }

    #[test]
    fn closed_range_single_bound() {
        assert!((closed_range_selectivity(Some(0.4), None, 0.11).unwrap() - 0.4).abs() < 1e-10);
        assert!((closed_range_selectivity(None, Some(0.4), 0.11).unwrap() - 0.4).abs() < 1e-10);
    }

    #[test]
    fn closed_range_no_bounds() {
        assert!(closed_range_selectivity(None, None, 0.11).is_none());
    }
}
