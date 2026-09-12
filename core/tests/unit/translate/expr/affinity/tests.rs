use super::*;

#[test]
fn both_sides_have_affinity_numeric_wins() {
    assert_eq!(
        do_comparison_affinity(Affinity::Real, Affinity::Text,),
        Affinity::Numeric
    );
}

#[test]
fn both_sides_have_affinity_neither_numeric_is_blob() {
    assert_eq!(
        do_comparison_affinity(Affinity::Text, Affinity::Blob,),
        Affinity::Blob
    );
}

#[test]
fn only_lhs_has_real_affinity_collapses_to_numeric() {
    assert_eq!(
        do_comparison_affinity(Affinity::Real, Affinity::None,),
        Affinity::Numeric
    );
}

#[test]
fn only_lhs_has_integer_affinity_collapses_to_numeric() {
    assert_eq!(
        do_comparison_affinity(Affinity::Integer, Affinity::None,),
        Affinity::Numeric
    );
}

#[test]
fn only_rhs_has_real_affinity_collapses_to_numeric() {
    assert_eq!(
        do_comparison_affinity(Affinity::None, Affinity::Real,),
        Affinity::Numeric
    );
}

#[test]
fn only_lhs_has_text_affinity_is_preserved() {
    assert_eq!(
        do_comparison_affinity(Affinity::Text, Affinity::None,),
        Affinity::Text
    );
}

#[test]
fn only_rhs_has_text_affinity_is_preserved() {
    assert_eq!(
        do_comparison_affinity(Affinity::None, Affinity::Text,),
        Affinity::Text
    );
}

#[test]
fn neither_side_has_affinity_is_blob() {
    assert_eq!(
        do_comparison_affinity(Affinity::None, Affinity::None,),
        Affinity::Blob
    );
}
