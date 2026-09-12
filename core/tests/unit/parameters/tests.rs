use super::Parameters;
use std::num::NonZero;

#[test]
fn count_and_name_are_stable_with_reused_parameters() {
    let mut parameters = Parameters::new();

    parameters.push(":a");
    parameters.push(":a");
    parameters.push("1");
    parameters.push("1");
    parameters.push(":b");
    parameters.push("2");

    assert_eq!(parameters.count(), 2);

    let idx1 = 1usize.try_into().unwrap();
    let idx2 = 2usize.try_into().unwrap();
    let idx3 = 3usize.try_into().unwrap();

    assert!(parameters.has_index(idx1));
    assert!(parameters.has_index(idx2));
    assert!(!parameters.has_index(idx3));
    assert!(!parameters.is_indexed(idx1));
    assert!(!parameters.is_indexed(idx2));

    assert_eq!(parameters.name(idx1).as_deref(), Some(":a"));
    assert_eq!(parameters.name(idx2).as_deref(), Some(":b"));
}

#[test]
fn is_indexed_is_true_only_for_indexed_parameters() {
    let mut parameters = Parameters::new();
    parameters.push("1");
    parameters.push(":b");

    let idx1 = 1usize.try_into().unwrap();
    let idx2 = 2usize.try_into().unwrap();

    assert!(parameters.is_indexed(idx1));
    assert!(!parameters.is_indexed(idx2));
}

#[test]
fn many_distinct_named_params_lookup_round_trips() {
    // Exercises the O(1) name<->index maps with many distinct names,
    // the case that was previously O(n^2) via a linear list scan.
    let mut parameters = Parameters::new();
    let n = 1000usize;
    for i in 0..n {
        let idx = parameters.push(format!(":p{i}"));
        // Distinct names get consecutive indices in encounter order.
        assert_eq!(idx.get(), i + 1);
    }
    assert_eq!(parameters.count(), n);
    for i in 0..n {
        let expected: NonZero<usize> = (i + 1).try_into().unwrap();
        assert_eq!(parameters.index(format!(":p{i}")), Some(expected));
        assert_eq!(
            parameters.name(expected).as_deref(),
            Some(format!(":p{i}").as_str())
        );
        assert!(parameters.has_index(expected));
        assert!(!parameters.is_indexed(expected));
    }
    assert_eq!(parameters.list.len(), n, "no duplicate slots");
}

#[test]
fn reused_named_param_returns_same_index() {
    let mut parameters = Parameters::new();
    let a1 = parameters.push(":a");
    let b = parameters.push(":b");
    let a2 = parameters.push(":a");
    assert_eq!(a1, a2);
    assert_ne!(a1, b);
    assert_eq!(parameters.count(), 2);
    assert_eq!(parameters.list.len(), 2);
}

#[test]
fn push_named_at_replaces_existing_indexed_slot() {
    // push_index then push_named_at at the same index: the slot becomes Named.
    let mut parameters = Parameters::new();
    let idx: NonZero<usize> = 1.try_into().unwrap();
    parameters.push_index(idx);
    assert!(parameters.is_indexed(idx));
    parameters.push_named_at(":x", idx);
    assert!(parameters.has_index(idx));
    assert!(!parameters.is_indexed(idx));
    assert_eq!(parameters.name(idx).as_deref(), Some(":x"));
    assert_eq!(parameters.index(":x"), Some(idx));
    assert_eq!(
        parameters.list.len(),
        1,
        "indexed slot replaced, not duplicated"
    );
}

#[test]
fn numbered_spelling_does_not_rename_a_named_slot() {
    // SELECT :v, ?1 — one variable; its name stays :v and "?1" does
    // not resolve, as in SQLite (first spelling wins).
    let mut parameters = Parameters::new();
    let idx: NonZero<usize> = 1.try_into().unwrap();
    parameters.push_named_at(":v", idx);
    parameters.push_numbered(idx);
    assert_eq!(parameters.count(), 1);
    assert_eq!(parameters.name(idx).as_deref(), Some(":v"));
    assert_eq!(parameters.index("?1"), None);
    assert_eq!(parameters.index(":v"), Some(idx));
}

#[test]
fn numbered_slot_keeps_its_name_across_bare_occurrences() {
    let mut parameters = Parameters::new();
    let idx: NonZero<usize> = 1.try_into().unwrap();
    parameters.push_numbered(idx);
    parameters.push_index(idx);
    assert_eq!(parameters.name(idx).as_deref(), Some("?1"));
    assert_eq!(parameters.index("?1"), Some(idx));
}

#[test]
fn repeated_indexed_param_dedups_to_single_slot() {
    let mut parameters = Parameters::new();
    let idx: NonZero<usize> = 5.try_into().unwrap();
    for _ in 0..100 {
        assert_eq!(parameters.push_index(idx), idx);
    }
    assert_eq!(parameters.list.len(), 1);
    assert_eq!(parameters.count(), 5);
    assert!(parameters.has_index(idx));
}

#[test]
fn count_tracks_highest_index_for_sparse_parameters() {
    let mut parameters = Parameters::new();
    parameters.push("3");

    let idx1 = 1usize.try_into().unwrap();
    let idx2 = 2usize.try_into().unwrap();
    let idx3 = 3usize.try_into().unwrap();
    let idx4 = 4usize.try_into().unwrap();

    assert_eq!(parameters.count(), 3);
    assert!(parameters.has_slot(idx1));
    assert!(parameters.has_slot(idx2));
    assert!(parameters.has_slot(idx3));
    assert!(!parameters.has_slot(idx4));

    assert!(!parameters.has_index(idx1));
    assert!(!parameters.has_index(idx2));
    assert!(parameters.has_index(idx3));
}
