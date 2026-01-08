use divan::{black_box, Bencher};
use std::collections::HashMap;
use turso_core::vdbe::likeop::{exec_glob, exec_like_with_escape};

// =============================================================================
// LIKE Pattern Matching Benchmarks
// =============================================================================

#[divan::bench]
fn like_simple_exact_match(bencher: Bencher) {
    bencher.bench_local(|| exec_like_with_escape(black_box("hello"), black_box("hello"), '\\'));
}

#[divan::bench]
fn like_simple_no_match(bencher: Bencher) {
    bencher.bench_local(|| exec_like_with_escape(black_box("hello"), black_box("world"), '\\'));
}

#[divan::bench]
fn like_percent_prefix(bencher: Bencher) {
    // Pattern: %world - matches anything ending with "world"
    bencher
        .bench_local(|| exec_like_with_escape(black_box("%world"), black_box("hello world"), '\\'));
}

#[divan::bench]
fn like_percent_suffix(bencher: Bencher) {
    // Pattern: hello% - matches anything starting with "hello"
    bencher
        .bench_local(|| exec_like_with_escape(black_box("hello%"), black_box("hello world"), '\\'));
}

#[divan::bench]
fn like_percent_both(bencher: Bencher) {
    // Pattern: %llo wor% - matches anything containing "llo wor"
    bencher.bench_local(|| {
        exec_like_with_escape(black_box("%llo wor%"), black_box("hello world"), '\\')
    });
}

#[divan::bench]
fn like_underscore_single(bencher: Bencher) {
    // Pattern: h_llo - matches "hello", "hallo", etc.
    bencher.bench_local(|| exec_like_with_escape(black_box("h_llo"), black_box("hello"), '\\'));
}

#[divan::bench]
fn like_underscore_multiple(bencher: Bencher) {
    // Pattern: h___o - matches 5 character words starting with h, ending with o
    bencher.bench_local(|| exec_like_with_escape(black_box("h___o"), black_box("hello"), '\\'));
}

#[divan::bench]
fn like_mixed_wildcards(bencher: Bencher) {
    // Pattern: %h_llo% - complex pattern
    bencher.bench_local(|| {
        exec_like_with_escape(black_box("%h_llo%"), black_box("say hello world"), '\\')
    });
}

#[divan::bench]
fn like_escape_percent(bencher: Bencher) {
    // Testing escaped percent sign
    bencher.bench_local(|| exec_like_with_escape(black_box("100\\%"), black_box("100%"), '\\'));
}

#[divan::bench]
fn like_escape_underscore(bencher: Bencher) {
    // Testing escaped underscore
    bencher.bench_local(|| {
        exec_like_with_escape(black_box("file\\_name"), black_box("file_name"), '\\')
    });
}

#[divan::bench]
fn like_case_insensitive(bencher: Bencher) {
    // LIKE is case-insensitive by default
    bencher.bench_local(|| exec_like_with_escape(black_box("HELLO"), black_box("hello"), '\\'));
}

#[divan::bench]
fn like_long_pattern(bencher: Bencher) {
    let pattern = "The quick brown fox %";
    let text = "The quick brown fox jumps over the lazy dog";
    bencher.bench_local(|| exec_like_with_escape(black_box(pattern), black_box(text), '\\'));
}

#[divan::bench]
fn like_long_text_short_pattern(bencher: Bencher) {
    let pattern = "%dog";
    let text = "The quick brown fox jumps over the lazy dog";
    bencher.bench_local(|| exec_like_with_escape(black_box(pattern), black_box(text), '\\'));
}

#[divan::bench]
fn like_many_percent_wildcards(bencher: Bencher) {
    // Pattern with multiple % wildcards - can be expensive
    let pattern = "%quick%fox%lazy%";
    let text = "The quick brown fox jumps over the lazy dog";
    bencher.bench_local(|| exec_like_with_escape(black_box(pattern), black_box(text), '\\'));
}

// =============================================================================
// GLOB Pattern Matching Benchmarks
// =============================================================================

#[divan::bench]
fn glob_simple_exact_match(bencher: Bencher) {
    bencher.bench_local(|| exec_glob(None, black_box("hello"), black_box("hello")));
}

#[divan::bench]
fn glob_simple_no_match(bencher: Bencher) {
    bencher.bench_local(|| exec_glob(None, black_box("hello"), black_box("world")));
}

#[divan::bench]
fn glob_star_prefix(bencher: Bencher) {
    // Pattern: *world - matches anything ending with "world"
    bencher.bench_local(|| exec_glob(None, black_box("*world"), black_box("hello world")));
}

#[divan::bench]
fn glob_star_suffix(bencher: Bencher) {
    // Pattern: hello* - matches anything starting with "hello"
    bencher.bench_local(|| exec_glob(None, black_box("hello*"), black_box("hello world")));
}

#[divan::bench]
fn glob_star_both(bencher: Bencher) {
    // Pattern: *llo wor* - matches anything containing "llo wor"
    bencher.bench_local(|| exec_glob(None, black_box("*llo wor*"), black_box("hello world")));
}

#[divan::bench]
fn glob_question_single(bencher: Bencher) {
    // Pattern: h?llo - matches "hello", "hallo", etc.
    bencher.bench_local(|| exec_glob(None, black_box("h?llo"), black_box("hello")));
}

#[divan::bench]
fn glob_question_multiple(bencher: Bencher) {
    // Pattern: h???o - matches 5 character words starting with h, ending with o
    bencher.bench_local(|| exec_glob(None, black_box("h???o"), black_box("hello")));
}

#[divan::bench]
fn glob_character_class(bencher: Bencher) {
    // Pattern: [abc]* - matches words starting with a, b, or c
    bencher.bench_local(|| exec_glob(None, black_box("[abc]*"), black_box("apple")));
}

#[divan::bench]
fn glob_character_class_range(bencher: Bencher) {
    // Pattern: [a-z]* - matches words starting with lowercase letter
    bencher.bench_local(|| exec_glob(None, black_box("[a-z]*"), black_box("hello")));
}

#[divan::bench]
fn glob_character_class_negation(bencher: Bencher) {
    // Pattern: [^0-9]* - matches words not starting with digit
    bencher.bench_local(|| exec_glob(None, black_box("[^0-9]*"), black_box("hello")));
}

#[divan::bench]
fn glob_mixed_wildcards(bencher: Bencher) {
    // Complex pattern with multiple wildcard types
    bencher.bench_local(|| exec_glob(None, black_box("*h?llo*"), black_box("say hello world")));
}

#[divan::bench]
fn glob_file_path_pattern(bencher: Bencher) {
    // Common use case: file path matching
    bencher.bench_local(|| {
        exec_glob(
            None,
            black_box("*/src/*.rs"),
            black_box("/home/user/src/main.rs"),
        )
    });
}

#[divan::bench]
fn glob_long_pattern(bencher: Bencher) {
    let pattern = "The quick brown fox *";
    let text = "The quick brown fox jumps over the lazy dog";
    bencher.bench_local(|| exec_glob(None, black_box(pattern), black_box(text)));
}

#[divan::bench]
fn glob_many_star_wildcards(bencher: Bencher) {
    // Pattern with multiple * wildcards
    let pattern = "*quick*fox*lazy*";
    let text = "The quick brown fox jumps over the lazy dog";
    bencher.bench_local(|| exec_glob(None, black_box(pattern), black_box(text)));
}

// =============================================================================
// GLOB with Cache Benchmarks
// =============================================================================

#[divan::bench]
fn glob_with_cache_first_call(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut cache = HashMap::new();
        exec_glob(
            Some(&mut cache),
            black_box("hello*"),
            black_box("hello world"),
        )
    });
}

#[divan::bench]
fn glob_with_cache_cached_hit(bencher: Bencher) {
    let mut cache = HashMap::new();
    // Warm up the cache
    exec_glob(Some(&mut cache), "hello*", "hello world");

    bencher.bench_local(|| {
        exec_glob(
            Some(black_box(&mut cache)),
            black_box("hello*"),
            black_box("hello world"),
        )
    });
}

#[divan::bench]
fn glob_complex_pattern_cached(bencher: Bencher) {
    let mut cache = HashMap::new();
    let pattern = "*quick*fox*lazy*";
    let text = "The quick brown fox jumps over the lazy dog";
    // Warm up the cache
    exec_glob(Some(&mut cache), pattern, text);

    bencher.bench_local(|| {
        exec_glob(
            Some(black_box(&mut cache)),
            black_box(pattern),
            black_box(text),
        )
    });
}

// =============================================================================
// Edge Cases and Special Patterns
// =============================================================================

#[divan::bench]
fn like_empty_pattern(bencher: Bencher) {
    bencher.bench_local(|| exec_like_with_escape(black_box(""), black_box(""), '\\'));
}

#[divan::bench]
fn like_only_percent(bencher: Bencher) {
    // % matches everything
    bencher.bench_local(|| {
        exec_like_with_escape(black_box("%"), black_box("any string at all"), '\\')
    });
}

#[divan::bench]
fn glob_only_star(bencher: Bencher) {
    // * matches everything
    bencher.bench_local(|| exec_glob(None, black_box("*"), black_box("any string at all")));
}

#[divan::bench]
fn like_special_regex_chars(bencher: Bencher) {
    // Pattern with characters that are special in regex
    bencher.bench_local(|| {
        exec_like_with_escape(black_box("test.file"), black_box("test.file"), '\\')
    });
}

#[divan::bench]
fn glob_bracket_special_cases(bencher: Bencher) {
    // Test bracket edge cases
    bencher.bench_local(|| exec_glob(None, black_box("a[]]b"), black_box("a]b")));
}

#[divan::bench]
fn like_unicode_pattern(bencher: Bencher) {
    bencher
        .bench_local(|| exec_like_with_escape(black_box("héllo%"), black_box("héllo world"), '\\'));
}

#[divan::bench]
fn glob_unicode_pattern(bencher: Bencher) {
    bencher.bench_local(|| exec_glob(None, black_box("héllo*"), black_box("héllo world")));
}
