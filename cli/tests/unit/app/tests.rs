use super::*;

#[test]
fn test_normalize_db_path_adds_file_prefix_for_query_params() {
    assert_eq!(
        normalize_db_path("test.db?locking=shared_reads".into()),
        "file:test.db?locking=shared_reads"
    );
}

#[test]
fn test_normalize_db_path_preserves_existing_file_prefix() {
    assert_eq!(
        normalize_db_path("file:test.db?mode=ro".into()),
        "file:test.db?mode=ro"
    );
}

#[test]
fn test_normalize_db_path_preserves_file_triple_slash() {
    assert_eq!(
        normalize_db_path("file:///tmp/test.db?mode=ro".into()),
        "file:///tmp/test.db?mode=ro"
    );
}

#[test]
fn test_normalize_db_path_plain_path_unchanged() {
    assert_eq!(normalize_db_path("test.db".into()), "test.db");
}

#[test]
fn test_normalize_db_path_memory_unchanged() {
    assert_eq!(normalize_db_path(":memory:".into()), ":memory:");
}

#[test]
fn test_normalize_db_path_multiple_query_params() {
    assert_eq!(
        normalize_db_path("test.db?locking=shared_reads&cache=shared".into()),
        "file:test.db?locking=shared_reads&cache=shared"
    );
}

#[test]
fn test_normalize_db_path_absolute_path_with_query() {
    assert_eq!(
        normalize_db_path("/tmp/my.db?mode=ro".into()),
        "file:/tmp/my.db?mode=ro"
    );
}

#[test]
fn test_normalize_db_path_question_mark_in_filename_no_query() {
    // '?' is legitimately part of the filename, no key=value follows
    assert_eq!(normalize_db_path("what?.db".into()), "what?.db");
}

#[test]
fn test_normalize_db_path_filename_contains_question_mark_with_query() {
    // File is literally "foo.bar?mode=ro", opened with ?mode=ro query.
    // The '?' in the filename must be percent-encoded so the URI parser
    // treats only the last ?mode=ro as the query string.
    assert_eq!(
        normalize_db_path("foo.bar?mode=ro?mode=ro".into()),
        "file:foo.bar%3Fmode=ro?mode=ro"
    );
}
