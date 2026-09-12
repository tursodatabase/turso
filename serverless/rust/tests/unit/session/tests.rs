use super::*;

#[test]
fn normalize_url_rewrites_schemes() {
    assert_eq!(normalize_url("libsql://db.turso.io"), "https://db.turso.io");
    assert_eq!(normalize_url("turso://db.turso.io"), "https://db.turso.io");
    assert_eq!(normalize_url("https://db.turso.io"), "https://db.turso.io");
    assert_eq!(
        normalize_url("http://localhost:8080"),
        "http://localhost:8080"
    );
}

#[test]
fn normalize_url_strips_trailing_slash() {
    assert_eq!(normalize_url("https://db.turso.io/"), "https://db.turso.io");
    assert_eq!(
        normalize_url("libsql://db.turso.io/"),
        "https://db.turso.io"
    );
    assert_eq!(normalize_url("turso://db.turso.io/"), "https://db.turso.io");
}
