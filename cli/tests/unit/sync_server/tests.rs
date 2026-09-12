use super::*;
use std::path::Path;

#[test]
fn validates_database_names() {
    for ok in ["db1", "a-b_c", "x", "nul", "con", &"n".repeat(128)] {
        assert!(validate_db_name(ok), "expected {ok:?} to be valid");
    }
    for bad in [
        "",
        "..",
        "../x",
        "a/b",
        "a\\b",
        ".hidden",
        "a.b",
        "a%2fb",
        "a b",
        "A1",
        "Db1",
        "DB1",
        &"n".repeat(129),
    ] {
        assert!(!validate_db_name(bad), "expected {bad:?} to be rejected");
    }
}

/// Mirrors the read loop: the terminator must be found whatever the chunk
/// boundaries, including when it straddles two reads.
#[test]
fn finds_header_end_across_read_boundaries() {
    let request = b"POST / HTTP/1.1\r\nHost: x\r\n\r\nbody".to_vec();
    let expected = find_header_end(&request, 0).expect("terminator is present");

    for chunk in 1..=request.len() {
        let mut data = Vec::new();
        let mut found = None;
        for piece in request.chunks(chunk) {
            let unscanned = data.len().saturating_sub(3);
            data.extend_from_slice(piece);
            if let Some(end) = find_header_end(&data, unscanned) {
                found = Some(end);
                break;
            }
        }
        assert_eq!(found, Some(expected), "missed terminator at chunk {chunk}");
    }
}

#[test]
fn rejects_content_length_that_overflows() {
    assert!(request_end(0, usize::MAX).is_err());
    assert_eq!(request_end(10, 5).unwrap(), 19);
}

#[test]
fn parses_single_and_multi_routes() {
    assert_eq!(
        parse_route("POST", "/v2/pipeline"),
        Route::Pipeline { db: None }
    );
    assert_eq!(
        parse_route("POST", "/pull-updates"),
        Route::PullUpdates { db: None }
    );
    assert_eq!(
        parse_route("POST", "/db/db1/v2/pipeline"),
        Route::Pipeline { db: Some("db1") }
    );
    assert_eq!(
        parse_route("POST", "/db/db1/pull-updates"),
        Route::PullUpdates { db: Some("db1") }
    );
    assert_eq!(parse_route("OPTIONS", "/anything"), Route::Options);
    assert_eq!(parse_route("GET", "/v2/pipeline"), Route::NotFound);
    assert_eq!(parse_route("POST", "/nope"), Route::NotFound);
    assert_eq!(parse_route("POST", "/db/a/b/v2/pipeline"), Route::NotFound);
    assert_eq!(
        parse_route("POST", "/db//v2/pipeline"),
        Route::Pipeline { db: Some("") }
    );
}

#[test]
fn each_database_resolves_its_own_files() {
    let base = Path::new("/tmp/dbs");
    let db1 = db_path_for(base, "db1");
    let db2 = db_path_for(base, "db2");

    assert_eq!(db1, Path::new("/tmp/dbs/db1/data"));
    assert_ne!(db1, db2);

    let log1 = logical_log_path(&db1.to_string_lossy()).unwrap();
    let log2 = logical_log_path(&db2.to_string_lossy()).unwrap();
    assert_ne!(log1, log2, "databases must not share a logical log");
    assert_eq!(log1, Path::new("/tmp/dbs/db1/data.db-log"));
}

const TEST_MAX_OPEN: usize = 4;

fn dir_server(base: &Path) -> TursoSyncServer {
    TursoSyncServer::new_dir(
        "127.0.0.1:0".to_string(),
        base.to_path_buf(),
        Arc::new(AtomicUsize::new(0)),
        OpenConfig {
            vfs: None,
            flags: OpenFlags::default(),
            db_opts: DatabaseOpts::new(),
            max_open: TEST_MAX_OPEN,
        },
    )
    .unwrap()
}

#[test]
fn refuses_new_databases_once_the_open_map_is_full() {
    let base = tempfile::TempDir::new().unwrap();
    let server = dir_server(base.path());

    for i in 0..TEST_MAX_OPEN {
        let name = format!("db{i}");
        if let Err(resp) = server.resolve_db(Some(&name)) {
            panic!(
                "opening {name} under the cap must succeed, got {}",
                resp.status
            );
        }
    }

    assert!(
        server.resolve_db(Some("db0")).is_ok(),
        "an already open database stays reachable at capacity"
    );
    let Err(refused) = server.resolve_db(Some("overflow")) else {
        panic!("a full open map must refuse an unknown database");
    };
    assert_eq!(refused.status, 503);
    assert!(
        !base.path().join("overflow").exists(),
        "a refused database must not reach the filesystem"
    );
}

#[test]
fn refuses_a_database_directory_that_escapes_the_served_tree() {
    let base = tempfile::TempDir::new().unwrap();
    let outside = tempfile::TempDir::new().unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(outside.path(), base.path().join("escaped")).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(outside.path(), base.path().join("escaped")).unwrap();

    let server = dir_server(base.path());
    let Err(refused) = server.resolve_db(Some("escaped")) else {
        panic!("a symlinked database directory must be refused");
    };
    assert_eq!(refused.status, 404);
    assert!(
        !outside.path().join("data").exists(),
        "a refused name must not create files outside the served tree"
    );
}
