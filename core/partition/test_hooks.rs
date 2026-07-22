//! Test-only coordination hooks for process-level partition crash tests.

use std::io::Write;

const POINT_ENV: &str = "TURSO_PARTITION_TEST_PAUSE_POINT";
const MARKER_ENV: &str = "TURSO_PARTITION_TEST_PAUSE_MARKER";
const RELEASE_ENV: &str = "TURSO_PARTITION_TEST_RELEASE_MARKER";

pub(crate) fn pause_if_requested(point: &str) {
    let Ok(requested) = std::env::var(POINT_ENV) else {
        return;
    };
    if requested != point {
        return;
    }

    let marker = std::env::var_os(MARKER_ENV)
        .map(std::path::PathBuf::from)
        .expect("partition crash-test pause requires a marker path");
    let mut marker_file =
        std::fs::File::create(&marker).expect("partition crash-test marker must be writable");
    marker_file
        .write_all(point.as_bytes())
        .and_then(|()| marker_file.sync_all())
        .expect("partition crash-test marker must be durable");

    if let Some(release) = std::env::var_os(RELEASE_ENV).map(std::path::PathBuf::from) {
        while !release.exists() {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        return;
    }

    loop {
        std::thread::park_timeout(std::time::Duration::from_secs(60));
    }
}
