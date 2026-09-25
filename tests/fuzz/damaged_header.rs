#[cfg(all(test, not(feature = "checksum")))]
mod damaged_header_tests {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::panic::AssertUnwindSafe;
    use std::path::Path;
    use std::sync::Arc;

    use rand::seq::IndexedRandom;
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use tempfile::TempDir;
    use turso_core::{Database, PlatformIO, SqliteDialect};

    use core_tester::common::{
        maybe_setup_tracing, rng_from_time_or_env, rusqlite_integrity_check,
    };

    const ROUNDS: usize = 60;
    const FREELIST_TRUNK_OFFSET: u64 = 32;
    const FREELIST_COUNT_OFFSET: u64 = 36;

    #[derive(Debug, Clone, Copy)]
    enum Damage {
        FreelistCount(u32),
        FreelistTrunk(u32),
    }

    #[derive(Debug)]
    struct Outcome {
        first_error: Option<String>,
        rows: Vec<(i64, String)>,
        sqlite_integrity_ok: bool,
    }

    /// Damages one freelist field of a database that SQLite wrote, then runs
    /// the same writes through Turso and SQLite. When SQLite rejects the file,
    /// Turso must return an error too; when SQLite accepts it, Turso must
    /// accept it and end with the same rows and integrity_check verdict.
    #[test]
    fn damaged_freelist_header_is_handled_like_sqlite() {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("damaged_freelist_header_is_handled_like_sqlite seed: {seed}");

        for round in 0..ROUNDS {
            let dir = TempDir::new().unwrap();
            let original = dir.path().join("original.db");
            create_database_with_freelist(&mut rng, &original);
            let damage = damage_database(&mut rng, &original);
            let workload = random_workload(&mut rng);
            let context = format!("seed {seed}, round {round}, {damage:?}");

            let sqlite_path = dir.path().join("sqlite.db");
            std::fs::copy(&original, &sqlite_path).unwrap();
            let sqlite = run_sqlite(&sqlite_path, &workload);

            let turso_path = dir.path().join("turso.db");
            std::fs::copy(&original, &turso_path).unwrap();
            let turso =
                std::panic::catch_unwind(AssertUnwindSafe(|| run_turso(&turso_path, &workload)))
                    .unwrap_or_else(|_| panic!("{context}: Turso panicked"));

            match (&sqlite.first_error, &turso.first_error) {
                (Some(_), Some(_)) => {}
                (Some(sqlite_error), None) => panic!(
                    "{context}: SQLite rejected the damaged file ({sqlite_error}), Turso accepted it"
                ),
                (None, Some(turso_error)) => panic!(
                    "{context}: SQLite accepted the damaged file, Turso rejected it ({turso_error})"
                ),
                (None, None) => {
                    assert_eq!(turso.rows, sqlite.rows, "{context}: rows differ");
                    assert_eq!(
                        turso.sqlite_integrity_ok, sqlite.sqlite_integrity_ok,
                        "{context}: integrity_check verdicts differ"
                    );
                }
            }
        }
    }

    fn create_database_with_freelist(rng: &mut ChaCha8Rng, path: &Path) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT)", ())
            .unwrap();
        if rng.random_bool(0.5) {
            conn.execute("CREATE INDEX t_data ON t(data)", ()).unwrap();
        }
        let rows = rng.random_range(50..400);
        conn.execute("BEGIN", ()).unwrap();
        for id in 0..rows {
            conn.execute("INSERT INTO t VALUES (?1, ?2)", (id, random_text(rng)))
                .unwrap();
        }
        conn.execute("COMMIT", ()).unwrap();
        let keep_below = rng.random_range(1..rows / 2);
        conn.execute("DELETE FROM t WHERE id >= ?1", (keep_below,))
            .unwrap();
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |_| Ok(()))
            .unwrap();
    }

    fn damage_database(rng: &mut ChaCha8Rng, path: &Path) -> Damage {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let trunk = read_u32(&mut file, FREELIST_TRUNK_OFFSET);
        let count = read_u32(&mut file, FREELIST_COUNT_OFFSET);
        assert!(trunk > 0 && count > 0, "the workload must leave free pages");

        let damage = *[Damage::FreelistCount(0), Damage::FreelistTrunk(0)]
            .choose(rng)
            .unwrap();
        let (offset, value) = match damage {
            Damage::FreelistCount(value) => (FREELIST_COUNT_OFFSET, value),
            Damage::FreelistTrunk(value) => (FREELIST_TRUNK_OFFSET, value),
        };
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&value.to_be_bytes()).unwrap();
        file.sync_all().unwrap();
        damage
    }

    fn random_workload(rng: &mut ChaCha8Rng) -> Vec<String> {
        let mut next_id = 1_000;
        (0..rng.random_range(20..120))
            .map(|_| match rng.random_range(0..6) {
                0..6 => {
                    next_id += 1;
                    format!("INSERT INTO t VALUES ({next_id}, '{}')", random_text(rng))
                }
                6..8 => format!(
                    "UPDATE t SET data = '{}' WHERE id % 3 = {}",
                    random_text(rng),
                    rng.random_range(0..3)
                ),
                _ => format!("DELETE FROM t WHERE id % 5 = {}", rng.random_range(0..5)),
            })
            .collect()
    }

    fn random_text(rng: &mut ChaCha8Rng) -> String {
        let letter = *[b'a', b'b', b'c', b'x', b'y'].choose(rng).unwrap() as char;
        letter.to_string().repeat(rng.random_range(20..600))
    }

    fn run_sqlite(path: &Path, workload: &[String]) -> Outcome {
        let conn = rusqlite::Connection::open(path).unwrap();
        let mut first_error = None;
        for sql in workload {
            if let Err(e) = conn.execute(sql, ()) {
                first_error.get_or_insert(e.to_string());
            }
        }
        let rows = conn
            .prepare("SELECT id, data FROM t ORDER BY id")
            .and_then(|mut stmt| {
                stmt.query_map((), |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<Result<_, _>>()
            });
        drop(conn);
        finish(first_error, rows.map_err(|e| e.to_string()), path)
    }

    fn run_turso(path: &Path, workload: &[String]) -> Outcome {
        let io = Arc::new(PlatformIO::new().unwrap());
        let (first_error, rows) =
            match Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)) {
                Err(e) => (Some(e.to_string()), Ok(Vec::new())),
                Ok(db) => {
                    let conn = db.connect().unwrap();
                    let mut first_error = None;
                    for sql in workload {
                        if let Err(e) = conn.execute(sql) {
                            first_error.get_or_insert(e.to_string());
                        }
                    }
                    (first_error, turso_rows(&conn).map_err(|e| e.to_string()))
                }
            };
        finish(first_error, rows, path)
    }

    fn finish(
        first_error: Option<String>,
        rows: Result<Vec<(i64, String)>, String>,
        path: &Path,
    ) -> Outcome {
        match (first_error, rows) {
            (Some(error), _) | (None, Err(error)) => Outcome {
                first_error: Some(error),
                rows: Vec::new(),
                sqlite_integrity_ok: false,
            },
            (None, Ok(rows)) => Outcome {
                first_error: None,
                rows,
                sqlite_integrity_ok: rusqlite_integrity_check(path).is_ok(),
            },
        }
    }

    fn turso_rows(conn: &Arc<turso_core::Connection>) -> turso_core::Result<Vec<(i64, String)>> {
        let mut stmt = conn.prepare("SELECT id, data FROM t ORDER BY id")?;
        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push((row.get::<i64>(0)?, row.get::<&str>(1)?.to_string()));
            Ok(())
        })?;
        Ok(rows)
    }

    fn read_u32(file: &mut std::fs::File, offset: u64) -> u32 {
        let mut bytes = [0u8; 4];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        u32::from_be_bytes(bytes)
    }
}
