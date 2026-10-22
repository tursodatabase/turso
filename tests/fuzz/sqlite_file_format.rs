use super::helpers;
use core_tester::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};
use rand::{
    seq::{IndexedRandom, SliceRandom},
    Rng,
};
use rand_chacha::ChaCha8Rng;
use rusqlite::{params, params_from_iter, types::Value, Connection};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use tempfile::TempDir;

#[test]
fn sqlite_generated_file_format_fuzz() {
    let (mut rng, seed) = helpers::init_fuzz_test_tracing("sqlite_generated_file_format");
    for iteration in 0..helpers::fuzz_iterations(1) {
        for page_size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            for journal_mode in ["delete", "wal"] {
                let context = format!(
                    "SEED={seed} iteration={iteration} page_size={page_size} journal_mode={journal_mode}"
                );
                println!("{context}");

                let dir = TempDir::new().expect("create file-format test directory");
                let path = dir.path().join("sqlite.db");

                let result = catch_unwind(AssertUnwindSafe(|| {
                    let mut sqlite = Connection::open(&path).expect("create SQLite database");
                    sqlite
                        .pragma_update(None, "page_size", page_size)
                        .expect("set SQLite page size");
                    let mode: String = sqlite
                        .query_row(&format!("PRAGMA journal_mode={journal_mode}"), [], |row| {
                            row.get(0)
                        })
                        .expect("set SQLite journal mode");
                    assert_eq!(mode, journal_mode);

                    let queries = generate_database_and_queries(&mut sqlite, &mut rng, page_size);

                    if journal_mode == "wal" {
                        let checkpoint: (i64, i64, i64) = sqlite
                            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                            })
                            .expect("checkpoint SQLite-generated database");
                        assert_eq!(checkpoint, (0, 0, 0));
                    }

                    sqlite
                        .close()
                        .expect("close SQLite database after generation");

                    let sqlite = Connection::open(&path).expect("reopen SQLite-generated database");
                    check_layout(&sqlite, page_size);

                    let expected = queries
                        .iter()
                        .map(|query| sqlite_exec_rows(&sqlite, query))
                        .collect::<Vec<_>>();
                    sqlite
                        .close()
                        .expect("close SQLite database after verification");

                    let db = TempDatabase::new_with_existent(&path);
                    let conn = db.connect_limbo();
                    for (query, expected) in queries.iter().zip(expected) {
                        let actual = limbo_exec_rows(&conn, query);
                        assert_eq!(actual.len(), expected.len(), "{context}\nquery: {query}");
                        for (row_number, (actual, expected)) in
                            actual.iter().zip(&expected).enumerate()
                        {
                            assert_eq!(
                                actual, expected,
                                "{context}\nquery: {query}\nrow: {row_number}"
                            );
                            for (column_number, (actual, expected)) in
                                actual.iter().zip(expected).enumerate()
                            {
                                if let (Value::Real(actual), Value::Real(expected)) =
                                    (actual, expected)
                                {
                                    assert_eq!(
                                        actual.to_bits(),
                                        expected.to_bits(),
                                        "{context}\nquery: {query}\nrow: {row_number}\ncolumn: {column_number}"
                                    );
                                }
                            }
                        }
                    }
                }));

                if let Err(error) = result {
                    let path = dir.keep();
                    eprintln!("{context}\nTest files preserved in {}", path.display());
                    resume_unwind(error);
                }
            }
        }
    }
}

fn generate_database_and_queries(
    sqlite: &mut Connection,
    rng: &mut ChaCha8Rng,
    page_size: usize,
) -> Vec<String> {
    let tx = sqlite
        .transaction()
        .expect("begin SQLite generation transaction");

    let mut queries = vec![
        "SELECT type, name, tbl_name, rootpage, sql FROM sqlite_schema ORDER BY type, name".into(),
        "PRAGMA page_size".into(),
        "PRAGMA freelist_count".into(),
    ];

    for table in 1..=rng.random_range(2..=6) {
        let column_count = rng.random_range(2..=6);
        let columns = (0..column_count)
            .map(|column| char::from(b'a' + column as u8).to_string())
            .collect::<Vec<_>>();
        let definitions = columns
            .iter()
            .enumerate()
            .map(|(column, name)| {
                let affinity = if column == 0 {
                    ""
                } else {
                    *["", "INTEGER", "REAL", "TEXT", "BLOB", "NUMERIC"]
                        .choose(rng)
                        .unwrap()
                };
                format!("{name} {affinity}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let direction = if rng.random::<bool>() { "ASC" } else { "DESC" };
        tx.execute_batch(&format!(
            "CREATE TABLE t{table}(id INTEGER PRIMARY KEY, {definitions});
             CREATE INDEX idx{table} ON t{table}(a {direction}, b DESC);"
        ))
        .expect("create generated table and index");

        let mut rowids = (0..rng.random_range(64..=if page_size == 512 { 1024 } else { 256 }))
            .map(|_| random_integer(rng))
            .collect::<Vec<_>>();
        rowids.sort_unstable();
        rowids.dedup();
        rowids.shuffle(rng);

        let placeholders = (0..=column_count)
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");

        for &rowid in &rowids {
            let mut values = vec![Value::Integer(rowid)];
            values.extend((0..column_count).map(|_| random_value(rng, page_size)));
            tx.execute(
                &format!("INSERT INTO t{table} VALUES ({placeholders})"),
                params_from_iter(&values),
            )
            .expect("insert generated table row");
        }

        for &rowid in &rowids {
            if rng.random_bool(0.2) {
                tx.execute(&format!("DELETE FROM t{table} WHERE id=?"), [rowid])
                    .expect("delete generated table row");
            } else if rng.random_bool(0.3) {
                tx.execute(
                    &format!("UPDATE t{table} SET b=? WHERE id=?"),
                    params![random_value(rng, page_size), rowid],
                )
                .expect("update generated table row");
            }
        }

        if table == 1 && page_size == 512 {
            // Two 256-byte blobs cannot share a 512-byte leaf. 128 leaves exceed root capacity.
            tx.execute_batch(
                "WITH RECURSIVE row_numbers(a) AS (
                     VALUES(1)
                     UNION ALL
                     SELECT a + 1 FROM row_numbers WHERE a < 128
                 )
                 INSERT INTO t1(a) SELECT zeroblob(256) FROM row_numbers;",
            )
            .expect("populate table for three-level B-tree coverage");
        }

        let projection = format!("id, {}", columns.join(", "));
        queries.extend([
            format!("SELECT {projection} FROM t{table} NOT INDEXED ORDER BY id"),
            format!("SELECT {projection} FROM t{table} NOT INDEXED ORDER BY id DESC"),
            format!(
                "SELECT id, a, b FROM t{table} INDEXED BY idx{table}
                 ORDER BY a {direction}, b DESC, id"
            ),
            format!(
                "SELECT {projection} FROM t{table} INDEXED BY idx{table}
                 WHERE a >= 0 AND a < 128 ORDER BY id"
            ),
            format!(
                "SELECT {projection} FROM t{table} INDEXED BY idx{table}
                 WHERE a IS NULL ORDER BY id"
            ),
            format!(
                "SELECT {projection} FROM t{table} WHERE id >= {} ORDER BY id LIMIT 10",
                rowids.choose(rng).unwrap()
            ),
        ]);
    }

    tx.execute_batch(
        "CREATE TABLE config(value BLOB, key TEXT, n INTEGER, PRIMARY KEY(key, n)) WITHOUT ROWID",
    )
    .expect("create WITHOUT ROWID table");

    for row in 0..64 {
        let value = match row {
            0 => random_blob(rng, 65536 + 1),
            1 => Value::Integer(i64::MIN),
            2 => Value::Integer(i64::MAX),
            _ => random_value(rng, page_size),
        };
        tx.execute(
            "INSERT INTO config VALUES (?, ?, ?)",
            params![value, format!("key{}", rng.random_range(0..8)), row],
        )
        .expect("insert WITHOUT ROWID table row");
    }

    queries.push("SELECT value, key, n FROM config ORDER BY key, n".into());

    tx.execute_batch("CREATE TABLE discarded(data BLOB)")
        .expect("create table to generate free pages");
    tx.execute(
        "INSERT INTO discarded VALUES (zeroblob(?))",
        [page_size * rng.random_range(24..=512)],
    )
    .expect("populate table to generate free pages");

    tx.execute_batch("DROP TABLE discarded; CREATE TABLE reused(data BLOB)")
        .expect("drop discarded table and create reused table");
    tx.execute("INSERT INTO reused VALUES (?)", [Value::Blob(Vec::new())])
        .expect("insert empty blob into reused table");

    queries.push("SELECT data FROM reused ORDER BY rowid".into());

    tx.commit().expect("commit SQLite generation transaction");
    queries
}

fn random_value(rng: &mut ChaCha8Rng, page_size: usize) -> Value {
    match rng.random_range(0..5) {
        0 => Value::Null,
        1 => Value::Integer(random_integer(rng)),
        2 => Value::Real(random_real(rng)),
        3 => Value::Text(random_text(rng, page_size)),
        4 => {
            let length = random_length(rng, page_size);
            random_blob(rng, length)
        }
        _ => unreachable!(),
    }
}

fn random_integer(rng: &mut ChaCha8Rng) -> i64 {
    let bits = *[8, 16, 24, 32, 48, 64].choose(rng).unwrap();
    rng.random::<i64>() >> (64 - bits)
}

fn random_real(rng: &mut ChaCha8Rng) -> f64 {
    let fraction_bits = rng.random_range(0..=52);
    let bits = rng.random::<u64>();
    let fraction = bits & ((1 << fraction_bits) - 1);
    f64::from_bits((bits & (u64::MAX << 52)) | fraction)
}

fn random_text(rng: &mut ChaCha8Rng, page_size: usize) -> String {
    let max_width = rng.random_range(1..=4);
    let length = random_length(rng, page_size);
    let mut text = String::with_capacity(length);
    while text.len() < length {
        let character = match max_width.min(length - text.len()) {
            1 => rng.random_range('\0'..='\u{7f}'),
            2 => rng.random_range('\0'..='\u{7ff}'),
            3 => rng.random_range('\0'..='\u{ffff}'),
            _ => rng.random::<char>(),
        };
        text.push(character);
    }
    text
}

fn random_length(rng: &mut ChaCha8Rng, page_size: usize) -> usize {
    let max_length = page_size * 2 + 1;
    let bits = rng.random_range(0..=max_length.ilog2() + 1);
    rng.random_range(0..=max_length.min((1 << bits) - 1))
}

fn random_blob(rng: &mut ChaCha8Rng, length: usize) -> Value {
    let mut bytes = vec![0; length];
    rng.fill(bytes.as_mut_slice());
    Value::Blob(bytes)
}

fn check_layout(sqlite: &Connection, page_size: usize) {
    let actual_page_size: usize = sqlite
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("read SQLite page size");
    assert_eq!(actual_page_size, page_size);

    assert_eq!(
        sqlite_exec_rows(sqlite, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );

    let freelist_count: usize = sqlite
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .expect("read SQLite freelist count");
    assert!(freelist_count > 0);

    if page_size == 512 {
        // dbstat leaf paths have one slash per B-tree level.
        let table_btree_levels: usize = sqlite
            .query_row(
                "SELECT max(length(path) - length(replace(path, '/', '')))
                 FROM dbstat WHERE name = 't1' AND pagetype = 'leaf'",
                [],
                |row| row.get(0),
            )
            .expect("read generated table B-tree depth");
        assert!(
            table_btree_levels >= 3,
            "table B-tree levels: {table_btree_levels}"
        );

        let min_overflow_pages = 128;
        let overflow_path_pattern = format!("*+{:06x}", min_overflow_pages - 1);
        let has_long_overflow_chain: bool = sqlite
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM dbstat
                     WHERE name = 'config' AND pagetype = 'overflow'
                       AND path GLOB ?
                 )",
                [overflow_path_pattern],
                |row| row.get(0),
            )
            .expect("read generated overflow chain layout");
        assert!(
            has_long_overflow_chain,
            "expected at least {min_overflow_pages} overflow pages in one chain"
        );
    }
}
