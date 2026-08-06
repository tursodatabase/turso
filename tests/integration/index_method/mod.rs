use std::collections::HashMap;
use std::sync::Arc;

use core_tester::common::rng_from_time_or_env;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use turso_core::index_method::fts::FtsIndexMethod;
use turso_core::{
    index_method::{
        toy_vector_sparse_ivf::VectorSparseInvertedIndexMethod, IndexMethod, IndexMethodAttachment,
        IndexMethodConfiguration, IndexMethodContext,
    },
    schema::IndexColumn,
    types::IOResult,
    vector::{self, vector_types::VectorType},
    Numeric, Register, Result, Value, MAIN_DB_ID,
};

use crate::common::{limbo_exec_rows, TempDatabase};

fn run<T>(db: &TempDatabase, mut f: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
    loop {
        match f()? {
            IOResult::Done(value) => return Ok(value),
            IOResult::IO(iocompletions) => {
                while !iocompletions.finished() {
                    db.io.step().unwrap();
                }
            }
        }
    }
}

fn index_method_context(
    connection: &Arc<turso_core::Connection>,
    attachment: &dyn IndexMethodAttachment,
) -> IndexMethodContext {
    IndexMethodContext::for_test(connection, MAIN_DB_ID, attachment).unwrap()
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
fn fts_test_stats(
    db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    table_name: &str,
    index_name: &str,
    columns: &[(&str, usize)],
) -> turso_core::index_method::IndexMethodTestStats {
    let attachment = FtsIndexMethod
        .attach(&IndexMethodConfiguration {
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
            columns: columns
                .iter()
                .map(|&(name, index)| IndexColumn::new(name, index))
                .collect(),
            parameters: HashMap::default(),
        })
        .unwrap();
    let mut cursor = attachment.init().unwrap();
    run(db, || {
        cursor.open_read(&index_method_context(conn, attachment.as_ref()))
    })
    .unwrap();
    cursor.test_stats().unwrap().unwrap()
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
fn fts_attachment_test_stats(
    db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    table_name: &str,
    index_name: &str,
) -> turso_core::index_method::IndexMethodTestStats {
    let attachment = conn
        .with_schema_mut(|schema| {
            schema
                .get_index(table_name, index_name)
                .and_then(|index| index.index_method.clone())
        })
        .unwrap()
        .expect("FTS attachment must exist in the connection schema");
    let mut cursor = attachment.init().unwrap();
    run(db, || {
        cursor.open_read(&index_method_context(conn, attachment.as_ref()))
    })
    .unwrap();
    cursor.test_stats().unwrap().unwrap()
}

fn sparse_vector(v: &str) -> Value {
    let vector = vector::operations::text::vector_from_text(VectorType::Float32Sparse, v).unwrap();
    vector::operations::serialize::vector_serialize(vector).expect(turso_core::alloc::ALLOC_ERR_MSG)
}

// This raw-cursor test manually opens pager write transactions.
#[turso_macros::test(init_sql = "CREATE TABLE t(name, embedding)")]
fn test_vector_sparse_ivf_create_destroy(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let schema_rows = || {
        limbo_exec_rows(&conn, "SELECT * FROM sqlite_master")
            .into_iter()
            .map(|x| match &x[1] {
                rusqlite::types::Value::Text(t) => t.clone(),
                _ => unreachable!(),
            })
            .collect::<Vec<String>>()
    };

    assert_eq!(schema_rows(), vec!["t"]);

    let index = VectorSparseInvertedIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "t".to_string(),
            index_name: "t_idx".to_string(),
            columns: vec![IndexColumn::new("embedding", 1)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();
    assert_eq!(
        schema_rows(),
        vec!["t", "t_idx_inverted_index", "t_idx_stats"]
    );

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.destroy(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();
    assert_eq!(schema_rows(), vec!["t"]);
}

// This raw-cursor test manually opens pager write transactions.
#[turso_macros::test(init_sql = "CREATE TABLE t(name, embedding)")]
fn test_vector_sparse_ivf_insert_query(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let index = VectorSparseInvertedIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "t".to_string(),
            index_name: "t_idx".to_string(),
            columns: vec![IndexColumn::new("embedding", 1)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    for (i, vector_str) in [
        "[0, 0, 0, 1]",
        "[0, 0, 1, 0]",
        "[0, 1, 0, 0]",
        "[1, 0, 0, 0]",
    ]
    .iter()
    .enumerate()
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_write(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        let values = [
            Register::Value(sparse_vector(vector_str)),
            Register::Value(Value::from_i64((i + 1) as i64)),
        ];
        run(&tmp_db, || cursor.insert(&values)).unwrap();
        conn.execute(format!(
            "INSERT INTO t VALUES ('{i}', vector32_sparse('{vector_str}'))"
        ))
        .unwrap();
    }
    for (vector, results) in [
        ("[0, 0, 0, 1]", &[(1, 0.0)][..]),
        ("[0, 0, 1, 0]", &[(2, 0.0)][..]),
        ("[0, 1, 0, 0]", &[(3, 0.0)][..]),
        ("[1, 0, 0, 0]", &[(4, 0.0)][..]),
        ("[1, 0, 0, 1]", &[(1, 0.5), (4, 0.5)][..]),
        (
            "[1, 1, 1, 1]",
            &[(1, 0.75), (2, 0.75), (3, 0.75), (4, 0.75)][..],
        ),
    ] {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_read(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        let values = [
            Register::Value(Value::from_i64(0)),
            Register::Value(sparse_vector(vector)),
            Register::Value(Value::from_i64(5)),
        ];
        assert!(run(&tmp_db, || cursor.query_start(&values)).unwrap());

        for (i, (rowid, dist)) in results.iter().enumerate() {
            assert_eq!(
                *rowid,
                run(&tmp_db, || cursor.query_rowid()).unwrap().unwrap()
            );
            assert_eq!(
                *dist,
                run(&tmp_db, || cursor.query_column(0)).unwrap().as_float()
            );
            assert_eq!(
                i + 1 < results.len(),
                run(&tmp_db, || cursor.query_next()).unwrap()
            );
        }
    }
}

// This raw-cursor test manually opens pager write transactions.
#[turso_macros::test(init_sql = "CREATE TABLE t(name, embedding)")]
fn test_vector_sparse_ivf_update(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let index = VectorSparseInvertedIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "t".to_string(),
            index_name: "t_idx".to_string(),
            columns: vec![IndexColumn::new("embedding", 1)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    let mut writer = attached.init().unwrap();
    run(&tmp_db, || {
        writer.open_write(&index_method_context(&conn, attached.as_ref()))
    })
    .unwrap();

    let v0_str = "[0, 1, 0, 0]";
    let v1_str = "[1, 0, 0, 1]";
    let q = sparse_vector("[1, 0, 0, 1]");
    let v0 = sparse_vector(v0_str);
    let v1 = sparse_vector(v1_str);
    let insert0_values = [
        Register::Value(v0.clone()),
        Register::Value(Value::from_i64(1)),
    ];
    let insert1_values = [
        Register::Value(v1.clone()),
        Register::Value(Value::from_i64(1)),
    ];
    let query_values = [
        Register::Value(Value::from_i64(0)),
        Register::Value(q.clone()),
        Register::Value(Value::from_i64(1)),
    ];
    run(&tmp_db, || writer.insert(&insert0_values)).unwrap();
    conn.execute(format!(
        "INSERT INTO t VALUES ('test', vector32_sparse('{v0_str}'))"
    ))
    .unwrap();

    let mut reader = attached.init().unwrap();
    run(&tmp_db, || {
        reader.open_read(&index_method_context(&conn, attached.as_ref()))
    })
    .unwrap();
    assert!(!run(&tmp_db, || reader.query_start(&query_values)).unwrap());

    conn.execute(format!(
        "UPDATE t SET embedding = vector32_sparse('{v1_str}') WHERE rowid = 1"
    ))
    .unwrap();
    run(&tmp_db, || writer.delete(&insert0_values)).unwrap();
    run(&tmp_db, || writer.insert(&insert1_values)).unwrap();

    let mut reader = attached.init().unwrap();
    run(&tmp_db, || {
        reader.open_read(&index_method_context(&conn, attached.as_ref()))
    })
    .unwrap();
    assert!(run(&tmp_db, || reader.query_start(&query_values)).unwrap());
    assert_eq!(1, run(&tmp_db, || reader.query_rowid()).unwrap().unwrap());
    assert_eq!(
        0.0,
        run(&tmp_db, || reader.query_column(0)).unwrap().as_float()
    );
    assert!(!run(&tmp_db, || reader.query_next()).unwrap());
}

#[turso_macros::test(mvcc)]
fn test_vector_sparse_ivf_mvcc_sql(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE vectors(id INTEGER PRIMARY KEY, embedding)")
        .unwrap();
    conn.execute("CREATE INDEX vectors_idx ON vectors USING toy_vector_sparse_ivf (embedding)")
        .unwrap();
    conn.execute(
        "INSERT INTO vectors VALUES \
         (1, vector32_sparse('[1, 0, 0]')), \
         (2, vector32_sparse('[0, 1, 0]'))",
    )
    .unwrap();

    let nearest = |vector: &str| {
        limbo_exec_rows(
            &conn,
            &format!(
                "SELECT id FROM vectors \
                 ORDER BY vector_distance_jaccard(embedding, vector32_sparse('{vector}')) \
                 LIMIT 1"
            ),
        )
    };
    assert_eq!(
        nearest("[1, 0, 0]"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE vectors SET embedding = vector32_sparse('[0, 0, 1]') WHERE id = 1")
        .unwrap();
    assert_eq!(
        nearest("[0, 0, 1]"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(
        nearest("[1, 0, 0]"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(
        nearest("[0, 1, 0]"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

// This differential harness disables automatic WAL actions on both databases.
#[turso_macros::test]
fn test_vector_sparse_ivf_fuzz(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();

    let opts = tmp_db.db_opts;
    let flags = tmp_db.db_flags;

    const DIMS: usize = 40;
    const MOD: u32 = 5;

    let (mut rng, _) = rng_from_time_or_env();
    let mut operation = 0;
    for delta in [0.0, 0.01, 0.05, 0.1, 0.5] {
        let seed = rng.next_u64();
        tracing::info!("======== seed: {} ========", seed);

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let builder = TempDatabase::builder()
            .with_opts(opts)
            .with_flags(flags)
            .with_init_sql("CREATE TABLE t(key TEXT PRIMARY KEY, embedding)");
        let simple_db = builder.clone().build();
        let index_db = builder.build();
        tracing::info!(
            "simple_db: {:?}, index_db: {:?}",
            simple_db.path,
            index_db.path,
        );
        let simple_conn = simple_db.connect_limbo();
        let index_conn = index_db.connect_limbo();
        simple_conn.wal_auto_actions_disable();
        index_conn.wal_auto_actions_disable();
        index_conn
            .execute(format!("CREATE INDEX t_idx ON t USING toy_vector_sparse_ivf (embedding) WITH (delta = {delta})"))
            .unwrap();

        let vector = |rng: &mut ChaCha8Rng| {
            let mut values = Vec::with_capacity(DIMS);
            for _ in 0..DIMS {
                if rng.next_u32() % MOD == 0 {
                    values.push((rng.next_u32() as f32 / (u32::MAX as f32)).to_string());
                } else {
                    values.push("0".to_string())
                }
            }
            format!("[{}]", values.join(", "))
        };

        let mut keys = Vec::new();
        for _ in 0..200 {
            let choice = rng.next_u32() % 4;
            operation += 1;
            if choice == 0 {
                let key = rng.next_u64().to_string();
                let v = vector(&mut rng);
                let sql = format!("INSERT INTO t VALUES ('{key}', vector32_sparse('{v}'))");
                tracing::info!("({}) {}", operation, sql);
                simple_conn.execute(&sql).unwrap();
                index_conn.execute(sql).unwrap();
                keys.push(key);
            } else if choice == 1 && !keys.is_empty() {
                let idx = rng.next_u32() as usize % keys.len();
                let key = &keys[idx];
                let v = vector(&mut rng);
                let sql =
                    format!("UPDATE t SET embedding = vector32_sparse('{v}') WHERE key = '{key}'",);
                tracing::info!("({}) {}", operation, sql);
                simple_conn.execute(&sql).unwrap();
                index_conn.execute(&sql).unwrap();
            } else if choice == 2 && !keys.is_empty() {
                let idx = rng.next_u32() as usize % keys.len();
                let key = &keys[idx];
                let sql = format!("DELETE FROM t WHERE key = '{key}'");
                tracing::info!("({}) {}", operation, sql);
                simple_conn.execute(&sql).unwrap();
                index_conn.execute(&sql).unwrap();
                keys.remove(idx);
            } else {
                let v = vector(&mut rng);
                let k = rng.next_u32() % 20 + 1;
                let sql = format!(
                    "SELECT key, vector_distance_jaccard(embedding, vector32_sparse('{v}')) as d FROM t ORDER BY d LIMIT {k}"
                );
                tracing::info!("({}) {}", operation, sql);
                let simple_rows = limbo_exec_rows(&simple_conn, &sql);
                let index_rows = limbo_exec_rows(&index_conn, &sql);
                tracing::info!("simple: {:?}, index_rows: {:?}", simple_rows, index_rows);
                assert!(index_rows.len() <= simple_rows.len());
                for (a, b) in index_rows.iter().zip(simple_rows.iter()) {
                    if delta == 0.0 {
                        assert_eq!(a, b);
                    } else {
                        match (&a[1], &b[1]) {
                            (rusqlite::types::Value::Real(a), rusqlite::types::Value::Real(b)) => {
                                assert!(
                                    *a >= *b || (*a - *b).abs() < 1e-5,
                                    "a={}, b={}, delta={}",
                                    *a,
                                    *b,
                                    delta
                                );
                                assert!(
                                    *a - delta <= *b || (*a - delta - *b).abs() < 1e-5,
                                    "a={}, b={}, delta={}",
                                    *a,
                                    *b,
                                    delta
                                );
                            }
                            _ => panic!("unexpected column values"),
                        }
                    }
                }
                for row in simple_rows.iter().skip(index_rows.len()) {
                    match row[1] {
                        rusqlite::types::Value::Real(r) => assert!((1.0 - r) < 1e-5),
                        _ => panic!("unexpected simple row value"),
                    }
                }
            }
        }
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(init_sql = "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")]
fn test_fts_create_destroy(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let schema_rows = || {
        limbo_exec_rows(
            &conn,
            "SELECT name FROM sqlite_master WHERE type='table' OR type='index'",
        )
        .into_iter()
        .map(|x| match &x[0] {
            rusqlite::types::Value::Text(t) => t.clone(),
            _ => unreachable!(),
        })
        .collect::<Vec<String>>()
    };

    // Initially just the docs table
    assert_eq!(schema_rows(), vec!["docs"]);

    let index = FtsIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "docs".to_string(),
            index_name: "fts_docs".to_string(),
            columns: vec![IndexColumn::new("title", 1), IndexColumn::new("body", 2)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    // After create, should have docs table plus FTS internal tables
    let tables = schema_rows();
    assert!(tables.contains(&"docs".to_string()));
    // FTS creates internal directory table for Tantivy storage
    assert!(tables.iter().any(|t| t.contains("fts_dir")));

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.destroy(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    // After destroy, internal FTS directory tables should be removed
    let tables_after = schema_rows();
    assert!(tables_after.contains(&"docs".to_string()));
    assert!(!tables_after.iter().any(|t| t.contains("fts_dir")));
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(init_sql = "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")]
fn test_fts_insert_query(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let index = FtsIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "docs".to_string(),
            index_name: "fts_docs".to_string(),
            columns: vec![IndexColumn::new("title", 1), IndexColumn::new("body", 2)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    // Insert test documents
    let docs = [
        (
            1,
            "Introduction to Rust",
            "Rust is a systems programming language",
        ),
        (2, "Python Basics", "Python is great for beginners"),
        (
            3,
            "Advanced Rust",
            "Rust has powerful features like ownership",
        ),
        (
            4,
            "Database Systems",
            "Databases store and retrieve data efficiently",
        ),
    ];

    for (id, title, body) in docs {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_write(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        let values = [
            Register::Value(Value::Text(turso_core::types::Text::from(title))),
            Register::Value(Value::Text(turso_core::types::Text::from(body))),
            Register::Value(Value::from_i64(id)),
        ];
        run(&tmp_db, || cursor.insert(&values)).unwrap();
        // Flush FTS data before executing SQL (which auto-commits the transaction)
        // This mimics the VDBE's explicit statement-finalization phase.
        run(&tmp_db, || {
            cursor.prepare_statement_commit(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
        conn.execute(format!(
            "INSERT INTO docs VALUES ({id}, '{title}', '{body}')"
        ))
        .unwrap();
    }

    // Query for "Rust" - should match docs 1 and 3
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_read(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        // Pattern 0 = fts_score pattern with ORDER BY DESC LIMIT
        let values = [
            Register::Value(Value::from_i64(0)), // pattern index
            Register::Value(Value::Text(turso_core::types::Text::from("Rust"))),
            Register::Value(Value::from_i64(10)), // limit
        ];
        assert!(run(&tmp_db, || cursor.query_start(&values)).unwrap());

        // Collect results
        let mut results = Vec::new();
        loop {
            let rowid = run(&tmp_db, || cursor.query_rowid()).unwrap().unwrap();
            let score = run(&tmp_db, || cursor.query_column(0)).unwrap();
            if let Value::Numeric(Numeric::Float(s)) = score {
                results.push((rowid, f64::from(s)));
            }
            if !run(&tmp_db, || cursor.query_next()).unwrap() {
                break;
            }
        }

        // Should have 2 results for "Rust" (docs 1 and 3)
        assert_eq!(results.len(), 2);
        // Both rowids should be 1 or 3
        assert!(results.iter().all(|(r, _)| *r == 1 || *r == 3));
        // Scores should be positive
        assert!(results.iter().all(|(_, s)| *s > 0.0));
    }

    // Query for "Python" - should match doc 2
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_read(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        let values = [
            Register::Value(Value::from_i64(0)),
            Register::Value(Value::Text(turso_core::types::Text::from("Python"))),
            Register::Value(Value::from_i64(10)),
        ];
        assert!(run(&tmp_db, || cursor.query_start(&values)).unwrap());

        let rowid = run(&tmp_db, || cursor.query_rowid()).unwrap().unwrap();
        assert_eq!(rowid, 2);
        assert!(!run(&tmp_db, || cursor.query_next()).unwrap());
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_sql_queries(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table and FTS index via SQL
    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert test data
    conn.execute("INSERT INTO articles VALUES (1, 'Database Performance', 'Optimizing database queries is important for performance')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'Web Development', 'Modern web applications use JavaScript and APIs')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (3, 'Database Design', 'Good database design leads to better performance')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (4, 'API Development', 'RESTful APIs are common in web services')")
        .unwrap();

    // Test fts_score with fts_match query (FTS index requires fts_match in WHERE to be used)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'database') as score, id, title FROM articles WHERE fts_match(title, body, 'database') ORDER BY score DESC LIMIT 10",
    );
    assert_eq!(rows.len(), 2); // Should match docs 1 and 3
                               // Verify results contain expected IDs
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));

    // Test fts_match in WHERE clause with fts_score (combined pattern)
    // 'web' appears in doc 2 ("Web Development") and doc 4 ("web services")
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'web') as score, id, title FROM articles WHERE fts_match(title, body, 'web')",
    );
    assert_eq!(rows.len(), 2); // Should match docs 2 and 4
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&2));
    assert!(ids.contains(&4));
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_order_by_and_limit(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table and FTS index
    conn.execute("CREATE TABLE notes(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_notes ON notes USING fts (title, body)")
        .unwrap();

    // Insert multiple documents with the search term appearing different number of times
    conn.execute("INSERT INTO notes VALUES (1, 'test', 'This is a test document')")
        .unwrap();
    conn.execute("INSERT INTO notes VALUES (2, 'test test', 'test test test')")
        .unwrap();
    conn.execute("INSERT INTO notes VALUES (3, 'another', 'Another document without the keyword')")
        .unwrap();
    conn.execute("INSERT INTO notes VALUES (4, 'test again', 'The test word appears in test')")
        .unwrap();

    // Test ORDER BY score DESC LIMIT
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'test') as score, id FROM notes WHERE fts_match(title, body, 'test') ORDER BY score DESC LIMIT 2",
    );
    assert_eq!(rows.len(), 2);
    // First result should have higher score than second
    let score1 = match &rows[0][0] {
        rusqlite::types::Value::Real(r) => *r,
        _ => panic!("Expected Real"),
    };
    let score2 = match &rows[1][0] {
        rusqlite::types::Value::Real(r) => *r,
        _ => panic!("Expected Real"),
    };
    assert!(score1 >= score2, "Results should be ordered by score DESC");

    // Test without LIMIT - should return all matches
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'test') as score, id FROM notes WHERE fts_match(title, body, 'test') ORDER BY score DESC",
    );
    assert_eq!(rows.len(), 3); // Posts 1, 2, and 4 contain "test"

    // Verify all scores are in descending order
    let scores: Vec<f64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Real(r) => Some(*r),
            _ => None,
        })
        .collect();
    for i in 1..scores.len() {
        assert!(
            scores[i - 1] >= scores[i],
            "Scores should be in descending order"
        );
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_limit_zero_and_negative(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    conn.execute("INSERT INTO articles VALUES (1, 'hello world', 'this is a test')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'another', 'hello again')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (3, 'no match', 'something else')")
        .unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'hello') as score FROM articles ORDER BY score DESC LIMIT 0",
    );
    assert!(rows.is_empty());

    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'hello') as score FROM articles ORDER BY score DESC LIMIT -1",
    );
    assert_eq!(rows.len(), 2);

    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'hello') as score FROM articles WHERE fts_match(title, body, 'hello') ORDER BY score DESC",
    );
    assert_eq!(rows.len(), 2);
}

/// Test FTS function recognition mode - queries that don't match predefined patterns
/// but are optimized via fts_match/fts_score function detection.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_function_recognition(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table with extra columns to ensure queries don't match simple patterns
    conn.execute(
        "CREATE TABLE articles(id INTEGER PRIMARY KEY, author TEXT, category TEXT, title TEXT, body TEXT, views INTEGER)",
    )
    .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert test data
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Alice', 'tech', 'Rust Programming Guide', 'Learn Rust from scratch', 100)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Bob', 'tech', 'Python Basics', 'Introduction to Python', 200)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (3, 'Alice', 'science', 'Rust in Nature', 'Oxidation and rust formation', 50)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (4, 'Charlie', 'tech', 'Advanced Rust Patterns', 'Rust ownership and lifetimes', 300)",
    )
    .unwrap();

    // Test 1: Query with many extra SELECT columns (doesn't match patterns)
    // This exercises function recognition: pattern expects only fts_score() as score
    // but we SELECT multiple additional columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, author, title, category, views, fts_score(title, body, 'Rust') as score FROM articles WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 3); // Posts 1, 3, 4 contain "Rust"
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(ids.contains(&4));

    // Test 2: Query with extra WHERE and multiple columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, title, views FROM articles WHERE fts_match(title, body, 'Rust') AND author = 'Alice'",
    );
    assert_eq!(rows.len(), 2); // Posts 1 and 3 by Alice containing Rust
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));

    // Test 3: Complex query with score, extra columns, WHERE, and ORDER BY
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'Rust') as score, id, title, author FROM articles WHERE fts_match(title, body, 'Rust') AND category = 'tech' ORDER BY score DESC",
    );
    assert_eq!(rows.len(), 2); // Posts 1 and 4 are tech posts about Rust
                               // Verify scores are in descending order
    let scores: Vec<f64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Real(r) => Some(*r),
            _ => None,
        })
        .collect();
    assert!(scores.len() == 2);
    assert!(scores[0] >= scores[1]);

    // Test 4: Query with only fts_match (no fts_score) and extra columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, author, views FROM articles WHERE fts_match(title, body, 'Python')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => assert_eq!(*i, 2),
        _ => panic!("Expected integer id"),
    }
}

/// Test query patterns that wouldn't work with pattern-based matching
/// but should work with function recognition.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_flexible_query_patterns(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY, author TEXT, category TEXT, title TEXT, body TEXT, created_at INTEGER)",
    )
    .unwrap();
    conn.execute("CREATE INDEX fts_docs ON docs USING fts (title, body)")
        .unwrap();

    // Insert test data
    conn.execute("INSERT INTO docs VALUES (1, 'Alice', 'tech', 'Rust Guide', 'Learn Rust programming', 1000)")
        .unwrap();
    conn.execute(
        "INSERT INTO docs VALUES (2, 'Bob', 'tech', 'Python Guide', 'Learn Python basics', 2000)",
    )
    .unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'Alice', 'science', 'Rust Chemistry', 'Rust and oxidation', 3000)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (4, 'Charlie', 'tech', 'Advanced Rust', 'Rust patterns and idioms', 4000)")
        .unwrap();
    conn.execute(
        "INSERT INTO docs VALUES (5, 'Alice', 'tech', 'More Rust', 'Even more Rust content', 5000)",
    )
    .unwrap();

    // Test 1: SELECT specific columns (not * or just score) - wouldn't match patterns
    // Patterns expect SELECT * or SELECT fts_score(...) as score
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, title FROM docs WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 4); // Posts 1, 3, 4, 5

    // Test 2: ORDER BY non-score column ASC - patterns only support ORDER BY score DESC
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, title FROM docs WHERE fts_match(title, body, 'Rust') ORDER BY id ASC",
    );
    assert_eq!(rows.len(), 4);
    // Verify order by id
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![1, 3, 4, 5]);

    // Test 3: ORDER BY non-score column DESC - wouldn't match patterns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, created_at FROM docs WHERE fts_match(title, body, 'Rust') ORDER BY created_at DESC",
    );
    assert_eq!(rows.len(), 4);
    // Verify order by created_at DESC
    let created_ats: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert_eq!(created_ats, vec![5000, 4000, 3000, 1000]);

    // Test 4: Multiple WHERE conditions with different operators
    // Patterns don't have additional WHERE conditions
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Rust') AND created_at >= 3000 AND author = 'Alice'",
    );
    assert_eq!(rows.len(), 2); // Posts 3 and 5 (Alice, Rust, created_at >= 3000)
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&3));
    assert!(ids.contains(&5));

    // Test 5: LIMIT with non-pattern SELECT columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, author FROM docs WHERE fts_match(title, body, 'Rust') LIMIT 2",
    );
    assert_eq!(rows.len(), 2); // Should return exactly 2 rows

    // Test 6: Computed expressions in SELECT - patterns don't handle expressions
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, author || ' wrote ' || title as description FROM docs WHERE fts_match(title, body, 'Python')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][1] {
        rusqlite::types::Value::Text(t) => assert_eq!(t, "Bob wrote Python Guide"),
        _ => panic!("Expected text"),
    }

    // Test 7: fts_score with extra columns and WHERE - wouldn't match combined patterns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'Rust') as score, id, author, category FROM docs WHERE fts_match(title, body, 'Rust') AND category = 'tech'",
    );
    // Should return tech posts about Rust: 1, 4, 5
    assert_eq!(rows.len(), 3);
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&4));
    assert!(ids.contains(&5));
    // Verify scores are returned
    for row in &rows {
        match &row[0] {
            rusqlite::types::Value::Real(score) => assert!(*score > 0.0),
            _ => panic!("Expected real score"),
        }
    }

    // Test 8: Multiple SELECT expressions with score
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id * 10 as id_times_ten, fts_score(title, body, 'Rust') as score FROM docs WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 4);
    // Verify id * 10 calculation works
    let id_times_tens: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    // Should contain 10, 30, 40, 50 (ids 1,3,4,5 * 10)
    assert!(id_times_tens.contains(&10));
    assert!(id_times_tens.contains(&30));
    assert!(id_times_tens.contains(&40));
    assert!(id_times_tens.contains(&50));
}

/// Test FTS with different tokenizer configurations via WITH clause
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_tokenizer_configuration(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Test 1: Default tokenizer (should work without WITH clause)
    conn.execute("CREATE TABLE docs_default(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_default ON docs_default USING fts (content)")
        .unwrap();

    conn.execute("INSERT INTO docs_default VALUES (1, 'Hello World')")
        .unwrap();
    conn.execute("INSERT INTO docs_default VALUES (2, 'hello there')")
        .unwrap();

    // Default tokenizer lowercases, so "hello" should match both
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs_default WHERE fts_match(content, 'hello')",
    );
    assert_eq!(rows.len(), 2);

    // Test 2: Raw tokenizer (exact match only, no tokenization)
    conn.execute("CREATE TABLE docs_raw(id INTEGER PRIMARY KEY, tag TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_raw ON docs_raw USING fts (tag) WITH (tokenizer = 'raw')")
        .unwrap();

    conn.execute("INSERT INTO docs_raw VALUES (1, 'user-123')")
        .unwrap();
    conn.execute("INSERT INTO docs_raw VALUES (2, 'user-456')")
        .unwrap();
    conn.execute("INSERT INTO docs_raw VALUES (3, 'admin-123')")
        .unwrap();

    // Raw tokenizer should only match exact string
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs_raw WHERE fts_match(tag, 'user-123')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => assert_eq!(*i, 1),
        _ => panic!("Expected integer"),
    }

    // Partial match should NOT work with raw tokenizer
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs_raw WHERE fts_match(tag, 'user')",
    );
    assert_eq!(rows.len(), 0);

    // Test 3: Simple tokenizer (whitespace/punctuation split)
    conn.execute("CREATE TABLE docs_simple(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    conn.execute(
        "CREATE INDEX fts_simple ON docs_simple USING fts (content) WITH (tokenizer = 'simple')",
    )
    .unwrap();

    conn.execute("INSERT INTO docs_simple VALUES (1, 'Hello World')")
        .unwrap();
    conn.execute("INSERT INTO docs_simple VALUES (2, 'HELLO there')")
        .unwrap();

    // Simple tokenizer does basic split but preserves case
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs_simple WHERE fts_match(content, 'Hello')",
    );
    // Simple tokenizer in Tantivy lowercases by default too
    assert!(!rows.is_empty());
}

/// Test that invalid tokenizer names are rejected
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_invalid_tokenizer_rejected(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();

    // This should fail because 'invalid_tokenizer' is not a supported tokenizer
    let result = conn.execute(
        "CREATE INDEX fts_docs ON docs USING fts (content) WITH (tokenizer = 'invalid_tokenizer')",
    );
    assert!(result.is_err());
}

/// Test FTS with ngram tokenizer for substring matching
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_ngram_tokenizer(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE INDEX fts_products ON products USING fts (name) WITH (tokenizer = 'ngram')",
    )
    .unwrap();

    conn.execute("INSERT INTO products VALUES (1, 'iPhone 15 Pro')")
        .unwrap();
    conn.execute("INSERT INTO products VALUES (2, 'Samsung Galaxy')")
        .unwrap();
    conn.execute("INSERT INTO products VALUES (3, 'Google Pixel')")
        .unwrap();

    // Ngram tokenizer should allow partial matches
    // Search for "Pho" should match "iPhone"
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM products WHERE fts_match(name, 'Pho')",
    );
    // With ngram(2,3), "Pho" generates ngrams that should match ngrams in "iPhone"
    assert!(!rows.is_empty());

    // Search for "Gal" should match "Galaxy"
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM products WHERE fts_match(name, 'Gal')",
    );
    assert!(!rows.is_empty());
}

/// Test fts_highlight function for text highlighting
/// Signature: fts_highlight(text1, text2, ..., before_tag, after_tag, query)
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_highlight_basic(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Test basic highlighting (single text column)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('The quick brown fox', '<b>', '</b>', 'quick')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "The <b>quick</b> brown fox");
        }
        _ => panic!("Expected text result"),
    }

    // Test multiple matches
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('hello world hello', '[', ']', 'hello')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "[hello] world [hello]");
        }
        _ => panic!("Expected text result"),
    }

    // Test case-insensitive matching (tokenizer lowercases)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('Hello World', '<em>', '</em>', 'hello')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "<em>Hello</em> World");
        }
        _ => panic!("Expected text result"),
    }

    // Test no matches - should return original text
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('The quick brown fox', '<b>', '</b>', 'zebra')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "The quick brown fox");
        }
        _ => panic!("Expected text result"),
    }

    // Test empty query - should return original text
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('Some text here', '<b>', '</b>', '')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "Some text here");
        }
        _ => panic!("Expected text result"),
    }

    // Test multiple text columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('Hello world', 'Goodbye moon', '<b>', '</b>', 'world')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "Hello <b>world</b> Goodbye moon");
        }
        _ => panic!("Expected text result"),
    }
}

/// Test fts_highlight with FTS index queries
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_highlight_with_fts_query(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table and FTS index
    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert test data
    conn.execute("INSERT INTO articles VALUES (1, 'Database Design', 'Learn about database optimization and query performance')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'Web Development', 'Building modern web applications with databases')")
        .unwrap();

    // Query with fts_match and fts_highlight together
    // New signature: fts_highlight(text..., before_tag, after_tag, query)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, fts_highlight(body, '<mark>', '</mark>', 'database') as highlighted FROM articles WHERE fts_match(title, body, 'database')",
    );

    // Should match article 1 (has "database" in both title and body)
    assert!(!rows.is_empty());

    // Check that the highlighted body contains the mark tags
    let mut found_highlight = false;
    for row in &rows {
        if let rusqlite::types::Value::Text(s) = &row[1] {
            if s.contains("<mark>") && s.contains("</mark>") {
                found_highlight = true;
                break;
            }
        }
    }
    assert!(
        found_highlight,
        "Expected highlighted text with <mark> tags"
    );
}

/// Test fts_highlight with NULL values
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_highlight_null_handling(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // NULL text should skip that column (not return NULL)
    // New behavior: NULL text columns are skipped when concatenating
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight(NULL, 'some text', '<b>', '</b>', 'text')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "some <b>text</b>");
        }
        _ => panic!("Expected text result"),
    }

    // NULL query should return NULL
    let rows = limbo_exec_rows(&conn, "SELECT fts_highlight('text', '<b>', '</b>', NULL)");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], rusqlite::types::Value::Null));

    // NULL before_tag should return NULL
    let rows = limbo_exec_rows(&conn, "SELECT fts_highlight('text', NULL, '</b>', 'query')");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], rusqlite::types::Value::Null));

    // NULL after_tag should return NULL
    let rows = limbo_exec_rows(&conn, "SELECT fts_highlight('text', '<b>', NULL, 'query')");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], rusqlite::types::Value::Null));
}

/// Test field weights configuration for FTS indexes
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_field_weights(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table with title and body columns
    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();

    // Create FTS index with title weighted 2x higher than body
    conn.execute(
        "CREATE INDEX fts_weighted ON articles USING fts (title, body) WITH (weights='title=2.0,body=1.0')",
    )
    .unwrap();

    // Insert test data - same word in different columns
    conn.execute("INSERT INTO articles VALUES (1, 'rust programming', 'learn python programming')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'python basics', 'rust is fast')")
        .unwrap();

    // Search for "rust" - article 1 has it in title (2x boost), article 2 has it in body (1x boost)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, fts_score(title, body, 'rust') as score FROM articles WHERE fts_match(title, body, 'rust') ORDER BY score DESC",
    );
    assert_eq!(rows.len(), 2);

    // Article 1 should have higher score (rust in title with 2x boost)
    match &rows[0][0] {
        rusqlite::types::Value::Integer(id) => assert_eq!(*id, 1),
        _ => panic!("Expected integer id"),
    }

    // Article 2 should have lower score (rust in body with 1x boost)
    match &rows[1][0] {
        rusqlite::types::Value::Integer(id) => assert_eq!(*id, 2),
        _ => panic!("Expected integer id"),
    }

    // Verify scores - title match should have higher score than body match
    let score1 = match &rows[0][1] {
        rusqlite::types::Value::Real(s) => *s,
        _ => panic!("Expected real score"),
    };
    let score2 = match &rows[1][1] {
        rusqlite::types::Value::Real(s) => *s,
        _ => panic!("Expected real score"),
    };
    assert!(
        score1 > score2,
        "Title match (boosted 2x) should score higher than body match"
    );
}

/// Test that invalid weight configurations are rejected
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_invalid_weights_rejected(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();

    // Unknown column name should fail
    let result = conn.execute(
        "CREATE INDEX fts_bad ON docs USING fts (title, body) WITH (weights='unknown=2.0')",
    );
    assert!(result.is_err());

    // Invalid weight value should fail
    let result =
        conn.execute("CREATE INDEX fts_bad2 ON docs USING fts (title) WITH (weights='title=abc')");
    assert!(result.is_err());

    // Negative weight should fail
    let result =
        conn.execute("CREATE INDEX fts_bad3 ON docs USING fts (title) WITH (weights='title=-1.0')");
    assert!(result.is_err());

    // Missing equals sign should fail
    let result =
        conn.execute("CREATE INDEX fts_bad4 ON docs USING fts (title) WITH (weights='title2.0')");
    assert!(result.is_err());
}

/// Regression test: Query -> Insert -> Query should not panic with "dirty pages must be empty"
/// This tests that FTS cursor caching doesn't share pending_writes between cursors,
/// which would cause writes from one cursor to affect the Drop behavior of another.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(
    init_sql = "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)"
)]
fn test_fts_query_insert_query_no_panic(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create FTS index
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert some initial data
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Rust Programming', 'Rust is a systems language')",
    )
    .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'Python Guide', 'Python is easy to learn')")
        .unwrap();

    // Query a few times (this caches the directory)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 1);

    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'Python')",
    );
    assert_eq!(rows.len(), 1);

    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'programming')",
    );
    assert_eq!(rows.len(), 1);

    // Insert more data (this should not cause dirty pages to leak to next read)
    conn.execute("INSERT INTO articles VALUES (3, 'Go Tutorial', 'Go is great for concurrency')")
        .unwrap();

    // Query again, should NOT panic with "dirty pages must be empty for read txn"
    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'Go')",
    );
    assert_eq!(rows.len(), 1);
    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 1);
}

/// Comprehensive FTS lifecycle test:
/// 1. Create index on table with many rows
/// 2. Query with FTS methods
/// 3. Insert into table
/// 4. Query again
/// 5. Delete from table
/// 6. Query again
/// 7. Large update
/// 8. Query again
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(
    init_sql = "CREATE TABLE docs(id INTEGER PRIMARY KEY, category TEXT, title TEXT, body TEXT)"
)]
fn test_fts_comprehensive_lifecycle(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // 1. Create FTS index
    conn.execute("CREATE INDEX fts_docs ON docs USING fts (title, body)")
        .unwrap();

    // Insert a moderate number of rows (100 documents across 4 categories)
    let categories = ["tech", "science", "business", "entertainment"];
    let tech_terms = [
        "Rust",
        "Python",
        "JavaScript",
        "programming",
        "software",
        "database",
    ];
    let science_terms = [
        "physics",
        "chemistry",
        "biology",
        "research",
        "experiment",
        "discovery",
    ];
    let business_terms = [
        "market",
        "investment",
        "startup",
        "revenue",
        "growth",
        "strategy",
    ];
    let entertainment_terms = [
        "movie",
        "music",
        "concert",
        "festival",
        "celebrity",
        "streaming",
    ];

    for i in 1..=100 {
        let category = categories[(i - 1) % 4];
        let terms = match category {
            "tech" => &tech_terms,
            "science" => &science_terms,
            "business" => &business_terms,
            _ => &entertainment_terms,
        };
        let term1 = terms[(i - 1) % terms.len()];
        let term2 = terms[i % terms.len()];
        let title = format!("{term1} Article {i}");
        let body = format!("This is article {i} about {term1} and {term2}. More content here.",);
        conn.execute(format!(
            "INSERT INTO docs VALUES ({i}, '{category}', '{title}', '{body}')",
        ))
        .unwrap();
    }

    // 2. Query with FTS methods - verify initial state
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Rust')",
    );
    assert!(!rows.is_empty(), "Should find Rust documents");
    let rust_count_initial = rows.len();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Python')",
    );
    assert!(!rows.is_empty(), "Should find Python documents");

    // Query with score ordering
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'programming') as score, id FROM docs WHERE fts_match(title, body, 'programming') ORDER BY score DESC LIMIT 10",
    );
    assert!(!rows.is_empty(), "Should find programming documents");

    // 3. Insert new documents
    conn.execute("INSERT INTO docs VALUES (101, 'tech', 'Advanced Rust Techniques', 'Deep dive into Rust programming patterns and idioms')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (102, 'tech', 'Rust Memory Safety', 'Exploring Rust ownership and borrowing mechanisms')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (103, 'science', 'Rust Prevention', 'Studying corrosion and metal oxidation')")
        .unwrap();

    // 4. Query again - verify inserts are indexed
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Rust')",
    );
    // Should have more Rust documents now (original + new inserts)
    assert!(
        rows.len() >= rust_count_initial + 2,
        "Should find more Rust documents after insert. Got {}, expected at least {}",
        rows.len(),
        rust_count_initial + 2
    );

    // Verify specific new document is findable
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'ownership borrowing')",
    );
    assert_eq!(rows.len(), 1, "Should find the memory safety document");
    match &rows[0][0] {
        rusqlite::types::Value::Integer(id) => assert_eq!(*id, 102),
        _ => panic!("Expected integer id"),
    }

    // 5. Delete from table
    conn.execute("DELETE FROM docs WHERE id = 101").unwrap();

    // 6. Query again - verify delete is reflected
    // Note: FTS delete support depends on implementation
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Advanced Techniques')",
    );
    // After delete, should not find document 101's content
    let has_deleted_doc = rows
        .iter()
        .any(|r| matches!(&r[0], rusqlite::types::Value::Integer(101)));
    assert!(!has_deleted_doc && rows.is_empty());

    // Other documents should still be queryable
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'ownership')",
    );
    assert_eq!(
        rows.len(),
        1,
        "Document 102 should still be findable after deleting 101"
    );

    // 7. Large update - update many rows
    conn.execute("UPDATE docs SET title = 'Updated ' || title WHERE category = 'tech'")
        .unwrap();

    // 8. Query again after update
    // Note: FTS update support may vary - just verify no panics and basic queries work
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Python')",
    );
    assert!(
        !rows.is_empty(),
        "Should still find Python documents after update"
    );

    let _science_rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'science')",
    );
    // Science docs weren't updated, should still work
    // Note: "science" might be in body text or not

    // Verify fts_score still works
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'database') as score, id FROM docs WHERE fts_match(title, body, 'database') ORDER BY score DESC",
    );
    // Just verify it doesn't panic and returns valid results
    for row in &rows {
        match &row[0] {
            rusqlite::types::Value::Real(score) => assert!(*score >= 0.0),
            rusqlite::types::Value::Integer(_) => {} // Some implementations may return int
            _ => panic!("Expected numeric score"),
        }
    }

    // Final verification - complex query with multiple conditions
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'Rust') as score, id, category FROM docs WHERE fts_match(title, body, 'Rust') AND category = 'tech' ORDER BY score DESC LIMIT 5",
    );
    // Should find tech documents about Rust
    assert!(
        !rows.is_empty(),
        "Should find tech documents about Rust with complex query"
    );

    // Verify all results have category='tech'
    for row in &rows {
        match &row[2] {
            rusqlite::types::Value::Text(cat) => assert_eq!(cat, "tech"),
            _ => panic!("Expected text category"),
        }
    }
}

/// Test FTS behavior with explicit transactions
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(
    init_sql = "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, content TEXT)"
)]
fn test_fts_with_explicit_transactions(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create FTS index
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, content)")
        .unwrap();

    // Insert initial data
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Rust Basics', 'Introduction to Rust programming')",
    )
    .unwrap();

    // Verify initial data is indexed
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Rust')",
    );
    assert_eq!(rows.len(), 1);

    // Start explicit transaction
    conn.execute("BEGIN").unwrap();

    // Insert within transaction
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Advanced Rust', 'Rust ownership and lifetimes')",
    )
    .unwrap();
    conn.execute("INSERT INTO articles VALUES (3, 'Python Guide', 'Python for beginners')")
        .unwrap();

    // Commit transaction
    conn.execute("COMMIT").unwrap();

    // Verify all data is now indexed
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Rust')",
    );
    assert_eq!(rows.len(), 2, "Should find 2 Rust articles after commit");

    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Python')",
    );
    assert_eq!(rows.len(), 1, "Should find 1 Python article after commit");

    // Test rollback scenario
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO articles VALUES (4, 'Go Guide', 'Go concurrency patterns')")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Verify rollback worked - Go article should not exist
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Go')",
    );
    assert_eq!(rows.len(), 0, "Should not find Go article after rollback");

    // Verify other data still intact
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Rust')",
    );
    assert_eq!(
        rows.len(),
        2,
        "Rust articles should still be indexed after rollback"
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn test_fts_mvcc_lifecycle(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'committed alpha'), (2, 'committed beta')")
        .unwrap();

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'committed') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE docs SET body = 'ephemeral update' WHERE id = 1")
        .unwrap();
    conn.execute("DELETE FROM docs WHERE id = 2").unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'ephemeral insert')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'ephemeral') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'committed') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert!(limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(body, 'ephemeral')"
    )
    .is_empty());

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE docs SET body = 'durable update' WHERE id = 1")
        .unwrap();
    conn.execute("DELETE FROM docs WHERE id = 2").unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'durable insert')")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'durable') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );

    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'durable') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );

    conn.execute("DROP INDEX docs_fts").unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'durable') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_trigger_writes_survive_repeated_subprogram_runs(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("CREATE TABLE source(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER copy_docs AFTER INSERT ON source BEGIN \
         INSERT INTO docs VALUES(NEW.id, NEW.body); END",
    )
    .unwrap();

    conn.execute("INSERT INTO source VALUES (1, 'first trigger'), (2, 'second trigger')")
        .unwrap();

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'trigger') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_raise_fail_keeps_base_rows_and_index_in_sync(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER fail_second BEFORE INSERT ON docs WHEN NEW.id = 2 BEGIN \
         SELECT RAISE(FAIL, 'stop'); END",
    )
    .unwrap();

    assert!(conn
        .execute("INSERT INTO docs VALUES (1, 'first kept row'), (2, 'second rejected row')")
        .is_err());

    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs ORDER BY id"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'kept') ORDER BY id"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    conn.execute("BEGIN").unwrap();
    assert!(conn
        .execute("INSERT INTO docs VALUES (3, 'transaction kept row'), (2, 'still rejected')")
        .is_err());
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'kept') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'kept') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );

    conn.execute("CREATE TABLE source(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER copy_then_fail AFTER INSERT ON source BEGIN \
         INSERT INTO docs VALUES(NEW.id, NEW.body); \
         SELECT RAISE(FAIL, 'after copy'); END",
    )
    .unwrap();
    assert!(conn
        .execute("INSERT INTO source VALUES (4, 'trigger kept row')")
        .is_err());
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'kept') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
            vec![rusqlite::types::Value::Integer(4)],
        ]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn test_fts_mvcc_connection_isolation(tmp_db: TempDatabase) {
    let writer = tmp_db.connect_limbo();
    let observer = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (1, 'committed token')")
        .unwrap();

    writer.execute("BEGIN").unwrap();
    writer
        .execute("UPDATE docs SET body = 'uncommitted token' WHERE id = 1")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (2, 'uncommitted token')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &writer,
            "SELECT id FROM docs WHERE fts_match(body, 'uncommitted') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert!(limbo_exec_rows(
        &observer,
        "SELECT id FROM docs WHERE fts_match(body, 'uncommitted')"
    )
    .is_empty());
    assert_eq!(
        limbo_exec_rows(
            &observer,
            "SELECT id FROM docs WHERE fts_match(body, 'committed')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    writer.execute("COMMIT").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &observer,
            "SELECT id FROM docs WHERE fts_match(body, 'uncommitted') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_same_index_writer_conflicts_before_tantivy_work() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-same-index-writer-conflict.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO docs VALUES (1, 'writer one')")
        .unwrap();
    let stats_before_conflict = fts_attachment_test_stats(&tmp_db, &first, "docs", "docs_fts");

    let conflict = second
        .execute("INSERT INTO docs VALUES (2, 'writer two')")
        .unwrap_err();
    assert!(matches!(
        conflict,
        turso_core::LimboError::WriteWriteConflict
    ));
    let stats_after_conflict = fts_attachment_test_stats(&tmp_db, &first, "docs", "docs_fts");
    assert_eq!(
        stats_after_conflict.tantivy_writer_constructions,
        stats_before_conflict.tantivy_writer_constructions,
        "the losing transaction must not construct a Tantivy writer"
    );
    assert_eq!(
        stats_after_conflict.write_lease_rejections,
        stats_before_conflict
            .write_lease_rejections
            .map(|count| count + 1)
    );

    first.execute("COMMIT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    second
        .execute("INSERT INTO docs VALUES (2, 'writer two retry')")
        .unwrap();
    second.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(&first, "SELECT id FROM docs ORDER BY id"),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert_eq!(
        limbo_exec_rows(
            &first,
            "SELECT id FROM docs WHERE fts_match(body, 'writer') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_different_index_writers_do_not_conflict() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-different-index-writers.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE first_docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX first_fts ON first_docs USING fts(body)")
        .unwrap();
    first
        .execute("CREATE TABLE second_docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX second_fts ON second_docs USING fts(body)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO first_docs VALUES (1, 'first writer')")
        .unwrap();
    second
        .execute("INSERT INTO second_docs VALUES (2, 'second writer')")
        .unwrap();
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(
            &first,
            "SELECT id FROM first_docs WHERE fts_match(body, 'writer')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &first,
            "SELECT id FROM second_docs WHERE fts_match(body, 'writer')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_opposite_index_order_rejects_without_deadlock() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-opposite-index-order.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE a(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX a_fts ON a USING fts(body)")
        .unwrap();
    first
        .execute("CREATE TABLE b(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX b_fts ON b USING fts(body)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO a VALUES (1, 'rolled back')")
        .unwrap();
    second
        .execute("INSERT INTO b VALUES (2, 'survives')")
        .unwrap();

    let conflict = first
        .execute("INSERT INTO b VALUES (1, 'rolled back')")
        .unwrap_err();
    assert!(matches!(
        conflict,
        turso_core::LimboError::WriteWriteConflict
    ));

    second
        .execute("INSERT INTO a VALUES (2, 'survives')")
        .unwrap();
    second.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(&second, "SELECT id FROM a"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    assert_eq!(
        limbo_exec_rows(&second, "SELECT id FROM b"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &second,
            "SELECT id FROM a WHERE fts_match(body, 'survives')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &second,
            "SELECT id FROM b WHERE fts_match(body, 'survives')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_savepoint_rollback_keeps_transaction_lease() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-savepoint-writer-lease.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    first.execute("SAVEPOINT pending_write").unwrap();
    first
        .execute("INSERT INTO docs VALUES (1, 'rolled back savepoint')")
        .unwrap();
    first.execute("ROLLBACK TO pending_write").unwrap();

    second.execute("BEGIN CONCURRENT").unwrap();
    let conflict = second
        .execute("INSERT INTO docs VALUES (2, 'blocked writer')")
        .unwrap_err();
    assert!(matches!(
        conflict,
        turso_core::LimboError::WriteWriteConflict
    ));

    first.execute("ROLLBACK").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    second
        .execute("INSERT INTO docs VALUES (2, 'successful retry')")
        .unwrap();
    second.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(&second, "SELECT id FROM docs"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &second,
            "SELECT id FROM docs WHERE fts_match(body, 'retry')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_connection_close_releases_writer_lease() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-close-writer-lease.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO docs VALUES (1, 'abandoned writer')")
        .unwrap();
    first.close().unwrap();

    second.execute("BEGIN CONCURRENT").unwrap();
    second
        .execute("INSERT INTO docs VALUES (2, 'replacement writer')")
        .unwrap();
    second.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(&second, "SELECT id FROM docs"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &second,
            "SELECT id FROM docs WHERE fts_match(body, 'replacement')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn test_fts_mvcc_recovery(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'logical recovery'), (2, 'logical recovery')")
        .unwrap();

    let path = tmp_db.path.clone();
    let io = tmp_db.io.clone();
    let opts = tmp_db.db_opts;
    let flags = tmp_db.db_flags;
    conn.close().unwrap();
    drop(conn);
    drop(tmp_db);

    let db = turso_core::Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        flags,
        opts,
        None,
        std::sync::Arc::new(turso_core::SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'recovery') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.close().unwrap();
    drop(conn);
    drop(db);

    let db = turso_core::Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        flags,
        opts,
        None,
        std::sync::Arc::new(turso_core::SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'recovery') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_checkpoint_modes_preserve_manifest() {
    for mvcc in [false, true] {
        let opts = turso_core::DatabaseOpts::new()
            .with_index_method(true)
            .with_experimental_mvcc_passive_checkpoint(true);
        let mut builder = TempDatabase::builder().with_opts(opts);
        if mvcc {
            builder = builder.with_mvcc(true);
        }
        let tmp_db = builder.build();
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();

        let cases = [
            ("PASSIVE", "checkpointalpha"),
            ("FULL", "checkpointbravo"),
            ("RESTART", "checkpointcharlie"),
            ("TRUNCATE", "checkpointdelta"),
        ];
        for (id, (mode, token)) in cases.into_iter().enumerate() {
            let id = id as i64 + 1;
            conn.execute(format!("INSERT INTO docs VALUES ({id}, '{token}')"))
                .unwrap();
            conn.execute(format!("PRAGMA wal_checkpoint({mode})"))
                .unwrap();

            for (visible_id, (_, visible_token)) in cases.iter().take(id as usize).enumerate() {
                let visible_id = visible_id as i64 + 1;
                assert_eq!(
                    limbo_exec_rows(
                        &conn,
                        &format!("SELECT id FROM docs WHERE fts_match(body, '{visible_token}')")
                    ),
                    vec![vec![rusqlite::types::Value::Integer(visible_id)]],
                    "{mode} checkpoint lost FTS generation {visible_id}"
                );
            }
        }
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_passive_checkpoint_preserves_pinned_reader() {
    for mvcc in [false, true] {
        let opts = turso_core::DatabaseOpts::new()
            .with_index_method(true)
            .with_experimental_mvcc_passive_checkpoint(true);
        let mut builder = TempDatabase::builder().with_opts(opts);
        if mvcc {
            builder = builder.with_mvcc(true);
        }
        let tmp_db = builder.build();
        let writer = tmp_db.connect_limbo();
        let reader = tmp_db.connect_limbo();
        writer
            .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        writer
            .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        writer
            .execute("INSERT INTO docs VALUES (1, 'pinned generation')")
            .unwrap();

        reader.execute("BEGIN").unwrap();
        assert_eq!(
            limbo_exec_rows(
                &reader,
                "SELECT id FROM docs WHERE fts_match(body, 'pinned')"
            ),
            vec![vec![rusqlite::types::Value::Integer(1)]]
        );

        writer
            .execute("INSERT INTO docs VALUES (2, 'new generation')")
            .unwrap();
        if let Err(error) = writer.execute("PRAGMA wal_checkpoint(PASSIVE)") {
            assert!(
                matches!(error, turso_core::LimboError::Busy),
                "passive checkpoint returned unexpected error in {} mode: {error}",
                if mvcc { "MVCC" } else { "WAL" }
            );
        }
        assert!(
            limbo_exec_rows(&reader, "SELECT id FROM docs WHERE fts_match(body, 'new')").is_empty(),
            "a checkpoint must not move a pinned reader to the new FTS manifest"
        );
        reader.execute("COMMIT").unwrap();

        assert_eq!(
            limbo_exec_rows(&reader, "SELECT id FROM docs WHERE fts_match(body, 'new')"),
            vec![vec![rusqlite::types::Value::Integer(2)]],
            "the next transaction must observe the post-checkpoint manifest"
        );
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_switch_to_mvcc(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'before switch')")
        .unwrap();

    conn.pragma_update("journal_mode", "'mvcc'").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'switch')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    conn.execute("UPDATE docs SET body = 'updated after transition' WHERE id = 1")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'inserted after transition')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'transition') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'switch')").is_empty()
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'transition') ORDER BY id"
        )
        .len(),
        2
    );

    conn.pragma_update("journal_mode", "'wal'").unwrap();
    conn.execute("UPDATE docs SET body = 'returned to wal' WHERE id = 1")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'created in wal')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'wal') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ],
        "FTS state must survive the MVCC-to-WAL transition"
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(init_sql = "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")]
fn test_fts_optimize_index(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create FTS index
    conn.execute("CREATE INDEX fts_docs ON docs USING fts (title, body)")
        .unwrap();

    // Insert multiple batches of documents to create multiple segments
    for i in 0..10 {
        conn.execute(format!(
            "INSERT INTO docs VALUES ({i}, 'Document {i}', 'Content about topic {i} with keywords')",
        ))
        .unwrap();
    }

    // Verify documents are searchable
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Document')",
    );
    assert_eq!(rows.len(), 10, "Should find all 10 documents");

    // Open an independent cursor so the catalog is reconstructed from the
    // physical backing B-tree rather than inherited from an attachment cache.
    #[cfg(feature = "test_helper")]
    let stats_before_optimize = fts_test_stats(
        &tmp_db,
        &conn,
        "docs",
        "fts_docs",
        &[("title", 1), ("body", 2)],
    );
    #[cfg(feature = "test_helper")]
    assert_eq!(
        stats_before_optimize.segment_count,
        Some(3),
        "base-8 maintenance should compact 10 single-row commits to 3 segments"
    );

    // Run OPTIMIZE INDEX on specific index
    conn.execute("OPTIMIZE INDEX fts_docs").unwrap();
    #[cfg(feature = "test_helper")]
    let stats_after_optimize = fts_test_stats(
        &tmp_db,
        &conn,
        "docs",
        "fts_docs",
        &[("title", 1), ("body", 2)],
    );
    #[cfg(feature = "test_helper")]
    assert_eq!(stats_after_optimize.segment_count, Some(1));
    #[cfg(feature = "test_helper")]
    assert!(
        stats_after_optimize.storage_file_count < stats_before_optimize.storage_file_count,
        "optimize must physically remove obsolete segment files: \
         before={}, after={}",
        stats_before_optimize.storage_file_count,
        stats_after_optimize.storage_file_count
    );

    // Verify documents are still searchable after optimize
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE (title, body) MATCH 'Document'",
    );
    assert_eq!(
        rows.len(),
        10,
        "Should still find all 10 documents after optimize"
    );

    // Verify content is correct
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE (title, body) MATCH 'topic'",
    );
    assert_eq!(rows.len(), 10, "Should find all documents with 'topic'");
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(init_sql = "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT)")]
fn test_fts_optimize_all_indexes(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create second table manually
    conn.execute("CREATE TABLE posts(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();

    // Create FTS indexes on multiple tables
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title)")
        .unwrap();
    conn.execute("CREATE INDEX fts_posts ON posts USING fts (content)")
        .unwrap();

    // Insert data
    conn.execute("INSERT INTO articles VALUES (1, 'Rust Programming')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'Python Guide')")
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (1, 'Learning Rust is fun')")
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (2, 'Advanced Rust patterns')")
        .unwrap();

    // Run OPTIMIZE INDEX without specifying index name (optimizes all)
    conn.execute("OPTIMIZE INDEX").unwrap();

    // Verify all indexes still work
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, 'Rust')",
    );
    assert_eq!(rows.len(), 1, "Should find Rust article");

    let rows = limbo_exec_rows(&conn, "SELECT id FROM posts WHERE content MATCH 'Rust'");
    assert_eq!(rows.len(), 2, "Should find both Rust posts");
}

/// Test that FTS functions work with column arguments in any order.
/// The index is created with columns (title, body), but queries should work
/// with fts_match(body, title, ...) as well as fts_match(title, body, ...).
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_column_order_agnostic(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table and FTS index with columns in order (title, body)
    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert test data - use 'database' in both articles 1 and 3
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Database Design', 'Learn about database systems')",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Web Development', 'Building modern web applications')",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (3, 'SQL Basics', 'Introduction to database and SQL')",
    )
    .unwrap();

    // Test standard column order: (title, body)
    let rows_standard = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE (title, body) MATCH 'database'",
    );
    assert_eq!(
        rows_standard.len(),
        2,
        "Standard order should find 2 matches (articles 1 and 3)"
    );
    let ids_standard: Vec<i64> = rows_standard
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids_standard.contains(&1));
    assert!(ids_standard.contains(&3));

    // Test reversed column order: (body, title)
    // This should work with column-order-agnostic matching
    let rows_reversed = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE (body, title) MATCH 'database'",
    );
    assert_eq!(
        rows_reversed.len(),
        2,
        "Reversed column order should find same 2 matches"
    );
    let ids_reversed: Vec<i64> = rows_reversed
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids_reversed.contains(&1));
    assert!(ids_reversed.contains(&3));

    // Test fts_score with reversed column order
    let rows_score_reversed = limbo_exec_rows(
        &conn,
        "SELECT id, fts_score(body, title, 'database') as score FROM articles WHERE (body, title) MATCH 'database' ORDER BY score DESC",
    );
    assert_eq!(
        rows_score_reversed.len(),
        2,
        "fts_score with reversed columns should work"
    );

    // Verify both orderings return the same results
    assert_eq!(
        ids_standard.len(),
        ids_reversed.len(),
        "Both column orderings should return same number of results"
    );
    for id in &ids_standard {
        assert!(
            ids_reversed.contains(id),
            "Both orderings should return same IDs"
        );
    }
}

/// Test that FTS works with JOINS
/// This tests the removal of the single-table restriction for custom index methods.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_with_join(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create tables
    conn.execute(
        "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT, author_id INTEGER)",
    )
    .unwrap();
    conn.execute("CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    // Create FTS index on articles
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert authors
    conn.execute("INSERT INTO authors VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO authors VALUES (2, 'Bob')")
        .unwrap();
    conn.execute("INSERT INTO authors VALUES (3, 'Charlie')")
        .unwrap();

    // Insert articles with author references - use 'database' consistently
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Database Design', 'Learn about database systems', 1)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Web Development', 'Building modern web applications', 2)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (3, 'SQL Basics', 'Introduction to database and SQL', 1)",
    )
    .unwrap();
    conn.execute("INSERT INTO articles VALUES (4, 'API Design', 'RESTful API best practices', 3)")
        .unwrap();

    // Test FTS with JOIN - find articles about 'database' with author names
    let rows = limbo_exec_rows(
        &conn,
        "SELECT a.id, a.title, u.name FROM articles a JOIN authors u ON a.author_id = u.id WHERE (a.title, a.body) MATCH 'database'",
    );
    assert_eq!(
        rows.len(),
        2,
        "Should find 2 articles about database (articles 1 and 3)"
    );

    // Verify the results contain expected data
    let result_ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(result_ids.contains(&1), "Should include article 1");
    assert!(result_ids.contains(&3), "Should include article 3");

    // Verify author names are correctly joined
    let author_names: Vec<String> = rows
        .iter()
        .filter_map(|r| match &r[2] {
            rusqlite::types::Value::Text(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    // Both articles 1 and 3 are by Alice
    assert_eq!(
        author_names.iter().filter(|&n| n == "Alice").count(),
        2,
        "Both matching articles should be by Alice"
    );

    // Test FTS with JOIN and additional WHERE conditions
    let rows = limbo_exec_rows(
        &conn,
        "SELECT a.id, a.title, u.name FROM articles a JOIN authors u ON a.author_id = u.id WHERE (a.title, a.body) MATCH 'web' AND u.name = 'Bob'",
    );
    assert_eq!(rows.len(), 1, "Should find 1 article about web by Bob");
    let id = match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => *i,
        _ => panic!("Expected integer id"),
    };
    assert_eq!(id, 2, "Should be article 2 (Web Development by Bob)");
}

/// Test FTS with LEFT JOIN to ensure outer joins work correctly with FTS.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_with_left_join(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create tables
    conn.execute(
        "CREATE TABLE posts(id INTEGER PRIMARY KEY, title TEXT, content TEXT, category_id INTEGER)",
    )
    .unwrap();
    conn.execute("CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    // Create FTS index
    conn.execute("CREATE INDEX fts_posts ON posts USING fts (title, content)")
        .unwrap();

    // Insert categories
    conn.execute("INSERT INTO categories VALUES (1, 'Technology')")
        .unwrap();
    conn.execute("INSERT INTO categories VALUES (2, 'Science')")
        .unwrap();

    // Insert posts - some with category, some without (NULL category_id)
    conn.execute(
        "INSERT INTO posts VALUES (1, 'Rust Programming', 'Systems programming with Rust', 1)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO posts VALUES (2, 'Python Basics', 'Introduction to Python programming', 1)",
    )
    .unwrap();
    conn.execute("INSERT INTO posts VALUES (3, 'Rust in Nature', 'How rust affects metal', 2)")
        .unwrap();
    conn.execute(
        "INSERT INTO posts VALUES (4, 'Uncategorized Rust', 'A post about Rust without category', NULL)",
    )
    .unwrap();

    // Test FTS with LEFT JOIN - should include post without category
    let rows = limbo_exec_rows(
        &conn,
        "SELECT p.id, p.title, c.name FROM posts p LEFT JOIN categories c ON p.category_id = c.id WHERE fts_match(p.title, p.content, 'Rust')",
    );
    assert_eq!(rows.len(), 3, "Should find 3 posts about Rust");

    // Verify we got the right posts
    let result_ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(result_ids.contains(&1), "Should include post 1");
    assert!(result_ids.contains(&3), "Should include post 3");
    assert!(
        result_ids.contains(&4),
        "Should include post 4 (uncategorized)"
    );

    // Verify NULL category is preserved in LEFT JOIN
    let null_category_count = rows
        .iter()
        .filter(|r| matches!(&r[2], rusqlite::types::Value::Null))
        .count();
    assert_eq!(null_category_count, 1, "One post should have NULL category");
}

/// Test that FTS participates in join order optimization.
/// Uses EXPLAIN QUERY PLAN to verify the actual join order and that FTS is used.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_join_order_optimization(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create a small authors table and a larger articles table
    conn.execute("CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT, author_id INTEGER)",
    )
    .unwrap();

    // Create FTS index on articles
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert a few authors (small table)
    for i in 1..=5 {
        conn.execute(format!("INSERT INTO authors VALUES ({i}, 'Author{i}')"))
            .unwrap();
    }
    // so we use real statistics
    conn.execute("ANALYZE").unwrap();

    // Insert many articles (larger table) - more than authors to show cardinality difference
    for i in 1..=50 {
        let author_id = (i % 5) + 1;
        let (title, body) = if i % 10 == 0 {
            // Every 10th article is about database
            (
                format!("Database Article {i}"),
                "Content about database systems and SQL".to_string(),
            )
        } else {
            (
                format!("General Article {i}"),
                "General content about various topics".to_string(),
            )
        };
        conn.execute(format!(
            "INSERT INTO articles VALUES ({i}, '{title}', '{body}', {author_id})"
        ))
        .unwrap();
    }

    // Check the query plan using EXPLAIN QUERY PLAN
    let query = "SELECT a.id, a.title, u.name FROM articles a JOIN authors u ON a.author_id = u.id WHERE fts_match(a.title, a.body, 'database')";
    let eqp_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));

    // Extract table access order and check for FTS usage
    let mut table_order = Vec::new();
    let mut has_fts_search = false;
    for row in &eqp_rows {
        if let rusqlite::types::Value::Text(detail) = &row[3] {
            // Check for FTS index method query (format: "QUERY INDEX METHOD fts")
            if detail.contains("INDEX METHOD") || detail.contains("fts_articles") {
                has_fts_search = true;
            }
            // Extract table name from SCAN or SEARCH lines
            if let Some(rest) = detail.strip_prefix("SCAN ") {
                let table = rest.split_whitespace().next().unwrap();
                table_order.push(table.to_string());
            } else if let Some(rest) = detail.strip_prefix("SEARCH ") {
                let table = rest.split_whitespace().next().unwrap();
                table_order.push(table.to_string());
            } else if detail.starts_with("QUERY INDEX METHOD") {
                // FTS queries show up as "QUERY INDEX METHOD fts"
                table_order.push("articles".to_string());
            }
        }
    }

    // Verify that the optimizer is using the FTS index
    assert!(
        has_fts_search,
        "Expected FTS index to be used in query plan. Plan details: {:?}",
        eqp_rows
            .iter()
            .filter_map(|r| r.get(3).and_then(|v| match v {
                rusqlite::types::Value::Text(t) => Some(t.as_str()),
                _ => None,
            }))
            .collect::<Vec<_>>()
    );

    // Verify the join order: FTS should be first, authors second
    assert_eq!(
        table_order.len(),
        2,
        "Expected 2 tables in join order, got: {table_order:?}"
    );
    assert_eq!(
        table_order[0], "articles",
        "Expected articles (FTS) to be first in join order, got: {table_order:?}"
    );
    assert!(
        table_order[1] == "u" || table_order[1] == "authors",
        "Expected authors to be second in join order, got: {table_order:?}"
    );

    // Execute the query and verify results
    let rows = limbo_exec_rows(&conn, query);

    // Should find 5 articles about database
    assert_eq!(rows.len(), 5, "Should find 5 articles about database");

    // Verify all results have valid author names
    for row in &rows {
        let author_name = match &row[2] {
            rusqlite::types::Value::Text(t) => t.clone(),
            _ => panic!("Expected text for author name"),
        };
        assert!(
            author_name.starts_with("Author"),
            "Author name should start with 'Author'"
        );
    }

    // Test with reversed table order in SQL, optimizer should still use FTS
    let query2 = "SELECT a.id, a.title, u.name FROM authors u JOIN articles a ON u.id = a.author_id WHERE fts_match(a.title, a.body, 'database')";
    let eqp_rows2 = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query2}"));

    let mut has_fts_search2 = false;
    for row in &eqp_rows2 {
        if let rusqlite::types::Value::Text(detail) = &row[3] {
            if detail.contains("INDEX METHOD") || detail.contains("fts_articles") {
                has_fts_search2 = true;
            }
        }
    }
    assert!(
        has_fts_search2,
        "Expected FTS index to be used with reversed table order. Plan details: {:?}",
        eqp_rows2
            .iter()
            .filter_map(|r| r.get(3).and_then(|v| match v {
                rusqlite::types::Value::Text(t) => Some(t.as_str()),
                _ => None,
            }))
            .collect::<Vec<_>>()
    );

    let rows2 = limbo_exec_rows(&conn, query2);
    assert_eq!(
        rows2.len(),
        5,
        "Should find same 5 articles with reversed table order"
    );
}

/// Test FTS with multiple joins to verify cost-based optimization works
/// with more complex join patterns.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_multi_table_join(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create three tables: categories, authors, articles
    conn.execute("CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT, author_id INTEGER, category_id INTEGER)",
    )
    .unwrap();

    // Create FTS index on articles
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert categories
    conn.execute("INSERT INTO categories VALUES (1, 'Technology')")
        .unwrap();
    conn.execute("INSERT INTO categories VALUES (2, 'Science')")
        .unwrap();
    conn.execute("INSERT INTO categories VALUES (3, 'Arts')")
        .unwrap();

    // Insert authors
    conn.execute("INSERT INTO authors VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO authors VALUES (2, 'Bob')")
        .unwrap();

    // Insert articles
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Database Systems', 'Introduction to database management', 1, 1)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Machine Learning', 'AI and neural networks', 2, 2)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (3, 'SQL Performance', 'Optimizing database queries', 1, 1)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (4, 'Modern Art', 'Contemporary art movements', 2, 3)",
    )
    .unwrap();

    // Test three-way join with FTS
    let rows = limbo_exec_rows(
        &conn,
        "SELECT a.title, u.name, c.name FROM articles a \
         JOIN authors u ON a.author_id = u.id \
         JOIN categories c ON a.category_id = c.id \
         WHERE (a.title, a.body) MATCH 'database'",
    );

    // Should find 2 articles about database (articles 1 and 3)
    assert_eq!(rows.len(), 2, "Should find 2 articles about database");

    // Verify we got the right combination
    let titles: Vec<String> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Text(t) => Some(t.clone()),
            _ => None,
        })
        .collect();
    assert!(titles.contains(&"Database Systems".to_string()));
    assert!(titles.contains(&"SQL Performance".to_string()));
}

/// Regression test for issue 7522: a rolled-back transaction containing FTS
/// writes and OPTIMIZE INDEX must not leave the shared directory cache
/// pointing at segment files whose BTree rows were rolled back. Before the
/// fix, the next write against the index failed with
/// `FileDoesNotExist("<uuid>.term")`.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_rolled_back_optimize_does_not_leak_segment_state() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, f TEXT, b BLOB)")
        .unwrap();
    conn.execute("CREATE INDEX idx ON t USING fts(f)").unwrap();
    conn.execute(
        "INSERT INTO t(id,x,f,b) VALUES (270323, 'x', 'optimize', X'01'), (-596572, NULL, 'foo', X'02')",
    )
    .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET b=X'D9', f='rust token full search text search rollback'")
        .unwrap();
    conn.execute("OPTIMIZE INDEX idx").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Writes after the rollback must see the pre-transaction index state.
    conn.execute("INSERT INTO t(id) VALUES (32378), (NULL), (524997)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE x").unwrap();

    // The rolled-back UPDATE must not be searchable; the surviving row is.
    let hits = limbo_exec_rows(&conn, "SELECT id FROM t WHERE f MATCH 'foo'");
    assert_eq!(
        hits.len(),
        1,
        "pre-transaction document must remain searchable"
    );
    let rolled_back = limbo_exec_rows(&conn, "SELECT id FROM t WHERE f MATCH 'rollback'");
    assert!(
        rolled_back.is_empty(),
        "rolled-back document must not be searchable"
    );
}

/// Automatic segment maintenance runs inside the caller's transaction. A
/// rollback must restore both the pre-merge metadata and every old segment
/// file, and the next write must be able to merge those restored segments.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_rolled_back_automatic_merge_restores_segments() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..7 {
        conn.execute(format!(
            "INSERT INTO docs VALUES ({id}, 'committed common document {id}')"
        ))
        .unwrap();
    }
    assert_eq!(
        fts_test_stats(&tmp_db, &conn, "docs", "docs_fts", &[("body", 1)]).segment_count,
        Some(7)
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (7, 'ephemeralrollbacktoken common document')")
        .unwrap();
    assert_eq!(
        fts_test_stats(&tmp_db, &conn, "docs", "docs_fts", &[("body", 1)]).segment_count,
        Some(1),
        "the eighth segment should trigger maintenance inside the transaction"
    );
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(
        fts_test_stats(&tmp_db, &conn, "docs", "docs_fts", &[("body", 1)]).segment_count,
        Some(7),
        "rollback must restore the seven pre-merge segments"
    );
    assert!(limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(body, 'ephemeralrollbacktoken')"
    )
    .is_empty());

    conn.execute("INSERT INTO docs VALUES (8, 'surviving common document')")
        .unwrap();
    assert_eq!(
        fts_test_stats(&tmp_db, &conn, "docs", "docs_fts", &[("body", 1)]).segment_count,
        Some(1),
        "the next committed write should merge the restored segments"
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'common')").len(),
        8
    );
}

/// Sequential INSERT statements should retain the committed Tantivy writer
/// while the backing B-tree metadata remains unchanged.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_reuses_committed_writer_across_insert_statements() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.execute("INSERT INTO docs VALUES (1, 'first retained writer document')")
        .unwrap();
    assert_eq!(
        fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts").cached_writer,
        Some(true)
    );

    conn.execute("INSERT INTO docs VALUES (2, 'second retained writer document')")
        .unwrap();
    assert_eq!(
        fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts").cached_writer,
        Some(true)
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'retained')"
        )
        .len(),
        2
    );
    let before_drop = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");

    // Destroying an index must release the retained Tantivy writer and its
    // directory lock so an index with the same name can be created immediately.
    conn.execute("DROP INDEX docs_fts").unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'retained')"
        )
        .len(),
        2
    );
    let after_recreate = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_ne!(
        after_recreate.index_incarnation, before_drop.index_incarnation,
        "drop/recreate must allocate a distinct persistent index incarnation"
    );
}

/// A cursor prepared by a statement inside BEGIN must survive statement reset
/// until the later COMMIT delivers the transaction outcome. The committed
/// writer should then be reusable by the next transaction.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_explicit_commit_publishes_transaction_scoped_writer() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'explicit transaction writer')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'newest transaction cursor')")
        .unwrap();
    assert_eq!(
        fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts").cached_writer,
        Some(true),
        "statement success must retain transaction-private writer state"
    );
    conn.execute("COMMIT").unwrap();

    let committed = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(committed.cached_writer, Some(true));
    let constructions = committed.tantivy_writer_constructions;

    conn.execute("INSERT INTO docs VALUES (3, 'reused explicit writer')")
        .unwrap();
    assert_eq!(
        fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts").tantivy_writer_constructions,
        constructions,
        "the post-COMMIT statement should restore the transaction-published writer"
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'transaction') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)]
        ]
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_reuses_writer_within_explicit_transaction() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    let before = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("INSERT INTO docs VALUES (1, 'first retained writer')")
        .unwrap();
    let after_first = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(after_first.cached_writer, Some(true));
    assert!(
        after_first.tantivy_writer_constructions > before.tantivy_writer_constructions,
        "the first write must construct one Tantivy writer"
    );

    conn.execute("INSERT INTO docs VALUES (2, 'second retained writer')")
        .unwrap();
    let after_second = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(
        after_second.tantivy_writer_constructions, after_first.tantivy_writer_constructions,
        "the next statement in the same MVCC transaction must reuse the lease-owned writer"
    );
    assert!(
        after_second.writer_cache_hits > after_first.writer_cache_hits,
        "writer-cache telemetry must record the transaction-private reuse"
    );
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'retained') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)]
        ]
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_reuses_validated_writer_across_transactions() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'first transaction')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'first')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after_first = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    conn.execute("INSERT INTO docs VALUES (2, 'second transaction')")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'second')"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    let after_second = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        after_second.tantivy_writer_constructions, after_first.tantivy_writer_constructions,
        "an unchanged committed manifest must reuse the asynchronously validated MVCC writer"
    );
    assert!(
        after_second.writer_cache_hits > after_first.writer_cache_hits,
        "writer-cache telemetry must record cross-transaction validation reuse"
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'transaction') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)]
        ]
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_savepoint_rollback_invalidates_transaction_scoped_writer() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT before_fts").unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'rolled back writer state')")
        .unwrap();
    conn.execute("ROLLBACK TO before_fts").unwrap();
    conn.execute("COMMIT").unwrap();

    let after_rollback = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(
        after_rollback.cached_writer,
        Some(false),
        "ROLLBACK TO must invalidate the retained transaction cursor before COMMIT"
    );
    assert!(
        after_rollback.writer_cache_rollback_discards > Some(0),
        "rollback telemetry must record the discarded transaction-private writer"
    );
    assert!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'rolled')").is_empty()
    );

    conn.execute("INSERT INTO docs VALUES (2, 'surviving writer state')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'surviving')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_loser_rollback_keeps_winner_cached_writer() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let winner = tmp_db.connect_limbo();
    let loser = tmp_db.connect_limbo();

    winner
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    winner
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    winner.execute("BEGIN CONCURRENT").unwrap();
    loser.execute("BEGIN CONCURRENT").unwrap();
    winner
        .execute("INSERT INTO docs VALUES (1, 'winner writer')")
        .unwrap();
    let before_conflict = fts_attachment_test_stats(&tmp_db, &winner, "docs", "docs_fts");
    assert_eq!(
        before_conflict.cached_writer,
        Some(true),
        "the winner's statement must retain its transaction-tagged writer"
    );

    let conflict = loser
        .execute("INSERT INTO docs VALUES (2, 'loser writer')")
        .unwrap_err();
    assert!(matches!(
        conflict,
        turso_core::LimboError::WriteWriteConflict
    ));

    let after_conflict = fts_attachment_test_stats(&tmp_db, &winner, "docs", "docs_fts");
    assert_eq!(
        after_conflict.cached_writer,
        Some(true),
        "the loser's rollback must not evict the winner's cached writer"
    );
    assert_eq!(
        after_conflict.writer_cache_rollback_discards,
        before_conflict.writer_cache_rollback_discards,
        "the loser's rollback must not count a discard of a writer it does not own"
    );

    winner
        .execute("INSERT INTO docs VALUES (3, 'winner reuses writer')")
        .unwrap();
    let after_reuse = fts_attachment_test_stats(&tmp_db, &winner, "docs", "docs_fts");
    assert_eq!(
        after_reuse.tantivy_writer_constructions, after_conflict.tantivy_writer_constructions,
        "the winner's next statement must reuse its writer instead of rebuilding it"
    );
    assert!(
        after_reuse.writer_cache_hits > after_conflict.writer_cache_hits,
        "writer-cache telemetry must record the winner's reuse after the conflict"
    );
    winner.execute("COMMIT").unwrap();

    // The conflict rolled back the loser's whole transaction; a retry from a
    // fresh transaction succeeds.
    loser.execute("BEGIN CONCURRENT").unwrap();
    loser
        .execute("INSERT INTO docs VALUES (2, 'loser retry writer')")
        .unwrap();
    loser.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(
            &winner,
            "SELECT id FROM docs WHERE fts_match(body, 'writer') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_wal_other_connection_commit_does_not_revalidate_writer() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let writer_conn = tmp_db.connect_limbo();
    let other = tmp_db.connect_limbo();

    writer_conn
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer_conn
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    writer_conn
        .execute("CREATE TABLE plain(x INTEGER)")
        .unwrap();
    writer_conn
        .execute("INSERT INTO docs VALUES (1, 'alpha document')")
        .unwrap();

    let before = fts_attachment_test_stats(&tmp_db, &writer_conn, "docs", "docs_fts");
    assert_eq!(before.cached_writer, Some(true));

    // Another connection's transaction reads the FTS index and commits an
    // unrelated write. Its commit hook must not re-stamp the first
    // connection's cached writer to the post-commit WAL position: that would
    // revalidate a writer whose WAL snapshot has moved.
    other.execute("BEGIN").unwrap();
    other.execute("INSERT INTO plain VALUES (1)").unwrap();
    assert_eq!(
        limbo_exec_rows(&other, "SELECT id FROM docs WHERE fts_match(body, 'alpha')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    other.execute("COMMIT").unwrap();

    writer_conn
        .execute("INSERT INTO docs VALUES (2, 'beta document')")
        .unwrap();
    let after = fts_attachment_test_stats(&tmp_db, &writer_conn, "docs", "docs_fts");
    assert!(
        after.tantivy_writer_constructions > before.tantivy_writer_constructions,
        "a WAL-position change committed by another connection must invalidate the \
         cached writer, not revalidate it"
    );
    assert_eq!(
        limbo_exec_rows(
            &writer_conn,
            "SELECT id FROM docs WHERE fts_match(body, 'document') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_wal_commit_must_not_revalidate_stale_budget_rejected_writer() {
    use turso_core::index_method::fts::set_fts_retained_cache_bytes_for_test;

    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn_a = tmp_db.connect_limbo();
    let conn_b = tmp_db.connect_limbo();

    conn_a
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn_a
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn_a
        .execute("INSERT INTO docs VALUES (1, 'alpha stays visible')")
        .unwrap();
    // Connection A's committed writer sits in the shared slot, stamped at A's
    // post-commit WAL position.
    assert_eq!(
        fts_attachment_test_stats(&tmp_db, &conn_a, "docs", "docs_fts").cached_writer,
        Some(true)
    );

    // Shrink the retention budget: B's newer writer now fails cache admission,
    // so A's stale writer stays in the slot while the index moves past it.
    set_fts_retained_cache_bytes_for_test(Some(1));
    conn_b
        .execute("INSERT INTO docs VALUES (2, 'bravo must survive')")
        .unwrap();

    // A read-only FTS statement on connection A commits with A's WAL mark
    // advanced past B's commit. Its commit hook must not re-stamp the stale
    // writer to that mark: the writer's segments predate B's document, and
    // reusing it would drop the document from the index.
    assert_eq!(
        limbo_exec_rows(
            &conn_a,
            "SELECT id FROM docs WHERE fts_match(body, 'bravo')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );

    let before = fts_attachment_test_stats(&tmp_db, &conn_a, "docs", "docs_fts");
    conn_a
        .execute("INSERT INTO docs VALUES (3, 'charlie added later')")
        .unwrap();
    let after = fts_attachment_test_stats(&tmp_db, &conn_a, "docs", "docs_fts");
    set_fts_retained_cache_bytes_for_test(None);

    assert!(
        after.tantivy_writer_constructions > before.tantivy_writer_constructions,
        "connection A must rebuild its writer: the cached one predates B's committed document"
    );
    assert_eq!(
        limbo_exec_rows(
            &conn_a,
            "SELECT id FROM docs WHERE fts_match(body, 'bravo') OR fts_match(body, 'charlie') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(2)],
            vec![rusqlite::types::Value::Integer(3)],
        ],
        "every committed document must stay searchable after the writer churn"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_create_persists_real_index_incarnation() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    // A never-written index uses the deterministic placeholder incarnation 0;
    // staging the first control record (which CREATE INDEX does) must mint a
    // real incarnation so cache validation can distinguish incarnations.
    let stats = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_ne!(
        stats.index_incarnation,
        Some(0),
        "the persisted control record must never carry the empty-index placeholder incarnation"
    );
    assert_eq!(stats.manifest_generation, Some(1));
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_manifest_generation_is_transactional() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    let created = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(created.storage_format_version, Some(1));
    assert_eq!(created.manifest_generation, Some(1));
    assert_eq!(
        created.storage_file_count,
        created.manifest_file_count.unwrap()
    );

    conn.execute("INSERT INTO docs VALUES (1, 'committed generation')")
        .unwrap();
    let committed = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert!(
        committed.manifest_generation > created.manifest_generation,
        "committed write must advance the manifest"
    );
    assert_eq!(committed.index_incarnation, created.index_incarnation);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'rolled back generation')")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();
    let rolled_back = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(
        rolled_back.manifest_generation, committed.manifest_generation,
        "rollback must restore the prior control record"
    );
    assert_eq!(rolled_back.index_incarnation, created.index_incarnation);
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_reuses_snapshot_within_one_read_transaction() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'same snapshot cache')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'snapshot')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after_first = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'snapshot')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after_second = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        after_second.full_snapshot_loads, after_first.full_snapshot_loads,
        "the second read in one MVCC transaction must not rescan the directory"
    );
    assert!(
        after_second.read_cache_hits > after_first.read_cache_hits,
        "the second read must use the transaction-bound snapshot cache"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_wal_reuses_manifest_after_unrelated_commit() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE unrelated(value TEXT)").unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'stable manifest')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'stable')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let before = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");

    conn.execute("INSERT INTO unrelated VALUES ('changes the WAL position')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'stable')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");

    assert_eq!(
        after.full_snapshot_loads, before.full_snapshot_loads,
        "an unrelated commit must validate the FTS manifest without reloading its files"
    );
    assert!(
        after.manifest_validation_hits > before.manifest_validation_hits,
        "the changed WAL snapshot must be accepted through control-record validation"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_reuses_manifest_across_read_transactions() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'autocommit manifest')")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'autocommit')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let before = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'autocommit')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        after.full_snapshot_loads, before.full_snapshot_loads,
        "a new MVCC read transaction must not reload an unchanged FTS manifest"
    );
    assert!(
        after.manifest_validation_hits > before.manifest_validation_hits,
        "the new MVCC snapshot must validate the cached manifest by its control record"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_manifest_generation_invalidates_stale_snapshot_once() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let writer = tmp_db.connect_limbo();
    let reader = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (1, 'first generation')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'first')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let before_write = fts_attachment_test_stats(&tmp_db, &reader, "docs", "docs_fts");

    writer
        .execute("INSERT INTO docs VALUES (2, 'second generation')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'second')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]],
        "the observer must reject its stale snapshot after the writer commits"
    );
    let after_write = fts_attachment_test_stats(&tmp_db, &reader, "docs", "docs_fts");
    assert!(
        after_write.manifest_validation_misses > before_write.manifest_validation_misses,
        "an advanced manifest generation must reject the stale read snapshot"
    );
    assert!(
        after_write.full_snapshot_loads > before_write.full_snapshot_loads,
        "the first reader of a new generation must load its directory snapshot"
    );

    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'second')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    let after_reuse = fts_attachment_test_stats(&tmp_db, &reader, "docs", "docs_fts");
    assert_eq!(
        after_reuse.full_snapshot_loads, after_write.full_snapshot_loads,
        "the newly loaded generation must be reusable without another full scan"
    );
}

/// FTS read state belongs to the connection snapshot that populated it.
/// Sharing that state with another connection must not expose uncommitted index
/// maintenance from an active writer transaction.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_uncommitted_changes_are_connection_isolated() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let writer = tmp_db.connect_limbo();
    let observer = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(content)")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (10, 'charlie'), (13, 'charlie'), (20, 'unrelated')")
        .unwrap();

    let query = "SELECT id FROM docs WHERE fts_match(content, 'charlie') ORDER BY id";
    assert_eq!(
        limbo_exec_rows(&observer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
        ]
    );
    assert_eq!(
        limbo_exec_rows(&writer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
        ],
        "writer should warm its own cached read state before starting the transaction"
    );

    writer.execute("BEGIN").unwrap();
    writer
        .execute("UPDATE docs SET content = NULL WHERE id = 10")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (14, 'charlie')")
        .unwrap();

    assert_eq!(
        limbo_exec_rows(&writer, query),
        vec![
            vec![rusqlite::types::Value::Integer(13)],
            vec![rusqlite::types::Value::Integer(14)],
        ]
    );
    assert_eq!(
        limbo_exec_rows(&observer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
        ],
        "observer must retain its committed FTS snapshot"
    );

    writer.execute("ROLLBACK").unwrap();
    assert_eq!(
        limbo_exec_rows(&writer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
        ]
    );

    writer
        .execute("INSERT INTO docs VALUES (14, 'charlie')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&observer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
            vec![rusqlite::types::Value::Integer(14)],
        ],
        "observer must discard its cached FTS state when the WAL snapshot advances"
    );
}

/// Alternating readers must retain independent snapshot caches instead of
/// repeatedly replacing one global cache entry. The cache is bounded so a
/// large connection pool cannot retain unbounded Tantivy state.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_read_cache_is_connection_local_and_bounded() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let setup = tmp_db.connect_limbo();
    setup
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    setup
        .execute("CREATE INDEX docs_fts ON docs USING fts(content)")
        .unwrap();
    setup
        .execute("INSERT INTO docs VALUES (1, 'database document')")
        .unwrap();

    let attachment = FtsIndexMethod
        .attach(&IndexMethodConfiguration {
            table_name: "docs".to_string(),
            index_name: "docs_fts".to_string(),
            columns: vec![IndexColumn::new("content", 1)],
            parameters: HashMap::default(),
        })
        .unwrap();
    let readers = (0..5).map(|_| tmp_db.connect_limbo()).collect::<Vec<_>>();

    for (reader_index, expected_cached) in [(0, 1), (1, 2), (0, 2), (1, 2), (2, 3), (3, 4), (4, 4)]
    {
        let mut cursor = attachment.init().unwrap();
        run(&tmp_db, || {
            cursor.open_read(&index_method_context(
                &readers[reader_index],
                attachment.as_ref(),
            ))
        })
        .unwrap();
        let stats = cursor.test_stats().unwrap().unwrap();
        assert_eq!(
            stats.cached_connection_count,
            Some(expected_cached),
            "unexpected cache size after reader {reader_index}"
        );
        assert!(
            stats.cached_bytes.unwrap() <= 192 * 1024 * 1024,
            "retained connection caches exceeded the aggregate file-cache budget"
        );
    }
}

/// Unordered MATCH cursors stream from Tantivy. UPDATE and DELETE must first
/// collect their rowids so index maintenance cannot perturb the active scorer.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_streaming_dml_collects_stable_rowids() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(content)")
        .unwrap();

    let values = (0..32)
        .map(|id| format!("({id}, 'common document {id}')"))
        .collect::<Vec<_>>()
        .join(",");
    conn.execute(format!("INSERT INTO docs VALUES {values}"))
        .unwrap();

    conn.execute(
        "UPDATE docs SET content = 'updated document' \
         WHERE fts_match(content, 'common')",
    )
    .unwrap();
    assert!(limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(content, 'common')"
    )
    .is_empty());
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(content, 'updated')"
        )
        .len(),
        32
    );

    conn.execute("DELETE FROM docs WHERE fts_match(content, 'updated')")
        .unwrap();
    assert!(limbo_exec_rows(&conn, "SELECT id FROM docs").is_empty());
    assert!(limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(content, 'updated')"
    )
    .is_empty());
}
