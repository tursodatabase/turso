use std::collections::HashMap;

use core_tester::common::rng_from_time_or_env;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use turso_core::index_method::fts::FtsIndexMethod;
use turso_core::{
    index_method::{
        toy_vector_sparse_ivf::VectorSparseInvertedIndexMethod, IndexMethod,
        IndexMethodConfiguration,
    },
    schema::IndexColumn,
    types::IOResult,
    vector::{self, vector_types::VectorType},
    Numeric, Register, Result, Value,
};
use turso_parser::ast::SortOrder;

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

fn sparse_vector(v: &str) -> Value {
    let vector = vector::operations::text::vector_from_text(VectorType::Float32Sparse, v).unwrap();
    vector::operations::serialize::vector_serialize(vector)
}

// TODO: cannot use MVCC as we use indexes here
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
            columns: vec![IndexColumn {
                name: "embedding".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 1,
                collation: None,
                default: None,
                expr: None,
            }],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.create(&conn)).unwrap();
    }
    conn.wal_insert_end(true).unwrap();
    assert_eq!(
        schema_rows(),
        vec!["t", "t_idx_inverted_index", "t_idx_stats"]
    );

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.destroy(&conn)).unwrap();
    }
    conn.wal_insert_end(true).unwrap();
    assert_eq!(schema_rows(), vec!["t"]);
}

// TODO: cannot use MVCC as we use indexes here
#[turso_macros::test(init_sql = "CREATE TABLE t(name, embedding)")]
fn test_vector_sparse_ivf_insert_query(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let index = VectorSparseInvertedIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "t".to_string(),
            index_name: "t_idx".to_string(),
            columns: vec![IndexColumn {
                name: "embedding".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 1,
                collation: None,
                default: None,
                expr: None,
            }],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.create(&conn)).unwrap();
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
        run(&tmp_db, || cursor.open_write(&conn)).unwrap();

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
        run(&tmp_db, || cursor.open_read(&conn)).unwrap();

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

// TODO: cannot use MVCC as we use indexes here
#[turso_macros::test(init_sql = "CREATE TABLE t(name, embedding)")]
fn test_vector_sparse_ivf_update(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let index = VectorSparseInvertedIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "t".to_string(),
            index_name: "t_idx".to_string(),
            columns: vec![IndexColumn {
                name: "embedding".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 1,
                collation: None,
                default: None,
                expr: None,
            }],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.create(&conn)).unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    let mut writer = attached.init().unwrap();
    run(&tmp_db, || writer.open_write(&conn)).unwrap();

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
    run(&tmp_db, || reader.open_read(&conn)).unwrap();
    assert!(!run(&tmp_db, || reader.query_start(&query_values)).unwrap());

    conn.execute(format!(
        "UPDATE t SET embedding = vector32_sparse('{v1_str}') WHERE rowid = 1"
    ))
    .unwrap();
    run(&tmp_db, || writer.delete(&insert0_values)).unwrap();
    run(&tmp_db, || writer.insert(&insert1_values)).unwrap();

    let mut reader = attached.init().unwrap();
    run(&tmp_db, || reader.open_read(&conn)).unwrap();
    assert!(run(&tmp_db, || reader.query_start(&query_values)).unwrap());
    assert_eq!(1, run(&tmp_db, || reader.query_rowid()).unwrap().unwrap());
    assert_eq!(
        0.0,
        run(&tmp_db, || reader.query_column(0)).unwrap().as_float()
    );
    assert!(!run(&tmp_db, || reader.query_next()).unwrap());
}

// TODO: cannot use MVCC as we use indexes here
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
        simple_conn.wal_auto_checkpoint_disable();
        index_conn.wal_auto_checkpoint_disable();
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
                let sql = format!("SELECT key, vector_distance_jaccard(embedding, vector32_sparse('{v}')) as d FROM t ORDER BY d LIMIT {k}");
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
            columns: vec![
                IndexColumn {
                    name: "title".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 1,
                    collation: None,
                    default: None,
                    expr: None,
                },
                IndexColumn {
                    name: "body".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 2,
                    collation: None,
                    default: None,
                    expr: None,
                },
            ],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.create(&conn)).unwrap();
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
        run(&tmp_db, || cursor.destroy(&conn)).unwrap();
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
            columns: vec![
                IndexColumn {
                    name: "title".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 1,
                    collation: None,
                    default: None,
                    expr: None,
                },
                IndexColumn {
                    name: "body".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 2,
                    collation: None,
                    default: None,
                    expr: None,
                },
            ],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.create(&conn)).unwrap();
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
        run(&tmp_db, || cursor.open_write(&conn)).unwrap();

        let values = [
            Register::Value(Value::Text(turso_core::types::Text::from(title))),
            Register::Value(Value::Text(turso_core::types::Text::from(body))),
            Register::Value(Value::from_i64(id)),
        ];
        run(&tmp_db, || cursor.insert(&values)).unwrap();
        // Flush FTS data before executing SQL (which auto-commits the transaction)
        // This mimics what VDBE does via index_method_pre_commit_all()
        run(&tmp_db, || cursor.pre_commit()).unwrap();
        conn.execute(format!(
            "INSERT INTO docs VALUES ({id}, '{title}', '{body}')"
        ))
        .unwrap();
    }

    // Query for "Rust" - should match docs 1 and 3
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.open_read(&conn)).unwrap();

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
        run(&tmp_db, || cursor.open_read(&conn)).unwrap();

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

    // Run OPTIMIZE INDEX on specific index
    conn.execute("OPTIMIZE INDEX fts_docs").unwrap();

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
