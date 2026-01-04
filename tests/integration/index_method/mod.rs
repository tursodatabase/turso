use std::collections::HashMap;

use core_tester::common::rng_from_time_or_env;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
#[cfg(feature = "fts")]
use turso_core::index_method::fts::FtsIndexMethod;
use turso_core::{
    index_method::{
        toy_vector_sparse_ivf::VectorSparseInvertedIndexMethod, IndexMethod,
        IndexMethodConfiguration,
    },
    schema::IndexColumn,
    types::IOResult,
    vector::{self, vector_types::VectorType},
    Register, Result, Value,
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
            parameters: HashMap::new(),
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
            parameters: HashMap::new(),
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
            Register::Value(Value::Integer((i + 1) as i64)),
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
            Register::Value(Value::Integer(0)),
            Register::Value(sparse_vector(vector)),
            Register::Value(Value::Integer(5)),
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
            parameters: HashMap::new(),
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
        Register::Value(Value::Integer(1)),
    ];
    let insert1_values = [
        Register::Value(v1.clone()),
        Register::Value(Value::Integer(1)),
    ];
    let query_values = [
        Register::Value(Value::Integer(0)),
        Register::Value(q.clone()),
        Register::Value(Value::Integer(1)),
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

#[cfg(feature = "fts")]
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
            parameters: HashMap::new(),
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
    // The internal FTS tables should no longer contain data (they may still exist as empty tables)
}

#[cfg(feature = "fts")]
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
            parameters: HashMap::new(),
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
            Register::Value(Value::Integer(id)),
        ];
        run(&tmp_db, || cursor.insert(&values)).unwrap();
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
            Register::Value(Value::Integer(0)), // pattern index
            Register::Value(Value::Text(turso_core::types::Text::from("Rust"))),
            Register::Value(Value::Integer(10)), // limit
        ];
        assert!(run(&tmp_db, || cursor.query_start(&values)).unwrap());

        // Collect results
        let mut results = Vec::new();
        loop {
            let rowid = run(&tmp_db, || cursor.query_rowid()).unwrap().unwrap();
            let score = run(&tmp_db, || cursor.query_column(0)).unwrap();
            if let Value::Float(s) = score {
                results.push((rowid, s));
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
            Register::Value(Value::Integer(0)),
            Register::Value(Value::Text(turso_core::types::Text::from("Python"))),
            Register::Value(Value::Integer(10)),
        ];
        assert!(run(&tmp_db, || cursor.query_start(&values)).unwrap());

        let rowid = run(&tmp_db, || cursor.query_rowid()).unwrap().unwrap();
        assert_eq!(rowid, 2);
        assert!(!run(&tmp_db, || cursor.query_next()).unwrap());
    }
}

#[cfg(feature = "fts")]
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

#[cfg(feature = "fts")]
#[turso_macros::test]
fn test_fts_combined_score_match_with_extra_where(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table and FTS index
    conn.execute(
        "CREATE TABLE posts(id INTEGER PRIMARY KEY, category TEXT, title TEXT, content TEXT)",
    )
    .unwrap();
    conn.execute("CREATE INDEX fts_posts ON posts USING fts (title, content)")
        .unwrap();

    // Insert test data with different categories
    conn.execute(
        "INSERT INTO posts VALUES (1, 'tech', 'Rust Programming', 'Rust is a systems language')",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO posts VALUES (2, 'tech', 'Python Programming', 'Python is versatile')",
    )
    .unwrap();
    conn.execute("INSERT INTO posts VALUES (3, 'science', 'Rust in Nature', 'Rust affects metal structures')")
        .unwrap();
    conn.execute(
        "INSERT INTO posts VALUES (4, 'tech', 'Advanced Rust', 'Learn advanced Rust patterns')",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO posts VALUES (5, 'science', 'Metal Corrosion', 'Rust is a form of corrosion')",
    )
    .unwrap();

    // Test combined fts_score + fts_match with extra WHERE condition
    // Should only return tech posts about Rust
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, content, 'Rust') as score, id, title FROM posts WHERE fts_match(title, content, 'Rust') AND category = 'tech' ORDER BY score DESC LIMIT 10",
    );

    // Should match posts 1 and 4 (tech posts with Rust)
    assert_eq!(rows.len(), 2);
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&4));
    // Should NOT contain science posts (3 and 5)
    assert!(!ids.contains(&3));
    assert!(!ids.contains(&5));

    // Test with numeric range filter
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, content, 'Rust') as score, id FROM posts WHERE fts_match(title, content, 'Rust') AND id > 2 ORDER BY score DESC LIMIT 10",
    );
    // Should match posts 3, 4, 5 (id > 2 and contains Rust)
    assert_eq!(rows.len(), 3);
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&3));
    assert!(ids.contains(&4));
    assert!(ids.contains(&5));
    assert!(!ids.contains(&1));
    assert!(!ids.contains(&2));

    // Test with multiple extra WHERE conditions
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, content, 'Rust') as score, id FROM posts WHERE fts_match(title, content, 'Rust') AND id >= 2 AND id <= 4 ORDER BY score DESC",
    );
    // Should match posts 3 and 4 (id between 2-4 and contains Rust, excluding 2 which doesn't have Rust)
    assert_eq!(rows.len(), 2);
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&3));
    assert!(ids.contains(&4));
}

#[cfg(feature = "fts")]
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

/// Test FTS function recognition mode - queries that don't match predefined patterns
/// but are optimized via fts_match/fts_score function detection.
#[cfg(feature = "fts")]
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
#[cfg(feature = "fts")]
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
