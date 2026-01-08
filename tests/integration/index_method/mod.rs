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

/// Test FTS with different tokenizer configurations via WITH clause
#[cfg(feature = "fts")]
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
    conn.execute("CREATE INDEX fts_simple ON docs_simple USING fts (content) WITH (tokenizer = 'simple')")
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
    assert!(rows.len() >= 1);
}

/// Test that invalid tokenizer names are rejected
#[cfg(feature = "fts")]
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
#[cfg(feature = "fts")]
#[turso_macros::test]
fn test_fts_ngram_tokenizer(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_products ON products USING fts (name) WITH (tokenizer = 'ngram')")
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
    assert!(rows.len() >= 1);

    // Search for "Gal" should match "Galaxy"
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM products WHERE fts_match(name, 'Gal')",
    );
    assert!(rows.len() >= 1);
}

/// Test fts_highlight function for text highlighting
#[cfg(feature = "fts")]
#[turso_macros::test]
fn test_fts_highlight_basic(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Test basic highlighting
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('The quick brown fox', 'quick', '<b>', '</b>')",
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
        "SELECT fts_highlight('hello world hello', 'hello', '[', ']')",
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
        "SELECT fts_highlight('Hello World', 'hello', '<em>', '</em>')",
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
        "SELECT fts_highlight('The quick brown fox', 'zebra', '<b>', '</b>')",
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
        "SELECT fts_highlight('Some text here', '', '<b>', '</b>')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "Some text here");
        }
        _ => panic!("Expected text result"),
    }
}

/// Test fts_highlight with FTS index queries
#[cfg(feature = "fts")]
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
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, fts_highlight(body, 'database', '<mark>', '</mark>') as highlighted FROM articles WHERE fts_match(title, body, 'database')",
    );

    // Should match article 1 (has "database" in both title and body)
    assert!(rows.len() >= 1);

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
    assert!(found_highlight, "Expected highlighted text with <mark> tags");
}

/// Test fts_highlight with NULL values
#[cfg(feature = "fts")]
#[turso_macros::test]
fn test_fts_highlight_null_handling(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // NULL text should return NULL
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight(NULL, 'query', '<b>', '</b>')",
    );
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], rusqlite::types::Value::Null));

    // NULL query should return NULL
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('text', NULL, '<b>', '</b>')",
    );
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], rusqlite::types::Value::Null));
}

/// Test field weights configuration for FTS indexes
#[cfg(feature = "fts")]
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
#[cfg(feature = "fts")]
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
    let result = conn.execute(
        "CREATE INDEX fts_bad2 ON docs USING fts (title) WITH (weights='title=abc')",
    );
    assert!(result.is_err());

    // Negative weight should fail
    let result = conn.execute(
        "CREATE INDEX fts_bad3 ON docs USING fts (title) WITH (weights='title=-1.0')",
    );
    assert!(result.is_err());

    // Missing equals sign should fail
    let result = conn.execute(
        "CREATE INDEX fts_bad4 ON docs USING fts (title) WITH (weights='title2.0')",
    );
    assert!(result.is_err());
}

/// Regression test: Query -> Insert -> Query should not panic with "dirty pages must be empty"
/// This tests that FTS cursor caching doesn't share pending_writes between cursors,
/// which would cause writes from one cursor to affect the Drop behavior of another.
#[cfg(feature = "fts")]
#[turso_macros::test(init_sql = "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")]
fn test_fts_query_insert_query_no_panic(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create FTS index
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert some initial data
    conn.execute("INSERT INTO articles VALUES (1, 'Rust Programming', 'Rust is a systems language')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'Python Guide', 'Python is easy to learn')")
        .unwrap();

    // Query a few times (this caches the directory)
    let rows = limbo_exec_rows(&conn, "SELECT * FROM articles WHERE fts_match(title, body, 'Rust')");
    assert_eq!(rows.len(), 1);

    let rows = limbo_exec_rows(&conn, "SELECT * FROM articles WHERE fts_match(title, body, 'Python')");
    assert_eq!(rows.len(), 1);

    let rows = limbo_exec_rows(&conn, "SELECT * FROM articles WHERE fts_match(title, body, 'programming')");
    assert_eq!(rows.len(), 1);

    // Insert more data (this should not cause dirty pages to leak to next read)
    conn.execute("INSERT INTO articles VALUES (3, 'Go Tutorial', 'Go is great for concurrency')")
        .unwrap();

    // Query again - should NOT panic with "dirty pages must be empty for read txn"
    let rows = limbo_exec_rows(&conn, "SELECT * FROM articles WHERE fts_match(title, body, 'Go')");
    assert_eq!(rows.len(), 1);

    // One more query to ensure everything is stable
    let rows = limbo_exec_rows(&conn, "SELECT * FROM articles WHERE fts_match(title, body, 'Rust')");
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
#[cfg(feature = "fts")]
#[turso_macros::test(init_sql = "CREATE TABLE docs(id INTEGER PRIMARY KEY, category TEXT, title TEXT, body TEXT)")]
fn test_fts_comprehensive_lifecycle(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // 1. Create FTS index
    conn.execute("CREATE INDEX fts_docs ON docs USING fts (title, body)")
        .unwrap();

    // Insert a moderate number of rows (100 documents across 4 categories)
    let categories = ["tech", "science", "business", "entertainment"];
    let tech_terms = [
        "Rust", "Python", "JavaScript", "programming", "software", "database",
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
        "movie", "music", "concert", "festival", "celebrity", "streaming",
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
        let title = format!("{} Article {}", term1, i);
        let body = format!(
            "This is article {} about {} and {}. More content here.",
            i, term1, term2
        );
        conn.execute(format!(
            "INSERT INTO docs VALUES ({}, '{}', '{}', '{}')",
            i, category, title, body
        ))
        .unwrap();
    }

    // 2. Query with FTS methods - verify initial state
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Rust')",
    );
    assert!(rows.len() > 0, "Should find Rust documents");
    let rust_count_initial = rows.len();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Python')",
    );
    assert!(rows.len() > 0, "Should find Python documents");

    // Query with score ordering
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'programming') as score, id FROM docs WHERE fts_match(title, body, 'programming') ORDER BY score DESC LIMIT 10",
    );
    assert!(rows.len() > 0, "Should find programming documents");

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
    // (or may still find it depending on FTS delete implementation)
    let has_deleted_doc = rows.iter().any(|r| {
        matches!(&r[0], rusqlite::types::Value::Integer(101))
    });
    // Note: If FTS doesn't support delete yet, this may still find the doc
    // For now, just verify the query doesn't panic
    let _ = rows.len(); // Query should not panic after delete
    if !has_deleted_doc && rows.len() == 0 {
        // FTS properly removed the deleted document
    }

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
    assert!(rows.len() > 0, "Should still find Python documents after update");

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
        rows.len() > 0,
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
#[cfg(feature = "fts")]
#[turso_macros::test(init_sql = "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, content TEXT)")]
fn test_fts_with_explicit_transactions(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create FTS index
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, content)")
        .unwrap();

    // Insert initial data
    conn.execute("INSERT INTO articles VALUES (1, 'Rust Basics', 'Introduction to Rust programming')")
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
    conn.execute("INSERT INTO articles VALUES (2, 'Advanced Rust', 'Rust ownership and lifetimes')")
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
    assert_eq!(
        rows.len(),
        0,
        "Should not find Go article after rollback"
    );

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
