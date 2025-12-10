use std::collections::HashMap;

use core_tester::common::rng_from_time_or_env;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
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
        limbo_exec_rows(
            &conn,
            &format!("INSERT INTO t VALUES ('{i}', vector32_sparse('{vector_str}'))"),
        );
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
    limbo_exec_rows(
        &conn,
        &format!("INSERT INTO t VALUES ('test', vector32_sparse('{v0_str}'))"),
    );

    let mut reader = attached.init().unwrap();
    run(&tmp_db, || reader.open_read(&conn)).unwrap();
    assert!(!run(&tmp_db, || reader.query_start(&query_values)).unwrap());

    limbo_exec_rows(
        &conn,
        &format!("UPDATE t SET embedding = vector32_sparse('{v1_str}') WHERE rowid = 1"),
    );
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
