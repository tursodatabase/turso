use std::collections::HashMap;

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

fn run<T>(db: &TempDatabase, mut f: impl FnMut() -> Result<IOResult<T>>) -> T {
    loop {
        match f().unwrap() {
            IOResult::Done(value) => return value,
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

#[test]
fn test_vector_sparse_ivf_create_destroy() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t(name, embedding)");
    let conn = tmp_db.connect_limbo();

    let schema_rows = || {
        limbo_exec_rows(&tmp_db, &conn, "SELECT * FROM sqlite_master")
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
            }],
            parameters: HashMap::new(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.create(&conn));
    }
    conn.wal_insert_end(true).unwrap();
    assert_eq!(schema_rows(), vec!["t", "t_idx_scratch"]);

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.destroy(&conn));
    }
    conn.wal_insert_end(true).unwrap();
    assert_eq!(schema_rows(), vec!["t"]);
}

#[test]
fn test_vector_sparse_ivf_insert_query() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t(name, embedding)");
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
            }],
            parameters: HashMap::new(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.create(&conn));
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
        run(&tmp_db, || cursor.open_write(&conn));

        let values = [
            Register::Value(sparse_vector(vector_str)),
            Register::Value(Value::Integer((i + 1) as i64)),
        ];
        run(&tmp_db, || cursor.insert(&values));
        limbo_exec_rows(
            &tmp_db,
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
        run(&tmp_db, || cursor.open_read(&conn));

        let values = [
            Register::Value(Value::Integer(0)),
            Register::Value(sparse_vector(vector)),
            Register::Value(Value::Integer(5)),
        ];
        run(&tmp_db, || cursor.query_start(&values));

        for (rowid, dist) in results {
            assert!(run(&tmp_db, || cursor.query_next()));
            assert_eq!(*rowid, run(&tmp_db, || cursor.query_rowid()).unwrap());
            assert_eq!(*dist, run(&tmp_db, || cursor.query_column(0)).as_float());
        }

        assert!(!run(&tmp_db, || cursor.query_next()));
    }
}

#[test]
fn test_vector_sparse_ivf_update() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t(name, embedding)");
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
            }],
            parameters: HashMap::new(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || cursor.create(&conn));
    }
    conn.wal_insert_end(true).unwrap();

    let mut writer = attached.init().unwrap();
    run(&tmp_db, || writer.open_write(&conn));

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
    run(&tmp_db, || writer.insert(&insert0_values));
    limbo_exec_rows(
        &tmp_db,
        &conn,
        &format!("INSERT INTO t VALUES ('test', vector32_sparse('{v0_str}'))"),
    );

    let mut reader = attached.init().unwrap();
    run(&tmp_db, || reader.open_read(&conn));
    run(&tmp_db, || reader.query_start(&query_values));
    assert!(!run(&tmp_db, || reader.query_next()));

    limbo_exec_rows(
        &tmp_db,
        &conn,
        &format!("UPDATE t SET embedding = vector32_sparse('{v1_str}') WHERE rowid = 1"),
    );
    run(&tmp_db, || writer.delete(&insert0_values));
    run(&tmp_db, || writer.insert(&insert1_values));

    let mut reader = attached.init().unwrap();
    run(&tmp_db, || reader.open_read(&conn));
    run(&tmp_db, || reader.query_start(&query_values));
    assert!(run(&tmp_db, || reader.query_next()));
    assert_eq!(1, run(&tmp_db, || reader.query_rowid()).unwrap());
    assert_eq!(0.0, run(&tmp_db, || reader.query_column(0)).as_float());
    assert!(!run(&tmp_db, || reader.query_next()));
}
