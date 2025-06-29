#[cfg(feature = "futures")]
use futures_util::TryStreamExt;
use turso::{Builder, Value};

#[cfg(not(feature = "futures"))]
macro_rules! rows_next {
    ($rows:expr) => {
        $rows.next()
    };
}

#[cfg(feature = "futures")]
macro_rules! rows_next {
    ($rows:expr) => {
        $rows.try_next()
    };
}

#[tokio::test]
async fn test_rows_next() {
    let builder = Builder::new_local(":memory:");
    let db = builder.build().await.unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE test (x INTEGER)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO test (x) VALUES (1)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO test (x) VALUES (2)", ())
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO test (x) VALUES (:x)",
        vec![(":x".to_string(), Value::Integer(3))],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO test (x) VALUES (@x)",
        vec![("@x".to_string(), Value::Integer(4))],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO test (x) VALUES ($x)",
        vec![("$x".to_string(), Value::Integer(5))],
    )
    .await
    .unwrap();
    let mut res = conn.query("SELECT * FROM test", ()).await.unwrap();
    assert_eq!(
        rows_next!(res)
            .await
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap(),
        1.into()
    );
    assert_eq!(
        rows_next!(res)
            .await
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap(),
        2.into()
    );
    assert_eq!(
        rows_next!(res)
            .await
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap(),
        3.into()
    );
    assert_eq!(
        rows_next!(res)
            .await
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap(),
        4.into()
    );
    assert_eq!(
        rows_next!(res)
            .await
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap(),
        5.into()
    );
    assert!(rows_next!(res).await.unwrap().is_none());
}
