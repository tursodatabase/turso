use limbo::Builder;

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
    let mut res = conn.query("SELECT * FROM test", ()).await.unwrap();
    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        1.into()
    );
    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        2.into()
    );
    assert!(res.next().await.unwrap().is_none());
}
