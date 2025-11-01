use core_tester::common::{self, maybe_setup_tracing, rusqlite_integrity_check, TempDatabase};
use turso_core::Row;

#[test]

fn test_autovacuum_pointer_map_integrity() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    maybe_setup_tracing();

    let tmp_db = TempDatabase::new_with_autovacuum_full(
        "CREATE TABLE seed_table(id INTEGER PRIMARY KEY, value TEXT);\
         INSERT INTO seed_table VALUES (1, 'seed');",
    );
    let conn = tmp_db.connect_limbo();

    common::run_query(&tmp_db, &conn, "CREATE TABLE third(a);")?;
    for i in 1..=2000 {
        common::run_query(&tmp_db, &conn, &format!("INSERT INTO third VALUES ({i});"))?;
    }

    common::run_query_on_row(&tmp_db, &conn, "PRAGMA integrity_check;", |row: &Row| {
        let res = row.get::<String>(0).unwrap();
        assert_eq!(res, "ok");
    })?;

    rusqlite_integrity_check(tmp_db.path.as_path())?;

    Ok(())
}
