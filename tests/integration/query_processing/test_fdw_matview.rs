use crate::common::{self, ExecRows, TempDatabase};
use std::io::Write;
use tempfile::TempDir;

#[turso_macros::test(views)]
fn test_matview_on_csv_foreign_table(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // Create CSV file
    let csv_path = tmp_db.path.parent().unwrap().join("data.csv");
    let mut f = std::fs::File::create(&csv_path)?;
    writeln!(f, "id,name,score")?;
    writeln!(f, "1,alice,90")?;
    writeln!(f, "2,bob,75")?;
    writeln!(f, "3,carol,85")?;
    drop(f);

    // Set up foreign table
    common::run_query(
        &tmp_db,
        &conn,
        "CREATE SERVER csv_srv OPTIONS (driver 'csv')",
    )?;
    common::run_query(
        &tmp_db,
        &conn,
        &format!(
            "CREATE FOREIGN TABLE scores (id TEXT, name TEXT, score TEXT) \
             SERVER csv_srv OPTIONS (path '{}', skip_header 'true')",
            csv_path.display()
        ),
    )?;

    // Create materialized view on the foreign table
    common::run_query(
        &tmp_db,
        &conn,
        "CREATE MATERIALIZED VIEW mv_scores AS SELECT * FROM scores",
    )?;

    // Query the matview
    let rows: Vec<(String, String, String)> =
        conn.exec_rows("SELECT id, name, score FROM mv_scores");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        ("1".to_string(), "alice".to_string(), "90".to_string())
    );
    assert_eq!(
        rows[1],
        ("2".to_string(), "bob".to_string(), "75".to_string())
    );
    assert_eq!(
        rows[2],
        ("3".to_string(), "carol".to_string(), "85".to_string())
    );

    Ok(())
}

#[turso_macros::test(views)]
fn test_matview_on_fdw_with_projection(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let csv_path = tmp_db.path.parent().unwrap().join("proj.csv");
    let mut f = std::fs::File::create(&csv_path)?;
    writeln!(f, "id,name,score")?;
    writeln!(f, "1,alice,90")?;
    writeln!(f, "2,bob,75")?;
    drop(f);

    common::run_query(
        &tmp_db,
        &conn,
        "CREATE SERVER csv_srv OPTIONS (driver 'csv')",
    )?;
    common::run_query(
        &tmp_db,
        &conn,
        &format!(
            "CREATE FOREIGN TABLE items (id TEXT, name TEXT, score TEXT) \
             SERVER csv_srv OPTIONS (path '{}', skip_header 'true')",
            csv_path.display()
        ),
    )?;

    // Matview with projection (only name column)
    common::run_query(
        &tmp_db,
        &conn,
        "CREATE MATERIALIZED VIEW mv_names AS SELECT name FROM items",
    )?;

    let rows: Vec<(String,)> = conn.exec_rows("SELECT name FROM mv_names");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "alice");
    assert_eq!(rows[1].0, "bob");

    Ok(())
}

#[turso_macros::test(views)]
fn test_matview_on_fdw_join_with_local_table(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let csv_path = tmp_db.path.parent().unwrap().join("join.csv");
    let mut f = std::fs::File::create(&csv_path)?;
    writeln!(f, "id,name")?;
    writeln!(f, "1,alice")?;
    writeln!(f, "2,bob")?;
    writeln!(f, "3,carol")?;
    drop(f);

    common::run_query(
        &tmp_db,
        &conn,
        "CREATE SERVER csv_srv OPTIONS (driver 'csv')",
    )?;
    common::run_query(
        &tmp_db,
        &conn,
        &format!(
            "CREATE FOREIGN TABLE people (id TEXT, name TEXT) \
             SERVER csv_srv OPTIONS (path '{}', skip_header 'true')",
            csv_path.display()
        ),
    )?;

    // Create local table
    common::run_query(
        &tmp_db,
        &conn,
        "CREATE TABLE grades (person_id TEXT, grade TEXT)",
    )?;
    common::run_query(
        &tmp_db,
        &conn,
        "INSERT INTO grades VALUES ('1', 'A'), ('2', 'B')",
    )?;

    // Matview joining FDW with local table
    common::run_query(
        &tmp_db,
        &conn,
        "CREATE MATERIALIZED VIEW mv_grades AS \
         SELECT p.name, g.grade FROM people p \
         JOIN grades g ON p.id = g.person_id",
    )?;

    let rows: Vec<(String, String)> =
        conn.exec_rows("SELECT name, grade FROM mv_grades ORDER BY name");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("alice".to_string(), "A".to_string()));
    assert_eq!(rows[1], ("bob".to_string(), "B".to_string()));

    Ok(())
}

#[turso_macros::test(views)]
fn test_drop_matview_on_fdw(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let csv_path = tmp_db.path.parent().unwrap().join("drop.csv");
    let mut f = std::fs::File::create(&csv_path)?;
    writeln!(f, "x")?;
    writeln!(f, "1")?;
    drop(f);

    common::run_query(
        &tmp_db,
        &conn,
        "CREATE SERVER csv_srv OPTIONS (driver 'csv')",
    )?;
    common::run_query(
        &tmp_db,
        &conn,
        &format!(
            "CREATE FOREIGN TABLE t (x TEXT) SERVER csv_srv OPTIONS (path '{}', skip_header 'true')",
            csv_path.display()
        ),
    )?;

    common::run_query(
        &tmp_db,
        &conn,
        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t",
    )?;

    // Verify it exists
    let rows: Vec<(String,)> = conn.exec_rows("SELECT x FROM mv");
    assert_eq!(rows.len(), 1);

    // Drop it
    common::run_query(&tmp_db, &conn, "DROP VIEW mv")?;

    // Verify it's gone
    let result = conn.query("SELECT * FROM mv");
    assert!(result.is_err());

    Ok(())
}

#[turso_macros::test(views)]
fn test_refresh_matview_on_fdw(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let csv_path = tmp_db.path.parent().unwrap().join("refresh.csv");
    {
        let mut f = std::fs::File::create(&csv_path)?;
        writeln!(f, "id,val")?;
        writeln!(f, "1,old")?;
    }

    common::run_query(
        &tmp_db,
        &conn,
        "CREATE SERVER csv_srv OPTIONS (driver 'csv')",
    )?;
    common::run_query(
        &tmp_db,
        &conn,
        &format!(
            "CREATE FOREIGN TABLE data (id TEXT, val TEXT) \
             SERVER csv_srv OPTIONS (path '{}', skip_header 'true')",
            csv_path.display()
        ),
    )?;

    common::run_query(
        &tmp_db,
        &conn,
        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM data",
    )?;

    // Initial state
    let rows: Vec<(String, String)> = conn.exec_rows("SELECT id, val FROM mv");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], ("1".to_string(), "old".to_string()));

    // Update the CSV file
    {
        let mut f = std::fs::File::create(&csv_path)?;
        writeln!(f, "id,val")?;
        writeln!(f, "1,new")?;
        writeln!(f, "2,added")?;
    }

    // Matview still shows old data
    let rows: Vec<(String, String)> = conn.exec_rows("SELECT id, val FROM mv");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "old");

    // REFRESH picks up new data
    common::run_query(&tmp_db, &conn, "REFRESH MATERIALIZED VIEW mv")?;

    let rows: Vec<(String, String)> = conn.exec_rows("SELECT id, val FROM mv ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("1".to_string(), "new".to_string()));
    assert_eq!(rows[1], ("2".to_string(), "added".to_string()));

    Ok(())
}

#[test]
fn test_fdw_matview_survives_reopen() {
    let dir = TempDir::new().unwrap().keep();
    let db_path = dir.join("fdw_matview_reopen.db");
    let csv_path = dir.join("reopen.csv");
    {
        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "id,name").unwrap();
        writeln!(f, "1,alice").unwrap();
        writeln!(f, "2,bob").unwrap();
    }

    let opts = turso_core::DatabaseOpts::new()
        .with_encryption(true)
        .with_views(true);

    // Session 1: create FDW + matview
    {
        let db = TempDatabase::new_with_existent_with_opts(&db_path, opts.clone());
        let conn = db.connect_limbo();
        common::run_query(&db, &conn, "CREATE SERVER csv_srv OPTIONS (driver 'csv')").unwrap();
        common::run_query(
            &db,
            &conn,
            &format!(
                "CREATE FOREIGN TABLE people (id TEXT, name TEXT) \
                 SERVER csv_srv OPTIONS (path '{}', skip_header 'true')",
                csv_path.display()
            ),
        )
        .unwrap();
        common::run_query(
            &db,
            &conn,
            "CREATE MATERIALIZED VIEW mv_people AS SELECT * FROM people",
        )
        .unwrap();

        let rows: Vec<(String, String)> = conn.exec_rows("SELECT id, name FROM mv_people");
        assert_eq!(rows.len(), 2);
        conn.close().unwrap();
    }

    // Session 2: reopen — matview should still be queryable
    {
        let db = TempDatabase::new_with_existent_with_opts(&db_path, opts);
        let conn = db.connect_limbo();
        let rows: Vec<(String, String)> = conn.exec_rows("SELECT id, name FROM mv_people");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], ("1".to_string(), "alice".to_string()));
        assert_eq!(rows[1], ("2".to_string(), "bob".to_string()));
        conn.close().unwrap();
    }
}
