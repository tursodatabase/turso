use std::sync::{atomic::AtomicUsize, Arc};

use turso_core::{Statement, StepResult};

use crate::common::{maybe_setup_tracing, TempDatabase};

#[turso_macros::test(mvcc)]
fn test_schema_reprepare(tmp_db: TempDatabase) {
    let conn1 = tmp_db.connect_limbo();
    conn1.execute("CREATE TABLE t(x, y, z)").unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1, 2, 3), (10, 20, 30)")
        .unwrap();
    let conn2 = tmp_db.connect_limbo();
    let mut stmt = conn2.prepare("SELECT y, z FROM t").unwrap();
    let mut stmt2 = conn2.prepare("SELECT x, z FROM t").unwrap();
    conn1.execute("ALTER TABLE t DROP COLUMN x").unwrap();
    loop {
        match stmt2.step() {
            Ok(StepResult::IO) => tmp_db.io.step().unwrap(),
            Err(err) => {
                assert_eq!(err.to_string(), "Parse error: no such column: x");
                break;
            }
            r => panic!("unexpected response: {r:?}"),
        }
    }

    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push((row.get::<i64>(0).unwrap(), row.get::<i64>(1).unwrap()));
        Ok(())
    })
    .unwrap();

    let row = rows[0];
    assert_eq!(row, (2, 3));
    let row = rows[1];
    assert_eq!(row, (20, 30));
}

#[turso_macros::test(mvcc)]
#[ignore]
fn test_create_multiple_connections(tmp_db: TempDatabase) -> anyhow::Result<()> {
    maybe_setup_tracing();
    let tries = 1;
    let opts = tmp_db.db_opts;
    let flags = tmp_db.db_flags;
    for _ in 0..tries {
        let tmp_db = Arc::new(
            TempDatabase::builder()
                .with_flags(flags)
                .with_opts(opts)
                .build(),
        );
        {
            let conn = tmp_db.connect_limbo();
            conn.execute("CREATE TABLE t (x)").unwrap();
        }

        let mut threads = Vec::new();
        for i in 0..10 {
            let tmp_db_ = tmp_db.clone();
            threads.push(std::thread::spawn(move || {
                let conn = tmp_db_.connect_limbo();
                'outer: loop {
                    let mut stmt = conn
                        .prepare(format!("INSERT INTO t VALUES ({i})").as_str())
                        .unwrap();
                    tracing::info!("inserting row {}", i);
                    loop {
                        match stmt.step().unwrap() {
                            StepResult::Row => {
                                panic!("unexpected row result");
                            }
                            StepResult::IO => {
                                stmt._io().step().unwrap();
                            }
                            StepResult::Done => {
                                tracing::info!("inserted row {}", i);
                                break 'outer;
                            }
                            StepResult::Interrupt => {
                                panic!("unexpected step result");
                            }
                            StepResult::Busy => {
                                // repeat until we can insert it
                                tracing::info!("busy {}, repeating", i);
                                break;
                            }
                        }
                    }
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }

        let conn = tmp_db.connect_limbo();
        let mut stmt = conn.prepare("SELECT * FROM t").unwrap();
        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push(row.get::<i64>(0).unwrap());
            Ok(())
        })
        .unwrap();

        rows.sort();
        assert_eq!(rows, (0..10).collect::<Vec<_>>());
    }
    Ok(())
}

#[turso_macros::test(mvcc)]
#[ignore]
fn test_reader_writer(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let tries = 10;
    let opts = tmp_db.db_opts;
    let flags = tmp_db.db_flags;
    for _ in 0..tries {
        let tmp_db = Arc::new(
            TempDatabase::builder()
                .with_flags(flags)
                .with_opts(opts)
                .build(),
        );
        {
            let conn = tmp_db.connect_limbo();
            conn.execute("CREATE TABLE t (x)").unwrap();
        }

        let mut threads = Vec::new();
        let number_of_writers = 100;
        let current_written_rows = Arc::new(AtomicUsize::new(0));
        {
            let tmp_db = tmp_db.clone();
            let current_written_rows = current_written_rows.clone();
            threads.push(std::thread::spawn(move || {
                let conn = tmp_db.connect_limbo();
                for i in 0..number_of_writers {
                    conn.execute(format!("INSERT INTO t VALUES ({i})").as_str())
                        .unwrap();
                    current_written_rows.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }));
        }
        {
            let current_written_rows = current_written_rows.clone();
            threads.push(std::thread::spawn(move || {
                let conn = tmp_db.connect_limbo();
                loop {
                    let current_written_rows =
                        current_written_rows.load(std::sync::atomic::Ordering::Relaxed);
                    if current_written_rows == number_of_writers {
                        break;
                    }
                    let mut stmt = conn.prepare("SELECT * FROM t").unwrap();
                    let mut rows = Vec::new();
                    stmt.run_with_row_callback(|row| {
                        let x = row.get::<i64>(0).unwrap();
                        rows.push(x);
                        Ok(())
                    }).unwrap();
                    rows.sort();
                    for i in 0..current_written_rows {
                        let i = i as i64;
                        assert!(
                            rows.contains(&i),
                            "row {i} not found in {rows:?}. current_written_rows: {current_written_rows}",
                        );
                    }
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }
    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_schema_reprepare_write(tmp_db: TempDatabase) {
    maybe_setup_tracing();
    let conn1 = tmp_db.connect_limbo();
    conn1.execute("CREATE TABLE t(x, y, z)").unwrap();
    let conn2 = tmp_db.connect_limbo();
    let mut stmt = conn2.prepare("INSERT INTO t(y, z) VALUES (1, 2)").unwrap();
    let mut stmt2 = conn2.prepare("INSERT INTO t(y, z) VALUES (3, 4)").unwrap();
    conn1.execute("ALTER TABLE t DROP COLUMN x").unwrap();

    tracing::info!("Executing Stmt 1");
    stmt.run_with_row_callback(|_| panic!("unexpected row"))
        .unwrap();

    tracing::info!("Executing Stmt 2");
    stmt2
        .run_with_row_callback(|_| panic!("unexpected row"))
        .unwrap();
}

fn advance(stmt: &mut Statement) -> anyhow::Result<()> {
    tracing::info!("Advancing statement: {:?}", stmt.get_sql());
    while matches!(stmt.step()?, StepResult::IO) {
        stmt._io().step()?;
    }
    Ok(())
}

/// Regression test detected by whopper
// TODO: cannot run with mvcc as we don't support indexes
#[turso_macros::test]
fn test_interleaved_transactions(tmp_db: TempDatabase) -> anyhow::Result<()> {
    maybe_setup_tracing();
    {
        let bootstrap_conn = tmp_db.connect_limbo();
        bootstrap_conn.execute("CREATE TABLE table_0 (id INTEGER,col_1 REAL,col_2 INTEGER,col_3 REAL,col_4 TEXT,col_5 REAL,col_6 TEXT)")?;
        bootstrap_conn.execute("CREATE INDEX idx_table_0_0 ON table_0 (col_4 ASC)")?;
        bootstrap_conn.execute("CREATE INDEX idx_table_0_1 ON table_0 (id DESC)")?;
        bootstrap_conn.execute("CREATE INDEX idx_table_0_2 ON table_0 (col_5 DESC)")?;
        bootstrap_conn.close()?;
    }

    let conn = [
        tmp_db.connect_limbo(),
        tmp_db.connect_limbo(),
        tmp_db.connect_limbo(),
        tmp_db.connect_limbo(),
    ];

    let mut statement0 = conn[0].prepare("BEGIN")?;
    let mut statement1 = conn[1].prepare("BEGIN")?;
    let mut statement2 = conn[2].prepare("BEGIN")?;

    advance(&mut statement2)?;

    let mut statement2 = conn[2].prepare("DELETE FROM table_0 WHERE (TRUE)")?;

    advance(&mut statement0)?;

    let mut statement0 = conn[0].prepare("COMMIT")?;

    advance(&mut statement1)?;

    let mut statement1 = conn[1].prepare("UPDATE table_0 SET col_5 = -2926216022.864461, col_1 = 1136343846.3760414, col_2 = 1260332354248861058, col_6 = 'breathtaking_wallace', col_3 = -8354252674.968108, id = -2763965266862900284 WHERE (TRUE)")?;

    advance(&mut statement2)?;
    advance(&mut statement0)?;
    advance(&mut statement1)?;
    advance(&mut statement2)?;

    let mut statement2 = conn[2].prepare("COMMIT")?;

    advance(&mut statement1)?;
    advance(&mut statement2)?;

    let mut statement2 = conn[2].prepare("BEGIN")?;

    let mut statement3 = conn[3].prepare("BEGIN")?;

    advance(&mut statement1)?;
    advance(&mut statement2)?;

    let mut statement2 = conn[2].prepare("INSERT INTO table_0 VALUES (3433031730186055493, -9049649117.499245, 377748201198469116, -303828055.307354, 'hardworking_pinotnoir', 3130880977.346573, 'dazzling_identity'), (4047491512698975530, -6415241771.805258, 8252804953477887816, 6468710871.6649, 'diplomatic_karamazov', 590358226.8343716, 'hilarious_strasbourg'), (2865599543545078376, 84894401.22016525, 9113810426850381627, -1136160051.7521439, 'funny_benton', 9522389352.598354, 'magnificent_french'), (-157899885850804353, -303833796.57147026, 486259919370287064, -9427424128.005714, 'considerate_diamant', -3105334243.936157, 'kind_walker'), (8502247374804763489, -2126532888.2616653, 5690470012873526939, 4011656749.2326107, 'kind_ladrido', 1381034902.7760563, 'humorous_crane'), (2487742055507017334, -5830452441.986847, 3661939929057695925, -6299976423.211256, 'adaptable_raevsky', -7871748970.666381, 'technological_bataille'), (-5348095239593101865, -998225440.4524403, 5195262288395229508, -8305444803.975374, 'upbeat_courtney', -3943473497.4281626, 'imaginative_arrigoni'), (-6470080787150464674, -5833281407.383408, 5877012236478010308, 3023123550.254177, 'fearless_cairo', -141073531.91679573, 'generous_university')")?;

    advance(&mut statement3)?;

    let mut statement3 = conn[3].prepare("INSERT INTO table_0 VALUES (-7468459471409934075, 8179435779.870651, -7868006515434924912, -5415470506.527203, 'affectionate_n1x', -88866295.57206345, 'agreeable_treloar'), (3000445982321368777, 3099814982.0727863, -5101787605795972474, -925278326.7265358, 'giving_individualiste', -6553332857.366568, 'brave_patrizia'), (406163996859206098, -3340292138.289094, -5058217201699339610, 2605267874.8582096, 'fabulous_burnett', -4601912326.914466, 'super_jedi'), (6398781934600428549, -6770226564.882048, -2332649333251794167, 6904161964.055864, 'shining_sergent', -4779129294.073781, 'hardworking_beggar'), (1530150677936272307, -8683321096.443897, -2211014401610293017, 2417417840.8996468, 'magnificent_datacide', -2218929107.793541, 'ravishing_nw'), (8028216547992752413, -8876487798.088352, 8974386493479719872, -6723037189.199554, 'glimmering_murray', -1973499548.0633707, 'spectacular_fitzpatrick')")?;

    advance(&mut statement1)?;

    let mut statement1 = conn[1].prepare(
        "CREATE INDEX idx_table_0_persiste ON table_0 (col_2 ASC, col_6 ASC, col_3 ASC)",
    )?;

    advance(&mut statement2)?;
    advance(&mut statement3)?;

    let mut statement0 = conn[0].prepare("BEGIN")?;

    advance(&mut statement1)?;

    let mut statement1 = conn[1].prepare("COMMIT")?;

    advance(&mut statement2)?;
    advance(&mut statement0)?;

    let mut _statement0 = conn[0].prepare("UPDATE table_0 SET col_3 = 7634893024.5729065, col_6 = 'glimmering_besnard', col_1 = 9240915430.267292, col_4 = 'efficient_mekan' WHERE (TRUE)")?;

    advance(&mut statement1)?;
    advance(&mut statement2)?;

    Ok(())
}
