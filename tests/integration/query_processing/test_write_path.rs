use crate::common::{self, maybe_setup_tracing};
use crate::common::{compare_string, do_flush, TempDatabase};
use log::debug;
use std::io::{Read, Seek, Write};
use std::sync::Arc;
use turso_core::{CheckpointMode, Connection, Database, Row, Statement, StepResult, Value};

const WAL_HEADER_SIZE: usize = 32;
const WAL_FRAME_HEADER_SIZE: usize = 24;

#[macro_export]
macro_rules! change_state {
    ($current:expr, $pattern:pat => $selector:expr) => {
        let state = match std::mem::replace($current, unsafe { std::mem::zeroed() }) {
            $pattern => $selector,
            _ => panic!("unexpected state"),
        };
        #[allow(clippy::forget_non_drop)]
        std::mem::forget(std::mem::replace($current, state));
    };
}

#[test]
#[ignore]
fn test_simple_overflow_page() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite(
        "CREATE TABLE test (x INTEGER PRIMARY KEY, t TEXT);",
        false,
    );
    let conn = tmp_db.connect_limbo();

    let mut huge_text = String::new();
    for i in 0..8192 {
        huge_text.push((b'A' + (i % 24) as u8) as char);
    }

    let list_query = "SELECT * FROM test LIMIT 1";
    let insert_query = format!("INSERT INTO test VALUES (1, '{}')", huge_text.as_str());

    match conn.query(insert_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::IO => {
                    rows.run_once()?;
                }
                StepResult::Done => break,
                _ => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    };

    // this flush helped to review hex of test.db
    do_flush(&conn, &tmp_db)?;

    match conn.query(list_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::Row => {
                    let row = rows.row().unwrap();
                    let id = row.get::<i64>(0).unwrap();
                    let text = row.get::<&str>(0).unwrap();
                    assert_eq!(1, id);
                    compare_string(&huge_text, text);
                }
                StepResult::IO => {
                    rows.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    }
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[test]
fn test_sequential_overflow_page() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new_with_rusqlite(
        "CREATE TABLE test (x INTEGER PRIMARY KEY, t TEXT);",
        false,
    );
    let conn = tmp_db.connect_limbo();
    let iterations = 10_usize;

    let mut huge_texts = Vec::new();
    for i in 0..iterations {
        let mut huge_text = String::new();
        for _j in 0..8192 {
            huge_text.push((b'A' + i as u8) as char);
        }
        huge_texts.push(huge_text);
    }

    for (i, huge_text) in huge_texts.iter().enumerate().take(iterations) {
        let insert_query = format!("INSERT INTO test VALUES ({}, '{}')", i, huge_text.as_str());
        match conn.query(insert_query) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::IO => {
                        rows.run_once()?;
                    }
                    StepResult::Done => break,
                    _ => unreachable!(),
                }
            },
            Ok(None) => {}
            Err(err) => {
                eprintln!("{err}");
            }
        };
    }

    let list_query = "SELECT * FROM test LIMIT 1";
    let mut current_index = 0;
    match conn.query(list_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::Row => {
                    let row = rows.row().unwrap();
                    let id = row.get::<i64>(0).unwrap();
                    let text = row.get::<String>(1).unwrap();
                    let huge_text = &huge_texts[current_index];
                    compare_string(huge_text, text);
                    assert_eq!(current_index, id as usize);
                    current_index += 1;
                }
                StepResult::IO => {
                    rows.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    }
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[test_log::test]
#[ignore = "this takes too long :)"]
fn test_sequential_write() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    maybe_setup_tracing();

    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY);", false);
    let conn = tmp_db.connect_limbo();

    let list_query = "SELECT * FROM test";
    let max_iterations = 10000;
    for i in 0..max_iterations {
        println!("inserting {i} ");
        if (i % 100) == 0 {
            let progress = (i as f64 / max_iterations as f64) * 100.0;
            println!("progress {progress:.1}%");
        }
        let insert_query = format!("INSERT INTO test VALUES ({i})");
        run_query(&tmp_db, &conn, &insert_query)?;

        let mut current_read_index = 0;
        run_query_on_row(&tmp_db, &conn, list_query, |row: &Row| {
            let first_value = row.get::<&Value>(0).expect("missing id");
            let id = match first_value {
                turso_core::Value::Integer(i) => *i as i32,
                turso_core::Value::Float(f) => *f as i32,
                _ => unreachable!(),
            };
            assert_eq!(current_read_index, id);
            current_read_index += 1;
        })?;
        common::do_flush(&conn, &tmp_db)?;
    }
    Ok(())
}

#[test]
/// There was a regression with inserting multiple rows with a column containing an unary operator :)
/// https://github.com/tursodatabase/turso/pull/679
fn test_regression_multi_row_insert() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x REAL);", false);
    let conn = tmp_db.connect_limbo();

    let insert_query = "INSERT INTO test VALUES (-2), (-3), (-1)";
    let list_query = "SELECT * FROM test";

    run_query(&tmp_db, &conn, insert_query)?;

    common::do_flush(&conn, &tmp_db)?;

    let mut current_read_index = 1;
    let expected_ids = vec![-3, -2, -1];
    let mut actual_ids = Vec::new();
    run_query_on_row(&tmp_db, &conn, list_query, |row: &Row| {
        let first_value = row.get::<&Value>(0).expect("missing id");
        let id = match first_value {
            Value::Float(f) => *f as i32,
            _ => panic!("expected float"),
        };
        actual_ids.push(id);
        current_read_index += 1;
    })?;

    assert_eq!(current_read_index, 4); // Verify we read all rows
                                       // sort ids
    actual_ids.sort();
    assert_eq!(actual_ids, expected_ids);
    Ok(())
}

#[test]
fn test_statement_reset() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);", false);
    let conn = tmp_db.connect_limbo();

    conn.execute("insert into test values (1)")?;
    conn.execute("insert into test values (2)")?;

    let mut stmt = conn.prepare("select * from test")?;

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&Value>(0).unwrap(),
                    turso_core::Value::Integer(1)
                );
                break;
            }
            StepResult::IO => stmt.run_once()?,
            _ => break,
        }
    }

    stmt.reset();

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&Value>(0).unwrap(),
                    turso_core::Value::Integer(1)
                );
                break;
            }
            StepResult::IO => stmt.run_once()?,
            _ => break,
        }
    }

    Ok(())
}

#[test]
#[ignore]
fn test_wal_checkpoint() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY);", false);
    // threshold is 1000 by default
    let iterations = 1001_usize;
    let conn = tmp_db.connect_limbo();

    for i in 0..iterations {
        let insert_query = format!("INSERT INTO test VALUES ({i})");
        do_flush(&conn, &tmp_db)?;
        conn.checkpoint(CheckpointMode::Passive)?;
        run_query(&tmp_db, &conn, &insert_query)?;
    }

    do_flush(&conn, &tmp_db)?;
    let list_query = "SELECT * FROM test LIMIT 1";
    let mut current_index = 0;
    run_query_on_row(&tmp_db, &conn, list_query, |row: &Row| {
        let id = row.get::<i64>(0).unwrap();
        assert_eq!(current_index, id as usize);
        current_index += 1;
    })?;
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[test]
fn test_wal_restart() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY);", false);
    // threshold is 1000 by default

    fn insert(i: usize, conn: &Arc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<()> {
        debug!("inserting {i}");
        let insert_query = format!("INSERT INTO test VALUES ({i})");
        run_query(tmp_db, conn, &insert_query)?;
        debug!("inserted {i}");
        tmp_db.io.run_once()?;
        Ok(())
    }

    fn count(conn: &Arc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<usize> {
        debug!("counting");
        let list_query = "SELECT count(x) FROM test";
        let mut count = None;
        run_query_on_row(tmp_db, conn, list_query, |row: &Row| {
            assert!(count.is_none());
            count = Some(row.get::<i64>(0).unwrap() as usize);
            debug!("counted {count:?}");
        })?;
        Ok(count.unwrap())
    }

    {
        let conn = tmp_db.connect_limbo();
        insert(1, &conn, &tmp_db)?;
        assert_eq!(count(&conn, &tmp_db)?, 1);
        conn.close()?;
    }
    {
        let conn = tmp_db.connect_limbo();
        assert_eq!(
            count(&conn, &tmp_db)?,
            1,
            "failed to read from wal from another connection"
        );
        conn.close()?;
    }
    Ok(())
}

#[test]
fn test_insert_after_big_blob() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE temp (t1 BLOB, t2 INTEGER)", false);
    let conn = tmp_db.connect_limbo();

    conn.execute("insert into temp(t1) values (zeroblob (262144))")?;
    conn.execute("insert into temp(t2) values (1)")?;

    Ok(())
}

#[test_log::test]
#[ignore = "this takes too long :)"]
fn test_write_delete_with_index() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    maybe_setup_tracing();

    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x PRIMARY KEY);", false);
    let conn = tmp_db.connect_limbo();

    let list_query = "SELECT * FROM test";
    let max_iterations = 1000;
    for i in 0..max_iterations {
        println!("inserting {i} ");
        let insert_query = format!("INSERT INTO test VALUES ({i})");
        run_query(&tmp_db, &conn, &insert_query)?;
    }
    for i in 0..max_iterations {
        println!("deleting {i} ");
        let delete_query = format!("delete from test where x={i}");
        run_query(&tmp_db, &conn, &delete_query)?;
        println!("listing after deleting {i} ");
        let mut current_read_index = i + 1;
        run_query_on_row(&tmp_db, &conn, list_query, |row: &Row| {
            let first_value = row.get::<&Value>(0).expect("missing id");
            let id = match first_value {
                turso_core::Value::Integer(i) => *i as i32,
                turso_core::Value::Float(f) => *f as i32,
                _ => unreachable!(),
            };
            assert_eq!(current_read_index, id);
            current_read_index += 1;
        })?;
        for i in i + 1..max_iterations {
            // now test with seek
            run_query_on_row(
                &tmp_db,
                &conn,
                &format!("select * from test where x = {i}"),
                |row| {
                    let first_value = row.get::<&Value>(0).expect("missing id");
                    let id = match first_value {
                        turso_core::Value::Integer(i) => *i as i32,
                        turso_core::Value::Float(f) => *f as i32,
                        _ => unreachable!(),
                    };
                    assert_eq!(i, id);
                },
            )?;
        }
    }

    Ok(())
}

#[test]
fn test_update_with_index() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    maybe_setup_tracing();

    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (x REAL PRIMARY KEY, y TEXT);", true);
    let conn = tmp_db.connect_limbo();

    run_query(&tmp_db, &conn, "INSERT INTO test VALUES (1.0, 'foo')")?;
    run_query(&tmp_db, &conn, "INSERT INTO test VALUES (2.0, 'bar')")?;

    run_query_on_row(&tmp_db, &conn, "SELECT * from test WHERE x=10.0", |row| {
        assert_eq!(row.get::<f64>(0).unwrap(), 1.0);
    })?;
    run_query(&tmp_db, &conn, "UPDATE test SET x=10.0 WHERE x=1.0")?;
    run_query_on_row(&tmp_db, &conn, "SELECT * from test WHERE x=10.0", |row| {
        assert_eq!(row.get::<f64>(0).unwrap(), 10.0);
    })?;

    let mut count_1 = 0;
    let mut count_10 = 0;
    run_query_on_row(&tmp_db, &conn, "SELECT * from test", |row| {
        let v = row.get::<f64>(0).unwrap();
        if v == 1.0 {
            count_1 += 1;
        } else if v == 10.0 {
            count_10 += 1;
        }
    })?;
    assert_eq!(count_1, 0, "1.0 shouldn't be inside table");
    assert_eq!(count_10, 1, "10.0 should have existed");

    Ok(())
}

#[test]
fn test_delete_with_index() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    maybe_setup_tracing();

    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (x UNIQUE)", true);
    let conn = tmp_db.connect_limbo();

    run_query(&tmp_db, &conn, "INSERT INTO t VALUES (1), (2)")?;
    run_query(&tmp_db, &conn, "DELETE FROM t WHERE x >= 1")?;

    run_query_on_row(&tmp_db, &conn, "SELECT * FROM t", |_| {
        panic!("Delete should've deleted every row!");
    })?;

    Ok(())
}

#[test]
fn test_update_regression() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE imaginative_baroja (blithesome_hall BLOB,remarkable_lester INTEGER,generous_balagun TEXT,ample_earth INTEGER,marvelous_khadzhiev BLOB,glowing_parissi TEXT,insightful_ryner BLOB)", false);
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO imaginative_baroja VALUES (X'617070726F61636861626C655F6F6D6164', 5581285929211692372, 'approachable_podur', -4145754929970306534, X'666F72747569746F75735F7368617270', 'sensible_amesly', X'636F6D70657469746976655F6669746368'), (X'6D6972746866756C5F686F6673746565', -8554670009677647372, 'shimmering_modkraftdk', 4993627046425025026, X'636F6E73696465726174655F63616765', 'breathtaking_boggs', X'616D617A696E675F73696D6F6E65'), (X'7669766163696F75735F7363687761727A', 5860599187854155616, 'sparkling_aurora', 3757552048117668067, X'756E697175655F6769617A', 'lovely_leroy', X'68617264776F726B696E675F6D696C6C6572'), (X'677265676172696F75735F7061657065', -488992130149088413, 'focused_brinker', 4503849242092922100, X'66756E6E795F6A616B736963', 'competitive_communications', X'657863656C6C656E745F7873696C656E74'), (X'7374756E6E696E675F74616E6E656E6261756D', -5634782647279946253, 'fabulous_crute', -3978009805517476564, X'72656C617865645F63617272796F7574', 'spellbinding_erkan', X'66756E6E795F646F626273'), (X'696D6167696E61746976655F746F6C6F6B6F6E6E696B6F7661', 4236471363502323025, 'excellent_wolke', 7606168469334609395, X'736C65656B5F6D6361666565', 'magnificent_riley', X'616D6961626C655F706173736164616B6973'), (X'77696C6C696E675F736872657665', 5048296470820985219, 'ambitious_jeppesen', 6961857167361512834, X'70617469656E745F6272696E6B6572', 'giving_kramm', X'726573706F6E7369626C655F7363686D696474'), (X'73656E7369626C655F6D757865726573', -5519194136843846790, 'frank_ruggero', 4354855935194921345, X'76697669645F63617365', 'focused_lovecruft', X'6D61676E69666963656E745F736B79')")?;
    conn.execute("DELETE FROM imaginative_baroja WHERE + 4993627046425025026 AND imaginative_baroja.ample_earth < 7479543205763713093")?;
    conn.execute("DELETE FROM imaginative_baroja WHERE imaginative_baroja.glowing_parissi < 'focused_lovebvtww' OR imaginative_baroja.remarkable_lester > 4151587545396021981")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'7175616C69666965645F6672616E6B73', -8502354892194147735, 'devoted_radioazioneitaly', -3157395452089373042, X'6C757374726F75735F7A6170617461', 'alluring_correa', X'616772656561626C655F616465616E65'), (X'6F75747374616E64696E675F67757373656C7370726F757473', 620235519350935371, 'mirthful_feeney', 1889409447941811348, X'746563686E6F6C6F676963616C5F696E636F6E74726F6C61646F73', 'stellar_maddock', X'68696C6172696F75735F72696F74657273')")?;
    conn.execute("DELETE FROM imaginative_baroja WHERE 1")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'7175616C69666965645F616E617263686F', 3711431168910703367, 'nice_steenwyk', -5575264720768661804, X'6272696768745F647265616D73', 'elegant_wong', X'6D6972746866756C5F617672696368'), (X'66756E6E795F6D61726B7573736F6E', 7214748853132812681, 'relaxed_onken', 3496713790694683093, X'73696E636572655F706572726F6E', 'engrossing_urdanibia', X'63726561746976655F6D6361666565'), (X'72656C617865645F6E6163686965', 7272714609462898177, 'organized_submedia', -6429535555778013200, X'7175616C69666965645F67757275', 'flexible_anarchosyndicalists', X'61646570745F726162696E')")?;
    conn.execute("DELETE FROM imaginative_baroja WHERE imaginative_baroja.generous_balagun != 'relaxed_onken' OR imaginative_baroja.glowing_parissi != 'engrossing_urdanibia' OR + x'73696e636572655f706572726f6e'")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'68617264776F726B696E675F6F6E6B656E', 1522407664853556224, 'unique_leier', -6677767486983706611, X'656666696369656E745F676F6465736B79', 'inquisitive_kid', X'726F6D616E7469635F6E65636861796576'), (X'70657273697374656E745F776F6C6D616E', -1829188030856108483, 'adaptable_buelinckx', 8146398066550207891, X'63726561746976655F726F62626965', 'creative_pappenheim', X'68656C7066756C5F6C6974746C65'), (X'676C65616D696E675F6D65616E73', -8661249255762850356, 'devoted_aktimon', -3038215166199543107, X'61646570745F636C617373', 'glimmering_lester', X'6C696B61626C655F6861747A696D696368656C616B6973'), (X'696E7175697369746976655F737079726F', -7773009311699661291, 'efficient_abc', -8324641888573037031, X'6F7074696D69737469635F7368616E6E6F6E', 'captivating_collaboration', X'7368696D6D6572696E675F6F6E66726179'), (X'706572666563745F63616C6C6573', 7360121425124953605, 'warmhearted_misein', 4695817530110433811, X'6F75747374616E64696E675F6B68616E', 'unique_winn', X'73706563746163756C61725F726F6E616E'), (X'657863656C6C656E745F686170676F6F64', 7259914082435634276, 'humorous_karabulut', 3999775492175586928, X'706F6C6974655F6D616E', 'considerate_sk', X'7477696E6B6C696E675F726562656C617A65'), (X'696E646570656E64656E745F7368696675', 8271095316761542146, 'charming_kemsky', -767005917540244139, X'73696E636572655F636F6C736F6E', 'fearless_preti', X'626F756E746966756C5F6D6F6F6E')")?;
    conn.execute("UPDATE imaginative_baroja SET blithesome_hall = X'676C6F77696E675F66656465726174696F6E', remarkable_lester = 78166371321618711 WHERE imaginative_baroja.marvelous_khadzhiev != x'63726561746976655f726f62626965'")?;
    conn.execute("UPDATE imaginative_baroja SET marvelous_khadzhiev = X'67656E65726F75735F67757275', insightful_ryner = X'726F6D616E7469635F67726179', generous_balagun = 'confident_vaillant', remarkable_lester = 4586287216842233067, ample_earth = -9099714189111535232, blithesome_hall = X'666162756C6F75735F6E6573656C6265726773' WHERE + 'unique_leier'")?;
    conn.execute("UPDATE imaginative_baroja SET blithesome_hall = X'73696E636572655F616E74696B616C79707365', marvelous_khadzhiev = X'68617264776F726B696E675F676172736F6E' WHERE imaginative_baroja.ample_earth = 3999775492175586928")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'666F72747569746F75735F73696E6768', -7302726019836481969, 'plucky_pino', -6045217642689709782, X'70726F706974696F75735F6469736F62656469656E6365', 'adept_fiala', X'73706C656E6469645F6D6178696D696C69656E6E65'), (X'726F6D616E7469635F626C616E717569', -2567040995111530104, 'splendid_feyerabend', -5479596969655918138, X'746563686E6F6C6F676963616C5F616C6F6E61', 'warmhearted_coombs', X'64657465726D696E65645F636F6174696D756E6469'), (X'617765736F6D655F6C7574616C6F', 1563154887922664909, 'sensible_biehl', -1320904457282326747, X'636F6E73696465726174655F6469736F7264696E65', 'vibrant_bachmann', X'7761726D686561727465645F706F7374696C6C6F6E'), (X'7570626561745F6B68616E', 6806968793733132762, 'vibrant_katie', 5821721925410414073, X'6469706C6F6D617469635F7374616D61746F76', 'vivid_davidneel', X'68617264776F726B696E675F696C6C6567616C')")?;
    conn.execute("UPDATE imaginative_baroja SET ample_earth = -8563593492347847545, blithesome_hall = X'6D6F76696E675F6672616E6B', remarkable_lester = -8596517228606625242 WHERE - x'666f72747569746f75735f73696e6768'")?;
    conn.execute("UPDATE imaginative_baroja SET generous_balagun = 'relaxed_annwen' WHERE imaginative_baroja.insightful_ryner != x'73706c656e6469645f6d6178696d696c69656e6e65' OR ~ x'70726f706974696f75735f6469736f62656469656e6365' OR NOT -7302726019836481969")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'64696C6967656E745F62756665', -8644677780795594594, 'relaxed_yassour', 2154981713465610316, X'656C6567616E745F6261676E696E69', 'focused_stephens', X'696E7175697369746976655F6672616E6B')")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'657863656C6C656E745F6B61666B61', -5060975004149748093, 'elegant_lester', -5166536099657289280, X'6D61676E69666963656E745F67616C6172696173', 'determined_rayne', X'68656C7066756C5F676174746F'), (X'696E646570656E64656E745F776F6C6C73746F6E656372616674', -8116489392609341652, 'glittering_again', 679309322715420034, X'6672616E6B5F6F66666C6579', 'marvelous_libcomorg', X'68656C7066756C5F68616D6D6572'), (X'706572666563745F6D6167', -4559256564578676261, 'loyal_wallis', -708675214896422276, X'667269656E646C795F65676F756D656E69646573', 'proficient_seattle', X'6661766F7261626C655F6D696B6861696C61'), (X'666F72747569746F75735F776172', -7959338984493715124, 'passionate_moore', 5463161019106938682, X'617765736F6D655F7370726F6E73656E', 'independent_hapgood', X'6F7267616E697A65645F6372757465'), (X'706572666563745F636F6C6C696E73', 7472118541178423654, 'plucky_sansom', -5121277435048512005, X'6C6F79616C5F736172616D6269', 'plucky_conspiracy', X'656E6368616E74696E675F6A72'), (X'616D706C655F63617374726F', -966625795137736376, 'twinkling_argenti', 6935584934702807686, X'7A65737466756C5F7A6162616C617A61', 'technological_emmanuel', X'76697669645F77617272656E')")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'656E657267657469635F7369646577696E646572', -3370178418234314400, 'splendid_energy', -4675020833755082958, X'7175616C69666965645F6368726973', 'approachable_thoreau', X'6C6F79616C5F6361727269636F'), (X'626F756E746966756C5F73756D6D6572737065616B6572', 6251465204820819258, 'amazing_foote', -5380234567579397830, X'6D6F76696E675F6C6A75626C6A616E61', 'likable_forum', X'6F7267616E697A65645F68656E647269636B73'), (X'6B696E645F64757075697364657269', -1040761958618564185, 'confident_schwitzguebel', 1869566572012257848, X'676C697374656E696E675F6B6F6C6C656B74697661', 'wondrous_drinnon', X'656E657267657469635F6D617272696F7474'), (X'696E7175697369746976655F6B6861647A68696576', -5094521018405096184, 'rousing_sanna', 6255059584605476746, X'6D61676E69666963656E745F686576616C', 'elegant_porcu', X'76696272616E745F636F6E7363696F75736E657373'), (X'6C6F79616C5F6D616E', 8475959159308381944, 'mirthful_qruz', 444512543603270133, X'726F7573696E675F6D6F7265', 'rousing_doesburg', X'6C6F79616C5F6B726F6C69636B'), (X'6C6F76656C795F6172746E6F6F7365', -1434910520873907411, 'vivid_bezboznik', 8543492166239130812, X'676C6F77696E675F63756E6E696E6768616D', 'honest_aversion', X'706C75636B795F626C61636B'), (X'6272696768745F636C65797265', -5413751288832062094, 'fearless_bertolo', 288736393203961657, X'6C757374726F75735F626F6F6B73', 'capable_reducto', X'616666656374696F6E6174655F626579657261726E6573656E')")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F505152535455565758595A4142434445464748494A4B4C4D4E4F', -6538633566212356965, 'sensible_dzhermanova', -5013305982875876373, X'726F7573696E675F6D6F72616E', 'gorgeous_mahe', X'616461707461626C655F6D63626172726F6E'), (X'696E646570656E64656E745F77616C7368', 2373576288486988936, 'remarkable_xander', 9043020823585813796, X'65647563617465645F646566656E7365', 'wondrous_york', X'617765736F6D655F757A63617465677569'), (X'616666656374696F6E6174655F6D6178696D6F76', -8102170227516331126, 'efficient_rovescio', 4962011438281918227, X'73656E7369626C655F646F6E676861696C65', 'agreeable_enough', X'73757065725F6D656E75636B'), (X'7374656C6C61725F6D61726C6F77', 6978218807511546063, 'shimmering_porcu', 8540751178074621417, X'62726176655F626165636B6572', 'persistent_anarcocomunista', X'616461707461626C655F76696E61677265'), (X'676C65616D696E675F706865627573', 6873699886132450704, 'passionate_prudhommeaux', -1170262624251312696, X'706F77657266756C5F61706174726973', 'dynamic_frank', X'636F75726167656F75735F6261676E696E69'), (X'616D617A696E675F6D65616E73', 8231150278054386766, 'approachable_cospito', 573571915150962594, X'62726176655F6D6F746865726675636B657273', 'propitious_whittenbergjames', X'656E676167696E675F7368616E747A')")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'62726176655F636173746F726961646973', 9085720273291510174, 'technological_proll', -1139663847356190693, X'72656D61726B61626C655F6C6C', 'imaginative_podshivalov', X'706572736F6E61626C655F616A697468'), (X'76696272616E745F73796E646963617465', 8935351016196288445, 'gleaming_moon', 6958293133342356700, X'616D626974696F75735F736368617069726F', 'relaxed_filippi', X'73656E7369626C655F6C6F69646C'), (X'66756E6E795F626572746F6C6F', -5017184992763589138, 'diplomatic_krivokapic', 1667759670464135761, X'676C6F77696E675F6275726B6F7769637A', 'honest_stetner', X'6C6F79616C5F6D726163686E696B'), (X'696E736967687466756C5F647261676F6E6F776C', 1488426002938705435, 'spellbinding_garon', 5191104452852206805, X'68696C6172696F75735F6D6174746973', 'hardworking_sindikalis', X'6F7267616E697A65645F6261747469737475747461'), (X'6772697070696E675F7375687579696E69', -8376653359418493563, 'nice_mompo', 3514360837619983832, X'76696272616E745F676C6176696E', 'friendly_kanavalchyk', X'6E6963655F6F767368696E736B79'), (X'6C6F76696E675F626576696E67746F6E', -7422834762102813077, 'romantic_petrossiants', -8491442514406092983, X'656666696369656E745F6576616E73', 'generous_osterweil', X'636F6E666964656E745F6B6C65697374'), (X'70726F647563746976655F7661696C6C616E74', 896408500793061951, 'generous_ackermann', 130835466244995464, X'706F6C6974655F6368617264726F6E6E6574', 'stellar_schwarz', X'696E646570656E64656E745F61646F6E696465'), (X'706F6C6974655F7975', 4360997161952352650, 'energetic_tolhildan', -682828720232496740, X'676C697374656E696E675F76656E657A75656C61', 'sparkling_weyde', X'676C697374656E696E675F64616E746F6E')")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'666561726C6573735F7065746974', -3373019397059388839, 'glowing_orourke', 7332167962870327833, X'726F7573696E675F73687569656E', 'relaxed_vargas', X'7368696D6D6572696E675F6A656666726579'), (X'667269656E646C795F666F6C6C6D616E6E', -6204768702459621155, 'persistent_matthews', 6978093617476526230, X'736C65656B5F6E657A756D69', 'marvelous_cells', X'706F6C6974655F657272616E646F6E6561'), (X'6465766F7465645F666162627269', 4074143365978872958, 'spectacular_gang', 4467197932821142344, X'616666656374696F6E6174655F766172696F7573', 'awesome_ridge', X'756E697175655F77696C736F6E'), (X'7374756E6E696E675F626568656D6F7468', -5834234981048794280, 'kind_calvert', -5094948883894460956, X'64696C6967656E745F72616E7375', 'glistening_crone', X'6C6F76656C795F7A757A656E6B6F'), (X'7374656C6C61725F636F6F7065726174697665', 3860853067725331313, 'gregarious_man', 4029075166239237009, X'70657273697374656E745F6669637469636961', 'imaginative_pesotta', X'70726F66696369656E745F646973636F72646961'), (X'73757065725F636C6F766572', -3206050709826278770, 'brilliant_moai', -3400202004314548573, X'6F7074696D69737469635F70696E6F', 'stupendous_fitch', X'7368696D6D6572696E675F6D75727461756768'), (X'73747570656E646F75735F6172656E646172656E6B6F', -1865659981961892550, 'faithful_hiatt', -1498572117664460662, X'70726F647563746976655F736B7262696E61', 'affectionate_lefrancais', X'7068696C6F736F70686963616C5F62756C6761726961')")?;
    conn.execute("INSERT INTO imaginative_baroja VALUES (X'68617264776F726B696E675F736B726F7A6974736B79', 3573795234674258645, 'romantic_read', -8077515003292265992, X'70726F647563746976655F6F6666656E686172747A', 'nice_coull', X'62726176655F6D63616F696468'), (X'666561726C6573735F646573747275637461626C6573', -3380052750674515649, 'relaxed_witkoprocker', 634237228496795928, X'73757065725F6D656E', 'brave_walt', X'66616E7461737469635F736E79646572'), (X'616D617A696E675F7363687769747A67756562656C', -5978347381755596696, 'breathtaking_hostis', 4835657367129258513, X'70726F706974696F75735F636F686E62656E646974', 'ample_tempetes', X'656E67726F7373696E675F6C6176696E'), (X'6F7267616E697A65645F796F757468', -8604111030632737021, 'thoughtful_goodman', -2275866164080461133, X'656E6368616E74696E675F6B756D706572', 'helpful_chomsky', X'656E676167696E675F61626973736F6E696368696C69737461')")?;
    conn.execute("UPDATE imaginative_baroja SET ample_earth = -7099009285992304294, remarkable_lester = 7860481406646607706, blithesome_hall = X'636F6D70657469746976655F736F6369657479', glowing_parissi = 'captivating_dreams', insightful_ryner = X'61646570745F6B6F7A6172656B' WHERE 1")?;

    check_integrity_is_ok(tmp_db, conn)?;

    Ok(())
}

#[test]
fn test_multiple_statements() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (x)", false);
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO t values(1); insert into t values(2);")?;

    run_query_on_row(&tmp_db, &conn, "SELECT count(1) from t;", |row| {
        let count = row.get::<i64>(0).unwrap();
        assert_eq!(count, 2);
    })
    .unwrap();

    Ok(())
}

fn check_integrity_is_ok(tmp_db: TempDatabase, conn: Arc<Connection>) -> Result<(), anyhow::Error> {
    run_query_on_row(&tmp_db, &conn, "pragma integrity_check", |row: &Row| {
        let res = row.get::<String>(0).unwrap();
        assert!(res.contains("ok"));
    })?;
    Ok(())
}

enum ConnectionState {
    PrepareQuery {
        query_idx: usize,
    },
    ExecuteQuery {
        query_idx: usize,
        stmt: Box<Statement>,
    },
    Done,
}

struct ConnectionPlan {
    queries: Vec<String>,
    conn: Arc<Connection>,
    state: ConnectionState,
}

impl ConnectionPlan {
    pub fn step(&mut self) -> anyhow::Result<bool> {
        loop {
            match &mut self.state {
                ConnectionState::PrepareQuery { query_idx } => {
                    if *query_idx >= self.queries.len() {
                        self.state = ConnectionState::Done;
                        return Ok(true);
                    }
                    let query = &self.queries[*query_idx];
                    tracing::info!("preparing {}", query);
                    let stmt = Box::new(self.conn.query(query)?.unwrap());
                    self.state = ConnectionState::ExecuteQuery {
                        query_idx: *query_idx,
                        stmt,
                    };
                }
                ConnectionState::ExecuteQuery { stmt, query_idx } => loop {
                    let query = &self.queries[*query_idx];
                    tracing::info!("stepping {}", query);
                    let current_query_idx = *query_idx;
                    let step_result = stmt.step()?;
                    match step_result {
                        StepResult::IO => {
                            return Ok(false);
                        }
                        StepResult::Done => {
                            change_state!(&mut self.state, ConnectionState::ExecuteQuery { .. } => ConnectionState::PrepareQuery { query_idx: current_query_idx + 1 });
                            return Ok(false);
                        }
                        StepResult::Row => {}
                        StepResult::Busy => {
                            return Ok(false);
                        }
                        _ => unreachable!(),
                    }
                },
                ConnectionState::Done => {
                    return Ok(true);
                }
            }
        }
    }

    pub fn is_finished(&self) -> bool {
        matches!(self.state, ConnectionState::Done)
    }
}

#[test]
fn test_write_concurrent_connections() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    maybe_setup_tracing();

    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (x)", false);
    let num_connections = 4;
    let num_inserts_per_connection = 100;
    let mut connections = vec![];
    for connection_idx in 0..num_connections {
        let conn = tmp_db.connect_limbo();
        let mut queries = Vec::with_capacity(num_inserts_per_connection);
        for query_idx in 0..num_inserts_per_connection {
            queries.push(format!(
                "INSERT INTO t VALUES({})",
                (connection_idx * num_inserts_per_connection) + query_idx
            ));
        }
        connections.push(ConnectionPlan {
            queries,
            conn,
            state: ConnectionState::PrepareQuery { query_idx: 0 },
        });
    }

    let mut connections_finished = 0;
    while connections_finished != num_connections {
        for conn in &mut connections {
            if conn.is_finished() {
                continue;
            }
            let finished = conn.step()?;
            if finished {
                connections_finished += 1;
            }
        }
    }

    let conn = tmp_db.connect_limbo();
    run_query_on_row(&tmp_db, &conn, "SELECT count(1) from t", |row: &Row| {
        let count = row.get::<i64>(0).unwrap();
        assert_eq!(
            count,
            (num_connections * num_inserts_per_connection) as i64,
            "received wrong number of rows"
        );
    })?;

    Ok(())
}

#[test]
fn test_wal_bad_frame() -> anyhow::Result<()> {
    maybe_setup_tracing();
    let _ = env_logger::try_init();
    let db_path = {
        let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t1 (x)", false);
        let db_path = tmp_db.path.clone();
        let conn = tmp_db.connect_limbo();
        conn.execute("BEGIN")?;
        conn.execute("CREATE TABLE t2 (x)")?;
        conn.execute("CREATE TABLE t3 (x)")?;
        conn.execute("INSERT INTO t2(x) VALUES (1)")?;
        conn.execute("INSERT INTO t3(x) VALUES (1)")?;
        conn.execute("COMMIT")?;
        run_query_on_row(&tmp_db, &conn, "SELECT count(1) from t2", |row| {
            let x = row.get::<i64>(0).unwrap();
            assert_eq!(x, 1);
        })
        .unwrap();
        run_query_on_row(&tmp_db, &conn, "SELECT count(1) from t3", |row| {
            let x = row.get::<i64>(0).unwrap();
            assert_eq!(x, 1);
        })
        .unwrap();
        // Now let's modify last frame record
        let path = tmp_db.path.clone();
        let path = path.with_extension("db-wal");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let offset = WAL_HEADER_SIZE + (WAL_FRAME_HEADER_SIZE + 4096) * 2;
        let mut buf = [0u8; WAL_FRAME_HEADER_SIZE];
        file.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        file.read_exact(&mut buf).unwrap();
        dbg!(&buf);
        let db_size = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        dbg!(offset);
        assert_eq!(db_size, 4);
        // let's overwrite size_after to be 0 so that we think transaction never finished
        buf[4..8].copy_from_slice(&[0, 0, 0, 0]);
        file.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(&buf).unwrap();
        file.flush().unwrap();

        db_path
    };
    {
        let result = {
            let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
            let db = Database::open_file_with_flags(
                io.clone(),
                db_path.to_str().unwrap(),
                turso_core::OpenFlags::default(),
                false,
                false,
            )
            .unwrap();
            let tmp_db = TempDatabase {
                path: db_path,
                io,
                db,
            };
            let conn = tmp_db.connect_limbo();
            run_query_on_row(&tmp_db, &conn, "SELECT count(1) from t2", |row| {
                let x = row.get::<i64>(0).unwrap();
                assert_eq!(x, 0);
            })
        };

        match result {
            Err(error) => {
                dbg!(&error);
                let panic_msg = error.downcast_ref::<LimboError>().unwrap();
                let msg = match panic_msg {
                    LimboError::ParseError(message) => message,
                    _ => panic!("Unexpected panic message: {panic_msg}"),
                };

                assert!(
                    msg.contains("no such table: t2"),
                    "Expected panic message not found. Got: {msg}"
                );
            }
            Ok(_) => panic!("Expected query to panic, but it succeeded"),
        }
    }

    Ok(())
}

#[test]
fn test_read_wal_dumb_no_frames() -> anyhow::Result<()> {
    maybe_setup_tracing();
    let _ = env_logger::try_init();
    let db_path = {
        let tmp_db = TempDatabase::new_empty(false);
        let conn = tmp_db.connect_limbo();
        conn.close()?;
        tmp_db.path.clone()
    };
    // Second connection must recover from the WAL file. Last checksum should be filled correctly.
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path, false);
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE t0 (x)")?;
        conn.close()?;
    }
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path, false);
        let conn = tmp_db.connect_limbo();
        conn.execute("INSERT INTO t0(x) VALUES (1)")?;
        conn.close()?;
    }

    Ok(())
}

pub fn run_query(tmp_db: &TempDatabase, conn: &Arc<Connection>, query: &str) -> anyhow::Result<()> {
    run_query_core(tmp_db, conn, query, None::<fn(&Row)>)
}

pub fn run_query_on_row(
    tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
    query: &str,
    on_row: impl FnMut(&Row),
) -> anyhow::Result<()> {
    run_query_core(tmp_db, conn, query, Some(on_row))
}

pub fn run_query_core(
    _tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
    query: &str,
    mut on_row: Option<impl FnMut(&Row)>,
) -> anyhow::Result<()> {
    if let Some(ref mut rows) = conn.query(query)? {
        loop {
            match rows.step()? {
                StepResult::IO => {
                    rows.run_once()?;
                }
                StepResult::Done => break,
                StepResult::Row => {
                    if let Some(on_row) = on_row.as_mut() {
                        let row = rows.row().unwrap();
                        on_row(row)
                    }
                }
                _ => unreachable!(),
            }
        }
    };
    Ok(())
}
