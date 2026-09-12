use super::*;
use crate::alloc::vec;
use crate::translate::collate::CollationSeq;
use crate::vdbe::BranchOffset;
use crate::SqliteDialect;
use crate::{Database, DatabaseOpts, MemoryIO, IO};

fn prepare_test_statement() -> Statement {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.prepare("SELECT 1;").unwrap()
}

fn make_spilled_hash_table() -> (HashTable, crate::alloc::Vec<Value>, usize) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        ..Default::default()
    };
    let mut ht = HashTable::new(config, io).unwrap();

    for i in 0..1024 {
        match ht
            .insert(vec![Value::from_i64(i)], i, vec![], None)
            .unwrap()
        {
            IOResult::Done(()) => {}
            IOResult::IO(_) => panic!("memory IO should complete synchronously"),
        }
    }

    match ht.finalize_build(None).unwrap() {
        IOResult::Done(()) => {}
        IOResult::IO(_) => panic!("memory IO should complete synchronously"),
    }
    assert!(ht.has_spilled(), "test requires spilled hash table");

    let probe_key = (0..1024)
        .map(|i| vec![Value::from_i64(i)])
        .find(|key| {
            let partition_idx = ht.partition_for_keys(key).unwrap();
            !ht.is_partition_loaded(partition_idx)
        })
        .expect("expected an unloaded spilled partition");
    let partition_idx = ht.partition_for_keys(&probe_key).unwrap();

    (ht, probe_key, partition_idx)
}

/// test to check that vacuum into connection state is reset if it is
/// interrupted mid way
#[test]
fn test_vacuum_into_busy_after_source_begin_rolls_back_source_txn() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    let source_txn_progress_calls = Arc::new(AtomicUsize::new(0));
    let did_interrupt = Arc::new(AtomicBool::new(false));
    let conn_for_progress = conn.clone();
    let source_txn_progress_calls_for_handler = source_txn_progress_calls.clone();
    let did_interrupt_for_handler = did_interrupt.clone();
    conn.set_progress_handler(
        1,
        Some(Box::new(move || {
            if !conn_for_progress.get_auto_commit() {
                let calls = source_txn_progress_calls_for_handler.fetch_add(1, Ordering::SeqCst);
                calls >= 10 && !did_interrupt_for_handler.swap(true, Ordering::SeqCst)
            } else {
                false
            }
        })),
    );

    let dest_dir = tempfile::TempDir::new().unwrap();
    let dest_path = dest_dir.path().join("busy_vacuum.db");
    let dest_path = dest_path.to_str().expect("temp path should be UTF-8");
    let mut stmt = conn.prepare(format!("VACUUM INTO '{dest_path}'")).unwrap();
    let step = stmt.step().unwrap();
    conn.set_progress_handler(0, None);

    assert!(
        matches!(step, StepResult::Busy),
        "progress interruption inside VACUUM INTO should surface as Busy, got {step:?}"
    );
    assert!(
        source_txn_progress_calls.load(Ordering::SeqCst) > 10,
        "test should interrupt after VACUUM INTO opens the source transaction"
    );
    assert!(
        did_interrupt.load(Ordering::SeqCst),
        "progress handler should have interrupted VACUUM INTO exactly once"
    );
    assert!(
        conn.get_auto_commit(),
        "Busy cleanup should roll back the source transaction before returning"
    );
}

/// same like `test_vacuum_into_busy_after_source_begin_rolls_back_source_txn`
/// but for attached dbs
#[test]
fn test_cleanup_vacuum_into_rolls_back_attached_only_source_txn() {
    let dir = tempfile::TempDir::new().unwrap();
    let main_path = dir.path().join("vacuum-into-cleanup-main.db");
    let attached_path = dir.path().join("vacuum-into-cleanup-attached.db");

    let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        main_path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    conn.execute(format!(
        "ATTACH DATABASE '{}' AS att",
        attached_path.display()
    ))
    .unwrap();
    conn.execute("CREATE TABLE att.t(x)").unwrap();
    conn.execute("INSERT INTO att.t VALUES (1)").unwrap();

    conn.execute("BEGIN").unwrap();
    let attached_db_id = conn.get_database_id_by_name("att").unwrap();
    let attached_pager = conn.get_pager_from_database_index(&attached_db_id).unwrap();
    attached_pager.begin_read_tx().unwrap();

    assert!(
        !conn.pager.load().holds_read_lock(),
        "attached-only cleanup regression requires the main pager to stay lock-free"
    );
    assert!(
        attached_pager.holds_read_lock(),
        "attached source pager should hold the pinned read snapshot"
    );

    let mut state = ProgramState::new(0, 0);
    state.auto_txn_cleanup = TxnCleanup::RollbackTxn;

    cleanup_op_vacuum_into(&conn, &mut state, Box::default()).unwrap();

    assert!(
        conn.get_auto_commit(),
        "cleanup should restore auto-commit without going through SQL ROLLBACK"
    );
    assert_eq!(state.auto_txn_cleanup, TxnCleanup::None);
    assert!(
        !attached_pager.holds_read_lock(),
        "cleanup should release the attached source read snapshot"
    );

    conn.execute("INSERT INTO att.t VALUES (2)").unwrap();
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM att.t").unwrap();
    let mut count = 0_i64;
    stmt.run_with_row_callback(|row| {
        count = row.get(0)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_savepoint_loads_evicted_attached_header_before_mirroring() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        "savepoint-main.db",
        OpenFlags::Create,
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("ATTACH 'savepoint-aux.db' AS aux").unwrap();
    conn.execute("CREATE TABLE aux.t(x)").unwrap();

    let attached_db_id = conn.get_database_id_by_name("aux").unwrap();
    let attached_pager = conn.get_pager_from_database_index(&attached_db_id).unwrap();
    attached_pager.begin_read_tx().unwrap();
    attached_pager.clear_page_cache(false);

    conn.execute("SAVEPOINT s").unwrap();
    conn.execute("RELEASE s").unwrap();
}

#[test]
fn test_in_place_vacuum_succeeds_and_releases_source_locks() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        "in-place-vacuum-design-b.db",
        OpenFlags::Create,
        DatabaseOpts::new().with_vacuum(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    for i in 0..128 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'value-{i}')"))
            .unwrap();
    }
    conn.execute("DELETE FROM t WHERE id % 2 = 0").unwrap();

    conn.execute("VACUUM").unwrap();

    let mut stmt = conn
        .prepare("SELECT count(*), coalesce(sum(id), 0) FROM t")
        .unwrap();
    let mut count = 0_i64;
    let mut sum = 0_i64;
    stmt.run_with_row_callback(|row| {
        count = row.get(0)?;
        sum = row.get(1)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 64);
    assert_eq!(sum, (1..128).step_by(2).sum::<i64>());
    assert!(conn.get_auto_commit());

    let pager = conn.pager.load();
    assert!(!pager.holds_read_lock());
    assert!(!pager.holds_write_lock());
}

#[test]
fn test_in_place_vacuum_busy_before_copyback_restores_source_txn() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        "in-place-vacuum-busy-before-copyback.db",
        OpenFlags::Create,
        DatabaseOpts::new().with_vacuum(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    for i in 0..32 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'value-{i}')"))
            .unwrap();
    }

    let did_interrupt = Arc::new(AtomicBool::new(false));
    let conn_for_progress = conn.clone();
    let did_interrupt_for_handler = did_interrupt.clone();
    conn.set_progress_handler(
        1,
        Some(Box::new(move || {
            !conn_for_progress.get_auto_commit()
                && !did_interrupt_for_handler.swap(true, Ordering::SeqCst)
        })),
    );

    let mut stmt = conn.prepare("VACUUM").unwrap();
    let step = stmt.step().unwrap();
    conn.set_progress_handler(0, None);

    assert!(
        matches!(step, StepResult::Busy),
        "progress interruption inside in-place VACUUM should surface as Busy, got {step:?}"
    );
    assert!(
        did_interrupt.load(Ordering::SeqCst),
        "test should interrupt after in-place VACUUM opens the source snapshot"
    );
    assert!(
        conn.get_auto_commit(),
        "Busy cleanup should restore auto-commit before returning"
    );
    let pager = conn.pager.load();
    assert!(!pager.holds_read_lock());
    assert!(!pager.holds_write_lock());
}

#[test]
fn test_hash_probe_rejects_unloaded_spilled_partition_without_probe_rowid() {
    let stmt = prepare_test_statement();
    let (ht, probe_key, _) = make_spilled_hash_table();

    let mut state = ProgramState::new(2, 0);
    state.hash_tables.insert(7, ht);
    state.set_register(0, Register::Value(probe_key[0].clone()));

    let insn = Insn::HashProbe {
        hash_table_id: 7,
        key_start_reg: 0,
        num_keys: 1,
        dest_reg: 1,
        target_pc: BranchOffset::Offset(99),
        payload_dest_reg: None,
        num_payload: 0,
        probe_rowid_reg: None,
    };

    let err = match op_hash_probe(stmt.get_program(), &mut state, &insn, stmt.get_pager()) {
        Ok(_) => {
            panic!("HashProbe should reject grace-only probing without a loaded partition")
        }
        Err(err) => err,
    };

    assert!(
        matches!(*err, LimboError::InternalError(ref message) if message.contains("probe_rowid_reg=None is grace-only")),
        "unexpected error: {err:?}"
    );
    assert_eq!(state.pc, 0, "pc should not advance on invariant violation");
    assert!(
        state.active_op_state.hash_probe().is_none(),
        "HashProbe should not stash resumable state for the removed fallback path"
    );
}

#[test]
fn test_hash_probe_allows_grace_style_probe_after_partition_preload() {
    let stmt = prepare_test_statement();
    let (mut ht, probe_key, partition_idx) = make_spilled_hash_table();

    loop {
        match ht.load_spilled_partition(partition_idx, None).unwrap() {
            IOResult::Done(()) => break,
            IOResult::IO(_) => continue,
        }
    }
    assert!(
        ht.is_partition_loaded(partition_idx),
        "grace-style probe requires the build partition to be resident"
    );

    let expected_rowid = match &probe_key[0] {
        Value::Numeric(Numeric::Integer(i)) => *i,
        ref other => panic!("expected integer probe key, got {other:?}"),
    };

    let mut state = ProgramState::new(2, 0);
    state.hash_tables.insert(7, ht);
    state.set_register(0, Register::Value(probe_key[0].clone()));

    let insn = Insn::HashProbe {
        hash_table_id: 7,
        key_start_reg: 0,
        num_keys: 1,
        dest_reg: 1,
        target_pc: BranchOffset::Offset(99),
        payload_dest_reg: None,
        num_payload: 0,
        probe_rowid_reg: None,
    };

    let step = op_hash_probe(stmt.get_program(), &mut state, &insn, stmt.get_pager())
        .expect("preloaded grace probe should succeed");
    assert!(matches!(step, InsnFunctionStepResult::Step));
    assert_eq!(state.pc, 1, "matching probe should fall through");
    assert_eq!(
        state.get_register(1).get_value(),
        &Value::from_i64(expected_rowid),
        "HashProbe should return the matching build rowid"
    );
}

#[test]
fn test_decr_jump_zero_non_integer_register_returns_error() {
    let stmt = prepare_test_statement();

    let mut state = ProgramState::new(1, 0);
    state.set_register(0, Register::Value(Value::Text("not-an-int".into())));

    let insn = Insn::DecrJumpZero {
        reg: 0,
        target_pc: crate::vdbe::BranchOffset::Offset(1),
    };

    let err = match op_decr_jump_zero(stmt.get_program(), &mut state, &insn, stmt.get_pager()) {
        Ok(_) => panic!("non-integer register must fail"),
        Err(err) => err,
    };
    assert!(matches!(*err, LimboError::Constraint(message) if message == "datatype mismatch"));
    assert_eq!(state.pc, 0);
}

#[test]
fn test_make_record_overlapping_dest_returns_error() {
    let stmt = prepare_test_statement();

    let mut state = ProgramState::new(3, 0);
    for i in 0..3 {
        state.set_register(i, Register::Value(Value::from_i64(i as i64)));
    }
    let insn = Insn::MakeRecord {
        start_reg: 0,
        count: 3,
        dest_reg: 1,
        index_name: None,
        affinity_str: None,
    };

    let err = match op_make_record(stmt.get_program(), &mut state, &insn, stmt.get_pager()) {
        Ok(_) => panic!("overlapping destination register must fail"),
        Err(err) => err,
    };
    assert!(
        matches!(*err, LimboError::InternalError(ref message) if message.contains("overlaps its source range")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_sorter_data_unpaired_content_register_returns_error() {
    let stmt = prepare_test_statement();

    // Pseudo cursor 1 exposes register 2, but SorterData targets register 3.
    let mut state = ProgramState::new(4, 2);
    state.cursors[1] = Some(Cursor::new_pseudo(crate::pseudo::PseudoCursor::new(2)));
    let insn = Insn::SorterData {
        cursor_id: 0,
        dest_reg: 3,
        pseudo_cursor: 1,
    };

    let err = match op_sorter_data(stmt.get_program(), &mut state, &insn, stmt.get_pager()) {
        Ok(_) => panic!("unpaired content register must fail"),
        Err(err) => err,
    };
    assert!(
        matches!(*err, LimboError::InternalError(ref message) if message.contains("content register")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_execute_sqlite_version() {
    assert_eq!(
        execute_sqlite_version(),
        crate::dialect::sqlite::SQLITE_VERSION
    );
}

#[test]
fn test_execute_turso_version() {
    let version_integer = 3046001;
    let expected = "3.46.1";
    assert_eq!(execute_turso_version(version_integer), expected);
}

#[test]
fn test_ascii_whitespace_is_trimmed() {
    // Regular ASCII whitespace SHOULD be trimmed
    let ascii_whitespace_cases = vec![
        (" 12", 12i64),            // space
        ("12 ", 12i64),            // trailing space
        (" 12 ", 12i64),           // both sides
        ("\t42\t", 42i64),         // tab
        ("\n99\n", 99i64),         // newline
        (" \t\n123\r\n ", 123i64), // mixed ASCII whitespace
        ("\x0b12", 12i64),         // leading vertical tab (0x0B)
        ("12\x0b", 12i64),         // trailing vertical tab (0x0B)
    ];

    for (input, expected_int) in ascii_whitespace_cases {
        let mut register = Register::Value(Value::Text(input.into()));
        apply_affinity_char(&mut register, Affinity::Integer);

        match register {
            Register::Value(Value::Numeric(Numeric::Integer(i))) => {
                assert_eq!(
                    i, expected_int,
                    "String '{input}' should convert to {expected_int}, got {i}"
                );
            }
            other => {
                panic!(
                    "String '{input}' should be converted to integer {expected_int}, got {other:?}"
                );
            }
        }
    }
}

#[test]
fn test_non_breaking_space_not_trimmed() {
    let test_strings = vec![
        ("12\u{00A0}", "text", 3),   // '12' + non-breaking space (3 chars, 4 bytes)
        ("\u{00A0}12", "text", 3),   // non-breaking space + '12' (3 chars, 4 bytes)
        ("12\u{00A0}34", "text", 5), // '12' + nbsp + '34' (5 chars, 6 bytes)
    ];

    for (input, _expected_type, expected_len) in test_strings {
        let mut register = Register::Value(Value::Text(input.into()));
        apply_affinity_char(&mut register, Affinity::Integer);

        match register {
            Register::Value(Value::Text(t)) => {
                assert_eq!(
                    t.as_str().chars().count(),
                    expected_len,
                    "String '{input}' should have {expected_len} characters",
                );
            }
            Register::Value(Value::Numeric(Numeric::Integer(_))) => {
                panic!("String '{input}' should NOT be converted to integer");
            }
            other => panic!("Unexpected value type: {other:?}"),
        }
    }
}

#[test]
fn test_affinity_keeps_nan_inf_text() {
    let cases = ["nan", "inf"];

    for input in cases {
        let mut register = Register::Value(Value::Text(input.into()));
        apply_affinity_char(&mut register, Affinity::Integer);
        match register {
            Register::Value(Value::Text(t)) => {
                assert_eq!(t.as_str(), input, "Unexpected conversion for '{input}'");
            }
            other => {
                panic!("'{input}' should remain text, got {other:?}");
            }
        }

        let mut register = Register::Value(Value::Text(input.into()));
        apply_affinity_char(&mut register, Affinity::Numeric);
        match register {
            Register::Value(Value::Text(t)) => {
                assert_eq!(t.as_str(), input, "Unexpected conversion for '{input}'");
            }
            other => {
                panic!("'{input}' should remain text, got {other:?}");
            }
        }
    }
}

#[test]
fn test_init_agg_payload_reserves_exact_capacity() {
    let funcs = [
        AggFunc::Count,
        AggFunc::Count0,
        AggFunc::Sum,
        AggFunc::Total,
        AggFunc::Avg,
        AggFunc::Min,
        AggFunc::Max,
        AggFunc::GroupConcat,
        AggFunc::StringAgg,
        AggFunc::ArrayAgg,
        AggFunc::Mode,
        AggFunc::PercentileCont,
        AggFunc::PercentileDisc,
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject,
        #[cfg(feature = "json")]
        AggFunc::JsonbGroupObject,
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray,
        #[cfg(feature = "json")]
        AggFunc::JsonbGroupArray,
    ];
    for func in funcs {
        let mut payload = crate::alloc::vec![];
        init_agg_payload(&func, &mut payload).unwrap();
        assert_eq!(payload.capacity(), payload.len(), "{func:?}");
    }
}

#[test]
fn test_init_agg_payload_count() {
    let mut payload = crate::alloc::vec![];
    init_agg_payload(&AggFunc::Count, &mut payload).unwrap();
    assert_eq!(payload.len(), 1);
    assert_eq!(payload[0], Value::from_i64(0));
}

#[test]
fn test_init_agg_payload_sum() {
    let mut payload = crate::alloc::vec![];
    init_agg_payload(&AggFunc::Sum, &mut payload).unwrap();
    assert_eq!(payload.len(), 5);
    assert_eq!(payload[0], Value::Null); // acc
    assert_eq!(payload[1], Value::from_f64(0.0)); // r_err
    assert_eq!(payload[2], Value::from_i64(0)); // approx
    assert_eq!(payload[3], Value::from_i64(0)); // ovrfl
    assert_eq!(payload[4], Value::from_i64(0)); // count
}

#[test]
fn test_init_agg_payload_avg() {
    let mut payload = crate::alloc::vec![];
    init_agg_payload(&AggFunc::Avg, &mut payload).unwrap();
    assert_eq!(payload.len(), 3);
    assert_eq!(payload[0], Value::from_f64(0.0)); // sum
    assert_eq!(payload[1], Value::from_f64(0.0)); // r_err
    assert_eq!(payload[2], Value::from_i64(0)); // count
}

#[test]
fn test_update_count_skips_null() {
    let mut payload = crate::alloc::vec![Value::from_i64(5)];
    update_agg_payload(
        &AggFunc::Count,
        &Value::Null,
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_i64(5)); // unchanged
}

#[test]
fn test_update_count_increments() {
    let mut payload = crate::alloc::vec![Value::from_i64(5)];
    update_agg_payload(
        &AggFunc::Count,
        &Value::from_i64(42),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_i64(6));
}

#[test]
fn test_update_sum_integers() {
    let mut payload = crate::alloc::vec![
        Value::Null,
        Value::from_f64(0.0),
        Value::from_i64(0),
        Value::from_i64(0),
        Value::from_i64(0),
    ];
    update_agg_payload(
        &AggFunc::Sum,
        &Value::from_i64(10),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_i64(10));

    update_agg_payload(
        &AggFunc::Sum,
        &Value::from_i64(5),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_i64(15));
}

#[test]
fn test_update_sum_null_is_skipped() {
    let mut payload = crate::alloc::vec![
        Value::from_i64(10),
        Value::from_f64(0.0),
        Value::from_i64(0),
        Value::from_i64(0),
        Value::from_i64(1),
    ];
    update_agg_payload(
        &AggFunc::Sum,
        &Value::Null,
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_i64(10)); // unchanged
}

#[test]
fn test_update_min_max() {
    let mut payload = crate::alloc::vec![Value::Null];
    // First value sets the min/max
    update_agg_payload(
        &AggFunc::Min,
        &Value::from_i64(5),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_i64(5));

    // Smaller value updates min
    update_agg_payload(
        &AggFunc::Min,
        &Value::from_i64(3),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_i64(3));

    // Larger value doesn't update min
    update_agg_payload(
        &AggFunc::Min,
        &Value::from_i64(10),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_i64(3));
}

#[test]
fn test_update_avg() {
    // Payload: [sum, r_err, count]
    let mut payload = crate::alloc::vec![
        Value::from_f64(0.0),
        Value::from_f64(0.0),
        Value::from_i64(0),
    ];
    update_agg_payload(
        &AggFunc::Avg,
        &Value::from_i64(10),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_f64(10.0));
    assert_eq!(payload[2], Value::from_i64(1));

    update_agg_payload(
        &AggFunc::Avg,
        &Value::from_i64(20),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    assert_eq!(payload[0], Value::from_f64(30.0));
    assert_eq!(payload[2], Value::from_i64(2));
}

#[test]
fn test_finalize_count() {
    let payload = vec![Value::from_i64(42)];
    let result = finalize_agg_payload(&AggFunc::Count, &payload).unwrap();
    assert_eq!(result, Value::from_i64(42));
}

#[test]
fn test_finalize_avg() {
    // Payload: [sum, r_err, count]
    let payload = vec![
        Value::from_f64(30.0),
        Value::from_f64(0.0),
        Value::from_i64(3),
    ];
    let result = finalize_agg_payload(&AggFunc::Avg, &payload).unwrap();
    assert_eq!(result, Value::from_f64(10.0));
}

#[test]
fn test_finalize_avg_empty() {
    // Payload: [sum, r_err, count]
    let payload = vec![
        Value::from_f64(0.0),
        Value::from_f64(0.0),
        Value::from_i64(0),
    ];
    let result = finalize_agg_payload(&AggFunc::Avg, &payload).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn test_finalize_avg_large_integers() {
    let mut payload = crate::alloc::vec![
        Value::from_f64(0.0),
        Value::from_f64(0.0),
        Value::from_i64(0),
    ];

    update_agg_payload(
        &AggFunc::Avg,
        &Value::from_i64(9007199254740994),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();
    update_agg_payload(
        &AggFunc::Avg,
        &Value::from_i64(-9007199254740993),
        None,
        &mut payload,
        CollationSeq::Binary,
        || Ok(None),
    )
    .unwrap();

    let result = finalize_agg_payload(&AggFunc::Avg, &payload).unwrap();
    assert_eq!(result, Value::from_f64(0.5));
}

#[test]
fn test_array_agg_accumulates_correctly() {
    // Verify that array_agg produces correct results when accumulating
    // multiple values. Uses the direct payload approach (O(1) per row).
    let mut payload = crate::alloc::vec![];
    init_agg_payload(&AggFunc::ArrayAgg, &mut payload).unwrap();

    // Simulate how AggStep accumulates values directly into the payload Vec.
    for i in 0..100 {
        let count = payload[0].as_int().unwrap_or(0) as usize;
        payload[0] = Value::from_i64((count + 1) as i64);
        payload.push(Value::from_i64(i));
    }

    let result = finalize_agg_payload(&AggFunc::ArrayAgg, &payload).unwrap();
    let blob = match &result {
        Value::Blob(b) => b,
        _ => panic!("Expected Blob, got {result:?}"),
    };
    let elements = array_values_from_blob(blob).unwrap();
    assert_eq!(elements.len(), 100);
    for (i, elem) in elements.iter().enumerate() {
        assert_eq!(*elem, Value::from_i64(i as i64));
    }
}

#[test]
fn test_array_agg_zero_rows_produces_valid_result() {
    // array_agg with zero rows should return NULL, matching PostgreSQL.
    // The result must not be an invalid empty blob that crashes on decode.
    let mut payload = crate::alloc::vec![];
    init_agg_payload(&AggFunc::ArrayAgg, &mut payload).unwrap();
    // No values accumulated — count stays 0.
    let result = finalize_agg_payload(&AggFunc::ArrayAgg, &payload).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn test_array_agg_finalize_bounds_check() {
    // If payload[0] count is larger than the actual payload length,
    // finalize should return an error rather than panicking.
    let payload = vec![Value::from_i64(999)]; // claims 999 elements but has none
    let result = finalize_agg_payload(&AggFunc::ArrayAgg, &payload);
    assert!(
        result.is_err(),
        "Should error on count exceeding payload length"
    );
}

#[test]
fn test_negate_blob_subscript_invalid_utf8_no_panic() {
    // Reproduces fuzzer bug at seed 27035.
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        conn.execute("SELECT -X'18530E218A8D2D8D8F7456733370E68357745AFE13FC1B94751B77FCB00D0CAD971017936278BFF49BB4C8BD47F874ECA5226D3A433B7DFCD18661673598CED1FDB30A795F6F25'[2]")
    }));
    assert!(
        result.is_ok(),
        "Negating a blob subscript with invalid UTF-8 text should not panic"
    );
}
