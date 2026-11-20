use super::test_db::MvccTestDbNoConn;
use super::transactions::commit_tx;
use super::transactions::generate_simple_string_row;
use crate::common::maybe_setup_tracing;
use rand::rng;
use rand::Rng;
use rand_chacha::rand_core::RngCore;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeSet;
use turso_core::mvcc::database::MVTableId;
use turso_core::mvcc::database::Row;
use turso_core::mvcc::database::RowID;
use turso_core::mvcc::database::RowKey;
use turso_core::mvcc::database::SortableIndexKey;
use turso_core::namespace::NamespaceId;
use turso_core::schema::Table;
use turso_core::sync::Arc;
use turso_core::types::ImmutableRecord;
use turso_core::types::ImmutableRecordRef;
use turso_core::types::IndexInfo;
use turso_core::types::Text;
use turso_core::Value;
use turso_core::ValueRef;

/// What this test checks: A committed transaction written to the logical log is replayed correctly after restart.
/// Why this matters: This is the baseline durability/recovery guarantee for MVCC commits.
#[test]
fn test_logical_log_read() {
    maybe_setup_tracing();
    // Load a transaction
    // let's not drop db as we don't want files to be removed
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        let pager = conn.get_pager();
        let mvcc_store = db.get_mvcc_store();
        let table_id: MVTableId = (-100).into();
        let tx_id = mvcc_store.begin_tx(pager).unwrap();
        // insert table id -2 into sqlite_schema table (table_id -1)
        let data = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new("table")),  // type
                Value::Text(Text::new("test")),   // name
                Value::Text(Text::new("test")),   // tbl_name
                Value::from_i64(table_id.into()), // rootpage
                Value::Text(Text::new(
                    "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                )), // sql
            ],
            5,
        )
        .unwrap();
        mvcc_store
            .insert(
                tx_id,
                Row::new_table_row(RowID::new((-1).into(), RowKey::Int(1000)), data.as_blob())
                    .unwrap(),
            )
            .unwrap();
        // now insert a row into table -2
        let row = generate_simple_string_row(table_id, 1, "foo");
        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store, &conn, tx_id).unwrap();
    }

    // Restart the database to trigger recovery
    db.restart();

    // Now try to read it back - recovery happens automatically during bootstrap
    let conn = db.connect();
    let pager = conn.get_pager();
    let mvcc_store = db.get_mvcc_store();
    let tx = mvcc_store.begin_tx(pager).unwrap();
    let row = mvcc_store
        .read(tx, &RowID::new((-100).into(), RowKey::Int(1)))
        .unwrap()
        .unwrap();
    let record = ImmutableRecordRef::from_bin_record(row.payload());
    let foo = record.iter().unwrap().next().unwrap().unwrap();
    let ValueRef::Text(foo) = foo else {
        unreachable!()
    };
    assert_eq!(foo.as_str(), "foo");
}

/// What this test checks: A long sequence of committed frames is replayed in order without dropping or reordering transactions.
/// Why this matters: Recovery must preserve commit order to maintain MVCC visibility semantics.
#[test]
fn test_logical_log_read_multiple_transactions() {
    maybe_setup_tracing();
    let table_id: MVTableId = (-100).into();
    let values = (0..100)
        .map(|i| {
            (
                RowID::new(table_id, RowKey::Int(i as i64)),
                format!("foo_{i}"),
            )
        })
        .collect::<Vec<(RowID, String)>>();
    // let's not drop db as we don't want files to be removed
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        let pager = conn.get_pager();
        let mvcc_store = db.get_mvcc_store();

        let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
        // insert table id -2 into sqlite_schema table (table_id -1)
        let data = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new("table")),  // type
                Value::Text(Text::new("test")),   // name
                Value::Text(Text::new("test")),   // tbl_name
                Value::from_i64(table_id.into()), // rootpage
                Value::Text(Text::new(
                    "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                )), // sql
            ],
            5,
        )
        .unwrap();
        mvcc_store
            .insert(
                tx_id,
                Row::new_table_row(RowID::new((-1).into(), RowKey::Int(1000)), data.as_blob())
                    .unwrap(),
            )
            .unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
        // now insert a row into table -2
        // generate insert per transaction
        for (rowid, value) in &values {
            let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
            let row =
                generate_simple_string_row(rowid.table_id, rowid.row_id.to_int_or_panic(), value);
            mvcc_store.insert(tx_id, row).unwrap();
            commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
        }
    }

    // Restart the database to trigger recovery
    db.restart();

    // Now try to read it back - recovery happens automatically during bootstrap
    let conn = db.connect();
    let pager = conn.get_pager();
    let mvcc_store = db.get_mvcc_store();
    for (rowid, value) in &values {
        let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
        let row = mvcc_store.read(tx, rowid).unwrap().unwrap();
        let record = ImmutableRecordRef::from_bin_record(row.payload());
        let foo = record.iter().unwrap().next().unwrap().unwrap();
        let ValueRef::Text(foo) = foo else {
            unreachable!()
        };
        assert_eq!(foo.as_str(), value.as_str());
    }
}

/// What this test checks: Randomized insert/delete workloads round-trip through write + restart replay with matching final contents.
/// Why this matters: Fuzz-style coverage catches edge combinations that hand-written examples miss.
#[test]
fn test_logical_log_read_fuzz() {
    maybe_setup_tracing();
    let table_id: MVTableId = (-100).into();
    let seed = rng().random();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let num_transactions = rng.next_u64() % 128;
    let mut txns = vec![];
    let mut present_rowids = BTreeSet::new();
    let mut non_present_rowids = BTreeSet::new();
    for _ in 0..num_transactions {
        let num_operations = rng.next_u64() % 8;
        let mut ops = vec![];
        for _ in 0..num_operations {
            let op_type = rng.next_u64() % 2;
            match op_type {
                0 => {
                    // Generate a positive rowid that fits in i64
                    let row_id = (rng.next_u64() % (i64::MAX as u64)) as i64;
                    let rowid = RowID::new(table_id, RowKey::Int(row_id));
                    let row = generate_simple_string_row(
                        rowid.table_id,
                        rowid.row_id.to_int_or_panic(),
                        &format!("row_{row_id}"),
                    );
                    ops.push((true, Some(row), rowid.clone()));
                    present_rowids.insert(rowid.clone());
                    non_present_rowids.remove(&rowid);
                    tracing::debug!("insert {rowid:?}");
                }
                1 => {
                    if present_rowids.is_empty() {
                        continue;
                    }
                    let row_id_pos = rng.next_u64() as usize % present_rowids.len();
                    let row_id = present_rowids.iter().nth(row_id_pos).unwrap().clone();
                    ops.push((false, None, row_id.clone()));
                    present_rowids.remove(&row_id);
                    non_present_rowids.insert(row_id.clone());
                    tracing::debug!("removed {row_id:?}");
                }
                _ => unreachable!(),
            }
        }
        txns.push(ops);
    }
    // let's not drop db as we don't want files to be removed
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let pager = {
        let conn = db.connect();
        let pager = conn.get_pager();
        let mvcc_store = db.get_mvcc_store();

        // insert table id -2 into sqlite_schema table (table_id -1)
        let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
        let data = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new("table")),  // type
                Value::Text(Text::new("test")),   // name
                Value::Text(Text::new("test")),   // tbl_name
                Value::from_i64(table_id.into()), // rootpage
                Value::Text(Text::new(
                    "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                )), // sql
            ],
            5,
        )
        .unwrap();
        mvcc_store
            .insert(
                tx_id,
                Row::new_table_row(RowID::new((-1).into(), RowKey::Int(1000)), data.as_blob())
                    .unwrap(),
            )
            .unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();

        // insert rows
        for ops in &txns {
            let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
            for (is_insert, maybe_row, rowid) in ops {
                if *is_insert {
                    mvcc_store
                        .insert(tx_id, maybe_row.as_ref().unwrap().clone())
                        .unwrap();
                } else {
                    mvcc_store.delete(tx_id, rowid.clone()).unwrap();
                }
            }
            commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
        }

        conn.close().unwrap();
        pager
    };

    db.restart();

    // connect after restart should recover log.
    let _conn = db.connect();
    let mvcc_store = db.get_mvcc_store();

    // Check rowids that weren't deleted
    let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
    for present_rowid in present_rowids {
        let row = mvcc_store.read(tx, &present_rowid).unwrap().unwrap();
        let record = ImmutableRecordRef::from_bin_record(row.payload());
        let foo = record.iter().unwrap().next().unwrap().unwrap();
        let ValueRef::Text(foo) = foo else {
            unreachable!()
        };

        assert_eq!(
            foo.as_str(),
            format!("row_{}", present_rowid.row_id.to_int_or_panic())
        );
    }

    // Check rowids that were deleted
    let tx = mvcc_store.begin_tx(pager).unwrap();
    for present_rowid in non_present_rowids {
        let row = mvcc_store.read(tx, &present_rowid).unwrap();
        assert!(
            row.is_none(),
            "row {present_rowid:?} should have been removed"
        );
    }
}

/// What this test checks: Recovery rebuilds both table rows and index rows from logical-log operations.
/// Why this matters: Table/index divergence after restart would break query correctness.
#[test]
fn test_logical_log_read_table_and_index_rows() {
    maybe_setup_tracing();
    // Test that both table rows and index rows can be read back after recovery
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();

        // Create a table with an index
        conn.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_data ON test(data)").unwrap();

        // Checkpoint to ensure the index has a root_page mapping
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        // Insert some data - this will create both table rows and index rows in the logical log
        // Don't checkpoint after inserts so they remain in the logical log for recovery testing
        conn.execute("INSERT INTO test(id, data) VALUES (1, 'foo')")
            .unwrap();
        conn.execute("INSERT INTO test(id, data) VALUES (2, 'bar')")
            .unwrap();
        conn.execute("INSERT INTO test(id, data) VALUES (3, 'baz')")
            .unwrap();
    }

    // Restart the database to trigger recovery
    db.restart();

    // Now verify that both table rows and index rows can be read back
    let conn = db.connect();
    let pager = conn.get_pager();
    let mvcc_store = db.get_mvcc_store();
    let schema = conn.current_schema();
    let table = schema
        .get_table(NamespaceId::ROOT, "test")
        .expect("table test should exist");
    let Table::BTree(table) = table.as_ref() else {
        panic!("table test should be btree");
    };
    let table_id = mvcc_store.get_table_id_from_root_page(table.root_page);

    // Get the index from schema
    let index = schema
        .get_index(NamespaceId::ROOT, "test", "idx_data")
        .expect("Index should exist");
    // Use get_table_id_from_root_page to get the correct index_id (handles both checkpointed and non-checkpointed)
    let index_id = mvcc_store.get_table_id_from_root_page(index.root_page);
    let index_info = Arc::new(IndexInfo::new_from_index(index).unwrap());

    // Verify table rows can be read
    let tx = mvcc_store.begin_tx(pager).unwrap();
    for (row_id, expected_data) in [(1, "foo"), (2, "bar"), (3, "baz")] {
        let row = mvcc_store
            .read(tx, &RowID::new(table_id, RowKey::Int(row_id)))
            .unwrap()
            .expect("Table row should exist");
        let record = ImmutableRecordRef::from_bin_record(row.payload());
        let values = record.get_values().unwrap();
        let data_value = values.get(1).expect("Should have data column");
        let ValueRef::Text(data_text) = data_value else {
            panic!("Data column should be text");
        };
        assert_eq!(data_text.as_str(), expected_data);
    }

    // Verify index rows can be read
    // Note: Index rows are written to the logical log, but we need to construct the correct key format
    // The index key format is (indexed_column_value, table_rowid)
    for (row_id, data_value) in [(1, "foo"), (2, "bar"), (3, "baz")] {
        // Create the index key: (data_value, rowid)
        // The index on data column stores (data_value, table_rowid) as the key
        let key_record = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new(data_value.to_string())),
                Value::from_i64(row_id),
            ],
            2,
        )
        .unwrap();
        let sortable_key = SortableIndexKey::new_from_payload_in(
            &key_record,
            index_info.clone(),
            turso_core::alloc::TursoAllocator,
        )
        .unwrap();
        let index_rowid = RowID::new(index_id, RowKey::Record(Arc::new(sortable_key)));

        // Use read_from_table_or_index to read the index row
        // This verifies that index rows were properly serialized and deserialized from the logical log
        let index_row_opt = mvcc_store
            .read_from_table_or_index(tx, &index_rowid, Some(index_id))
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to read index row for ({}, {}): {:?}. Index ID: {:?}, root_page: {}",
                    data_value, row_id, e, index_id, index.root_page
                )
            });

        let Some(index_row) = index_row_opt else {
            panic!(
                "Index row for ({data_value}, {row_id}) not found after recovery. Index rows should be in the logical log."
            );
        };
        // Verify the index row contains the correct data
        let RowKey::Record(sortable_key) = index_row.id.row_id else {
            panic!("Index row should have a record row_id");
        };
        let record = sortable_key.key.clone();
        let values = record.get_values().unwrap();
        assert_eq!(
            values.len(),
            2,
            "Index row should have 2 columns (data, rowid)"
        );
        let ValueRef::Text(index_data) = values[0] else {
            panic!("First index column should be text");
        };
        assert_eq!(index_data.as_str(), data_value, "Index data should match");
        let ValueRef::Numeric(turso_core::numeric::Numeric::Integer(index_rowid_val)) = values[1]
        else {
            panic!("Second index column should be integer (rowid)");
        };
        assert_eq!(index_rowid_val, row_id, "Index rowid should match");
    }
}
