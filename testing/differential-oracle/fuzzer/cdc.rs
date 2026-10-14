//! Turso-only check of the change data capture (CDC) table. SQLite has no
//! CDC, so the records are checked against what the statement did on Turso.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use anyhow::{Context, Result};
use turso_core::{Connection, Value};

pub const ENABLE_CDC: &str = "PRAGMA capture_data_changes_conn('full')";

const DELETE: i64 = -1;
const INSERT: i64 = 1;
const COMMIT: i64 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub change_id: i64,
    pub change_type: i64,
    pub table_name: Option<String>,
    pub txn_id: i64,
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.change_type {
            DELETE => "DELETE",
            0 => "UPDATE",
            INSERT => "INSERT",
            COMMIT => "COMMIT",
            other => return write!(f, "#{} type {other}", self.change_id),
        };
        write!(f, "#{} {kind}", self.change_id)?;
        if let Some(table) = &self.table_name {
            write!(f, " {table}")?;
        }
        write!(f, " txn={}", self.txn_id)
    }
}

pub fn last_change_id(conn: &Arc<Connection>) -> Result<i64> {
    let mut last = None;
    conn.prepare("SELECT coalesce(max(change_id), 0) FROM turso_cdc")?
        .run_with_row_callback(|row| {
            last = Some(integer(row.get_value(0)));
            Ok(())
        })?;
    last.context("max(change_id) returned no row")
}

pub fn records_after(conn: &Arc<Connection>, change_id: i64) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    conn.prepare(format!(
        "SELECT change_id, change_type, table_name, change_txn_id FROM turso_cdc \
         WHERE change_id > {change_id} ORDER BY change_id"
    ))?
    .run_with_row_callback(|row| {
        records.push(Record {
            change_id: integer(row.get_value(0)),
            change_type: integer(row.get_value(1)),
            table_name: match row.get_value(2) {
                Value::Text(name) => Some(name.as_str().to_string()),
                Value::Null => None,
                other => panic!("turso_cdc.table_name is {other:?}"),
            },
            txn_id: integer(row.get_value(3)),
        });
        Ok(())
    })?;
    Ok(records)
}

fn integer(value: &Value) -> i64 {
    match value {
        Value::Numeric(turso_core::Numeric::Integer(n)) => *n,
        other => panic!("expected an integer in turso_cdc, got {other:?}"),
    }
}

/// Check the records written since the current transaction began. A
/// transaction that is still open has no COMMIT record yet. A finished one
/// wrote nothing, or wrote its changes and then exactly one COMMIT record.
/// All records of one transaction carry the same transaction id, never -1.
pub fn check_transaction(records: &[Record], finished: bool) -> Result<(), String> {
    let Some(last) = records.last() else {
        return Ok(());
    };
    if let Some(record) = records.iter().find(|r| r.txn_id == -1) {
        return Err(format!("record {record} has change_txn_id -1"));
    }
    if let Some(record) = records.iter().find(|r| r.txn_id != records[0].txn_id) {
        return Err(format!(
            "record {record} has a different change_txn_id than {}",
            records[0]
        ));
    }
    let commits = records.iter().filter(|r| r.change_type == COMMIT).count();
    match (finished, commits) {
        (false, 0) => Ok(()),
        (false, _) => Err("a COMMIT record inside an open transaction".to_string()),
        (true, 1) if last.change_type == COMMIT && records.len() > 1 => Ok(()),
        (true, 1) if last.change_type == COMMIT => {
            Err("a COMMIT record without a change before it".to_string())
        }
        (true, 0) => Err("the transaction ended without a COMMIT record".to_string()),
        (true, 1) => Err("the COMMIT record is not the last record".to_string()),
        (true, _) => Err(format!("{commits} COMMIT records in one transaction")),
    }
}

/// For each table, the INSERT records minus the DELETE records must equal
/// the change in its row count.
pub fn check_row_counts(
    records: &[Record],
    before: &BTreeMap<String, i64>,
    after: &BTreeMap<String, i64>,
) -> Result<(), String> {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>(),
        "the table set changed during a statement that is not DDL"
    );
    let mut net: BTreeMap<&str, i64> = before.keys().map(|t| (t.as_str(), 0)).collect();
    for record in records.iter().filter(|r| r.change_type != COMMIT) {
        let table = record
            .table_name
            .as_deref()
            .expect("a change names its table");
        let Some(count) = net.get_mut(table) else {
            return Err(format!(
                "record {record} is for a table the check does not know"
            ));
        };
        match record.change_type {
            INSERT => *count += 1,
            DELETE => *count -= 1,
            _ => {}
        }
    }
    for (table, net) in net {
        let diff = after[table] - before[table];
        if net != diff {
            return Err(format!(
                "table {table}: the row count changed by {diff}, but the records add up to {net}"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(change_id: i64, change_type: i64, txn_id: i64) -> Record {
        Record {
            change_id,
            change_type,
            table_name: (change_type != COMMIT).then(|| "t".to_string()),
            txn_id,
        }
    }

    #[test]
    fn a_finished_transaction_is_its_changes_and_one_commit() {
        assert_eq!(check_transaction(&[], true), Ok(()));
        assert_eq!(
            check_transaction(
                &[
                    record(3, INSERT, 3),
                    record(4, DELETE, 3),
                    record(5, COMMIT, 3)
                ],
                true
            ),
            Ok(())
        );
        assert_eq!(
            check_transaction(&[record(3, COMMIT, -1)], true),
            Err("record #3 COMMIT txn=-1 has change_txn_id -1".to_string())
        );
        assert_eq!(
            check_transaction(&[record(3, COMMIT, 3)], true),
            Err("a COMMIT record without a change before it".to_string())
        );
        assert_eq!(
            check_transaction(
                &[
                    record(3, DELETE, 3),
                    record(4, COMMIT, 3),
                    record(5, INSERT, 3),
                    record(6, COMMIT, 3)
                ],
                true
            ),
            Err("2 COMMIT records in one transaction".to_string())
        );
        assert_eq!(
            check_transaction(&[record(3, INSERT, 3)], true),
            Err("the transaction ended without a COMMIT record".to_string())
        );
        assert_eq!(
            check_transaction(&[record(3, INSERT, 3), record(4, COMMIT, 4)], true),
            Err(
                "record #4 COMMIT txn=4 has a different change_txn_id than #3 INSERT t txn=3"
                    .to_string()
            )
        );
    }

    #[test]
    fn an_open_transaction_has_no_commit_record() {
        assert_eq!(check_transaction(&[record(3, INSERT, 3)], false), Ok(()));
        assert_eq!(
            check_transaction(&[record(3, INSERT, 3), record(4, COMMIT, 3)], false),
            Err("a COMMIT record inside an open transaction".to_string())
        );
    }

    #[test]
    fn inserts_minus_deletes_match_the_row_count_change() {
        let counts = |t: i64, u: i64| BTreeMap::from([("t".to_string(), t), ("u".to_string(), u)]);
        let records = [
            record(1, INSERT, 1),
            record(2, INSERT, 1),
            record(3, DELETE, 1),
            record(4, 0, 1),
            record(5, COMMIT, 1),
        ];
        assert_eq!(
            check_row_counts(&records, &counts(4, 0), &counts(5, 0)),
            Ok(())
        );
        assert_eq!(
            check_row_counts(&records, &counts(4, 2), &counts(5, 0)),
            Err("table u: the row count changed by -2, but the records add up to 0".to_string())
        );
        let other_table = Record {
            table_name: Some("w".to_string()),
            ..record(1, INSERT, 1)
        };
        assert_eq!(
            check_row_counts(&[other_table], &counts(0, 0), &counts(0, 0)),
            Err("record #1 INSERT w txn=1 is for a table the check does not know".to_string())
        );
    }
}
