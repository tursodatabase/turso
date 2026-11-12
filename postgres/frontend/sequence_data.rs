//! `pg_get_sequence_data(oid)`, PostgreSQL's function for the stored state
//! of a sequence, which pg_dump reads to restore each sequence's value.

use std::sync::Arc;

use turso_core::{
    namespace::{mangle_name, NamespaceRef},
    native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule},
    schema::SEQ_BACKING_TABLE_PREFIX,
    types::IOResultOr,
    Connection, IOResult, LimboError, Result, Statement, Value, MAIN_DB_ID,
};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};

use crate::catalog::sequence_catalog_entries;

const SEQUENCE_OID_COLUMN: usize = 3;

#[derive(Debug)]
pub(crate) struct PgGetSequenceData;

impl VirtualTableModule for PgGetSequenceData {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok(
            "CREATE TABLE pg_get_sequence_data (last_value BIGINT, is_called BOOLEAN, \
            page_lsn TEXT, sequence_oid INTEGER HIDDEN)"
                .into(),
        )
    }

    fn create(&self, _args: &[Value]) -> Result<Self> {
        Ok(Self)
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for PgGetSequenceData {
    type Cursor = SequenceDataCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(SequenceDataCursor {
            conn,
            statement: None,
            row: None,
            sequence_oid: Value::Null,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let input = constraints.iter().position(|c| {
            c.column_index == SEQUENCE_OID_COLUMN as u32 && c.op == ConstraintOp::Eq && c.usable
        });
        let Some(input) = input else {
            return Err(ResultCode::ConstraintViolation);
        };
        Ok(IndexInfo {
            constraint_usages: (0..constraints.len())
                .map(|i| ConstraintUsage {
                    argv_index: (i == input).then_some(1),
                    omit: i == input,
                })
                .collect(),
            estimated_cost: 1.0,
            estimated_rows: 1,
            ..Default::default()
        })
    }
}

/// The one row of `pg_get_sequence_data` for the sequence it was called with.
pub(crate) struct SequenceDataCursor {
    conn: Arc<Connection>,
    /// The read of the sequence's backing table while it waits for I/O.
    statement: Option<Statement>,
    /// `(last_value, is_called)`, or None after the row was read.
    row: Option<[Value; 2]>,
    sequence_oid: Value,
}

impl VirtualTableCursor for SequenceDataCursor {
    fn next(&mut self) -> IOResultOr<bool> {
        self.row = None;
        Ok(IOResult::Done(false))
    }

    fn rowid(&self) -> i64 {
        0
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        let row = self
            .row
            .as_ref()
            .expect("the cursor is on its row while columns are read");
        let value = match column {
            0 | 1 => row[column].clone(),
            2 => Value::Null,
            SEQUENCE_OID_COLUMN => self.sequence_oid.clone(),
            _ => unreachable!("invalid pg_get_sequence_data column"),
        };
        Ok(IOResult::Done(value))
    }

    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> IOResultOr<bool> {
        let [sequence_oid] = args else {
            return Err(LimboError::InvalidArgument(
                "pg_get_sequence_data requires one argument".into(),
            )
            .into());
        };
        if self.statement.is_none() {
            self.sequence_oid = sequence_oid.clone();
            let Some(read) = self.state_read(sequence_oid) else {
                // As in PostgreSQL, a relation that is not a sequence, or a
                // sequence the role may not read, gives a row of NULLs.
                self.row = Some([Value::Null, Value::Null]);
                return Ok(IOResult::Done(true));
            };
            self.row = Some([read.start_value, Value::from_i64(0)]);
            self.statement = Some(self.conn.prepare_internal(read.sql)?);
        }
        let row = &mut self.row;
        if let IOResult::IO(completions) = self
            .statement
            .as_mut()
            .expect("the read was prepared above")
            .run_with_row_callback_nonblock(|stored| {
                *row = Some([
                    stored.get_value(0).clone(),
                    Value::from_i64(i64::from(stored.get_value(1) != &Value::from_i64(0))),
                ]);
                Ok(())
            })?
        {
            return Ok(IOResult::IO(completions));
        }
        self.statement = None;
        Ok(IOResult::Done(true))
    }
}

/// How to read the stored state of one sequence.
struct SequenceStateRead {
    /// The query of the sequence's current `(value, is_called)`.
    sql: String,
    /// The value of a sequence that has not handed out any value.
    start_value: Value,
}

impl SequenceDataCursor {
    /// The read of the sequence with oid `sequence_oid`, or None when no
    /// sequence has that oid or the current role may not read it.
    fn state_read(&self, sequence_oid: &Value) -> Option<SequenceStateRead> {
        let oid = sequence_oid.as_int()?;
        let schema = self.conn.current_schema();
        let (_, namespace, sequence) = sequence_catalog_entries(&schema)
            .into_iter()
            .find(|(sequence_oid, ..)| *sequence_oid == oid)?;
        let namespace_ref = NamespaceRef {
            database_id: MAIN_DB_ID,
            id: namespace,
        };
        if !self.conn.may_read_sequence(namespace_ref, &sequence.name) {
            return None;
        }
        let backing_table = format!(
            "{SEQ_BACKING_TABLE_PREFIX}{}",
            mangle_name(namespace, &sequence.name)
        );
        // Every nextval adds a row keyed by its value, so the current one is
        // the highest value of an ascending sequence and the lowest of a
        // descending one.
        let order = if sequence.increment_by >= 0 {
            "DESC"
        } else {
            "ASC"
        };
        Some(SequenceStateRead {
            sql: format!(
                "SELECT value, is_called FROM \"{}\" ORDER BY value {order} LIMIT 1",
                backing_table.replace('"', "\"\"")
            ),
            start_value: Value::from_i64(sequence.start_value),
        })
    }
}
