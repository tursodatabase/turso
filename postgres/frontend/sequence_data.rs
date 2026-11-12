//! `pg_get_sequence_data(oid)`, PostgreSQL's function for the stored state
//! of a sequence, which pg_dump reads to restore each sequence's value.

use std::sync::Arc;

use turso_core::{
    namespace::{mangle_name, NamespaceRef},
    native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule},
    types::IOResultOr,
    Connection, IOResult, LimboError, Result, Statement, Value, MAIN_DB_ID,
};
use turso_ext::{ConstraintInfo, IndexInfo, OrderByInfo, ResultCode};

use crate::catalog::sequence_catalog_entries;
use crate::table_function::one_argument_best_index;

const SEQUENCE_OID_COLUMN: usize = 2;

#[derive(Debug)]
pub(crate) struct PgGetSequenceData;

impl VirtualTableModule for PgGetSequenceData {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok(
            "CREATE TABLE pg_get_sequence_data (last_value BIGINT, is_called BOOLEAN, \
            sequence_oid INTEGER HIDDEN)"
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
        one_argument_best_index(constraints, SEQUENCE_OID_COLUMN as u32, 1)
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
            let Some(sql) = self.state_read_sql(sequence_oid) else {
                // As in PostgreSQL, a relation that is not a sequence, or a
                // sequence the role may not read, gives a row of NULLs.
                self.row = Some([Value::Null, Value::Null]);
                return Ok(IOResult::Done(true));
            };
            self.row = None;
            self.statement = Some(self.conn.prepare_internal(sql)?);
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
        assert!(self.row.is_some(), "a sequence state read returns one row");
        Ok(IOResult::Done(true))
    }
}

impl SequenceDataCursor {
    /// The query of the current `(value, is_called)` of the sequence with
    /// oid `sequence_oid`, or None when no sequence has that oid or the
    /// current role may not read it. sequence_state_experimental reads the
    /// state the way nextval does, so inside a transaction it also sees the
    /// values that nextval handed out after the transaction's snapshot, as
    /// PostgreSQL does.
    fn state_read_sql(&self, sequence_oid: &Value) -> Option<String> {
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
        let name = mangle_name(namespace, &sequence.name).replace('\'', "''");
        Some(format!(
            "SELECT state[1], state[2] \
             FROM (SELECT sequence_state_experimental('{name}') AS state) AS sequence_state"
        ))
    }
}
