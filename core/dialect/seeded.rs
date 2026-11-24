//! The read-only tables that serve the rows of a [`SeededTable`] before the
//! table is stored.
//!
//! The schema of a database gets one read-only table for each seeded table
//! of its dialect's catalog, when no stored object has the name. The loader
//! adds them after it reads the schema rows, so a stored table always keeps
//! its name. A read-only table is not a table-valued function, so the paths
//! that copy table-valued functions into a new schema, or into the temp
//! schema, do not copy it.

use crate::dialect::{Dialect, SeededTable};
use crate::namespace::NamespaceId;
use crate::numeric::Numeric;
use crate::schema::{Column, Schema, Table};
use crate::sync::{Arc, RwLock};
use crate::vtab::{InternalVirtualTable, InternalVirtualTableCursor, VirtualTable};
use crate::{ast, turso_assert, Connection, LimboError, Result, Value};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};

/// `idx_num` of a scan of all rows.
const SCAN: i32 = 0;
/// `idx_num` of a lookup of the row whose rowid is the first argument.
const FIND: i32 = 1;

/// The read-only tables of the seeded tables of a database, built once when
/// the database is opened.
pub(crate) struct SeededTables {
    /// Each table under its normalized name.
    tables: Vec<(String, Arc<Table>)>,
}

impl SeededTables {
    /// The read-only tables of the seeded tables of `dialect`'s catalog, or
    /// none when `enabled` is false.
    pub(crate) fn new(dialect: &dyn Dialect, enabled: bool) -> Result<Self> {
        let seeded_tables = if enabled {
            dialect.catalog().seeded_tables()
        } else {
            &[]
        };
        let mut tables = Vec::with_capacity(seeded_tables.len());
        for seeded_table in seeded_tables {
            let vtab = read_only_table(dialect, seeded_table)?;
            let name = crate::util::normalize_ident(seeded_table.name);
            turso_assert!(
                tables.iter().all(|(other, _)| *other != name),
                "two seeded tables have the same name",
                { "name": name }
            );
            tables.push((name, Arc::new(Table::Virtual(Arc::new(vtab)))));
        }
        Ok(Self { tables })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Whether `schema` has no object under the name of one of the tables.
    pub(crate) fn any_missing_from(&self, schema: &Schema) -> bool {
        self.tables
            .iter()
            .any(|(name, _)| !name_is_used(schema, name))
    }

    /// Adds to the root namespace of `schema` each table whose name no
    /// object of that namespace has. A stored table of the same name keeps
    /// the name.
    pub(crate) fn add_missing_to(&self, schema: &mut Schema) {
        for (name, table) in &self.tables {
            if !name_is_used(schema, name) {
                schema.insert_table(NamespaceId::ROOT, name.clone(), table.clone());
            }
        }
    }

    /// Refuses a statement that writes `table` when `table` is one of the
    /// read-only tables. The seed rows change only when the dialect changes.
    pub(crate) fn refuse_write(&self, table: &Table, name: &str) -> Result<()> {
        if self
            .tables
            .iter()
            .any(|(_, read_only)| std::ptr::eq(read_only.as_ref(), table))
        {
            return Err(LimboError::ParseError(format!(
                "table {name} may not be modified"
            )));
        }
        Ok(())
    }
}

fn name_is_used(schema: &Schema, name: &str) -> bool {
    schema.get_object_type(NamespaceId::ROOT, name).is_some()
}

/// The read-only table of `seeded_table`. Its columns are the columns that
/// `create_sql` declares.
fn read_only_table(
    dialect: &dyn Dialect,
    seeded_table: &'static SeededTable,
) -> Result<VirtualTable> {
    let ast::Stmt::CreateTable { body, .. } =
        crate::dialect::parse_table_sql_ast(dialect, seeded_table.create_sql)?
    else {
        unreachable!("parse_table_sql_ast returns CREATE TABLE");
    };
    let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = &body else {
        return Err(LimboError::InternalError(format!(
            "seeded table {} must define its columns",
            seeded_table.name
        )));
    };
    let definitions = columns.clone();
    let btree = crate::schema::create_table(seeded_table.name, &body, 0, false)?;
    turso_assert!(btree.has_rowid, "a seeded table must have a rowid", {
        "name": seeded_table.name
    });
    turso_assert!(
        !btree.has_virtual_columns,
        "a seeded table must not have generated columns",
        { "name": seeded_table.name }
    );
    let columns: Vec<Column> = btree.columns().to_vec();
    let rowid_column = btree.get_rowid_alias_column().map(|(position, _)| position);
    let table = SeededVirtualTable {
        table: seeded_table,
        definitions,
        rowid_column,
        column_count: columns.len(),
    };
    Ok(VirtualTable::new_seeded(
        seeded_table.name.to_string(),
        columns,
        Arc::new(RwLock::new(table)),
    ))
}

/// The read-only table of one seeded table.
struct SeededVirtualTable {
    table: &'static SeededTable,
    definitions: Vec<ast::ColumnDefinition>,
    /// The position of the `INTEGER PRIMARY KEY` column, if there is one.
    rowid_column: Option<usize>,
    column_count: usize,
}

impl std::fmt::Debug for SeededVirtualTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeededVirtualTable")
            .field("name", &self.table.name)
            .finish()
    }
}

impl InternalVirtualTable for SeededVirtualTable {
    fn name(&self) -> String {
        self.table.name.to_string()
    }

    fn open(&self, _conn: Arc<Connection>) -> Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        Ok(Arc::new(RwLock::new(SeededCursor {
            table: self.table,
            rowid_column: self.rowid_column,
            column_count: self.column_count,
            position: Position::Done,
            row: Vec::with_capacity(self.column_count),
        })))
    }

    /// Uses an equality on the `INTEGER PRIMARY KEY` column, which is also
    /// the rowid, to find one row. The constraint is not omitted: the cursor
    /// can return a row that does not match, for example when the value is
    /// text, and VDBE then compares the value with SQL rules.
    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        let mut constraint_usages = vec![
            ConstraintUsage {
                argv_index: None,
                omit: false,
            };
            constraints.len()
        ];
        let rowid_equality = self.rowid_column.and_then(|rowid_column| {
            constraints.iter().position(|constraint| {
                constraint.usable
                    && constraint.op == ConstraintOp::Eq
                    && constraint.column_index as usize == rowid_column
            })
        });
        if let Some(position) = rowid_equality {
            constraint_usages[position].argv_index = Some(1);
            return Ok(IndexInfo {
                idx_num: FIND,
                idx_str: None,
                order_by_consumed: false,
                estimated_cost: 1.0,
                estimated_rows: 1,
                constraint_usages,
            });
        }
        let len = self.table.seed.len();
        Ok(IndexInfo {
            idx_num: SCAN,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: len as f64,
            estimated_rows: u32::try_from(len).unwrap_or(u32::MAX - 1),
            constraint_usages,
        })
    }

    fn columns(&self) -> Vec<ast::ColumnDefinition> {
        self.definitions.clone()
    }

    fn superuser_only(&self) -> bool {
        false
    }
}

/// Where a [`SeededCursor`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    /// At the row with this index, in a scan of all rows.
    Scan(usize),
    /// At the one row that a lookup found.
    Found(usize),
    /// After the last row.
    Done,
}

/// A cursor over the seed rows. It reads one row at a time from the seed.
struct SeededCursor {
    table: &'static SeededTable,
    rowid_column: Option<usize>,
    column_count: usize,
    position: Position,
    /// The values of the current row.
    row: Vec<Value>,
}

impl SeededCursor {
    /// Moves to the row at `index` and reads its values.
    fn read_row(&mut self, index: usize) {
        self.row.clear();
        self.table.seed.row(index, &mut self.row);
        turso_assert!(
            self.row.len() == self.column_count,
            "a seed row must have one value for each column",
            { "table": self.table.name, "index": index, "values": self.row.len(), "columns": self.column_count }
        );
    }

    /// Starts a scan at the first row.
    fn scan_from_start(&mut self) -> bool {
        if self.table.seed.is_empty() {
            self.position = Position::Done;
            return false;
        }
        self.read_row(0);
        self.position = Position::Scan(0);
        true
    }

    /// Moves to the row whose rowid is `rowid`, if there is one.
    fn find(&mut self, rowid: i64) -> bool {
        let Some(index) = self.table.seed.find(rowid) else {
            self.position = Position::Done;
            return false;
        };
        turso_assert!(
            index < self.table.seed.len(),
            "Seed::find returned an index after the last row",
            { "table": self.table.name, "index": index }
        );
        self.read_row(index);
        self.position = Position::Found(index);
        let found = self.current_rowid();
        turso_assert!(
            found == rowid,
            "Seed::find returned a row with a different rowid",
            { "table": self.table.name, "rowid": rowid, "found": found }
        );
        true
    }

    /// The rowid of the current row.
    fn current_rowid(&self) -> i64 {
        let index = match self.position {
            Position::Scan(index) | Position::Found(index) => index,
            Position::Done => return 0,
        };
        match self.rowid_column {
            Some(column) => match &self.row[column] {
                Value::Numeric(Numeric::Integer(rowid)) => *rowid,
                other => panic!(
                    "the INTEGER PRIMARY KEY of seed row {index} of {} is not an integer: {other:?}",
                    self.table.name
                ),
            },
            None => index as i64 + 1,
        }
    }
}

impl InternalVirtualTableCursor for SeededCursor {
    fn filter(&mut self, args: &[Value], _idx_str: Option<String>, idx_num: i32) -> Result<bool> {
        match idx_num {
            SCAN => Ok(self.scan_from_start()),
            FIND => {
                turso_assert!(args.len() == 1, "a lookup of a seed row has one argument");
                match key_of(&args[0]) {
                    Key::Rowid(rowid) => Ok(self.find(rowid)),
                    Key::NoRow => {
                        self.position = Position::Done;
                        Ok(false)
                    }
                    Key::AnyRow => Ok(self.scan_from_start()),
                }
            }
            _ => unreachable!("best_index gives only SCAN or FIND, got {idx_num}"),
        }
    }

    fn next(&mut self) -> Result<bool> {
        match self.position {
            Position::Scan(index) if index + 1 < self.table.seed.len() => {
                self.read_row(index + 1);
                self.position = Position::Scan(index + 1);
                Ok(true)
            }
            Position::Scan(_) | Position::Found(_) | Position::Done => {
                self.position = Position::Done;
                Ok(false)
            }
        }
    }

    fn rowid(&self) -> i64 {
        self.current_rowid()
    }

    fn column(&self, column: usize) -> Result<Value> {
        if self.position == Position::Done {
            return Ok(Value::Null);
        }
        Ok(self.row[column].clone())
    }
}

/// What the argument of a lookup asks for.
enum Key {
    /// The row with this rowid.
    Rowid(i64),
    /// No row: no integer is equal to the value.
    NoRow,
    /// Any row. VDBE compares each row with the value.
    AnyRow,
}

/// The rowid that `value` can be equal to. An integer column is equal only
/// to an integer, or to a real with the same value. A text or blob value can
/// be equal after the comparison converts it, so the cursor then scans and
/// VDBE decides.
fn key_of(value: &Value) -> Key {
    match value {
        Value::Null => Key::NoRow,
        Value::Numeric(Numeric::Integer(rowid)) => Key::Rowid(*rowid),
        Value::Numeric(Numeric::Float(real)) => {
            let real = f64::from(*real);
            // -2^63 and 2^63 are exact as f64. Every real in this range
            // that has no fraction is an i64.
            if real.fract() == 0.0
                && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&real)
            {
                Key::Rowid(real as i64)
            } else {
                Key::NoRow
            }
        }
        Value::Text(_) | Value::Blob(_) => Key::AnyRow,
    }
}
