//! The read-only tables that serve the rows of a [`SeededTable`] before the
//! table is stored.
//!
//! The schema of a database gets one read-only table for each seeded table
//! of its dialect's catalog, when no stored object has the name. The loader
//! adds them after it reads the schema rows, so a stored table always keeps
//! its name. A read-only table is not a table-valued function, so the paths
//! that copy table-valued functions into a new schema, or into the temp
//! schema, do not copy it.

use crate::dialect::{Dialect, Seed, SeededTable};
use crate::namespace::NamespaceId;
use crate::numeric::Numeric;
use crate::schema::{BTreeTable, Column, Index, Schema, Table, TURSO_TYPES_TABLE_NAME};
use crate::sync::{Arc, OnceLock, RwLock};
use crate::util::{UnparsedFromSqlIndex, PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX};
use crate::vdbe::affinity::Affinity;
use crate::vtab::{InternalVirtualTable, InternalVirtualTableCursor, VirtualTable};
use crate::SymbolTable;
use crate::{ast, turso_assert, Connection, LimboError, Result, Value};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};

/// `idx_num` of a scan of all rows.
const SCAN: i32 = 0;
/// `idx_num` of a scan of all rows, for a `VFilter` that core emits.
pub(crate) const SEED_SCAN: usize = SCAN as usize;
/// `idx_num` of a lookup of the row whose rowid is the first argument.
const FIND: i32 = 1;

/// The seeded tables of core. Core stores each one on its first write, as it
/// stores the seeded tables of a catalog, but core gives them no read-only
/// table: their seeds are empty, and a database without the stored table
/// has no table of that name.
static CORE_SEEDED_TABLES: [SeededTable; 1] = [SeededTable {
    name: TURSO_TYPES_TABLE_NAME,
    create_sql: TURSO_TYPES_TABLE_SQL,
    index_sql: &[],
    seed: &EmptySeed,
    hidden_columns: &[],
    seed_rows: None,
}];

/// The `CREATE TABLE` statement of the table of the types made by
/// `CREATE TYPE` and `CREATE DOMAIN`.
const TURSO_TYPES_TABLE_SQL: &str =
    "CREATE TABLE __turso_internal_types(name TEXT PRIMARY KEY, sql TEXT)";

/// A seed without rows.
struct EmptySeed;

impl Seed for EmptySeed {
    fn len(&self) -> usize {
        0
    }

    fn row(&self, index: usize, _out: &mut Vec<Value>) {
        unreachable!("an empty seed has no row {index}");
    }

    fn find(&self, _rowid: i64) -> Option<usize> {
        None
    }

    fn version(&self) -> u32 {
        0
    }
}

/// The seeded tables of a database, built once when the database is opened:
/// the read-only tables of its catalog's seeded tables, and how each seeded
/// table of the catalog and of core is stored.
pub(crate) struct SeededTables {
    /// Each read-only table under its normalized name.
    tables: Vec<(String, Arc<Table>)>,
    /// How each seeded table of the catalog is stored.
    stored_forms: Vec<StoredForm>,
    /// How each seeded table of core is stored, built on the first write,
    /// so that the open of a database parses no SQL of core. None when the
    /// database has no seeded tables.
    core_stored_forms: Option<OnceLock<std::result::Result<Vec<StoredForm>, String>>>,
}

impl SeededTables {
    /// The seeded tables of `dialect`'s catalog and of core, or none when
    /// `enabled` is false.
    pub(crate) fn new(dialect: &dyn Dialect, enabled: bool) -> Result<Self> {
        if !enabled {
            return Ok(Self {
                tables: Vec::new(),
                stored_forms: Vec::new(),
                core_stored_forms: None,
            });
        }
        let catalog_tables = dialect.catalog().seeded_tables();
        let mut tables = Vec::with_capacity(catalog_tables.len());
        let mut stored_forms = Vec::with_capacity(catalog_tables.len());
        for seeded_table in catalog_tables {
            let vtab = read_only_table(dialect, seeded_table)?;
            let read_only = Arc::new(Table::Virtual(Arc::new(vtab)));
            let name = crate::util::normalize_ident(seeded_table.name);
            tables.push((name, read_only.clone()));
            stored_forms.push(StoredForm::new(dialect, seeded_table, Some(read_only))?);
        }
        let names = stored_forms
            .iter()
            .map(|form| form.name.clone())
            .chain(
                CORE_SEEDED_TABLES
                    .iter()
                    .map(|table| crate::util::normalize_ident(table.name)),
            )
            .collect::<Vec<_>>();
        for (position, name) in names.iter().enumerate() {
            turso_assert!(
                !names[..position].contains(name),
                "two seeded tables have the same name",
                { "name": name }
            );
        }
        Ok(Self {
            tables,
            stored_forms,
            core_stored_forms: Some(OnceLock::new()),
        })
    }

    /// Whether the database has no read-only tables.
    pub(crate) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// How the seeded table `name` is stored, or None when no seeded table
    /// has the name. `dialect` is the dialect of the database.
    pub(crate) fn stored_form(
        &self,
        dialect: &dyn Dialect,
        name: &str,
    ) -> Result<Option<&StoredForm>> {
        let name = crate::util::normalize_ident(name);
        if let Some(form) = self.stored_forms.iter().find(|form| form.name == name) {
            return Ok(Some(form));
        }
        let Some(core_stored_forms) = &self.core_stored_forms else {
            return Ok(None);
        };
        let core_stored_forms = core_stored_forms
            .get_or_init(|| {
                CORE_SEEDED_TABLES
                    .iter()
                    .map(|seeded_table| StoredForm::new(dialect, seeded_table, None))
                    .collect::<Result<Vec<_>>>()
                    .map_err(|err| err.to_string())
            })
            .as_ref()
            .map_err(|err| LimboError::InternalError(err.clone()))?;
        Ok(core_stored_forms.iter().find(|form| form.name == name))
    }

    /// Whether `table` is the read-only table of the seeded table `name`.
    pub(crate) fn is_read_only_table(&self, name: &str, table: &Arc<Table>) -> bool {
        let name = crate::util::normalize_ident(name);
        self.tables
            .iter()
            .any(|(other, read_only)| *other == name && Arc::ptr_eq(read_only, table))
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

/// How a seeded table is stored: its `sqlite_schema` rows, and the table and
/// indexes that the schema gets when it reads them. The table and the
/// indexes have root page 0, because the B-trees do not exist yet.
pub(crate) struct StoredForm {
    /// The normalized name of the table.
    pub(crate) name: String,
    pub(crate) seeded_table: &'static SeededTable,
    /// The read-only table that serves the seed rows before the table is
    /// stored. Core's seeded tables have none.
    pub(crate) read_only: Option<Arc<Table>>,
    pub(crate) table: Arc<BTreeTable>,
    /// The `sqlite_schema` rows of the indexes, in the order they are
    /// written: the automatic indexes, then the indexes of `index_sql`.
    pub(crate) index_rows: Vec<IndexRow>,
    /// The indexes in the order that the schema lists them, which is the
    /// order in which an insert writes them.
    pub(crate) indexes: Vec<Arc<Index>>,
    /// The column and the range of the seed rows. None for a seeded table
    /// of core.
    pub(crate) seed_rows: Option<SeedRowColumn>,
}

/// The [`SeedRowRange`] of a seeded table, with the position of its column
/// in the table.
///
/// [`SeedRowRange`]: crate::dialect::SeedRowRange
#[derive(Debug, Clone)]
pub(crate) struct SeedRowColumn {
    pub(crate) position: usize,
    pub(crate) range: std::ops::Range<i64>,
}

/// The `sqlite_schema` row of an index of a seeded table.
pub(crate) struct IndexRow {
    pub(crate) name: String,
    /// The SQL text, or None for an automatic index, as `CREATE TABLE`
    /// stores it.
    pub(crate) sql: Option<&'static str>,
}

impl StoredForm {
    /// Reads the table and its indexes from the rows that storing
    /// `seeded_table` writes, as the schema reads them.
    fn new(
        dialect: &dyn Dialect,
        seeded_table: &'static SeededTable,
        read_only: Option<Arc<Table>>,
    ) -> Result<Self> {
        let name = crate::util::normalize_ident(seeded_table.name);
        turso_assert!(
            read_only.is_some() || seeded_table.seed.is_empty(),
            "a seeded table of core has an empty seed",
            { "name": name }
        );
        let refuse = |reason: &str| {
            Err(LimboError::InternalError(format!(
                "seeded table {name} cannot be stored: {reason}"
            )))
        };
        let table = dialect.parse_table_sql(seeded_table.create_sql, 0)?;
        if table.name != name {
            return refuse("its CREATE TABLE has a different name");
        }
        if let Some(reason) = unwritable_table_reason(&table) {
            return refuse(reason);
        }
        turso_assert!(
            read_only.is_some() || seeded_table.seed_rows.is_none(),
            "a seeded table of core has no seed row range",
            { "name": name }
        );
        let seed_rows = match &seeded_table.seed_rows {
            Some(seed_rows) => {
                let Some(position) = table.columns().iter().position(|column| {
                    column
                        .name
                        .as_deref()
                        .is_some_and(|other| other.eq_ignore_ascii_case(seed_rows.column))
                }) else {
                    return refuse("the column of its seed row range does not exist");
                };
                if table.columns()[position].affinity() != Affinity::Integer {
                    return refuse("the column of its seed row range is not an INTEGER column");
                }
                Some(SeedRowColumn {
                    position,
                    range: seed_rows.range.clone(),
                })
            }
            None if read_only.is_some() => return refuse("it has no seed row range"),
            None => None,
        };
        let table = Arc::new(table);

        // Fake root pages tell the indexes apart until they have names.
        let automatic_count = crate::translate::schema::automatic_index_count(&table)?;
        let mut index_rows = Vec::with_capacity(automatic_count + seeded_table.index_sql.len());
        let mut automatic = Vec::with_capacity(automatic_count);
        for number in 1..=automatic_count {
            let index_name = format!("{PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX}{name}_{number}");
            automatic.push((index_name.clone(), index_rows.len() as i64 + 2));
            index_rows.push(IndexRow {
                name: index_name,
                sql: None,
            });
        }
        let mut from_sql = Vec::with_capacity(seeded_table.index_sql.len());
        for sql in seeded_table.index_sql {
            let on_table = match crate::dialect::parse_statement(dialect, sql)? {
                Some(ast::Cmd::Stmt(ast::Stmt::CreateIndex { tbl_name, .. })) => {
                    crate::util::normalize_ident(tbl_name.as_str())
                }
                _ => return refuse("index_sql has a statement that is not CREATE INDEX"),
            };
            if on_table != name {
                return refuse("an index of index_sql is on another table");
            }
            from_sql.push(UnparsedFromSqlIndex {
                table_name: name.clone(),
                root_page: index_rows.len() as i64 + 2,
                sql: sql.to_string(),
            });
            index_rows.push(IndexRow {
                name: String::new(),
                sql: Some(sql),
            });
        }
        let mut automatic_indices = crate::HashMap::default();
        if !automatic.is_empty() {
            automatic_indices.insert(name.clone(), automatic);
        }

        let mut schema = Schema::new();
        schema.add_btree_table(NamespaceId::ROOT, table.clone())?;
        schema.populate_indices(
            dialect,
            &SymbolTable::new(),
            from_sql,
            automatic_indices,
            false,
        )?;
        let mut indexes = Vec::with_capacity(index_rows.len());
        for index in schema.get_indices(NamespaceId::ROOT, &name) {
            if let Some(reason) = unwritable_index_reason(index) {
                return refuse(reason);
            }
            let row = usize::try_from(index.root_page - 2)
                .ok()
                .and_then(|position| index_rows.get_mut(position));
            let Some(row) = row else {
                unreachable!("each index has the root page of its row");
            };
            row.name.clone_from(&index.name);
            let mut index = Index::clone(index);
            index.root_page = 0;
            indexes.push(Arc::new(index));
        }
        turso_assert!(
            indexes.len() == index_rows.len(),
            "the schema has one index for each index row",
            { "name": name }
        );
        Ok(Self {
            name,
            seeded_table,
            read_only,
            table,
            index_rows,
            indexes,
            seed_rows,
        })
    }
}

/// The read-only table of `seeded_table`. Its columns are the columns that
/// `create_sql` declares, then the hidden columns.
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
    let mut definitions = columns.clone();
    let btree = crate::schema::create_table(seeded_table.name, &body, 0, false)?;
    turso_assert!(btree.has_rowid, "a seeded table must have a rowid", {
        "name": seeded_table.name
    });
    turso_assert!(
        !btree.has_virtual_columns,
        "a seeded table must not have generated columns",
        { "name": seeded_table.name }
    );
    let mut columns: Vec<Column> = btree.columns().to_vec();
    let column_count = columns.len();
    let hidden_definitions = seeded_table
        .hidden_columns
        .iter()
        .map(|column| crate::vtab::column_definition(column.name, "INTEGER HIDDEN"))
        .collect::<Vec<_>>();
    columns.extend(VirtualTable::columns_from_definitions(&hidden_definitions)?);
    definitions.extend(hidden_definitions);
    for (position, column) in columns.iter().enumerate() {
        let name = column.name.as_deref().map(crate::util::normalize_ident);
        turso_assert!(
            !columns[..position]
                .iter()
                .any(|other| other.name.as_deref().map(crate::util::normalize_ident) == name),
            "two columns of a seeded table have the same name",
            { "table": seeded_table.name, "column": name }
        );
    }
    let rowid_column = btree.get_rowid_alias_column().map(|(position, _)| position);
    let table = SeededVirtualTable {
        table: seeded_table,
        definitions,
        rowid_column,
        column_count,
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
    /// The number of columns of `create_sql`. The hidden columns follow
    /// them.
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
    /// The number of columns of `create_sql`. The hidden columns follow
    /// them.
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
        if let Some(hidden) = column.checked_sub(self.column_count) {
            return Ok(Value::from_i64(self.table.hidden_columns[hidden].value));
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

/// Why a [`SeededTableWriter`] cannot write `table`, or None when it can.
/// The insert of a row checks NOT NULL and UNIQUE constraints only, and
/// computes no column.
///
/// [`SeededTableWriter`]: crate::dialect::SeededTableWriter
pub(crate) fn unwritable_table_reason(table: &BTreeTable) -> Option<&'static str> {
    if !table.has_rowid {
        Some("it has no rowid")
    } else if table.is_strict {
        Some("it is STRICT")
    } else if table.has_autoincrement {
        Some("it has AUTOINCREMENT")
    } else if table.has_virtual_columns || table.columns().iter().any(|c| c.is_generated()) {
        Some("it has generated columns")
    } else if !table.check_constraints.is_empty() {
        Some("it has CHECK constraints")
    } else if !table.foreign_keys.is_empty() {
        Some("it has foreign keys")
    } else {
        None
    }
}

/// Why a [`SeededTableWriter`] cannot write `index`, or None when it can.
/// The writer copies the key of an index from the columns of the row.
///
/// [`SeededTableWriter`]: crate::dialect::SeededTableWriter
pub(crate) fn unwritable_index_reason(index: &Index) -> Option<&'static str> {
    if index.is_expression_index() || index.where_clause.is_some() {
        Some("an index has an expression or a WHERE clause")
    } else if index.index_method.is_some() {
        Some("an index uses an index method")
    } else {
        None
    }
}
