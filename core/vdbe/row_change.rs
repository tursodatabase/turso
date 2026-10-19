use std::sync::Arc;

use either::Either;

use crate::numeric::Numeric;
use crate::schema::BTreeTable;
use crate::storage::btree::CursorTrait;
use crate::types::{IOResult, ImmutableRecord, RecordBuf};
use crate::vdbe::builder::CursorType;
use crate::vdbe::{CursorID, Program, ProgramState, Register};
use crate::{turso_assert, Connection, LimboError, Result, Value, ValueRef, MAIN_DB_ID};

/// Where the row changes of one written btree go in the materialized-view
/// change log.
pub(crate) struct ChangeLogRoute {
    pub(crate) table: Arc<BTreeTable>,
    dependent_views: crate::alloc::Vec<String>,
    is_materialized_view: bool,
}

/// Resolve change-log routing from the btree being written, never from the
/// SQL spelling carried in the opcode for tracing/update hooks. Returns None
/// when no materialized view depends on the btree.
#[cold]
pub(crate) fn change_log_route(
    program: &Program,
    state: &ProgramState,
    cursor_id: usize,
) -> Result<Option<ChangeLogRoute>> {
    // Materialized views currently live in main. Writes to temp or attached
    // databases must not be resolved through main's schema just because a
    // table there happens to have the same name or root page.
    if state.cursor_databases.get(cursor_id).copied().flatten() != Some(MAIN_DB_ID) {
        return Ok(None);
    }
    let cursor_table = match program.cursor_ref.get(cursor_id) {
        Some((_, CursorType::BTreeTable(table)))
        | Some((_, CursorType::MaterializedView(table, _))) => table.clone(),
        _ => return Ok(None),
    };
    let schema = program.connection.schema.read();
    // Check for dependent views before doing anything else: a plain write to
    // a table with no materialized views is the overwhelmingly common case,
    // and it must stay as cheap as it was before change-log routing existed,
    // i.e. a single schema-map lookup rather than also resolving the
    // schema-owned table descriptor and the is_materialized_view check below.
    let dependent_views = schema.get_dependent_materialized_views(&cursor_table.name);
    if dependent_views.is_empty() {
        return Ok(None);
    }
    // Maintenance write cursors intentionally use a synthetic physical
    // descriptor that includes the trailing weight column. Resolve the bound
    // cursor back to the schema-owned descriptor so change capture always
    // deals in the relation's logical row shape.
    let Some(table) = schema.get_btree_table(&cursor_table.name) else {
        return Ok(None);
    };
    // A zero root belongs to a cursor whose btree is assigned at runtime
    // (CREATE) or to an ephemeral relation. Otherwise the root is part of the
    // bound relation identity and must still match the current schema.
    if cursor_table.root_page != 0 && cursor_table.root_page != table.root_page {
        return Err(LimboError::InternalError(format!(
            "changed btree {} has root {}, but the current schema binds it to root {}",
            cursor_table.name, cursor_table.root_page, table.root_page
        )));
    }
    // The planner rejects WITHOUT ROWID base tables with dependent
    // materialized views, DROP of a table with dependent views, and temp or
    // attached shadow tables never get a route at all, so every table a
    // route is built for here has a rowid.
    turso_assert!(
        table.has_rowid,
        "a materialized view's base table always has a rowid"
    );
    let is_materialized_view = schema.is_materialized_view(&table.name);
    Ok(Some(ChangeLogRoute {
        table,
        dependent_views,
        is_materialized_view,
    }))
}

/// One row written by `Insert` or removed by `Delete`, with its images as
/// they are stored in the btree.
pub(crate) struct RowChange {
    pub(crate) table: Arc<BTreeTable>,
    pub(crate) database_id: usize,
    pub(crate) rowid: i64,
    pub(crate) images: RowImages,
}

/// The images a row change carries. Which images exist follows from the
/// change itself, so a `RowChange` can never claim an image it does not
/// have.
pub(crate) enum RowImages {
    Insert {
        after: ImmutableRecord,
    },
    Update {
        before: ImmutableRecord,
        after: ImmutableRecord,
    },
    Delete {
        before: ImmutableRecord,
    },
}

impl RowImages {
    fn before(&self) -> Option<&ImmutableRecord> {
        match self {
            RowImages::Insert { .. } => None,
            RowImages::Update { before, .. } | RowImages::Delete { before } => Some(before),
        }
    }

    fn after(&self) -> Option<&ImmutableRecord> {
        match self {
            RowImages::Insert { after } | RowImages::Update { after, .. } => Some(after),
            RowImages::Delete { .. } => None,
        }
    }
}

impl RowChange {
    /// True when an UPDATE writes exactly the bytes that are already stored.
    pub(crate) fn writes_same_record(&self) -> bool {
        match &self.images {
            RowImages::Update { before, after } => before.get_payload() == after.get_payload(),
            RowImages::Insert { .. } | RowImages::Delete { .. } => false,
        }
    }
}

pub(crate) enum RowWrite {
    Insert {
        key_reg: usize,
        record_reg: usize,
        /// False when a `Delete` on this cursor already captured the old
        /// row: an UPDATE that changes the rowid.
        may_replace_row: bool,
    },
    Delete,
}

/// Build the `RowChange` for the write the cursor is positioned for. The
/// cursor must be on the target row (Delete), or at the position where the
/// new row goes (Insert). Reads the stored row at most once.
#[cold]
pub(crate) fn capture_row_change(
    state: &mut ProgramState,
    cursor_id: CursorID,
    table: Arc<BTreeTable>,
    write: RowWrite,
) -> Result<IOResult<RowChange>> {
    let database_id = state.cursor_databases[cursor_id]
        .expect("change_log_route only resolves a route for a cursor bound to a database");
    let change = match write {
        RowWrite::Insert {
            key_reg,
            record_reg,
            may_replace_row,
        } => {
            let rowid = match state.registers[key_reg].get_value() {
                Value::Numeric(Numeric::Integer(rowid)) => *rowid,
                _ => unreachable!("expected integer key in insert"),
            };
            let before = if may_replace_row {
                let cursor = state.get_cursor(cursor_id).as_btree_mut();
                if crate::return_if_io!(cursor.rowid()) == Some(rowid) {
                    Some(crate::return_if_io!(copy_current_record(cursor, rowid)))
                } else {
                    None
                }
            } else {
                None
            };
            let after = record_from_register(&state.registers[record_reg])?;
            let images = match before {
                Some(before) => RowImages::Update { before, after },
                None => RowImages::Insert { after },
            };
            RowChange {
                table,
                database_id,
                rowid,
                images,
            }
        }
        RowWrite::Delete => {
            let cursor = state.get_cursor(cursor_id).as_btree_mut();
            let rowid = crate::return_if_io!(cursor.rowid()).ok_or_else(|| {
                LimboError::InternalError("Cannot delete: no current row".to_string())
            })?;
            let before = crate::return_if_io!(copy_current_record(cursor, rowid));
            RowChange {
                table,
                database_id,
                rowid,
                images: RowImages::Delete { before },
            }
        }
    };
    Ok(IOResult::Done(change))
}

fn copy_current_record(
    cursor: &mut dyn CursorTrait,
    rowid: i64,
) -> Result<IOResult<ImmutableRecord>> {
    let record = crate::return_if_io!(cursor.record()).ok_or_else(|| {
        LimboError::InternalError(format!("cursor is on row {rowid} but has no record"))
    })?;
    let record = ImmutableRecord::copy_payload(record.get_payload(), RecordBuf::alloc())?;
    Ok(IOResult::Done(record))
}

fn record_from_register(register: &Register) -> Result<ImmutableRecord> {
    match register {
        Register::Record(record) => Ok(ImmutableRecord::copy_payload(
            record.get_payload(),
            RecordBuf::alloc(),
        )?),
        Register::Value(value) => {
            let values = [value];
            ImmutableRecord::from_values(values, values.len())
        }
        Register::Aggregate(..) => unreachable!("Cannot insert an aggregate value."),
    }
}

/// A change-log route together with the row change captured for it so far.
/// Boxed into the opcode's state so that a write with no dependent view,
/// which never populates the row change, pays nothing for it.
pub(crate) struct ViewChange {
    pub(crate) route: ChangeLogRoute,
    pub(crate) row_change: Option<RowChange>,
    pub(crate) log_step: ViewLogStep,
}

impl ViewChange {
    pub(crate) fn new(route: ChangeLogRoute) -> Self {
        Self {
            route,
            row_change: None,
            log_step: ViewLogStep::Retraction,
        }
    }
}

/// Progress of `log_row_change_for_views`, kept in the opcode state so that
/// a spill wait can be resumed without logging an image twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ViewLogStep {
    Retraction,
    WaitForRetractionSpill,
    Insertion,
    WaitForInsertionSpill,
}

/// Feed `view_change`'s captured row to the materialized-view change log: an
/// UPDATE is a retraction of the old image followed by an insertion of the
/// new one. The cursor's `CaptureRowChange` sub-state always runs before
/// this is reached, so `row_change` is always set here.
#[cold]
pub(crate) fn log_row_change_for_views(
    connection: &Connection,
    view_change: &mut ViewChange,
) -> Result<IOResult<()>> {
    let change = view_change
        .row_change
        .as_ref()
        .expect("CaptureRowChange always runs before a routed write reaches the change log");
    turso_assert!(
        change.database_id == MAIN_DB_ID,
        "change_log_route only resolves routes for cursors in the main database"
    );
    loop {
        match view_change.log_step {
            ViewLogStep::Retraction => {
                let Some(before) = change.images.before() else {
                    view_change.log_step = ViewLogStep::Insertion;
                    continue;
                };
                let spill_started = log_image(
                    connection,
                    &view_change.route,
                    change,
                    before,
                    ImageSide::Before,
                )?;
                view_change.log_step = if spill_started {
                    ViewLogStep::WaitForRetractionSpill
                } else {
                    ViewLogStep::Insertion
                };
            }
            ViewLogStep::WaitForRetractionSpill => {
                crate::return_if_io!(connection.transaction_changes.finish_spill());
                view_change.log_step = ViewLogStep::Insertion;
            }
            ViewLogStep::Insertion => {
                let Some(after) = change.images.after() else {
                    return Ok(IOResult::Done(()));
                };
                let spill_started = log_image(
                    connection,
                    &view_change.route,
                    change,
                    after,
                    ImageSide::After,
                )?;
                if !spill_started {
                    return Ok(IOResult::Done(()));
                }
                view_change.log_step = ViewLogStep::WaitForInsertionSpill;
            }
            ViewLogStep::WaitForInsertionSpill => {
                crate::return_if_io!(connection.transaction_changes.finish_spill());
                return Ok(IOResult::Done(()));
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ImageSide {
    Before,
    After,
}

/// Returns true when logging started a change-log spill that must finish
/// before anything else is logged.
fn log_image(
    connection: &Connection,
    route: &ChangeLogRoute,
    change: &RowChange,
    image: &ImmutableRecord,
    side: ImageSide,
) -> Result<bool> {
    let table = &change.table;
    let values = image.get_values_owned()?;
    let temp_store = connection.get_temp_store();
    let changes = &connection.transaction_changes;
    if route.is_materialized_view {
        let direction = match side {
            ImageSide::Before => -1,
            ImageSide::After => 1,
        };
        let (values, weight) =
            materialized_view_record_change(&table.name, table, values, direction)?;
        return changes.change(
            table,
            &route.dependent_views,
            change.rowid,
            values,
            weight,
            temp_store,
        );
    }
    let values = normalize_table_change_record(table, change.rowid, values)?;
    match side {
        ImageSide::Before => changes.delete(
            table,
            &route.dependent_views,
            change.rowid,
            values,
            temp_store,
        ),
        ImageSide::After => changes.insert(
            table,
            &route.dependent_views,
            change.rowid,
            values,
            temp_store,
        ),
    }
}

fn materialized_view_record_change(
    table_name: &str,
    table: &BTreeTable,
    mut physical_values: crate::alloc::Vec<Value>,
    direction: isize,
) -> Result<(crate::alloc::Vec<Value>, isize)> {
    let logical_width = table.columns().len();
    if physical_values.len() != logical_width + 1 {
        return Err(LimboError::Corrupt(format!(
            "materialized view {table_name} record has {} fields, expected {} logical fields and one weight",
            physical_values.len(),
            logical_width
        )));
    }
    let stored_weight = match physical_values.pop() {
        Some(Value::Numeric(Numeric::Integer(weight))) => weight,
        _ => {
            return Err(LimboError::Corrupt(format!(
                "materialized view {table_name} record has a non-integer weight"
            )));
        }
    };
    if stored_weight <= 0 {
        return Err(LimboError::Corrupt(format!(
            "materialized view {table_name} record has non-positive weight {stored_weight}"
        )));
    }
    let stored_weight = isize::try_from(stored_weight).map_err(|_| {
        LimboError::Corrupt(format!(
            "materialized view {table_name} weight does not fit the maintenance weight type"
        ))
    })?;
    let weight = stored_weight.checked_mul(direction).ok_or_else(|| {
        LimboError::Corrupt(format!(
            "materialized view {table_name} maintenance weight overflow"
        ))
    })?;
    Ok((physical_values, weight))
}

fn normalize_table_change_record(
    table: &BTreeTable,
    rowid: i64,
    mut values: crate::alloc::Vec<Value>,
) -> Result<crate::alloc::Vec<Value>> {
    if values.len() > table.columns().len() {
        return Err(LimboError::Corrupt(format!(
            "table {} record has {} fields, but the schema has {} columns",
            table.name,
            values.len(),
            table.columns().len()
        )));
    }

    values.try_reserve(table.columns().len() - values.len())?;
    for column in table.columns().iter().skip(values.len()) {
        let mut value = if column.is_rowid_alias() {
            Value::from_i64(rowid)
        } else if let Some(default) = column.default.as_deref() {
            crate::translate::alter::eval_constant_default_value(default)?
        } else {
            Value::Null
        };
        if let Some(converted) = column.affinity().convert(&value) {
            value = match converted {
                Either::Left(ValueRef::Numeric(numeric)) => Value::from(numeric),
                Either::Left(_) => {
                    unreachable!("affinity conversion returned an unexpected borrowed value")
                }
                Either::Right(value) => value,
            };
        }
        values.push(value);
    }

    for (index, column) in table.columns().iter().enumerate() {
        if column.is_rowid_alias() {
            values[index] = Value::from_i64(rowid);
        }
    }
    Ok(values)
}
