use crate::coro::{Co, StepContext};
use crate::incremental::operator::{AggregateState, DbspStateCursors};
use crate::numeric::Numeric;
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
use crate::types::IOResultOr;
use crate::types::{IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult};
use crate::{return_if_io, LimboError, Value};

/// A step context whose steps reach the DBSP state cursors. The row read
/// and the row write run on every such context.
pub trait CursorStep: StepContext<Error = Box<LimboError>> {
    fn cursors<'c, 'a>(ctx: &'c mut Self::Ctx<'a>) -> &'c mut DbspStateCursors;
}

/// Reads the aggregate state stored under `rowid`, or None when there is
/// no row with that rowid.
pub async fn read_record<C: CursorStep>(
    co: &mut Co<C>,
    rowid: i64,
) -> Result<Option<AggregateState>, Box<LimboError>> {
    let res = co
        .io(|ctx| {
            C::cursors(ctx)
                .table_cursor
                .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })
        })
        .await;
    if !matches!(res, SeekResult::Found) {
        return Ok(None);
    }
    let state = co
        .io(|ctx| state_of_record(&mut C::cursors(ctx).table_cursor, rowid))
        .await;
    Ok(Some(state))
}

/// The aggregate state in the record the table cursor is on. The blob is
/// in column 3 of (operator_id, zset_id, element_id, value, weight). A
/// NULL value is a plain DISTINCT row, which only tracks a weight.
fn state_of_record(cursor: &mut BTreeCursor, rowid: i64) -> IOResultOr<AggregateState> {
    let record = return_if_io!(cursor.record());
    let r = record.ok_or_else(|| {
        LimboError::InternalError(format!(
            "Found key {:?} in aggregate storage but could not read record",
            SeekKey::TableRowId(rowid)
        ))
    })?;
    let blob = r.get_value(3)?.to_owned()?;

    let (state, _group_key) = match blob {
        Value::Blob(blob) => AggregateState::from_blob(&blob),
        Value::Null => Ok((AggregateState::default(), vec![])),
        _ => Err(LimboError::ParseError(
            "Value in aggregator not blob or null".to_string(),
        )),
    }?;
    Ok(IOResult::Done(state))
}

/// Writes a row with weight management, with the index for the lookup.
/// Adds `weight` to the weight of the row under `index_key` when the row
/// exists, deletes the row when the sum is zero or less, and inserts a
/// new row with `weight` otherwise.
///
/// # Arguments
/// * `index_key` - The key to seek in the index
/// * `record_values` - The record values (without weight) to insert
/// * `weight` - The weight delta to apply
pub async fn write_row<C: CursorStep>(
    co: &mut Co<C>,
    index_key: Vec<Value>,
    record_values: Vec<Value>,
    weight: isize,
) -> Result<(), Box<LimboError>> {
    let index_record = ImmutableRecord::from_values(&index_key, index_key.len())?;
    let res = co
        .io(|ctx| {
            C::cursors(ctx).index_cursor.seek(
                SeekKey::IndexKey(index_record.as_record_ref()),
                SeekOp::GE { eq_only: true },
            )
        })
        .await;
    if !matches!(res, SeekResult::Found) {
        return insert_new_row(co, index_key, record_values, weight).await;
    }

    let rowid = co.io(|ctx| C::cursors(ctx).index_cursor.rowid()).await;
    let rowid = rowid.ok_or_else(|| {
        LimboError::InternalError("Index cursor does not have a valid rowid".to_string())
    })?;

    let table_res = co
        .io(|ctx| {
            C::cursors(ctx)
                .table_cursor
                .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })
        })
        .await;
    if !matches!(table_res, SeekResult::Found) {
        return Err(LimboError::InternalError(
            "Index points to non-existent table row".to_string(),
        )
        .into());
    }

    let existing_weight = co
        .io(|ctx| weight_of_record(&mut C::cursors(ctx).table_cursor))
        .await;
    let final_weight = existing_weight + weight;
    if final_weight <= 0 {
        co.io(|ctx| {
            C::cursors(ctx)
                .table_cursor
                .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })
        })
        .await;
        co.io(|ctx| C::cursors(ctx).table_cursor.delete()).await;
        co.io(|ctx| C::cursors(ctx).index_cursor.delete()).await;
    } else {
        insert_table_row(co, rowid, &record_values, final_weight).await?;
    }
    Ok(())
}

/// The weight of the record the table cursor is on. The weight is always
/// the last value (column 4 in the 5-column structure).
fn weight_of_record(cursor: &mut BTreeCursor) -> IOResultOr<isize> {
    let existing_record = return_if_io!(cursor.record());
    let r = existing_record.ok_or_else(|| {
        LimboError::InternalError("Found rowid in table but could not read record".to_string())
    })?;
    let weight = match r.get_value_opt(4) {
        Some(val) => match val.to_owned()? {
            Value::Numeric(Numeric::Integer(w)) => w as isize,
            _ => {
                return Err(LimboError::InternalError(
                    "Invalid weight value in storage".to_string(),
                )
                .into())
            }
        },
        None => {
            return Err(
                LimboError::InternalError("No weight value found in storage".to_string()).into(),
            )
        }
    };
    Ok(IOResult::Done(weight))
}

/// Inserts a row that does not exist yet: gives it the rowid after the
/// last one, writes it to the table, then adds it to the index.
async fn insert_new_row<C: CursorStep>(
    co: &mut Co<C>,
    index_key: Vec<Value>,
    record_values: Vec<Value>,
    weight: isize,
) -> Result<(), Box<LimboError>> {
    co.io(|ctx| C::cursors(ctx).table_cursor.last()).await;
    let rowid = if co.with(|ctx| C::cursors(ctx).table_cursor.is_empty()) {
        1
    } else {
        match co.io(|ctx| C::cursors(ctx).table_cursor.rowid()).await {
            Some(id) => id + 1,
            None => {
                return Err(LimboError::InternalError(
                    "Table cursor has rows but no valid rowid".to_string(),
                )
                .into())
            }
        }
    };

    // The seek positions the cursor for the insert.
    co.io(|ctx| {
        C::cursors(ctx)
            .table_cursor
            .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: false })
    })
    .await;
    insert_table_row(co, rowid, &record_values, weight).await?;

    // The index has a rowid, so the index key gets the rowid at the end.
    let mut index_values = index_key;
    index_values.push(Value::from_i64(rowid));
    let index_record = ImmutableRecord::from_values(&index_values, index_values.len())?;
    let index_btree_key = BTreeKey::new_index_key(index_record.as_record_ref());
    co.io(|ctx| C::cursors(ctx).index_cursor.insert(&index_btree_key))
        .await;
    Ok(())
}

/// Writes the record with the weight at the end under `rowid`. An insert
/// under a rowid that exists replaces the old record.
async fn insert_table_row<C: CursorStep>(
    co: &mut Co<C>,
    rowid: i64,
    record_values: &[Value],
    final_weight: isize,
) -> Result<(), Box<LimboError>> {
    let mut complete_record = record_values.to_vec();
    complete_record.push(Value::from_i64(final_weight as i64));
    let immutable_record = ImmutableRecord::from_values(&complete_record, complete_record.len())?;
    let btree_key = BTreeKey::new_table_rowid(rowid, Some(&immutable_record));
    co.io(|ctx| C::cursors(ctx).table_cursor.insert(&btree_key))
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coro::{with_handle, BoxedResumable, Runner, YieldSlot};
    use crate::incremental::operator::{create_dbsp_state_index, DbspStateCursors};
    use crate::incremental::yield_test_support::OneShotYieldInjector;
    use crate::mvcc::yield_hooks::YieldPointMarker;
    use crate::storage::btree::{
        BTreeCursor, BTreeWriteYieldPoint, CursorTrait, BTREE_WRITE_YIELD_FAMILY,
    };
    use crate::storage::pager::CreateBTreeFlags;
    use crate::sync::Arc;
    use crate::types::IOCompletions;
    use crate::util::IOExt;
    use crate::{Connection, Database, MemoryIO, SqliteDialect, IO};

    /// A step context of only the state cursors, for the tests.
    struct CursorsStep;

    impl StepContext for CursorsStep {
        type Error = Box<LimboError>;
        type Ctx<'a> = CursorsCtx<'a>;
    }

    struct CursorsCtx<'a> {
        cursors: &'a mut DbspStateCursors,
        io: Option<IOCompletions>,
        err: Option<Box<LimboError>>,
    }

    impl YieldSlot<Box<LimboError>> for CursorsCtx<'_> {
        fn park_io(&mut self, io: IOCompletions) {
            self.io = Some(io);
        }

        fn take_io(&mut self) -> Option<IOCompletions> {
            self.io.take()
        }

        fn park_err(&mut self, err: Box<LimboError>) {
            self.err = Some(err);
        }

        fn take_err(&mut self) -> Option<Box<LimboError>> {
            self.err.take()
        }
    }

    impl CursorStep for CursorsStep {
        fn cursors<'c, 'a>(ctx: &'c mut CursorsCtx<'a>) -> &'c mut DbspStateCursors {
            ctx.cursors
        }
    }

    type WriteRowArgs = (Vec<Value>, Vec<Value>, isize);

    fn write_row_runner() -> BoxedResumable<CursorsStep, WriteRowArgs, ()> {
        Runner::boxed(|co, args| {
            with_handle(co, args, async |co, (index_key, record_values, weight)| {
                write_row(co, index_key, record_values, weight).await
            })
        })
    }

    fn setup() -> (Arc<Connection>, Arc<crate::Pager>, i64, i64) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        let pager = conn.pager.load().clone();
        let _ = pager.io.block(|| pager.allocate_page1());
        let table_root = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .unwrap() as i64;
        let index_root = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .unwrap() as i64;
        (conn, pager, table_root, index_root)
    }

    // ~1200-byte cells stay on the leaf, so enough of them overflow the page and trip
    // AfterInsertOverflowCellBeforeBalance.
    fn page_filling_record(op_id: i64, zset_id: i64, elem_id: i64) -> Vec<Value> {
        vec![
            Value::from_i64(op_id),
            Value::from_i64(zset_id),
            Value::from_i64(elem_id),
            Value::from_slice(&[0xcd_u8; 1200]).unwrap(),
        ]
    }

    /// If the table insert yields mid-balance, `write_row` must re-drive it before
    /// advancing; advancing first strands the overflow cell and the row vanishes.
    #[test]
    fn write_row_completes_yielded_overflowing_table_insert() {
        let (conn, pager, table_root, index_root) = setup();

        let injector = OneShotYieldInjector::new(
            BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
            BTREE_WRITE_YIELD_FAMILY ^ table_root as u64,
        );
        conn.set_yield_injector(Some(injector.clone()));

        let mut table_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
        table_cursor.install_yield_context(&conn);
        let index_def = create_dbsp_state_index(index_root);
        let mut index_cursor =
            BTreeCursor::new_index(pager.clone(), index_root, &index_def, 4).unwrap();
        index_cursor.install_yield_context(&conn);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);
        let mut write = write_row_runner();

        let (op_id, zset_id) = (1i64, 1i64);
        // rowid == elem_id here: a new row gets the last rowid plus one.
        let mut victim_rowid = None;
        for elem_id in 1i64..=200 {
            let index_key = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(elem_id),
            ];
            let record_values = page_filling_record(op_id, zset_id, elem_id);

            pager
                .io
                .block(|| {
                    let mut ctx = CursorsCtx {
                        cursors: &mut cursors,
                        io: None,
                        err: None,
                    };
                    write.resume(&mut ctx, (index_key.clone(), record_values.clone(), 1))
                })
                .unwrap();

            if injector.fired() {
                victim_rowid = Some(elem_id);
                break;
            }
        }
        let victim_rowid =
            victim_rowid.expect("no insert ever overflowed a page; test does not exercise the bug");
        conn.set_yield_injector(None);

        // Fresh cursor: the working one may be parked mid-balance.
        let mut verify_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
        let found = pager
            .io
            .block(|| {
                verify_cursor.seek(
                    SeekKey::TableRowId(victim_rowid),
                    SeekOp::GE { eq_only: true },
                )
            })
            .unwrap();
        assert!(
            matches!(found, SeekResult::Found),
            "table row {victim_rowid} lost: write_row advanced past a yielded (mid-balance) insert"
        );
    }
}
