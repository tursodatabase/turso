use super::{BTreeCursor, CursorTrait, NotedPayload};
use crate::alloc::{ArcSlice, DynAllocator};
#[cfg(any(test, injected_yields))]
use crate::mvcc::cursor::{cursor_yield_key, CursorYieldPoint};
use crate::mvcc::cursor::{static_iterator_hack, MvccEntry, MvccIterator};
use crate::mvcc::database::{
    MVTableId, MvccReadSnapshot, Row as VersionRow, RowID, RowKey, RowVersions, SortableIndexKey,
    TableWriteCount, TxID, VisibleRow,
};
use crate::mvcc::yield_points::inject_io_yield;
use crate::sync::Arc;
use crate::translate::plan::IterationDirection;
use crate::types::{
    cmp_in_column, IOResult, IOResultOr, ImmutableRecord, ImmutableRecordRef, IndexInfo, KeyInfo,
    SeekKey, SeekOp, SeekResult, ValueIterator,
};
use crate::{return_if_io, turso_assert, Connection, LimboError, MvStore, Result};
use std::cmp::Ordering;
use std::ops::Bound;

/// The version chains of one table or one index that a `BTreeCursor` merges
/// with its cells, in key order.
pub(crate) struct MvccReader<K: MvccKeys> {
    chains: Option<MvccIterator<'static, K::MapKey, DynAllocator>>,
    pub(super) store: Arc<MvStore>,
    pub(super) snapshot: MvccReadSnapshot,
    pub(super) table_id: MVTableId,
    btree: BtreeAccess,
    chain: Chain<K>,
    chain_range: K::Range,
    pub(super) own_table_writes: TableWriteCount,
    pub(super) own_table_writes_seen: u64,
    direction: IterationDirection,
    exact_seek_key: Option<K::SeekKey>,
    cell: Option<K::CellKey>,
    pub(super) row: Row<K>,
    step: Step<K>,
    count_step: CountStep,
    pub(super) keys: K::State,
}

/// How a reader handles the keys of its B-tree: the rowids of a table or the
/// records of an index.
pub(crate) trait MvccKeys: Copy + Sized + 'static {
    type MapKey: 'static;
    type CellKey: Copy;
    type ChainKey: Clone;
    type ChainRow;
    type SeekKey: Clone;
    type Range: Clone;
    type State;

    /// An exact seek looks at one key, and the cursor seeks again before it
    /// moves on from that key.
    const EXACT_SEEK_HAS_ONE_KEY: bool;

    fn reader(cursor: &BTreeCursor) -> &MvccReader<Self>;
    fn reader_mut(cursor: &mut BTreeCursor) -> &mut MvccReader<Self>;
    fn reader_slot(cursor: &mut BTreeCursor) -> &mut Option<Box<MvccReader<Self>>>;

    fn whole_range(direction: IterationDirection) -> Self::Range;
    fn seek_range(key: &Self::SeekKey, op: SeekOp) -> Self::Range;
    fn range_after_row(reader: &MvccReader<Self>) -> Self::Range;
    fn range_direction(range: &Self::Range) -> IterationDirection;

    fn seek_btree(
        cursor: &mut BTreeCursor,
        key: &Self::SeekKey,
        op: SeekOp,
    ) -> IOResultOr<SeekResult>;
    fn read_cell(cursor: &mut BTreeCursor) -> IOResultOr<Option<Self::CellKey>>;

    fn restart_chains(reader: &mut MvccReader<Self>, range: Self::Range) -> Result<()>;
    fn next_chain(reader: &mut MvccReader<Self>) -> Chain<Self>;
    fn has_chains(reader: &MvccReader<Self>) -> bool;
    fn compare_cell_with_chain(
        reader: &MvccReader<Self>,
        cell: Self::CellKey,
        chain: &Self::ChainKey,
    ) -> Result<Ordering>;
    fn pick_is_in_exact_seek(reader: &MvccReader<Self>, pick: Pick) -> Result<bool>;
    fn keep_cell_as_row(reader: &mut MvccReader<Self>);
    fn chain_row(key: &Self::ChainKey, version: VersionRow) -> Self::ChainRow;
    fn chain_row_payload(row: &Self::ChainRow) -> &[u8];
    fn chain_row_rowid(reader: &MvccReader<Self>, row: &Self::ChainRow) -> Option<i64>;
    fn row_seek_key(reader: &MvccReader<Self>) -> Result<Self::SeekKey>;
}

#[derive(Clone, Copy)]
pub(crate) struct TableKeys;

#[derive(Clone, Copy)]
pub(crate) struct IndexKeys;

/// The key of an index cell is in `IndexState::cell_key`.
#[derive(Clone, Copy)]
pub(crate) struct IndexCell;

pub(crate) struct TableChainRow {
    pub(super) rowid: i64,
    pub(super) payload: ArcSlice<u8>,
}

#[derive(Default)]
pub(crate) struct TableState {
    pub(super) held_rowid_lock: Option<HeldRowidLock>,
}

pub(crate) struct IndexState {
    pub(super) index_info: Arc<IndexInfo>,
    /// The key of the cell under the B-tree cursor.
    pub(super) cell_key: Vec<u8>,
    /// The key of the current row when the row is a cell. The cursor keeps it
    /// after the B-tree cursor moves, to seek the version chains after it.
    pub(super) row_key: Vec<u8>,
    seek_infos: Vec<Option<Arc<IndexInfo>>>,
}

/// The rowid allocator lock of a table, which the cursor holds while
/// `NewRowid` reads the largest rowid of the table.
pub(crate) struct HeldRowidLock(pub(super) Arc<crate::mvcc::database::RowidAllocator>);

impl Drop for HeldRowidLock {
    fn drop(&mut self) {
        self.0.unlock();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BtreeAccess {
    Undecided,
    Readable,
    NotReadable,
}

pub(crate) enum Chain<K: MvccKeys> {
    At {
        key: K::ChainKey,
        versions: RowVersions<DynAllocator>,
    },
    Exhausted,
}

pub(crate) enum Row<K: MvccKeys> {
    Unpositioned,
    BeforeFirst,
    AfterLast,
    At {
        from: RowFrom<K>,
        /// The cell under the B-tree cursor has the key of the row.
        cell_has_key: bool,
        /// The chain under the chain iterator has the key of the row.
        chain_has_key: bool,
    },
    /// A row that the cursor wrote away from its cell and chain positions.
    /// The cursor seeks again before it moves on from this row.
    Written(K::ChainRow),
}

pub(crate) enum RowFrom<K: MvccKeys> {
    Cell(K::CellKey),
    Chain(K::ChainRow),
}

#[derive(Clone)]
enum Step<K: MvccKeys> {
    Idle,
    Start {
        motion: Motion<K>,
        range: K::Range,
    },
    Move {
        motion: Motion<K>,
        then: ChainChange<K>,
    },
    ReadCell {
        then: ChainChange<K>,
    },
    Resolve,
    SkipHiddenCell,
}

#[derive(Clone)]
enum Motion<K: MvccKeys> {
    Rewind,
    Last,
    Next,
    Prev,
    Seek { key: K::SeekKey, op: SeekOp },
}

#[derive(Clone)]
enum ChainChange<K: MvccKeys> {
    Restart(K::Range),
    Advance,
    Keep,
}

#[derive(Clone, Copy)]
pub(crate) struct ChainRange {
    start: Bound<i64>,
    end: Bound<i64>,
    direction: IterationDirection,
    one_key: bool,
}

#[derive(Clone)]
pub(crate) struct IndexRange {
    start: IndexRangeStart,
    direction: IterationDirection,
}

#[derive(Clone)]
enum IndexRangeStart {
    Whole,
    Key {
        key: Arc<SortableIndexKey>,
        inclusive: bool,
    },
    AfterRow,
}

#[derive(Clone, Copy)]
enum CountStep {
    Idle,
    Cells,
    Rewind,
    Check { count: usize },
    Advance { count: usize },
}

#[derive(Clone, Copy)]
pub(crate) enum Pick {
    End,
    Cell,
    Chain,
    CellAndChain,
}

impl<K: MvccKeys> MvccReader<K> {
    fn new(
        store: Arc<MvStore>,
        connection: &Arc<Connection>,
        tx_id: TxID,
        root_page: i64,
        keys: K::State,
    ) -> Result<Self> {
        let snapshot = store.read_snapshot(tx_id)?;
        let table_id = if connection.experimental_mvcc_passive_checkpoint_enabled() {
            store
                .try_get_table_id_from_root_page_at(root_page, snapshot.begin_ts)
                .ok_or(LimboError::SchemaUpdated)?
        } else {
            store.get_table_id_from_root_page_at(root_page, snapshot.begin_ts)
        };
        let own_table_writes = store.table_write_count(tx_id, table_id)?;
        Ok(Self {
            chains: None,
            store,
            snapshot,
            table_id,
            btree: BtreeAccess::Undecided,
            chain: Chain::Exhausted,
            chain_range: K::whole_range(IterationDirection::Forwards),
            own_table_writes,
            own_table_writes_seen: 0,
            direction: IterationDirection::Forwards,
            exact_seek_key: None,
            cell: None,
            row: Row::Unpositioned,
            step: Step::Idle,
            count_step: CountStep::Idle,
            keys,
        })
    }
}

impl BTreeCursor {
    #[inline(always)]
    pub(super) fn has_mvcc_reader(&self) -> bool {
        self.mvcc_table_reader.is_some() || self.mvcc_index_reader.is_some()
    }

    pub(crate) fn read_mvcc_table(
        &mut self,
        store: Arc<MvStore>,
        connection: &Arc<Connection>,
        tx_id: TxID,
        root_page: i64,
    ) -> Result<()> {
        let reader = MvccReader::<TableKeys>::new(
            store,
            connection,
            tx_id,
            root_page,
            TableState::default(),
        )?;
        self.mvcc_table_reader = Some(Box::new(reader));
        Ok(())
    }

    pub(crate) fn read_mvcc_index(
        &mut self,
        store: Arc<MvStore>,
        connection: &Arc<Connection>,
        tx_id: TxID,
        root_page: i64,
        index_info: Arc<IndexInfo>,
    ) -> Result<()> {
        let keys = IndexState {
            index_info: index_info.clone(),
            cell_key: Vec::new(),
            row_key: Vec::new(),
            seek_infos: Vec::new(),
        };
        let reader = MvccReader::<IndexKeys>::new(store, connection, tx_id, root_page, keys)?;
        self.index_info = Some(index_info);
        self.mvcc_index_reader = Some(Box::new(reader));
        Ok(())
    }

    pub(super) fn mvcc_rewind<K: MvccKeys>(&mut self) -> IOResultOr<()> {
        if matches!(K::reader(self).step, Step::Idle) {
            self.null_flag = false;
            self.start_mvcc_scan::<K>(IterationDirection::Forwards);
        }
        self.drive_mvcc_step::<K>()
    }

    pub(super) fn mvcc_last<K: MvccKeys>(&mut self) -> IOResultOr<()> {
        if matches!(K::reader(self).step, Step::Idle) {
            self.null_flag = false;
            self.start_mvcc_scan::<K>(IterationDirection::Backwards);
        }
        self.drive_mvcc_step::<K>()
    }

    pub(super) fn mvcc_next<K: MvccKeys>(&mut self) -> IOResultOr<()> {
        if matches!(K::reader(self).step, Step::Idle) {
            if !self.plan_mvcc_step::<K>(IterationDirection::Forwards)? {
                return Ok(IOResult::Done(()));
            }
            inject_io_yield!(self, CursorYieldPoint::NextStart);
        }
        self.drive_mvcc_step::<K>()
    }

    pub(super) fn mvcc_prev<K: MvccKeys>(&mut self) -> IOResultOr<()> {
        if matches!(K::reader(self).step, Step::Idle)
            && !self.plan_mvcc_step::<K>(IterationDirection::Backwards)?
        {
            return Ok(IOResult::Done(()));
        }
        self.drive_mvcc_step::<K>()
    }

    pub(super) fn mvcc_seek_rowid(
        &mut self,
        key: SeekKey<'_>,
        op: SeekOp,
    ) -> IOResultOr<SeekResult> {
        let SeekKey::TableRowId(rowid) = key else {
            return Err(LimboError::InternalError(
                "an MVCC table cursor can only seek a rowid".to_string(),
            )
            .into());
        };
        let result = return_if_io!(self.mvcc_seek::<TableKeys>(op, |_| Ok(rowid)));
        turso_assert!(
            !op.eq_only() || self.mvcc_row_rowid().is_none_or(|found| found == rowid),
            "an exact seek must stop on its key or on no row"
        );
        Ok(IOResult::Done(result))
    }

    pub(super) fn mvcc_seek_index_key(
        &mut self,
        key: SeekKey<'_>,
        op: SeekOp,
    ) -> IOResultOr<SeekResult> {
        let SeekKey::IndexKey(record) = key else {
            return Err(LimboError::InternalError(
                "an MVCC index cursor can only seek an index key".to_string(),
            )
            .into());
        };
        self.mvcc_seek::<IndexKeys>(op, |reader| {
            reader.index_seek_key(record.get_payload(), record.column_count())
        })
    }

    pub(super) fn mvcc_seek_index_registers(
        &mut self,
        registers: &[crate::vdbe::Register],
        op: SeekOp,
    ) -> IOResultOr<SeekResult> {
        self.mvcc_seek::<IndexKeys>(op, |reader| {
            let record = ImmutableRecord::from_registers(registers, registers.len())?;
            reader.index_seek_key(record.get_payload(), registers.len())
        })
    }

    fn mvcc_seek<K: MvccKeys>(
        &mut self,
        op: SeekOp,
        seek_key: impl FnOnce(&mut MvccReader<K>) -> Result<K::SeekKey>,
    ) -> IOResultOr<SeekResult> {
        if matches!(K::reader(self).step, Step::Idle) {
            let key = seek_key(K::reader_mut(self))?;
            self.null_flag = false;
            self.start_mvcc_seek::<K>(key, op);
            inject_io_yield!(self, CursorYieldPoint::SeekStart);
        }
        return_if_io!(self.drive_mvcc_step::<K>());
        Ok(IOResult::Done(match K::reader(self).row {
            Row::At { .. } => SeekResult::Found,
            _ => SeekResult::NotFound,
        }))
    }

    pub(super) fn mvcc_count<K: MvccKeys>(&mut self) -> IOResultOr<usize> {
        loop {
            match K::reader(self).count_step {
                CountStep::Idle => {
                    if K::reader(self).btree == BtreeAccess::Undecided {
                        self.decide_btree_access::<K>();
                    }
                    let reader = K::reader_mut(self);
                    reader.count_step =
                        if reader.btree == BtreeAccess::Readable && !K::has_chains(reader) {
                            CountStep::Cells
                        } else {
                            CountStep::Rewind
                        };
                }
                CountStep::Cells => {
                    let count =
                        return_if_io!(self.without_mvcc_reader::<K, _>(|cursor| cursor.count()));
                    let reader = K::reader_mut(self);
                    reader.count_step = CountStep::Idle;
                    reader.cell = None;
                    reader.row = Row::AfterLast;
                    reader.direction = IterationDirection::Forwards;
                    self.settle_mvcc_row(false, false);
                    return Ok(IOResult::Done(count));
                }
                CountStep::Rewind => {
                    return_if_io!(self.mvcc_rewind::<K>());
                    K::reader_mut(self).count_step = CountStep::Check { count: 0 };
                    inject_io_yield!(self, CursorYieldPoint::CountProgress);
                }
                CountStep::Check { count } => {
                    if !self.has_record && !self.row_is_from_version_chain {
                        K::reader_mut(self).count_step = CountStep::Idle;
                        return Ok(IOResult::Done(count));
                    }
                    K::reader_mut(self).count_step = CountStep::Advance { count: count + 1 };
                }
                CountStep::Advance { count } => {
                    return_if_io!(self.mvcc_next::<K>());
                    K::reader_mut(self).count_step = CountStep::Check { count };
                    inject_io_yield!(self, CursorYieldPoint::CountProgress);
                }
            }
        }
    }

    pub(super) fn version_chain_rowid(&self) -> Option<i64> {
        if !self.row_is_from_version_chain {
            return None;
        }
        if let Some(reader) = self.mvcc_table_reader.as_deref() {
            return reader.chain_row().map(|row| row.rowid);
        }
        let reader = self.mvcc_index_reader.as_deref()?;
        reader
            .chain_row()
            .and_then(|row| IndexKeys::chain_row_rowid(reader, row))
    }

    pub(super) fn version_chain_payload(&self) -> Option<(&'static [u8], usize)> {
        if self.null_flag {
            return None;
        }
        let payload = self.chain_row_payload()?;
        Some((payload, payload.len()))
    }

    pub(super) fn version_chain_record(&mut self) -> Result<Option<&ImmutableRecord>> {
        let Some(payload) = self.chain_row_payload() else {
            return Ok(None);
        };
        let invalidated = self
            .reusable_immutable_record
            .as_ref()
            .is_none_or(|record| record.is_invalidated());
        if invalidated {
            let record = self
                .get_immutable_record_or_create()?
                .expect("record was allocated above");
            record.invalidate();
            record.start_serialization(payload)?;
        }
        Ok(self.reusable_immutable_record.as_ref())
    }

    fn chain_row_payload(&self) -> Option<&'static [u8]> {
        if !self.row_is_from_version_chain {
            return None;
        }
        let payload = if let Some(reader) = self.mvcc_table_reader.as_deref() {
            TableKeys::chain_row_payload(reader.chain_row()?)
        } else {
            IndexKeys::chain_row_payload(self.mvcc_index_reader.as_deref()?.chain_row()?)
        };
        // SAFETY: the bytes belong to the row version that the cursor keeps in
        // `row` until it moves, so they stay valid as long as the page bytes
        // that `fetch_payload_static` returns for a cell.
        Some(unsafe { std::mem::transmute::<&[u8], &'static [u8]>(payload) })
    }

    pub(super) fn forget_mvcc_position(&mut self) {
        self.row_is_from_version_chain = false;
        if let Some(reader) = self.mvcc_table_reader.as_deref_mut() {
            reader.forget_position();
        }
        if let Some(reader) = self.mvcc_index_reader.as_deref_mut() {
            reader.forget_position();
        }
    }

    #[cfg(any(test, injected_yields))]
    pub(super) fn mvcc_yield_key(&self) -> Option<u64> {
        let (tx_id, table_id) = if let Some(reader) = self.mvcc_table_reader.as_deref() {
            (reader.snapshot.tx_id, reader.table_id)
        } else {
            let reader = self.mvcc_index_reader.as_deref()?;
            (reader.snapshot.tx_id, reader.table_id)
        };
        Some(cursor_yield_key(tx_id, table_id))
    }

    fn mvcc_row_rowid(&self) -> Option<i64> {
        let reader = self.mvcc_table_reader.as_deref()?;
        match &reader.row {
            Row::Unpositioned | Row::BeforeFirst | Row::AfterLast => None,
            Row::At { .. } | Row::Written(_) => Some(reader.row_rowid()),
        }
    }

    fn plan_mvcc_step<K: MvccKeys>(&mut self, direction: IterationDirection) -> Result<bool> {
        let reader = K::reader(self);
        let (move_cell, advance_chain) = match &reader.row {
            Row::Unpositioned => return Ok(false),
            Row::BeforeFirst => {
                if direction == IterationDirection::Backwards {
                    return Ok(false);
                }
                self.start_mvcc_scan::<K>(direction);
                return Ok(true);
            }
            Row::AfterLast => {
                if direction == IterationDirection::Forwards {
                    return Ok(false);
                }
                self.start_mvcc_scan::<K>(direction);
                return Ok(true);
            }
            Row::At {
                cell_has_key,
                chain_has_key,
                ..
            } => (*cell_has_key, *chain_has_key),
            Row::Written(_) => (false, false),
        };
        if reader.direction != direction
            || reader.exact_seek_key.is_some()
            || matches!(reader.row, Row::Written(_))
        {
            let key = K::row_seek_key(reader)?;
            let op = match direction {
                IterationDirection::Forwards => SeekOp::GT,
                IterationDirection::Backwards => SeekOp::LT,
            };
            self.start_mvcc_seek::<K>(key, op);
            return Ok(true);
        }
        let reader = K::reader_mut(self);
        reader.chain_range = K::range_after_row(reader);
        if move_cell {
            let motion = match direction {
                IterationDirection::Forwards => Motion::Next,
                IterationDirection::Backwards => Motion::Prev,
            };
            let then = if advance_chain {
                ChainChange::Advance
            } else {
                ChainChange::Keep
            };
            reader.step = Step::Move { motion, then };
            self.has_record = true;
        } else {
            if advance_chain {
                reader.advance_chain();
            }
            reader.step = Step::Resolve;
        }
        Ok(true)
    }

    fn start_mvcc_scan<K: MvccKeys>(&mut self, direction: IterationDirection) {
        let motion = match direction {
            IterationDirection::Forwards => Motion::Rewind,
            IterationDirection::Backwards => Motion::Last,
        };
        let reader = K::reader_mut(self);
        reader.exact_seek_key = None;
        reader.step = Step::Start {
            motion,
            range: K::whole_range(direction),
        };
    }

    fn start_mvcc_seek<K: MvccKeys>(&mut self, key: K::SeekKey, op: SeekOp) {
        let reader = K::reader_mut(self);
        reader.exact_seek_key = op.eq_only().then(|| key.clone());
        reader.step = Step::Start {
            range: K::seek_range(&key, op),
            motion: Motion::Seek { key, op },
        };
    }

    fn start_mvcc_motion<K: MvccKeys>(&mut self, motion: Motion<K>, range: K::Range) -> Result<()> {
        if K::reader(self).btree == BtreeAccess::Undecided {
            self.decide_btree_access::<K>();
        }
        let reader = K::reader_mut(self);
        reader.direction = K::range_direction(&range);
        if reader.btree == BtreeAccess::Readable {
            let on_cell = reader.cell.is_some();
            reader.step = Step::Move {
                motion,
                then: ChainChange::Restart(range),
            };
            self.has_record = on_cell;
        } else {
            reader.cell = None;
            K::restart_chains(reader, range)?;
            reader.step = Step::Resolve;
        }
        Ok(())
    }

    fn decide_btree_access<K: MvccKeys>(&mut self) {
        let reader = K::reader(self);
        let root_page = if reader.store.is_btree_readable_at(
            &reader.table_id,
            reader.snapshot.begin_ts,
            reader.snapshot.read_mark,
        ) {
            reader.store.current_root_page(&reader.table_id)
        } else {
            None
        };
        let access = match root_page {
            Some(root_page) => {
                if self.root_page < 0 {
                    self.root_page = root_page as i64;
                }
                BtreeAccess::Readable
            }
            None => BtreeAccess::NotReadable,
        };
        K::reader_mut(self).btree = access;
    }

    fn drive_mvcc_step<K: MvccKeys>(&mut self) -> IOResultOr<()> {
        loop {
            match K::reader(self).step.clone() {
                Step::Idle => return Ok(IOResult::Done(())),
                Step::Start { motion, range } => self.start_mvcc_motion::<K>(motion, range)?,
                Step::Move { motion, then } => {
                    match motion {
                        Motion::Rewind => {
                            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeForwardProgress)
                        }
                        Motion::Last => {
                            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeBackwardProgress)
                        }
                        Motion::Next => inject_io_yield!(self, CursorYieldPoint::NextBtreeAdvance),
                        Motion::Prev => inject_io_yield!(self, CursorYieldPoint::PrevBtreeAdvance),
                        Motion::Seek { .. } => {
                            inject_io_yield!(self, CursorYieldPoint::SeekBtreeProgress)
                        }
                    }
                    let landed = match &motion {
                        Motion::Rewind => {
                            return_if_io!(
                                self.without_mvcc_reader::<K, _>(|cursor| cursor.rewind())
                            );
                            true
                        }
                        Motion::Last => {
                            return_if_io!(self.without_mvcc_reader::<K, _>(|cursor| cursor.last()));
                            true
                        }
                        Motion::Next => {
                            return_if_io!(self.btree_next());
                            true
                        }
                        Motion::Prev => {
                            return_if_io!(self.btree_prev());
                            true
                        }
                        Motion::Seek { key, op } => !matches!(
                            return_if_io!(K::seek_btree(self, key, *op)),
                            SeekResult::TryAdvance
                        ),
                    };
                    if !landed {
                        let Motion::Seek { op, .. } = motion else {
                            unreachable!("only a seek can stop before it lands on a cell");
                        };
                        let motion = match op.iteration_direction() {
                            IterationDirection::Forwards => Motion::Next,
                            IterationDirection::Backwards => Motion::Prev,
                        };
                        K::reader_mut(self).step = Step::Move { motion, then };
                        continue;
                    }
                    return_if_io!(self.read_mvcc_cell::<K>(then));
                }
                Step::ReadCell { then } => return_if_io!(self.read_mvcc_cell::<K>(then)),
                Step::Resolve => {
                    if self.resolve_mvcc_row::<K>()? {
                        return Ok(IOResult::Done(()));
                    }
                }
                Step::SkipHiddenCell => {
                    match K::reader(self).direction {
                        IterationDirection::Forwards => {
                            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeForwardProgress)
                        }
                        IterationDirection::Backwards => {
                            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeBackwardProgress)
                        }
                    }
                    match K::reader(self).direction {
                        IterationDirection::Forwards => return_if_io!(self.btree_next()),
                        IterationDirection::Backwards => return_if_io!(self.btree_prev()),
                    }
                    return_if_io!(self.read_mvcc_cell::<K>(ChainChange::Advance));
                }
            }
        }
    }

    /// Notes the cell where the B-tree cursor stopped, then changes the chain
    /// iterator as `then` says. An index cell can need IO to read its key, so
    /// on IO the step waits in `ReadCell` and does not move the B-tree again.
    fn read_mvcc_cell<K: MvccKeys>(&mut self, then: ChainChange<K>) -> IOResultOr<()> {
        let cell = match K::read_cell(self)? {
            IOResult::Done(cell) => cell,
            IOResult::IO(io) => {
                K::reader_mut(self).step = Step::ReadCell { then };
                return Ok(IOResult::IO(io));
            }
        };
        let reader = K::reader_mut(self);
        reader.cell = cell;
        match then {
            ChainChange::Restart(range) => K::restart_chains(reader, range)?,
            ChainChange::Advance => reader.advance_chain(),
            ChainChange::Keep => {}
        }
        reader.step = Step::Resolve;
        Ok(IOResult::Done(()))
    }

    pub(super) fn without_mvcc_reader<K: MvccKeys, T>(
        &mut self,
        step: impl FnOnce(&mut BTreeCursor) -> IOResultOr<T>,
    ) -> IOResultOr<T> {
        let reader = K::reader_slot(self).take();
        let result = step(self);
        *K::reader_slot(self) = reader;
        result
    }

    fn resolve_mvcc_row<K: MvccKeys>(&mut self) -> Result<bool> {
        let reader = K::reader_mut(self);
        if reader.own_table_writes_seen != reader.own_table_writes.get() {
            let range = reader.chain_range.clone();
            K::restart_chains(reader, range)?;
        }
        loop {
            let reader = K::reader_mut(self);
            let mut pick = reader.pick()?;
            if reader.exact_seek_key.is_some()
                && !matches!(pick, Pick::End)
                && !K::pick_is_in_exact_seek(reader, pick)?
            {
                pick = Pick::End;
            }
            match pick {
                Pick::End => {
                    self.end_mvcc_row::<K>();
                    return Ok(true);
                }
                Pick::Cell => {
                    reader.set_cell_row(false);
                    self.finish_mvcc_row::<K>(true, false);
                    return Ok(true);
                }
                Pick::Chain => {
                    reader
                        .store
                        .require_readable_transaction(reader.snapshot.tx_id)?;
                    let Chain::At { key, versions } = &reader.chain else {
                        unreachable!("a picked chain must be under the chain iterator");
                    };
                    let visible = reader
                        .store
                        .read_visible_from_versions(reader.snapshot, versions)?;
                    let Some(visible) = visible else {
                        reader.advance_chain();
                        continue;
                    };
                    reader.row = Row::At {
                        from: RowFrom::Chain(K::chain_row(key, visible)),
                        cell_has_key: false,
                        chain_has_key: true,
                    };
                    self.finish_mvcc_row::<K>(false, true);
                    return Ok(true);
                }
                Pick::CellAndChain => {
                    reader
                        .store
                        .require_readable_transaction(reader.snapshot.tx_id)?;
                    let Chain::At { key, versions } = &reader.chain else {
                        unreachable!("a picked chain must be under the chain iterator");
                    };
                    match reader
                        .store
                        .resolve_cell_with_chain(reader.snapshot, versions)?
                    {
                        VisibleRow::Cell => {
                            reader.set_cell_row(true);
                            self.finish_mvcc_row::<K>(true, false);
                        }
                        VisibleRow::Version(visible) => {
                            reader.row = Row::At {
                                from: RowFrom::Chain(K::chain_row(key, visible)),
                                cell_has_key: true,
                                chain_has_key: true,
                            };
                            self.finish_mvcc_row::<K>(false, true);
                        }
                        VisibleRow::NoRow
                            if reader.exact_seek_key.is_some() && K::EXACT_SEEK_HAS_ONE_KEY =>
                        {
                            self.end_mvcc_row::<K>();
                        }
                        VisibleRow::NoRow => {
                            reader.step = Step::SkipHiddenCell;
                            self.has_record = true;
                            return Ok(false);
                        }
                    }
                    return Ok(true);
                }
            }
        }
    }

    fn finish_mvcc_row<K: MvccKeys>(&mut self, cell_row: bool, chain_row: bool) {
        let reader = K::reader_mut(self);
        reader.step = Step::Idle;
        if !K::EXACT_SEEK_HAS_ONE_KEY {
            reader.exact_seek_key = None;
        }
        self.settle_mvcc_row(cell_row, chain_row);
    }

    fn end_mvcc_row<K: MvccKeys>(&mut self) {
        let reader = K::reader_mut(self);
        reader.row = match (&reader.exact_seek_key, reader.direction) {
            (Some(_), _) => Row::Unpositioned,
            (None, IterationDirection::Forwards) => Row::AfterLast,
            (None, IterationDirection::Backwards) => Row::BeforeFirst,
        };
        self.finish_mvcc_row::<K>(false, false);
    }

    pub(super) fn settle_mvcc_row(&mut self, cell_row: bool, chain_row: bool) {
        self.has_record = cell_row;
        self.row_is_from_version_chain = chain_row;
        if let Some(record) = self.reusable_immutable_record.as_mut() {
            record.invalidate();
        }
    }

    fn read_table_cell(&mut self) -> Result<Option<i64>> {
        if !self.has_record {
            return Ok(None);
        }
        let page = self.stack.top_ref();
        let contents = page.get_contents();
        let cell_idx = self.stack.current_cell_index();
        let cell = contents.cell_table_leaf_read_header(cell_idx as usize)?;
        self.noted_payload = NotedPayload {
            start: cell.payload_start as u32,
            size: u32::try_from(cell.payload_size).unwrap_or(0),
        };
        Ok(Some(cell.rowid))
    }

    fn read_index_cell(&mut self) -> IOResultOr<Option<IndexCell>> {
        if !self.has_record {
            return Ok(IOResult::Done(None));
        }
        let (local, _, first_overflow_page) = {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            let cell_idx = self.stack.current_cell_index();
            contents.cell_read_payload_ptr(cell_idx as usize, self.payload_limits)?
        };
        let mut key = std::mem::take(&mut IndexKeys::reader_mut(self).keys.cell_key);
        let copied: IOResultOr<()> = match first_overflow_page {
            None => copy_index_key(&mut key, local).map_err(Into::into),
            Some(_) => match self.record() {
                Ok(IOResult::Done(record)) => {
                    let record = record.expect("a B-tree cursor on a cell must have a record");
                    copy_index_key(&mut key, record.get_payload()).map_err(Into::into)
                }
                Ok(IOResult::IO(io)) => Ok(IOResult::IO(io)),
                Err(err) => Err(err),
            },
        };
        IndexKeys::reader_mut(self).keys.cell_key = key;
        return_if_io!(copied);
        return Ok(IOResult::Done(Some(IndexCell)));

        fn copy_index_key(key: &mut Vec<u8>, payload: &[u8]) -> Result<IOResult<()>> {
            key.clear();
            key.try_reserve(payload.len())?;
            key.extend_from_slice(payload);
            Ok(IOResult::Done(()))
        }
    }
}
impl<K: MvccKeys> MvccReader<K> {
    fn pick(&self) -> Result<Pick> {
        Ok(match (self.cell, &self.chain) {
            (None, Chain::Exhausted) => Pick::End,
            (Some(_), Chain::Exhausted) => Pick::Cell,
            (None, Chain::At { .. }) => Pick::Chain,
            (Some(cell), Chain::At { key, .. }) => {
                match K::compare_cell_with_chain(self, cell, key)? {
                    Ordering::Equal => Pick::CellAndChain,
                    order => {
                        let cell_first = match self.direction {
                            IterationDirection::Forwards => order.is_lt(),
                            IterationDirection::Backwards => order.is_gt(),
                        };
                        if cell_first {
                            Pick::Cell
                        } else {
                            Pick::Chain
                        }
                    }
                }
            }
        })
    }

    fn set_cell_row(&mut self, chain_has_key: bool) {
        let key = self
            .cell
            .expect("a picked cell must be under the B-tree cursor");
        self.row = Row::At {
            from: RowFrom::Cell(key),
            cell_has_key: true,
            chain_has_key,
        };
        K::keep_cell_as_row(self);
    }

    fn advance_chain(&mut self) {
        self.chain = K::next_chain(self);
    }

    pub(super) fn chain_row(&self) -> Option<&K::ChainRow> {
        match &self.row {
            Row::At {
                from: RowFrom::Chain(row),
                ..
            }
            | Row::Written(row) => Some(row),
            _ => None,
        }
    }

    pub(super) fn is_idle(&self) -> bool {
        matches!(self.step, Step::Idle)
    }

    /// Whether the current row is a cell, or `None` when there is no row.
    pub(super) fn row_is_cell(&self) -> Option<bool> {
        match &self.row {
            Row::At {
                from: RowFrom::Cell(_),
                ..
            } => Some(true),
            Row::At {
                from: RowFrom::Chain(_),
                ..
            }
            | Row::Written(_) => Some(false),
            Row::Unpositioned | Row::BeforeFirst | Row::AfterLast => None,
        }
    }

    fn forget_position(&mut self) {
        self.chains = None;
        self.chain = Chain::Exhausted;
        self.exact_seek_key = None;
        self.cell = None;
        self.row = Row::Unpositioned;
        self.step = Step::Idle;
        self.count_step = CountStep::Idle;
    }

    fn mark_chains_current(&mut self) {
        self.own_table_writes_seen = self.own_table_writes.get();
    }
}

impl MvccReader<IndexKeys> {
    /// The key of the current row, or `None` when there is no row.
    pub(super) fn row_key_payload(&self) -> Option<&[u8]> {
        match &self.row {
            Row::At {
                from: RowFrom::Cell(_),
                ..
            } => Some(&self.keys.row_key),
            Row::At {
                from: RowFrom::Chain(key),
                ..
            }
            | Row::Written(key) => Some(key.key.get_payload()),
            Row::Unpositioned | Row::BeforeFirst | Row::AfterLast => None,
        }
    }

    /// The key of a seek, with the index info of its column count, so that
    /// a seek key with fewer columns than the index compares equal to every
    /// entry that starts with it.
    fn index_seek_key(
        &mut self,
        payload: &[u8],
        column_count: usize,
    ) -> Result<Arc<SortableIndexKey>> {
        let metadata = if column_count == self.keys.index_info.num_cols {
            self.keys.index_info.clone()
        } else {
            if self.keys.seek_infos.len() <= column_count {
                self.keys.seek_infos.resize(column_count + 1, None);
            }
            match &self.keys.seek_infos[column_count] {
                Some(metadata) => metadata.clone(),
                None => {
                    let info = self.keys.index_info.as_ref();
                    let metadata = Arc::new(IndexInfo::new_in(
                        info.key_info.iter().cloned(),
                        info.has_rowid,
                        column_count,
                        info.is_unique,
                        self.store.allocator(),
                    )?);
                    self.keys.seek_infos[column_count] = Some(metadata.clone());
                    metadata
                }
            }
        };
        Ok(Arc::new(SortableIndexKey::new_from_payload_in(
            payload,
            metadata,
            self.store.allocator(),
        )?))
    }
}

impl MvccKeys for TableKeys {
    type MapKey = RowID;
    type CellKey = i64;
    type ChainKey = i64;
    type ChainRow = TableChainRow;
    type SeekKey = i64;
    type Range = ChainRange;
    type State = TableState;

    const EXACT_SEEK_HAS_ONE_KEY: bool = true;

    #[inline(always)]
    fn reader(cursor: &BTreeCursor) -> &MvccReader<Self> {
        cursor
            .mvcc_table_reader
            .as_deref()
            .expect("the cursor must read an MVCC table")
    }

    #[inline(always)]
    fn reader_mut(cursor: &mut BTreeCursor) -> &mut MvccReader<Self> {
        cursor
            .mvcc_table_reader
            .as_deref_mut()
            .expect("the cursor must read an MVCC table")
    }

    fn reader_slot(cursor: &mut BTreeCursor) -> &mut Option<Box<MvccReader<Self>>> {
        &mut cursor.mvcc_table_reader
    }

    fn whole_range(direction: IterationDirection) -> ChainRange {
        ChainRange {
            start: Bound::Included(i64::MIN),
            end: Bound::Included(i64::MAX),
            direction,
            one_key: false,
        }
    }

    fn seek_range(rowid: &i64, op: SeekOp) -> ChainRange {
        let rowid = *rowid;
        let (start, end) = match op {
            SeekOp::GE { eq_only: true } | SeekOp::LE { eq_only: true } => {
                (Bound::Included(rowid), Bound::Included(rowid))
            }
            SeekOp::GE { eq_only: false } => (Bound::Included(rowid), Bound::Included(i64::MAX)),
            SeekOp::GT => (Bound::Excluded(rowid), Bound::Included(i64::MAX)),
            SeekOp::LE { eq_only: false } => (Bound::Included(i64::MIN), Bound::Included(rowid)),
            SeekOp::LT => (Bound::Included(i64::MIN), Bound::Excluded(rowid)),
        };
        ChainRange {
            start,
            end,
            direction: op.iteration_direction(),
            one_key: op.eq_only(),
        }
    }

    fn range_after_row(reader: &MvccReader<Self>) -> ChainRange {
        reader.chain_range.after(reader.row_rowid())
    }

    fn range_direction(range: &ChainRange) -> IterationDirection {
        range.direction
    }

    fn seek_btree(cursor: &mut BTreeCursor, rowid: &i64, op: SeekOp) -> IOResultOr<SeekResult> {
        let rowid = *rowid;
        cursor.without_mvcc_reader::<Self, _>(|cursor| cursor.seek(SeekKey::TableRowId(rowid), op))
    }

    #[inline(always)]
    fn read_cell(cursor: &mut BTreeCursor) -> IOResultOr<Option<i64>> {
        Ok(IOResult::Done(cursor.read_table_cell()?))
    }

    fn restart_chains(reader: &mut MvccReader<Self>, range: ChainRange) -> Result<()> {
        reader.chain_range = range;
        reader.mark_chains_current();
        if range.one_key {
            let Bound::Included(rowid) = range.start else {
                unreachable!("a one-key chain range must include its key");
            };
            reader.chains = None;
            let row_id = RowID::new(reader.table_id, RowKey::Int(rowid));
            reader.chain = match reader.store.rows.get(&row_id) {
                Some(entry) => Chain::At {
                    key: rowid,
                    versions: entry.value().clone(),
                },
                None => Chain::Exhausted,
            };
            return Ok(());
        }
        let table_id = reader.table_id;
        let to_row_id =
            |bound: Bound<i64>| bound.map(|rowid| RowID::new(table_id, RowKey::Int(rowid)));
        let bounds = (to_row_id(range.start), to_row_id(range.end));
        let iter: Box<dyn Iterator<Item = MvccEntry<'_, RowID, DynAllocator>> + Send + Sync> =
            match range.direction {
                IterationDirection::Forwards => Box::new(reader.store.rows.range(bounds)),
                IterationDirection::Backwards => Box::new(reader.store.rows.range(bounds).rev()),
            };
        reader.chains = Some(static_iterator_hack!(iter, RowID, DynAllocator));
        reader.advance_chain();
        Ok(())
    }

    fn next_chain(reader: &mut MvccReader<Self>) -> Chain<Self> {
        let Some(chains) = reader.chains.as_mut() else {
            turso_assert!(
                reader.chain_range.one_key,
                "only a one-key chain range has no iterator"
            );
            return Chain::Exhausted;
        };
        match chains.next() {
            Some(entry) => {
                let RowKey::Int(rowid) = entry.key().row_id else {
                    unreachable!("a table row must have an integer key");
                };
                Chain::At {
                    key: rowid,
                    versions: entry.value().clone(),
                }
            }
            None => Chain::Exhausted,
        }
    }

    fn has_chains(reader: &MvccReader<Self>) -> bool {
        let start = RowID::new(reader.table_id, RowKey::Int(i64::MIN));
        let end = RowID::new(reader.table_id, RowKey::Int(i64::MAX));
        reader
            .store
            .rows
            .range((Bound::Included(start), Bound::Included(end)))
            .next()
            .is_some()
    }

    #[inline(always)]
    fn compare_cell_with_chain(
        _reader: &MvccReader<Self>,
        cell: i64,
        chain: &i64,
    ) -> Result<Ordering> {
        Ok(cell.cmp(chain))
    }

    fn pick_is_in_exact_seek(reader: &MvccReader<Self>, pick: Pick) -> Result<bool> {
        let key = match (pick, &reader.chain) {
            (Pick::Chain, Chain::At { key, .. }) => *key,
            _ => reader
                .cell
                .expect("a picked cell must be under the B-tree cursor"),
        };
        Ok(reader.exact_seek_key == Some(key))
    }

    #[inline(always)]
    fn keep_cell_as_row(_reader: &mut MvccReader<Self>) {}

    fn chain_row(key: &i64, version: VersionRow) -> TableChainRow {
        TableChainRow {
            rowid: *key,
            payload: version
                .data
                .expect("a table row version must have a payload"),
        }
    }

    fn chain_row_payload(row: &TableChainRow) -> &[u8] {
        &row.payload
    }

    fn chain_row_rowid(_reader: &MvccReader<Self>, row: &TableChainRow) -> Option<i64> {
        Some(row.rowid)
    }

    fn row_seek_key(reader: &MvccReader<Self>) -> Result<i64> {
        Ok(reader.row_rowid())
    }
}

impl MvccReader<TableKeys> {
    pub(super) fn row_rowid(&self) -> i64 {
        match &self.row {
            Row::At {
                from: RowFrom::Cell(rowid),
                ..
            } => *rowid,
            Row::At {
                from: RowFrom::Chain(row),
                ..
            }
            | Row::Written(row) => row.rowid,
            Row::Unpositioned | Row::BeforeFirst | Row::AfterLast => {
                unreachable!("only a positioned cursor has a row")
            }
        }
    }
}

impl ChainRange {
    fn after(self, rowid: i64) -> Self {
        turso_assert!(
            !self.one_key,
            "a one-key chain range has no rows after its key"
        );
        match self.direction {
            IterationDirection::Forwards => Self {
                start: Bound::Excluded(rowid),
                ..self
            },
            IterationDirection::Backwards => Self {
                end: Bound::Excluded(rowid),
                ..self
            },
        }
    }
}

impl MvccKeys for IndexKeys {
    type MapKey = Arc<SortableIndexKey>;
    type CellKey = IndexCell;
    type ChainKey = Arc<SortableIndexKey>;
    type ChainRow = Arc<SortableIndexKey>;
    type SeekKey = Arc<SortableIndexKey>;
    type Range = IndexRange;
    type State = IndexState;

    const EXACT_SEEK_HAS_ONE_KEY: bool = false;

    #[inline(always)]
    fn reader(cursor: &BTreeCursor) -> &MvccReader<Self> {
        cursor
            .mvcc_index_reader
            .as_deref()
            .expect("the cursor must read an MVCC index")
    }

    #[inline(always)]
    fn reader_mut(cursor: &mut BTreeCursor) -> &mut MvccReader<Self> {
        cursor
            .mvcc_index_reader
            .as_deref_mut()
            .expect("the cursor must read an MVCC index")
    }

    fn reader_slot(cursor: &mut BTreeCursor) -> &mut Option<Box<MvccReader<Self>>> {
        &mut cursor.mvcc_index_reader
    }

    fn whole_range(direction: IterationDirection) -> IndexRange {
        IndexRange {
            start: IndexRangeStart::Whole,
            direction,
        }
    }

    fn seek_range(key: &Arc<SortableIndexKey>, op: SeekOp) -> IndexRange {
        IndexRange {
            start: IndexRangeStart::Key {
                key: key.clone(),
                inclusive: matches!(op, SeekOp::GE { .. } | SeekOp::LE { .. }),
            },
            direction: op.iteration_direction(),
        }
    }

    fn range_after_row(reader: &MvccReader<Self>) -> IndexRange {
        IndexRange {
            start: IndexRangeStart::AfterRow,
            direction: reader.chain_range.direction,
        }
    }

    fn range_direction(range: &IndexRange) -> IterationDirection {
        range.direction
    }

    fn seek_btree(
        cursor: &mut BTreeCursor,
        key: &Arc<SortableIndexKey>,
        op: SeekOp,
    ) -> IOResultOr<SeekResult> {
        let key = ImmutableRecordRef::from_bin_record(key.key.get_payload());
        cursor.without_mvcc_reader::<Self, _>(|cursor| cursor.seek(SeekKey::IndexKey(key), op))
    }

    fn read_cell(cursor: &mut BTreeCursor) -> IOResultOr<Option<IndexCell>> {
        cursor.read_index_cell()
    }

    fn restart_chains(reader: &mut MvccReader<Self>, range: IndexRange) -> Result<()> {
        let start = match &range.start {
            IndexRangeStart::Whole => None,
            IndexRangeStart::Key { key, inclusive } => Some((key.clone(), *inclusive)),
            IndexRangeStart::AfterRow => Some((Self::row_seek_key(reader)?, false)),
        };
        reader.chain_range = range;
        reader.mark_chains_current();
        let direction = reader.chain_range.direction;
        reader.chains = match reader.store.index_rows.get(&reader.table_id) {
            Some(index_rows) => {
                let index_rows = index_rows.value();
                let bound = |key: Arc<SortableIndexKey>, inclusive: bool| {
                    let key = SortableIndexKey::clone(&key);
                    if inclusive {
                        Bound::Included(key)
                    } else {
                        Bound::Excluded(key)
                    }
                };
                let iter: Box<
                    dyn Iterator<Item = MvccEntry<'_, Arc<SortableIndexKey>, DynAllocator>>
                        + Send
                        + Sync,
                > = match (direction, start) {
                    (IterationDirection::Forwards, None) => Box::new(index_rows.iter()),
                    (IterationDirection::Backwards, None) => Box::new(index_rows.iter().rev()),
                    (IterationDirection::Forwards, Some((key, inclusive))) => {
                        Box::new(index_rows.range::<SortableIndexKey, _>((
                            bound(key, inclusive),
                            Bound::Unbounded,
                        )))
                    }
                    (IterationDirection::Backwards, Some((key, inclusive))) => Box::new(
                        index_rows
                            .range::<SortableIndexKey, _>((Bound::Unbounded, bound(key, inclusive)))
                            .rev(),
                    ),
                };
                // SAFETY: an index keeps its map in `MvStore::index_rows` as
                // long as the store lives, and the reader holds the store.
                Some(static_iterator_hack!(
                    iter,
                    Arc<SortableIndexKey>,
                    DynAllocator
                ))
            }
            None => None,
        };
        reader.advance_chain();
        Ok(())
    }

    fn next_chain(reader: &mut MvccReader<Self>) -> Chain<Self> {
        let Some(chains) = reader.chains.as_mut() else {
            return Chain::Exhausted;
        };
        match chains.next() {
            Some(entry) => Chain::At {
                key: entry.key().clone(),
                versions: entry.value().clone(),
            },
            None => Chain::Exhausted,
        }
    }

    fn has_chains(reader: &MvccReader<Self>) -> bool {
        reader
            .store
            .index_rows
            .get(&reader.table_id)
            .is_some_and(|index_rows| index_rows.value().iter().next().is_some())
    }

    fn compare_cell_with_chain(
        reader: &MvccReader<Self>,
        _cell: IndexCell,
        chain: &Arc<SortableIndexKey>,
    ) -> Result<Ordering> {
        let info = reader.keys.index_info.as_ref();
        compare_index_keys(
            &reader.keys.cell_key,
            chain.key.get_payload(),
            &info.key_info[..info.num_cols],
        )
    }

    fn pick_is_in_exact_seek(reader: &MvccReader<Self>, pick: Pick) -> Result<bool> {
        let prefix = reader
            .exact_seek_key
            .as_ref()
            .expect("an exact seek must have a key");
        let key = match (pick, &reader.chain) {
            (Pick::Chain, Chain::At { key, .. }) => key.key.get_payload(),
            _ => &reader.keys.cell_key,
        };
        let info = reader.keys.index_info.as_ref();
        Ok(compare_index_keys(
            key,
            prefix.key.get_payload(),
            &info.key_info[..prefix.metadata.num_cols],
        )?
        .is_eq())
    }

    fn keep_cell_as_row(reader: &mut MvccReader<Self>) {
        std::mem::swap(&mut reader.keys.cell_key, &mut reader.keys.row_key);
    }

    fn chain_row(key: &Arc<SortableIndexKey>, _version: VersionRow) -> Arc<SortableIndexKey> {
        key.clone()
    }

    fn chain_row_payload(row: &Arc<SortableIndexKey>) -> &[u8] {
        row.key.get_payload()
    }

    fn chain_row_rowid(reader: &MvccReader<Self>, row: &Arc<SortableIndexKey>) -> Option<i64> {
        if !reader.keys.index_info.has_rowid {
            return None;
        }
        match row.key.last_value() {
            Some(Ok(crate::types::ValueRef::Numeric(crate::numeric::Numeric::Integer(rowid)))) => {
                Some(rowid)
            }
            _ => unreachable!(
                "index where has_rowid() is true should have an integer rowid as the last value"
            ),
        }
    }

    fn row_seek_key(reader: &MvccReader<Self>) -> Result<Arc<SortableIndexKey>> {
        match &reader.row {
            Row::At {
                from: RowFrom::Cell(_),
                ..
            } => Ok(Arc::new(SortableIndexKey::new_from_payload_in(
                &reader.keys.row_key,
                reader.keys.index_info.clone(),
                reader.store.allocator(),
            )?)),
            Row::At {
                from: RowFrom::Chain(key),
                ..
            }
            | Row::Written(key) => Ok(key.clone()),
            Row::Unpositioned | Row::BeforeFirst | Row::AfterLast => {
                unreachable!("only a positioned cursor can seek after its row")
            }
        }
    }
}

/// Compares two index keys column by column, as `SortableIndexKey` orders
/// the keys of `MvStore::index_rows`.
pub(super) fn compare_index_keys(
    left: &[u8],
    right: &[u8],
    key_info: &[KeyInfo],
) -> Result<Ordering> {
    let mut left = ValueIterator::new(left)?;
    let mut right = ValueIterator::new(right)?;
    for column in key_info {
        let (Some(left), Some(right)) = (left.next(), right.next()) else {
            return Err(LimboError::Corrupt(
                "an index key has fewer columns than its index".to_string(),
            ));
        };
        let order = cmp_in_column(&left?, &right?, column);
        if order.is_ne() {
            return Ok(order);
        }
    }
    Ok(Ordering::Equal)
}
