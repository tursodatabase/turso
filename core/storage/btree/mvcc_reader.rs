use super::{BTreeCursor, CursorTrait, NotedPayload};
use crate::alloc::{ArcSlice, DynAllocator};
#[cfg(any(test, injected_yields))]
use crate::mvcc::cursor::{cursor_yield_key, CursorYieldPoint};
use crate::mvcc::cursor::{static_iterator_hack, MvccEntry, MvccIterator};
use crate::mvcc::database::{
    MVTableId, MvccReadSnapshot, RowID, RowKey, RowVersions, TableWriteCount, TxID, VisibleRow,
};
use crate::mvcc::yield_points::inject_io_yield;
use crate::sync::Arc;
use crate::translate::plan::IterationDirection;
use crate::types::{IOResult, IOResultOr, ImmutableRecord, SeekKey, SeekOp, SeekResult};
use crate::{return_if_io, turso_assert, Connection, LimboError, MvStore, Result};
use std::ops::Bound;

pub(crate) struct MvccReader {
    chains: Option<MvccIterator<'static, RowID, DynAllocator>>,
    store: Arc<MvStore>,
    snapshot: MvccReadSnapshot,
    table_id: MVTableId,
    btree: BtreeAccess,
    chain: Chain,
    chain_range: ChainRange,
    own_table_writes: TableWriteCount,
    own_table_writes_seen: u64,
    direction: IterationDirection,
    exact_seek_key: Option<i64>,
    cell: Option<i64>,
    row: Row,
    step: Step,
    count_step: CountStep,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BtreeAccess {
    Undecided,
    Readable,
    NotReadable,
}

enum Chain {
    At {
        rowid: i64,
        versions: RowVersions<DynAllocator>,
    },
    Exhausted,
}

enum Row {
    Unpositioned,
    BeforeFirst,
    AfterLast,
    Cell(i64),
    Chain { rowid: i64, payload: ArcSlice<u8> },
}

#[derive(Clone, Copy)]
enum Step {
    Idle,
    Start { motion: Motion, range: ChainRange },
    Move { motion: Motion, then: ChainChange },
    Resolve,
    SkipHiddenCell,
}

#[derive(Clone, Copy)]
enum Motion {
    Rewind,
    Last,
    Next,
    Prev,
    Seek { rowid: i64, op: SeekOp },
}

#[derive(Clone, Copy)]
enum ChainChange {
    Restart(ChainRange),
    Advance,
    Keep,
}

#[derive(Clone, Copy)]
struct ChainRange {
    start: Bound<i64>,
    end: Bound<i64>,
    direction: IterationDirection,
    one_key: bool,
}

#[derive(Clone, Copy)]
enum CountStep {
    Idle,
    Cells,
    Rewind,
    Check { count: usize },
    Advance { count: usize },
}

enum Pick {
    End,
    Cell(i64),
    Chain(i64),
    CellAndChain(i64),
}

impl MvccReader {
    pub(crate) fn new(
        store: Arc<MvStore>,
        connection: &Arc<Connection>,
        tx_id: TxID,
        root_page: i64,
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
            chain_range: ChainRange::whole_table(IterationDirection::Forwards),
            own_table_writes,
            own_table_writes_seen: 0,
            direction: IterationDirection::Forwards,
            exact_seek_key: None,
            cell: None,
            row: Row::Unpositioned,
            step: Step::Idle,
            count_step: CountStep::Idle,
        })
    }
}

impl BTreeCursor {
    pub(crate) fn read_mvcc_table(&mut self, reader: MvccReader) {
        self.mvcc_reader = Some(Box::new(reader));
    }

    pub(super) fn mvcc_rewind(&mut self) -> IOResultOr<()> {
        if matches!(self.mvcc().step, Step::Idle) {
            self.null_flag = false;
            self.start_mvcc_scan(IterationDirection::Forwards);
        }
        self.drive_mvcc_step()
    }

    pub(super) fn mvcc_last(&mut self) -> IOResultOr<()> {
        if matches!(self.mvcc().step, Step::Idle) {
            self.null_flag = false;
            self.start_mvcc_scan(IterationDirection::Backwards);
        }
        self.drive_mvcc_step()
    }

    pub(super) fn mvcc_next(&mut self) -> IOResultOr<()> {
        if matches!(self.mvcc().step, Step::Idle) {
            if !self.plan_mvcc_step(IterationDirection::Forwards) {
                return Ok(IOResult::Done(()));
            }
            inject_io_yield!(self, CursorYieldPoint::NextStart);
        }
        self.drive_mvcc_step()
    }

    pub(super) fn mvcc_prev(&mut self) -> IOResultOr<()> {
        if matches!(self.mvcc().step, Step::Idle)
            && !self.plan_mvcc_step(IterationDirection::Backwards)
        {
            return Ok(IOResult::Done(()));
        }
        self.drive_mvcc_step()
    }

    pub(super) fn mvcc_seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        let SeekKey::TableRowId(rowid) = key else {
            return Err(LimboError::InternalError(
                "an MVCC table cursor can only seek a rowid".to_string(),
            )
            .into());
        };
        if matches!(self.mvcc().step, Step::Idle) {
            self.null_flag = false;
            self.start_mvcc_seek(rowid, op);
            inject_io_yield!(self, CursorYieldPoint::SeekStart);
        }
        return_if_io!(self.drive_mvcc_step());
        let found = self.mvcc().row.rowid();
        turso_assert!(
            !op.eq_only() || found.is_none_or(|found_rowid| found_rowid == rowid),
            "an exact seek must stop on its key or on no row"
        );
        Ok(IOResult::Done(match found {
            Some(_) => SeekResult::Found,
            None => SeekResult::NotFound,
        }))
    }

    pub(super) fn mvcc_count(&mut self) -> IOResultOr<usize> {
        loop {
            match self.mvcc().count_step {
                CountStep::Idle => {
                    if self.mvcc().btree == BtreeAccess::Undecided {
                        self.decide_btree_access();
                    }
                    let reader = self.mvcc_mut();
                    reader.count_step =
                        if reader.btree == BtreeAccess::Readable && !reader.table_has_chains() {
                            CountStep::Cells
                        } else {
                            CountStep::Rewind
                        };
                }
                CountStep::Cells => {
                    let count = return_if_io!(self.without_mvcc_reader(|cursor| cursor.count()));
                    let reader = self.mvcc_mut();
                    reader.count_step = CountStep::Idle;
                    reader.cell = None;
                    reader.row = Row::AfterLast;
                    reader.direction = IterationDirection::Forwards;
                    self.settle_mvcc_row(false, false);
                    return Ok(IOResult::Done(count));
                }
                CountStep::Rewind => {
                    return_if_io!(self.mvcc_rewind());
                    self.mvcc_mut().count_step = CountStep::Check { count: 0 };
                    inject_io_yield!(self, CursorYieldPoint::CountProgress);
                }
                CountStep::Check { count } => {
                    if !self.has_record && !self.row_is_from_version_chain {
                        self.mvcc_mut().count_step = CountStep::Idle;
                        return Ok(IOResult::Done(count));
                    }
                    self.mvcc_mut().count_step = CountStep::Advance { count: count + 1 };
                }
                CountStep::Advance { count } => {
                    return_if_io!(self.mvcc_next());
                    self.mvcc_mut().count_step = CountStep::Check { count };
                    inject_io_yield!(self, CursorYieldPoint::CountProgress);
                }
            }
        }
    }

    pub(super) fn version_chain_rowid(&self) -> Option<i64> {
        if !self.row_is_from_version_chain {
            return None;
        }
        self.mvcc().row.rowid()
    }

    pub(super) fn version_chain_payload(&self) -> Option<(&'static [u8], usize)> {
        if self.null_flag || !self.row_is_from_version_chain {
            return None;
        }
        let Row::Chain { payload, .. } = &self.mvcc().row else {
            unreachable!("the current row of the cursor is not from a version chain");
        };
        // SAFETY: the bytes belong to the row version that the cursor keeps in
        // `row` until it moves, so they stay valid as long as the page bytes
        // that `fetch_payload_static` returns for a cell.
        let payload = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(&payload[..]) };
        Some((payload, payload.len()))
    }

    pub(super) fn version_chain_record(&mut self) -> Result<Option<&ImmutableRecord>> {
        if !self.row_is_from_version_chain {
            return Ok(None);
        }
        let invalidated = self
            .reusable_immutable_record
            .as_ref()
            .is_none_or(|record| record.is_invalidated());
        if invalidated {
            let payload = match &self.mvcc().row {
                Row::Chain { payload, .. } => payload.clone(),
                _ => unreachable!("the current row of the cursor is not from a version chain"),
            };
            let record = self
                .get_immutable_record_or_create()?
                .expect("record was allocated above");
            record.invalidate();
            record.start_serialization(&payload)?;
        }
        Ok(self.reusable_immutable_record.as_ref())
    }

    pub(super) fn forget_mvcc_position(&mut self) {
        self.row_is_from_version_chain = false;
        let Some(reader) = self.mvcc_reader.as_deref_mut() else {
            return;
        };
        reader.chains = None;
        reader.chain = Chain::Exhausted;
        reader.exact_seek_key = None;
        reader.cell = None;
        reader.row = Row::Unpositioned;
        reader.step = Step::Idle;
        reader.count_step = CountStep::Idle;
    }

    #[cfg(any(test, injected_yields))]
    pub(super) fn mvcc_yield_key(&self) -> Option<u64> {
        self.mvcc_reader
            .as_deref()
            .map(|reader| cursor_yield_key(reader.snapshot.tx_id, reader.table_id))
    }

    fn plan_mvcc_step(&mut self, direction: IterationDirection) -> bool {
        let reader = self.mvcc();
        let rowid = match reader.row {
            Row::Unpositioned => return false,
            Row::BeforeFirst => {
                if direction == IterationDirection::Backwards {
                    return false;
                }
                self.start_mvcc_scan(direction);
                return true;
            }
            Row::AfterLast => {
                if direction == IterationDirection::Forwards {
                    return false;
                }
                self.start_mvcc_scan(direction);
                return true;
            }
            Row::Cell(rowid) | Row::Chain { rowid, .. } => rowid,
        };
        if reader.direction != direction || reader.exact_seek_key.is_some() {
            let op = match direction {
                IterationDirection::Forwards => SeekOp::GT,
                IterationDirection::Backwards => SeekOp::LT,
            };
            self.start_mvcc_seek(rowid, op);
            return true;
        }
        let move_cell = reader.cell == Some(rowid);
        let advance_chain = reader.chain.rowid() == Some(rowid);
        let reader = self.mvcc_mut();
        reader.chain_range = reader.chain_range.after(rowid);
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
        true
    }

    fn start_mvcc_scan(&mut self, direction: IterationDirection) {
        let motion = match direction {
            IterationDirection::Forwards => Motion::Rewind,
            IterationDirection::Backwards => Motion::Last,
        };
        self.mvcc_mut().step = Step::Start {
            motion,
            range: ChainRange::whole_table(direction),
        };
    }

    fn start_mvcc_seek(&mut self, rowid: i64, op: SeekOp) {
        let (start, end) = match op {
            SeekOp::GE { eq_only: true } | SeekOp::LE { eq_only: true } => {
                (Bound::Included(rowid), Bound::Included(rowid))
            }
            SeekOp::GE { eq_only: false } => (Bound::Included(rowid), Bound::Included(i64::MAX)),
            SeekOp::GT => (Bound::Excluded(rowid), Bound::Included(i64::MAX)),
            SeekOp::LE { eq_only: false } => (Bound::Included(i64::MIN), Bound::Included(rowid)),
            SeekOp::LT => (Bound::Included(i64::MIN), Bound::Excluded(rowid)),
        };
        let range = ChainRange {
            start,
            end,
            direction: op.iteration_direction(),
            one_key: op.eq_only(),
        };
        self.mvcc_mut().step = Step::Start {
            motion: Motion::Seek { rowid, op },
            range,
        };
    }

    fn start_mvcc_motion(&mut self, motion: Motion, range: ChainRange) {
        if self.mvcc().btree == BtreeAccess::Undecided {
            self.decide_btree_access();
        }
        let reader = self.mvcc_mut();
        reader.direction = range.direction;
        if reader.btree == BtreeAccess::Readable {
            let on_cell = reader.cell.is_some();
            reader.step = Step::Move {
                motion,
                then: ChainChange::Restart(range),
            };
            self.has_record = on_cell;
        } else {
            reader.cell = None;
            reader.restart_chains(range);
            reader.step = Step::Resolve;
        }
    }

    fn decide_btree_access(&mut self) {
        let reader = self.mvcc();
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
        self.mvcc_mut().btree = access;
    }

    fn drive_mvcc_step(&mut self) -> IOResultOr<()> {
        loop {
            match self.mvcc().step {
                Step::Idle => return Ok(IOResult::Done(())),
                Step::Start { motion, range } => self.start_mvcc_motion(motion, range),
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
                    let landed = match motion {
                        Motion::Rewind => {
                            return_if_io!(self.without_mvcc_reader(|cursor| cursor.rewind()));
                            true
                        }
                        Motion::Last => {
                            return_if_io!(self.without_mvcc_reader(|cursor| cursor.last()));
                            true
                        }
                        Motion::Next => {
                            return_if_io!(self.without_mvcc_reader(|cursor| cursor.next()));
                            true
                        }
                        Motion::Prev => {
                            return_if_io!(self.without_mvcc_reader(|cursor| cursor.prev()));
                            true
                        }
                        Motion::Seek { rowid, op } => !matches!(
                            return_if_io!(self.without_mvcc_reader(
                                |cursor| cursor.seek(SeekKey::TableRowId(rowid), op)
                            )),
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
                        self.mvcc_mut().step = Step::Move { motion, then };
                        continue;
                    }
                    let cell = self.cell_under_btree_cursor()?;
                    let reader = self.mvcc_mut();
                    reader.cell = cell;
                    match then {
                        ChainChange::Restart(range) => reader.restart_chains(range),
                        ChainChange::Advance => reader.advance_chain(),
                        ChainChange::Keep => {}
                    }
                    reader.step = Step::Resolve;
                }
                Step::Resolve => {
                    if self.resolve_mvcc_row()? {
                        return Ok(IOResult::Done(()));
                    }
                }
                Step::SkipHiddenCell => {
                    match self.mvcc().direction {
                        IterationDirection::Forwards => {
                            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeForwardProgress)
                        }
                        IterationDirection::Backwards => {
                            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeBackwardProgress)
                        }
                    }
                    match self.mvcc().direction {
                        IterationDirection::Forwards => {
                            return_if_io!(self.without_mvcc_reader(|cursor| cursor.next()))
                        }
                        IterationDirection::Backwards => {
                            return_if_io!(self.without_mvcc_reader(|cursor| cursor.prev()))
                        }
                    }
                    let cell = self.cell_under_btree_cursor()?;
                    let reader = self.mvcc_mut();
                    reader.cell = cell;
                    reader.advance_chain();
                    reader.step = Step::Resolve;
                }
            }
        }
    }

    fn without_mvcc_reader<T>(
        &mut self,
        step: impl FnOnce(&mut BTreeCursor) -> IOResultOr<T>,
    ) -> IOResultOr<T> {
        let reader = self.mvcc_reader.take();
        let result = step(self);
        self.mvcc_reader = reader;
        result
    }

    fn resolve_mvcc_row(&mut self) -> Result<bool> {
        let reader = self.mvcc_mut();
        if reader.own_table_writes_seen != reader.own_table_writes.get() {
            let range = reader.chain_range;
            reader.restart_chains(range);
        }
        loop {
            let reader = self.mvcc_mut();
            match reader.pick() {
                Pick::End => {
                    self.end_mvcc_row();
                    return Ok(true);
                }
                Pick::Cell(rowid) => {
                    reader.row = Row::Cell(rowid);
                    reader.step = Step::Idle;
                    self.settle_mvcc_row(true, false);
                    return Ok(true);
                }
                Pick::Chain(rowid) => {
                    reader
                        .store
                        .require_readable_transaction(reader.snapshot.tx_id)?;
                    let visible = reader
                        .store
                        .read_visible_from_versions(reader.snapshot, reader.chain.versions())?;
                    let Some(visible) = visible else {
                        reader.advance_chain();
                        continue;
                    };
                    let payload = visible
                        .data
                        .expect("a table row version must have a payload");
                    reader.row = Row::Chain { rowid, payload };
                    reader.step = Step::Idle;
                    self.settle_mvcc_row(false, true);
                    return Ok(true);
                }
                Pick::CellAndChain(rowid) => {
                    reader
                        .store
                        .require_readable_transaction(reader.snapshot.tx_id)?;
                    match reader
                        .store
                        .resolve_cell_with_chain(reader.snapshot, reader.chain.versions())?
                    {
                        VisibleRow::Cell => {
                            reader.row = Row::Cell(rowid);
                            reader.step = Step::Idle;
                            self.settle_mvcc_row(true, false);
                        }
                        VisibleRow::Version(payload) => {
                            reader.row = Row::Chain { rowid, payload };
                            reader.step = Step::Idle;
                            self.settle_mvcc_row(false, true);
                        }
                        VisibleRow::NoRow if reader.exact_seek_key.is_some() => {
                            self.end_mvcc_row();
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

    fn end_mvcc_row(&mut self) {
        let reader = self.mvcc_mut();
        reader.row = match (reader.exact_seek_key, reader.direction) {
            (Some(_), _) => Row::Unpositioned,
            (None, IterationDirection::Forwards) => Row::AfterLast,
            (None, IterationDirection::Backwards) => Row::BeforeFirst,
        };
        reader.step = Step::Idle;
        self.settle_mvcc_row(false, false);
    }

    fn settle_mvcc_row(&mut self, cell_row: bool, chain_row: bool) {
        self.has_record = cell_row;
        self.row_is_from_version_chain = chain_row;
        if let Some(record) = self.reusable_immutable_record.as_mut() {
            record.invalidate();
        }
    }

    fn cell_under_btree_cursor(&mut self) -> Result<Option<i64>> {
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

    fn mvcc(&self) -> &MvccReader {
        self.mvcc_reader
            .as_deref()
            .expect("the cursor must read an MVCC table")
    }

    fn mvcc_mut(&mut self) -> &mut MvccReader {
        self.mvcc_reader
            .as_deref_mut()
            .expect("the cursor must read an MVCC table")
    }
}

impl MvccReader {
    fn pick(&self) -> Pick {
        if let Some(key) = self.exact_seek_key {
            return match (self.cell == Some(key), self.chain.rowid() == Some(key)) {
                (true, true) => Pick::CellAndChain(key),
                (true, false) => Pick::Cell(key),
                (false, true) => Pick::Chain(key),
                (false, false) => Pick::End,
            };
        }
        match (self.cell, self.chain.rowid()) {
            (None, None) => Pick::End,
            (Some(cell), None) => Pick::Cell(cell),
            (None, Some(chain)) => Pick::Chain(chain),
            (Some(cell), Some(chain)) if cell == chain => Pick::CellAndChain(cell),
            (Some(cell), Some(chain)) => {
                let cell_first = match self.direction {
                    IterationDirection::Forwards => cell < chain,
                    IterationDirection::Backwards => cell > chain,
                };
                if cell_first {
                    Pick::Cell(cell)
                } else {
                    Pick::Chain(chain)
                }
            }
        }
    }

    fn restart_chains(&mut self, range: ChainRange) {
        self.chain_range = range;
        self.own_table_writes_seen = self.own_table_writes.get();
        self.exact_seek_key = None;
        if range.one_key {
            let Bound::Included(rowid) = range.start else {
                unreachable!("a one-key chain range must include its key");
            };
            self.exact_seek_key = Some(rowid);
            self.chains = None;
            let row_id = RowID::new(self.table_id, RowKey::Int(rowid));
            self.chain = match self.store.rows.get(&row_id) {
                Some(entry) => Chain::At {
                    rowid,
                    versions: entry.value().clone(),
                },
                None => Chain::Exhausted,
            };
            return;
        }
        let table_id = self.table_id;
        let to_row_id =
            |bound: Bound<i64>| bound.map(|rowid| RowID::new(table_id, RowKey::Int(rowid)));
        let bounds = (to_row_id(range.start), to_row_id(range.end));
        let iter: Box<dyn Iterator<Item = MvccEntry<'_, RowID, DynAllocator>> + Send + Sync> =
            match range.direction {
                IterationDirection::Forwards => Box::new(self.store.rows.range(bounds)),
                IterationDirection::Backwards => Box::new(self.store.rows.range(bounds).rev()),
            };
        self.chains = Some(static_iterator_hack!(iter, RowID, DynAllocator));
        self.advance_chain();
    }

    fn table_has_chains(&self) -> bool {
        let start = RowID::new(self.table_id, RowKey::Int(i64::MIN));
        let end = RowID::new(self.table_id, RowKey::Int(i64::MAX));
        self.store
            .rows
            .range((Bound::Included(start), Bound::Included(end)))
            .next()
            .is_some()
    }

    fn advance_chain(&mut self) {
        let Some(chains) = self.chains.as_mut() else {
            turso_assert!(
                self.exact_seek_key.is_some(),
                "only a one-key chain range has no iterator"
            );
            self.chain = Chain::Exhausted;
            return;
        };
        self.chain = match chains.next() {
            Some(entry) => {
                let RowKey::Int(rowid) = entry.key().row_id else {
                    unreachable!("a table row must have an integer key");
                };
                Chain::At {
                    rowid,
                    versions: entry.value().clone(),
                }
            }
            None => Chain::Exhausted,
        };
    }
}

impl ChainRange {
    fn whole_table(direction: IterationDirection) -> Self {
        Self {
            start: Bound::Included(i64::MIN),
            end: Bound::Included(i64::MAX),
            direction,
            one_key: false,
        }
    }

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

impl Chain {
    fn rowid(&self) -> Option<i64> {
        match self {
            Chain::At { rowid, .. } => Some(*rowid),
            Chain::Exhausted => None,
        }
    }

    fn versions(&self) -> &RowVersions<DynAllocator> {
        match self {
            Chain::At { versions, .. } => versions,
            Chain::Exhausted => unreachable!("an exhausted chain iterator has no versions"),
        }
    }
}

impl Row {
    fn rowid(&self) -> Option<i64> {
        match self {
            Row::Cell(rowid) | Row::Chain { rowid, .. } => Some(*rowid),
            Row::Unpositioned | Row::BeforeFirst | Row::AfterLast => None,
        }
    }
}
