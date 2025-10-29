//! The virtual database engine (VDBE).
//!
//! The VDBE is a register-based virtual machine that execute bytecode
//! instructions that represent SQL statements. When an application prepares
//! an SQL statement, the statement is compiled into a sequence of bytecode
//! instructions that perform the needed operations, such as reading or
//! writing to a b-tree, sorting, or aggregating data.
//!
//! The instruction set of the VDBE is similar to SQLite's instruction set,
//! but with the exception that bytecodes that perform I/O operations are
//! return execution back to the caller instead of blocking. This is because
//! Turso is designed for applications that need high concurrency such as
//! serverless runtimes. In addition, asynchronous I/O makes storage
//! disaggregation easier.
//!
//! You can find a full list of SQLite opcodes at:
//!
//! https://www.sqlite.org/opcode.html

pub mod builder;
pub mod execute;
pub mod explain;
pub mod insn;
pub mod likeop;
pub mod metrics;
pub mod sorter;

use crate::{
    error::LimboError,
    function::{AggFunc, FuncCtx},
    mvcc::{database::CommitStateMachine, LocalClock},
    return_if_io,
    state_machine::StateMachine,
    storage::{pager::PagerCommitResult, sqlite3_ondisk::SmallVec},
    translate::{collate::CollationSeq, plan::TableReferences},
    types::{IOCompletions, IOResult},
    vdbe::{
        execute::{
            OpCheckpointState, OpColumnState, OpDeleteState, OpDeleteSubState, OpDestroyState,
            OpIdxInsertState, OpInsertState, OpInsertSubState, OpNewRowidState, OpNoConflictState,
            OpRowIdState, OpSeekState, OpTransactionState,
        },
        metrics::StatementMetrics,
    },
    ValueRef,
};

use crate::{
    storage::pager::Pager,
    translate::plan::ResultSetColumn,
    types::{AggContext, Cursor, ImmutableRecord, Value},
    vdbe::{builder::CursorType, insn::Insn},
};

#[cfg(feature = "json")]
use crate::json::JsonCacheCell;
use crate::{Connection, MvStore, Result, TransactionState};
use builder::{CursorKey, QueryMode};
use execute::{
    InsnFunction, InsnFunctionStepResult, OpIdxDeleteState, OpIntegrityCheckState,
    OpOpenEphemeralState,
};

use explain::{insn_to_row_with_comment, EXPLAIN_COLUMNS, EXPLAIN_QUERY_PLAN_COLUMNS};
use regex::Regex;
use std::{
    collections::HashMap,
    num::NonZero,
    sync::{
        atomic::{AtomicI64, AtomicIsize, Ordering},
        Arc,
    },
    task::Waker,
};
use tracing::{instrument, Level};

/// State machine for committing view deltas with I/O handling
#[derive(Debug, Clone)]
pub enum ViewDeltaCommitState {
    NotStarted,
    Processing {
        views: Vec<String>, // view names (all materialized views have storage)
        current_index: usize,
    },
    Done,
}

/// We use labels to indicate that we want to jump to whatever the instruction offset
/// will be at runtime, because the offset cannot always be determined when the jump
/// instruction is created.
///
/// In some cases, we want to jump to EXACTLY a specific instruction.
/// - Example: a condition is not met, so we want to jump to wherever Halt is.
///
/// In other cases, we don't care what the exact instruction is, but we know that we
/// want to jump to whatever comes AFTER a certain instruction.
/// - Example: a Next instruction will want to jump to "whatever the start of the loop is",
///   but it doesn't care what instruction that is.
///
/// The reason this distinction is important is that we might reorder instructions that are
/// constant at compile time, and when we do that, we need to change the offsets of any impacted
/// jump instructions, so the instruction that comes immediately after "next Insn" might have changed during the reordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JumpTarget {
    ExactlyThisInsn,
    AfterThisInsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Represents a target for a jump instruction.
/// Stores 32-bit ints to keep the enum word-sized.
pub enum BranchOffset {
    /// A label is a named location in the program.
    /// If there are references to it, it must always be resolved to an Offset
    /// via program.resolve_label().
    Label(u32),
    /// An offset is a direct index into the instruction list.
    Offset(InsnReference),
    /// A placeholder is a temporary value to satisfy the compiler.
    /// It must be set later.
    Placeholder,
}

impl BranchOffset {
    /// Returns true if the branch offset is a label.
    pub fn is_label(&self) -> bool {
        matches!(self, BranchOffset::Label(_))
    }

    /// Returns true if the branch offset is an offset.
    pub fn is_offset(&self) -> bool {
        matches!(self, BranchOffset::Offset(_))
    }

    /// Returns the offset value. Panics if the branch offset is a label or placeholder.
    pub fn as_offset_int(&self) -> InsnReference {
        match self {
            BranchOffset::Label(v) => unreachable!("Unresolved label: {}", v),
            BranchOffset::Offset(v) => *v,
            BranchOffset::Placeholder => unreachable!("Unresolved placeholder"),
        }
    }

    /// Returns the branch offset as a signed integer.
    /// Used in explain output, where we don't want to panic in case we have an unresolved
    /// label or placeholder.
    pub fn as_debug_int(&self) -> i32 {
        match self {
            BranchOffset::Label(v) => *v as i32,
            BranchOffset::Offset(v) => *v as i32,
            BranchOffset::Placeholder => i32::MAX,
        }
    }

    /// Adds an integer value to the branch offset.
    /// Returns a new branch offset.
    /// Panics if the branch offset is a label or placeholder.
    pub fn add<N: Into<u32>>(self, n: N) -> BranchOffset {
        BranchOffset::Offset(self.as_offset_int() + n.into())
    }

    pub fn sub<N: Into<u32>>(self, n: N) -> BranchOffset {
        BranchOffset::Offset(self.as_offset_int() - n.into())
    }
}

pub type CursorID = usize;

pub type PageIdx = i64;

// Index of insn in list of insns
type InsnReference = u32;

#[derive(Debug)]
pub enum StepResult {
    Done,
    IO,
    Row,
    Interrupt,
    Busy,
}

struct RegexCache {
    like: HashMap<String, Regex>,
    glob: HashMap<String, Regex>,
}

impl RegexCache {
    fn new() -> Self {
        Self {
            like: HashMap::new(),
            glob: HashMap::new(),
        }
    }
}

struct Bitfield<const N: usize>([u64; N]);

impl<const N: usize> Bitfield<N> {
    fn new() -> Self {
        Self([0; N])
    }

    fn set(&mut self, bit: usize) {
        assert!(bit < N * 64, "bit out of bounds");
        self.0[bit / 64] |= 1 << (bit % 64);
    }

    fn unset(&mut self, bit: usize) {
        assert!(bit < N * 64, "bit out of bounds");
        self.0[bit / 64] &= !(1 << (bit % 64));
    }

    fn get(&self, bit: usize) -> bool {
        assert!(bit < N * 64, "bit out of bounds");
        (self.0[bit / 64] & (1 << (bit % 64))) != 0
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
/// The commit state of the program.
/// There are two states:
/// - Ready: The program is ready to run the next instruction, or has shut down after
///   the last instruction.
/// - Committing: The program is committing a write transaction. It is waiting for the pager to finish flushing the cache to disk,
///   primarily to the WAL, but also possibly checkpointing the WAL to the database file.
enum CommitState {
    Ready,
    Committing,
    CommitingMvcc {
        state_machine: StateMachine<CommitStateMachine<LocalClock>>,
    },
}

#[derive(Debug, Clone)]
pub enum Register {
    Value(Value),
    Aggregate(AggContext),
    Record(ImmutableRecord),
}

impl Register {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Register::Value(Value::Null))
    }
}

/// A row is a the list of registers that hold the values for a filtered row. This row is a pointer, therefore
/// after stepping again, row will be invalidated to be sure it doesn't point to somewhere unexpected.
#[derive(Debug)]
pub struct Row {
    values: *const Register,
    count: usize,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
unsafe impl Send for Row {}
unsafe impl Sync for Row {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TxnCleanup {
    None,
    RollbackTxn,
}

/// The program state describes the environment in which the program executes.
pub struct ProgramState {
    pub io_completions: Option<IOCompletions>,
    pub pc: InsnReference,
    pub(crate) cursors: Vec<Option<Cursor>>,
    cursor_seqs: Vec<i64>,
    registers: Vec<Register>,
    pub(crate) result_row: Option<Row>,
    last_compare: Option<std::cmp::Ordering>,
    deferred_seeks: Vec<Option<(CursorID, CursorID)>>,
    ended_coroutine: Bitfield<4>, // flag to indicate that a coroutine has ended (key is the yield register. currently we assume that the yield register is always between 0-255, YOLO)
    /// Indicate whether an [Insn::Once] instruction at a given program counter position has already been executed, well, once.
    once: SmallVec<u32, 4>,
    regex_cache: RegexCache,
    interrupted: bool,
    pub parameters: HashMap<NonZero<usize>, Value>,
    commit_state: CommitState,
    #[cfg(feature = "json")]
    json_cache: JsonCacheCell,
    op_delete_state: OpDeleteState,
    op_destroy_state: OpDestroyState,
    op_idx_delete_state: Option<OpIdxDeleteState>,
    op_integrity_check_state: OpIntegrityCheckState,
    /// Metrics collected during statement execution
    pub metrics: StatementMetrics,
    op_open_ephemeral_state: OpOpenEphemeralState,
    op_new_rowid_state: OpNewRowidState,
    op_idx_insert_state: OpIdxInsertState,
    op_insert_state: OpInsertState,
    op_no_conflict_state: OpNoConflictState,
    seek_state: OpSeekState,
    /// Current collation sequence set by OP_CollSeq instruction
    current_collation: Option<CollationSeq>,
    op_column_state: OpColumnState,
    op_row_id_state: OpRowIdState,
    op_transaction_state: OpTransactionState,
    op_checkpoint_state: OpCheckpointState,
    /// State machine for committing view deltas with I/O handling
    view_delta_state: ViewDeltaCommitState,
    /// Marker which tells about auto transaction cleanup necessary for that connection in case of reset
    /// This is used when statement in auto-commit mode reseted after previous uncomplete execution - in which case we may need to rollback transaction started on previous attempt
    /// Note, that MVCC transactions are always explicit - so they do not update auto_txn_cleanup marker
    pub(crate) auto_txn_cleanup: TxnCleanup,
    /// Number of deferred foreign key violations when the statement started.
    /// When a statement subtransaction rolls back, the connection's deferred foreign key violations counter
    /// is reset to this value.
    fk_deferred_violations_when_stmt_started: AtomicIsize,
    /// Number of immediate foreign key violations that occurred during the active statement. If nonzero,
    /// the statement subtransactionwill roll back.
    fk_immediate_violations_during_stmt: AtomicIsize,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
unsafe impl Send for ProgramState {}
unsafe impl Sync for ProgramState {}

impl ProgramState {
    pub fn new(max_registers: usize, max_cursors: usize) -> Self {
        let cursors: Vec<Option<Cursor>> = (0..max_cursors).map(|_| None).collect();
        let cursor_seqs = vec![0i64; max_cursors];
        let registers = vec![Register::Value(Value::Null); max_registers];
        Self {
            io_completions: None,
            pc: 0,
            cursors,
            cursor_seqs,
            registers,
            result_row: None,
            last_compare: None,
            deferred_seeks: vec![None; max_cursors],
            ended_coroutine: Bitfield::new(),
            once: SmallVec::<u32, 4>::new(),
            regex_cache: RegexCache::new(),
            interrupted: false,
            parameters: HashMap::new(),
            commit_state: CommitState::Ready,
            #[cfg(feature = "json")]
            json_cache: JsonCacheCell::new(),
            op_delete_state: OpDeleteState {
                sub_state: OpDeleteSubState::MaybeCaptureRecord,
                deleted_record: None,
            },
            op_destroy_state: OpDestroyState::CreateCursor,
            op_idx_delete_state: None,
            op_integrity_check_state: OpIntegrityCheckState::Start,
            metrics: StatementMetrics::new(),
            op_open_ephemeral_state: OpOpenEphemeralState::Start,
            op_new_rowid_state: OpNewRowidState::Start,
            op_idx_insert_state: OpIdxInsertState::MaybeSeek,
            op_insert_state: OpInsertState {
                sub_state: OpInsertSubState::MaybeCaptureRecord,
                old_record: None,
            },
            op_no_conflict_state: OpNoConflictState::Start,
            seek_state: OpSeekState::Start,
            current_collation: None,
            op_column_state: OpColumnState::Start,
            op_row_id_state: OpRowIdState::Start,
            op_transaction_state: OpTransactionState::Start,
            op_checkpoint_state: OpCheckpointState::StartCheckpoint,
            view_delta_state: ViewDeltaCommitState::NotStarted,
            auto_txn_cleanup: TxnCleanup::None,
            fk_deferred_violations_when_stmt_started: AtomicIsize::new(0),
            fk_immediate_violations_during_stmt: AtomicIsize::new(0),
        }
    }

    pub fn set_register(&mut self, idx: usize, value: Register) {
        self.registers[idx] = value;
    }

    pub fn get_register(&self, idx: usize) -> &Register {
        &self.registers[idx]
    }

    pub fn column_count(&self) -> usize {
        self.registers.len()
    }

    pub fn column(&self, i: usize) -> Option<String> {
        Some(format!("{:?}", self.registers[i]))
    }

    pub fn interrupt(&mut self) {
        self.interrupted = true;
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted
    }

    pub fn bind_at(&mut self, index: NonZero<usize>, value: Value) {
        self.parameters.insert(index, value);
    }

    pub fn clear_bindings(&mut self) {
        self.parameters.clear();
    }

    pub fn get_parameter(&self, index: NonZero<usize>) -> Value {
        self.parameters.get(&index).cloned().unwrap_or(Value::Null)
    }

    pub fn reset(&mut self, max_registers: Option<usize>, max_cursors: Option<usize>) {
        self.pc = 0;

        if let Some(max_cursors) = max_cursors {
            self.cursors.resize_with(max_cursors, || None);
            self.cursor_seqs.resize(max_cursors, 0);
        }
        if let Some(max_registers) = max_registers {
            self.registers
                .resize_with(max_registers, || Register::Value(Value::Null));
        }
        // reset cursors as they can have cached information which will be no longer relevant on next program execution
        self.cursors.iter_mut().for_each(|c| {
            let _ = c.take();
        });
        self.registers
            .iter_mut()
            .for_each(|r| *r = Register::Value(Value::Null));
        self.last_compare = None;
        self.deferred_seeks.iter_mut().for_each(|s| *s = None);
        self.ended_coroutine.0 = [0; 4];
        self.regex_cache.like.clear();
        self.interrupted = false;
        self.current_collation = None;
        #[cfg(feature = "json")]
        self.json_cache.clear();

        // Reset state machines
        self.op_delete_state = OpDeleteState {
            sub_state: OpDeleteSubState::MaybeCaptureRecord,
            deleted_record: None,
        };
        self.op_idx_delete_state = None;
        self.op_integrity_check_state = OpIntegrityCheckState::Start;
        self.metrics = StatementMetrics::new();
        self.op_open_ephemeral_state = OpOpenEphemeralState::Start;
        self.op_new_rowid_state = OpNewRowidState::Start;
        self.op_idx_insert_state = OpIdxInsertState::MaybeSeek;
        self.op_insert_state = OpInsertState {
            sub_state: OpInsertSubState::MaybeCaptureRecord,
            old_record: None,
        };
        self.op_no_conflict_state = OpNoConflictState::Start;
        self.seek_state = OpSeekState::Start;
        self.current_collation = None;
        self.op_column_state = OpColumnState::Start;
        self.op_row_id_state = OpRowIdState::Start;
        self.view_delta_state = ViewDeltaCommitState::NotStarted;
        self.auto_txn_cleanup = TxnCleanup::None;
        self.fk_immediate_violations_during_stmt
            .store(0, Ordering::SeqCst);
        self.fk_deferred_violations_when_stmt_started
            .store(0, Ordering::SeqCst);
    }

    pub fn get_cursor(&mut self, cursor_id: CursorID) -> &mut Cursor {
        self.cursors
            .get_mut(cursor_id)
            .unwrap_or_else(|| panic!("cursor id {cursor_id} out of bounds"))
            .as_mut()
            .unwrap_or_else(|| panic!("cursor id {cursor_id} is None"))
    }

    /// Begin a statement subtransaction.
    pub fn begin_statement(
        &mut self,
        connection: &Connection,
        pager: &Arc<Pager>,
        write: bool,
    ) -> Result<IOResult<()>> {
        // Store the deferred foreign key violations counter at the start of the statement.
        // This is used to ensure that if an interactive transaction had deferred FK violations and a statement subtransaction rolls back,
        // the deferred FK violations are not lost.
        self.fk_deferred_violations_when_stmt_started.store(
            connection.fk_deferred_violations.load(Ordering::Acquire),
            Ordering::SeqCst,
        );
        // Reset the immediate foreign key violations counter to 0. If this is nonzero when the statement completes, the statement subtransaction will roll back.
        self.fk_immediate_violations_during_stmt
            .store(0, Ordering::SeqCst);
        if write {
            let db_size = return_if_io!(pager.with_header(|header| header.database_size.get()));
            pager.begin_statement(db_size)?;
        }
        Ok(IOResult::Done(()))
    }

    /// End a statement subtransaction.
    pub fn end_statement(
        &mut self,
        connection: &Connection,
        pager: &Arc<Pager>,
        end_statement: EndStatement,
    ) -> Result<()> {
        match end_statement {
            EndStatement::ReleaseSavepoint => pager.release_savepoint(),
            EndStatement::RollbackSavepoint => {
                pager.rollback_to_newest_savepoint()?;
                // Reset the deferred foreign key violations counter to the value it had at the start of the statement.
                // This is used to ensure that if an interactive transaction had deferred FK violations, they are not lost.
                connection.fk_deferred_violations.store(
                    self.fk_deferred_violations_when_stmt_started
                        .load(Ordering::Acquire),
                    Ordering::SeqCst,
                );
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Action to take at the end of a statement subtransaction.
pub enum EndStatement {
    /// Release (commit) the savepoint -- effectively removing the savepoint as it is no longer needed for undo purposes.
    ReleaseSavepoint,
    /// Rollback (abort) to the newest savepoint: read pages from the subjournal and restore them to the page cache.
    /// This is used to undo the changes made by the statement.
    RollbackSavepoint,
}

impl Register {
    pub fn get_value(&self) -> &Value {
        match self {
            Register::Value(v) => v,
            Register::Record(r) => {
                assert!(!r.is_invalidated());
                r.as_blob_value()
            }
            _ => panic!("register holds unexpected value: {self:?}"),
        }
    }
}

#[macro_export]
macro_rules! must_be_btree_cursor {
    ($cursor_id:expr, $cursor_ref:expr, $state:expr, $insn_name:expr) => {{
        let (_, cursor_type) = $cursor_ref.get($cursor_id).unwrap();
        if matches!(
            cursor_type,
            CursorType::BTreeTable(_)
                | CursorType::BTreeIndex(_)
                | CursorType::MaterializedView(_, _)
        ) {
            $crate::get_cursor!($state, $cursor_id)
        } else {
            panic!("{} on unexpected cursor", $insn_name)
        }
    }};
}

/// Macro is necessary to help the borrow checker see we are only accessing state.cursor field
/// and nothing else
#[macro_export]
macro_rules! get_cursor {
    ($state:expr, $cursor_id:expr) => {
        $state
            .cursors
            .get_mut($cursor_id)
            .unwrap_or_else(|| panic!("cursor id {} out of bounds", $cursor_id))
            .as_mut()
            .unwrap_or_else(|| panic!("cursor id {} is None", $cursor_id))
    };
}

pub struct Program {
    pub max_registers: usize,
    // we store original indices because we don't want to create new vec from
    // ProgramBuilder
    pub insns: Vec<(Insn, usize)>,
    pub cursor_ref: Vec<(Option<CursorKey>, CursorType)>,
    pub comments: Vec<(InsnReference, &'static str)>,
    pub parameters: crate::parameters::Parameters,
    pub connection: Arc<Connection>,
    pub n_change: AtomicI64,
    pub change_cnt_on: bool,
    pub result_columns: Vec<ResultSetColumn>,
    pub table_references: TableReferences,
    pub sql: String,
    /// Whether the program accesses the database.
    /// Used to determine whether we need to check for schema changes when
    /// starting a transaction.
    pub accesses_db: bool,
    /// In SQLite, whether statement subtransactions will be used for executing a program (`usesStmtJournal`)
    /// is determined by the parser flags "mayAbort" and "isMultiWrite". Essentially this means that the individual
    /// statement may need to be aborted due to a constraint conflict, etc. instead of the entire transaction.
    pub needs_stmt_subtransactions: bool,
}

impl Program {
    fn get_pager_from_database_index(&self, idx: &usize) -> Arc<Pager> {
        self.connection.get_pager_from_database_index(idx)
    }

    pub fn step(
        &self,
        state: &mut ProgramState,
        mv_store: Option<&Arc<MvStore>>,
        pager: Arc<Pager>,
        query_mode: QueryMode,
        waker: Option<&Waker>,
    ) -> Result<StepResult> {
        match query_mode {
            QueryMode::Normal => self.normal_step(state, mv_store, pager, waker),
            QueryMode::Explain => self.explain_step(state, mv_store, pager),
            QueryMode::ExplainQueryPlan => self.explain_query_plan_step(state, mv_store, pager),
        }
    }

    fn explain_step(
        &self,
        state: &mut ProgramState,
        _mv_store: Option<&Arc<MvStore>>,
        pager: Arc<Pager>,
    ) -> Result<StepResult> {
        debug_assert!(state.column_count() == EXPLAIN_COLUMNS.len());
        if self.connection.is_closed() {
            // Connection is closed for whatever reason, rollback the transaction.
            let state = self.connection.get_tx_state();
            if let TransactionState::Write { .. } = state {
                pager.rollback_tx(&self.connection);
            }
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }

        if state.is_interrupted() {
            return Ok(StepResult::Interrupt);
        }

        // FIXME: do we need this?
        state.metrics.vm_steps = state.metrics.vm_steps.saturating_add(1);

        if state.pc as usize >= self.insns.len() {
            return Ok(StepResult::Done);
        }

        let (current_insn, _) = &self.insns[state.pc as usize];
        let (opcode, p1, p2, p3, p4, p5, comment) = insn_to_row_with_comment(
            self,
            current_insn,
            self.comments
                .iter()
                .find(|(offset, _)| *offset == state.pc)
                .map(|(_, comment)| comment)
                .copied(),
        );

        state.registers[0] = Register::Value(Value::Integer(state.pc as i64));
        state.registers[1] = Register::Value(Value::from_text(opcode));
        state.registers[2] = Register::Value(Value::Integer(p1 as i64));
        state.registers[3] = Register::Value(Value::Integer(p2 as i64));
        state.registers[4] = Register::Value(Value::Integer(p3 as i64));
        state.registers[5] = Register::Value(p4);
        state.registers[6] = Register::Value(Value::Integer(p5 as i64));
        state.registers[7] = Register::Value(Value::from_text(&comment));
        state.result_row = Some(Row {
            values: &state.registers[0] as *const Register,
            count: EXPLAIN_COLUMNS.len(),
        });
        state.pc += 1;
        Ok(StepResult::Row)
    }

    fn explain_query_plan_step(
        &self,
        state: &mut ProgramState,
        _mv_store: Option<&Arc<MvStore>>,
        pager: Arc<Pager>,
    ) -> Result<StepResult> {
        debug_assert!(state.column_count() == EXPLAIN_QUERY_PLAN_COLUMNS.len());
        loop {
            if self.connection.is_closed() {
                // Connection is closed for whatever reason, rollback the transaction.
                let state = self.connection.get_tx_state();
                if let TransactionState::Write { .. } = state {
                    pager.rollback_tx(&self.connection);
                }
                return Err(LimboError::InternalError("Connection closed".to_string()));
            }

            if state.is_interrupted() {
                return Ok(StepResult::Interrupt);
            }

            // FIXME: do we need this?
            state.metrics.vm_steps = state.metrics.vm_steps.saturating_add(1);

            if state.pc as usize >= self.insns.len() {
                return Ok(StepResult::Done);
            }

            let Insn::Explain { p1, p2, detail } = &self.insns[state.pc as usize].0 else {
                state.pc += 1;
                continue;
            };

            state.registers[0] = Register::Value(Value::Integer(*p1 as i64));
            state.registers[1] =
                Register::Value(Value::Integer(p2.as_ref().map(|p| *p).unwrap_or(0) as i64));
            state.registers[2] = Register::Value(Value::Integer(0));
            state.registers[3] = Register::Value(Value::from_text(detail.as_str()));
            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: EXPLAIN_QUERY_PLAN_COLUMNS.len(),
            });
            state.pc += 1;
            return Ok(StepResult::Row);
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn normal_step(
        &self,
        state: &mut ProgramState,
        mv_store: Option<&Arc<MvStore>>,
        pager: Arc<Pager>,
        waker: Option<&Waker>,
    ) -> Result<StepResult> {
        let enable_tracing = tracing::enabled!(tracing::Level::TRACE);
        loop {
            if self.connection.is_closed() {
                // Connection is closed for whatever reason, rollback the transaction.
                let state = self.connection.get_tx_state();
                if let TransactionState::Write { .. } = state {
                    pager.rollback_tx(&self.connection);
                }
                return Err(LimboError::InternalError("Connection closed".to_string()));
            }
            if state.is_interrupted() {
                self.abort(mv_store, &pager, None, &mut state.auto_txn_cleanup);
                return Ok(StepResult::Interrupt);
            }
            if let Some(io) = &state.io_completions {
                if !io.finished() {
                    io.set_waker(waker);
                    return Ok(StepResult::IO);
                }
                if let Some(err) = io.get_error() {
                    let err = err.into();
                    self.abort(mv_store, &pager, Some(&err), &mut state.auto_txn_cleanup);
                    return Err(err);
                }
                state.io_completions = None;
            }
            // invalidate row
            let _ = state.result_row.take();
            let (insn, _) = &self.insns[state.pc as usize];
            let insn_function = insn.to_function();
            if enable_tracing {
                trace_insn(self, state.pc as InsnReference, insn);
            }
            // Always increment VM steps for every loop iteration
            state.metrics.vm_steps = state.metrics.vm_steps.saturating_add(1);

            match insn_function(self, state, insn, &pager, mv_store) {
                Ok(InsnFunctionStepResult::Step) => {
                    // Instruction completed, moving to next
                    state.metrics.insn_executed = state.metrics.insn_executed.saturating_add(1);
                }
                Ok(InsnFunctionStepResult::Done) => {
                    // Instruction completed execution
                    state.metrics.insn_executed = state.metrics.insn_executed.saturating_add(1);
                    state.auto_txn_cleanup = TxnCleanup::None;
                    return Ok(StepResult::Done);
                }
                Ok(InsnFunctionStepResult::IO(io)) => {
                    // Instruction not complete - waiting for I/O, will resume at same PC
                    io.set_waker(waker);
                    state.io_completions = Some(io);
                    return Ok(StepResult::IO);
                }
                Ok(InsnFunctionStepResult::Row) => {
                    // Instruction completed (ResultRow already incremented PC)
                    state.metrics.insn_executed = state.metrics.insn_executed.saturating_add(1);
                    return Ok(StepResult::Row);
                }
                Err(LimboError::Busy) => {
                    // Instruction blocked - will retry at same PC
                    return Ok(StepResult::Busy);
                }
                Err(err) => {
                    self.abort(mv_store, &pager, Some(&err), &mut state.auto_txn_cleanup);
                    return Err(err);
                }
            }
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn apply_view_deltas(
        &self,
        state: &mut ProgramState,
        rollback: bool,
        pager: &Arc<Pager>,
    ) -> Result<IOResult<()>> {
        use crate::types::IOResult;

        loop {
            match &state.view_delta_state {
                ViewDeltaCommitState::NotStarted => {
                    if self.connection.view_transaction_states.is_empty() {
                        return Ok(IOResult::Done(()));
                    }

                    if rollback {
                        // On rollback, just clear and done
                        self.connection.view_transaction_states.clear();
                        return Ok(IOResult::Done(()));
                    }

                    // Not a rollback - proceed with processing
                    let schema = self.connection.schema.read();

                    // Collect materialized views - they should all have storage
                    let mut views = Vec::new();
                    for view_name in self.connection.view_transaction_states.get_view_names() {
                        if let Some(view_mutex) = schema.get_materialized_view(&view_name) {
                            let view = view_mutex.lock().unwrap();
                            let root_page = view.get_root_page();

                            // Materialized views should always have storage (root_page != 0)
                            assert!(
                                root_page != 0,
                                "Materialized view '{view_name}' should have a root page"
                            );

                            views.push(view_name);
                        }
                    }

                    state.view_delta_state = ViewDeltaCommitState::Processing {
                        views,
                        current_index: 0,
                    };
                }

                ViewDeltaCommitState::Processing {
                    views,
                    current_index,
                } => {
                    // At this point we know it's not a rollback
                    if *current_index >= views.len() {
                        // All done, clear the transaction states
                        self.connection.view_transaction_states.clear();
                        state.view_delta_state = ViewDeltaCommitState::Done;
                        return Ok(IOResult::Done(()));
                    }

                    let view_name = &views[*current_index];

                    let table_deltas = self
                        .connection
                        .view_transaction_states
                        .get(view_name)
                        .unwrap()
                        .get_table_deltas();

                    let schema = self.connection.schema.read();
                    if let Some(view_mutex) = schema.get_materialized_view(view_name) {
                        let mut view = view_mutex.lock().unwrap();

                        // Create a DeltaSet from the per-table deltas
                        let mut delta_set = crate::incremental::compiler::DeltaSet::new();
                        for (table_name, delta) in table_deltas {
                            delta_set.insert(table_name, delta);
                        }

                        // Handle I/O from merge_delta - pass pager, circuit will create its own cursor
                        match view.merge_delta(delta_set, pager.clone())? {
                            IOResult::Done(_) => {
                                // Move to next view
                                state.view_delta_state = ViewDeltaCommitState::Processing {
                                    views: views.clone(),
                                    current_index: current_index + 1,
                                };
                            }
                            IOResult::IO(io) => {
                                // Return I/O, will resume at same index
                                return Ok(IOResult::IO(io));
                            }
                        }
                    }
                }

                ViewDeltaCommitState::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    pub fn commit_txn(
        &self,
        pager: Arc<Pager>,
        program_state: &mut ProgramState,
        mv_store: Option<&Arc<MvStore>>,
        rollback: bool,
    ) -> Result<IOResult<()>> {
        // Apply view deltas with I/O handling
        match self.apply_view_deltas(program_state, rollback, &pager)? {
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
            IOResult::Done(_) => {}
        }

        // Reset state for next use
        program_state.view_delta_state = ViewDeltaCommitState::NotStarted;
        if self.connection.get_tx_state() == TransactionState::None {
            // No need to do any work here if not in tx. Current MVCC logic doesn't work with this assumption,
            // hence the mv_store.is_none() check.
            return Ok(IOResult::Done(()));
        }
        if self.connection.is_nested_stmt() {
            // We don't want to commit on nested statements. Let parent handle it.
            return Ok(IOResult::Done(()));
        }
        if let Some(mv_store) = mv_store {
            let conn = self.connection.clone();
            let auto_commit = conn.auto_commit.load(Ordering::SeqCst);
            if auto_commit {
                // FIXME: we don't want to commit stuff from other programs.
                if matches!(program_state.commit_state, CommitState::Ready) {
                    let Some(tx_id) = conn.get_mv_tx_id() else {
                        return Ok(IOResult::Done(()));
                    };
                    let state_machine = mv_store.commit_tx(tx_id, &conn).unwrap();
                    program_state.commit_state = CommitState::CommitingMvcc { state_machine };
                }
                let CommitState::CommitingMvcc { state_machine } = &mut program_state.commit_state
                else {
                    panic!("invalid state for mvcc commit step")
                };
                match self.step_end_mvcc_txn(state_machine, mv_store)? {
                    IOResult::Done(_) => {
                        assert!(state_machine.is_finalized());
                        *conn.mv_tx.write() = None;
                        conn.set_tx_state(TransactionState::None);
                        program_state.commit_state = CommitState::Ready;
                        return Ok(IOResult::Done(()));
                    }
                    IOResult::IO(io) => {
                        return Ok(IOResult::IO(io));
                    }
                }
            }
            Ok(IOResult::Done(()))
        } else {
            let connection = self.connection.clone();
            let auto_commit = connection.auto_commit.load(Ordering::SeqCst);
            tracing::debug!(
                "Halt auto_commit {}, state={:?}",
                auto_commit,
                program_state.commit_state
            );
            if matches!(program_state.commit_state, CommitState::Committing) {
                let TransactionState::Write { .. } = connection.get_tx_state() else {
                    unreachable!("invalid state for write commit step")
                };
                self.step_end_write_txn(
                    &pager,
                    &mut program_state.commit_state,
                    &connection,
                    rollback,
                )
            } else if auto_commit {
                let current_state = connection.get_tx_state();
                tracing::trace!("Auto-commit state: {:?}", current_state);
                match current_state {
                    TransactionState::Write { .. } => self.step_end_write_txn(
                        &pager,
                        &mut program_state.commit_state,
                        &connection,
                        rollback,
                    ),
                    TransactionState::Read => {
                        connection.set_tx_state(TransactionState::None);
                        pager.end_read_tx();
                        Ok(IOResult::Done(()))
                    }
                    TransactionState::None => Ok(IOResult::Done(())),
                    TransactionState::PendingUpgrade => {
                        panic!("Unexpected transaction state: {current_state:?} during auto-commit",)
                    }
                }
            } else {
                if self.change_cnt_on {
                    self.connection
                        .set_changes(self.n_change.load(Ordering::SeqCst));
                }
                Ok(IOResult::Done(()))
            }
        }
    }

    #[instrument(skip(self, pager, connection), level = Level::DEBUG)]
    fn step_end_write_txn(
        &self,
        pager: &Arc<Pager>,
        commit_state: &mut CommitState,
        connection: &Connection,
        rollback: bool,
    ) -> Result<IOResult<()>> {
        let cacheflush_status = if !rollback {
            pager.commit_tx(connection)?
        } else {
            pager.rollback_tx(connection);
            IOResult::Done(PagerCommitResult::Rollback)
        };
        match cacheflush_status {
            IOResult::Done(_) => {
                if self.change_cnt_on {
                    self.connection
                        .set_changes(self.n_change.load(Ordering::SeqCst));
                }
                connection.set_tx_state(TransactionState::None);
                *commit_state = CommitState::Ready;
            }
            IOResult::IO(io) => {
                tracing::trace!("Cacheflush IO");
                *commit_state = CommitState::Committing;
                return Ok(IOResult::IO(io));
            }
        }
        Ok(IOResult::Done(()))
    }

    #[instrument(skip(self, commit_state, mv_store), level = Level::DEBUG)]
    fn step_end_mvcc_txn(
        &self,
        commit_state: &mut StateMachine<CommitStateMachine<LocalClock>>,
        mv_store: &Arc<MvStore>,
    ) -> Result<IOResult<()>> {
        commit_state.step(mv_store)
    }

    /// Aborts the program due to various conditions (explicit error, interrupt or reset of unfinished statement) by rolling back the transaction
    /// This method is no-op if program was already finished (either aborted or executed to completion)
    pub fn abort(
        &self,
        mv_store: Option<&Arc<MvStore>>,
        pager: &Arc<Pager>,
        err: Option<&LimboError>,
        cleanup: &mut TxnCleanup,
    ) {
        // Errors from nested statements are handled by the parent statement.
        if !self.connection.is_nested_stmt() {
            match err {
                // Transaction errors, e.g. trying to start a nested transaction, do not cause a rollback.
                Some(LimboError::TxError(_)) => {}
                // Table locked errors, e.g. trying to checkpoint in an interactive transaction, do not cause a rollback.
                Some(LimboError::TableLocked) => {}
                // Busy errors do not cause a rollback.
                Some(LimboError::Busy) => {}
                // Constraint errors do not cause a rollback of the transaction by default;
                // Instead individual statement subtransactions will roll back and these are handled in op_auto_commit
                // and op_halt.
                Some(LimboError::Constraint(_)) => {}
                _ => {
                    if *cleanup != TxnCleanup::None || err.is_some() {
                        if let Some(mv_store) = mv_store {
                            if let Some(tx_id) = self.connection.get_mv_tx_id() {
                                self.connection.auto_commit.store(true, Ordering::SeqCst);
                                mv_store.rollback_tx(tx_id, pager.clone(), &self.connection);
                            }
                        } else {
                            pager.rollback_tx(&self.connection);
                            self.connection.auto_commit.store(true, Ordering::SeqCst);
                        }
                        self.connection.set_tx_state(TransactionState::None);
                    }
                }
            }
        }
        *cleanup = TxnCleanup::None;
    }
}

fn make_record(registers: &[Register], start_reg: &usize, count: &usize) -> ImmutableRecord {
    let regs = &registers[*start_reg..*start_reg + *count];
    ImmutableRecord::from_registers(regs, regs.len())
}

pub fn registers_to_ref_values<'a>(registers: &'a [Register]) -> Vec<ValueRef<'a>> {
    registers
        .iter()
        .map(|reg| reg.get_value().as_ref())
        .collect()
}

#[instrument(skip(program), level = Level::DEBUG)]
fn trace_insn(program: &Program, addr: InsnReference, insn: &Insn) {
    tracing::trace!(
        "\n{}",
        explain::insn_to_str(
            program,
            addr,
            insn,
            String::new(),
            program
                .comments
                .iter()
                .find(|(offset, _)| *offset == addr)
                .map(|(_, comment)| comment)
                .copied()
        )
    );
}

pub trait FromValueRow<'a> {
    fn from_value(value: &'a Value) -> Result<Self>
    where
        Self: Sized + 'a;
}

impl<'a> FromValueRow<'a> for i64 {
    fn from_value(value: &'a Value) -> Result<Self> {
        match value {
            Value::Integer(i) => Ok(*i),
            _ => Err(LimboError::ConversionError("Expected integer value".into())),
        }
    }
}

impl<'a> FromValueRow<'a> for f64 {
    fn from_value(value: &'a Value) -> Result<Self> {
        match value {
            Value::Float(f) => Ok(*f),
            _ => Err(LimboError::ConversionError("Expected integer value".into())),
        }
    }
}

impl<'a> FromValueRow<'a> for String {
    fn from_value(value: &'a Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.as_str().to_string()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

impl<'a> FromValueRow<'a> for &'a str {
    fn from_value(value: &'a Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.as_str()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

impl<'a> FromValueRow<'a> for &'a Value {
    fn from_value(value: &'a Value) -> Result<Self> {
        Ok(value)
    }
}

impl Row {
    pub fn get<'a, T: FromValueRow<'a> + 'a>(&'a self, idx: usize) -> Result<T> {
        let value = unsafe { self.values.add(idx).as_ref().unwrap() };
        let value = match value {
            Register::Value(value) => value,
            _ => unreachable!("a row should be formed of values only"),
        };
        T::from_value(value)
    }

    pub fn get_value(&self, idx: usize) -> &Value {
        let value = unsafe { self.values.add(idx).as_ref().unwrap() };
        match value {
            Register::Value(value) => value,
            _ => unreachable!("a row should be formed of values only"),
        }
    }

    pub fn get_values(&self) -> impl Iterator<Item = &Value> {
        let values = unsafe { std::slice::from_raw_parts(self.values, self.count) };
        // This should be ownedvalues
        // TODO: add check for this
        values.iter().map(|v| v.get_value())
    }

    pub fn len(&self) -> usize {
        self.count
    }
}
