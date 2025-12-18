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

pub mod affinity;
pub mod bloom_filter;
pub mod builder;
pub mod execute;
pub mod explain;
#[allow(dead_code)]
pub mod hash_table;
pub mod insn;
pub mod likeop;
pub mod metrics;
pub mod rowset;
pub mod sorter;
pub mod value;

use crate::{
    error::LimboError,
    function::{AggFunc, FuncCtx},
    mvcc::{database::CommitStateMachine, LocalClock},
    return_if_io,
    schema::Trigger,
    state_machine::StateMachine,
    storage::{pager::PagerCommitResult, sqlite3_ondisk::SmallVec},
    translate::{collate::CollationSeq, plan::TableReferences},
    types::{IOCompletions, IOResult},
    vdbe::{
        execute::{
            OpColumnState, OpDeleteState, OpDeleteSubState, OpDestroyState, OpIdxInsertState,
            OpInsertState, OpInsertSubState, OpJournalModeState, OpNewRowidState,
            OpNoConflictState, OpProgramState, OpRowIdState, OpSeekState, OpTransactionState,
        },
        hash_table::HashTable,
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
use parking_lot::RwLock;
use turso_parser::ast::ResolveType;

use crate::vdbe::bloom_filter::BloomFilter;
use crate::vdbe::rowset::RowSet;
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProgramExecutionState {
    /// No steps of the program was executed
    Init,
    /// Program started execution but didn't reach any terminal state
    Running,
    /// Interrupt requested for the program
    Interrupting,
    /// Terminal state: program interrupted
    Interrupted,
    /// Terminal state: program finished successfully
    Done,
    /// Terminal state: program failed with error
    Failed,
}

impl ProgramExecutionState {
    pub fn is_running(&self) -> bool {
        matches!(
            self,
            ProgramExecutionState::Interrupting | ProgramExecutionState::Running
        )
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ProgramExecutionState::Interrupted
                | ProgramExecutionState::Failed
                | ProgramExecutionState::Done
        )
    }
}

/// Re-entrant state for [Insn::HashBuild].
/// Allows HashBuild to resume cleanly after async I/O without re-reading the row.
#[derive(Debug, Default)]
pub struct OpHashBuildState {
    pub key_values: Vec<Value>,
    pub key_idx: usize,
    pub payload_values: Vec<Value>,
    pub payload_idx: usize,
    pub rowid: Option<i64>,
    pub cursor_id: CursorID,
    pub hash_table_id: usize,
    pub key_start_reg: usize,
    pub num_keys: usize,
}

/// Re-entrant state for [Insn::HashProbe].
/// Allows HashProbe to resume cleanly after async I/O when loading spilled partitions.
#[derive(Debug, Default)]
pub struct OpHashProbeState {
    /// Cached probe key values to avoid re-reading from registers
    pub probe_keys: Vec<Value>,
    /// Hash table register being probed
    pub hash_table_id: usize,
    /// Partition index being loaded (if any)
    pub partition_idx: usize,
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
    /// Indicate whether a coroutine has ended for a given yield register.
    /// If an element is present, it means the coroutine with the given register number has ended.
    ended_coroutine: Vec<u32>,
    /// Indicate whether an [Insn::Once] instruction at a given program counter position has already been executed, well, once.
    once: SmallVec<u32, 4>,
    regex_cache: RegexCache,
    pub execution_state: ProgramExecutionState,
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
    op_program_state: OpProgramState,
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
    op_journal_mode_state: OpJournalModeState,
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
    /// RowSet objects stored by register index
    rowsets: HashMap<usize, RowSet>,
    /// Bloom filters stored by cursor ID for probabilistic set membership testing
    /// Used to avoid unnecessary seeks on ephemeral indexes and hash tables
    pub(crate) bloom_filters: HashMap<usize, BloomFilter>,
    op_hash_build_state: Option<OpHashBuildState>,
    op_hash_probe_state: Option<OpHashProbeState>,
    hash_tables: HashMap<usize, HashTable>,
    uses_subjournal: bool,
}

impl std::fmt::Debug for Program {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Program").finish()
    }
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
            ended_coroutine: vec![],
            once: SmallVec::<u32, 4>::new(),
            regex_cache: RegexCache::new(),
            execution_state: ProgramExecutionState::Init,
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
            op_program_state: OpProgramState::Start,
            op_new_rowid_state: OpNewRowidState::Start,
            op_idx_insert_state: OpIdxInsertState::MaybeSeek,
            op_insert_state: OpInsertState {
                sub_state: OpInsertSubState::MaybeCaptureRecord,
                old_record: None,
            },
            op_no_conflict_state: OpNoConflictState::Start,
            op_hash_build_state: None,
            op_hash_probe_state: None,
            seek_state: OpSeekState::Start,
            current_collation: None,
            op_column_state: OpColumnState::Start,
            op_row_id_state: OpRowIdState::Start,
            op_transaction_state: OpTransactionState::Start,
            op_journal_mode_state: OpJournalModeState::default(),
            view_delta_state: ViewDeltaCommitState::NotStarted,
            auto_txn_cleanup: TxnCleanup::None,
            fk_deferred_violations_when_stmt_started: AtomicIsize::new(0),
            fk_immediate_violations_during_stmt: AtomicIsize::new(0),
            rowsets: HashMap::new(),
            bloom_filters: HashMap::new(),
            hash_tables: HashMap::new(),
            uses_subjournal: false,
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
        self.execution_state = ProgramExecutionState::Interrupting;
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
        self.ended_coroutine.clear();
        self.regex_cache.like.clear();
        self.execution_state = ProgramExecutionState::Init;
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
        self.rowsets.clear();
        self.bloom_filters.clear();
        self.hash_tables.clear();
        self.op_hash_build_state = None;
        self.op_hash_probe_state = None;
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
        if write {
            let db_size = return_if_io!(pager.with_header(|header| header.database_size.get()));
            pager.open_subjournal()?;
            pager.try_use_subjournal()?;
            let result = pager.open_savepoint(db_size);
            if result.is_err() {
                pager.stop_use_subjournal();
            }
            result?;
            self.uses_subjournal = true;
        }

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
        Ok(IOResult::Done(()))
    }

    /// End a statement subtransaction.
    pub fn end_statement(
        &mut self,
        connection: &Connection,
        pager: &Arc<Pager>,
        end_statement: EndStatement,
    ) -> Result<()> {
        let result = 'outer: {
            match end_statement {
                EndStatement::ReleaseSavepoint => pager.release_savepoint(),
                EndStatement::RollbackSavepoint => {
                    match pager.rollback_to_newest_savepoint() {
                        // We sometimes call end_statement() on errors without explicitly knowing whether a stmt transaction
                        // caused the error or not. If it didn't, don't reset any FK violation counters.
                        Ok(false) => break 'outer Ok(()),
                        Err(err) => break 'outer Err(err),
                        _ => {}
                    }
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
        };
        if self.uses_subjournal {
            pager.stop_use_subjournal();
            self.uses_subjournal = false;
        }
        result
    }

    /// Gets or creates a bloom filter for the given cursor ID.
    pub fn get_or_create_bloom_filter(&mut self, cursor_id: usize) -> &mut BloomFilter {
        self.bloom_filters.entry(cursor_id).or_default()
    }

    /// Gets or creates a bloom filter with a specific capacity for the given cursor ID.
    pub fn get_or_create_bloom_filter_with_capacity(
        &mut self,
        cursor_id: usize,
        expected_items: u32,
        false_positive_rate: f32,
    ) -> &mut BloomFilter {
        self.bloom_filters
            .entry(cursor_id)
            .or_insert_with(|| BloomFilter::with_capacity(expected_items, false_positive_rate))
    }

    /// Gets an existing bloom filter for the given cursor ID.
    pub fn get_bloom_filter(&self, cursor_id: usize) -> Option<&BloomFilter> {
        self.bloom_filters.get(&cursor_id)
    }

    /// Gets a mutable reference to an existing bloom filter for the given cursor ID.
    pub fn get_bloom_filter_mut(&mut self, cursor_id: usize) -> Option<&mut BloomFilter> {
        self.bloom_filters.get_mut(&cursor_id)
    }

    /// Removes and drops the bloom filter for the given cursor ID.
    pub fn remove_bloom_filter(&mut self, cursor_id: usize) {
        self.bloom_filters.remove(&cursor_id);
    }

    /// Checks if a bloom filter exists for the given cursor ID.
    pub fn has_bloom_filter(&self, cursor_id: usize) -> bool {
        self.bloom_filters.contains_key(&cursor_id)
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

/// Tracks the state of explain mode execution, including which subprograms need to be processed.
#[derive(Default)]
pub struct ExplainState {
    /// Program counter positions in the parent program where `Insn::Program` instructions occur.
    parent_program_pcs: Vec<usize>,
    /// Index of the subprogram currently being processed, if any.
    current_subprogram_index: Option<usize>,
    /// PC value when we started processing the current subprogram, to detect if we need to reset.
    subprogram_start_pc: Option<usize>,
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
    /// If this Program is a trigger subprogram, a ref to the trigger is stored here.
    pub trigger: Option<Arc<Trigger>>,
    /// Whether the program contains any trigger subprograms.
    pub contains_trigger_subprograms: bool,
    pub resolve_type: ResolveType,
    pub explain_state: RwLock<ExplainState>,
}

impl Program {
    fn get_pager_from_database_index(&self, idx: &usize) -> Arc<Pager> {
        self.connection.get_pager_from_database_index(idx)
    }

    pub fn step(
        &self,
        state: &mut ProgramState,
        pager: Arc<Pager>,
        query_mode: QueryMode,
        waker: Option<&Waker>,
    ) -> Result<StepResult> {
        state.execution_state = ProgramExecutionState::Running;
        let result = match query_mode {
            QueryMode::Normal => self.normal_step(state, pager, waker),
            QueryMode::Explain => self.explain_step(state, pager),
            QueryMode::ExplainQueryPlan => self.explain_query_plan_step(state, pager),
        };
        match &result {
            Ok(StepResult::Done) => {
                state.execution_state = ProgramExecutionState::Done;
            }
            Ok(StepResult::Interrupt) => {
                state.execution_state = ProgramExecutionState::Interrupted;
            }
            Err(_) => {
                state.execution_state = ProgramExecutionState::Failed;
            }
            _ => {}
        }
        result
    }

    fn explain_step(&self, state: &mut ProgramState, pager: Arc<Pager>) -> Result<StepResult> {
        debug_assert!(state.column_count() == EXPLAIN_COLUMNS.len());
        if self.connection.is_closed() {
            // Connection is closed for whatever reason, rollback the transaction.
            let state = self.connection.get_tx_state();
            if let TransactionState::Write { .. } = state {
                pager.rollback_tx(&self.connection);
            }
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }

        if matches!(state.execution_state, ProgramExecutionState::Interrupting) {
            return Ok(StepResult::Interrupt);
        }

        // FIXME: do we need this?
        state.metrics.vm_steps = state.metrics.vm_steps.saturating_add(1);

        let mut explain_state = self.explain_state.write();

        // Check if we're processing a subprogram
        if let Some(sub_idx) = explain_state.current_subprogram_index {
            if sub_idx >= explain_state.parent_program_pcs.len() {
                // All subprograms processed
                *explain_state = ExplainState::default();
                return Ok(StepResult::Done);
            }

            let parent_pc = explain_state.parent_program_pcs[sub_idx];
            let Insn::Program { program: p, .. } = &self.insns[parent_pc].0 else {
                panic!("Expected program insn at pc {parent_pc}");
            };
            let p = &mut p.write().program;

            let subprogram_insn_count = p.insns.len();

            // Check if the subprogram has already finished (PC is out of bounds)
            // This can happen if the subprogram finished in a previous call but we're being called again
            if state.pc as usize >= subprogram_insn_count {
                // Subprogram is done, move to next one
                explain_state.subprogram_start_pc = None;
                if sub_idx + 1 < explain_state.parent_program_pcs.len() {
                    explain_state.current_subprogram_index = Some(sub_idx + 1);
                    state.pc = 0;
                    drop(explain_state);
                    return self.explain_step(state, pager);
                } else {
                    *explain_state = ExplainState::default();
                    return Ok(StepResult::Done);
                }
            }

            // Reset PC to 0 only when starting a new subprogram (when subprogram_start_pc is None)
            // Once we've started, let the subprogram manage its own PC through its explain_step
            if explain_state.subprogram_start_pc.is_none() {
                state.pc = 0;
                explain_state.subprogram_start_pc = Some(0);
            }

            // Process the subprogram - it will handle its own explain_step internally
            // The subprogram's explain_step will process all its instructions (including any nested subprograms)
            // and return StepResult::Row for each instruction, then StepResult::Done when finished
            let result = p.step(state, pager.clone(), QueryMode::Explain, None)?;

            match result {
                StepResult::Done => {
                    // This subprogram is done, move to next one
                    explain_state.subprogram_start_pc = None; // Clear the start PC marker
                    if sub_idx + 1 < explain_state.parent_program_pcs.len() {
                        // Move to next subprogram
                        explain_state.current_subprogram_index = Some(sub_idx + 1);
                        // Reset PC to 0 for the next subprogram
                        state.pc = 0;
                        // Recursively call to process the next subprogram
                        drop(explain_state);
                        return self.explain_step(state, pager);
                    } else {
                        // All subprograms done
                        *explain_state = ExplainState::default();
                        return Ok(StepResult::Done);
                    }
                }
                StepResult::Row => {
                    // Output a row from the subprogram
                    // The subprogram's step already set up the registers with PC starting at 0
                    // Don't reset subprogram_start_pc - we're still processing this subprogram
                    drop(explain_state);
                    return Ok(StepResult::Row);
                }
                other => {
                    drop(explain_state);
                    return Ok(other);
                }
            }
        }

        // We're processing the parent program
        if state.pc as usize >= self.insns.len() {
            // Parent program is done, start processing subprograms
            if explain_state.parent_program_pcs.is_empty() {
                // No subprograms to process
                *explain_state = ExplainState::default();
                return Ok(StepResult::Done);
            }

            // Start processing the first subprogram
            explain_state.current_subprogram_index = Some(0);
            explain_state.subprogram_start_pc = None; // Will be set when we actually start processing
            state.pc = 0; // Reset PC to 0 for the first subprogram
            drop(explain_state);
            return self.explain_step(state, pager);
        }

        let (current_insn, _) = &self.insns[state.pc as usize];

        if matches!(current_insn, Insn::Program { .. }) {
            explain_state.parent_program_pcs.push(state.pc as usize);
        }
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
        state.registers[2] = Register::Value(Value::Integer(p1));
        state.registers[3] = Register::Value(Value::Integer(p2));
        state.registers[4] = Register::Value(Value::Integer(p3));
        state.registers[5] = Register::Value(p4);
        state.registers[6] = Register::Value(Value::Integer(p5));
        state.registers[7] = Register::Value(Value::from_text(comment));
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

            if matches!(state.execution_state, ProgramExecutionState::Interrupting) {
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
            state.registers[3] = Register::Value(Value::from_text(detail.clone()));
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
            if matches!(state.execution_state, ProgramExecutionState::Interrupting) {
                self.abort(&pager, None, state);
                return Ok(StepResult::Interrupt);
            }

            if let Some(io) = &state.io_completions {
                if !io.finished() {
                    io.set_waker(waker);
                    return Ok(StepResult::IO);
                }
                if let Some(err) = io.get_error() {
                    if pager.is_checkpointing() {
                        // Wrap IO errors that occurred during checkpointing in CheckpointFailed error,
                        // so that abort() knows not to try to rollback the transaction, because the transaction
                        // is already durable in the WAL and hence committed.
                        // This also lets the simulator know that it should shadow the results of the query because
                        // the write itself succeeded.
                        let checkpoint_err = LimboError::CheckpointFailed(err.to_string());
                        tracing::error!("Checkpoint failed: {checkpoint_err}");
                        self.abort(&pager, Some(&checkpoint_err), state);
                        return Err(checkpoint_err);
                    }
                    let err = err.into();
                    self.abort(&pager, Some(&err), state);
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

            match insn_function(self, state, insn, &pager) {
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
                    self.abort(&pager, Some(&err), state);
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
                            let view = view_mutex.lock();
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
                        .expect("view should have transaction state")
                        .get_table_deltas();

                    let schema = self.connection.schema.read();
                    if let Some(view_mutex) = schema.get_materialized_view(view_name) {
                        let mut view = view_mutex.lock();

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
                    let state_machine = mv_store.commit_tx(tx_id, &conn)?;
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
            match pager.commit_tx(connection) {
                Ok(status) => status,
                Err(LimboError::CheckpointFailed(msg)) => {
                    // CheckpointFailed means the WAL commit succeeded but autocheckpoint failed.
                    // The transaction is durable - clean up transaction state and propagate the error.
                    tracing::warn!("Commit succeeded but autocheckpoint failed: {}", msg);
                    if self.change_cnt_on {
                        self.connection
                            .set_changes(self.n_change.load(Ordering::SeqCst));
                    }
                    // Update global schema if this was a DDL transaction.
                    // Must be done before clearing TX state, otherwise abort() won't know
                    // to update the schema.
                    if connection.get_tx_state().is_ddl_write_tx() {
                        let schema = connection.schema.read().clone();
                        connection.db.update_schema_if_newer(schema);
                    }
                    connection.set_tx_state(TransactionState::None);
                    *commit_state = CommitState::Ready;
                    return Err(LimboError::CheckpointFailed(msg));
                }
                Err(e) => return Err(e),
            }
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
    pub fn abort(&self, pager: &Arc<Pager>, err: Option<&LimboError>, state: &mut ProgramState) {
        if self.is_trigger_subprogram() {
            self.connection.end_trigger_execution();
        }
        // Errors from nested statements are handled by the parent statement.
        if !self.connection.is_nested_stmt() && !self.is_trigger_subprogram() {
            if err.is_some() && !pager.is_checkpointing() {
                // Any error apart from deferred FK violations and checkpoint failures causes the statement subtransaction to roll back.
                let res =
                    state.end_statement(&self.connection, pager, EndStatement::RollbackSavepoint);
                if let Err(e) = res {
                    tracing::error!("Error rolling back statement: {}", e);
                }
            }
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
                // Schema updated errors do not cause a rollback; the statement will be reprepared and retried,
                // and the caller is expected to handle transaction cleanup explicitly if needed.
                Some(LimboError::SchemaUpdated) => {}
                // CheckpointFailed means the WAL commit succeeded but autocheckpoint failed.
                // The transaction is already committed and durable, so no rollback is needed.
                // Clean up the WAL write/read transactions that would normally be cleaned up in commit_tx().
                Some(LimboError::CheckpointFailed(_)) => {
                    pager.finish_commit_after_checkpoint_failure();
                    // If a checkpoint failed, that doesn't mean the transaction is not committed;
                    // hence: if there were schema changes, we need to update the global schema.
                    if self.connection.get_tx_state().is_ddl_write_tx() {
                        let schema = self.connection.schema.read().clone();
                        self.connection.db.update_schema_if_newer(schema);
                    }
                    self.connection.set_tx_state(TransactionState::None);
                }
                _ => {
                    if state.auto_txn_cleanup != TxnCleanup::None || err.is_some() {
                        if let Some(mv_store) = self.connection.mv_store().as_ref() {
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
        state.auto_txn_cleanup = TxnCleanup::None;
    }

    pub fn is_trigger_subprogram(&self) -> bool {
        self.trigger.is_some()
    }
}

fn make_record(registers: &[Register], start_reg: &usize, count: &usize) -> ImmutableRecord {
    let regs = &registers[*start_reg..*start_reg + *count];
    ImmutableRecord::from_registers(regs, regs.len())
}

pub fn registers_to_ref_values<'a>(
    registers: &'a [Register],
) -> impl ExactSizeIterator<Item = ValueRef<'a>> {
    registers.iter().map(|reg| reg.get_value().as_ref())
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
        let value = unsafe {
            self.values
                .add(idx)
                .as_ref()
                .expect("row value pointer should be valid")
        };
        let value = match value {
            Register::Value(value) => value,
            _ => unreachable!("a row should be formed of values only"),
        };
        T::from_value(value)
    }

    pub fn get_value(&self, idx: usize) -> &Value {
        let value = unsafe {
            self.values
                .add(idx)
                .as_ref()
                .expect("row value pointer should be valid")
        };
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
