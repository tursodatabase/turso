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
//! Limbo is designed for applications that need high concurrency such as
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
pub mod sorter;

use crate::{
    error::LimboError,
    fast_lock::SpinLock,
    function::{AggFunc, FuncCtx},
    storage::{pager::PagerCacheflushStatus, sqlite3_ondisk::SmallVec},
    translate::plan::TableReferences,
};

use crate::{
    storage::{btree::BTreeCursor, pager::Pager, sqlite3_ondisk::DatabaseHeader},
    translate::plan::ResultSetColumn,
    types::{AggContext, Cursor, CursorResult, ImmutableRecord, SeekKey, SeekOp, Value},
    vdbe::{builder::CursorType, insn::Insn},
};

#[cfg(feature = "json")]
use crate::json::JsonCacheCell;
use crate::{Connection, MvStore, Result, TransactionState};
use builder::CursorKey;
use execute::{InsnFunction, InsnFunctionStepResult, OpIdxDeleteState};

use rand::{
    distributions::{Distribution, Uniform},
    Rng,
};
use regex::Regex;
use std::ptr::NonNull;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    num::NonZero,
    ops::Deref,
    rc::{Rc, Weak},
    sync::Arc,
};

/// We use labels to indicate that we want to jump to whatever the instruction offset
/// will be at runtime, because the offset cannot always be determined when the jump
/// instruction is created.
///
/// In some cases, we want to jump to EXACTLY a specific instruction.
/// - Example: a condition is not met, so we want to jump to wherever Halt is.
/// In other cases, we don't care what the exact instruction is, but we know that we
/// want to jump to whatever comes AFTER a certain instruction.
/// - Example: a Next instruction will want to jump to "whatever the start of the loop is",
/// but it doesn't care what instruction that is.
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
    pub fn to_offset_int(&self) -> InsnReference {
        match self {
            BranchOffset::Label(v) => unreachable!("Unresolved label: {}", v),
            BranchOffset::Offset(v) => *v,
            BranchOffset::Placeholder => unreachable!("Unresolved placeholder"),
        }
    }

    /// Returns the branch offset as a signed integer.
    /// Used in explain output, where we don't want to panic in case we have an unresolved
    /// label or placeholder.
    pub fn to_debug_int(&self) -> i32 {
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
        BranchOffset::Offset(self.to_offset_int() + n.into())
    }

    pub fn sub<N: Into<u32>>(self, n: N) -> BranchOffset {
        BranchOffset::Offset(self.to_offset_int() - n.into())
    }
}

pub type CursorID = usize;

pub type PageIdx = usize;

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

/// If there is I/O, the instruction is restarted.
/// Evaluate a Result<CursorResult<T>>, if IO return Ok(StepResult::IO).
#[macro_export]
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr? {
            CursorResult::Ok(v) => v,
            CursorResult::IO => return Ok(StepResult::IO),
        }
    };
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

#[derive(Copy, Clone, PartialEq, Eq)]
/// The commit state of the program.
/// There are two states:
/// - Ready: The program is ready to run the next instruction, or has shut down after
/// the last instruction.
/// - Committing: The program is committing a write transaction. It is waiting for the pager to finish flushing the cache to disk,
/// primarily to the WAL, but also possibly checkpointing the WAL to the database file.
enum CommitState {
    Ready,
    Committing,
}

#[derive(Debug, Clone)]
pub enum Register {
    Value(Value),
    Aggregate(AggContext),
    Record(ImmutableRecord),
}

/// A row is a the list of registers that hold the values for a filtered row. This row is a pointer, therefore
/// after stepping again, row will be invalidated to be sure it doesn't point to somewhere unexpected.
#[derive(Debug)]
pub struct Row {
    values: *const Register,
    count: usize,
}

/// The program state describes the environment in which the program executes.
pub struct ProgramState {
    pub pc: InsnReference,
    cursors: RefCell<Vec<Option<Cursor>>>,
    registers: Vec<Register>,
    pub(crate) result_row: Option<Row>,
    last_compare: Option<std::cmp::Ordering>,
    deferred_seeks: Vec<Option<(CursorID, CursorID)>>,
    ended_coroutine: Bitfield<4>, // flag to indicate that a coroutine has ended (key is the yield register. currently we assume that the yield register is always between 0-255, YOLO)
    /// Indicate whether an [Insn::Once] instruction at a given program counter position has already been executed, well, once.
    once: SmallVec<u32, 4>,
    regex_cache: RegexCache,
    pub(crate) mv_tx_id: Option<crate::mvcc::database::TxID>,
    interrupted: bool,
    parameters: HashMap<NonZero<usize>, Value>,
    commit_state: CommitState,
    #[cfg(feature = "json")]
    json_cache: JsonCacheCell,
    op_idx_delete_state: Option<OpIdxDeleteState>,
    expiration_status: Option<ExpirationStatus>,
}

impl ProgramState {
    pub fn new(max_registers: usize, max_cursors: usize) -> Self {
        let cursors: RefCell<Vec<Option<Cursor>>> =
            RefCell::new((0..max_cursors).map(|_| None).collect());
        let registers = vec![Register::Value(Value::Null); max_registers];
        Self {
            pc: 0,
            cursors,
            registers,
            result_row: None,
            last_compare: None,
            deferred_seeks: vec![None; max_cursors],
            ended_coroutine: Bitfield::new(),
            once: SmallVec::<u32, 4>::new(),
            regex_cache: RegexCache::new(),
            mv_tx_id: None,
            interrupted: false,
            parameters: HashMap::new(),
            commit_state: CommitState::Ready,
            #[cfg(feature = "json")]
            json_cache: JsonCacheCell::new(),
            op_idx_delete_state: None,
            expiration_status: None,
        }
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

    pub fn get_parameter(&self, index: NonZero<usize>) -> Value {
        self.parameters.get(&index).cloned().unwrap_or(Value::Null)
    }

    pub fn reset(&mut self) {
        self.pc = 0;
        self.cursors.borrow_mut().iter_mut().for_each(|c| *c = None);
        self.registers
            .iter_mut()
            .for_each(|r| *r = Register::Value(Value::Null));
        self.last_compare = None;
        self.deferred_seeks.iter_mut().for_each(|s| *s = None);
        self.ended_coroutine.0 = [0; 4];
        self.regex_cache.like.clear();
        self.interrupted = false;
        self.parameters.clear();
        #[cfg(feature = "json")]
        self.json_cache.clear();
        self.expiration_status = None;
    }

    pub fn get_cursor<'a>(&'a self, cursor_id: CursorID) -> std::cell::RefMut<'a, Cursor> {
        let cursors = self.cursors.borrow_mut();
        std::cell::RefMut::map(cursors, |c| {
            c.get_mut(cursor_id)
                .unwrap_or_else(|| panic!("cursor id {} out of bounds", cursor_id))
                .as_mut()
                .unwrap_or_else(|| panic!("cursor id {} is None", cursor_id))
        })
    }
}

#[derive(Debug)]
pub enum ExpirationStatus {
    Pending,
    Expired,
}

impl Register {
    pub fn get_owned_value(&self) -> &Value {
        match self {
            Register::Value(v) => v,
            _ => unreachable!(),
        }
    }
}

#[macro_export]
macro_rules! must_be_btree_cursor {
    ($cursor_id:expr, $cursor_ref:expr, $state:expr, $insn_name:expr) => {{
        let (_, cursor_type) = $cursor_ref.get($cursor_id).unwrap();
        let cursor = match cursor_type {
            CursorType::BTreeTable(_) => $state.get_cursor($cursor_id),
            CursorType::BTreeIndex(_) => $state.get_cursor($cursor_id),
            CursorType::Pseudo(_) => panic!("{} on pseudo cursor", $insn_name),
            CursorType::Sorter => panic!("{} on sorter cursor", $insn_name),
            CursorType::VirtualTable(_) => panic!("{} on virtual table cursor", $insn_name),
        };
        cursor
    }};
}

#[derive(Debug)]
pub struct Program {
    pub max_registers: usize,
    pub insns: Vec<(Insn, InsnFunction)>,
    pub cursor_ref: Vec<(Option<CursorKey>, CursorType)>,
    pub database_header: Arc<SpinLock<DatabaseHeader>>,
    pub comments: Option<Vec<(InsnReference, &'static str)>>,
    pub parameters: crate::parameters::Parameters,
    pub connection: Weak<Connection>,
    pub n_change: Cell<i64>,
    pub change_cnt_on: bool,
    pub result_columns: Vec<ResultSetColumn>,
    pub table_references: TableReferences,
    pub prev_program: Cell<Option<NonNull<Program>>>,
    pub next_program: Cell<Option<NonNull<Program>>>,
    pub expired: Option<ExpirationStatus>,
}

impl Program {
    pub fn step(
        &self,
        state: &mut ProgramState,
        mv_store: Option<Rc<MvStore>>,
        pager: Rc<Pager>,
    ) -> Result<StepResult> {
        if self.expired.is_some() || state.expiration_status.is_some() {
            return Err(LimboError::Schema);
        }

        loop {
            if state.is_interrupted() {
                return Ok(StepResult::Interrupt);
            }

            if let Some(ExpirationStatus::Expired) = state.expiration_status {
                return Err(LimboError::Schema);
            } else if let Some(ExpirationStatus::Pending) = state.expiration_status {
            }
            // invalidate row
            let _ = state.result_row.take();
            let (insn, insn_function) = &self.insns[state.pc as usize];
            trace_insn(self, state.pc as InsnReference, insn);
            let res = insn_function(self, state, insn, &pager, mv_store.as_ref())?;
            match res {
                InsnFunctionStepResult::Step => {}
                InsnFunctionStepResult::Done => return Ok(StepResult::Done),
                InsnFunctionStepResult::IO => return Ok(StepResult::IO),
                InsnFunctionStepResult::Row => return Ok(StepResult::Row),
                InsnFunctionStepResult::Interrupt => return Ok(StepResult::Interrupt),
                InsnFunctionStepResult::Busy => return Ok(StepResult::Busy),
            }
        }
    }

    pub fn commit_txn(
        &self,
        pager: Rc<Pager>,
        program_state: &mut ProgramState,
        mv_store: Option<&Rc<MvStore>>,
    ) -> Result<StepResult> {
        if let Some(mv_store) = mv_store {
            let conn = self.connection.upgrade().unwrap();
            let auto_commit = conn.auto_commit.get();
            if auto_commit {
                let mut mv_transactions = conn.mv_transactions.borrow_mut();
                for tx_id in mv_transactions.iter() {
                    mv_store.commit_tx(*tx_id).unwrap();
                }
                mv_transactions.clear();
            }
            Ok(StepResult::Done)
        } else {
            let connection = self
                .connection
                .upgrade()
                .expect("only weak ref to connection?");
            let auto_commit = connection.auto_commit.get();
            tracing::trace!("Halt auto_commit {}", auto_commit);
            if program_state.commit_state == CommitState::Committing {
                self.step_end_write_txn(&pager, &mut program_state.commit_state, connection.deref())
            } else if auto_commit {
                let current_state = connection.transaction_state.get();
                match current_state {
                    TransactionState::Write => self.step_end_write_txn(
                        &pager,
                        &mut program_state.commit_state,
                        connection.deref(),
                    ),
                    TransactionState::Read => {
                        connection.transaction_state.replace(TransactionState::None);
                        pager.end_read_tx()?;
                        Ok(StepResult::Done)
                    }
                    TransactionState::None => Ok(StepResult::Done),
                }
            } else {
                if self.change_cnt_on {
                    if let Some(conn) = self.connection.upgrade() {
                        conn.set_changes(self.n_change.get());
                    }
                }
                Ok(StepResult::Done)
            }
        }
    }

    fn step_end_write_txn(
        &self,
        pager: &Rc<Pager>,
        commit_state: &mut CommitState,
        connection: &Connection,
    ) -> Result<StepResult> {
        let cacheflush_status = pager.end_tx()?;
        match cacheflush_status {
            PagerCacheflushStatus::Done(_) => {
                if self.change_cnt_on {
                    if let Some(conn) = self.connection.upgrade() {
                        conn.set_changes(self.n_change.get());
                    }
                }
                connection.transaction_state.replace(TransactionState::None);
                *commit_state = CommitState::Ready;
            }
            PagerCacheflushStatus::IO => {
                tracing::trace!("Cacheflush IO");
                *commit_state = CommitState::Committing;
                return Ok(StepResult::IO);
            }
        }
        Ok(StepResult::Done)
    }

    #[rustfmt::skip]
    pub fn explain(&self) -> String {
        let mut buff = String::with_capacity(1024);
        buff.push_str("addr  opcode             p1    p2    p3    p4             p5  comment\n");
        buff.push_str("----  -----------------  ----  ----  ----  -------------  --  -------\n");
        let mut indent_count: usize = 0;
        let indent = "  ";
        let mut prev_insn: Option<&Insn> = None;
        for (addr, (insn, _)) in self.insns.iter().enumerate() {
            indent_count = get_indent_count(indent_count, insn, prev_insn);
            print_insn(
                self,
                addr as InsnReference,
                insn,
                indent.repeat(indent_count),
                &mut buff,
            );
            buff.push('\n');
            prev_insn = Some(insn);
        }
        buff
    }
}

impl Drop for Program {
    fn drop(&mut self) {
        if let Some(conn_rc) = self.connection.upgrade() {
            conn_rc.untrack_program(self);
        }
    }
}

fn get_new_rowid<R: Rng>(cursor: &mut BTreeCursor, mut rng: R) -> Result<CursorResult<i64>> {
    match cursor.seek_to_last()? {
        CursorResult::Ok(()) => {}
        CursorResult::IO => return Ok(CursorResult::IO),
    }
    let mut rowid = cursor
        .rowid()?
        .unwrap_or(0) // if BTree is empty - use 0 as initial value for rowid
        .checked_add(1) // add 1 but be careful with overflows
        .unwrap_or(i64::MAX); // in case of overflow - use i64::MAX
    if rowid > i64::MAX.try_into().unwrap() {
        let distribution = Uniform::from(1..=i64::MAX);
        let max_attempts = 100;
        for count in 0..max_attempts {
            rowid = distribution.sample(&mut rng).try_into().unwrap();
            match cursor.seek(SeekKey::TableRowId(rowid), SeekOp::EQ)? {
                CursorResult::Ok(false) => break, // Found a non-existing rowid
                CursorResult::Ok(true) => {
                    if count == max_attempts - 1 {
                        return Err(LimboError::InternalError(
                            "Failed to generate a new rowid".to_string(),
                        ));
                    } else {
                        continue; // Try next random rowid
                    }
                }
                CursorResult::IO => return Ok(CursorResult::IO),
            }
        }
    }
    Ok(CursorResult::Ok(rowid.try_into().unwrap()))
}

fn make_record(registers: &[Register], start_reg: &usize, count: &usize) -> ImmutableRecord {
    ImmutableRecord::from_registers(&registers[*start_reg..*start_reg + *count])
}

#[tracing::instrument(skip(program), level = tracing::Level::TRACE)]
fn trace_insn(program: &Program, addr: InsnReference, insn: &Insn) {
    if !tracing::enabled!(tracing::Level::TRACE) {
        return;
    }
    tracing::trace!(
        "{}",
        explain::insn_to_str(
            program,
            addr,
            insn,
            String::new(),
            program.comments.as_ref().and_then(|comments| comments
                .iter()
                .find(|(offset, _)| *offset == addr)
                .map(|(_, comment)| comment)
                .copied())
        )
    );
}

fn print_insn(program: &Program, addr: InsnReference, insn: &Insn, indent: String, w: &mut String) {
    let s = explain::insn_to_str(
        program,
        addr,
        insn,
        indent,
        program.comments.as_ref().and_then(|comments| {
            comments
                .iter()
                .find(|(offset, _)| *offset == addr)
                .map(|(_, comment)| comment)
                .copied()
        }),
    );
    w.push_str(&s);
}

fn get_indent_count(indent_count: usize, curr_insn: &Insn, prev_insn: Option<&Insn>) -> usize {
    let indent_count = if let Some(insn) = prev_insn {
        match insn {
            Insn::Rewind { .. }
            | Insn::Last { .. }
            | Insn::SorterSort { .. }
            | Insn::SeekGE { .. }
            | Insn::SeekGT { .. }
            | Insn::SeekLE { .. }
            | Insn::SeekLT { .. } => indent_count + 1,

            _ => indent_count,
        }
    } else {
        indent_count
    };

    match curr_insn {
        Insn::Next { .. } | Insn::SorterNext { .. } | Insn::Prev { .. } => indent_count - 1,
        _ => indent_count,
    }
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
            Register::Value(owned_value) => owned_value,
            _ => unreachable!("a row should be formed of values only"),
        };
        T::from_value(value)
    }

    pub fn get_value<'a>(&'a self, idx: usize) -> &'a Value {
        let value = unsafe { self.values.add(idx).as_ref().unwrap() };
        match value {
            Register::Value(owned_value) => owned_value,
            _ => unreachable!("a row should be formed of values only"),
        }
    }

    pub fn get_values(&self) -> impl Iterator<Item = &Value> {
        let values = unsafe { std::slice::from_raw_parts(self.values, self.count) };
        // This should be ownedvalues
        // TODO: add check for this
        values.iter().map(|v| v.get_owned_value())
    }

    pub fn len(&self) -> usize {
        self.count
    }
}
