#![allow(unused_variables)]
use crate::error::SQLITE_CONSTRAINT_UNIQUE;
use crate::function::AlterTableFunc;
use crate::mvcc::database::CheckpointStateMachine;
use crate::numeric::{NullableInteger, Numeric};
use crate::schema::Table;
use crate::state_machine::StateMachine;
use crate::storage::btree::{
    integrity_check, IntegrityCheckError, IntegrityCheckState, PageCategory,
};
use crate::storage::database::DatabaseFile;
use crate::storage::page_cache::PageCache;
use crate::storage::pager::{AtomicDbState, CreateBTreeFlags, DbState};
use crate::storage::sqlite3_ondisk::{read_varint, DatabaseHeader, PageSize};
use crate::translate::collate::CollationSeq;
use crate::types::{
    compare_immutable, compare_records_generic, Extendable, IOCompletions, ImmutableRecord,
    SeekResult, Text,
};
use crate::util::normalize_ident;
use crate::vdbe::insn::InsertFlags;
use crate::vdbe::registers_to_ref_values;
use crate::vector::{vector_concat, vector_slice};
use crate::{
    error::{
        LimboError, SQLITE_CONSTRAINT, SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY,
    },
    ext::ExtValue,
    function::{AggFunc, ExtFunc, MathFunc, MathFuncArity, ScalarFunc, VectorFunc},
    functions::{
        datetime::{
            exec_date, exec_datetime_full, exec_julianday, exec_strftime, exec_time, exec_unixepoch,
        },
        printf::exec_printf,
    },
    translate::emitter::TransactionMode,
};
use crate::{get_cursor, CheckpointMode, MvCursor};
use std::env::temp_dir;
use std::ops::DerefMut;
use std::{
    borrow::BorrowMut,
    sync::{atomic::Ordering, Arc, Mutex},
};
use turso_macros::match_ignore_ascii_case;

use crate::pseudo::PseudoCursor;

use crate::{
    schema::{affinity, Affinity},
    storage::btree::{BTreeCursor, BTreeKey},
};

use crate::{
    storage::wal::CheckpointResult,
    types::{
        AggContext, Cursor, ExternalAggState, IOResult, SeekKey, SeekOp, SumAggState, Value,
        ValueType,
    },
    util::{cast_real_to_integer, checked_cast_text_to_numeric, parse_schema_rows},
    vdbe::{
        builder::CursorType,
        insn::{IdxInsertFlags, Insn},
    },
    vector::{vector32, vector64, vector_distance_cos, vector_distance_l2, vector_extract},
};

use crate::{info, turso_assert, OpenFlags, RefValue, Row, TransactionState};

use super::{
    insn::{Cookie, RegisterOrLiteral},
    CommitState,
};
use parking_lot::RwLock;
use rand::{thread_rng, Rng};
use turso_parser::ast;
use turso_parser::parser::Parser;

use super::{
    likeop::{construct_like_escape_arg, exec_glob, exec_like_with_escape},
    sorter::Sorter,
};
use regex::{Regex, RegexBuilder};
use std::collections::HashMap;

#[cfg(feature = "json")]
use crate::{
    function::JsonFunc, json, json::convert_dbtype_to_raw_jsonb, json::get_json,
    json::is_json_valid, json::json_array, json::json_array_length, json::json_arrow_extract,
    json::json_arrow_shift_extract, json::json_error_position, json::json_extract,
    json::json_from_raw_bytes_agg, json::json_insert, json::json_object, json::json_patch,
    json::json_quote, json::json_remove, json::json_replace, json::json_set, json::json_type,
    json::jsonb, json::jsonb_array, json::jsonb_extract, json::jsonb_insert, json::jsonb_object,
    json::jsonb_patch, json::jsonb_remove, json::jsonb_replace, json::jsonb_set,
};

use super::{make_record, Program, ProgramState, Register};

#[cfg(feature = "fs")]
use crate::resolve_ext_path;
use crate::{bail_constraint_error, must_be_btree_cursor, MvStore, Pager, Result};

/// Macro to destructure an Insn enum variant, only to be used when it
/// is *impossible* to be another variant.
macro_rules! load_insn {
    ($variant:ident { $($field:tt $(: $binding:pat)?),* $(,)? }, $insn:expr) => {
        #[cfg(debug_assertions)]
        let Insn::$variant { $($field $(: $binding)?),* } = $insn else {
            panic!("Expected Insn::{}, got {:?}", stringify!($variant), $insn);
        };
        #[cfg(not(debug_assertions))]
        let Insn::$variant { $($field $(: $binding)?),*} = $insn else {
             // this will optimize away the branch
            unsafe { std::hint::unreachable_unchecked() };
        };
    };
}

macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr {
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
            Err(err) => return Err(err),
        }
    };
}

pub type InsnFunction = fn(
    &Program,
    &mut ProgramState,
    &Insn,
    &Arc<Pager>,
    Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult>;

/// Compare two values using the specified collation for text values.
/// Non-text values are compared using their natural ordering.
fn compare_with_collation(
    lhs: &Value,
    rhs: &Value,
    collation: Option<CollationSeq>,
) -> std::cmp::Ordering {
    match (lhs, rhs) {
        (Value::Text(lhs_text), Value::Text(rhs_text)) => {
            if let Some(coll) = collation {
                coll.compare_strings(lhs_text.as_str(), rhs_text.as_str())
            } else {
                lhs.cmp(rhs)
            }
        }
        _ => lhs.cmp(rhs),
    }
}

pub enum InsnFunctionStepResult {
    Done,
    IO(IOCompletions),
    Row,
    Interrupt,
    Step,
}

impl<T> From<IOResult<T>> for InsnFunctionStepResult {
    fn from(value: IOResult<T>) -> Self {
        match value {
            IOResult::Done(_) => InsnFunctionStepResult::Done,
            IOResult::IO(io) => InsnFunctionStepResult::IO(io),
        }
    }
}

pub fn op_init(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Init { target_pc }, insn);
    assert!(target_pc.is_offset());
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Add { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_add(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_subtract(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Subtract { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_subtract(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_multiply(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Multiply { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_multiply(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_divide(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Divide { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_divide(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_index(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(DropIndex { index, db: _ }, insn);
    program
        .connection
        .with_schema_mut(|schema| schema.remove_index(index));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_remainder(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Remainder { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_remainder(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_and(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(BitAnd { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_bit_and(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_or(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(BitOr { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_bit_or(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_not(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(BitNot { reg, dest }, insn);
    state.registers[*dest] = Register::Value(state.registers[*reg].get_value().exec_bit_not());
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Debug)]
pub enum OpCheckpointState {
    StartCheckpoint,
    FinishCheckpoint { result: Option<CheckpointResult> },
    CompleteResult { result: Result<CheckpointResult> },
}

pub fn op_checkpoint(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    match op_checkpoint_inner(program, state, insn, pager, mv_store) {
        Ok(result) => Ok(result),
        Err(err) => {
            state.op_checkpoint_state = OpCheckpointState::StartCheckpoint;
            Err(err)
        }
    }
}

pub fn op_checkpoint_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Checkpoint {
            database: _,
            checkpoint_mode,
            dest,
        },
        insn
    );
    if !program.connection.auto_commit.load(Ordering::SeqCst) {
        // TODO: sqlite returns "Runtime error: database table is locked (6)" when a table is in use
        // when a checkpoint is attempted. We don't have table locks, so return TableLocked for any
        // attempt to checkpoint in an interactive transaction. This does not end the transaction,
        // however.
        return Err(LimboError::TableLocked);
    }
    if let Some(mv_store) = mv_store {
        if !matches!(checkpoint_mode, CheckpointMode::Truncate { .. }) {
            return Err(LimboError::InvalidArgument(
                "Only TRUNCATE checkpoint mode is supported for MVCC".to_string(),
            ));
        }
        let mut ckpt_sm = StateMachine::new(CheckpointStateMachine::new(
            pager.clone(),
            mv_store.clone(),
            program.connection.clone(),
        ));
        loop {
            let result = ckpt_sm.step(&())?;
            match result {
                IOResult::IO(io) => {
                    pager.io.step()?;
                }
                IOResult::Done(result) => {
                    state.op_checkpoint_state =
                        OpCheckpointState::CompleteResult { result: Ok(result) };
                    break;
                }
            }
        }
    }
    loop {
        match &mut state.op_checkpoint_state {
            OpCheckpointState::StartCheckpoint => {
                let step_result = program
                    .connection
                    .pager
                    .write()
                    .wal_checkpoint_start(*checkpoint_mode);
                match step_result {
                    Ok(IOResult::Done(result)) => {
                        state.op_checkpoint_state = OpCheckpointState::FinishCheckpoint {
                            result: Some(result),
                        };
                        continue;
                    }
                    Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
                    Err(err) => {
                        state.op_checkpoint_state =
                            OpCheckpointState::CompleteResult { result: Err(err) };
                        continue;
                    }
                }
            }
            OpCheckpointState::FinishCheckpoint { result } => {
                let step_result = program
                    .connection
                    .pager
                    .write()
                    .wal_checkpoint_finish(result.as_mut().unwrap());
                match step_result {
                    Ok(IOResult::Done(())) => {
                        state.op_checkpoint_state = OpCheckpointState::CompleteResult {
                            result: Ok(result.take().unwrap()),
                        };
                        continue;
                    }
                    Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
                    Err(err) => {
                        state.op_checkpoint_state =
                            OpCheckpointState::CompleteResult { result: Err(err) };
                        continue;
                    }
                }
            }
            OpCheckpointState::CompleteResult { result } => {
                match result {
                    Ok(CheckpointResult {
                        num_attempted,
                        num_backfilled,
                        ..
                    }) => {
                        // https://sqlite.org/pragma.html#pragma_wal_checkpoint
                        // 1st col: 1 (checkpoint SQLITE_BUSY) or 0 (not busy).
                        state.registers[*dest] = Register::Value(Value::Integer(0));
                        // 2nd col: # modified pages written to wal file
                        state.registers[*dest + 1] =
                            Register::Value(Value::Integer(*num_attempted as i64));
                        // 3rd col: # pages moved to db after checkpoint
                        state.registers[*dest + 2] =
                            Register::Value(Value::Integer(*num_backfilled as i64));
                    }
                    Err(_err) => state.registers[*dest] = Register::Value(Value::Integer(1)),
                }

                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    match insn {
        Insn::Null { dest, dest_end } | Insn::BeginSubrtn { dest, dest_end } => {
            if let Some(dest_end) = dest_end {
                for i in *dest..=*dest_end {
                    state.registers[i] = Register::Value(Value::Null);
                }
            } else {
                state.registers[*dest] = Register::Value(Value::Null);
            }
        }
        _ => unreachable!("unexpected Insn {:?}", insn),
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_null_row(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(NullRow { cursor_id }, insn);
    {
        let cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "NullRow");
        let cursor = cursor.as_btree_mut();
        cursor.set_null_flag(true);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_compare(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Compare {
            start_reg_a,
            start_reg_b,
            count,
            collation,
        },
        insn
    );
    let start_reg_a = *start_reg_a;
    let start_reg_b = *start_reg_b;
    let count = *count;
    let collation = collation.unwrap_or_default();

    if start_reg_a + count > start_reg_b {
        return Err(LimboError::InternalError(
            "Compare registers overlap".to_string(),
        ));
    }

    let mut cmp = None;
    for i in 0..count {
        let a = state.registers[start_reg_a + i].get_value();
        let b = state.registers[start_reg_b + i].get_value();
        cmp = match (a, b) {
            (Value::Text(left), Value::Text(right)) => {
                Some(collation.compare_strings(left.as_str(), right.as_str()))
            }
            _ => Some(a.cmp(b)),
        };
        if cmp != Some(std::cmp::Ordering::Equal) {
            break;
        }
    }
    state.last_compare = cmp;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_jump(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Jump {
            target_pc_lt,
            target_pc_eq,
            target_pc_gt,
        },
        insn
    );
    assert!(target_pc_lt.is_offset());
    assert!(target_pc_eq.is_offset());
    assert!(target_pc_gt.is_offset());
    let cmp = state.last_compare.take();
    if cmp.is_none() {
        return Err(LimboError::InternalError(
            "Jump without compare".to_string(),
        ));
    }
    let target_pc = match cmp.unwrap() {
        std::cmp::Ordering::Less => *target_pc_lt,
        std::cmp::Ordering::Equal => *target_pc_eq,
        std::cmp::Ordering::Greater => *target_pc_gt,
    };
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_move(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Move {
            source_reg,
            dest_reg,
            count,
        },
        insn
    );
    let source_reg = *source_reg;
    let dest_reg = *dest_reg;
    let count = *count;
    for i in 0..count {
        state.registers[dest_reg + i] = std::mem::replace(
            &mut state.registers[source_reg + i],
            Register::Value(Value::Null),
        );
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if_pos(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IfPos {
            reg,
            target_pc,
            decrement_by,
        },
        insn
    );
    assert!(target_pc.is_offset());
    let reg = *reg;
    let target_pc = *target_pc;
    match state.registers[reg].get_value() {
        Value::Integer(n) if *n > 0 => {
            state.pc = target_pc.as_offset_int();
            state.registers[reg] = Register::Value(Value::Integer(*n - *decrement_by as i64));
        }
        Value::Integer(_) => {
            state.pc += 1;
        }
        _ => {
            return Err(LimboError::InternalError(
                "IfPos: the value in the register is not an integer".into(),
            ));
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_not_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(NotNull { reg, target_pc }, insn);
    assert!(target_pc.is_offset());
    let reg = *reg;
    let target_pc = *target_pc;
    match &state.registers[reg].get_value() {
        Value::Null => {
            state.pc += 1;
        }
        _ => {
            state.pc = target_pc.as_offset_int();
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl ComparisonOp {
    fn compare(&self, lhs: &Value, rhs: &Value, collation: &CollationSeq) -> bool {
        match (lhs, rhs) {
            (Value::Text(lhs_text), Value::Text(rhs_text)) => {
                let order = collation.compare_strings(lhs_text.as_str(), rhs_text.as_str());
                match self {
                    ComparisonOp::Eq => order.is_eq(),
                    ComparisonOp::Ne => order.is_ne(),
                    ComparisonOp::Lt => order.is_lt(),
                    ComparisonOp::Le => order.is_le(),
                    ComparisonOp::Gt => order.is_gt(),
                    ComparisonOp::Ge => order.is_ge(),
                }
            }
            (_, _) => match self {
                ComparisonOp::Eq => *lhs == *rhs,
                ComparisonOp::Ne => *lhs != *rhs,
                ComparisonOp::Lt => *lhs < *rhs,
                ComparisonOp::Le => *lhs <= *rhs,
                ComparisonOp::Gt => *lhs > *rhs,
                ComparisonOp::Ge => *lhs >= *rhs,
            },
        }
    }

    fn compare_integers(&self, lhs: &Value, rhs: &Value) -> bool {
        match self {
            ComparisonOp::Eq => lhs == rhs,
            ComparisonOp::Ne => lhs != rhs,
            ComparisonOp::Lt => lhs < rhs,
            ComparisonOp::Le => lhs <= rhs,
            ComparisonOp::Gt => lhs > rhs,
            ComparisonOp::Ge => lhs >= rhs,
        }
    }

    fn handle_nulls(&self, lhs: &Value, rhs: &Value, null_eq: bool, jump_if_null: bool) -> bool {
        match self {
            ComparisonOp::Eq => {
                let both_null = lhs == rhs;
                (null_eq && both_null) || (!null_eq && jump_if_null)
            }
            ComparisonOp::Ne => {
                let at_least_one_null = lhs != rhs;
                (null_eq && at_least_one_null) || (!null_eq && jump_if_null)
            }
            ComparisonOp::Lt | ComparisonOp::Le | ComparisonOp::Gt | ComparisonOp::Ge => {
                jump_if_null
            }
        }
    }
}

pub fn op_comparison(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let (lhs, rhs, target_pc, flags, collation, op) = match insn {
        Insn::Eq {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        } => (
            *lhs,
            *rhs,
            *target_pc,
            *flags,
            collation.unwrap_or_default(),
            ComparisonOp::Eq,
        ),
        Insn::Ne {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        } => (
            *lhs,
            *rhs,
            *target_pc,
            *flags,
            collation.unwrap_or_default(),
            ComparisonOp::Ne,
        ),
        Insn::Lt {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        } => (
            *lhs,
            *rhs,
            *target_pc,
            *flags,
            collation.unwrap_or_default(),
            ComparisonOp::Lt,
        ),
        Insn::Le {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        } => (
            *lhs,
            *rhs,
            *target_pc,
            *flags,
            collation.unwrap_or_default(),
            ComparisonOp::Le,
        ),
        Insn::Gt {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        } => (
            *lhs,
            *rhs,
            *target_pc,
            *flags,
            collation.unwrap_or_default(),
            ComparisonOp::Gt,
        ),
        Insn::Ge {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        } => (
            *lhs,
            *rhs,
            *target_pc,
            *flags,
            collation.unwrap_or_default(),
            ComparisonOp::Ge,
        ),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };

    assert!(target_pc.is_offset());

    let nulleq = flags.has_nulleq();
    let jump_if_null = flags.has_jump_if_null();
    let affinity = flags.get_affinity();

    let lhs_value = state.registers[lhs].get_value();
    let rhs_value = state.registers[rhs].get_value();

    // Fast path for integers
    if matches!(lhs_value, Value::Integer(_)) && matches!(rhs_value, Value::Integer(_)) {
        if op.compare_integers(lhs_value, rhs_value) {
            state.pc = target_pc.as_offset_int();
        } else {
            state.pc += 1;
        }
        return Ok(InsnFunctionStepResult::Step);
    }

    // Handle NULL values
    if matches!(lhs_value, Value::Null) || matches!(rhs_value, Value::Null) {
        if op.handle_nulls(lhs_value, rhs_value, nulleq, jump_if_null) {
            state.pc = target_pc.as_offset_int();
        } else {
            state.pc += 1;
        }
        return Ok(InsnFunctionStepResult::Step);
    }

    let mut lhs_temp_reg = None;
    let mut rhs_temp_reg = None;

    let mut lhs_converted = false;
    let mut rhs_converted = false;

    // Apply affinity conversions
    match affinity {
        Affinity::Numeric | Affinity::Integer => {
            let lhs_is_text = matches!(state.registers[lhs].get_value(), Value::Text(_));
            let rhs_is_text = matches!(state.registers[rhs].get_value(), Value::Text(_));

            if lhs_is_text || rhs_is_text {
                if lhs_is_text {
                    lhs_temp_reg = Some(state.registers[lhs].clone());
                    lhs_converted = apply_numeric_affinity(lhs_temp_reg.as_mut().unwrap(), false);
                }
                if rhs_is_text {
                    rhs_temp_reg = Some(state.registers[rhs].clone());
                    rhs_converted = apply_numeric_affinity(rhs_temp_reg.as_mut().unwrap(), false);
                }
            }
        }

        Affinity::Text => {
            let lhs_is_text = matches!(state.registers[lhs].get_value(), Value::Text(_));
            let rhs_is_text = matches!(state.registers[rhs].get_value(), Value::Text(_));

            if lhs_is_text || rhs_is_text {
                if is_numeric_value(&state.registers[lhs]) {
                    lhs_temp_reg = Some(state.registers[lhs].clone());
                    lhs_converted = stringify_register(lhs_temp_reg.as_mut().unwrap());
                }

                if is_numeric_value(&state.registers[rhs]) {
                    rhs_temp_reg = Some(state.registers[rhs].clone());
                    rhs_converted = stringify_register(rhs_temp_reg.as_mut().unwrap());
                }
            }
        }

        Affinity::Real => {
            if matches!(state.registers[lhs].get_value(), Value::Text(_)) {
                lhs_temp_reg = Some(state.registers[lhs].clone());
                lhs_converted = apply_numeric_affinity(lhs_temp_reg.as_mut().unwrap(), false);
            }

            if matches!(state.registers[rhs].get_value(), Value::Text(_)) {
                rhs_temp_reg = Some(state.registers[rhs].clone());
                rhs_converted = apply_numeric_affinity(rhs_temp_reg.as_mut().unwrap(), false);
            }

            if let Value::Integer(i) =
                (lhs_temp_reg.as_ref().unwrap_or(&state.registers[lhs])).get_value()
            {
                lhs_temp_reg = Some(Register::Value(Value::Float(*i as f64)));
                lhs_converted = true;
            }

            if let Value::Integer(i) = rhs_temp_reg
                .as_ref()
                .unwrap_or(&state.registers[rhs])
                .get_value()
            {
                rhs_temp_reg = Some(Register::Value(Value::Float(*i as f64)));
                rhs_converted = true;
            }
        }

        Affinity::Blob => {} // Do nothing for blob affinity.
    }

    let should_jump = op.compare(
        lhs_temp_reg
            .as_ref()
            .unwrap_or(&state.registers[lhs])
            .get_value(),
        rhs_temp_reg
            .as_ref()
            .unwrap_or(&state.registers[rhs])
            .get_value(),
        &collation,
    );

    if lhs_converted {
        state.registers[lhs] = lhs_temp_reg.unwrap();
    }

    if rhs_converted {
        state.registers[rhs] = rhs_temp_reg.unwrap();
    }

    if should_jump {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }

    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        If {
            reg,
            target_pc,
            jump_if_null,
        },
        insn
    );
    assert!(target_pc.is_offset());
    if state.registers[*reg]
        .get_value()
        .exec_if(*jump_if_null, false)
    {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if_not(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IfNot {
            reg,
            target_pc,
            jump_if_null,
        },
        insn
    );
    assert!(target_pc.is_offset());
    if state.registers[*reg]
        .get_value()
        .exec_if(*jump_if_null, true)
    {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_open_read(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        OpenRead {
            cursor_id,
            root_page,
            db,
        },
        insn
    );

    let pager = program.get_pager_from_database_index(db);

    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let mv_cursor = if let Some(tx_id) = program.connection.get_mv_tx_id() {
        let table_id = *root_page as u64;
        let mv_store = mv_store.unwrap().clone();
        let mv_cursor = Arc::new(RwLock::new(
            MvCursor::new(mv_store, tx_id, table_id, pager.clone()).unwrap(),
        ));
        Some(mv_cursor)
    } else {
        None
    };
    let cursors = &mut state.cursors;
    let num_columns = match cursor_type {
        CursorType::BTreeTable(table_rc) => table_rc.columns.len(),
        CursorType::BTreeIndex(index_arc) => index_arc.columns.len(),
        CursorType::MaterializedView(table_rc, _) => table_rc.columns.len(),
        _ => unreachable!("This should not have happened"),
    };

    match cursor_type {
        CursorType::MaterializedView(_, view_mutex) => {
            // This is a materialized view with storage
            // Create btree cursor for reading the persistent data
            let btree_cursor = Box::new(BTreeCursor::new_table(
                mv_cursor,
                pager.clone(),
                *root_page,
                num_columns,
            ));

            // Get the view name and look up or create its transaction state
            let view_name = view_mutex.lock().unwrap().name().to_string();
            let tx_state = program
                .connection
                .view_transaction_states
                .get_or_create(&view_name);

            // Create materialized view cursor with this view's transaction state
            let mv_cursor = crate::incremental::cursor::MaterializedViewCursor::new(
                btree_cursor,
                view_mutex.clone(),
                pager.clone(),
                tx_state,
            )?;

            cursors
                .get_mut(*cursor_id)
                .unwrap()
                .replace(Cursor::new_materialized_view(mv_cursor));
        }
        CursorType::BTreeTable(_) => {
            // Regular table
            let cursor = BTreeCursor::new_table(mv_cursor, pager.clone(), *root_page, num_columns);
            cursors
                .get_mut(*cursor_id)
                .unwrap()
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::BTreeIndex(index) => {
            let cursor = BTreeCursor::new_index(
                mv_cursor,
                pager.clone(),
                *root_page,
                index.as_ref(),
                num_columns,
            );
            cursors
                .get_mut(*cursor_id)
                .unwrap()
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::Pseudo(_) => {
            panic!("OpenRead on pseudo cursor");
        }
        CursorType::Sorter => {
            panic!("OpenRead on sorter cursor");
        }
        CursorType::VirtualTable(_) => {
            panic!("OpenRead on virtual table cursor, use Insn:VOpen instead");
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vopen(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(VOpen { cursor_id }, insn);
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VOpen on non-virtual table cursor");
    };
    let cursor = virtual_table.open(program.connection.clone())?;
    state
        .cursors
        .get_mut(*cursor_id)
        .unwrap_or_else(|| panic!("cursor id {} out of bounds", *cursor_id))
        .replace(Cursor::Virtual(cursor));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vcreate(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        VCreate {
            module_name,
            table_name,
            args_reg,
        },
        insn
    );
    let module_name = state.registers[*module_name].get_value().to_string();
    let table_name = state.registers[*table_name].get_value().to_string();
    let args = if let Some(args_reg) = args_reg {
        if let Register::Record(rec) = &state.registers[*args_reg] {
            rec.get_values().iter().map(|v| v.to_ffi()).collect()
        } else {
            return Err(LimboError::InternalError(
                "VCreate: args_reg is not a record".to_string(),
            ));
        }
    } else {
        vec![]
    };
    let conn = program.connection.clone();
    let table =
        crate::VirtualTable::table(Some(&table_name), &module_name, args, &conn.syms.read())?;
    {
        conn.syms.write().vtabs.insert(table_name, table.clone());
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vfilter(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        VFilter {
            cursor_id,
            pc_if_empty,
            arg_count,
            args_reg,
            idx_str,
            idx_num,
        },
        insn
    );
    let has_rows = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_virtual_mut();
        let mut args = Vec::with_capacity(*arg_count);
        for i in 0..*arg_count {
            args.push(state.registers[args_reg + i].get_value().clone());
        }
        let idx_str = if let Some(idx_str) = idx_str {
            Some(state.registers[*idx_str].get_value().to_string())
        } else {
            None
        };
        cursor.filter(*idx_num as i32, idx_str, *arg_count, args)?
    };
    // Increment filter_operations metric for virtual table filter
    state.metrics.filter_operations = state.metrics.filter_operations.saturating_add(1);
    if !has_rows {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        // VFilter positions to the first row if any exist, which counts as a read
        state.metrics.rows_read = state.metrics.rows_read.saturating_add(1);
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vcolumn(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        VColumn {
            cursor_id,
            column,
            dest,
        },
        insn
    );
    let value = {
        let cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_virtual_mut();
        cursor.column(*column)?
    };
    state.registers[*dest] = Register::Value(value);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vupdate(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        VUpdate {
            cursor_id,
            arg_count,
            start_reg,
            conflict_action,
            ..
        },
        insn
    );
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VUpdate on non-virtual table cursor");
    };
    if virtual_table.readonly() {
        return Err(LimboError::ReadOnly);
    }

    if *arg_count < 2 {
        return Err(LimboError::InternalError(
            "VUpdate: arg_count must be at least 2 (rowid and insert_rowid)".to_string(),
        ));
    }
    let mut argv = Vec::with_capacity(*arg_count);
    for i in 0..*arg_count {
        if let Some(value) = state.registers.get(*start_reg + i) {
            argv.push(value.get_value().clone());
        } else {
            return Err(LimboError::InternalError(format!(
                "VUpdate: register out of bounds at {}",
                *start_reg + i
            )));
        }
    }
    let result = virtual_table.update(&argv);
    match result {
        Ok(Some(new_rowid)) => {
            if *conflict_action == 5 {
                // ResolveType::Replace
                program.connection.update_last_rowid(new_rowid);
            }
            state.pc += 1;
        }
        Ok(None) => {
            // no-op or successful update without rowid return
            state.pc += 1;
        }
        Err(e) => {
            // virtual table update failed
            return Err(LimboError::ExtensionError(format!(
                "Virtual table update failed: {e}"
            )));
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vnext(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        VNext {
            cursor_id,
            pc_if_next,
        },
        insn
    );
    let has_more = {
        let cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_virtual_mut();
        cursor.next()?
    };
    if has_more {
        // Increment metrics for row read from virtual table (including materialized views)
        state.metrics.rows_read = state.metrics.rows_read.saturating_add(1);
        state.pc = pc_if_next.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vdestroy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(VDestroy { db, table_name }, insn);
    let conn = program.connection.clone();
    {
        let Some(vtab) = conn.syms.write().vtabs.remove(table_name) else {
            return Err(crate::LimboError::InternalError(
                "Could not find Virtual Table to Destroy".to_string(),
            ));
        };
        vtab.destroy()?;
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_open_pseudo(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        OpenPseudo {
            cursor_id,
            content_reg: _,
            num_fields: _,
        },
        insn
    );
    {
        let cursors = &mut state.cursors;
        let cursor = PseudoCursor::default();
        cursors
            .get_mut(*cursor_id)
            .unwrap()
            .replace(Cursor::new_pseudo(cursor));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_rewind(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Rewind {
            cursor_id,
            pc_if_empty,
        },
        insn
    );
    assert!(pc_if_empty.is_offset());
    let is_empty = {
        let cursor = state.get_cursor(*cursor_id);
        match cursor {
            Cursor::BTree(btree_cursor) => {
                return_if_io!(btree_cursor.rewind());
                btree_cursor.is_empty()
            }
            Cursor::MaterializedView(mv_cursor) => {
                return_if_io!(mv_cursor.rewind());
                !mv_cursor.is_valid()?
            }
            _ => panic!("Rewind on non-btree/materialized-view cursor"),
        }
    };
    if is_empty {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        // Rewind positions to the first row, which is effectively a read
        state.metrics.rows_read = state.metrics.rows_read.saturating_add(1);
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_last(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Last {
            cursor_id,
            pc_if_empty,
        },
        insn
    );
    assert!(pc_if_empty.is_offset());
    let is_empty = {
        let cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Last");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.last());
        cursor.is_empty()
    };
    if is_empty {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        // Last positions to the last row, which is effectively a read
        state.metrics.rows_read = state.metrics.rows_read.saturating_add(1);
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

/// Fast varint reader optimized for the common cases of 1-byte and 2-byte varints.
///
/// This function is a performance-optimized version of `read_varint()` that handles
/// the most common varint cases inline before falling back to the full implementation.
/// It follows the same varint encoding as SQLite.
///
/// # Optimized Cases
///
/// - **Single-byte case**: Values 0-127 (0x00-0x7F) are returned immediately
/// - **Two-byte case**: Values 128-16383 (0x80-0x3FFF) are handled inline
/// - **Multi-byte case**: Larger values fall back to the full `read_varint()` implementation
///
/// This function is similar to `sqlite3GetVarint32`
#[inline(always)]
fn read_varint_fast(buf: &[u8]) -> Result<(u64, usize)> {
    // Fast path: Single-byte varint
    if let Some(&first_byte) = buf.first() {
        if first_byte & 0x80 == 0 {
            return Ok((first_byte as u64, 1));
        }
    } else {
        crate::bail_corrupt_error!("Invalid varint");
    }

    // Fast path: Two-byte varint
    if let Some(&second_byte) = buf.get(1) {
        if second_byte & 0x80 == 0 {
            let v = (((buf[0] & 0x7f) as u64) << 7) + (second_byte as u64);
            return Ok((v, 2));
        }
    } else {
        crate::bail_corrupt_error!("Invalid varint");
    }

    //Fallback: Multi-byte varint
    read_varint(buf)
}

#[derive(Debug, Clone, Copy)]
pub enum OpColumnState {
    Start,
    Rowid {
        index_cursor_id: usize,
        table_cursor_id: usize,
    },
    Seek {
        rowid: i64,
        table_cursor_id: usize,
    },
    GetColumn,
}

pub fn op_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Column {
            cursor_id,
            column,
            dest,
            default,
        },
        insn
    );
    'outer: loop {
        match state.op_column_state {
            OpColumnState::Start => {
                if let Some((index_cursor_id, table_cursor_id)) =
                    state.deferred_seeks[*cursor_id].take()
                {
                    state.op_column_state = OpColumnState::Rowid {
                        index_cursor_id,
                        table_cursor_id,
                    };
                } else {
                    state.op_column_state = OpColumnState::GetColumn;
                }
            }
            OpColumnState::Rowid {
                index_cursor_id,
                table_cursor_id,
            } => {
                let Some(rowid) = ({
                    let index_cursor = state.get_cursor(index_cursor_id);
                    let index_cursor = index_cursor.as_btree_mut();
                    return_if_io!(index_cursor.rowid())
                }) else {
                    state.registers[*dest] = Register::Value(Value::Null);
                    break 'outer;
                };
                state.op_column_state = OpColumnState::Seek {
                    rowid,
                    table_cursor_id,
                };
            }
            OpColumnState::Seek {
                rowid,
                table_cursor_id,
            } => {
                {
                    let table_cursor = state.get_cursor(table_cursor_id);
                    // MaterializedView cursors shouldn't go through deferred seek logic
                    // but if we somehow get here, handle it appropriately
                    match table_cursor {
                        Cursor::MaterializedView(mv_cursor) => {
                            // Seek to the rowid in the materialized view
                            return_if_io!(mv_cursor
                                .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));
                        }
                        _ => {
                            // Regular btree cursor
                            let table_cursor = table_cursor.as_btree_mut();
                            return_if_io!(table_cursor
                                .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));
                        }
                    }
                }
                state.op_column_state = OpColumnState::GetColumn;
            }
            OpColumnState::GetColumn => {
                // First check if this is a MaterializedViewCursor
                {
                    let cursor = state.get_cursor(*cursor_id);
                    if let Cursor::MaterializedView(mv_cursor) = cursor {
                        // Handle materialized view column access
                        let value = return_if_io!(mv_cursor.column(*column));
                        state.registers[*dest] = Register::Value(value);
                        break 'outer;
                    }
                    // Fall back to normal handling
                }

                let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
                match cursor_type {
                    CursorType::BTreeTable(_)
                    | CursorType::BTreeIndex(_)
                    | CursorType::MaterializedView(_, _) => {
                        'ifnull: {
                            let cursor_ref = must_be_btree_cursor!(
                                *cursor_id,
                                program.cursor_ref,
                                state,
                                "Column"
                            );
                            let cursor = cursor_ref.as_btree_mut();

                            if cursor.get_null_flag() {
                                state.registers[*dest] = Register::Value(Value::Null);
                                break 'outer;
                            }

                            let record_result = return_if_io!(cursor.record());
                            let Some(payload) = record_result.as_ref().map(|r| r.get_payload())
                            else {
                                break 'ifnull;
                            };

                            let mut record_cursor = cursor.record_cursor.borrow_mut();

                            if record_cursor.offsets.is_empty() {
                                let (header_size, header_len_bytes) = read_varint_fast(payload)?;
                                let header_size = header_size as usize;

                                debug_assert!(header_size <= payload.len() && header_size <= 98307, "header_size: {header_size}, header_len_bytes: {header_len_bytes}, payload.len(): {}", payload.len());

                                record_cursor.header_size = header_size;
                                record_cursor.header_offset = header_len_bytes;
                                record_cursor.offsets.push(header_size);
                            }

                            let target_column = *column;
                            let mut parse_pos = record_cursor.header_offset;
                            let mut data_offset = record_cursor
                                .offsets
                                .last()
                                .copied()
                                .expect("header_offset must be set");

                            // Parse the header for serial types incrementally until we have the target column
                            while record_cursor.serial_types.len() <= target_column
                                && parse_pos < record_cursor.header_size
                            {
                                let (serial_type, varint_len) =
                                    read_varint_fast(&payload[parse_pos..])?;
                                record_cursor.serial_types.push(serial_type);
                                parse_pos += varint_len;
                                let data_size = match serial_type {
                                    // NULL
                                    0 => 0,
                                    // I8
                                    1 => 1,
                                    // I16
                                    2 => 2,
                                    // I24
                                    3 => 3,
                                    // I32
                                    4 => 4,
                                    // I48
                                    5 => 6,
                                    // I64
                                    6 => 8,
                                    // F64
                                    7 => 8,
                                    // CONST_INT0
                                    8 => 0,
                                    // CONST_INT1
                                    9 => 0,
                                    // BLOB
                                    n if n >= 12 && n & 1 == 0 => (n - 12) >> 1,
                                    // TEXT
                                    n if n >= 13 && n & 1 == 1 => (n - 13) >> 1,
                                    // Reserved
                                    10 | 11 => {
                                        return Err(LimboError::Corrupt(format!(
                                            "Reserved serial type: {serial_type}"
                                        )))
                                    }
                                    _ => unreachable!("Invalid serial type: {serial_type}"),
                                } as usize;
                                data_offset += data_size;
                                record_cursor.offsets.push(data_offset);
                            }

                            debug_assert!(
                                parse_pos <= record_cursor.header_size,
                                "parse_pos: {parse_pos}, header_size: {}",
                                record_cursor.header_size
                            );
                            record_cursor.header_offset = parse_pos;
                            if target_column >= record_cursor.serial_types.len() {
                                break 'ifnull;
                            }

                            let start_offset = record_cursor.offsets[target_column];
                            let end_offset = record_cursor.offsets[target_column + 1];
                            // SAFETY: We know that the payload is valid until the next row is processed.
                            let buf = unsafe {
                                std::mem::transmute::<&[u8], &'static [u8]>(
                                    &payload[start_offset..end_offset],
                                )
                            };
                            let serial_type = record_cursor.serial_types[target_column];
                            drop(record_result);
                            drop(record_cursor);

                            match serial_type {
                                // NULL
                                0 => break 'ifnull,
                                // I8
                                1 => {
                                    state.registers[*dest] =
                                        Register::Value(Value::Integer(buf[0] as i8 as i64));
                                }
                                // I16
                                2 => {
                                    state.registers[*dest] =
                                        Register::Value(Value::Integer(i16::from_be_bytes([
                                            buf[0], buf[1],
                                        ])
                                            as i64));
                                }
                                // I24
                                3 => {
                                    let sign_extension = (buf[0] > 0x7F) as u8 * 0xFF;
                                    let value = Value::Integer(i32::from_be_bytes([
                                        sign_extension,
                                        buf[0],
                                        buf[1],
                                        buf[2],
                                    ])
                                        as i64);
                                    state.registers[*dest] = Register::Value(value);
                                }
                                // I32
                                4 => {
                                    let value = Value::Integer(i32::from_be_bytes([
                                        buf[0], buf[1], buf[2], buf[3],
                                    ])
                                        as i64);
                                    state.registers[*dest] = Register::Value(value);
                                }
                                // I48
                                5 => {
                                    let sign_extension = (buf[0] > 0x7F) as u8 * 0xFF;
                                    let value = Value::Integer(i64::from_be_bytes([
                                        sign_extension,
                                        sign_extension,
                                        buf[0],
                                        buf[1],
                                        buf[2],
                                        buf[3],
                                        buf[4],
                                        buf[5],
                                    ]));
                                    state.registers[*dest] = Register::Value(value);
                                }
                                // I64
                                6 => {
                                    let value = Value::Integer(i64::from_be_bytes(
                                        buf[..8].try_into().unwrap(),
                                    ));
                                    state.registers[*dest] = Register::Value(value);
                                }
                                // F64
                                7 => {
                                    let value = Value::Float(f64::from_be_bytes(
                                        buf[..8].try_into().unwrap(),
                                    ));
                                    state.registers[*dest] = Register::Value(value);
                                }
                                // CONST_INT0
                                8 => {
                                    state.registers[*dest] = Register::Value(Value::Integer(0));
                                }
                                // CONST_INT1
                                9 => {
                                    state.registers[*dest] = Register::Value(Value::Integer(1));
                                }
                                // BLOB
                                n if n >= 12 && n & 1 == 0 => {
                                    // Try to reuse the registers when allocation is not needed.
                                    match state.registers[*dest] {
                                        Register::Value(Value::Blob(ref mut existing_blob)) => {
                                            existing_blob.do_extend(&buf);
                                        }
                                        _ => {
                                            state.registers[*dest] =
                                                Register::Value(Value::Blob(buf.to_vec()));
                                        }
                                    }
                                }
                                // TEXT
                                n if n >= 13 && n & 1 == 1 => {
                                    // Try to reuse the registers when allocation is not needed.
                                    match state.registers[*dest] {
                                        Register::Value(Value::Text(ref mut existing_text)) => {
                                            // SAFETY: We know the text is valid UTF-8 because we only accept valid UTF-8 and the serial type is TEXT.
                                            let text =
                                                unsafe { std::str::from_utf8_unchecked(buf) };
                                            existing_text.do_extend(&text);
                                        }
                                        _ => {
                                            // SAFETY: We know the text is valid UTF-8 because we only accept valid UTF-8 and the serial type is TEXT.
                                            let text =
                                                unsafe { std::str::from_utf8_unchecked(buf) };
                                            state.registers[*dest] =
                                                Register::Value(Value::Text(Text::new(text)));
                                        }
                                    }
                                }
                                _ => panic!("Invalid serial type: {serial_type}"),
                            }

                            break 'outer;
                        };

                        // DEFAULT handling. Try to reuse the registers when allocation is not needed.
                        let Some(ref default) = default else {
                            state.registers[*dest] = Register::Value(Value::Null);
                            break;
                        };
                        match (default, &mut state.registers[*dest]) {
                            (
                                Value::Text(new_text),
                                Register::Value(Value::Text(existing_text)),
                            ) => {
                                existing_text.do_extend(new_text);
                            }
                            (
                                Value::Blob(new_blob),
                                Register::Value(Value::Blob(existing_blob)),
                            ) => {
                                existing_blob.do_extend(new_blob);
                            }
                            _ => {
                                state.registers[*dest] = Register::Value(default.clone());
                            }
                        }
                        break;
                    }
                    CursorType::Sorter => {
                        let record = {
                            let cursor = state.get_cursor(*cursor_id);
                            let cursor = cursor.as_sorter_mut();
                            cursor.record().cloned()
                        };
                        if let Some(record) = record {
                            state.registers[*dest] =
                                Register::Value(match record.get_value_opt(*column) {
                                    Some(val) => val.to_owned(),
                                    None => default.clone().unwrap_or(Value::Null),
                                });
                        } else {
                            state.registers[*dest] = Register::Value(Value::Null);
                        }
                    }
                    CursorType::Pseudo(_) => {
                        let value = {
                            let cursor = state.get_cursor(*cursor_id);
                            let cursor = cursor.as_pseudo_mut();
                            if let Some(record) = cursor.record() {
                                record.get_value(*column)?.to_owned()
                            } else {
                                Value::Null
                            }
                        };
                        state.registers[*dest] = Register::Value(value);
                    }
                    CursorType::VirtualTable(_) => {
                        panic!("Insn:Column on virtual table cursor, use Insn:VColumn instead");
                    }
                }
                break;
            }
        }
    }

    state.op_column_state = OpColumnState::Start;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_type_check(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        TypeCheck {
            start_reg,
            count,
            check_generated,
            table_reference,
        },
        insn
    );
    assert!(table_reference.is_strict);
    state.registers[*start_reg..*start_reg + *count]
        .iter_mut()
        .zip(table_reference.columns.iter())
        .try_for_each(|(reg, col)| {
            // INT PRIMARY KEY is not row_id_alias so we throw error if this col is NULL
            if !col.is_rowid_alias && col.primary_key && matches!(reg.get_value(), Value::Null) {
                bail_constraint_error!(
                    "NOT NULL constraint failed: {}.{} ({})",
                    &table_reference.name,
                    col.name.as_deref().unwrap_or(""),
                    SQLITE_CONSTRAINT
                )
            } else if col.is_rowid_alias && matches!(reg.get_value(), Value::Null) {
                // Handle INTEGER PRIMARY KEY for null as usual (Rowid will be auto-assigned)
                return Ok(());
            }
            let col_affinity = col.affinity();
            let ty_str = &col.ty_str;
            let ty_bytes = ty_str.as_bytes();
            let applied = apply_affinity_char(reg, col_affinity);
            let value_type = reg.get_value().value_type();
            match_ignore_ascii_case!(match ty_bytes {
                b"INTEGER" | b"INT" if value_type == ValueType::Integer => {}
                b"REAL" if value_type == ValueType::Float => {}
                b"BLOB" if value_type == ValueType::Blob => {}
                b"TEXT" if value_type == ValueType::Text => {}
                b"ANY" => {}
                _ => bail_constraint_error!(
                    "cannot store {} value in {} column {}.{} ({})",
                    value_type,
                    ty_str,
                    &table_reference.name,
                    col.name.as_deref().unwrap_or(""),
                    SQLITE_CONSTRAINT
                ),
            });
            Ok(())
        })?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_make_record(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        MakeRecord {
            start_reg,
            count,
            dest_reg,
            affinity_str,
            ..
        },
        insn
    );

    if let Some(affinity_str) = affinity_str {
        if affinity_str.len() != *count {
            return Err(LimboError::InternalError(format!(
                "MakeRecord: the length of affinity string ({}) does not match the count ({})",
                affinity_str.len(),
                *count
            )));
        }
        for (i, affinity_ch) in affinity_str.chars().enumerate().take(*count) {
            let reg_index = *start_reg + i;
            let affinity = Affinity::from_char(affinity_ch);
            apply_affinity_char(&mut state.registers[reg_index], affinity);
        }
    }

    let record = make_record(&state.registers, start_reg, count);
    state.registers[*dest_reg] = Register::Record(record);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_mem_max(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(MemMax { dest_reg, src_reg }, insn);

    let dest_val = state.registers[*dest_reg].get_value();
    let src_val = state.registers[*src_reg].get_value();

    let dest_int = extract_int_value(dest_val);
    let src_int = extract_int_value(src_val);

    if dest_int < src_int {
        state.registers[*dest_reg] = Register::Value(Value::Integer(src_int));
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_result_row(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ResultRow { start_reg, count }, insn);
    let row = Row {
        values: &state.registers[*start_reg] as *const Register,
        count: *count,
    };
    state.result_row = Some(row);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Row)
}

pub fn op_next(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Next {
            cursor_id,
            pc_if_next,
        },
        insn
    );
    assert!(pc_if_next.is_offset());
    let is_empty = {
        let cursor = state.get_cursor(*cursor_id);
        match cursor {
            Cursor::BTree(btree_cursor) => {
                btree_cursor.set_null_flag(false);
                return_if_io!(btree_cursor.next());
                btree_cursor.is_empty()
            }
            Cursor::MaterializedView(mv_cursor) => {
                let has_more = return_if_io!(mv_cursor.next());
                !has_more
            }
            _ => panic!("Next on non-btree/materialized-view cursor"),
        }
    };
    if !is_empty {
        // Increment metrics for row read
        state.metrics.rows_read = state.metrics.rows_read.saturating_add(1);
        state.metrics.btree_next = state.metrics.btree_next.saturating_add(1);
        // Track if this is a full table scan or index scan
        if let Some((_, cursor_type)) = program.cursor_ref.get(*cursor_id) {
            if cursor_type.is_index() {
                state.metrics.index_steps = state.metrics.index_steps.saturating_add(1);
            } else if matches!(cursor_type, CursorType::BTreeTable(_)) {
                state.metrics.fullscan_steps = state.metrics.fullscan_steps.saturating_add(1);
            }
        }
        state.pc = pc_if_next.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_prev(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Prev {
            cursor_id,
            pc_if_prev,
        },
        insn
    );
    assert!(pc_if_prev.is_offset());
    let is_empty = {
        let cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Prev");
        let cursor = cursor.as_btree_mut();
        cursor.set_null_flag(false);
        return_if_io!(cursor.prev());

        cursor.is_empty()
    };
    if !is_empty {
        // Increment metrics for row read
        state.metrics.rows_read = state.metrics.rows_read.saturating_add(1);
        state.metrics.btree_prev = state.metrics.btree_prev.saturating_add(1);
        // Track if this is a full table scan or index scan
        if let Some((_, cursor_type)) = program.cursor_ref.get(*cursor_id) {
            if cursor_type.is_index() {
                state.metrics.index_steps = state.metrics.index_steps.saturating_add(1);
            } else if matches!(cursor_type, CursorType::BTreeTable(_)) {
                state.metrics.fullscan_steps = state.metrics.fullscan_steps.saturating_add(1);
            }
        }
        state.pc = pc_if_prev.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn halt(
    program: &Program,
    state: &mut ProgramState,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
    err_code: usize,
    description: &str,
) -> Result<InsnFunctionStepResult> {
    if err_code > 0 {
        // invalidate page cache in case of error
        pager.clear_page_cache();
    }
    match err_code {
        0 => {}
        SQLITE_CONSTRAINT_PRIMARYKEY => {
            return Err(LimboError::Constraint(format!(
                "UNIQUE constraint failed: {description} (19)"
            )));
        }
        SQLITE_CONSTRAINT_NOTNULL => {
            return Err(LimboError::Constraint(format!(
                "NOT NULL constraint failed: {description} (19)"
            )));
        }
        SQLITE_CONSTRAINT_UNIQUE => {
            return Err(LimboError::Constraint(format!(
                "UNIQUE constraint failed: {description} (19)"
            )));
        }
        _ => {
            return Err(LimboError::Constraint(format!(
                "undocumented halt error code {description}"
            )));
        }
    }

    let auto_commit = program.connection.auto_commit.load(Ordering::SeqCst);
    tracing::trace!("halt(auto_commit={})", auto_commit);
    if auto_commit {
        program
            .commit_txn(pager.clone(), state, mv_store, false)
            .map(Into::into)
    } else {
        Ok(InsnFunctionStepResult::Done)
    }
}

pub fn op_halt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Halt {
            err_code,
            description,
        },
        insn
    );
    halt(program, state, pager, mv_store, *err_code, description)
}

pub fn op_halt_if_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        HaltIfNull {
            target_reg,
            err_code,
            description,
        },
        insn
    );
    if state.registers[*target_reg].get_value() == &Value::Null {
        halt(program, state, pager, mv_store, *err_code, description)
    } else {
        state.pc += 1;
        Ok(InsnFunctionStepResult::Step)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OpTransactionState {
    Start,
    CheckSchemaCookie,
}

pub fn op_transaction(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    match op_transaction_inner(program, state, insn, pager, mv_store) {
        Ok(result) => Ok(result),
        Err(err) => {
            state.op_transaction_state = OpTransactionState::Start;
            Err(err)
        }
    }
}

pub fn op_transaction_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Transaction {
            db,
            tx_mode,
            schema_cookie,
        },
        insn
    );
    let pager = program.get_pager_from_database_index(db);
    loop {
        match state.op_transaction_state {
            OpTransactionState::Start => {
                let conn = program.connection.clone();
                let write = matches!(tx_mode, TransactionMode::Write);
                if write && conn.db.open_flags.contains(OpenFlags::ReadOnly) {
                    return Err(LimboError::ReadOnly);
                }

                // 1. We try to upgrade current version
                let current_state = conn.get_tx_state();
                let (new_transaction_state, updated) = if conn.is_nested_stmt.load(Ordering::SeqCst)
                {
                    (current_state, false)
                } else {
                    match (current_state, write) {
                        // pending state means that we tried beginning a tx and the method returned IO.
                        // instead of ending the read tx, just update the state to pending.
                        (TransactionState::PendingUpgrade, write) => {
                            turso_assert!(
                                write,
                                "pending upgrade should only be set for write transactions"
                            );
                            (
                                TransactionState::Write {
                                    schema_did_change: false,
                                },
                                true,
                            )
                        }
                        (TransactionState::Write { schema_did_change }, true) => {
                            (TransactionState::Write { schema_did_change }, false)
                        }
                        (TransactionState::Write { schema_did_change }, false) => {
                            (TransactionState::Write { schema_did_change }, false)
                        }
                        (TransactionState::Read, true) => (
                            TransactionState::Write {
                                schema_did_change: false,
                            },
                            true,
                        ),
                        (TransactionState::Read, false) => (TransactionState::Read, false),
                        (TransactionState::None, true) => (
                            TransactionState::Write {
                                schema_did_change: false,
                            },
                            true,
                        ),
                        (TransactionState::None, false) => (TransactionState::Read, true),
                    }
                };

                // 2. Start transaction if needed
                if let Some(mv_store) = &mv_store {
                    // In MVCC we don't have write exclusivity, therefore we just need to start a transaction if needed.
                    // Programs can run Transaction twice, first with read flag and then with write flag. So a single txid is enough
                    // for both.
                    let current_mv_tx = program.connection.get_mv_tx();
                    let has_existing_mv_tx = current_mv_tx.is_some();

                    let conn_has_executed_begin_deferred = !has_existing_mv_tx
                        && !program.connection.auto_commit.load(Ordering::SeqCst);
                    if conn_has_executed_begin_deferred && *tx_mode == TransactionMode::Concurrent {
                        return Err(LimboError::TxError(
                            "Cannot start CONCURRENT transaction after BEGIN DEFERRED".to_string(),
                        ));
                    }

                    if !has_existing_mv_tx {
                        let tx_id = match tx_mode {
                            TransactionMode::None
                            | TransactionMode::Read
                            | TransactionMode::Concurrent => mv_store.begin_tx(pager.clone())?,
                            TransactionMode::Write => {
                                return_if_io!(mv_store.begin_exclusive_tx(pager.clone(), None))
                            }
                        };
                        *program.connection.mv_tx.write() = Some((tx_id, *tx_mode));
                    } else if updated {
                        // TODO: fix tx_mode in Insn::Transaction, now each statement overrides it even if there's already a CONCURRENT Tx in progress, for example
                        let (tx_id, mv_tx_mode) = current_mv_tx.unwrap();
                        let actual_tx_mode = if mv_tx_mode == TransactionMode::Concurrent {
                            TransactionMode::Concurrent
                        } else {
                            *tx_mode
                        };
                        if matches!(new_transaction_state, TransactionState::Write { .. })
                            && matches!(actual_tx_mode, TransactionMode::Write)
                        {
                            return_if_io!(mv_store.begin_exclusive_tx(pager.clone(), Some(tx_id)));
                        }
                    }
                } else {
                    if matches!(tx_mode, TransactionMode::Concurrent) {
                        return Err(LimboError::TxError(
                            "Concurrent transaction mode is only supported when MVCC is enabled"
                                .to_string(),
                        ));
                    }
                    if updated && matches!(current_state, TransactionState::None) {
                        turso_assert!(
                            !conn.is_nested_stmt.load(Ordering::SeqCst),
                            "nested stmt should not begin a new read transaction"
                        );
                        pager.begin_read_tx()?;
                    }

                    if updated && matches!(new_transaction_state, TransactionState::Write { .. }) {
                        turso_assert!(
                            !conn.is_nested_stmt.load(Ordering::SeqCst),
                            "nested stmt should not begin a new write transaction"
                        );
                        let begin_w_tx_res = pager.begin_write_tx();
                        if let Err(LimboError::Busy) = begin_w_tx_res {
                            // We failed to upgrade to write transaction so put the transaction into its original state.
                            // That is, if the transaction had not started, end the read transaction so that next time we
                            // start a new one.
                            if matches!(current_state, TransactionState::None) {
                                pager.end_read_tx()?;
                                conn.set_tx_state(TransactionState::None);
                            }
                            assert_eq!(conn.get_tx_state(), current_state);
                            return Err(LimboError::Busy);
                        }
                        if let IOResult::IO(io) = begin_w_tx_res? {
                            // set the transaction state to pending so we don't have to
                            // end the read transaction.
                            program
                                .connection
                                .set_tx_state(TransactionState::PendingUpgrade);
                            return Ok(InsnFunctionStepResult::IO(io));
                        }
                    }
                }

                // 3. Transaction state should be updated before checking for Schema cookie so that the tx is ended properly on error
                if updated {
                    conn.set_tx_state(new_transaction_state);
                }
                state.op_transaction_state = OpTransactionState::CheckSchemaCookie;
                continue;
            }
            // 4. Check whether schema has changed if we are actually going to access the database.
            // Can only read header if page 1 has been allocated already
            // begin_write_tx that happens, but not begin_read_tx
            OpTransactionState::CheckSchemaCookie => {
                let res = with_header(&pager, mv_store, program, |header| {
                    header.schema_cookie.get()
                });
                match res {
                    Ok(IOResult::Done(header_schema_cookie)) => {
                        if header_schema_cookie != *schema_cookie {
                            tracing::debug!(
                                "schema changed, force reprepare: {} != {}",
                                header_schema_cookie,
                                *schema_cookie
                            );
                            return Err(LimboError::SchemaUpdated);
                        }
                    }
                    Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
                    // This means we are starting a read_tx and we do not have a page 1 yet, so we just continue execution
                    Err(LimboError::Page1NotAlloc) => {}
                    Err(err) => {
                        return Err(err);
                    }
                }

                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_auto_commit(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        AutoCommit {
            auto_commit,
            rollback,
        },
        insn
    );
    let conn = program.connection.clone();
    if matches!(state.commit_state, CommitState::Committing) {
        return program
            .commit_txn(pager.clone(), state, mv_store, *rollback)
            .map(Into::into);
    }

    if *auto_commit != conn.auto_commit.load(Ordering::SeqCst) {
        if *rollback {
            // TODO(pere): add rollback I/O logic once we implement rollback journal
            if let Some(mv_store) = mv_store {
                if let Some(tx_id) = conn.get_mv_tx_id() {
                    mv_store.rollback_tx(tx_id, pager.clone(), &conn)?;
                }
            } else {
                return_if_io!(pager.end_tx(true, &conn));
            }
            conn.set_tx_state(TransactionState::None);
            conn.auto_commit.store(true, Ordering::SeqCst);
        } else {
            conn.auto_commit.store(*auto_commit, Ordering::SeqCst);
        }
    } else {
        let mvcc_tx_active = program.connection.get_mv_tx().is_some();
        if !mvcc_tx_active {
            if !*auto_commit {
                return Err(LimboError::TxError(
                    "cannot start a transaction within a transaction".to_string(),
                ));
            } else if *rollback {
                return Err(LimboError::TxError(
                    "cannot rollback - no transaction is active".to_string(),
                ));
            } else {
                return Err(LimboError::TxError(
                    "cannot commit - no transaction is active".to_string(),
                ));
            }
        } else {
            let is_begin = !*auto_commit && !*rollback;
            return Err(LimboError::TxError(
                "cannot use BEGIN after BEGIN CONCURRENT".to_string(),
            ));
        }
    }

    program
        .commit_txn(pager.clone(), state, mv_store, *rollback)
        .map(Into::into)
}

pub fn op_goto(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Goto { target_pc }, insn);
    assert!(target_pc.is_offset());
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_gosub(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Gosub {
            target_pc,
            return_reg,
        },
        insn
    );
    assert!(target_pc.is_offset());
    state.registers[*return_reg] = Register::Value(Value::Integer((state.pc + 1) as i64));
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_return(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Return {
            return_reg,
            can_fallthrough,
        },
        insn
    );
    if let Value::Integer(pc) = state.registers[*return_reg].get_value() {
        let pc: u32 = (*pc)
            .try_into()
            .unwrap_or_else(|_| panic!("Return register is negative: {pc}"));
        state.pc = pc;
    } else {
        if !*can_fallthrough {
            return Err(LimboError::InternalError(
                "Return register is not an integer".to_string(),
            ));
        }
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_integer(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Integer { value, dest }, insn);
    state.registers[*dest] = Register::Value(Value::Integer(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_real(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Real { value, dest }, insn);
    state.registers[*dest] = Register::Value(Value::Float(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_real_affinity(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(RealAffinity { register }, insn);
    if let Value::Integer(i) = &state.registers[*register].get_value() {
        state.registers[*register] = Register::Value(Value::Float(*i as f64));
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_string8(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(String8 { value, dest }, insn);
    state.registers[*dest] = Register::Value(Value::build_text(value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_blob(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Blob { value, dest }, insn);
    state.registers[*dest] = Register::Value(Value::Blob(value.clone()));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_row_data(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(RowData { cursor_id, dest }, insn);

    let record = {
        let cursor_ref = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "RowData");
        let cursor = cursor_ref.as_btree_mut();
        let record_option = return_if_io!(cursor.record());

        let record = record_option.ok_or_else(|| {
            LimboError::InternalError("RowData: cursor has no record".to_string())
        })?;

        record.clone()
    };

    let reg = &mut state.registers[*dest];
    *reg = Register::Record(record);

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Debug, Clone, Copy)]
pub enum OpRowIdState {
    Start,
    Record {
        index_cursor_id: usize,
        table_cursor_id: usize,
    },
    Seek {
        rowid: i64,
        table_cursor_id: usize,
    },
    GetRowid,
}

pub fn op_row_id(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(RowId { cursor_id, dest }, insn);
    loop {
        match state.op_row_id_state {
            OpRowIdState::Start => {
                if let Some((index_cursor_id, table_cursor_id)) =
                    state.deferred_seeks[*cursor_id].take()
                {
                    state.op_row_id_state = OpRowIdState::Record {
                        index_cursor_id,
                        table_cursor_id,
                    };
                } else {
                    state.op_row_id_state = OpRowIdState::GetRowid;
                }
            }
            OpRowIdState::Record {
                index_cursor_id,
                table_cursor_id,
            } => {
                let rowid = {
                    let index_cursor = state.get_cursor(index_cursor_id);
                    let index_cursor = index_cursor.as_btree_mut();
                    let record = return_if_io!(index_cursor.record());
                    let record = record.as_ref().unwrap();
                    let mut record_cursor_ref = index_cursor.record_cursor.borrow_mut();
                    let record_cursor = record_cursor_ref.deref_mut();
                    let rowid = record.last_value(record_cursor).unwrap();
                    match rowid {
                        Ok(RefValue::Integer(rowid)) => rowid,
                        _ => unreachable!(),
                    }
                };
                state.op_row_id_state = OpRowIdState::Seek {
                    rowid,
                    table_cursor_id,
                }
            }
            OpRowIdState::Seek {
                rowid,
                table_cursor_id,
            } => {
                {
                    let table_cursor = state.get_cursor(table_cursor_id);
                    let table_cursor = table_cursor.as_btree_mut();
                    return_if_io!(
                        table_cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })
                    );
                }
                state.op_row_id_state = OpRowIdState::GetRowid;
            }
            OpRowIdState::GetRowid => {
                let cursors = &mut state.cursors;
                if let Some(Cursor::BTree(btree_cursor)) = cursors.get_mut(*cursor_id).unwrap() {
                    if let Some(ref rowid) = return_if_io!(btree_cursor.rowid()) {
                        state.registers[*dest] = Register::Value(Value::Integer(*rowid));
                    } else {
                        state.registers[*dest] = Register::Value(Value::Null);
                    }
                } else if let Some(Cursor::Virtual(virtual_cursor)) =
                    cursors.get_mut(*cursor_id).unwrap()
                {
                    let rowid = virtual_cursor.rowid();
                    if rowid != 0 {
                        state.registers[*dest] = Register::Value(Value::Integer(rowid));
                    } else {
                        state.registers[*dest] = Register::Value(Value::Null);
                    }
                } else if let Some(Cursor::MaterializedView(mv_cursor)) =
                    cursors.get_mut(*cursor_id).unwrap()
                {
                    if let Some(rowid) = return_if_io!(mv_cursor.rowid()) {
                        state.registers[*dest] = Register::Value(Value::Integer(rowid));
                    } else {
                        state.registers[*dest] = Register::Value(Value::Null);
                    }
                } else {
                    return Err(LimboError::InternalError(
                        "RowId: cursor is not a table, virtual, or materialized view cursor"
                            .to_string(),
                    ));
                }
                break;
            }
        }
    }

    state.op_row_id_state = OpRowIdState::Start;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_idx_row_id(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(IdxRowId { cursor_id, dest }, insn);
    let cursors = &mut state.cursors;
    let cursor = cursors.get_mut(*cursor_id).unwrap().as_mut().unwrap();
    let cursor = cursor.as_btree_mut();
    let rowid = return_if_io!(cursor.rowid());
    state.registers[*dest] = match rowid {
        Some(rowid) => Register::Value(Value::Integer(rowid)),
        None => Register::Value(Value::Null),
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_seek_rowid(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SeekRowid {
            cursor_id,
            src_reg,
            target_pc,
        },
        insn
    );
    assert!(target_pc.is_offset());
    let (pc, did_seek) = {
        let cursor = get_cursor!(state, *cursor_id);

        // Handle MaterializedView cursor
        let (pc, did_seek) = match cursor {
            Cursor::MaterializedView(mv_cursor) => {
                let rowid = match state.registers[*src_reg].get_value() {
                    Value::Integer(rowid) => Some(*rowid),
                    Value::Null => None,
                    _ => None,
                };

                match rowid {
                    Some(rowid) => {
                        let seek_result = return_if_io!(mv_cursor
                            .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));
                        let pc = if !matches!(seek_result, SeekResult::Found) {
                            target_pc.as_offset_int()
                        } else {
                            state.pc + 1
                        };
                        (pc, true)
                    }
                    None => (target_pc.as_offset_int(), false),
                }
            }
            Cursor::BTree(btree_cursor) => {
                let rowid = match state.registers[*src_reg].get_value() {
                    Value::Integer(rowid) => Some(*rowid),
                    Value::Null => None,
                    // For non-integer values try to apply affinity and convert them to integer.
                    other => {
                        let mut temp_reg = Register::Value(other.clone());
                        let converted = apply_affinity_char(&mut temp_reg, Affinity::Numeric);
                        if converted {
                            match temp_reg.get_value() {
                                Value::Integer(i) => Some(*i),
                                Value::Float(f) => Some(*f as i64),
                                _ => unreachable!("apply_affinity_char with Numeric should produce an integer if it returns true"),
                            }
                        } else {
                            None
                        }
                    }
                };

                match rowid {
                    Some(rowid) => {
                        let seek_result = return_if_io!(btree_cursor
                            .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));
                        let pc = if !matches!(seek_result, SeekResult::Found) {
                            target_pc.as_offset_int()
                        } else {
                            state.pc + 1
                        };
                        (pc, true)
                    }
                    None => (target_pc.as_offset_int(), false),
                }
            }
            _ => panic!("SeekRowid on non-btree/materialized-view cursor"),
        };
        (pc, did_seek)
    };
    // Increment btree_seeks metric for SeekRowid operation after cursor is dropped
    if did_seek {
        state.metrics.btree_seeks = state.metrics.btree_seeks.saturating_add(1);
    }
    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_deferred_seek(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        DeferredSeek {
            index_cursor_id,
            table_cursor_id,
        },
        insn
    );
    state.deferred_seeks[*table_cursor_id] = Some((*index_cursor_id, *table_cursor_id));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Separate enum for seek key to avoid lifetime issues
/// with using [SeekKey] - OpSeekState always owns the key,
/// unless it's [OpSeekKey::IndexKeyFromRegister] in which case the record
/// is owned by the program state's registers and we store the register number.
#[derive(Debug)]
pub enum OpSeekKey {
    TableRowId(i64),
    IndexKeyOwned(ImmutableRecord),
    IndexKeyFromRegister(usize),
}

#[derive(Debug)]
pub enum OpSeekState {
    /// Initial state
    Start,
    /// Position cursor with seek operation with (rowid, op) search parameters
    Seek { key: OpSeekKey, op: SeekOp },
    /// Advance cursor (with [BTreeCursor::next]/[BTreeCursor::prev] methods) which was
    /// positioned after [OpSeekState::Seek] state if [BTreeCursor::seek] returned [SeekResult::TryAdvance]
    Advance { op: SeekOp },
    /// Move cursor to the last BTree row if DB knows that comparison result will be fixed (due to type ordering, e.g. NUMBER always <= TEXT)
    MoveLast,
}

pub fn op_seek(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let (cursor_id, is_index, record_source, target_pc) = match insn {
        Insn::SeekGE {
            cursor_id,
            is_index,
            start_reg,
            num_regs,
            target_pc,
            ..
        }
        | Insn::SeekLE {
            cursor_id,
            is_index,
            start_reg,
            num_regs,
            target_pc,
            ..
        }
        | Insn::SeekGT {
            cursor_id,
            is_index,
            start_reg,
            num_regs,
            target_pc,
            ..
        }
        | Insn::SeekLT {
            cursor_id,
            is_index,
            start_reg,
            num_regs,
            target_pc,
            ..
        } => (
            cursor_id,
            *is_index,
            RecordSource::Unpacked {
                start_reg: *start_reg,
                num_regs: *num_regs,
            },
            target_pc,
        ),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    assert!(
        target_pc.is_offset(),
        "op_seek: target_pc should be an offset, is: {target_pc:?}"
    );
    let eq_only = match insn {
        Insn::SeekGE { eq_only, .. } | Insn::SeekLE { eq_only, .. } => *eq_only,
        _ => false,
    };
    let op = match insn {
        Insn::SeekGE { eq_only, .. } => SeekOp::GE { eq_only: *eq_only },
        Insn::SeekGT { .. } => SeekOp::GT,
        Insn::SeekLE { eq_only, .. } => SeekOp::LE { eq_only: *eq_only },
        Insn::SeekLT { .. } => SeekOp::LT,
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    match seek_internal(
        program,
        state,
        pager,
        mv_store,
        record_source,
        *cursor_id,
        is_index,
        op,
    ) {
        Ok(SeekInternalResult::Found) => {
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        Ok(SeekInternalResult::NotFound) => {
            state.pc = target_pc.as_offset_int();
            Ok(InsnFunctionStepResult::Step)
        }
        Ok(SeekInternalResult::IO(io)) => Ok(InsnFunctionStepResult::IO(io)),
        Err(e) => Err(e),
    }
}

#[derive(Debug)]
pub enum SeekInternalResult {
    Found,
    NotFound,
    IO(IOCompletions),
}
#[derive(Clone, Copy)]
pub enum RecordSource {
    Unpacked { start_reg: usize, num_regs: usize },
    Packed { record_reg: usize },
}

/// Internal function used by many VDBE instructions that need to perform a seek operation.
///
/// Explanation for some of the arguments:
/// - `record_source`: whether the seek key record is already a record (packed) or it will be constructed from registers (unpacked)
/// - `cursor_id`: the cursor id
/// - `is_index`: true if the cursor is an index, false if it is a table
/// - `op`: the [SeekOp] to perform
#[allow(clippy::too_many_arguments)]
pub fn seek_internal(
    program: &Program,
    state: &mut ProgramState,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
    record_source: RecordSource,
    cursor_id: usize,
    is_index: bool,
    op: SeekOp,
) -> Result<SeekInternalResult> {
    /// wrapper so we can use the ? operator and handle errors correctly in this outer function
    fn inner(
        program: &Program,
        state: &mut ProgramState,
        pager: &Arc<Pager>,
        mv_store: Option<&Arc<MvStore>>,
        record_source: RecordSource,
        cursor_id: usize,
        is_index: bool,
        op: SeekOp,
    ) -> Result<SeekInternalResult> {
        loop {
            match &state.seek_state {
                OpSeekState::Start => {
                    if is_index {
                        // FIXME: text-to-numeric conversion should also happen here when applicable (when index column is numeric)
                        // See below for the table-btree implementation of this
                        match record_source {
                            RecordSource::Unpacked {
                                start_reg,
                                num_regs,
                            } => {
                                let record_from_regs =
                                    make_record(&state.registers, &start_reg, &num_regs);
                                state.seek_state = OpSeekState::Seek {
                                    key: OpSeekKey::IndexKeyOwned(record_from_regs),
                                    op,
                                };
                            }
                            RecordSource::Packed { record_reg } => {
                                state.seek_state = OpSeekState::Seek {
                                    key: OpSeekKey::IndexKeyFromRegister(record_reg),
                                    op,
                                };
                            }
                        };
                        continue;
                    }
                    let RecordSource::Unpacked {
                        start_reg,
                        num_regs,
                    } = record_source
                    else {
                        unreachable!("op_seek: record_source should be Unpacked for table-btree");
                    };
                    assert_eq!(num_regs, 1, "op_seek: num_regs should be 1 for table-btree");
                    let original_value = state.registers[start_reg].get_value().clone();
                    let mut temp_value = original_value.clone();

                    let conversion_successful = if matches!(temp_value, Value::Text(_)) {
                        let mut temp_reg = Register::Value(temp_value);
                        let converted = apply_numeric_affinity(&mut temp_reg, false);
                        temp_value = temp_reg.get_value().clone();
                        converted
                    } else {
                        true // Non-text values don't need conversion
                    };
                    let int_key = extract_int_value(&temp_value);
                    let lost_precision =
                        !conversion_successful || !matches!(temp_value, Value::Integer(_));
                    let actual_op = if lost_precision {
                        match &temp_value {
                            Value::Float(f) => {
                                let int_key_as_float = int_key as f64;
                                let c = if int_key_as_float > *f {
                                    1
                                } else if int_key_as_float < *f {
                                    -1
                                } else {
                                    0
                                };

                                match c.cmp(&0) {
                                    std::cmp::Ordering::Less => match op {
                                        SeekOp::LT => SeekOp::LE { eq_only: false }, // (x < 5.1) -> (x <= 5)
                                        SeekOp::GE { .. } => SeekOp::GT, // (x >= 5.1) -> (x > 5)
                                        other => other,
                                    },
                                    std::cmp::Ordering::Greater => match op {
                                        SeekOp::GT => SeekOp::GE { eq_only: false }, // (x > 4.9) -> (x >= 5)
                                        SeekOp::LE { .. } => SeekOp::LT, // (x <= 4.9) -> (x < 5)
                                        other => other,
                                    },
                                    std::cmp::Ordering::Equal => op,
                                }
                            }
                            Value::Text(_) | Value::Blob(_) => {
                                match op {
                                    SeekOp::GT | SeekOp::GE { .. } => {
                                        // No integers are > or >= non-numeric text
                                        return Ok(SeekInternalResult::NotFound);
                                    }
                                    SeekOp::LT | SeekOp::LE { .. } => {
                                        // All integers are < or <= non-numeric text
                                        // Move to last position and then use the normal seek logic
                                        state.seek_state = OpSeekState::MoveLast;
                                        continue;
                                    }
                                }
                            }
                            _ => op,
                        }
                    } else {
                        op
                    };
                    let rowid = if matches!(original_value, Value::Null) {
                        match actual_op {
                            SeekOp::GE { .. } | SeekOp::GT => {
                                return Ok(SeekInternalResult::NotFound);
                            }
                            SeekOp::LE { .. } | SeekOp::LT => {
                                // No integers are < NULL
                                return Ok(SeekInternalResult::NotFound);
                            }
                        }
                    } else {
                        int_key
                    };
                    state.seek_state = OpSeekState::Seek {
                        key: OpSeekKey::TableRowId(rowid),
                        op: actual_op,
                    };
                    continue;
                }
                OpSeekState::Seek { key, op } => {
                    let seek_result = {
                        let cursor = get_cursor!(state, cursor_id);
                        let cursor = cursor.as_btree_mut();
                        let seek_key = match key {
                        OpSeekKey::TableRowId(rowid) => SeekKey::TableRowId(*rowid),
                        OpSeekKey::IndexKeyOwned(record) => SeekKey::IndexKey(record),
                        OpSeekKey::IndexKeyFromRegister(record_reg) => match &state.registers[*record_reg] {
                            Register::Record(ref record) => SeekKey::IndexKey(record),
                            _ => unreachable!("op_seek: record_reg should be a Record register when OpSeekKey::IndexKeyFromRegister is used"),
                        }
                    };
                        match cursor.seek(seek_key, *op)? {
                            IOResult::Done(seek_result) => seek_result,
                            IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                        }
                    };
                    // Increment btree_seeks metric after seek operation and cursor is dropped
                    state.metrics.btree_seeks = state.metrics.btree_seeks.saturating_add(1);
                    let found = match seek_result {
                        SeekResult::Found => true,
                        SeekResult::NotFound => false,
                        SeekResult::TryAdvance => {
                            state.seek_state = OpSeekState::Advance { op: *op };
                            continue;
                        }
                    };
                    return Ok(if found {
                        SeekInternalResult::Found
                    } else {
                        SeekInternalResult::NotFound
                    });
                }
                OpSeekState::Advance { op } => {
                    let found = {
                        let cursor = get_cursor!(state, cursor_id);
                        let cursor = cursor.as_btree_mut();
                        // Seek operation has anchor number which equals to the closed boundary of the range
                        // (e.g. for >= x - anchor is x, for > x - anchor is x + 1)
                        //
                        // Before Advance state, cursor was positioned to the leaf page which should hold the anchor.
                        // Sometimes this leaf page can have no matching rows, and in this case
                        // we need to move cursor in the direction of Seek to find record which matches the seek filter
                        //
                        // Consider following scenario: Seek [> 666]
                        // interior page dividers:       I1: [ .. 667 .. ]
                        //                                       /   \
                        //             leaf pages:    P1[661,665]   P2[anything here is GT 666]
                        // After the initial Seek, cursor will be positioned after the end of leaf page P1 [661, 665]
                        // because this is potential position for insertion of value 666.
                        // But as P1 has no row matching Seek criteria - we need to move it to the right
                        // (and as we at the page boundary, we will move cursor to the next neighbor leaf, which guaranteed to have
                        // row keys greater than divider, which is greater or equal than anchor)

                        // this same logic applies for indexes, but the next/prev record is expected to be found in the parent page's
                        // divider cell.
                        turso_assert!(
                            !cursor.skip_advance.get(),
                            "skip_advance should not be true in the middle of a seek operation"
                        );
                        let result = match op {
                            // deliberately call get_next_record() instead of next() to avoid skip_advance triggering unwantedly
                            SeekOp::GT | SeekOp::GE { .. } => cursor.next()?,
                            SeekOp::LT | SeekOp::LE { .. } => cursor.prev()?,
                        };
                        match result {
                            IOResult::Done(found) => {
                                cursor.has_record.set(found);
                                cursor.invalidate_record();
                                found
                            }
                            IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                        }
                    };
                    return Ok(if found {
                        SeekInternalResult::Found
                    } else {
                        SeekInternalResult::NotFound
                    });
                }
                OpSeekState::MoveLast => {
                    let cursor = state.get_cursor(cursor_id);
                    let cursor = cursor.as_btree_mut();
                    match cursor.last()? {
                        IOResult::Done(()) => {}
                        IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                    }
                    // the MoveLast variant is only used for SeekOp::LT and SeekOp::LE when the seek condition is always true,
                    // so we have always found what we were looking for.
                    return Ok(SeekInternalResult::Found);
                }
            }
        }
    }

    let result = inner(
        program,
        state,
        pager,
        mv_store,
        record_source,
        cursor_id,
        is_index,
        op,
    );
    if !matches!(result, Ok(SeekInternalResult::IO(..))) {
        state.seek_state = OpSeekState::Start;
    }
    result
}

/// Returns the tie-breaker ordering for SQLite index comparison opcodes.
///
/// When comparing index keys that omit the PRIMARY KEY/ROWID, SQLite uses a
/// tie-breaker value (`default_rc` in the C code) to determine the result when
/// the non-primary-key portions of the keys are equal.
///
/// This function extracts the appropriate tie-breaker based on the comparison opcode:
///
/// ## Tie-breaker Logic
///
/// - **`IdxLE` and `IdxGT`**: Return `Ordering::Less` (equivalent to `default_rc = -1`)
///   - When keys are equal, these operations should favor the "less than" result
///   - `IdxLE`: "less than or equal" - equality should be treated as "less"
///   - `IdxGT`: "greater than" - equality should be treated as "less" (so condition fails)
///
/// - **`IdxGE` and `IdxLT`**: Return `Ordering::Equal` (equivalent to `default_rc = 0`)
///   - When keys are equal, these operations should treat it as true equality
///   - `IdxGE`: "greater than or equal" - equality should be treated as "equal"
///   - `IdxLT`: "less than" - equality should be treated as "equal" (so condition fails)
///
/// ## SQLite Implementation Details
///
/// In SQLite's C implementation, this corresponds to:
/// ```c
/// if( pOp->opcode<OP_IdxLT ){
///     assert( pOp->opcode==OP_IdxLE || pOp->opcode==OP_IdxGT );
///     r.default_rc = -1;  // Ordering::Less
/// }else{
///     assert( pOp->opcode==OP_IdxGE || pOp->opcode==OP_IdxLT );
///     r.default_rc = 0;   // Ordering::Equal
/// }
/// ```
#[inline(always)]
fn get_tie_breaker_from_idx_comp_op(insn: &Insn) -> std::cmp::Ordering {
    match insn {
        Insn::IdxLE { .. } | Insn::IdxGT { .. } => std::cmp::Ordering::Less,
        Insn::IdxGE { .. } | Insn::IdxLT { .. } => std::cmp::Ordering::Equal,
        _ => panic!("Invalid instruction for index comparison"),
    }
}

#[allow(clippy::let_and_return)]
pub fn op_idx_ge(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IdxGE {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        },
        insn
    );
    assert!(target_pc.is_offset());

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            // Create the comparison record from registers
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);
            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(
                &idx_record,                         // The serialized record from the index
                &values,                             // The record built from registers
                cursor.index_info.as_ref().unwrap(), // Sort order flags
                0,
                tie_breaker,
            )?;

            if ord.is_ge() {
                target_pc.as_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            // No record at cursor position, jump to target
            target_pc.as_offset_int()
        };
        pc
    };

    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_seek_end(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(SeekEnd { cursor_id }, *insn);
    {
        let cursor = state.get_cursor(cursor_id);
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.seek_end());
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[allow(clippy::let_and_return)]
pub fn op_idx_le(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IdxLE {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        },
        insn
    );
    assert!(target_pc.is_offset());

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);
            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(
                &idx_record,
                &values,
                cursor.index_info.as_ref().unwrap(),
                0,
                tie_breaker,
            )?;

            if ord.is_le() {
                target_pc.as_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            // No record at cursor position, jump to target
            target_pc.as_offset_int()
        };
        pc
    };

    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

#[allow(clippy::let_and_return)]
pub fn op_idx_gt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IdxGT {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        },
        insn
    );
    assert!(target_pc.is_offset());

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);
            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(
                &idx_record,
                &values,
                cursor.index_info.as_ref().unwrap(),
                0,
                tie_breaker,
            )?;

            if ord.is_gt() {
                target_pc.as_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            // No record at cursor position, jump to target
            target_pc.as_offset_int()
        };
        pc
    };

    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

#[allow(clippy::let_and_return)]
pub fn op_idx_lt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IdxLT {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        },
        insn
    );
    assert!(target_pc.is_offset());

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);

            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(
                &idx_record,
                &values,
                cursor.index_info.as_ref().unwrap(),
                0,
                tie_breaker,
            )?;

            if ord.is_lt() {
                target_pc.as_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            // No record at cursor position, jump to target
            target_pc.as_offset_int()
        };
        pc
    };

    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_decr_jump_zero(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(DecrJumpZero { reg, target_pc }, insn);
    assert!(target_pc.is_offset());
    match state.registers[*reg].get_value() {
        Value::Integer(n) => {
            let n = n - 1;
            state.registers[*reg] = Register::Value(Value::Integer(n));
            if n == 0 {
                state.pc = target_pc.as_offset_int();
            } else {
                state.pc += 1;
            }
        }
        _ => unreachable!("DecrJumpZero on non-integer register"),
    }
    Ok(InsnFunctionStepResult::Step)
}

fn apply_kbn_step(acc: &mut Value, r: f64, state: &mut SumAggState) {
    let s = acc.as_float();
    let t = s + r;
    let correction = if s.abs() > r.abs() {
        (s - t) + r
    } else {
        (r - t) + s
    };
    state.r_err += correction;
    *acc = Value::Float(t);
}

// Add a (possibly large) integer to the running sum.
fn apply_kbn_step_int(acc: &mut Value, i: i64, state: &mut SumAggState) {
    const THRESHOLD: i64 = 4503599627370496; // 2^52

    if i <= -THRESHOLD || i >= THRESHOLD {
        let i_sm = i % 16384;
        let i_big = i - i_sm;

        apply_kbn_step(acc, i_big as f64, state);
        apply_kbn_step(acc, i_sm as f64, state);
    } else {
        apply_kbn_step(acc, i as f64, state);
    }
}

pub fn op_agg_step(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        AggStep {
            acc_reg,
            col,
            delimiter,
            func,
        },
        insn
    );
    if let Register::Value(Value::Null) = state.registers[*acc_reg] {
        state.registers[*acc_reg] = match func {
            AggFunc::Avg => {
                Register::Aggregate(AggContext::Avg(Value::Float(0.0), Value::Integer(0)))
            }
            AggFunc::Sum => {
                Register::Aggregate(AggContext::Sum(Value::Null, SumAggState::default()))
            }
            AggFunc::Total => {
                // The result of total() is always a floating point value.
                // No overflow error is ever raised if any prior input was a floating point value.
                // Total() never throws an integer overflow.
                Register::Aggregate(AggContext::Sum(Value::Float(0.0), SumAggState::default()))
            }
            AggFunc::Count | AggFunc::Count0 => {
                Register::Aggregate(AggContext::Count(Value::Integer(0)))
            }
            AggFunc::Max => Register::Aggregate(AggContext::Max(None)),
            AggFunc::Min => Register::Aggregate(AggContext::Min(None)),
            AggFunc::GroupConcat | AggFunc::StringAgg => {
                Register::Aggregate(AggContext::GroupConcat(Value::build_text("")))
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
                Register::Aggregate(AggContext::GroupConcat(Value::Blob(vec![])))
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
                Register::Aggregate(AggContext::GroupConcat(Value::Blob(vec![])))
            }
            AggFunc::External(func) => match func.as_ref() {
                ExtFunc::Aggregate {
                    init,
                    step,
                    finalize,
                    argc,
                } => Register::Aggregate(AggContext::External(ExternalAggState {
                    state: unsafe { (init)() },
                    argc: *argc,
                    step_fn: *step,
                    finalize_fn: *finalize,
                })),
                _ => unreachable!("scalar function called in aggregate context"),
            },
        };
    }
    match func {
        AggFunc::Avg => {
            let col = state.registers[*col].clone();
            // > The avg() function returns the average value of all non-NULL X within a group
            // https://sqlite.org/lang_aggfunc.html#avg
            if !col.is_null() {
                let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                    panic!(
                        "Unexpected value {:?} in AggStep at register {}",
                        state.registers[*acc_reg], *acc_reg
                    );
                };
                let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                    unreachable!();
                };
                *acc = acc.exec_add(col.get_value());
                *count += 1;
            }
        }
        AggFunc::Sum | AggFunc::Total => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                panic!(
                    "Unexpected value {:?} at register {:?} in AggStep",
                    state.registers[*acc_reg], *acc_reg
                );
            };
            let AggContext::Sum(acc, sum_state) = agg.borrow_mut() else {
                unreachable!();
            };
            match col {
                Register::Value(value) => {
                    match value {
                        Value::Null => {
                            // Ignore NULLs
                        }

                        Value::Integer(i) => match acc {
                            Value::Null => {
                                *acc = Value::Integer(i);
                            }
                            Value::Integer(acc_i) => {
                                match acc_i.checked_add(i) {
                                    Some(sum) => *acc = Value::Integer(sum),
                                    None => {
                                        // Overflow -> switch to float with KBN summation
                                        let acc_f = *acc_i as f64;
                                        *acc = Value::Float(acc_f);
                                        sum_state.approx = true;
                                        sum_state.ovrfl = true;

                                        apply_kbn_step_int(acc, i, sum_state);
                                    }
                                }
                            }
                            Value::Float(_) => {
                                apply_kbn_step_int(acc, i, sum_state);
                            }
                            _ => unreachable!(),
                        },

                        Value::Float(f) => match acc {
                            Value::Null => {
                                *acc = Value::Float(f);
                            }
                            Value::Integer(i) => {
                                let i_f = *i as f64;
                                *acc = Value::Float(i_f);
                                sum_state.approx = true;
                                apply_kbn_step(acc, f, sum_state);
                            }
                            Value::Float(_) => {
                                sum_state.approx = true;
                                apply_kbn_step(acc, f, sum_state);
                            }
                            _ => unreachable!(),
                        },

                        _ => {
                            //  If any input to sum() is neither an integer nor a NULL, then sum() returns a float
                            // https://sqlite.org/lang_aggfunc.html
                            sum_state.approx = true;
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        AggFunc::Count | AggFunc::Count0 => {
            let skip = (matches!(func, AggFunc::Count)
                && matches!(state.registers[*col].get_value(), Value::Null));
            if matches!(&state.registers[*acc_reg], Register::Value(Value::Null)) {
                state.registers[*acc_reg] =
                    Register::Aggregate(AggContext::Count(Value::Integer(0)));
            }
            let Register::Aggregate(agg) = &mut state.registers[*acc_reg] else {
                panic!(
                    "Unexpected value {:?} in AggStep at register {}",
                    state.registers[*acc_reg], *acc_reg
                );
            };
            let AggContext::Count(count) = agg else {
                unreachable!();
            };
            if !skip {
                *count += 1;
            };
        }
        AggFunc::Max => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                panic!(
                    "Unexpected value {:?} in AggStep at register {}",
                    state.registers[*acc_reg], *acc_reg
                );
            };
            let AggContext::Max(acc) = agg.borrow_mut() else {
                unreachable!();
            };

            let new_value = col.get_value();
            if *new_value != Value::Null
                && acc.as_ref().is_none_or(|acc| {
                    use std::cmp::Ordering;
                    compare_with_collation(new_value, acc, state.current_collation)
                        == Ordering::Greater
                })
            {
                *acc = Some(new_value.clone());
            }
        }
        AggFunc::Min => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                panic!(
                    "Unexpected value {:?} in AggStep",
                    state.registers[*acc_reg]
                );
            };
            let AggContext::Min(acc) = agg.borrow_mut() else {
                unreachable!();
            };

            let new_value = col.get_value();

            if *new_value != Value::Null
                && acc.as_ref().is_none_or(|acc| {
                    use std::cmp::Ordering;
                    compare_with_collation(new_value, acc, state.current_collation)
                        == Ordering::Less
                })
            {
                *acc = Some(new_value.clone());
            }
        }
        AggFunc::GroupConcat | AggFunc::StringAgg => {
            let col = state.registers[*col].get_value().clone();
            let delimiter = state.registers[*delimiter].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                unreachable!();
            };
            if acc.to_string().is_empty() {
                *acc = col;
            } else {
                match col {
                    Value::Null => {}
                    _ => {
                        match delimiter {
                            Register::Value(value) => {
                                *acc += value;
                            }
                            _ => unreachable!(),
                        }
                        *acc += col;
                    }
                }
            }
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            let key = state.registers[*col].clone();
            let value = state.registers[*delimiter].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                unreachable!();
            };

            let mut key_vec = convert_dbtype_to_raw_jsonb(key.get_value())?;
            let mut val_vec = convert_dbtype_to_raw_jsonb(value.get_value())?;

            match acc {
                Value::Blob(vec) => {
                    if vec.is_empty() {
                        // bits for obj header
                        vec.push(12);
                        vec.append(&mut key_vec);
                        vec.append(&mut val_vec);
                    } else {
                        vec.append(&mut key_vec);
                        vec.append(&mut val_vec);
                    }
                }
                _ => unreachable!(),
            };
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                unreachable!();
            };

            let mut data = convert_dbtype_to_raw_jsonb(col.get_value())?;
            match acc {
                Value::Blob(vec) => {
                    if vec.is_empty() {
                        vec.push(11);
                        vec.append(&mut data)
                    } else {
                        vec.append(&mut data);
                    }
                }
                _ => unreachable!(),
            };
        }
        AggFunc::External(_) => {
            let (step_fn, state_ptr, argc) = {
                let Register::Aggregate(agg) = &state.registers[*acc_reg] else {
                    unreachable!();
                };
                let AggContext::External(agg_state) = agg else {
                    unreachable!();
                };
                (agg_state.step_fn, agg_state.state, agg_state.argc)
            };
            if argc == 0 {
                unsafe { step_fn(state_ptr, 0, std::ptr::null()) };
            } else {
                let register_slice = &state.registers[*col..*col + argc];
                let mut ext_values: Vec<ExtValue> = Vec::with_capacity(argc);
                for ov in register_slice.iter() {
                    ext_values.push(ov.get_value().to_ffi());
                }
                let argv_ptr = ext_values.as_ptr();
                unsafe { step_fn(state_ptr, argc as i32, argv_ptr) };
                for ext_value in ext_values {
                    unsafe { ext_value.__free_internal_type() };
                }
            }
        }
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_agg_final(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let (acc_reg, dest_reg, func) = match insn {
        Insn::AggFinal { register, func } => (*register, *register, func),
        Insn::AggValue {
            acc_reg,
            dest_reg,
            func,
        } => (*acc_reg, *dest_reg, func),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    match &state.registers[acc_reg] {
        Register::Aggregate(agg) => match func {
            AggFunc::Avg => {
                let AggContext::Avg(acc, count) = agg else {
                    unreachable!();
                };
                let acc = if count.as_int() == Some(0) {
                    Value::Null
                } else {
                    acc.clone() / count.clone()
                };
                state.registers[dest_reg] = Register::Value(acc);
            }
            AggFunc::Sum => {
                let AggContext::Sum(acc, sum_state) = agg else {
                    unreachable!();
                };
                let value = match acc {
                    Value::Null => match sum_state.approx {
                        true => Value::Float(0.0),
                        false => Value::Null,
                    },
                    Value::Integer(i) if !sum_state.approx && !sum_state.ovrfl => {
                        Value::Integer(*i)
                    }
                    _ => Value::Float(acc.as_float() + sum_state.r_err),
                };
                state.registers[dest_reg] = Register::Value(value);
            }
            AggFunc::Total => {
                let AggContext::Sum(acc, _) = agg else {
                    unreachable!();
                };
                let value = match acc {
                    Value::Null => Value::Float(0.0),
                    Value::Integer(i) => Value::Float(*i as f64),
                    Value::Float(f) => Value::Float(*f),
                    _ => unreachable!(),
                };
                state.registers[dest_reg] = Register::Value(value);
            }
            AggFunc::Count | AggFunc::Count0 => {
                let AggContext::Count(count) = agg else {
                    unreachable!();
                };
                state.registers[dest_reg] = Register::Value(count.clone());
            }
            AggFunc::Max => {
                let AggContext::Max(acc) = agg else {
                    unreachable!();
                };
                match acc {
                    Some(value) => state.registers[dest_reg] = Register::Value(value.clone()),
                    None => state.registers[dest_reg] = Register::Value(Value::Null),
                }
            }
            AggFunc::Min => {
                let AggContext::Min(acc) = agg else {
                    unreachable!();
                };
                match acc {
                    Some(value) => state.registers[dest_reg] = Register::Value(value.clone()),
                    None => state.registers[dest_reg] = Register::Value(Value::Null),
                }
            }
            AggFunc::GroupConcat | AggFunc::StringAgg => {
                let AggContext::GroupConcat(acc) = agg else {
                    unreachable!();
                };
                state.registers[dest_reg] = Register::Value(acc.clone());
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupObject => {
                let AggContext::GroupConcat(acc) = agg else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[dest_reg] = Register::Value(json_from_raw_bytes_agg(data, false)?);
            }
            #[cfg(feature = "json")]
            AggFunc::JsonbGroupObject => {
                let AggContext::GroupConcat(acc) = agg else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[dest_reg] = Register::Value(json_from_raw_bytes_agg(data, true)?);
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupArray => {
                let AggContext::GroupConcat(acc) = agg else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[dest_reg] = Register::Value(json_from_raw_bytes_agg(data, false)?);
            }
            #[cfg(feature = "json")]
            AggFunc::JsonbGroupArray => {
                let AggContext::GroupConcat(acc) = agg else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[dest_reg] = Register::Value(json_from_raw_bytes_agg(data, true)?);
            }
            AggFunc::External(_) => {
                let AggContext::External(agg_state) = agg else {
                    unreachable!();
                };
                let value = agg.compute_external()?;
                state.registers[dest_reg] = Register::Value(value)
            }
        },
        Register::Value(Value::Null) => {
            // when the set is empty
            match func {
                AggFunc::Total => {
                    state.registers[dest_reg] = Register::Value(Value::Float(0.0));
                }
                AggFunc::Count | AggFunc::Count0 => {
                    state.registers[dest_reg] = Register::Value(Value::Integer(0));
                }
                _ => {}
            }
        }
        other => {
            panic!("Unexpected value {other:?} in AggFinal");
        }
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_open(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SorterOpen {
            cursor_id,
            columns: _,
            order,
            collations,
        },
        insn
    );
    // be careful here - we must not use any async operations after pager.with_header because this op-code has no proper state-machine
    let page_size = match pager.with_header(|header| header.page_size) {
        Ok(IOResult::Done(page_size)) => page_size,
        Err(_) => PageSize::default(),
        Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
    };
    let page_size = page_size.get() as usize;

    let cache_size = program.connection.get_cache_size();

    // Set the buffer size threshold to be roughly the same as the limit configured for the page-cache.
    let max_buffer_size_bytes = if cache_size < 0 {
        (cache_size.abs() * 1024) as usize
    } else {
        (cache_size as usize) * page_size
    };
    let cursor = Sorter::new(
        order,
        collations
            .iter()
            .map(|collation| collation.unwrap_or_default())
            .collect(),
        max_buffer_size_bytes,
        page_size,
        pager.io.clone(),
    );
    let cursors = &mut state.cursors;
    cursors
        .get_mut(*cursor_id)
        .unwrap()
        .replace(Cursor::new_sorter(cursor));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_data(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SorterData {
            cursor_id,
            dest_reg,
            pseudo_cursor,
        },
        insn
    );
    let record = {
        let cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        cursor.record().cloned()
    };
    let record = match record {
        Some(record) => record,
        None => {
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
    };
    state.registers[*dest_reg] = Register::Record(record.clone());
    {
        let pseudo_cursor = state.get_cursor(*pseudo_cursor);
        pseudo_cursor.as_pseudo_mut().insert(record);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SorterInsert {
            cursor_id,
            record_reg,
        },
        insn
    );
    {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_sorter_mut();
        let record = match &state.registers[*record_reg] {
            Register::Record(record) => record,
            _ => unreachable!("SorterInsert on non-record register"),
        };
        return_if_io!(cursor.insert(record));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_sort(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SorterSort {
            cursor_id,
            pc_if_empty,
        },
        insn
    );
    let (is_empty, did_sort) = {
        let cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        let is_empty = cursor.is_empty();
        if !is_empty {
            return_if_io!(cursor.sort());
        }
        (is_empty, !is_empty)
    };
    // Increment metrics for sort operation after cursor is dropped
    if did_sort {
        state.metrics.sort_operations = state.metrics.sort_operations.saturating_add(1);
    }
    if is_empty {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_next(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SorterNext {
            cursor_id,
            pc_if_next,
        },
        insn
    );
    assert!(pc_if_next.is_offset());
    let has_more = {
        let cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        return_if_io!(cursor.next());
        cursor.has_more()
    };
    if has_more {
        state.pc = pc_if_next.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_function(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Function {
            constant_mask,
            func,
            start_reg,
            dest,
        },
        insn
    );
    let arg_count = func.arg_count;

    match &func.func {
        #[cfg(feature = "json")]
        crate::function::Func::Json(json_func) => match json_func {
            JsonFunc::Json => {
                let json_value = &state.registers[*start_reg];
                let json_str = get_json(json_value.get_value(), None);
                match json_str {
                    Ok(json) => state.registers[*dest] = Register::Value(json),
                    Err(e) => return Err(e),
                }
            }

            JsonFunc::Jsonb => {
                let json_value = &state.registers[*start_reg];
                let json_blob = jsonb(json_value.get_value(), &state.json_cache);
                match json_blob {
                    Ok(json) => state.registers[*dest] = Register::Value(json),
                    Err(e) => return Err(e),
                }
            }

            JsonFunc::JsonArray
            | JsonFunc::JsonObject
            | JsonFunc::JsonbArray
            | JsonFunc::JsonbObject => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];

                let json_func = match json_func {
                    JsonFunc::JsonArray => json_array,
                    JsonFunc::JsonObject => json_object,
                    JsonFunc::JsonbArray => jsonb_array,
                    JsonFunc::JsonbObject => jsonb_object,
                    _ => unreachable!(),
                };
                let json_result = json_func(reg_values);

                match json_result {
                    Ok(json) => state.registers[*dest] = Register::Value(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonExtract => {
                let result = match arg_count {
                    0 => Ok(Value::Null),
                    _ => {
                        let val = &state.registers[*start_reg];
                        let reg_values = &state.registers[*start_reg + 1..*start_reg + arg_count];

                        json_extract(val.get_value(), reg_values, &state.json_cache)
                    }
                };

                match result {
                    Ok(json) => state.registers[*dest] = Register::Value(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonbExtract => {
                let result = match arg_count {
                    0 => Ok(Value::Null),
                    _ => {
                        let val = &state.registers[*start_reg];
                        let reg_values = &state.registers[*start_reg + 1..*start_reg + arg_count];

                        jsonb_extract(val.get_value(), reg_values, &state.json_cache)
                    }
                };

                match result {
                    Ok(json) => state.registers[*dest] = Register::Value(json),
                    Err(e) => return Err(e),
                }
            }

            JsonFunc::JsonArrowExtract | JsonFunc::JsonArrowShiftExtract => {
                assert_eq!(arg_count, 2);
                let json = &state.registers[*start_reg];
                let path = &state.registers[*start_reg + 1];
                let json_func = match json_func {
                    JsonFunc::JsonArrowExtract => json_arrow_extract,
                    JsonFunc::JsonArrowShiftExtract => json_arrow_shift_extract,
                    _ => unreachable!(),
                };
                let json_str = json_func(json.get_value(), path.get_value(), &state.json_cache);
                match json_str {
                    Ok(json) => state.registers[*dest] = Register::Value(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonArrayLength | JsonFunc::JsonType => {
                let json_value = &state.registers[*start_reg];
                let path_value = if arg_count > 1 {
                    Some(&state.registers[*start_reg + 1])
                } else {
                    None
                };
                let func_result = match json_func {
                    JsonFunc::JsonArrayLength => json_array_length(
                        json_value.get_value(),
                        path_value.map(|x| x.get_value()),
                        &state.json_cache,
                    ),
                    JsonFunc::JsonType => {
                        json_type(json_value.get_value(), path_value.map(|x| x.get_value()))
                    }
                    _ => unreachable!(),
                };

                match func_result {
                    Ok(result) => state.registers[*dest] = Register::Value(result),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonErrorPosition => {
                let json_value = &state.registers[*start_reg];
                match json_error_position(json_value.get_value()) {
                    Ok(pos) => state.registers[*dest] = Register::Value(pos),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonValid => {
                let json_value = &state.registers[*start_reg];
                state.registers[*dest] = Register::Value(is_json_valid(json_value.get_value()));
            }
            JsonFunc::JsonPatch => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let target = &state.registers[*start_reg];
                let patch = &state.registers[*start_reg + 1];
                state.registers[*dest] = Register::Value(json_patch(
                    target.get_value(),
                    patch.get_value(),
                    &state.json_cache,
                )?);
            }
            JsonFunc::JsonbPatch => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let target = &state.registers[*start_reg];
                let patch = &state.registers[*start_reg + 1];
                state.registers[*dest] = Register::Value(jsonb_patch(
                    target.get_value(),
                    patch.get_value(),
                    &state.json_cache,
                )?);
            }
            JsonFunc::JsonRemove => {
                if let Ok(json) = json_remove(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonbRemove => {
                if let Ok(json) = jsonb_remove(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonReplace => {
                if let Ok(json) = json_replace(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonbReplace => {
                if let Ok(json) = jsonb_replace(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonInsert => {
                if let Ok(json) = json_insert(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonbInsert => {
                if let Ok(json) = jsonb_insert(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonPretty => {
                let json_value = &state.registers[*start_reg];
                let indent = if arg_count > 1 {
                    Some(&state.registers[*start_reg + 1])
                } else {
                    None
                };

                // Blob should be converted to Ascii in a lossy way
                // However, Rust strings uses utf-8
                // so the behavior at the moment is slightly different
                // To the way blobs are parsed here in SQLite.
                let indent = match indent {
                    Some(value) => match value.get_value() {
                        Value::Text(text) => text.as_str(),
                        Value::Integer(val) => &val.to_string(),
                        Value::Float(val) => &val.to_string(),
                        Value::Blob(val) => &String::from_utf8_lossy(val),
                        _ => "    ",
                    },
                    // If the second argument is omitted or is NULL, then indentation is four spaces per level
                    None => "    ",
                };

                let json_str = get_json(json_value.get_value(), Some(indent))?;
                state.registers[*dest] = Register::Value(json_str);
            }
            JsonFunc::JsonSet => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_set() needs an odd number of arguments")
                }
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];

                let json_result = json_set(reg_values, &state.json_cache);

                match json_result {
                    Ok(json) => state.registers[*dest] = Register::Value(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonbSet => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_set() needs an odd number of arguments")
                }
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];

                let json_result = jsonb_set(reg_values, &state.json_cache);

                match json_result {
                    Ok(json) => state.registers[*dest] = Register::Value(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonQuote => {
                let json_value = &state.registers[*start_reg];

                match json_quote(json_value.get_value()) {
                    Ok(result) => state.registers[*dest] = Register::Value(result),
                    Err(e) => return Err(e),
                }
            }
        },
        crate::function::Func::Scalar(scalar_func) => match scalar_func {
            ScalarFunc::Cast => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let reg_value_argument = state.registers[*start_reg].clone();
                let Value::Text(reg_value_type) =
                    state.registers[*start_reg + 1].get_value().clone()
                else {
                    unreachable!("Cast with non-text type");
                };
                let result = reg_value_argument
                    .get_value()
                    .exec_cast(reg_value_type.as_str());
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Changes => {
                let res = &program.connection.last_change;
                let changes = res.load(Ordering::SeqCst);
                state.registers[*dest] = Register::Value(Value::Integer(changes));
            }
            ScalarFunc::Char => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest] = Register::Value(exec_char(reg_values));
            }
            ScalarFunc::Coalesce => {}
            ScalarFunc::Concat => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                let result = exec_concat_strings(reg_values);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::ConcatWs => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                let result = exec_concat_ws(reg_values);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Glob => {
                let pattern = &state.registers[*start_reg];
                let text = &state.registers[*start_reg + 1];
                let result = match (pattern.get_value(), text.get_value()) {
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    (Value::Text(pattern), Value::Text(text)) => {
                        let cache = if *constant_mask > 0 {
                            Some(&mut state.regex_cache.glob)
                        } else {
                            None
                        };
                        Value::Integer(exec_glob(cache, pattern.as_str(), text.as_str()) as i64)
                    }
                    // Convert any other value types to text for GLOB comparison
                    (pattern_val, text_val) => {
                        let pattern_str = pattern_val.to_string();
                        let text_str = text_val.to_string();
                        let cache = if *constant_mask > 0 {
                            Some(&mut state.regex_cache.glob)
                        } else {
                            None
                        };
                        Value::Integer(exec_glob(cache, &pattern_str, &text_str) as i64)
                    }
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::IfNull => {}
            ScalarFunc::Iif => {}
            ScalarFunc::Instr => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = &state.registers[*start_reg + 1];
                let result = reg_value.get_value().exec_instr(pattern_value.get_value());
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::LastInsertRowid => {
                state.registers[*dest] =
                    Register::Value(Value::Integer(program.connection.last_insert_rowid()));
            }
            ScalarFunc::Like => {
                let pattern = &state.registers[*start_reg];
                let match_expression = &state.registers[*start_reg + 1];

                let pattern = match pattern.get_value() {
                    Value::Text(_) => pattern.get_value(),
                    _ => &pattern.get_value().exec_cast("TEXT"),
                };
                let match_expression = match match_expression.get_value() {
                    Value::Text(_) => match_expression.get_value(),
                    _ => &match_expression.get_value().exec_cast("TEXT"),
                };

                let result = match (pattern, match_expression) {
                    (Value::Text(pattern), Value::Text(match_expression)) if arg_count == 3 => {
                        let escape =
                            construct_like_escape_arg(state.registers[*start_reg + 2].get_value())?;

                        Value::Integer(exec_like_with_escape(
                            pattern.as_str(),
                            match_expression.as_str(),
                            escape,
                        ) as i64)
                    }
                    (Value::Text(pattern), Value::Text(match_expression)) => {
                        let cache = if *constant_mask > 0 {
                            Some(&mut state.regex_cache.like)
                        } else {
                            None
                        };
                        Value::Integer(Value::exec_like(
                            cache,
                            pattern.as_str(),
                            match_expression.as_str(),
                        ) as i64)
                    }
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    _ => {
                        unreachable!("Like failed");
                    }
                };

                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Abs
            | ScalarFunc::Lower
            | ScalarFunc::Upper
            | ScalarFunc::Length
            | ScalarFunc::OctetLength
            | ScalarFunc::Typeof
            | ScalarFunc::Unicode
            | ScalarFunc::Quote
            | ScalarFunc::RandomBlob
            | ScalarFunc::Sign
            | ScalarFunc::Soundex
            | ScalarFunc::ZeroBlob => {
                let reg_value = state.registers[*start_reg].borrow_mut().get_value();
                let result = match scalar_func {
                    ScalarFunc::Sign => reg_value.exec_sign(),
                    ScalarFunc::Abs => Some(reg_value.exec_abs()?),
                    ScalarFunc::Lower => reg_value.exec_lower(),
                    ScalarFunc::Upper => reg_value.exec_upper(),
                    ScalarFunc::Length => Some(reg_value.exec_length()),
                    ScalarFunc::OctetLength => Some(reg_value.exec_octet_length()),
                    ScalarFunc::Typeof => Some(reg_value.exec_typeof()),
                    ScalarFunc::Unicode => Some(reg_value.exec_unicode()),
                    ScalarFunc::Quote => Some(reg_value.exec_quote()),
                    ScalarFunc::RandomBlob => Some(reg_value.exec_randomblob()),
                    ScalarFunc::ZeroBlob => Some(reg_value.exec_zeroblob()),
                    ScalarFunc::Soundex => Some(reg_value.exec_soundex()),
                    _ => unreachable!(),
                };
                state.registers[*dest] = Register::Value(result.unwrap_or(Value::Null));
            }
            ScalarFunc::Hex => {
                let reg_value = state.registers[*start_reg].borrow_mut();
                let result = reg_value.get_value().exec_hex();
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Unhex => {
                let reg_value = &state.registers[*start_reg];
                let ignored_chars = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_unhex(ignored_chars.map(|x| x.get_value()));
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Random => {
                state.registers[*dest] = Register::Value(Value::exec_random());
            }
            ScalarFunc::Trim => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_trim(pattern_value.map(|x| x.get_value()));
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::LTrim => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_ltrim(pattern_value.map(|x| x.get_value()));
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::RTrim => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_rtrim(pattern_value.map(|x| x.get_value()));
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Round => {
                let reg_value = &state.registers[*start_reg];
                assert!(arg_count == 1 || arg_count == 2);
                let precision_value = if arg_count > 1 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_round(precision_value.map(|x| x.get_value()));
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Min => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest] =
                    Register::Value(Value::exec_min(reg_values.iter().map(|v| v.get_value())));
            }
            ScalarFunc::Max => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest] =
                    Register::Value(Value::exec_max(reg_values.iter().map(|v| v.get_value())));
            }
            ScalarFunc::Nullif => {
                let first_value = &state.registers[*start_reg];
                let second_value = &state.registers[*start_reg + 1];
                state.registers[*dest] = Register::Value(Value::exec_nullif(
                    first_value.get_value(),
                    second_value.get_value(),
                ));
            }
            ScalarFunc::Substr | ScalarFunc::Substring => {
                let str_value = &state.registers[*start_reg];
                let start_value = &state.registers[*start_reg + 1];
                let length_value = if func.arg_count == 3 {
                    Some(&state.registers[*start_reg + 2])
                } else {
                    None
                };
                let result = Value::exec_substring(
                    str_value.get_value(),
                    start_value.get_value(),
                    length_value.map(|x| x.get_value()),
                );
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Date => {
                let result = exec_date(&state.registers[*start_reg..*start_reg + arg_count]);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Time => {
                let values = &state.registers[*start_reg..*start_reg + arg_count];
                let result = exec_time(values);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::TimeDiff => {
                if arg_count != 2 {
                    state.registers[*dest] = Register::Value(Value::Null);
                } else {
                    let start = state.registers[*start_reg].get_value().clone();
                    let end = state.registers[*start_reg + 1].get_value().clone();

                    let result = crate::functions::datetime::exec_timediff(&[
                        Register::Value(start),
                        Register::Value(end),
                    ]);

                    state.registers[*dest] = Register::Value(result);
                }
            }
            ScalarFunc::TotalChanges => {
                let res = &program.connection.total_changes;
                let total_changes = res.load(Ordering::SeqCst);
                state.registers[*dest] = Register::Value(Value::Integer(total_changes));
            }
            ScalarFunc::DateTime => {
                let result =
                    exec_datetime_full(&state.registers[*start_reg..*start_reg + arg_count]);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::JulianDay => {
                let result = exec_julianday(&state.registers[*start_reg..*start_reg + arg_count]);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::UnixEpoch => {
                if *start_reg == 0 {
                    let result = exec_unixepoch(&Value::build_text("now"))?;
                    state.registers[*dest] = Register::Value(result);
                } else {
                    let datetime_value = &state.registers[*start_reg];
                    let unixepoch = exec_unixepoch(datetime_value.get_value());
                    match unixepoch {
                        Ok(time) => state.registers[*dest] = Register::Value(time),
                        Err(e) => {
                            return Err(LimboError::ParseError(format!(
                                "Error encountered while parsing datetime value: {e}"
                            )));
                        }
                    }
                }
            }
            ScalarFunc::SqliteVersion => {
                if !program.connection.is_db_initialized() {
                    state.registers[*dest] =
                        Register::Value(Value::build_text(info::build::PKG_VERSION));
                } else {
                    let version_integer =
                        return_if_io!(pager.with_header(|header| header.version_number)).get()
                            as i64;
                    let version = execute_sqlite_version(version_integer);
                    state.registers[*dest] = Register::Value(Value::build_text(version));
                }
            }
            ScalarFunc::SqliteSourceId => {
                let src_id = format!(
                    "{} {}",
                    info::build::BUILT_TIME_SQLITE,
                    info::build::GIT_COMMIT_HASH.unwrap_or("unknown")
                );
                state.registers[*dest] = Register::Value(Value::build_text(src_id));
            }
            ScalarFunc::Replace => {
                assert_eq!(arg_count, 3);
                let source = &state.registers[*start_reg];
                let pattern = &state.registers[*start_reg + 1];
                let replacement = &state.registers[*start_reg + 2];
                state.registers[*dest] = Register::Value(Value::exec_replace(
                    source.get_value(),
                    pattern.get_value(),
                    replacement.get_value(),
                ));
            }
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            ScalarFunc::LoadExtension => {
                if !program.connection.db.can_load_extensions() {
                    crate::bail_parse_error!("runtime extension loading is disabled");
                }
                let extension = &state.registers[*start_reg];
                let ext = resolve_ext_path(&extension.get_value().to_string())?;
                program.connection.load_extension(ext)?;
            }
            ScalarFunc::StrfTime => {
                let result = exec_strftime(&state.registers[*start_reg..*start_reg + arg_count]);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Printf => {
                let result = exec_printf(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::TableColumnsJsonArray => {
                assert_eq!(arg_count, 1);
                #[cfg(not(feature = "json"))]
                {
                    return Err(LimboError::InvalidArgument(
                        "table_columns_json_array: turso must be compiled with JSON support"
                            .to_string(),
                    ));
                }
                #[cfg(feature = "json")]
                {
                    use crate::types::{TextRef, TextSubtype};

                    let table = state.registers[*start_reg].get_value();
                    let Value::Text(table) = table else {
                        return Err(LimboError::InvalidArgument(
                            "table_columns_json_array: function argument must be of type TEXT"
                                .to_string(),
                        ));
                    };
                    let table = {
                        let schema = program.connection.schema.read();
                        match schema.get_table(table.as_str()) {
                            Some(table) => table,
                            None => {
                                return Err(LimboError::InvalidArgument(format!(
                                    "table_columns_json_array: table {table} doesn't exists"
                                )))
                            }
                        }
                    };

                    let mut json = json::jsonb::Jsonb::make_empty_array(table.columns().len() * 10);
                    for column in table.columns() {
                        let name = column.name.as_ref().unwrap();
                        let name_json = json::convert_ref_dbtype_to_jsonb(
                            &RefValue::Text(TextRef::create_from(
                                name.as_str().as_bytes(),
                                TextSubtype::Text,
                            )),
                            json::Conv::ToString,
                        )?;
                        json.append_jsonb_to_end(name_json.data());
                    }
                    json.finalize_unsafe(json::jsonb::ElementType::ARRAY)?;
                    state.registers[*dest] = Register::Value(json::json_string_to_db_type(
                        json,
                        json::jsonb::ElementType::ARRAY,
                        json::OutputVariant::String,
                    )?);
                }
            }
            ScalarFunc::BinRecordJsonObject => {
                assert_eq!(arg_count, 2);
                #[cfg(not(feature = "json"))]
                {
                    return Err(LimboError::InvalidArgument(
                        "bin_record_json_object: turso must be compiled with JSON support"
                            .to_string(),
                    ));
                }
                #[cfg(feature = "json")]
                'outer: {
                    use crate::types::RecordCursor;
                    use std::str::FromStr;

                    let columns_str = state.registers[*start_reg].get_value();
                    let bin_record = state.registers[*start_reg + 1].get_value();
                    let Value::Text(columns_str) = columns_str else {
                        return Err(LimboError::InvalidArgument(
                            "bin_record_json_object: function arguments must be of type TEXT and BLOB correspondingly".to_string()
                        ));
                    };

                    if let Value::Null = bin_record {
                        state.registers[*dest] = Register::Value(Value::Null);
                        break 'outer;
                    }

                    let Value::Blob(bin_record) = bin_record else {
                        return Err(LimboError::InvalidArgument(
                            "bin_record_json_object: function arguments must be of type TEXT and BLOB correspondingly".to_string()
                        ));
                    };
                    let mut columns_json_array =
                        json::jsonb::Jsonb::from_str(columns_str.as_str())?;
                    let columns_len = columns_json_array.array_len()?;

                    let mut record = ImmutableRecord::new(bin_record.len());
                    record.start_serialization(bin_record);
                    let mut record_cursor = RecordCursor::new();

                    let mut json = json::jsonb::Jsonb::make_empty_obj(columns_len);
                    for i in 0..columns_len {
                        let mut op = json::jsonb::SearchOperation::new(0);
                        let path = json::path::JsonPath {
                            elements: vec![
                                json::path::PathElement::Root(),
                                json::path::PathElement::ArrayLocator(Some(i as i32)),
                            ],
                        };

                        columns_json_array.operate_on_path(&path, &mut op)?;
                        let column_name = op.result();
                        json.append_jsonb_to_end(column_name.data());

                        let val = record_cursor.get_value(&record, i)?;
                        if let RefValue::Blob(..) = val {
                            return Err(LimboError::InvalidArgument(
                                "bin_record_json_object: formatting of BLOB values stored in binary record is not supported".to_string()
                            ));
                        }
                        let val_json =
                            json::convert_ref_dbtype_to_jsonb(&val, json::Conv::NotStrict)?;
                        json.append_jsonb_to_end(val_json.data());
                    }
                    json.finalize_unsafe(json::jsonb::ElementType::OBJECT)?;

                    state.registers[*dest] = Register::Value(json::json_string_to_db_type(
                        json,
                        json::jsonb::ElementType::OBJECT,
                        json::OutputVariant::String,
                    )?);
                }
            }
            ScalarFunc::Attach => {
                assert_eq!(arg_count, 3);
                let filename = state.registers[*start_reg].get_value();
                let dbname = state.registers[*start_reg + 1].get_value();
                let _key = state.registers[*start_reg + 2].get_value(); // Not used in read-only implementation

                let Value::Text(filename_str) = filename else {
                    return Err(LimboError::InvalidArgument(
                        "attach: filename argument must be text".to_string(),
                    ));
                };

                let Value::Text(dbname_str) = dbname else {
                    return Err(LimboError::InvalidArgument(
                        "attach: database name argument must be text".to_string(),
                    ));
                };

                program
                    .connection
                    .attach_database(filename_str.as_str(), dbname_str.as_str())?;

                state.registers[*dest] = Register::Value(Value::Null);
            }
            ScalarFunc::Detach => {
                assert_eq!(arg_count, 1);
                let dbname = state.registers[*start_reg].get_value();

                let Value::Text(dbname_str) = dbname else {
                    return Err(LimboError::InvalidArgument(
                        "detach: database name argument must be text".to_string(),
                    ));
                };

                // Call the detach_database method on the connection
                program.connection.detach_database(dbname_str.as_str())?;

                // Set result to NULL (detach doesn't return a value)
                state.registers[*dest] = Register::Value(Value::Null);
            }
            ScalarFunc::Unlikely | ScalarFunc::Likely | ScalarFunc::Likelihood => {
                panic!(
                    "{scalar_func:?} should be stripped during expression translation and never reach VDBE",
                );
            }
        },
        crate::function::Func::Vector(vector_func) => match vector_func {
            VectorFunc::Vector => {
                let result = vector32(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::Value(result);
            }
            VectorFunc::Vector32 => {
                let result = vector32(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::Value(result);
            }
            VectorFunc::Vector64 => {
                let result = vector64(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::Value(result);
            }
            VectorFunc::VectorExtract => {
                let result = vector_extract(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::Value(result);
            }
            VectorFunc::VectorDistanceCos => {
                let result =
                    vector_distance_cos(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::Value(result);
            }
            VectorFunc::VectorDistanceEuclidean => {
                let result =
                    vector_distance_l2(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::Value(result);
            }
            VectorFunc::VectorConcat => {
                let result = vector_concat(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::Value(result);
            }
            VectorFunc::VectorSlice => {
                let result = vector_slice(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::Value(result)
            }
        },
        crate::function::Func::External(f) => match f.func {
            ExtFunc::Scalar(f) => {
                if arg_count == 0 {
                    let result_c_value: ExtValue = unsafe { (f)(0, std::ptr::null()) };
                    match Value::from_ffi(result_c_value) {
                        Ok(result_ov) => {
                            state.registers[*dest] = Register::Value(result_ov);
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                } else {
                    let register_slice = &state.registers[*start_reg..*start_reg + arg_count];
                    let mut ext_values: Vec<ExtValue> = Vec::with_capacity(arg_count);
                    for ov in register_slice.iter() {
                        let val = ov.get_value().to_ffi();
                        ext_values.push(val);
                    }
                    let argv_ptr = ext_values.as_ptr();
                    let result_c_value: ExtValue = unsafe { (f)(arg_count as i32, argv_ptr) };
                    match Value::from_ffi(result_c_value) {
                        Ok(result_ov) => {
                            state.registers[*dest] = Register::Value(result_ov);
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
            }
            _ => unreachable!("aggregate called in scalar context"),
        },
        crate::function::Func::Math(math_func) => match math_func.arity() {
            MathFuncArity::Nullary => match math_func {
                MathFunc::Pi => {
                    state.registers[*dest] = Register::Value(Value::Float(std::f64::consts::PI));
                }
                _ => {
                    unreachable!("Unexpected mathematical Nullary function {:?}", math_func);
                }
            },

            MathFuncArity::Unary => {
                let reg_value = &state.registers[*start_reg];
                let result = reg_value.get_value().exec_math_unary(math_func);
                state.registers[*dest] = Register::Value(result);
            }

            MathFuncArity::Binary => {
                let lhs = &state.registers[*start_reg];
                let rhs = &state.registers[*start_reg + 1];
                let result = lhs.get_value().exec_math_binary(rhs.get_value(), math_func);
                state.registers[*dest] = Register::Value(result);
            }

            MathFuncArity::UnaryOrBinary => match math_func {
                MathFunc::Log => {
                    let result = match arg_count {
                        1 => {
                            let arg = &state.registers[*start_reg];
                            arg.get_value().exec_math_log(None)
                        }
                        2 => {
                            let base = &state.registers[*start_reg];
                            let arg = &state.registers[*start_reg + 1];
                            arg.get_value().exec_math_log(Some(base.get_value()))
                        }
                        _ => unreachable!(
                            "{:?} function with unexpected number of arguments",
                            math_func
                        ),
                    };
                    state.registers[*dest] = Register::Value(result);
                }
                _ => unreachable!(
                    "Unexpected mathematical UnaryOrBinary function {:?}",
                    math_func
                ),
            },
        },
        crate::function::Func::AlterTable(alter_func) => {
            let r#type = &state.registers[*start_reg].get_value().clone();

            let Value::Text(name) = &state.registers[*start_reg + 1].get_value() else {
                panic!("sqlite_schema.name should be TEXT")
            };
            let name = name.to_string();

            let Value::Text(tbl_name) = &state.registers[*start_reg + 2].get_value() else {
                panic!("sqlite_schema.tbl_name should be TEXT")
            };
            let tbl_name = tbl_name.to_string();

            let Value::Integer(root_page) = &state.registers[*start_reg + 3].get_value().clone()
            else {
                panic!("sqlite_schema.root_page should be INTEGER")
            };

            let sql = &state.registers[*start_reg + 4].get_value().clone();

            let (new_name, new_tbl_name, new_sql) = match alter_func {
                AlterTableFunc::RenameTable => {
                    let rename_from = {
                        match &state.registers[*start_reg + 5].get_value() {
                            Value::Text(rename_from) => normalize_ident(rename_from.as_str()),
                            _ => panic!("rename_from parameter should be TEXT"),
                        }
                    };

                    let original_rename_to = {
                        match &state.registers[*start_reg + 6].get_value() {
                            Value::Text(rename_to) => rename_to,
                            _ => panic!("rename_to parameter should be TEXT"),
                        }
                    };
                    let rename_to = normalize_ident(original_rename_to.as_str());

                    let new_name = if let Some(column) =
                        &name.strip_prefix(&format!("sqlite_autoindex_{rename_from}_"))
                    {
                        format!("sqlite_autoindex_{rename_to}_{column}")
                    } else if name == rename_from {
                        rename_to.clone()
                    } else {
                        name
                    };

                    let new_tbl_name = if tbl_name == rename_from {
                        rename_to.clone()
                    } else {
                        tbl_name
                    };

                    let new_sql = 'sql: {
                        let Value::Text(sql) = sql else {
                            break 'sql None;
                        };

                        let mut parser = Parser::new(sql.as_str().as_bytes());
                        let ast::Cmd::Stmt(stmt) = parser.next().unwrap().unwrap() else {
                            todo!()
                        };

                        match stmt {
                            ast::Stmt::CreateIndex {
                                tbl_name,
                                unique,
                                if_not_exists,
                                idx_name,
                                columns,
                                where_clause,
                            } => {
                                let table_name = normalize_ident(tbl_name.as_str());

                                if rename_from != table_name {
                                    break 'sql None;
                                }

                                Some(
                                    ast::Stmt::CreateIndex {
                                        tbl_name: ast::Name::new(original_rename_to),
                                        unique,
                                        if_not_exists,
                                        idx_name,
                                        columns,
                                        where_clause,
                                    }
                                    .to_string(),
                                )
                            }
                            ast::Stmt::CreateTable {
                                tbl_name,
                                temporary,
                                if_not_exists,
                                body,
                            } => {
                                let table_name = normalize_ident(tbl_name.name.as_str());

                                if rename_from != table_name {
                                    break 'sql None;
                                }

                                Some(
                                    ast::Stmt::CreateTable {
                                        tbl_name: ast::QualifiedName {
                                            db_name: None,
                                            name: ast::Name::new(original_rename_to),
                                            alias: None,
                                        },
                                        temporary,
                                        if_not_exists,
                                        body,
                                    }
                                    .to_string(),
                                )
                            }
                            _ => todo!(),
                        }
                    };

                    (new_name, new_tbl_name, new_sql)
                }
                AlterTableFunc::AlterColumn | AlterTableFunc::RenameColumn => {
                    let table = {
                        match &state.registers[*start_reg + 5].get_value() {
                            Value::Text(rename_to) => normalize_ident(rename_to.as_str()),
                            _ => panic!("table parameter should be TEXT"),
                        }
                    };

                    let original_rename_from = {
                        match &state.registers[*start_reg + 6].get_value() {
                            Value::Text(rename_from) => rename_from,
                            _ => panic!("rename_from parameter should be TEXT"),
                        }
                    };
                    let rename_from = normalize_ident(original_rename_from.as_str());

                    let column_def = {
                        match &state.registers[*start_reg + 7].get_value() {
                            Value::Text(column_def) => column_def.as_str(),
                            _ => panic!("rename_to parameter should be TEXT"),
                        }
                    };

                    let column_def = Parser::new(column_def.as_bytes())
                        .parse_column_definition(true)
                        .unwrap();

                    let new_sql = 'sql: {
                        if table != tbl_name {
                            break 'sql None;
                        }

                        let Value::Text(sql) = sql else {
                            break 'sql None;
                        };

                        let mut parser = Parser::new(sql.as_str().as_bytes());
                        let ast::Cmd::Stmt(stmt) = parser.next().unwrap().unwrap() else {
                            todo!()
                        };

                        match stmt {
                            ast::Stmt::CreateIndex {
                                tbl_name,
                                mut columns,
                                unique,
                                if_not_exists,
                                idx_name,
                                where_clause,
                            } => {
                                if table != normalize_ident(tbl_name.as_str()) {
                                    break 'sql None;
                                }

                                for column in &mut columns {
                                    match column.expr.as_mut() {
                                        ast::Expr::Id(ast::Name::Ident(id))
                                        | ast::Expr::Id(ast::Name::Quoted(id))
                                            if normalize_ident(id) == rename_from =>
                                        {
                                            *id = column_def.col_name.as_str().to_owned();
                                        }
                                        _ => {}
                                    }
                                }

                                Some(
                                    ast::Stmt::CreateIndex {
                                        tbl_name,
                                        columns,
                                        unique,
                                        if_not_exists,
                                        idx_name,
                                        where_clause,
                                    }
                                    .to_string(),
                                )
                            }
                            ast::Stmt::CreateTable {
                                tbl_name,
                                body,
                                temporary,
                                if_not_exists,
                            } => {
                                if table != normalize_ident(tbl_name.name.as_str()) {
                                    break 'sql None;
                                }

                                let ast::CreateTableBody::ColumnsAndConstraints {
                                    mut columns,
                                    constraints,
                                    options,
                                } = body
                                else {
                                    todo!()
                                };

                                let column = columns
                                    .iter_mut()
                                    .find(|column| {
                                        column.col_name == ast::Name::new(original_rename_from)
                                    })
                                    .expect("column being renamed should be present");

                                match alter_func {
                                    AlterTableFunc::AlterColumn => *column = column_def,
                                    AlterTableFunc::RenameColumn => {
                                        column.col_name = column_def.col_name
                                    }
                                    _ => unreachable!(),
                                }

                                Some(
                                    ast::Stmt::CreateTable {
                                        tbl_name,
                                        body: ast::CreateTableBody::ColumnsAndConstraints {
                                            columns,
                                            constraints,
                                            options,
                                        },
                                        temporary,
                                        if_not_exists,
                                    }
                                    .to_string(),
                                )
                            }
                            _ => todo!(),
                        }
                    };

                    (name, tbl_name, new_sql)
                }
            };

            state.registers[*dest] = Register::Value(r#type.clone());
            state.registers[*dest + 1] = Register::Value(Value::Text(Text::from(new_name)));
            state.registers[*dest + 2] = Register::Value(Value::Text(Text::from(new_tbl_name)));
            state.registers[*dest + 3] = Register::Value(Value::Integer(*root_page));

            if let Some(new_sql) = new_sql {
                state.registers[*dest + 4] = Register::Value(Value::Text(Text::from(new_sql)));
            } else {
                state.registers[*dest + 4] = Register::Value(sql.clone());
            }
        }
        crate::function::Func::Agg(_) => {
            unreachable!("Aggregate functions should not be handled here")
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sequence(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Sequence {
            cursor_id,
            target_reg
        },
        insn
    );
    let cursor = state.get_cursor(*cursor_id).as_sorter_mut();
    let seq_num = cursor.next_sequence();
    state.registers[*target_reg] = Register::Value(Value::Integer(seq_num));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sequence_test(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SequenceTest {
            cursor_id,
            target_pc,
            value_reg
        },
        insn
    );
    let cursor = state.get_cursor(*cursor_id).as_sorter_mut();
    state.pc = if cursor.seq_beginning() {
        target_pc.as_offset_int()
    } else {
        state.pc + 1
    };
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_init_coroutine(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        InitCoroutine {
            yield_reg,
            jump_on_definition,
            start_offset,
        },
        insn
    );
    assert!(jump_on_definition.is_offset());
    let start_offset = start_offset.as_offset_int();
    state.registers[*yield_reg] = Register::Value(Value::Integer(start_offset as i64));
    state.ended_coroutine.unset(*yield_reg);
    let jump_on_definition = jump_on_definition.as_offset_int();
    state.pc = if jump_on_definition == 0 {
        state.pc + 1
    } else {
        jump_on_definition
    };
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_end_coroutine(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(EndCoroutine { yield_reg }, insn);

    if let Value::Integer(pc) = state.registers[*yield_reg].get_value() {
        state.ended_coroutine.set(*yield_reg);
        let pc: u32 = (*pc)
            .try_into()
            .unwrap_or_else(|_| panic!("EndCoroutine: pc overflow: {pc}"));
        state.pc = pc - 1; // yield jump is always next to yield. Here we subtract 1 to go back to yield instruction
    } else {
        unreachable!();
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_yield(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Yield {
            yield_reg,
            end_offset,
        },
        insn
    );
    if let Value::Integer(pc) = state.registers[*yield_reg].get_value() {
        if state.ended_coroutine.get(*yield_reg) {
            state.pc = end_offset.as_offset_int();
        } else {
            let pc: u32 = (*pc)
                .try_into()
                .unwrap_or_else(|_| panic!("Yield: pc overflow: {pc}"));
            // swap the program counter with the value in the yield register
            // this is the mechanism that allows jumping back and forth between the coroutine and the caller
            (state.pc, state.registers[*yield_reg]) =
                (pc, Register::Value(Value::Integer((state.pc + 1) as i64)));
        }
    } else {
        unreachable!(
            "yield_reg {} contains non-integer value: {:?}",
            *yield_reg, state.registers[*yield_reg]
        );
    }
    Ok(InsnFunctionStepResult::Step)
}

pub struct OpInsertState {
    pub sub_state: OpInsertSubState,
    pub old_record: Option<(i64, Vec<Value>)>,
}

#[derive(Debug, PartialEq)]
pub enum OpInsertSubState {
    /// If this insert overwrites a record, capture the old record for incremental view maintenance.
    MaybeCaptureRecord,
    /// Seek to the correct position if needed.
    /// In a table insert, if the caller does not pass InsertFlags::REQUIRE_SEEK, they must ensure that a seek has already happened to the correct location.
    /// This typically happens by invoking either Insn::NewRowid or Insn::NotExists, because:
    /// 1. op_new_rowid() seeks to the end of the table, which is the correct insertion position.
    /// 2. op_not_exists() seeks to the position in the table where the target rowid would be inserted.
    Seek,
    /// Insert the row into the table.
    Insert,
    /// Updating last_insert_rowid may return IO, so we need a separate state for it so that we don't
    /// start inserting the same row multiple times.
    UpdateLastRowid,
    /// If there are dependent incremental views, apply the change.
    ApplyViewChange,
}

pub fn op_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Insert {
            cursor: cursor_id,
            key_reg,
            record_reg,
            flag,
            table_name,
        },
        insn
    );

    loop {
        match &state.op_insert_state.sub_state {
            OpInsertSubState::MaybeCaptureRecord => {
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                // If there are no dependent views, we don't need to capture the old record.
                // We also don't need to do it if the rowid of the UPDATEd row was changed, because that means
                // we deleted it earlier and `op_delete` already captured the change.
                if dependent_views.is_empty() || flag.has(InsertFlags::UPDATE_ROWID_CHANGE) {
                    if flag.has(InsertFlags::REQUIRE_SEEK) {
                        state.op_insert_state.sub_state = OpInsertSubState::Seek;
                    } else {
                        state.op_insert_state.sub_state = OpInsertSubState::Insert;
                    }
                    continue;
                }

                turso_assert!(!flag.has(InsertFlags::REQUIRE_SEEK), "to capture old record accurately, we must be located at the correct position in the table");

                let old_record = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    // Get the current key - for INSERT operations, there may not be a current row
                    let maybe_key = return_if_io!(cursor.rowid());
                    if let Some(key) = maybe_key {
                        // Get the current record before deletion and extract values
                        let maybe_record = return_if_io!(cursor.record());
                        if let Some(record) = maybe_record {
                            let mut values = record
                                .get_values()
                                .into_iter()
                                .map(|v| v.to_owned())
                                .collect::<Vec<_>>();

                            // Fix rowid alias columns: replace Null with actual rowid value
                            if let Some(table) = schema.get_table(table_name) {
                                for (i, col) in table.columns().iter().enumerate() {
                                    if col.is_rowid_alias && i < values.len() {
                                        values[i] = Value::Integer(key);
                                    }
                                }
                            }
                            Some((key, values))
                        } else {
                            None
                        }
                    } else {
                        // No current row - this is a fresh INSERT, not an UPDATE
                        None
                    }
                };

                state.op_insert_state.old_record = old_record;
                if flag.has(InsertFlags::REQUIRE_SEEK) {
                    state.op_insert_state.sub_state = OpInsertSubState::Seek;
                } else {
                    state.op_insert_state.sub_state = OpInsertSubState::Insert;
                }
                continue;
            }
            OpInsertSubState::Seek => {
                if let SeekInternalResult::IO(io) = seek_internal(
                    program,
                    state,
                    pager,
                    mv_store,
                    RecordSource::Unpacked {
                        start_reg: *key_reg,
                        num_regs: 1,
                    },
                    *cursor_id,
                    false,
                    SeekOp::GE { eq_only: true },
                )? {
                    return Ok(InsnFunctionStepResult::IO(io));
                }
                state.op_insert_state.sub_state = OpInsertSubState::Insert;
            }
            OpInsertSubState::Insert => {
                let key = match &state.registers[*key_reg].get_value() {
                    Value::Integer(i) => *i,
                    _ => unreachable!("expected integer key"),
                };
                let record = match &state.registers[*record_reg] {
                    Register::Record(r) => std::borrow::Cow::Borrowed(r),
                    Register::Value(value) => {
                        let x = 1;
                        let regs = &state.registers[*record_reg..*record_reg + 1];
                        let new_regs = [&state.registers[*record_reg]];
                        let record = ImmutableRecord::from_registers(new_regs, new_regs.len());
                        std::borrow::Cow::Owned(record)
                    }
                    Register::Aggregate(..) => unreachable!("Cannot insert an aggregate value."),
                };

                {
                    let cursor = get_cursor!(state, *cursor_id);
                    let cursor = cursor.as_btree_mut();

                    return_if_io!(cursor.insert(&BTreeKey::new_table_rowid(key, Some(&record))));
                }
                // Increment metrics for row write
                state.metrics.rows_written = state.metrics.rows_written.saturating_add(1);

                // Only update last_insert_rowid for regular table inserts, not schema modifications
                let root_page = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    cursor.root_page()
                };
                if root_page != 1 && table_name != "sqlite_sequence" {
                    state.op_insert_state.sub_state = OpInsertSubState::UpdateLastRowid;
                } else {
                    let schema = program.connection.schema.read();
                    let dependent_views = schema.get_dependent_materialized_views(table_name);
                    if !dependent_views.is_empty() {
                        state.op_insert_state.sub_state = OpInsertSubState::ApplyViewChange;
                    } else {
                        break;
                    }
                }
            }
            OpInsertSubState::UpdateLastRowid => {
                let maybe_rowid = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.rowid())
                };
                if let Some(rowid) = maybe_rowid {
                    program.connection.update_last_rowid(rowid);

                    let prev_changes = program.n_change.get();
                    program.n_change.set(prev_changes + 1);
                }
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                if !dependent_views.is_empty() {
                    state.op_insert_state.sub_state = OpInsertSubState::ApplyViewChange;
                    continue;
                }
                break;
            }
            OpInsertSubState::ApplyViewChange => {
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                assert!(!dependent_views.is_empty());

                let (key, values) = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();

                    let key = match &state.registers[*key_reg].get_value() {
                        Value::Integer(i) => *i,
                        _ => unreachable!("expected integer key"),
                    };

                    let record = match &state.registers[*record_reg] {
                        Register::Record(r) => std::borrow::Cow::Borrowed(r),
                        Register::Value(value) => {
                            let x = 1;
                            let regs = &state.registers[*record_reg..*record_reg + 1];
                            let new_regs = [&state.registers[*record_reg]];
                            let record = ImmutableRecord::from_registers(new_regs, new_regs.len());
                            std::borrow::Cow::Owned(record)
                        }
                        Register::Aggregate(..) => {
                            unreachable!("Cannot insert an aggregate value.")
                        }
                    };

                    // Add insertion of new row to view deltas
                    let mut new_values = record
                        .get_values()
                        .into_iter()
                        .map(|v| v.to_owned())
                        .collect::<Vec<_>>();

                    // Fix rowid alias columns: replace Null with actual rowid value
                    let schema = program.connection.schema.read();
                    if let Some(table) = schema.get_table(table_name) {
                        for (i, col) in table.columns().iter().enumerate() {
                            if col.is_rowid_alias && i < new_values.len() {
                                new_values[i] = Value::Integer(key);
                            }
                        }
                    }

                    (key, new_values)
                };

                if let Some((key, values)) = state.op_insert_state.old_record.take() {
                    for view_name in dependent_views.iter() {
                        let tx_state = program
                            .connection
                            .view_transaction_states
                            .get_or_create(view_name);
                        tx_state.delete(table_name, key, values.clone());
                    }
                }
                for view_name in dependent_views.iter() {
                    let tx_state = program
                        .connection
                        .view_transaction_states
                        .get_or_create(view_name);

                    tx_state.insert(table_name, key, values.clone());
                }

                break;
            }
        }
    }

    state.op_insert_state.sub_state = OpInsertSubState::MaybeCaptureRecord;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_int_64(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Int64 {
            _p1,
            out_reg,
            _p3,
            value,
        },
        insn
    );
    state.registers[*out_reg] = Register::Value(Value::Integer(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub struct OpDeleteState {
    pub sub_state: OpDeleteSubState,
    pub deleted_record: Option<(i64, Vec<Value>)>,
}

pub enum OpDeleteSubState {
    /// Capture the record before deletion, if the are dependent views.
    MaybeCaptureRecord,
    /// Delete the record.
    Delete,
    /// Apply the change to the dependent views.
    ApplyViewChange,
}

pub fn op_delete(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Delete {
            cursor_id,
            table_name
        },
        insn
    );

    loop {
        match &state.op_delete_state.sub_state {
            OpDeleteSubState::MaybeCaptureRecord => {
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                if dependent_views.is_empty() {
                    state.op_delete_state.sub_state = OpDeleteSubState::Delete;
                    continue;
                }

                let deleted_record = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    // Get the current key
                    let maybe_key = return_if_io!(cursor.rowid());
                    let key = maybe_key.ok_or_else(|| {
                        LimboError::InternalError("Cannot delete: no current row".to_string())
                    })?;
                    // Get the current record before deletion and extract values
                    let maybe_record = return_if_io!(cursor.record());
                    if let Some(record) = maybe_record {
                        let mut values = record
                            .get_values()
                            .into_iter()
                            .map(|v| v.to_owned())
                            .collect::<Vec<_>>();

                        // Fix rowid alias columns: replace Null with actual rowid value
                        if let Some(table) = schema.get_table(table_name) {
                            for (i, col) in table.columns().iter().enumerate() {
                                if col.is_rowid_alias && i < values.len() {
                                    values[i] = Value::Integer(key);
                                }
                            }
                        }
                        Some((key, values))
                    } else {
                        None
                    }
                };
                state.op_delete_state.deleted_record = deleted_record;
                state.op_delete_state.sub_state = OpDeleteSubState::Delete;
                continue;
            }
            OpDeleteSubState::Delete => {
                {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.delete());
                }
                // Increment metrics for row write (DELETE is a write operation)
                state.metrics.rows_written = state.metrics.rows_written.saturating_add(1);
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                if dependent_views.is_empty() {
                    break;
                }
                state.op_delete_state.sub_state = OpDeleteSubState::ApplyViewChange;
                continue;
            }
            OpDeleteSubState::ApplyViewChange => {
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                assert!(!dependent_views.is_empty());
                let maybe_deleted_record = state.op_delete_state.deleted_record.take();
                if let Some((key, values)) = maybe_deleted_record {
                    for view_name in dependent_views {
                        let tx_state = program
                            .connection
                            .view_transaction_states
                            .get_or_create(&view_name);
                        tx_state.delete(table_name, key, values.clone());
                    }
                }
                break;
            }
        }
    }

    state.op_delete_state.sub_state = OpDeleteSubState::MaybeCaptureRecord;
    let prev_changes = program.n_change.get();
    program.n_change.set(prev_changes + 1);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Debug)]
pub enum OpIdxDeleteState {
    Seeking,
    Verifying,
    Deleting,
}
pub fn op_idx_delete(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IdxDelete {
            cursor_id,
            start_reg,
            num_regs,
            raise_error_if_no_matching_entry,
        },
        insn
    );

    loop {
        #[cfg(debug_assertions)]
        tracing::debug!(
            "op_idx_delete(cursor_id={}, start_reg={}, num_regs={}, rootpage={}, state={:?})",
            cursor_id,
            start_reg,
            num_regs,
            state.get_cursor(*cursor_id).as_btree_mut().root_page(),
            state.op_idx_delete_state
        );
        match &state.op_idx_delete_state {
            Some(OpIdxDeleteState::Seeking) => {
                let found = match seek_internal(
                    program,
                    state,
                    pager,
                    mv_store,
                    RecordSource::Unpacked {
                        start_reg: *start_reg,
                        num_regs: *num_regs,
                    },
                    *cursor_id,
                    true,
                    SeekOp::GE { eq_only: true },
                ) {
                    Ok(SeekInternalResult::Found) => true,
                    Ok(SeekInternalResult::NotFound) => false,
                    Ok(SeekInternalResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
                    Err(e) => return Err(e),
                };

                if !found {
                    // If P5 is not zero, then raise an SQLITE_CORRUPT_INDEX error if no matching index entry is found
                    // Also, do not raise this (self-correcting and non-critical) error if in writable_schema mode.
                    if *raise_error_if_no_matching_entry {
                        let reg_values = (*start_reg..*start_reg + *num_regs)
                            .map(|i| &state.registers[i])
                            .collect::<Vec<_>>();
                        return Err(LimboError::Corrupt(format!(
                            "IdxDelete: no matching index entry found for key {reg_values:?}"
                        )));
                    }
                    state.pc += 1;
                    state.op_idx_delete_state = None;
                    return Ok(InsnFunctionStepResult::Step);
                }
                state.op_idx_delete_state = Some(OpIdxDeleteState::Verifying);
            }
            Some(OpIdxDeleteState::Verifying) => {
                let rowid = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.rowid())
                };

                if rowid.is_none() && *raise_error_if_no_matching_entry {
                    let reg_values = (*start_reg..*start_reg + *num_regs)
                        .map(|i| &state.registers[i])
                        .collect::<Vec<_>>();
                    return Err(LimboError::Corrupt(format!(
                        "IdxDelete: no matching index entry found for key {reg_values:?}"
                    )));
                }
                state.op_idx_delete_state = Some(OpIdxDeleteState::Deleting);
            }
            Some(OpIdxDeleteState::Deleting) => {
                {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.delete());
                }
                // Increment metrics for index write (delete is a write operation)
                state.metrics.rows_written = state.metrics.rows_written.saturating_add(1);
                state.pc += 1;
                state.op_idx_delete_state = None;
                return Ok(InsnFunctionStepResult::Step);
            }
            None => {
                state.op_idx_delete_state = Some(OpIdxDeleteState::Seeking);
            }
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum OpIdxInsertState {
    /// Optional seek step done before an unique constraint check or if the caller indicates a seek is required.
    MaybeSeek,
    /// Optional unique constraint check done before an insert.
    UniqueConstraintCheck,
    /// Main insert step. This is always performed.
    Insert,
}

pub fn op_idx_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IdxInsert {
            cursor_id,
            record_reg,
            flags,
            ..
        },
        *insn
    );

    let record_to_insert = match &state.registers[record_reg] {
        Register::Record(ref r) => r,
        o => {
            return Err(LimboError::InternalError(format!(
                "expected record, got {o:?}"
            )));
        }
    };

    match state.op_idx_insert_state {
        OpIdxInsertState::MaybeSeek => {
            let (_, cursor_type) = program.cursor_ref.get(cursor_id).unwrap();
            let CursorType::BTreeIndex(index_meta) = cursor_type else {
                panic!("IdxInsert: not a BTreeIndex cursor");
            };

            // TODO: currently we never pass USE_SEEK, so this other check is a bit redundant and we always seek,
            // but I guess it's FutureProofed™®
            if !index_meta.unique && flags.has(IdxInsertFlags::USE_SEEK) {
                state.op_idx_insert_state = OpIdxInsertState::Insert;
                return Ok(InsnFunctionStepResult::Step);
            }

            match seek_internal(
                program,
                state,
                pager,
                mv_store,
                RecordSource::Packed { record_reg },
                cursor_id,
                true,
                SeekOp::GE { eq_only: true },
            )? {
                SeekInternalResult::Found => {
                    state.op_idx_insert_state = if index_meta.unique {
                        OpIdxInsertState::UniqueConstraintCheck
                    } else {
                        OpIdxInsertState::Insert
                    };
                    Ok(InsnFunctionStepResult::Step)
                }
                SeekInternalResult::NotFound => {
                    state.op_idx_insert_state = OpIdxInsertState::Insert;
                    Ok(InsnFunctionStepResult::Step)
                }
                SeekInternalResult::IO(io) => Ok(InsnFunctionStepResult::IO(io)),
            }
        }
        OpIdxInsertState::UniqueConstraintCheck => {
            let ignore_conflict = 'i: {
                let cursor = get_cursor!(state, cursor_id);
                let cursor = cursor.as_btree_mut();
                let record_opt = return_if_io!(cursor.record());
                let Some(record) = record_opt.as_ref() else {
                    // Cursor not pointing at a record — table is empty or past last
                    break 'i false;
                };
                // Cursor is pointing at a record; if the index has a rowid, exclude it from the comparison since it's a pointer to the table row;
                // UNIQUE indexes disallow duplicates like (a=1,b=2,rowid=1) and (a=1,b=2,rowid=2).
                let existing_key = if cursor.has_rowid() {
                    let count = cursor.record_cursor.borrow_mut().count(record);
                    record.get_values()[..count.saturating_sub(1)].to_vec()
                } else {
                    record.get_values().to_vec()
                };
                let inserted_key_vals = &record_to_insert.get_values();
                if existing_key.len() != inserted_key_vals.len() {
                    break 'i false;
                }

                let conflict = compare_immutable(
                    existing_key.as_slice(),
                    inserted_key_vals,
                    &cursor.index_info.as_ref().unwrap().key_info,
                ) == std::cmp::Ordering::Equal;
                if conflict {
                    if flags.has(IdxInsertFlags::NO_OP_DUPLICATE) {
                        break 'i true;
                    }
                    return Err(LimboError::Constraint(
                        "UNIQUE constraint failed: duplicate key".into(),
                    ));
                }

                false
            };
            state.op_idx_insert_state = if ignore_conflict {
                state.pc += 1;
                OpIdxInsertState::MaybeSeek
            } else {
                OpIdxInsertState::Insert
            };
            Ok(InsnFunctionStepResult::Step)
        }
        OpIdxInsertState::Insert => {
            {
                let cursor = get_cursor!(state, cursor_id);
                let cursor = cursor.as_btree_mut();
                return_if_io!(cursor.insert(&BTreeKey::new_index_key(record_to_insert)));
            }
            // Increment metrics for index write
            if flags.has(IdxInsertFlags::NCHANGE) {
                state.metrics.rows_written = state.metrics.rows_written.saturating_add(1);
            }
            state.op_idx_insert_state = OpIdxInsertState::MaybeSeek;
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OpNewRowidState {
    Start,
    SeekingToLast,
    ReadingMaxRowid,
    GeneratingRandom {
        attempts: u32,
    },
    VerifyingCandidate {
        attempts: u32,
        candidate: i64,
    },
    /// In case a rowid was generated and not provided by the user, we need to call next() on the cursor
    /// after generating the rowid. This is because the rowid was generated by seeking to the last row in the
    /// table, and we need to insert _after_ that row.
    GoNext,
}

pub fn op_new_rowid(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        NewRowid {
            cursor,
            rowid_reg,
            ..
        },
        insn
    );

    if let Some(mv_store) = mv_store {
        let rowid = {
            let cursor = state.get_cursor(*cursor);
            let cursor = cursor.as_btree_mut();
            let mvcc_cursor = cursor.get_mvcc_cursor();
            let mut mvcc_cursor = mvcc_cursor.write();
            mvcc_cursor.get_next_rowid()
        };
        state.registers[*rowid_reg] = Register::Value(Value::Integer(rowid));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    const MAX_ROWID: i64 = i64::MAX;
    const MAX_ATTEMPTS: u32 = 100;

    loop {
        match state.op_new_rowid_state {
            OpNewRowidState::Start => {
                state.op_new_rowid_state = OpNewRowidState::SeekingToLast;
            }

            OpNewRowidState::SeekingToLast => {
                {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.seek_to_last());
                }
                state.op_new_rowid_state = OpNewRowidState::ReadingMaxRowid;
            }

            OpNewRowidState::ReadingMaxRowid => {
                let current_max = {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.rowid())
                };

                match current_max {
                    Some(rowid) if rowid < MAX_ROWID => {
                        // Can use sequential
                        state.registers[*rowid_reg] = Register::Value(Value::Integer(rowid + 1));
                        state.op_new_rowid_state = OpNewRowidState::GoNext;
                        continue;
                    }
                    Some(_) => {
                        // Must use random (rowid == MAX_ROWID)
                        state.op_new_rowid_state =
                            OpNewRowidState::GeneratingRandom { attempts: 0 };
                    }
                    None => {
                        // Empty table
                        state.registers[*rowid_reg] = Register::Value(Value::Integer(1));
                        state.op_new_rowid_state = OpNewRowidState::GoNext;
                        continue;
                    }
                }
            }

            OpNewRowidState::GeneratingRandom { attempts } => {
                if attempts >= MAX_ATTEMPTS {
                    return Err(LimboError::DatabaseFull("Unable to find an unused rowid after 100 attempts - database is probably full".to_string()));
                }

                // Generate a random i64 and constrain it to the lower half of the rowid range.
                // We use the lower half (1 to MAX_ROWID/2) because we're in random mode only
                // when sequential allocation reached MAX_ROWID, meaning the upper range is full.
                let mut rng = thread_rng();
                let mut random_rowid: i64 = rng.gen();
                random_rowid &= MAX_ROWID >> 1; // Mask to keep value in range [0, MAX_ROWID/2]
                random_rowid += 1; // Ensure positive

                state.op_new_rowid_state = OpNewRowidState::VerifyingCandidate {
                    attempts,
                    candidate: random_rowid,
                };
            }

            OpNewRowidState::VerifyingCandidate {
                attempts,
                candidate,
            } => {
                let exists = {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    let seek_result =
                        return_if_io!(cursor
                            .seek(SeekKey::TableRowId(candidate), SeekOp::GE { eq_only: true }));
                    matches!(seek_result, SeekResult::Found)
                };

                if !exists {
                    // Found unused rowid!
                    state.registers[*rowid_reg] = Register::Value(Value::Integer(candidate));
                    state.op_new_rowid_state = OpNewRowidState::Start;
                    state.pc += 1;
                    return Ok(InsnFunctionStepResult::Step);
                } else {
                    // Collision, try again
                    state.op_new_rowid_state = OpNewRowidState::GeneratingRandom {
                        attempts: attempts + 1,
                    };
                }
            }
            OpNewRowidState::GoNext => {
                {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.next());
                }
                state.op_new_rowid_state = OpNewRowidState::Start;
                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_must_be_int(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(MustBeInt { reg }, insn);
    match &state.registers[*reg].get_value() {
        Value::Integer(_) => {}
        Value::Float(f) => match cast_real_to_integer(*f) {
            Ok(i) => state.registers[*reg] = Register::Value(Value::Integer(i)),
            Err(_) => crate::bail_parse_error!(
                "MustBeInt: the value in register cannot be cast to integer"
            ),
        },
        Value::Text(text) => match checked_cast_text_to_numeric(text.as_str()) {
            Ok(Value::Integer(i)) => state.registers[*reg] = Register::Value(Value::Integer(i)),
            Ok(Value::Float(f)) => {
                state.registers[*reg] = Register::Value(Value::Integer(f as i64))
            }
            _ => crate::bail_parse_error!(
                "MustBeInt: the value in register cannot be cast to integer"
            ),
        },
        _ => {
            crate::bail_parse_error!("MustBeInt: the value in register cannot be cast to integer");
        }
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_soft_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(SoftNull { reg }, insn);
    state.registers[*reg] = Register::Value(Value::Null);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Clone, Copy)]
pub enum OpNoConflictState {
    Start,
    Seeking(RecordSource),
}

/// If a matching record is not found in the btree ("no conflict"), jump to the target PC.
/// Otherwise, continue execution.
pub fn op_no_conflict(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        NoConflict {
            cursor_id,
            target_pc,
            record_reg,
            num_regs,
        },
        insn
    );

    loop {
        match state.op_no_conflict_state {
            OpNoConflictState::Start => {
                let cursor_ref = state.get_cursor(*cursor_id);
                let cursor = cursor_ref.as_btree_mut();

                let record_source = if *num_regs == 0 {
                    RecordSource::Packed {
                        record_reg: *record_reg,
                    }
                } else {
                    RecordSource::Unpacked {
                        start_reg: *record_reg,
                        num_regs: *num_regs,
                    }
                };

                // If there is at least one NULL in the index record, there cannot be a conflict so we can immediately jump.
                let contains_nulls = match &record_source {
                    RecordSource::Packed { record_reg } => {
                        let Register::Record(record) = &state.registers[*record_reg] else {
                            return Err(LimboError::InternalError(
                                "NoConflict: expected a record in the register".into(),
                            ));
                        };
                        record
                            .get_values()
                            .iter()
                            .any(|val| matches!(val, RefValue::Null))
                    }
                    RecordSource::Unpacked {
                        start_reg,
                        num_regs,
                    } => (0..*num_regs).any(|i| {
                        matches!(
                            &state.registers[start_reg + i],
                            Register::Value(Value::Null)
                        )
                    }),
                };

                if contains_nulls {
                    state.pc = target_pc.as_offset_int();
                    state.op_no_conflict_state = OpNoConflictState::Start;
                    return Ok(InsnFunctionStepResult::Step);
                } else {
                    state.op_no_conflict_state = OpNoConflictState::Seeking(record_source);
                }
            }
            OpNoConflictState::Seeking(record_source) => {
                return match seek_internal(
                    program,
                    state,
                    pager,
                    mv_store,
                    record_source,
                    *cursor_id,
                    true,
                    SeekOp::GE { eq_only: true },
                )? {
                    SeekInternalResult::Found => {
                        state.pc += 1;
                        state.op_no_conflict_state = OpNoConflictState::Start;
                        Ok(InsnFunctionStepResult::Step)
                    }
                    SeekInternalResult::NotFound => {
                        state.pc = target_pc.as_offset_int();
                        state.op_no_conflict_state = OpNoConflictState::Start;
                        Ok(InsnFunctionStepResult::Step)
                    }
                    SeekInternalResult::IO(io) => Ok(InsnFunctionStepResult::IO(io)),
                };
            }
        }
    }
}

pub fn op_not_exists(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        NotExists {
            cursor,
            rowid_reg,
            target_pc,
        },
        insn
    );
    let exists = if let Some(mv_store) = mv_store {
        let cursor = must_be_btree_cursor!(*cursor, program.cursor_ref, state, "NotExists");
        let cursor = cursor.as_btree_mut();
        let mvcc_cursor = cursor.get_mvcc_cursor();
        false
    } else {
        let cursor = must_be_btree_cursor!(*cursor, program.cursor_ref, state, "NotExists");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.exists(state.registers[*rowid_reg].get_value()))
    };
    if exists {
        state.pc += 1;
    } else {
        state.pc = target_pc.as_offset_int();
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_offset_limit(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        OffsetLimit {
            limit_reg,
            combined_reg,
            offset_reg,
        },
        insn
    );
    let limit_val = match state.registers[*limit_reg].get_value() {
        Value::Integer(val) => val,
        _ => {
            return Err(LimboError::InternalError(
                "OffsetLimit: the value in limit_reg is not an integer".into(),
            ));
        }
    };
    let offset_val = match state.registers[*offset_reg].get_value() {
        Value::Integer(val) if *val < 0 => 0,
        Value::Integer(val) if *val >= 0 => *val,
        _ => {
            return Err(LimboError::InternalError(
                "OffsetLimit: the value in offset_reg is not an integer".into(),
            ));
        }
    };

    let offset_limit_sum = limit_val.overflowing_add(offset_val);
    if *limit_val <= 0 || offset_limit_sum.1 {
        state.registers[*combined_reg] = Register::Value(Value::Integer(-1));
    } else {
        state.registers[*combined_reg] = Register::Value(Value::Integer(offset_limit_sum.0));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}
// this cursor may be reused for next insert
// Update: tablemoveto is used to travers on not exists, on insert depending on flags if nonseek it traverses again.
// If not there might be some optimizations obviously.
pub fn op_open_write(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        OpenWrite {
            cursor_id,
            root_page,
            db,
        },
        insn
    );
    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly);
    }
    let pager = program.get_pager_from_database_index(db);

    let root_page = match root_page {
        RegisterOrLiteral::Literal(lit) => *lit as u64,
        RegisterOrLiteral::Register(reg) => match &state.registers[*reg].get_value() {
            Value::Integer(val) => *val as u64,
            _ => {
                return Err(LimboError::InternalError(
                    "OpenWrite: the value in root_page is not an integer".into(),
                ));
            }
        },
    };

    const SQLITE_SCHEMA_ROOT_PAGE: u64 = 1;

    if root_page == SQLITE_SCHEMA_ROOT_PAGE {
        if let Some(mv_store) = mv_store {
            let Some(tx_id) = program.connection.get_mv_tx_id() else {
                return Err(LimboError::InternalError(
                    "Schema changes in MVCC mode require an exclusive MVCC transaction".to_string(),
                ));
            };
            if !mv_store.is_exclusive_tx(&tx_id) {
                // Schema changes in MVCC mode require an exclusive transaction
                return Err(LimboError::TableLocked);
            }
        }
    }

    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let cursors = &mut state.cursors;
    let maybe_index = match cursor_type {
        CursorType::BTreeIndex(index) => Some(index),
        _ => None,
    };
    let mv_cursor = if let Some(tx_id) = program.connection.get_mv_tx_id() {
        let table_id = root_page;
        let mv_store = mv_store.unwrap().clone();
        let mv_cursor = Arc::new(RwLock::new(
            MvCursor::new(mv_store.clone(), tx_id, table_id, pager.clone()).unwrap(),
        ));
        Some(mv_cursor)
    } else {
        None
    };
    if let Some(index) = maybe_index {
        let conn = program.connection.clone();
        let schema = conn.schema.read();
        let table = schema
            .get_table(&index.table_name)
            .and_then(|table| table.btree());

        let num_columns = index.columns.len();
        let cursor = BTreeCursor::new_index(
            mv_cursor,
            pager.clone(),
            root_page as usize,
            index.as_ref(),
            num_columns,
        );
        cursors
            .get_mut(*cursor_id)
            .unwrap()
            .replace(Cursor::new_btree(cursor));
    } else {
        let num_columns = match cursor_type {
            CursorType::BTreeTable(table_rc) => table_rc.columns.len(),
            CursorType::MaterializedView(table_rc, _) => table_rc.columns.len(),
            _ => unreachable!(
                "Expected BTreeTable or MaterializedView. This should not have happened."
            ),
        };

        let cursor =
            BTreeCursor::new_table(mv_cursor, pager.clone(), root_page as usize, num_columns);
        cursors
            .get_mut(*cursor_id)
            .unwrap()
            .replace(Cursor::new_btree(cursor));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_copy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Copy {
            src_reg,
            dst_reg,
            extra_amount,
        },
        insn
    );
    for i in 0..=*extra_amount {
        state.registers[*dst_reg + i] = state.registers[*src_reg + i].clone();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_create_btree(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(CreateBtree { db, root, flags }, insn);

    assert_eq!(*db, 0);

    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly);
    }
    if *db > 0 {
        // TODO: implement temp databases
        todo!("temp databases not implemented yet");
    }

    if let Some(mv_store) = mv_store {
        let root_page = mv_store.get_next_table_id();
        state.registers[*root] = Register::Value(Value::Integer(root_page as i64));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    // FIXME: handle page cache is full
    let root_page = return_if_io!(pager.btree_create(flags));
    state.registers[*root] = Register::Value(Value::Integer(root_page as i64));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_destroy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Destroy {
            root,
            former_root_reg,
            is_temp,
        },
        insn
    );
    if *is_temp == 1 {
        todo!("temp databases not implemented yet.");
    }
    // TODO not sure if should be BTreeCursor::new_table or BTreeCursor::new_index here or neither and just pass an emtpy vec
    let mut cursor = BTreeCursor::new(None, pager.clone(), *root, 0);
    let former_root_page_result = cursor.btree_destroy()?;
    if let IOResult::Done(former_root_page) = former_root_page_result {
        state.registers[*former_root_reg] =
            Register::Value(Value::Integer(former_root_page.unwrap_or(0) as i64));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_reset_sorter(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ResetSorter { cursor_id }, insn);

    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let cursor = state.get_cursor(*cursor_id);

    match cursor_type {
        CursorType::BTreeTable(table) => {
            let cursor = cursor.as_btree_mut();
            return_if_io!(cursor.clear_btree());
        }
        CursorType::Sorter => {
            unimplemented!("ResetSorter is not supported for sorter cursors yet")
        }
        _ => panic!("ResetSorter is not supported for {cursor_type:?}"),
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_table(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(DropTable { db, table_name, .. }, insn);
    if *db > 0 {
        todo!("temp databases not implemented yet");
    }
    let conn = program.connection.clone();
    {
        conn.with_schema_mut(|schema| {
            schema.remove_indices_for_table(table_name);
            schema.remove_table(table_name);
        });
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_view(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
    _mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(DropView { db, view_name }, insn);
    if *db > 0 {
        todo!("temp databases not implemented yet");
    }
    let conn = program.connection.clone();
    conn.with_schema_mut(|schema| {
        schema.remove_view(view_name)?;
        Ok::<(), crate::LimboError>(())
    })?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_close(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Close { cursor_id }, insn);
    let cursors = &mut state.cursors;
    cursors.get_mut(*cursor_id).unwrap().take();
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_is_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(IsNull { reg, target_pc }, insn);
    if matches!(state.registers[*reg], Register::Value(Value::Null)) {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_coll_seq(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
    _mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::CollSeq { reg, collation } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    // Set the current collation sequence for use by subsequent functions
    state.current_collation = Some(*collation);

    // If P1 is not zero, initialize that register to 0
    if let Some(reg_idx) = reg {
        state.registers[*reg_idx] = Register::Value(Value::Integer(0));
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_page_count(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(PageCount { db, dest }, insn);
    if *db > 0 {
        // TODO: implement temp databases
        todo!("temp databases not implemented yet");
    }
    let count = match with_header(pager, mv_store, program, |header| {
        header.database_size.get()
    }) {
        Err(_) => 0.into(),
        Ok(IOResult::Done(v)) => v.into(),
        Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
    };
    state.registers[*dest] = Register::Value(Value::Integer(count));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_parse_schema(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        ParseSchema {
            db: _,
            where_clause,
        },
        insn
    );

    let conn = program.connection.clone();
    // set auto commit to false in order for parse schema to not commit changes as transaction state is stored in connection,
    // and we use the same connection for nested query.
    let previous_auto_commit = conn.auto_commit.load(Ordering::SeqCst);
    conn.auto_commit.store(false, Ordering::SeqCst);

    let maybe_nested_stmt_err = if let Some(where_clause) = where_clause {
        let stmt = conn.prepare(format!("SELECT * FROM sqlite_schema WHERE {where_clause}"))?;

        conn.with_schema_mut(|schema| {
            // TODO: This function below is synchronous, make it async
            let existing_views = schema.incremental_views.clone();
            conn.is_nested_stmt.store(true, Ordering::SeqCst);
            parse_schema_rows(
                stmt,
                schema,
                &conn.syms.read(),
                program.connection.get_mv_tx(),
                existing_views,
                mv_store,
            )
        })
    } else {
        let stmt = conn.prepare("SELECT * FROM sqlite_schema")?;

        conn.with_schema_mut(|schema| {
            // TODO: This function below is synchronous, make it async
            let existing_views = schema.incremental_views.clone();
            conn.is_nested_stmt.store(true, Ordering::SeqCst);
            parse_schema_rows(
                stmt,
                schema,
                &conn.syms.read(),
                program.connection.get_mv_tx(),
                existing_views,
                mv_store,
            )
        })
    };
    conn.is_nested_stmt.store(false, Ordering::SeqCst);
    conn.auto_commit
        .store(previous_auto_commit, Ordering::SeqCst);
    maybe_nested_stmt_err?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_populate_materialized_views(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    _mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(PopulateMaterializedViews { cursors }, insn);

    let conn = program.connection.clone();

    // For each view, get its cursor and root page
    let mut view_info = Vec::new();
    {
        let cursors_ref = &state.cursors;
        for (view_name, cursor_id) in cursors {
            // Get the cursor to find the root page
            let cursor = cursors_ref
                .get(*cursor_id)
                .and_then(|c| c.as_ref())
                .ok_or_else(|| {
                    LimboError::InternalError(format!("Cursor {cursor_id} not found"))
                })?;

            let root_page = match cursor {
                crate::types::Cursor::BTree(btree_cursor) => btree_cursor.root_page(),
                _ => {
                    return Err(LimboError::InternalError(
                        "Expected BTree cursor for materialized view".into(),
                    ))
                }
            };

            view_info.push((view_name.clone(), root_page, *cursor_id));
        }
    }

    // Now populate the views (after releasing the schema borrow)
    for (view_name, _root_page, cursor_id) in view_info {
        let schema = conn.schema.read();
        if let Some(view) = schema.get_materialized_view(&view_name) {
            let mut view = view.lock().unwrap();
            // Drop the schema borrow before calling populate_from_table
            drop(schema);

            // Get the cursor for writing
            // Get a mutable reference to the cursor
            let cursors_ref = &mut state.cursors;
            let cursor = cursors_ref
                .get_mut(cursor_id)
                .and_then(|c| c.as_mut())
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "Cursor {cursor_id} not found for population"
                    ))
                })?;

            // Extract the BTreeCursor
            let btree_cursor = match cursor {
                crate::types::Cursor::BTree(btree_cursor) => btree_cursor,
                _ => {
                    return Err(LimboError::InternalError(
                        "Expected BTree cursor for materialized view population".into(),
                    ))
                }
            };

            // Now populate it with the cursor for writing
            return_if_io!(view.populate_from_table(&conn, pager, btree_cursor.as_mut()));
        }
    }

    // All views populated, advance to next instruction
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_read_cookie(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ReadCookie { db, dest, cookie }, insn);
    if *db > 0 {
        // TODO: implement temp databases
        todo!("temp databases not implemented yet");
    }

    let cookie_value = match with_header(pager, mv_store, program, |header| match cookie {
        Cookie::ApplicationId => header.application_id.get().into(),
        Cookie::UserVersion => header.user_version.get().into(),
        Cookie::SchemaVersion => header.schema_cookie.get().into(),
        Cookie::LargestRootPageNumber => header.vacuum_mode_largest_root_page.get().into(),
        cookie => todo!("{cookie:?} is not yet implement for ReadCookie"),
    }) {
        Err(_) => 0.into(),
        Ok(IOResult::Done(v)) => v,
        Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
    };

    state.registers[*dest] = Register::Value(Value::Integer(cookie_value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_set_cookie(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SetCookie {
            db,
            cookie,
            value,
            p5,
        },
        insn
    );
    if *db > 0 {
        todo!("temp databases not implemented yet");
    }

    return_if_io!(with_header_mut(pager, mv_store, program, |header| {
        match cookie {
            Cookie::ApplicationId => header.application_id = (*value).into(),
            Cookie::UserVersion => header.user_version = (*value).into(),
            Cookie::LargestRootPageNumber => {
                header.vacuum_mode_largest_root_page = (*value as u32).into();
            }
            Cookie::IncrementalVacuum => header.incremental_vacuum_enabled = (*value as u32).into(),
            Cookie::SchemaVersion => {
                // we update transaction state to indicate that the schema has changed
                match program.connection.get_tx_state() {
                    TransactionState::Write { schema_did_change } => {
                        program.connection.set_tx_state(TransactionState::Write { schema_did_change: true });
                    },
                    TransactionState::Read => unreachable!("invalid transaction state for SetCookie: TransactionState::Read, should be write"),
                    TransactionState::None => unreachable!("invalid transaction state for SetCookie: TransactionState::None, should be write"),
                    TransactionState::PendingUpgrade => unreachable!("invalid transaction state for SetCookie: TransactionState::PendingUpgrade, should be write"),
                }
                program
                    .connection
                    .with_schema_mut(|schema| schema.schema_version = *value as u32);
                header.schema_cookie = (*value as u32).into();
            }
            cookie => todo!("{cookie:?} is not yet implement for SetCookie"),
        };
    }));

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_shift_right(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ShiftRight { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_shift_right(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_shift_left(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ShiftLeft { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_shift_left(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add_imm(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(AddImm { register, value }, insn);

    let current = &state.registers[*register];
    let current_value = match current {
        Register::Value(val) => val,
        Register::Aggregate(_) => &Value::Null,
        Register::Record(_) => &Value::Null,
    };

    let int_val = match current_value {
        Value::Integer(i) => i + value,
        Value::Float(f) => (*f as i64) + value,
        Value::Text(s) => s.as_str().parse::<i64>().unwrap_or(0) + value,
        Value::Blob(_) => *value, // BLOB becomes the added value
        Value::Null => *value,    // NULL becomes the added value
    };

    state.registers[*register] = Register::Value(Value::Integer(int_val));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_variable(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Variable { index, dest }, insn);
    state.registers[*dest] = Register::Value(state.get_parameter(*index));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_zero_or_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ZeroOrNull { rg1, rg2, dest }, insn);
    if state.registers[*rg1].is_null() || state.registers[*rg2].is_null() {
        state.registers[*dest] = Register::Value(Value::Null)
    } else {
        state.registers[*dest] = Register::Value(Value::Integer(0));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_not(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Not { reg, dest }, insn);
    state.registers[*dest] = Register::Value(state.registers[*reg].get_value().exec_boolean_not());
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_concat(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Concat { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_concat(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_and(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(And { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_and(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_or(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Or { lhs, rhs, dest }, insn);
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_value()
            .exec_or(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_noop(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    // Do nothing
    // Advance the program counter for the next opcode
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Default)]
pub enum OpOpenEphemeralState {
    #[default]
    Start,
    StartingTxn {
        pager: Arc<Pager>,
    },
    CreateBtree {
        pager: Arc<Pager>,
    },
    // clippy complains this variant is too big when compared to the rest of the variants
    // so it says we need to box it here
    Rewind {
        cursor: Box<BTreeCursor>,
    },
}
pub fn op_open_ephemeral(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let (cursor_id, is_table) = match insn {
        Insn::OpenEphemeral {
            cursor_id,
            is_table,
        } => (*cursor_id, *is_table),
        Insn::OpenAutoindex { cursor_id } => (*cursor_id, false),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    match &mut state.op_open_ephemeral_state {
        OpOpenEphemeralState::Start => {
            tracing::trace!("Start");
            let page_size =
                return_if_io!(with_header(pager, mv_store, program, |header| header.page_size));
            let conn = program.connection.clone();
            let io = conn.pager.read().io.clone();
            let rand_num = io.generate_random_number();
            let db_file;
            let db_file_io: Arc<dyn crate::IO>;

            // we support OPFS in WASM - but it require files to be pre-opened in the browser before use
            // we can fix this if we will make open_file interface async
            // but for now for simplicity we use MemoryIO for all intermediate calculations
            #[cfg(target_family = "wasm")]
            {
                use crate::MemoryIO;

                db_file_io = Arc::new(MemoryIO::new());
                let file = db_file_io.open_file("temp-file", OpenFlags::Create, false)?;
                db_file = Arc::new(DatabaseFile::new(file));
            }
            #[cfg(not(target_family = "wasm"))]
            {
                let temp_dir = temp_dir();
                let rand_path =
                    std::path::Path::new(&temp_dir).join(format!("tursodb-ephemeral-{rand_num}"));
                let Some(rand_path_str) = rand_path.to_str() else {
                    return Err(LimboError::InternalError(
                        "Failed to convert path to string".to_string(),
                    ));
                };
                let file = io.open_file(rand_path_str, OpenFlags::Create, false)?;
                db_file = Arc::new(DatabaseFile::new(file));
                db_file_io = io;
            }

            let buffer_pool = program.connection.db.buffer_pool.clone();
            let page_cache = Arc::new(RwLock::new(PageCache::default()));

            let pager = Arc::new(Pager::new(
                db_file,
                None,
                db_file_io,
                page_cache,
                buffer_pool.clone(),
                Arc::new(AtomicDbState::new(DbState::Uninitialized)),
                Arc::new(Mutex::new(())),
            )?);

            pager.set_page_size(page_size);

            state.op_open_ephemeral_state = OpOpenEphemeralState::StartingTxn { pager };
        }
        OpOpenEphemeralState::StartingTxn { pager } => {
            tracing::trace!("StartingTxn");
            pager
                .begin_read_tx() // we have to begin a read tx before beginning a write
                .expect("Failed to start read transaction");
            return_if_io!(pager.begin_write_tx());
            state.op_open_ephemeral_state = OpOpenEphemeralState::CreateBtree {
                pager: pager.clone(),
            };
        }
        OpOpenEphemeralState::CreateBtree { pager } => {
            tracing::trace!("CreateBtree");
            // FIXME: handle page cache is full
            let flag = if is_table {
                &CreateBTreeFlags::new_table()
            } else {
                &CreateBTreeFlags::new_index()
            };
            let root_page = return_if_io!(pager.btree_create(flag));

            let (_, cursor_type) = program.cursor_ref.get(cursor_id).unwrap();
            let mv_cursor = if let Some(tx_id) = program.connection.get_mv_tx_id() {
                let table_id = root_page as u64;
                let mv_store = mv_store.unwrap().clone();
                let mv_cursor = Arc::new(RwLock::new(
                    MvCursor::new(mv_store.clone(), tx_id, table_id, pager.clone()).unwrap(),
                ));
                Some(mv_cursor)
            } else {
                None
            };

            let num_columns = match cursor_type {
                CursorType::BTreeTable(table_rc) => table_rc.columns.len(),
                CursorType::BTreeIndex(index_arc) => index_arc.columns.len(),
                _ => unreachable!("This should not have happened"),
            };

            let cursor = if let CursorType::BTreeIndex(index) = cursor_type {
                BTreeCursor::new_index(
                    mv_cursor,
                    pager.clone(),
                    root_page as usize,
                    index,
                    num_columns,
                )
            } else {
                BTreeCursor::new_table(mv_cursor, pager.clone(), root_page as usize, num_columns)
            };
            state.op_open_ephemeral_state = OpOpenEphemeralState::Rewind {
                cursor: Box::new(cursor),
            };
        }
        OpOpenEphemeralState::Rewind { cursor } => {
            return_if_io!(cursor.rewind());

            let cursors = &mut state.cursors;

            let (_, cursor_type) = program.cursor_ref.get(cursor_id).unwrap();

            let OpOpenEphemeralState::Rewind { cursor } =
                std::mem::take(&mut state.op_open_ephemeral_state)
            else {
                unreachable!()
            };

            // Table content is erased if the cursor already exists
            match cursor_type {
                CursorType::BTreeTable(_) => {
                    cursors
                        .get_mut(cursor_id)
                        .unwrap()
                        .replace(Cursor::new_btree(*cursor));
                }
                CursorType::BTreeIndex(_) => {
                    cursors
                        .get_mut(cursor_id)
                        .unwrap()
                        .replace(Cursor::new_btree(*cursor));
                }
                CursorType::Pseudo(_) => {
                    panic!("OpenEphemeral on pseudo cursor");
                }
                CursorType::Sorter => {
                    panic!("OpenEphemeral on sorter cursor");
                }
                CursorType::VirtualTable(_) => {
                    panic!("OpenEphemeral on virtual table cursor, use Insn::VOpen instead");
                }
                CursorType::MaterializedView(_, _) => {
                    panic!("OpenEphemeral on materialized view cursor");
                }
            }

            state.pc += 1;
            state.op_open_ephemeral_state = OpOpenEphemeralState::Start;
        }
    }

    Ok(InsnFunctionStepResult::Step)
}

pub fn op_open_dup(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        OpenDup {
            new_cursor_id,
            original_cursor_id,
        },
        insn
    );

    let original_cursor = state.get_cursor(*original_cursor_id);
    let original_cursor = original_cursor.as_btree_mut();

    let root_page = original_cursor.root_page();
    // We use the pager from the original cursor instead of the one attached to
    // the connection because each ephemeral table creates its own pager (and
    // a separate database file).
    let pager = &original_cursor.pager;

    let mv_cursor = if let Some(tx_id) = program.connection.get_mv_tx_id() {
        let table_id = root_page as u64;
        let mv_store = mv_store.unwrap().clone();
        let mv_cursor = Arc::new(RwLock::new(MvCursor::new(
            mv_store,
            tx_id,
            table_id,
            pager.clone(),
        )?));
        Some(mv_cursor)
    } else {
        None
    };

    let (_, cursor_type) = program.cursor_ref.get(*original_cursor_id).unwrap();
    match cursor_type {
        CursorType::BTreeTable(table) => {
            let cursor =
                BTreeCursor::new_table(mv_cursor, pager.clone(), root_page, table.columns.len());
            let cursors = &mut state.cursors;
            cursors
                .get_mut(*new_cursor_id)
                .unwrap()
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::BTreeIndex(table) => {
            // In principle, we could implement OpenDup for BTreeIndex,
            // but doing so now would create dead code since we have no use case,
            // and it wouldn't be possible to test it.
            unimplemented!("OpenDup is not supported for BTreeIndex yet")
        }
        _ => panic!("OpenDup is not supported for {cursor_type:?}"),
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Execute the [Insn::Once] instruction.
///
/// This instruction is used to execute a block of code only once.
/// If the instruction is executed again, it will jump to the target program counter.
pub fn op_once(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Once {
            target_pc_when_reentered,
        },
        insn
    );
    assert!(target_pc_when_reentered.is_offset());
    let offset = state.pc;
    if state.once.iter().any(|o| o == offset) {
        state.pc = target_pc_when_reentered.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    }
    state.once.push(offset);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_found(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let (cursor_id, target_pc, record_reg, num_regs) = match insn {
        Insn::NotFound {
            cursor_id,
            target_pc,
            record_reg,
            num_regs,
        } => (cursor_id, target_pc, record_reg, num_regs),
        Insn::Found {
            cursor_id,
            target_pc,
            record_reg,
            num_regs,
        } => (cursor_id, target_pc, record_reg, num_regs),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };

    let not = matches!(insn, Insn::NotFound { .. });

    let record_source = if *num_regs == 0 {
        RecordSource::Packed {
            record_reg: *record_reg,
        }
    } else {
        RecordSource::Unpacked {
            start_reg: *record_reg,
            num_regs: *num_regs,
        }
    };
    let seek_result = match seek_internal(
        program,
        state,
        pager,
        mv_store,
        record_source,
        *cursor_id,
        true,
        SeekOp::GE { eq_only: true },
    ) {
        Ok(SeekInternalResult::Found) => SeekResult::Found,
        Ok(SeekInternalResult::NotFound) => SeekResult::NotFound,
        Ok(SeekInternalResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
        Err(e) => return Err(e),
    };

    let found = matches!(seek_result, SeekResult::Found);
    let do_jump = (!found && not) || (found && !not);
    if do_jump {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }

    Ok(InsnFunctionStepResult::Step)
}

pub fn op_affinity(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Affinity {
            start_reg,
            count,
            affinities,
        },
        insn
    );

    if affinities.len() != count.get() {
        return Err(LimboError::InternalError(
            "Affinity: the length of affinities does not match the count".into(),
        ));
    }

    for (i, affinity_char) in affinities.chars().enumerate().take(count.get()) {
        let reg_index = *start_reg + i;

        let affinity = Affinity::from_char(affinity_char);

        apply_affinity_char(&mut state.registers[reg_index], affinity);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_count(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Count {
            cursor_id,
            target_reg,
            exact,
        },
        insn
    );

    let count = {
        let cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Count");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.count())
    };

    state.registers[*target_reg] = Register::Value(Value::Integer(count as i64));

    // For optimized COUNT(*) queries, the count represents rows that would be read
    // SQLite tracks this differently (as pages read), but for consistency we track as rows
    if *exact {
        state.metrics.rows_read = state.metrics.rows_read.saturating_add(count as u64);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Debug)]
pub enum OpIntegrityCheckState {
    Start,
    Checking {
        errors: Vec<IntegrityCheckError>,
        current_root_idx: usize,
        state: IntegrityCheckState,
    },
}
pub fn op_integrity_check(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IntegrityCk {
            max_errors,
            roots,
            message_register,
        },
        insn
    );
    match &mut state.op_integrity_check_state {
        OpIntegrityCheckState::Start => {
            let freelist_trunk_page =
                return_if_io!(with_header(pager, mv_store, program, |header| header
                    .freelist_trunk_page
                    .get()));
            let mut errors = Vec::new();
            let mut integrity_check_state = IntegrityCheckState::new();
            let mut current_root_idx = 0;
            // check freelist pages first, if there are any for database
            if freelist_trunk_page > 0 {
                let expected_freelist_count =
                    return_if_io!(with_header(pager, mv_store, program, |header| header
                        .freelist_pages
                        .get()));
                integrity_check_state.set_expected_freelist_count(expected_freelist_count as usize);
                integrity_check_state.start(
                    freelist_trunk_page as usize,
                    PageCategory::FreeListTrunk,
                    &mut errors,
                );
            } else {
                integrity_check_state.start(roots[0], PageCategory::Normal, &mut errors);
                current_root_idx += 1;
            }
            state.op_integrity_check_state = OpIntegrityCheckState::Checking {
                errors,
                state: integrity_check_state,
                current_root_idx,
            };
        }
        OpIntegrityCheckState::Checking {
            errors,
            current_root_idx,
            state: integrity_check_state,
        } => {
            return_if_io!(integrity_check(integrity_check_state, errors, pager));
            if *current_root_idx < roots.len() {
                integrity_check_state.start(roots[*current_root_idx], PageCategory::Normal, errors);
                *current_root_idx += 1;
                return Ok(InsnFunctionStepResult::Step);
            } else {
                if integrity_check_state.freelist_count.actual_count
                    != integrity_check_state.freelist_count.expected_count
                {
                    errors.push(IntegrityCheckError::FreelistCountMismatch {
                        actual_count: integrity_check_state.freelist_count.actual_count,
                        expected_count: integrity_check_state.freelist_count.expected_count,
                    });
                }
                let message = if errors.is_empty() {
                    "ok".to_string()
                } else {
                    errors
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join("\n")
                };
                state.registers[*message_register] = Register::Value(Value::build_text(message));
                state.op_integrity_check_state = OpIntegrityCheckState::Start;
                state.pc += 1;
            }
        }
    }

    Ok(InsnFunctionStepResult::Step)
}

pub fn op_cast(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
    _mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Cast { reg, affinity }, insn);

    let value = state.registers[*reg].get_value().clone();
    let result = match affinity {
        Affinity::Blob => value.exec_cast("BLOB"),
        Affinity::Text => value.exec_cast("TEXT"),
        Affinity::Numeric => value.exec_cast("NUMERIC"),
        Affinity::Integer => value.exec_cast("INTEGER"),
        Affinity::Real => value.exec_cast("REAL"),
    };

    state.registers[*reg] = Register::Value(result);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_rename_table(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(RenameTable { from, to }, insn);

    let normalized_from = normalize_ident(from.as_str());
    let normalized_to = normalize_ident(to.as_str());

    let conn = program.connection.clone();

    conn.with_schema_mut(|schema| {
        if let Some(mut indexes) = schema.indexes.remove(&normalized_from) {
            indexes.iter_mut().for_each(|index| {
                let index = Arc::make_mut(index);
                index.table_name = normalized_to.to_owned();
            });

            schema.indexes.insert(normalized_to.to_owned(), indexes);
        };

        let mut table = schema
            .tables
            .remove(&normalized_from)
            .expect("table being renamed should be in schema");

        {
            let table = Arc::make_mut(&mut table);

            let Table::BTree(btree) = table else {
                panic!("only btree tables can be renamed");
            };

            let btree = Arc::make_mut(btree);
            btree.name = normalized_to.to_owned();
        }

        schema.tables.insert(normalized_to.to_owned(), table);
    });

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        DropColumn {
            table,
            column_index
        },
        insn
    );

    let conn = program.connection.clone();

    conn.with_schema_mut(|schema| {
        let table = schema
            .tables
            .get_mut(table)
            .expect("table being renamed should be in schema");

        let table = Arc::make_mut(table);

        let Table::BTree(btree) = table else {
            panic!("only btree tables can be renamed");
        };

        let btree = Arc::make_mut(btree);
        btree.columns.remove(*column_index)
    });

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(AddColumn { table, column }, insn);

    let conn = program.connection.clone();

    conn.with_schema_mut(|schema| {
        let table = schema
            .tables
            .get_mut(table)
            .expect("table being renamed should be in schema");

        let table = Arc::make_mut(table);

        let Table::BTree(btree) = table else {
            panic!("only btree tables can be renamed");
        };

        let btree = Arc::make_mut(btree);
        btree.columns.push(column.clone())
    });

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_alter_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        AlterColumn {
            table: table_name,
            column_index,
            definition,
            rename,
        },
        insn
    );

    let conn = program.connection.clone();

    let normalized_table_name = normalize_ident(table_name.as_str());
    let new_column = crate::schema::Column::from(definition);

    conn.with_schema_mut(|schema| {
        let table = schema
            .tables
            .get_mut(&normalized_table_name)
            .expect("table being renamed should be in schema");

        let table = Arc::make_mut(table);

        let Table::BTree(btree) = table else {
            panic!("only btree tables can be renamed");
        };

        let btree = Arc::make_mut(btree);

        let column = btree
            .columns
            .get_mut(*column_index)
            .expect("renamed column should be in schema");

        if let Some(indexes) = schema.indexes.get_mut(&normalized_table_name) {
            for index in indexes {
                let index = Arc::make_mut(index);
                for index_column in &mut index.columns {
                    if index_column.name
                        == *column.name.as_ref().expect("btree column should be named")
                    {
                        index_column.name = definition.col_name.as_str().to_owned();
                    }
                }
            }
        }

        if *rename {
            column.name = new_column.name;
        } else {
            *column = new_column;
        }
    });

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if_neg(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(IfNeg { reg, target_pc }, insn);

    match &state.registers[*reg] {
        Register::Value(Value::Integer(i)) if *i < 0 => {
            state.pc = target_pc.as_offset_int();
        }
        Register::Value(Value::Float(f)) if *f < 0.0 => {
            state.pc = target_pc.as_offset_int();
        }
        Register::Value(Value::Null) => {
            state.pc += 1;
        }
        _ => {
            state.pc += 1;
        }
    }

    Ok(InsnFunctionStepResult::Step)
}

mod cmath {
    extern "C" {
        pub fn exp(x: f64) -> f64;
        pub fn log(x: f64) -> f64;
        pub fn log10(x: f64) -> f64;
        pub fn log2(x: f64) -> f64;
        pub fn pow(x: f64, y: f64) -> f64;

        pub fn sin(x: f64) -> f64;
        pub fn sinh(x: f64) -> f64;
        pub fn asin(x: f64) -> f64;
        pub fn asinh(x: f64) -> f64;

        pub fn cos(x: f64) -> f64;
        pub fn cosh(x: f64) -> f64;
        pub fn acos(x: f64) -> f64;
        pub fn acosh(x: f64) -> f64;

        pub fn tan(x: f64) -> f64;
        pub fn tanh(x: f64) -> f64;
        pub fn atan(x: f64) -> f64;
        pub fn atanh(x: f64) -> f64;
        pub fn atan2(x: f64, y: f64) -> f64;
    }
}

impl Value {
    pub fn exec_lower(&self) -> Option<Self> {
        self.cast_text()
            .map(|s| Value::build_text(s.to_ascii_lowercase()))
    }

    pub fn exec_length(&self) -> Self {
        match self {
            Value::Text(t) => {
                // Count Unicode scalar values (characters)
                Value::Integer(t.as_str().chars().count() as i64)
            }
            Value::Integer(_) | Value::Float(_) => {
                // For numbers, SQLite returns the length of the string representation
                Value::Integer(self.to_string().chars().count() as i64)
            }
            Value::Blob(blob) => Value::Integer(blob.len() as i64),
            _ => self.to_owned(),
        }
    }

    pub fn exec_octet_length(&self) -> Self {
        match self {
            Value::Text(_) | Value::Integer(_) | Value::Float(_) => {
                Value::Integer(self.to_string().into_bytes().len() as i64)
            }
            Value::Blob(blob) => Value::Integer(blob.len() as i64),
            _ => self.to_owned(),
        }
    }

    pub fn exec_upper(&self) -> Option<Self> {
        self.cast_text()
            .map(|s| Value::build_text(s.to_ascii_uppercase()))
    }

    pub fn exec_sign(&self) -> Option<Value> {
        let v = Numeric::from_value_strict(self).try_into_f64()?;

        Some(Value::Integer(if v > 0.0 {
            1
        } else if v < 0.0 {
            -1
        } else {
            0
        }))
    }

    /// Generates the Soundex code for a given word
    pub fn exec_soundex(&self) -> Value {
        let s = match self {
            Value::Null => return Value::build_text("?000"),
            Value::Text(s) => {
                // return ?000 if non ASCII alphabet character is found
                if !s.as_str().chars().all(|c| c.is_ascii_alphabetic()) {
                    return Value::build_text("?000");
                }
                s.clone()
            }
            _ => return Value::build_text("?000"), // For unsupported types, return NULL
        };

        // Remove numbers and spaces
        let word: String = s
            .as_str()
            .chars()
            .filter(|c| !c.is_ascii_digit())
            .collect::<String>()
            .replace(" ", "");
        if word.is_empty() {
            return Value::build_text("0000");
        }

        let soundex_code = |c| match c {
            'b' | 'f' | 'p' | 'v' => Some('1'),
            'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
            'd' | 't' => Some('3'),
            'l' => Some('4'),
            'm' | 'n' => Some('5'),
            'r' => Some('6'),
            _ => None,
        };

        // Convert the word to lowercase for consistent lookups
        let word = word.to_lowercase();
        let first_letter = word.chars().next().unwrap();

        // Remove all occurrences of 'h' and 'w' except the first letter
        let code: String = word
            .chars()
            .skip(1)
            .filter(|&ch| ch != 'h' && ch != 'w')
            .fold(first_letter.to_string(), |mut acc, ch| {
                acc.push(ch);
                acc
            });

        // Replace consonants with digits based on Soundex mapping
        let tmp: String = code
            .chars()
            .map(|ch| match soundex_code(ch) {
                Some(code) => code.to_string(),
                None => ch.to_string(),
            })
            .collect();

        // Remove adjacent same digits
        let tmp = tmp.chars().fold(String::new(), |mut acc, ch| {
            if !acc.ends_with(ch) {
                acc.push(ch);
            }
            acc
        });

        // Remove all occurrences of a, e, i, o, u, y except the first letter
        let mut result = tmp
            .chars()
            .enumerate()
            .filter(|(i, ch)| *i == 0 || !matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u' | 'y'))
            .map(|(_, ch)| ch)
            .collect::<String>();

        // If the first symbol is a digit, replace it with the saved first letter
        if let Some(first_digit) = result.chars().next() {
            if first_digit.is_ascii_digit() {
                result.replace_range(0..1, &first_letter.to_string());
            }
        }

        // Append zeros if the result contains less than 4 characters
        while result.len() < 4 {
            result.push('0');
        }

        // Retain the first 4 characters and convert to uppercase
        result.truncate(4);
        Value::build_text(result.to_uppercase())
    }

    pub fn exec_abs(&self) -> Result<Self> {
        Ok(match self {
            Value::Null => Value::Null,
            Value::Integer(v) => {
                Value::Integer(v.checked_abs().ok_or(LimboError::IntegerOverflow)?)
            }
            Value::Float(non_nan) => Value::Float(non_nan.abs()),
            _ => {
                let s = match self {
                    Value::Text(text) => text.to_string(),
                    Value::Blob(blob) => String::from_utf8_lossy(blob).to_string(),
                    _ => unreachable!(),
                };

                crate::numeric::str_to_f64(s)
                    .map(|v| Value::Float(f64::from(v).abs()))
                    .unwrap_or(Value::Float(0.0))
            }
        })
    }

    pub fn exec_random() -> Self {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        let random_number = i64::from_ne_bytes(buf);
        Value::Integer(random_number)
    }

    pub fn exec_randomblob(&self) -> Value {
        let length = match self {
            Value::Integer(i) => *i,
            Value::Float(f) => *f as i64,
            Value::Text(t) => t.as_str().parse().unwrap_or(1),
            _ => 1,
        }
        .max(1) as usize;

        let mut blob: Vec<u8> = vec![0; length];
        getrandom::getrandom(&mut blob).expect("Failed to generate random blob");
        Value::Blob(blob)
    }

    pub fn exec_quote(&self) -> Self {
        match self {
            Value::Null => Value::build_text("NULL"),
            Value::Integer(_) | Value::Float(_) => self.to_owned(),
            Value::Blob(_) => todo!(),
            Value::Text(s) => {
                let mut quoted = String::with_capacity(s.as_str().len() + 2);
                quoted.push('\'');
                for c in s.as_str().chars() {
                    if c == '\0' {
                        break;
                    } else if c == '\'' {
                        quoted.push('\'');
                        quoted.push(c);
                    } else {
                        quoted.push(c);
                    }
                }
                quoted.push('\'');
                Value::build_text(quoted)
            }
        }
    }

    pub fn exec_nullif(&self, second_value: &Self) -> Self {
        if self != second_value {
            self.clone()
        } else {
            Value::Null
        }
    }

    pub fn exec_substring(
        str_value: &Value,
        start_value: &Value,
        length_value: Option<&Value>,
    ) -> Value {
        if let (Value::Text(str), Value::Integer(start)) = (str_value, start_value) {
            let str_len = str.as_str().len() as i64;

            // The left-most character of X is number 1.
            // If Y is negative then the first character of the substring is found by counting from the right rather than the left.
            let first_position = if *start < 0 {
                str_len.saturating_sub((*start).abs())
            } else {
                *start - 1
            };
            // If Z is negative then the abs(Z) characters preceding the Y-th character are returned.
            let last_position = match length_value {
                Some(Value::Integer(length)) => first_position + *length,
                _ => str_len,
            };
            let (start, end) = if first_position <= last_position {
                (first_position, last_position)
            } else {
                (last_position, first_position)
            };
            Value::build_text(
                &str.as_str()[start.clamp(-0, str_len) as usize..end.clamp(0, str_len) as usize],
            )
        } else {
            Value::Null
        }
    }

    pub fn exec_instr(&self, pattern: &Value) -> Value {
        if self == &Value::Null || pattern == &Value::Null {
            return Value::Null;
        }

        if let (Value::Blob(reg), Value::Blob(pattern)) = (self, pattern) {
            let result = reg
                .windows(pattern.len())
                .position(|window| window == *pattern)
                .map_or(0, |i| i + 1);
            return Value::Integer(result as i64);
        }

        let reg_str;
        let reg = match self {
            Value::Text(s) => s.as_str(),
            _ => {
                reg_str = self.to_string();
                reg_str.as_str()
            }
        };

        let pattern_str;
        let pattern = match pattern {
            Value::Text(s) => s.as_str(),
            _ => {
                pattern_str = pattern.to_string();
                pattern_str.as_str()
            }
        };

        match reg.find(pattern) {
            Some(position) => Value::Integer(position as i64 + 1),
            None => Value::Integer(0),
        }
    }

    pub fn exec_typeof(&self) -> Value {
        match self {
            Value::Null => Value::build_text("null"),
            Value::Integer(_) => Value::build_text("integer"),
            Value::Float(_) => Value::build_text("real"),
            Value::Text(_) => Value::build_text("text"),
            Value::Blob(_) => Value::build_text("blob"),
        }
    }

    pub fn exec_hex(&self) -> Value {
        match self {
            Value::Text(_) | Value::Integer(_) | Value::Float(_) => {
                let text = self.to_string();
                Value::build_text(hex::encode_upper(text))
            }
            Value::Blob(blob_bytes) => Value::build_text(hex::encode_upper(blob_bytes)),
            Value::Null => Value::build_text(""),
        }
    }

    pub fn exec_unhex(&self, ignored_chars: Option<&Value>) -> Value {
        match self {
            Value::Null => Value::Null,
            _ => match ignored_chars {
                None => match self
                    .cast_text()
                    .map(|s| hex::decode(&s[0..s.find('\0').unwrap_or(s.len())]))
                {
                    Some(Ok(bytes)) => Value::Blob(bytes),
                    _ => Value::Null,
                },
                Some(ignore) => match ignore {
                    Value::Text(_) => {
                        let pat = ignore.to_string();
                        let trimmed = self
                            .to_string()
                            .trim_start_matches(|x| pat.contains(x))
                            .trim_end_matches(|x| pat.contains(x))
                            .to_string();
                        match hex::decode(trimmed) {
                            Ok(bytes) => Value::Blob(bytes),
                            _ => Value::Null,
                        }
                    }
                    _ => Value::Null,
                },
            },
        }
    }

    pub fn exec_unicode(&self) -> Value {
        match self {
            Value::Text(_) | Value::Integer(_) | Value::Float(_) | Value::Blob(_) => {
                let text = self.to_string();
                if let Some(first_char) = text.chars().next() {
                    Value::Integer(first_char as u32 as i64)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        }
    }

    pub fn exec_round(&self, precision: Option<&Value>) -> Value {
        let Some(f) = Numeric::from(self).try_into_f64() else {
            return Value::Null;
        };

        let precision = match precision.map(|v| Numeric::from(v).try_into_f64()) {
            None => 0.0,
            Some(Some(v)) => v,
            Some(None) => return Value::Null,
        };

        if !(-4503599627370496.0..=4503599627370496.0).contains(&f) {
            return Value::Float(f);
        }

        let precision = if precision < 1.0 { 0.0 } else { precision };
        let precision = precision.clamp(0.0, 30.0) as usize;

        if precision == 0 {
            return Value::Float(((f + if f < 0.0 { -0.5 } else { 0.5 }) as i64) as f64);
        }

        let f: f64 = crate::numeric::str_to_f64(format!("{f:.precision$}"))
            .unwrap()
            .into();

        Value::Float(f)
    }

    // Implements TRIM pattern matching.
    pub fn exec_trim(&self, pattern: Option<&Value>) -> Value {
        match (self, pattern) {
            (reg, Some(pattern)) => match reg {
                Value::Text(_) | Value::Integer(_) | Value::Float(_) => {
                    let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                    Value::build_text(reg.to_string().trim_matches(&pattern_chars[..]))
                }
                _ => reg.to_owned(),
            },
            (Value::Text(t), None) => Value::build_text(t.as_str().trim()),
            (reg, _) => reg.to_owned(),
        }
    }
    // Implements RTRIM pattern matching.
    pub fn exec_rtrim(&self, pattern: Option<&Value>) -> Value {
        match (self, pattern) {
            (reg, Some(pattern)) => match reg {
                Value::Text(_) | Value::Integer(_) | Value::Float(_) => {
                    let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                    Value::build_text(reg.to_string().trim_end_matches(&pattern_chars[..]))
                }
                _ => reg.to_owned(),
            },
            (Value::Text(t), None) => Value::build_text(t.as_str().trim_end()),
            (reg, _) => reg.to_owned(),
        }
    }

    // Implements LTRIM pattern matching.
    pub fn exec_ltrim(&self, pattern: Option<&Value>) -> Value {
        match (self, pattern) {
            (reg, Some(pattern)) => match reg {
                Value::Text(_) | Value::Integer(_) | Value::Float(_) => {
                    let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                    Value::build_text(reg.to_string().trim_start_matches(&pattern_chars[..]))
                }
                _ => reg.to_owned(),
            },
            (Value::Text(t), None) => Value::build_text(t.as_str().trim_start()),
            (reg, _) => reg.to_owned(),
        }
    }

    pub fn exec_zeroblob(&self) -> Value {
        let length: i64 = match self {
            Value::Integer(i) => *i,
            Value::Float(f) => *f as i64,
            Value::Text(s) => s.as_str().parse().unwrap_or(0),
            _ => 0,
        };
        Value::Blob(vec![0; length.max(0) as usize])
    }

    // exec_if returns whether you should jump
    pub fn exec_if(&self, jump_if_null: bool, not: bool) -> bool {
        Numeric::from(self)
            .try_into_bool()
            .map(|jump| if not { !jump } else { jump })
            .unwrap_or(jump_if_null)
    }

    pub fn exec_cast(&self, datatype: &str) -> Value {
        if matches!(self, Value::Null) {
            return Value::Null;
        }
        match affinity(datatype) {
            // NONE	Casting a value to a type-name with no affinity causes the value to be converted into a BLOB. Casting to a BLOB consists of first casting the value to TEXT in the encoding of the database connection, then interpreting the resulting byte sequence as a BLOB instead of as TEXT.
            // Historically called NONE, but it's the same as BLOB
            Affinity::Blob => {
                // Convert to TEXT first, then interpret as BLOB
                // TODO: handle encoding
                let text = self.to_string();
                Value::Blob(text.into_bytes())
            }
            // TEXT To cast a BLOB value to TEXT, the sequence of bytes that make up the BLOB is interpreted as text encoded using the database encoding.
            // Casting an INTEGER or REAL value into TEXT renders the value as if via sqlite3_snprintf() except that the resulting TEXT uses the encoding of the database connection.
            Affinity::Text => {
                // Convert everything to text representation
                // TODO: handle encoding and whatever sqlite3_snprintf does
                Value::build_text(self.to_string())
            }
            Affinity::Real => match self {
                Value::Blob(b) => {
                    let text = String::from_utf8_lossy(b);
                    Value::Float(
                        crate::numeric::str_to_f64(&text)
                            .map(f64::from)
                            .unwrap_or(0.0),
                    )
                }
                Value::Text(t) => {
                    Value::Float(crate::numeric::str_to_f64(t).map(f64::from).unwrap_or(0.0))
                }
                Value::Integer(i) => Value::Float(*i as f64),
                Value::Float(f) => Value::Float(*f),
                _ => Value::Float(0.0),
            },
            Affinity::Integer => match self {
                Value::Blob(b) => {
                    // Convert BLOB to TEXT first
                    let text = String::from_utf8_lossy(b);
                    Value::Integer(crate::numeric::str_to_i64(&text).unwrap_or(0))
                }
                Value::Text(t) => Value::Integer(crate::numeric::str_to_i64(t).unwrap_or(0)),
                Value::Integer(i) => Value::Integer(*i),
                // A cast of a REAL value into an INTEGER results in the integer between the REAL value and zero
                // that is closest to the REAL value. If a REAL is greater than the greatest possible signed integer (+9223372036854775807)
                // then the result is the greatest possible signed integer and if the REAL is less than the least possible signed integer (-9223372036854775808)
                // then the result is the least possible signed integer.
                Value::Float(f) => {
                    let i = f.trunc() as i128;
                    if i > i64::MAX as i128 {
                        Value::Integer(i64::MAX)
                    } else if i < i64::MIN as i128 {
                        Value::Integer(i64::MIN)
                    } else {
                        Value::Integer(i as i64)
                    }
                }
                _ => Value::Integer(0),
            },
            Affinity::Numeric => match self {
                Value::Null => Value::Null,
                Value::Integer(v) => Value::Integer(*v),
                Value::Float(v) => Self::Float(*v),
                _ => {
                    let s = match self {
                        Value::Text(text) => text.to_string(),
                        Value::Blob(blob) => String::from_utf8_lossy(blob.as_slice()).to_string(),
                        _ => unreachable!(),
                    };

                    match crate::numeric::str_to_f64(&s) {
                        Some(parsed) => {
                            let Some(int) = crate::numeric::str_to_i64(&s) else {
                                return Value::Integer(0);
                            };

                            if f64::from(parsed) == int as f64 {
                                return Value::Integer(int);
                            }

                            Value::Float(parsed.into())
                        }
                        None => Value::Integer(0),
                    }
                }
            },
        }
    }

    pub fn exec_replace(source: &Value, pattern: &Value, replacement: &Value) -> Value {
        // The replace(X,Y,Z) function returns a string formed by substituting string Z for every occurrence of
        // string Y in string X. The BINARY collating sequence is used for comparisons. If Y is an empty string
        // then return X unchanged. If Z is not initially a string, it is cast to a UTF-8 string prior to processing.

        // If any of the arguments is NULL, the result is NULL.
        if matches!(source, Value::Null)
            || matches!(pattern, Value::Null)
            || matches!(replacement, Value::Null)
        {
            return Value::Null;
        }

        let source = source.exec_cast("TEXT");
        let pattern = pattern.exec_cast("TEXT");
        let replacement = replacement.exec_cast("TEXT");

        // If any of the casts failed, panic as text casting is not expected to fail.
        match (&source, &pattern, &replacement) {
            (Value::Text(source), Value::Text(pattern), Value::Text(replacement)) => {
                if pattern.as_str().is_empty() {
                    return Value::Text(source.clone());
                }

                let result = source
                    .as_str()
                    .replace(pattern.as_str(), replacement.as_str());
                Value::build_text(result)
            }
            _ => unreachable!("text cast should never fail"),
        }
    }

    fn exec_math_unary(&self, function: &MathFunc) -> Value {
        let v = Numeric::from_value_strict(self);

        // In case of some functions and integer input, return the input as is
        if let Numeric::Integer(i) = v {
            if matches! { function, MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc }
            {
                return Value::Integer(i);
            }
        }

        let Some(f) = v.try_into_f64() else {
            return Value::Null;
        };

        if matches! { function, MathFunc::Ln | MathFunc::Log10 | MathFunc::Log2 } && f <= 0.0 {
            return Value::Null;
        }

        let result = match function {
            MathFunc::Acos => unsafe { cmath::acos(f) },
            MathFunc::Acosh => unsafe { cmath::acosh(f) },
            MathFunc::Asin => unsafe { cmath::asin(f) },
            MathFunc::Asinh => unsafe { cmath::asinh(f) },
            MathFunc::Atan => unsafe { cmath::atan(f) },
            MathFunc::Atanh => unsafe { cmath::atanh(f) },
            MathFunc::Ceil | MathFunc::Ceiling => libm::ceil(f),
            MathFunc::Cos => unsafe { cmath::cos(f) },
            MathFunc::Cosh => unsafe { cmath::cosh(f) },
            MathFunc::Degrees => f.to_degrees(),
            MathFunc::Exp => unsafe { cmath::exp(f) },
            MathFunc::Floor => libm::floor(f),
            MathFunc::Ln => unsafe { cmath::log(f) },
            MathFunc::Log10 => unsafe { cmath::log10(f) },
            MathFunc::Log2 => unsafe { cmath::log2(f) },
            MathFunc::Radians => f.to_radians(),
            MathFunc::Sin => unsafe { cmath::sin(f) },
            MathFunc::Sinh => unsafe { cmath::sinh(f) },
            MathFunc::Sqrt => libm::sqrt(f),
            MathFunc::Tan => unsafe { cmath::tan(f) },
            MathFunc::Tanh => unsafe { cmath::tanh(f) },
            MathFunc::Trunc => libm::trunc(f),
            _ => unreachable!("Unexpected mathematical unary function {:?}", function),
        };

        if result.is_nan() {
            Value::Null
        } else {
            Value::Float(result)
        }
    }

    fn exec_math_binary(&self, rhs: &Value, function: &MathFunc) -> Value {
        let Some(lhs) = Numeric::from_value_strict(self).try_into_f64() else {
            return Value::Null;
        };

        let Some(rhs) = Numeric::from_value_strict(rhs).try_into_f64() else {
            return Value::Null;
        };

        let result = match function {
            MathFunc::Atan2 => unsafe { cmath::atan2(lhs, rhs) },
            MathFunc::Mod => libm::fmod(lhs, rhs),
            MathFunc::Pow | MathFunc::Power => unsafe { cmath::pow(lhs, rhs) },
            _ => unreachable!("Unexpected mathematical binary function {:?}", function),
        };

        if result.is_nan() {
            Value::Null
        } else {
            Value::Float(result)
        }
    }

    fn exec_math_log(&self, base: Option<&Value>) -> Value {
        let Some(f) = Numeric::from_value_strict(self).try_into_f64() else {
            return Value::Null;
        };

        let base = match base.map(|value| Numeric::from_value_strict(value).try_into_f64()) {
            Some(Some(f)) => f,
            Some(None) => return Value::Null,
            None => 10.0,
        };

        if f <= 0.0 || base <= 0.0 || base == 1.0 {
            return Value::Null;
        }

        if base == 2.0 {
            return Value::Float(libm::log2(f));
        } else if base == 10.0 {
            return Value::Float(libm::log10(f));
        };

        let log_x = libm::log(f);
        let log_base = libm::log(base);

        if log_base <= 0.0 {
            return Value::Null;
        }

        let result = log_x / log_base;
        Value::Float(result)
    }

    pub fn exec_add(&self, rhs: &Value) -> Value {
        (Numeric::from(self) + Numeric::from(rhs)).into()
    }

    pub fn exec_subtract(&self, rhs: &Value) -> Value {
        (Numeric::from(self) - Numeric::from(rhs)).into()
    }

    pub fn exec_multiply(&self, rhs: &Value) -> Value {
        (Numeric::from(self) * Numeric::from(rhs)).into()
    }

    pub fn exec_divide(&self, rhs: &Value) -> Value {
        (Numeric::from(self) / Numeric::from(rhs)).into()
    }

    pub fn exec_bit_and(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) & NullableInteger::from(rhs)).into()
    }

    pub fn exec_bit_or(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) | NullableInteger::from(rhs)).into()
    }

    pub fn exec_remainder(&self, rhs: &Value) -> Value {
        let convert_to_float = matches!(Numeric::from(self), Numeric::Float(_))
            || matches!(Numeric::from(rhs), Numeric::Float(_));

        match NullableInteger::from(self) % NullableInteger::from(rhs) {
            NullableInteger::Null => Value::Null,
            NullableInteger::Integer(v) => {
                if convert_to_float {
                    Value::Float(v as f64)
                } else {
                    Value::Integer(v)
                }
            }
        }
    }

    pub fn exec_bit_not(&self) -> Value {
        (!NullableInteger::from(self)).into()
    }

    pub fn exec_shift_left(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) << NullableInteger::from(rhs)).into()
    }

    pub fn exec_shift_right(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) >> NullableInteger::from(rhs)).into()
    }

    pub fn exec_boolean_not(&self) -> Value {
        match Numeric::from(self).try_into_bool() {
            None => Value::Null,
            Some(v) => Value::Integer(!v as i64),
        }
    }

    pub fn exec_concat(&self, rhs: &Value) -> Value {
        if let (Value::Blob(lhs), Value::Blob(rhs)) = (self, rhs) {
            return Value::build_text(String::from_utf8_lossy(
                &[lhs.as_slice(), rhs.as_slice()].concat(),
            ));
        }

        let Some(lhs) = self.cast_text() else {
            return Value::Null;
        };

        let Some(rhs) = rhs.cast_text() else {
            return Value::Null;
        };

        Value::build_text(lhs + &rhs)
    }

    pub fn exec_and(&self, rhs: &Value) -> Value {
        match (
            Numeric::from(self).try_into_bool(),
            Numeric::from(rhs).try_into_bool(),
        ) {
            (Some(false), _) | (_, Some(false)) => Value::Integer(0),
            (None, _) | (_, None) => Value::Null,
            _ => Value::Integer(1),
        }
    }

    pub fn exec_or(&self, rhs: &Value) -> Value {
        match (
            Numeric::from(self).try_into_bool(),
            Numeric::from(rhs).try_into_bool(),
        ) {
            (Some(true), _) | (_, Some(true)) => Value::Integer(1),
            (None, _) | (_, None) => Value::Null,
            _ => Value::Integer(0),
        }
    }

    // Implements LIKE pattern matching. Caches the constructed regex if a cache is provided
    pub fn exec_like(
        regex_cache: Option<&mut HashMap<String, Regex>>,
        pattern: &str,
        text: &str,
    ) -> bool {
        if let Some(cache) = regex_cache {
            match cache.get(pattern) {
                Some(re) => re.is_match(text),
                None => {
                    let re = construct_like_regex(pattern);
                    let res = re.is_match(text);
                    cache.insert(pattern.to_string(), re);
                    res
                }
            }
        } else {
            let re = construct_like_regex(pattern);
            re.is_match(text)
        }
    }

    pub fn exec_min<'a, T: Iterator<Item = &'a Value>>(regs: T) -> Value {
        regs.min().map(|v| v.to_owned()).unwrap_or(Value::Null)
    }

    pub fn exec_max<'a, T: Iterator<Item = &'a Value>>(regs: T) -> Value {
        regs.max().map(|v| v.to_owned()).unwrap_or(Value::Null)
    }
}

fn exec_concat_strings(registers: &[Register]) -> Value {
    let mut result = String::new();
    for reg in registers {
        match reg.get_value() {
            Value::Null => continue,
            Value::Blob(_) => todo!("TODO concat blob"),
            v => result.push_str(&format!("{v}")),
        }
    }
    Value::build_text(result)
}

fn exec_concat_ws(registers: &[Register]) -> Value {
    if registers.is_empty() {
        return Value::Null;
    }

    let separator = match registers[0].get_value() {
        Value::Null | Value::Blob(_) => return Value::Null,
        v => format!("{v}"),
    };

    let parts = registers[1..]
        .iter()
        .filter_map(|reg| match reg.get_value() {
            Value::Text(_) | Value::Integer(_) | Value::Float(_) => {
                Some(format!("{}", reg.get_value()))
            }
            _ => None,
        });

    let result = parts.collect::<Vec<_>>().join(&separator);
    Value::build_text(result)
}

fn exec_char(values: &[Register]) -> Value {
    let result: String = values
        .iter()
        .filter_map(|x| {
            if let Value::Integer(i) = x.get_value() {
                Some(*i as u8 as char)
            } else {
                None
            }
        })
        .collect();
    Value::build_text(result)
}

fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    for c in pattern.chars() {
        match c {
            '\\' => regex_pattern.push_str("\\\\"),
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            ch => {
                if regex_syntax::is_meta_character(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(ch);
            }
        }
    }

    regex_pattern.push('$');

    RegexBuilder::new(&regex_pattern)
        .case_insensitive(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap()
}

fn apply_affinity_char(target: &mut Register, affinity: Affinity) -> bool {
    if let Register::Value(value) = target {
        if matches!(value, Value::Blob(_)) {
            return true;
        }

        match affinity {
            Affinity::Blob => return true,

            Affinity::Text => {
                if matches!(value, Value::Text(_) | Value::Null) {
                    return true;
                }
                let text = value.to_string();
                *value = Value::Text(text.into());
                return true;
            }

            Affinity::Integer | Affinity::Numeric => {
                if matches!(value, Value::Integer(_)) {
                    return true;
                }
                if !matches!(value, Value::Text(_) | Value::Float(_)) {
                    return true;
                }

                if let Value::Float(fl) = *value {
                    // For floats, try to convert to integer if it's exact
                    // This is similar to sqlite3VdbeIntegerAffinity
                    return try_float_to_integer_affinity(value, fl);
                }

                if let Value::Text(t) = value {
                    let text = t.as_str().trim();

                    // Handle hex numbers - they shouldn't be converted
                    if text.starts_with("0x") {
                        return false;
                    }

                    // For affinity conversion, only convert strings that are entirely numeric
                    let num = if let Ok(i) = text.parse::<i64>() {
                        Value::Integer(i)
                    } else if let Ok(f) = text.parse::<f64>() {
                        Value::Float(f)
                    } else {
                        return false;
                    };

                    match num {
                        Value::Integer(i) => {
                            *value = Value::Integer(i);
                            return true;
                        }
                        Value::Float(fl) => {
                            // For Numeric affinity, try to convert float to int if exact
                            if affinity == Affinity::Numeric {
                                return try_float_to_integer_affinity(value, fl);
                            } else {
                                *value = Value::Float(fl);
                                return true;
                            }
                        }
                        other => {
                            *value = other;
                            return true;
                        }
                    }
                }

                return false;
            }

            Affinity::Real => {
                if let Value::Integer(i) = *value {
                    *value = Value::Float(i as f64);
                    return true;
                }
                if let Value::Text(t) = value {
                    let s = t.as_str();
                    if s.starts_with("0x") {
                        return false;
                    }
                    if let Ok(num) = checked_cast_text_to_numeric(s) {
                        *value = num;
                        return true;
                    } else {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    true
}

fn try_float_to_integer_affinity(value: &mut Value, fl: f64) -> bool {
    // Check if the float can be exactly represented as an integer
    if let Ok(int_val) = cast_real_to_integer(fl) {
        // Additional check: ensure round-trip conversion is exact
        // and value is within safe bounds (similar to SQLite's checks)
        if (int_val as f64) == fl && int_val > i64::MIN + 1 && int_val < i64::MAX - 1 {
            *value = Value::Integer(int_val);
            return true;
        }
    }

    // If we can't convert to exact integer, keep as float for Numeric affinity
    // but return false to indicate the conversion wasn't "complete"
    *value = Value::Float(fl);
    false
}

fn execute_sqlite_version(version_integer: i64) -> String {
    let major = version_integer / 1_000_000;
    let minor = (version_integer % 1_000_000) / 1_000;
    let release = version_integer % 1_000;

    format!("{major}.{minor}.{release}")
}

pub fn extract_int_value(value: &Value) -> i64 {
    match value {
        Value::Integer(i) => *i,
        Value::Float(f) => {
            // Use sqlite3RealToI64 equivalent
            if *f < -9223372036854774784.0 {
                i64::MIN
            } else if *f > 9223372036854774784.0 {
                i64::MAX
            } else {
                *f as i64
            }
        }
        Value::Text(t) => {
            // Try to parse as integer, return 0 if failed
            t.as_str().parse::<i64>().unwrap_or(0)
        }
        Value::Blob(b) => {
            // Try to parse blob as string then as integer
            if let Ok(s) = std::str::from_utf8(b) {
                s.parse::<i64>().unwrap_or(0)
            } else {
                0
            }
        }
        Value::Null => 0,
    }
}

#[derive(Debug, PartialEq)]
enum NumericParseResult {
    NotNumeric,      // not a valid number
    PureInteger,     // pure integer (entire string)
    HasDecimalOrExp, // has decimal point or exponent (entire string)
    ValidPrefixOnly, // valid prefix but not entire string
}

#[derive(Debug)]
enum ParsedNumber {
    None,
    Integer(i64),
    Float(f64),
}

impl ParsedNumber {
    fn as_integer(&self) -> Option<i64> {
        match self {
            ParsedNumber::Integer(i) => Some(*i),
            _ => None,
        }
    }

    fn as_float(&self) -> Option<f64> {
        match self {
            ParsedNumber::Float(f) => Some(*f),
            _ => None,
        }
    }
}

fn try_for_float(text: &str) -> (NumericParseResult, ParsedNumber) {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return (NumericParseResult::NotNumeric, ParsedNumber::None);
    }

    let mut pos = 0;
    let len = bytes.len();

    while pos < len && is_space(bytes[pos]) {
        pos += 1;
    }

    if pos >= len {
        return (NumericParseResult::NotNumeric, ParsedNumber::None);
    }

    let start_pos = pos;

    let mut sign = 1i64;

    if bytes[pos] == b'-' {
        sign = -1;
        pos += 1;
    } else if bytes[pos] == b'+' {
        pos += 1;
    }

    if pos >= len {
        return (NumericParseResult::NotNumeric, ParsedNumber::None);
    }

    let mut significand = 0u64;
    let mut digit_count = 0;
    let mut decimal_adjust = 0i32;
    let mut has_digits = false;

    // Parse digits before decimal point
    while pos < len && bytes[pos].is_ascii_digit() {
        has_digits = true;
        let digit = (bytes[pos] - b'0') as u64;

        if significand <= (u64::MAX - 9) / 10 {
            significand = significand * 10 + digit;
            digit_count += 1;
        } else {
            // Skip overflow digits but adjust exponent
            decimal_adjust += 1;
        }
        pos += 1;
    }

    let mut has_decimal = false;
    let mut has_exponent = false;

    // Check for decimal point
    if pos < len && bytes[pos] == b'.' {
        has_decimal = true;
        pos += 1;

        // Parse fractional digits
        while pos < len && bytes[pos].is_ascii_digit() {
            has_digits = true;
            let digit = (bytes[pos] - b'0') as u64;

            if significand <= (u64::MAX - 9) / 10 {
                significand = significand * 10 + digit;
                digit_count += 1;
                decimal_adjust -= 1;
            }
            pos += 1;
        }
    }

    if !has_digits {
        return (NumericParseResult::NotNumeric, ParsedNumber::None);
    }

    // Check for exponent
    let mut exponent = 0i32;
    if pos < len && (bytes[pos] == b'e' || bytes[pos] == b'E') {
        has_exponent = true;
        pos += 1;

        if pos >= len {
            // Incomplete exponent, but we have valid digits before
            return create_result_from_significand(
                significand,
                sign,
                decimal_adjust,
                has_decimal,
                has_exponent,
                NumericParseResult::ValidPrefixOnly,
            );
        }

        let mut exp_sign = 1i32;
        if bytes[pos] == b'-' {
            exp_sign = -1;
            pos += 1;
        } else if bytes[pos] == b'+' {
            pos += 1;
        }

        if pos >= len || !bytes[pos].is_ascii_digit() {
            // Incomplete exponent
            return create_result_from_significand(
                significand,
                sign,
                decimal_adjust,
                has_decimal,
                false,
                NumericParseResult::ValidPrefixOnly,
            );
        }

        // Parse exponent digits
        while pos < len && bytes[pos].is_ascii_digit() {
            let digit = (bytes[pos] - b'0') as i32;
            if exponent < 10000 {
                exponent = exponent * 10 + digit;
            } else {
                exponent = 10000; // Cap at large value
            }
            pos += 1;
        }
        exponent *= exp_sign;
    }

    // Skip trailing whitespace
    while pos < len && is_space(bytes[pos]) {
        pos += 1;
    }

    // Determine if we consumed the entire string
    let consumed_all = pos >= len;
    let final_exponent = decimal_adjust + exponent;

    let parse_result = if !consumed_all {
        NumericParseResult::ValidPrefixOnly
    } else if has_decimal || has_exponent {
        NumericParseResult::HasDecimalOrExp
    } else {
        NumericParseResult::PureInteger
    };

    create_result_from_significand(
        significand,
        sign,
        final_exponent,
        has_decimal,
        has_exponent,
        parse_result,
    )
}

fn create_result_from_significand(
    significand: u64,
    sign: i64,
    exponent: i32,
    has_decimal: bool,
    has_exponent: bool,
    parse_result: NumericParseResult,
) -> (NumericParseResult, ParsedNumber) {
    if significand == 0 {
        match parse_result {
            NumericParseResult::PureInteger => {
                return (parse_result, ParsedNumber::Integer(0));
            }
            _ => {
                return (parse_result, ParsedNumber::Float(0.0));
            }
        }
    }

    // For pure integers without exponent, try to return as integer
    if !has_decimal && !has_exponent && exponent == 0 && significand <= i64::MAX as u64 {
        let signed_val = (significand as i64).wrapping_mul(sign);
        return (parse_result, ParsedNumber::Integer(signed_val));
    }

    // Convert to float
    let mut result = significand as f64;

    let mut exp = exponent;
    match exp.cmp(&0) {
        std::cmp::Ordering::Greater => {
            while exp >= 100 {
                result *= 1e100;
                exp -= 100;
            }
            while exp >= 10 {
                result *= 1e10;
                exp -= 10;
            }
            while exp >= 1 {
                result *= 10.0;
                exp -= 1;
            }
        }
        std::cmp::Ordering::Less => {
            while exp <= -100 {
                result *= 1e-100;
                exp += 100;
            }
            while exp <= -10 {
                result *= 1e-10;
                exp += 10;
            }
            while exp <= -1 {
                result *= 0.1;
                exp += 1;
            }
        }
        std::cmp::Ordering::Equal => {}
    }

    if sign < 0 {
        result = -result;
    }

    (parse_result, ParsedNumber::Float(result))
}

pub fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | b'\x0c')
}

fn real_to_i64(r: f64) -> i64 {
    if r < -9223372036854774784.0 {
        i64::MIN
    } else if r > 9223372036854774784.0 {
        i64::MAX
    } else {
        r as i64
    }
}

fn apply_integer_affinity(register: &mut Register) -> bool {
    let Register::Value(Value::Float(f)) = register else {
        return false;
    };

    let ix = real_to_i64(*f);

    // Only convert if round-trip is exact and not at extreme values
    if *f == (ix as f64) && ix > i64::MIN && ix < i64::MAX {
        *register = Register::Value(Value::Integer(ix));
        true
    } else {
        false
    }
}

/// Try to convert a value into a numeric representation if we can
/// do so without loss of information. In other words, if the string
/// looks like a number, convert it into a number. If it does not
/// look like a number, leave it alone.
pub fn apply_numeric_affinity(register: &mut Register, try_for_int: bool) -> bool {
    let Register::Value(Value::Text(text)) = register else {
        return false; // Only apply to text values
    };

    let text_str = text.as_str();
    let (parse_result, parsed_value) = try_for_float(text_str);

    // Only convert if we have a complete valid number (not just a prefix)
    match parse_result {
        NumericParseResult::NotNumeric | NumericParseResult::ValidPrefixOnly => {
            false // Leave as text
        }
        NumericParseResult::PureInteger => {
            if let Some(int_val) = parsed_value.as_integer() {
                *register = Register::Value(Value::Integer(int_val));
                true
            } else if let Some(float_val) = parsed_value.as_float() {
                *register = Register::Value(Value::Float(float_val));
                if try_for_int {
                    apply_integer_affinity(register);
                }
                true
            } else {
                false
            }
        }
        NumericParseResult::HasDecimalOrExp => {
            if let Some(float_val) = parsed_value.as_float() {
                *register = Register::Value(Value::Float(float_val));
                // If try_for_int is true, try to convert float to int if exact
                if try_for_int {
                    apply_integer_affinity(register);
                }
                true
            } else {
                false
            }
        }
    }
}

fn is_numeric_value(reg: &Register) -> bool {
    matches!(reg.get_value(), Value::Integer(_) | Value::Float(_))
}

fn stringify_register(reg: &mut Register) -> bool {
    match reg.get_value() {
        Value::Integer(i) => {
            *reg = Register::Value(Value::build_text(i.to_string()));
            true
        }
        Value::Float(f) => {
            *reg = Register::Value(Value::build_text(f.to_string()));
            true
        }
        Value::Text(_) | Value::Null | Value::Blob(_) => false,
    }
}

pub fn op_max_pgcnt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(MaxPgcnt { db, dest, new_max }, insn);

    if *db > 0 {
        return Err(LimboError::InternalError(
            "temp/attached databases not implemented yet".to_string(),
        ));
    }

    let result_value = if *new_max == 0 {
        // If new_max is 0, just return current maximum without changing it
        pager.get_max_page_count()
    } else {
        // Set new maximum page count (will be clamped to current database size)
        return_if_io!(pager.set_max_page_count(*new_max as u32))
    };

    state.registers[*dest] = Register::Value(Value::Integer(result_value.into()));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_journal_mode(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(JournalMode { db, dest, new_mode }, insn);
    if *db > 0 {
        return Err(LimboError::InternalError(
            "temp/attached databases not implemented yet".to_string(),
        ));
    }

    // Currently, Turso only supports WAL mode
    // If a new mode is specified, we validate it but always return "wal"
    if let Some(mode) = new_mode {
        let mode_bytes = mode.as_bytes();
        // Valid journal modes in SQLite are: delete, truncate, persist, memory, wal, off
        // We accept any valid mode but always use WAL
        match_ignore_ascii_case!(match mode_bytes {
            b"delete" | b"truncate" | b"persist" | b"memory" | b"wal" | b"off" => {
                // Mode is valid, but we stay in WAL mode
            }
            _ => {
                // Invalid journal mode
                return Err(LimboError::ParseError(format!(
                    "Unknown journal mode: {mode}"
                )));
            }
        })
    }

    // Always return "wal" as the current journal mode
    state.registers[*dest] = Register::Value(Value::build_text("wal"));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

fn with_header<T, F>(
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
    program: &Program,
    f: F,
) -> Result<IOResult<T>>
where
    F: Fn(&DatabaseHeader) -> T,
{
    if let Some(mv_store) = mv_store {
        let tx_id = program.connection.get_mv_tx_id();
        mv_store.with_header(f, tx_id.as_ref()).map(IOResult::Done)
    } else {
        pager.with_header(&f)
    }
}

fn with_header_mut<T, F>(
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
    program: &Program,
    f: F,
) -> Result<IOResult<T>>
where
    F: Fn(&mut DatabaseHeader) -> T,
{
    if let Some(mv_store) = mv_store {
        let tx_id = program.connection.get_mv_tx_id();
        mv_store
            .with_header_mut(f, tx_id.as_ref())
            .map(IOResult::Done)
    } else {
        pager.with_header_mut(&f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_apply_numeric_affinity_partial_numbers() {
        let mut reg = Register::Value(Value::Text("123abc".into()));
        assert!(!apply_numeric_affinity(&mut reg, false));
        assert!(matches!(reg, Register::Value(Value::Text(_))));

        let mut reg = Register::Value(Value::Text("-53093015420544-15062897".into()));
        assert!(!apply_numeric_affinity(&mut reg, false));
        assert!(matches!(reg, Register::Value(Value::Text(_))));

        let mut reg = Register::Value(Value::Text("123.45xyz".into()));
        assert!(!apply_numeric_affinity(&mut reg, false));
        assert!(matches!(reg, Register::Value(Value::Text(_))));
    }

    #[test]
    fn test_apply_numeric_affinity_complete_numbers() {
        let mut reg = Register::Value(Value::Text("123".into()));
        assert!(apply_numeric_affinity(&mut reg, false));
        assert_eq!(*reg.get_value(), Value::Integer(123));

        let mut reg = Register::Value(Value::Text("123.45".into()));
        assert!(apply_numeric_affinity(&mut reg, false));
        assert_eq!(*reg.get_value(), Value::Float(123.45));

        let mut reg = Register::Value(Value::Text("  -456  ".into()));
        assert!(apply_numeric_affinity(&mut reg, false));
        assert_eq!(*reg.get_value(), Value::Integer(-456));

        let mut reg = Register::Value(Value::Text("0".into()));
        assert!(apply_numeric_affinity(&mut reg, false));
        assert_eq!(*reg.get_value(), Value::Integer(0));
    }

    #[test]
    fn test_exec_add() {
        let inputs = vec![
            (Value::Integer(3), Value::Integer(1)),
            (Value::Float(3.0), Value::Float(1.0)),
            (Value::Float(3.0), Value::Integer(1)),
            (Value::Integer(3), Value::Float(1.0)),
            (Value::Null, Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Float(1.0)),
            (Value::Null, Value::Text("2".into())),
            (Value::Integer(1), Value::Null),
            (Value::Float(1.0), Value::Null),
            (Value::Text("1".into()), Value::Null),
            (Value::Text("1".into()), Value::Text("3".into())),
            (Value::Text("1.0".into()), Value::Text("3.0".into())),
            (Value::Text("1.0".into()), Value::Float(3.0)),
            (Value::Text("1.0".into()), Value::Integer(3)),
            (Value::Float(1.0), Value::Text("3.0".into())),
            (Value::Integer(1), Value::Text("3".into())),
        ];

        let outputs = [
            Value::Integer(4),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Integer(4),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Float(4.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_add(rhs),
                outputs[i],
                "Wrong ADD for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_subtract() {
        let inputs = vec![
            (Value::Integer(3), Value::Integer(1)),
            (Value::Float(3.0), Value::Float(1.0)),
            (Value::Float(3.0), Value::Integer(1)),
            (Value::Integer(3), Value::Float(1.0)),
            (Value::Null, Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Float(1.0)),
            (Value::Null, Value::Text("1".into())),
            (Value::Integer(1), Value::Null),
            (Value::Float(1.0), Value::Null),
            (Value::Text("4".into()), Value::Null),
            (Value::Text("1".into()), Value::Text("3".into())),
            (Value::Text("1.0".into()), Value::Text("3.0".into())),
            (Value::Text("1.0".into()), Value::Float(3.0)),
            (Value::Text("1.0".into()), Value::Integer(3)),
            (Value::Float(1.0), Value::Text("3.0".into())),
            (Value::Integer(1), Value::Text("3".into())),
        ];

        let outputs = [
            Value::Integer(2),
            Value::Float(2.0),
            Value::Float(2.0),
            Value::Float(2.0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Integer(-2),
            Value::Float(-2.0),
            Value::Float(-2.0),
            Value::Float(-2.0),
            Value::Float(-2.0),
            Value::Float(-2.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_subtract(rhs),
                outputs[i],
                "Wrong subtract for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_multiply() {
        let inputs = vec![
            (Value::Integer(3), Value::Integer(2)),
            (Value::Float(3.0), Value::Float(2.0)),
            (Value::Float(3.0), Value::Integer(2)),
            (Value::Integer(3), Value::Float(2.0)),
            (Value::Null, Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Float(1.0)),
            (Value::Null, Value::Text("1".into())),
            (Value::Integer(1), Value::Null),
            (Value::Float(1.0), Value::Null),
            (Value::Text("4".into()), Value::Null),
            (Value::Text("2".into()), Value::Text("3".into())),
            (Value::Text("2.0".into()), Value::Text("3.0".into())),
            (Value::Text("2.0".into()), Value::Float(3.0)),
            (Value::Text("2.0".into()), Value::Integer(3)),
            (Value::Float(2.0), Value::Text("3.0".into())),
            (Value::Integer(2), Value::Text("3.0".into())),
        ];

        let outputs = [
            Value::Integer(6),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Integer(6),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Float(6.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_multiply(rhs),
                outputs[i],
                "Wrong multiply for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_divide() {
        let inputs = vec![
            (Value::Integer(1), Value::Integer(0)),
            (Value::Float(1.0), Value::Float(0.0)),
            (Value::Integer(i64::MIN), Value::Integer(-1)),
            (Value::Float(6.0), Value::Float(2.0)),
            (Value::Float(6.0), Value::Integer(2)),
            (Value::Integer(6), Value::Integer(2)),
            (Value::Null, Value::Integer(2)),
            (Value::Integer(2), Value::Null),
            (Value::Null, Value::Null),
            (Value::Text("6".into()), Value::Text("2".into())),
            (Value::Text("6".into()), Value::Integer(2)),
        ];

        let outputs = [
            Value::Null,
            Value::Null,
            Value::Float(9.223372036854776e18),
            Value::Float(3.0),
            Value::Float(3.0),
            Value::Float(3.0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Float(3.0),
            Value::Float(3.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_divide(rhs),
                outputs[i],
                "Wrong divide for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_remainder() {
        let inputs = vec![
            (Value::Null, Value::Null),
            (Value::Null, Value::Float(1.0)),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Text("1".into())),
            (Value::Float(1.0), Value::Null),
            (Value::Integer(1), Value::Null),
            (Value::Integer(12), Value::Integer(0)),
            (Value::Float(12.0), Value::Float(0.0)),
            (Value::Float(12.0), Value::Integer(0)),
            (Value::Integer(12), Value::Float(0.0)),
            (Value::Integer(i64::MIN), Value::Integer(-1)),
            (Value::Integer(12), Value::Integer(3)),
            (Value::Float(12.0), Value::Float(3.0)),
            (Value::Float(12.0), Value::Integer(3)),
            (Value::Integer(12), Value::Float(3.0)),
            (Value::Integer(12), Value::Integer(-3)),
            (Value::Float(12.0), Value::Float(-3.0)),
            (Value::Float(12.0), Value::Integer(-3)),
            (Value::Integer(12), Value::Float(-3.0)),
            (Value::Text("12.0".into()), Value::Text("3.0".into())),
            (Value::Text("12.0".into()), Value::Float(3.0)),
            (Value::Float(12.0), Value::Text("3.0".into())),
        ];
        let outputs = vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Float(0.0),
            Value::Integer(0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Integer(0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );

        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_remainder(rhs),
                outputs[i],
                "Wrong remainder for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_and() {
        let inputs = vec![
            (Value::Integer(0), Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Null),
            (Value::Float(0.0), Value::Null),
            (Value::Integer(1), Value::Float(2.2)),
            (Value::Integer(0), Value::Text("string".into())),
            (Value::Integer(0), Value::Text("1".into())),
            (Value::Integer(1), Value::Text("1".into())),
        ];
        let outputs = [
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(1),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_and(rhs),
                outputs[i],
                "Wrong AND for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_or() {
        let inputs = vec![
            (Value::Integer(0), Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Null),
            (Value::Float(0.0), Value::Null),
            (Value::Integer(1), Value::Float(2.2)),
            (Value::Float(0.0), Value::Integer(0)),
            (Value::Integer(0), Value::Text("string".into())),
            (Value::Integer(0), Value::Text("1".into())),
            (Value::Integer(0), Value::Text("".into())),
        ];
        let outputs = [
            Value::Null,
            Value::Integer(1),
            Value::Null,
            Value::Null,
            Value::Integer(1),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_or(rhs),
                outputs[i],
                "Wrong OR for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    use crate::vdbe::{Bitfield, Register};

    use super::{exec_char, execute_sqlite_version};
    use std::collections::HashMap;

    #[test]
    fn test_length() {
        let input_str = Value::build_text("bob");
        let expected_len = Value::Integer(3);
        assert_eq!(input_str.exec_length(), expected_len);

        let input_integer = Value::Integer(123);
        let expected_len = Value::Integer(3);
        assert_eq!(input_integer.exec_length(), expected_len);

        let input_float = Value::Float(123.456);
        let expected_len = Value::Integer(7);
        assert_eq!(input_float.exec_length(), expected_len);

        let expected_blob = Value::Blob("example".as_bytes().to_vec());
        let expected_len = Value::Integer(7);
        assert_eq!(expected_blob.exec_length(), expected_len);
    }

    #[test]
    fn test_quote() {
        let input = Value::build_text("abc\0edf");
        let expected = Value::build_text("'abc'");
        assert_eq!(input.exec_quote(), expected);

        let input = Value::Integer(123);
        let expected = Value::Integer(123);
        assert_eq!(input.exec_quote(), expected);

        let input = Value::build_text("hello''world");
        let expected = Value::build_text("'hello''''world'");
        assert_eq!(input.exec_quote(), expected);
    }

    #[test]
    fn test_typeof() {
        let input = Value::Null;
        let expected: Value = Value::build_text("null");
        assert_eq!(input.exec_typeof(), expected);

        let input = Value::Integer(123);
        let expected: Value = Value::build_text("integer");
        assert_eq!(input.exec_typeof(), expected);

        let input = Value::Float(123.456);
        let expected: Value = Value::build_text("real");
        assert_eq!(input.exec_typeof(), expected);

        let input = Value::build_text("hello");
        let expected: Value = Value::build_text("text");
        assert_eq!(input.exec_typeof(), expected);

        let input = Value::Blob("limbo".as_bytes().to_vec());
        let expected: Value = Value::build_text("blob");
        assert_eq!(input.exec_typeof(), expected);
    }

    #[test]
    fn test_unicode() {
        assert_eq!(Value::build_text("a").exec_unicode(), Value::Integer(97));
        assert_eq!(
            Value::build_text("😊").exec_unicode(),
            Value::Integer(128522)
        );
        assert_eq!(Value::build_text("").exec_unicode(), Value::Null);
        assert_eq!(Value::Integer(23).exec_unicode(), Value::Integer(50));
        assert_eq!(Value::Integer(0).exec_unicode(), Value::Integer(48));
        assert_eq!(Value::Float(0.0).exec_unicode(), Value::Integer(48));
        assert_eq!(Value::Float(23.45).exec_unicode(), Value::Integer(50));
        assert_eq!(Value::Null.exec_unicode(), Value::Null);
        assert_eq!(
            Value::Blob("example".as_bytes().to_vec()).exec_unicode(),
            Value::Integer(101)
        );
    }

    #[test]
    fn test_min_max() {
        let input_int_vec = [
            Register::Value(Value::Integer(-1)),
            Register::Value(Value::Integer(10)),
        ];
        assert_eq!(
            Value::exec_min(input_int_vec.iter().map(|v| v.get_value())),
            Value::Integer(-1)
        );
        assert_eq!(
            Value::exec_max(input_int_vec.iter().map(|v| v.get_value())),
            Value::Integer(10)
        );

        let str1 = Register::Value(Value::build_text("A"));
        let str2 = Register::Value(Value::build_text("z"));
        let input_str_vec = [str2, str1.clone()];
        assert_eq!(
            Value::exec_min(input_str_vec.iter().map(|v| v.get_value())),
            Value::build_text("A")
        );
        assert_eq!(
            Value::exec_max(input_str_vec.iter().map(|v| v.get_value())),
            Value::build_text("z")
        );

        let input_null_vec = [Register::Value(Value::Null), Register::Value(Value::Null)];
        assert_eq!(
            Value::exec_min(input_null_vec.iter().map(|v| v.get_value())),
            Value::Null
        );
        assert_eq!(
            Value::exec_max(input_null_vec.iter().map(|v| v.get_value())),
            Value::Null
        );

        let input_mixed_vec = [Register::Value(Value::Integer(10)), str1];
        assert_eq!(
            Value::exec_min(input_mixed_vec.iter().map(|v| v.get_value())),
            Value::Integer(10)
        );
        assert_eq!(
            Value::exec_max(input_mixed_vec.iter().map(|v| v.get_value())),
            Value::build_text("A")
        );
    }

    #[test]
    fn test_trim() {
        let input_str = Value::build_text("     Bob and Alice     ");
        let expected_str = Value::build_text("Bob and Alice");
        assert_eq!(input_str.exec_trim(None), expected_str);

        let input_str = Value::build_text("     Bob and Alice     ");
        let pattern_str = Value::build_text("Bob and");
        let expected_str = Value::build_text("Alice");
        assert_eq!(input_str.exec_trim(Some(&pattern_str)), expected_str);
    }

    #[test]
    fn test_ltrim() {
        let input_str = Value::build_text("     Bob and Alice     ");
        let expected_str = Value::build_text("Bob and Alice     ");
        assert_eq!(input_str.exec_ltrim(None), expected_str);

        let input_str = Value::build_text("     Bob and Alice     ");
        let pattern_str = Value::build_text("Bob and");
        let expected_str = Value::build_text("Alice     ");
        assert_eq!(input_str.exec_ltrim(Some(&pattern_str)), expected_str);
    }

    #[test]
    fn test_rtrim() {
        let input_str = Value::build_text("     Bob and Alice     ");
        let expected_str = Value::build_text("     Bob and Alice");
        assert_eq!(input_str.exec_rtrim(None), expected_str);

        let input_str = Value::build_text("     Bob and Alice     ");
        let pattern_str = Value::build_text("Bob and");
        let expected_str = Value::build_text("     Bob and Alice");
        assert_eq!(input_str.exec_rtrim(Some(&pattern_str)), expected_str);

        let input_str = Value::build_text("     Bob and Alice     ");
        let pattern_str = Value::build_text("and Alice");
        let expected_str = Value::build_text("     Bob");
        assert_eq!(input_str.exec_rtrim(Some(&pattern_str)), expected_str);
    }

    #[test]
    fn test_soundex() {
        let input_str = Value::build_text("Pfister");
        let expected_str = Value::build_text("P236");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("husobee");
        let expected_str = Value::build_text("H210");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Tymczak");
        let expected_str = Value::build_text("T522");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Ashcraft");
        let expected_str = Value::build_text("A261");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Robert");
        let expected_str = Value::build_text("R163");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Rupert");
        let expected_str = Value::build_text("R163");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Rubin");
        let expected_str = Value::build_text("R150");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Kant");
        let expected_str = Value::build_text("K530");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Knuth");
        let expected_str = Value::build_text("K530");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("x");
        let expected_str = Value::build_text("X000");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("闪电五连鞭");
        let expected_str = Value::build_text("?000");
        assert_eq!(input_str.exec_soundex(), expected_str);
    }

    #[test]
    fn test_upper_case() {
        let input_str = Value::build_text("Limbo");
        let expected_str = Value::build_text("LIMBO");
        assert_eq!(input_str.exec_upper().unwrap(), expected_str);

        let input_int = Value::Integer(10);
        assert_eq!(input_int.exec_upper().unwrap(), Value::build_text("10"));
        assert_eq!(Value::Null.exec_upper(), None)
    }

    #[test]
    fn test_lower_case() {
        let input_str = Value::build_text("Limbo");
        let expected_str = Value::build_text("limbo");
        assert_eq!(input_str.exec_lower().unwrap(), expected_str);

        let input_int = Value::Integer(10);
        assert_eq!(input_int.exec_lower().unwrap(), Value::build_text("10"));
        assert_eq!(Value::Null.exec_lower(), None)
    }

    #[test]
    fn test_hex() {
        let input_str = Value::build_text("limbo");
        let expected_val = Value::build_text("6C696D626F");
        assert_eq!(input_str.exec_hex(), expected_val);

        let input_int = Value::Integer(100);
        let expected_val = Value::build_text("313030");
        assert_eq!(input_int.exec_hex(), expected_val);

        let input_float = Value::Float(12.34);
        let expected_val = Value::build_text("31322E3334");
        assert_eq!(input_float.exec_hex(), expected_val);

        let input_blob = Value::Blob(vec![0xff]);
        let expected_val = Value::build_text("FF");
        assert_eq!(input_blob.exec_hex(), expected_val);
    }

    #[test]
    fn test_unhex() {
        let input = Value::build_text("6f");
        let expected = Value::Blob(vec![0x6f]);
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::build_text("6f");
        let expected = Value::Blob(vec![0x6f]);
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::build_text("611");
        let expected = Value::Null;
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::build_text("");
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::build_text("61x");
        let expected = Value::Null;
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::Null;
        let expected = Value::Null;
        assert_eq!(input.exec_unhex(None), expected);
    }

    #[test]
    fn test_abs() {
        let int_positive_reg = Value::Integer(10);
        let int_negative_reg = Value::Integer(-10);
        assert_eq!(int_positive_reg.exec_abs().unwrap(), int_positive_reg);
        assert_eq!(int_negative_reg.exec_abs().unwrap(), int_positive_reg);

        let float_positive_reg = Value::Integer(10);
        let float_negative_reg = Value::Integer(-10);
        assert_eq!(float_positive_reg.exec_abs().unwrap(), float_positive_reg);
        assert_eq!(float_negative_reg.exec_abs().unwrap(), float_positive_reg);

        assert_eq!(
            Value::build_text("a").exec_abs().unwrap(),
            Value::Float(0.0)
        );
        assert_eq!(Value::Null.exec_abs().unwrap(), Value::Null);

        // ABS(i64::MIN) should return RuntimeError
        assert!(Value::Integer(i64::MIN).exec_abs().is_err());
    }

    #[test]
    fn test_char() {
        assert_eq!(
            exec_char(&[
                Register::Value(Value::Integer(108)),
                Register::Value(Value::Integer(105))
            ]),
            Value::build_text("li")
        );
        assert_eq!(exec_char(&[]), Value::build_text(""));
        assert_eq!(
            exec_char(&[Register::Value(Value::Null)]),
            Value::build_text("")
        );
        assert_eq!(
            exec_char(&[Register::Value(Value::build_text("a"))]),
            Value::build_text("")
        );
    }

    #[test]
    fn test_like_with_escape_or_regexmeta_chars() {
        assert!(Value::exec_like(None, r#"\%A"#, r#"\A"#));
        assert!(Value::exec_like(None, "%a%a", "aaaa"));
    }

    #[test]
    fn test_like_no_cache() {
        assert!(Value::exec_like(None, "a%", "aaaa"));
        assert!(Value::exec_like(None, "%a%a", "aaaa"));
        assert!(!Value::exec_like(None, "%a.a", "aaaa"));
        assert!(!Value::exec_like(None, "a.a%", "aaaa"));
        assert!(!Value::exec_like(None, "%a.ab", "aaaa"));
    }

    #[test]
    fn test_like_with_cache() {
        let mut cache = HashMap::new();
        assert!(Value::exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(Value::exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "%a.ab", "aaaa"));

        // again after values have been cached
        assert!(Value::exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(Value::exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "%a.ab", "aaaa"));
    }

    #[test]
    fn test_random() {
        match Value::exec_random() {
            Value::Integer(value) => {
                // Check that the value is within the range of i64
                assert!(
                    (i64::MIN..=i64::MAX).contains(&value),
                    "Random number out of range"
                );
            }
            _ => panic!("exec_random did not return an Integer variant"),
        }
    }

    #[test]
    fn test_exec_randomblob() {
        struct TestCase {
            input: Value,
            expected_len: usize,
        }

        let test_cases = vec![
            TestCase {
                input: Value::Integer(5),
                expected_len: 5,
            },
            TestCase {
                input: Value::Integer(0),
                expected_len: 1,
            },
            TestCase {
                input: Value::Integer(-1),
                expected_len: 1,
            },
            TestCase {
                input: Value::build_text(""),
                expected_len: 1,
            },
            TestCase {
                input: Value::build_text("5"),
                expected_len: 5,
            },
            TestCase {
                input: Value::build_text("0"),
                expected_len: 1,
            },
            TestCase {
                input: Value::build_text("-1"),
                expected_len: 1,
            },
            TestCase {
                input: Value::Float(2.9),
                expected_len: 2,
            },
            TestCase {
                input: Value::Float(-3.15),
                expected_len: 1,
            },
            TestCase {
                input: Value::Null,
                expected_len: 1,
            },
        ];

        for test_case in &test_cases {
            let result = test_case.input.exec_randomblob();
            match result {
                Value::Blob(blob) => {
                    assert_eq!(blob.len(), test_case.expected_len);
                }
                _ => panic!("exec_randomblob did not return a Blob variant"),
            }
        }
    }

    #[test]
    fn test_exec_round() {
        let input_val = Value::Float(123.456);
        let expected_val = Value::Float(123.0);
        assert_eq!(input_val.exec_round(None), expected_val);

        let input_val = Value::Float(123.456);
        let precision_val = Value::Integer(2);
        let expected_val = Value::Float(123.46);
        assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

        let input_val = Value::Float(123.456);
        let precision_val = Value::build_text("1");
        let expected_val = Value::Float(123.5);
        assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

        let input_val = Value::build_text("123.456");
        let precision_val = Value::Integer(2);
        let expected_val = Value::Float(123.46);
        assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

        let input_val = Value::Integer(123);
        let precision_val = Value::Integer(1);
        let expected_val = Value::Float(123.0);
        assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

        let input_val = Value::Float(100.123);
        let expected_val = Value::Float(100.0);
        assert_eq!(input_val.exec_round(None), expected_val);

        let input_val = Value::Float(100.123);
        let expected_val = Value::Null;
        assert_eq!(input_val.exec_round(Some(&Value::Null)), expected_val);
    }

    #[test]
    fn test_exec_if() {
        let reg = Value::Integer(0);
        assert!(!reg.exec_if(false, false));
        assert!(reg.exec_if(false, true));

        let reg = Value::Integer(1);
        assert!(reg.exec_if(false, false));
        assert!(!reg.exec_if(false, true));

        let reg = Value::Null;
        assert!(!reg.exec_if(false, false));
        assert!(!reg.exec_if(false, true));

        let reg = Value::Null;
        assert!(reg.exec_if(true, false));
        assert!(reg.exec_if(true, true));

        let reg = Value::Null;
        assert!(!reg.exec_if(false, false));
        assert!(!reg.exec_if(false, true));
    }

    #[test]
    fn test_nullif() {
        assert_eq!(
            Value::Integer(1).exec_nullif(&Value::Integer(1)),
            Value::Null
        );
        assert_eq!(
            Value::Float(1.1).exec_nullif(&Value::Float(1.1)),
            Value::Null
        );
        assert_eq!(
            Value::build_text("limbo").exec_nullif(&Value::build_text("limbo")),
            Value::Null
        );

        assert_eq!(
            Value::Integer(1).exec_nullif(&Value::Integer(2)),
            Value::Integer(1)
        );
        assert_eq!(
            Value::Float(1.1).exec_nullif(&Value::Float(1.2)),
            Value::Float(1.1)
        );
        assert_eq!(
            Value::build_text("limbo").exec_nullif(&Value::build_text("limb")),
            Value::build_text("limbo")
        );
    }

    #[test]
    fn test_substring() {
        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(1);
        let length_value = Value::Integer(3);
        let expected_val = Value::build_text("lim");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(1);
        let length_value = Value::Integer(10);
        let expected_val = Value::build_text("limbo");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(10);
        let length_value = Value::Integer(3);
        let expected_val = Value::build_text("");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(3);
        let length_value = Value::Null;
        let expected_val = Value::build_text("mbo");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(10);
        let length_value = Value::Null;
        let expected_val = Value::build_text("");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );
    }

    #[test]
    fn test_exec_instr() {
        let input = Value::build_text("limbo");
        let pattern = Value::build_text("im");
        let expected = Value::Integer(2);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::build_text("limbo");
        let expected = Value::Integer(1);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::build_text("o");
        let expected = Value::Integer(5);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("liiiiimbo");
        let pattern = Value::build_text("ii");
        let expected = Value::Integer(2);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::build_text("limboX");
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::build_text("");
        let expected = Value::Integer(1);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("");
        let pattern = Value::build_text("limbo");
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("");
        let pattern = Value::build_text("");
        let expected = Value::Integer(1);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Null;
        let pattern = Value::Null;
        let expected = Value::Null;
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::Null;
        let expected = Value::Null;
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Null;
        let pattern = Value::build_text("limbo");
        let expected = Value::Null;
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Integer(123);
        let pattern = Value::Integer(2);
        let expected = Value::Integer(2);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Integer(123);
        let pattern = Value::Integer(5);
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Float(12.34);
        let pattern = Value::Float(2.3);
        let expected = Value::Integer(2);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Float(12.34);
        let pattern = Value::Float(5.6);
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Float(12.34);
        let pattern = Value::build_text(".");
        let expected = Value::Integer(3);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Blob(vec![1, 2, 3, 4, 5]);
        let pattern = Value::Blob(vec![3, 4]);
        let expected = Value::Integer(3);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Blob(vec![1, 2, 3, 4, 5]);
        let pattern = Value::Blob(vec![3, 2]);
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Blob(vec![0x61, 0x62, 0x63, 0x64, 0x65]);
        let pattern = Value::build_text("cd");
        let expected = Value::Integer(3);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("abcde");
        let pattern = Value::Blob(vec![0x63, 0x64]);
        let expected = Value::Integer(3);
        assert_eq!(input.exec_instr(&pattern), expected);
    }

    #[test]
    fn test_exec_sign() {
        let input = Value::Integer(42);
        let expected = Some(Value::Integer(1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Integer(-42);
        let expected = Some(Value::Integer(-1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Integer(0);
        let expected = Some(Value::Integer(0));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Float(0.0);
        let expected = Some(Value::Integer(0));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Float(0.1);
        let expected = Some(Value::Integer(1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Float(42.0);
        let expected = Some(Value::Integer(1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Float(-42.0);
        let expected = Some(Value::Integer(-1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::build_text("abc");
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::build_text("42");
        let expected = Some(Value::Integer(1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::build_text("-42");
        let expected = Some(Value::Integer(-1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::build_text("0");
        let expected = Some(Value::Integer(0));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"abc".to_vec());
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"42".to_vec());
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"-42".to_vec());
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"0".to_vec());
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Null;
        let expected = None;
        assert_eq!(input.exec_sign(), expected);
    }

    #[test]
    fn test_exec_zeroblob() {
        let input = Value::Integer(0);
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Null;
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Integer(4);
        let expected = Value::Blob(vec![0; 4]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Integer(-1);
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::build_text("5");
        let expected = Value::Blob(vec![0; 5]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::build_text("-5");
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::build_text("text");
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Float(2.6);
        let expected = Value::Blob(vec![0; 2]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Blob(vec![1]);
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);
    }

    #[test]
    fn test_execute_sqlite_version() {
        let version_integer = 3046001;
        let expected = "3.46.1";
        assert_eq!(execute_sqlite_version(version_integer), expected);
    }

    #[test]
    fn test_replace() {
        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("b");
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("aoa");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("b");
        let replace_str = Value::build_text("");
        let expected_str = Value::build_text("o");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("b");
        let replace_str = Value::build_text("abc");
        let expected_str = Value::build_text("abcoabc");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("a");
        let replace_str = Value::build_text("b");
        let expected_str = Value::build_text("bob");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("");
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("bob");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::Null;
        let replace_str = Value::build_text("a");
        let expected_str = Value::Null;
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bo5");
        let pattern_str = Value::Integer(5);
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("boa");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bo5.0");
        let pattern_str = Value::Float(5.0);
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("boa");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bo5");
        let pattern_str = Value::Float(5.0);
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("bo5");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bo5.0");
        let pattern_str = Value::Float(5.0);
        let replace_str = Value::Float(6.0);
        let expected_str = Value::build_text("bo6.0");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        // todo: change this test to use (0.1 + 0.2) instead of 0.3 when decimals are implemented.
        let input_str = Value::build_text("tes3");
        let pattern_str = Value::Integer(3);
        let replace_str = Value::Float(0.3);
        let expected_str = Value::build_text("tes0.3");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );
    }

    #[test]
    fn test_bitfield() {
        let mut bitfield = Bitfield::<4>::new();
        for i in 0..256 {
            bitfield.set(i);
            assert!(bitfield.get(i));
            for j in 0..i {
                assert!(bitfield.get(j));
            }
            for j in i + 1..256 {
                assert!(!bitfield.get(j));
            }
        }
        for i in 0..256 {
            bitfield.unset(i);
            assert!(!bitfield.get(i));
        }
    }
}
