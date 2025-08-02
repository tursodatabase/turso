#![allow(unused_variables)]
use crate::function::AlterTableFunc;
use crate::numeric::{NullableInteger, Numeric};
use crate::storage::btree::{integrity_check, IntegrityCheckError, IntegrityCheckState};
use crate::storage::database::DatabaseFile;
use crate::storage::page_cache::DumbLruPageCache;
use crate::storage::pager::{AtomicDbState, CreateBTreeFlags, DbState};
use crate::storage::sqlite3_ondisk::read_varint;
use crate::translate::collate::CollationSeq;
use crate::types::{
    compare_immutable, compare_records_generic, Extendable, ImmutableRecord, RawSlice, SeekResult,
    Text, TextRef, TextSubtype,
};
use crate::util::{normalize_ident, IOExt as _};
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
};
use std::env::temp_dir;
use std::ops::DerefMut;
use std::{
    borrow::BorrowMut,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{pseudo::PseudoCursor, result::LimboResult};

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
    util::{
        cast_real_to_integer, cast_text_to_integer, cast_text_to_numeric, cast_text_to_real,
        checked_cast_text_to_numeric, parse_schema_rows, RoundToPrecision,
    },
    vdbe::{
        builder::CursorType,
        insn::{IdxInsertFlags, Insn},
    },
    vector::{vector32, vector64, vector_distance_cos, vector_distance_l2, vector_extract},
};

use crate::{
    info, turso_assert, BufferPool, MvCursor, OpenFlags, RefValue, Row, StepResult,
    TransactionState,
};

use super::{
    insn::{Cookie, RegisterOrLiteral},
    CommitState,
};
use fallible_iterator::FallibleIterator;
use parking_lot::RwLock;
use rand::{thread_rng, Rng};
use turso_sqlite3_parser::ast;
use turso_sqlite3_parser::ast::fmt::ToTokens;
use turso_sqlite3_parser::lexer::sql::Parser;

use super::{
    likeop::{construct_like_escape_arg, exec_glob, exec_like_with_escape},
    sorter::Sorter,
};
use regex::{Regex, RegexBuilder};
use std::{cell::RefCell, collections::HashMap};

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

macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr? {
            IOResult::Done(v) => v,
            IOResult::IO => return Ok(InsnFunctionStepResult::IO),
        }
    };
}
pub type InsnFunction = fn(
    &Program,
    &mut ProgramState,
    &Insn,
    &Rc<Pager>,
    Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult>;

pub enum InsnFunctionStepResult {
    Done,
    IO,
    Row,
    Interrupt,
    Busy,
    Step,
}

impl From<StepResult> for InsnFunctionStepResult {
    fn from(value: StepResult) -> Self {
        match value {
            super::StepResult::Done => Self::Done,
            super::StepResult::IO => Self::IO,
            super::StepResult::Row => Self::Row,
            super::StepResult::Interrupt => Self::Interrupt,
            super::StepResult::Busy => Self::Busy,
        }
    }
}

pub fn op_init(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Init { target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Add { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_add(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_subtract(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Subtract { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_subtract(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_multiply(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Multiply { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_multiply(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_divide(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Divide { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_divide(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_index(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::DropIndex { index, db: _ } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Remainder { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_remainder(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_and(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::BitAnd { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_bit_and(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_or(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::BitOr { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_bit_or(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_not(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::BitNot { reg, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] =
        Register::Value(state.registers[*reg].get_owned_value().exec_bit_not());
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_checkpoint(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Checkpoint {
        database: _,
        checkpoint_mode,
        dest,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if !program.connection.auto_commit.get() {
        // TODO: sqlite returns "Runtime error: database table is locked (6)" when a table is in use
        // when a checkpoint is attempted. We don't have table locks, so return TableLocked for any
        // attempt to checkpoint in an interactive transaction. This does not end the transaction,
        // however.
        return Err(LimboError::TableLocked);
    }
    let result = program.connection.checkpoint(*checkpoint_mode);
    match result {
        Ok(CheckpointResult {
            num_wal_frames: num_wal_pages,
            num_checkpointed_frames: num_checkpointed_pages,
            ..
        }) => {
            // https://sqlite.org/pragma.html#pragma_wal_checkpoint
            // 1st col: 1 (checkpoint SQLITE_BUSY) or 0 (not busy).
            state.registers[*dest] = Register::Value(Value::Integer(0));
            // 2nd col: # modified pages written to wal file
            state.registers[*dest + 1] = Register::Value(Value::Integer(num_wal_pages as i64));
            // 3rd col: # pages moved to db after checkpoint
            state.registers[*dest + 2] =
                Register::Value(Value::Integer(num_checkpointed_pages as i64));
        }
        Err(_err) => state.registers[*dest] = Register::Value(Value::Integer(1)),
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NullRow { cursor_id } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "NullRow");
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Compare {
        start_reg_a,
        start_reg_b,
        count,
        collation,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
        let a = state.registers[start_reg_a + i].get_owned_value();
        let b = state.registers[start_reg_b + i].get_owned_value();
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Jump {
        target_pc_lt,
        target_pc_eq,
        target_pc_gt,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Move {
        source_reg,
        dest_reg,
        count,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IfPos {
        reg,
        target_pc,
        decrement_by,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let reg = *reg;
    let target_pc = *target_pc;
    match state.registers[reg].get_owned_value() {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NotNull { reg, target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let reg = *reg;
    let target_pc = *target_pc;
    match &state.registers[reg].get_owned_value() {
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
    pager: &Rc<Pager>,
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

    let lhs_value = state.registers[lhs].get_owned_value();
    let rhs_value = state.registers[rhs].get_owned_value();

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
            let lhs_is_text = matches!(state.registers[lhs].get_owned_value(), Value::Text(_));
            let rhs_is_text = matches!(state.registers[rhs].get_owned_value(), Value::Text(_));

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
            let lhs_is_text = matches!(state.registers[lhs].get_owned_value(), Value::Text(_));
            let rhs_is_text = matches!(state.registers[rhs].get_owned_value(), Value::Text(_));

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
            if matches!(state.registers[lhs].get_owned_value(), Value::Text(_)) {
                lhs_temp_reg = Some(state.registers[lhs].clone());
                lhs_converted = apply_numeric_affinity(lhs_temp_reg.as_mut().unwrap(), false);
            }

            if matches!(state.registers[rhs].get_owned_value(), Value::Text(_)) {
                rhs_temp_reg = Some(state.registers[rhs].clone());
                rhs_converted = apply_numeric_affinity(rhs_temp_reg.as_mut().unwrap(), false);
            }

            if let Value::Integer(i) =
                (lhs_temp_reg.as_ref().unwrap_or(&state.registers[lhs])).get_owned_value()
            {
                lhs_temp_reg = Some(Register::Value(Value::Float(*i as f64)));
                lhs_converted = true;
            }

            if let Value::Integer(i) = rhs_temp_reg
                .as_ref()
                .unwrap_or(&state.registers[rhs])
                .get_owned_value()
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
            .get_owned_value(),
        rhs_temp_reg
            .as_ref()
            .unwrap_or(&state.registers[rhs])
            .get_owned_value(),
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::If {
        reg,
        target_pc,
        jump_if_null,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    if state.registers[*reg]
        .get_owned_value()
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IfNot {
        reg,
        target_pc,
        jump_if_null,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    if state.registers[*reg]
        .get_owned_value()
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
    _pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::OpenRead {
        cursor_id,
        root_page,
        db,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    let pager = program.get_pager_from_database_index(db);

    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let mv_cursor = match state.mv_tx_id {
        Some(tx_id) => {
            let table_id = *root_page as u64;
            let mv_store = mv_store.unwrap().clone();
            let mv_cursor = Rc::new(RefCell::new(
                MvCursor::new(mv_store, tx_id, table_id, pager.clone()).unwrap(),
            ));
            Some(mv_cursor)
        }
        None => None,
    };
    let mut cursors = state.cursors.borrow_mut();
    let num_columns = match cursor_type {
        CursorType::BTreeTable(table_rc) => table_rc.columns.len(),
        CursorType::BTreeIndex(index_arc) => index_arc.columns.len(),
        _ => unreachable!("This should not have happened"),
    };

    match cursor_type {
        CursorType::BTreeTable(_) => {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VOpen { cursor_id } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VOpen on non-virtual table cursor");
    };
    let cursor = virtual_table.open(program.connection.clone())?;
    state
        .cursors
        .borrow_mut()
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VCreate {
        module_name,
        table_name,
        args_reg,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let module_name = state.registers[*module_name].get_owned_value().to_string();
    let table_name = state.registers[*table_name].get_owned_value().to_string();
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
        crate::VirtualTable::table(Some(&table_name), &module_name, args, &conn.syms.borrow())?;
    {
        conn.syms
            .borrow_mut()
            .vtabs
            .insert(table_name, table.clone());
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vfilter(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VFilter {
        cursor_id,
        pc_if_empty,
        arg_count,
        args_reg,
        idx_str,
        idx_num,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let has_rows = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_virtual_mut();
        let mut args = Vec::with_capacity(*arg_count);
        for i in 0..*arg_count {
            args.push(state.registers[args_reg + i].get_owned_value().clone());
        }
        let idx_str = if let Some(idx_str) = idx_str {
            Some(state.registers[*idx_str].get_owned_value().to_string())
        } else {
            None
        };
        cursor.filter(*idx_num as i32, idx_str, *arg_count, args)?
    };
    if !has_rows {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vcolumn(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VColumn {
        cursor_id,
        column,
        dest,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let value = {
        let mut cursor = state.get_cursor(*cursor_id);
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VUpdate {
        cursor_id,
        arg_count,
        start_reg,
        conflict_action,
        ..
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
            argv.push(value.get_owned_value().clone());
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VNext {
        cursor_id,
        pc_if_next,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let has_more = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_virtual_mut();
        cursor.next()?
    };
    if has_more {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VDestroy { db, table_name } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let conn = program.connection.clone();
    {
        let Some(vtab) = conn.syms.borrow_mut().vtabs.remove(table_name) else {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::OpenPseudo {
        cursor_id,
        content_reg: _,
        num_fields: _,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    {
        let mut cursors = state.cursors.borrow_mut();
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Rewind {
        cursor_id,
        pc_if_empty,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(pc_if_empty.is_offset());
    let is_empty = {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Rewind");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.rewind());
        cursor.is_empty()
    };
    if is_empty {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_last(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Last {
        cursor_id,
        pc_if_empty,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(pc_if_empty.is_offset());
    let is_empty = {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Last");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.last());
        cursor.is_empty()
    };
    if is_empty {
        state.pc = pc_if_empty.as_offset_int();
    } else {
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

/// This function directly interprets bytes as big-endian signed integers with proper
/// sign extension. It's used when the caller already knows the value is an integer
/// from parsing the record header.
///
/// # How OP_Column Uses This
///
/// In `op_column()`, the record header is parsed incrementally to extract serial types.
/// When a serial type indicates an integer (values 1-6), OP_Column can skip the generic
/// `read_value()` path and call this function directly:
///
///
/// match serial_type {
///     1..=6 => {
///         let expected_len = match serial_type {
///             1 => 1, 2 => 2, 3 => 3, 4 => 4, 5 => 6, 6 => 8, _ => 0,
///         };
///         Value::Integer(read_integer_fast(data_slice, expected_len))
///     }
///     // ... other types use generic path
/// }
///
///
/// This avoids the general case path: `SerialType::try_from() → match kind() → read_integer()`.
#[inline(always)]
fn read_integer_fast(buf: &[u8], len: usize) -> i64 {
    debug_assert!(buf.len() >= len, "Buffer too short for requested length");
    match len {
        1 => buf[0] as i8 as i64,
        2 => i16::from_be_bytes([buf[0], buf[1]]) as i64,
        3 => {
            let sign_extension = if buf[0] <= 0x7F { 0 } else { 0xFF };
            i32::from_be_bytes([sign_extension, buf[0], buf[1], buf[2]]) as i64
        }
        4 => i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64,
        6 => {
            let sign_extension = if buf[0] <= 0x7F { 0 } else { 0xFF };
            i64::from_be_bytes([
                sign_extension,
                sign_extension,
                buf[0],
                buf[1],
                buf[2],
                buf[3],
                buf[4],
                buf[5],
            ])
        }
        8 => i64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]),
        _ => 0,
    }
}

pub fn op_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Column {
        cursor_id,
        column,
        dest,
        default,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seeks[*cursor_id].take() {
        let deferred_seek = 'd: {
            let rowid = {
                let mut index_cursor = state.get_cursor(index_cursor_id);
                let index_cursor = index_cursor.as_btree_mut();
                match index_cursor.rowid()? {
                    IOResult::IO => {
                        break 'd Some((index_cursor_id, table_cursor_id));
                    }
                    IOResult::Done(rowid) => rowid,
                }
            };
            let mut table_cursor = state.get_cursor(table_cursor_id);
            let table_cursor = table_cursor.as_btree_mut();
            match table_cursor.seek(
                SeekKey::TableRowId(rowid.unwrap()),
                SeekOp::GE { eq_only: true },
            )? {
                IOResult::Done(_) => None,
                IOResult::IO => Some((index_cursor_id, table_cursor_id)),
            }
        };
        if let Some(deferred_seek) = deferred_seek {
            state.deferred_seeks[*cursor_id] = Some(deferred_seek);
            return Ok(InsnFunctionStepResult::IO);
        }
    }

    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    match cursor_type {
        CursorType::BTreeTable(_) | CursorType::BTreeIndex(_) => {
            let value = 'value: {
                let mut cursor =
                    must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Column");
                let cursor = cursor.as_btree_mut();

                if cursor.get_null_flag() {
                    break 'value Some(RefValue::Null);
                }

                let record_result = return_if_io!(cursor.record());
                let Some(record) = record_result.as_ref() else {
                    break 'value None;
                };

                let payload = record.get_payload();

                if payload.is_empty() {
                    break 'value None;
                }

                let mut record_cursor = cursor.record_cursor.borrow_mut();

                if record_cursor.serial_types.is_empty() && record_cursor.offsets.is_empty() {
                    let (header_size, header_len_bytes) = read_varint_fast(payload)?;
                    let header_size = header_size as usize;

                    if header_size > payload.len() || header_size > 98307 {
                        return Err(LimboError::Corrupt("Header size exceeds bounds".into()));
                    }

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
                    .unwrap_or(record_cursor.header_size);

                // Adjust data_offset if we already have some columns parsed
                if !record_cursor.serial_types.is_empty() && !record_cursor.offsets.is_empty() {
                    data_offset = *record_cursor.offsets.last().unwrap();
                }

                // Parse the header for serial types incrementally until we have the target column
                while record_cursor.serial_types.len() <= target_column
                    && parse_pos < record_cursor.header_size
                    && parse_pos < payload.len()
                {
                    let (serial_type, varint_len) = read_varint_fast(&payload[parse_pos..])?;

                    record_cursor.serial_types.push(serial_type);
                    parse_pos += varint_len;
                    let data_size = match serial_type {
                        0 => 0,
                        1 => 1,
                        2 => 2,
                        3 => 3,
                        4 => 4,
                        5 => 6,
                        6 => 8,
                        7 => 8,
                        8 => 0,
                        9 => 0,
                        n if n >= 12 && n % 2 == 0 => (n - 12) / 2,
                        n if n >= 13 && n % 2 == 1 => (n - 13) / 2,
                        10 | 11 => {
                            return Err(LimboError::Corrupt(format!(
                                "Reserved serial type: {serial_type}"
                            )))
                        }
                        _ => {
                            return Err(LimboError::Corrupt(format!(
                                "Invalid serial type: {serial_type}"
                            )))
                        }
                    } as usize;
                    data_offset += data_size;
                    record_cursor.offsets.push(data_offset);
                }

                record_cursor.header_offset = parse_pos;

                if parse_pos > record_cursor.header_size || data_offset > payload.len() {
                    record_cursor.serial_types.clear();
                    record_cursor.offsets.clear();
                    record_cursor.header_offset = 0;
                    record_cursor.header_size = 0;
                    break 'value None;
                }

                if target_column >= record_cursor.serial_types.len() {
                    break 'value None;
                }

                let serial_type = record_cursor.serial_types[target_column];

                // Fast path for common constant cases
                match serial_type {
                    0 => break 'value Some(RefValue::Null),
                    8 => break 'value Some(RefValue::Integer(0)),
                    9 => break 'value Some(RefValue::Integer(1)),
                    _ => {}
                }

                if target_column + 1 >= record_cursor.offsets.len() {
                    break 'value None;
                }

                let start_offset = record_cursor.offsets[target_column];
                let end_offset = record_cursor.offsets[target_column + 1];

                let data_slice = &payload[start_offset..end_offset];
                let data_len = end_offset - start_offset;

                match serial_type {
                    1..=6 => {
                        let expected_len = match serial_type {
                            1 => 1,
                            2 => 2,
                            3 => 3,
                            4 => 4,
                            5 => 6,
                            6 => 8,
                            _ => 0,
                        };

                        if data_len >= expected_len {
                            Some(RefValue::Integer(read_integer_fast(
                                data_slice,
                                expected_len,
                            )))
                        } else {
                            return Err(LimboError::Corrupt(format!(
                                "Insufficient data for integer type {serial_type}: expected {expected_len}, got {data_len}"
                            )));
                        }
                    }
                    7 => {
                        if data_len >= 8 {
                            let bytes = [
                                data_slice[0],
                                data_slice[1],
                                data_slice[2],
                                data_slice[3],
                                data_slice[4],
                                data_slice[5],
                                data_slice[6],
                                data_slice[7],
                            ];
                            Some(RefValue::Float(f64::from_be_bytes(bytes)))
                        } else {
                            None
                        }
                    }
                    n if n >= 12 && n % 2 == 0 => {
                        Some(RefValue::Blob(RawSlice::create_from(data_slice)))
                    }
                    n if n >= 13 && n % 2 == 1 => Some(RefValue::Text(TextRef::create_from(
                        data_slice,
                        TextSubtype::Text,
                    ))),
                    _ => None,
                }
            };

            let Some(value) = value else {
                // DEFAULT handling. Try to reuse the registers when allocation is not needed.
                let Some(ref default) = default else {
                    state.registers[*dest] = Register::Value(Value::Null);
                    state.pc += 1;
                    return Ok(InsnFunctionStepResult::Step);
                };
                match (default, &mut state.registers[*dest]) {
                    (Value::Text(new_text), Register::Value(Value::Text(existing_text))) => {
                        existing_text.do_extend(new_text);
                    }
                    (Value::Blob(new_blob), Register::Value(Value::Blob(existing_blob))) => {
                        existing_blob.do_extend(new_blob);
                    }
                    _ => {
                        state.registers[*dest] = Register::Value(default.clone());
                    }
                }
                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            };

            // Try to reuse the registers when allocation is not needed.
            match (&value, &mut state.registers[*dest]) {
                (RefValue::Text(new_text), Register::Value(Value::Text(existing_text))) => {
                    existing_text.do_extend(new_text);
                }
                (RefValue::Blob(new_blob), Register::Value(Value::Blob(existing_blob))) => {
                    existing_blob.do_extend(new_blob);
                }
                _ => {
                    state.registers[*dest] = Register::Value(match value {
                        RefValue::Integer(i) => Value::Integer(i),
                        RefValue::Float(f) => Value::Float(f),
                        RefValue::Text(t) => Value::Text(Text::new(t.as_str())),
                        RefValue::Blob(b) => Value::Blob(b.to_slice().to_vec()),
                        RefValue::Null => Value::Null,
                    });
                }
            }
        }
        CursorType::Sorter => {
            let record = {
                let mut cursor = state.get_cursor(*cursor_id);
                let cursor = cursor.as_sorter_mut();
                cursor.record().cloned()
            };
            if let Some(record) = record {
                state.registers[*dest] = Register::Value(match record.get_value_opt(*column) {
                    Some(val) => val.to_owned(),
                    None => default.clone().unwrap_or(Value::Null),
                });
            } else {
                state.registers[*dest] = Register::Value(Value::Null);
            }
        }
        CursorType::Pseudo(_) => {
            let value = {
                let mut cursor = state.get_cursor(*cursor_id);
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

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_type_check(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::TypeCheck {
        start_reg,
        count,
        check_generated,
        table_reference,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(table_reference.is_strict);
    state.registers[*start_reg..*start_reg + *count]
        .iter_mut()
        .zip(table_reference.columns.iter())
        .try_for_each(|(reg, col)| {
            // INT PRIMARY KEY is not row_id_alias so we throw error if this col is NULL
            if !col.is_rowid_alias
                && col.primary_key
                && matches!(reg.get_owned_value(), Value::Null)
            {
                bail_constraint_error!(
                    "NOT NULL constraint failed: {}.{} ({})",
                    &table_reference.name,
                    col.name.as_deref().unwrap_or(""),
                    SQLITE_CONSTRAINT
                )
            } else if col.is_rowid_alias && matches!(reg.get_owned_value(), Value::Null) {
                // Handle INTEGER PRIMARY KEY for null as usual (Rowid will be auto-assigned)
                return Ok(());
            }
            let col_affinity = col.affinity();
            let ty_str = col.ty_str.as_str();
            let applied = apply_affinity_char(reg, col_affinity);
            let value_type = reg.get_owned_value().value_type();
            match (ty_str, value_type) {
                ("INTEGER" | "INT", ValueType::Integer) => {}
                ("REAL", ValueType::Float) => {}
                ("BLOB", ValueType::Blob) => {}
                ("TEXT", ValueType::Text) => {}
                ("ANY", _) => {}
                (t, v) => bail_constraint_error!(
                    "cannot store {} value in {} column {}.{} ({})",
                    v,
                    t,
                    &table_reference.name,
                    col.name.as_deref().unwrap_or(""),
                    SQLITE_CONSTRAINT
                ),
            };
            Ok(())
        })?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_make_record(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::MakeRecord {
        start_reg,
        count,
        dest_reg,
        ..
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let record = make_record(&state.registers, start_reg, count);
    state.registers[*dest_reg] = Register::Record(record);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_result_row(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ResultRow { start_reg, count } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Next {
        cursor_id,
        pc_if_next,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(pc_if_next.is_offset());
    let is_empty = {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Next");
        let cursor = cursor.as_btree_mut();
        cursor.set_null_flag(false);
        return_if_io!(cursor.next());

        cursor.is_empty()
    };
    if !is_empty {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Prev {
        cursor_id,
        pc_if_prev,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(pc_if_prev.is_offset());
    let is_empty = {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Prev");
        let cursor = cursor.as_btree_mut();
        cursor.set_null_flag(false);
        return_if_io!(cursor.prev());

        cursor.is_empty()
    };
    if !is_empty {
        state.pc = pc_if_prev.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn halt(
    program: &Program,
    state: &mut ProgramState,
    pager: &Rc<Pager>,
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
        _ => {
            return Err(LimboError::Constraint(format!(
                "undocumented halt error code {description}"
            )));
        }
    }
    match program.commit_txn(pager.clone(), state, mv_store, false)? {
        StepResult::Done => Ok(InsnFunctionStepResult::Done),
        StepResult::IO => Ok(InsnFunctionStepResult::IO),
        StepResult::Row => Ok(InsnFunctionStepResult::Row),
        StepResult::Interrupt => Ok(InsnFunctionStepResult::Interrupt),
        StepResult::Busy => Ok(InsnFunctionStepResult::Busy),
    }
}

pub fn op_halt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Halt {
        err_code,
        description,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *err_code > 0 {
        // invalidate page cache in case of error
        pager.clear_page_cache();
    }
    match *err_code {
        0 => {}
        SQLITE_CONSTRAINT_PRIMARYKEY => {
            return Err(LimboError::Constraint(format!(
                "UNIQUE constraint failed: {description} (19)"
            )));
        }
        SQLITE_CONSTRAINT_NOTNULL => {
            return Err(LimboError::Constraint(format!(
                "NOTNULL constraint failed: {description} (19)"
            )));
        }
        _ => {
            return Err(LimboError::Constraint(format!(
                "undocumented halt error code {description}"
            )));
        }
    }
    let auto_commit = program.connection.auto_commit.get();
    tracing::trace!("op_halt(auto_commit={})", auto_commit);
    if auto_commit {
        match program.commit_txn(pager.clone(), state, mv_store, false)? {
            StepResult::Done => Ok(InsnFunctionStepResult::Done),
            StepResult::IO => Ok(InsnFunctionStepResult::IO),
            StepResult::Row => Ok(InsnFunctionStepResult::Row),
            StepResult::Interrupt => Ok(InsnFunctionStepResult::Interrupt),
            StepResult::Busy => Ok(InsnFunctionStepResult::Busy),
        }
    } else {
        Ok(InsnFunctionStepResult::Done)
    }
}

pub fn op_halt_if_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::HaltIfNull {
        target_reg,
        err_code,
        description,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if state.registers[*target_reg].get_owned_value() == &Value::Null {
        halt(program, state, pager, mv_store, *err_code, description)
    } else {
        state.pc += 1;
        Ok(InsnFunctionStepResult::Step)
    }
}

pub fn op_transaction(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Transaction { db, write } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let conn = program.connection.clone();
    if *write && conn._db.open_flags.contains(OpenFlags::ReadOnly) {
        return Err(LimboError::ReadOnly);
    }

    let pager = program.get_pager_from_database_index(db);

    if let Some(mv_store) = &mv_store {
        if state.mv_tx_id.is_none() {
            // We allocate the first page lazily in the first transaction.
            return_if_io!(pager.maybe_allocate_page1());
            let tx_id = mv_store.begin_tx(pager.clone());
            conn.mv_transactions.borrow_mut().push(tx_id);
            state.mv_tx_id = Some(tx_id);
        }
    } else {
        let current_state = conn.transaction_state.get();
        let (new_transaction_state, updated) = match (current_state, write) {
            // pending state means that we tried beginning a tx and the method returned IO.
            // instead of ending the read tx, just update the state to pending.
            (TransactionState::PendingUpgrade, write) => {
                turso_assert!(
                    *write,
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
        };
        if updated && matches!(current_state, TransactionState::None) {
            if let LimboResult::Busy = pager.begin_read_tx()? {
                return Ok(InsnFunctionStepResult::Busy);
            }
        }

        if updated && matches!(new_transaction_state, TransactionState::Write { .. }) {
            match pager.begin_write_tx()? {
                IOResult::Done(r) => {
                    if let LimboResult::Busy = r {
                        pager.end_read_tx()?;
                        conn.transaction_state.replace(TransactionState::None);
                        conn.auto_commit.replace(true);
                        return Ok(InsnFunctionStepResult::Busy);
                    }
                }
                IOResult::IO => {
                    // set the transaction state to pending so we don't have to
                    // end the read transaction.
                    program
                        .connection
                        .transaction_state
                        .replace(TransactionState::PendingUpgrade);
                    return Ok(InsnFunctionStepResult::IO);
                }
            }
        }
        if updated {
            conn.transaction_state.replace(new_transaction_state);
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_auto_commit(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::AutoCommit {
        auto_commit,
        rollback,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let conn = program.connection.clone();
    if state.commit_state == CommitState::Committing {
        return program
            .commit_txn(pager.clone(), state, mv_store, *rollback)
            .map(Into::into);
    }
    let schema_did_change =
        if let TransactionState::Write { schema_did_change } = conn.transaction_state.get() {
            schema_did_change
        } else {
            false
        };

    if *auto_commit != conn.auto_commit.get() {
        if *rollback {
            // TODO(pere): add rollback I/O logic once we implement rollback journal
            return_if_io!(pager.end_tx(true, schema_did_change, &conn, false));
            conn.transaction_state.replace(TransactionState::None);
            conn.auto_commit.replace(true);
        } else {
            conn.auto_commit.replace(*auto_commit);
        }
    } else if !*auto_commit {
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
    program
        .commit_txn(pager.clone(), state, mv_store, *rollback)
        .map(Into::into)
}

pub fn op_goto(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Goto { target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_gosub(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Gosub {
        target_pc,
        return_reg,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    state.registers[*return_reg] = Register::Value(Value::Integer((state.pc + 1) as i64));
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_return(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Return {
        return_reg,
        can_fallthrough,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Value::Integer(pc) = state.registers[*return_reg].get_owned_value() {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Integer { value, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(Value::Integer(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_real(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Real { value, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(Value::Float(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_real_affinity(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::RealAffinity { register } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Value::Integer(i) = &state.registers[*register].get_owned_value() {
        state.registers[*register] = Register::Value(Value::Float(*i as f64));
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_string8(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::String8 { value, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(Value::build_text(value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_blob(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Blob { value, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(Value::Blob(value.clone()));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_row_data(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::RowData { cursor_id, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    let record = {
        let mut cursor_ref =
            must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "RowData");
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

pub fn op_row_id(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::RowId { cursor_id, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seeks[*cursor_id].take() {
        let deferred_seek = 'd: {
            let rowid = {
                let mut index_cursor = state.get_cursor(index_cursor_id);
                let index_cursor = index_cursor.as_btree_mut();
                let record = match index_cursor.record()? {
                    IOResult::IO => {
                        break 'd Some((index_cursor_id, table_cursor_id));
                    }
                    IOResult::Done(record) => record,
                };
                let record = record.as_ref().unwrap();
                let mut record_cursor_ref = index_cursor.record_cursor.borrow_mut();
                let record_cursor = record_cursor_ref.deref_mut();
                let rowid = record.last_value(record_cursor).unwrap();
                match rowid {
                    Ok(RefValue::Integer(rowid)) => rowid,
                    _ => unreachable!(),
                }
            };
            let mut table_cursor = state.get_cursor(table_cursor_id);
            let table_cursor = table_cursor.as_btree_mut();
            match table_cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })? {
                IOResult::Done(_) => None,
                IOResult::IO => Some((index_cursor_id, table_cursor_id)),
            }
        };
        if let Some(deferred_seek) = deferred_seek {
            state.deferred_seeks[*cursor_id] = Some(deferred_seek);
            return Ok(InsnFunctionStepResult::IO);
        }
    }
    let mut cursors = state.cursors.borrow_mut();
    if let Some(Cursor::BTree(btree_cursor)) = cursors.get_mut(*cursor_id).unwrap() {
        if let Some(ref rowid) = return_if_io!(btree_cursor.rowid()) {
            state.registers[*dest] = Register::Value(Value::Integer(*rowid));
        } else {
            state.registers[*dest] = Register::Value(Value::Null);
        }
    } else if let Some(Cursor::Virtual(virtual_cursor)) = cursors.get_mut(*cursor_id).unwrap() {
        let rowid = virtual_cursor.rowid();
        if rowid != 0 {
            state.registers[*dest] = Register::Value(Value::Integer(rowid));
        } else {
            state.registers[*dest] = Register::Value(Value::Null);
        }
    } else {
        return Err(LimboError::InternalError(
            "RowId: cursor is not a table or virtual cursor".to_string(),
        ));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_idx_row_id(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxRowId { cursor_id, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let mut cursors = state.cursors.borrow_mut();
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SeekRowid {
        cursor_id,
        src_reg,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();
        let rowid = match state.registers[*src_reg].get_owned_value() {
            Value::Integer(rowid) => Some(*rowid),
            Value::Null => None,
            // For non-integer values try to apply affinity and convert them to integer.
            other => {
                let mut temp_reg = Register::Value(other.clone());
                let converted = apply_affinity_char(&mut temp_reg, Affinity::Numeric);
                if converted {
                    match temp_reg.get_owned_value() {
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
                let seek_result = return_if_io!(
                    cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })
                );
                if !matches!(seek_result, SeekResult::Found) {
                    target_pc.as_offset_int()
                } else {
                    state.pc + 1
                }
            }
            None => target_pc.as_offset_int(),
        }
    };
    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_deferred_seek(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::DeferredSeek {
        index_cursor_id,
        table_cursor_id,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
    pager: &Rc<Pager>,
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
        Ok(SeekInternalResult::IO) => Ok(InsnFunctionStepResult::IO),
        Err(e) => Err(e),
    }
}

#[derive(Debug, PartialEq)]
pub enum SeekInternalResult {
    Found,
    NotFound,
    IO,
}
#[derive(Clone)]
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
    pager: &Rc<Pager>,
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
        pager: &Rc<Pager>,
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
                    let original_value = state.registers[start_reg].get_owned_value().clone();
                    let mut temp_value = original_value.clone();

                    let conversion_successful = if matches!(temp_value, Value::Text(_)) {
                        let mut temp_reg = Register::Value(temp_value);
                        let converted = apply_numeric_affinity(&mut temp_reg, false);
                        temp_value = temp_reg.get_owned_value().clone();
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
                        let mut cursor = state.get_cursor(cursor_id);
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
                            IOResult::IO => return Ok(SeekInternalResult::IO),
                        }
                    };
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
                        let mut cursor = state.get_cursor(cursor_id);
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
                        let result = match op {
                            SeekOp::GT | SeekOp::GE { .. } => cursor.next()?,
                            SeekOp::LT | SeekOp::LE { .. } => cursor.prev()?,
                        };
                        match result {
                            IOResult::Done(found) => found,
                            IOResult::IO => return Ok(SeekInternalResult::IO),
                        }
                    };
                    return Ok(if found {
                        SeekInternalResult::Found
                    } else {
                        SeekInternalResult::NotFound
                    });
                }
                OpSeekState::MoveLast => {
                    let mut cursor = state.get_cursor(cursor_id);
                    let cursor = cursor.as_btree_mut();
                    match cursor.last()? {
                        IOResult::Done(()) => {}
                        IOResult::IO => return Ok(SeekInternalResult::IO),
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
    if !matches!(result, Ok(SeekInternalResult::IO)) {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxGE {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());

    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    if let Insn::SeekEnd { cursor_id } = *insn {
        let mut cursor = state.get_cursor(cursor_id);
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.seek_end());
    } else {
        unreachable!("unexpected Insn {:?}", insn)
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[allow(clippy::let_and_return)]
pub fn op_idx_le(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxLE {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());

    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxGT {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());

    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxLT {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());

    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::DecrJumpZero { reg, target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    match state.registers[*reg].get_owned_value() {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::AggStep {
        acc_reg,
        col,
        delimiter,
        func,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
                    finalized_value: None,
                })),
                _ => unreachable!("scalar function called in aggregate context"),
            },
        };
    }
    match func {
        AggFunc::Avg => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                panic!(
                    "Unexpected value {:?} in AggStep at register {}",
                    state.registers[*acc_reg], *acc_reg
                );
            };
            let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                unreachable!();
            };
            *acc = acc.exec_add(col.get_owned_value());
            *count += 1;
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
                Register::Value(owned_value) => {
                    match owned_value {
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
            let col = state.registers[*col].get_owned_value().clone();
            if matches!(&state.registers[*acc_reg], Register::Value(Value::Null)) {
                state.registers[*acc_reg] =
                    Register::Aggregate(AggContext::Count(Value::Integer(0)));
            }
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                panic!(
                    "Unexpected value {:?} in AggStep at register {}",
                    state.registers[*acc_reg], *acc_reg
                );
            };
            let AggContext::Count(count) = agg.borrow_mut() else {
                unreachable!();
            };

            if !(matches!(func, AggFunc::Count) && matches!(col, Value::Null)) {
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

            let new_value = col.get_owned_value();
            if *new_value != Value::Null && acc.as_ref().is_none_or(|acc| new_value > acc) {
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

            let new_value = col.get_owned_value();

            if *new_value != Value::Null && acc.as_ref().is_none_or(|acc| new_value < acc) {
                *acc = Some(new_value.clone());
            }
        }
        AggFunc::GroupConcat | AggFunc::StringAgg => {
            let col = state.registers[*col].get_owned_value().clone();
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
                match delimiter {
                    Register::Value(owned_value) => {
                        *acc += owned_value;
                    }
                    _ => unreachable!(),
                }
                *acc += col;
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

            let mut key_vec = convert_dbtype_to_raw_jsonb(key.get_owned_value())?;
            let mut val_vec = convert_dbtype_to_raw_jsonb(value.get_owned_value())?;

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

            let mut data = convert_dbtype_to_raw_jsonb(col.get_owned_value())?;
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
                    ext_values.push(ov.get_owned_value().to_ffi());
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::AggFinal { register, func } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    match state.registers[*register].borrow_mut() {
        Register::Aggregate(agg) => match func {
            AggFunc::Avg => {
                let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                    unreachable!();
                };
                *acc /= count.clone();
                state.registers[*register] = Register::Value(acc.clone());
            }
            AggFunc::Sum => {
                let AggContext::Sum(acc, sum_state) = agg.borrow_mut() else {
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
                state.registers[*register] = Register::Value(value);
            }
            AggFunc::Total => {
                let AggContext::Sum(acc, _) = agg.borrow_mut() else {
                    unreachable!();
                };
                let value = match acc {
                    Value::Null => Value::Float(0.0),
                    Value::Integer(i) => Value::Float(*i as f64),
                    Value::Float(f) => Value::Float(*f),
                    _ => unreachable!(),
                };
                state.registers[*register] = Register::Value(value);
            }
            AggFunc::Count | AggFunc::Count0 => {
                let AggContext::Count(count) = agg.borrow_mut() else {
                    unreachable!();
                };
                state.registers[*register] = Register::Value(count.clone());
            }
            AggFunc::Max => {
                let AggContext::Max(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                match acc {
                    Some(value) => state.registers[*register] = Register::Value(value.clone()),
                    None => state.registers[*register] = Register::Value(Value::Null),
                }
            }
            AggFunc::Min => {
                let AggContext::Min(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                match acc {
                    Some(value) => state.registers[*register] = Register::Value(value.clone()),
                    None => state.registers[*register] = Register::Value(Value::Null),
                }
            }
            AggFunc::GroupConcat | AggFunc::StringAgg => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                state.registers[*register] = Register::Value(acc.clone());
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupObject => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[*register] = Register::Value(json_from_raw_bytes_agg(data, false)?);
            }
            #[cfg(feature = "json")]
            AggFunc::JsonbGroupObject => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[*register] = Register::Value(json_from_raw_bytes_agg(data, true)?);
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupArray => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[*register] = Register::Value(json_from_raw_bytes_agg(data, false)?);
            }
            #[cfg(feature = "json")]
            AggFunc::JsonbGroupArray => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[*register] = Register::Value(json_from_raw_bytes_agg(data, true)?);
            }
            AggFunc::External(_) => {
                agg.compute_external()?;
                let AggContext::External(agg_state) = agg else {
                    unreachable!();
                };
                match &agg_state.finalized_value {
                    Some(value) => state.registers[*register] = Register::Value(value.clone()),
                    None => state.registers[*register] = Register::Value(Value::Null),
                }
            }
        },
        Register::Value(Value::Null) => {
            // when the set is empty
            match func {
                AggFunc::Total => {
                    state.registers[*register] = Register::Value(Value::Float(0.0));
                }
                AggFunc::Count | AggFunc::Count0 => {
                    state.registers[*register] = Register::Value(Value::Integer(0));
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterOpen {
        cursor_id,
        columns: _,
        order,
        collations,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let cache_size = program.connection.get_cache_size();
    // Set the buffer size threshold to be roughly the same as the limit configured for the page-cache.
    let page_size = pager
        .io
        .block(|| pager.with_header(|header| header.page_size))
        .unwrap_or_default()
        .get() as usize;

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
    let mut cursors = state.cursors.borrow_mut();
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterData {
        cursor_id,
        dest_reg,
        pseudo_cursor,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let record = {
        let mut cursor = state.get_cursor(*cursor_id);
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
        let mut pseudo_cursor = state.get_cursor(*pseudo_cursor);
        pseudo_cursor.as_pseudo_mut().insert(record);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterInsert {
        cursor_id,
        record_reg,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        let record = match &state.registers[*record_reg] {
            Register::Record(record) => record,
            _ => unreachable!("SorterInsert on non-record register"),
        };
        cursor.insert(record)?;
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_sort(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterSort {
        cursor_id,
        pc_if_empty,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let is_empty = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        let is_empty = cursor.is_empty();
        if !is_empty {
            if let IOResult::IO = cursor.sort()? {
                return Ok(InsnFunctionStepResult::IO);
            }
        }
        is_empty
    };
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterNext {
        cursor_id,
        pc_if_next,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(pc_if_next.is_offset());
    let has_more = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        if let IOResult::IO = cursor.next()? {
            return Ok(InsnFunctionStepResult::IO);
        }
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Function {
        constant_mask,
        func,
        start_reg,
        dest,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let arg_count = func.arg_count;

    match &func.func {
        #[cfg(feature = "json")]
        crate::function::Func::Json(json_func) => match json_func {
            JsonFunc::Json => {
                let json_value = &state.registers[*start_reg];
                let json_str = get_json(json_value.get_owned_value(), None);
                match json_str {
                    Ok(json) => state.registers[*dest] = Register::Value(json),
                    Err(e) => return Err(e),
                }
            }

            JsonFunc::Jsonb => {
                let json_value = &state.registers[*start_reg];
                let json_blob = jsonb(json_value.get_owned_value(), &state.json_cache);
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

                        json_extract(val.get_owned_value(), reg_values, &state.json_cache)
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

                        jsonb_extract(val.get_owned_value(), reg_values, &state.json_cache)
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
                let json_str = json_func(
                    json.get_owned_value(),
                    path.get_owned_value(),
                    &state.json_cache,
                );
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
                        json_value.get_owned_value(),
                        path_value.map(|x| x.get_owned_value()),
                        &state.json_cache,
                    ),
                    JsonFunc::JsonType => json_type(
                        json_value.get_owned_value(),
                        path_value.map(|x| x.get_owned_value()),
                    ),
                    _ => unreachable!(),
                };

                match func_result {
                    Ok(result) => state.registers[*dest] = Register::Value(result),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonErrorPosition => {
                let json_value = &state.registers[*start_reg];
                match json_error_position(json_value.get_owned_value()) {
                    Ok(pos) => state.registers[*dest] = Register::Value(pos),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonValid => {
                let json_value = &state.registers[*start_reg];
                state.registers[*dest] =
                    Register::Value(is_json_valid(json_value.get_owned_value()));
            }
            JsonFunc::JsonPatch => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let target = &state.registers[*start_reg];
                let patch = &state.registers[*start_reg + 1];
                state.registers[*dest] = Register::Value(json_patch(
                    target.get_owned_value(),
                    patch.get_owned_value(),
                    &state.json_cache,
                )?);
            }
            JsonFunc::JsonbPatch => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let target = &state.registers[*start_reg];
                let patch = &state.registers[*start_reg + 1];
                state.registers[*dest] = Register::Value(jsonb_patch(
                    target.get_owned_value(),
                    patch.get_owned_value(),
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
                    Some(value) => match value.get_owned_value() {
                        Value::Text(text) => text.as_str(),
                        Value::Integer(val) => &val.to_string(),
                        Value::Float(val) => &val.to_string(),
                        Value::Blob(val) => &String::from_utf8_lossy(val),
                        _ => "    ",
                    },
                    // If the second argument is omitted or is NULL, then indentation is four spaces per level
                    None => "    ",
                };

                let json_str = get_json(json_value.get_owned_value(), Some(indent))?;
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

                match json_quote(json_value.get_owned_value()) {
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
                    state.registers[*start_reg + 1].get_owned_value().clone()
                else {
                    unreachable!("Cast with non-text type");
                };
                let result = reg_value_argument
                    .get_owned_value()
                    .exec_cast(reg_value_type.as_str());
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Changes => {
                let res = &program.connection.last_change;
                let changes = res.get();
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
                let result = match (pattern.get_owned_value(), text.get_owned_value()) {
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
                let result = reg_value
                    .get_owned_value()
                    .exec_instr(pattern_value.get_owned_value());
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::LastInsertRowid => {
                state.registers[*dest] =
                    Register::Value(Value::Integer(program.connection.last_insert_rowid()));
            }
            ScalarFunc::Like => {
                let pattern = &state.registers[*start_reg];
                let match_expression = &state.registers[*start_reg + 1];

                let pattern = match pattern.get_owned_value() {
                    Value::Text(_) => pattern.get_owned_value(),
                    _ => &pattern.get_owned_value().exec_cast("TEXT"),
                };
                let match_expression = match match_expression.get_owned_value() {
                    Value::Text(_) => match_expression.get_owned_value(),
                    _ => &match_expression.get_owned_value().exec_cast("TEXT"),
                };

                let result = match (pattern, match_expression) {
                    (Value::Text(pattern), Value::Text(match_expression)) if arg_count == 3 => {
                        let escape = construct_like_escape_arg(
                            state.registers[*start_reg + 2].get_owned_value(),
                        )?;

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
                let reg_value = state.registers[*start_reg].borrow_mut().get_owned_value();
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
                let result = reg_value.get_owned_value().exec_hex();
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Unhex => {
                let reg_value = &state.registers[*start_reg];
                let ignored_chars = state.registers.get(*start_reg + 1);
                let result = reg_value
                    .get_owned_value()
                    .exec_unhex(ignored_chars.map(|x| x.get_owned_value()));
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
                    .get_owned_value()
                    .exec_trim(pattern_value.map(|x| x.get_owned_value()));
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
                    .get_owned_value()
                    .exec_ltrim(pattern_value.map(|x| x.get_owned_value()));
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
                    .get_owned_value()
                    .exec_rtrim(pattern_value.map(|x| x.get_owned_value()));
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
                    .get_owned_value()
                    .exec_round(precision_value.map(|x| x.get_owned_value()));
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Min => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest] = Register::Value(Value::exec_min(
                    reg_values.iter().map(|v| v.get_owned_value()),
                ));
            }
            ScalarFunc::Max => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest] = Register::Value(Value::exec_max(
                    reg_values.iter().map(|v| v.get_owned_value()),
                ));
            }
            ScalarFunc::Nullif => {
                let first_value = &state.registers[*start_reg];
                let second_value = &state.registers[*start_reg + 1];
                state.registers[*dest] = Register::Value(Value::exec_nullif(
                    first_value.get_owned_value(),
                    second_value.get_owned_value(),
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
                    str_value.get_owned_value(),
                    start_value.get_owned_value(),
                    length_value.map(|x| x.get_owned_value()),
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
                    let start = state.registers[*start_reg].get_owned_value().clone();
                    let end = state.registers[*start_reg + 1].get_owned_value().clone();

                    let result = crate::functions::datetime::exec_timediff(&[
                        Register::Value(start),
                        Register::Value(end),
                    ]);

                    state.registers[*dest] = Register::Value(result);
                }
            }
            ScalarFunc::TotalChanges => {
                let res = &program.connection.total_changes;
                let total_changes = res.get();
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
                    let unixepoch = exec_unixepoch(datetime_value.get_owned_value());
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
                let version_integer =
                    return_if_io!(pager.with_header(|header| header.version_number)).get() as i64;
                let version = execute_sqlite_version(version_integer);
                state.registers[*dest] = Register::Value(Value::build_text(version));
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
                    source.get_owned_value(),
                    pattern.get_owned_value(),
                    replacement.get_owned_value(),
                ));
            }
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            ScalarFunc::LoadExtension => {
                let extension = &state.registers[*start_reg];
                let ext = resolve_ext_path(&extension.get_owned_value().to_string())?;
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
            ScalarFunc::Likely => {
                let value = &state.registers[*start_reg].borrow_mut();
                let result = value.get_owned_value().exec_likely();
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Likelihood => {
                assert_eq!(arg_count, 2);
                let value = &state.registers[*start_reg];
                let probability = &state.registers[*start_reg + 1];
                let result = value
                    .get_owned_value()
                    .exec_likelihood(probability.get_owned_value());
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

                    let table = state.registers[*start_reg].get_owned_value();
                    let Value::Text(table) = table else {
                        return Err(LimboError::InvalidArgument(
                            "table_columns_json_array: function argument must be of type TEXT"
                                .to_string(),
                        ));
                    };
                    let table = {
                        let schema = program.connection.schema.borrow();
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

                    let columns_str = state.registers[*start_reg].get_owned_value();
                    let bin_record = state.registers[*start_reg + 1].get_owned_value();
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
                let filename = state.registers[*start_reg].get_owned_value();
                let dbname = state.registers[*start_reg + 1].get_owned_value();
                let _key = state.registers[*start_reg + 2].get_owned_value(); // Not used in read-only implementation

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
                let dbname = state.registers[*start_reg].get_owned_value();

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
                        let val = ov.get_owned_value().to_ffi();
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
                let result = reg_value.get_owned_value().exec_math_unary(math_func);
                state.registers[*dest] = Register::Value(result);
            }

            MathFuncArity::Binary => {
                let lhs = &state.registers[*start_reg];
                let rhs = &state.registers[*start_reg + 1];
                let result = lhs
                    .get_owned_value()
                    .exec_math_binary(rhs.get_owned_value(), math_func);
                state.registers[*dest] = Register::Value(result);
            }

            MathFuncArity::UnaryOrBinary => match math_func {
                MathFunc::Log => {
                    let result = match arg_count {
                        1 => {
                            let arg = &state.registers[*start_reg];
                            arg.get_owned_value().exec_math_log(None)
                        }
                        2 => {
                            let base = &state.registers[*start_reg];
                            let arg = &state.registers[*start_reg + 1];
                            arg.get_owned_value()
                                .exec_math_log(Some(base.get_owned_value()))
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
            let r#type = &state.registers[*start_reg].get_owned_value().clone();

            let Value::Text(name) = &state.registers[*start_reg + 1].get_owned_value() else {
                panic!("sqlite_schema.name should be TEXT")
            };
            let name = name.to_string();

            let Value::Text(tbl_name) = &state.registers[*start_reg + 2].get_owned_value() else {
                panic!("sqlite_schema.tbl_name should be TEXT")
            };
            let tbl_name = tbl_name.to_string();

            let Value::Integer(root_page) =
                &state.registers[*start_reg + 3].get_owned_value().clone()
            else {
                panic!("sqlite_schema.root_page should be INTEGER")
            };

            let sql = &state.registers[*start_reg + 4].get_owned_value().clone();

            let (new_name, new_tbl_name, new_sql) = match alter_func {
                AlterTableFunc::RenameTable => {
                    let rename_from = {
                        match &state.registers[*start_reg + 5].get_owned_value() {
                            Value::Text(rename_from) => normalize_ident(rename_from.as_str()),
                            _ => panic!("rename_from parameter should be TEXT"),
                        }
                    };

                    let rename_to = {
                        match &state.registers[*start_reg + 6].get_owned_value() {
                            Value::Text(rename_to) => normalize_ident(rename_to.as_str()),
                            _ => panic!("rename_to parameter should be TEXT"),
                        }
                    };

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
                                unique,
                                if_not_exists,
                                idx_name,
                                tbl_name,
                                columns,
                                where_clause,
                            } => {
                                let table_name = normalize_ident(tbl_name.as_str());

                                if rename_from != table_name {
                                    break 'sql None;
                                }

                                Some(
                                    ast::Stmt::CreateIndex {
                                        unique,
                                        if_not_exists,
                                        idx_name,
                                        tbl_name: ast::Name::from_str(&rename_to),
                                        columns,
                                        where_clause,
                                    }
                                    .format()
                                    .unwrap(),
                                )
                            }
                            ast::Stmt::CreateTable {
                                temporary,
                                if_not_exists,
                                tbl_name,
                                body,
                            } => {
                                let table_name = normalize_ident(tbl_name.name.as_str());

                                if rename_from != table_name {
                                    break 'sql None;
                                }

                                Some(
                                    ast::Stmt::CreateTable {
                                        temporary,
                                        if_not_exists,
                                        tbl_name: ast::QualifiedName {
                                            db_name: None,
                                            name: ast::Name::from_str(&rename_to),
                                            alias: None,
                                        },
                                        body,
                                    }
                                    .format()
                                    .unwrap(),
                                )
                            }
                            _ => todo!(),
                        }
                    };

                    (new_name, new_tbl_name, new_sql)
                }
                AlterTableFunc::RenameColumn => {
                    let table = {
                        match &state.registers[*start_reg + 5].get_owned_value() {
                            Value::Text(rename_to) => normalize_ident(rename_to.as_str()),
                            _ => panic!("table parameter should be TEXT"),
                        }
                    };

                    let rename_from = {
                        match &state.registers[*start_reg + 6].get_owned_value() {
                            Value::Text(rename_from) => normalize_ident(rename_from.as_str()),
                            _ => panic!("rename_from parameter should be TEXT"),
                        }
                    };

                    let rename_to = {
                        match &state.registers[*start_reg + 7].get_owned_value() {
                            Value::Text(rename_to) => normalize_ident(rename_to.as_str()),
                            _ => panic!("rename_to parameter should be TEXT"),
                        }
                    };

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
                                unique,
                                if_not_exists,
                                idx_name,
                                tbl_name,
                                mut columns,
                                where_clause,
                            } => {
                                if table != normalize_ident(tbl_name.as_str()) {
                                    break 'sql None;
                                }

                                for column in &mut columns {
                                    match &mut column.expr {
                                        ast::Expr::Id(ast::Name::Ident(id))
                                            if normalize_ident(id) == rename_from =>
                                        {
                                            *id = rename_to.clone();
                                        }
                                        _ => {}
                                    }
                                }

                                Some(
                                    ast::Stmt::CreateIndex {
                                        unique,
                                        if_not_exists,
                                        idx_name,
                                        tbl_name,
                                        columns,
                                        where_clause,
                                    }
                                    .format()
                                    .unwrap(),
                                )
                            }
                            ast::Stmt::CreateTable {
                                temporary,
                                if_not_exists,
                                tbl_name,
                                body,
                            } => {
                                if table != normalize_ident(tbl_name.name.as_str()) {
                                    break 'sql None;
                                }

                                let ast::CreateTableBody::ColumnsAndConstraints {
                                    mut columns,
                                    constraints,
                                    options,
                                } = *body
                                else {
                                    todo!()
                                };

                                let column_index = columns
                                    .get_index_of(&ast::Name::from_str(&rename_from))
                                    .expect("column being renamed should be present");

                                let mut column_definition =
                                    columns.get_index(column_index).unwrap().1.clone();

                                column_definition.col_name = ast::Name::from_str(&rename_to);

                                assert!(columns
                                    .insert(
                                        ast::Name::from_str(&rename_to),
                                        column_definition.clone()
                                    )
                                    .is_none());

                                // Swaps indexes with the last one and pops the end, effectively
                                // replacing the entry.
                                columns.swap_remove_index(column_index).unwrap();

                                Some(
                                    ast::Stmt::CreateTable {
                                        temporary,
                                        if_not_exists,
                                        tbl_name,
                                        body: Box::new(
                                            ast::CreateTableBody::ColumnsAndConstraints {
                                                columns,
                                                constraints,
                                                options,
                                            },
                                        ),
                                    }
                                    .format()
                                    .unwrap(),
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

pub fn op_init_coroutine(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::InitCoroutine {
        yield_reg,
        jump_on_definition,
        start_offset,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::EndCoroutine { yield_reg } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Value::Integer(pc) = state.registers[*yield_reg].get_owned_value() {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Yield {
        yield_reg,
        end_offset,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Value::Integer(pc) = state.registers[*yield_reg].get_owned_value() {
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

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum OpInsertState {
    Insert,
    /// Updating last_insert_rowid may return IO, so we need a separate state for it so that we don't
    /// start inserting the same row multiple times.
    UpdateLastRowid,
}

pub fn op_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Insert {
        cursor: cursor_id,
        key_reg,
        record_reg,
        flag,
        table_name: _,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    if state.op_insert_state == OpInsertState::UpdateLastRowid {
        let maybe_rowid = {
            let mut cursor = state.get_cursor(*cursor_id);
            let cursor = cursor.as_btree_mut();
            return_if_io!(cursor.rowid())
        };
        if let Some(rowid) = maybe_rowid {
            program.connection.update_last_rowid(rowid);

            let prev_changes = program.n_change.get();
            program.n_change.set(prev_changes + 1);
        }
        state.op_insert_state = OpInsertState::Insert;
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    {
        let mut cursor_ref = state.get_cursor(*cursor_id);
        let cursor = cursor_ref.as_btree_mut();

        let key = match &state.registers[*key_reg].get_owned_value() {
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

        // In a table insert, if the caller does not pass InsertFlags::REQUIRE_SEEK, they must ensure that a seek has already happened to the correct location.
        // This typically happens by invoking either Insn::NewRowid or Insn::NotExists, because:
        // 1. op_new_rowid() seeks to the end of the table, which is the correct insertion position.
        // 2. op_not_exists() seeks to the position in the table where the target rowid would be inserted.
        let moved_before = !flag.has(InsertFlags::REQUIRE_SEEK);
        return_if_io!(cursor.insert(
            &BTreeKey::new_table_rowid(key, Some(record.as_ref())),
            moved_before
        ));
    }

    // Only update last_insert_rowid for regular table inserts, not schema modifications
    let root_page = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();
        cursor.root_page()
    };
    if root_page != 1 {
        state.op_insert_state = OpInsertState::UpdateLastRowid;
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_int_64(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Int64 {
        _p1,
        out_reg,
        _p3,
        value,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*out_reg] = Register::Value(Value::Integer(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_delete(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Delete { cursor_id } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.delete());
    }
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxDelete {
        cursor_id,
        start_reg,
        num_regs,
        raise_error_if_no_matching_entry,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    loop {
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
                    Ok(SeekInternalResult::IO) => return Ok(InsnFunctionStepResult::IO),
                    Err(e) => return Err(e),
                };

                if !found {
                    // If P5 is not zero, then raise an SQLITE_CORRUPT_INDEX error if no matching index entry is found
                    // Also, do not raise this (self-correcting and non-critical) error if in writable_schema mode.
                    if *raise_error_if_no_matching_entry {
                        let record = make_record(&state.registers, start_reg, num_regs);
                        return Err(LimboError::Corrupt(format!(
                            "IdxDelete: no matching index entry found for record {record:?}"
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
                    let mut cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.rowid())
                };

                if rowid.is_none() && *raise_error_if_no_matching_entry {
                    return Err(LimboError::Corrupt(format!(
                        "IdxDelete: no matching index entry found for record {:?}",
                        make_record(&state.registers, start_reg, num_regs)
                    )));
                }
                state.op_idx_delete_state = Some(OpIdxDeleteState::Deleting);
            }
            Some(OpIdxDeleteState::Deleting) => {
                {
                    let mut cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.delete());
                }
                let n_change = program.n_change.get();
                program.n_change.set(n_change + 1);
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
    /// Optional seek step done before an unique constraint check.
    SeekIfUnique,
    /// Optional unique constraint check done before an insert.
    UniqueConstraintCheck,
    /// Main insert step. This is always performed. Usually the state machine just
    /// skips to this step unless the insertion is made into a unique index.
    Insert { moved_before: bool },
}

pub fn op_idx_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxInsert {
        cursor_id,
        record_reg,
        flags,
        ..
    } = *insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    let record_to_insert = match &state.registers[record_reg] {
        Register::Record(ref r) => r,
        o => {
            return Err(LimboError::InternalError(format!(
                "expected record, got {o:?}"
            )));
        }
    };

    match state.op_idx_insert_state {
        OpIdxInsertState::SeekIfUnique => {
            let (_, cursor_type) = program.cursor_ref.get(cursor_id).unwrap();
            let CursorType::BTreeIndex(index_meta) = cursor_type else {
                panic!("IdxInsert: not a BTreeIndex cursor");
            };
            if !index_meta.unique {
                state.op_idx_insert_state = OpIdxInsertState::Insert {
                    moved_before: false,
                };
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
                    state.op_idx_insert_state = OpIdxInsertState::UniqueConstraintCheck;
                    Ok(InsnFunctionStepResult::Step)
                }
                SeekInternalResult::NotFound => {
                    state.op_idx_insert_state = OpIdxInsertState::Insert { moved_before: true };
                    Ok(InsnFunctionStepResult::Step)
                }
                SeekInternalResult::IO => Ok(InsnFunctionStepResult::IO),
            }
        }
        OpIdxInsertState::UniqueConstraintCheck => {
            let ignore_conflict = 'i: {
                let mut cursor = state.get_cursor(cursor_id);
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
                OpIdxInsertState::SeekIfUnique
            } else {
                OpIdxInsertState::Insert { moved_before: true }
            };
            Ok(InsnFunctionStepResult::Step)
        }
        OpIdxInsertState::Insert { moved_before } => {
            {
                let mut cursor = state.get_cursor(cursor_id);
                let cursor = cursor.as_btree_mut();
                // To make this reentrant in case of `moved_before` = false, we need to check if the previous cursor.insert started
                // a write/balancing operation. If it did, it means we already moved to the place we wanted.
                let moved_before = moved_before
                    || cursor.is_write_in_progress()
                    || flags.has(IdxInsertFlags::USE_SEEK);
                // Start insertion of row. This might trigger a balance procedure which will take care of moving to different pages,
                // therefore, we don't want to seek again if that happens, meaning we don't want to return on io without moving to the following opcode
                // because it could trigger a movement to child page after a balance root which will leave the current page as the root page.
                return_if_io!(
                    cursor.insert(&BTreeKey::new_index_key(record_to_insert), moved_before)
                );
            }
            state.op_idx_insert_state = OpIdxInsertState::SeekIfUnique;
            state.pc += 1;
            // TODO: flag optimizations, update n_change if OPFLAG_NCHANGE
            Ok(InsnFunctionStepResult::Step)
        }
    }
}

#[derive(Debug)]
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NewRowid {
        cursor, rowid_reg, ..
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    if let Some(mv_store) = mv_store {
        let rowid = mv_store.get_next_rowid();
        state.registers[*rowid_reg] = Register::Value(Value::Integer(rowid));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    const MAX_ROWID: i64 = i64::MAX;
    const MAX_ATTEMPTS: u32 = 100;

    loop {
        match &state.op_new_rowid_state {
            OpNewRowidState::Start => {
                state.op_new_rowid_state = OpNewRowidState::SeekingToLast;
            }

            OpNewRowidState::SeekingToLast => {
                {
                    let mut cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.seek_to_last());
                }
                state.op_new_rowid_state = OpNewRowidState::ReadingMaxRowid;
            }

            OpNewRowidState::ReadingMaxRowid => {
                let current_max = {
                    let mut cursor = state.get_cursor(*cursor);
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
                if *attempts >= MAX_ATTEMPTS {
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
                    attempts: *attempts,
                    candidate: random_rowid,
                };
            }

            OpNewRowidState::VerifyingCandidate {
                attempts,
                candidate,
            } => {
                let exists = {
                    let mut cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    let seek_result = return_if_io!(cursor.seek(
                        SeekKey::TableRowId(*candidate),
                        SeekOp::GE { eq_only: true }
                    ));
                    matches!(seek_result, SeekResult::Found)
                };

                if !exists {
                    // Found unused rowid!
                    state.registers[*rowid_reg] = Register::Value(Value::Integer(*candidate));
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
                    let mut cursor = state.get_cursor(*cursor);
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::MustBeInt { reg } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    match &state.registers[*reg].get_owned_value() {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SoftNull { reg } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*reg] = Register::Value(Value::Null);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NoConflict {
        cursor_id,
        target_pc,
        record_reg,
        num_regs,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    loop {
        match &state.op_no_conflict_state {
            OpNoConflictState::Start => {
                let mut cursor_ref = state.get_cursor(*cursor_id);
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

                drop(cursor_ref);

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
                    record_source.clone(),
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
                    SeekInternalResult::IO => Ok(InsnFunctionStepResult::IO),
                };
            }
        }
    }
}

pub fn op_not_exists(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NotExists {
        cursor,
        rowid_reg,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let exists = {
        let mut cursor = must_be_btree_cursor!(*cursor, program.cursor_ref, state, "NotExists");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.exists(state.registers[*rowid_reg].get_owned_value()))
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::OffsetLimit {
        limit_reg,
        combined_reg,
        offset_reg,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let limit_val = match state.registers[*limit_reg].get_owned_value() {
        Value::Integer(val) => val,
        _ => {
            return Err(LimboError::InternalError(
                "OffsetLimit: the value in limit_reg is not an integer".into(),
            ));
        }
    };
    let offset_val = match state.registers[*offset_reg].get_owned_value() {
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
    _pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::OpenWrite {
        cursor_id,
        root_page,
        db,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly);
    }
    let pager = program.get_pager_from_database_index(db);

    let root_page = match root_page {
        RegisterOrLiteral::Literal(lit) => *lit as u64,
        RegisterOrLiteral::Register(reg) => match &state.registers[*reg].get_owned_value() {
            Value::Integer(val) => *val as u64,
            _ => {
                return Err(LimboError::InternalError(
                    "OpenWrite: the value in root_page is not an integer".into(),
                ));
            }
        },
    };
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let mut cursors = state.cursors.borrow_mut();
    let maybe_index = match cursor_type {
        CursorType::BTreeIndex(index) => Some(index),
        _ => None,
    };
    let mv_cursor = match state.mv_tx_id {
        Some(tx_id) => {
            let table_id = root_page;
            let mv_store = mv_store.unwrap().clone();
            let mv_cursor = Rc::new(RefCell::new(
                MvCursor::new(mv_store.clone(), tx_id, table_id, pager.clone()).unwrap(),
            ));
            Some(mv_cursor)
        }
        None => None,
    };
    if let Some(index) = maybe_index {
        let conn = program.connection.clone();
        let schema = conn.schema.borrow();
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
            _ => unreachable!("Expected BTreeTable. This should not have happened."),
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Copy {
        src_reg,
        dst_reg,
        extra_amount,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::CreateBtree { db, root, flags } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    assert_eq!(*db, 0);

    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly);
    }
    if *db > 0 {
        // TODO: implement temp databases
        todo!("temp databases not implemented yet");
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Destroy {
        root,
        former_root_reg,
        is_temp,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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

pub fn op_drop_table(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::DropTable { db, table_name, .. } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
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

pub fn op_close(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Close { cursor_id } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let mut cursors = state.cursors.borrow_mut();
    cursors.get_mut(*cursor_id).unwrap().take();
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_is_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IsNull { reg, target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if matches!(state.registers[*reg], Register::Value(Value::Null)) {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_page_count(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::PageCount { db, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *db > 0 {
        // TODO: implement temp databases
        todo!("temp databases not implemented yet");
    }
    let count = match pager.with_header(|header| header.database_size.get()) {
        Err(_) => 0.into(),
        Ok(IOResult::Done(v)) => v.into(),
        Ok(IOResult::IO) => return Ok(InsnFunctionStepResult::IO),
    };
    state.registers[*dest] = Register::Value(Value::Integer(count));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_parse_schema(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ParseSchema {
        db: _,
        where_clause,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let conn = program.connection.clone();
    // set auto commit to false in order for parse schema to not commit changes as transaction state is stored in connection,
    // and we use the same connection for nested query.
    let previous_auto_commit = conn.auto_commit.get();
    conn.auto_commit.set(false);

    if let Some(where_clause) = where_clause {
        let stmt = conn.prepare(format!("SELECT * FROM sqlite_schema WHERE {where_clause}"))?;

        conn.with_schema_mut(|schema| {
            // TODO: This function below is synchronous, make it async
            parse_schema_rows(stmt, schema, &conn.syms.borrow(), state.mv_tx_id)
        })?;
    } else {
        let stmt = conn.prepare("SELECT * FROM sqlite_schema")?;

        conn.with_schema_mut(|schema| {
            // TODO: This function below is synchronous, make it async
            parse_schema_rows(stmt, schema, &conn.syms.borrow(), state.mv_tx_id)
        })?;
    }
    conn.auto_commit.set(previous_auto_commit);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_read_cookie(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ReadCookie { db, dest, cookie } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *db > 0 {
        // TODO: implement temp databases
        todo!("temp databases not implemented yet");
    }

    let cookie_value = match pager.with_header(|header| match cookie {
        Cookie::ApplicationId => header.application_id.get().into(),
        Cookie::UserVersion => header.user_version.get().into(),
        Cookie::SchemaVersion => header.schema_cookie.get().into(),
        Cookie::LargestRootPageNumber => header.vacuum_mode_largest_root_page.get().into(),
        cookie => todo!("{cookie:?} is not yet implement for ReadCookie"),
    }) {
        Err(_) => 0.into(),
        Ok(IOResult::Done(v)) => v,
        Ok(IOResult::IO) => return Ok(InsnFunctionStepResult::IO),
    };

    state.registers[*dest] = Register::Value(Value::Integer(cookie_value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_set_cookie(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SetCookie {
        db,
        cookie,
        value,
        p5,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *db > 0 {
        todo!("temp databases not implemented yet");
    }

    return_if_io!(pager.with_header_mut(|header| {
        match cookie {
            Cookie::ApplicationId => header.application_id = (*value).into(),
            Cookie::UserVersion => header.user_version = (*value).into(),
            Cookie::LargestRootPageNumber => {
                header.vacuum_mode_largest_root_page = (*value as u32).into();
            }
            Cookie::IncrementalVacuum => {
                header.incremental_vacuum_enabled = (*value as u32).into()
            }
            Cookie::SchemaVersion => {
                if mv_store.is_none() {
                    // we update transaction state to indicate that the schema has changed
                    match program.connection.transaction_state.get() {
                        TransactionState::Write { schema_did_change } => {
                            program.connection.transaction_state.set(TransactionState::Write { schema_did_change: true });
                        },
                        TransactionState::Read => unreachable!("invalid transaction state for SetCookie: TransactionState::Read, should be write"),
                        TransactionState::None => unreachable!("invalid transaction state for SetCookie: TransactionState::None, should be write"),
                        TransactionState::PendingUpgrade => unreachable!("invalid transaction state for SetCookie: TransactionState::PendingUpgrade, should be write"),
                    }
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ShiftRight { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_shift_right(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_shift_left(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ShiftLeft { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_shift_left(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add_imm(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::AddImm { register, value } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };

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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Variable { index, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(state.get_parameter(*index));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_zero_or_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ZeroOrNull { rg1, rg2, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *state.registers[*rg1].get_owned_value() == Value::Null
        || *state.registers[*rg2].get_owned_value() == Value::Null
    {
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Not { reg, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] =
        Register::Value(state.registers[*reg].get_owned_value().exec_boolean_not());
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_concat(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Concat { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_concat(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_and(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::And { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_and(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_or(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Or { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::Value(
        state.registers[*lhs]
            .get_owned_value()
            .exec_or(state.registers[*rhs].get_owned_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_noop(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    // Do nothing
    // Advance the program counter for the next opcode
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub enum OpOpenEphemeralState {
    Start,
    StartingTxn { pager: Rc<Pager> },
    CreateBtree { pager: Rc<Pager> },
}
pub fn op_open_ephemeral(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
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
    match &state.op_open_ephemeral_state {
        OpOpenEphemeralState::Start => {
            tracing::trace!("Start");
            let conn = program.connection.clone();
            let io = conn.pager.borrow().io.clone();
            let rand_num = io.generate_random_number();
            let temp_dir = temp_dir();
            let rand_path =
                std::path::Path::new(&temp_dir).join(format!("tursodb-ephemeral-{rand_num}"));
            let Some(rand_path_str) = rand_path.to_str() else {
                return Err(LimboError::InternalError(
                    "Failed to convert path to string".to_string(),
                ));
            };
            let file = io.open_file(rand_path_str, OpenFlags::Create, false)?;
            let db_file = Arc::new(DatabaseFile::new(file));

            let page_size = pager
                .io
                .block(|| pager.with_header(|header| header.page_size))?
                .get();

            let buffer_pool = Arc::new(BufferPool::new(Some(page_size as usize)));
            let page_cache = Arc::new(RwLock::new(DumbLruPageCache::default()));

            let pager = Rc::new(Pager::new(
                db_file,
                None,
                io,
                page_cache,
                buffer_pool.clone(),
                Arc::new(AtomicDbState::new(DbState::Uninitialized)),
                Arc::new(Mutex::new(())),
            )?);

            let page_size = pager
                .io
                .block(|| pager.with_header(|header| header.page_size))
                .unwrap_or_default()
                .get() as usize;
            buffer_pool.set_page_size(page_size);

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
            let mv_cursor = match state.mv_tx_id {
                Some(tx_id) => {
                    let table_id = root_page as u64;
                    let mv_store = mv_store.unwrap().clone();
                    let mv_cursor = Rc::new(RefCell::new(
                        MvCursor::new(mv_store.clone(), tx_id, table_id, pager.clone()).unwrap(),
                    ));
                    Some(mv_cursor)
                }
                None => None,
            };

            let num_columns = match cursor_type {
                CursorType::BTreeTable(table_rc) => table_rc.columns.len(),
                CursorType::BTreeIndex(index_arc) => index_arc.columns.len(),
                _ => unreachable!("This should not have happened"),
            };

            let mut cursor = if let CursorType::BTreeIndex(index) = cursor_type {
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
            let res = cursor.rewind()?; // Will never return io

            let mut cursors: std::cell::RefMut<'_, Vec<Option<Cursor>>> =
                state.cursors.borrow_mut();

            // Table content is erased if the cursor already exists
            match cursor_type {
                CursorType::BTreeTable(_) => {
                    cursors
                        .get_mut(cursor_id)
                        .unwrap()
                        .replace(Cursor::new_btree(cursor));
                }
                CursorType::BTreeIndex(_) => {
                    cursors
                        .get_mut(cursor_id)
                        .unwrap()
                        .replace(Cursor::new_btree(cursor));
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
            }

            state.pc += 1;
            state.op_open_ephemeral_state = OpOpenEphemeralState::Start;
        }
    }

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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Once {
        target_pc_when_reentered,
    } = insn
    else {
        unreachable!("unexpected Insn: {:?}", insn)
    };
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
    pager: &Rc<Pager>,
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
        Ok(SeekInternalResult::IO) => return Ok(InsnFunctionStepResult::IO),
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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Affinity {
        start_reg,
        count,
        affinities,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    if affinities.len() != count.get() {
        return Err(LimboError::InternalError(
            "Affinity: the length of affinities does not match the count".into(),
        ));
    }

    for (i, affinity_char) in affinities.chars().enumerate().take(count.get()) {
        let reg_index = *start_reg + i;

        let affinity = Affinity::from_char(affinity_char)?;

        apply_affinity_char(&mut state.registers[reg_index], affinity);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_count(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Count {
        cursor_id,
        target_reg,
        exact,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    let count = {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Count");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.count())
    };

    state.registers[*target_reg] = Register::Value(Value::Integer(count as i64));

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
    pager: &Rc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IntegrityCk {
        max_errors,
        roots,
        message_register,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    match &mut state.op_integrity_check_state {
        OpIntegrityCheckState::Start => {
            state.op_integrity_check_state = OpIntegrityCheckState::Checking {
                errors: Vec::new(),
                current_root_idx: 0,
                state: IntegrityCheckState::new(roots[0]),
            };
        }
        OpIntegrityCheckState::Checking {
            errors,
            current_root_idx,
            state: integrity_check_state,
        } => {
            return_if_io!(integrity_check(integrity_check_state, errors, pager));
            *current_root_idx += 1;
            if *current_root_idx < roots.len() {
                *integrity_check_state = IntegrityCheckState::new(roots[*current_root_idx]);
                return Ok(InsnFunctionStepResult::Step);
            } else {
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
    _pager: &Rc<Pager>,
    _mv_store: Option<&Arc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Cast { reg, affinity } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    let value = state.registers[*reg].get_owned_value().clone();
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

impl Value {
    pub fn exec_lower(&self) -> Option<Self> {
        match self {
            Value::Text(t) => Some(Value::build_text(t.as_str().to_lowercase())),
            t => Some(t.to_owned()),
        }
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
        match self {
            Value::Text(t) => Some(Value::build_text(t.as_str().to_uppercase())),
            t => Some(t.to_owned()),
        }
    }

    pub fn exec_sign(&self) -> Option<Value> {
        let num = match self {
            Value::Integer(i) => *i as f64,
            Value::Float(f) => *f,
            Value::Text(s) => {
                if let Ok(i) = s.as_str().parse::<i64>() {
                    i as f64
                } else if let Ok(f) = s.as_str().parse::<f64>() {
                    f
                } else {
                    return Some(Value::Null);
                }
            }
            Value::Blob(b) => match std::str::from_utf8(b) {
                Ok(s) => {
                    if let Ok(i) = s.parse::<i64>() {
                        i as f64
                    } else if let Ok(f) = s.parse::<f64>() {
                        f
                    } else {
                        return Some(Value::Null);
                    }
                }
                Err(_) => return Some(Value::Null),
            },
            _ => return Some(Value::Null),
        };

        let sign = if num > 0.0 {
            1
        } else if num < 0.0 {
            -1
        } else {
            0
        };

        Some(Value::Integer(sign))
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
        match self {
            Value::Integer(x) => {
                match i64::checked_abs(*x) {
                    Some(y) => Ok(Value::Integer(y)),
                    // Special case: if we do the abs of "-9223372036854775808", it causes overflow.
                    // return IntegerOverflow error
                    None => Err(LimboError::IntegerOverflow),
                }
            }
            Value::Float(x) => {
                if x < &0.0 {
                    Ok(Value::Float(-x))
                } else {
                    Ok(Value::Float(*x))
                }
            }
            Value::Null => Ok(Value::Null),
            _ => Ok(Value::Float(0.0)),
        }
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
            _ => Value::Null,
        }
    }

    pub fn exec_unhex(&self, ignored_chars: Option<&Value>) -> Value {
        match self {
            Value::Null => Value::Null,
            _ => match ignored_chars {
                None => match hex::decode(self.to_string()) {
                    Ok(bytes) => Value::Blob(bytes),
                    Err(_) => Value::Null,
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
                            Err(_) => Value::Null,
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

    fn _to_float(&self) -> f64 {
        match self {
            Value::Text(x) => match cast_text_to_numeric(x.as_str()) {
                Value::Integer(i) => i as f64,
                Value::Float(f) => f,
                _ => unreachable!(),
            },
            Value::Integer(x) => *x as f64,
            Value::Float(x) => *x,
            _ => 0.0,
        }
    }

    pub fn exec_round(&self, precision: Option<&Value>) -> Value {
        let reg = self._to_float();
        let round = |reg: f64, f: f64| {
            let precision = if f < 1.0 { 0.0 } else { f };
            Value::Float(reg.round_to_precision(precision as i32))
        };
        match precision {
            Some(Value::Text(x)) => match cast_text_to_numeric(x.as_str()) {
                Value::Integer(i) => round(reg, i as f64),
                Value::Float(f) => round(reg, f),
                _ => unreachable!(),
            },
            Some(Value::Integer(i)) => round(reg, *i as f64),
            Some(Value::Float(f)) => round(reg, *f),
            None => round(reg, 0.0),
            _ => Value::Null,
        }
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
                    // Convert BLOB to TEXT first
                    let text = String::from_utf8_lossy(b);
                    cast_text_to_real(&text)
                }
                Value::Text(t) => cast_text_to_real(t.as_str()),
                Value::Integer(i) => Value::Float(*i as f64),
                Value::Float(f) => Value::Float(*f),
                _ => Value::Float(0.0),
            },
            Affinity::Integer => match self {
                Value::Blob(b) => {
                    // Convert BLOB to TEXT first
                    let text = String::from_utf8_lossy(b);
                    cast_text_to_integer(&text)
                }
                Value::Text(t) => cast_text_to_integer(t.as_str()),
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
                Value::Blob(b) => {
                    let text = String::from_utf8_lossy(b);
                    cast_text_to_numeric(&text)
                }
                Value::Text(t) => cast_text_to_numeric(t.as_str()),
                Value::Integer(i) => Value::Integer(*i),
                Value::Float(f) => Value::Float(*f),
                _ => self.clone(), // TODO probably wrong
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

    fn to_f64(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Text(t) => t.as_str().parse::<f64>().ok(),
            _ => None,
        }
    }

    fn exec_math_unary(&self, function: &MathFunc) -> Value {
        // In case of some functions and integer input, return the input as is
        if let Value::Integer(_) = self {
            if matches! { function, MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc }
            {
                return self.clone();
            }
        }

        let f = match self.to_f64() {
            Some(f) => f,
            None => return Value::Null,
        };

        let result = match function {
            MathFunc::Acos => libm::acos(f),
            MathFunc::Acosh => libm::acosh(f),
            MathFunc::Asin => libm::asin(f),
            MathFunc::Asinh => libm::asinh(f),
            MathFunc::Atan => libm::atan(f),
            MathFunc::Atanh => libm::atanh(f),
            MathFunc::Ceil | MathFunc::Ceiling => libm::ceil(f),
            MathFunc::Cos => libm::cos(f),
            MathFunc::Cosh => libm::cosh(f),
            MathFunc::Degrees => f.to_degrees(),
            MathFunc::Exp => libm::exp(f),
            MathFunc::Floor => libm::floor(f),
            MathFunc::Ln => libm::log(f),
            MathFunc::Log10 => libm::log10(f),
            MathFunc::Log2 => libm::log2(f),
            MathFunc::Radians => f.to_radians(),
            MathFunc::Sin => libm::sin(f),
            MathFunc::Sinh => libm::sinh(f),
            MathFunc::Sqrt => libm::sqrt(f),
            MathFunc::Tan => libm::tan(f),
            MathFunc::Tanh => libm::tanh(f),
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
        let lhs = match self.to_f64() {
            Some(f) => f,
            None => return Value::Null,
        };

        let rhs = match rhs.to_f64() {
            Some(f) => f,
            None => return Value::Null,
        };

        let result = match function {
            MathFunc::Atan2 => libm::atan2(lhs, rhs),
            MathFunc::Mod => libm::fmod(lhs, rhs),
            MathFunc::Pow | MathFunc::Power => libm::pow(lhs, rhs),
            _ => unreachable!("Unexpected mathematical binary function {:?}", function),
        };

        if result.is_nan() {
            Value::Null
        } else {
            Value::Float(result)
        }
    }

    fn exec_math_log(&self, base: Option<&Value>) -> Value {
        let f = match self.to_f64() {
            Some(f) => f,
            None => return Value::Null,
        };

        let base = match base {
            Some(base) => match base.to_f64() {
                Some(f) => f,
                None => return Value::Null,
            },
            None => 10.0,
        };

        if base == 2.0 {
            return Value::Float(libm::log2(f));
        } else if base == 10.0 {
            return Value::Float(libm::log10(f));
        };

        if f <= 0.0 || base <= 0.0 || base == 1.0 {
            return Value::Null;
        }
        let log_x = libm::log(f);
        let log_base = libm::log(base);
        let result = log_x / log_base;
        Value::Float(result)
    }

    fn exec_likely(&self) -> Value {
        self.clone()
    }

    fn exec_likelihood(&self, _probability: &Value) -> Value {
        self.clone()
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
        match (self, rhs) {
            (Value::Text(lhs_text), Value::Text(rhs_text)) => {
                Value::build_text(lhs_text.as_str().to_string() + rhs_text.as_str())
            }
            (Value::Text(lhs_text), Value::Integer(rhs_int)) => {
                Value::build_text(lhs_text.as_str().to_string() + &rhs_int.to_string())
            }
            (Value::Text(lhs_text), Value::Float(rhs_float)) => {
                Value::build_text(lhs_text.as_str().to_string() + &rhs_float.to_string())
            }
            (Value::Integer(lhs_int), Value::Text(rhs_text)) => {
                Value::build_text(lhs_int.to_string() + rhs_text.as_str())
            }
            (Value::Integer(lhs_int), Value::Integer(rhs_int)) => {
                Value::build_text(lhs_int.to_string() + &rhs_int.to_string())
            }
            (Value::Integer(lhs_int), Value::Float(rhs_float)) => {
                Value::build_text(lhs_int.to_string() + &rhs_float.to_string())
            }
            (Value::Float(lhs_float), Value::Text(rhs_text)) => {
                Value::build_text(lhs_float.to_string() + rhs_text.as_str())
            }
            (Value::Float(lhs_float), Value::Integer(rhs_int)) => {
                Value::build_text(lhs_float.to_string() + &rhs_int.to_string())
            }
            (Value::Float(lhs_float), Value::Float(rhs_float)) => {
                Value::build_text(lhs_float.to_string() + &rhs_float.to_string())
            }
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (Value::Blob(_), _) | (_, Value::Blob(_)) => {
                todo!("TODO: Handle Blob conversion to String")
            }
        }
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
        match reg.get_owned_value() {
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

    let separator = match registers[0].get_owned_value() {
        Value::Null | Value::Blob(_) => return Value::Null,
        v => format!("{v}"),
    };

    let parts = registers[1..]
        .iter()
        .filter_map(|reg| match reg.get_owned_value() {
            Value::Text(_) | Value::Integer(_) | Value::Float(_) => {
                Some(format!("{}", reg.get_owned_value()))
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
            if let Value::Integer(i) = x.get_owned_value() {
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
                    let text = t.as_str();

                    // Handle hex numbers - they shouldn't be converted
                    if text.starts_with("0x") {
                        return false;
                    }

                    // Try to parse as number (similar to applyNumericAffinity)
                    let Ok(num) = checked_cast_text_to_numeric(text) else {
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
    matches!(reg.get_owned_value(), Value::Integer(_) | Value::Float(_))
}

fn stringify_register(reg: &mut Register) -> bool {
    match reg.get_owned_value() {
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
        assert_eq!(*reg.get_owned_value(), Value::Integer(123));

        let mut reg = Register::Value(Value::Text("123.45".into()));
        assert!(apply_numeric_affinity(&mut reg, false));
        assert_eq!(*reg.get_owned_value(), Value::Float(123.45));

        let mut reg = Register::Value(Value::Text("  -456  ".into()));
        assert!(apply_numeric_affinity(&mut reg, false));
        assert_eq!(*reg.get_owned_value(), Value::Integer(-456));

        let mut reg = Register::Value(Value::Text("0".into()));
        assert!(apply_numeric_affinity(&mut reg, false));
        assert_eq!(*reg.get_owned_value(), Value::Integer(0));
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
            Value::exec_min(input_int_vec.iter().map(|v| v.get_owned_value())),
            Value::Integer(-1)
        );
        assert_eq!(
            Value::exec_max(input_int_vec.iter().map(|v| v.get_owned_value())),
            Value::Integer(10)
        );

        let str1 = Register::Value(Value::build_text("A"));
        let str2 = Register::Value(Value::build_text("z"));
        let input_str_vec = [str2, str1.clone()];
        assert_eq!(
            Value::exec_min(input_str_vec.iter().map(|v| v.get_owned_value())),
            Value::build_text("A")
        );
        assert_eq!(
            Value::exec_max(input_str_vec.iter().map(|v| v.get_owned_value())),
            Value::build_text("z")
        );

        let input_null_vec = [Register::Value(Value::Null), Register::Value(Value::Null)];
        assert_eq!(
            Value::exec_min(input_null_vec.iter().map(|v| v.get_owned_value())),
            Value::Null
        );
        assert_eq!(
            Value::exec_max(input_null_vec.iter().map(|v| v.get_owned_value())),
            Value::Null
        );

        let input_mixed_vec = [Register::Value(Value::Integer(10)), str1];
        assert_eq!(
            Value::exec_min(input_mixed_vec.iter().map(|v| v.get_owned_value())),
            Value::Integer(10)
        );
        assert_eq!(
            Value::exec_max(input_mixed_vec.iter().map(|v| v.get_owned_value())),
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
        assert_eq!(input_int.exec_upper().unwrap(), input_int);
        assert_eq!(Value::Null.exec_upper().unwrap(), Value::Null)
    }

    #[test]
    fn test_lower_case() {
        let input_str = Value::build_text("Limbo");
        let expected_str = Value::build_text("limbo");
        assert_eq!(input_str.exec_lower().unwrap(), expected_str);

        let input_int = Value::Integer(10);
        assert_eq!(input_int.exec_lower().unwrap(), input_int);
        assert_eq!(Value::Null.exec_lower().unwrap(), Value::Null)
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
        let expected = Some(Value::Null);
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
        let expected = Some(Value::Null);
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"42".to_vec());
        let expected = Some(Value::Integer(1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"-42".to_vec());
        let expected = Some(Value::Integer(-1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"0".to_vec());
        let expected = Some(Value::Integer(0));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Null;
        let expected = Some(Value::Null);
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
    fn test_likely() {
        let input = Value::build_text("limbo");
        let expected = Value::build_text("limbo");
        assert_eq!(input.exec_likely(), expected);

        let input = Value::Integer(100);
        let expected = Value::Integer(100);
        assert_eq!(input.exec_likely(), expected);

        let input = Value::Float(12.34);
        let expected = Value::Float(12.34);
        assert_eq!(input.exec_likely(), expected);

        let input = Value::Null;
        let expected = Value::Null;
        assert_eq!(input.exec_likely(), expected);

        let input = Value::Blob(vec![1, 2, 3, 4]);
        let expected = Value::Blob(vec![1, 2, 3, 4]);
        assert_eq!(input.exec_likely(), expected);
    }

    #[test]
    fn test_likelihood() {
        let value = Value::build_text("limbo");
        let prob = Value::Float(0.5);
        assert_eq!(value.exec_likelihood(&prob), value);

        let value = Value::build_text("database");
        let prob = Value::Float(0.9375);
        assert_eq!(value.exec_likelihood(&prob), value);

        let value = Value::Integer(100);
        let prob = Value::Float(1.0);
        assert_eq!(value.exec_likelihood(&prob), value);

        let value = Value::Float(12.34);
        let prob = Value::Float(0.5);
        assert_eq!(value.exec_likelihood(&prob), value);

        let value = Value::Null;
        let prob = Value::Float(0.5);
        assert_eq!(value.exec_likelihood(&prob), value);

        let value = Value::Blob(vec![1, 2, 3, 4]);
        let prob = Value::Float(0.5);
        assert_eq!(value.exec_likelihood(&prob), value);

        let prob = Value::build_text("0.5");
        assert_eq!(value.exec_likelihood(&prob), value);

        let prob = Value::Null;
        assert_eq!(value.exec_likelihood(&prob), value);
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
