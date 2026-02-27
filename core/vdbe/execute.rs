use crate::error::SQLITE_CONSTRAINT_UNIQUE;
use crate::function::AlterTableFunc;
use crate::mvcc::cursor::{MvccCursorType, NextRowidResult};
use crate::mvcc::database::CheckpointStateMachine;
use crate::mvcc::LocalClock;
use crate::numeric::Numeric;
use crate::schema::{Table, SQLITE_SEQUENCE_TABLE_NAME};
use crate::state_machine::StateMachine;
use crate::storage::btree::{
    integrity_check, CursorTrait, IntegrityCheckError, IntegrityCheckState, PageCategory,
};
use crate::storage::database::DatabaseFile;
use crate::storage::journal_mode;
use crate::storage::page_cache::PageCache;
use crate::storage::pager::{default_page1, CreateBTreeFlags, PageRef, SavepointResult};
use crate::storage::sqlite3_ondisk::{DatabaseHeader, PageSize, RawVersion};
use crate::translate::collate::CollationSeq;
use crate::translate::pragma::TURSO_CDC_VERSION_TABLE_NAME;
use crate::types::{
    compare_immutable, compare_records_generic, AsValueRef, Extendable, IOCompletions, IOResult,
    ImmutableRecord, IndexInfo, SeekResult, Text,
};
use crate::util::{
    normalize_ident, rename_identifiers, rewrite_check_expr_table_refs,
    rewrite_column_references_if_needed, rewrite_fk_parent_cols_if_self_ref,
    rewrite_fk_parent_table_if_needed, rewrite_inline_col_fk_target_if_needed,
    trim_ascii_whitespace,
};
use crate::vdbe::affinity::{
    apply_numeric_affinity, try_for_float, Affinity, NumericParseResult, ParsedNumber,
};
use crate::vdbe::hash_table::{HashEntry, HashTable, HashTableConfig, DEFAULT_MEM_BUDGET};
use crate::vdbe::insn::InsertFlags;
use crate::vdbe::metrics::HashJoinMetrics;
use crate::vdbe::value::ComparisonOp;
use crate::vdbe::{
    registers_to_ref_values, DeferredSeekState, EndStatement, OpHashBuildState, OpHashProbeState,
    StepResult, TxnCleanup,
};
use crate::vector::{
    vector1bit, vector32, vector32_sparse, vector64, vector8, vector_concat, vector_distance_cos,
    vector_distance_dot, vector_distance_jaccard, vector_distance_l2, vector_extract, vector_slice,
};
use crate::{
    error::{
        LimboError, SQLITE_CONSTRAINT, SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_FOREIGNKEY,
        SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_TRIGGER,
        SQLITE_ERROR,
    },
    ext::ExtValue,
    function::{AggFunc, ExtFunc, MathFunc, MathFuncArity, ScalarFunc, VectorFunc},
    functions::{
        datetime::{
            exec_date, exec_datetime_full, exec_julianday, exec_strftime, exec_time, exec_unixepoch,
        },
        printf::exec_printf,
    },
    stats::StatAccum,
    translate::emitter::TransactionMode,
};
use crate::{
    get_cursor, CaptureDataChangesInfo, CheckpointMode, Completion, Connection, DatabaseStorage,
    IOExt, MvCursor, QueryMode,
};
use crate::{CdcVersion, Statement};
use either::Either;
use smallvec::SmallVec;
use std::any::Any;
use std::env::temp_dir;
use std::str::FromStr;
use std::{
    borrow::BorrowMut,
    num::NonZero,
    sync::{atomic::Ordering, Arc},
};
use turso_macros::match_ignore_ascii_case;

use crate::pseudo::PseudoCursor;

use crate::storage::btree::{BTreeCursor, BTreeKey};

use crate::{
    storage::wal::CheckpointResult,
    types::{AggContext, Cursor, ExternalAggState, SeekKey, SeekOp, SumAggState, Value, ValueType},
    util::{cast_real_to_integer, checked_cast_text_to_numeric, parse_schema_rows},
    vdbe::{
        builder::CursorType,
        insn::{IdxInsertFlags, Insn, SavepointOp},
    },
};

use crate::{connection::Row, info, turso_assert, OpenFlags, TransactionState, ValueRef};

use super::{
    insn::{Cookie, RegisterOrLiteral},
    CommitState,
};
use crate::sync::{Mutex, RwLock};
use turso_parser::ast::{self, ForeignKeyClause, Name, ResolveType};
use turso_parser::parser::Parser;

use super::sorter::Sorter;

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
use crate::connection::resolve_ext_path;
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

macro_rules! check_arg_count {
    ($actual:expr, $expected:expr) => {
        if $actual != $expected {
            return Err(LimboError::InternalError(format!(
                "expected {} argument(s), got {}",
                $expected, $actual
            )));
        }
    };
}

pub type InsnFunction =
    fn(&Program, &mut ProgramState, &Insn, &Arc<Pager>) -> Result<InsnFunctionStepResult>;

/// Parse a Value (text, int, float, or blob) into a BigDecimal.
fn value_to_bigdecimal(val: &Value) -> Result<bigdecimal::BigDecimal> {
    use bigdecimal::BigDecimal;
    use std::str::FromStr;
    match val {
        Value::Numeric(Numeric::Integer(i)) => Ok(BigDecimal::from(*i)),
        Value::Numeric(Numeric::Float(f)) => BigDecimal::from_str(&f.to_string())
            .map_err(|_| LimboError::Constraint(format!("invalid numeric value: {f}"))),
        Value::Text(t) => BigDecimal::from_str(&t.value)
            .map_err(|_| LimboError::Constraint(format!("invalid numeric value: \"{}\"", t.value))),
        Value::Blob(b) => crate::numeric::decimal::blob_to_bigdecimal(b),
        _ => Err(LimboError::Constraint(format!(
            "cannot convert to numeric: \"{val}\""
        ))),
    }
}

/// Create a sort comparator closure from a custom type `<` operator function name.
/// Returns None if the function name is not recognized as a comparator.
fn make_sort_comparator(func_name: &str) -> Option<crate::vdbe::sorter::SortComparator> {
    use crate::types::ValueRef;
    use std::cmp::Ordering;
    match func_name {
        "numeric_lt" => Some(std::sync::Arc::new(
            |a: &ValueRef, b: &ValueRef| -> Ordering {
                match (a, b) {
                    (ValueRef::Null, ValueRef::Null) => Ordering::Equal,
                    (ValueRef::Null, _) => Ordering::Less,
                    (_, ValueRef::Null) => Ordering::Greater,
                    _ => {
                        // Decode from ValueRef to Value for value_to_bigdecimal
                        let a_val = a.to_owned();
                        let b_val = b.to_owned();
                        match (value_to_bigdecimal(&a_val), value_to_bigdecimal(&b_val)) {
                            (Ok(a_dec), Ok(b_dec)) => a_dec.cmp(&b_dec),
                            _ => a.partial_cmp(b).unwrap_or(Ordering::Equal),
                        }
                    }
                }
            },
        )),
        "string_reverse" => Some(std::sync::Arc::new(
            |a: &ValueRef, b: &ValueRef| -> Ordering {
                fn reverse_str(v: &ValueRef) -> String {
                    match v {
                        ValueRef::Text(t) => t.to_string().chars().rev().collect(),
                        _ => String::new(),
                    }
                }
                match (a, b) {
                    (ValueRef::Null, ValueRef::Null) => Ordering::Equal,
                    (ValueRef::Null, _) => Ordering::Less,
                    (_, ValueRef::Null) => Ordering::Greater,
                    _ => reverse_str(a).cmp(&reverse_str(b)),
                }
            },
        )),
        "test_uint_lt" => Some(std::sync::Arc::new(
            |a: &ValueRef, b: &ValueRef| -> Ordering {
                fn to_u64(v: &ValueRef) -> Option<u64> {
                    match v {
                        ValueRef::Null => None,
                        ValueRef::Numeric(Numeric::Integer(i)) => {
                            if *i >= 0 {
                                Some(*i as u64)
                            } else {
                                None
                            }
                        }
                        ValueRef::Text(t) => t.to_string().parse::<u64>().ok(),
                        _ => None,
                    }
                }
                match (a, b) {
                    (ValueRef::Null, ValueRef::Null) => Ordering::Equal,
                    (ValueRef::Null, _) => Ordering::Less,
                    (_, ValueRef::Null) => Ordering::Greater,
                    _ => match (to_u64(a), to_u64(b)) {
                        (Some(a), Some(b)) => a.cmp(&b),
                        _ => a.partial_cmp(b).unwrap_or(Ordering::Equal),
                    },
                }
            },
        )),
        _ => None,
    }
}

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
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Init { target_pc }, insn);
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(DropIndex { index, db }, insn);
    let conn = program.connection.clone();
    let is_mvcc = conn.mv_store_for_db(*db).is_some();
    conn.with_database_schema_mut(*db, |schema| {
        // In MVCC mode, track dropped index root pages so integrity_check knows about them.
        // The btree pages won't be freed until checkpoint, so integrity_check needs to
        // include them to avoid "page never used" false positives.
        if is_mvcc && index.root_page > 0 {
            schema.dropped_root_pages.insert(index.root_page);
        }
        schema.remove_index(index);
    });
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_remainder(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(BitNot { reg, dest }, insn);
    state.registers[*dest] = Register::Value(state.registers[*reg].get_value().exec_bit_not());
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_checkpoint(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Checkpoint {
            database,
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
    let pager = program.get_pager_from_database_index(database);
    // In autocommit mode, this statement can still hold an implicit read tx.
    // RESTART/TRUNCATE checkpoint needs to restart WAL and may fail with Busy
    // if we keep our own statement read slot while checkpointing.
    if matches!(
        checkpoint_mode,
        CheckpointMode::Restart | CheckpointMode::Truncate { .. }
    ) && pager.holds_read_lock()
    {
        pager.end_read_tx();
    }
    // Re-fetch mv_store from connection to get the latest value.
    // This is necessary because the mv_store may have been set by a preceding JournalMode instruction
    // (e.g., when switching from WAL to MVCC mode via `PRAGMA journal_mode = "experimental_mvcc"`).
    let mv_store = program.connection.mv_store_for_db(*database);
    if let Some(mv_store) = mv_store.as_ref() {
        if !matches!(checkpoint_mode, CheckpointMode::Truncate { .. }) {
            return Err(LimboError::InvalidArgument(
                "Only TRUNCATE checkpoint mode is supported for MVCC".to_string(),
            ));
        }
        use crate::state_machine::{StateTransition, TransitionResult};
        let mut ckpt_sm = CheckpointStateMachine::new(
            pager.clone(),
            mv_store.clone(),
            program.connection.clone(),
            true,
            program.connection.get_sync_mode(),
        );
        let CheckpointResult {
            wal_max_frame,
            wal_total_backfilled,
            ..
        } = loop {
            match ckpt_sm.step(&()) {
                Ok(TransitionResult::Continue) => {}
                Ok(TransitionResult::Done(result)) => break result,
                Ok(TransitionResult::Io(iocompletions)) => {
                    if let Err(err) = iocompletions.wait(pager.io.as_ref()) {
                        ckpt_sm.cleanup_after_external_io_error();
                        return Err(err);
                    }
                }
                Err(err) => return Err(err),
            }
        };
        // https://sqlite.org/pragma.html#pragma_wal_checkpoint
        // 1st col: 1 (checkpoint SQLITE_BUSY) or 0 (not busy).
        state.registers[*dest] = Register::Value(Value::from_i64(0));
        // 2nd col: # modified pages written to wal file
        state.registers[*dest + 1] = Register::Value(Value::from_i64(wal_max_frame as i64));
        // 3rd col: # pages moved to db after checkpoint
        state.registers[*dest + 2] = Register::Value(Value::from_i64(wal_total_backfilled as i64));

        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    let step_result = pager.checkpoint(*checkpoint_mode, program.connection.get_sync_mode(), true);
    match step_result {
        Ok(IOResult::Done(CheckpointResult {
            wal_max_frame,
            wal_total_backfilled,
            ..
        })) => {
            // https://sqlite.org/pragma.html#pragma_wal_checkpoint
            // 1st col: 1 (checkpoint SQLITE_BUSY) or 0 (not busy).
            state.registers[*dest] = Register::Value(Value::from_i64(0));
            // 2nd col: # modified pages written to wal file
            state.registers[*dest + 1] = Register::Value(Value::from_i64(wal_max_frame as i64));
            // 3rd col: # pages moved to db after checkpoint
            state.registers[*dest + 2] =
                Register::Value(Value::from_i64(wal_total_backfilled as i64));

            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        Ok(IOResult::IO(io)) => Ok(InsnFunctionStepResult::IO(io)),
        Err(err) => {
            tracing::error!("PRAGMA wal_checkpoint failed: {err:?}");
            pager.clear_checkpoint_state();
            state.registers[*dest] = Register::Value(Value::from_i64(1));
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
    }
}

pub fn op_null(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    match insn {
        Insn::Null { dest, dest_end } | Insn::BeginSubrtn { dest, dest_end } => {
            if let Some(dest_end) = dest_end {
                for i in *dest..=*dest_end {
                    state.registers[i] = Register::Value(Value::Null);
                    // Clear any associated RowSet so it can be reused in a fresh
                    // state.  In SQLite the RowSet lives inside the register and
                    // is destroyed by OP_Null; we keep RowSets in a side map, so
                    // we must remove them explicitly.
                    state.rowsets.remove(&i);
                }
            } else {
                state.registers[*dest] = Register::Value(Value::Null);
                state.rowsets.remove(dest);
            }
        }
        _ => unreachable!("unexpected Insn {:?}", insn),
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_null_row(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(NullRow { cursor_id }, insn);
    state.get_cursor(*cursor_id).set_null_flag(true);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_compare(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Compare {
            start_reg_a,
            start_reg_b,
            count,
            key_info,
        },
        insn
    );
    let start_reg_a = *start_reg_a;
    let start_reg_b = *start_reg_b;
    let count = *count;

    if start_reg_a + count > start_reg_b {
        return Err(LimboError::InternalError(
            "Compare registers overlap".to_string(),
        ));
    }

    // (https://github.com/tursodatabase/turso/issues/2304): reusing logic from compare_immutable().
    // TODO: There are tons of cases like this where we could reuse this in a similar vein
    let a_range =
        (start_reg_a..start_reg_a + count + 1).map(|idx| state.registers[idx].get_value());
    let b_range =
        (start_reg_b..start_reg_b + count + 1).map(|idx| state.registers[idx].get_value());
    let cmp = compare_immutable(a_range, b_range, key_info);

    state.last_compare = Some(cmp);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_jump(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    let target_pc = match cmp.expect("comparison should succeed for valid operands") {
        std::cmp::Ordering::Less => *target_pc_lt,
        std::cmp::Ordering::Equal => *target_pc_eq,
        std::cmp::Ordering::Greater => *target_pc_gt,
    };
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_move(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IfPos {
            reg,
            target_pc,
            decrement_by,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    let reg = *reg;
    let target_pc = *target_pc;
    match state.registers[reg].get_value() {
        Value::Numeric(Numeric::Integer(n)) if *n > 0 => {
            state.pc = target_pc.as_offset_int();
            state.registers[reg] = Register::Value(Value::from_i64(*n - *decrement_by as i64));
        }
        Value::Numeric(Numeric::Integer(_)) => {
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(NotNull { reg, target_pc }, insn);
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
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

pub fn op_comparison(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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

    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let null_eq = flags.has_nulleq();
    let jump_if_null = flags.has_jump_if_null();
    let affinity = flags.get_affinity();

    let lhs_value = state.registers[lhs].get_value();
    let rhs_value = state.registers[rhs].get_value();

    // Fast path for integers
    if matches!(lhs_value, Value::Numeric(Numeric::Integer(_)))
        && matches!(rhs_value, Value::Numeric(Numeric::Integer(_)))
    {
        if op.compare(lhs_value, rhs_value, collation) {
            state.pc = target_pc.as_offset_int();
        } else {
            state.pc += 1;
        }
        return Ok(InsnFunctionStepResult::Step);
    }

    // Handle NULL values
    if matches!(lhs_value, Value::Null) || matches!(rhs_value, Value::Null) {
        let cmp_res = op.compare_nulls(lhs_value, rhs_value, null_eq);
        let jump = match op {
            ComparisonOp::Eq => cmp_res || (!null_eq && jump_if_null),
            ComparisonOp::Ne => cmp_res || (!null_eq && jump_if_null),
            ComparisonOp::Lt | ComparisonOp::Le | ComparisonOp::Gt | ComparisonOp::Ge => {
                jump_if_null
            }
        };
        if jump {
            state.pc = target_pc.as_offset_int();
        } else {
            state.pc += 1;
        }
        return Ok(InsnFunctionStepResult::Step);
    }

    let (new_lhs, new_rhs) = (affinity.convert(lhs_value), affinity.convert(rhs_value));

    let should_jump = op.compare(
        new_lhs
            .as_ref()
            .map_or(Either::Left(lhs_value), Either::Right),
        new_rhs
            .as_ref()
            .map_or(Either::Left(rhs_value), Either::Right),
        collation,
    );

    match (new_lhs, new_rhs) {
        (Some(new_lhs), None) => {
            state.registers[lhs] = Register::Value(new_lhs.as_value_ref().to_owned());
        }
        (None, Some(new_rhs)) => {
            state.registers[rhs] = Register::Value(new_rhs.as_value_ref().to_owned());
        }
        (Some(new_lhs), Some(new_rhs)) => {
            let (new_lhs, new_rhs) = (
                new_lhs.as_value_ref().to_owned(),
                new_rhs.as_value_ref().to_owned(),
            );
            state.registers[lhs] = Register::Value(new_lhs);
            state.registers[rhs] = Register::Value(new_rhs);
        }
        (None, None) => {}
    }

    if should_jump {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }

    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        If {
            reg,
            target_pc,
            jump_if_null,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IfNot {
            reg,
            target_pc,
            jump_if_null,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
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
    let mv_store = program.connection.mv_store_for_db(*db);

    if let (_, CursorType::IndexMethod(module)) = &program.cursor_ref[*cursor_id] {
        if state.cursors[*cursor_id].is_none() {
            let cursor = module.init()?;
            let cursor_ref = &mut state.cursors[*cursor_id];
            *cursor_ref = Some(Cursor::IndexMethod(cursor));
        }

        let cursor = state.cursors[*cursor_id]
            .as_mut()
            .expect("cursor should exist after initialization");
        let cursor = cursor.as_index_method_mut();
        return_if_io!(cursor.open_read(&program.connection));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    if program.connection.get_mv_tx_id_for_db(*db).is_none() {
        assert!(
            *root_page >= 0,
            "root page should be non negative when we are not in a MVCC transaction"
        );
    }
    let cursors = &mut state.cursors;
    let num_columns = match cursor_type {
        CursorType::BTreeTable(table_rc) => table_rc.columns.len(),
        CursorType::BTreeIndex(index_arc) => index_arc.columns.len(),
        CursorType::MaterializedView(table_rc, _) => table_rc.columns.len(),
        _ => unreachable!("This should not have happened"),
    };

    let maybe_promote_to_mvcc_cursor = |btree_cursor: Box<dyn CursorTrait>,
                                        mv_cursor_type: MvccCursorType|
     -> Result<Box<dyn CursorTrait>> {
        if let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) {
            let mv_store = mv_store
                .as_ref()
                .expect("mv_store should be Some when MVCC transaction is active")
                .clone();
            Ok(Box::new(MvCursor::new(
                mv_store,
                tx_id,
                *root_page,
                mv_cursor_type,
                btree_cursor,
            )?))
        } else {
            Ok(btree_cursor)
        }
    };

    match cursor_type {
        CursorType::MaterializedView(_, view_mutex) => {
            // This is a materialized view with storage
            // Create btree cursor for reading the persistent data

            let btree_cursor = Box::new(BTreeCursor::new_table(
                pager.clone(),
                maybe_transform_root_page_to_positive(mv_store.as_ref(), *root_page),
                num_columns,
            ));
            let cursor = maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Table)?;

            // Get the view name and look up or create its transaction state
            let view_name = view_mutex.lock().name().to_string();
            let tx_state = program
                .connection
                .view_transaction_states
                .get_or_create(&view_name);

            // Create materialized view cursor with this view's transaction state
            let mv_cursor = crate::incremental::cursor::MaterializedViewCursor::new(
                cursor,
                view_mutex.clone(),
                pager,
                tx_state,
            )?;

            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_materialized_view(mv_cursor));
        }
        CursorType::BTreeTable(_) => {
            // Regular table
            let btree_cursor = Box::new(BTreeCursor::new_table(
                pager,
                maybe_transform_root_page_to_positive(mv_store.as_ref(), *root_page),
                num_columns,
            ));
            let cursor = maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Table)?;
            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::BTreeIndex(index) => {
            let btree_cursor = Box::new(BTreeCursor::new_index(
                pager,
                *root_page,
                index.as_ref(),
                num_columns,
            ));
            let index_info = Arc::new(IndexInfo::new_from_index(index));
            let cursor =
                maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Index(index_info))?;
            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::Pseudo(_) => {
            panic!("OpenRead on pseudo cursor");
        }
        CursorType::Sorter => {
            panic!("OpenRead on sorter cursor");
        }
        CursorType::IndexMethod(..) => {
            unreachable!("IndexMethod handled above")
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
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(VOpen { cursor_id }, insn);
    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
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
    _pager: &Arc<Pager>,
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
            rec.iter()?
                .map(|v| v.map(|v| v.to_ffi()))
                .collect::<Result<_, _>>()?
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
        conn.syms.write().vtabs.insert(table_name, table);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vfilter(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
) -> Result<InsnFunctionStepResult> {
    #[cfg(not(feature = "cli_only"))]
    let _ = pager;
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
    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VUpdate on non-virtual table cursor");
    };
    let allow_dbpage_write = {
        #[cfg(feature = "cli_only")]
        {
            virtual_table.name == crate::dbpage::DBPAGE_TABLE_NAME
                && program.connection.db.opts.unsafe_testing
        }
        #[cfg(not(feature = "cli_only"))]
        {
            false
        }
    };
    if virtual_table.readonly() && !allow_dbpage_write {
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
    let result = if allow_dbpage_write {
        #[cfg(feature = "cli_only")]
        {
            crate::dbpage::update_dbpage(pager, &argv)
        }
        #[cfg(not(feature = "cli_only"))]
        {
            unreachable!("sqlite_dbpage writes require cli_only feature");
        }
    } else {
        virtual_table.update(&argv)
    };
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(VDestroy { db: _, table_name }, insn);
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

pub fn op_vbegin(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(VBegin { cursor_id }, insn);
    let cursor = state.get_cursor(*cursor_id);
    let cursor = cursor.as_virtual_mut();
    let vtab_id = cursor
        .vtab_id()
        .expect("VBegin on non ext-virtual table cursor");
    let mut states = program.connection.vtab_txn_states.write();
    if states.insert(vtab_id) {
        // Only begin a new transaction if one is not already active for this virtual table module
        let vtabs = &program.connection.syms.read().vtabs;
        let vtab = vtabs
            .iter()
            .find(|p| p.1.id().eq(&vtab_id))
            .expect("Could not find virtual table for VBegin");
        vtab.1.begin()?;
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vrename(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        VRename {
            cursor_id,
            new_name_reg
        },
        insn
    );
    let name = state.registers[*new_name_reg].get_value().to_string();
    let cursor = state.get_cursor(*cursor_id);
    let cursor = cursor.as_virtual_mut();
    let vtabs = &program.connection.syms.read().vtabs;
    let vtab = vtabs
        .iter()
        .find(|p| {
            p.1.id().eq(&cursor
                .vtab_id()
                .expect("non ext-virtual table used in VRollback"))
        })
        .expect("Could not find virtual table for VRollback");
    vtab.1.rename(&name)?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_open_pseudo(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
            .expect("cursor_id should be valid")
            .replace(Cursor::new_pseudo(cursor));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_rewind(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Rewind {
            cursor_id,
            pc_if_empty,
        },
        insn
    );
    assert!(pc_if_empty.is_offset());
    // Clear any bloom filter associated with this cursor so stale filter data
    // does not incorrectly reject valid matches in subsequent iterations.
    if let Some(filter) = state.get_bloom_filter_mut(*cursor_id) {
        filter.clear();
    }
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
    _pager: &Arc<Pager>,
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
    _pager: &Arc<Pager>,
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
                if let Some(deferred) = state.deferred_seeks[*cursor_id].take() {
                    state.op_column_state = OpColumnState::Rowid {
                        index_cursor_id: deferred.index_cursor_id,
                        table_cursor_id: deferred.table_cursor_id,
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
                    match index_cursor {
                        Cursor::BTree(cursor) => return_if_io!(cursor.rowid()),
                        Cursor::IndexMethod(cursor) => return_if_io!(cursor.query_rowid()),
                        _ => panic!("unexpected cursor type"),
                    }
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
                let (active_cursor_id, active_column) = (*cursor_id, *column);
                // First check if this is a MaterializedViewCursor
                {
                    let cursor = state.get_cursor(active_cursor_id);
                    if let Cursor::MaterializedView(mv_cursor) = cursor {
                        // Handle materialized view column access
                        let value = return_if_io!(mv_cursor.column(active_column));
                        state.registers[*dest] = Register::Value(value);
                        break 'outer;
                    }
                    // Fall back to normal handling
                }

                let (_, cursor_type) = program
                    .cursor_ref
                    .get(active_cursor_id)
                    .expect("cursor_id should exist in cursor_ref");
                match cursor_type {
                    CursorType::BTreeTable(_)
                    | CursorType::BTreeIndex(_)
                    | CursorType::MaterializedView(_, _) => {
                        {
                            let cursor_ref = must_be_btree_cursor!(
                                active_cursor_id,
                                program.cursor_ref,
                                state,
                                "Column"
                            );
                            let cursor = cursor_ref.as_btree_mut();

                            if cursor.get_null_flag() {
                                tracing::trace!("op_column(null_flag)");
                                state.registers[*dest] = Register::Value(Value::Null);
                                break 'outer;
                            }

                            let record_result = return_if_io!(cursor.record());
                            let Some(record) = record_result else {
                                // Cursor is not positioned on a valid row (e.g., empty table).
                                // Return NULL, not the column's default value.
                                // DEFAULT handling below is for when record exists
                                // but has fewer columns than expected.
                                state.registers[*dest] = Register::Value(Value::Null);
                                break 'outer;
                            };

                            let mut payload_iterator = record.iter()?;

                            // Parse the header for serial types incrementally until we have the target column
                            // Use nth_into_register to write directly to the register without
                            // creating intermediate ValueRef allocations
                            use crate::vdbe::ValueIteratorExt;
                            match payload_iterator
                                .nth_into_register(*column, &mut state.registers[*dest])
                            {
                                Some(result) => {
                                    result?;
                                    break 'outer;
                                }
                                None => {
                                    branches::mark_unlikely();
                                    // record has fewer columns than expected
                                }
                            };

                            //break;
                        };

                        // DEFAULT handling
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
                            cursor.get_value(*column)?
                        };
                        state.registers[*dest] = Register::Value(value);
                    }
                    CursorType::IndexMethod(..) => {
                        let cursor = state.cursors[*cursor_id]
                            .as_mut()
                            .expect("cursor should exist");
                        let cursor = cursor.as_index_method_mut();
                        let value = return_if_io!(cursor.query_column(*column));
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        TypeCheck {
            start_reg,
            count,
            check_generated: _,
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
            if !col.is_rowid_alias() && col.primary_key() && matches!(reg.get_value(), Value::Null)
            {
                bail_constraint_error!(
                    "NOT NULL constraint failed: {}.{} ({})",
                    &table_reference.name,
                    col.name.as_deref().unwrap_or(""),
                    SQLITE_CONSTRAINT
                )
            } else if col.is_rowid_alias() && matches!(reg.get_value(), Value::Null) {
                // Handle INTEGER PRIMARY KEY for null as usual (Rowid will be auto-assigned)
                return Ok(());
            } else if matches!(reg.get_value(), Value::Null) {
                // STRICT only enforces type affinity on non-NULL values.
                // NULL is valid in any column without NOT NULL constraint.
                return Ok(());
            }
            let ty_str = &col.ty_str;
            let ty_bytes = ty_str.as_bytes();
            let is_builtin_type = turso_macros::match_ignore_ascii_case!(match ty_bytes {
                b"ANY" | b"INTEGER" | b"INT" | b"REAL" | b"BLOB" | b"TEXT" => true,
                _ => false,
            });
            if is_builtin_type {
                match_ignore_ascii_case!(match ty_bytes {
                    b"ANY" => {}
                    _ => {
                        let col_affinity = col.affinity();
                        let _applied = apply_affinity_char(reg, col_affinity);
                        let value_type = reg.get_value().value_type();
                        match_ignore_ascii_case!(match ty_bytes {
                            b"INTEGER" | b"INT" if value_type == ValueType::Integer => {}
                            b"REAL" if value_type == ValueType::Float => {}
                            b"BLOB" if value_type == ValueType::Blob => {}
                            b"TEXT" if value_type == ValueType::Text => {}
                            _ => bail_constraint_error!(
                                "cannot store {} value in {} column {}.{} ({})",
                                value_type,
                                ty_str,
                                &table_reference.name,
                                col.name.as_deref().unwrap_or(""),
                                SQLITE_CONSTRAINT
                            ),
                        });
                    }
                });
            }
            // Custom types: skip type check — encode function validates
            Ok(())
        })?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_make_record(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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

    let start_reg = *start_reg as usize;
    let count = *count as usize;
    let dest_reg = *dest_reg as usize;

    if let Some(affinity_str) = affinity_str {
        if affinity_str.len() != count {
            return Err(LimboError::InternalError(format!(
                "MakeRecord: the length of affinity string ({}) does not match the count ({})",
                affinity_str.len(),
                count
            )));
        }
        for (i, affinity_ch) in affinity_str.chars().enumerate().take(count) {
            let reg_index = start_reg + i;
            let affinity = Affinity::from_char(affinity_ch);
            apply_affinity_char(&mut state.registers[reg_index], affinity);
        }
    }

    let record = make_record(&state.registers, &start_reg, &count);
    state.registers[dest_reg] = Register::Record(record);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_mem_max(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(MemMax { dest_reg, src_reg }, insn);

    let dest_val = state.registers[*dest_reg].get_value();
    let src_val = state.registers[*src_reg].get_value();

    let dest_int = extract_int_value(dest_val);
    let src_int = extract_int_value(src_val);

    if dest_int < src_int {
        state.registers[*dest_reg] = Register::Value(Value::from_i64(src_int));
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_result_row(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _pager: &Arc<Pager>,
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
                // If cursor is in NullRow state, don't advance - just return empty.
                // This matches SQLite's OP_Next behavior: btreeNext() returns
                // SQLITE_DONE when eState==CURSOR_INVALID (NullRow calls
                // sqlite3BtreeClearCursor which sets CURSOR_INVALID).
                let is_null_row = btree_cursor.get_null_flag();
                btree_cursor.set_null_flag(false);
                if is_null_row {
                    true // is_empty = true
                } else {
                    return_if_io!(btree_cursor.next());
                    btree_cursor.is_empty()
                }
            }
            Cursor::MaterializedView(mv_cursor) => {
                let has_more = return_if_io!(mv_cursor.next());
                !has_more
            }
            Cursor::IndexMethod(_) => {
                let cursor = cursor.as_index_method_mut();
                let has_more = return_if_io!(cursor.query_next());
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
    _pager: &Arc<Pager>,
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
        // If cursor is in NullRow state, don't advance - just return empty.
        // This matches SQLite's OP_Prev behavior which checks nullRow first.
        let is_null_row = cursor.get_null_flag();
        cursor.set_null_flag(false);
        if is_null_row {
            true // is_empty = true
        } else {
            return_if_io!(cursor.prev());
            cursor.is_empty()
        }
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
    err_code: usize,
    description: &str,
    on_error: Option<ResolveType>,
) -> Result<InsnFunctionStepResult> {
    let mv_store = program.connection.mv_store();
    let auto_commit = program.connection.auto_commit.load(Ordering::SeqCst);

    // Check if we're resuming from a FAIL commit I/O wait.
    // If pending_fail_error is set, we were in the middle of committing partial changes
    // for FAIL mode and need to continue the commit, then return the stored error.
    if let Some(pending_error) = state.pending_fail_error.take() {
        match program.commit_txn(pager.clone(), state, mv_store.as_ref(), false)? {
            IOResult::Done(_) => return Err(pending_error),
            IOResult::IO(io) => {
                state.pending_fail_error = Some(pending_error); // put it back and wait
                return Ok(InsnFunctionStepResult::IO(io));
            }
        }
    }

    if err_code > 0 {
        vtab_rollback_all(&program.connection)?;
    }

    // Handle RAISE errors - these carry their own resolve type
    if let Some(resolve_type) = on_error {
        // RAISE(IGNORE) signals the parent to skip the current row
        if resolve_type == ResolveType::Ignore {
            return Err(LimboError::RaiseIgnore);
        }
        if err_code > 0 {
            let error = match resolve_type {
                ResolveType::Abort | ResolveType::Rollback | ResolveType::Fail => {
                    LimboError::Raise(resolve_type, description.to_string())
                }
                ResolveType::Ignore => unreachable!("handled above"),
                ResolveType::Replace => unreachable!("Replace not valid for RAISE"),
            };

            // Trigger subprograms must not commit — just propagate the error.
            // The parent program's abort() handles transaction state.
            if program.is_trigger_subprogram() {
                return Err(error);
            }

            return Err(error);
        }
    }

    // Determine the constraint error (if any) based on error code
    let constraint_error = match err_code {
        0 => None,
        SQLITE_CONSTRAINT_PRIMARYKEY => Some(LimboError::Constraint(format!(
            "UNIQUE constraint failed: {description} (19)"
        ))),
        SQLITE_CONSTRAINT_CHECK => Some(LimboError::Constraint(format!(
            "CHECK constraint failed: {description} (19)"
        ))),
        SQLITE_CONSTRAINT_NOTNULL => Some(LimboError::Constraint(format!(
            "NOT NULL constraint failed: {description} (19)"
        ))),
        SQLITE_CONSTRAINT_UNIQUE => Some(LimboError::Constraint(format!(
            "UNIQUE constraint failed: {description} (19)"
        ))),
        SQLITE_CONSTRAINT_FOREIGNKEY => {
            Some(LimboError::ForeignKeyConstraint(description.to_string()))
        }
        SQLITE_CONSTRAINT_TRIGGER => Some(LimboError::Constraint(description.to_string())),
        // SQLITE_ERROR is a generic error (e.g. ALTER TABLE validation), not a constraint.
        // Use InternalError so abort() doesn't apply ON CONFLICT resolution to it.
        SQLITE_ERROR => Some(LimboError::InternalError(description.to_string())),
        _ => Some(LimboError::Constraint(format!(
            "undocumented halt error code {description}"
        ))),
    };

    // Handle constraint errors
    if let Some(error) = constraint_error {
        // For FAIL mode with autocommit, commit partial changes before returning error.
        // This matches SQLite behavior where FAIL keeps changes made before the error.
        // Note: ON CONFLICT FAIL does NOT apply to FK violations, so we check for those first.
        if program.resolve_type == ResolveType::Fail && auto_commit {
            // Check for immediate FK violations - FK errors don't respect ON CONFLICT
            if program.connection.foreign_keys_enabled()
                && state.get_fk_immediate_violations_during_stmt() > 0
            {
                return Err(LimboError::ForeignKeyConstraint(
                    "immediate foreign key constraint failed".to_string(),
                ));
            }

            // Release savepoint to preserve partial changes, then commit
            state.end_statement(&program.connection, pager, EndStatement::ReleaseSavepoint)?;
            vtab_commit_all(&program.connection)?;
            index_method_pre_commit_all(state, pager)?;

            // Commit the transaction with partial changes
            match program.commit_txn(pager.clone(), state, mv_store.as_ref(), false)? {
                IOResult::Done(_) => return Err(error),
                IOResult::IO(io) => {
                    // store the error for reentrancy
                    state.pending_fail_error = Some(error);
                    return Ok(InsnFunctionStepResult::IO(io));
                }
            }
        }

        // For non-FAIL modes (or non-autocommit), just return the error.
        // abort() will handle rollback based on resolve_type.
        return Err(error);
    }

    tracing::trace!("halt(auto_commit={})", auto_commit);

    // Check for immediate foreign key violations.
    // Any immediate violation causes the statement subtransaction to roll back.
    if program.connection.foreign_keys_enabled()
        && state.get_fk_immediate_violations_during_stmt() > 0
    {
        return Err(LimboError::ForeignKeyConstraint(
            "immediate foreign key constraint failed".to_string(),
        ));
    }

    if program.is_trigger_subprogram() {
        return Ok(InsnFunctionStepResult::Done);
    }

    if auto_commit {
        // In autocommit mode, a statement that leaves deferred violations must fail here,
        // and it also ends the transaction.
        if program.connection.foreign_keys_enabled() {
            let deferred_violations = program
                .connection
                .fk_deferred_violations
                .swap(0, Ordering::AcqRel);
            if deferred_violations > 0 {
                vtab_rollback_all(&program.connection)?;
                if let Some(mv_store) = mv_store.as_ref() {
                    if let Some(tx_id) = program.connection.get_mv_tx_id() {
                        mv_store.rollback_tx(
                            tx_id,
                            pager.clone(),
                            &program.connection,
                            crate::MAIN_DB_ID,
                        );
                    }
                    pager.end_read_tx();
                } else {
                    pager.rollback_tx(&program.connection);
                }
                program.connection.set_tx_state(TransactionState::None);
                return Err(LimboError::ForeignKeyConstraint(
                    "deferred foreign key constraint failed".to_string(),
                ));
            }
        }
        state.end_statement(&program.connection, pager, EndStatement::ReleaseSavepoint)?;
        vtab_commit_all(&program.connection)?;
        index_method_pre_commit_all(state, pager)?;
        let result = program
            .commit_txn(pager.clone(), state, mv_store.as_ref(), false)
            .map(Into::into);
        // Apply deferred CDC state and reset CDC txn ID after successful commit
        if matches!(result, Ok(InsnFunctionStepResult::Done)) {
            if let Some(cdc_info) = state.pending_cdc_info.take() {
                program.connection.set_capture_data_changes_info(cdc_info);
            }
            program.connection.set_cdc_transaction_id(-1);
        }
        result
    } else {
        // Even if deferred violations are present, the statement subtransaction completes successfully when
        // it is part of an interactive transaction.
        state.end_statement(&program.connection, pager, EndStatement::ReleaseSavepoint)?;
        // Apply deferred CDC state after successful statement completion
        if let Some(cdc_info) = state.pending_cdc_info.take() {
            program.connection.set_capture_data_changes_info(cdc_info);
        }
        if program.change_cnt_on {
            program
                .connection
                .set_changes(state.n_change.load(Ordering::SeqCst));
        }
        Ok(InsnFunctionStepResult::Done)
    }
}

/// Call xCommit on all virtual tables that participated in the current transaction.
pub(crate) fn vtab_commit_all(conn: &Connection) -> crate::Result<()> {
    let mut set = conn.vtab_txn_states.write();
    if set.is_empty() {
        return Ok(());
    }
    let reg = &conn.syms.read().vtabs;
    for id in set.drain() {
        let vtab = reg
            .iter()
            .find(|(_, vtab)| vtab.id() == id)
            .expect("vtab must exist");
        vtab.1.commit()?;
    }
    Ok(())
}

/// Flush pending writes on all index method cursors before transaction commit.
/// This ensures index method writes are persisted as part of the transaction.
pub(crate) fn index_method_pre_commit_all(
    state: &mut ProgramState,
    pager: &Arc<Pager>,
) -> crate::Result<()> {
    for cursor_opt in state.cursors.iter_mut().flatten() {
        let Cursor::IndexMethod(cursor) = cursor_opt else {
            continue;
        };
        loop {
            match cursor.pre_commit()? {
                IOResult::Done(()) => break,
                IOResult::IO(io) => {
                    while !io.finished() {
                        pager.io.step()?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Rollback all virtual tables that are part of the current transaction.
fn vtab_rollback_all(conn: &Connection) -> crate::Result<()> {
    let mut set = conn.vtab_txn_states.write();
    if set.is_empty() {
        return Ok(());
    }
    let reg = &conn.syms.read().vtabs;
    for id in set.drain() {
        let vtab = reg
            .iter()
            .find(|(_, vtab)| vtab.id() == id)
            .expect("vtab must exist");
        vtab.1.rollback()?;
    }
    Ok(())
}

pub fn op_halt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Halt {
            err_code,
            description,
            on_error,
        },
        insn
    );
    halt(program, state, pager, *err_code, description, *on_error)
}

pub fn op_halt_if_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
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
        halt(program, state, pager, *err_code, description, None)
    } else {
        state.pc += 1;
        Ok(InsnFunctionStepResult::Step)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OpTransactionState {
    Start,
    AttachedBeginWriteTx,
    CheckSchemaCookie,
    BeginStatement,
}

pub fn op_transaction(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    let result = op_transaction_inner(program, state, insn, pager);
    tracing::debug!(
        "op_transaction: end: state={:?}, tx_state={:?}",
        state.op_transaction_state,
        program.connection.get_tx_state()
    );
    match result {
        Ok(result) => Ok(result),
        Err(err) => {
            state.op_transaction_state = OpTransactionState::Start;
            Err(err)
        }
    }
}

/// Begin an MVCC transaction on the given MvStore using the specified mode.
/// When `existing_tx_id` is `Some`, upgrades an existing transaction to exclusive.
fn begin_mvcc_tx(
    mv_store: &MvStore,
    pager: &Arc<Pager>,
    mode: &TransactionMode,
    existing_tx_id: Option<u64>,
) -> Result<u64> {
    match mode {
        TransactionMode::None | TransactionMode::Read | TransactionMode::Concurrent => {
            mv_store.begin_tx(pager.clone())
        }
        TransactionMode::Write => mv_store.begin_exclusive_tx(pager.clone(), existing_tx_id),
    }
}

pub fn op_transaction_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Transaction {
            db,
            tx_mode,
            schema_cookie,
        },
        insn
    );
    if program.is_trigger_subprogram() {
        crate::bail_parse_error!(
            "Transaction instruction should not be used in trigger subprograms"
        );
    }
    let pager = program.get_pager_from_database_index(db);
    // Get the MvStore for the specific database (main or attached).
    let mv_store = program.connection.mv_store_for_db(*db);
    loop {
        match state.op_transaction_state {
            OpTransactionState::Start => {
                let conn = program.connection.clone();
                let write = matches!(tx_mode, TransactionMode::Write);
                if write && conn.is_readonly(*db) {
                    return Err(LimboError::ReadOnly);
                }

                // 1. We try to upgrade current version
                let current_state = conn.get_tx_state();
                let is_attached = crate::is_attached_db(*db);
                let (new_transaction_state, updated) = if conn.is_nested_stmt() {
                    (current_state, false)
                } else if is_attached {
                    // For attached databases, don't modify the connection-level
                    // transaction state — it tracks the main database's state.
                    // Attached pager locks are managed independently below.
                    (current_state, false)
                } else {
                    match (current_state, write) {
                        // pending state means that we tried beginning a tx and the method returned IO.
                        // instead of ending the read tx, just update the state to pending.
                        (TransactionState::PendingUpgrade { .. }, write) => {
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
                if let Some(mv_store) = mv_store.as_ref() {
                    if is_attached {
                        // Attached databases don't participate in the connection-level
                        // transaction state machine above (phase 1), so the pager read
                        // tx that the main DB path starts on None→Read isn't triggered
                        // for them. We need it here to pin a consistent WAL snapshot
                        // for the attached pager's B-tree page reads.
                        if !pager.holds_read_lock() {
                            pager.begin_read_tx()?;
                        }
                        pager.mvcc_refresh_if_db_changed();

                        let current_mv_tx = conn.get_mv_tx_for_db(*db);
                        if current_mv_tx.is_none() {
                            // Reject CONCURRENT on an attached DB if the main
                            // DB already started with BEGIN DEFERRED.
                            let conn_has_executed_begin_deferred =
                                !conn.auto_commit.load(Ordering::SeqCst)
                                    && conn.get_mv_tx().is_none();
                            if conn_has_executed_begin_deferred
                                && *tx_mode == TransactionMode::Concurrent
                            {
                                return Err(LimboError::TxError(
                                    "Cannot start CONCURRENT transaction after BEGIN DEFERRED"
                                        .to_string(),
                                ));
                            }
                            // Use the same tx_mode as the main DB's active
                            // transaction when available, so BEGIN CONCURRENT
                            // applies to all databases uniformly.
                            let effective_mode =
                                conn.get_mv_tx().map(|(_, mode)| mode).unwrap_or(*tx_mode);
                            match begin_mvcc_tx(mv_store, &pager, &effective_mode, None) {
                                Ok(tx_id) => {
                                    conn.set_mv_tx_for_db(*db, Some((tx_id, effective_mode)));
                                }
                                Err(err) => {
                                    pager.end_read_tx();
                                    return Err(err);
                                }
                            }
                        } else if write {
                            // Upgrade: attached DB has a Read/Concurrent tx but the
                            // statement needs write access. Mirror the main DB's
                            // upgrade logic so that exclusive locks are acquired.
                            let (tx_id, current_mode) = current_mv_tx.unwrap();
                            if matches!(current_mode, TransactionMode::None | TransactionMode::Read)
                                && matches!(tx_mode, TransactionMode::Write)
                            {
                                if let Err(err) =
                                    begin_mvcc_tx(mv_store, &pager, tx_mode, Some(tx_id))
                                {
                                    pager.end_read_tx();
                                    return Err(err);
                                }
                                conn.set_mv_tx_for_db(*db, Some((tx_id, *tx_mode)));
                            }
                        }
                    } else {
                        // Main database MVCC path (unchanged logic)
                        let started_read_tx =
                            updated && matches!(current_state, TransactionState::None);
                        if started_read_tx {
                            turso_assert!(
                                !conn.is_nested_stmt(),
                                "nested stmt should not begin a new read transaction"
                            );
                            pager.begin_read_tx()?;
                            state.auto_txn_cleanup = TxnCleanup::RollbackTxn;
                        }
                        // MVCC reads must refresh WAL change counters to avoid stale page-cache reads.
                        pager.mvcc_refresh_if_db_changed();
                        // In MVCC we don't have write exclusivity, therefore we just need to start a transaction if needed.
                        // Programs can run Transaction twice, first with read flag and then with write flag. So a single txid is enough
                        // for both.
                        let current_mv_tx = program.connection.get_mv_tx_for_db(*db);
                        let has_existing_mv_tx = current_mv_tx.is_some();

                        let conn_has_executed_begin_deferred = !has_existing_mv_tx
                            && !program.connection.auto_commit.load(Ordering::SeqCst);
                        if conn_has_executed_begin_deferred
                            && *tx_mode == TransactionMode::Concurrent
                        {
                            return Err(LimboError::TxError(
                                "Cannot start CONCURRENT transaction after BEGIN DEFERRED"
                                    .to_string(),
                            ));
                        }

                        if !has_existing_mv_tx {
                            match begin_mvcc_tx(mv_store, &pager, tx_mode, None) {
                                Ok(tx_id) => {
                                    program
                                        .connection
                                        .set_mv_tx_for_db(*db, Some((tx_id, *tx_mode)));
                                }
                                Err(err) => {
                                    if started_read_tx {
                                        pager.end_read_tx();
                                        conn.set_tx_state(TransactionState::None);
                                        state.auto_txn_cleanup = TxnCleanup::None;
                                    }
                                    return Err(err);
                                }
                            }
                        } else if updated {
                            // TODO: fix tx_mode in Insn::Transaction, now each statement overrides it even if there's already a CONCURRENT Tx in progress, for example
                            let (tx_id, mv_tx_mode) = current_mv_tx
                                .expect("current_mv_tx should be Some when updated is true");
                            let actual_tx_mode = if mv_tx_mode == TransactionMode::Concurrent {
                                TransactionMode::Concurrent
                            } else {
                                *tx_mode
                            };
                            if matches!(new_transaction_state, TransactionState::Write { .. })
                                && matches!(actual_tx_mode, TransactionMode::Write)
                            {
                                if let Err(err) =
                                    begin_mvcc_tx(mv_store, &pager, &actual_tx_mode, Some(tx_id))
                                {
                                    if started_read_tx {
                                        pager.end_read_tx();
                                        conn.set_tx_state(TransactionState::None);
                                        state.auto_txn_cleanup = TxnCleanup::None;
                                    }
                                    return Err(err);
                                }
                            }
                        }
                    }
                } else {
                    if matches!(tx_mode, TransactionMode::Concurrent) {
                        return Err(LimboError::TxError(
                            "Concurrent transaction mode is only supported when MVCC is enabled"
                                .to_string(),
                        ));
                    }
                    // For attached databases without MVCC, always start read/write
                    // transactions on the attached pager, since the connection-level
                    // transaction state may already be Read/Write from the main database.
                    let is_attached = crate::is_attached_db(*db);
                    if is_attached {
                        // If the pager already holds a read lock (e.g., after
                        // SchemaUpdated reprepare or prior write tx), skip
                        // locks that are already held.
                        if pager.holds_read_lock() {
                            if matches!(tx_mode, TransactionMode::Write)
                                && !pager.holds_write_lock()
                            {
                                state.op_transaction_state =
                                    OpTransactionState::AttachedBeginWriteTx;
                                continue;
                            }
                            state.op_transaction_state = OpTransactionState::CheckSchemaCookie;
                            continue;
                        }
                        pager.begin_read_tx()?;
                        if matches!(tx_mode, TransactionMode::Write) {
                            // Transition to AttachedBeginWriteTx to handle begin_write_tx
                            // separately, so if it returns IO we don't re-call begin_read_tx
                            // on re-entry.
                            state.op_transaction_state = OpTransactionState::AttachedBeginWriteTx;
                            continue;
                        }
                    } else if updated && matches!(current_state, TransactionState::None) {
                        turso_assert!(
                            !conn.is_nested_stmt(),
                            "nested stmt should not begin a new read transaction"
                        );
                        pager.begin_read_tx()?;
                        state.auto_txn_cleanup = TxnCleanup::RollbackTxn;
                    }

                    if !is_attached
                        && updated
                        && matches!(new_transaction_state, TransactionState::Write { .. })
                    {
                        turso_assert!(
                            !conn.is_nested_stmt(),
                            "nested stmt should not begin a new write transaction"
                        );
                        let begin_w_tx_res = pager.begin_write_tx();
                        if matches!(
                            begin_w_tx_res,
                            Err(LimboError::Busy | LimboError::BusySnapshot)
                        ) {
                            // We failed to upgrade to write transaction so put the transaction into its original state.
                            // That is, if the transaction had not started, end the read transaction so that next time we
                            // start a new one.
                            match current_state {
                                TransactionState::None
                                | TransactionState::PendingUpgrade {
                                    has_read_txn: false,
                                } => {
                                    pager.end_read_tx();
                                    conn.set_tx_state(TransactionState::None);
                                    state.auto_txn_cleanup = TxnCleanup::None;
                                }
                                TransactionState::Read
                                | TransactionState::PendingUpgrade { has_read_txn: true } => {
                                    conn.set_tx_state(TransactionState::Read);
                                }
                                TransactionState::Write { .. } => {
                                    panic!("impossible state: {current_state:?}")
                                }
                            }
                            return Err(begin_w_tx_res.unwrap_err());
                        }
                        if let IOResult::IO(io) = begin_w_tx_res? {
                            // set the transaction state to pending so we don't have to
                            // end the read transaction.
                            conn.set_tx_state(TransactionState::PendingUpgrade {
                                has_read_txn: matches!(current_state, TransactionState::Read),
                            });
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
            // 3b. For attached databases, begin the write transaction after
            // begin_read_tx has already completed in the Start state.
            OpTransactionState::AttachedBeginWriteTx => {
                let res = pager.begin_write_tx()?;
                if let IOResult::IO(io) = res {
                    return Ok(InsnFunctionStepResult::IO(io));
                }
                state.op_transaction_state = OpTransactionState::CheckSchemaCookie;
                continue;
            }
            // 4. Check whether schema has changed if we are actually going to access the database.
            // Can only read header if page 1 has been allocated already
            // begin_write_tx that happens, but not begin_read_tx
            OpTransactionState::CheckSchemaCookie => {
                let res = get_schema_cookie(&pager, mv_store.as_ref(), program, *db);
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

                state.op_transaction_state = OpTransactionState::BeginStatement;
            }
            OpTransactionState::BeginStatement => {
                if *db == crate::MAIN_DB_ID
                    && program.needs_stmt_subtransactions.load(Ordering::Relaxed)
                {
                    let write = matches!(tx_mode, TransactionMode::Write);
                    let res = state.begin_statement(&program.connection, &pager, write)?;
                    if let IOResult::IO(io) = res {
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                } else if crate::is_attached_db(*db)
                    && matches!(tx_mode, TransactionMode::Write)
                    && program.needs_stmt_subtransactions.load(Ordering::Relaxed)
                {
                    if let Some(mv_store) = program.connection.mv_store_for_db(*db) {
                        // Attached MVCC DB: open an MvStore savepoint.
                        if let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) {
                            mv_store.begin_savepoint(tx_id);
                        }
                    } else {
                        // Attached WAL DB: open a pager savepoint for statement rollback.
                        let db_size =
                            return_if_io!(pager.with_header(|header| header.database_size.get()));
                        pager.open_subjournal()?;
                        pager.try_use_subjournal()?;
                        let result = pager.open_savepoint(db_size);
                        if result.is_err() {
                            pager.stop_use_subjournal();
                        }
                        result?;
                        state.attached_savepoint_pagers.push(pager.clone());
                    }
                }

                state.pc += 1;
                state.op_transaction_state = OpTransactionState::Start;
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
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        AutoCommit {
            auto_commit,
            rollback
        },
        insn
    );

    // Main DB's MvStore drives the commit/rollback routing.  The attach-time
    // journal-mode compatibility check ensures all attached DBs match, so
    // checking the main DB is sufficient to choose the MVCC vs WAL path.
    let mv_store = program.connection.mv_store();
    let conn = program.connection.clone();
    let fk_on = conn.foreign_keys_enabled();
    let had_autocommit = conn.auto_commit.load(Ordering::SeqCst); // true, not in tx

    // Drive any multi-step commit/rollback that's already in progress.
    // This handles main DB commits (Committing), attached DB commits
    // (CommittingAttached), MVCC commits (CommittingMvcc), and attached
    // MVCC commits (CommittingAttachedMvcc) that yielded on IO and need re-entry.
    if !matches!(state.commit_state, CommitState::Ready) {
        let res = program
            .commit_txn(pager.clone(), state, mv_store.as_ref(), *rollback)
            .map(Into::into);
        // Only clear after a final, successful non-rollback COMMIT.
        if fk_on
            && !*rollback
            && matches!(
                res,
                Ok(InsnFunctionStepResult::Step | InsnFunctionStepResult::Done)
            )
        {
            conn.clear_deferred_foreign_key_violations();
        }
        return res;
    }

    if program.is_trigger_subprogram() {
        // Trigger subprograms never commit or rollback.
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    // The logic in this opcode can be a bit confusing, so to make things a bit clearer lets be
    // very explicit about the currently existing and requested state.
    let requested_autocommit = *auto_commit;
    let requested_rollback = *rollback;
    let changed = requested_autocommit != had_autocommit;
    let is_txn_end_eq = changed && requested_autocommit;
    // what the requested operation is
    let is_begin_req = had_autocommit && !requested_autocommit && !requested_rollback;
    let is_commit_req = !had_autocommit && requested_autocommit && !requested_rollback;
    let is_rollback_req = !had_autocommit && requested_autocommit && requested_rollback;

    let another_stmt_using_subjournal = !state.uses_subjournal && pager.subjournal_in_use();
    if is_txn_end_eq && another_stmt_using_subjournal {
        return Err(LimboError::Busy);
    }

    if changed {
        if requested_rollback {
            // ROLLBACK transition
            if let Some(mv_store) = mv_store.as_ref() {
                if let Some(tx_id) = conn.get_mv_tx_id() {
                    mv_store.rollback_tx(tx_id, pager.clone(), &conn, crate::MAIN_DB_ID);
                }
                pager.end_read_tx();
                conn.rollback_attached_mvcc_txs(true);
            } else {
                pager.rollback_tx(&conn);
            }
            conn.rollback_attached_wal_txns();
            conn.set_tx_state(TransactionState::None);
            conn.auto_commit.store(true, Ordering::SeqCst);
            conn.set_cdc_transaction_id(-1);
        } else {
            // BEGIN (true->false) or COMMIT (false->true)
            if is_commit_req {
                // Pre-check deferred FKs; leave tx open and do NOT clear violations
                check_deferred_fk_on_commit(&conn)?;
            }
            conn.auto_commit
                .store(requested_autocommit, Ordering::SeqCst);
        }
    } else {
        // No autocommit flip.
        let mvcc_tx_active = conn.get_mv_tx().is_some();
        if !mvcc_tx_active {
            if !requested_autocommit {
                return Err(LimboError::TxError(
                    "cannot start a transaction within a transaction".to_string(),
                ));
            } else if requested_rollback {
                return Err(LimboError::TxError(
                    "cannot rollback - no transaction is active".to_string(),
                ));
            } else {
                return Err(LimboError::TxError(
                    "cannot commit - no transaction is active".to_string(),
                ));
            }
        } else if is_begin_req {
            return Err(LimboError::TxError(
                "cannot use BEGIN after BEGIN CONCURRENT".to_string(),
            ));
        }
    }

    // For explicit COMMIT, flush any pending index method writes first
    if is_commit_req {
        index_method_pre_commit_all(state, pager)?;
    }

    let res = program
        .commit_txn(pager.clone(), state, mv_store.as_ref(), requested_rollback)
        .map(Into::into);

    if mv_store.is_none()
        && matches!(
            res,
            Ok(InsnFunctionStepResult::Step | InsnFunctionStepResult::Done)
        )
        && (is_rollback_req || is_commit_req)
    {
        pager.clear_savepoints()?;
    }

    // Clear deferred FK counters only after FINAL success of COMMIT/ROLLBACK.
    if fk_on
        && matches!(
            res,
            Ok(InsnFunctionStepResult::Step | InsnFunctionStepResult::Done)
        )
        && (is_rollback_req || is_commit_req)
    {
        conn.clear_deferred_foreign_key_violations();
    }

    // Reset CDC transaction ID after successful COMMIT or ROLLBACK.
    if matches!(
        res,
        Ok(InsnFunctionStepResult::Step | InsnFunctionStepResult::Done)
    ) && (is_rollback_req || is_commit_req)
    {
        conn.set_cdc_transaction_id(-1);
    }

    res
}

pub fn op_savepoint(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Savepoint { op, name }, insn);
    let conn = program.connection.clone();
    let mv_store = conn.mv_store();

    match *op {
        SavepointOp::Begin => {
            let starts_transaction = conn.auto_commit.load(Ordering::SeqCst);
            let deferred_fk_violations = conn.get_deferred_foreign_key_violations();

            if let Some(mv_store) = mv_store.as_ref() {
                let tx_id = if let Some(tx_id) = conn.get_mv_tx_id() {
                    tx_id
                } else {
                    let tx_id = mv_store.begin_tx(pager.clone())?;
                    conn.set_mv_tx(Some((tx_id, TransactionMode::Read)));
                    if matches!(conn.get_tx_state(), TransactionState::None) {
                        conn.set_tx_state(TransactionState::Read);
                    }
                    tx_id
                };
                mv_store.begin_named_savepoint(
                    tx_id,
                    name.clone(),
                    starts_transaction,
                    deferred_fk_violations,
                );
                // Open matching named savepoints on attached MVCC databases.
                conn.for_each_attached_mv_tx(|db_id, att_tx_id| {
                    if let Some(att_mv) = conn.mv_store_for_db(db_id) {
                        att_mv.begin_named_savepoint(
                            att_tx_id,
                            name.clone(),
                            false,
                            deferred_fk_violations,
                        );
                    }
                });
            } else {
                pager.open_subjournal()?;
                let db_size = return_if_io!(pager.with_header(|header| header.database_size.get()));
                pager.open_named_savepoint(
                    name.clone(),
                    db_size,
                    starts_transaction,
                    deferred_fk_violations,
                )?;
            }

            if starts_transaction {
                conn.auto_commit.store(false, Ordering::SeqCst);
            }

            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        SavepointOp::Release => {
            let release_result = if let Some(mv_store) = mv_store.as_ref() {
                match conn.get_mv_tx_id() {
                    Some(tx_id) => mv_store.release_named_savepoint(tx_id, name)?,
                    None => SavepointResult::NotFound,
                }
            } else {
                pager.release_named_savepoint(name)?
            };
            // Release matching named savepoints on attached MVCC databases.
            if mv_store.is_some() {
                conn.for_each_attached_mv_tx(|db_id, att_tx_id| {
                    if let Some(att_mv) = conn.mv_store_for_db(db_id) {
                        let _ = att_mv.release_named_savepoint(att_tx_id, name);
                    }
                });
            }
            match release_result {
                SavepointResult::NotFound => {
                    return Err(LimboError::TxError(format!("no such savepoint: {name}")));
                }
                SavepointResult::Release => {
                    // Savepoint released successfully, just continue
                }
                SavepointResult::Commit => {
                    // This means that releasing the savepoint caused the transaction to commit, so we need to auto-commit here.
                    let auto_commit = Insn::AutoCommit {
                        auto_commit: true,
                        rollback: false,
                    };
                    return op_auto_commit(program, state, &auto_commit, pager);
                }
            }

            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        SavepointOp::RollbackTo => {
            let deferred_fk_snapshot = if let Some(mv_store) = mv_store.as_ref() {
                // Rollback named savepoints on attached MVCC databases.
                conn.for_each_attached_mv_tx(|db_id, att_tx_id| {
                    if let Some(att_mv) = conn.mv_store_for_db(db_id) {
                        let _ = att_mv.rollback_to_named_savepoint(att_tx_id, name);
                    }
                });
                match conn.get_mv_tx_id() {
                    Some(tx_id) => mv_store.rollback_to_named_savepoint(tx_id, name)?,
                    None => None,
                }
            } else {
                pager.rollback_to_named_savepoint(name)?
            };

            let Some(deferred_fk_snapshot) = deferred_fk_snapshot else {
                return Err(LimboError::TxError(format!("no such savepoint: {name}")));
            };
            conn.fk_deferred_violations
                .store(deferred_fk_snapshot, Ordering::SeqCst);

            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
    }
}

fn check_deferred_fk_on_commit(conn: &Connection) -> Result<()> {
    if !conn.foreign_keys_enabled() {
        return Ok(());
    }
    if conn.get_deferred_foreign_key_violations() > 0 {
        return Err(LimboError::ForeignKeyConstraint(
            "deferred foreign key constraint failed on commit".into(),
        ));
    }
    Ok(())
}

pub fn op_goto(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Goto { target_pc }, insn);
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_gosub(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Gosub {
            target_pc,
            return_reg,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    state.registers[*return_reg] = Register::Value(Value::from_i64((state.pc + 1) as i64));
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_return(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Return {
            return_reg,
            can_fallthrough,
        },
        insn
    );
    if let Value::Numeric(Numeric::Integer(pc)) = state.registers[*return_reg].get_value() {
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Integer { value, dest }, insn);
    state.registers[*dest] = Register::Value(Value::from_i64(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub enum OpProgramState {
    Start,
    /// Step state tracks whether we're executing a trigger subprogram (vs FK action subprogram)
    Step {
        is_trigger: bool,
        statement: Box<Statement>,
    },
}

/// Execute a subprogram (Program opcode).
/// Used for both triggers and FK actions (CASCADE, SET NULL, etc.)
pub fn op_program(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Program {
            params,
            program: subprogram,
            ignore_jump_target,
        },
        insn
    );
    loop {
        match &mut state.op_program_state {
            OpProgramState::Start => {
                let mut statement = Statement::new(
                    Program::from_prepared(subprogram.clone(), program.connection.clone()),
                    pager.clone(),
                    QueryMode::Normal,
                    0,
                );
                statement.reset()?;

                // Check if this is a trigger subprogram - if so, track execution
                let is_trigger = if let Some(ref trigger) = statement.get_trigger() {
                    program.connection.start_trigger_execution(trigger.clone());
                    true
                } else {
                    false
                };

                // Extract register values from params (which contain register indices encoded as negative integers)
                // and bind them to the subprogram's parameters
                for (param_idx, param_value) in params.iter().enumerate() {
                    if let Value::Numeric(Numeric::Integer(reg_idx)) = param_value {
                        let reg_idx = *reg_idx as usize;
                        if reg_idx < state.registers.len() {
                            let value = state.registers[reg_idx].get_value().clone();
                            let param_index = NonZero::<usize>::new(param_idx + 1)
                                .expect("param_idx + 1 should be non-zero");
                            statement.bind_at(param_index, value);
                        } else {
                            crate::bail_corrupt_error!(
                                "Register index {} out of bounds (len={})",
                                reg_idx,
                                state.registers.len()
                            );
                        }
                    } else {
                        crate::bail_parse_error!(
                            "Subprogram parameters should be integers, got {:?}",
                            param_value
                        );
                    }
                }

                state.op_program_state = OpProgramState::Step {
                    is_trigger,
                    statement: Box::new(statement),
                };
            }
            OpProgramState::Step {
                is_trigger,
                statement,
            } => {
                let is_trigger = *is_trigger;
                let mut raise_ignore = false;
                // Track whether the subprogram aborted with an error. When abort()
                // runs inside the subprogram, it already calls end_trigger_execution(),
                // so we must not call it again after the loop.
                let mut subprogram_aborted = false;
                loop {
                    let res = statement.step();
                    match res {
                        Ok(step_result) => match step_result {
                            StepResult::Done => break,
                            StepResult::IO => {
                                return Ok(InsnFunctionStepResult::IO(IOCompletions::Single(
                                    Completion::new_yield(),
                                )));
                            }
                            StepResult::Row => continue,
                            StepResult::Interrupt | StepResult::Busy => {
                                return Err(LimboError::Busy);
                            }
                        },
                        Err(LimboError::Constraint(constraint_err)) => {
                            if program.resolve_type != ResolveType::Ignore {
                                return Err(LimboError::Constraint(constraint_err));
                            }
                            subprogram_aborted = true;
                            break;
                        }
                        Err(LimboError::RaiseIgnore) => {
                            raise_ignore = true;
                            subprogram_aborted = true;
                            break;
                        }
                        Err(err) => {
                            return Err(err);
                        }
                    }
                }

                // Only end trigger execution for normal completion. Error paths
                // already called end_trigger_execution() via abort() in the subprogram.
                if is_trigger && !subprogram_aborted {
                    program.connection.end_trigger_execution();
                }

                state.op_program_state = OpProgramState::Start;
                if raise_ignore {
                    // RAISE(IGNORE) — skip the current row by jumping to ignore_jump_target
                    state.pc = ignore_jump_target.as_offset_int();
                } else {
                    state.pc += 1;
                }
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_real(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Real { value, dest }, insn);
    state.registers[*dest] = Register::Value(Value::from_f64(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_real_affinity(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(RealAffinity { register }, insn);
    if let Value::Numeric(Numeric::Integer(i)) = &state.registers[*register].get_value() {
        state.registers[*register] = Register::Value(Value::from_f64(*i as f64));
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_string8(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(String8 { value, dest }, insn);
    state.registers[*dest] = Register::Value(Value::build_text(value.clone()));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_blob(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(RowId { cursor_id, dest }, insn);
    loop {
        match state.op_row_id_state {
            OpRowIdState::Start => {
                if let Some(deferred) = state.deferred_seeks[*cursor_id].take() {
                    state.op_row_id_state = OpRowIdState::Record {
                        index_cursor_id: deferred.index_cursor_id,
                        table_cursor_id: deferred.table_cursor_id,
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
                    match index_cursor {
                        Cursor::BTree(index_cursor) => {
                            let record = return_if_io!(index_cursor.record());
                            let record =
                                record.as_ref().expect("index cursor should have a record");
                            let rowid = record
                                .last_value()
                                .expect("record should have a last value");
                            match rowid {
                                Ok(ValueRef::Numeric(Numeric::Integer(rowid))) => rowid,
                                _ => unreachable!(),
                            }
                        }
                        Cursor::IndexMethod(index_cursor) => {
                            return_if_io!(index_cursor.query_rowid())
                                .expect("index cursor should have a rowid")
                        }
                        _ => panic!("unexpected cursor type"),
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
                if let Some(Cursor::BTree(btree_cursor)) = cursors
                    .get_mut(*cursor_id)
                    .expect("cursor_id should be valid")
                {
                    if btree_cursor.get_null_flag() {
                        state.registers[*dest] = Register::Value(Value::Null);
                        break;
                    }
                    if let Some(ref rowid) = return_if_io!(btree_cursor.rowid()) {
                        state.registers[*dest].set_int(*rowid);
                    } else {
                        state.registers[*dest] = Register::Value(Value::Null);
                    }
                } else if let Some(Cursor::Virtual(virtual_cursor)) = cursors
                    .get_mut(*cursor_id)
                    .expect("cursor_id should be valid")
                {
                    let rowid = virtual_cursor.rowid();
                    if rowid != 0 {
                        state.registers[*dest].set_int(rowid);
                    } else {
                        state.registers[*dest] = Register::Value(Value::Null);
                    }
                } else if let Some(Cursor::MaterializedView(mv_cursor)) = cursors
                    .get_mut(*cursor_id)
                    .expect("cursor_id should be valid")
                {
                    if let Some(rowid) = return_if_io!(mv_cursor.rowid()) {
                        state.registers[*dest].set_int(rowid);
                    } else {
                        state.registers[*dest] = Register::Value(Value::Null);
                    }
                } else if let Some(Cursor::IndexMethod(cursor)) = cursors
                    .get_mut(*cursor_id)
                    .expect("cursor_id should be valid")
                {
                    if let Some(rowid) = return_if_io!(cursor.query_rowid()) {
                        state.registers[*dest].set_int(rowid);
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(IdxRowId { cursor_id, dest }, insn);
    let cursors = &mut state.cursors;
    let cursor = cursors
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid")
        .as_mut()
        .expect("cursor should exist");

    let rowid = match cursor {
        Cursor::BTree(cursor) => return_if_io!(cursor.rowid()),
        Cursor::IndexMethod(cursor) => return_if_io!(cursor.query_rowid()),
        _ => panic!("unexpected cursor type"),
    };
    state.registers[*dest] = match rowid {
        Some(rowid) => Register::Value(Value::from_i64(rowid)),
        None => Register::Value(Value::Null),
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_seek_rowid(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SeekRowid {
            cursor_id,
            src_reg,
            target_pc,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    let (pc, did_seek) = {
        let cursor = get_cursor!(state, *cursor_id);

        // Handle MaterializedView cursor
        let (pc, did_seek) = match cursor {
            Cursor::MaterializedView(mv_cursor) => {
                let rowid = match state.registers[*src_reg].get_value() {
                    Value::Numeric(Numeric::Integer(rowid)) => Some(*rowid),
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
                    Value::Numeric(Numeric::Integer(rowid)) => Some(*rowid),
                    Value::Null => None,
                    // For non-integer values try to apply affinity and convert them to integer.
                    other => {
                        let mut temp_reg = Register::Value(other.clone());
                        let converted = apply_affinity_char(&mut temp_reg, Affinity::Numeric);
                        if converted {
                            match temp_reg.get_value() {
                                Value::Numeric(Numeric::Integer(i)) => Some(*i),
                                Value::Numeric(Numeric::Float(f)) => Some(f64::from(*f) as i64),
                                _ => None,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        DeferredSeek {
            index_cursor_id,
            table_cursor_id,
        },
        insn
    );
    state.deferred_seeks[*table_cursor_id] = Some(DeferredSeekState {
        index_cursor_id: *index_cursor_id,
        table_cursor_id: *table_cursor_id,
    });
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
    IndexKeyFromRegister(usize),
    IndexKeyUnpacked { start_reg: usize, num_regs: usize },
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
    record_source: RecordSource,
    cursor_id: usize,
    is_index: bool,
    op: SeekOp,
) -> Result<SeekInternalResult> {
    let mv_store = program.connection.mv_store();
    /// wrapper so we can use the ? operator and handle errors correctly in this outer function
    fn inner(
        _program: &Program,
        state: &mut ProgramState,
        _pager: &Arc<Pager>,
        _mv_store: Option<&Arc<MvStore>>,
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
                                state.seek_state = OpSeekState::Seek {
                                    key: OpSeekKey::IndexKeyUnpacked {
                                        start_reg,
                                        num_regs,
                                    },
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
                    let original_value = state.registers[start_reg].get_value();
                    let mut temp_value = original_value.clone();

                    let conversion_successful = if matches!(temp_value, Value::Text(_)) {
                        let new_val = apply_numeric_affinity(temp_value.as_value_ref(), false);
                        let converted = new_val.is_some();
                        if let Some(new_val) = new_val {
                            temp_value = new_val.to_owned();
                        }
                        converted
                    } else {
                        true // Non-text values don't need conversion
                    };
                    let int_key = extract_int_value(&temp_value);
                    let lost_precision = !conversion_successful
                        || !matches!(temp_value, Value::Numeric(Numeric::Integer(_)));
                    let actual_op = if lost_precision {
                        match &temp_value {
                            Value::Numeric(Numeric::Float(f)) => {
                                let f_val = f64::from(*f);
                                // When extract_int_value clamped to i64::MAX/MIN,
                                // the cast `int_key as f64` loses the fact that the
                                // float is outside the i64 range. Detect this and
                                // set the comparison result directly.
                                //
                                // For i64::MAX: any float > 9223372036854774784.0
                                // is >= 9223372036854775808.0 (the next f64), which
                                // exceeds i64::MAX (9223372036854775807). i64::MAX
                                // is not exactly representable as f64, so the float
                                // is always strictly greater.
                                //
                                // For i64::MIN: -2^63 IS exactly representable as
                                // f64, so we must distinguish the exact match from
                                // floats that are strictly less.
                                let c = if int_key == i64::MAX && f_val > 9223372036854774784.0 {
                                    // Float exceeds i64::MAX, so int_key < float
                                    -1
                                } else if int_key == i64::MIN && f_val < -9223372036854774784.0 {
                                    if f_val == (i64::MIN as f64) {
                                        // Float is exactly i64::MIN
                                        0
                                    } else {
                                        // Float is below i64::MIN, so int_key > float
                                        1
                                    }
                                } else {
                                    let int_key_as_float = int_key as f64;
                                    if int_key_as_float > f_val {
                                        1
                                    } else if int_key_as_float < f_val {
                                        -1
                                    } else {
                                        0
                                    }
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
                    let seek_result = match key {
                        OpSeekKey::TableRowId(rowid) => {
                            let cursor = get_cursor!(state, cursor_id).as_btree_mut();
                            match cursor.seek(SeekKey::TableRowId(*rowid), *op)? {
                                IOResult::Done(seek_result) => seek_result,
                                IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                            }
                        }
                        OpSeekKey::IndexKeyFromRegister(record_reg) => {
                            let (cursor, record) = {
                                let (cursors, registers) = (&mut state.cursors, &state.registers);
                                let cursor = cursors
                                    .get_mut(cursor_id)
                                    .and_then(|c| c.as_mut())
                                    .expect("op_seek: cursor should be allocated")
                                    .as_btree_mut();
                                let record = match &registers[*record_reg] {
                                    Register::Record(ref record) => record,
                                    _ => unreachable!(
                                        "op_seek: record_reg should be a Record register when OpSeekKey::IndexKeyFromRegister is used"
                                    ),
                                };
                                (cursor, record)
                            };
                            match cursor.seek(SeekKey::IndexKey(record), *op)? {
                                IOResult::Done(seek_result) => seek_result,
                                IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                            }
                        }
                        OpSeekKey::IndexKeyUnpacked {
                            start_reg,
                            num_regs,
                        } => {
                            let start_reg = *start_reg;
                            let num_regs = *num_regs;
                            let cursor = get_cursor!(state, cursor_id).as_btree_mut();
                            let registers = &state.registers[start_reg..start_reg + num_regs];
                            match cursor.seek_unpacked(registers, *op)? {
                                IOResult::Done(seek_result) => seek_result,
                                IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                            }
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
                            !cursor.get_skip_advance(),
                            "skip_advance should not be true in the middle of a seek operation"
                        );
                        let result = match op {
                            // deliberately call get_next_record() instead of next() to avoid skip_advance triggering unwantedly
                            SeekOp::GT | SeekOp::GE { .. } => cursor.next()?,
                            SeekOp::LT | SeekOp::LE { .. } => cursor.prev()?,
                        };
                        match result {
                            IOResult::Done(()) => cursor.has_record(),
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
        mv_store.as_ref(),
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();
        let index_info = cursor.get_index_info().clone();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            // Create the comparison record from registers
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);
            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(
                idx_record,  // The serialized record from the index
                values,      // The record built from registers
                &index_info, // Sort order flags
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();
        let index_info = cursor.get_index_info().clone();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);
            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(idx_record, values, &index_info, 0, tie_breaker)?;

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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();
        let index_info = cursor.get_index_info().clone();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);
            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(idx_record, values, &index_info, 0, tie_breaker)?;

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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();
        let index_info = cursor.get_index_info().clone();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);

            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(idx_record, values, &index_info, 0, tie_breaker)?;

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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(DecrJumpZero { reg, target_pc }, insn);
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    match &mut state.registers[*reg] {
        Register::Value(Value::Numeric(Numeric::Integer(n))) => {
            *n = n.saturating_sub(1);
            if *n == 0 {
                state.pc = target_pc.as_offset_int();
            } else {
                state.pc += 1;
            }
        }
        Register::Value(_) | Register::Record(_) => {
            bail_constraint_error!("datatype mismatch");
        }
        Register::Aggregate(_) => {
            return Err(LimboError::InternalError(
                "DecrJumpZero: unexpected aggregate register".into(),
            ));
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

fn apply_kbn_step(acc: &mut Value, r: f64, state: &mut SumAggState) {
    let s = acc.as_float();
    let t = s + r;
    // When t is infinite, the KBN correction computes inf - inf = NaN,
    // which is meaningless. Skip compensation in that case.
    if t.is_finite() {
        let correction = if s.abs() > r.abs() {
            (s - t) + r
        } else {
            (r - t) + s
        };
        state.r_err += correction;
    }
    *acc = Value::from_f64(t);
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

/// Initialize aggregate payload with default values.
/// Payload layout by aggregate type:
/// - Count/Count0: [Integer(0)]
/// - Sum: [Null, Float(0.0), Integer(0), Integer(0)]  // acc, r_err, approx, ovrfl
/// - Total: [Float(0.0), Float(0.0), Integer(0), Integer(0)]  // same but starts at 0.0
/// - Avg: [Float(0.0), Float(0.0), Integer(0)]  // sum, r_err, count - uses KBN like SUM
/// - Min/Max: [Null]
/// - GroupConcat/StringAgg: [Null] (becomes Text on first non-null value)
/// - JsonGroupObject/JsonbGroupObject: [Blob([])]
/// - JsonGroupArray/JsonbGroupArray: [Blob([])]
fn init_agg_payload(func: &AggFunc, payload: &mut Vec<Value>) -> Result<()> {
    match func {
        AggFunc::Count | AggFunc::Count0 => payload.push(Value::from_i64(0)),
        AggFunc::Sum | AggFunc::Total => {
            let acc = if matches!(func, AggFunc::Total) {
                Value::from_f64(0.0)
            } else {
                Value::Null
            };
            payload.push(acc);
            payload.push(Value::from_f64(0.0));
            payload.push(Value::from_i64(0));
            payload.push(Value::from_i64(0));
        }
        AggFunc::Avg => {
            payload.push(Value::from_f64(0.0));
            payload.push(Value::from_f64(0.0));
            payload.push(Value::from_i64(0));
        }
        AggFunc::Min | AggFunc::Max => payload.push(Value::Null),
        AggFunc::GroupConcat | AggFunc::StringAgg => {
            // Use Null as sentinel to distinguish "no values yet" from "accumulated empty string"
            payload.push(Value::Null);
        }
        AggFunc::External(_) => {
            // External aggregates use ExternalAggState, not flat payload
            return Err(LimboError::InternalError(
                "External aggregate not supported in init_agg_payload".to_string(),
            ));
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            payload.push(Value::Blob(vec![]));
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            payload.push(Value::Blob(vec![]));
        }
    };
    Ok(())
}

/// Process a single input row and update the aggregate state in the payload.
///
/// This is the core aggregation logic shared between both aggregation strategies:
/// - **Register-based (sort-stream)**: Called from `op_agg_step` (AggStep instruction).
///   The payload lives in `AggContext::Builtin` stored in a register.
/// - **Hash-based (future enhancement)**: Called from `step_aggregate` during HashAggStep. The payload lives
///   in hash table entries keyed by GROUP BY values.
///
/// The payload slice contains the intermediate aggregate state (initialized by
/// `init_agg_payload`), and this function incorporates the new row's values.
///
/// # Payload layouts (see `init_agg_payload` for initial values):
/// - **Count**: `[count: Integer]` - increments if arg is not NULL
/// - **Count0**: `[count: Integer]` - always increments (COUNT(*))
/// - **Avg**: `[sum: Float, r_err: Float, count: Integer]` - uses KBN compensation like SUM
/// - **Sum/Total**: `[acc, r_err: Float, approx: Integer, ovrfl: Integer]`
///   - `acc`: running sum (Null/Integer/Float depending on inputs)
///   - `r_err`: Kahan-Babuška-Neumaier compensation term for floating-point precision
///   - `approx`: 1 if result is approximate (float arithmetic used)
///   - `ovrfl`: 1 if integer overflow occurred (Total promotes to float, Sum errors)
/// - **Min/Max**: `[current_extreme: Value]` - tracks min/max seen so far
/// - **GroupConcat/StringAgg**: `[accumulated: Null|Text]` - Null until first value, then Text
/// - **JsonGroup***: `[raw_jsonb: Blob]` - accumulated raw JSONB bytes
fn update_agg_payload(
    func: &AggFunc,
    arg: Value,                // most agg functions take one argument
    maybe_arg2: Option<Value>, // for GroupConcat/StringAgg, JsonGroupObject/JsonbGroupObject,
    payload: &mut [Value],
    collation: CollationSeq,
    comparator: &Option<crate::vdbe::sorter::SortComparator>,
) -> Result<()> {
    match func {
        AggFunc::Count => {
            // COUNT(column) increments only when arg is not NULL. Empty args treated as non-NULL
            // (would indicate a bug in query translation, but matches SQLite behavior of counting).
            if !matches!(arg, Value::Null) {
                // invariant as per init_agg_payload: payload[0] is always an integer
                let Value::Numeric(Numeric::Integer(i)) = &mut payload[0] else {
                    return Err(LimboError::InternalError(
                        "Count: payload is not an integer".to_string(),
                    ));
                };
                *i = i.checked_add(1).ok_or(LimboError::IntegerOverflow)?;
            }
        }
        AggFunc::Count0 => {
            // invariant as per init_agg_payload: payload[0] is always an integer
            let Value::Numeric(Numeric::Integer(i)) = &mut payload[0] else {
                return Err(LimboError::InternalError(
                    "Count0: payload is not an integer".to_string(),
                ));
            };
            *i = i.checked_add(1).ok_or(LimboError::IntegerOverflow)?;
        }
        AggFunc::Avg => {
            if matches!(arg, Value::Null) {
                return Ok(());
            }
            // invariant as per init_agg_payload: payload[0] is Float (sum), payload[1] is Float (r_err), payload[2] is Integer (count)
            let [sum_val, r_err_val, count_val, ..] = payload else {
                return Err(LimboError::InternalError(
                    "Avg: payload too short".to_string(),
                ));
            };
            let r_err = r_err_val.as_float();
            let Value::Numeric(Numeric::Integer(count)) = count_val else {
                return Err(LimboError::InternalError(
                    "Avg: payload[2] is not an integer".to_string(),
                ));
            };
            let val = match arg {
                Value::Numeric(Numeric::Integer(i)) => i as f64,
                Value::Numeric(Numeric::Float(f)) => f64::from(f),
                Value::Text(t) => match try_for_float(t.as_str().as_bytes()).1 {
                    ParsedNumber::Integer(i) => i as f64,
                    ParsedNumber::Float(f) => f,
                    ParsedNumber::None => 0.0,
                },
                Value::Blob(b) => match try_for_float(&b).1 {
                    ParsedNumber::Integer(i) => i as f64,
                    ParsedNumber::Float(f) => f,
                    ParsedNumber::None => 0.0,
                },
                _ => unreachable!(),
            };
            // Use Kahan-Babuška-Neumaier compensation for better floating-point precision
            let s = sum_val.as_float();
            let t = s + val;
            // When t is infinite, the KBN correction computes inf - inf = NaN,
            // which is meaningless. Skip compensation in that case.
            if t.is_finite() {
                let correction = if s.abs() > val.abs() {
                    (s - t) + val
                } else {
                    (val - t) + s
                };
                *r_err_val = Value::from_f64(r_err + correction);
            }
            *sum_val = Value::from_f64(t);
            *count = count.checked_add(1).ok_or(LimboError::IntegerOverflow)?;
        }
        AggFunc::Sum | AggFunc::Total => {
            // invariant as per init_agg_payload: payload[0] is acc (Null/Integer/Float),
            // payload[1] is Float (r_err), payload[2] is Integer (approx), payload[3] is Integer (ovrfl)
            let [acc, r_err_val, approx_val, ovrfl_val, ..] = payload else {
                return Err(LimboError::InternalError(
                    "Sum/Total: payload too short".to_string(),
                ));
            };
            let r_err_f = r_err_val.as_float();
            let Value::Numeric(Numeric::Integer(approx_i)) = approx_val else {
                return Err(LimboError::InternalError(
                    "Sum/Total: payload[2] is not an integer".to_string(),
                ));
            };
            let Value::Numeric(Numeric::Integer(ovrfl_i)) = ovrfl_val else {
                return Err(LimboError::InternalError(
                    "Sum/Total: payload[3] is not an integer".to_string(),
                ));
            };
            let mut sum_state = SumAggState {
                r_err: r_err_f,
                approx: *approx_i != 0,
                ovrfl: *ovrfl_i != 0,
            };
            match arg {
                Value::Null => {}
                Value::Numeric(Numeric::Integer(i)) => match acc {
                    Value::Null => {
                        *acc = Value::from_i64(i);
                    }
                    Value::Numeric(Numeric::Integer(acc_i)) => match acc_i.checked_add(i) {
                        Some(sum) => *acc_i = sum,
                        None => {
                            if matches!(func, AggFunc::Total) {
                                let acc_f = *acc_i as f64;
                                *acc = Value::from_f64(acc_f);
                                sum_state.approx = true;
                                sum_state.ovrfl = true;
                                apply_kbn_step_int(acc, i, &mut sum_state);
                            } else {
                                return Err(LimboError::IntegerOverflow);
                            }
                        }
                    },
                    Value::Numeric(Numeric::Float(_)) => {
                        apply_kbn_step_int(acc, i, &mut sum_state);
                    }
                    _ => unreachable!("Sum/Total accumulator initialized to Null/Integer/Float"),
                },
                Value::Numeric(Numeric::Float(f)) => match acc {
                    Value::Null => {
                        *acc = Value::Numeric(Numeric::Float(f));
                        sum_state.approx = true;
                    }
                    Value::Numeric(Numeric::Integer(i)) => {
                        *acc = Value::from_f64(*i as f64);
                        sum_state.approx = true;
                        apply_kbn_step(acc, f64::from(f), &mut sum_state);
                    }
                    Value::Numeric(Numeric::Float(_)) => {
                        sum_state.approx = true;
                        apply_kbn_step(acc, f64::from(f), &mut sum_state);
                    }
                    _ => unreachable!("Sum/Total accumulator initialized to Null/Integer/Float"),
                },
                Value::Text(t) => {
                    let (parse_result, parsed_number) = try_for_float(t.as_str().as_bytes());
                    handle_text_sum(acc, &mut sum_state, parsed_number, parse_result, false);
                }
                Value::Blob(b) => {
                    let (parse_result, parsed_number) = try_for_float(&b);
                    handle_text_sum(acc, &mut sum_state, parsed_number, parse_result, true);
                }
            }
            *r_err_val = Value::from_f64(sum_state.r_err);
            *approx_i = sum_state.approx as i64;
            *ovrfl_i = sum_state.ovrfl as i64;
        }
        AggFunc::Min | AggFunc::Max => {
            if matches!(arg, Value::Null) {
                return Ok(());
            }
            if matches!(payload[0], Value::Null) {
                payload[0] = arg;
                return Ok(());
            }
            use std::cmp::Ordering;
            // Use custom type comparator if available, otherwise fall back to collation
            let cmp = if let Some(ref cmp_fn) = comparator {
                let arg_ref = arg.as_ref();
                let payload_ref = payload[0].as_ref();
                cmp_fn(&arg_ref, &payload_ref)
            } else {
                compare_with_collation(&arg, &payload[0], Some(collation))
            };
            let should_update = match func {
                AggFunc::Max => cmp == Ordering::Greater,
                AggFunc::Min => cmp == Ordering::Less,
                _ => false,
            };
            if should_update {
                payload[0] = arg;
            }
        }
        AggFunc::GroupConcat | AggFunc::StringAgg => {
            if matches!(arg, Value::Null) {
                return Ok(());
            }
            let delimiter = maybe_arg2.unwrap_or_else(|| Value::build_text(","));
            let acc = &mut payload[0];
            if matches!(acc, Value::Null) {
                // First non-null value: convert to Text
                *acc = Value::build_text(arg.to_string());
            } else {
                acc.exec_group_concat(&delimiter);
                acc.exec_group_concat(&arg);
            }
        }
        AggFunc::External(_) => {
            return Err(LimboError::InternalError(
                "External aggregate not supported in update_agg_payload".to_string(),
            ));
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            // arg = key, maybe_arg2 = value
            let Some(value) = maybe_arg2 else {
                return Err(LimboError::InternalError(
                    "JsonGroupObject/JsonbGroupObject: no value provided".to_string(),
                ));
            };
            let mut key_vec = convert_dbtype_to_raw_jsonb(&arg)?;
            let mut val_vec = convert_dbtype_to_raw_jsonb(&value)?;
            let Value::Blob(vec) = &mut payload[0] else {
                return Err(LimboError::InternalError(
                    "JsonGroupObject: payload[0] is not a blob".to_string(),
                ));
            };
            if vec.is_empty() {
                // bits for obj header
                vec.push(12);
            }
            vec.append(&mut key_vec);
            vec.append(&mut val_vec);
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            // arg = value
            let mut data = convert_dbtype_to_raw_jsonb(&arg)?;
            let Value::Blob(vec) = &mut payload[0] else {
                return Err(LimboError::InternalError(
                    "JsonGroupArray: payload[0] is not a blob".to_string(),
                ));
            };
            if vec.is_empty() {
                vec.push(11); // bits for array header
            }
            vec.append(&mut data);
        }
    }
    Ok(())
}

/// Convert the intermediate aggregate state in `payload` into the final result value.
///
/// This finalization logic is shared between both aggregation strategies:
/// - **Register-based (sort-stream)**: Called from `op_agg_final` (AggFinal/AggValue
///   instructions) when a group boundary is crossed.
/// - **Hash-based (future enhancement)**: Called during the emit phase of HashAggNext after all rows have
///   been processed. The payload may have been merged via `merge_agg_payload` if
///   spilling occurred.
///
/// # Finalization logic by aggregate type:
/// - **Count/Count0**: Returns the count directly
/// - **Avg**: Computes `sum / count`, returns NULL if count is 0
/// - **Sum**: Returns the accumulated value, applying Kahan compensation if approximate.
///   Returns NULL if no non-NULL values were seen (unless float arithmetic was used).
/// - **Total**: Like Sum but always returns Float, defaulting to 0.0 for empty groups
/// - **Min/Max**: Returns the tracked extreme value directly
/// - **GroupConcat/StringAgg**: Returns the accumulated string
/// - **JsonGroup***: Parses accumulated raw JSONB bytes into proper JSON output
fn finalize_agg_payload(func: &AggFunc, payload: &[Value]) -> Result<Value> {
    let val = match func {
        AggFunc::Count | AggFunc::Count0 => payload[0].clone(),
        AggFunc::Avg => {
            // Payload: [sum, r_err, count]
            let sum = payload[0].as_float();
            let r_err = payload[1].as_float();
            let count = payload[2].as_int().unwrap_or(0);
            if count == 0 {
                Value::Null
            } else {
                // Apply KBN compensation before dividing
                Value::from_f64((sum + r_err) / count as f64)
            }
        }
        AggFunc::Sum => {
            let acc = &payload[0];
            let approx = payload[2].as_int().unwrap_or(0) != 0;
            let ovrfl = payload[3].as_int().unwrap_or(0) != 0;
            let r_err = payload[1].as_float();
            match acc {
                Value::Null => {
                    if approx {
                        Value::from_f64(0.0)
                    } else {
                        Value::Null
                    }
                }
                Value::Numeric(Numeric::Integer(i)) if !approx && !ovrfl => Value::from_i64(*i),
                _ => Value::from_f64(acc.as_float() + r_err),
            }
        }
        AggFunc::Total => {
            // Payload: [acc, r_err, approx, ovrfl]
            let acc = &payload[0];
            let r_err = payload[1].as_float();
            match acc {
                Value::Null => Value::from_f64(0.0),
                Value::Numeric(Numeric::Integer(i)) => Value::from_f64(*i as f64 + r_err),
                Value::Numeric(Numeric::Float(f)) => Value::from_f64(f64::from(*f) + r_err),
                _ => unreachable!("Total accumulator initialized to Null/Integer/Float"),
            }
        }
        AggFunc::Min | AggFunc::Max => payload[0].clone(),
        AggFunc::GroupConcat | AggFunc::StringAgg => payload[0].clone(),
        AggFunc::External(_) => {
            // External aggregates are finalized via AggContext::compute_external()
            return Err(LimboError::InternalError(
                "finalize_agg_payload called for External aggregate".to_string(),
            ));
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject => {
            let data = payload[0].to_blob().expect("Should be blob");
            json_from_raw_bytes_agg(data, false)?
        }
        #[cfg(feature = "json")]
        AggFunc::JsonbGroupObject => {
            let data = payload[0].to_blob().expect("Should be blob");
            json_from_raw_bytes_agg(data, true)?
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray => {
            let data = payload[0].to_blob().expect("Should be blob");
            json_from_raw_bytes_agg(data, false)?
        }
        #[cfg(feature = "json")]
        AggFunc::JsonbGroupArray => {
            let data = payload[0].to_blob().expect("Should be blob");
            json_from_raw_bytes_agg(data, true)?
        }
    };

    Ok(val)
}

pub fn op_agg_step(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        AggStep {
            acc_reg,
            col,
            delimiter,
            func,
            comparator_func_name,
        },
        insn
    );

    // Initialize aggregate state if not already done
    if let Register::Value(Value::Null) = state.registers[*acc_reg] {
        state.registers[*acc_reg] = match func {
            AggFunc::External(ext_func) => match ext_func.as_ref() {
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
            _ => {
                // Built-in aggregates use flat payload
                let mut payload = Vec::new();
                init_agg_payload(func, &mut payload)?;
                Register::Aggregate(AggContext::Builtin(payload))
            }
        };
    }

    // Resolve custom type comparator for MIN/MAX if provided
    let comparator = comparator_func_name
        .as_deref()
        .and_then(make_sort_comparator);

    // Step the aggregate
    match func {
        AggFunc::External(_) => {
            // External aggregates use FFI and need special handling
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
        _ => {
            let arg = state.registers[*col].get_value().clone();
            // Only a subset of aggregate functions take two arguments
            let maybe_arg2 = match func {
                AggFunc::GroupConcat | AggFunc::StringAgg => {
                    Some(state.registers[*delimiter].get_value().clone())
                }
                #[cfg(feature = "json")]
                AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
                    Some(state.registers[*delimiter].get_value().clone())
                }
                _ => None,
            };
            let collation = state.current_collation.unwrap_or(CollationSeq::Binary);

            // Now get mutable borrow on payload
            let Register::Aggregate(agg) = &mut state.registers[*acc_reg] else {
                panic!(
                    "Unexpected value {:?} in AggStep at register {}",
                    state.registers[*acc_reg], *acc_reg
                );
            };
            let payload = agg.payload_mut();
            update_agg_payload(func, arg, maybe_arg2, payload, collation, &comparator)?;
        }
    };

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_agg_final(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
        Register::Aggregate(agg) => {
            let value = match agg {
                AggContext::External(_) => {
                    // External aggregates use FFI finalization
                    agg.compute_external()?
                }
                AggContext::Builtin(payload) => {
                    // Built-in aggregates use shared finalization
                    finalize_agg_payload(func, payload)?
                }
            };
            state.registers[dest_reg] = Register::Value(value);
        }
        Register::Value(Value::Null) => {
            // When the set is empty, return appropriate default
            match func {
                AggFunc::Total => {
                    state.registers[dest_reg] = Register::Value(Value::from_f64(0.0));
                }
                AggFunc::Count | AggFunc::Count0 => {
                    state.registers[dest_reg] = Register::Value(Value::from_i64(0));
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
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SorterOpen {
            cursor_id,
            columns: _,
            order_and_collations,
            comparator_func_names,
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
    let (order, collations): (Vec<_>, Vec<_>) = order_and_collations
        .iter()
        .map(|(ord, coll)| (*ord, coll.unwrap_or_default()))
        .unzip();
    let comparators = comparator_func_names
        .iter()
        .map(|name| name.as_deref().and_then(make_sort_comparator))
        .collect();
    let temp_store = program.connection.get_temp_store();
    let cursor = Sorter::new(
        &order,
        collations,
        comparators,
        max_buffer_size_bytes,
        page_size,
        pager.io.clone(),
        temp_store,
    );
    let cursors = &mut state.cursors;
    cursors
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid")
        .replace(Cursor::new_sorter(cursor));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_data(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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

pub fn op_sorter_compare(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SorterCompare {
            cursor_id,
            sorted_record_reg,
            num_regs,
            pc_when_nonequal,
        },
        insn
    );

    let previous_sorter_values = {
        let Register::Record(record) = &state.registers[*sorted_record_reg] else {
            return Err(LimboError::InternalError(
                "Sorted record must be a record".to_string(),
            ));
        };
        &record.get_values_range(0..*num_regs)?
    };

    // Inlined `state.get_cursor` to prevent borrowing conflit with `state.registers`
    let cursor = state
        .cursors
        .get_mut(*cursor_id)
        .unwrap_or_else(|| panic!("cursor id {cursor_id} out of bounds"))
        .as_mut()
        .unwrap_or_else(|| panic!("cursor id {cursor_id} is None"));

    let cursor = cursor.as_sorter_mut();
    let Some(current_sorter_record) = cursor.record() else {
        return Err(LimboError::InternalError(
            "Sorter must have a record".to_string(),
        ));
    };

    let current_sorter_values = &current_sorter_record.get_values_range(0..*num_regs)?;
    // If the current sorter record has a NULL in any of the significant fields, the comparison is not equal.
    let is_equal = current_sorter_values
        .iter()
        .all(|v| !matches!(v, ValueRef::Null))
        && compare_immutable(
            previous_sorter_values,
            current_sorter_values,
            &cursor.index_key_info,
        )
        .is_eq();
    if is_equal {
        state.pc += 1;
    } else {
        state.pc = pc_when_nonequal.as_offset_int();
    }
    Ok(InsnFunctionStepResult::Step)
}

/// Insert the integer value held by register P2 into a RowSet object held in register P1.
///
/// An assertion fails if P2 is not an integer.
pub fn op_rowset_add(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        RowSetAdd {
            rowset_reg,
            value_reg,
        },
        insn
    );

    let value = state.registers[*value_reg].get_value();
    let rowid = match value {
        Value::Numeric(Numeric::Integer(i)) => *i,
        _ => {
            return Err(LimboError::InternalError(
                "RowSetAdd: P2 must be an integer".to_string(),
            ));
        }
    };

    let rowset = state.rowsets.entry(*rowset_reg).or_default();

    rowset.insert(rowid);

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Extract the smallest value from the RowSet object in P1 and put that value into register P3.
/// Or, if RowSet object P1 is initially empty, leave P3 unchanged and jump to instruction P2.
pub fn op_rowset_read(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        RowSetRead {
            rowset_reg,
            pc_if_empty,
            dest_reg,
        },
        insn
    );
    assert!(pc_if_empty.is_offset());

    let rowset = state.rowsets.get_mut(rowset_reg);

    match rowset {
        Some(rowset) => {
            if rowset.is_empty() {
                state.pc = pc_if_empty.as_offset_int();
            } else if let Some(smallest) = rowset.smallest() {
                state.registers[*dest_reg] = Register::Value(Value::from_i64(smallest));
                state.pc += 1;
            } else {
                state.pc = pc_if_empty.as_offset_int();
            }
        }
        None => {
            state.pc = pc_if_empty.as_offset_int();
        }
    }

    Ok(InsnFunctionStepResult::Step)
}

/// Register P3 is assumed to hold a 64-bit integer value. If register P1 contains a RowSet object
/// and that RowSet object contains the value held in P3, jump to register P2. Otherwise, insert
/// the integer in P3 into the RowSet and continue on to the next opcode.
///
/// The RowSet object is optimized for the case where sets of integers are inserted in distinct
/// phases, which each set contains no duplicates. Each set is identified by a unique P4 value.
/// The first set must have P4==0, the final set must have P4==-1, and for all other sets must
/// have P4>0.
///
/// This allows optimizations: (a) when P4==0 there is no need to test the RowSet object for P3,
/// as it is guaranteed not to contain it, (b) when P4==-1 there is no need to insert the value,
/// as it will never be tested for, and (c) when a value that is part of set X is inserted, there
/// is no need to search to see if the same value was previously inserted as part of set X (only
/// if it was previously inserted as part of some other set).
pub fn op_rowset_test(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        RowSetTest {
            rowset_reg,
            pc_if_found,
            value_reg,
            batch,
        },
        insn
    );
    assert!(pc_if_found.is_offset());

    let value = state.registers[*value_reg].get_value();
    let rowid = match value {
        Value::Numeric(Numeric::Integer(i)) => *i,
        _ => {
            return Err(LimboError::InternalError(
                "RowSetTest: P3 must be an integer".to_string(),
            ));
        }
    };

    let rowset = state.rowsets.entry(*rowset_reg).or_default();

    let found = if *batch == 0 {
        // SQLite rowsets assume that in each batch, the caller makes sure no
        // duplicates are inserted. Hence if batch==0, we can return false without
        // checking.
        false
    } else {
        rowset.test(rowid, *batch)
    };

    if found {
        state.pc = pc_if_found.as_offset_int();
    } else {
        if *batch != -1 {
            rowset.insert(rowid);
        }
        state.pc += 1;
    }

    Ok(InsnFunctionStepResult::Step)
}

pub fn op_function(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Function {
            constant_mask: _,
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
                let reg_values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);

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
                        let reg_values = registers_to_ref_values(
                            &state.registers[*start_reg + 1..*start_reg + arg_count],
                        );

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
                        let reg_values = registers_to_ref_values(
                            &state.registers[*start_reg + 1..*start_reg + arg_count],
                        );

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
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonbRemove => {
                if let Ok(json) = jsonb_remove(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonReplace => {
                if let Ok(json) = json_replace(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonbReplace => {
                if let Ok(json) = jsonb_replace(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonInsert => {
                if let Ok(json) = json_insert(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::Value(json);
                } else {
                    state.registers[*dest] = Register::Value(Value::Null);
                }
            }
            JsonFunc::JsonbInsert => {
                if let Ok(json) = jsonb_insert(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
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
                        Value::Numeric(Numeric::Integer(val)) => &val.to_string(),
                        Value::Numeric(Numeric::Float(val)) => &f64::from(*val).to_string(),
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
                let reg_values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);

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
                let reg_values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);

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
                state.registers[*dest] = Register::Value(Value::from_i64(changes));
            }
            ScalarFunc::Char => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest] = Register::Value(Value::exec_char(
                    reg_values.iter().map(|reg| reg.get_value()),
                ));
            }
            ScalarFunc::Coalesce => {}
            ScalarFunc::Concat => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                let result =
                    Value::exec_concat_strings(reg_values.iter().map(|reg| reg.get_value()));
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::ConcatWs => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                let result = Value::exec_concat_ws(reg_values.iter().map(|reg| reg.get_value()));
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Glob => {
                if arg_count != 2 {
                    return Err(LimboError::ParseError(
                        "wrong number of arguments to function GLOB()".to_string(),
                    ));
                }
                let pattern_reg = &state.registers[*start_reg];
                let match_reg = &state.registers[*start_reg + 1];

                let pattern_value = pattern_reg.get_value();
                let match_value = match_reg.get_value();

                if pattern_value == &Value::Null || match_value == &Value::Null {
                    state.registers[*dest] = Register::Value(Value::Null);
                } else {
                    let pattern_cow = match pattern_value {
                        Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                        v => match v.exec_cast("TEXT") {
                            Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                            _ => unreachable!("Cast to TEXT should yield Text"),
                        },
                    };

                    let match_cow = match match_value {
                        Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                        v => match v.exec_cast("TEXT") {
                            Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                            _ => unreachable!("Cast to TEXT should yield Text"),
                        },
                    };

                    let matches = Value::exec_glob(&pattern_cow, &match_cow)?;
                    state.registers[*dest] = Register::Value(Value::from_i64(matches as i64));
                }
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
                    Register::Value(Value::from_i64(program.connection.last_insert_rowid()));
            }
            ScalarFunc::Like => {
                let pattern_reg = &state.registers[*start_reg];
                let match_reg = &state.registers[*start_reg + 1];

                let pattern_value = pattern_reg.get_value();
                let match_value = match_reg.get_value();

                // 1. Check for NULL inputs
                if pattern_value == &Value::Null || match_value == &Value::Null {
                    state.registers[*dest] = Register::Value(Value::Null);
                } else {
                    // 2. Resolve Escape Character (if 3rd arg exists)
                    let mut escape_char = None;
                    let mut is_null_result = false;

                    if arg_count == 3 {
                        let escape_value = state.registers[*start_reg + 2].get_value();
                        match escape_value {
                            Value::Null => {
                                is_null_result = true;
                            }
                            _ => {
                                let escape_cow = match escape_value {
                                    Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                                    v => match v.exec_cast("TEXT") {
                                        Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                                        _ => unreachable!("Cast to TEXT should yield Text"),
                                    },
                                };

                                let mut chars = escape_cow.chars();
                                let c = chars.next();
                                if c.is_none() || chars.next().is_some() {
                                    return Err(LimboError::Constraint(
                                        "ESCAPE expression must be a single character".to_string(),
                                    ));
                                }
                                escape_char = c;
                            }
                        }
                    }

                    if is_null_result {
                        state.registers[*dest] = Register::Value(Value::Null);
                    } else {
                        // 3. Prepare Pattern and Text
                        let pattern_cow = match pattern_value {
                            Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                            v => match v.exec_cast("TEXT") {
                                Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                                _ => unreachable!("Cast to TEXT should yield Text"),
                            },
                        };

                        let match_cow = match match_value {
                            Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                            v => match v.exec_cast("TEXT") {
                                Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                                _ => unreachable!("Cast to TEXT should yield Text"),
                            },
                        };

                        // 4. Execute Like
                        let matches = Value::exec_like(&pattern_cow, &match_cow, escape_char)?;
                        state.registers[*dest] = Register::Value(Value::from_i64(matches as i64));
                    }
                }
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
                    ScalarFunc::RandomBlob => {
                        Some(reg_value.exec_randomblob(|dest| pager.io.fill_bytes(dest))?)
                    }
                    ScalarFunc::ZeroBlob => Some(reg_value.exec_zeroblob()?),
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
                state.registers[*dest] =
                    Register::Value(Value::exec_random(|| pager.io.generate_random_number()));
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
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_date(values);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::Time => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_time(values);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::TimeDiff => {
                if arg_count != 2 {
                    state.registers[*dest] = Register::Value(Value::Null);
                } else {
                    let start = state.registers[*start_reg].get_value();
                    let end = state.registers[*start_reg + 1].get_value();

                    let result = crate::functions::datetime::exec_timediff([start, end]);

                    state.registers[*dest] = Register::Value(result);
                }
            }
            ScalarFunc::TotalChanges => {
                let res = &program.connection.total_changes;
                let total_changes = res.load(Ordering::SeqCst);
                state.registers[*dest] = Register::Value(Value::from_i64(total_changes));
            }
            ScalarFunc::DateTime => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_datetime_full(values);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::JulianDay => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_julianday(values);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::UnixEpoch => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_unixepoch(values);
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::TursoVersion => {
                if !program.connection.is_db_initialized() {
                    state.registers[*dest] =
                        Register::Value(Value::build_text(info::build::PKG_VERSION));
                } else {
                    let version_integer =
                        return_if_io!(pager.with_header(|header| header.version_number)).get()
                            as i64;
                    let version = execute_turso_version(version_integer);
                    state.registers[*dest] = Register::Value(Value::build_text(version));
                }
            }
            ScalarFunc::SqliteVersion => {
                let version = execute_sqlite_version();
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
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_strftime(values);
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
                    use crate::types::TextSubtype;

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
                                )));
                            }
                        }
                    };

                    let mut json = json::jsonb::Jsonb::make_empty_array(table.columns().len() * 10);
                    for column in table.columns() {
                        use crate::types::TextRef;

                        let name = column.name.as_ref().expect("column should have a name");
                        let name_json = json::convert_ref_dbtype_to_jsonb(
                            ValueRef::Text(TextRef::new(name, TextSubtype::Text)),
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
                    use crate::types::ValueIterator;
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

                    let mut payload_iterator = ValueIterator::new(bin_record.as_slice())?;

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

                        let val = match payload_iterator.next() {
                            Some(Ok(v)) => v,
                            Some(Err(e)) => return Err(e),
                            None => {
                                return Err(LimboError::InvalidArgument(
                                    "bin_record_json_object: binary record has fewer columns than specified in the columns argument".to_string()
                                ));
                            }
                        };

                        if let ValueRef::Blob(..) = val {
                            return Err(LimboError::InvalidArgument(
                                "bin_record_json_object: formatting of BLOB values stored in binary record is not supported".to_string()
                            ));
                        }
                        let val_json =
                            json::convert_ref_dbtype_to_jsonb(val, json::Conv::NotStrict)?;
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
            ScalarFunc::StatInit => {
                // stat_init(n_col): Initialize a statistics accumulator
                // Returns a blob containing the serialized StatAccum
                assert!(arg_count >= 1);
                let n_col = match state.registers[*start_reg].get_value() {
                    Value::Numeric(Numeric::Integer(n)) => *n as usize,
                    _ => 0,
                };
                let accum = StatAccum::new(n_col);
                state.registers[*dest] = Register::Value(Value::Blob(accum.to_bytes()));
            }
            ScalarFunc::StatPush => {
                // stat_push(accum_blob, i_chng): Push a row into the accumulator
                // i_chng is the index of the leftmost column that changed from the previous row
                // Returns the updated accumulator blob
                assert!(arg_count >= 2);
                let accum_blob = state.registers[*start_reg].get_value();
                let i_chng = match state.registers[*start_reg + 1].get_value() {
                    Value::Numeric(Numeric::Integer(n)) => *n as usize,
                    _ => 0,
                };
                let result = match accum_blob {
                    Value::Blob(bytes) => {
                        if let Some(mut accum) = StatAccum::from_bytes(bytes) {
                            accum.push(i_chng);
                            Value::Blob(accum.to_bytes())
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null,
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::StatGet => {
                // stat_get(accum_blob): Get the stat1 string from the accumulator
                // Returns the stat string "total avg1 avg2 ..."
                assert!(arg_count >= 1);
                let accum_blob = state.registers[*start_reg].get_value();
                let result = match accum_blob {
                    Value::Blob(bytes) => {
                        if let Some(accum) = StatAccum::from_bytes(bytes) {
                            let stat_str = accum.get_stat1();
                            if stat_str.is_empty() {
                                Value::Null
                            } else {
                                Value::build_text(stat_str)
                            }
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null,
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::ConnTxnId => {
                // conn_txn_id(candidate): get-or-set semantics for CDC transaction ID.
                // If unset (-1), store the candidate and return it.
                // If already set, return the existing value, ignoring the candidate.
                assert_eq!(arg_count, 1);
                let candidate = match state.registers[*start_reg].get_value() {
                    Value::Numeric(Numeric::Integer(n)) => *n,
                    _ => -1,
                };
                let current = program.connection.get_cdc_transaction_id();
                if current == -1 {
                    program.connection.set_cdc_transaction_id(candidate);
                    state.registers[*dest] = Register::Value(Value::from_i64(candidate));
                } else {
                    state.registers[*dest] = Register::Value(Value::from_i64(current));
                }
            }
            ScalarFunc::IsAutocommit => {
                // is_autocommit(): returns 1 if autocommit, 0 otherwise.
                let auto_commit = program.connection.auto_commit.load(Ordering::SeqCst);
                state.registers[*dest] =
                    Register::Value(Value::from_i64(if auto_commit { 1 } else { 0 }));
            }
            ScalarFunc::TestUintEncode => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Numeric(Numeric::Integer(i)) => {
                        if *i < 0 {
                            return Err(LimboError::InternalError(
                                "test_uint_encode: negative value".to_string(),
                            ));
                        }
                        Value::build_text(i.to_string())
                    }
                    Value::Numeric(Numeric::Float(f)) => {
                        if *f < 0.0 || f.fract() != 0.0 {
                            return Err(LimboError::InternalError(
                                "test_uint_encode: not a non-negative integer".to_string(),
                            ));
                        }
                        Value::build_text((f64::from(*f) as u64).to_string())
                    }
                    Value::Text(t) => {
                        let s = t.to_string();
                        s.parse::<u64>().map_err(|_| {
                            LimboError::InternalError(format!(
                                "test_uint_encode: invalid uint: {s}"
                            ))
                        })?;
                        Value::build_text(s)
                    }
                    _ => {
                        return Err(LimboError::InternalError(
                            "test_uint_encode: unsupported type".to_string(),
                        ));
                    }
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::TestUintDecode => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    other => other.clone(),
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::TestUintAdd
            | ScalarFunc::TestUintSub
            | ScalarFunc::TestUintMul
            | ScalarFunc::TestUintDiv => {
                check_arg_count!(arg_count, 2);
                let a = parse_test_uint(&state.registers[*start_reg])?;
                let b = parse_test_uint(&state.registers[*start_reg + 1])?;
                let result = match (a, b) {
                    (Some(a), Some(b)) => {
                        let r = match scalar_func {
                            ScalarFunc::TestUintAdd => a.checked_add(b),
                            ScalarFunc::TestUintSub => a.checked_sub(b),
                            ScalarFunc::TestUintMul => a.checked_mul(b),
                            ScalarFunc::TestUintDiv => {
                                if b == 0 {
                                    None
                                } else {
                                    Some(a / b)
                                }
                            }
                            _ => unreachable!(),
                        };
                        match r {
                            Some(v) => Value::build_text(v.to_string()),
                            None => {
                                return Err(LimboError::InternalError(
                                    "test_uint arithmetic overflow/underflow".to_string(),
                                ));
                            }
                        }
                    }
                    _ => Value::Null,
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::TestUintLt | ScalarFunc::TestUintEq => {
                check_arg_count!(arg_count, 2);
                let a = parse_test_uint(&state.registers[*start_reg])?;
                let b = parse_test_uint(&state.registers[*start_reg + 1])?;
                let result = match (a, b) {
                    (Some(a), Some(b)) => {
                        let cmp = match scalar_func {
                            ScalarFunc::TestUintLt => a < b,
                            ScalarFunc::TestUintEq => a == b,
                            _ => unreachable!(),
                        };
                        Value::from_i64(if cmp { 1 } else { 0 })
                    }
                    _ => Value::Null,
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::StringReverse => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Text(t) => {
                        let reversed: String = t.to_string().chars().rev().collect();
                        Value::build_text(reversed)
                    }
                    other => {
                        let s = other.to_string();
                        let reversed: String = s.chars().rev().collect();
                        Value::build_text(reversed)
                    }
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::BooleanToInt => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Numeric(Numeric::Integer(i)) => match *i {
                        0 => Value::from_i64(0),
                        1 => Value::from_i64(1),
                        _ => {
                            return Err(LimboError::Constraint(format!(
                                "invalid input for type boolean: \"{i}\""
                            )));
                        }
                    },
                    Value::Text(t) => {
                        let v = &t.value;
                        match v.to_ascii_lowercase().as_str() {
                            "true" | "t" | "yes" | "on" | "1" => Value::from_i64(1),
                            "false" | "f" | "no" | "off" | "0" => Value::from_i64(0),
                            _ => {
                                return Err(LimboError::Constraint(format!(
                                    "invalid input for type boolean: \"{v}\""
                                )));
                            }
                        }
                    }
                    other => {
                        return Err(LimboError::Constraint(format!(
                            "invalid input for type boolean: \"{other}\""
                        )));
                    }
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::IntToBoolean => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Numeric(Numeric::Integer(0)) => Value::build_text("false".to_string()),
                    _ => Value::build_text("true".to_string()),
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::ValidateIpAddr => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Text(t) => {
                        let v = &t.value;
                        v.parse::<std::net::IpAddr>().map_err(|_| {
                            LimboError::Constraint(format!("invalid input for type inet: \"{v}\""))
                        })?;
                        val.get_value().clone()
                    }
                    other => {
                        return Err(LimboError::Constraint(format!(
                            "invalid input for type inet: \"{other}\""
                        )));
                    }
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::NumericEncode => {
                check_arg_count!(arg_count, 3);
                let val = &state.registers[*start_reg];
                let precision_reg = &state.registers[*start_reg + 1];
                let scale_reg = &state.registers[*start_reg + 2];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    other => {
                        use crate::numeric::decimal::{
                            bigdecimal_to_blob, validate_precision_scale,
                        };
                        use bigdecimal::BigDecimal;
                        use std::str::FromStr;

                        let precision = match precision_reg.get_value() {
                            Value::Numeric(Numeric::Integer(i)) => *i,
                            _ => {
                                return Err(LimboError::Constraint(
                                    "numeric_encode: precision must be an integer".to_string(),
                                ));
                            }
                        };
                        let scale = match scale_reg.get_value() {
                            Value::Numeric(Numeric::Integer(i)) => *i,
                            _ => {
                                return Err(LimboError::Constraint(
                                    "numeric_encode: scale must be an integer".to_string(),
                                ));
                            }
                        };
                        let text = match other {
                            Value::Numeric(Numeric::Integer(i)) => i.to_string(),
                            Value::Numeric(Numeric::Float(f)) => f.to_string(),
                            Value::Text(t) => t.value.to_string(),
                            _ => {
                                return Err(LimboError::Constraint(format!(
                                    "invalid input for type numeric: \"{other}\""
                                )));
                            }
                        };
                        let bd = BigDecimal::from_str(&text).map_err(|_| {
                            LimboError::Constraint(format!(
                                "invalid input for type numeric: \"{text}\""
                            ))
                        })?;
                        let validated = validate_precision_scale(&bd, precision, scale)?;
                        Value::from_blob(bigdecimal_to_blob(&validated))
                    }
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::NumericDecode => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Blob(b) => {
                        let bd = crate::numeric::decimal::blob_to_bigdecimal(b)?;
                        Value::build_text(crate::numeric::decimal::format_numeric(&bd))
                    }
                    other => {
                        return Err(LimboError::Constraint(format!(
                            "numeric_decode: expected blob, got \"{other}\""
                        )));
                    }
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::NumericAdd
            | ScalarFunc::NumericSub
            | ScalarFunc::NumericMul
            | ScalarFunc::NumericDiv => {
                check_arg_count!(arg_count, 2);
                let lhs_val = state.registers[*start_reg].get_value().clone();
                let rhs_val = state.registers[*start_reg + 1].get_value().clone();
                let result = match (&lhs_val, &rhs_val) {
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    _ => {
                        let a = value_to_bigdecimal(&lhs_val)?;
                        let b = value_to_bigdecimal(&rhs_val)?;
                        let res = match scalar_func {
                            ScalarFunc::NumericAdd => a + b,
                            ScalarFunc::NumericSub => a - b,
                            ScalarFunc::NumericMul => a * b,
                            ScalarFunc::NumericDiv => {
                                use bigdecimal::Zero;
                                if b.is_zero() {
                                    return Err(LimboError::Constraint(
                                        "division by zero".to_string(),
                                    ));
                                }
                                a / b
                            }
                            _ => unreachable!(),
                        };
                        Value::build_text(crate::numeric::decimal::format_numeric(&res))
                    }
                };
                state.registers[*dest] = Register::Value(result);
            }
            ScalarFunc::NumericLt | ScalarFunc::NumericEq => {
                check_arg_count!(arg_count, 2);
                let lhs_val = state.registers[*start_reg].get_value().clone();
                let rhs_val = state.registers[*start_reg + 1].get_value().clone();
                let result = match (&lhs_val, &rhs_val) {
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    _ => {
                        let a = value_to_bigdecimal(&lhs_val)?;
                        let b = value_to_bigdecimal(&rhs_val)?;
                        let cmp_result = match scalar_func {
                            ScalarFunc::NumericLt => a < b,
                            ScalarFunc::NumericEq => a == b,
                            _ => unreachable!(),
                        };
                        Value::from_i64(cmp_result as i64)
                    }
                };
                state.registers[*dest] = Register::Value(result);
            }
        },
        crate::function::Func::Vector(vector_func) => {
            let args = &state.registers[*start_reg..*start_reg + arg_count];
            match vector_func {
                VectorFunc::Vector => {
                    let result = vector32(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::Vector32 => {
                    let result = vector32(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::Vector32Sparse => {
                    let result = vector32_sparse(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::Vector64 => {
                    let result = vector64(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::Vector8 => {
                    let result = vector8(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::Vector1Bit => {
                    let result = vector1bit(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::VectorExtract => {
                    let result = vector_extract(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::VectorDistanceCos => {
                    let result = vector_distance_cos(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::VectorDistanceDot => {
                    let result = vector_distance_dot(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::VectorDistanceL2 => {
                    let result = vector_distance_l2(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::VectorDistanceJaccard => {
                    let result = vector_distance_jaccard(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::VectorConcat => {
                    let result = vector_concat(args)?;
                    state.registers[*dest] = Register::Value(result);
                }
                VectorFunc::VectorSlice => {
                    let result = vector_slice(args)?;
                    state.registers[*dest] = Register::Value(result)
                }
            }
        }
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
                    state.registers[*dest] = Register::Value(Value::from_f64(std::f64::consts::PI));
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

            let Value::Numeric(Numeric::Integer(root_page)) =
                &state.registers[*start_reg + 3].get_value().clone()
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
                        let ast::Cmd::Stmt(stmt) =
                            parser.next().expect("parser should have next item")?
                        else {
                            return Err(LimboError::InternalError(
                                "Unexpected command during ALTER TABLE RENAME processing"
                                    .to_string(),
                            ));
                        };

                        match stmt {
                            ast::Stmt::CreateIndex {
                                tbl_name,
                                unique,
                                if_not_exists,
                                idx_name,
                                columns,
                                where_clause,
                                using,
                                with_clause,
                            } => {
                                let table_name = normalize_ident(tbl_name.as_str());

                                if rename_from != table_name {
                                    break 'sql None;
                                }

                                Some(
                                    ast::Stmt::CreateIndex {
                                        tbl_name: ast::Name::exact(original_rename_to.to_string()),
                                        unique,
                                        if_not_exists,
                                        idx_name,
                                        columns,
                                        where_clause,
                                        using,
                                        with_clause,
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
                                let this_table = normalize_ident(tbl_name.name.as_str());

                                let ast::CreateTableBody::ColumnsAndConstraints {
                                    mut columns,
                                    mut constraints,
                                    options,
                                } = body
                                else {
                                    return Err(LimboError::InternalError(
                                        "CREATE TABLE AS SELECT schemas cannot be altered"
                                            .to_string(),
                                    ));
                                };

                                let mut any_change = false;

                                // Rewrite FK targets in both paths
                                for c in &mut constraints {
                                    if let ast::TableConstraint::ForeignKey { clause, .. } =
                                        &mut c.constraint
                                    {
                                        any_change |= rewrite_fk_parent_table_if_needed(
                                            clause,
                                            &rename_from,
                                            original_rename_to.as_str(),
                                        );
                                    }
                                }
                                for col in &mut columns {
                                    any_change |= rewrite_inline_col_fk_target_if_needed(
                                        col,
                                        &rename_from,
                                        original_rename_to.as_str(),
                                    );
                                }

                                // Rewrite table-qualified refs in CHECK constraints
                                // (e.g. t1.a > 0 → t2.a > 0)
                                if this_table == rename_from {
                                    for c in &mut constraints {
                                        if let ast::TableConstraint::Check(ref mut expr) =
                                            c.constraint
                                        {
                                            rewrite_check_expr_table_refs(
                                                expr,
                                                &rename_from,
                                                &rename_to,
                                            );
                                        }
                                    }
                                    for col in &mut columns {
                                        for cc in &mut col.constraints {
                                            if let ast::ColumnConstraint::Check(ref mut expr) =
                                                cc.constraint
                                            {
                                                rewrite_check_expr_table_refs(
                                                    expr,
                                                    &rename_from,
                                                    &rename_to,
                                                );
                                            }
                                        }
                                    }
                                }

                                if this_table == rename_from {
                                    // Rebuild with new table identifier so SQL persists the new name.
                                    let new_stmt = ast::Stmt::CreateTable {
                                        tbl_name: ast::QualifiedName {
                                            db_name: None,
                                            name: ast::Name::exact(original_rename_to.to_string()),
                                            alias: None,
                                        },
                                        temporary,
                                        if_not_exists,
                                        body: ast::CreateTableBody::ColumnsAndConstraints {
                                            columns,
                                            constraints,
                                            options,
                                        },
                                    };
                                    Some(new_stmt.to_string())
                                } else {
                                    // Other tables: only emit if we actually changed their FK targets.
                                    if !any_change {
                                        break 'sql None;
                                    }
                                    Some(
                                        ast::Stmt::CreateTable {
                                            tbl_name,
                                            temporary,
                                            if_not_exists,
                                            body: ast::CreateTableBody::ColumnsAndConstraints {
                                                columns,
                                                constraints,
                                                options,
                                            },
                                        }
                                        .to_string(),
                                    )
                                }
                            }
                            ast::Stmt::CreateVirtualTable(ast::CreateVirtualTable {
                                tbl_name,
                                if_not_exists,
                                module_name,
                                args,
                            }) => {
                                let this_table = normalize_ident(tbl_name.name.as_str());
                                if this_table != rename_from {
                                    None
                                } else {
                                    let new_stmt =
                                        ast::Stmt::CreateVirtualTable(ast::CreateVirtualTable {
                                            tbl_name: ast::QualifiedName {
                                                db_name: tbl_name.db_name,
                                                name: ast::Name::exact(
                                                    original_rename_to.to_string(),
                                                ),
                                                alias: None,
                                            },
                                            if_not_exists,
                                            module_name,
                                            args,
                                        });
                                    Some(new_stmt.to_string())
                                }
                            }
                            _ => None,
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

                    let column_def =
                        Parser::new(column_def.as_bytes()).parse_column_definition(true)?;

                    let _rename_to = normalize_ident(column_def.col_name.as_str());

                    let new_sql = 'sql: {
                        let Value::Text(sql) = sql else {
                            break 'sql None;
                        };

                        let mut parser = Parser::new(sql.as_str().as_bytes());
                        let ast::Cmd::Stmt(stmt) =
                            parser.next().expect("parser should have next item")?
                        else {
                            return Err(LimboError::InternalError(
                                "Unexpected command during ALTER TABLE RENAME COLUMN processing"
                                    .to_string(),
                            ));
                        };

                        match stmt {
                            ast::Stmt::CreateIndex {
                                tbl_name,
                                mut columns,
                                unique,
                                if_not_exists,
                                idx_name,
                                mut where_clause,
                                using,
                                with_clause,
                            } => {
                                if table != normalize_ident(tbl_name.as_str()) {
                                    break 'sql None;
                                }

                                for column in &mut columns {
                                    match column.expr.as_mut() {
                                        ast::Expr::Id(id)
                                            if normalize_ident(id.as_str()) == rename_from =>
                                        {
                                            *id = Name::exact(
                                                column_def.col_name.as_str().to_owned(),
                                            );
                                        }
                                        _ => {}
                                    }
                                }

                                if let Some(ref mut wc) = where_clause {
                                    rename_identifiers(
                                        wc,
                                        &rename_from,
                                        column_def.col_name.as_str(),
                                    );
                                }

                                Some(
                                    ast::Stmt::CreateIndex {
                                        tbl_name,
                                        columns,
                                        unique,
                                        if_not_exists,
                                        idx_name,
                                        where_clause,
                                        using,
                                        with_clause,
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
                                let ast::CreateTableBody::ColumnsAndConstraints {
                                    mut columns,
                                    mut constraints,
                                    options,
                                } = body
                                else {
                                    return Err(LimboError::InternalError(
                                        "CREATE TABLE AS SELECT schemas cannot be altered"
                                            .to_string(),
                                    ));
                                };

                                let normalized_tbl_name = normalize_ident(tbl_name.name.as_str());

                                if normalized_tbl_name == table {
                                    // This is the table being altered - update its column
                                    let column = columns
                                        .iter_mut()
                                        .find(|column| {
                                            normalize_ident(column.col_name.as_str()) == rename_from
                                        })
                                        .expect("column being renamed should be present");

                                    match alter_func {
                                        AlterTableFunc::AlterColumn => *column = column_def.clone(),
                                        AlterTableFunc::RenameColumn => {
                                            column.col_name = column_def.col_name.clone()
                                        }
                                        _ => unreachable!(),
                                    }

                                    // Update table-level constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY)
                                    for constraint in &mut constraints {
                                        match &mut constraint.constraint {
                                            ast::TableConstraint::PrimaryKey {
                                                columns: pk_cols,
                                                ..
                                            } => {
                                                for col in pk_cols {
                                                    let (ast::Expr::Name(ref name)
                                                    | ast::Expr::Id(ref name)) = *col.expr
                                                    else {
                                                        return Err(LimboError::ParseError("Unexpected expression in PRIMARY KEY constraint".to_string()));
                                                    };
                                                    if normalize_ident(name.as_str()) == rename_from
                                                    {
                                                        *col.expr = ast::Expr::Name(Name::exact(
                                                            column_def.col_name.as_str().to_owned(),
                                                        ));
                                                    }
                                                }
                                            }
                                            ast::TableConstraint::Unique {
                                                columns: uniq_cols,
                                                ..
                                            } => {
                                                for col in uniq_cols {
                                                    let (ast::Expr::Name(ref name)
                                                    | ast::Expr::Id(ref name)) = *col.expr
                                                    else {
                                                        return Err(LimboError::ParseError("Unexpected expression in UNIQUE constraint".to_string()));
                                                    };
                                                    if normalize_ident(name.as_str()) == rename_from
                                                    {
                                                        *col.expr = ast::Expr::Name(Name::exact(
                                                            column_def.col_name.as_str().to_owned(),
                                                        ));
                                                    }
                                                }
                                            }
                                            ast::TableConstraint::ForeignKey {
                                                columns: child_cols,
                                                clause,
                                                ..
                                            } => {
                                                // Update child columns in this table's FK definitions
                                                for child_col in child_cols {
                                                    if normalize_ident(child_col.col_name.as_str())
                                                        == rename_from
                                                    {
                                                        child_col.col_name = Name::exact(
                                                            column_def.col_name.as_str().to_owned(),
                                                        );
                                                    }
                                                }
                                                rewrite_fk_parent_cols_if_self_ref(
                                                    clause,
                                                    &normalized_tbl_name,
                                                    &rename_from,
                                                    column_def.col_name.as_str(),
                                                );
                                            }
                                            ast::TableConstraint::Check(ref mut expr) => {
                                                rename_identifiers(
                                                    expr,
                                                    &rename_from,
                                                    column_def.col_name.as_str(),
                                                );
                                            }
                                        }
                                    }

                                    for col in &mut columns {
                                        rewrite_column_references_if_needed(
                                            col,
                                            &normalized_tbl_name,
                                            &rename_from,
                                            column_def.col_name.as_str(),
                                        );
                                    }
                                } else {
                                    // This is a different table, check if it has FKs referencing the renamed column
                                    let mut fk_updated = false;

                                    for constraint in &mut constraints {
                                        if let ast::TableConstraint::ForeignKey {
                                            columns: _,
                                            clause:
                                                ForeignKeyClause {
                                                    tbl_name,
                                                    columns: parent_cols,
                                                    ..
                                                },
                                            ..
                                        } = &mut constraint.constraint
                                        {
                                            // Check if this FK references the table being altered
                                            if normalize_ident(tbl_name.as_str()) == table {
                                                // Update parent column references if they match the renamed column
                                                for parent_col in parent_cols {
                                                    if normalize_ident(parent_col.col_name.as_str())
                                                        == rename_from
                                                    {
                                                        parent_col.col_name = Name::exact(
                                                            column_def.col_name.as_str().to_owned(),
                                                        );
                                                        fk_updated = true;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    for col in &mut columns {
                                        let _before = fk_updated;
                                        let mut local_col = col.clone();
                                        rewrite_column_references_if_needed(
                                            &mut local_col,
                                            &table,
                                            &rename_from,
                                            column_def.col_name.as_str(),
                                        );
                                        if local_col != *col {
                                            *col = local_col;
                                            fk_updated = true;
                                        }
                                    }

                                    // Only return updated SQL if we actually changed something
                                    if !fk_updated {
                                        break 'sql None;
                                    }
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
                            _ => None,
                        }
                    };

                    (name, tbl_name, new_sql)
                }
            };

            state.registers[*dest] = Register::Value(r#type.clone());
            state.registers[*dest + 1] = Register::Value(Value::Text(Text::from(new_name)));
            state.registers[*dest + 2] = Register::Value(Value::Text(Text::from(new_tbl_name)));
            state.registers[*dest + 3] = Register::Value(Value::from_i64(*root_page));

            if let Some(new_sql) = new_sql {
                state.registers[*dest + 4] = Register::Value(Value::Text(Text::from(new_sql)));
            } else {
                state.registers[*dest + 4] = Register::Value(sql.clone());
            }
        }
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        crate::function::Func::Fts(fts_func) => {
            // FTS functions are typically handled via index method pattern matching.
            // If we reach here, just return a fallback since no FTS index matched.
            use crate::function::FtsFunc;
            match fts_func {
                FtsFunc::Score => {
                    // Without an FTS index match, return 0.0 as a default score
                    state.registers[*dest] = Register::Value(Value::from_f64(0.0));
                }
                FtsFunc::Match => {
                    // fts_match(col1, col2, ..., query): returns 1 if any column matches query
                    // Minimum: fts_match(text, query) = 2 args
                    if arg_count < 2 {
                        return Err(LimboError::InvalidArgument(
                            "fts_match requires at least 2 arguments: text, query".to_string(),
                        ));
                    }

                    // Last arg is the query, first N-1 args are text columns
                    let num_text_cols = arg_count - 1;
                    let query = state.registers[*start_reg + num_text_cols].get_value();

                    if matches!(query, Value::Null) {
                        state.registers[*dest] = Register::Value(Value::from_i64(0));
                    } else {
                        let query_str = query.to_string();

                        // Concatenate all text columns with space separator
                        let est_len = 16;
                        let mut combined_text = String::with_capacity(num_text_cols * est_len);
                        for i in 0..num_text_cols {
                            let text = state.registers[*start_reg + i].get_value();
                            if !matches!(text, Value::Null) {
                                if !combined_text.is_empty() {
                                    combined_text.push(' ');
                                }
                                combined_text.push_str(&text.to_string());
                            }
                        }

                        let matches =
                            crate::index_method::fts::fts_match(&combined_text, &query_str);
                        state.registers[*dest] = Register::Value(Value::from_i64(matches.into()));
                    }
                }
                FtsFunc::Highlight => {
                    // fts_highlight(col1, col2, ..., before_tag, after_tag, query)
                    // Variable number of text columns, followed by before_tag, after_tag, query
                    // Minimum: fts_highlight(text, before_tag, after_tag, query) = 4 args
                    if arg_count < 4 {
                        return Err(LimboError::InvalidArgument(
                            "fts_highlight requires at least 4 arguments: text, before_tag, after_tag, query"
                                .to_string(),
                        ));
                    }

                    // Last 3 args are: before_tag, after_tag, query
                    // First N-3 args are text columns
                    let num_text_cols = arg_count - 3;
                    let before_tag = state.registers[*start_reg + num_text_cols].get_value();
                    let after_tag = state.registers[*start_reg + num_text_cols + 1].get_value();
                    let query = state.registers[*start_reg + num_text_cols + 2].get_value();

                    // Handle NULL values in tags or query
                    if matches!(query, Value::Null)
                        || matches!(before_tag, Value::Null)
                        || matches!(after_tag, Value::Null)
                    {
                        state.registers[*dest] = Register::Value(Value::Null);
                    } else {
                        let query_str = query.to_string();
                        let before_str = before_tag.to_string();
                        let after_str = after_tag.to_string();

                        // Concatenate all text columns with space separator
                        let mut combined_text = String::new();
                        for i in 0..num_text_cols {
                            let text = state.registers[*start_reg + i].get_value();
                            if !matches!(text, Value::Null) {
                                if !combined_text.is_empty() {
                                    combined_text.push(' ');
                                }
                                combined_text.push_str(&text.to_string());
                            }
                        }

                        let highlighted = crate::index_method::fts::fts_highlight(
                            &combined_text,
                            &query_str,
                            &before_str,
                            &after_str,
                        );
                        state.registers[*dest] = Register::Value(Value::build_text(highlighted));
                    }
                }
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Sequence {
            cursor_id,
            target_reg
        },
        insn
    );
    let cursor_seq = state
        .cursor_seqs
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid");
    let seq_num = *cursor_seq;
    *cursor_seq += 1;
    state.registers[*target_reg] = Register::Value(Value::from_i64(seq_num));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sequence_test(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SequenceTest {
            cursor_id,
            target_pc,
            value_reg: _
        },
        insn
    );
    let cursor_seq = state
        .cursor_seqs
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid");
    let was_zero = *cursor_seq == 0;
    *cursor_seq += 1;
    state.pc = if was_zero {
        target_pc.as_offset_int()
    } else {
        state.pc + 1
    };
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_init_coroutine(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    state.registers[*yield_reg] = Register::Value(Value::from_i64(start_offset as i64));
    state.ended_coroutine.retain(|n| *n != *yield_reg as u32);
    let jump_on_definition = jump_on_definition.as_offset_int();
    state.pc = if jump_on_definition == 0 {
        state.pc + 1
    } else {
        jump_on_definition
    };
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_end_coroutine(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(EndCoroutine { yield_reg }, insn);

    if let Value::Numeric(Numeric::Integer(pc)) = state.registers[*yield_reg].get_value() {
        state.ended_coroutine.push(*yield_reg as u32);
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Yield {
            yield_reg,
            end_offset,
        },
        insn
    );
    if let Value::Numeric(Numeric::Integer(pc)) = state.registers[*yield_reg].get_value() {
        if state.ended_coroutine.contains(&(*yield_reg as u32)) {
            state.pc = end_offset.as_offset_int();
        } else {
            let pc: u32 = (*pc)
                .try_into()
                .unwrap_or_else(|_| panic!("Yield: pc overflow: {pc}"));
            // swap the program counter with the value in the yield register
            // this is the mechanism that allows jumping back and forth between the coroutine and the caller
            (state.pc, state.registers[*yield_reg]) =
                (pc, Register::Value(Value::from_i64((state.pc + 1) as i64)));
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
    /// If cursor is already positioned (no REQUIRE_SEEK), capture directly.
    /// If REQUIRE_SEEK is set, transition to Seek first.
    MaybeCaptureRecord,
    /// Seek to the correct position if needed.
    /// In a table insert, if the caller does not pass InsertFlags::REQUIRE_SEEK, they must ensure that a seek has already happened to the correct location.
    /// This typically happens by invoking either Insn::NewRowid or Insn::NotExists, because:
    /// 1. op_new_rowid() seeks to the end of the table, which is the correct insertion position.
    /// 2. op_not_exists() seeks to the position in the table where the target rowid would be inserted.
    Seek,
    /// Capture the old record at the current cursor position for IVM.
    /// The cursor must already be positioned (by a prior seek or by NotExists/NewRowid).
    CaptureRecord,
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
                let has_dependent_views = {
                    let schema = program.connection.schema.read();
                    !schema
                        .get_dependent_materialized_views(table_name)
                        .is_empty()
                };
                // If there are no dependent views, we don't need to capture the old record.
                // We also don't need to do it if the rowid of the UPDATEd row was changed, because
                // op_delete already captured the deletion for IVM, and this insert only needs to
                // record the new row (which ApplyViewChange handles without old_record).
                let needs_capture =
                    has_dependent_views && !flag.has(InsertFlags::UPDATE_ROWID_CHANGE);

                if flag.has(InsertFlags::REQUIRE_SEEK) {
                    state.op_insert_state.sub_state = OpInsertSubState::Seek;
                } else if needs_capture {
                    state.op_insert_state.sub_state = OpInsertSubState::CaptureRecord;
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
                let has_dependent_views = {
                    let schema = program.connection.schema.read();
                    !schema
                        .get_dependent_materialized_views(table_name)
                        .is_empty()
                };
                let needs_capture =
                    has_dependent_views && !flag.has(InsertFlags::UPDATE_ROWID_CHANGE);
                if needs_capture {
                    state.op_insert_state.sub_state = OpInsertSubState::CaptureRecord;
                } else {
                    state.op_insert_state.sub_state = OpInsertSubState::Insert;
                }
                continue;
            }
            OpInsertSubState::CaptureRecord => {
                let insert_key = match &state.registers[*key_reg].get_value() {
                    Value::Numeric(Numeric::Integer(i)) => *i,
                    _ => unreachable!("expected integer key in insert"),
                };

                let cursor = state.get_cursor(*cursor_id);
                let cursor = cursor.as_btree_mut();
                let maybe_key = return_if_io!(cursor.rowid());
                let old_record = if let Some(key) = maybe_key {
                    if key == insert_key {
                        let maybe_record = return_if_io!(cursor.record());
                        if let Some(record) = maybe_record {
                            let mut values = record.get_values_owned()?;
                            let schema = program.connection.schema.read();
                            if let Some(table) = schema.get_table(table_name) {
                                for (i, col) in table.columns().iter().enumerate() {
                                    if col.is_rowid_alias() && i < values.len() {
                                        values[i] = Value::from_i64(key);
                                    }
                                }
                            }
                            Some((key, values))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                state.op_insert_state.old_record = old_record;
                state.op_insert_state.sub_state = OpInsertSubState::Insert;
                continue;
            }
            OpInsertSubState::Insert => {
                let key = match &state.registers[*key_reg].get_value() {
                    Value::Numeric(Numeric::Integer(i)) => *i,
                    _ => unreachable!("expected integer key"),
                };
                let record = match &state.registers[*record_reg] {
                    Register::Record(r) => std::borrow::Cow::Borrowed(r),
                    Register::Value(value) => {
                        let values = [value];
                        let record = ImmutableRecord::from_values(values, values.len());
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
                if root_page != 1
                    && table_name != SQLITE_SEQUENCE_TABLE_NAME
                    && !flag.has(InsertFlags::EPHEMERAL_TABLE_INSERT)
                {
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
                    if !flag.has(InsertFlags::SKIP_LAST_ROWID) {
                        program.connection.update_last_rowid(rowid);
                    }
                    state
                        .n_change
                        .fetch_add(1, crate::sync::atomic::Ordering::SeqCst);
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
                    let key = match &state.registers[*key_reg].get_value() {
                        Value::Numeric(Numeric::Integer(i)) => *i,
                        _ => unreachable!("expected integer key"),
                    };

                    let record = match &state.registers[*record_reg] {
                        Register::Record(r) => std::borrow::Cow::Borrowed(r),
                        Register::Value(value) => {
                            let values = [value];
                            let record = ImmutableRecord::from_values(values, values.len());
                            std::borrow::Cow::Owned(record)
                        }
                        Register::Aggregate(..) => {
                            unreachable!("Cannot insert an aggregate value.")
                        }
                    };

                    // Add insertion of new row to view deltas
                    let mut new_values = record.get_values_owned()?;

                    // Fix rowid alias columns: replace Null with actual rowid value
                    let schema = program.connection.schema.read();
                    if let Some(table) = schema.get_table(table_name) {
                        for (i, col) in table.columns().iter().enumerate() {
                            if col.is_rowid_alias() && i < new_values.len() {
                                new_values[i] = Value::from_i64(key);
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    state.registers[*out_reg] = Register::Value(Value::from_i64(*value));
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
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Delete {
            cursor_id,
            table_name,
            is_part_of_update,
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
                        let mut values = record.get_values_owned()?;

                        // Fix rowid alias columns: replace Null with actual rowid value
                        if let Some(table) = schema.get_table(table_name) {
                            for (i, col) in table.columns().iter().enumerate() {
                                if col.is_rowid_alias() && i < values.len() {
                                    values[i] = Value::from_i64(key);
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
    if !is_part_of_update {
        // DELETEs do not count towards the total changes if they are part of an UPDATE statement,
        // i.e. the DELETE and subsequent INSERT of a row are the same "change".
        state
            .n_change
            .fetch_add(1, crate::sync::atomic::Ordering::SeqCst);
    }
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

    if let Some(Cursor::IndexMethod(cursor)) = &mut state.cursors[*cursor_id] {
        return_if_io!(cursor.delete(&state.registers[*start_reg..*start_reg + *num_regs]));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

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
                            "IdxDelete: no matching index entry found for key {reg_values:?} while seeking"
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
                        "IdxDelete: no matching index entry found for key while verifying: {reg_values:?}"
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
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IdxInsert {
            cursor_id,
            record_reg,
            flags,
            unpacked_start,
            unpacked_count,
            ..
        },
        *insn
    );

    if let Some(Cursor::IndexMethod(cursor)) = &mut state.cursors[cursor_id] {
        let Some(start) = unpacked_start else {
            return Err(LimboError::InternalError(
                "IndexMethod must receive unpacked values".to_string(),
            ));
        };
        let Some(count) = unpacked_count else {
            return Err(LimboError::InternalError(
                "IndexMethod must receive unpacked values".to_string(),
            ));
        };
        return_if_io!(cursor.insert(&state.registers[start..start + count as usize]));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

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
            let (_, cursor_type) = program
                .cursor_ref
                .get(cursor_id)
                .expect("cursor_id should exist in cursor_ref");
            let CursorType::BTreeIndex(index_meta) = cursor_type else {
                panic!("IdxInsert: not a BTreeIndex cursor");
            };

            // USE_SEEK: cursor was already positioned by a preceding NoConflict operation.
            // Skip the redundant seek and go directly to insert.
            // For unique indexes, this also skips UniqueConstraintCheck since NoConflict already verified uniqueness.
            //
            // HOWEVER: If the record contains NULLs, NoConflict skips the seek entirely
            // (since NULLs can't conflict), so we must fall back to seeking here.
            if flags.has(IdxInsertFlags::USE_SEEK) && !record_to_insert.contains_null()? {
                state.op_idx_insert_state = OpIdxInsertState::Insert;
                return Ok(InsnFunctionStepResult::Step);
                // Fall through to do the seek since NoConflict skipped it due to NULLs
            }

            match seek_internal(
                program,
                state,
                pager,
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
                let has_rowid = cursor.has_rowid();
                let index_info = cursor.get_index_info().clone();
                let record_opt = return_if_io!(cursor.record());
                let Some(record) = record_opt.as_ref() else {
                    // Cursor not pointing at a record — table is empty or past last
                    break 'i false;
                };
                // Cursor is pointing at a record; if the index has a rowid, exclude it from the comparison since it's a pointer to the table row;
                // UNIQUE indexes disallow duplicates like (a=1,b=2,rowid=1) and (a=1,b=2,rowid=2).
                let existing_key = if has_rowid {
                    let count = record.column_count();
                    &record.get_values_range(0..count.saturating_sub(1))?
                } else {
                    &record.get_values()?[..]
                };
                let inserted_key_vals = &record_to_insert.get_values()?;
                if existing_key.len() != inserted_key_vals.len() {
                    break 'i false;
                }

                let conflict =
                    compare_immutable(existing_key, inserted_key_vals, &index_info.key_info)
                        == std::cmp::Ordering::Equal;
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
    SeekingToLast {
        mvcc_already_initialized: bool,
    },
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
) -> Result<InsnFunctionStepResult> {
    new_rowid_inner(program, state, insn, pager).inspect_err(|_| {
        // In case of error we need to unlock rowid lock from mvcc cursor
        load_insn!(
            NewRowid {
                cursor,
                rowid_reg: _,
                prev_largest_reg: _,
            },
            insn
        );
        let mv_store = program.connection.mv_store();
        if mv_store.is_some() {
            let cursor = state.get_cursor(*cursor);
            let cursor = cursor.as_btree_mut() as &mut dyn Any;
            if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                mvcc_cursor.end_new_rowid();
            }
        }
    })
}

fn new_rowid_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        NewRowid {
            cursor,
            rowid_reg,
            prev_largest_reg,
        },
        insn
    );

    const MAX_ROWID: i64 = i64::MAX;
    const MAX_ATTEMPTS: u32 = 100;
    let mv_store = program.connection.mv_store();
    loop {
        match state.op_new_rowid_state {
            OpNewRowidState::Start => {
                if mv_store.is_some() {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut() as &mut dyn Any;
                    if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                        match return_if_io!(mvcc_cursor.start_new_rowid()) {
                            NextRowidResult::Uninitialized => {
                                state.op_new_rowid_state = OpNewRowidState::SeekingToLast {
                                    mvcc_already_initialized: false,
                                };
                            }
                            NextRowidResult::Next {
                                new_rowid,
                                prev_rowid,
                            } => {
                                // Allocator already initialized — release lock immediately
                                mvcc_cursor.end_new_rowid();
                                state.registers[*rowid_reg] =
                                    Register::Value(Value::from_i64(new_rowid));
                                if *prev_largest_reg > 0 {
                                    state.registers[*prev_largest_reg] =
                                        Register::Value(Value::from_i64(prev_rowid.unwrap_or(0)));
                                }
                                state.op_new_rowid_state = OpNewRowidState::SeekingToLast {
                                    mvcc_already_initialized: true,
                                };
                            }
                            NextRowidResult::FindRandom => {
                                mvcc_cursor.end_new_rowid();
                                state.op_new_rowid_state =
                                    OpNewRowidState::GeneratingRandom { attempts: 0 };
                            }
                        }
                    } else {
                        // Not an MvCursor — must be an ephemeral cursor or an attached
                        // DB cursor without MVCC (e.g., :memory: attached DBs skip MVCC).
                        // Keep the downcast check as a safety net against unexpected cursor types.
                        assert!(
                            cursor.downcast_ref::<BTreeCursor>().is_some(),
                            "Expected MvCursor or BTreeCursor in op_new_rowid"
                        );
                        state.op_new_rowid_state = OpNewRowidState::SeekingToLast {
                            mvcc_already_initialized: false,
                        };
                    }
                } else {
                    state.op_new_rowid_state = OpNewRowidState::SeekingToLast {
                        mvcc_already_initialized: false,
                    };
                }
            }

            OpNewRowidState::SeekingToLast {
                mvcc_already_initialized,
            } => {
                {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    // We have an optimization in the btree cursor to not seek if we know the rightmost page and are already on it.
                    // However, this optimization should NOT never performed in cases where we cannot be sure that the btree wasn't modified from under us
                    // e.g. by a trigger subprogram.
                    let always_seek = program.contains_trigger_subprograms;
                    return_if_io!(cursor.seek_to_last(always_seek));
                }
                if mvcc_already_initialized {
                    state.op_new_rowid_state = OpNewRowidState::GoNext;
                } else {
                    state.op_new_rowid_state = OpNewRowidState::ReadingMaxRowid;
                }
            }

            OpNewRowidState::ReadingMaxRowid => {
                let current_max = {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.rowid())
                };

                if mv_store.is_some() {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut() as &mut dyn Any;
                    if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                        // Initialize the monotonic counter from the btree max.
                        // The allocator lock is held, so no other thread can
                        // race between this read and initialize.
                        mvcc_cursor.initialize_max_rowid(current_max)?;
                        // Allocate the first rowid from the freshly initialized counter.
                        match mvcc_cursor.allocate_next_rowid() {
                            Some((new_rowid, prev_rowid)) => {
                                state.registers[*rowid_reg] =
                                    Register::Value(Value::from_i64(new_rowid));
                                if *prev_largest_reg > 0 {
                                    state.registers[*prev_largest_reg] =
                                        Register::Value(Value::from_i64(prev_rowid.unwrap_or(0)));
                                }
                                tracing::trace!("new_rowid={}", new_rowid);
                                state.op_new_rowid_state = OpNewRowidState::GoNext;
                                continue;
                            }
                            None => {
                                // At i64::MAX — fall back to random
                                state.op_new_rowid_state =
                                    OpNewRowidState::GeneratingRandom { attempts: 0 };
                                continue;
                            }
                        }
                    }
                }

                // Non-MVCC path (or ephemeral cursor in MVCC mode)
                if *prev_largest_reg > 0 {
                    state.registers[*prev_largest_reg] =
                        Register::Value(Value::from_i64(current_max.unwrap_or(0)));
                }
                match current_max {
                    Some(rowid) if rowid < MAX_ROWID => {
                        state.registers[*rowid_reg] = Register::Value(Value::from_i64(rowid + 1));
                        tracing::trace!("new_rowid={}", rowid + 1);
                        state.op_new_rowid_state = OpNewRowidState::GoNext;
                        continue;
                    }
                    Some(_) => {
                        state.op_new_rowid_state =
                            OpNewRowidState::GeneratingRandom { attempts: 0 };
                    }
                    None => {
                        tracing::trace!("new_rowid=1");
                        state.registers[*rowid_reg] = Register::Value(Value::from_i64(1));
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
                let mut random_rowid: i64 = pager.io.generate_random_number();
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
                    state.registers[*rowid_reg] = Register::Value(Value::from_i64(candidate));
                    state.op_new_rowid_state = OpNewRowidState::Start;
                    state.pc += 1;

                    if mv_store.is_some() {
                        let cursor = state.get_cursor(*cursor);
                        let cursor = cursor.as_btree_mut() as &mut dyn Any;
                        if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                            mvcc_cursor.end_new_rowid();
                        }
                    }

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

                if mv_store.is_some() {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut() as &mut dyn Any;
                    if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                        mvcc_cursor.end_new_rowid();
                    }
                }

                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_must_be_int(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(MustBeInt { reg }, insn);
    match &state.registers[*reg].get_value() {
        Value::Numeric(Numeric::Integer(_)) => {}
        Value::Numeric(Numeric::Float(f)) => match cast_real_to_integer(f64::from(*f)) {
            Ok(i) => state.registers[*reg] = Register::Value(Value::from_i64(i)),
            Err(_) => bail_constraint_error!("datatype mismatch"),
        },
        Value::Text(text) => match checked_cast_text_to_numeric(text.as_str(), true) {
            Ok(Value::Numeric(Numeric::Integer(i))) => {
                state.registers[*reg] = Register::Value(Value::from_i64(i))
            }
            Ok(Value::Numeric(Numeric::Float(f))) => match cast_real_to_integer(f64::from(f)) {
                Ok(i) => state.registers[*reg] = Register::Value(Value::from_i64(i)),
                Err(_) => bail_constraint_error!("datatype mismatch"),
            },
            _ => bail_constraint_error!("datatype mismatch"),
        },
        _ => {
            bail_constraint_error!("datatype mismatch");
        }
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_soft_null(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
                        record.iter()?.any(|val| matches!(val, Ok(ValueRef::Null)))
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
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        NotExists {
            cursor,
            rowid_reg,
            target_pc,
        },
        insn
    );
    let cursor = must_be_btree_cursor!(*cursor, program.cursor_ref, state, "NotExists");
    let cursor = cursor.as_btree_mut();
    let exists = return_if_io!(cursor.exists(state.registers[*rowid_reg].get_value()));

    if exists {
        state.pc += 1;
    } else {
        state.pc = target_pc.as_offset_int();
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_offset_limit(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
        Value::Numeric(Numeric::Integer(val)) => val,
        _ => {
            return Err(LimboError::InternalError(
                "OffsetLimit: the value in limit_reg is not an integer".into(),
            ));
        }
    };
    let offset_val = match state.registers[*offset_reg].get_value() {
        Value::Numeric(Numeric::Integer(val)) if *val < 0 => 0,
        Value::Numeric(Numeric::Integer(val)) if *val >= 0 => *val,
        _ => {
            return Err(LimboError::InternalError(
                "OffsetLimit: the value in offset_reg is not an integer".into(),
            ));
        }
    };

    let offset_limit_sum = limit_val.overflowing_add(offset_val);
    if *limit_val <= 0 || offset_limit_sum.1 {
        state.registers[*combined_reg] = Register::Value(Value::from_i64(-1));
    } else {
        state.registers[*combined_reg] = Register::Value(Value::from_i64(offset_limit_sum.0));
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
    let mv_store = program.connection.mv_store_for_db(*db);

    if let (_, CursorType::IndexMethod(module)) = &program.cursor_ref[*cursor_id] {
        if state.cursors[*cursor_id].is_none() {
            let cursor = module.init()?;
            let cursor_ref = &mut state.cursors[*cursor_id];
            *cursor_ref = Some(Cursor::IndexMethod(cursor));
        }

        let cursor = state.cursors[*cursor_id]
            .as_mut()
            .expect("cursor should exist");
        let cursor = cursor.as_index_method_mut();
        return_if_io!(cursor.open_write(&program.connection));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let root_page = match root_page {
        RegisterOrLiteral::Literal(lit) => *lit,
        RegisterOrLiteral::Register(reg) => match &state.registers[*reg].get_value() {
            Value::Numeric(Numeric::Integer(val)) => *val,
            _ => {
                return Err(LimboError::InternalError(
                    "OpenWrite: the value in root_page is not an integer".into(),
                ));
            }
        },
    };

    const SQLITE_SCHEMA_ROOT_PAGE: i64 = 1;

    if root_page == SQLITE_SCHEMA_ROOT_PAGE {
        if let Some(mv_store) = mv_store.as_ref() {
            let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) else {
                return Err(LimboError::InternalError(
                    "Schema changes in MVCC mode require an exclusive MVCC transaction".to_string(),
                ));
            };
            if !mv_store.is_exclusive_tx(&tx_id) {
                return Err(LimboError::TxError(
                    "DDL statements require an exclusive transaction (use BEGIN instead of BEGIN CONCURRENT)".to_string(),
                ));
            }
        }
    }

    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    let cursors = &mut state.cursors;
    let maybe_index = match cursor_type {
        CursorType::BTreeIndex(index) => Some(index),
        _ => None,
    };

    // Check if we can reuse the existing cursor
    let can_reuse_cursor = if let Some(Some(Cursor::BTree(btree_cursor))) = cursors.get(*cursor_id)
    {
        // Reuse if the root_page matches (same table/index)
        btree_cursor.root_page() == root_page
    } else {
        false
    };

    if !can_reuse_cursor {
        let maybe_promote_to_mvcc_cursor = |btree_cursor: Box<dyn CursorTrait>,
                                            mv_cursor_type: MvccCursorType|
         -> Result<Box<dyn CursorTrait>> {
            if let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) {
                let mv_store = mv_store
                    .as_ref()
                    .expect("mv_store should be Some when MVCC transaction is active")
                    .clone();
                Ok(Box::new(MvCursor::new(
                    mv_store,
                    tx_id,
                    root_page,
                    mv_cursor_type,
                    btree_cursor,
                )?))
            } else {
                Ok(btree_cursor)
            }
        };
        if let Some(index) = maybe_index {
            let num_columns = index.columns.len();
            let btree_cursor = Box::new(BTreeCursor::new_index(
                pager,
                maybe_transform_root_page_to_positive(mv_store.as_ref(), root_page),
                index.as_ref(),
                num_columns,
            ));
            let index_info = Arc::new(IndexInfo::new_from_index(index));
            let cursor =
                maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Index(index_info))?;
            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        } else {
            let num_columns = match cursor_type {
                CursorType::BTreeTable(table_rc) => table_rc.columns.len(),
                CursorType::MaterializedView(table_rc, _) => table_rc.columns.len(),
                _ => unreachable!(
                    "Expected BTreeTable or MaterializedView. This should not have happened."
                ),
            };

            let btree_cursor = Box::new(BTreeCursor::new_table(
                pager,
                maybe_transform_root_page_to_positive(mv_store.as_ref(), root_page),
                num_columns,
            ));
            let cursor = maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Table)?;
            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_copy(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(CreateBtree { db, root, flags }, insn);

    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly);
    }
    let mv_store = program.connection.mv_store_for_db(*db);

    if let Some(mv_store) = mv_store.as_ref() {
        let root_page = mv_store.get_next_table_id();
        state.registers[*root] = Register::Value(Value::from_i64(root_page));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    let pager = program.get_pager_from_database_index(db);
    // FIXME: handle page cache is full
    let root_page = return_if_io!(pager.btree_create(flags));
    state.registers[*root] = Register::Value(Value::from_i64(root_page as i64));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_index_method_create(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(IndexMethodCreate { db, cursor_id }, insn);
    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly);
    }
    let mv_store = program.connection.mv_store_for_db(*db);
    if let Some(_mv_store) = mv_store.as_ref() {
        todo!("MVCC is not supported yet");
    }
    if let (_, CursorType::IndexMethod(module)) = &program.cursor_ref[*cursor_id] {
        if state.cursors[*cursor_id].is_none() {
            let cursor = module.init()?;
            let cursor_ref = &mut state.cursors[*cursor_id];
            *cursor_ref = Some(Cursor::IndexMethod(cursor));
        }
    }
    let cursor = state.cursors[*cursor_id]
        .as_mut()
        .expect("cursor should exist");
    let cursor = cursor.as_index_method_mut();
    return_if_io!(cursor.create(&program.connection));

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_index_method_destroy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(IndexMethodDestroy { db, cursor_id }, insn);
    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly);
    }
    let mv_store = program.connection.mv_store_for_db(*db);
    if let Some(_mv_store) = mv_store.as_ref() {
        todo!("MVCC is not supported yet");
    }
    if let Some((_, CursorType::IndexMethod(module))) = program.cursor_ref.get(*cursor_id) {
        if state.cursors[*cursor_id].is_none() {
            let cursor = module.init()?;
            let cursor_ref = &mut state.cursors[*cursor_id];
            *cursor_ref = Some(Cursor::IndexMethod(cursor));
        }
    }
    let cursor = state.cursors[*cursor_id]
        .as_mut()
        .expect("cursor should exist");
    let cursor = cursor.as_index_method_mut();
    return_if_io!(cursor.destroy(&program.connection));

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_index_method_optimize(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(IndexMethodOptimize { db, cursor_id }, insn);
    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly);
    }
    let mv_store = program.connection.mv_store_for_db(*db);
    if let Some(_mv_store) = mv_store.as_ref() {
        todo!("MVCC is not supported yet");
    }
    if let Some((_, CursorType::IndexMethod(module))) = program.cursor_ref.get(*cursor_id) {
        if state.cursors[*cursor_id].is_none() {
            let cursor = module.init()?;
            let cursor_ref = &mut state.cursors[*cursor_id];
            *cursor_ref = Some(Cursor::IndexMethod(cursor));
        }
    }
    let cursor = state.cursors[*cursor_id]
        .as_mut()
        .expect("cursor should exist");
    let cursor = cursor.as_index_method_mut();
    return_if_io!(cursor.optimize(&program.connection));

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_index_method_query(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IndexMethodQuery {
            db: _,
            cursor_id,
            start_reg,
            count_reg,
            pc_if_empty,
        },
        insn
    );
    let mv_store = program.connection.mv_store();
    if let Some(_mv_store) = mv_store.as_ref() {
        todo!("MVCC is not supported yet");
    }
    let cursor = state.cursors[*cursor_id]
        .as_mut()
        .expect("cursor should exist");
    let cursor = cursor.as_index_method_mut();
    let has_rows =
        return_if_io!(cursor.query_start(&state.registers[*start_reg..*start_reg + *count_reg]));
    if !has_rows {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub enum OpDestroyState {
    CreateCursor,
    DestroyBtree(Arc<RwLock<BTreeCursor>>),
}

pub fn op_destroy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Destroy {
            db,
            root,
            former_root_reg,
            is_temp,
        },
        insn
    );
    if *is_temp == 1 {
        todo!("temp databases not implemented yet.");
    }
    let mv_store = program.connection.mv_store_for_db(*db);
    if mv_store.is_some() {
        // MVCC only does pager operations in checkpoint
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let destroy_pager = if *db != crate::MAIN_DB_ID {
        program.get_pager_from_database_index(db)
    } else {
        pager.clone()
    };

    loop {
        match state.op_destroy_state {
            OpDestroyState::CreateCursor => {
                // Destroy doesn't do anything meaningful with the table/index distinction so we can just use a
                // table btree cursor for both.
                let cursor = BTreeCursor::new(destroy_pager.clone(), *root, 0);
                state.op_destroy_state =
                    OpDestroyState::DestroyBtree(Arc::new(RwLock::new(cursor)));
            }
            OpDestroyState::DestroyBtree(ref mut cursor) => {
                let maybe_former_root_page = return_if_io!(cursor.write().btree_destroy());
                state.registers[*former_root_reg] =
                    Register::Value(Value::from_i64(maybe_former_root_page.unwrap_or(0) as i64));
                state.op_destroy_state = OpDestroyState::CreateCursor;
                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_reset_sorter(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ResetSorter { cursor_id }, insn);

    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    let cursor = state.get_cursor(*cursor_id);

    match cursor_type {
        CursorType::BTreeTable(_) => {
            let cursor = cursor.as_btree_mut();
            return_if_io!(cursor.clear_btree());
            // FIXME: cuurently we don't have a good way to identify cursors that are
            // iterating in the same underlying BTree

            // After clearing the btree, invalidate cached navigation state on all
            // other cursors that share the same underlying btree (e.g. OpenDup cursors).
            // Without this, dup cursors may use stale cached rightmost-page info and
            // attempt to insert into freed pages, causing corruption.
            let cleared_pager = {
                let cursor = state.get_cursor(*cursor_id);
                cursor.as_btree_mut().get_pager()
            };
            for (i, other_cursor_opt) in state.cursors.iter_mut().enumerate() {
                if i == *cursor_id {
                    continue;
                }
                if let Some(Cursor::BTree(ref mut btree_cursor)) = other_cursor_opt {
                    if Arc::ptr_eq(&btree_cursor.get_pager(), &cleared_pager) {
                        btree_cursor.invalidate_btree_cache();
                    }
                }
            }
        }
        CursorType::Sorter => {
            return Err(LimboError::InternalError(
                "ResetSorter is not supported for sorter cursors".to_string(),
            ));
        }
        _ => {
            return Err(LimboError::InternalError(format!(
                "ResetSorter is not supported for {cursor_type:?}"
            )));
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_table(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(DropTable { db, table_name, .. }, insn);
    let conn = program.connection.clone();
    let is_mvcc = conn.mv_store_for_db(*db).is_some();
    {
        conn.with_database_schema_mut(*db, |schema| {
            // In MVCC mode, track dropped root pages so integrity_check knows about them.
            // The btree pages won't be freed until checkpoint, so integrity_check needs
            // to include them to avoid "page never used" false positives.
            if is_mvcc {
                let table = schema
                    .get_table(table_name)
                    .expect("DROP TABLE: table must exist in schema");
                if let Some(btree) = table.btree() {
                    // Only track positive root pages (checkpointed tables).
                    // Negative root pages are non-checkpointed and don't exist in btree file.
                    if btree.root_page > 0 {
                        schema.dropped_root_pages.insert(btree.root_page);
                    }
                }
                // Capture index root pages (table may not have indexes)
                if let Some(indexes) = schema.indexes.get(table_name) {
                    for index in indexes.iter() {
                        if index.root_page > 0 {
                            schema.dropped_root_pages.insert(index.root_page);
                        }
                    }
                }
            }
            schema.remove_indices_for_table(table_name);
            schema.remove_triggers_for_table(table_name);
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
) -> Result<InsnFunctionStepResult> {
    load_insn!(DropView { db, view_name }, insn);
    let conn = program.connection.clone();
    conn.with_database_schema_mut(*db, |schema| {
        schema.remove_view(view_name).ok();
    });
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_type(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(DropType { db, type_name }, insn);
    if *db > 0 {
        todo!("temp databases not implemented yet");
    }
    let conn = program.connection.clone();
    conn.with_schema_mut(|schema| {
        schema.remove_type(type_name);
        Ok::<(), crate::LimboError>(())
    })?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add_type(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(AddType { db, sql }, insn);
    if *db > 0 {
        todo!("temp databases not implemented yet");
    }
    let conn = program.connection.clone();
    conn.with_schema_mut(|schema| schema.add_type_from_sql(sql))?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_trigger(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(DropTrigger { db, trigger_name }, insn);

    let conn = program.connection.clone();
    conn.with_database_schema_mut(*db, |schema| {
        schema.remove_trigger(trigger_name).ok();
    });
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_close(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Close { cursor_id }, insn);
    let cursors = &mut state.cursors;
    cursors
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid")
        .take();
    if let Some(deferred_seek) = state.deferred_seeks.get_mut(*cursor_id) {
        deferred_seek.take();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_is_null(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
) -> Result<InsnFunctionStepResult> {
    let Insn::CollSeq { reg, collation } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    // Set the current collation sequence for use by subsequent functions
    state.current_collation = Some(*collation);

    // If P1 is not zero, initialize that register to 0
    if let Some(reg_idx) = reg {
        state.registers[*reg_idx] = Register::Value(Value::from_i64(0));
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_page_count(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(PageCount { db, dest }, insn);
    let pager = program.get_pager_from_database_index(db);
    let mv_store = program.connection.mv_store_for_db(*db);
    let count = match with_header(&pager, mv_store.as_ref(), program, *db, |header| {
        header.database_size.get()
    }) {
        Err(_) => 0.into(),
        Ok(IOResult::Done(v)) => v.into(),
        Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
    };
    state.registers[*dest] = Register::Value(Value::from_i64(count));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_parse_schema(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ParseSchema { db, where_clause }, insn);

    let conn = program.connection.clone();
    // set auto commit to false in order for parse schema to not commit changes as transaction state is stored in connection,
    // and we use the same connection for nested query.
    let previous_auto_commit = conn.auto_commit.load(Ordering::SeqCst);
    conn.auto_commit.store(false, Ordering::SeqCst);

    let enable_triggers = conn.experimental_triggers_enabled();
    // For attached databases, qualify the sqlite_schema table with the database name
    let schema_table = if crate::is_attached_db(*db) {
        let db_name = conn
            .get_database_name_by_index(*db)
            .unwrap_or_else(|| "main".to_string());
        format!("{db_name}.sqlite_schema")
    } else {
        "sqlite_schema".to_string()
    };
    let sql = if let Some(where_clause) = where_clause {
        format!("SELECT * FROM {schema_table} WHERE {where_clause}")
    } else {
        format!("SELECT * FROM {schema_table}")
    };
    let stmt = conn.prepare(sql)?;

    // Get a mutable schema clone *without* holding the schema lock during
    // nested statement execution.  The nested Statement may call reprepare()
    // which also acquires the schema / database_schemas write lock, so holding
    // it here would deadlock on the same thread (parking_lot RwLock is not
    // re-entrant).
    let mut schema_arc = if crate::is_attached_db(*db) {
        // Fetch the fallback schema from attached_databases BEFORE acquiring
        // database_schemas.write() to avoid a nested lock ordering dependency
        // (database_schemas.write -> attached_databases.read).
        let fallback_schema = {
            let attached_dbs = conn.attached_databases.read();
            attached_dbs
                .index_to_data
                .get(db)
                .map(|(db_inst, _pager)| db_inst.schema.lock().clone())
        };
        let Some(fallback_schema) = fallback_schema else {
            // The db index refers to a database that was detached after this
            // program was compiled. The schema cookie check should have caught
            // this, but defensively return an error instead of panicking.
            return Err(LimboError::InternalError(format!(
                "stale reference to detached database (index {db})"
            )));
        };
        let mut schemas = conn.database_schemas().write();
        schemas
            .entry(*db)
            .or_insert_with(|| fallback_schema)
            .clone() // cheap Arc clone; write lock released at end of block
    } else {
        conn.schema.read().clone()
    };

    let schema = Arc::make_mut(&mut schema_arc);
    // TODO: This function below is synchronous, make it async
    let existing_views = schema.incremental_views.clone();
    conn.start_nested();
    let maybe_nested_stmt_err = parse_schema_rows(
        stmt,
        schema,
        &conn.syms.read(),
        // NOTE: We always pass the main DB's mv_tx here because
        // Statement::set_mv_tx() writes to connection.mv_tx (main DB field).
        // Passing an attached DB's tx would corrupt the main DB's transaction
        // state.  The nested statement's opcodes use get_mv_tx_id_for_db(db)
        // to read the correct per-database tx, so the attached DB case works
        // correctly without setting mv_tx.
        program.connection.get_mv_tx(),
        existing_views,
        enable_triggers,
    );

    // Store the modified schema back
    if crate::is_attached_db(*db) {
        conn.database_schemas().write().insert(*db, schema_arc);
    } else {
        *conn.schema.write() = schema_arc;
    }
    conn.end_nested();
    conn.auto_commit
        .store(previous_auto_commit, Ordering::SeqCst);
    maybe_nested_stmt_err?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_init_cdc_version(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        InitCdcVersion {
            cdc_table_name,
            version,
            cdc_mode,
        },
        insn
    );

    let conn = program.connection.clone();

    // "off" — disable CDC (table and version entry are preserved)
    if CaptureDataChangesInfo::parse(cdc_mode, None)?.is_none() {
        state.pending_cdc_info = Some(None);
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    // If CDC is already enabled, re-parse with current version and exit early.
    // This makes the operation idempotent and avoids CDC capturing its own
    // table creation when the pragma is called multiple times.
    {
        let current = conn.get_capture_data_changes_info();
        if let Some(info) = current.as_ref() {
            let opts = CaptureDataChangesInfo::parse(cdc_mode, info.version)?;
            state.pending_cdc_info = Some(opts);
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
    }

    // Step 0: Check if the CDC table already exists but has no version row.
    // If so, it's a legacy v1 table that pre-dates version tracking.
    let cdc_table_exists = {
        conn.start_nested();
        let mut stmt = conn.prepare(format!(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='{cdc_table_name}'",
        ))?;
        stmt.program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
        let rows = stmt.run_collect_rows();
        conn.end_nested();
        !rows?.is_empty()
    };

    // Step 1: Create CDC table if needed
    {
        conn.start_nested();
        let create_sql = match version {
            CdcVersion::V1 => format!(
                "CREATE TABLE IF NOT EXISTS {cdc_table_name} (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)",
            ),
            CdcVersion::V2 => format!(
                "CREATE TABLE IF NOT EXISTS {cdc_table_name} (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_txn_id INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)",
            ),
        };
        let mut stmt = conn.prepare(create_sql)?;
        stmt.program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
        let res = stmt.run_ignore_rows();
        conn.end_nested();
        res?;
    }

    // Step 2: Create version table if needed
    {
        conn.start_nested();
        let mut stmt = conn.prepare(format!(
            "CREATE TABLE IF NOT EXISTS {TURSO_CDC_VERSION_TABLE_NAME} (table_name TEXT PRIMARY KEY, version TEXT NOT NULL)",
        ))?;
        stmt.program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
        let res = stmt.run_ignore_rows();
        conn.end_nested();
        res?;
    }

    // Step 3: Insert version row only if one doesn't already exist.
    // If the CDC table pre-existed without a version row, it's a legacy v1 table.
    let version_to_insert = if cdc_table_exists {
        CdcVersion::V1
    } else {
        *version
    };
    {
        conn.start_nested();
        let mut stmt = conn.prepare(format!(
            "INSERT OR IGNORE INTO {TURSO_CDC_VERSION_TABLE_NAME} (table_name, version) VALUES ('{cdc_table_name}', '{version_to_insert}')",
        ))?;
        stmt.program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
        let res = stmt.run_ignore_rows();
        conn.end_nested();
        res?;
    }

    // Step 4: Read back the actual version from the table (may differ from
    // `version` if the row already existed with an older version).
    let actual_version = {
        conn.start_nested();
        let mut stmt = conn.prepare(format!(
            "SELECT version FROM {TURSO_CDC_VERSION_TABLE_NAME} WHERE table_name = '{cdc_table_name}'",
        ))?;
        stmt.program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
        let rows = stmt.run_collect_rows();
        conn.end_nested();
        let rows = rows?;
        match rows.first().and_then(|r| r.first()) {
            Some(crate::Value::Text(text)) => text.to_string().parse::<CdcVersion>()?,
            _ => *version,
        }
    };

    // Defer enabling CDC until the program completes successfully (Halt).
    // This ensures that if the transaction rolls back, the connection's
    // CDC state remains unchanged.
    let opts = CaptureDataChangesInfo::parse(cdc_mode, Some(actual_version))?;
    state.pending_cdc_info = Some(opts);

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_populate_materialized_views(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
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
                    ));
                }
            };

            view_info.push((view_name.clone(), root_page, *cursor_id));
        }
    }

    // Now populate the views (after releasing the schema borrow)
    for (view_name, _root_page, cursor_id) in view_info {
        let schema = conn.schema.read();
        if let Some(view) = schema.get_materialized_view(&view_name) {
            let mut view = view.lock();
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
                    ));
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
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ReadCookie { db, dest, cookie }, insn);
    let pager = program.get_pager_from_database_index(db);
    let mv_store = program.connection.mv_store_for_db(*db);

    let cookie_value =
        match with_header(
            &pager,
            mv_store.as_ref(),
            program,
            *db,
            |header| match cookie {
                Cookie::ApplicationId => header.application_id.get().into(),
                Cookie::UserVersion => header.user_version.get().into(),
                Cookie::SchemaVersion => header.schema_cookie.get().into(),
                Cookie::LargestRootPageNumber => header.vacuum_mode_largest_root_page.get().into(),
                cookie => todo!("{cookie:?} is not yet implement for ReadCookie"),
            },
        ) {
            Err(_) => 0.into(),
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
        };

    state.registers[*dest] = Register::Value(Value::from_i64(cookie_value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_set_cookie(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        SetCookie {
            db,
            cookie,
            value,
            p5: _,
        },
        insn
    );
    let pager = program.get_pager_from_database_index(db);
    let mv_store = program.connection.mv_store_for_db(*db);
    if let Some(mv_store) = mv_store.as_ref() {
        let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) else {
            return Err(LimboError::InternalError(
                "Header updates in MVCC mode require an active transaction".to_string(),
            ));
        };
        if !mv_store.is_exclusive_tx(&tx_id) {
            // Header cookies are global metadata with no row-level conflict keys; require
            // SQLite-style single-writer semantics (same policy as DDL in MVCC).
            return Err(LimboError::TxError(
                "Header updates require an exclusive transaction (use BEGIN instead of BEGIN CONCURRENT)".to_string(),
            ));
        }
    }

    return_if_io!(with_header_mut(
        &pager,
        mv_store.as_ref(),
        program,
        *db,
        |header| {
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
                    // Only mark schema_did_change on connection for main database (db 0).
                    // Attached databases track their schema independently.
                    if *db == crate::MAIN_DB_ID {
                        match program.connection.get_tx_state() {
                            TransactionState::Write { .. } => {
                                program.connection.set_tx_state(TransactionState::Write { schema_did_change: true });
                            },
                            TransactionState::Read => unreachable!("invalid transaction state for SetCookie: TransactionState::Read, should be write"),
                            TransactionState::None => unreachable!("invalid transaction state for SetCookie: TransactionState::None, should be write"),
                            TransactionState::PendingUpgrade { .. } => unreachable!("invalid transaction state for SetCookie: TransactionState::PendingUpgrade, should be write"),
                        }
                    }
                    program.connection.with_database_schema_mut(*db, |schema| {
                        schema.schema_version = *value as u32
                    });
                    header.schema_cookie = (*value as u32).into();
                }
                cookie => todo!("{cookie:?} is not yet implement for SetCookie"),
            };
        }
    ));

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_shift_right(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(AddImm { register, value }, insn);

    let current = &state.registers[*register];
    let current_value = match current {
        Register::Value(val) => val,
        Register::Aggregate(_) => &Value::Null,
        Register::Record(_) => &Value::Null,
    };

    let int_val = match current_value {
        Value::Numeric(Numeric::Integer(i)) => i + value,
        Value::Numeric(Numeric::Float(f)) => (f64::from(*f) as i64) + value,
        Value::Text(s) => s.as_str().parse::<i64>().unwrap_or(0) + value,
        Value::Blob(_) => *value, // BLOB becomes the added value
        Value::Null => *value,    // NULL becomes the added value
    };

    state.registers[*register] = Register::Value(Value::from_i64(int_val));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_variable(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Variable { index, dest }, insn);
    state.registers[*dest] = Register::Value(state.get_parameter(*index));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_zero_or_null(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(ZeroOrNull { rg1, rg2, dest }, insn);
    if state.registers[*rg1].is_null() || state.registers[*rg2].is_null() {
        state.registers[*dest] = Register::Value(Value::Null)
    } else {
        state.registers[*dest] = Register::Value(Value::from_i64(0));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_not(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(Not { reg, dest }, insn);
    state.registers[*dest] = Register::Value(state.registers[*reg].get_value().exec_boolean_not());
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Implements IS TRUE, IS FALSE, IS NOT TRUE, IS NOT FALSE.
/// A value is "true" only if it's a non-zero number.
/// Text and blobs are parsed as numbers; if not parseable, treated as 0 (falsy).
/// NULL is handled specially with the null_value parameter.
pub fn op_is_true(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IsTrue {
            reg,
            dest,
            null_value,
            invert
        },
        insn
    );
    let value = state.registers[*reg].get_value();
    // Use Numeric::try_into_bool which handles the conversion of text/blob to numbers
    let final_result = match Numeric::from_value(value).map(|val| val.to_bool()) {
        // For NULL, store null_value directly (no inversion)
        None => {
            if *null_value {
                1
            } else {
                0
            }
        }
        // For non-NULL, optionally invert the boolean result
        Some(is_truthy) => {
            let result = if is_truthy { 1 } else { 0 };
            if *invert {
                1 - result
            } else {
                result
            }
        }
    };
    state.registers[*dest] = Register::Value(Value::from_i64(final_result));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_concat(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _program: &Program,
    state: &mut ProgramState,
    _insn: &Insn,
    _pager: &Arc<Pager>,
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
    // Fast path states for reusing existing ephemeral cursor
    ClearExisting,
    RewindExisting,
    // Slow path states for creating new ephemeral cursor
    StartingTxn {
        pager: Arc<Pager>,
    },
    CreateBtree {
        pager: Arc<Pager>,
    },
    // clippy complains this variant is too big when compared to the rest of the variants
    // so it says we need to box it here
    Rewind {
        cursor: Box<dyn CursorTrait>,
    },
}
pub fn op_open_ephemeral(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    let (cursor_id, is_table) = match insn {
        Insn::OpenEphemeral {
            cursor_id,
            is_table,
        } => (*cursor_id, *is_table),
        Insn::OpenAutoindex { cursor_id } => (*cursor_id, false),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    let mv_store = program.connection.mv_store();
    match &mut state.op_open_ephemeral_state {
        OpOpenEphemeralState::Start => {
            tracing::trace!("Start");
            // Fast path: if cursor already has an ephemeral btree, just clear it instead of
            // recreating the entire pager/file/btree. This is important for performance when
            // OpenEphemeral is called repeatedly during statement execution.
            if state.cursors[cursor_id].is_some() {
                state.op_open_ephemeral_state = OpOpenEphemeralState::ClearExisting;
                return Ok(InsnFunctionStepResult::Step);
            }
            // Ephemeral tables always use the main DB's page size (db index 0)
            // regardless of which database triggered the ephemeral allocation.
            let page_size = return_if_io!(with_header(
                pager,
                mv_store.as_ref(),
                program,
                0,
                |header| { header.page_size }
            ));
            let conn = program.connection.clone();
            let io = conn.pager.load().io.clone();
            let rand_num = io.generate_random_number();
            let db_file: Arc<dyn DatabaseStorage>;
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
                let temp_store = conn.get_temp_store();
                if matches!(temp_store, crate::TempStore::Memory) {
                    // When temp_store=memory, use in-memory storage for ephemeral tables
                    use crate::MemoryIO;
                    db_file_io = Arc::new(MemoryIO::new());
                    let file = db_file_io.open_file("temp-file", OpenFlags::Create, false)?;
                    db_file = Arc::new(DatabaseFile::new(file));
                } else {
                    let temp_dir = temp_dir();
                    let rand_path = std::path::Path::new(&temp_dir)
                        .join(format!("tursodb-ephemeral-{rand_num}"));
                    let Some(rand_path_str) = rand_path.to_str() else {
                        return Err(LimboError::InternalError(
                            "Failed to convert path to string".to_string(),
                        ));
                    };
                    let file = io.open_file(rand_path_str, OpenFlags::Create, false)?;
                    db_file = Arc::new(DatabaseFile::new(file));
                    db_file_io = io;
                }
            }

            let buffer_pool = program.connection.db.buffer_pool.clone();

            // Ephemeral databases always start empty, so create their own init_page_1
            let ephemeral_init_page_1 =
                Arc::new(arc_swap::ArcSwapOption::new(Some(default_page1(None))));

            let pager = Arc::new(Pager::new(
                db_file,
                None,
                db_file_io,
                PageCache::default(),
                buffer_pool,
                Arc::new(Mutex::new(())),
                ephemeral_init_page_1,
            )?);

            pager.set_page_size(page_size);

            state.op_open_ephemeral_state = OpOpenEphemeralState::StartingTxn { pager };
        }
        OpOpenEphemeralState::ClearExisting => {
            tracing::trace!("ClearExisting");
            let cursor = state.cursors[cursor_id]
                .as_mut()
                .expect("cursor should exist in ClearExisting state");
            let btree_cursor = cursor.as_btree_mut();
            btree_cursor.set_null_flag(false);
            return_if_io!(btree_cursor.clear_btree());
            // iterate over existing deferred seeks and clear them as well,
            // as any deferred seek on this cursor is now invalid.
            for deferred_seek in &mut state.deferred_seeks {
                if let Some(ds) = deferred_seek {
                    if ds.index_cursor_id == cursor_id || ds.table_cursor_id == cursor_id {
                        *deferred_seek = None;
                    }
                }
            }
            state.op_open_ephemeral_state = OpOpenEphemeralState::RewindExisting;
        }
        OpOpenEphemeralState::RewindExisting => {
            tracing::trace!("RewindExisting");
            let cursor = state.cursors[cursor_id]
                .as_mut()
                .expect("cursor should exist in RewindExisting state");
            let btree_cursor = cursor.as_btree_mut();
            return_if_io!(btree_cursor.rewind());
            state.pc += 1;
            state.op_open_ephemeral_state = OpOpenEphemeralState::Start;
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
            let root_page = return_if_io!(pager.btree_create(flag)) as i64;

            let (_, cursor_type) = program
                .cursor_ref
                .get(cursor_id)
                .expect("cursor_id should exist in cursor_ref");

            let num_columns = match cursor_type {
                CursorType::BTreeTable(table_rc) => table_rc.columns.len(),
                CursorType::BTreeIndex(index_arc) => index_arc.columns.len(),
                _ => unreachable!("This should not have happened"),
            };

            let cursor = if let CursorType::BTreeIndex(index) = cursor_type {
                BTreeCursor::new_index(pager.clone(), root_page, index, num_columns)
            } else {
                BTreeCursor::new_table(pager.clone(), root_page, num_columns)
            };
            state.op_open_ephemeral_state = OpOpenEphemeralState::Rewind {
                cursor: Box::new(cursor),
            };
        }
        OpOpenEphemeralState::Rewind { cursor } => {
            return_if_io!(cursor.rewind());

            let cursors = &mut state.cursors;

            let (_, cursor_type) = program
                .cursor_ref
                .get(cursor_id)
                .expect("cursor_id should exist in cursor_ref");

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
                        .expect("cursor_id should be valid")
                        .replace(Cursor::new_btree(cursor));
                }
                CursorType::BTreeIndex(_) => {
                    cursors
                        .get_mut(cursor_id)
                        .expect("cursor_id should be valid")
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
                CursorType::IndexMethod(..) => {
                    panic!("OpenEphemeral on index method cursor")
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
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        OpenDup {
            new_cursor_id,
            original_cursor_id,
        },
        insn
    );
    let mv_store = program.connection.mv_store();

    let original_cursor = state.get_cursor(*original_cursor_id);
    let original_cursor = original_cursor.as_btree_mut();

    let root_page = original_cursor.root_page();
    // We use the pager from the original cursor instead of the one attached to
    // the connection because each ephemeral table creates its own pager (and
    // a separate database file).
    let pager = original_cursor.get_pager();

    // Ephemeral tables have their own pager, so we need to check if this is an
    // ephemeral cursor by comparing pagers. Ephemeral cursors should NOT be wrapped
    // in MvCursor because the mv_store doesn't have mappings for ephemeral table root pages.
    let is_ephemeral = pager.wal.is_none();

    let (_, cursor_type) = program
        .cursor_ref
        .get(*original_cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    match cursor_type {
        CursorType::BTreeTable(table) => {
            let cursor = Box::new(BTreeCursor::new_table(
                pager,
                maybe_transform_root_page_to_positive(mv_store.as_ref(), root_page),
                table.columns.len(),
            ));
            let cursor: Box<dyn CursorTrait> = if !is_ephemeral {
                if let Some(tx_id) = program.connection.get_mv_tx_id() {
                    let mv_store = mv_store
                        .as_ref()
                        .expect("mv_store should be Some when MVCC transaction is active")
                        .clone();
                    Box::new(MvCursor::new(
                        mv_store,
                        tx_id,
                        root_page,
                        MvccCursorType::Table,
                        cursor,
                    )?)
                } else {
                    cursor
                }
            } else {
                cursor
            };
            let cursors = &mut state.cursors;
            cursors
                .get_mut(*new_cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::BTreeIndex(_) => {
            return Err(LimboError::InternalError(
                "OpenDup is not supported for BTreeIndex".to_string(),
            ));
        }
        _ => {
            return Err(LimboError::InternalError(format!(
                "OpenDup is not supported for {cursor_type:?}"
            )));
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Execute the [Insn::Once] instruction.
///
/// This instruction is used to execute a block of code only once.
/// If the instruction is executed again, it will jump to the target program counter.
pub fn op_once(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Once {
            target_pc_when_reentered,
        },
        insn
    );
    assert!(target_pc_when_reentered.is_offset());
    let offset = state.pc;
    if state.once.contains(&offset) {
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
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _pager: &Arc<Pager>,
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

    state.registers[*target_reg] = Register::Value(Value::from_i64(count as i64));

    // For optimized COUNT(*) queries, the count represents rows that would be read
    // SQLite tracks this differently (as pages read), but for consistency we track as rows
    if *exact {
        state.metrics.rows_read = state.metrics.rows_read.saturating_add(count as u64);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Format integrity check errors into a result string.
/// Returns NULL when no errors were found.
fn format_integrity_check_result(errors: &[IntegrityCheckError]) -> Option<String> {
    if errors.is_empty() {
        None
    } else {
        Some(
            errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join("\n"),
        )
    }
}

fn has_freelist_error(errors: &[IntegrityCheckError]) -> bool {
    errors.iter().any(|err| match err {
        IntegrityCheckError::FreelistTrunkCorrupt { .. }
        | IntegrityCheckError::FreelistPointerOutOfRange { .. } => true,
        IntegrityCheckError::PageReferencedMultipleTimes { page_category, .. } => {
            matches!(
                page_category,
                PageCategory::FreeListTrunk | PageCategory::FreePage
            )
        }
        _ => false,
    })
}

pub enum OpIntegrityCheckState {
    Start,
    CheckingBTreeStructure {
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
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        IntegrityCk {
            db,
            max_errors,
            roots,
            message_register,
        },
        insn
    );

    let mv_store = program.connection.mv_store_for_db(*db);
    // Use the correct pager for the target database (main or attached)
    let target_pager = if *db == crate::MAIN_DB_ID {
        pager.clone()
    } else {
        program.get_pager_from_database_index(db)
    };
    match &mut state.op_integrity_check_state {
        OpIntegrityCheckState::Start => {
            let (freelist_trunk_page, db_size) = return_if_io!(with_header(
                &target_pager,
                mv_store.as_ref(),
                program,
                *db,
                |header| (header.freelist_trunk_page.get(), header.database_size.get())
            ));
            let mut errors = Vec::new();
            let mut integrity_check_state = IntegrityCheckState::new(db_size as usize);
            let mut current_root_idx = 0;

            if freelist_trunk_page > 0 {
                let expected_freelist_count = return_if_io!(with_header(
                    &target_pager,
                    mv_store.as_ref(),
                    program,
                    *db,
                    |header| { header.freelist_pages.get() }
                ));
                integrity_check_state.set_expected_freelist_count(expected_freelist_count as usize);
                integrity_check_state.start(
                    freelist_trunk_page as i64,
                    PageCategory::FreeListTrunk,
                    &mut errors,
                );
            } else if !roots.is_empty() {
                integrity_check_state.start(roots[0], PageCategory::Normal, &mut errors);
                current_root_idx += 1;
            }

            state.op_integrity_check_state = OpIntegrityCheckState::CheckingBTreeStructure {
                errors,
                state: integrity_check_state,
                current_root_idx,
            };
        }
        OpIntegrityCheckState::CheckingBTreeStructure {
            errors,
            current_root_idx,
            state: integrity_check_state,
        } => {
            return_if_io!(integrity_check(
                integrity_check_state,
                errors,
                &target_pager,
                mv_store.as_ref()
            ));

            if errors.len() >= *max_errors {
                errors.truncate(*max_errors);
                let message = format_integrity_check_result(errors);
                state.registers[*message_register] = match message {
                    Some(msg) => Register::Value(Value::build_text(msg)),
                    None => Register::Value(Value::Null),
                };
                state.op_integrity_check_state = OpIntegrityCheckState::Start;
                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }

            if *current_root_idx < roots.len() {
                integrity_check_state.start(roots[*current_root_idx], PageCategory::Normal, errors);
                *current_root_idx += 1;
                return Ok(InsnFunctionStepResult::Step);
            }

            if !has_freelist_error(errors)
                && integrity_check_state.freelist_count.actual_count
                    != integrity_check_state.freelist_count.expected_count
            {
                errors.push(IntegrityCheckError::FreelistCountMismatch {
                    actual_count: integrity_check_state.freelist_count.actual_count,
                    expected_count: integrity_check_state.freelist_count.expected_count,
                });
            }

            #[cfg(not(feature = "omit_autovacuum"))]
            let skip_page_never_used = !matches!(
                target_pager.get_auto_vacuum_mode(),
                crate::storage::pager::AutoVacuumMode::None
            );
            #[cfg(feature = "omit_autovacuum")]
            let skip_page_never_used = false;

            if !skip_page_never_used {
                for page_number in 2..=integrity_check_state.db_size {
                    if !integrity_check_state
                        .page_reference
                        .contains_key(&(page_number as i64))
                    {
                        if target_pager.pending_byte_page_id() != Some(page_number as u32) {
                            errors.push(IntegrityCheckError::PageNeverUsed {
                                page_id: page_number as i64,
                            });
                        }
                    } else if target_pager.pending_byte_page_id() == Some(page_number as u32) {
                        errors.push(IntegrityCheckError::PendingBytePageUsed {
                            page_id: page_number as i64,
                        })
                    }

                    if errors.len() >= *max_errors {
                        break;
                    }
                }
            }

            errors.truncate(*max_errors);
            let message = format_integrity_check_result(errors);
            state.registers[*message_register] = match message {
                Some(msg) => Register::Value(Value::build_text(msg)),
                None => Register::Value(Value::Null),
            };
            state.op_integrity_check_state = OpIntegrityCheckState::Start;
            state.pc += 1;
        }
    }

    Ok(InsnFunctionStepResult::Step)
}
pub fn op_cast(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
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
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(RenameTable { db, from, to }, insn);

    let normalized_from = normalize_ident(from.as_str());
    let normalized_to = normalize_ident(to.as_str());

    let conn = program.connection.clone();

    conn.with_database_schema_mut(*db, |schema| -> crate::Result<()> {
        if let Some(mut indexes) = schema.indexes.remove(&normalized_from) {
            indexes.iter_mut().for_each(|index| {
                let index = Arc::make_mut(index);
                normalized_to.clone_into(&mut index.table_name);
            });

            schema.indexes.insert(normalized_to.to_owned(), indexes);
        };

        let mut table = schema
            .tables
            .remove(&normalized_from)
            .expect("table being renamed should be in schema");
        match Arc::make_mut(&mut table) {
            Table::BTree(btree) => {
                let btree = Arc::make_mut(btree);
                // update this table's own foreign keys
                for fk_arc in &mut btree.foreign_keys {
                    let fk = Arc::make_mut(fk_arc);
                    if normalize_ident(&fk.parent_table) == normalized_from {
                        fk.parent_table.clone_from(&normalized_to);
                    }
                }

                // Rewrite table-qualified refs in CHECK constraints
                for check in &mut btree.check_constraints {
                    rewrite_check_expr_table_refs(
                        &mut check.expr,
                        &normalized_from,
                        &normalized_to,
                    );
                }

                normalized_to.clone_into(&mut btree.name);
            }
            Table::Virtual(vtab) => {
                Arc::make_mut(vtab).name.clone_from(&normalized_to);
            }
            _ => panic!("only btree and virtual tables can be renamed"),
        }

        schema.tables.insert(normalized_to.to_owned(), table);

        for (tname, t_arc) in schema.tables.iter_mut() {
            // skip the table we just renamed
            if normalize_ident(tname) == normalized_to {
                continue;
            }
            if let Table::BTree(ref mut child_btree_arc) = Arc::make_mut(t_arc) {
                let child_btree = Arc::make_mut(child_btree_arc);
                for fk_arc in &mut child_btree.foreign_keys {
                    if normalize_ident(&fk_arc.parent_table) == normalized_from {
                        let fk = Arc::make_mut(fk_arc);
                        fk.parent_table.clone_from(&normalized_to);
                    }
                }
            }
        }

        Ok(())
    })?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        DropColumn {
            db,
            table,
            column_index
        },
        insn
    );

    let conn = program.connection.clone();

    let normalized_table_name = normalize_ident(table.as_str());

    let column_name = conn.with_schema(*db, |schema| {
        let table = schema
            .tables
            .get(&normalized_table_name)
            .expect("table being ALTERed should be in schema");
        table
            .get_column_at(*column_index)
            .expect("column being ALTERed should be in schema")
            .name
            .as_ref()
            .expect("column being ALTERed should be named")
            .clone()
    });

    conn.with_database_schema_mut(*db, |schema| {
        let table = schema
            .tables
            .get_mut(&normalized_table_name)
            .expect("table being renamed should be in schema");

        let table = Arc::get_mut(table).expect("this should be the only strong reference");

        let Table::BTree(btree) = table else {
            panic!("only btree tables can be renamed");
        };

        let btree = Arc::make_mut(btree);
        btree.columns.remove(*column_index);
        // Remove column-level CHECK constraints for the dropped column
        let col_name = column_name.clone();
        btree.check_constraints.retain(|c| {
            c.column
                .as_ref()
                .is_none_or(|col| normalize_ident(col) != normalize_ident(&col_name))
        });
    });

    conn.with_schema(*db, |schema| -> crate::Result<()> {
        if let Some(indexes) = schema.indexes.get(&normalized_table_name) {
            for index in indexes {
                if index
                    .columns
                    .iter()
                    .any(|column| column.pos_in_table == *column_index)
                {
                    return Err(LimboError::ParseError(format!(
                        "cannot drop column \"{column_name}\": indexed"
                    )));
                }
            }
        }
        Ok(())
    })?;

    // Update index.pos_in_table for all indexes.
    // For example, if the dropped column had index 2, then anything that was indexed on column 3 or higher should be decremented by 1.
    conn.with_database_schema_mut(*db, |schema| {
        if let Some(indexes) = schema.indexes.get_mut(&normalized_table_name) {
            for index in indexes {
                let index = Arc::get_mut(index).expect("this should be the only strong reference");
                for index_column in index.columns.iter_mut() {
                    if index_column.pos_in_table > *column_index {
                        index_column.pos_in_table -= 1;
                    }
                }
            }
        }
    });

    conn.with_schema(*db, |schema| -> crate::Result<()> {
        for (view_name, view) in schema.views.iter() {
            let view_select_sql = format!("SELECT * FROM {view_name}");
            let _ = conn.prepare(view_select_sql.as_str()).map_err(|e| {
                LimboError::ParseError(format!(
                    "cannot drop column \"{}\": referenced in VIEW {view_name}: {}. {e}",
                    column_name, view.sql,
                ))
            })?;
        }
        Ok(())
    })?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        AddColumn {
            db,
            table,
            column,
            check_constraints
        },
        insn
    );

    let conn = program.connection.clone();

    conn.with_database_schema_mut(*db, |schema| {
        let table_ref = schema
            .tables
            .get_mut(table)
            .expect("table being altered should be in schema");

        let table_ref = Arc::make_mut(table_ref);

        let crate::schema::Table::BTree(btree) = table_ref else {
            panic!("only btree tables can have columns added");
        };

        let btree = Arc::make_mut(btree);
        btree.columns.push((**column).clone());
        // Update CHECK constraints to include any constraints from the new column
        btree.check_constraints.clone_from(check_constraints);
    });

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_alter_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        AlterColumn {
            db,
            table: table_name,
            column_index,
            definition,
            rename,
        },
        insn
    );

    let conn = program.connection.clone();

    let normalized_table_name = normalize_ident(table_name.as_str());
    let old_column_name = conn.with_schema(*db, |schema| {
        let table = schema
            .tables
            .get(&normalized_table_name)
            .expect("table being ALTERed should be in schema");
        table
            .get_column_at(*column_index)
            .expect("column being ALTERed should be in schema")
            .name
            .as_ref()
            .expect("column being ALTERed should be named")
            .clone()
    });
    let new_column = crate::schema::Column::try_from(definition.as_ref())?;
    let new_name = definition.col_name.as_str().to_owned();

    conn.with_database_schema_mut(*db, |schema| {
        let table_arc = schema
            .tables
            .get_mut(&normalized_table_name)
            .expect("table being ALTERed should be in schema");
        let table = Arc::make_mut(table_arc);

        let Table::BTree(ref mut btree_arc) = table else {
            panic!("only btree tables can be altered");
        };
        let btree = Arc::make_mut(btree_arc);
        let col = btree
            .columns
            .get_mut(*column_index)
            .expect("column being ALTERed should be in schema");

        // Update indexes on THIS table that name the old column (you already had this)
        if let Some(idxs) = schema.indexes.get_mut(&normalized_table_name) {
            for idx in idxs {
                let idx = Arc::make_mut(idx);
                for ic in &mut idx.columns {
                    if ic.name.eq_ignore_ascii_case(
                        col.name.as_ref().expect("btree column should be named"),
                    ) {
                        ic.name.clone_from(&new_name);
                    }
                }
                // Update partial index WHERE clause column references
                if let Some(ref mut wc) = idx.where_clause {
                    rename_identifiers(wc, &old_column_name, &new_name);
                }
            }
        }
        if *rename {
            col.name = Some(new_name.clone());
        } else {
            *col = new_column.clone();
        }

        // Keep primary_key_columns consistent (names may change on rename)
        for (pk_name, _ord) in &mut btree.primary_key_columns {
            if pk_name.eq_ignore_ascii_case(&old_column_name) {
                pk_name.clone_from(&new_name);
            }
        }

        // Update unique_sets to reflect the renamed column
        for unique_set in &mut btree.unique_sets {
            for (col_name, _) in &mut unique_set.columns {
                if col_name.eq_ignore_ascii_case(&old_column_name) {
                    col_name.clone_from(&new_name);
                }
            }
        }

        // Update CHECK constraint expressions to reference the new column name
        let old_col_normalized = normalize_ident(&old_column_name);
        for check in &mut btree.check_constraints {
            rename_identifiers(&mut check.expr, &old_col_normalized, &new_name);
            if let Some(ref mut col) = check.column {
                if col.eq_ignore_ascii_case(&old_column_name) {
                    col.clone_from(&new_name);
                }
            }
        }

        // Maintain rowid-alias bit after change/rename (INTEGER PRIMARY KEY)
        if !*rename {
            // recompute alias from `new_column`
            btree.columns[*column_index].set_rowid_alias(new_column.is_rowid_alias());
        }

        // Update this table's OWN foreign keys
        for fk_arc in &mut btree.foreign_keys {
            let fk = Arc::make_mut(fk_arc);
            // child side: rename child column if it matches
            for cc in &mut fk.child_columns {
                if cc.eq_ignore_ascii_case(&old_column_name) {
                    cc.clone_from(&new_name);
                }
            }
            // parent side: if self-referencing, rename parent column too
            if normalize_ident(&fk.parent_table) == normalized_table_name {
                for pc in &mut fk.parent_columns {
                    if pc.eq_ignore_ascii_case(&old_column_name) {
                        pc.clone_from(&new_name);
                    }
                }
            }
        }

        // fix OTHER tables that reference this table as parent
        for (tname, t_arc) in schema.tables.iter_mut() {
            if normalize_ident(tname) == normalized_table_name {
                continue;
            }
            if let Table::BTree(ref mut child_btree_arc) = Arc::make_mut(t_arc) {
                let child_btree = Arc::make_mut(child_btree_arc);
                for fk_arc in &mut child_btree.foreign_keys {
                    if normalize_ident(&fk_arc.parent_table) != normalized_table_name {
                        continue;
                    }
                    let fk = Arc::make_mut(fk_arc);
                    for pc in &mut fk.parent_columns {
                        if pc.eq_ignore_ascii_case(&old_column_name) {
                            pc.clone_from(&new_name);
                        }
                    }
                }
            }
        }
    });

    if *rename {
        conn.with_schema(*db, |schema| -> crate::Result<()> {
            let table = schema
                .tables
                .get(&normalized_table_name)
                .expect("table being ALTERed should be in schema");
            let _column = table
                .get_column_at(*column_index)
                .expect("column being ALTERed should be in schema");
            for (view_name, view) in schema.views.iter() {
                let view_select_sql = format!("SELECT * FROM {view_name}");
                // FIXME: this should rewrite the view to reference the new column name
                let _ = conn.prepare(view_select_sql.as_str()).map_err(|e| {
                    LimboError::ParseError(format!(
                        "cannot rename column \"{}\": referenced in VIEW {view_name}: {}. {e}",
                        old_column_name, view.sql,
                    ))
                })?;
            }
            Ok(())
        })?;
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if_neg(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(IfNeg { reg, target_pc }, insn);

    match &state.registers[*reg] {
        Register::Value(Value::Numeric(Numeric::Integer(i))) if *i < 0 => {
            state.pc = target_pc.as_offset_int();
        }
        Register::Value(Value::Numeric(Numeric::Float(f))) if f64::from(*f) < 0.0 => {
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

fn handle_text_sum(
    acc: &mut Value,
    sum_state: &mut SumAggState,
    parsed_number: ParsedNumber,
    parse_result: NumericParseResult,
    force_approx: bool,
) {
    // SQLite treats text that only partially parses as numeric (ValidPrefixOnly)
    // as approximate, so SUM returns real instead of integer. Non-integer inputs
    // (e.g. BLOB) should also force approximate results.
    let is_approx = force_approx || matches!(parse_result, NumericParseResult::ValidPrefixOnly);
    match parsed_number {
        ParsedNumber::Integer(i) => {
            if is_approx {
                sum_state.approx = true;
                match acc {
                    Value::Null => {
                        *acc = Value::from_f64(i as f64);
                    }
                    Value::Numeric(Numeric::Integer(acc_i)) => {
                        *acc = Value::from_f64(*acc_i as f64);
                        apply_kbn_step_int(acc, i, sum_state);
                    }
                    Value::Numeric(Numeric::Float(_)) => {
                        apply_kbn_step_int(acc, i, sum_state);
                    }
                    _ => unreachable!(),
                }
            } else {
                match acc {
                    Value::Null => {
                        *acc = Value::from_i64(i);
                    }
                    Value::Numeric(Numeric::Integer(acc_i)) => match acc_i.checked_add(i) {
                        Some(sum) => *acc = Value::from_i64(sum),
                        None => {
                            let acc_f = *acc_i as f64;
                            *acc = Value::from_f64(acc_f);
                            sum_state.approx = true;
                            sum_state.ovrfl = true;
                            apply_kbn_step_int(acc, i, sum_state);
                        }
                    },
                    Value::Numeric(Numeric::Float(_)) => {
                        apply_kbn_step_int(acc, i, sum_state);
                    }
                    _ => unreachable!(),
                }
            }
        }
        ParsedNumber::Float(f) => {
            if !sum_state.approx {
                if let Value::Numeric(Numeric::Integer(current_sum)) = *acc {
                    *acc = Value::from_f64(current_sum as f64);
                } else if matches!(*acc, Value::Null) {
                    *acc = Value::from_f64(0.0);
                }
                sum_state.approx = true;
            }
            apply_kbn_step(acc, f, sum_state);
        }
        ParsedNumber::None => {
            if !sum_state.approx {
                if let Value::Numeric(Numeric::Integer(current_sum)) = *acc {
                    *acc = Value::from_f64(current_sum as f64);
                } else if matches!(*acc, Value::Null) {
                    *acc = Value::from_f64(0.0);
                }
                sum_state.approx = true;
            }
        }
    }
}

pub fn op_fk_counter(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        FkCounter {
            increment_value,
            deferred,
        },
        insn
    );
    if !*deferred {
        state.increment_fk_immediate_violations_during_stmt(*increment_value);
    } else {
        // Transaction-level counter: add/subtract for deferred FKs.
        program
            .connection
            .increment_deferred_foreign_key_violations(*increment_value);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_fk_if_zero(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        FkIfZero {
            deferred,
            target_pc,
        },
        insn
    );
    let fk_enabled = program.connection.foreign_keys_enabled();

    // Jump if any:
    // Foreign keys are disabled globally
    // p1 is true AND deferred constraint counter is zero
    // p1 is false AND deferred constraint counter is non-zero
    if !fk_enabled {
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    }
    let v = if *deferred {
        program.connection.get_deferred_foreign_key_violations()
    } else {
        state.get_fk_immediate_violations_during_stmt()
    };

    state.pc = if v == 0 {
        target_pc.as_offset_int()
    } else {
        state.pc + 1
    };
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_fk_check(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(FkCheck { deferred }, insn);
    if !program.connection.foreign_keys_enabled() {
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    let v = if *deferred {
        program.connection.get_deferred_foreign_key_violations()
    } else {
        state.get_fk_immediate_violations_during_stmt()
    };
    if v > 0 {
        return Err(LimboError::ForeignKeyConstraint(
            "immediate foreign key constraint failed".to_string(),
        ));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_build(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(HashBuild { data }, insn);

    let mut op_state = state
        .op_hash_build_state
        .take()
        .filter(|s| {
            s.hash_table_id == data.hash_table_id
                && s.cursor_id == data.cursor_id
                && s.key_start_reg == data.key_start_reg
                && s.num_keys == data.num_keys
        })
        .unwrap_or_else(|| OpHashBuildState {
            key_values: Vec::with_capacity(data.num_keys),
            key_idx: 0,
            payload_values: Vec::with_capacity(data.num_payload),
            payload_idx: 0,
            rowid: None,
            cursor_id: data.cursor_id,
            hash_table_id: data.hash_table_id,
            key_start_reg: data.key_start_reg,
            num_keys: data.num_keys,
        });

    // Create hash table if it doesn't exist yet
    let temp_store = program.connection.get_temp_store();
    // When temp_store=memory, disable the memory limit entirely to avoid spilling.
    // Spilling to an in-memory file has serialization overhead - simpler to never spill.
    let mem_budget = if matches!(temp_store, crate::TempStore::Memory) {
        usize::MAX
    } else {
        data.mem_budget
    };
    state
        .hash_tables
        .entry(data.hash_table_id)
        .or_insert_with(|| {
            let config = HashTableConfig {
                initial_buckets: 1024,
                mem_budget,
                num_keys: data.num_keys,
                collations: data.collations.clone(),
                temp_store,
                track_matched: data.track_matched,
                partition_count: None,
            };
            HashTable::new(config, pager.io.clone())
        });

    // Read pre-computed key values directly from registers
    while op_state.key_idx < data.num_keys {
        let i = op_state.key_idx;
        let reg = &state.registers[data.key_start_reg + i];
        let value = reg.get_value().clone();
        op_state.key_values.push(value);
        op_state.key_idx += 1;
    }

    // Read payload values from registers if provided
    if let Some(payload_reg) = data.payload_start_reg {
        while op_state.payload_idx < data.num_payload {
            let i = op_state.payload_idx;
            let reg = &state.registers[payload_reg + i];
            let value = reg.get_value().clone();
            op_state.payload_values.push(value);
            op_state.payload_idx += 1;
        }
    }

    // Get the rowid from the cursor
    if op_state.rowid.is_none() {
        let cursor = state.get_cursor(data.cursor_id);
        let rowid_val = match cursor {
            Cursor::BTree(btree_cursor) => {
                let rowid_opt = match btree_cursor.rowid() {
                    Ok(IOResult::Done(v)) => v,
                    Ok(IOResult::IO(io)) => {
                        state.op_hash_build_state = Some(op_state);
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                    Err(e) => {
                        state.op_hash_build_state = Some(op_state);
                        return Err(e);
                    }
                };
                rowid_opt.ok_or_else(|| {
                    LimboError::InternalError("HashBuild: cursor has no rowid".to_string())
                })?
            }
            _ => {
                return Err(LimboError::InternalError(
                    "HashBuild: unsupported cursor type".to_string(),
                ));
            }
        };
        op_state.rowid = Some(rowid_val);
    }

    // Insert the rowid into the hash table
    if let Some(ht) = state.hash_tables.get_mut(&data.hash_table_id) {
        let rowid = op_state.rowid.expect("rowid set");
        let key_values = std::mem::take(&mut op_state.key_values);
        let payload_values = std::mem::take(&mut op_state.payload_values);
        match ht.insert(
            key_values.clone(),
            rowid,
            payload_values.clone(),
            Some(&mut state.metrics.hash_join),
        ) {
            Ok(IOResult::Done(())) => {}
            Ok(IOResult::IO(io)) => {
                op_state.key_values = key_values;
                op_state.payload_values = payload_values;
                state.op_hash_build_state = Some(op_state);
                return Ok(InsnFunctionStepResult::IO(io));
            }
            Err(e) => {
                op_state.key_values = key_values;
                op_state.payload_values = payload_values;
                state.op_hash_build_state = Some(op_state);
                return Err(e);
            }
        }
    }

    state.op_hash_build_state = None;
    state.metrics.rows_read = state.metrics.rows_read.saturating_add(1);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_distinct(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(HashDistinct { data }, insn);

    let temp_store = program.connection.get_temp_store();
    // When temp_store=memory, disable the memory limit entirely to avoid spilling.
    let mem_budget = if matches!(temp_store, crate::TempStore::Memory) {
        usize::MAX
    } else {
        DEFAULT_MEM_BUDGET
    };
    let hash_table = state
        .hash_tables
        .entry(data.hash_table_id)
        .or_insert_with(|| {
            let config = HashTableConfig {
                initial_buckets: 1024,
                mem_budget,
                num_keys: data.num_keys,
                collations: data.collations.clone(),
                temp_store,
                track_matched: false,
                partition_count: None,
            };
            HashTable::new(config, pager.io.clone())
        });

    let key_values = &mut state.distinct_key_values;
    key_values.clear();
    for i in 0..data.num_keys {
        let reg = &state.registers[data.key_start_reg + i];
        key_values.push(reg.get_value().clone());
    }

    let mut key_refs: SmallVec<[ValueRef; 2]> = SmallVec::with_capacity(data.num_keys);
    key_refs.extend(key_values.iter().map(|v| v.as_ref()));
    match hash_table.insert_distinct(key_values, &key_refs, Some(&mut state.metrics.hash_join))? {
        IOResult::Done(inserted) => {
            state.pc = if inserted {
                state.pc + 1
            } else {
                data.target_pc.as_offset_int()
            };
            Ok(InsnFunctionStepResult::Step)
        }
        IOResult::IO(io) => Ok(InsnFunctionStepResult::IO(io)),
    }
}

pub fn op_hash_build_finalize(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(HashBuildFinalize { hash_table_id }, insn);
    if let Some(ht) = state.hash_tables.get_mut(hash_table_id) {
        // Finalize the build phase, may flush remaining partitions to disk if spilled
        match ht.finalize_build(Some(&mut state.metrics.hash_join))? {
            crate::types::IOResult::Done(()) => {
                // Partitions will be loaded on-demand during probing
            }
            crate::types::IOResult::IO(io) => {
                return Ok(InsnFunctionStepResult::IO(io));
            }
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Write payload values from a hash entry to registers.
fn write_hash_payload_to_registers(
    registers: &mut [Register],
    entry: &HashEntry,
    payload_dest_reg: Option<usize>,
    num_payload: usize,
) {
    if let Some(dest_reg) = payload_dest_reg {
        for (i, value) in entry.payload_values.iter().take(num_payload).enumerate() {
            registers[dest_reg + i] = Register::Value(value.clone());
        }
    }
}

pub fn op_hash_probe(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        HashProbe {
            hash_table_id,
            key_start_reg,
            num_keys,
            dest_reg,
            target_pc,
            payload_dest_reg,
            num_payload,
        },
        insn
    );
    let hash_table_id = *hash_table_id as usize;
    let key_start_reg = *key_start_reg as usize;
    let num_keys = *num_keys as usize;
    let dest_reg = *dest_reg as usize;
    let payload_dest_reg = payload_dest_reg.map(|r| r as usize);
    let num_payload = *num_payload as usize;
    let (probe_keys, partition_idx) = if let Some(op_state) = state.op_hash_probe_state.take() {
        if op_state.hash_table_id == hash_table_id {
            (op_state.probe_keys, Some(op_state.partition_idx))
        } else {
            // Different hash table, read fresh keys
            let mut keys = Vec::with_capacity(num_keys);
            for i in 0..num_keys {
                let reg = &state.registers[key_start_reg + i];
                keys.push(reg.get_value().clone());
            }
            (keys, None)
        }
    } else {
        // First entry, read probe keys from registers
        let mut keys = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            let reg = &state.registers[key_start_reg + i];
            keys.push(reg.get_value().clone());
        }
        (keys, None)
    };

    let Some(hash_table) = state.hash_tables.get_mut(&hash_table_id) else {
        // Empty build side: treat as no match and jump to target.
        state.op_hash_probe_state = None;
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    };

    // For spilled hash tables, load the appropriate partition on demand
    if hash_table.has_spilled() {
        let partition_idx =
            partition_idx.unwrap_or_else(|| hash_table.partition_for_keys(&probe_keys));

        // Load partition if not already loaded (may require multiple re-entries for multi-chunk partitions)
        if !hash_table.is_partition_loaded(partition_idx) {
            match hash_table
                .load_spilled_partition(partition_idx, Some(&mut state.metrics.hash_join))?
            {
                IOResult::Done(()) => {
                    // Partition loaded or nothing to load
                }
                IOResult::IO(io) => {
                    state.op_hash_probe_state = Some(OpHashProbeState {
                        probe_keys,
                        hash_table_id,
                        partition_idx,
                    });
                    return Ok(InsnFunctionStepResult::IO(io));
                }
            }
        }

        // Probe the loaded partition
        match hash_table.probe_partition(
            partition_idx,
            &probe_keys,
            Some(&mut state.metrics.hash_join),
        ) {
            Some(entry) => {
                state.registers[dest_reg] = Register::Value(Value::from_i64(entry.rowid));
                write_hash_payload_to_registers(
                    &mut state.registers,
                    entry,
                    payload_dest_reg,
                    num_payload,
                );
                state.pc += 1;
                Ok(InsnFunctionStepResult::Step)
            }
            None => {
                state.pc = target_pc.as_offset_int();
                Ok(InsnFunctionStepResult::Step)
            }
        }
    } else {
        // Non-spilled hash table, use normal probe
        match hash_table.probe(probe_keys, Some(&mut state.metrics.hash_join)) {
            Some(entry) => {
                state.registers[dest_reg] = Register::Value(Value::from_i64(entry.rowid));
                write_hash_payload_to_registers(
                    &mut state.registers,
                    entry,
                    payload_dest_reg,
                    num_payload,
                );
                state.pc += 1;
                Ok(InsnFunctionStepResult::Step)
            }
            None => {
                state.pc = target_pc.as_offset_int();
                Ok(InsnFunctionStepResult::Step)
            }
        }
    }
}

pub fn op_hash_next(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        HashNext {
            hash_table_id,
            dest_reg,
            target_pc,
            payload_dest_reg,
            num_payload,
        },
        insn
    );

    let hash_table = state.hash_tables.get_mut(hash_table_id).ok_or_else(|| {
        LimboError::InternalError(format!("Hash table not found with ID: {hash_table_id}"))
    })?;
    match hash_table.next_match() {
        Some(entry) => {
            state.registers[*dest_reg] = Register::Value(Value::from_i64(entry.rowid));
            write_hash_payload_to_registers(
                &mut state.registers,
                entry,
                *payload_dest_reg,
                *num_payload,
            );
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        None => {
            state.pc = target_pc.as_offset_int();
            Ok(InsnFunctionStepResult::Step)
        }
    }
}

pub fn op_hash_close(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(HashClose { hash_table_id }, insn);
    if let Some(mut hash_table) = state.hash_tables.remove(hash_table_id) {
        hash_table.close();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_clear(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(HashClear { hash_table_id }, insn);
    if let Some(hash_table) = state.hash_tables.get_mut(hash_table_id) {
        hash_table.clear();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_mark_matched(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(HashMarkMatched { hash_table_id }, insn);
    if let Some(hash_table) = state.hash_tables.get_mut(hash_table_id) {
        hash_table.mark_current_matched();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_scan_unmatched(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        HashScanUnmatched {
            hash_table_id,
            dest_reg,
            target_pc,
            payload_dest_reg,
            num_payload,
        },
        insn
    );

    let Some(hash_table) = state.hash_tables.get_mut(hash_table_id) else {
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    };

    hash_table.begin_unmatched_scan();
    advance_unmatched_scan(
        hash_table,
        &mut state.registers,
        &mut state.pc,
        *dest_reg,
        target_pc.as_offset_int(),
        *payload_dest_reg,
        *num_payload,
        &mut state.metrics.hash_join,
    )
}

pub fn op_hash_next_unmatched(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        HashNextUnmatched {
            hash_table_id,
            dest_reg,
            target_pc,
            payload_dest_reg,
            num_payload,
        },
        insn
    );

    let hash_table = state.hash_tables.get_mut(hash_table_id).ok_or_else(|| {
        LimboError::InternalError(format!("Hash table not found with ID: {hash_table_id}"))
    })?;

    advance_unmatched_scan(
        hash_table,
        &mut state.registers,
        &mut state.pc,
        *dest_reg,
        target_pc.as_offset_int(),
        *payload_dest_reg,
        *num_payload,
        &mut state.metrics.hash_join,
    )
}

/// Shared logic for HashScanUnmatched/HashNextUnmatched: find the next unmatched
/// entry, loading spilled partitions as needed.
#[allow(clippy::too_many_arguments)]
fn advance_unmatched_scan(
    hash_table: &mut HashTable,
    registers: &mut [Register],
    pc: &mut u32,
    dest_reg: usize,
    target_pc: u32,
    payload_dest_reg: Option<usize>,
    num_payload: usize,
    metrics: &mut HashJoinMetrics,
) -> Result<InsnFunctionStepResult> {
    if hash_table.has_spilled() {
        if let Some(partition_idx) = hash_table.unmatched_scan_current_partition() {
            if !hash_table.is_partition_loaded(partition_idx) {
                match hash_table.load_spilled_partition(partition_idx, Some(metrics))? {
                    crate::types::IOResult::Done(()) => {}
                    crate::types::IOResult::IO(io) => {
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                }
            }
        }
    }

    loop {
        match hash_table.next_unmatched() {
            Some(entry) => {
                registers[dest_reg] =
                    Register::Value(Value::Numeric(Numeric::Integer(entry.rowid)));
                write_hash_payload_to_registers(registers, entry, payload_dest_reg, num_payload);
                *pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
            None => {
                if hash_table.has_spilled() {
                    if let Some(partition_idx) = hash_table.unmatched_scan_current_partition() {
                        if !hash_table.is_partition_loaded(partition_idx) {
                            match hash_table.load_spilled_partition(partition_idx, Some(metrics))? {
                                crate::types::IOResult::Done(()) => continue,
                                crate::types::IOResult::IO(io) => {
                                    return Ok(InsnFunctionStepResult::IO(io));
                                }
                            }
                        }
                    }
                }
                *pc = target_pc;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
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
                if matches!(value, Value::Numeric(Numeric::Integer(_))) {
                    return true;
                }
                if !matches!(value, Value::Text(_) | Value::Numeric(Numeric::Float(_))) {
                    return true;
                }

                if let Value::Numeric(Numeric::Float(fl)) = *value {
                    // For floats, try to convert to integer if it's exact
                    // This is similar to sqlite3VdbeIntegerAffinity
                    return try_float_to_integer_affinity(value, f64::from(fl));
                }

                if let Value::Text(t) = value {
                    let text = trim_ascii_whitespace(t.as_str());

                    // Handle hex numbers - they shouldn't be converted
                    if text.starts_with("0x") {
                        return false;
                    }

                    let (parse_result, parsed_value) = try_for_float(text.as_bytes());
                    let num = match parse_result {
                        NumericParseResult::NotNumeric | NumericParseResult::ValidPrefixOnly => {
                            return false;
                        }
                        NumericParseResult::PureInteger | NumericParseResult::HasDecimalOrExp => {
                            match parsed_value {
                                ParsedNumber::Integer(i) => Value::from_i64(i),
                                ParsedNumber::Float(f) => Value::from_f64(f),
                                ParsedNumber::None => return false,
                            }
                        }
                    };

                    match num {
                        Value::Numeric(Numeric::Integer(i)) => {
                            *value = Value::from_i64(i);
                            return true;
                        }
                        Value::Numeric(Numeric::Float(fl)) => {
                            // For both Numeric and Integer affinity, try to convert
                            // float to int if exact. SQLite treats INTEGER identically
                            // to NUMERIC here: both enter applyNumericAffinity() with
                            // bTryForInt=1 (sqlite/src/vdbe.c:403-408).
                            return try_float_to_integer_affinity(value, f64::from(fl));
                        }
                        other => {
                            *value = other;
                            return true;
                        }
                    }
                }

                return false;
            }

            Affinity::Real => match value {
                Value::Numeric(Numeric::Integer(i)) => {
                    *value = Value::from_f64(*i as f64);
                    return true;
                }
                Value::Numeric(Numeric::Float(_)) | Value::Null => {
                    return true;
                }
                Value::Text(t) => {
                    let text = trim_ascii_whitespace(t.as_str());
                    if text.starts_with("0x") {
                        return false;
                    }

                    let (parse_result, parsed_value) = try_for_float(text.as_bytes());
                    let coerced = match parse_result {
                        NumericParseResult::NotNumeric | NumericParseResult::ValidPrefixOnly => {
                            return false;
                        }
                        NumericParseResult::PureInteger | NumericParseResult::HasDecimalOrExp => {
                            match parsed_value {
                                ParsedNumber::Integer(i) => Value::from_f64(i as f64),
                                ParsedNumber::Float(f) => Value::from_f64(f),
                                ParsedNumber::None => return false,
                            }
                        }
                    };

                    *value = coerced;
                    return true;
                }
                _ => return true,
            },
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
            *value = Value::from_i64(int_val);
            return true;
        }
    }

    // If we can't convert to exact integer, keep as float for Numeric affinity
    // but return false to indicate the conversion wasn't "complete"
    *value = Value::from_f64(fl);
    false
}

fn parse_test_uint(reg: &Register) -> Result<Option<u64>> {
    match reg.get_value() {
        Value::Null => Ok(None),
        Value::Numeric(Numeric::Integer(i)) => {
            if *i < 0 {
                Err(LimboError::InternalError(
                    "test_uint: negative value".to_string(),
                ))
            } else {
                Ok(Some(*i as u64))
            }
        }
        Value::Text(t) => {
            let s = t.to_string();
            let v = s
                .parse::<u64>()
                .map_err(|_| LimboError::InternalError(format!("test_uint: invalid uint: {s}")))?;
            Ok(Some(v))
        }
        _ => Err(LimboError::InternalError(
            "test_uint: unsupported type".to_string(),
        )),
    }
}

// Compat for applications that test for SQLite.
fn execute_sqlite_version() -> String {
    "3.50.4".to_string()
}

fn execute_turso_version(version_integer: i64) -> String {
    let major = version_integer / 1_000_000;
    let minor = (version_integer % 1_000_000) / 1_000;
    let release = version_integer % 1_000;

    format!("{major}.{minor}.{release}")
}

pub fn extract_int_value<V: AsValueRef>(value: V) -> i64 {
    let value = value.as_value_ref();
    match value {
        ValueRef::Numeric(Numeric::Integer(i)) => i,
        ValueRef::Numeric(Numeric::Float(f)) => {
            let f = f64::from(f);
            // Use sqlite3RealToI64 equivalent
            if f < -9223372036854774784.0 {
                i64::MIN
            } else if f > 9223372036854774784.0 {
                i64::MAX
            } else {
                f as i64
            }
        }
        ValueRef::Text(t) => {
            // Try to parse as integer, return 0 if failed
            t.as_str().parse::<i64>().unwrap_or(0)
        }
        ValueRef::Blob(b) => {
            // Try to parse blob as string then as integer
            if let Ok(s) = std::str::from_utf8(b) {
                s.parse::<i64>().unwrap_or(0)
            } else {
                0
            }
        }
        ValueRef::Null => 0,
    }
}

pub fn op_max_pgcnt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(MaxPgcnt { db, dest, new_max }, insn);

    let pager = program.get_pager_from_database_index(db);
    let result_value = if *new_max == 0 {
        // If new_max is 0, just return current maximum without changing it
        pager.get_max_page_count()
    } else {
        // Set new maximum page count (will be clamped to current database size)
        return_if_io!(pager.set_max_page_count(*new_max as u32))
    };

    state.registers[*dest] = Register::Value(Value::from_i64(result_value.into()));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// State machine for PRAGMA journal_mode changes
#[derive(Debug, Clone, Copy, Default)]
pub enum OpJournalModeSubState {
    /// Initial state - read header to get current mode
    #[default]
    Start,
    /// Checkpointing WAL/MVCC before mode change
    Checkpoint,
    /// Update the header with new version and get page reference
    UpdateHeader,
    /// Write page 1 to disk
    WritePage,
    /// Finalize - clear cache and setup new mode
    Finalize,
}

/// Holds the state for the journal mode change operation
#[derive(Default)]
pub struct OpJournalModeState {
    pub sub_state: OpJournalModeSubState,
    /// The previous journal mode (before the change)
    pub prev_mode: Option<journal_mode::JournalMode>,
    /// The new journal mode we're changing to
    pub new_mode: Option<journal_mode::JournalMode>,
    /// Checkpoint state machine for MVCC mode
    pub checkpoint_sm: Option<StateMachine<CheckpointStateMachine<LocalClock>>>,
    /// Page reference for writing header
    pub page_ref: Option<PageRef>,
}

pub fn op_journal_mode(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    match op_journal_mode_inner(program, state, insn, pager) {
        Ok(result) => {
            if !matches!(result, InsnFunctionStepResult::IO(_)) {
                // Reset state if we are done with this instruction
                state.op_journal_mode_state = Default::default();
            }
            Ok(result)
        }
        Err(err) => {
            // Reset state on error
            state.op_journal_mode_state = Default::default();
            Err(err)
        }
    }
}

fn op_journal_mode_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    use crate::storage::sqlite3_ondisk::begin_write_btree_page;

    load_insn!(JournalMode { db, dest, new_mode }, insn);
    let pager = program.get_pager_from_database_index(db);
    let pager = &pager;

    loop {
        match state.op_journal_mode_state.sub_state {
            OpJournalModeSubState::Start => {
                // Read header to get current mode
                let mv_store = program.connection.mv_store_for_db(*db);
                let header_result = with_header(pager, mv_store.as_ref(), program, *db, |header| {
                    header.read_version
                });

                let prev_mode_raw = return_if_io!(header_result);

                let prev_mode_version = prev_mode_raw
                    .to_version()
                    .map_err(|val| LimboError::Corrupt(format!("Invalid read_version: {val}")))?;

                let prev_mode = journal_mode::JournalMode::from(prev_mode_version);
                state.op_journal_mode_state.prev_mode = Some(prev_mode);

                // If no new mode specified, just return current mode
                let Some(mode_str) = new_mode else {
                    let ret: &'static str = prev_mode.into();
                    state.registers[*dest] = Register::Value(Value::build_text(ret));
                    state.pc += 1;
                    return Ok(InsnFunctionStepResult::Step);
                };

                // Parse the new mode
                let new_mode =
                    journal_mode::JournalMode::from_str(mode_str.as_str()).map_err(|_| {
                        LimboError::ParseError(format!("Unknown journal mode: {mode_str}"))
                    })?;

                // Validate the new mode
                if !new_mode.supported() {
                    return Err(LimboError::ParseError(format!(
                        "Journal Mode `{new_mode}` is not supported"
                    )));
                }

                // If same mode, just return
                if prev_mode == new_mode {
                    let ret: &'static str = new_mode.into();
                    state.registers[*dest] = Register::Value(Value::build_text(ret));
                    state.pc += 1;
                    return Ok(InsnFunctionStepResult::Step);
                }

                // Check if database is readonly - cannot change journal mode on readonly databases
                if program.connection.is_readonly(*db) {
                    return Err(LimboError::ReadOnly);
                }

                state.op_journal_mode_state.new_mode = Some(new_mode);
                state.op_journal_mode_state.sub_state = OpJournalModeSubState::Checkpoint;
            }

            OpJournalModeSubState::Checkpoint => {
                // Checkpoint WAL or MVCC before changing mode
                let mv_store = program.connection.mv_store_for_db(*db);
                if let Some(mv_store) = mv_store.as_ref() {
                    // MVCC checkpoint using state machine
                    if state.op_journal_mode_state.checkpoint_sm.is_none() {
                        state.op_journal_mode_state.checkpoint_sm =
                            Some(StateMachine::new(CheckpointStateMachine::new(
                                pager.clone(),
                                mv_store.clone(),
                                program.connection.clone(),
                                true,
                                program.connection.get_sync_mode(),
                            )));
                    }

                    let ckpt_sm = state.op_journal_mode_state.checkpoint_sm.as_mut().unwrap();
                    return_if_io!(ckpt_sm.step(&()));
                    state.op_journal_mode_state.checkpoint_sm = None;
                    state.op_journal_mode_state.sub_state = OpJournalModeSubState::UpdateHeader;
                } else {
                    // WAL checkpoint
                    let checkpoint_result = pager.checkpoint(
                        CheckpointMode::Truncate {
                            upper_bound_inclusive: None,
                        },
                        program.connection.get_sync_mode(),
                        false, // Don't clear cache yet, we'll do it in Finalize
                    );
                    return_if_io!(checkpoint_result);
                    state.op_journal_mode_state.sub_state = OpJournalModeSubState::UpdateHeader;
                }
            }

            OpJournalModeSubState::UpdateHeader => {
                let new_mode = state
                    .op_journal_mode_state
                    .new_mode
                    .expect("new_mode should be set");
                let new_version = new_mode
                    .as_version()
                    .expect("Should be a supported Journal Mode");
                let raw_version = RawVersion::from(new_version);

                // Get the header page reference (handles both initialized and uninitialized databases)
                // This uses the pager's cache and won't fail for empty database files
                let header_ref =
                    return_if_io!(crate::storage::pager::HeaderRefMut::from_pager(pager));

                // Update the header version
                {
                    let header = header_ref.borrow_mut();
                    header.read_version = raw_version;
                    header.write_version = raw_version;
                }

                // Save the page reference for writing
                state.op_journal_mode_state.page_ref = Some(header_ref.page().clone());
                // Skip ReadPage and go directly to WritePage
                state.op_journal_mode_state.sub_state = OpJournalModeSubState::WritePage;
            }

            OpJournalModeSubState::WritePage => {
                // Write page 1 to disk to flush the header
                let page = state
                    .op_journal_mode_state
                    .page_ref
                    .as_ref()
                    .expect("page_ref should be set");
                let completion = begin_write_btree_page(pager, page)?;
                state.op_journal_mode_state.sub_state = OpJournalModeSubState::Finalize;
                return Ok(InsnFunctionStepResult::IO(IOCompletions::Single(
                    completion,
                )));
            }

            OpJournalModeSubState::Finalize => {
                let new_mode = state
                    .op_journal_mode_state
                    .new_mode
                    .expect("new_mode should be set");

                // Clear page cache
                pager.clear_page_cache(true);

                // Setup new mode
                if matches!(new_mode, journal_mode::JournalMode::ExperimentalMvcc) {
                    if program.connection.get_capture_data_changes_info().is_some() {
                        return Err(LimboError::InternalError(
                            "cannot enable MVCC while CDC is active".to_string(),
                        ));
                    }
                    let db_path = program.connection.get_database_canonical_path();
                    let mv_store = journal_mode::open_mv_store(
                        pager.io.clone(),
                        &db_path,
                        program.connection.db.open_flags,
                    )?;
                    program.connection.db.mv_store.store(Some(mv_store.clone()));
                    program.connection.demote_to_mvcc_connection();
                    mv_store.bootstrap(program.connection.clone())?;
                }

                if matches!(new_mode, journal_mode::JournalMode::Wal) {
                    program.connection.db.mv_store.store(None);
                }

                // Return result
                let ret: &'static str = new_mode.into();
                state.registers[*dest] = Register::Value(Value::build_text(ret));
                state.pc += 1;

                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_filter(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        Filter {
            cursor_id,
            target_pc,
            key_reg,
            num_keys
        },
        insn
    );
    let Some(filter) = state.get_bloom_filter(*cursor_id) else {
        // always safe to fall though, no filter present
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    };
    let contains = if *num_keys == 1 {
        // Single key optimization, avoid allocating a Vec
        let value = state.registers[*key_reg].get_value();
        if matches!(value, Value::Null) {
            // its always safe to fall through, so this *should* be `true` but
            // since it's always an equality predicate and we have a NULL value,
            // we can just short-circuit to false here.
            false
        } else {
            filter.contains_value(value)
        }
    } else {
        let values: Vec<&Value> = (0..*num_keys)
            .map(|i| state.registers[*key_reg + i].get_value())
            .collect();
        if values.iter().any(|v| matches!(*v, Value::Null)) {
            false
        } else {
            filter.contains_values(&values)
        }
    };

    if !contains {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_filter_add(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    load_insn!(
        FilterAdd {
            cursor_id,
            key_reg,
            num_keys
        },
        insn
    );

    if *num_keys == 1 {
        let reg: *const Register = &state.registers[*key_reg];
        let value = unsafe { &*reg }.get_value();
        let filter = state.get_or_create_bloom_filter(*cursor_id);
        filter.insert_value(value);
    } else {
        let values: Vec<Value> = (0..*num_keys)
            .map(|i| state.registers[*key_reg + i].get_value().clone())
            .collect();
        let filter = state.get_or_create_bloom_filter(*cursor_id);
        let value_refs: Vec<&Value> = values.iter().collect();
        filter.insert_values(&value_refs);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

fn extract_pragma_int<T>(rows: &[Vec<Value>], pragma_name: &str) -> Result<T>
where
    T: TryFrom<i64>,
{
    rows.first()
        .and_then(|row| row.first())
        .and_then(|v| match v {
            Value::Numeric(Numeric::Integer(i)) => T::try_from(*i).ok(),
            _ => None,
        })
        .ok_or_else(|| {
            LimboError::InternalError(format!("failed to read {pragma_name} from source"))
        })
}

/// Sub-states for the VACUUM INTO operation state machine.
#[derive(Default)]
pub(crate) enum OpVacuumIntoSubState {
    /// Initial state - validate preconditions and create destination database
    #[default]
    Init,
    /// Step through schema query to collect rows
    CollectSchemaRows {
        dest_conn: Arc<Connection>,
        schema_stmt: Box<crate::Statement>,
    },
    /// Prepare CREATE statement on destination (idx into schema_rows)
    PrepareDestSchema {
        dest_conn: Arc<Connection>,
        idx: usize,
    },
    /// Step through CREATE statement on destination (async)
    StepDestSchema {
        dest_conn: Arc<Connection>,
        dest_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Start copying a table - prepare column info query
    StartCopyTable {
        dest_conn: Arc<Connection>,
        table_idx: usize,
    },
    /// Collect column info for current table
    CollectColumnInfo {
        dest_conn: Arc<Connection>,
        column_stmt: Box<crate::Statement>,
        table_idx: usize,
    },
    /// Select rows from source table and insert into destination
    CopyRows {
        dest_conn: Arc<Connection>,
        select_stmt: Box<crate::Statement>,
        dest_insert_stmt: Box<crate::Statement>,
        table_idx: usize,
    },
    /// Step through INSERT statement on destination (async)
    StepDestInsert {
        dest_conn: Arc<Connection>,
        select_stmt: Box<crate::Statement>,
        dest_insert_stmt: Box<crate::Statement>,
        table_idx: usize,
    },
    /// Copy meta values (user_version, application_id) from source to destination
    CopyMetaValues { dest_conn: Arc<Connection> },
    /// Create triggers and views after data copy (to avoid triggers firing during copy)
    PrepareTriggersViews {
        dest_conn: Arc<Connection>,
        idx: usize,
    },
    /// Step through CREATE TRIGGER/VIEW statement on destination
    StepTriggersViews {
        dest_conn: Arc<Connection>,
        dest_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Operation complete
    Done { dest_conn: Arc<Connection> },
}

/// Holds the state for the VACUUM INTO operation.
#[derive(Default)]
pub(crate) struct OpVacuumIntoState {
    sub_state: OpVacuumIntoSubState,
    /// Keep dest_db alive while vacuum is in progress.
    #[allow(dead_code)]
    dest_db: Option<Arc<crate::Database>>,
    /// Schema rows: [(type, name, tbl_name, sql), ...]
    schema_rows: Vec<Vec<Value>>,
    /// Names of tables to copy data for
    table_names: Vec<String>,
    /// Column names for the current table being copied
    current_table_columns: Vec<String>,
    /// Meta values read from source database header
    source_user_version: i32,
    source_application_id: i32,
}

/// VACUUM INTO - create a compacted copy of the database at the specified path.
///
/// This is an async state machine implementation that yields on I/O operations.
/// It:
/// 1. Creates a new database at the destination path with matching page_size
/// 2. Queries sqlite_schema for all schema objects (tables, indexes, triggers, views)
/// 3. Creates tables and indexes in destination (skipping sqlite_sequence - it's
///    auto-created when AUTOINCREMENT tables are created, see translate/schema.rs)
/// 4. Copies data for each table, including sqlite_sequence to preserve AUTOINCREMENT counters
/// 5. Copies meta values (user_version, application_id) from source to destination
/// 6. Creates triggers and views last (after data copy to avoid triggers firing during copy)
pub fn op_vacuum_into(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> Result<InsnFunctionStepResult> {
    match op_vacuum_into_inner(program, state, insn) {
        Ok(InsnFunctionStepResult::Step) => {
            // Instruction complete, reset state
            state.op_vacuum_into_state = OpVacuumIntoState::default();
            Ok(InsnFunctionStepResult::Step)
        }
        Ok(InsnFunctionStepResult::IO(io)) => {
            // Waiting for I/O, keep state for resumption
            Ok(InsnFunctionStepResult::IO(io))
        }
        Ok(InsnFunctionStepResult::Done | InsnFunctionStepResult::Row) => {
            unreachable!("op_vacuum_into_inner only returns Step or IO")
        }
        Err(err) => {
            // Reset state on error
            state.op_vacuum_into_state = OpVacuumIntoState::default();
            Err(err)
        }
    }
}

fn op_vacuum_into_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
) -> Result<InsnFunctionStepResult> {
    load_insn!(VacuumInto { dest_path }, insn);

    let vacuum_state = &mut state.op_vacuum_into_state;

    loop {
        let current_sub_state = std::mem::take(&mut vacuum_state.sub_state);

        match current_sub_state {
            OpVacuumIntoSubState::Init => {
                // Check if we're in a transaction
                // as vacuum cannot be run inside a transaction
                if !program.connection.auto_commit.load(Ordering::SeqCst) {
                    return Err(LimboError::TxError(
                        "cannot VACUUM INTO from within a transaction".to_string(),
                    ));
                }

                // we always vacuum into a new file, so check if it exists
                if std::path::Path::new(dest_path).exists() {
                    return Err(LimboError::ParseError(format!(
                        "output file already exists: {dest_path}"
                    )));
                }

                // make sure to create destination database with same experimental features as source
                // Always use PlatformIO for the destination file, even if source is in-memory.
                // This ensures VACUUM INTO actually writes to disk.
                let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
                let source_db = &program.connection.db;
                let dest_opts = crate::DatabaseOpts::new()
                    .with_views(source_db.experimental_views_enabled())
                    .with_triggers(source_db.experimental_triggers_enabled())
                    .with_index_method(source_db.experimental_index_method_enabled());

                program.connection.execute("BEGIN")?;
                // lets set the same meta values as source db
                let user_version: i32 = extract_pragma_int(
                    &program.connection.pragma_query("user_version")?,
                    "user_version",
                )?;
                let application_id: i32 = extract_pragma_int(
                    &program.connection.pragma_query("application_id")?,
                    "application_id",
                )?;
                let page_size: u32 = extract_pragma_int(
                    &program.connection.pragma_query("page_size")?,
                    "page_size",
                )?;

                let reserved_space = {
                    let pager = program.connection.pager.load();
                    let reserved_space: u8 = match program.connection.get_reserved_bytes() {
                        Some(val) => val,
                        None => io.block(|| pager.with_header(|header| header.reserved_space))?,
                    };
                    reserved_space
                };

                let dest_db = crate::Database::open_file_with_flags(
                    io,
                    dest_path,
                    OpenFlags::Create,
                    dest_opts,
                    None,
                )?;
                let dest_conn = dest_db.connect()?;
                dest_conn.reset_page_size(page_size)?;
                // set reserved_space on destination to match source
                // this is important for databases using encryption or checksums
                // must be set before page 1 is allocated (before any schema operations)
                dest_conn.set_reserved_bytes(reserved_space)?;

                // Enable MVCC on destination if source has it enabled
                // Must be done before any schema operations to ensure the log file is created
                if program.connection.db.mvcc_enabled() {
                    dest_conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")?;
                }

                // Performance optimizations for destination database:
                // 1. Disable fsync - destination is a new file, if crash occurs we just delete it
                // 2. Disable foreign key checks - source data is already consistent
                // These match SQLite's vacuum.c optimizations (PAGER_SYNCHRONOUS_OFF, ~SQLITE_ForeignKeys)
                dest_conn.execute("PRAGMA synchronous = OFF")?;
                dest_conn.execute("PRAGMA foreign_keys = OFF")?;

                // Wrap all operations in a single transaction for atomicity and performance.
                // This batches all writes and ensures destination is either empty or complete.
                dest_conn.execute("BEGIN")?;

                // Exclude the MVCC metadata table from the vacuum destination — it is an
                // internal artifact of experimental_mvcc mode and must not appear in a
                // standalone SQLite file produced by VACUUM INTO.
                let schema_sql = format!(
                    "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE sql IS NOT NULL AND name <> '{}' ORDER BY CASE type WHEN 'table' THEN 1 WHEN 'index' THEN 2 WHEN 'trigger' THEN 3 WHEN 'view' THEN 4 ELSE 5 END",
                    crate::mvcc::database::MVCC_META_TABLE_NAME
                );
                let schema_stmt = program.connection.prepare(schema_sql.as_str())?;

                vacuum_state.dest_db = Some(dest_db);
                vacuum_state.source_user_version = user_version;
                vacuum_state.source_application_id = application_id;

                vacuum_state.sub_state = OpVacuumIntoSubState::CollectSchemaRows {
                    dest_conn,
                    schema_stmt: Box::new(schema_stmt),
                };
                continue;
            }

            OpVacuumIntoSubState::CollectSchemaRows {
                dest_conn,
                mut schema_stmt,
            } => {
                // Collect rows from sqlite_schema query: (type, name, tbl_name, sql)
                // These define all tables, indexes, triggers, and views to recreate in destination
                match schema_stmt.step()? {
                    crate::StepResult::Row => {
                        let row = schema_stmt
                            .row()
                            .expect("StepResult::Row but row() returned None");
                        let values: Vec<Value> = row.get_values().cloned().collect();
                        vacuum_state.schema_rows.push(values);
                        vacuum_state.sub_state = OpVacuumIntoSubState::CollectSchemaRows {
                            dest_conn,
                            schema_stmt,
                        };
                        continue;
                    }
                    crate::StepResult::Done => {
                        // Extract table names for data copy phase
                        // Include sqlite_sequence for AUTOINCREMENT counters, but not other sqlite_ tables
                        vacuum_state.table_names = vacuum_state
                            .schema_rows
                            .iter()
                            .filter_map(|row| {
                                if row.len() >= 2 {
                                    if let (Value::Text(type_val), Value::Text(name_val)) =
                                        (&row[0], &row[1])
                                    {
                                        let name = name_val.as_str();
                                        if type_val.as_str() == "table"
                                            && (!name.starts_with("sqlite_")
                                                || name == "sqlite_sequence")
                                            && name != crate::mvcc::database::MVCC_META_TABLE_NAME
                                        {
                                            return Some(name.to_string());
                                        }
                                    }
                                }
                                None
                            })
                            .collect();

                        vacuum_state.sub_state =
                            OpVacuumIntoSubState::PrepareDestSchema { dest_conn, idx: 0 };
                        continue;
                    }
                    crate::StepResult::IO => {
                        let io = schema_stmt
                            .take_io_completions()
                            .expect("StepResult::IO returned but no completions available");
                        vacuum_state.sub_state = OpVacuumIntoSubState::CollectSchemaRows {
                            dest_conn,
                            schema_stmt,
                        };
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                    crate::StepResult::Busy | crate::StepResult::Interrupt => {
                        return Err(LimboError::Busy);
                    }
                }
            }

            OpVacuumIntoSubState::PrepareDestSchema { dest_conn, idx } => {
                let schema_rows_len = vacuum_state.schema_rows.len();
                turso_assert!(
                    idx <= schema_rows_len,
                    "idx incremented past end of schema_rows",
                    { "idx": idx, "schema_rows_len": schema_rows_len }
                );
                if idx == schema_rows_len {
                    // Done creating schema, start copying data
                    vacuum_state.sub_state = OpVacuumIntoSubState::StartCopyTable {
                        dest_conn,
                        table_idx: 0,
                    };
                    continue;
                }

                let row = &vacuum_state.schema_rows[idx];
                turso_assert!(
                    row.len() == 4,
                    "schema row should have exactly 4 columns (type, name, tbl_name, sql)",
                    { "row_len": row.len() }
                );

                // Skip triggers and views - they'll be created after data copy
                // to avoid triggers firing during data copy
                if let Value::Text(type_val) = &row[0] {
                    let type_str = type_val.as_str();
                    if type_str == "trigger" || type_str == "view" {
                        vacuum_state.sub_state = OpVacuumIntoSubState::PrepareDestSchema {
                            dest_conn,
                            idx: idx + 1,
                        };
                        continue;
                    }
                }

                // Skip sqlite_sequence in schema creation phase. When we create an AUTOINCREMENT
                // table, Turso automatically creates sqlite_sequence if it doesn't exist (see
                // translate/schema.rs). Since schema_rows order depends on sqlite_schema rowids,
                // an AUTOINCREMENT table may appear before sqlite_sequence. If we create that
                // table first (which auto-creates sqlite_sequence), then later try to run
                // "CREATE TABLE sqlite_sequence(name,seq)", it fails with "table already exists".
                // We still copy sqlite_sequence data in StartCopyTable to preserve counters.
                if let Value::Text(name_val) = &row[1] {
                    if name_val.as_str() == "sqlite_sequence" {
                        vacuum_state.sub_state = OpVacuumIntoSubState::PrepareDestSchema {
                            dest_conn,
                            idx: idx + 1,
                        };
                        continue;
                    }
                }

                // Query filters WHERE sql IS NOT NULL, so sql column must be text
                let Value::Text(sql) = &row[3] else {
                    unreachable!("sql column should be text (query has WHERE sql IS NOT NULL)");
                };
                let sql_str = sql.as_str();

                // Internal tables (e.g. __turso_internal_types) have a reserved
                // name prefix that translate_create_table rejects for user SQL.
                // Temporarily mark the dest connection as nested during prepare()
                // so the reserved-name check is bypassed at compile time. We must
                // NOT keep it nested during step() because that would prevent
                // sub-statements from upgrading to write transactions.
                let is_internal = matches!(&row[1], Value::Text(n) if n.as_str().starts_with(crate::schema::TURSO_INTERNAL_PREFIX));
                if is_internal {
                    dest_conn.start_nested();
                }
                let dest_stmt = dest_conn.prepare(sql_str);
                if is_internal {
                    dest_conn.end_nested();
                }
                let dest_stmt = dest_stmt?;
                vacuum_state.sub_state = OpVacuumIntoSubState::StepDestSchema {
                    dest_conn,
                    dest_schema_stmt: Box::new(dest_stmt),
                    idx,
                };
                continue;
            }

            OpVacuumIntoSubState::StepDestSchema {
                dest_conn,
                mut dest_schema_stmt,
                idx,
            } => match dest_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    // After creating __turso_internal_types in the dest, load
                    // custom type definitions from the source so that subsequent
                    // CREATE TABLE statements for STRICT tables with custom type
                    // columns can resolve those types.
                    let row = &vacuum_state.schema_rows[idx];
                    if matches!(&row[1], Value::Text(n) if n.as_str() == crate::schema::TURSO_TYPES_TABLE_NAME)
                    {
                        let source_types: Vec<(String, std::sync::Arc<crate::schema::TypeDef>)> = {
                            let source_schema = program.connection.schema.read();
                            source_schema
                                .type_registry
                                .iter()
                                .filter(|(_, td)| !td.is_builtin)
                                .map(|(name, td)| (name.clone(), td.clone()))
                                .collect()
                        };
                        dest_conn.with_schema_mut(|dest_schema| {
                            for (name, td) in source_types {
                                dest_schema.type_registry.insert(name, td);
                            }
                        });
                    }

                    vacuum_state.sub_state = OpVacuumIntoSubState::PrepareDestSchema {
                        dest_conn,
                        idx: idx + 1,
                    };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = dest_schema_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    vacuum_state.sub_state = OpVacuumIntoSubState::StepDestSchema {
                        dest_conn,
                        dest_schema_stmt,
                        idx,
                    };
                    return Ok(InsnFunctionStepResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            OpVacuumIntoSubState::StartCopyTable {
                dest_conn,
                table_idx,
            } => {
                let table_names_len = vacuum_state.table_names.len();
                turso_assert!(
                    table_idx <= table_names_len,
                    "table_idx incremented past end of table_names",
                    { "table_idx": table_idx, "table_names_len": table_names_len }
                );
                if table_idx == table_names_len {
                    // Done copying all tables, now copy meta values
                    vacuum_state.sub_state = OpVacuumIntoSubState::CopyMetaValues { dest_conn };
                    continue;
                }

                let table_name = &vacuum_state.table_names[table_idx];
                // Escape double quotes in table name for safe SQL
                let escaped_table_name = table_name.replace('"', "\"\"");
                let pragma_sql = format!("PRAGMA table_info(\"{escaped_table_name}\")");
                let column_stmt = program.connection.prepare(&pragma_sql)?;
                vacuum_state.current_table_columns.clear();
                vacuum_state.sub_state = OpVacuumIntoSubState::CollectColumnInfo {
                    dest_conn,
                    column_stmt: Box::new(column_stmt),
                    table_idx,
                };
                continue;
            }

            OpVacuumIntoSubState::CollectColumnInfo {
                dest_conn,
                mut column_stmt,
                table_idx,
            } => {
                match column_stmt.step()? {
                    crate::StepResult::Row => {
                        let row = column_stmt
                            .row()
                            .expect("StepResult::Row but row() returned None");
                        // Column name is at index 1
                        if let Value::Text(name) = row.get_value(1) {
                            // Escape double quotes in column name for safe SQL
                            let escaped_name = name.as_str().replace('"', "\"\"");
                            let col_name = format!("\"{escaped_name}\"");
                            vacuum_state.current_table_columns.push(col_name);
                        }
                        vacuum_state.sub_state = OpVacuumIntoSubState::CollectColumnInfo {
                            dest_conn,
                            column_stmt,
                            table_idx,
                        };
                        continue;
                    }
                    crate::StepResult::Done => {
                        if vacuum_state.current_table_columns.is_empty() {
                            // if no columns, then db is corrupt
                            return Err(LimboError::Corrupt(
                                "found a table without any columns".to_string(),
                            ));
                        }

                        // Prepare SELECT and INSERT statements for this table
                        let table_name = &vacuum_state.table_names[table_idx];
                        let escaped_table_name = table_name.replace('"', "\"\"");
                        let select_sql = format!("SELECT * FROM \"{escaped_table_name}\"");
                        let select_stmt = program.connection.prepare(&select_sql)?;

                        // Prepare INSERT statement once per table (reused for all rows)
                        let column_names = vacuum_state.current_table_columns.join(", ");
                        let placeholders: String = (0..vacuum_state.current_table_columns.len())
                            .map(|_| "?")
                            .collect::<Vec<_>>()
                            .join(", ");
                        let insert_sql = format!(
                            "INSERT INTO \"{escaped_table_name}\" ({column_names}) VALUES ({placeholders})"
                        );

                        // Internal tables need nested mode to bypass "may not
                        // be modified" checks during prepare (compile time).
                        let is_internal =
                            table_name.starts_with(crate::schema::TURSO_INTERNAL_PREFIX);
                        if is_internal {
                            dest_conn.start_nested();
                        }
                        let dest_insert_stmt = dest_conn.prepare(&insert_sql);
                        if is_internal {
                            dest_conn.end_nested();
                        }
                        let dest_insert_stmt = dest_insert_stmt?;

                        vacuum_state.sub_state = OpVacuumIntoSubState::CopyRows {
                            dest_conn,
                            select_stmt: Box::new(select_stmt),
                            dest_insert_stmt: Box::new(dest_insert_stmt),
                            table_idx,
                        };
                        continue;
                    }
                    crate::StepResult::IO => {
                        let io = column_stmt
                            .take_io_completions()
                            .expect("StepResult::IO returned but no completions available");
                        vacuum_state.sub_state = OpVacuumIntoSubState::CollectColumnInfo {
                            dest_conn,
                            column_stmt,
                            table_idx,
                        };
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                    crate::StepResult::Busy | crate::StepResult::Interrupt => {
                        return Err(LimboError::Busy);
                    }
                }
            }

            OpVacuumIntoSubState::CopyRows {
                dest_conn,
                mut select_stmt,
                mut dest_insert_stmt,
                table_idx,
            } => match select_stmt.step()? {
                crate::StepResult::Row => {
                    let row = select_stmt
                        .row()
                        .expect("StepResult::Row but row() returned None");

                    let values: Vec<Value> = row.get_values().cloned().collect();

                    dest_insert_stmt.reset()?;
                    dest_insert_stmt.clear_bindings();
                    for (i, value) in values.iter().enumerate() {
                        let index =
                            std::num::NonZero::new(i + 1).expect("i + 1 is always non-zero");
                        dest_insert_stmt.bind_at(index, value.clone());
                    }

                    vacuum_state.sub_state = OpVacuumIntoSubState::StepDestInsert {
                        dest_conn,
                        select_stmt,
                        dest_insert_stmt,
                        table_idx,
                    };
                    continue;
                }
                crate::StepResult::Done => {
                    // Move to next table
                    vacuum_state.sub_state = OpVacuumIntoSubState::StartCopyTable {
                        dest_conn,
                        table_idx: table_idx + 1,
                    };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = select_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    vacuum_state.sub_state = OpVacuumIntoSubState::CopyRows {
                        dest_conn,
                        select_stmt,
                        dest_insert_stmt,
                        table_idx,
                    };
                    return Ok(InsnFunctionStepResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            OpVacuumIntoSubState::StepDestInsert {
                dest_conn,
                select_stmt,
                mut dest_insert_stmt,
                table_idx,
            } => match dest_insert_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("INSERT statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    // Go back to get next row from source
                    vacuum_state.sub_state = OpVacuumIntoSubState::CopyRows {
                        dest_conn,
                        select_stmt,
                        dest_insert_stmt,
                        table_idx,
                    };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = dest_insert_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    vacuum_state.sub_state = OpVacuumIntoSubState::StepDestInsert {
                        dest_conn,
                        select_stmt,
                        dest_insert_stmt,
                        table_idx,
                    };
                    return Ok(InsnFunctionStepResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            OpVacuumIntoSubState::CopyMetaValues { dest_conn } => {
                // Copy meta values to destination database
                // Use pragma_update to set user_version and application_id
                // Note: schema_version is not copied - VACUUM INTO creates a new file so
                // there's no cache to invalidate. The destination will have its own
                // schema_version based on the schema operations performed.
                dest_conn
                    .pragma_update("user_version", vacuum_state.source_user_version.to_string())?;
                dest_conn.pragma_update(
                    "application_id",
                    vacuum_state.source_application_id.to_string(),
                )?;

                // Now create triggers and views (after data copy to avoid triggers firing)
                vacuum_state.sub_state =
                    OpVacuumIntoSubState::PrepareTriggersViews { dest_conn, idx: 0 };
                continue;
            }

            OpVacuumIntoSubState::PrepareTriggersViews { dest_conn, idx } => {
                let schema_rows_len = vacuum_state.schema_rows.len();
                turso_assert!(
                    idx <= schema_rows_len,
                    "idx incremented past end of schema_rows",
                    { "idx": idx, "schema_rows_len": schema_rows_len }
                );
                if idx == schema_rows_len {
                    // Done creating triggers and views
                    vacuum_state.sub_state = OpVacuumIntoSubState::Done { dest_conn };
                    continue;
                }

                // We validated row.len() == 4 in PrepareDestSchema
                let row = &vacuum_state.schema_rows[idx];

                // Only process triggers and views in this phase
                if let Value::Text(type_val) = &row[0] {
                    let type_str = type_val.as_str();
                    if type_str == "trigger" || type_str == "view" {
                        if let Value::Text(sql) = &row[3] {
                            let sql_str = sql.as_str();
                            let dest_stmt = dest_conn.prepare(sql_str)?;
                            vacuum_state.sub_state = OpVacuumIntoSubState::StepTriggersViews {
                                dest_conn,
                                dest_schema_stmt: Box::new(dest_stmt),
                                idx,
                            };
                            continue;
                        }
                    }
                }

                // Skip non-trigger/view entries
                vacuum_state.sub_state = OpVacuumIntoSubState::PrepareTriggersViews {
                    dest_conn,
                    idx: idx + 1,
                };
            }

            OpVacuumIntoSubState::StepTriggersViews {
                dest_conn,
                mut dest_schema_stmt,
                idx,
            } => match dest_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE TRIGGER/VIEW statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    vacuum_state.sub_state = OpVacuumIntoSubState::PrepareTriggersViews {
                        dest_conn,
                        idx: idx + 1,
                    };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = dest_schema_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    vacuum_state.sub_state = OpVacuumIntoSubState::StepTriggersViews {
                        dest_conn,
                        dest_schema_stmt,
                        idx,
                    };
                    return Ok(InsnFunctionStepResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            OpVacuumIntoSubState::Done { dest_conn } => {
                // Commit the transaction that was started in Init state
                dest_conn.execute("COMMIT")?;
                program.connection.execute("COMMIT")?;

                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

fn with_header<T, F>(
    pager: &Pager,
    mv_store: Option<&Arc<MvStore>>,
    program: &Program,
    db: usize,
    f: F,
) -> Result<IOResult<T>>
where
    F: Fn(&DatabaseHeader) -> T,
{
    if let Some(mv_store) = mv_store {
        let tx_id = program.connection.get_mv_tx_id_for_db(db);
        mv_store.with_header(f, tx_id.as_ref()).map(IOResult::Done)
    } else {
        pager.with_header(&f)
    }
}

pub fn with_header_mut<T, F>(
    pager: &Pager,
    mv_store: Option<&Arc<MvStore>>,
    program: &Program,
    db: usize,
    f: F,
) -> Result<IOResult<T>>
where
    F: Fn(&mut DatabaseHeader) -> T,
{
    if let Some(mv_store) = mv_store {
        let tx_id = program.connection.get_mv_tx_id_for_db(db);
        mv_store
            .with_header_mut(f, tx_id.as_ref())
            .map(IOResult::Done)
    } else {
        pager.with_header_mut(&f)
    }
}

fn get_schema_cookie(
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
    program: &Program,
    db: usize,
) -> Result<IOResult<u32>> {
    if let Some(mv_store) = mv_store {
        let tx_id = program.connection.get_mv_tx_id_for_db(db);
        mv_store
            .with_header(|header| header.schema_cookie.get(), tx_id.as_ref())
            .map(IOResult::Done)
    } else {
        pager.get_schema_cookie()
    }
}

/// A root page in MVCC might still be marked as negative in schema. On restart it is automatically transformed to positive but in other cases
/// we need to map it to positive if we can in case checkpoint happened.
fn maybe_transform_root_page_to_positive(mvcc_store: Option<&Arc<MvStore>>, root_page: i64) -> i64 {
    if let Some(mvcc_store) = mvcc_store {
        if root_page < 0 {
            mvcc_store.get_real_table_id(root_page)
        } else {
            root_page
        }
    } else {
        root_page
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, DatabaseOpts, MemoryIO, IO};

    #[test]
    fn test_decr_jump_zero_non_integer_register_returns_error() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();
        let stmt = conn.prepare("SELECT 1;").unwrap();

        let mut state = ProgramState::new(1, 0);
        state.set_register(0, Register::Value(Value::Text("not-an-int".into())));

        let insn = Insn::DecrJumpZero {
            reg: 0,
            target_pc: crate::vdbe::BranchOffset::Offset(1),
        };

        let err = match op_decr_jump_zero(stmt.get_program(), &mut state, &insn, stmt.get_pager()) {
            Ok(_) => panic!("non-integer register must fail"),
            Err(err) => err,
        };
        assert!(matches!(err, LimboError::Constraint(message) if message == "datatype mismatch"));
        assert_eq!(state.pc, 0);
    }

    #[test]
    fn test_execute_sqlite_version() {
        let version_integer = 3046001;
        let expected = "3.46.1";
        assert_eq!(execute_turso_version(version_integer), expected);
    }

    #[test]
    fn test_ascii_whitespace_is_trimmed() {
        // Regular ASCII whitespace SHOULD be trimmed
        let ascii_whitespace_cases = vec![
            (" 12", 12i64),            // space
            ("12 ", 12i64),            // trailing space
            (" 12 ", 12i64),           // both sides
            ("\t42\t", 42i64),         // tab
            ("\n99\n", 99i64),         // newline
            (" \t\n123\r\n ", 123i64), // mixed ASCII whitespace
        ];

        for (input, expected_int) in ascii_whitespace_cases {
            let mut register = Register::Value(Value::Text(input.into()));
            apply_affinity_char(&mut register, Affinity::Integer);

            match register {
                Register::Value(Value::Numeric(Numeric::Integer(i))) => {
                    assert_eq!(
                        i, expected_int,
                        "String '{input}' should convert to {expected_int}, got {i}"
                    );
                }
                other => {
                    panic!(
                        "String '{input}' should be converted to integer {expected_int}, got {other:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_non_breaking_space_not_trimmed() {
        let test_strings = vec![
            ("12\u{00A0}", "text", 3),   // '12' + non-breaking space (3 chars, 4 bytes)
            ("\u{00A0}12", "text", 3),   // non-breaking space + '12' (3 chars, 4 bytes)
            ("12\u{00A0}34", "text", 5), // '12' + nbsp + '34' (5 chars, 6 bytes)
        ];

        for (input, _expected_type, expected_len) in test_strings {
            let mut register = Register::Value(Value::Text(input.into()));
            apply_affinity_char(&mut register, Affinity::Integer);

            match register {
                Register::Value(Value::Text(t)) => {
                    assert_eq!(
                        t.as_str().chars().count(),
                        expected_len,
                        "String '{input}' should have {expected_len} characters",
                    );
                }
                Register::Value(Value::Numeric(Numeric::Integer(_))) => {
                    panic!("String '{input}' should NOT be converted to integer");
                }
                other => panic!("Unexpected value type: {other:?}"),
            }
        }
    }

    #[test]
    fn test_affinity_keeps_nan_inf_text() {
        let cases = ["nan", "inf"];

        for input in cases {
            let mut register = Register::Value(Value::Text(input.into()));
            apply_affinity_char(&mut register, Affinity::Integer);
            match register {
                Register::Value(Value::Text(t)) => {
                    assert_eq!(t.as_str(), input, "Unexpected conversion for '{input}'");
                }
                other => {
                    panic!("'{input}' should remain text, got {other:?}");
                }
            }

            let mut register = Register::Value(Value::Text(input.into()));
            apply_affinity_char(&mut register, Affinity::Numeric);
            match register {
                Register::Value(Value::Text(t)) => {
                    assert_eq!(t.as_str(), input, "Unexpected conversion for '{input}'");
                }
                other => {
                    panic!("'{input}' should remain text, got {other:?}");
                }
            }
        }
    }

    #[test]
    fn test_init_agg_payload_count() {
        let mut payload = Vec::new();
        init_agg_payload(&AggFunc::Count, &mut payload).unwrap();
        assert_eq!(payload.len(), 1);
        assert_eq!(payload[0], Value::from_i64(0));
    }

    #[test]
    fn test_init_agg_payload_sum() {
        let mut payload = Vec::new();
        init_agg_payload(&AggFunc::Sum, &mut payload).unwrap();
        assert_eq!(payload.len(), 4);
        assert_eq!(payload[0], Value::Null); // acc
        assert_eq!(payload[1], Value::from_f64(0.0)); // r_err
        assert_eq!(payload[2], Value::from_i64(0)); // approx
        assert_eq!(payload[3], Value::from_i64(0)); // ovrfl
    }

    #[test]
    fn test_init_agg_payload_avg() {
        let mut payload = Vec::new();
        init_agg_payload(&AggFunc::Avg, &mut payload).unwrap();
        assert_eq!(payload.len(), 3);
        assert_eq!(payload[0], Value::from_f64(0.0)); // sum
        assert_eq!(payload[1], Value::from_f64(0.0)); // r_err
        assert_eq!(payload[2], Value::from_i64(0)); // count
    }

    #[test]
    fn test_update_count_skips_null() {
        let mut payload = vec![Value::from_i64(5)];
        update_agg_payload(
            &AggFunc::Count,
            Value::Null,
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(5)); // unchanged
    }

    #[test]
    fn test_update_count_increments() {
        let mut payload = vec![Value::from_i64(5)];
        update_agg_payload(
            &AggFunc::Count,
            Value::from_i64(42),
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(6));
    }

    #[test]
    fn test_update_sum_integers() {
        let mut payload = vec![
            Value::Null,
            Value::from_f64(0.0),
            Value::from_i64(0),
            Value::from_i64(0),
        ];
        update_agg_payload(
            &AggFunc::Sum,
            Value::from_i64(10),
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(10));

        update_agg_payload(
            &AggFunc::Sum,
            Value::from_i64(5),
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(15));
    }

    #[test]
    fn test_update_sum_null_is_skipped() {
        let mut payload = vec![
            Value::from_i64(10),
            Value::from_f64(0.0),
            Value::from_i64(0),
            Value::from_i64(0),
        ];
        update_agg_payload(
            &AggFunc::Sum,
            Value::Null,
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(10)); // unchanged
    }

    #[test]
    fn test_update_min_max() {
        let mut payload = vec![Value::Null];
        // First value sets the min/max
        update_agg_payload(
            &AggFunc::Min,
            Value::from_i64(5),
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(5));

        // Smaller value updates min
        update_agg_payload(
            &AggFunc::Min,
            Value::from_i64(3),
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(3));

        // Larger value doesn't update min
        update_agg_payload(
            &AggFunc::Min,
            Value::from_i64(10),
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(3));
    }

    #[test]
    fn test_update_avg() {
        // Payload: [sum, r_err, count]
        let mut payload = vec![
            Value::from_f64(0.0),
            Value::from_f64(0.0),
            Value::from_i64(0),
        ];
        update_agg_payload(
            &AggFunc::Avg,
            Value::from_i64(10),
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_f64(10.0));
        assert_eq!(payload[2], Value::from_i64(1));

        update_agg_payload(
            &AggFunc::Avg,
            Value::from_i64(20),
            None,
            &mut payload,
            CollationSeq::Binary,
            &None,
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_f64(30.0));
        assert_eq!(payload[2], Value::from_i64(2));
    }

    #[test]
    fn test_finalize_count() {
        let payload = vec![Value::from_i64(42)];
        let result = finalize_agg_payload(&AggFunc::Count, &payload).unwrap();
        assert_eq!(result, Value::from_i64(42));
    }

    #[test]
    fn test_finalize_avg() {
        // Payload: [sum, r_err, count]
        let payload = vec![
            Value::from_f64(30.0),
            Value::from_f64(0.0),
            Value::from_i64(3),
        ];
        let result = finalize_agg_payload(&AggFunc::Avg, &payload).unwrap();
        assert_eq!(result, Value::from_f64(10.0));
    }

    #[test]
    fn test_finalize_avg_empty() {
        // Payload: [sum, r_err, count]
        let payload = vec![
            Value::from_f64(0.0),
            Value::from_f64(0.0),
            Value::from_i64(0),
        ];
        let result = finalize_agg_payload(&AggFunc::Avg, &payload).unwrap();
        assert_eq!(result, Value::Null);
    }
}
