use turso_parser::ast::{Expr, SortOrder, TableInternalId};

use super::{
    aggregation::{translate_aggregation_step, AggArgumentSource},
    emitter::{
        MaterializedBuildInputMode, MaterializedColumnRef, OperationMode, Resolver, TranslateCtx,
        UpdateRowSource,
    },
    expr::{
        expr_references_subquery_id, translate_condition_expr, translate_expr,
        translate_expr_no_constant_opt, walk_expr, ConditionMetadata, NoConstantOptReason,
        WalkControl,
    },
    group_by::{group_by_agg_phase, GroupByMetadata, GroupByRowSource},
    optimizer::{constraints::BinaryExprSide, Optimizable},
    order_by::sorter_insert,
    plan::{
        Aggregate, DistinctCtx, Distinctness, EvalAt, HashJoinOp, HashJoinType, InSeekSource,
        IterationDirection, JoinOrderMember, JoinedTable, MultiIndexScanOp, NonFromClauseSubquery,
        Operation, QueryDestination, Scan, Search, SeekDef, SeekKey, SeekKeyComponent, SelectPlan,
        SetOperation, TableReferences, WhereTerm,
    },
};
use crate::{
    emit_explain,
    schema::{Index, IndexColumn, Table},
    translate::{
        collate::{
            get_collseq_from_expr_with_symbols, resolve_comparison_collseq_with_symbols,
            CollationSeq,
        },
        emitter::{prepare_cdc_if_necessary, HashCtx},
        planner::{table_mask_from_expr, TableMask},
        result_row::emit_select_result,
    },
    turso_assert, turso_assert_eq,
    types::SeekOp,
    vdbe::{
        affinity::{self, Affinity},
        builder::{
            CursorKey, CursorType, HashBuildSignature, MaterializedBuildInputModeTag,
            ProgramBuilder,
        },
        insn::{to_u32, CmpInsFlags, HashBuildData, IdxInsertFlags, Insn},
        BranchOffset, CursorID,
    },
    Result,
};
use std::{borrow::Cow, collections::HashSet, ops::Range, sync::Arc};
use turso_macros::turso_assert_some;

mod body;
mod close;
mod conditions;
mod hash;
mod in_seek;
mod init;
mod multi_index;
mod open;
mod seek;

use body::emit_unmatched_row_conditions_and_loop;
pub(crate) use body::LoopBodyEmitter;
pub(crate) use close::CloseLoop;
pub(crate) use close::{emit_autoindex, AutoIndexBuild, AutoIndexResult};
use in_seek::open_in_seek_source_cursor;
pub(crate) use in_seek::{
    emit_in_seek_advance, emit_in_seek_start, open_in_seek_values_cursor, InSeekLoop,
};
pub(crate) use init::{init_distinct, InitLoop};
use multi_index::emit_multi_index_scan_loop;
pub(crate) use open::OpenLoop;
pub(crate) use seek::{SeekEmitter, SeekExpressionLowering};

#[derive(Clone, Copy, Debug)]
pub struct LeftJoinMetadata {
    pub reg_match_flag: usize,
    pub label_match_flag_set_true: BranchOffset,
    pub label_match_flag_check_value: BranchOffset,
}

impl LeftJoinMetadata {
    pub(crate) fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            reg_match_flag: program.alloc_register(),
            label_match_flag_set_true: program.allocate_label(),
            label_match_flag_check_value: program.allocate_label(),
        }
    }

    pub(crate) fn reset(&self, program: &mut ProgramBuilder) {
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: self.reg_match_flag,
        });
    }

    pub(crate) fn mark_matched(&self, program: &mut ProgramBuilder) {
        program.preassign_label_to_next_insn(self.label_match_flag_set_true);
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: self.reg_match_flag,
        });
    }

    pub(crate) fn begin_unmatched_row(&self, program: &mut ProgramBuilder) -> BranchOffset {
        program.preassign_label_to_next_insn(self.label_match_flag_check_value);
        let finished = program.allocate_label();
        program.emit_insn(Insn::IfPos {
            reg: self.reg_match_flag,
            target_pc: finished,
            decrement_by: 0,
        });
        finished
    }

    pub(crate) fn finish_unmatched_row(
        &self,
        program: &mut ProgramBuilder,
        finished: BranchOffset,
    ) {
        program.emit_insn(Insn::Goto {
            target_pc: self.label_match_flag_set_true,
        });
        program.preassign_label_to_next_insn(finished);
    }
}

#[derive(Debug)]
pub struct SemiAntiJoinMetadata {
    pub label_body: BranchOffset,
    pub label_next_outer: BranchOffset,
    pub outer_table_idx: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct LoopLabels {
    pub loop_start: BranchOffset,
    pub next: BranchOffset,
    pub loop_end: BranchOffset,
}

impl LoopLabels {
    pub fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            loop_start: program.allocate_label(),
            next: program.allocate_label(),
            loop_end: program.allocate_label(),
        }
    }
}

fn find_non_semi_anti_ancestor(
    join_order: &[JoinOrderMember],
    tables: &[JoinedTable],
    join_idx: usize,
) -> usize {
    assert!(join_idx > 0, "semi/anti-join cannot be the first table");
    let mut idx = join_idx - 1;
    while idx > 0 {
        let prev = &tables[join_order[idx].original_idx];
        if !prev
            .join_info
            .as_ref()
            .is_some_and(|ji| ji.is_semi_or_anti())
        {
            break;
        }
        idx -= 1;
    }
    join_order[idx].original_idx
}
