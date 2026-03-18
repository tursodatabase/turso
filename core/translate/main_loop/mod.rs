use turso_parser::ast::{Expr, SortOrder, TableInternalId};

use super::{
    aggregation::{translate_aggregation_step, AggArgumentSource},
    emitter::{
        InSeekMetadata, MaterializedBuildInputMode, MaterializedColumnRef, OperationMode, Resolver,
        TranslateCtx, UpdateRowSource,
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
        collate::{get_collseq_from_expr, resolve_comparison_collseq, CollationSeq},
        emitter::{prepare_cdc_if_necessary, HashCtx},
        expr::comparison_affinity,
        planner::{table_mask_from_expr, TableMask},
        result_row::emit_select_result,
    },
    turso_assert, turso_assert_eq,
    types::SeekOp,
    util::expr_tables_subset_of,
    vdbe::{
        affinity::{self, Affinity},
        builder::{
            CursorKey, CursorType, HashBuildSignature, MaterializedBuildInputModeTag,
            ProgramBuilder,
        },
        insn::{to_u16, CmpInsFlags, HashBuildData, IdxInsertFlags, Insn},
        BranchOffset, CursorID,
    },
    Result,
};
use std::{borrow::Cow, collections::HashSet, sync::Arc};
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
use close::{emit_autoindex, AutoIndexResult};
use in_seek::open_in_seek_source_cursor;
pub(crate) use init::{init_distinct, InitLoop};
use multi_index::emit_multi_index_scan_loop;
pub(crate) use open::OpenLoop;
use seek::SeekEmitter;

#[derive(Debug)]
pub struct LeftJoinMetadata {
    pub reg_match_flag: usize,
    pub label_match_flag_set_true: BranchOffset,
    pub label_match_flag_check_value: BranchOffset,
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
