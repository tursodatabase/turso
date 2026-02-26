use turso_parser::ast::{Expr, SortOrder, TableInternalId};

use super::{
    aggregation::{translate_aggregation_step, AggArgumentSource},
    emitter::{
        HashJoinLabels, MaterializedBuildInputMode, MaterializedColumnRef, OperationMode, Resolver,
        TranslateCtx, UpdateRowSource,
    },
    expr::{
        translate_condition_expr, translate_expr, translate_expr_no_constant_opt, walk_expr,
        ConditionMetadata, NoConstantOptReason, WalkControl,
    },
    group_by::{group_by_agg_phase, GroupByMetadata, GroupByRowSource},
    optimizer::{constraints::BinaryExprSide, Optimizable},
    order_by::{order_by_sorter_insert, sorter_insert},
    plan::{
        Aggregate, DistinctCtx, Distinctness, EvalAt, HashJoinOp, HashJoinType, IterationDirection,
        JoinOrderMember, JoinedTable, MultiIndexScanOp, NonFromClauseSubquery, Operation,
        QueryDestination, Scan, Search, SeekDef, SeekKey, SeekKeyComponent, SelectPlan,
        SetOperation, TableReferences, WhereTerm,
    },
};
use crate::{
    emit_explain,
    schema::{Index, Table},
    translate::{
        collate::{get_collseq_from_expr, resolve_comparison_collseq, CollationSeq},
        emitter::{prepare_cdc_if_necessary, HashCtx},
        expr::comparison_affinity,
        planner::{table_mask_from_expr, TableMask},
        result_row::emit_select_result,
        subquery::emit_non_from_clause_subquery,
        window::emit_window_loop_source,
    },
    turso_assert, turso_assert_eq,
    types::SeekOp,
    util::expr_tables_subset_of,
    vdbe::{
        affinity::{self, Affinity},
        builder::{
            CursorKey, CursorType, HashBuildSignature, MaterializedBuildInputModeTag,
            ProgramBuilder, QueryMode,
        },
        insn::{to_u16, CmpInsFlags, HashBuildData, IdxInsertFlags, Insn},
        BranchOffset, CursorID,
    },
    Result,
};
use std::{borrow::Cow, collections::HashSet, sync::Arc};
use turso_macros::turso_assert_some;

/// Information captured during hash table build for use in probe emission.
///
/// This describes the payload layout, key affinities, bloom filter settings,
/// and whether the probe phase may seek into the build table for missing data.
#[derive(Debug, Clone)]
pub struct HashBuildPayloadInfo {
    /// Column references stored as payload, in order.
    /// These may reference multiple tables when using a materialized join prefix.
    pub payload_columns: Vec<MaterializedColumnRef>,
    /// Affinity string for the join keys.
    pub key_affinities: String,
    /// Whether to use a bloom filter for probe-side pruning.
    pub use_bloom_filter: bool,
    /// Cursor id used to store the bloom filter.
    pub bloom_filter_cursor_id: CursorID,
    /// Whether it's safe to SeekRowid into the build table when payload is missing.
    /// This is false when keys/payload are sourced from a materialized input.
    pub allow_seek: bool,
}

mod close;
mod common;
mod emit;
mod init;
mod open;
mod predicates;
mod seek;

pub use close::close_loop;
pub use common::{LeftJoinMetadata, LoopLabels, SemiAntiJoinMetadata};
pub use emit::emit_loop;
pub use init::{init_distinct, init_loop};
pub use open::open_loop;

use common::{emit_cursor_advance, find_previous_non_semi_anti_table_idx};
use predicates::{emit_condition_with_fail_target, emit_conditions_with_subqueries};
use seek::{emit_autoindex, emit_seek, emit_seek_termination};
