//! Runtime destination preparation for resolved HIR queries.

use crate::alloc::TursoIteratorExt;
use std::ops::ControlFlow;
use turso_parser::ast::{CompoundOperator, Distinctness, Literal, NullsOrder, SortOrder};

use crate::translate::collate::CollationSeq;
use crate::{
    emit_explain,
    function::{AccumulatorFunc, AggFunc, Func, ScalarFunc},
    schema::{
        BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, PseudoCursorType,
        Table, Type,
    },
    sync::Arc,
    translate::{
        aggregation::{
            hir_aggregate_function, translate_hir_aggregation_step, HirAggregateDistinct,
        },
        eqp::{
            seek_constraint_parts, EqpCompoundOp, EqpDetail, EqpIndex, EqpJoin, EqpRowSource,
            EqpSearchKind, EqpSortMethod, EqpSubquery, EqpSubqueryExec, EqpTable,
        },
        expr::ConditionMetadata,
        main_loop::{
            emit_autoindex, emit_in_seek_advance, emit_in_seek_start, open_in_seek_values_cursor,
            AutoIndexBuild, InSeekLoop, LeftJoinMetadata, SeekEmitter, SeekExpressionLowering,
        },
        optimizer::{HirBtreeOperation, HirInSeekSource, HirVirtualTableOperation},
        order_by::{custom_type_comparator_from_type_fact, sorter_insert},
        plan::{
            self, EphemeralRowidMode, HirCteMaterialization, HirSeekDef, IterationDirection,
            QueryDestination,
        },
        recursive_cte::{
            emit_recursive_runtime, RecursivePhase, RecursiveRuntimeLimit, RecursiveRuntimeOrder,
            RecursiveRuntimeSpec,
        },
        result_row::emit_columns_to_destination,
        semantic::hir::{self, HirDocument, QueryId, SourceId, SubqueryExpr},
        semantic_to_plan::{
            BtreeTableLookup, HirHashBuildInput, HirHashLookup, HirPlan, HirPlannedLoop,
            HirQueryBlockPlan, HirSourceAccess,
        },
        window::{
            WindowBufferInput, WindowBufferRuntime, WindowFrameOffsets, WindowFrameTracking,
            WindowFunctionRuntime, WindowInputState, WindowLabels, WindowPeerState,
        },
    },
    types::KeyInfo,
    util::parse_numeric_literal,
    vdbe::{
        affinity::Affinity,
        builder::{
            CteFillSubroutine, CursorType, MaterializedCteInfo, ProgramBuilder, SourceBinding,
            SubqueryBinding,
        },
        insn::{to_u32, HashDistinctData, InsertFlags, Insn, SorterOpenData},
        BranchOffset, CursorID,
    },
    LimboError, Numeric, Result, Value,
};

/// Physical destination and execution facts for one expression subquery.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct PreparedSubquery {
    pub(crate) query: QueryId,
    pub(crate) destination: QueryDestination,
    pub(crate) correlated: bool,
}

struct DestinationBinding {
    destination: QueryDestination,
    binding: SubqueryBinding,
}

#[derive(Clone, Copy)]
struct QueryLimitRegisters {
    limit: usize,
    offset: Option<usize>,
}

struct HirDistinctOutput {
    hash_table: usize,
    collations: Vec<CollationSeq>,
}

#[derive(Clone, Copy)]
enum HirSortKeySource {
    Output(usize),
    EncodedOutput(usize),
    Expression,
}

struct HirSorterOutput<'a> {
    cursor: CursorID,
    record: usize,
    row: usize,
    column_count: usize,
    order_by: &'a [hir::OrderTerm],
    key_sources: Vec<HirSortKeySource>,
    output_columns: Vec<usize>,
}

enum HirRowTarget<'a> {
    Direct {
        destination: &'a QueryDestination,
        limit: Option<QueryLimitRegisters>,
        done: BranchOffset,
    },
    Sorter(&'a HirSorterOutput<'a>),
    /// The block's rows feed a window; its output subroutine produces the
    /// SELECT outputs later.
    Window(&'a HirWindowLayer<'a>),
}

struct HirRowOutput<'a> {
    target: HirRowTarget<'a>,
    distinct: Option<&'a HirDistinctOutput>,
}

/// Values supplied by a buffered row. Sort inputs are prepared separately because
/// custom ORDER BY keys can need encoded values while SELECT needs decoded ones.
#[derive(Default)]
struct HirRowInputs<'a> {
    values: &'a [super::expr::ExprRegisterInput<'a>],
    sort_keys: &'a [super::expr::ExprRegisterInput<'a>],
}

/// Each terminal keeps its exact HIR node identity, even when equivalent nodes
/// share a buffer column. Scalars above these terminals still run at output time.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Default)]
struct HirWindowOutputPlan<'a> {
    columns: Vec<usize>,
    values: Vec<(&'a hir::Expr, usize)>,
    sort_keys: Vec<(&'a hir::Expr, usize)>,
}

#[cfg_attr(not(test), allow(dead_code))]
struct HirWindowOutputRegisters<'a> {
    start: usize,
    values: Vec<super::expr::ExprRegisterInput<'a>>,
    sort_keys: Vec<super::expr::ExprRegisterInput<'a>>,
}

impl<'a> HirWindowOutputPlan<'a> {
    /// Row-result preparation reads `columns` into this contiguous register range.
    #[cfg_attr(not(test), allow(dead_code))]
    fn prepare_registers(&self, program: &mut ProgramBuilder) -> HirWindowOutputRegisters<'a> {
        let start = program.alloc_registers(self.columns.len());
        let bind = |entries: &[(&'a hir::Expr, usize)]| {
            entries
                .iter()
                .map(|(expr, slot)| super::expr::ExprRegisterInput::new(expr, start + slot))
                .collect()
        };
        HirWindowOutputRegisters {
            start,
            values: bind(&self.values),
            sort_keys: bind(&self.sort_keys),
        }
    }
}

impl HirWindowOutputRegisters<'_> {
    #[cfg_attr(not(test), allow(dead_code))]
    fn row_inputs(&self) -> HirRowInputs<'_> {
        HirRowInputs {
            values: &self.values,
            sort_keys: &self.sort_keys,
        }
    }
}

struct HirWindowOutputCollector<'plan, 'expr> {
    buffer: &'plan mut super::window::HirWindowBufferPlan<'expr>,
    columns: &'plan mut Vec<usize>,
    inputs: &'plan mut Vec<(&'expr hir::Expr, usize)>,
    encoded: bool,
}

impl HirWindowOutputCollector<'_, '_> {
    fn is_terminal(expression: &hir::Expr) -> bool {
        matches!(
            expression,
            hir::Expr::Column(_) | hir::Expr::RowId(_) | hir::Expr::Subquery(_)
        ) || matches!(
            expression,
            hir::Expr::Function(call)
                if matches!(
                    call.evaluation,
                    hir::FunctionEvaluation::Aggregate { .. }
                        | hir::FunctionEvaluation::Window { .. }
                )
        )
    }
}

impl<'expr> hir::ExprVisitor<'expr> for HirWindowOutputCollector<'_, 'expr> {
    type Context = ();
    type Output = ();
    type Error = LimboError;

    fn child(&mut self, expression: &'expr hir::Expr, index: usize) -> Option<&'expr hir::Expr> {
        if Self::is_terminal(expression) {
            None
        } else {
            expression.child(index)
        }
    }

    fn pre_order(
        &mut self,
        _parent: &'expr hir::Expr,
        _context: &mut (),
        _index: usize,
        _child: &'expr hir::Expr,
    ) -> Result<std::ops::ControlFlow<(), ()>> {
        Ok(std::ops::ControlFlow::Continue(()))
    }

    fn post_order(
        &mut self,
        expression: &'expr hir::Expr,
        _context: (),
        _children: &[()],
    ) -> Result<()> {
        if !Self::is_terminal(expression) {
            return Ok(());
        }
        if let hir::Expr::Function(call) = expression {
            if let hir::FunctionEvaluation::Window { window, .. } = &call.evaluation {
                if *window == self.buffer.window.id {
                    return Ok(());
                }
            }
        }
        let column =
            super::window::push_hir_window_output_column(self.buffer, expression, self.encoded);
        let slot = self
            .columns
            .iter()
            .position(|existing| *existing == column)
            .unwrap_or_else(|| {
                let slot = self.columns.len();
                self.columns.push(column);
                slot
            });
        self.inputs.push((expression, slot));
        Ok(())
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn plan_hir_window_output_inputs<'a>(
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
    buffer: &mut super::window::HirWindowBufferPlan<'a>,
) -> Result<HirWindowOutputPlan<'a>> {
    if buffer.window.id.block != block.id {
        return Err(LimboError::InternalError(
            "HIR window output belongs to a different query block".into(),
        ));
    }
    let mut plan = HirWindowOutputPlan::default();
    for output in &block.outputs {
        output.expr.walk(
            (),
            &mut HirWindowOutputCollector {
                buffer,
                columns: &mut plan.columns,
                inputs: &mut plan.values,
                encoded: false,
            },
        )?;
    }
    for term in &query.order_by {
        let encoded = term
            .type_fact
            .declared
            .as_ref()
            .and_then(|declared| declared.custom())
            .is_some_and(|custom| custom.value().decode().is_some());
        let expression = match hir_order_output_position(block, &term.expr) {
            Some(output) if encoded => &block.outputs[output].expr,
            Some(_) => continue, // The sorter copies an already evaluated SELECT output.
            None => &term.expr,
        };
        expression.walk(
            (),
            &mut HirWindowOutputCollector {
                buffer,
                columns: &mut plan.columns,
                inputs: &mut plan.sort_keys,
                encoded,
            },
        )?;
    }
    Ok(plan)
}

/// Plan what an inner window layer stores for the next outer layer: every
/// value the outer layer's buffer columns and stored-value sort keys read.
fn plan_hir_window_layer_inputs<'a>(
    outer: &super::window::HirWindowBufferPlan<'a>,
    outer_keys: &[super::window::HirWindowInputKey<'a>],
    buffer: &mut super::window::HirWindowBufferPlan<'a>,
) -> Result<HirWindowOutputPlan<'a>> {
    let mut plan = HirWindowOutputPlan::default();
    let encoded_keys = outer_keys
        .iter()
        .filter(|key| key.is_encoded())
        .map(|key| (key.expression, true));
    for (expression, encoded) in outer.column_expressions().chain(encoded_keys) {
        expression.walk(
            (),
            &mut HirWindowOutputCollector {
                buffer,
                columns: &mut plan.columns,
                inputs: if encoded {
                    &mut plan.sort_keys
                } else {
                    &mut plan.values
                },
                encoded,
            },
        )?;
    }
    Ok(plan)
}

/// The seen-values set of a DISTINCT aggregate. Each emission of the step
/// gets its own skip label, because a RIGHT or FULL join emits the loop body
/// twice.
struct HirAggregateDistinctSet {
    hash_table: usize,
    collation: CollationSeq,
}

struct HirAggregateRuntime<'a> {
    call: &'a hir::FunctionCall,
    accumulator: usize,
    distinct: Option<HirAggregateDistinctSet>,
    percentile_fraction: Option<usize>,
    ordered: Option<HirOrderedArguments>,
}

/// The rows of an aggregate with an argument ORDER BY, as SQLite keeps them:
/// an ephemeral index of each row's ORDER BY values, a sequence number that
/// keeps tied rows in the order they came, and the arguments. The aggregate
/// steps through it in order when it is finalized.
struct HirOrderedArguments {
    cursor: CursorID,
    terms: usize,
    values: usize,
    sequence: usize,
    distinct_on_order_key: bool,
}

struct HirAggregateState<'a> {
    aggregates: Vec<HirAggregateRuntime<'a>>,
    bare_columns: Vec<hir::ColumnRef>,
    bare_rowids: Vec<SourceId>,
    min_max_row: Option<HirMinMaxRow>,
}

/// SQLite's bare-column rule: in a query with exactly one min() or max()
/// aggregate, bare columns come from a row that holds the aggregate's
/// value. A row is kept when the value changes; NULL to a value counts, a
/// tie or a value equal under the collation does not.
struct HirMinMaxRow {
    accumulator: usize,
    function: AggFunc,
    collation: CollationSeq,
    /// The aggregate's value when a row was last kept.
    previous: usize,
}

enum HirLoopBody<'a> {
    /// Send the block's outputs to a query output.
    Rows(&'a HirRowOutput<'a>, &'a [hir::Output]),
    /// Step the block's aggregates. With a label, the block is a MIN or MAX
    /// whose loop reads its argument in order, so the first non-NULL
    /// argument ends the scan at that label.
    UngroupedAggregate(&'a HirAggregateState<'a>, Option<BranchOffset>),
    Group(&'a HirGroupRuntime<'a>),
    /// Run DML code for each joined row; it continues at the given label to
    /// reject the row. It may be emitted more than once.
    Dml(&'a dyn Fn(&mut ProgramBuilder, BranchOffset) -> Result<()>),
}

/// The loops of one FROM clause and the terms they check: a query block's,
/// or an UPDATE ... FROM's.
#[derive(Clone, Copy)]
pub(super) struct HirLoopsPlan<'a> {
    groups: &'a [crate::translate::plan::HirFromGroupBoundary],
    loops: &'a [HirPlannedLoop],
    predicates: &'a [crate::translate::plan::HirWhereTerm],
    /// Index of the first WHERE term; earlier terms come from the FROM clause.
    where_start: usize,
    /// See [HirQueryBlockPlan::later_on_terms].
    later_on_terms: &'a [(SourceId, usize)],
    /// LEFT JOINs the plan runs as inner joins.
    inner_joins: &'a [SourceId],
    /// See [HirAccessPlan::deferred_predicates].
    deferred_predicates: &'a [(SourceId, usize)],
    /// These loops fill a hash lookup's build input: they are the loops
    /// before that lookup's probe, so the outer joins of later loops are
    /// not theirs.
    build_input: bool,
}

impl<'a> HirLoopsPlan<'a> {
    fn of_block(plan: &'a HirQueryBlockPlan) -> Self {
        Self {
            groups: &plan.groups,
            loops: &plan.loops,
            predicates: &plan.predicates,
            where_start: plan.where_start,
            later_on_terms: &plan.later_on_terms,
            inner_joins: &plan.inner_joins,
            deferred_predicates: &plan.deferred_predicates,
            build_input: false,
        }
    }

    pub(super) fn of_access(plan: &'a crate::translate::semantic_to_plan::HirAccessPlan) -> Self {
        Self {
            groups: &plan.groups,
            loops: &plan.loops,
            predicates: &plan.predicates,
            where_start: plan.where_start,
            later_on_terms: &plan.later_on_terms,
            inner_joins: &[],
            deferred_predicates: &plan.deferred_predicates,
            build_input: false,
        }
    }
}

struct HirAggregateCapture {
    source: SourceId,
    columns_start: usize,
    columns: Vec<usize>,
    rowid: Option<usize>,
}

struct HirGroupRuntime<'a> {
    grouping: &'a hir::Grouping,
    aggregates: HirAggregateState<'a>,
    rows: HirGroupRows,
    inputs: Vec<HirGroupInput<'a>>,
    sources: Vec<HirGroupSource>,
    state: HirGroupState,
}

enum HirGroupRows {
    Sorter(HirGroupSorter),
    InputOrdered,
}

struct HirGroupSorter {
    cursor: CursorID,
    pseudo_cursor: CursorID,
    record: usize,
    inputs_start: usize,
}

struct HirGroupState {
    output_group: BranchOffset,
    output_done: BranchOffset,
    finalize_group: BranchOffset,
    clear_group: BranchOffset,
    group_end: BranchOffset,
    output_return: usize,
    clear_return: usize,
    data_in_group: usize,
    previous_keys: usize,
    current_keys: usize,
}

enum HirGroupInput<'a> {
    Key(usize),
    Column {
        reference: hir::ColumnRef,
        row_register: usize,
        group_register: usize,
    },
    RowId {
        source: SourceId,
        row_register: usize,
        group_register: usize,
    },
    /// An expression the loop's index stores, such as `b->>'x'` for an
    /// index on `t(b->>'x')`, with every place the block reads it. The group
    /// row keeps the stored value once, as SQLite does, instead of the
    /// columns the expression reads.
    Expression {
        expressions: Vec<&'a hir::Expr>,
        row_register: usize,
        group_register: usize,
    },
}

struct HirGroupSource {
    source: SourceId,
    row_start: usize,
    group_start: usize,
    rowid: Option<HirGroupRowId>,
}

struct HirGroupRowId {
    row_register: usize,
    group_register: usize,
}

#[derive(Clone, Copy)]
enum HirGroupSourceValues {
    Row,
    Group,
}

impl HirRowOutput<'_> {
    fn needs_values(&self) -> bool {
        self.distinct.is_some()
            || match self.target {
                HirRowTarget::Direct { destination, .. } => {
                    !matches!(destination, QueryDestination::ExistsSubqueryResult { .. })
                }
                HirRowTarget::Sorter(_) => true,
                HirRowTarget::Window(_) => false,
            }
    }
}

fn open_hir_in_seek_source_cursor(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    index: Option<&Arc<Index>>,
    source: &HirInSeekSource,
) -> Result<CursorID> {
    match source {
        HirInSeekSource::Values { values, affinity } => {
            // A list that reads a column changes with the row of that
            // column's loop, as in a LATERAL subquery or a correlated
            // subquery, so it is filled again each time the loop starts.
            let mut reads_sources = false;
            for value in values {
                value.for_each(&mut |expression| {
                    if matches!(expression, hir::Expr::Column(_) | hir::Expr::RowId(_)) {
                        reads_sources = true;
                    }
                });
            }
            open_in_seek_values_cursor(
                program,
                index,
                values,
                *affinity,
                !reads_sources,
                |program, expression, target| {
                    super::expr::translate_expr_no_constant_opt(
                        program, document, expression, target,
                    )
                    .map(|_| ())
                },
            )
        }
        HirInSeekSource::Query(subquery) => {
            if let Some(plan) = program.hir_plan() {
                emit_expression_subquery(program, &plan, subquery)?;
            }
            let SubqueryExpr::In { query, .. } = subquery else {
                return Err(LimboError::InternalError(
                    "HIR IN seek source is not an IN subquery".into(),
                ));
            };
            let Some(SubqueryBinding::InIndex { cursor }) = program.subquery_binding(*query) else {
                return Err(LimboError::InternalError(format!(
                    "HIR IN seek query {query} has no index runtime binding"
                )));
            };
            Ok(cursor)
        }
    }
}

/// Whether no outer join can give `source` a NULL row: no LEFT, RIGHT or
/// FULL JOIN in its block's FROM, or in a parenthesized group holding it,
/// can NULL-extend it.
fn source_is_never_null_extended(document: &HirDocument, source: SourceId) -> bool {
    let Some(hir::SourceOwner::QueryBlock(block)) =
        document.source(source).map(|source| source.owner)
    else {
        return false;
    };
    let Some(from) = document
        .query(block.query)
        .and_then(|query| query.blocks.get(block.index))
        .and_then(|block| block.from.as_ref())
    else {
        return false;
    };
    from_never_null_extends(document, from, source) == Some(true)
}

/// Whether `from` holds `source`, directly or inside a parenthesized group,
/// and no outer join in it can NULL-extend `source`. None when `from` does
/// not hold `source`.
fn from_never_null_extends(
    document: &HirDocument,
    from: &hir::From,
    source: SourceId,
) -> Option<bool> {
    let members = std::iter::once(from.first).chain(from.joins.iter().map(|join| join.right));
    for member in members {
        let never_null_here = !from.outer_join_may_null_extend(member);
        if member == source {
            return Some(never_null_here);
        }
        let Some(hir::SourceKind::FromGroup(group)) =
            document.source(member).map(|member| &member.kind)
        else {
            continue;
        };
        if let Some(never_null_inside) = from_never_null_extends(document, &group.from, source) {
            return Some(never_null_here && never_null_inside);
        }
    }
    None
}

struct HirSeekExpressionLowering<'a> {
    document: &'a HirDocument,
    source: &'a hir::Source,
}

impl SeekExpressionLowering<hir::Expr> for HirSeekExpressionLowering<'_> {
    fn emit_expression(
        &mut self,
        program: &mut ProgramBuilder,
        expression: &hir::Expr,
        target: usize,
    ) -> Result<()> {
        super::expr::translate_expr_no_constant_opt(program, self.document, expression, target)?;
        Ok(())
    }

    fn is_nonnull(&self, expression: &hir::Expr) -> bool {
        match expression {
            hir::Expr::Literal(
                Literal::Numeric(_)
                | Literal::String(_)
                | Literal::Blob(_)
                | Literal::True
                | Literal::False,
            ) => true,
            hir::Expr::RowId(source) => source_is_never_null_extended(self.document, *source),
            hir::Expr::Column(column) => {
                let Some(source) = self.document.source(column.source) else {
                    return false;
                };
                let rowid_alias = source
                    .columns
                    .get(column.column)
                    .is_some_and(|column| column.rowid_alias);
                let not_null = match &source.kind {
                    hir::SourceKind::Table(table) => table
                        .value()
                        .columns()
                        .get(column.column)
                        .is_some_and(|column| column.notnull()),
                    _ => false,
                };
                (rowid_alias || not_null)
                    && source_is_never_null_extended(self.document, column.source)
            }
            _ => false,
        }
    }

    fn is_null_matching(
        &self,
        operator: turso_parser::ast::Operator,
        expression: &hir::Expr,
    ) -> bool {
        operator == turso_parser::ast::Operator::Is && !self.is_nonnull(expression)
    }

    fn affinity(&self, affinity: Affinity, expression: &hir::Expr) -> Affinity {
        // Skip the affinity a key value already has: a number under numeric
        // affinity, text under TEXT, any blob, and a rowid.
        let unchanged = match expression {
            hir::Expr::Literal(Literal::Numeric(_)) => affinity.is_numeric(),
            hir::Expr::Literal(Literal::String(_)) => affinity == Affinity::Text,
            hir::Expr::Literal(Literal::Blob(_)) => true,
            hir::Expr::RowId(_) => affinity.is_numeric(),
            hir::Expr::Column(column) => {
                affinity.is_numeric()
                    && self
                        .document
                        .source(column.source)
                        .and_then(|source| source.columns.get(column.column))
                        .is_some_and(|column| column.rowid_alias)
            }
            _ => false,
        };
        if unchanged || matches!(affinity, Affinity::Blob | Affinity::None) {
            Affinity::Blob
        } else {
            affinity
        }
    }

    fn encode_index_keys(
        &mut self,
        program: &mut ProgramBuilder,
        index: &Arc<Index>,
        start_reg: usize,
        num_keys: usize,
        index_column_offset: usize,
    ) -> Result<()> {
        for position in 0..num_keys {
            let Some(index_column) = index.columns.get(index_column_offset + position) else {
                break;
            };
            let Some(programs) = self
                .source
                .column_type_programs
                .get(index_column.pos_in_table)
                .and_then(Option::as_ref)
            else {
                continue;
            };
            // A virtual column's index holds its computed value, which is
            // not encoded (see `column_child`).
            let computed = matches!(
                self.source
                    .generated_expressions
                    .get(index_column.pos_in_table),
                Some(hir::ColumnReadExpression::Planned(_))
            );
            if programs.encode.is_empty() || computed {
                continue;
            }
            let register = start_reg + position;
            let encoded = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg: register,
                target_pc: encoded,
            });
            super::expr::translate_schema_calls_in_place(
                program,
                self.document,
                &programs.encode,
                register,
            )?;
            program.preassign_label_to_next_insn(encoded);
        }
        Ok(())
    }

    fn rowid_affinity(&self) -> Affinity {
        self.source
            .columns
            .iter()
            .find(|column| column.rowid_alias)
            .map_or(Affinity::Numeric, |column| column.affinity)
    }
}

/// Allocate the query destination from facts already frozen in HIR.
///
/// The matching expression binding is installed at the same time so query
/// emission and expression lowering cannot choose different storage.
pub(crate) fn prepare_subquery(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &SubqueryExpr,
) -> Result<PreparedSubquery> {
    let query_id = match expression {
        SubqueryExpr::Scalar { query, .. }
        | SubqueryExpr::Row { query }
        | SubqueryExpr::In { query, .. }
        | SubqueryExpr::Exists(query) => *query,
    };
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR subquery destination references missing query {query_id}"
        ))
    })?;
    let width = query.output.len();
    if width == 0 {
        return Err(LimboError::InternalError(format!(
            "HIR subquery {query_id} has no outputs"
        )));
    }

    let prepared = match expression {
        SubqueryExpr::Scalar { output, .. } => {
            if *output >= width {
                return Err(LimboError::InternalError(format!(
                    "HIR scalar subquery {query_id} output {output} exceeds query width {width}"
                )));
            }
            row_value_destination(program, width)
        }
        SubqueryExpr::Row { .. } => row_value_destination(program, width),
        SubqueryExpr::Exists(_) => {
            let register = program.alloc_register();
            DestinationBinding {
                destination: QueryDestination::ExistsSubqueryResult {
                    result_reg: register,
                },
                binding: SubqueryBinding::Exists { register },
            }
        }
        SubqueryExpr::In { comparison, .. } => {
            if comparison.components.len() != width {
                return Err(LimboError::InternalError(format!(
                    "HIR IN subquery {query_id} comparison width {} does not match query width {width}",
                    comparison.components.len()
                )));
            }
            let columns = query
                .output
                .iter()
                .zip(&comparison.components)
                .enumerate()
                .map(|(position, (output, component))| {
                    let output = document.output(*output).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR IN subquery {query_id} references missing output {output:?}"
                        ))
                    })?;
                    Ok(IndexColumn {
                        name: output.name.clone(),
                        order: SortOrder::Asc,
                        nulls_order: None,
                        pos_in_table: position,
                        collation: component
                            .collation
                            .as_ref()
                            .map(|collation| *collation.value()),
                        default: None,
                        expr: None,
                    })
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .collect_vec();
            let affinity_str = comparison
                .components
                .iter()
                .map(|component| component.affinity.aff_mask())
                .collect::<String>();
            let index = Arc::new(Index {
                constraint_name: None,
                columns,
                name: format!("ephemeral_index_hir_sub_{}", query_id.index()),
                table_name: String::new(),
                root_page: 0,
                unique: false,
                ephemeral: true,
                has_rowid: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            });
            let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            DestinationBinding {
                destination: QueryDestination::EphemeralIndex {
                    cursor_id: cursor,
                    index,
                    affinity_str: Some(Arc::new(affinity_str)),
                    is_delete: false,
                },
                binding: SubqueryBinding::InIndex { cursor },
            }
        }
    };

    program.bind_subquery(query_id, prepared.binding);
    Ok(PreparedSubquery {
        query: query_id,
        destination: prepared.destination,
        correlated: document.reads_outer_sources(query_id) || program.reruns_subquery(query_id),
    })
}

/// Emit the execution shell shared by all non-FROM expression subqueries.
pub(crate) fn emit_prepared_subquery(
    program: &mut ProgramBuilder,
    prepared: &PreparedSubquery,
    emit_body: impl FnOnce(&mut ProgramBuilder, QueryId, &QueryDestination) -> Result<()>,
) -> Result<()> {
    program.nested(|program| {
        let explain_id = program.next_subquery_eqp_id();
        match &prepared.destination {
            QueryDestination::ExistsSubqueryResult { .. } => {}
            QueryDestination::RowValueSubqueryResult { .. } => {
                crate::emit_explain!(
                    program,
                    true,
                    EqpDetail::ScalarSubquery {
                        id: explain_id,
                        correlated: prepared.correlated,
                    }
                );
            }
            QueryDestination::EphemeralIndex { .. } => {
                crate::emit_explain!(
                    program,
                    true,
                    EqpDetail::ListSubquery {
                        id: explain_id,
                        correlated: prepared.correlated,
                    }
                );
            }
            destination => {
                return Err(LimboError::InternalError(format!(
                    "HIR expression subquery has invalid destination {destination:?}"
                )));
            }
        }

        let once_done = if prepared.correlated {
            None
        } else {
            let done = program.allocate_label();
            program.emit_insn(Insn::Once {
                target_pc_when_reentered: done,
            });
            Some(done)
        };

        match &prepared.destination {
            QueryDestination::ExistsSubqueryResult { result_reg } => {
                let return_register = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: return_register,
                    dest_end: None,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
                emit_body(program, prepared.query, &prepared.destination)?;
                program.emit_insn(Insn::Return {
                    return_reg: return_register,
                    can_fallthrough: true,
                });
            }
            QueryDestination::RowValueSubqueryResult {
                result_reg_start,
                num_regs,
            } => {
                let return_register = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: return_register,
                    dest_end: None,
                });
                for register in *result_reg_start..*result_reg_start + *num_regs {
                    program.emit_insn(Insn::Null {
                        dest: register,
                        dest_end: None,
                    });
                }
                emit_body(program, prepared.query, &prepared.destination)?;
                program.emit_insn(Insn::Return {
                    return_reg: return_register,
                    can_fallthrough: true,
                });
            }
            QueryDestination::EphemeralIndex { cursor_id, .. } => {
                program.emit_insn(Insn::OpenEphemeral {
                    cursor_id: *cursor_id,
                    is_table: false,
                });
                emit_body(program, prepared.query, &prepared.destination)?;
            }
            _ => unreachable!("expression subquery destination was checked"),
        }

        if !matches!(
            prepared.destination,
            QueryDestination::ExistsSubqueryResult { .. }
        ) {
            program.pop_current_parent_explain();
        }
        if let Some(once_done) = once_done {
            program.preassign_label_to_next_insn(once_done);
        }
        Ok(())
    })
}

/// Run one expression subquery from the plan and bind its result for the
/// expression reading it. Like SQLite, the subquery is coded where its value
/// is needed: an uncorrelated one runs once, a correlated one on every use.
pub(crate) fn emit_expression_subquery(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    expression: &SubqueryExpr,
) -> Result<()> {
    let prepared = prepare_subquery(program, &plan.document, expression)?;
    let cache_keys = plan
        .planned_query(prepared.query)
        .map(|planned| planned.result_cache_keys.as_slice())
        .unwrap_or_default();
    if let QueryDestination::RowValueSubqueryResult {
        result_reg_start,
        num_regs,
    } = prepared.destination
    {
        if prepared.correlated && !cache_keys.is_empty() && !program.reruns_subquery(prepared.query)
        {
            return emit_cached_subquery(
                program,
                plan,
                &prepared,
                cache_keys,
                result_reg_start,
                num_regs,
            );
        }
    }
    emit_prepared_subquery(program, &prepared, |program, query, destination| {
        emit_planned_query_body(program, plan, query, destination)
    })
}

/// Run a correlated subquery through a cache of its results, keyed by the
/// outer values it reads. A run whose values were seen before reads the
/// stored result; any other run runs the subquery and stores its result.
///
/// Each value is stored with its `typeof()`, because an index treats 1 and
/// 1.0 as equal while the subquery may not, as in `t.text_column = o.value`.
/// Every key column matches NULL, since a NULL outer value is a key like any
/// other.
fn emit_cached_subquery(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    prepared: &PreparedSubquery,
    keys: &[hir::Expr],
    result_start: usize,
    width: usize,
) -> Result<()> {
    let key_width = keys.len() * 2;
    let columns = (0..key_width + width)
        .map(|position| IndexColumn {
            name: format!("c{position}"),
            order: SortOrder::Asc,
            nulls_order: None,
            pos_in_table: position,
            collation: None,
            default: None,
            expr: None,
        })
        .collect_vec();
    let index = Arc::new(Index {
        constraint_name: None,
        columns,
        name: format!("ephemeral_subquery_cache_{}", prepared.query.index()),
        table_name: String::new(),
        root_page: 0,
        unique: false,
        ephemeral: true,
        has_rowid: false,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index));
    let opened = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: opened,
    });
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: false,
    });
    program.preassign_label_to_next_insn(opened);

    let record_start = program.alloc_registers(key_width + width);
    for (position, key) in keys.iter().enumerate() {
        let value = record_start + position * 2;
        super::expr::translate_expr_no_constant_opt(program, &plan.document, key, value)?;
        program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: value,
            dest: value + 1,
            func: crate::function::FuncCtx {
                func: crate::function::Func::Scalar(crate::function::ScalarFunc::Typeof),
                arg_count: 1,
            },
        });
    }
    let mut every_key = crate::translate::plan::BitSet::default();
    for position in 0..key_width {
        every_key.set(position)?;
    }

    let miss = program.allocate_label();
    let done = program.allocate_label();
    program.emit_insn(Insn::SeekGE {
        is_index: true,
        cursor_id: cursor,
        start_reg: record_start,
        num_regs: key_width,
        target_pc: miss,
        eq_only: true,
        null_matching_mask: every_key.into(),
    });
    program.emit_insn(Insn::IdxGT {
        cursor_id: cursor,
        start_reg: record_start,
        num_regs: key_width,
        target_pc: miss,
    });
    for position in 0..width {
        program.emit_insn(Insn::Column {
            cursor_id: cursor,
            column: key_width + position,
            dest: result_start + position,
            default: None,
        });
    }
    program.emit_insn(Insn::Goto { target_pc: done });

    program.preassign_label_to_next_insn(miss);
    emit_prepared_subquery(program, prepared, |program, query, destination| {
        emit_planned_query_body(program, plan, query, destination)
    })?;
    program.emit_insn(Insn::Copy {
        src_reg: result_start,
        dst_reg: record_start + key_width,
        extra_amount: width - 1,
    });
    let record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(record_start),
        count: to_u32(key_width + width),
        dest_reg: to_u32(record),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: cursor,
        record_reg: record,
        unpacked_start: Some(record_start),
        unpacked_count: Some(to_u32(key_width + width)),
        flags: crate::vdbe::insn::IdxInsertFlags::new(),
    });
    program.preassign_label_to_next_insn(done);
    Ok(())
}

/// Run a correlated IN subquery that the planner turned into a probe: like
/// EXISTS, with the planned `lhs = value` term in its block, so it stops at
/// the first match. Return the register that holds 1 on a match and 0
/// otherwise.
pub(crate) fn emit_in_probe_subquery(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: QueryId,
) -> Result<usize> {
    let register = program.alloc_register();
    program.bind_subquery(query, SubqueryBinding::Exists { register });
    let prepared = PreparedSubquery {
        query,
        destination: QueryDestination::ExistsSubqueryResult {
            result_reg: register,
        },
        correlated: true,
    };
    // EXPLAIN lists it as the list subquery it was written as.
    let explain_id = program.next_subquery_eqp_id();
    crate::emit_explain!(
        program,
        true,
        EqpDetail::ListSubquery {
            id: explain_id,
            correlated: true,
        }
    );
    emit_prepared_subquery(program, &prepared, |program, query, destination| {
        emit_planned_query_body(program, plan, query, destination)
    })?;
    program.pop_current_parent_explain();
    Ok(register)
}

/// Emit SELECT and VALUES bodies that do not read a row source.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn emit_query_body(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query_id: QueryId,
    destination: &QueryDestination,
) -> Result<()> {
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR lowering references missing query {query_id}"))
    })?;
    if query.blocks.len() != 1 || !query.compounds.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR query {query_id} is not a supported non-FROM query body"
        )));
    }
    let block = &query.blocks[0];
    if block.from.is_some()
        || block.aggregate_count != 0
        || block.window_function_count != 0
        || !block.windows.is_empty()
    {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} is not a supported non-FROM query body",
            block.id
        )));
    }

    let done = program.allocate_label();
    let distinct = initialize_hir_distinct(program, block)?;
    let sorter = (!query.order_by.is_empty())
        .then(|| initialize_hir_sorter(program, query, block))
        .transpose()?;
    let limit = initialize_limit(
        program,
        document,
        query.limit.as_ref(),
        done,
        destination_stops_after_first_row(destination),
    )?;
    let output = if let Some(sorter) = sorter.as_ref() {
        HirRowOutput {
            target: HirRowTarget::Sorter(sorter),
            distinct: distinct.as_ref(),
        }
    } else {
        HirRowOutput {
            target: HirRowTarget::Direct {
                destination,
                limit,
                done,
            },
            distinct: distinct.as_ref(),
        }
    };
    emit_query_block_without_from(program, document, block, &output, done)?;
    if let Some(sorter) = sorter.as_ref() {
        emit_hir_sorted_rows(program, document, block, sorter, destination, limit, done)?;
    }
    program.preassign_label_to_next_insn(done);
    Ok(())
}

fn emit_query_block_without_from(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block: &hir::QueryBlock,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<()> {
    match &block.body {
        hir::QueryBlockBody::Select {
            filter,
            grouping: None,
            ..
        } => {
            let start = query_output_registers(program, &block.outputs, output.needs_values())?;
            if let Some(filter) = filter {
                let emit_row = program.allocate_label();
                super::expr::translate_condition_expr(
                    program,
                    document,
                    filter,
                    ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true: emit_row,
                        jump_target_when_false: block_done,
                    },
                )?;
                program.preassign_label_to_next_insn(emit_row);
            }
            emit_query_row(
                program,
                document,
                output,
                &block.outputs,
                block.outputs.iter().map(|output| &output.expr),
                start,
                block_done,
            )?;
            Ok(())
        }
        hir::QueryBlockBody::Values { rows } => {
            let start = query_output_registers(program, &block.outputs, output.needs_values())?;
            // Every row writes the same registers, so a row's constants are
            // computed where the row is, not once before the loops.
            let first_span = program.constant_spans_next_idx();
            for row in rows {
                let next_row = program.allocate_label();
                emit_query_row(
                    program,
                    document,
                    output,
                    &block.outputs,
                    row.iter(),
                    start,
                    next_row,
                )?;
                program.constant_spans_invalidate_after(first_span);
                program.preassign_label_to_next_insn(next_row);
                if let HirRowTarget::Direct {
                    destination, limit, ..
                } = &output.target
                {
                    let stop_after_first = destination_stops_after_first_row(destination);
                    let needs_runtime_stop =
                        stop_after_first && limit.and_then(|limit| limit.offset).is_some();
                    if stop_after_first && !needs_runtime_stop {
                        break;
                    }
                }
            }
            Ok(())
        }
        _ => Err(LimboError::InternalError(format!(
            "HIR query block {:?} has unsupported non-FROM clauses",
            block.id
        ))),
    }
}

/// Emit one planned HIR query whose source loop is a full B-tree scan.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn emit_planned_query_body(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query_id: QueryId,
    destination: &QueryDestination,
) -> Result<()> {
    let document = &plan.document;
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR lowering references missing query {query_id}"))
    })?;
    let planned_query = plan.planned_query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR lowering references unplanned query {query_id}"
        ))
    })?;
    if query.blocks.len() != planned_query.blocks.len()
        || query.compounds.len() + 1 != query.blocks.len()
    {
        return Err(LimboError::InternalError(format!(
            "HIR query {query_id} has inconsistent compound access plans"
        )));
    }
    if !query.compounds.is_empty() {
        return emit_hir_compound_query(program, plan, query, destination);
    }

    let block = &query.blocks[0];
    materialize_hir_shared_ctes(program, plan, &planned_query.blocks[0].loops)?;
    emit_hir_block_explain(program, document, block, &planned_query.blocks[0])?;
    let done = program.allocate_label();
    // EXISTS only checks whether a row comes out, so like SQLite its
    // subquery drops DISTINCT and never evaluates ORDER BY. LIMIT and
    // OFFSET stay and count plain rows.
    let exists = matches!(destination, QueryDestination::ExistsSubqueryResult { .. });
    let distinct = if exists {
        None
    } else {
        initialize_hir_distinct(program, block)?
    };
    // Without grouping, aggregates or windows, rows leave the loops in scan
    // order, so a scan that already follows ORDER BY needs no sorter.
    let scan_order_is_output_order =
        hir_grouping(block).is_none() && block.aggregate_count == 0 && block.windows.is_empty();
    let sort_elided = exists
        || (scan_order_is_output_order && planned_query.blocks[0].order_sort_elided)
        || hir_window_rows_follow_order_by(document, query, block)?
        || hir_group_rows_follow_order_by(document, query, block, &planned_query.blocks[0]);
    let sorter = (!query.order_by.is_empty() && !sort_elided)
        .then(|| initialize_hir_sorter(program, query, block))
        .transpose()?;
    // An index method may apply LIMIT and OFFSET while it scans.
    let limit = query
        .limit
        .as_ref()
        .filter(|_| !planned_query.blocks[0].limit_applied);
    let limit = initialize_limit(
        program,
        document,
        limit,
        done,
        destination_stops_after_first_row(destination),
    )?;
    let output = if let Some(sorter) = sorter.as_ref() {
        HirRowOutput {
            target: HirRowTarget::Sorter(sorter),
            distinct: distinct.as_ref(),
        }
    } else {
        HirRowOutput {
            target: HirRowTarget::Direct {
                destination,
                limit,
                done,
            },
            distinct: distinct.as_ref(),
        }
    };
    emit_planned_query_block_with_output(program, plan, query, 0, &output, done)?;
    if let Some(sorter) = sorter.as_ref() {
        emit_hir_sorted_rows(program, document, block, sorter, destination, limit, done)?;
    }
    program.preassign_label_to_next_insn(done);
    Ok(())
}

/// Whether grouped rows already leave in ORDER BY order, as SQLite finds:
/// when every ORDER BY term is a GROUP BY key, the GROUP BY
/// sorter orders groups by those keys first, in ORDER BY order and with each
/// term's direction, so no second sorter is needed. When the loops already
/// return rows in that order, the planner skips both sorts.
fn hir_group_rows_follow_order_by(
    document: &HirDocument,
    query: &hir::Query,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
) -> bool {
    let Some(grouping) = hir_grouping(block) else {
        return false;
    };
    if block_plan.group_sort_elided {
        return block_plan.order_sort_elided;
    }
    block.windows.is_empty() && hir_group_key_order(document, query, grouping).is_some()
}

/// The keys in the order the GROUP BY sorter compares them in; see
/// [`crate::translate::semantic_to_plan::group_key_order`].
fn hir_group_key_order(
    document: &HirDocument,
    query: &hir::Query,
    grouping: &hir::Grouping,
) -> Option<Vec<usize>> {
    crate::translate::semantic_to_plan::group_key_order(document, &query.order_by, grouping)
        .map(|keys| keys.into_iter().map(|key| key.key).collect())
}

/// Whether rows leave the block's windows already in ORDER BY order. The
/// outermost window layer returns rows sorted by its PARTITION BY and then
/// its ORDER BY, so like SQLite, an ORDER BY that starts those
/// keys needs no sorter, and LIMIT can stop before later rows are computed.
fn hir_window_rows_follow_order_by(
    document: &HirDocument,
    query: &hir::Query,
    block: &hir::QueryBlock,
) -> Result<bool> {
    let Some(window) = block.windows.first() else {
        return Ok(false);
    };
    if query.order_by.is_empty() {
        return Ok(false);
    }
    let plan = super::window::plan_hir_window_buffer(query, block, window.id)?;
    let keys = plan.input_sort_keys();
    if keys.len() < query.order_by.len() {
        return Ok(false);
    }
    let nulls_first = |order: SortOrder, nulls: Option<NullsOrder>| match nulls {
        Some(NullsOrder::First) => true,
        Some(NullsOrder::Last) => false,
        None => order == SortOrder::Asc,
    };
    Ok(query.order_by.iter().zip(&keys).all(|(term, key)| {
        let mut expression = &term.expr;
        while let hir::Expr::Output(id) = expression {
            match document.output(*id) {
                Some(output) => expression = &output.expr,
                None => return false,
            }
        }
        expression.equivalent(key.expression)
            && term.order == key.order
            && nulls_first(term.order, term.nulls) == nulls_first(key.order, key.nulls)
            && term
                .collation
                .as_ref()
                .map(|collation| *collation.value())
                .unwrap_or_default()
                == key.collation.unwrap_or_default()
    }))
}

fn emit_planned_query_block(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    block_index: usize,
    destination: &QueryDestination,
    limit: Option<QueryLimitRegisters>,
    done: BranchOffset,
) -> Result<()> {
    let block = query.blocks.get(block_index).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR query {} references missing block {block_index}",
            query.id
        ))
    })?;
    let distinct = initialize_hir_distinct(program, block)?;
    let output = HirRowOutput {
        target: HirRowTarget::Direct {
            destination,
            limit,
            done,
        },
        distinct: distinct.as_ref(),
    };
    // A row the block skips (WHERE, OFFSET) ends only this block; the next
    // block of a compound still runs. A spent LIMIT ends the whole query.
    let block_done = program.allocate_label();
    emit_planned_query_block_with_output(program, plan, query, block_index, &output, block_done)?;
    program.preassign_label_to_next_insn(block_done);
    Ok(())
}

fn emit_planned_query_block_with_output(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    block_index: usize,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<()> {
    let block = query.blocks.get(block_index).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR query {} references missing block {block_index}",
            query.id
        ))
    })?;
    let planned_query = plan.planned_query(query.id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR lowering references unplanned query {}",
            query.id
        ))
    })?;
    let block_plan = planned_query.blocks.get(block_index).ok_or_else(|| {
        LimboError::InternalError(format!("HIR query block {:?} has no access plan", block.id))
    })?;
    if block_plan.block != block.id {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} has mismatched access plan {:?}",
            block.id, block_plan.block
        )));
    }
    materialize_hir_shared_ctes(program, plan, &block_plan.loops)?;
    emit_hir_block_explain(program, &plan.document, block, block_plan)?;
    if !block.windows.is_empty() {
        return emit_hir_window_block(program, plan, query, block, block_plan, output);
    }
    emit_planned_block_rows(program, plan, query, block, block_plan, output, block_done)
}

/// Emit the rows of one planned block into `output`. A block with windows
/// sends them to its window layer; other blocks send them to the query output.
fn emit_planned_block_rows(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<()> {
    let feeds_window = matches!(output.target, HirRowTarget::Window(_));
    if block.windows.is_empty() == feeds_window {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} rows do not match its window layer",
            block.id
        )));
    }
    if block.from.is_none() && hir_grouping(block).is_none() {
        if is_hir_ungrouped_aggregate(block) {
            return emit_hir_ungrouped_without_from(
                program,
                &plan.document,
                query,
                block,
                output,
                block_done,
            );
        }
        return emit_query_block_without_from(program, &plan.document, block, output, block_done);
    }
    if is_hir_ungrouped_aggregate(block) {
        emit_hir_ungrouped_aggregate(program, plan, query, block, block_plan, output, block_done)
    } else if hir_grouping(block).is_some() {
        emit_hir_grouped_aggregate(program, plan, query, block, block_plan, output)
    } else {
        emit_btree_loops(
            program,
            plan,
            block,
            block_plan,
            HirLoopBody::Rows(output, &block.outputs),
        )
    }
}

fn emit_hir_ungrouped_without_from(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query: &hir::Query,
    block: &hir::QueryBlock,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<()> {
    let state = initialize_hir_aggregates(program, document, query, block)?;
    let scan_done = program.allocate_label();
    let hir::QueryBlockBody::Select { filter, .. } = &block.body else {
        return Err(LimboError::InternalError(format!(
            "HIR aggregate block {:?} is not a SELECT",
            block.id
        )));
    };
    if let Some(filter) = filter {
        let emit_step = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            filter,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: emit_step,
                jump_target_when_false: scan_done,
            },
        )?;
        program.preassign_label_to_next_insn(emit_step);
    }
    emit_hir_aggregate_steps(program, document, &state)?;
    program.preassign_label_to_next_insn(scan_done);
    emit_hir_aggregate_result(program, document, block, output, block_done, &state)
}

fn emit_hir_ungrouped_aggregate(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<()> {
    if block_plan.simple_count
        && emit_hir_simple_count(program, plan, block, block_plan, output, block_done)?
    {
        return Ok(());
    }
    let state = initialize_hir_aggregates(program, &plan.document, query, block)?;
    let first_value_done = block_plan
        .min_max_first_value
        .then(|| program.allocate_label());
    emit_btree_loops(
        program,
        plan,
        block,
        block_plan,
        HirLoopBody::UngroupedAggregate(&state, first_value_done),
    )?;
    if let Some(done) = first_value_done {
        program.preassign_label_to_next_insn(done);
    }
    emit_hir_aggregate_result(program, &plan.document, block, output, block_done, &state)
}

/// `SELECT count(*) FROM t`: like SQLite, count the entries of
/// the table, or of the index the planner chose to scan, without a loop.
/// Returns false, having emitted nothing, for a materialized view, whose
/// rows are not a plain B-tree.
fn emit_hir_simple_count(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<bool> {
    let document = &plan.document;
    let [planned_loop] = block_plan.loops.as_slice() else {
        return Err(LimboError::InternalError(
            "HIR count(*) shortcut needs exactly one loop".to_string(),
        ));
    };
    let source = document.source(planned_loop.source).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR count(*) shortcut references missing source {}",
            planned_loop.source
        ))
    })?;
    let (hir::SourceKind::Table(table), Some(database)) = (&source.kind, source.database) else {
        return Err(LimboError::InternalError(format!(
            "HIR count(*) shortcut source {} is not a stored table",
            source.id
        )));
    };
    let Some(btree) = table.value().btree() else {
        return Err(LimboError::InternalError(format!(
            "HIR count(*) shortcut source {} is not a B-tree table",
            source.id
        )));
    };
    if plan.materialized_view(source.id).is_some() {
        return Ok(false);
    }
    let database_snapshot = document.database(database).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR count(*) shortcut source {} references missing database {database:?}",
            source.id
        ))
    })?;
    program.begin_read_on_database(database.index(), database_snapshot.schema_version)?;
    let index = match &planned_loop.access {
        HirSourceAccess::BTree { operation, .. } => selected_hir_btree_index(operation),
        _ => None,
    };
    let cursor = match index {
        Some(index) => super::emit::open_read_index(program, index, database.index()),
        None => super::emit::open_read_table(program, &btree, database.index()),
    };
    let count = program.alloc_register();
    program.emit_insn(Insn::Count {
        cursor_id: cursor,
        target_reg: count,
        exact: true,
    });
    program.emit_insn(Insn::Close { cursor_id: cursor });
    let [count_output] = block.outputs.as_slice() else {
        return Err(LimboError::InternalError(
            "HIR count(*) shortcut needs exactly one output".to_string(),
        ));
    };
    program.bind_expression_register(&count_output.expr, count);
    let start = query_output_registers(program, &block.outputs, output.needs_values())?;
    emit_query_row(
        program,
        document,
        output,
        &block.outputs,
        block.outputs.iter().map(|output| &output.expr),
        start,
        block_done,
    )?;
    program.clear_expression_registers();
    Ok(true)
}

fn emit_hir_grouped_aggregate<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    output: &HirRowOutput<'_>,
) -> Result<()> {
    let runtime = initialize_hir_group_runtime(program, plan, query, block, block_plan)?;
    emit_btree_loops(
        program,
        plan,
        block,
        block_plan,
        HirLoopBody::Group(&runtime),
    )?;
    if matches!(runtime.rows, HirGroupRows::Sorter(_)) {
        emit_hir_group_sorter_rows(program, &plan.document, &runtime)?;
    }
    emit_hir_group_result(program, &plan.document, query, block, output, &runtime)
}

/// The expressions a grouped loop's index stores that the block reads from
/// it. Each is kept in the group row as the index stores it.
fn hir_group_loop_index_expressions<'a>(
    plan: &'a HirPlan,
    planned_loop: &HirPlannedLoop,
) -> Result<Vec<(SourceId, &'a hir::Expr, usize)>> {
    let HirSourceAccess::BTree { operation, .. } = &planned_loop.access else {
        return Ok(Vec::new());
    };
    let index = match operation {
        HirBtreeOperation::Scan { index, .. }
        | HirBtreeOperation::Seek { index, .. }
        | HirBtreeOperation::InSeek { index, .. } => index.as_ref(),
        HirBtreeOperation::RowidEq { .. } => None,
    };
    let Some(index) = index else {
        return Ok(Vec::new());
    };
    let source = plan.document.source(planned_loop.source).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR grouped loop references missing source {}",
            planned_loop.source
        ))
    })?;
    if !source_is_never_null_extended(&plan.document, source.id) {
        return Ok(Vec::new());
    }
    Ok(hir_index_key_expressions(plan, source, index)?
        .into_iter()
        .map(|(expression, column)| (source.id, expression, column))
        .collect())
}

/// The direction to sort each GROUP BY key in, like SQLite, so
/// ORDER BY ties come out in the same order. When every ORDER BY term is an
/// aggregate or a constant, all keys follow the first term's direction.
/// Otherwise a key follows the ORDER BY term equal to it, or sorts
/// ascending.
fn hir_group_key_orders(
    document: &HirDocument,
    query: &hir::Query,
    grouping: &hir::Grouping,
) -> Vec<(SortOrder, Option<turso_parser::ast::NullsOrder>)> {
    let Some(first) = query.order_by.first() else {
        return vec![(SortOrder::Asc, None); grouping.keys.len()];
    };
    let terms = query.order_by.iter().map(|term| {
        let mut expression = &term.expr;
        while let hir::Expr::Output(output) = expression {
            match document.output(*output) {
                Some(output) => expression = &output.expr,
                None => break,
            }
        }
        (expression, term.order, term.nulls)
    });
    if terms
        .clone()
        .all(|(expression, ..)| is_hir_aggregate_call(expression) || is_hir_constant(expression))
    {
        return vec![(first.order, first.nulls); grouping.keys.len()];
    }
    grouping
        .keys
        .iter()
        .map(|key| {
            terms
                .clone()
                .find(|(expression, ..)| expression.equivalent(key))
                .map_or((SortOrder::Asc, None), |(_, order, nulls)| (order, nulls))
        })
        .collect()
}

fn is_hir_aggregate_call(expression: &hir::Expr) -> bool {
    matches!(
        expression,
        hir::Expr::Function(call)
            if matches!(call.evaluation, hir::FunctionEvaluation::Aggregate { .. })
    )
}

/// Whether the expression reads no row and always gives the same value.
fn is_hir_constant(expression: &hir::Expr) -> bool {
    let mut constant = true;
    expression.for_each(&mut |expression| match expression {
        hir::Expr::Column(_)
        | hir::Expr::RowId(_)
        | hir::Expr::MergedColumn(_)
        | hir::Expr::Output(_)
        | hir::Expr::Subquery(_) => constant = false,
        hir::Expr::Function(call)
            if !matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                || !crate::function::Deterministic::is_deterministic(call.function.value()) =>
        {
            constant = false
        }
        _ => {}
    });
    constant
}

fn initialize_hir_group_runtime<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
) -> Result<HirGroupRuntime<'a>> {
    let grouping = hir_grouping(block).expect("grouped lowering has non-empty keys");
    if grouping.keys.len() != grouping.key_type_facts.len()
        || grouping.keys.len() != grouping.key_collations.len()
    {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} has inconsistent grouping facts",
            block.id
        )));
    }
    let aggregates = initialize_hir_aggregates(program, &plan.document, query, block)?;
    let mut covered = Vec::new();
    for planned_loop in &block_plan.loops {
        covered.extend(hir_group_loop_index_expressions(plan, planned_loop)?);
    }
    let mut usage = hir_group_row_columns(&plan.document, query, block, grouping, &covered);
    for reference in hir_nested_outer_column_reads(&plan.document, query) {
        if !usage.contains(&reference) {
            usage.push(reference);
        }
    }
    // A virtual generated column read from the group row is computed again
    // from the columns it depends on, so the group row keeps those too.
    let mut position = 0;
    while let Some(reference) = usage.get(position).copied() {
        position += 1;
        let Some(hir::ColumnReadExpression::Planned(expression)) = plan
            .document
            .source(reference.source)
            .and_then(|source| source.generated_expressions.get(reference.column))
        else {
            continue;
        };
        expression.for_each(&mut |expression| {
            if let hir::Expr::Column(dependency) = expression {
                if !usage.contains(dependency) {
                    usage.push(*dependency);
                }
            }
        });
    }
    let rowids = hir_group_rowids(query, block);
    let key_order = hir_group_key_order(&plan.document, query, grouping)
        .unwrap_or_else(|| (0..grouping.keys.len()).collect());
    let mut inputs = key_order
        .iter()
        .copied()
        .map(HirGroupInput::Key)
        .collect::<Vec<_>>();
    let mut sources = Vec::with_capacity(block_plan.loops.len());
    for loop_source in block_plan.loops.iter().flat_map(HirPlannedLoop::sources) {
        if sources
            .iter()
            .any(|source: &HirGroupSource| source.source == loop_source)
        {
            return Err(LimboError::InternalError(format!(
                "HIR grouped query has more than one loop for source {loop_source}"
            )));
        }
        let source = plan.document.source(loop_source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR grouped query references missing source {loop_source}"
            ))
        })?;
        let row_start = if source.columns.is_empty() {
            0
        } else {
            program.alloc_registers(source.columns.len())
        };
        let group_start = if source.columns.is_empty() {
            0
        } else {
            program.alloc_registers(source.columns.len())
        };
        for column in usage
            .iter()
            .filter(|reference| reference.source == source.id)
            .map(|reference| reference.column)
        {
            inputs.push(HirGroupInput::Column {
                reference: hir::ColumnRef {
                    source: source.id,
                    column,
                },
                row_register: row_start + column,
                group_register: group_start + column,
            });
        }
        let rowid = rowids.contains(&source.id).then(|| {
            let row_register = program.alloc_register();
            let group_register = program.alloc_register();
            inputs.push(HirGroupInput::RowId {
                source: source.id,
                row_register,
                group_register,
            });
            HirGroupRowId {
                row_register,
                group_register,
            }
        });
        let mut stored_columns: Vec<usize> = Vec::new();
        for (covered_source, _, column) in &covered {
            if *covered_source != source.id || stored_columns.contains(column) {
                continue;
            }
            stored_columns.push(*column);
            let expressions = covered
                .iter()
                .filter(|(other_source, _, other_column)| {
                    other_source == covered_source && other_column == column
                })
                .map(|(_, expression, _)| *expression)
                .collect();
            inputs.push(HirGroupInput::Expression {
                expressions,
                row_register: program.alloc_register(),
                group_register: program.alloc_register(),
            });
        }
        sources.push(HirGroupSource {
            source: source.id,
            row_start,
            group_start,
            rowid,
        });
    }
    let rows = if block_plan.group_sort_elided {
        HirGroupRows::InputOrdered
    } else {
        let orders = hir_group_key_orders(&plan.document, query, grouping);
        let order_collations_nulls = key_order
            .iter()
            .map(|key| {
                let (order, nulls) = orders[*key];
                (
                    order,
                    grouping.key_collations[*key]
                        .as_ref()
                        .map(|collation| *collation.value()),
                    nulls,
                )
            })
            .collect_vec();
        let comparators = key_order
            .iter()
            .map(|key| custom_type_comparator_from_type_fact(&grouping.key_type_facts[*key]))
            .collect_vec();
        let cursor = program.alloc_cursor_id(CursorType::Sorter);
        program.emit_insn(Insn::SorterOpen {
            data: Box::new(SorterOpenData {
                cursor_id: cursor,
                columns: inputs.len(),
                order_collations_nulls,
                comparators,
            }),
        });
        emit_explain!(
            program,
            false,
            EqpDetail::GroupBy {
                method: EqpSortMethod::Sorter,
            }
        );
        HirGroupRows::Sorter(HirGroupSorter {
            cursor,
            pseudo_cursor: program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
                column_count: inputs.len(),
            })),
            record: program.alloc_register(),
            inputs_start: program.alloc_registers(inputs.len()),
        })
    };
    let state = HirGroupState {
        output_group: program.allocate_label(),
        output_done: program.allocate_label(),
        finalize_group: program.allocate_label(),
        clear_group: program.allocate_label(),
        group_end: program.allocate_label(),
        output_return: program.alloc_register(),
        clear_return: program.alloc_register(),
        data_in_group: program.alloc_register(),
        previous_keys: program.alloc_registers(grouping.keys.len()),
        current_keys: program.alloc_registers(grouping.keys.len()),
    };
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: state.data_in_group,
    });
    program.emit_insn(Insn::Null {
        dest: state.previous_keys,
        dest_end: (grouping.keys.len() > 1)
            .then_some(state.previous_keys + grouping.keys.len() - 1),
    });
    program.emit_insn(Insn::Gosub {
        target_pc: state.clear_group,
        return_reg: state.clear_return,
    });
    Ok(HirGroupRuntime {
        grouping,
        aggregates,
        rows,
        inputs,
        sources,
        state,
    })
}

/// The columns a grouped row keeps: those read after the rows are grouped.
/// A column only WHERE or a join reads is not kept. A part of
/// an output, HAVING, ORDER BY or window expression equal to a GROUP BY key
/// is read from the key's register, so its columns are not kept for it; an
/// aggregate's arguments are read from each grouped row, so all of theirs
/// are.
fn hir_group_row_columns(
    document: &HirDocument,
    query: &hir::Query,
    block: &hir::QueryBlock,
    grouping: &hir::Grouping,
    covered: &[(SourceId, &hir::Expr, usize)],
) -> Vec<hir::ColumnRef> {
    struct Visitor<'a> {
        document: &'a HirDocument,
        keys: &'a [hir::Expr],
        covered: &'a [(SourceId, &'a hir::Expr, usize)],
        columns: Vec<hir::ColumnRef>,
    }
    impl Visitor<'_> {
        fn add(&mut self, reference: hir::ColumnRef) {
            if !self.columns.contains(&reference) {
                self.columns.push(reference);
            }
        }
        fn is_covered(&self, expression: &hir::Expr) -> bool {
            self.covered
                .iter()
                .any(|(_, covered, _)| std::ptr::eq(*covered, expression))
        }
        fn visit(&mut self, expression: &hir::Expr) {
            if hir_group_key_is_reused(self.document, self.keys, expression)
                || self.is_covered(expression)
            {
                return;
            }
            match expression {
                hir::Expr::Column(reference) => self.add(*reference),
                hir::Expr::Function(call)
                    if matches!(call.evaluation, hir::FunctionEvaluation::Aggregate { .. }) =>
                {
                    self.add_row_columns(expression)
                }
                hir::Expr::Output(output) => {
                    if let Some(output) = self.document.output(*output) {
                        self.visit(&output.expr);
                    }
                }
                _ => {
                    let mut index = 0;
                    while let Some(child) = expression.child(index) {
                        self.visit(child);
                        index += 1;
                    }
                }
            }
        }
        fn add_row_columns(&mut self, expression: &hir::Expr) {
            if self.is_covered(expression) {
                return;
            }
            match expression {
                hir::Expr::Column(reference) => self.add(*reference),
                hir::Expr::Output(output) => {
                    if let Some(output) = self.document.output(*output) {
                        self.add_row_columns(&output.expr);
                    }
                }
                _ => {
                    let mut index = 0;
                    while let Some(child) = expression.child(index) {
                        self.add_row_columns(child);
                        index += 1;
                    }
                }
            }
        }
    }
    let mut visitor = Visitor {
        document,
        keys: &grouping.keys,
        covered,
        columns: Vec::new(),
    };
    for output in &block.outputs {
        visitor.visit(&output.expr);
    }
    if let Some(having) = &grouping.having {
        visitor.visit(having);
    }
    for term in &query.order_by {
        visitor.visit(&term.expr);
    }
    for window in &block.windows {
        for expression in hir_window_expressions(window) {
            visitor.visit(expression);
        }
    }
    visitor.columns
}

/// Whether `expression` is a GROUP BY key whose stored value the group's
/// outputs, HAVING and ORDER BY read instead of computing it again. A value
/// read back from the group sorter has lost its JSON subtype, so a key that
/// can carry one is computed again from its columns.
fn hir_group_key_is_reused(
    document: &HirDocument,
    keys: &[hir::Expr],
    expression: &hir::Expr,
) -> bool {
    hir_group_key_position(document, keys, expression).is_some()
        && !hir_expr_can_return_json(expression)
}

/// The GROUP BY key equal to `expression`. `GROUP BY alias` names an output
/// through an [`hir::Expr::Output`] reference, and that output's expression
/// is the key.
fn hir_group_key_position(
    document: &HirDocument,
    keys: &[hir::Expr],
    expression: &hir::Expr,
) -> Option<usize> {
    keys.iter().position(|key| {
        if key.equivalent(expression) {
            return true;
        }
        let hir::Expr::Output(output) = key else {
            return false;
        };
        document
            .output(*output)
            .is_some_and(|output| output.expr.equivalent(expression))
    })
}

/// Whether `expression` can produce a value with the JSON subtype.
fn hir_expr_can_return_json(expression: &hir::Expr) -> bool {
    let mut json = false;
    expression.for_each(&mut |expression| {
        json |= match expression {
            #[cfg(feature = "json")]
            hir::Expr::Function(call) => {
                matches!(call.function.value(), crate::function::Func::Json(_))
            }
            hir::Expr::Binary {
                operator: turso_parser::ast::Operator::ArrowRight,
                ..
            } => true,
            _ => false,
        };
    });
    json
}

fn hir_group_rowids(query: &hir::Query, block: &hir::QueryBlock) -> Vec<SourceId> {
    let mut rowids = Vec::new();
    let mut collect = |expression: &hir::Expr| {
        expression.for_each(&mut |expression| {
            if let hir::Expr::RowId(source) = expression {
                if !rowids.contains(source) {
                    rowids.push(*source);
                }
            }
        });
    };
    for output in &block.outputs {
        collect(&output.expr);
    }
    if let hir::QueryBlockBody::Select {
        grouping: Some(grouping),
        ..
    } = &block.body
    {
        for key in &grouping.keys {
            collect(key);
        }
        if let Some(having) = &grouping.having {
            collect(having);
        }
    }
    for term in &query.order_by {
        collect(&term.expr);
    }
    for window in &block.windows {
        for expression in hir_window_expressions(window) {
            collect(expression);
        }
    }
    rowids.sort_unstable();
    rowids
}

fn emit_hir_group_input(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    runtime: &HirGroupRuntime<'_>,
) -> Result<()> {
    let HirGroupRows::Sorter(sorter) = &runtime.rows else {
        for input in &runtime.inputs {
            match input {
                HirGroupInput::Key(index) => super::expr::translate_expr(
                    program,
                    document,
                    &runtime.grouping.keys[*index],
                    runtime.state.current_keys + index,
                )?,
                // The group's row registers hold columns as stored; reading
                // them through the bound source decodes them once.
                HirGroupInput::Column {
                    reference,
                    row_register,
                    ..
                } => super::dml::with_stored_values(program, |program| {
                    super::expr::translate_expr(
                        program,
                        document,
                        &hir::Expr::Column(*reference),
                        *row_register,
                    )
                })?,
                HirGroupInput::RowId {
                    source,
                    row_register,
                    ..
                } => super::expr::translate_expr(
                    program,
                    document,
                    &hir::Expr::RowId(*source),
                    *row_register,
                )?,
                HirGroupInput::Expression {
                    expressions,
                    row_register,
                    ..
                } => super::expr::translate_expr(program, document, expressions[0], *row_register)?,
            };
        }
        return emit_hir_group_row(program, document, runtime);
    };
    for (position, input) in runtime.inputs.iter().enumerate() {
        let target = sorter.inputs_start + position;
        let expression = match input {
            HirGroupInput::Key(index) => &runtime.grouping.keys[*index],
            HirGroupInput::Column { reference, .. } => {
                super::dml::with_stored_values(program, |program| {
                    super::expr::translate_expr(
                        program,
                        document,
                        &hir::Expr::Column(*reference),
                        target,
                    )
                })?;
                continue;
            }
            HirGroupInput::RowId { source, .. } => {
                super::expr::translate_expr(program, document, &hir::Expr::RowId(*source), target)?;
                continue;
            }
            HirGroupInput::Expression { expressions, .. } => expressions[0],
        };
        super::expr::translate_expr(program, document, expression, target)?;
    }
    sorter_insert(
        program,
        sorter.inputs_start,
        runtime.inputs.len(),
        sorter.cursor,
        sorter.record,
    );
    Ok(())
}

fn bind_hir_group_sources(
    program: &mut ProgramBuilder,
    runtime: &HirGroupRuntime<'_>,
    values: HirGroupSourceValues,
) {
    for source in &runtime.sources {
        let (start, rowid) = match values {
            HirGroupSourceValues::Row => (
                source.row_start,
                source.rowid.as_ref().map(|rowid| rowid.row_register),
            ),
            HirGroupSourceValues::Group => (
                source.group_start,
                source.rowid.as_ref().map(|rowid| rowid.group_register),
            ),
        };
        program.bind_source(source.source, SourceBinding::Registers { start, rowid });
    }
    for input in &runtime.inputs {
        let HirGroupInput::Expression {
            expressions,
            row_register,
            group_register,
        } = input
        else {
            continue;
        };
        let register = match values {
            HirGroupSourceValues::Row => *row_register,
            HirGroupSourceValues::Group => *group_register,
        };
        for expression in expressions {
            program.unbind_expression_register(expression);
            program.bind_expression_register(expression, register);
        }
    }
}

fn reset_hir_group_aggregates(program: &mut ProgramBuilder, state: &HirAggregateState<'_>) {
    for aggregate in &state.aggregates {
        program.emit_insn(Insn::Null {
            dest: aggregate.accumulator,
            dest_end: None,
        });
        if let Some(ordered) = &aggregate.ordered {
            // Opening the open index again empties it.
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id: ordered.cursor,
                is_table: false,
            });
        }
        if let Some(distinct) = &aggregate.distinct {
            program.emit_insn(Insn::HashClear {
                hash_table_id: distinct.hash_table,
            });
        }
    }
}

fn emit_hir_group_sorter_rows(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    runtime: &HirGroupRuntime<'_>,
) -> Result<()> {
    let HirGroupRows::Sorter(sorter) = &runtime.rows else {
        unreachable!("sorter drain requires grouped sorter rows")
    };
    let sort_loop = program.allocate_label();
    let sort_end = program.allocate_label();
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: sorter.pseudo_cursor,
        content_reg: sorter.record,
        num_fields: runtime.inputs.len(),
    });
    program.emit_insn(Insn::SorterSort {
        cursor_id: sorter.cursor,
        pc_if_empty: sort_end,
    });
    program.preassign_label_to_next_insn(sort_loop);
    program.emit_insn(Insn::SorterData {
        cursor_id: sorter.cursor,
        dest_reg: sorter.record,
        pseudo_cursor: sorter.pseudo_cursor,
    });
    for (position, input) in runtime.inputs.iter().enumerate() {
        let target = match input {
            HirGroupInput::Key(index) => runtime.state.current_keys + index,
            HirGroupInput::Column { row_register, .. }
            | HirGroupInput::RowId { row_register, .. }
            | HirGroupInput::Expression { row_register, .. } => *row_register,
        };
        program.emit_column_or_rowid(sorter.pseudo_cursor, position, target);
    }
    emit_hir_group_row(program, document, runtime)?;
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter.cursor,
        pc_if_next: sort_loop,
    });
    program.preassign_label_to_next_insn(sort_end);
    Ok(())
}

fn emit_hir_group_row(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    runtime: &HirGroupRuntime<'_>,
) -> Result<()> {
    let new_group = program.allocate_label();
    let aggregate_step = program.allocate_label();
    let captured = program.allocate_label();
    let key_info = runtime
        .grouping
        .key_collations
        .iter()
        .map(|collation| KeyInfo {
            sort_order: SortOrder::Asc,
            collation: collation
                .as_ref()
                .map(|collation| *collation.value())
                .unwrap_or_default(),
            nulls_order: None,
        })
        .collect();
    program.emit_insn(Insn::Compare {
        start_reg_a: runtime.state.previous_keys,
        start_reg_b: runtime.state.current_keys,
        count: runtime.grouping.keys.len(),
        key_info,
    });
    program.emit_insn(Insn::Jump {
        target_pc_lt: new_group,
        target_pc_eq: aggregate_step,
        target_pc_gt: new_group,
    });
    program.preassign_label_to_next_insn(new_group);
    program.emit_insn(Insn::Gosub {
        target_pc: runtime.state.output_group,
        return_reg: runtime.state.output_return,
    });
    program.emit_insn(Insn::Move {
        source_reg: runtime.state.current_keys,
        dest_reg: runtime.state.previous_keys,
        count: runtime.grouping.keys.len(),
    });
    program.emit_insn(Insn::Gosub {
        target_pc: runtime.state.clear_group,
        return_reg: runtime.state.clear_return,
    });

    program.preassign_label_to_next_insn(aggregate_step);
    bind_hir_group_sources(program, runtime, HirGroupSourceValues::Row);
    emit_hir_aggregate_steps(program, document, &runtime.aggregates)?;
    emit_hir_bare_row_test(
        program,
        runtime.state.data_in_group,
        runtime.aggregates.min_max_row.as_ref(),
        captured,
    );
    for input in &runtime.inputs {
        match input {
            HirGroupInput::Key(_) => {}
            HirGroupInput::Column {
                row_register,
                group_register,
                ..
            }
            | HirGroupInput::RowId {
                row_register,
                group_register,
                ..
            }
            | HirGroupInput::Expression {
                row_register,
                group_register,
                ..
            } => program.emit_insn(Insn::Copy {
                src_reg: *row_register,
                dst_reg: *group_register,
                extra_amount: 0,
            }),
        }
    }
    program.preassign_label_to_next_insn(captured);
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: runtime.state.data_in_group,
    });
    Ok(())
}

/// Read every group-output expression equal to a GROUP BY key from the key's
/// register, as SQLite does. Re-evaluating it would need columns the
/// group row does not keep, such as the inputs of a virtual generated column
/// or the outer values of a correlated subquery.
fn bind_hir_group_key_registers(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query: &hir::Query,
    block: &hir::QueryBlock,
    runtime: &HirGroupRuntime<'_>,
) {
    let keys = &runtime.grouping.keys;
    let mut bind = |root: &hir::Expr| {
        root.for_each(&mut |expression| {
            if !hir_group_key_is_reused(document, keys, expression) {
                return;
            }
            if let Some(key) = hir_group_key_position(document, keys, expression) {
                program.bind_expression_register(expression, runtime.state.previous_keys + key);
            }
        });
    };
    for output in &block.outputs {
        bind(&output.expr);
    }
    if let Some(having) = &runtime.grouping.having {
        bind(having);
    }
    for term in &query.order_by {
        bind(&term.expr);
    }
    for window in &block.windows {
        for expression in hir_window_expressions(window) {
            bind(expression);
        }
    }
}

fn emit_hir_group_result(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query: &hir::Query,
    block: &hir::QueryBlock,
    output: &HirRowOutput<'_>,
    runtime: &HirGroupRuntime<'_>,
) -> Result<()> {
    program.emit_insn(Insn::Gosub {
        target_pc: runtime.state.output_group,
        return_reg: runtime.state.output_return,
    });
    program.emit_insn(Insn::Goto {
        target_pc: runtime.state.group_end,
    });

    program.preassign_label_to_next_insn(runtime.state.output_group);
    program.emit_insn(Insn::IfPos {
        reg: runtime.state.data_in_group,
        target_pc: runtime.state.finalize_group,
        decrement_by: 0,
    });
    program.preassign_label_to_next_insn(runtime.state.output_done);
    program.emit_insn(Insn::Return {
        return_reg: runtime.state.output_return,
        can_fallthrough: false,
    });

    program.preassign_label_to_next_insn(runtime.state.finalize_group);
    finalize_hir_aggregates(program, document, &runtime.aggregates)?;
    bind_hir_group_sources(program, runtime, HirGroupSourceValues::Group);
    bind_hir_group_key_registers(program, document, query, block, runtime);
    if let Some(having) = &runtime.grouping.having {
        let emit_row = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            having,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: emit_row,
                jump_target_when_false: runtime.state.output_done,
            },
        )?;
        program.preassign_label_to_next_insn(emit_row);
    }
    let start = query_output_registers(program, &block.outputs, output.needs_values())?;
    emit_query_row(
        program,
        document,
        output,
        &block.outputs,
        block.outputs.iter().map(|output| &output.expr),
        start,
        runtime.state.output_done,
    )?;
    program.clear_expression_registers();
    program.emit_insn(Insn::Return {
        return_reg: runtime.state.output_return,
        can_fallthrough: false,
    });

    program.preassign_label_to_next_insn(runtime.state.clear_group);
    reset_hir_group_aggregates(program, &runtime.aggregates);
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: runtime.state.data_in_group,
    });
    program.emit_insn(Insn::Return {
        return_reg: runtime.state.clear_return,
        can_fallthrough: false,
    });
    program.preassign_label_to_next_insn(runtime.state.group_end);
    Ok(())
}

fn emit_hir_aggregate_result(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block: &hir::QueryBlock,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
    state: &HirAggregateState<'_>,
) -> Result<()> {
    finalize_hir_aggregates(program, document, state)?;
    let hir::QueryBlockBody::Select { grouping, .. } = &block.body else {
        unreachable!("aggregate block is a SELECT")
    };
    if let Some(having) = grouping
        .as_ref()
        .and_then(|grouping| grouping.having.as_ref())
    {
        let emit_row = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            having,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: emit_row,
                jump_target_when_false: block_done,
            },
        )?;
        program.preassign_label_to_next_insn(emit_row);
    }
    let start = query_output_registers(program, &block.outputs, output.needs_values())?;
    emit_query_row(
        program,
        document,
        output,
        &block.outputs,
        block.outputs.iter().map(|output| &output.expr),
        start,
        block_done,
    )
}

fn emit_hir_compound_query(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    destination: &QueryDestination,
) -> Result<()> {
    emit_explain!(program, true, EqpDetail::Compound);
    let result = if query.order_by.is_empty() {
        let done = program.allocate_label();
        let limit = initialize_limit(
            program,
            &plan.document,
            query.limit.as_ref(),
            done,
            destination_stops_after_first_row(destination),
        )?;
        emit_hir_compound_prefix(
            program,
            plan,
            query,
            query.blocks.len() - 1,
            destination,
            limit,
            done,
        )?;
        program.preassign_label_to_next_insn(done);
        Ok(())
    } else {
        let (collection_cursor, collection_index) = open_hir_compound_index(
            program,
            query,
            0,
            query.blocks.len() - 1,
            "compound_collection",
            true,
        )?;
        let collection_destination = QueryDestination::EphemeralIndex {
            cursor_id: collection_cursor,
            index: collection_index,
            affinity_str: None,
            is_delete: false,
        };
        let collection_done = program.allocate_label();
        emit_hir_compound_prefix(
            program,
            plan,
            query,
            query.blocks.len() - 1,
            &collection_destination,
            None,
            collection_done,
        )?;
        program.preassign_label_to_next_insn(collection_done);
        // The sort of the collected rows is not part of the compound's row.
        program.pop_current_parent_explain();
        emit_hir_compound_order_by(
            program,
            &plan.document,
            query,
            collection_cursor,
            destination,
        )
    };
    if query.order_by.is_empty() {
        program.pop_current_parent_explain();
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn emit_hir_compound_prefix(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    last_block: usize,
    destination: &QueryDestination,
    limit: Option<QueryLimitRegisters>,
    done: BranchOffset,
) -> Result<()> {
    if last_block == 0 {
        emit_explain!(
            program,
            true,
            EqpDetail::CompoundArm {
                op: EqpCompoundOp::LeftMost,
                temp_btree: false,
            }
        );
        let result = emit_planned_query_block(program, plan, query, 0, destination, limit, done);
        program.pop_current_parent_explain();
        return result;
    }

    let operator = query.compounds[last_block - 1].operator;
    match operator {
        CompoundOperator::UnionAll => {
            emit_hir_compound_prefix(
                program,
                plan,
                query,
                last_block - 1,
                destination,
                limit,
                done,
            )?;
            emit_hir_compound_arm_explain(program, operator, false, |program| {
                emit_planned_query_block(program, plan, query, last_block, destination, limit, done)
            })
        }
        CompoundOperator::Union | CompoundOperator::Except => {
            let (cursor, index) = open_hir_compound_index(
                program,
                query,
                last_block - 1,
                last_block,
                "compound_dedupe",
                false,
            )?;
            let insert_destination = QueryDestination::EphemeralIndex {
                cursor_id: cursor,
                index: index.clone(),
                affinity_str: None,
                is_delete: false,
            };
            emit_hir_compound_prefix(
                program,
                plan,
                query,
                last_block - 1,
                &insert_destination,
                None,
                done,
            )?;
            let arm_destination = QueryDestination::EphemeralIndex {
                cursor_id: cursor,
                index: index.clone(),
                affinity_str: None,
                is_delete: operator == CompoundOperator::Except,
            };
            emit_hir_compound_arm_explain(program, operator, true, |program| {
                emit_planned_query_block(
                    program,
                    plan,
                    query,
                    last_block,
                    &arm_destination,
                    None,
                    done,
                )
            })?;
            emit_hir_deduplicated_rows(program, cursor, &index, limit, destination, done)
        }
        CompoundOperator::Intersect => {
            let (left_cursor, left_index) = open_hir_compound_index(
                program,
                query,
                last_block - 1,
                last_block,
                "compound_intersect_left",
                false,
            )?;
            let left_destination = QueryDestination::EphemeralIndex {
                cursor_id: left_cursor,
                index: left_index.clone(),
                affinity_str: None,
                is_delete: false,
            };
            emit_hir_compound_prefix(
                program,
                plan,
                query,
                last_block - 1,
                &left_destination,
                None,
                done,
            )?;

            let (right_cursor, right_index) = open_hir_compound_index(
                program,
                query,
                last_block - 1,
                last_block,
                "compound_intersect_right",
                false,
            )?;
            let right_destination = QueryDestination::EphemeralIndex {
                cursor_id: right_cursor,
                index: right_index,
                affinity_str: None,
                is_delete: false,
            };
            emit_hir_compound_arm_explain(program, operator, true, |program| {
                emit_planned_query_block(
                    program,
                    plan,
                    query,
                    last_block,
                    &right_destination,
                    None,
                    done,
                )
            })?;
            emit_hir_intersect_rows(
                program,
                left_cursor,
                &left_index,
                right_cursor,
                limit,
                destination,
                done,
            )
        }
    }
}

fn emit_hir_compound_arm_explain(
    program: &mut ProgramBuilder,
    operator: CompoundOperator,
    temp_btree: bool,
    emit: impl FnOnce(&mut ProgramBuilder) -> Result<()>,
) -> Result<()> {
    let op = match operator {
        CompoundOperator::Union => EqpCompoundOp::Union,
        CompoundOperator::UnionAll => EqpCompoundOp::UnionAll,
        CompoundOperator::Except => EqpCompoundOp::Except,
        CompoundOperator::Intersect => EqpCompoundOp::Intersect,
    };
    emit_explain!(program, true, EqpDetail::CompoundArm { op, temp_btree });
    let result = emit(program);
    program.pop_current_parent_explain();
    result
}

fn open_hir_compound_index(
    program: &mut ProgramBuilder,
    query: &hir::Query,
    left_block: usize,
    right_block: usize,
    name: &str,
    has_rowid: bool,
) -> Result<(CursorID, Arc<Index>)> {
    let left = query.blocks.get(left_block).ok_or_else(|| {
        LimboError::InternalError(format!("HIR compound query has no block {left_block}"))
    })?;
    let right = query.blocks.get(right_block).ok_or_else(|| {
        LimboError::InternalError(format!("HIR compound query has no block {right_block}"))
    })?;
    if left.outputs.len() != right.outputs.len() || left.outputs.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR compound blocks {:?} and {:?} have inconsistent widths",
            left.id, right.id
        )));
    }
    let mut columns = crate::alloc::vec![];
    for (position, left) in left.outputs.iter().enumerate() {
        columns.push(IndexColumn {
            name: left.name.clone(),
            order: SortOrder::Asc,
            nulls_order: None,
            pos_in_table: position,
            collation: query.blocks[..=right_block]
                .iter()
                .find_map(|block| block.outputs[position].collation.as_ref())
                .map(|collation| *collation.value()),
            default: None,
            expr: None,
        });
    }
    let index = Arc::new(Index {
        constraint_name: None,
        name: name.to_string(),
        table_name: String::new(),
        root_page: 0,
        columns,
        unique: false,
        ephemeral: true,
        has_rowid,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: false,
    });
    Ok((cursor, index))
}

fn emit_hir_deduplicated_rows(
    program: &mut ProgramBuilder,
    cursor: CursorID,
    index: &Index,
    limit: Option<QueryLimitRegisters>,
    destination: &QueryDestination,
    done: BranchOffset,
) -> Result<()> {
    let close = program.allocate_label();
    let next = program.allocate_label();
    let loop_start = program.allocate_label();
    let columns = program.alloc_registers(index.columns.len());
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: close,
    });
    program.preassign_label_to_next_insn(loop_start);
    emit_before_row(program, limit, next);
    for column in 0..index.columns.len() {
        program.emit_insn(Insn::Column {
            cursor_id: cursor,
            column,
            dest: columns + column,
            default: None,
        });
    }
    emit_hir_row_to_destination(program, destination, columns, index.columns.len())?;
    emit_after_row(
        program,
        limit,
        destination_stops_after_first_row(destination),
        done,
    );
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        is_index: program.cursor_is_index(cursor),
        pc_if_next: loop_start,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(close);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_hir_intersect_rows(
    program: &mut ProgramBuilder,
    left_cursor: CursorID,
    index: &Index,
    right_cursor: CursorID,
    limit: Option<QueryLimitRegisters>,
    destination: &QueryDestination,
    done: BranchOffset,
) -> Result<()> {
    let close = program.allocate_label();
    let next = program.allocate_label();
    let loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: left_cursor,
        pc_if_empty: close,
    });
    program.preassign_label_to_next_insn(loop_start);
    let record = program.alloc_register();
    program.emit_insn(Insn::RowData {
        cursor_id: left_cursor,
        dest: record,
    });
    program.emit_insn(Insn::NotFound {
        cursor_id: right_cursor,
        target_pc: next,
        record_reg: record,
        num_regs: 0,
    });
    emit_before_row(program, limit, next);
    let columns = program.alloc_registers(index.columns.len());
    for column in 0..index.columns.len() {
        program.emit_insn(Insn::Column {
            cursor_id: left_cursor,
            column,
            dest: columns + column,
            default: None,
        });
    }
    emit_hir_row_to_destination(program, destination, columns, index.columns.len())?;
    emit_after_row(
        program,
        limit,
        destination_stops_after_first_row(destination),
        done,
    );
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: left_cursor,
        is_index: program.cursor_is_index(left_cursor),
        pc_if_next: loop_start,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(close);
    program.emit_insn(Insn::Close {
        cursor_id: right_cursor,
    });
    program.emit_insn(Insn::Close {
        cursor_id: left_cursor,
    });
    Ok(())
}

fn emit_hir_compound_order_by(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query: &hir::Query,
    collection_cursor: CursorID,
    destination: &QueryDestination,
) -> Result<()> {
    let output_count = query.blocks[0].outputs.len();
    let mut order_positions = Vec::with_capacity(query.order_by.len());
    let mut order_collations_nulls = crate::alloc::vec![];
    let mut comparators = crate::alloc::vec![];
    for term in &query.order_by {
        let position = hir_compound_order_position(query, &term.expr)?;
        order_positions.push(position);
        order_collations_nulls.push((
            term.order,
            term.collation.as_ref().map(|collation| *collation.value()),
            term.nulls,
        ));
        comparators.push(custom_type_comparator_from_type_fact(&term.type_fact));
    }
    order_collations_nulls.push((SortOrder::Asc, None, None));
    comparators.push(None);

    let sequence_slot = order_positions.len();
    let data_start = sequence_slot + 1;
    let mut data_columns = 0;
    let mut remappings = Vec::with_capacity(output_count);
    for output in 0..output_count {
        if let Some(sort_key) = order_positions
            .iter()
            .position(|position| *position == output)
        {
            remappings.push((sort_key, true));
        } else {
            remappings.push((data_start + data_columns, false));
            data_columns += 1;
        }
    }
    let sorter_column_count = data_start + data_columns;
    let sort_cursor = program.alloc_cursor_id(CursorType::Sorter);
    program.emit_insn(Insn::SorterOpen {
        data: Box::new(SorterOpenData {
            cursor_id: sort_cursor,
            columns: order_collations_nulls.len(),
            order_collations_nulls,
            comparators,
        }),
    });

    let collection_done = program.allocate_label();
    let collection_loop = program.allocate_label();
    let read_registers = program.alloc_registers(output_count + 1);
    let sequence_register = read_registers + output_count;
    program.emit_insn(Insn::Rewind {
        cursor_id: collection_cursor,
        pc_if_empty: collection_done,
    });
    program.preassign_label_to_next_insn(collection_loop);
    for output in 0..output_count {
        program.emit_insn(Insn::Column {
            cursor_id: collection_cursor,
            column: output,
            dest: read_registers + output,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: collection_cursor,
        column: output_count,
        dest: sequence_register,
        default: None,
    });

    let sorter_registers = program.alloc_registers(sorter_column_count);
    for (sort_key, output) in order_positions.iter().enumerate() {
        program.emit_insn(Insn::Copy {
            src_reg: read_registers + output,
            dst_reg: sorter_registers + sort_key,
            extra_amount: 0,
        });
    }
    program.emit_insn(Insn::Copy {
        src_reg: sequence_register,
        dst_reg: sorter_registers + sequence_slot,
        extra_amount: 0,
    });
    let mut data_column = data_start;
    for (output, (_, deduplicated)) in remappings.iter().enumerate() {
        if !deduplicated {
            program.emit_insn(Insn::Copy {
                src_reg: read_registers + output,
                dst_reg: sorter_registers + data_column,
                extra_amount: 0,
            });
            data_column += 1;
        }
    }
    let sorter_record = program.alloc_register();
    sorter_insert(
        program,
        sorter_registers,
        sorter_column_count,
        sort_cursor,
        sorter_record,
    );
    program.emit_insn(Insn::Next {
        cursor_id: collection_cursor,
        is_index: program.cursor_is_index(collection_cursor),
        pc_if_next: collection_loop,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(collection_done);
    program.emit_insn(Insn::Close {
        cursor_id: collection_cursor,
    });

    let sort_done = program.allocate_label();
    let limit = initialize_limit(
        program,
        document,
        query.limit.as_ref(),
        sort_done,
        destination_stops_after_first_row(destination),
    )?;
    let output_columns = remappings
        .iter()
        .map(|(sorter_column, _)| *sorter_column)
        .collect::<Vec<_>>();
    emit_hir_sorter_rows(
        program,
        document,
        &query.blocks[0].outputs,
        sort_cursor,
        sorter_record,
        sorter_column_count,
        &output_columns,
        destination,
        limit,
        sort_done,
    )?;
    program.preassign_label_to_next_insn(sort_done);
    Ok(())
}

fn hir_compound_order_position(query: &hir::Query, expression: &hir::Expr) -> Result<usize> {
    let mut expression = expression;
    while let hir::Expr::Collate { expr, .. } = expression {
        expression = expr;
    }
    let hir::Expr::Output(output) = expression else {
        return Err(LimboError::InternalError(format!(
            "HIR compound ORDER BY expression is not an output reference: {expression:?}"
        )));
    };
    query.blocks[0]
        .outputs
        .iter()
        .position(|candidate| candidate.id == *output)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR compound ORDER BY references missing output {output:?}"
            ))
        })
}

enum HirLoopAdvance {
    None,
    /// Resume the coroutine for its next row.
    Coroutine,
    Cursor {
        direction: IterationDirection,
        fullscan: bool,
    },
    InSeek {
        state: InSeekLoop,
        index_backed: bool,
    },
    Virtual,
    /// Go back to the loop's start, which reads the next row itself.
    Restart,
    /// Find the next row with the same hash key.
    Hash {
        hash_table: usize,
        rowid: usize,
        payload: usize,
        columns: usize,
        waiting_pass: usize,
        waiting_next: BranchOffset,
        /// Where a lookup goes when its probe row is stored for a partition
        /// on disk: past the NULL row of a LEFT JOIN, to the next probe row.
        /// The waiting pass gives such a row its matches, or its NULL row.
        deferred: BranchOffset,
    },
}

pub(super) struct PreparedHirBtreeLoop<'a> {
    pub(super) source: &'a hir::Source,
    pub(super) operation: &'a HirBtreeOperation,
    pub(super) index: Option<&'a Arc<Index>>,
    pub(super) cursor: CursorID,
    pub(super) table_cursor: Option<CursorID>,
    pub(super) autoindex_source_cursor: Option<CursorID>,
    pub(super) table_has_rowid: bool,
    /// Expressions of the query block the index supplies, each with the
    /// index column holding it.
    pub(super) index_keys: Vec<(&'a hir::Expr, usize)>,
}

pub(super) struct PreparedHirMaterializedLoop<'a> {
    source: &'a hir::Source,
    cursor: CursorID,
    table: Arc<BTreeTable>,
    /// Backwards reads a stored CTE from its last row, for an outer ORDER BY
    /// in the opposite direction of the CTE's own order.
    direction: IterationDirection,
    /// The subroutine that fills the table, when the fill runs in one.
    fill_subroutine: Option<CteFillSubroutine>,
}

pub(super) struct PreparedHirVirtualLoop<'a> {
    source: &'a hir::Source,
    operation: &'a HirVirtualTableOperation,
    cursor: CursorID,
}

/// One seek per branch of an OR term: each branch adds the rowids it finds
/// to a row set, and the loop then reads each of those rows once.
pub(super) struct PreparedHirOrUnionLoop<'a> {
    table_cursor: CursorID,
    branches: Vec<PreparedHirBtreeLoop<'a>>,
    /// For each branch, the conditions checked before its seek.
    branch_filters: &'a [Vec<hir::Expr>],
}

/// A scan through a query pattern of an index method, such as FTS: the
/// index returns rowids, and the values of the pattern's result columns.
pub(super) struct PreparedHirIndexMethodLoop<'a> {
    access: &'a crate::translate::hir_index_method::HirIndexMethodAccess,
    index_cursor: CursorID,
    table_cursor: CursorID,
    database: usize,
    /// Expressions of the query block the index returns, each with the
    /// index column that holds it.
    covered: Vec<(&'a hir::Expr, usize)>,
}

pub(super) struct PreparedHirRegistersLoop<'a> {
    source: &'a hir::Source,
}

/// A FROM subquery or CTE that runs as a coroutine: the loop resumes it for
/// each row instead of reading a materialized table, so an outer LIMIT can
/// stop it early.
pub(super) struct PreparedHirCoroutineLoop<'a> {
    source: &'a hir::Source,
    yield_reg: usize,
    start: BranchOffset,
}

#[derive(Clone, Copy)]
enum HirMaterializedQuery {
    Derived(QueryId),
    Cte { cte: hir::CteId, query: QueryId },
}

impl HirMaterializedQuery {
    fn query(self) -> QueryId {
        match self {
            Self::Derived(query) | Self::Cte { query, .. } => query,
        }
    }
}

pub(super) enum PreparedHirLoop<'a> {
    BTree(PreparedHirBtreeLoop<'a>),
    Virtual(PreparedHirVirtualLoop<'a>),
    IndexMethod(PreparedHirIndexMethodLoop<'a>),
    OrUnion(PreparedHirOrUnionLoop<'a>),
    Materialized(PreparedHirMaterializedLoop<'a>),
    Registers(PreparedHirRegistersLoop<'a>),
    Coroutine(PreparedHirCoroutineLoop<'a>),
    Hash(PreparedHirHashLoop<'a>),
}

impl PreparedHirLoop<'_> {
    /// The cursors a DML statement closes once it has collected the rowids
    /// its loop finds: the table's, and those of the indexes the loop reads.
    pub(super) fn read_cursors(&self) -> Result<Vec<CursorID>> {
        match self {
            Self::BTree(prepared) => Ok(std::iter::once(prepared.cursor)
                .chain(prepared.table_cursor)
                .collect()),
            Self::OrUnion(prepared) => Ok(std::iter::once(prepared.table_cursor)
                .chain(
                    prepared
                        .branches
                        .iter()
                        .map(|branch| branch.cursor)
                        .filter(|cursor| *cursor != prepared.table_cursor),
                )
                .collect()),
            Self::IndexMethod(prepared) => Ok(vec![prepared.table_cursor, prepared.index_cursor]),
            _ => Err(LimboError::InternalError(
                "HIR DML target loop is not a table loop".into(),
            )),
        }
    }
}

/// A hash lookup's cursor, registers and labels, chosen before any loop
/// starts. See [HirSourceAccess::Hash].
pub(super) struct PreparedHirHashLoop<'a> {
    source: &'a hir::Source,
    lookup: &'a HirHashLookup,
    table: Arc<BTreeTable>,
    database: usize,
    /// Reads the whole table once to build the hash table.
    build_cursor: CursorID,
    hash_table: usize,
    /// One register per column of the table; the hash table keeps the
    /// columns in `columns` and leaves the others NULL.
    payload: usize,
    columns: Vec<usize>,
    /// How many registers the payload takes.
    payload_width: usize,
    /// The rowid of the row a lookup found. With a build input it is the
    /// temporary table's rowid, which nothing reads.
    rowid: usize,
    build_keys: usize,
    probe_keys: usize,
    /// The rowid of the previous loop's row, kept with a lookup that has to
    /// wait for rows the hash table moved to disk.
    probe_rowid: usize,
    /// 1 while the waiting lookups run after the previous loop ends.
    waiting_pass: usize,
    /// Where the waiting pass looks for the next match.
    waiting_next: BranchOffset,
    /// 1 once the hash table is built. The unmatched-row pass of an outer
    /// join before this loop emits the loops after it again, and that copy
    /// must not build the table a second time.
    built: usize,
    /// The materialized build input, when the hash table is built from the
    /// joined rows of earlier loops rather than from the whole table.
    build_input: Option<PreparedHirHashBuildInput<'a>>,
}

/// See [HirHashBuildInput]. Its loops run once, before the block's loops,
/// storing each joined row in a temporary table: the hash keys, then the
/// payload in its register layout. The hash table keeps that payload, and a
/// lookup writes it to the registers each supplied source is bound to.
struct PreparedHirHashBuildInput<'a> {
    input: &'a HirHashBuildInput,
    table: Arc<BTreeTable>,
    cursor: CursorID,
    /// Contiguous registers the loops fill with one row: the keys, then
    /// the payload.
    record: usize,
    sources: Vec<PreparedHirPayloadSource>,
}

/// Where a source a hash lookup supplies lives in the payload.
struct PreparedHirPayloadSource {
    source: SourceId,
    /// The columns the query reads, which the build input stores.
    columns: Vec<usize>,
    /// Its first register: one register per column of the source.
    start: usize,
    /// The register of its rowid, for a rowid table.
    rowid: Option<usize>,
}

impl PreparedHirLoop<'_> {
    fn cursor(&self) -> Option<CursorID> {
        match self {
            Self::BTree(prepared) => Some(prepared.cursor),
            Self::Virtual(prepared) => Some(prepared.cursor),
            Self::IndexMethod(prepared) => Some(prepared.index_cursor),
            Self::OrUnion(prepared) => Some(prepared.table_cursor),
            Self::Materialized(prepared) => Some(prepared.cursor),
            Self::Registers(_) | Self::Coroutine(_) | Self::Hash(_) => None,
        }
    }

    fn table_cursor(&self) -> Option<CursorID> {
        match self {
            Self::BTree(prepared) => prepared.table_cursor,
            Self::Virtual(_) => None,
            Self::IndexMethod(prepared) => Some(prepared.table_cursor),
            Self::OrUnion(_) => None,
            Self::Materialized(_) => None,
            Self::Registers(_) | Self::Coroutine(_) | Self::Hash(_) => None,
        }
    }
}

pub(super) struct HirBtreeLoop {
    cursor: Option<CursorID>,
    loop_start: BranchOffset,
    /// Reject the current row, then run this loop's advance operation.
    pub(super) next: BranchOffset,
    /// This access produced no row, so skip its advance operation.
    exhausted: BranchOffset,
    advance: HirLoopAdvance,
    left_join: Option<usize>,
}

/// An outer join's ON predicates, evaluated once its right side is positioned.
/// LEFT and FULL joins also NULL-extend that side when no row matched.
pub(super) struct HirLeftJoin {
    owner: SourceId,
    first_loop: usize,
    last_loop: usize,
    /// The first loop whose WHERE terms wait until `last_loop` records the
    /// match, like SQLite checks WHERE terms on a LEFT JOIN's right table
    /// after setting its match flag. For an outer hash join this is the
    /// probe loop: a term that drops a probe row must not keep it from
    /// matching a build row.
    where_from: usize,
    null_extends: bool,
    metadata: LeftJoinMetadata,
    null_cursors: Vec<CursorID>,
    /// Register-backed sources on the NULL-extended side, such as a
    /// recursive CTE's current row. They read private registers, filled
    /// from the shared row when their loop starts and set to NULL for the
    /// unmatched row, so the shared row stays intact for later outer rows.
    null_registers: Vec<HirNullRegisters>,
}

#[derive(Clone, Copy)]
struct HirNullRegisters {
    loop_index: usize,
    input: usize,
    private: usize,
    width: usize,
}

/// A RIGHT or FULL join. Its right source records each row that matched a
/// left row; after the loops finish, a second pass emits the right rows no
/// left row matched, with the left side NULL. This is SQLite's nested-loop
/// RIGHT JOIN: matched rows come first, then the rest.
struct HirRightOuterJoin {
    source: SourceId,
    /// RIGHT, FULL, or an anti-join run as a hash join, whose matched build
    /// rows are never emitted.
    kind: hir::JoinKind,
    /// The source whose ON terms decide a match: `source` itself, or the
    /// probe table of an outer hash join built from `source`.
    owner: SourceId,
    loop_index: usize,
    matched: CursorID,
    /// How the matched set identifies a right row.
    key: HirMatchedKey,
    empty_record: usize,
    /// The ephemeral table of a materialized right subquery, or of a hash
    /// lookup's build input. Its rowids key the matched set, and the
    /// unmatched pass scans it again.
    materialized: Option<CursorID>,
    /// The register a hash lookup over a build input returns the matched
    /// row's temporary-table rowid in.
    matched_rowid: Option<usize>,
    /// The probe loop of an outer hash join that runs under earlier loops.
    /// Each pass of the probe loop starts with an empty matched set and
    /// ends with its own unmatched pass, which NULL-fills
    /// only the probe table and keeps the rows of the earlier loops.
    probe_pass: Option<usize>,
}

/// How the matched-row set of a RIGHT or FULL JOIN identifies a right row.
enum HirMatchedKey {
    /// The row's rowid: the set is an ephemeral rowid table. A virtual
    /// table's rows are keyed by the rowid it reports.
    Rowid,
    /// The primary key columns of a WITHOUT ROWID table, in their declared
    /// order: the set is an ephemeral index over them.
    PrimaryKey(Vec<usize>),
}

/// An outer hash join: `build` was built into a hash table, and `probe`,
/// the right side of a LEFT or FULL join, is scanned and looks it up.
struct HirHashOuterJoin {
    probe: SourceId,
    build: SourceId,
    build_loop: usize,
    kind: hir::JoinKind,
}

/// The block's outer hash join, if any: a hash lookup whose previous loop
/// reads the right side of a LEFT JOIN that stays one, or of a FULL JOIN.
fn hash_outer_join(from: &hir::From, loops_plan: HirLoopsPlan<'_>) -> Option<HirHashOuterJoin> {
    loops_plan
        .loops
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(build_loop, planned_loop)| {
            if !matches!(planned_loop.access, HirSourceAccess::Hash(_)) {
                return None;
            }
            let probe = loops_plan.loops[build_loop - 1].source;
            let kind = from.joins.iter().find(|join| join.right == probe)?.kind;
            let outer = match kind {
                hir::JoinKind::Left => !loops_plan.inner_joins.contains(&probe),
                // A hash anti-join: the build rows the probe matches are
                // remembered, and only the others come out, after the
                // probe loop.
                hir::JoinKind::Full | hir::JoinKind::Anti => true,
                _ => false,
            };
            outer.then_some(HirHashOuterJoin {
                probe,
                build: planned_loop.source,
                build_loop,
                kind,
            })
        })
}

/// Labels of one semi-join or anti-join loop.
struct HirSemiAntiLabels {
    /// Where an anti-join loop that found no match continues: the next loop,
    /// or the rest of the block.
    body: BranchOffset,
}

/// The joins of one FROM clause that loop lowering supports.
#[derive(Default)]
struct HirOuterJoins {
    /// Sources owning ON predicates, and whether each is NULL-extended.
    owners: Vec<(SourceId, bool)>,
    /// Right sources of RIGHT and FULL joins.
    right_outer: Vec<(SourceId, hir::JoinKind)>,
}

#[derive(Clone, Copy)]
enum HirPredicateKind {
    Join(SourceId),
    Where {
        /// The loop of the block's RIGHT or FULL join, in the main pass.
        /// WHERE-clause terms ready before it wait for it: SQLite records a
        /// right row as matched once the ON terms pass, before any WHERE term
        /// runs, so a WHERE term must not hide a match.
        right_join_loop: Option<usize>,
        /// The block has a RIGHT or FULL JOIN, so a constant ON term of an
        /// inner join runs inside the first loop instead of before the
        /// loops: `JOIN t ON false RIGHT JOIN u` still emits the unmatched
        /// rows of `u`, which the pass after the loops produces. A constant
        /// WHERE term still ends the block before the loops.
        constant_on_terms_in_loops: bool,
        /// These loops run again for the unmatched rows of the RIGHT or
        /// FULL join at this loop. A WHERE term ready by then runs again
        /// even when the planner used it for a seek, since the seek's loop
        /// is NULL-extended now.
        unmatched_pass: Option<usize>,
    },
}

fn is_hir_ungrouped_aggregate(block: &hir::QueryBlock) -> bool {
    match &block.body {
        hir::QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } => grouping.keys.is_empty(),
        hir::QueryBlockBody::Select { grouping: None, .. } => block.aggregate_count != 0,
        _ => false,
    }
}

fn hir_grouping(block: &hir::QueryBlock) -> Option<&hir::Grouping> {
    let hir::QueryBlockBody::Select {
        grouping: Some(grouping),
        ..
    } = &block.body
    else {
        return None;
    };
    (!grouping.keys.is_empty()).then_some(grouping)
}

struct HirAggregateCollector<'a> {
    block: hir::QueryBlockId,
    aggregates: Vec<Option<&'a hir::FunctionCall>>,
}

/// Columns of `query`'s own sources that its nested queries read. Only a
/// nested query can see them, through correlation. A correlated subquery
/// evaluated after aggregation reads them from the captured group row, like
/// the bare columns of the query itself.
fn hir_nested_outer_column_reads(
    document: &HirDocument,
    query: &hir::Query,
) -> Vec<hir::ColumnRef> {
    let owned_by_query = |source: SourceId| {
        matches!(
            document.source(source).map(|source| source.owner),
            Some(hir::SourceOwner::QueryBlock(block)) if block.query == query.id
        )
    };
    let mut reads = Vec::new();
    for nested in document
        .queries
        .iter()
        .filter(|nested| nested.id != query.id)
    {
        if !nested.captures.iter().any(|source| owned_by_query(*source)) {
            continue;
        }
        for usage in nested.direct_column_usage(|source| document.source(source)) {
            if owned_by_query(usage.reference.source) && !reads.contains(&usage.reference) {
                reads.push(usage.reference);
            }
        }
    }
    reads
}

struct HirBareAggregateColumnCollector<'a> {
    document: &'a HirDocument,
    columns: Vec<hir::ColumnRef>,
    rowids: Vec<SourceId>,
}

impl<'expr> hir::ExprVisitor<'expr> for HirBareAggregateColumnCollector<'expr> {
    type Context = ();
    type Output = ();
    type Error = LimboError;

    fn child(&mut self, expression: &'expr hir::Expr, index: usize) -> Option<&'expr hir::Expr> {
        if let hir::Expr::Output(output) = expression {
            return (index == 0).then(|| {
                &self
                    .document
                    .output(*output)
                    .expect("validated HIR output reference exists")
                    .expr
            });
        }
        expression.child(index)
    }

    fn pre_order(
        &mut self,
        _parent: &'expr hir::Expr,
        _context: &mut (),
        _child_index: usize,
        child: &'expr hir::Expr,
    ) -> Result<ControlFlow<(), ()>> {
        if matches!(
            child,
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Aggregate { .. })
        ) {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    }

    fn post_order(
        &mut self,
        expression: &'expr hir::Expr,
        _context: (),
        _children: &[()],
    ) -> Result<()> {
        match expression {
            hir::Expr::Column(reference) if !self.columns.contains(reference) => {
                self.columns.push(*reference);
            }
            hir::Expr::RowId(source) if !self.rowids.contains(source) => {
                self.rowids.push(*source);
            }
            _ => {}
        }
        Ok(())
    }
}

impl<'expr> HirBareAggregateColumnCollector<'expr> {
    fn collect(&mut self, expression: &'expr hir::Expr) -> Result<()> {
        if matches!(
            expression,
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Aggregate { .. })
        ) {
            return Ok(());
        }
        expression.walk((), self)
    }
}

impl<'expr> hir::ExprVisitor<'expr> for HirAggregateCollector<'expr> {
    type Context = ();
    type Output = ();
    type Error = LimboError;

    fn pre_order(
        &mut self,
        _parent: &'expr hir::Expr,
        _context: &mut (),
        _child_index: usize,
        _child: &'expr hir::Expr,
    ) -> Result<ControlFlow<(), ()>> {
        Ok(ControlFlow::Continue(()))
    }

    fn post_order(
        &mut self,
        expression: &'expr hir::Expr,
        _context: (),
        _children: &[()],
    ) -> Result<()> {
        let hir::Expr::Function(call) = expression else {
            return Ok(());
        };
        let hir::FunctionEvaluation::Aggregate { id, .. } = call.evaluation else {
            return Ok(());
        };
        // A subquery's aggregate of the enclosing block's columns belongs
        // to that block, which collects it.
        if id.block != self.block {
            return Ok(());
        }
        if id.index >= self.aggregates.len() {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate {id:?} is outside query block {:?}",
                self.block
            )));
        }
        if self.aggregates[id.index].replace(call).is_some() {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate {id:?} has more than one definition"
            )));
        }
        Ok(())
    }
}

fn collect_hir_aggregates<'a>(
    document: &'a HirDocument,
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
) -> Result<Vec<&'a hir::FunctionCall>> {
    let mut collector = HirAggregateCollector {
        block: block.id,
        aggregates: (0..block.aggregate_count).map(|_| None).collect(),
    };
    let mut collect = |expression: &'a hir::Expr| expression.walk((), &mut collector);
    // Like SQLite, an aggregate of this block's columns may be written in a
    // subquery of the block.
    for subquery in document
        .queries
        .iter()
        .filter(|subquery| subquery.parent == Some(query.id))
    {
        for subquery_block in &subquery.blocks {
            for output in &subquery_block.outputs {
                collect(&output.expr)?;
            }
            if let hir::QueryBlockBody::Select {
                filter, grouping, ..
            } = &subquery_block.body
            {
                if let Some(filter) = filter {
                    collect(filter)?;
                }
                if let Some(grouping) = grouping {
                    for key in &grouping.keys {
                        collect(key)?;
                    }
                    if let Some(having) = &grouping.having {
                        collect(having)?;
                    }
                }
            }
        }
        for term in &subquery.order_by {
            collect(&term.expr)?;
        }
    }
    for output in &block.outputs {
        collect(&output.expr)?;
    }
    if let hir::QueryBlockBody::Select {
        filter, grouping, ..
    } = &block.body
    {
        if let Some(filter) = filter {
            collect(filter)?;
        }
        if let Some(grouping) = grouping {
            for key in &grouping.keys {
                collect(key)?;
            }
            if let Some(having) = &grouping.having {
                collect(having)?;
            }
        }
    }
    for term in &query.order_by {
        collect(&term.expr)?;
    }
    for window in &block.windows {
        for expression in hir_window_expressions(window) {
            collect(expression)?;
        }
    }
    collector
        .aggregates
        .into_iter()
        .enumerate()
        .map(|(index, aggregate)| {
            aggregate.ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR query block {:?} has no definition for aggregate {index}",
                    block.id
                ))
            })
        })
        .collect()
}

/// Expressions owned by a window definition rather than by a window call.
fn hir_window_expressions(window: &hir::ResolvedWindow) -> impl Iterator<Item = &hir::Expr> {
    let bounds = [Some(&window.frame.start), window.frame.end.as_ref()]
        .into_iter()
        .flatten()
        .filter_map(|bound| match bound {
            hir::WindowFrameBound::Preceding(expression)
            | hir::WindowFrameBound::Following(expression) => Some(expression.as_ref()),
            hir::WindowFrameBound::CurrentRow
            | hir::WindowFrameBound::UnboundedPreceding
            | hir::WindowFrameBound::UnboundedFollowing => None,
        });
    window
        .partition_by
        .iter()
        .map(|term| &term.expr)
        .chain(window.order_by.iter().map(|term| &term.expr))
        .chain(bounds)
}

fn initialize_hir_aggregates<'a>(
    program: &mut ProgramBuilder,
    document: &'a HirDocument,
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
) -> Result<HirAggregateState<'a>> {
    let calls = collect_hir_aggregates(document, query, block)?;
    let mut aggregates = Vec::with_capacity(calls.len());
    for (index, call) in calls.into_iter().enumerate() {
        let hir::FunctionEvaluation::Aggregate { id, .. } = call.evaluation else {
            unreachable!("aggregate collection only returns aggregate calls")
        };
        if id != hir::AggregateId::new(block.id, index) {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate {id:?} does not match block aggregate {index}"
            )));
        }
        let func = hir_aggregate_function(call)?;
        let percentile_fraction =
            if matches!(func, AggFunc::PercentileCont | AggFunc::PercentileDisc) {
                let hir::FunctionArguments::OrderedSet { direct, .. } = &call.arguments else {
                    return Err(LimboError::InternalError(format!(
                        "HIR percentile aggregate {id:?} has non-ordered-set arguments"
                    )));
                };
                let [fraction] = direct.as_slice() else {
                    return Err(LimboError::InternalError(format!(
                        "HIR percentile aggregate {id:?} does not have one direct argument"
                    )));
                };
                validate_hir_percentile_fraction(document, block, fraction, &func)?;
                let register = program.alloc_register();
                super::expr::translate_expr(program, document, fraction, register)?;
                crate::translate::aggregation::emit_percentile_fraction_range_check(
                    program, register,
                );
                Some(register)
            } else {
                None
            };
        let accumulator = program.alloc_register();
        program.emit_insn(Insn::Null {
            dest: accumulator,
            dest_end: None,
        });
        program.bind_aggregate_result(id, accumulator);
        let distinct = if matches!(call.arguments.distinctness(), Some(Distinctness::Distinct)) {
            let hir::FunctionArguments::Expressions { values, facts, .. } = &call.arguments else {
                return Err(LimboError::InternalError(format!(
                    "HIR DISTINCT aggregate {id:?} has non-expression arguments"
                )));
            };
            if values.len() != 1 || facts.len() != 1 {
                return Err(LimboError::InternalError(format!(
                    "HIR DISTINCT aggregate {id:?} does not have one argument"
                )));
            }
            let hash_table = program.alloc_hash_table_id();
            program.emit_insn(Insn::HashClear {
                hash_table_id: hash_table,
            });
            emit_explain!(
                program,
                false,
                EqpDetail::DistinctAggregate {
                    function: func.to_string(),
                }
            );
            Some(HirAggregateDistinctSet {
                hash_table,
                collation: facts[0]
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
                    .unwrap_or(CollationSeq::Binary),
            })
        } else {
            None
        };
        let ordered = match &call.arguments {
            _ if matches!(func, AggFunc::Min | AggFunc::Max) => None,
            hir::FunctionArguments::Expressions {
                values, order_by, ..
            } if !order_by.is_empty() => Some(open_hir_ordered_arguments(
                program,
                index,
                values,
                order_by,
                distinct.is_some(),
            )?),
            _ => None,
        };
        aggregates.push(HirAggregateRuntime {
            call,
            accumulator,
            distinct,
            percentile_fraction,
            ordered,
        });
    }
    let mut bare = HirBareAggregateColumnCollector {
        document,
        columns: Vec::new(),
        rowids: Vec::new(),
    };
    for output in &block.outputs {
        bare.collect(&output.expr)?;
    }
    if let hir::QueryBlockBody::Select {
        grouping: Some(grouping),
        ..
    } = &block.body
    {
        if let Some(having) = &grouping.having {
            bare.collect(having)?;
        }
    }
    for term in &query.order_by {
        bare.collect(&term.expr)?;
    }
    for window in &block.windows {
        for expression in hir_window_expressions(window) {
            bare.collect(expression)?;
        }
    }
    for reference in hir_nested_outer_column_reads(document, query) {
        if !bare.columns.contains(&reference) {
            bare.columns.push(reference);
        }
    }
    bare.columns
        .sort_unstable_by_key(|reference| (reference.source.index(), reference.column));
    bare.rowids.sort_unstable();
    let min_max_row = if bare.columns.is_empty() && bare.rowids.is_empty() {
        None
    } else {
        hir_min_max_row(program, &aggregates)?
    };
    Ok(HirAggregateState {
        aggregates,
        bare_columns: bare.columns,
        bare_rowids: bare.rowids,
        min_max_row,
    })
}

/// The one min() or max() aggregate whose row bare columns come from, as in
/// SQLite. With two or more, SQLite's choice is arbitrary, and bare columns
/// come from the first row; so they do when a custom type orders the values.
fn hir_min_max_row(
    program: &mut ProgramBuilder,
    aggregates: &[HirAggregateRuntime<'_>],
) -> Result<Option<HirMinMaxRow>> {
    let mut min_max = Vec::new();
    for aggregate in aggregates {
        let function = hir_aggregate_function(aggregate.call)?;
        if matches!(function, AggFunc::Min | AggFunc::Max) {
            min_max.push((aggregate, function));
        }
    }
    let [(aggregate, function)] = min_max.as_slice() else {
        return Ok(None);
    };
    // Its value is known only once its kept rows are stepped.
    if aggregate.ordered.is_some() {
        return Ok(None);
    }
    let hir::FunctionArguments::Expressions { values, facts, .. } = &aggregate.call.arguments
    else {
        return Ok(None);
    };
    let ([_], [fact]) = (values.as_slice(), facts.as_slice()) else {
        return Ok(None);
    };
    if crate::translate::order_by::custom_type_comparator_from_type_fact(&fact.type_fact).is_some()
    {
        return Ok(None);
    }
    Ok(Some(HirMinMaxRow {
        accumulator: aggregate.accumulator,
        function: function.clone(),
        collation: fact
            .collation
            .as_ref()
            .map(|collation| *collation.value())
            .unwrap_or(CollationSeq::Binary),
        previous: program.alloc_register(),
    }))
}

/// After the aggregates stepped: jump to `skip` unless this row's bare
/// columns are kept. `kept` is nonzero once a row was kept. Without a
/// min() or max() row, only the first row is kept.
fn emit_hir_bare_row_test(
    program: &mut ProgramBuilder,
    kept: usize,
    min_max_row: Option<&HirMinMaxRow>,
    skip: BranchOffset,
) {
    let Some(row) = min_max_row else {
        program.emit_insn(Insn::If {
            reg: kept,
            target_pc: skip,
            jump_if_null: false,
        });
        return;
    };
    let current = program.alloc_register();
    program.emit_insn(Insn::AggValue {
        acc_reg: row.accumulator,
        dest_reg: current,
        func: AccumulatorFunc::Agg(row.function.clone()),
    });
    let take = program.allocate_label();
    let compare = program.allocate_label();
    let previous_null = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: kept,
        target_pc: compare,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Goto { target_pc: take });
    program.preassign_label_to_next_insn(compare);
    program.emit_insn(Insn::IsNull {
        reg: row.previous,
        target_pc: previous_null,
    });
    program.emit_insn(Insn::Eq {
        lhs: row.previous,
        rhs: current,
        target_pc: skip,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: Some(row.collation),
    });
    program.emit_insn(Insn::Goto { target_pc: take });
    program.preassign_label_to_next_insn(previous_null);
    program.emit_insn(Insn::IsNull {
        reg: current,
        target_pc: skip,
    });
    program.preassign_label_to_next_insn(take);
    program.emit_insn(Insn::Copy {
        src_reg: current,
        dst_reg: row.previous,
        extra_amount: 0,
    });
}

fn open_hir_ordered_arguments(
    program: &mut ProgramBuilder,
    aggregate: usize,
    values: &[hir::Expr],
    order_by: &[hir::OrderTerm],
    distinct: bool,
) -> Result<HirOrderedArguments> {
    let distinct_on_order_key =
        distinct && matches!((values, order_by), ([value], [term]) if value.equivalent(&term.expr));
    let values = values.len();
    // An index orders by collation only, not by a custom type's order.
    if order_by.iter().any(|term| {
        crate::translate::order_by::custom_type_comparator_from_type_fact(&term.type_fact).is_some()
    }) {
        crate::bail_parse_error!("ORDER BY clause is not supported yet in aggregate functions");
    }
    let column = |position: usize, order: SortOrder, nulls_order, collation| IndexColumn {
        name: format!("c{position}"),
        order,
        nulls_order,
        pos_in_table: position,
        collation,
        default: None,
        expr: None,
    };
    let mut columns = order_by
        .iter()
        .enumerate()
        .map(|(position, term)| {
            column(
                position,
                term.order,
                term.nulls,
                term.collation.as_ref().map(|collation| *collation.value()),
            )
        })
        .collect_vec();
    let sequence_order = if distinct_on_order_key {
        SortOrder::Desc
    } else {
        SortOrder::Asc
    };
    columns.push(column(order_by.len(), sequence_order, None, None));
    for position in order_by.len() + 1..order_by.len() + 1 + 2 * values {
        columns.push(column(position, SortOrder::Asc, None, None));
    }
    let index = Arc::new(Index {
        constraint_name: None,
        columns,
        name: format!("ephemeral_aggregate_order_{aggregate}"),
        table_name: String::new(),
        root_page: 0,
        unique: false,
        ephemeral: true,
        has_rowid: false,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: false,
    });
    let sequence = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: sequence,
    });
    Ok(HirOrderedArguments {
        cursor,
        terms: order_by.len(),
        values,
        sequence,
        distinct_on_order_key,
    })
}

/// Keep one row of an aggregate with an argument ORDER BY for later.
fn emit_hir_ordered_argument_row(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    aggregate: &HirAggregateRuntime<'_>,
    ordered: &HirOrderedArguments,
) -> Result<()> {
    let hir::FunctionArguments::Expressions {
        values, order_by, ..
    } = &aggregate.call.arguments
    else {
        return Err(LimboError::InternalError(
            "HIR ordered aggregate has no argument expressions".into(),
        ));
    };
    let width = ordered.terms + 1 + 2 * ordered.values;
    let start = program.alloc_registers(width);
    for (position, term) in order_by.iter().enumerate() {
        super::expr::translate_expr(program, document, &term.expr, start + position)?;
    }
    program.emit_insn(Insn::AddImm {
        register: ordered.sequence,
        value: 1,
    });
    program.emit_insn(Insn::Copy {
        src_reg: ordered.sequence,
        dst_reg: start + ordered.terms,
        extra_amount: 0,
    });
    let arguments = start + ordered.terms + 1;
    for (position, value) in values.iter().enumerate() {
        if ordered.distinct_on_order_key {
            program.emit_insn(Insn::Copy {
                src_reg: start,
                dst_reg: arguments + position,
                extra_amount: 0,
            });
        } else {
            super::expr::translate_expr(program, document, value, arguments + position)?;
        }
    }
    for position in 0..ordered.values {
        program.emit_insn(Insn::GetSubtype {
            src_reg: arguments + position,
            dest_reg: arguments + ordered.values + position,
        });
    }
    let duplicate = match &aggregate.distinct {
        Some(distinct) if !ordered.distinct_on_order_key => {
            let duplicate = program.allocate_label();
            program.emit_insn(Insn::HashDistinct {
                data: Box::new(HashDistinctData {
                    hash_table_id: distinct.hash_table,
                    key_start_reg: arguments,
                    num_keys: 1,
                    collations: vec![distinct.collation],
                    target_pc: duplicate,
                }),
            });
            Some(duplicate)
        }
        _ => None,
    };
    let record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(start),
        count: to_u32(width),
        dest_reg: to_u32(record),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: ordered.cursor,
        record_reg: record,
        unpacked_start: Some(start),
        unpacked_count: Some(to_u32(width)),
        flags: crate::vdbe::insn::IdxInsertFlags::new(),
    });
    if let Some(duplicate) = duplicate {
        program.preassign_label_to_next_insn(duplicate);
    }
    Ok(())
}

/// Step an aggregate with an argument ORDER BY through its kept rows, in
/// order.
fn emit_hir_ordered_aggregate_steps(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    aggregate: &HirAggregateRuntime<'_>,
    ordered: &HirOrderedArguments,
) -> Result<()> {
    let hir::FunctionArguments::Expressions { values, .. } = &aggregate.call.arguments else {
        return Err(LimboError::InternalError(
            "HIR ordered aggregate has no argument expressions".into(),
        ));
    };
    let done = program.allocate_label();
    let row = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: ordered.cursor,
        pc_if_empty: done,
    });
    program.preassign_label_to_next_insn(row);
    let bound = program.expression_register_count();
    let start = program.alloc_registers(ordered.values.max(1));
    let subtype = program.alloc_register();
    for (position, value) in values.iter().enumerate() {
        program.emit_insn(Insn::Column {
            cursor_id: ordered.cursor,
            column: ordered.terms + 1 + position,
            dest: start + position,
            default: None,
        });
        program.emit_insn(Insn::Column {
            cursor_id: ordered.cursor,
            column: ordered.terms + 1 + ordered.values + position,
            dest: subtype,
            default: None,
        });
        program.emit_insn(Insn::SetSubtype {
            subtype_reg: subtype,
            dest_reg: start + position,
        });
        program.bind_expression_register(value, start + position);
    }
    let distinct = aggregate
        .distinct
        .as_ref()
        .filter(|_| ordered.distinct_on_order_key)
        .map(|distinct| HirAggregateDistinct {
            hash_table: distinct.hash_table,
            collation: distinct.collation,
            on_conflict: program.allocate_label(),
        });
    crate::translate::aggregation::translate_hir_sorted_aggregation_step(
        program,
        document,
        aggregate.call,
        aggregate.accumulator,
        distinct.as_ref(),
    )?;
    if let Some(distinct) = &distinct {
        program.preassign_label_to_next_insn(distinct.on_conflict);
    }
    program.truncate_expression_registers(bound);
    program.emit_insn(Insn::Next {
        cursor_id: ordered.cursor,
        is_index: program.cursor_is_index(ordered.cursor),
        pc_if_next: row,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(done);
    Ok(())
}

fn validate_hir_percentile_fraction(
    document: &HirDocument,
    block: &hir::QueryBlock,
    fraction: &hir::Expr,
    function: &AggFunc,
) -> Result<()> {
    let mut invalid = false;
    fraction.for_each(&mut |expression| {
        invalid |= matches!(expression, hir::Expr::Subquery(_));
    });
    document.visit_expr_sources(fraction, &mut |source| {
        invalid |= matches!(
            document.source(source).map(|source| source.owner),
            Some(hir::SourceOwner::QueryBlock(owner)) if owner == block.id
        );
    });
    if invalid {
        crate::bail_parse_error!(
            "the fraction argument of {}() must be a constant expression that does not depend on the aggregated rows",
            function
        );
    }
    Ok(())
}

fn emit_hir_aggregate_steps(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    state: &HirAggregateState<'_>,
) -> Result<()> {
    for aggregate in &state.aggregates {
        let filter = aggregate.call.evaluation.filter();
        if let Some(ordered) = &aggregate.ordered {
            let skip = filter.map(|_| program.allocate_label());
            if let (Some(filter), Some(skip)) = (filter, skip) {
                let register = program.alloc_register();
                super::expr::translate_expr(program, document, filter, register)?;
                program.emit_insn(Insn::IfNot {
                    reg: register,
                    target_pc: skip,
                    jump_if_null: true,
                });
            }
            emit_hir_ordered_argument_row(program, document, aggregate, ordered)?;
            if let Some(skip) = skip {
                program.preassign_label_to_next_insn(skip);
            }
            continue;
        }
        let distinct = aggregate
            .distinct
            .as_ref()
            .map(|distinct| HirAggregateDistinct {
                hash_table: distinct.hash_table,
                collation: distinct.collation,
                on_conflict: program.allocate_label(),
            });
        let skip = distinct
            .as_ref()
            .map(|distinct| distinct.on_conflict)
            .or_else(|| filter.map(|_| program.allocate_label()));
        if let Some(filter) = filter {
            let register = program.alloc_register();
            super::expr::translate_expr(program, document, filter, register)?;
            program.emit_insn(Insn::IfNot {
                reg: register,
                target_pc: skip.expect("aggregate filter has a skip label"),
                jump_if_null: true,
            });
        }
        translate_hir_aggregation_step(
            program,
            document,
            aggregate.call,
            aggregate.accumulator,
            distinct.as_ref(),
            aggregate.percentile_fraction,
        )?;
        if let Some(skip) = skip {
            program.preassign_label_to_next_insn(skip);
        }
    }
    Ok(())
}

fn finalize_hir_aggregates(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    state: &HirAggregateState<'_>,
) -> Result<()> {
    for aggregate in &state.aggregates {
        if let Some(ordered) = &aggregate.ordered {
            emit_hir_ordered_aggregate_steps(program, document, aggregate, ordered)?;
        }
    }
    for aggregate in &state.aggregates {
        program.emit_insn(Insn::AggFinal {
            register: aggregate.accumulator,
            func: AccumulatorFunc::Agg(hir_aggregate_function(aggregate.call)?),
        });
    }
    Ok(())
}

fn initialize_hir_aggregate_captures(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    loops: &[HirPlannedLoop],
    state: &HirAggregateState<'_>,
) -> Result<(usize, Vec<HirAggregateCapture>)> {
    let first_row = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: first_row,
    });
    let mut captures = Vec::with_capacity(loops.len());
    for loop_source in loops.iter().flat_map(HirPlannedLoop::sources) {
        let mut columns = state
            .bare_columns
            .iter()
            .filter(|reference| reference.source == loop_source)
            .map(|reference| reference.column)
            .collect::<Vec<_>>();
        columns.sort_unstable();
        columns.dedup();
        let needs_rowid = state.bare_rowids.contains(&loop_source);
        if columns.is_empty() && !needs_rowid {
            continue;
        }
        if captures
            .iter()
            .any(|capture: &HirAggregateCapture| capture.source == loop_source)
        {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate query has more than one loop for source {loop_source}"
            )));
        }
        let source = document.source(loop_source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR aggregate capture references missing source {loop_source}"
            ))
        })?;
        let columns_start = if source.columns.is_empty() {
            0
        } else {
            let start = program.alloc_registers(source.columns.len());
            program.emit_insn(Insn::Null {
                dest: start,
                dest_end: (source.columns.len() > 1).then_some(start + source.columns.len() - 1),
            });
            start
        };
        let rowid = needs_rowid.then(|| {
            let register = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: register,
                dest_end: None,
            });
            register
        });
        captures.push(HirAggregateCapture {
            source: source.id,
            columns_start,
            columns,
            rowid,
        });
    }
    Ok((first_row, captures))
}

fn emit_hir_aggregate_capture(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    first_row: usize,
    captures: &[HirAggregateCapture],
    min_max_row: Option<&HirMinMaxRow>,
) -> Result<()> {
    // With no bare column to keep, there is nothing to capture per row.
    if captures.is_empty() {
        return Ok(());
    }
    let captured = program.allocate_label();
    emit_hir_bare_row_test(program, first_row, min_max_row, captured);
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: first_row,
    });
    for capture in captures {
        let source = document.source(capture.source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR aggregate capture references missing source {}",
                capture.source
            ))
        })?;
        for column in &capture.columns {
            super::expr::translate_expr(
                program,
                document,
                &hir::Expr::column(source.id, *column),
                capture.columns_start + column,
            )?;
        }
        if let Some(rowid) = capture.rowid {
            super::expr::translate_expr(program, document, &hir::Expr::rowid(source.id), rowid)?;
        }
    }
    program.preassign_label_to_next_insn(captured);
    Ok(())
}

fn bind_hir_aggregate_captures(program: &mut ProgramBuilder, captures: &[HirAggregateCapture]) {
    for capture in captures {
        program.bind_source(
            capture.source,
            SourceBinding::Registers {
                start: capture.columns_start,
                rowid: capture.rowid,
            },
        );
    }
}

fn emit_btree_loops(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    body: HirLoopBody<'_>,
) -> Result<()> {
    let hir::QueryBlockBody::Select { grouping, .. } = &block.body else {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} has unsupported B-tree loop clauses",
            block.id
        )));
    };
    match (&body, grouping) {
        (HirLoopBody::Rows(..), None) if block.aggregate_count == 0 => {}
        (HirLoopBody::UngroupedAggregate(..), None) if block.aggregate_count != 0 => {}
        (HirLoopBody::UngroupedAggregate(..), Some(grouping)) if grouping.keys.is_empty() => {}
        (HirLoopBody::Group(_), Some(grouping)) if !grouping.keys.is_empty() => {}
        _ => {
            return Err(LimboError::InternalError(format!(
                "HIR query block {:?} has a mismatched loop body",
                block.id
            )));
        }
    }
    if block.from.is_some() == block_plan.loops.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} has {} loops for its FROM clause",
            block.id,
            block_plan.loops.len()
        )));
    }
    emit_join_loops(
        program,
        plan,
        block.from.as_ref(),
        HirLoopsPlan::of_block(block_plan),
        body,
    )
}

/// Emit the nested loops of a DML target joined with a FROM clause around
/// `body`, which runs for every joined row and continues at its label to
/// reject the row.
pub(super) fn emit_dml_join_loops(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    from: Option<&hir::From>,
    loops_plan: HirLoopsPlan<'_>,
    body: &dyn Fn(&mut ProgramBuilder, BranchOffset) -> Result<()>,
) -> Result<()> {
    emit_join_loops(program, plan, from, loops_plan, HirLoopBody::Dml(body))
}

/// Emit the nested loops of a FROM clause, or of a DML target joined with
/// one, around `body`. A plan without loops runs the body once.
fn emit_join_loops(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    from: Option<&hir::From>,
    loops_plan: HirLoopsPlan<'_>,
    body: HirLoopBody<'_>,
) -> Result<()> {
    let document = &plan.document;
    let mut joins = HirOuterJoins::default();
    // An outer hash join `b LEFT JOIN p` (or FULL) built from b and probed
    // by p runs like `p RIGHT JOIN b` in loop order: the ON
    // terms of p run after each lookup into b, the b rows they match are
    // recorded, and after the loops the other b rows run with p NULL. A
    // FULL join also gives each p row without a match a NULL b row.
    let hash_outer = from.and_then(|from| hash_outer_join(from, loops_plan));
    if let Some(from) = from {
        if !supported_btree_from(document, from, &mut joins) {
            return Err(LimboError::InternalError(
                "HIR FROM clause contains an unsupported join".into(),
            ));
        }
        joins
            .owners
            .retain(|(owner, _)| !loops_plan.inner_joins.contains(owner));
        // The loops that fill a hash lookup's build input run the outer
        // joins among them; the block's loops leave those to them, and a
        // build input leaves the outer joins of later loops to the block.
        let supplied_by_input = loops_plan
            .loops
            .iter()
            .filter_map(|planned_loop| match &planned_loop.access {
                HirSourceAccess::Hash(lookup) => lookup.build_input.as_deref(),
                _ => None,
            })
            .flat_map(|input| input.sources.iter().copied())
            .collect::<Vec<_>>();
        let handled_here = |source: SourceId| {
            loops_plan
                .loops
                .iter()
                .any(|planned_loop| planned_loop.source == source)
                || (!loops_plan.build_input && !supplied_by_input.contains(&source))
        };
        joins.owners.retain(|(owner, _)| handled_here(*owner));
        joins
            .right_outer
            .retain(|(source, _)| handled_here(*source));
        if let Some(hash_outer) = &hash_outer {
            for (owner, null_extends) in &mut joins.owners {
                if *owner == hash_outer.probe {
                    *null_extends = hash_outer.kind == hir::JoinKind::Full;
                }
            }
            joins
                .right_outer
                .retain(|(source, _)| *source != hash_outer.probe);
            joins.right_outer.push((hash_outer.build, hash_outer.kind));
        }
    }
    if loops_plan.predicates.iter().any(|predicate| {
        !predicate.consumed
            && predicate
                .from_outer_join
                .is_some_and(|source| !joins.owners.iter().any(|(owner, _)| *owner == source))
    }) {
        return Err(LimboError::InternalError(
            "HIR FROM clause contains an unsupported outer-join predicate owner".into(),
        ));
    }

    let block_done = program.allocate_label();
    let prepared_loops = loops_plan
        .loops
        .iter()
        .map(|planned_loop| {
            let streams = from.is_some_and(|from| hir_loop_streams(document, from, planned_loop));
            prepare_hir_loop(program, plan, planned_loop, streams)
        })
        .collect::<Result<Vec<_>>>()?;
    // The build inputs of hash lookups are filled before the loops.
    for prepared in &prepared_loops {
        let PreparedHirLoop::Hash(prepared) = prepared else {
            continue;
        };
        if let Some(input) = &prepared.build_input {
            emit_hir_hash_build_input(program, plan, from, loops_plan, prepared, input)?;
        }
    }
    let aggregate_captures = matches!(&body, HirLoopBody::UngroupedAggregate(..))
        .then(|| {
            let HirLoopBody::UngroupedAggregate(state, _) = &body else {
                unreachable!()
            };
            initialize_hir_aggregate_captures(program, document, loops_plan.loops, state)
        })
        .transpose()?;
    let left_joins = prepare_hir_left_joins(
        program,
        loops_plan,
        &prepared_loops,
        &joins.owners,
        hash_outer.as_ref(),
    )?;
    let right_outer = prepare_hir_right_outer_joins(
        program,
        loops_plan,
        &prepared_loops,
        &joins,
        hash_outer.as_ref(),
    )?;
    let semi_anti = loops_plan
        .loops
        .iter()
        .map(|planned_loop| {
            // The probe of a hash anti-join runs as the join's unmatched
            // pass, not as a nested anti-join loop.
            if hash_outer.as_ref().is_some_and(|hash_outer| {
                hash_outer.probe == planned_loop.source && hash_outer.kind == hir::JoinKind::Anti
            }) {
                return None;
            }
            from.and_then(|from| {
                from.joins.iter().find_map(|join| {
                    (join.right == planned_loop.source
                        && matches!(join.kind, hir::JoinKind::Semi | hir::JoinKind::Anti))
                    .then_some(join.kind)
                })
            })
        })
        .collect::<Vec<_>>();
    // A semi-join or anti-join loop comes after every other loop: it only
    // checks whether the rows before it have a match.
    if let Some(first) = semi_anti.iter().position(Option::is_some) {
        if first == 0 || semi_anti[first..].iter().any(Option::is_none) {
            return Err(LimboError::InternalError(
                "HIR semi-join or anti-join loop is not after every other loop".into(),
            ));
        }
    }

    let nest = HirLoopNest {
        plan,
        document,
        loops_plan,
        prepared_loops: &prepared_loops,
        left_joins: &left_joins,
        right_outer: &right_outer,
        semi_anti: &semi_anti,
        body: &body,
        aggregate_captures: aggregate_captures.as_ref(),
    };
    emit_ready_predicates(
        program,
        document,
        loops_plan,
        None,
        block_done,
        HirPredicateKind::Where {
            right_join_loop: None,
            constant_on_terms_in_loops: !right_outer.is_empty(),
            unmatched_pass: None,
        },
        &left_joins,
    )?;
    nest.emit_loops(program, 0, block_done, None)?;
    // The unmatched rows of each RIGHT or FULL join, in FROM order: the
    // pass over an earlier join's rows runs the later joins' loops, which
    // record the matches those joins' own passes must then skip.
    for right_outer in right_outer
        .iter()
        .filter(|right_outer| right_outer.probe_pass.is_none())
    {
        nest.emit_unmatched_right_rows(program, right_outer, 0)?;
    }
    program.preassign_label_to_next_insn(block_done);
    // A hash table is not closed here: the block may run again, as a FROM
    // subquery does for each outer row, and the table is built only once.
    // Statement reset frees it.
    if let Some((_, captures)) = aggregate_captures {
        bind_hir_aggregate_captures(program, &captures);
    }
    Ok(())
}

/// Everything needed to emit the nested loops of one block, or a suffix of
/// them, followed by the block's row body.
struct HirLoopNest<'n, 'a> {
    plan: &'a HirPlan,
    document: &'n HirDocument,
    loops_plan: HirLoopsPlan<'n>,
    prepared_loops: &'n [PreparedHirLoop<'a>],
    left_joins: &'n [HirLeftJoin],
    /// The RIGHT and FULL joins, in loop order.
    right_outer: &'n [HirRightOuterJoin],
    /// For each loop, `Semi` or `Anti` when it reads the right source of a
    /// semi-join or anti-join.
    semi_anti: &'n [Option<hir::JoinKind>],
    body: &'n HirLoopBody<'n>,
    aggregate_captures: Option<&'n (usize, Vec<HirAggregateCapture>)>,
}

impl HirLoopNest<'_, '_> {
    /// Emit loops `first..` around the body. `outer_next` continues the
    /// enclosing iteration when a row is rejected and no loop is emitted here.
    /// The pass over the unmatched rows of the RIGHT or FULL join at loop
    /// `unmatched_pass` re-emits the loops after it; the joins among those
    /// loops record their matches as in the main loops.
    fn emit_loops(
        &self,
        program: &mut ProgramBuilder,
        first: usize,
        outer_next: BranchOffset,
        unmatched_pass: Option<usize>,
    ) -> Result<()> {
        // Match flags and their labels belong to one emission of the loops.
        let pass_left_joins;
        let left_joins = if unmatched_pass.is_some() {
            pass_left_joins = self
                .left_joins
                .iter()
                .map(|left_join| HirLeftJoin {
                    owner: left_join.owner,
                    first_loop: left_join.first_loop,
                    last_loop: left_join.last_loop,
                    where_from: left_join.where_from,
                    null_extends: left_join.null_extends,
                    metadata: LeftJoinMetadata::new(program),
                    null_cursors: left_join.null_cursors.clone(),
                    null_registers: left_join.null_registers.clone(),
                })
                .collect::<Vec<_>>();
            &pass_left_joins
        } else {
            self.left_joins
        };
        let mut loops = Vec::with_capacity(self.prepared_loops.len() - first);
        // Where a semi-join or anti-join loop goes to try the next row of
        // the loops before it, and where an anti-join loop that found no
        // match continues.
        let semi_anti_labels = (first..self.prepared_loops.len())
            .map(|loop_index| {
                self.semi_anti[loop_index].map(|_| HirSemiAntiLabels {
                    body: program.allocate_label(),
                })
            })
            .collect::<Vec<_>>();
        let semi_anti_next_outer = |loops: &[HirBtreeLoop], position: usize| {
            (first..first + position)
                .rev()
                .find(|loop_index| self.semi_anti[*loop_index].is_none())
                .map_or(outer_next, |loop_index| loops[loop_index - first].next)
        };
        for loop_index in first..self.prepared_loops.len() {
            // An anti-join loop before this one found no match: it goes on
            // here.
            if let Some(labels) = loop_index
                .checked_sub(1)
                .filter(|previous| *previous >= first)
                .filter(|previous| self.semi_anti[*previous] == Some(hir::JoinKind::Anti))
                .and_then(|previous| semi_anti_labels[previous - first].as_ref())
            {
                program.preassign_label_to_next_insn(labels.body);
            }
            let mut starting_left_joins = left_joins.iter().enumerate().filter(|(_, left_join)| {
                left_join.null_extends && left_join.first_loop == loop_index
            });
            let left_join = starting_left_joins.next().map(|(index, left_join)| {
                left_join.metadata.reset(program);
                index
            });
            if starting_left_joins.next().is_some() {
                return Err(LimboError::InternalError(format!(
                    "HIR loop {loop_index} starts overlapping LEFT JOIN boundaries"
                )));
            }
            if let Some(right_outer) = self
                .right_outer
                .iter()
                .find(|right_outer| right_outer.probe_pass == Some(loop_index))
            {
                if unmatched_pass.is_some() {
                    return Err(LimboError::InternalError(
                        "HIR unmatched-row pass re-entered an outer hash join's probe".into(),
                    ));
                }
                program.emit_insn(Insn::ResetSorter {
                    cursor_id: right_outer.matched,
                });
            }
            let hir_loop = match &self.prepared_loops[loop_index] {
                PreparedHirLoop::Hash(prepared) => {
                    let previous_cursor = loop_index
                        .checked_sub(1)
                        .filter(|previous| *previous >= first)
                        .and_then(|previous| self.prepared_loops[previous].cursor())
                        .ok_or_else(|| {
                            LimboError::InternalError(
                                "HIR hash lookup has no table loop before it".into(),
                            )
                        })?;
                    start_hir_hash_loop(
                        program,
                        self.document,
                        prepared,
                        previous_cursor,
                        left_join,
                    )?
                }
                prepared => start_hir_loop(program, self.document, prepared, left_join)?,
            };
            for registers in left_joins
                .iter()
                .flat_map(|left_join| &left_join.null_registers)
                .filter(|registers| registers.loop_index == loop_index && registers.width > 0)
            {
                program.emit_insn(Insn::Copy {
                    src_reg: registers.input,
                    dst_reg: registers.private,
                    extra_amount: registers.width - 1,
                });
            }
            for left_join in left_joins
                .iter()
                .filter(|left_join| left_join.last_loop == loop_index)
            {
                emit_ready_predicates(
                    program,
                    self.document,
                    self.loops_plan,
                    Some(loop_index),
                    hir_loop.next,
                    HirPredicateKind::Join(left_join.owner),
                    left_joins,
                )?;
                if let Some(right_outer) = self
                    .right_outer
                    .iter()
                    .find(|right_outer| right_outer.owner == left_join.owner)
                {
                    if unmatched_pass == Some(right_outer.loop_index) {
                        return Err(LimboError::InternalError(
                            "HIR unmatched-row pass re-entered its right source".into(),
                        ));
                    }
                    right_outer.emit_mark_matched(program, self.document)?;
                }
                if left_join.null_extends {
                    left_join.metadata.mark_matched(program);
                }
            }
            emit_ready_predicates(
                program,
                self.document,
                self.loops_plan,
                Some(loop_index),
                hir_loop.next,
                HirPredicateKind::Where {
                    // WHERE terms wait for the innermost RIGHT or FULL join
                    // among these loops to record its matches.
                    right_join_loop: self
                        .right_outer
                        .iter()
                        .filter(|right_outer| right_outer.loop_index >= first)
                        .map(|right_outer| right_outer.loop_index)
                        .max(),
                    constant_on_terms_in_loops: !self.right_outer.is_empty(),
                    unmatched_pass,
                },
                left_joins,
            )?;
            // A build row of a hash anti-join that passes every check has a
            // match: it is remembered, so that the pass after the loops
            // skips it, and never emitted.
            if let Some(right_outer) = self.right_outer.iter().find(|right_outer| {
                right_outer.loop_index == loop_index && right_outer.kind == hir::JoinKind::Anti
            }) {
                program.add_comment(
                    program.offset(),
                    "hash anti-join: match found, remember the build row",
                );
                right_outer.emit_mark_matched(program, self.document)?;
                program.emit_insn(Insn::Goto {
                    target_pc: hir_loop.next,
                });
            }
            // A row of an anti-join loop that passes every check is a
            // match, so the rows before it are rejected.
            if self.semi_anti[loop_index] == Some(hir::JoinKind::Anti) {
                let next_outer = semi_anti_next_outer(&loops, loop_index - first);
                program.add_comment(program.offset(), "anti-join: match found, skip outer row");
                program.emit_insn(Insn::Goto {
                    target_pc: next_outer,
                });
            }
            loops.push(hir_loop);
        }
        if let Some(labels) = self
            .semi_anti
            .last()
            .filter(|kind| **kind == Some(hir::JoinKind::Anti))
            .and_then(|_| semi_anti_labels.last())
            .and_then(Option::as_ref)
        {
            program.preassign_label_to_next_insn(labels.body);
        }

        // A row the body skips, for OFFSET or a NULL MIN/MAX value, is done
        // with its outer row when the last loop is a semi-join or anti-join:
        // going on with that loop's rows would emit the outer row again.
        let innermost_next = match loops.len().checked_sub(1) {
            None => outer_next,
            Some(last) if self.semi_anti[first + last].is_some() => {
                semi_anti_next_outer(&loops, last)
            }
            Some(last) => loops[last].next,
        };
        match self.body {
            HirLoopBody::Rows(output, outputs) => {
                let start = query_output_registers(program, outputs, output.needs_values())?;
                emit_query_row(
                    program,
                    self.document,
                    output,
                    outputs,
                    outputs.iter().map(|output| &output.expr),
                    start,
                    innermost_next,
                )?;
            }
            HirLoopBody::Dml(body) => body(program, innermost_next)?,
            HirLoopBody::UngroupedAggregate(state, first_value_done) => {
                if first_value_done.is_some() {
                    let [aggregate] = state.aggregates.as_slice() else {
                        return Err(LimboError::InternalError(
                            "MIN/MAX scan has more than one aggregate".to_string(),
                        ));
                    };
                    let [argument] = aggregate.call.arguments.expressions() else {
                        return Err(LimboError::InternalError(
                            "MIN/MAX scan aggregate has more than one argument".to_string(),
                        ));
                    };
                    let value = program.alloc_register();
                    super::expr::translate_expr(program, self.document, argument, value)?;
                    program.emit_insn(Insn::IsNull {
                        reg: value,
                        target_pc: innermost_next,
                    });
                }
                let (first_row, captures) = self
                    .aggregate_captures
                    .expect("aggregate loop initializes first-row captures");
                emit_hir_aggregate_steps(program, self.document, state)?;
                emit_hir_aggregate_capture(
                    program,
                    self.document,
                    *first_row,
                    captures,
                    state.min_max_row.as_ref(),
                )?;
                if let Some(done) = first_value_done {
                    program.emit_insn(Insn::Goto { target_pc: *done });
                }
            }
            HirLoopBody::Group(runtime) => {
                emit_hir_group_input(program, self.document, runtime)?;
            }
        }

        for (position, hir_loop) in loops.iter().enumerate().rev() {
            let semi_anti = self.semi_anti[first + position];
            let next_outer = semi_anti.map(|_| semi_anti_next_outer(&loops, position));
            // After the rest of the block ran for one match, a semi-join is
            // done with the rows before it; an anti-join gets here only
            // when it found no match, and is done too.
            if let (Some(kind), Some(next_outer)) = (semi_anti, next_outer) {
                program.add_comment(
                    program.offset(),
                    if kind == hir::JoinKind::Semi {
                        "semi-join: early out after first match"
                    } else {
                        "anti-join: exit body, next outer row"
                    },
                );
                program.emit_insn(Insn::Goto {
                    target_pc: next_outer,
                });
            }
            close_hir_btree_loop(program, hir_loop, left_joins);
            // The loop ran out of rows without a match: a semi-join rejects
            // the rows before it, and an anti-join keeps them.
            match (semi_anti, next_outer) {
                (Some(hir::JoinKind::Semi), Some(next_outer)) => {
                    program.add_comment(program.offset(), "semi-join: no match, skip outer row");
                    program.emit_insn(Insn::Goto {
                        target_pc: next_outer,
                    });
                }
                (Some(hir::JoinKind::Anti), _) => {
                    let labels = semi_anti_labels[position]
                        .as_ref()
                        .expect("an anti-join loop has labels");
                    program.add_comment(program.offset(), "anti-join: no match, emit outer row");
                    program.emit_insn(Insn::Goto {
                        target_pc: labels.body,
                    });
                }
                _ => {}
            }
            // A hash lookup's waiting rows run after the loop before it.
            if let (Some(PreparedHirLoop::Hash(prepared)), Some(hash_loop)) = (
                self.prepared_loops.get(first + position + 1),
                loops.get(position + 1),
            ) {
                let previous_cursor = self.prepared_loops[first + position]
                    .cursor()
                    .expect("a hash lookup follows a table loop");
                let left_join = hash_loop.left_join.map(|index| &left_joins[index]);
                emit_hir_hash_waiting_lookups(
                    program,
                    prepared,
                    hash_loop,
                    previous_cursor,
                    left_join,
                );
            }
            if let Some(right_outer) = self
                .right_outer
                .iter()
                .find(|right_outer| right_outer.probe_pass == Some(first + position))
            {
                self.emit_unmatched_right_rows(program, right_outer, first + position)?;
            }
        }
        Ok(())
    }

    /// After the main loops: scan the right source of a RIGHT or FULL join
    /// and run the rest of the block for every row no left row matched. The
    /// left sources from loop `null_from` on read NULL, and every WHERE term
    /// up to the right source runs again, including terms the planner used
    /// for index seeks. An outer hash join under earlier loops runs this
    /// after each pass of its probe loop, with `null_from` at the probe.
    fn emit_unmatched_right_rows(
        &self,
        program: &mut ProgramBuilder,
        right_outer: &HirRightOuterJoin,
        null_from: usize,
    ) -> Result<()> {
        let right = right_outer.loop_index;
        // An expression an index of these loops supplied is read from the
        // index again only after the pass: the left indexes are NULL rows
        // now, and the right table is read through a new cursor.
        let mut index_key_bindings = Vec::new();
        for prepared in &self.prepared_loops[null_from..=right] {
            let PreparedHirLoop::BTree(prepared) = prepared else {
                continue;
            };
            for (expression, _) in &prepared.index_keys {
                if let Some(register) = program.unbind_expression_register(expression) {
                    index_key_bindings.push((*expression, register));
                }
            }
        }
        // Register-backed left sources, such as a recursive CTE's current
        // row, read NULL registers for this pass only: the pass emits its
        // own copy of the rest of the block.
        let mut register_bindings = Vec::new();
        for prepared in &self.prepared_loops[null_from..right] {
            match prepared {
                PreparedHirLoop::BTree(_)
                | PreparedHirLoop::OrUnion(_)
                | PreparedHirLoop::Materialized(_) => {}
                PreparedHirLoop::Registers(registers) => {
                    let width = registers.source.columns.len().max(1);
                    let nulls = program.alloc_registers(width);
                    program.emit_insn(Insn::Null {
                        dest: nulls,
                        dest_end: (width > 1).then_some(nulls + width - 1),
                    });
                    let original = program.source_binding(registers.source.id).cloned();
                    program.bind_source(
                        registers.source.id,
                        SourceBinding::Registers {
                            start: nulls,
                            rowid: None,
                        },
                    );
                    register_bindings.push((registers.source.id, original));
                }
                // A hash lookup's row lives in registers.
                PreparedHirLoop::Hash(prepared) => emit_hir_hash_null_row(
                    program,
                    prepared.payload,
                    prepared.payload_width,
                    prepared.rowid,
                ),
                // A virtual table cursor and an index method's table cursor
                // read NULL columns once NullRow flags them, like a B-tree
                // cursor.
                PreparedHirLoop::Virtual(_) | PreparedHirLoop::IndexMethod(_) => {}
                PreparedHirLoop::Coroutine(_) => {
                    return Err(LimboError::InternalError(
                        "HIR RIGHT or FULL JOIN left side cannot be NULL-extended".into(),
                    ));
                }
            }
            for cursor in prepared.cursor().into_iter().chain(prepared.table_cursor()) {
                program.emit_insn(Insn::NullRow { cursor_id: cursor });
            }
        }
        // A B-tree table runs the read planned for its unmatched rows,
        // which may seek an index, through new cursors.
        let planned_read = match (&self.prepared_loops[right], right_outer.materialized) {
            (PreparedHirLoop::BTree(_), None) => {
                self.loops_plan.loops[right].unmatched_right_rows.as_deref()
            }
            _ => None,
        };
        // A recursive CTE's current row is the one right row: the body runs
        // once for it, when no left row matched it.
        if let (PreparedHirLoop::Registers(_), None) =
            (&self.prepared_loops[right], right_outer.materialized)
        {
            let loop_start = program.allocate_label();
            let next = program.allocate_label();
            let exhausted = program.allocate_label();
            program.preassign_label_to_next_insn(loop_start);
            let hir_loop = HirBtreeLoop {
                cursor: None,
                loop_start,
                next,
                exhausted,
                advance: HirLoopAdvance::None,
                left_join: None,
            };
            self.emit_unmatched_right_row_body(program, right_outer, &hir_loop, None)?;
            close_hir_btree_loop(program, &hir_loop, &[]);
            restore_unmatched_pass_bindings(program, None, register_bindings, index_key_bindings);
            return Ok(());
        }
        if let Some(planned_read) = planned_read {
            let original = program.source_binding(right_outer.source).cloned();
            let prepared = prepare_hir_loop_unexplained(program, self.plan, planned_read)?;
            let hir_loop = start_hir_loop(program, self.document, &prepared, None)?;
            self.emit_unmatched_right_row_body(program, right_outer, &hir_loop, None)?;
            close_hir_btree_loop(program, &hir_loop, &[]);
            if let PreparedHirLoop::BTree(prepared) = &prepared {
                for (expression, _) in &prepared.index_keys {
                    program.unbind_expression_register(expression);
                }
            }
            restore_unmatched_pass_bindings(
                program,
                original.map(|original| (right_outer.source, original)),
                register_bindings,
                index_key_bindings,
            );
            return Ok(());
        }
        // A materialized subquery is scanned again through its own cursor.
        // A table gets a new cursor, since its loop may have read an index.
        // A virtual table is opened again on its cursor, since the loop
        // left the first open at its end.
        let mut hash_input = None;
        let mut virtual_read = None;
        let (scan, original) = match (&self.prepared_loops[right], right_outer.materialized) {
            (PreparedHirLoop::Virtual(prepared), None) => {
                let operation = prepared
                    .operation
                    .unmatched_right_rows
                    .as_deref()
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR RIGHT or FULL JOIN virtual source {} has no unmatched-row read",
                            right_outer.source
                        ))
                    })?;
                program.emit_insn(Insn::VOpen {
                    cursor_id: prepared.cursor,
                });
                virtual_read = Some(operation);
                (prepared.cursor, None)
            }
            (PreparedHirLoop::Materialized(_), Some(cursor)) => (cursor, None),
            (PreparedHirLoop::Hash(prepared), Some(cursor)) if prepared.build_input.is_some() => {
                hash_input = Some(prepared);
                (cursor, None)
            }
            (PreparedHirLoop::BTree(prepared), None) => {
                let hir::SourceKind::Table(table) = &prepared.source.kind else {
                    unreachable!("B-tree loops read table sources")
                };
                let Some(table) = table.value().btree() else {
                    unreachable!("B-tree loops read B-tree tables")
                };
                let database = prepared
                    .source
                    .database
                    .expect("B-tree loop sources have a database");
                let scan = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
                program.emit_insn(Insn::OpenRead {
                    cursor_id: scan,
                    root_page: table.root_page,
                    db: database.index(),
                });
                let original = program.source_binding(right_outer.source).cloned();
                program.bind_source(
                    right_outer.source,
                    SourceBinding::BTree {
                        scan_cursor: scan,
                        table_cursor: None,
                    },
                );
                (scan, original)
            }
            (PreparedHirLoop::Hash(prepared), None) => {
                let scan = program.alloc_cursor_id(CursorType::BTreeTable(prepared.table.clone()));
                program.emit_insn(Insn::OpenRead {
                    cursor_id: scan,
                    root_page: prepared.table.root_page,
                    db: prepared.database,
                });
                let original = program.source_binding(right_outer.source).cloned();
                program.bind_source(
                    right_outer.source,
                    SourceBinding::BTree {
                        scan_cursor: scan,
                        table_cursor: None,
                    },
                );
                (scan, original)
            }
            _ => {
                return Err(LimboError::InternalError(
                    "HIR RIGHT or FULL JOIN right loop does not match its matched set".into(),
                ))
            }
        };

        let done = program.allocate_label();
        let loop_start = program.allocate_label();
        let next = program.allocate_label();
        let advance = if virtual_read.is_some() {
            HirLoopAdvance::Virtual
        } else {
            HirLoopAdvance::Cursor {
                direction: IterationDirection::Forwards,
                fullscan: true,
            }
        };
        if let Some(operation) = virtual_read {
            // The arguments are evaluated here, after the left sources
            // became NULL rows: an argument that reads one is NULL now.
            let args_reg = program.alloc_registers(operation.arguments.len());
            for (index, argument) in operation.arguments.iter().enumerate() {
                super::expr::translate_expr(program, self.document, argument, args_reg + index)?;
            }
            let idx_str = operation.idx_str.as_ref().map(|value| {
                let register = program.alloc_register();
                program.emit_insn(Insn::String8 {
                    dest: register,
                    value: value.clone(),
                });
                register
            });
            program.emit_insn(Insn::VFilter {
                cursor_id: scan,
                pc_if_empty: done,
                arg_count: operation.arguments.len(),
                args_reg,
                idx_str,
                idx_num: operation.idx_num as usize,
            });
        } else {
            program.emit_insn(Insn::Rewind {
                cursor_id: scan,
                pc_if_empty: done,
            });
        }
        program.preassign_label_to_next_insn(loop_start);
        let hir_loop = HirBtreeLoop {
            cursor: Some(scan),
            loop_start,
            next,
            exhausted: done,
            advance,
            left_join: None,
        };
        self.emit_unmatched_right_row_body(program, right_outer, &hir_loop, hash_input)?;
        close_hir_btree_loop(program, &hir_loop, &[]);
        restore_unmatched_pass_bindings(
            program,
            original.map(|original| (right_outer.source, original)),
            register_bindings,
            index_key_bindings,
        );
        Ok(())
    }

    /// The body of the pass over a RIGHT or FULL join's unmatched rows, for
    /// the right row `hir_loop` is positioned on: skip the row when the
    /// matched set has it, check the WHERE terms, and run the loops after
    /// the right source.
    fn emit_unmatched_right_row_body(
        &self,
        program: &mut ProgramBuilder,
        right_outer: &HirRightOuterJoin,
        hir_loop: &HirBtreeLoop,
        hash_input: Option<&PreparedHirHashLoop<'_>>,
    ) -> Result<()> {
        let right = right_outer.loop_index;
        let next = hir_loop.next;
        match &right_outer.key {
            HirMatchedKey::Rowid => {
                let unmatched = program.allocate_label();
                let rowid = program.alloc_register();
                // A planned read may have positioned an index cursor: the
                // source's rowid comes through its current binding. A
                // register-backed row has the constant key it was marked by.
                if self.loops_plan.loops[right].unmatched_right_rows.is_some() {
                    super::expr::translate_expr(
                        program,
                        self.document,
                        &hir::Expr::RowId(right_outer.source),
                        rowid,
                    )?;
                } else if let Some(cursor) = hir_loop.cursor {
                    program.emit_insn(Insn::RowId {
                        cursor_id: cursor,
                        dest: rowid,
                    });
                } else if let Some(register) = right_outer.matched_rowid {
                    program.emit_insn(Insn::Copy {
                        src_reg: register,
                        dst_reg: rowid,
                        extra_amount: 0,
                    });
                } else {
                    return Err(LimboError::InternalError(
                        "HIR RIGHT or FULL JOIN unmatched pass has no row key".into(),
                    ));
                }
                program.emit_insn(Insn::NotExists {
                    cursor: right_outer.matched,
                    rowid_reg: rowid,
                    target_pc: unmatched,
                });
                program.emit_insn(Insn::Goto { target_pc: next });
                program.preassign_label_to_next_insn(unmatched);
            }
            HirMatchedKey::PrimaryKey(positions) => {
                let record =
                    right_outer.emit_primary_key_record(program, self.document, positions)?;
                program.emit_insn(Insn::Found {
                    cursor_id: right_outer.matched,
                    target_pc: next,
                    record_reg: record,
                    num_regs: 0,
                });
            }
        }
        if let Some(prepared) = hash_input {
            emit_hir_hash_input_payload(program, prepared);
        }
        // Like SQLite, the unmatched pass checks WHERE terms, and the ON
        // terms of the inner joins written after this outer join, which
        // apply to its NULL-extended rows the same way. The ON terms of this
        // join and of the joins before it are not checked again: those of
        // inner joins left of a RIGHT JOIN would reject every row whose left
        // side is NULL.
        let later_on_terms = self
            .loops_plan
            .later_on_terms
            .iter()
            .find(|(source, _)| *source == right_outer.owner)
            .map(|(_, start)| *start..self.loops_plan.where_start)
            .unwrap_or(0..0)
            .filter(|index| self.loops_plan.predicates[*index].from_outer_join.is_none());
        // A WHERE term waits for a later RIGHT or FULL join to record its
        // matches, as in the main loops: the loops this pass runs again
        // check it after that join's loop.
        let later_right_outer = self
            .right_outer
            .iter()
            .any(|later| later.loop_index > right);
        let where_terms = (self.loops_plan.where_start..self.loops_plan.predicates.len())
            .filter(|_| !later_right_outer);
        for index in later_on_terms.chain(where_terms) {
            let predicate = &self.loops_plan.predicates[index];
            // A hash anti-join's own terms read its probe table, which is
            // NULL here: they decided the matches, not the unmatched rows.
            if right_outer.kind == hir::JoinKind::Anti {
                let mut reads_probe = false;
                self.document
                    .visit_expr_sources(&predicate.expr, &mut |source| {
                        reads_probe |= source == right_outer.owner;
                    });
                if reads_probe {
                    continue;
                }
            }
            let ready_at = predicate_ready_loop(
                program,
                self.document,
                self.loops_plan.loops,
                self.loops_plan.deferred_predicates,
                self.loops_plan.predicates,
                index,
            )?;
            if ready_at.is_some_and(|ready_at| ready_at > right) {
                continue;
            }
            let passed = program.allocate_label();
            super::expr::translate_condition_expr(
                program,
                self.document,
                &predicate.expr,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true: passed,
                    jump_target_when_false: next,
                },
            )?;
            program.preassign_label_to_next_insn(passed);
        }
        self.emit_loops(program, right + 1, next, Some(right))
    }
}

/// Open the matched-row set of each RIGHT or FULL join of the block, in
/// loop order.
fn prepare_hir_right_outer_joins(
    program: &mut ProgramBuilder,
    block_plan: HirLoopsPlan<'_>,
    prepared_loops: &[PreparedHirLoop<'_>],
    joins: &HirOuterJoins,
    hash_outer: Option<&HirHashOuterJoin>,
) -> Result<Vec<HirRightOuterJoin>> {
    let mut right_outer = joins
        .right_outer
        .iter()
        .map(|(source, kind)| {
            prepare_hir_right_outer_join(
                program,
                block_plan,
                prepared_loops,
                *source,
                *kind,
                hash_outer,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    right_outer.sort_by_key(|right_outer| right_outer.loop_index);
    Ok(right_outer)
}

/// Open the matched-row set of the RIGHT or FULL join whose right source
/// is `source`.
fn prepare_hir_right_outer_join(
    program: &mut ProgramBuilder,
    block_plan: HirLoopsPlan<'_>,
    prepared_loops: &[PreparedHirLoop<'_>],
    source: SourceId,
    kind: hir::JoinKind,
    hash_outer: Option<&HirHashOuterJoin>,
) -> Result<HirRightOuterJoin> {
    let loop_index = block_plan
        .loops
        .iter()
        .position(|planned_loop| planned_loop.source == source)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR RIGHT or FULL JOIN source {source} has no loop"
            ))
        })?;
    let mut matched_rowid = None;
    let (table, materialized) = match &prepared_loops[loop_index] {
        PreparedHirLoop::BTree(prepared) => {
            let Some(table) = (match &prepared.source.kind {
                hir::SourceKind::Table(table) => table.value().btree(),
                _ => None,
            }) else {
                return Err(LimboError::InternalError(format!(
                    "HIR RIGHT or FULL JOIN source {source} is not a table"
                )));
            };
            if !table.has_rowid {
                // A WITHOUT ROWID table has no rowid: its primary key
                // identifies a row, so the matched set is an index over it.
                let (matched, key) = open_hir_primary_key_matched_set(program, &table)?;
                return Ok(HirRightOuterJoin {
                    source,
                    kind,
                    owner: source,
                    loop_index,
                    matched,
                    key,
                    empty_record: 0,
                    materialized: None,
                    matched_rowid: None,
                    probe_pass: None,
                });
            }
            (table, None)
        }
        PreparedHirLoop::Virtual(prepared) => (hir_matched_rows_table(prepared.source), None),
        // A recursive CTE's current row lives in registers: the one right
        // row of each run of the loops, keyed by the constant 1.
        PreparedHirLoop::Registers(prepared) => {
            let one = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: one,
            });
            matched_rowid = Some(one);
            (hir_matched_rows_table(prepared.source), None)
        }
        PreparedHirLoop::Materialized(prepared) => (prepared.table.clone(), Some(prepared.cursor)),
        PreparedHirLoop::Hash(prepared) => match &prepared.build_input {
            Some(input) => {
                matched_rowid = Some(prepared.rowid);
                (input.table.clone(), Some(input.cursor))
            }
            None => (prepared.table.clone(), None),
        },
        PreparedHirLoop::IndexMethod(_)
        | PreparedHirLoop::OrUnion(_)
        | PreparedHirLoop::Coroutine(_) => {
            return Err(LimboError::InternalError(format!(
                "HIR RIGHT or FULL JOIN source {source} has no rowids"
            )));
        }
    };
    let matched = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: matched,
        is_table: true,
    });
    // One NULL column rather than none: a table cell of an empty record and
    // a small rowid is 3 bytes, below the 4 bytes a B-tree cell takes.
    let null = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: null,
        dest_end: None,
    });
    let empty_record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(null),
        count: 1,
        dest_reg: to_u32(empty_record),
        index_name: None,
        affinity_str: None,
    });
    let hash_outer = hash_outer.filter(|hash_outer| hash_outer.build == source);
    let owner = hash_outer.map_or(source, |hash_outer| hash_outer.probe);
    let probe_pass = hash_outer
        .map(|hash_outer| hash_outer.build_loop - 1)
        .filter(|probe_loop| *probe_loop > 0);
    Ok(HirRightOuterJoin {
        source,
        kind,
        owner,
        loop_index,
        matched,
        key: HirMatchedKey::Rowid,
        empty_record,
        materialized,
        matched_rowid,
        probe_pass,
    })
}

/// The ephemeral rowid table that records the matched rows of a source
/// without a B-tree of its own, such as a virtual table.
fn hir_matched_rows_table(source: &hir::Source) -> Arc<BTreeTable> {
    Arc::new(BTreeTable::new(
        0,
        format!("{}_matched_rows", source.name),
        crate::alloc::vec![],
        crate::alloc::vec![Column::new(
            Some("matched".to_string()),
            "BLOB".to_string(),
            None,
            None,
            Type::Blob,
            None,
            ColDef::default(),
        )],
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ))
}

/// Open the matched set of a WITHOUT ROWID right table: an ephemeral index
/// over its primary key columns, compared with their collations.
fn open_hir_primary_key_matched_set(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
) -> Result<(CursorID, HirMatchedKey)> {
    let mut positions = Vec::with_capacity(table.primary_key_columns.len());
    let mut columns = crate::alloc::vec![];
    for (name, order) in &table.primary_key_columns {
        let (position, column) = table.get_column(name).ok_or_else(|| {
            LimboError::InternalError(format!(
                "WITHOUT ROWID table {} has no primary key column {name}",
                table.name
            ))
        })?;
        positions.push(position);
        columns.push(IndexColumn {
            name: name.clone(),
            order: *order,
            nulls_order: None,
            pos_in_table: position,
            collation: Some(column.collation()),
            default: None,
            expr: None,
        });
    }
    let index = Arc::new(Index {
        constraint_name: None,
        name: format!("{}_matched_rows", table.name),
        table_name: table.name.clone(),
        root_page: 0,
        columns,
        unique: false,
        ephemeral: true,
        has_rowid: false,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let matched = program.alloc_cursor_id(CursorType::BTreeIndex(index));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: matched,
        is_table: false,
    });
    Ok((matched, HirMatchedKey::PrimaryKey(positions)))
}

impl HirRightOuterJoin {
    /// Load the current right row's primary key into fresh registers and
    /// pack it as the matched set's key.
    fn emit_primary_key_record(
        &self,
        program: &mut ProgramBuilder,
        document: &HirDocument,
        positions: &[usize],
    ) -> Result<usize> {
        let start = program.alloc_registers(positions.len());
        for (offset, position) in positions.iter().enumerate() {
            super::expr::translate_expr(
                program,
                document,
                &hir::Expr::Column(hir::ColumnRef {
                    source: self.source,
                    column: *position,
                }),
                start + offset,
            )?;
        }
        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(start),
            count: to_u32(positions.len()),
            dest_reg: to_u32(record),
            index_name: None,
            affinity_str: None,
        });
        Ok(record)
    }

    /// Remember that the current right row matched a left row.
    fn emit_mark_matched(
        &self,
        program: &mut ProgramBuilder,
        document: &HirDocument,
    ) -> Result<()> {
        if let HirMatchedKey::PrimaryKey(positions) = &self.key {
            let record = self.emit_primary_key_record(program, document, positions)?;
            program.emit_insn(Insn::IdxInsert {
                cursor_id: self.matched,
                record_reg: record,
                unpacked_start: None,
                unpacked_count: None,
                flags: crate::vdbe::insn::IdxInsertFlags::new(),
            });
            return Ok(());
        }
        let rowid = program.alloc_register();
        if let Some(register) = self.matched_rowid {
            program.emit_insn(Insn::Copy {
                src_reg: register,
                dst_reg: rowid,
                extra_amount: 0,
            });
        } else if let Some(cursor) = self.materialized {
            program.emit_insn(Insn::RowId {
                cursor_id: cursor,
                dest: rowid,
            });
        } else {
            super::expr::translate_expr(program, document, &hir::Expr::RowId(self.source), rowid)?;
        }
        program.emit_insn(Insn::Insert {
            cursor: self.matched,
            key_reg: rowid,
            record_reg: self.empty_record,
            // The cursor is not positioned, and the matched-row set must not
            // change last_insert_rowid() or change counts.
            flag: InsertFlags::new()
                .require_seek()
                .is_ephemeral_table_insert(),
            table_name: String::new(),
        });
        Ok(())
    }
}

/// After the pass over a RIGHT or FULL join's unmatched rows: the right
/// source and the register-backed left sources read their main-loop rows
/// again, and the expressions the left indexes supply come from them again.
fn restore_unmatched_pass_bindings(
    program: &mut ProgramBuilder,
    right_source: Option<(SourceId, SourceBinding)>,
    register_bindings: Vec<(SourceId, Option<SourceBinding>)>,
    index_key_bindings: Vec<(&hir::Expr, usize)>,
) {
    if let Some((source, original)) = right_source {
        program.bind_source(source, original);
    }
    for (source, original) in register_bindings {
        if let Some(original) = original {
            program.bind_source(source, original);
        }
    }
    for (expression, register) in index_key_bindings {
        program.bind_expression_register(expression, register);
    }
}

fn supported_btree_from(
    document: &HirDocument,
    from: &hir::From,
    joins: &mut HirOuterJoins,
) -> bool {
    let Some(first) = document.source(from.first) else {
        return false;
    };
    if let hir::SourceKind::FromGroup(group) = &first.kind {
        if !supported_btree_from(document, &group.from, joins) {
            return false;
        }
    }
    for join in &from.joins {
        let Some(source) = document.source(join.right) else {
            return false;
        };
        match join.kind {
            hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross => {
                if let hir::SourceKind::FromGroup(group) = &source.kind {
                    if !supported_btree_from(document, &group.from, joins) {
                        return false;
                    }
                }
            }
            hir::JoinKind::Left => {
                if let hir::SourceKind::FromGroup(group) = &source.kind {
                    if !ordinary_btree_from(document, &group.from) {
                        return false;
                    }
                }
                joins.owners.push((join.right, true));
            }
            hir::JoinKind::Semi | hir::JoinKind::Anti => {
                if matches!(source.kind, hir::SourceKind::FromGroup(_)) {
                    return false;
                }
            }
            hir::JoinKind::Right | hir::JoinKind::Full => {
                if matches!(source.kind, hir::SourceKind::FromGroup(_)) {
                    return false;
                }
                joins
                    .owners
                    .push((join.right, join.kind == hir::JoinKind::Full));
                joins.right_outer.push((join.right, join.kind));
            }
        }
    }
    true
}

/// A FULL join over a recursive reference has no unmatched-row pass: the
/// recursive row is not stored anywhere the pass could read again.
fn ordinary_btree_from(document: &HirDocument, from: &hir::From) -> bool {
    from.joins.iter().all(|join| {
        matches!(
            join.kind,
            hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross
        )
    }) && std::iter::once(from.first)
        .chain(from.joins.iter().map(|join| join.right))
        .all(|source| {
            document.source(source).is_some_and(|source| {
                if let hir::SourceKind::FromGroup(group) = &source.kind {
                    ordinary_btree_from(document, &group.from)
                } else {
                    true
                }
            })
        })
}

fn prepare_hir_left_joins(
    program: &mut ProgramBuilder,
    block_plan: HirLoopsPlan<'_>,
    prepared_loops: &[PreparedHirLoop<'_>],
    owners: &[(SourceId, bool)],
    hash_outer: Option<&HirHashOuterJoin>,
) -> Result<Vec<HirLeftJoin>> {
    owners
        .iter()
        .map(|(owner, null_extends)| {
            // The ON terms of an outer hash join's probe table run after each
            // lookup into the build table, whose row a FULL join NULL-fills.
            let hash_loop = hash_outer
                .filter(|hash_outer| hash_outer.probe == *owner)
                .map(|hash_outer| hash_outer.build_loop);
            let loop_indices = if let Some(hash_loop) = hash_loop {
                vec![hash_loop]
            } else if let Some(group) = block_plan
                .groups
                .iter()
                .find(|group| group.source == *owner)
            {
                block_plan
                    .loops
                    .iter()
                    .enumerate()
                    .filter_map(|(loop_index, planned_loop)| {
                        group
                            .source_range
                            .contains(&planned_loop.source_position)
                            .then_some(loop_index)
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![block_plan
                    .loops
                    .iter()
                    .position(|planned_loop| planned_loop.source == *owner)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR LEFT JOIN owner {owner} has no physical loop"
                        ))
                    })?]
            };
            let Some((&first_loop, &last_loop)) = loop_indices.first().zip(loop_indices.last())
            else {
                return Err(LimboError::InternalError(format!(
                    "HIR LEFT JOIN group {owner} has no physical loops"
                )));
            };
            if loop_indices.len() != last_loop - first_loop + 1 {
                return Err(LimboError::InternalError(format!(
                    "HIR LEFT JOIN group {owner} is not contiguous in physical loop order"
                )));
            }
            let mut null_cursors = Vec::new();
            let mut null_registers = Vec::new();
            for (offset, prepared) in prepared_loops[first_loop..=last_loop].iter().enumerate() {
                if let Some(cursor) = prepared.cursor() {
                    null_cursors.push(cursor);
                }
                if let Some(table_cursor) = prepared.table_cursor() {
                    null_cursors.push(table_cursor);
                }
                // A coroutine's yielded row is NULL-extended by clearing
                // its registers. A recursive CTE's current row is not: like
                // SQLite, the unmatched row of a LEFT or FULL JOIN whose
                // right side is the recursive reference still reads the
                // current row.
                let register_source = match prepared {
                    PreparedHirLoop::Coroutine(coroutine) => Some((coroutine.source, true)),
                    _ => None,
                };
                if let Some((register_source, coroutine)) = register_source {
                    if !*null_extends {
                        continue;
                    }
                    let Some(&SourceBinding::Registers { start, rowid: None }) =
                        program.source_binding(register_source.id)
                    else {
                        return Err(LimboError::InternalError(format!(
                            "HIR register source {} has no plain row binding",
                            register_source.id
                        )));
                    };
                    let width = register_source.columns.len();
                    // A coroutine's own row registers are cleared in place:
                    // its next resume writes them again, and later loops
                    // prepared before this point already read them.
                    let private = if coroutine {
                        start
                    } else {
                        let private = program.alloc_registers(width.max(1));
                        program.bind_source(
                            register_source.id,
                            SourceBinding::Registers {
                                start: private,
                                rowid: None,
                            },
                        );
                        private
                    };
                    null_registers.push(HirNullRegisters {
                        loop_index: first_loop + offset,
                        input: start,
                        private,
                        width,
                    });
                }
            }
            Ok(HirLeftJoin {
                owner: *owner,
                first_loop,
                last_loop,
                where_from: hash_loop.map_or(first_loop, |hash_loop| hash_loop - 1),
                null_extends: *null_extends,
                metadata: LeftJoinMetadata::new(program),
                null_cursors,
                null_registers,
            })
        })
        .collect()
}

/// Whether a loop reads a CTE that is filled once and shared by its readers:
/// one read by several loops, or one hinted MATERIALIZED.
fn hir_loop_shares_cte(planned_loop: &HirPlannedLoop) -> bool {
    matches!(
        planned_loop.access,
        HirSourceAccess::Cte {
            materialization: HirCteMaterialization::Shared | HirCteMaterialization::Explicit,
            ..
        }
    )
}

/// Fill each shared CTE the loops read into its table before the block's
/// loops open. EXPLAIN then lists the CTE's body on its own,
/// under `cte_materializations`, and every reader points at that one fill.
fn materialize_hir_shared_ctes(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    loops: &[HirPlannedLoop],
) -> Result<()> {
    for planned_loop in loops {
        match &planned_loop.access {
            HirSourceAccess::Cte { cte, query, .. } if hir_loop_shares_cte(planned_loop) => {
                if program.hir_materialized_cte(*cte).is_some() {
                    continue;
                }
                let name = plan
                    .document
                    .cte(*cte)
                    .map(|cte| cte.name.clone())
                    .ok_or_else(|| {
                        LimboError::InternalError(format!("HIR references missing CTE {cte}"))
                    })?;
                let materialized_query = HirMaterializedQuery::Cte {
                    cte: *cte,
                    query: *query,
                };
                let prepared =
                    program.with_cte_materialization_eqp(cte.index(), &name, |program| {
                        prepare_hir_materialized_loop(
                            program,
                            plan,
                            planned_loop.source,
                            materialized_query,
                        )
                    })?;
                program.register_hir_materialized_cte(
                    *cte,
                    MaterializedCteInfo {
                        cursor_id: prepared.cursor,
                        table: prepared.table.clone(),
                        fill_subroutine: prepared.fill_subroutine,
                    },
                );
            }
            HirSourceAccess::Hash(HirHashLookup {
                build_input: Some(input),
                ..
            }) => materialize_hir_shared_ctes(program, plan, &input.loops)?,
            _ => {}
        }
    }
    Ok(())
}

/// Emit the EXPLAIN QUERY PLAN rows of a block's loops in loop order,
/// before any cursor is opened. A block without FROM reads one constant
/// row.
fn emit_hir_block_explain(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
) -> Result<()> {
    if tracing::enabled!(tracing::Level::DEBUG)
        && !matches!(
            program.get_query_mode(),
            crate::vdbe::builder::QueryMode::ExplainQueryPlan { .. }
        )
    {
        if let Some(from) = &block.from {
            log_hir_loops_plan(document, from, &block_plan.inner_joins, &block_plan.loops)?;
        }
    }
    if !program.start_block_explain(block.id) {
        return Ok(());
    }
    let Some(from) = &block.from else {
        emit_explain!(program, false, EqpDetail::ConstantRow);
        return Ok(());
    };
    emit_hir_loops_explain(
        program,
        document,
        from,
        &block_plan.inner_joins,
        &block_plan.loops,
    )
}

/// Log the read planned for each loop, in loop order, as EXPLAIN QUERY
/// PLAN would show it. A statement that is not EXPLAIN QUERY PLAN emits no
/// plan rows, so this is how a debug trace shows its plan.
fn log_hir_loops_plan(
    document: &HirDocument,
    from: &hir::From,
    inner_joins: &[SourceId],
    loops: &[HirPlannedLoop],
) -> Result<()> {
    for planned_loop in loops {
        if let HirSourceAccess::Hash(HirHashLookup {
            build_input: Some(input),
            ..
        }) = &planned_loop.access
        {
            log_hir_loops_plan(document, from, inner_joins, &input.loops)?;
        }
        let detail = hir_loop_explain(document, Some(from), inner_joins, planned_loop)?;
        tracing::debug!(plan = %detail);
    }
    Ok(())
}

/// The rows of `loops` in loop order. A hash join reads as `HASH JOIN`
/// with the table it scans, and the tables built into hash
/// tables come after every loop; the loops that fill a materialized build
/// input come first, under its `MATERIALIZE` row.
fn emit_hir_loops_explain(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    from: &hir::From,
    inner_joins: &[SourceId],
    loops: &[HirPlannedLoop],
) -> Result<()> {
    for planned_loop in loops {
        let HirSourceAccess::Hash(HirHashLookup {
            build_input: Some(input),
            ..
        }) = &planned_loop.access
        else {
            continue;
        };
        let table = hir_eqp_table(document, planned_loop.source)?;
        emit_explain!(
            program,
            true,
            EqpDetail::HashBuild {
                table,
                estimate: planned_loop.estimate
            }
        );
        emit_hir_loops_explain(program, document, from, inner_joins, &input.loops)?;
        program.pop_current_parent_explain();
    }
    let mut builds = Vec::new();
    for (position, planned_loop) in loops.iter().enumerate() {
        if let HirSourceAccess::Hash(lookup) = &planned_loop.access {
            // A build input's loops are listed above.
            if lookup.build_input.is_none() {
                let detail = hir_loop_explain(document, Some(from), inner_joins, planned_loop)?;
                builds.push((planned_loop.source, detail));
            }
            continue;
        }
        let mut detail = hir_loop_explain(document, Some(from), inner_joins, planned_loop)?;
        let probes = loops
            .get(position + 1)
            .is_some_and(|next| matches!(next.access, HirSourceAccess::Hash(_)));
        if probes {
            if let EqpDetail::Scan {
                estimate,
                table,
                join,
                subquery,
                ..
            } = detail
            {
                detail = EqpDetail::HashJoin {
                    estimate,
                    table,
                    join,
                    subquery,
                };
            }
        }
        program.emit_source_explain(planned_loop.source, detail);
    }
    for (source, detail) in builds {
        program.emit_source_explain(source, detail);
    }
    // SQLite lists the pass over each RIGHT or FULL JOIN's unmatched rows
    // after the loops, under a `RIGHT-JOIN` row.
    for planned_loop in loops {
        if !hir_loop_keeps_right_rows(document, from, planned_loop.source) {
            continue;
        }
        let table = hir_eqp_table(document, planned_loop.source)?;
        emit_explain!(program, true, EqpDetail::RightJoin { table });
        let read = match (&planned_loop.unmatched_right_rows, &planned_loop.access) {
            (Some(read), _) => hir_loop_explain(document, None, inner_joins, read)?,
            (
                None,
                HirSourceAccess::Virtual(HirVirtualTableOperation {
                    unmatched_right_rows: Some(read),
                    ..
                }),
            ) => hir_loop_explain(
                document,
                None,
                inner_joins,
                &HirPlannedLoop {
                    source: planned_loop.source,
                    source_position: planned_loop.source_position,
                    access: HirSourceAccess::Virtual((**read).clone()),
                    estimate: None,
                    unmatched_right_rows: None,
                },
            )?,
            // A stored subquery or hash input is scanned again.
            _ => hir_loop_explain(document, None, inner_joins, planned_loop)?,
        };
        emit_explain!(program, false, read);
        program.pop_current_parent_explain();
    }
    Ok(())
}

/// Whether `source` is the right side of a RIGHT or FULL JOIN of `from`,
/// directly or inside a parenthesized join.
fn hir_loop_keeps_right_rows(document: &HirDocument, from: &hir::From, source: SourceId) -> bool {
    from.joins.iter().any(|join| {
        if join.right == source {
            return matches!(join.kind, hir::JoinKind::Right | hir::JoinKind::Full);
        }
        matches!(
            document.source(join.right).map(|right| &right.kind),
            Some(hir::SourceKind::FromGroup(group))
                if hir_loop_keeps_right_rows(document, &group.from, source)
        )
    })
}

/// The EXPLAIN QUERY PLAN row of one planned loop. A DML target loop has
/// no FROM clause.
pub(super) fn hir_loop_explain(
    document: &HirDocument,
    from: Option<&hir::From>,
    // LEFT JOINs the plan runs as inner joins, reported as inner joins.
    inner_joins: &[SourceId],
    planned_loop: &HirPlannedLoop,
) -> Result<EqpDetail> {
    let source = document.source(planned_loop.source).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR loop references missing source {}",
            planned_loop.source
        ))
    })?;
    let table = hir_eqp_table(document, source.id)?;
    let join = from
        .and_then(|from| hir_loop_explain_join(document, from, source.id))
        .map(|join| {
            if join == EqpJoin::Left && inner_joins.contains(&source.id) {
                EqpJoin::Inner
            } else {
                join
            }
        });
    // A shared CTE is filled once, before the loops, and every reader reads
    // that one fill, reported as a reuse.
    let shared = hir_loop_shares_cte(planned_loop);
    let exec = if shared {
        EqpSubqueryExec::MaterializedReuse
    } else if from.is_some_and(|from| hir_loop_streams(document, from, planned_loop)) {
        EqpSubqueryExec::Coroutine
    } else {
        EqpSubqueryExec::Materialized
    };
    let subquery = |cte: Option<hir::CteId>, recursive| EqpDetail::Scan {
        estimate: planned_loop.estimate,
        table: table.clone(),
        index: None,
        source: EqpRowSource::Subquery,
        backwards: false,
        join,
        subquery: Some(EqpSubquery {
            exec,
            cte_id: cte.map(hir::CteId::index),
            recursive,
        }),
    };
    // A subquery read through a temporary index over its stored rows is a
    // SEARCH of that index, with the subquery under it.
    let materialized_search = |cte: Option<hir::CteId>, operation: &HirBtreeOperation| {
        let HirBtreeOperation::Seek {
            index: Some(index),
            seek_def,
        } = operation
        else {
            return Err(LimboError::InternalError(format!(
                "HIR subquery source {} has a seek without an index",
                source.id
            )));
        };
        Ok(EqpDetail::Search {
            estimate: planned_loop.estimate,
            table: table.clone(),
            kind: EqpSearchKind::Seek,
            index: Some(EqpIndex {
                name: index.name.clone(),
                covering: false,
                ephemeral: true,
            }),
            constraints: seek_constraint_parts(index, seek_def),
            backwards: seek_def.iter_dir == IterationDirection::Backwards,
            join,
            subquery: Some(EqpSubquery {
                exec: if shared {
                    EqpSubqueryExec::MaterializedReuse
                } else {
                    EqpSubqueryExec::Materialized
                },
                cte_id: cte.map(hir::CteId::index),
                recursive: false,
            }),
        })
    };
    Ok(match &planned_loop.access {
        HirSourceAccess::BTree {
            operation,
            table_lookup,
        } => {
            let eqp_index = |index: &Arc<Index>| EqpIndex {
                name: index.name.clone(),
                covering: *table_lookup == BtreeTableLookup::ScanOnly,
                ephemeral: index.ephemeral,
            };
            let search =
                |kind, index: Option<EqpIndex>, constraints, backwards| EqpDetail::Search {
                    estimate: planned_loop.estimate,
                    table: table.clone(),
                    kind,
                    index,
                    constraints,
                    backwards,
                    join,
                    subquery: None,
                };
            match operation {
                HirBtreeOperation::Scan { iter_dir, index } => EqpDetail::Scan {
                    estimate: planned_loop.estimate,
                    table: table.clone(),
                    index: index.as_ref().map(eqp_index),
                    source: EqpRowSource::BTreeTable,
                    backwards: *iter_dir == IterationDirection::Backwards,
                    join,
                    subquery: None,
                },
                HirBtreeOperation::Seek { index, seek_def } => search(
                    EqpSearchKind::Seek,
                    index.as_ref().map(eqp_index),
                    match index {
                        Some(index) => seek_constraint_parts(index, seek_def),
                        None => vec!["rowid=?".to_string()],
                    },
                    seek_def.iter_dir == IterationDirection::Backwards,
                ),
                HirBtreeOperation::RowidEq { .. } => search(
                    EqpSearchKind::RowidEq,
                    None,
                    vec!["rowid=?".to_string()],
                    false,
                ),
                HirBtreeOperation::InSeek { index, .. } => search(
                    EqpSearchKind::InSeek,
                    index.as_ref().map(eqp_index),
                    match index {
                        Some(index) => index
                            .columns
                            .first()
                            .map(|column| vec![format!("{}=?", column.name)])
                            .unwrap_or_default(),
                        None => vec!["rowid=?".to_string()],
                    },
                    false,
                ),
            }
        }
        HirSourceAccess::Hash(lookup) => match &lookup.explain {
            Some((operation, table_lookup)) => {
                return hir_loop_explain(
                    document,
                    from,
                    inner_joins,
                    &HirPlannedLoop {
                        source: planned_loop.source,
                        source_position: planned_loop.source_position,
                        access: HirSourceAccess::BTree {
                            operation: operation.clone(),
                            table_lookup: *table_lookup,
                        },
                        estimate: planned_loop.estimate,
                        unmatched_right_rows: None,
                    },
                );
            }
            None => EqpDetail::Scan {
                estimate: planned_loop.estimate,
                table,
                index: None,
                source: EqpRowSource::BTreeTable,
                backwards: false,
                join,
                subquery: None,
            },
        },
        HirSourceAccess::OrUnion(union) => EqpDetail::MultiIndex {
            estimate: planned_loop.estimate,
            table,
            union: true,
            indexes: union
                .branches
                .iter()
                .map(|branch| {
                    selected_hir_btree_index(branch)
                        .map_or_else(|| "PRIMARY KEY".to_string(), |index| index.name.clone())
                })
                .collect(),
        },
        HirSourceAccess::IndexMethod(access) => EqpDetail::IndexMethod {
            estimate: planned_loop.estimate,
            method: access
                .index
                .index_method
                .as_ref()
                .map(|method| method.definition().method_name.to_string())
                .unwrap_or_default(),
        },
        HirSourceAccess::Virtual(_) => EqpDetail::Scan {
            estimate: planned_loop.estimate,
            table,
            index: None,
            source: EqpRowSource::VirtualTable,
            backwards: false,
            join,
            subquery: None,
        },
        HirSourceAccess::Derived {
            seek: Some(operation),
            ..
        } => materialized_search(None, operation)?,
        HirSourceAccess::Cte {
            cte,
            seek: Some(operation),
            ..
        } => materialized_search(Some(*cte), operation)?,
        HirSourceAccess::Derived { seek: None, .. } => subquery(None, false),
        HirSourceAccess::Cte {
            cte, seek: None, ..
        } => subquery(Some(*cte), false),
        HirSourceAccess::RecursiveCte { cte } => subquery(Some(*cte), true),
        HirSourceAccess::RecursiveInput { .. } => EqpDetail::Scan {
            estimate: planned_loop.estimate,
            table,
            index: None,
            source: EqpRowSource::RecursiveCteInput,
            backwards: false,
            join,
            subquery: None,
        },
    })
}

/// How a source joins the sources before it, for EXPLAIN QUERY PLAN.
/// How EXPLAIN QUERY PLAN names a source: a FROM subquery by its alias, a
/// table by its name and alias.
fn hir_eqp_table(document: &HirDocument, source: SourceId) -> Result<EqpTable> {
    let source = document.source(source).ok_or_else(|| {
        LimboError::InternalError(format!("HIR loop references missing source {source}"))
    })?;
    Ok(match &source.kind {
        hir::SourceKind::Derived(_) => EqpTable {
            name: source.alias.clone().unwrap_or_else(|| source.name.clone()),
            alias: None,
        },
        kind => {
            let name = match kind {
                hir::SourceKind::Table(table) | hir::SourceKind::TableFunction { table, .. } => {
                    table.value().get_name().to_string()
                }
                _ => source.name.clone(),
            };
            let alias = source.alias.clone().filter(|alias| *alias != name);
            EqpTable { name, alias }
        }
    })
}

fn hir_loop_explain_join(
    document: &HirDocument,
    from: &hir::From,
    source: SourceId,
) -> Option<EqpJoin> {
    from.joins.iter().find_map(|join| {
        if join.right == source {
            return match join.kind {
                hir::JoinKind::Comma | hir::JoinKind::Right => None,
                hir::JoinKind::Inner => Some(EqpJoin::Inner),
                hir::JoinKind::Cross => Some(EqpJoin::Cross),
                hir::JoinKind::Left => Some(EqpJoin::Left),
                hir::JoinKind::Full => Some(EqpJoin::Full),
                hir::JoinKind::Semi => Some(EqpJoin::Semi),
                hir::JoinKind::Anti => Some(EqpJoin::Anti),
            };
        }
        match document.source(join.right).map(|right| &right.kind) {
            Some(hir::SourceKind::FromGroup(group)) => {
                hir_loop_explain_join(document, &group.from, source)
            }
            _ => None,
        }
    })
}

fn prepare_hir_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    planned_loop: &'a HirPlannedLoop,
    streams: bool,
) -> Result<PreparedHirLoop<'a>> {
    // A subquery's own rows go under its loop's row.
    program.with_source_explain_parent(planned_loop.source, |program| {
        if streams {
            prepare_hir_coroutine_loop(program, plan, planned_loop)
        } else {
            prepare_hir_loop_unexplained(program, plan, planned_loop)
        }
    })
}

/// Whether a FROM subquery or CTE runs as a coroutine, so the outer loop
/// pulls its rows on demand. It is materialized instead when
/// several references share it, when it is hinted MATERIALIZED, and in a
/// block with an outer join, whose NULL rows and unmatched-row pass need a
/// table.
fn hir_loop_streams(
    document: &HirDocument,
    from: &hir::From,
    planned_loop: &HirPlannedLoop,
) -> bool {
    // A LATERAL subquery reads the rows of the loops before it, so it
    // runs inside them: as a coroutine started again for each row, never
    // as a table filled before the loops.
    if let HirSourceAccess::Derived { query, .. } = &planned_loop.access {
        if crate::translate::semantic_to_plan::hir_source_is_lateral(
            document,
            planned_loop.source,
            *query,
        ) {
            return true;
        }
    }
    if hir_from_has_outer_join(document, from) {
        return false;
    }
    match &planned_loop.access {
        HirSourceAccess::Derived { seek: None, .. }
        | HirSourceAccess::Cte {
            materialization: HirCteMaterialization::PerReference,
            seek: None,
            ..
        } => true,
        HirSourceAccess::RecursiveCte { cte } => {
            let references = document
                .sources
                .iter()
                .filter(|source| matches!(source.kind, hir::SourceKind::Cte(id) if id == *cte))
                .count();
            references == 1
                && document
                    .cte(*cte)
                    .is_some_and(|cte| cte.materialized != turso_parser::ast::Materialized::Yes)
        }
        _ => false,
    }
}

fn hir_from_has_outer_join(document: &HirDocument, from: &hir::From) -> bool {
    std::iter::once(from.first)
        .chain(from.joins.iter().map(|join| join.right))
        .any(|source| {
            matches!(
                document.source(source).map(|source| &source.kind),
                Some(hir::SourceKind::FromGroup(group)) if hir_from_has_outer_join(document, &group.from)
            )
        })
        || from.joins.iter().any(|join| {
            matches!(
                join.kind,
                hir::JoinKind::Left | hir::JoinKind::Right | hir::JoinKind::Full
            )
        })
}

/// Define the coroutine of a FROM subquery or recursive CTE. Its body is
/// skipped here and runs each time the loop resumes it; the loop reads the
/// yielded row from registers.
fn prepare_hir_coroutine_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    planned_loop: &'a HirPlannedLoop,
) -> Result<PreparedHirLoop<'a>> {
    let document = &plan.document;
    let source = document.source(planned_loop.source).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR coroutine loop references missing source {}",
            planned_loop.source
        ))
    })?;
    let yield_reg = program.alloc_register();
    let start = program.allocate_label();
    let body_end = program.allocate_label();
    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: body_end,
        start_offset: start,
    });
    program.preassign_label_to_next_insn(start);
    let destination = QueryDestination::CoroutineYield {
        yield_reg,
        stored_outputs: crate::alloc::vec![],
    };
    let row = match &planned_loop.access {
        HirSourceAccess::Derived { query, .. } | HirSourceAccess::Cte { query, .. } => {
            let row = program.alloc_registers(source.columns.len().max(1));
            program.bind_coroutine_row(yield_reg, row);
            emit_planned_query_body(program, plan, *query, &destination)?;
            row
        }
        HirSourceAccess::RecursiveCte { cte } => {
            let definition = document.cte(*cte).ok_or_else(|| {
                LimboError::InternalError(format!("HIR references missing recursive CTE {cte}"))
            })?;
            let hir::CteBody::Recursive(recursive) = &definition.body else {
                return Err(LimboError::InternalError(format!(
                    "HIR CTE {cte} is not recursive"
                )));
            };
            emit_hir_recursive_cte(program, plan, definition, recursive, &destination)?
        }
        _ => {
            return Err(LimboError::InternalError(format!(
                "HIR source {} cannot run as a coroutine",
                planned_loop.source
            )))
        }
    };
    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(body_end);
    program.bind_source(
        source.id,
        SourceBinding::Registers {
            start: row,
            rowid: None,
        },
    );
    Ok(PreparedHirLoop::Coroutine(PreparedHirCoroutineLoop {
        source,
        yield_reg,
        start,
    }))
}

pub(super) fn prepare_hir_loop_unexplained<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    planned_loop: &'a HirPlannedLoop,
) -> Result<PreparedHirLoop<'a>> {
    match &planned_loop.access {
        HirSourceAccess::BTree { .. } => Ok(PreparedHirLoop::BTree(prepare_hir_btree_loop(
            program,
            plan,
            planned_loop,
        )?)),
        HirSourceAccess::Virtual(operation) => Ok(PreparedHirLoop::Virtual(
            prepare_hir_virtual_loop(program, &plan.document, planned_loop.source, operation)?,
        )),
        HirSourceAccess::IndexMethod(access) => Ok(PreparedHirLoop::IndexMethod(
            prepare_hir_index_method_loop(program, plan, planned_loop.source, access)?,
        )),
        HirSourceAccess::OrUnion(union) => Ok(PreparedHirLoop::OrUnion(prepare_hir_or_union_loop(
            program,
            plan,
            planned_loop.source,
            union,
        )?)),
        HirSourceAccess::Derived { query, seek } => {
            let materialized = prepare_hir_materialized_loop(
                program,
                plan,
                planned_loop.source,
                HirMaterializedQuery::Derived(*query),
            )?;
            Ok(with_hir_materialized_seek(
                program,
                materialized,
                seek.as_ref(),
            )?)
        }
        HirSourceAccess::Cte {
            cte,
            query,
            materialization: HirCteMaterialization::PerReference,
            seek,
            ..
        } => {
            let materialized = prepare_hir_materialized_loop(
                program,
                plan,
                planned_loop.source,
                HirMaterializedQuery::Cte {
                    cte: *cte,
                    query: *query,
                },
            )?;
            Ok(with_hir_materialized_seek(
                program,
                materialized,
                seek.as_ref(),
            )?)
        }
        HirSourceAccess::Cte {
            cte,
            query,
            materialization: HirCteMaterialization::Shared | HirCteMaterialization::Explicit,
            seek,
            direction,
        } => {
            let mut materialized =
                prepare_hir_shared_cte_loop(program, plan, planned_loop.source, *cte, *query)?;
            materialized.direction = *direction;
            Ok(with_hir_materialized_seek(
                program,
                materialized,
                seek.as_ref(),
            )?)
        }
        HirSourceAccess::RecursiveCte { cte } => Ok(PreparedHirLoop::Materialized(
            prepare_hir_recursive_cte_loop(program, plan, planned_loop.source, *cte)?,
        )),
        HirSourceAccess::RecursiveInput { cte } => Ok(PreparedHirLoop::Registers(
            prepare_hir_recursive_input_loop(program, &plan.document, planned_loop.source, *cte)?,
        )),
        HirSourceAccess::Hash(lookup) => Ok(PreparedHirLoop::Hash(prepare_hir_hash_loop(
            program,
            plan,
            planned_loop.source,
            lookup,
        )?)),
    }
}

fn prepare_hir_hash_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    lookup: &'a HirHashLookup,
) -> Result<PreparedHirHashLoop<'a>> {
    let document = &plan.document;
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR hash lookup references missing source {source_id}"
        ))
    })?;
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(format!(
            "HIR hash lookup source {source_id} is not a table"
        )));
    };
    let Table::BTree(table) = table.value() else {
        return Err(LimboError::InternalError(format!(
            "HIR hash lookup source {source_id} is not a B-tree table"
        )));
    };
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR hash lookup source {source_id} has no database"
        ))
    })?;
    let snapshot = document.database(database).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR hash lookup source {source_id} references missing database {database:?}"
        ))
    })?;
    program.begin_read_on_database(database.index(), snapshot.schema_version)?;

    let hir::SourceOwner::QueryBlock(block) = source.owner else {
        return Err(LimboError::InternalError(format!(
            "HIR hash lookup source {source_id} is not owned by a query block"
        )));
    };
    let columns = hir_hash_kept_columns(document, block.query, source, lookup.columns.clone());
    let (payload, payload_width, build_input) = match &lookup.build_input {
        None => {
            let payload = program.alloc_registers(source.columns.len().max(1));
            (payload, source.columns.len().max(1), None)
        }
        Some(input) => {
            let (payload, width, prepared) =
                prepare_hir_hash_build_input(program, document, block.query, input, lookup)?;
            (payload, width, Some(prepared))
        }
    };
    let rowid = program.alloc_register();
    if build_input.is_none() {
        program.bind_source(
            source_id,
            SourceBinding::Registers {
                start: payload,
                rowid: Some(rowid),
            },
        );
    }
    Ok(PreparedHirHashLoop {
        source,
        lookup,
        table: table.clone(),
        database: database.index(),
        build_cursor: program.alloc_cursor_id(CursorType::BTreeTable(table.clone())),
        hash_table: program.alloc_hash_table_id(),
        payload,
        columns,
        payload_width,
        rowid,
        build_keys: program.alloc_registers(lookup.keys.len()),
        probe_keys: program.alloc_registers(lookup.keys.len()),
        probe_rowid: program.alloc_register(),
        waiting_pass: program.alloc_register(),
        waiting_next: program.allocate_label(),
        built: {
            let built = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: built,
            });
            built
        },
        build_input,
    })
}

/// The columns of `source` a hash row keeps: `columns`, every column the
/// query reads, and the columns a virtual generated column among them is
/// computed from.
fn hir_hash_kept_columns(
    document: &HirDocument,
    query: QueryId,
    source: &hir::Source,
    mut columns: Vec<usize>,
) -> Vec<usize> {
    for usage in document.source_column_usage(query) {
        if usage.reference.source == source.id && !columns.contains(&usage.reference.column) {
            columns.push(usage.reference.column);
        }
    }
    let mut position = 0;
    while let Some(column) = columns.get(position).copied() {
        position += 1;
        if let Some(hir::ColumnReadExpression::Planned(expression)) =
            source.generated_expressions.get(column)
        {
            expression.for_each(&mut |expression| {
                if let hir::Expr::Column(dependency) = expression {
                    if dependency.source == source.id && !columns.contains(&dependency.column) {
                        columns.push(dependency.column);
                    }
                }
            });
        }
    }
    columns.sort_unstable();
    columns
}

/// Lay out the payload of a hash lookup with a build input: for each source
/// it supplies, one register per column, then its rowid when it has one.
/// Bind the sources to those registers, and open the temporary table the
/// build input fills. Returns the first payload register and its width.
fn prepare_hir_hash_build_input<'a>(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query: QueryId,
    input: &'a HirHashBuildInput,
    lookup: &HirHashLookup,
) -> Result<(usize, usize, PreparedHirHashBuildInput<'a>)> {
    let mut layout = Vec::with_capacity(input.sources.len());
    let mut width = 0;
    for &source_id in &input.sources {
        let source = document.source(source_id).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR hash build input supplies missing source {source_id}"
            ))
        })?;
        let columns = hir_hash_kept_columns(document, query, source, Vec::new());
        let has_rowid = matches!(
            &source.kind,
            hir::SourceKind::Table(table)
                if table.value().btree().is_some_and(|table| table.has_rowid)
        );
        let column_width = source.columns.len().max(1);
        layout.push((source_id, columns, column_width, has_rowid));
        width += column_width + usize::from(has_rowid);
    }
    let payload = program.alloc_registers(width);
    let mut start = payload;
    let mut sources = Vec::with_capacity(layout.len());
    for (source_id, columns, column_width, has_rowid) in layout {
        let rowid = has_rowid.then_some(start + column_width);
        program.bind_source(source_id, SourceBinding::Registers { start, rowid });
        sources.push(PreparedHirPayloadSource {
            source: source_id,
            columns,
            start,
            rowid,
        });
        start += column_width + usize::from(has_rowid);
    }
    let keys = lookup.keys.len();
    let columns = (0..keys)
        .map(|key| format!("key_{key}"))
        .chain((0..width).map(|slot| format!("payload_{slot}")))
        .map(|name| Column::new_default_text(Some(name), "BLOB".to_string(), None))
        .collect_vec();
    let table = Arc::new(BTreeTable::new(
        0,
        "hash_build_input".to_string(),
        crate::alloc::vec![],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ));
    Ok((
        payload,
        width,
        PreparedHirHashBuildInput {
            input,
            cursor: program.alloc_cursor_id(CursorType::BTreeTable(table.clone())),
            table,
            record: program.alloc_registers(keys + width),
            sources,
        },
    ))
}

/// Read the kept columns and rowids of the build input's current row into
/// the payload registers of the sources it supplies.
fn emit_hir_hash_input_payload(program: &mut ProgramBuilder, prepared: &PreparedHirHashLoop<'_>) {
    let input = prepared
        .build_input
        .as_ref()
        .expect("a hash lookup with a build input");
    let keys = prepared.lookup.keys.len();
    for source in &input.sources {
        for register in source
            .columns
            .iter()
            .map(|column| source.start + column)
            .chain(source.rowid)
        {
            program.emit_column_or_rowid(
                input.cursor,
                keys + (register - prepared.payload),
                register,
            );
        }
    }
}

/// Fill a hash lookup's build input once: run its loops, storing each
/// joined row's hash keys and the kept columns of the sources it supplies
/// in the temporary table. Its sources then read the hash row again.
fn emit_hir_hash_build_input(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    from: Option<&hir::From>,
    loops_plan: HirLoopsPlan<'_>,
    prepared: &PreparedHirHashLoop<'_>,
    input: &PreparedHirHashBuildInput<'_>,
) -> Result<()> {
    let document = &plan.document;
    let keys = &prepared.lookup.keys;
    let width = keys.len() + prepared.payload_width;
    let filled = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: filled,
    });
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: input.cursor,
        is_table: true,
    });
    // Columns the query never reads stay NULL.
    program.emit_insn(Insn::Null {
        dest: input.record,
        dest_end: (width > 1).then_some(input.record + width - 1),
    });
    let store = |program: &mut ProgramBuilder, _reject: BranchOffset| -> Result<()> {
        for (position, key) in keys.iter().enumerate() {
            super::expr::translate_expr_no_constant_opt(
                program,
                document,
                &key.build,
                input.record + position,
            )?;
        }
        let slot = |register: usize| input.record + keys.len() + (register - prepared.payload);
        for source in &input.sources {
            for column in &source.columns {
                super::expr::translate_expr_no_constant_opt(
                    program,
                    document,
                    &hir::Expr::column(source.source, *column),
                    slot(source.start + column),
                )?;
            }
            if let Some(rowid) = source.rowid {
                super::expr::translate_expr_no_constant_opt(
                    program,
                    document,
                    &hir::Expr::rowid(source.source),
                    slot(rowid),
                )?;
            }
        }
        let record = program.alloc_register();
        let rowid = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(input.record),
            count: to_u32(width),
            dest_reg: to_u32(record),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: input.cursor,
            rowid_reg: rowid,
            prev_largest_reg: 0,
        });
        program.emit_insn(Insn::Insert {
            cursor: input.cursor,
            key_reg: rowid,
            record_reg: record,
            flag: InsertFlags::new().is_ephemeral_table_insert(),
            table_name: String::new(),
        });
        Ok(())
    };
    // The build loops read the hash rows once. An expression their index
    // supplied is computed again from the stored row when a probe reads it.
    let bound_before_build = program.expression_register_count();
    emit_join_loops(
        program,
        plan,
        from,
        HirLoopsPlan {
            groups: &[],
            loops: &input.input.loops,
            predicates: &input.input.predicates,
            where_start: loops_plan.where_start,
            later_on_terms: loops_plan.later_on_terms,
            inner_joins: loops_plan.inner_joins,
            deferred_predicates: loops_plan.deferred_predicates,
            build_input: true,
        },
        HirLoopBody::Dml(&store),
    )?;
    program.truncate_expression_registers(bound_before_build);
    program.preassign_label_to_next_insn(filled);
    // The loops bound the sources to their own cursors.
    for source in &input.sources {
        program.bind_source(
            source.source,
            SourceBinding::Registers {
                start: source.start,
                rowid: source.rowid,
            },
        );
    }
    Ok(())
}

/// Build the hash table the first time the loop is reached, then look up
/// the current values from the earlier loops. `previous_cursor` reads the
/// previous loop's table, whose rowid a waiting lookup keeps.
fn start_hir_hash_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirHashLoop<'_>,
    previous_cursor: CursorID,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    let keys = &prepared.lookup.keys;
    let affinities = keys
        .iter()
        .map(|key| key.affinity.aff_mask())
        .collect::<String>();
    let key_count = std::num::NonZeroUsize::new(keys.len())
        .ok_or_else(|| LimboError::InternalError("HIR hash lookup has no keys".into()))?;
    let column_count = prepared.payload_width;

    let built = program.allocate_label();
    program.emit_insn(Insn::IfPos {
        reg: prepared.built,
        target_pc: built,
        decrement_by: 0,
    });
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: built,
    });
    // The table the hash table is built from: the build input's temporary
    // table, whose rows hold the keys then the payload, or the table itself.
    let build_cursor = match &prepared.build_input {
        Some(input) => input.cursor,
        None => {
            program.emit_insn(Insn::OpenRead {
                cursor_id: prepared.build_cursor,
                root_page: prepared.table.root_page,
                db: prepared.database,
            });
            program.bind_source(
                prepared.source.id,
                SourceBinding::BTree {
                    scan_cursor: prepared.build_cursor,
                    table_cursor: None,
                },
            );
            prepared.build_cursor
        }
    };
    program.emit_insn(Insn::Null {
        dest: prepared.payload,
        dest_end: (column_count > 1).then_some(prepared.payload + column_count - 1),
    });
    let build_loop = program.allocate_label();
    let build_end = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: build_cursor,
        pc_if_empty: build_end,
    });
    program.preassign_label_to_next_insn(build_loop);
    match &prepared.build_input {
        Some(input) => {
            for position in 0..keys.len() {
                program.emit_column_or_rowid(
                    input.cursor,
                    position,
                    prepared.build_keys + position,
                );
            }
        }
        None => {
            for (position, key) in keys.iter().enumerate() {
                super::expr::translate_expr_no_constant_opt(
                    program,
                    document,
                    &key.build,
                    prepared.build_keys + position,
                )?;
            }
        }
    }
    program.emit_insn(Insn::Affinity {
        start_reg: prepared.build_keys,
        count: key_count,
        affinities: affinities.clone(),
    });
    match &prepared.build_input {
        Some(_) => emit_hir_hash_input_payload(program, prepared),
        None => {
            for column in &prepared.columns {
                super::expr::translate_expr_no_constant_opt(
                    program,
                    document,
                    &hir::Expr::Column(hir::ColumnRef {
                        source: prepared.source.id,
                        column: *column,
                    }),
                    prepared.payload + column,
                )?;
            }
        }
    }
    program.emit_insn(Insn::HashBuild {
        data: Box::new(crate::vdbe::insn::HashBuildData {
            cursor_id: build_cursor,
            key_start_reg: prepared.build_keys,
            num_keys: keys.len(),
            hash_table_id: prepared.hash_table,
            mem_budget: crate::vdbe::hash_table::DEFAULT_MEM_BUDGET,
            collations: keys
                .iter()
                .map(|key| key.collation.unwrap_or(CollationSeq::Binary))
                .collect(),
            payload_start_reg: Some(prepared.payload),
            num_payload: column_count,
            track_matched: false,
        }),
    });
    program.emit_insn(Insn::Next {
        cursor_id: build_cursor,
        is_index: program.cursor_is_index(build_cursor),
        pc_if_next: build_loop,
        fullscan: true,
    });
    program.preassign_label_to_next_insn(build_end);
    program.emit_insn(Insn::HashBuildFinalize {
        hash_table_id: prepared.hash_table,
    });
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: prepared.built,
    });
    program.preassign_label_to_next_insn(built);
    match &prepared.build_input {
        Some(input) => {
            for source in &input.sources {
                program.bind_source(
                    source.source,
                    SourceBinding::Registers {
                        start: source.start,
                        rowid: source.rowid,
                    },
                );
            }
        }
        None => program.bind_source(
            prepared.source.id,
            SourceBinding::Registers {
                start: prepared.payload,
                rowid: Some(prepared.rowid),
            },
        ),
    }

    for (position, key) in keys.iter().enumerate() {
        super::expr::translate_expr(
            program,
            document,
            &key.value,
            prepared.probe_keys + position,
        )?;
    }
    program.emit_insn(Insn::Affinity {
        start_reg: prepared.probe_keys,
        count: key_count,
        affinities,
    });
    program.emit_insn(Insn::RowId {
        cursor_id: previous_cursor,
        dest: prepared.probe_rowid,
    });
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: prepared.waiting_pass,
    });
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let exhausted = program.allocate_label();
    let deferred = program.allocate_label();
    program.emit_insn(Insn::HashProbe {
        hash_table_id: to_u32(prepared.hash_table),
        key_start_reg: to_u32(prepared.probe_keys),
        num_keys: to_u32(keys.len()),
        dest_reg: to_u32(prepared.rowid),
        target_pc: exhausted,
        deferred_target_pc: deferred,
        payload_dest_reg: Some(to_u32(prepared.payload)),
        num_payload: to_u32(column_count),
        probe_rowid_reg: Some(to_u32(prepared.probe_rowid)),
    });
    program.preassign_label_to_next_insn(loop_start);
    Ok(HirBtreeLoop {
        cursor: None,
        loop_start,
        next,
        exhausted,
        advance: HirLoopAdvance::Hash {
            hash_table: prepared.hash_table,
            rowid: prepared.rowid,
            payload: prepared.payload,
            columns: column_count,
            waiting_pass: prepared.waiting_pass,
            waiting_next: prepared.waiting_next,
            deferred,
        },
        left_join,
    })
}

/// After the loop before a hash lookup ends, run the lookups that waited for
/// rows the hash table moved to disk, one disk partition at a time. Each
/// goes back to its row of the previous loop by rowid and joins the shared
/// loop body at the lookup's first match. Nothing runs when the hash table
/// stayed in memory.
///
/// In a LEFT JOIN, a waiting lookup whose matches all fail, or that has
/// none, gets its NULL row here.
fn emit_hir_hash_waiting_lookups(
    program: &mut ProgramBuilder,
    prepared: &PreparedHirHashLoop<'_>,
    hash_loop: &HirBtreeLoop,
    previous_cursor: CursorID,
    left_join: Option<&HirLeftJoin>,
) {
    let hash_table = to_u32(prepared.hash_table);
    let column_count = prepared.payload_width;
    let done = program.allocate_label();
    let partition = program.allocate_label();
    let next_lookup = program.allocate_label();
    let partition_done = program.allocate_label();
    let finished = program.allocate_label();
    // Where a lookup goes when it runs out of matches.
    let unmatched = if left_join.is_some() {
        program.allocate_label()
    } else {
        next_lookup
    };
    program.emit_insn(Insn::HashGraceInit {
        hash_table_id: hash_table,
        target_pc: done,
    });
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: prepared.waiting_pass,
    });
    program.preassign_label_to_next_insn(partition);
    program.emit_insn(Insn::HashGraceLoadPartition {
        hash_table_id: hash_table,
        target_pc: finished,
    });
    program.preassign_label_to_next_insn(next_lookup);
    program.emit_insn(Insn::HashGraceNextProbe {
        hash_table_id: hash_table,
        key_start_reg: to_u32(prepared.probe_keys),
        num_keys: to_u32(prepared.lookup.keys.len()),
        probe_rowid_dest: to_u32(prepared.probe_rowid),
        target_pc: partition_done,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: previous_cursor,
        src_reg: prepared.probe_rowid,
        target_pc: next_lookup,
    });
    if let Some(left_join) = left_join {
        left_join.metadata.reset(program);
    }
    program.emit_insn(Insn::HashProbe {
        hash_table_id: hash_table,
        key_start_reg: to_u32(prepared.probe_keys),
        num_keys: to_u32(prepared.lookup.keys.len()),
        dest_reg: to_u32(prepared.rowid),
        target_pc: unmatched,
        deferred_target_pc: next_lookup,
        payload_dest_reg: Some(to_u32(prepared.payload)),
        num_payload: to_u32(column_count),
        probe_rowid_reg: None,
    });
    program.emit_insn(Insn::Goto {
        target_pc: hash_loop.loop_start,
    });
    program.preassign_label_to_next_insn(prepared.waiting_next);
    if left_join.is_some() {
        // After its NULL row, the lookup has no match to advance.
        program.emit_insn(Insn::IsNull {
            reg: prepared.rowid,
            target_pc: unmatched,
        });
    }
    program.emit_insn(Insn::HashNext {
        hash_table_id: prepared.hash_table,
        dest_reg: prepared.rowid,
        target_pc: unmatched,
        payload_dest_reg: Some(prepared.payload),
        num_payload: column_count,
    });
    program.emit_insn(Insn::Goto {
        target_pc: hash_loop.loop_start,
    });
    if let Some(left_join) = left_join {
        program.preassign_label_to_next_insn(unmatched);
        program.emit_insn(Insn::IfPos {
            reg: left_join.metadata.reg_match_flag,
            target_pc: next_lookup,
            decrement_by: 0,
        });
        emit_hir_hash_null_row(program, prepared.payload, column_count, prepared.rowid);
        program.emit_insn(Insn::Goto {
            target_pc: left_join.metadata.label_match_flag_set_true,
        });
    }
    program.preassign_label_to_next_insn(partition_done);
    program.emit_insn(Insn::HashGraceAdvancePartition {
        hash_table_id: hash_table,
        target_pc: finished,
    });
    program.emit_insn(Insn::Goto {
        target_pc: partition,
    });
    program.preassign_label_to_next_insn(finished);
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: prepared.waiting_pass,
    });
    program.preassign_label_to_next_insn(done);
}

/// Read NULL for every column and the rowid of a hash lookup's table, for
/// a LEFT JOIN row without a match.
fn emit_hir_hash_null_row(
    program: &mut ProgramBuilder,
    payload: usize,
    columns: usize,
    rowid: usize,
) {
    program.emit_insn(Insn::Null {
        dest: payload,
        dest_end: (columns > 1).then_some(payload + columns - 1),
    });
    program.emit_insn(Insn::Null {
        dest: rowid,
        dest_end: None,
    });
}

fn prepare_hir_recursive_input_loop<'a>(
    program: &ProgramBuilder,
    document: &'a HirDocument,
    source_id: SourceId,
    cte: hir::CteId,
) -> Result<PreparedHirRegistersLoop<'a>> {
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR recursive input references missing source {source_id}"
        ))
    })?;
    if !matches!(source.kind, hir::SourceKind::RecursiveInput(source_cte) if source_cte == cte) {
        return Err(LimboError::InternalError(format!(
            "HIR recursive input source {source_id} does not match CTE {cte}"
        )));
    }
    if !matches!(
        program.source_binding(source_id),
        Some(SourceBinding::Registers { .. })
    ) {
        return Err(LimboError::InternalError(format!(
            "HIR recursive input source {source_id} has no row binding"
        )));
    }
    Ok(PreparedHirRegistersLoop { source })
}

fn prepare_hir_virtual_loop<'a>(
    program: &mut ProgramBuilder,
    document: &'a HirDocument,
    source_id: SourceId,
    operation: &'a HirVirtualTableOperation,
) -> Result<PreparedHirVirtualLoop<'a>> {
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR virtual-table loop references missing source {source_id}"
        ))
    })?;
    let table = match &source.kind {
        hir::SourceKind::Table(table) | hir::SourceKind::TableFunction { table, .. } => table,
        _ => {
            return Err(LimboError::InternalError(format!(
                "HIR virtual-table loop source {} is not a table",
                source.id
            )));
        }
    };
    let Table::Virtual(table) = table.value() else {
        return Err(LimboError::InternalError(format!(
            "HIR virtual-table loop source {} is not a virtual table",
            source.id
        )));
    };
    // The statement owns the read transaction a virtual table's helper
    // statement (such as the PRAGMA behind pragma_table_info) would
    // otherwise start as a nested statement; a transaction a nested
    // statement starts is never finished at this statement's Halt.
    program.begin_read_operation()?;
    let cursor = program.alloc_cursor_id(CursorType::VirtualTable(table.clone()));
    program.emit_insn(Insn::VOpen { cursor_id: cursor });
    program.bind_source(source.id, SourceBinding::Virtual { cursor });
    Ok(PreparedHirVirtualLoop {
        source,
        operation,
        cursor,
    })
}

/// The index a B-tree operation reads, if any.
fn selected_hir_btree_index(operation: &HirBtreeOperation) -> Option<&Arc<Index>> {
    match operation {
        HirBtreeOperation::Scan { index, .. }
        | HirBtreeOperation::Seek { index, .. }
        | HirBtreeOperation::InSeek { index, .. } => index.as_ref(),
        HirBtreeOperation::RowidEq { .. } => None,
    }
}

fn prepare_hir_or_union_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    union: &'a crate::translate::optimizer::HirOrUnion,
) -> Result<PreparedHirOrUnionLoop<'a>> {
    let document = &plan.document;
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR OR loop references missing source {source_id}"))
    })?;
    let table = match &source.kind {
        hir::SourceKind::Table(table) => table.value().btree(),
        _ => None,
    }
    .filter(|table| table.has_rowid)
    .ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR OR loop source {source_id} is not a rowid table"
        ))
    })?;
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!("HIR OR loop source {source_id} has no database"))
    })?;
    let database_snapshot = document.database(database).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR OR loop source {source_id} references missing database {database:?}"
        ))
    })?;
    program.begin_read_on_database(database.index(), database_snapshot.schema_version)?;
    let table_cursor = program.alloc_cursor_id(match plan.materialized_view(source.id) {
        Some(view) => CursorType::MaterializedView(table.clone(), view.clone()),
        None => CursorType::BTreeTable(table.clone()),
    });
    program.emit_insn(Insn::OpenRead {
        cursor_id: table_cursor,
        root_page: table.root_page,
        db: database.index(),
    });
    let branches = union
        .branches
        .iter()
        .map(|operation| {
            let index = selected_hir_btree_index(operation);
            // A rowid seek reads the table itself.
            let cursor = match index {
                Some(index) => {
                    let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
                    program.emit_insn(Insn::OpenRead {
                        cursor_id: cursor,
                        root_page: index.root_page,
                        db: database.index(),
                    });
                    cursor
                }
                None => table_cursor,
            };
            PreparedHirBtreeLoop {
                source,
                operation,
                index,
                cursor,
                table_cursor: None,
                autoindex_source_cursor: None,
                table_has_rowid: true,
                index_keys: Vec::new(),
            }
        })
        .collect();
    program.bind_source(
        source.id,
        SourceBinding::BTree {
            scan_cursor: table_cursor,
            table_cursor: None,
        },
    );
    Ok(PreparedHirOrUnionLoop {
        table_cursor,
        branches,
        branch_filters: &union.branch_filters,
    })
}

fn start_hir_or_union_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirOrUnionLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    // Collect the rowids each branch finds. The row set starts empty each
    // time the loop starts, as it does for each row of an outer loop.
    let rows = program.alloc_register();
    program.emit_null(rows, None);
    let rowid = program.alloc_register();
    for (branch, filters) in prepared.branches.iter().zip(prepared.branch_filters) {
        // An outer row the branch's own conditions reject seeks nothing.
        let branch_done = program.allocate_label();
        for filter in filters {
            let passed = program.allocate_label();
            super::expr::translate_condition_expr(
                program,
                document,
                filter,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true: passed,
                    jump_target_when_false: branch_done,
                },
            )?;
            program.preassign_label_to_next_insn(passed);
        }
        let branch_loop = start_hir_btree_loop(program, document, branch, None)?;
        match branch.index {
            Some(_) => program.emit_insn(Insn::IdxRowId {
                cursor_id: branch.cursor,
                dest: rowid,
            }),
            None => program.emit_insn(Insn::RowId {
                cursor_id: branch.cursor,
                dest: rowid,
            }),
        }
        program.emit_insn(Insn::RowSetAdd {
            rowset_reg: rows,
            value_reg: rowid,
        });
        close_hir_btree_loop(program, &branch_loop, &[]);
        program.preassign_label_to_next_insn(branch_done);
    }
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let exhausted = program.allocate_label();
    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::RowSetRead {
        rowset_reg: rows,
        pc_if_empty: exhausted,
        dest_reg: rowid,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: prepared.table_cursor,
        src_reg: rowid,
        target_pc: loop_start,
    });
    Ok(HirBtreeLoop {
        cursor: Some(prepared.table_cursor),
        loop_start,
        next,
        exhausted,
        advance: HirLoopAdvance::Restart,
        left_join,
    })
}

fn prepare_hir_index_method_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    access: &'a crate::translate::hir_index_method::HirIndexMethodAccess,
) -> Result<PreparedHirIndexMethodLoop<'a>> {
    let document = &plan.document;
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR index-method loop references missing source {source_id}"
        ))
    })?;
    let table = match &source.kind {
        hir::SourceKind::Table(table) => table.value().btree(),
        _ => None,
    }
    .ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR index-method loop source {source_id} is not a B-tree table"
        ))
    })?;
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR index-method loop source {source_id} has no database"
        ))
    })?;
    let database_snapshot = document.database(database).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR index-method loop source {source_id} references missing database {database:?}"
        ))
    })?;
    program.begin_read_on_database(database.index(), database_snapshot.schema_version)?;
    let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let index_cursor = program.alloc_cursor_index(None, &access.index)?;
    program.emit_insn(Insn::OpenRead {
        cursor_id: table_cursor,
        root_page: table.root_page,
        db: database.index(),
    });
    program.emit_insn(Insn::OpenRead {
        cursor_id: index_cursor,
        root_page: access.index.root_page,
        db: database.index(),
    });
    // Columns are read from the table, which follows the index's rowids.
    program.bind_source(
        source.id,
        SourceBinding::BTree {
            scan_cursor: table_cursor,
            table_cursor: None,
        },
    );
    // The expressions of this block that the index returns.
    let covered = find_block_subexpressions(plan, source, |expression| {
        access
            .covered
            .iter()
            .find(|(covered, _)| covered.equivalent(expression))
            .map(|(_, column)| *column)
    })?;
    Ok(PreparedHirIndexMethodLoop {
        access,
        index_cursor,
        table_cursor,
        database: database.index(),
        covered,
    })
}

/// Every expression the query block owning `source` evaluates per row: its
/// outputs, grouping keys and HAVING, its query's ORDER BY, and the WHERE
/// terms its plan left to check.
fn hir_block_expressions<'a>(
    plan: &'a HirPlan,
    source: &hir::Source,
) -> Result<Vec<&'a hir::Expr>> {
    let document = &plan.document;
    let hir::SourceOwner::QueryBlock(block_id) = source.owner else {
        return Ok(Vec::new());
    };
    let block = document.query_block(block_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR query block {block_id:?} is missing"))
    })?;
    let query = document.query(block_id.query).ok_or_else(|| {
        LimboError::InternalError(format!("HIR query {} is missing", block_id.query))
    })?;
    let mut roots = block
        .outputs
        .iter()
        .map(|output| &output.expr)
        .collect::<Vec<_>>();
    if let Some(grouping) = hir_grouping(block) {
        roots.extend(&grouping.keys);
        roots.extend(&grouping.having);
    }
    roots.extend(query.order_by.iter().map(|term| &term.expr));
    if let Some(planned) = plan.planned_query(block_id.query).and_then(|planned| {
        planned
            .blocks
            .iter()
            .find(|planned| planned.block == block_id)
    }) {
        roots.extend(
            planned
                .predicates
                .iter()
                .filter(|term| !term.consumed)
                .map(|term| &term.expr),
        );
    }
    Ok(roots)
}

/// The largest parts of the block's expressions for which `matches` names a
/// column of a cursor, with that column.
fn find_block_subexpressions<'a>(
    plan: &'a HirPlan,
    source: &hir::Source,
    matches: impl Fn(&hir::Expr) -> Option<usize>,
) -> Result<Vec<(&'a hir::Expr, usize)>> {
    let mut found = Vec::new();
    let mut pending = hir_block_expressions(plan, source)?;
    while let Some(expression) = pending.pop() {
        if let Some(column) = matches(expression) {
            found.push((expression, column));
            continue;
        }
        pending.extend((0..).map_while(|position| expression.child(position)));
    }
    Ok(found)
}

fn start_hir_index_method_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirIndexMethodLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let exhausted = program.allocate_label();
    emit_index_method_query(
        program,
        document,
        prepared.access,
        prepared.database,
        prepared.index_cursor,
        exhausted,
    )?;
    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::DeferredSeek {
        index_cursor_id: prepared.index_cursor,
        table_cursor_id: prepared.table_cursor,
    });
    for (expression, column) in &prepared.covered {
        let register = program.alloc_register();
        program.emit_column_or_rowid(prepared.index_cursor, *column, register);
        program.bind_expression_register(expression, register);
    }
    Ok(HirBtreeLoop {
        cursor: Some(prepared.index_cursor),
        loop_start,
        next,
        exhausted,
        advance: HirLoopAdvance::Cursor {
            direction: IterationDirection::Forwards,
            fullscan: false,
        },
        left_join,
    })
}

/// Start an index method's search on `cursor`: the pattern's number, the
/// values of its placeholders, then the FTS columns the query names, when
/// the method has them.
fn emit_index_method_query(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    access: &crate::translate::hir_index_method::HirIndexMethodAccess,
    database: usize,
    cursor: CursorID,
    pc_if_empty: BranchOffset,
) -> Result<()> {
    let arguments = &access.arguments;
    let fields = access.fields.as_ref();
    let count = arguments.len() + 1 + usize::from(fields.is_some());
    let start = program.alloc_registers(count);
    program.emit_int(access.pattern as i64, start);
    for (position, argument) in arguments.iter().enumerate() {
        super::expr::translate_expr(program, document, argument, start + 1 + position)?;
    }
    if let Some(fields) = fields {
        program.emit_string8(fields.clone(), start + 1 + arguments.len());
    }
    program.emit_insn(Insn::IndexMethodQuery {
        db: database,
        cursor_id: cursor,
        start_reg: start,
        count_reg: count,
        pc_if_empty,
    });
    Ok(())
}

/// Emit a WHERE or ON term for the row the loops found, jumping to `fail`
/// when it does not hold. A term an index method answers, such as
/// `fts_match(x, y)` with an FTS index on `x`, is checked through the
/// index: the term's arguments are read from the row, the index searches
/// for them, and the row passes when its rowid is among the results. Any
/// other term is evaluated as an expression.
pub(super) fn emit_hir_predicate(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    predicate: &crate::translate::plan::HirWhereTerm,
    fail: BranchOffset,
) -> Result<()> {
    let passed = program.allocate_label();
    if let Some((source, access)) = index_method_predicate(document, predicate) {
        emit_index_method_predicate(program, document, source, &access, passed, fail)?;
    } else {
        super::expr::translate_condition_expr(
            program,
            document,
            &predicate.expr,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: passed,
                jump_target_when_false: fail,
            },
        )?;
    }
    program.preassign_label_to_next_insn(passed);
    Ok(())
}

/// The source whose index method answers `predicate` for a row of it, with
/// the search to run.
fn index_method_predicate(
    document: &HirDocument,
    predicate: &crate::translate::plan::HirWhereTerm,
) -> Option<(
    SourceId,
    crate::translate::hir_index_method::HirIndexMethodAccess,
)> {
    let mut sources = Vec::new();
    document.visit_expr_sources(&predicate.expr, &mut |source| {
        if !sources.contains(&source) {
            sources.push(source);
        }
    });
    sources.into_iter().find_map(|source_id| {
        let source = document.source(source_id)?;
        if source.index_expressions.is_empty() {
            return None;
        }
        crate::translate::hir_index_method::match_index_method_predicate(
            document, source, predicate,
        )
        .map(|access| (source_id, access))
    })
}

fn emit_index_method_predicate(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    source_id: SourceId,
    access: &crate::translate::hir_index_method::HirIndexMethodAccess,
    passed: BranchOffset,
    fail: BranchOffset,
) -> Result<()> {
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR index-method predicate references missing source {source_id}"
        ))
    })?;
    let database = source
        .database
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR index-method predicate source {source_id} has no database"
            ))
        })?
        .index();
    crate::emit_explain!(
        program,
        false,
        EqpDetail::IndexMethod {
            method: access
                .index
                .index_method
                .as_ref()
                .map(|method| method.definition().method_name.to_string())
                .unwrap_or_default(),
            estimate: None,
        }
    );
    // The index is searched with its own cursor, so the loop's cursors
    // stay on the row.
    let cursor = program.alloc_cursor_index(None, &access.index)?;
    let opened = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: opened,
    });
    program.emit_insn(Insn::OpenRead {
        cursor_id: cursor,
        root_page: access.index.root_page,
        db: database,
    });
    program.preassign_label_to_next_insn(opened);
    let rowid = program.alloc_register();
    super::expr::translate_expr(program, document, &hir::Expr::RowId(source_id), rowid)?;
    program.emit_insn(Insn::IsNull {
        reg: rowid,
        target_pc: fail,
    });
    emit_index_method_query(program, document, access, database, cursor, fail)?;
    let found = program.alloc_register();
    let search_next = program.allocate_label();
    program.preassign_label_to_next_insn(search_next);
    program.emit_insn(Insn::IdxRowId {
        cursor_id: cursor,
        dest: found,
    });
    program.emit_insn(Insn::Eq {
        lhs: rowid,
        rhs: found,
        target_pc: passed,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        is_index: program.cursor_is_index(cursor),
        pc_if_next: search_next,
        fullscan: false,
    });
    program.emit_insn(Insn::Goto { target_pc: fail });
    Ok(())
}

fn prepare_hir_shared_cte_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    cte: hir::CteId,
    query: QueryId,
) -> Result<PreparedHirMaterializedLoop<'a>> {
    let materialized_query = HirMaterializedQuery::Cte { cte, query };
    let source = validate_hir_materialized_query(plan, source_id, materialized_query)?;
    if let Some(shared) = program.hir_materialized_cte(cte).cloned() {
        // The reader that filled the table reads it through its own cursor;
        // the others open a second cursor on the same table.
        let filled_here = matches!(
            program.source_binding(source_id),
            Some(SourceBinding::BTree { scan_cursor, .. }) if *scan_cursor == shared.cursor_id
        );
        let cursor = if filled_here {
            // This reader is being emitted again, such as by the pass that
            // reads the unmatched rows of a RIGHT JOIN: its first copy may
            // never run, so fill the table from here too.
            if let Some(fill_subroutine) = shared.fill_subroutine {
                program.emit_insn(Insn::Gosub {
                    target_pc: fill_subroutine.label,
                    return_reg: fill_subroutine.return_reg,
                });
            }
            shared.cursor_id
        } else {
            // The reader that filled the table may sit in a loop that never
            // runs, so fill it from here too before reading.
            if let Some(fill_subroutine) = shared.fill_subroutine {
                program.emit_insn(Insn::Gosub {
                    target_pc: fill_subroutine.label,
                    return_reg: fill_subroutine.return_reg,
                });
            }
            let cursor = program.alloc_cursor_id(CursorType::BTreeTable(shared.table.clone()));
            program.emit_insn(Insn::OpenDup {
                new_cursor_id: cursor,
                original_cursor_id: shared.cursor_id,
            });
            program.bind_source(
                source_id,
                SourceBinding::BTree {
                    scan_cursor: cursor,
                    table_cursor: None,
                },
            );
            cursor
        };
        return Ok(PreparedHirMaterializedLoop {
            source,
            cursor,
            table: shared.table,
            direction: IterationDirection::Forwards,
            fill_subroutine: None,
        });
    }

    let prepared = prepare_hir_materialized_loop(program, plan, source_id, materialized_query)?;
    program.register_hir_materialized_cte(
        cte,
        MaterializedCteInfo {
            cursor_id: prepared.cursor,
            table: prepared.table.clone(),
            fill_subroutine: prepared.fill_subroutine,
        },
    );
    Ok(prepared)
}

fn prepare_hir_recursive_cte_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    cte_id: hir::CteId,
) -> Result<PreparedHirMaterializedLoop<'a>> {
    let document = &plan.document;
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR recursive CTE loop references missing source {source_id}"
        ))
    })?;
    if !matches!(source.kind, hir::SourceKind::Cte(source_cte) if source_cte == cte_id) {
        return Err(LimboError::InternalError(format!(
            "HIR recursive CTE source {source_id} does not match CTE {cte_id}"
        )));
    }
    let cte = document.cte(cte_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR references missing recursive CTE {cte_id}"))
    })?;
    let hir::CteBody::Recursive(recursive) = &cte.body else {
        return Err(LimboError::InternalError(format!(
            "HIR CTE {cte_id} is not recursive"
        )));
    };
    if source.columns.len() != cte.columns.len() {
        return Err(LimboError::InternalError(format!(
            "HIR recursive CTE source {source_id} width does not match CTE {cte_id}"
        )));
    }
    // A recursive CTE that reads outer columns runs inside the outer loop:
    // it is computed again, into a fresh table, every time this point is
    // reached, and each reference computes its own.
    let correlated = hir_recursive_cte_reads_outer_sources(document, recursive);
    if let Some(shared) = program
        .hir_materialized_cte(cte_id)
        .cloned()
        .filter(|_| !correlated)
    {
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(shared.table.clone()));
        program.emit_insn(Insn::OpenDup {
            new_cursor_id: cursor,
            original_cursor_id: shared.cursor_id,
        });
        program.bind_source(
            source_id,
            SourceBinding::BTree {
                scan_cursor: cursor,
                table_cursor: None,
            },
        );
        return Ok(PreparedHirMaterializedLoop {
            source,
            cursor,
            table: shared.table,
            direction: IterationDirection::Forwards,
            fill_subroutine: None,
        });
    }

    let table = hir_materialized_table(source);
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let materialized = program.allocate_label();
    if !correlated {
        program.emit_insn(Insn::Once {
            target_pc_when_reentered: materialized,
        });
    }
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: true,
    });
    emit_hir_recursive_cte(
        program,
        plan,
        cte,
        recursive,
        &QueryDestination::EphemeralTable {
            cursor_id: cursor,
            table: table.clone(),
            rowid_mode: EphemeralRowidMode::Auto,
        },
    )?;
    program.preassign_label_to_next_insn(materialized);
    program.bind_source(
        source_id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor: None,
        },
    );
    if !correlated {
        program.register_hir_materialized_cte(
            cte_id,
            MaterializedCteInfo {
                cursor_id: cursor,
                table: table.clone(),
                fill_subroutine: None,
            },
        );
    }
    Ok(PreparedHirMaterializedLoop {
        source,
        cursor,
        table,
        direction: IterationDirection::Forwards,
        fill_subroutine: None,
    })
}

/// Whether a recursive CTE's seed or steps read a source of an enclosing
/// query, other than the CTE's own recursive input.
fn hir_recursive_cte_reads_outer_sources(
    document: &HirDocument,
    recursive: &hir::RecursiveCte,
) -> bool {
    std::iter::once(recursive.seed)
        .chain(recursive.arms.iter().map(|arm| arm.query))
        .flat_map(|query| document.outer_sources(query))
        .any(|source| !recursive.input_sources.contains(&source))
}

fn emit_hir_recursive_cte(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    cte: &hir::Cte,
    recursive: &hir::RecursiveCte,
    destination: &QueryDestination,
) -> Result<usize> {
    let union_all = match recursive.arms.first().map(|arm| arm.operator) {
        Some(CompoundOperator::UnionAll) => true,
        Some(CompoundOperator::Union) => false,
        _ => {
            return Err(LimboError::InternalError(format!(
                "recursive CTE {} does not use UNION or UNION ALL",
                cte.id
            )));
        }
    };
    if recursive.arms.iter().any(|arm| {
        arm.operator
            != if union_all {
                CompoundOperator::UnionAll
            } else {
                CompoundOperator::Union
            }
    }) {
        return Err(LimboError::InternalError(format!(
            "recursive CTE {} mixes recursive operators",
            cte.id
        )));
    }
    let comparison_collations = recursive
        .comparison_collations
        .iter()
        .map(|collation| collation.as_ref().map(|collation| *collation.value()))
        .collect::<Vec<_>>();
    let queue_order = recursive
        .queue_order
        .iter()
        .map(|term| RecursiveRuntimeOrder {
            result_column: term.output,
            order: term.order,
            nulls: term.nulls,
            explicit_collation: term
                .explicit_collation
                .as_ref()
                .map(|collation| *collation.value()),
        })
        .collect::<Vec<_>>();
    let spec = RecursiveRuntimeSpec {
        name: &cte.name,
        result_columns: cte.columns.len(),
        union_all,
        comparison_collations: &comparison_collations,
        queue_order: &queue_order,
    };
    emit_recursive_runtime(
        program,
        &spec,
        destination,
        |program, done| {
            initialize_limit(
                program,
                &plan.document,
                recursive.limit.as_ref(),
                done,
                destination_stops_after_first_row(destination),
            )
            .map(|limit| RecursiveRuntimeLimit {
                limit: limit.map(|limit| limit.limit),
                offset: limit.and_then(|limit| limit.offset),
            })
        },
        |program, input_start| {
            for source in &recursive.input_sources {
                program.bind_source(
                    *source,
                    SourceBinding::Registers {
                        start: input_start,
                        rowid: None,
                    },
                );
            }
            Ok(())
        },
        |program, phase, queue_destination| match phase {
            RecursivePhase::Seed => {
                emit_planned_query_body(program, plan, recursive.seed, queue_destination)
            }
            RecursivePhase::Step => {
                for arm in &recursive.arms {
                    emit_planned_query_body(program, plan, arm.query, queue_destination)?;
                }
                Ok(())
            }
        },
    )
}

fn validate_hir_materialized_query(
    plan: &HirPlan,
    source_id: SourceId,
    materialized_query: HirMaterializedQuery,
) -> Result<&hir::Source> {
    let document = &plan.document;
    let query_id = materialized_query.query();
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR materialized loop references missing source {source_id}"
        ))
    })?;
    let source_matches = match materialized_query {
        HirMaterializedQuery::Derived(query) => {
            matches!(source.kind, hir::SourceKind::Derived(source_query) if source_query == query)
        }
        HirMaterializedQuery::Cte { cte, .. } => {
            matches!(source.kind, hir::SourceKind::Cte(source_cte) if source_cte == cte)
        }
    };
    if !source_matches {
        return Err(LimboError::InternalError(format!(
            "HIR materialized loop source {source_id} does not match query {query_id}"
        )));
    }
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR materialized source {source_id} references missing query {query_id}"
        ))
    })?;
    if source.columns.len() != query.output.len() {
        return Err(LimboError::InternalError(format!(
            "HIR materialized source {source_id} width {} does not match query {query_id} width {}",
            source.columns.len(),
            query.output.len()
        )));
    }
    Ok(source)
}

fn prepare_hir_materialized_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    materialized_query: HirMaterializedQuery,
) -> Result<PreparedHirMaterializedLoop<'a>> {
    let query_id = materialized_query.query();
    let source = validate_hir_materialized_query(plan, source_id, materialized_query)?;
    // A subquery that reads outer columns runs inside the outer loop, so it
    // is filled again, into a fresh table, every time this point is reached.
    let correlated = plan.document.reads_outer_sources(query_id);

    let table = hir_materialized_table(source);
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    // An uncorrelated table is filled once for the whole statement, in a
    // subroutine. A correlated parent loop can reach this point more than
    // once, and the other readers of a shared CTE call the subroutine too,
    // so the table is filled even when this reader's loop never ran.
    let fill_subroutine = if correlated {
        None
    } else {
        let fill_subroutine = CteFillSubroutine {
            label: program.allocate_label(),
            return_reg: program.alloc_register(),
        };
        let fill_end = program.allocate_label();
        let after_subroutine = program.allocate_label();
        program.emit_insn(Insn::Gosub {
            target_pc: fill_subroutine.label,
            return_reg: fill_subroutine.return_reg,
        });
        program.emit_insn(Insn::Goto {
            target_pc: after_subroutine,
        });
        program.preassign_label_to_next_insn(fill_subroutine.label);
        program.emit_insn(Insn::Once {
            target_pc_when_reentered: fill_end,
        });
        Some((fill_subroutine, fill_end, after_subroutine))
    };
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: true,
    });
    emit_planned_query_body(
        program,
        plan,
        query_id,
        &QueryDestination::EphemeralTable {
            cursor_id: cursor,
            table: table.clone(),
            rowid_mode: EphemeralRowidMode::Auto,
        },
    )?;
    if let Some((fill_subroutine, fill_end, after_subroutine)) = fill_subroutine {
        program.preassign_label_to_next_insn(fill_end);
        program.emit_insn(Insn::Return {
            return_reg: fill_subroutine.return_reg,
            can_fallthrough: false,
        });
        program.preassign_label_to_next_insn(after_subroutine);
    }
    program.bind_source(
        source_id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor: None,
        },
    );
    Ok(PreparedHirMaterializedLoop {
        source,
        cursor,
        table,
        direction: IterationDirection::Forwards,
        fill_subroutine: fill_subroutine.map(|(fill_subroutine, _, _)| fill_subroutine),
    })
}

/// The loop of a stored FROM subquery or CTE: a scan of its table, or with
/// `seek`, a seek of a temporary index built from that table when the loop
/// first starts. The index holds every column, so the loop reads only the
/// index.
fn with_hir_materialized_seek<'a>(
    program: &mut ProgramBuilder,
    materialized: PreparedHirMaterializedLoop<'a>,
    seek: Option<&'a HirBtreeOperation>,
) -> Result<PreparedHirLoop<'a>> {
    let Some(operation) = seek else {
        return Ok(PreparedHirLoop::Materialized(materialized));
    };
    let HirBtreeOperation::Seek {
        index: Some(index), ..
    } = operation
    else {
        return Err(LimboError::InternalError(format!(
            "HIR subquery source {} has a seek without a temporary index",
            materialized.source.id
        )));
    };
    let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.bind_source(
        materialized.source.id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor: None,
        },
    );
    Ok(PreparedHirLoop::BTree(PreparedHirBtreeLoop {
        source: materialized.source,
        operation,
        index: Some(index),
        cursor,
        table_cursor: None,
        autoindex_source_cursor: Some(materialized.cursor),
        table_has_rowid: true,
        index_keys: Vec::new(),
    }))
}

fn hir_materialized_table(source: &hir::Source) -> Arc<BTreeTable> {
    let columns = source
        .columns
        .iter()
        .map(|column| {
            let storage = column.type_fact.storage.unwrap_or(Type::Null);
            let ty_str = match column.affinity {
                Affinity::Blob => "BLOB",
                Affinity::Text => "TEXT",
                Affinity::Numeric => "NUMERIC",
                Affinity::Integer => "INTEGER",
                Affinity::Real => "REAL",
                Affinity::None => "",
            };
            Column::new(
                Some(column.name.clone()),
                ty_str.to_string(),
                None,
                None,
                storage,
                column
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value()),
                ColDef {
                    flags: if column.hidden {
                        crate::schema::ColDefFlags::Hidden
                    } else {
                        crate::schema::ColDefFlags::empty()
                    },
                    ..ColDef::default()
                },
            )
        })
        .collect_vec();
    Arc::new(BTreeTable::new(
        0,
        source.name.clone(),
        crate::alloc::vec![],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ))
}

pub(super) fn prepare_hir_btree_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    planned_loop: &'a HirPlannedLoop,
) -> Result<PreparedHirBtreeLoop<'a>> {
    let document = &plan.document;
    let HirSourceAccess::BTree {
        operation,
        table_lookup,
    } = &planned_loop.access
    else {
        return Err(LimboError::InternalError(format!(
            "HIR source {} does not use a B-tree loop",
            planned_loop.source
        )));
    };
    let index = match operation {
        HirBtreeOperation::Scan { index, .. }
        | HirBtreeOperation::Seek { index, .. }
        | HirBtreeOperation::InSeek { index, .. } => index.as_ref(),
        HirBtreeOperation::RowidEq { .. } => None,
    };
    if index.is_some_and(|index| index.ephemeral)
        && !matches!(operation, HirBtreeOperation::Seek { .. })
    {
        return Err(LimboError::InternalError(
            "HIR automatic indexes require a seek operation".to_string(),
        ));
    }
    let source = document.source(planned_loop.source).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR B-tree loop references missing source {}",
            planned_loop.source
        ))
    })?;
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(format!(
            "HIR B-tree loop source {} is not a table",
            source.id
        )));
    };
    let Table::BTree(table) = table.value() else {
        return Err(LimboError::InternalError(format!(
            "HIR B-tree loop source {} is not a B-tree table",
            source.id
        )));
    };
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR B-tree loop source {} has no database",
            source.id
        ))
    })?;
    let database_snapshot = document.database(database).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR B-tree loop source {} references missing database {database:?}",
            source.id
        ))
    })?;
    program.begin_read_on_database(database.index(), database_snapshot.schema_version)?;
    // A materialized view's table cursor also sees the current
    // transaction's changes.
    let table_cursor_type = || match plan.materialized_view(source.id) {
        Some(view) => CursorType::MaterializedView(table.clone(), view.clone()),
        None => CursorType::BTreeTable(table.clone()),
    };
    let (cursor, table_cursor, autoindex_source_cursor) = if let Some(index) = index {
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        let table_cursor = match table_lookup {
            BtreeTableLookup::ScanOnly => None,
            BtreeTableLookup::TableRequired => Some(program.alloc_cursor_id(table_cursor_type())),
        };
        let autoindex_source_cursor =
            if index.ephemeral {
                Some(table_cursor.unwrap_or_else(|| {
                    program.alloc_cursor_id(CursorType::BTreeTable(table.clone()))
                }))
            } else {
                None
            };
        (index_cursor, table_cursor, autoindex_source_cursor)
    } else {
        if *table_lookup != BtreeTableLookup::ScanOnly {
            return Err(LimboError::InternalError(format!(
                "HIR table scan source {} unexpectedly requires a second table cursor",
                source.id
            )));
        }
        (program.alloc_cursor_id(table_cursor_type()), None, None)
    };
    program.bind_source(
        source.id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor,
        },
    );
    // A correlated subquery's loop is emitted inside the loops that run it,
    // so it opens its cursors on the first run only; each run positions
    // them again.
    let opened = program.is_nested().then(|| {
        let opened = program.allocate_label();
        program.emit_insn(Insn::Once {
            target_pc_when_reentered: opened,
        });
        opened
    });
    if index.is_none_or(|index| !index.ephemeral) {
        program.emit_insn(Insn::OpenRead {
            cursor_id: cursor,
            root_page: index.map_or(table.root_page, |index| index.root_page),
            db: database.index(),
        });
    }
    if let Some(source_cursor) = autoindex_source_cursor.or(table_cursor) {
        program.emit_insn(Insn::OpenRead {
            cursor_id: source_cursor,
            root_page: table.root_page,
            db: database.index(),
        });
    }
    if let Some(opened) = opened {
        program.preassign_label_to_next_insn(opened);
    }

    // An index supplies the expressions it keys, such as `a + b` for an
    // index on `t(a + b)`, as SQLite does. They are read once when the loop
    // positions the index, so a loop an outer join can NULL-extend computes
    // them from its NULL columns instead: `coalesce(v, -1)` is -1 on that
    // row, not the stored key.
    let index_keys = match index {
        Some(index) if source_is_never_null_extended(document, source.id) => {
            hir_index_key_expressions(plan, source, index)?
        }
        _ => Vec::new(),
    };
    Ok(PreparedHirBtreeLoop {
        source,
        operation,
        index,
        cursor,
        table_cursor,
        autoindex_source_cursor,
        table_has_rowid: table.has_rowid,
        index_keys,
    })
}

/// The block's subexpressions `index` stores, with the index column each is
/// read from. An automatic index stores no expressions.
fn hir_index_key_expressions<'a>(
    plan: &'a HirPlan,
    source: &hir::Source,
    index: &Index,
) -> Result<Vec<(&'a hir::Expr, usize)>> {
    if index.ephemeral {
        return Ok(Vec::new());
    }
    let expressions = source
        .index_expressions
        .iter()
        .find(|expressions| expressions.index.value().name == index.name);
    match expressions {
        Some(expressions) if expressions.columns.iter().any(Option::is_some) => {
            find_block_subexpressions(plan, source, |expression| {
                expressions
                    .columns
                    .iter()
                    .position(|key| key.as_ref().is_some_and(|key| key.equivalent(expression)))
            })
        }
        _ => Ok(Vec::new()),
    }
}

fn build_hir_autoindex(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirBtreeLoop<'_>,
    seek_def: &HirSeekDef,
) -> Result<bool> {
    let Some(index) = prepared.index.filter(|index| index.ephemeral) else {
        return Ok(false);
    };
    let table_cursor = prepared
        .autoindex_source_cursor
        .expect("automatic index has a source table cursor");
    let num_seek_keys = seek_def.size(&seek_def.start);
    let lowering = HirSeekExpressionLowering {
        document,
        source: prepared.source,
    };
    let has_null_matching_key = seek_def.prefix.iter().take(num_seek_keys).any(|component| {
        component
            .eq
            .as_ref()
            .is_some_and(|(operator, expression, _)| {
                lowering.is_null_matching(*operator, expression)
            })
    });
    let affinity_str = plan::synthesized_seek_affinity_str(index, seek_def);

    // Generated expressions must read the base-table row while the automatic
    // index is being populated. Restore the planned index binding before the seek.
    program.bind_source(
        prepared.source.id,
        SourceBinding::BTree {
            scan_cursor: table_cursor,
            table_cursor: None,
        },
    );
    let result = emit_autoindex(
        program,
        AutoIndexBuild {
            index,
            table_cursor_id: table_cursor,
            index_cursor_id: prepared.cursor,
            table_has_rowid: prepared.table_has_rowid,
            num_seek_keys,
            has_null_matching_key,
            seek_def,
            affinity_str: affinity_str.as_ref(),
        },
        |program, column, target| {
            let Some(expression) = prepared.source.generated_expressions.get(column) else {
                return Err(LimboError::InternalError(format!(
                    "automatic index column {column} is outside source {}",
                    prepared.source.id
                )));
            };
            match expression {
                hir::ColumnReadExpression::Absent => {
                    program.emit_column_or_rowid(table_cursor, column, target);
                    Ok(())
                }
                hir::ColumnReadExpression::Planned(expression) => {
                    super::expr::translate_expr_no_constant_opt(
                        program, document, expression, target,
                    )?;
                    // The index stores the value the column gives, which
                    // has the column's affinity, as a read of the column
                    // does.
                    if let Some(definition) = prepared.source.columns.get(column) {
                        if definition.has_affinity {
                            program.emit_column_affinity(target, definition.affinity);
                        }
                    }
                    Ok(())
                }
                hir::ColumnReadExpression::NotRequired => Err(LimboError::InternalError(format!(
                    "automatic index requires unplanned generated column {column} from source {}",
                    prepared.source.id
                ))),
            }
        },
    );
    program.bind_source(
        prepared.source.id,
        SourceBinding::BTree {
            scan_cursor: prepared.cursor,
            table_cursor: prepared.table_cursor,
        },
    );
    Ok(result?.use_bloom_filter)
}

pub(super) fn start_hir_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    match prepared {
        PreparedHirLoop::BTree(prepared) => {
            start_hir_btree_loop(program, document, prepared, left_join)
        }
        PreparedHirLoop::Coroutine(prepared) => {
            let loop_start = program.allocate_label();
            let next = program.allocate_label();
            let exhausted = program.allocate_label();
            // An inner loop runs the coroutine again for each outer row.
            program.emit_insn(Insn::InitCoroutine {
                yield_reg: prepared.yield_reg,
                jump_on_definition: BranchOffset::Offset(0),
                start_offset: prepared.start,
            });
            program.preassign_label_to_next_insn(loop_start);
            program.emit_insn(Insn::Yield {
                yield_reg: prepared.yield_reg,
                end_offset: exhausted,
                subtype_clear_start_reg: 0,
                subtype_clear_count: 0,
            });
            Ok(HirBtreeLoop {
                cursor: None,
                loop_start,
                next,
                exhausted,
                advance: HirLoopAdvance::Coroutine,
                left_join,
            })
        }
        PreparedHirLoop::Hash(_) => Err(LimboError::InternalError(
            "HIR hash lookup starts only inside its block's loops".into(),
        )),
        PreparedHirLoop::Virtual(prepared) => {
            start_hir_virtual_loop(program, document, prepared, left_join)
        }
        PreparedHirLoop::IndexMethod(prepared) => {
            start_hir_index_method_loop(program, document, prepared, left_join)
        }
        PreparedHirLoop::OrUnion(prepared) => {
            start_hir_or_union_loop(program, document, prepared, left_join)
        }
        PreparedHirLoop::Registers(prepared) => {
            if !matches!(
                program.source_binding(prepared.source.id),
                Some(SourceBinding::Registers { .. })
            ) {
                return Err(LimboError::InternalError(format!(
                    "HIR register source {} lost its row binding",
                    prepared.source.id
                )));
            }
            let loop_start = program.allocate_label();
            let next = program.allocate_label();
            let exhausted = program.allocate_label();
            program.preassign_label_to_next_insn(loop_start);
            Ok(HirBtreeLoop {
                cursor: None,
                loop_start,
                next,
                exhausted,
                advance: HirLoopAdvance::None,
                left_join,
            })
        }
        PreparedHirLoop::Materialized(prepared) => {
            if !matches!(
                program.source_binding(prepared.source.id),
                Some(SourceBinding::BTree {
                    scan_cursor,
                    table_cursor: None,
                }) if *scan_cursor == prepared.cursor
            ) {
                return Err(LimboError::InternalError(format!(
                    "HIR materialized source {} lost its cursor binding",
                    prepared.source.id
                )));
            }
            let loop_start = program.allocate_label();
            let next = program.allocate_label();
            let exhausted = program.allocate_label();
            match prepared.direction {
                IterationDirection::Forwards => program.emit_insn(Insn::Rewind {
                    cursor_id: prepared.cursor,
                    pc_if_empty: exhausted,
                }),
                IterationDirection::Backwards => program.emit_insn(Insn::Last {
                    cursor_id: prepared.cursor,
                    pc_if_empty: exhausted,
                }),
            }
            program.preassign_label_to_next_insn(loop_start);
            Ok(HirBtreeLoop {
                cursor: Some(prepared.cursor),
                loop_start,
                next,
                exhausted,
                advance: HirLoopAdvance::Cursor {
                    direction: prepared.direction,
                    fullscan: true,
                },
                left_join,
            })
        }
    }
}

fn start_hir_virtual_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirVirtualLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    if !matches!(
        program.source_binding(prepared.source.id),
        Some(SourceBinding::Virtual { cursor }) if *cursor == prepared.cursor
    ) {
        return Err(LimboError::InternalError(format!(
            "HIR virtual-table source {} lost its cursor binding",
            prepared.source.id
        )));
    }
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let exhausted = program.allocate_label();
    let args_reg = program.alloc_registers(prepared.operation.arguments.len());
    for (index, argument) in prepared.operation.arguments.iter().enumerate() {
        super::expr::translate_expr(program, document, argument, args_reg + index)?;
    }
    let idx_str = prepared.operation.idx_str.as_ref().map(|value| {
        let register = program.alloc_register();
        program.emit_insn(Insn::String8 {
            dest: register,
            value: value.clone(),
        });
        register
    });
    program.emit_insn(Insn::VFilter {
        cursor_id: prepared.cursor,
        pc_if_empty: exhausted,
        arg_count: prepared.operation.arguments.len(),
        args_reg,
        idx_str,
        idx_num: prepared.operation.idx_num as usize,
    });
    program.preassign_label_to_next_insn(loop_start);
    Ok(HirBtreeLoop {
        cursor: Some(prepared.cursor),
        loop_start,
        next,
        exhausted,
        advance: HirLoopAdvance::Virtual,
        left_join,
    })
}

pub(super) fn start_hir_btree_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirBtreeLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let exhausted = program.allocate_label();
    let advance = match prepared.operation {
        HirBtreeOperation::Scan { iter_dir, .. } => {
            match iter_dir {
                IterationDirection::Forwards => program.emit_insn(Insn::Rewind {
                    cursor_id: prepared.cursor,
                    pc_if_empty: exhausted,
                }),
                IterationDirection::Backwards => program.emit_insn(Insn::Last {
                    cursor_id: prepared.cursor,
                    pc_if_empty: exhausted,
                }),
            }
            program.preassign_label_to_next_insn(loop_start);
            HirLoopAdvance::Cursor {
                direction: *iter_dir,
                fullscan: true,
            }
        }
        HirBtreeOperation::RowidEq { cmp_expr } => {
            let key = program.alloc_register();
            super::expr::translate_expr(program, document, cmp_expr, key)?;
            program.emit_insn(Insn::SeekRowid {
                cursor_id: prepared.cursor,
                src_reg: key,
                target_pc: exhausted,
            });
            HirLoopAdvance::None
        }
        HirBtreeOperation::Seek { seek_def, .. } => {
            let use_bloom_filter = build_hir_autoindex(program, document, prepared, seek_def)?;
            let key_registers = seek_def
                .size(&seek_def.start)
                .max(seek_def.size(&seek_def.end));
            let start_register = program.alloc_registers(key_registers);
            SeekEmitter::with_lowering(
                program,
                seek_def,
                HirSeekExpressionLowering {
                    document,
                    source: prepared.source,
                },
                prepared.cursor,
                start_register,
                exhausted,
                prepared.index,
            )
            .emit(loop_start, use_bloom_filter)?;
            HirLoopAdvance::Cursor {
                direction: seek_def.iter_dir,
                fullscan: false,
            }
        }
        HirBtreeOperation::InSeek { source, .. } => {
            let source_cursor =
                open_hir_in_seek_source_cursor(program, document, prepared.index, source)?;
            let state = emit_in_seek_start(
                program,
                source_cursor,
                prepared.cursor,
                prepared.table_cursor,
                prepared.index.is_some(),
                loop_start,
                exhausted,
            );
            HirLoopAdvance::InSeek {
                state,
                index_backed: prepared.index.is_some(),
            }
        }
    };
    if !matches!(prepared.operation, HirBtreeOperation::InSeek { .. }) {
        if let Some(table_cursor) = prepared.table_cursor {
            program.emit_insn(Insn::DeferredSeek {
                index_cursor_id: prepared.cursor,
                table_cursor_id: table_cursor,
            });
        }
    }
    let mut index_key_registers: Vec<(usize, usize)> = Vec::new();
    for (expression, column) in &prepared.index_keys {
        let register = match index_key_registers
            .iter()
            .find(|(read_column, _)| read_column == column)
        {
            Some((_, register)) => *register,
            None => {
                let register = program.alloc_register();
                program.emit_column_or_rowid(prepared.cursor, *column, register);
                index_key_registers.push((*column, register));
                register
            }
        };
        program.bind_expression_register(expression, register);
    }
    Ok(HirBtreeLoop {
        cursor: Some(prepared.cursor),
        loop_start,
        next,
        exhausted,
        advance,
        left_join,
    })
}

fn emit_ready_predicates(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block_plan: HirLoopsPlan<'_>,
    loop_index: Option<usize>,
    fail: BranchOffset,
    kind: HirPredicateKind,
    left_joins: &[HirLeftJoin],
) -> Result<()> {
    for (index, predicate) in block_plan.predicates.iter().enumerate() {
        let selected = match kind {
            HirPredicateKind::Join(source) => predicate.from_outer_join == Some(source),
            HirPredicateKind::Where { .. } => predicate.from_outer_join.is_none(),
        };
        if !selected {
            continue;
        }
        let references_ready_at = predicate_ready_loop(
            program,
            document,
            block_plan.loops,
            block_plan.deferred_predicates,
            block_plan.predicates,
            index,
        )?;
        if predicate.consumed {
            let rechecked = matches!(
                kind,
                HirPredicateKind::Where {
                    unmatched_pass: Some(right),
                    ..
                } if index >= block_plan.where_start
                    && references_ready_at.is_some_and(|ready| ready <= right)
            );
            if !rechecked {
                continue;
            }
        }
        let evaluate_at = match kind {
            HirPredicateKind::Join(source) => {
                let owner = left_joins
                    .iter()
                    .find(|left_join| left_join.owner == source)
                    .map(|left_join| left_join.last_loop)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR outer-join predicate owner {source} has no LEFT JOIN boundary"
                        ))
                    })?;
                if references_ready_at.is_some_and(|ready| ready > owner) {
                    return Err(LimboError::InternalError(format!(
                        "HIR outer-join predicate owned by {source} references a later loop"
                    )));
                }
                Some(owner)
            }
            HirPredicateKind::Where {
                right_join_loop,
                constant_on_terms_in_loops,
                ..
            } => match references_ready_at {
                None if constant_on_terms_in_loops && index < block_plan.where_start => Some(0),
                None => None,
                Some(ready) => {
                    let ready = match right_join_loop {
                        Some(right) if index >= block_plan.where_start => ready.max(right),
                        _ => ready,
                    };
                    Some(left_joins.iter().fold(ready, |ready, left_join| {
                        if left_join.where_from <= ready && ready <= left_join.last_loop {
                            left_join.last_loop
                        } else {
                            ready
                        }
                    }))
                }
            },
        };
        if evaluate_at != loop_index {
            continue;
        }
        emit_hir_predicate(program, document, predicate, fail)?;
    }
    Ok(())
}

/// The loop after which the predicate at `index` can run: the last loop
/// supplying a source it reads, or the later loop the planner deferred it
/// to when that loop is among `loops`.
pub(super) fn predicate_ready_loop(
    program: &ProgramBuilder,
    document: &HirDocument,
    loops: &[HirPlannedLoop],
    deferred_predicates: &[(SourceId, usize)],
    predicates: &[crate::translate::plan::HirWhereTerm],
    index: usize,
) -> Result<Option<usize>> {
    let ready = predicate_loop(program, document, &predicates[index].expr, loops)?;
    let deferred = deferred_predicates
        .iter()
        .find(|(_, predicate)| *predicate == index)
        .and_then(|(source, _)| loops.iter().position(|hir_loop| hir_loop.supplies(*source)));
    Ok(match (ready, deferred) {
        (Some(ready), Some(deferred)) => Some(ready.max(deferred)),
        (ready, _) => ready,
    })
}

pub(super) fn predicate_loop(
    program: &ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    loops: &[HirPlannedLoop],
) -> Result<Option<usize>> {
    let mut eval_at = None;
    let mut missing = None;
    // Output references follow the aliased expression, and subqueries
    // contribute their outer captures.
    document.visit_expr_sources(expression, &mut |source| {
        if let Some(position) = loops.iter().position(|hir_loop| hir_loop.supplies(source)) {
            eval_at = Some(eval_at.map_or(position, |current: usize| current.max(position)));
        } else if program.source_binding(source).is_none() {
            missing = Some(source);
        }
    });
    if let Some(source) = missing {
        return Err(LimboError::InternalError(format!(
            "HIR predicate references source {source} outside its loop plan"
        )));
    }
    Ok(eval_at)
}

pub(super) fn close_hir_btree_loop(
    program: &mut ProgramBuilder,
    hir_loop: &HirBtreeLoop,
    left_joins: &[HirLeftJoin],
) {
    program.preassign_label_to_next_insn(hir_loop.next);
    match &hir_loop.advance {
        HirLoopAdvance::None => {}
        HirLoopAdvance::Coroutine | HirLoopAdvance::Restart => program.emit_insn(Insn::Goto {
            target_pc: hir_loop.loop_start,
        }),
        HirLoopAdvance::Cursor {
            direction,
            fullscan,
        } => {
            let cursor = hir_loop
                .cursor
                .expect("cursor advance has a physical cursor");
            match direction {
                IterationDirection::Forwards => program.emit_insn(Insn::Next {
                    cursor_id: cursor,
                    is_index: program.cursor_is_index(cursor),
                    pc_if_next: hir_loop.loop_start,
                    fullscan: *fullscan,
                }),
                IterationDirection::Backwards => program.emit_insn(Insn::Prev {
                    cursor_id: cursor,
                    is_index: program.cursor_is_index(cursor),
                    pc_if_prev: hir_loop.loop_start,
                    fullscan: *fullscan,
                }),
            }
        }
        HirLoopAdvance::InSeek {
            state,
            index_backed,
        } => emit_in_seek_advance(
            program,
            state,
            hir_loop
                .cursor
                .expect("IN seek advance has a physical cursor"),
            *index_backed,
            hir_loop.loop_start,
        ),
        HirLoopAdvance::Virtual => program.emit_insn(Insn::VNext {
            cursor_id: hir_loop
                .cursor
                .expect("virtual advance has a physical cursor"),
            pc_if_next: hir_loop.loop_start,
        }),
        HirLoopAdvance::Hash {
            hash_table,
            rowid,
            payload,
            columns,
            waiting_pass,
            waiting_next,
            ..
        } => {
            // The waiting pass shares this body but finds its next match
            // and its next waiting lookup on its own.
            program.emit_insn(Insn::IfPos {
                reg: *waiting_pass,
                target_pc: *waiting_next,
                decrement_by: 0,
            });
            // After its NULL row, a LEFT JOIN lookup has no match to advance.
            if hir_loop.left_join.is_some() {
                program.emit_insn(Insn::IsNull {
                    reg: *rowid,
                    target_pc: hir_loop.exhausted,
                });
            }
            program.emit_insn(Insn::HashNext {
                hash_table_id: *hash_table,
                dest_reg: *rowid,
                target_pc: hir_loop.exhausted,
                payload_dest_reg: Some(*payload),
                num_payload: *columns,
            });
            program.emit_insn(Insn::Goto {
                target_pc: hir_loop.loop_start,
            });
        }
    }
    program.preassign_label_to_next_insn(hir_loop.exhausted);
    if let Some(left_join) = hir_loop.left_join.map(|index| &left_joins[index]) {
        let finished = left_join.metadata.begin_unmatched_row(program);
        if let HirLoopAdvance::Hash {
            rowid,
            payload,
            columns,
            ..
        } = &hir_loop.advance
        {
            emit_hir_hash_null_row(program, *payload, *columns, *rowid);
        }
        for cursor in &left_join.null_cursors {
            program.emit_insn(Insn::NullRow { cursor_id: *cursor });
        }
        for registers in left_join
            .null_registers
            .iter()
            .filter(|registers| registers.width > 0)
        {
            program.emit_insn(Insn::Null {
                dest: registers.private,
                dest_end: (registers.width > 1).then_some(registers.private + registers.width - 1),
            });
        }
        left_join.metadata.finish_unmatched_row(program, finished);
    }
    if let HirLoopAdvance::Hash { deferred, .. } = &hir_loop.advance {
        program.preassign_label_to_next_insn(*deferred);
    }
}

fn initialize_hir_distinct(
    program: &mut ProgramBuilder,
    block: &hir::QueryBlock,
) -> Result<Option<HirDistinctOutput>> {
    let hir::QueryBlockBody::Select { distinctness, .. } = &block.body else {
        return Ok(None);
    };
    if !matches!(distinctness, Some(Distinctness::Distinct)) {
        return Ok(None);
    }
    if block.outputs.is_empty() {
        return Err(LimboError::InternalError(format!(
            "DISTINCT HIR query block {:?} has no outputs",
            block.id
        )));
    }
    let hash_table = program.alloc_hash_table_id();
    program.emit_insn(Insn::HashClear {
        hash_table_id: hash_table,
    });
    emit_explain!(program, false, EqpDetail::Distinct);
    Ok(Some(HirDistinctOutput {
        hash_table,
        collations: block
            .outputs
            .iter()
            .map(|output| {
                output
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
                    .unwrap_or(CollationSeq::Binary)
            })
            .collect(),
    }))
}

fn initialize_hir_sorter<'a>(
    program: &mut ProgramBuilder,
    query: &'a hir::Query,
    block: &hir::QueryBlock,
) -> Result<HirSorterOutput<'a>> {
    if query.order_by.is_empty() || block.outputs.is_empty() {
        return Err(LimboError::InternalError(format!(
            "ordered HIR query block {:?} has no ordering or outputs",
            block.id
        )));
    }

    let mut key_sources = Vec::with_capacity(query.order_by.len());
    let mut order_collations_nulls = crate::alloc::vec![];
    let mut comparators = crate::alloc::vec![];
    for term in &query.order_by {
        let output = hir_order_output_position(block, &term.expr);
        let custom = term
            .type_fact
            .declared
            .as_ref()
            .and_then(|declared| declared.custom())
            .filter(|custom| custom.value().decode().is_some());
        if let Some(custom) = custom {
            if !custom
                .value()
                .operators()
                .iter()
                .any(|operator| operator.op == "<")
            {
                if let Some(output) = output {
                    crate::bail_parse_error!(
                        "cannot ORDER BY column '{}' of type '{}': type does not declare OPERATOR '<'",
                        block.outputs[output].name,
                        custom.value().name
                    );
                }
                crate::bail_parse_error!(
                    "cannot ORDER BY a custom type column that does not declare OPERATOR '<'"
                );
            }
        }
        key_sources.push(match (output, custom.is_some()) {
            (Some(output), false) => HirSortKeySource::Output(output),
            (Some(output), true) => HirSortKeySource::EncodedOutput(output),
            (None, _) => HirSortKeySource::Expression,
        });
        order_collations_nulls.push((
            term.order,
            term.collation.as_ref().map(|collation| *collation.value()),
            term.nulls,
        ));
        comparators.push(custom_type_comparator_from_type_fact(&term.type_fact));
    }

    let mut next_output_column = query.order_by.len();
    let output_columns = (0..block.outputs.len())
        .map(|output| {
            if let Some(key) = key_sources.iter().position(
                |source| matches!(source, HirSortKeySource::Output(found) if *found == output),
            ) {
                key
            } else {
                let column = next_output_column;
                next_output_column += 1;
                column
            }
        })
        .collect::<Vec<_>>();
    let column_count = next_output_column;
    let cursor = program.alloc_cursor_id(CursorType::Sorter);
    program.emit_insn(Insn::SorterOpen {
        data: Box::new(SorterOpenData {
            cursor_id: cursor,
            columns: query.order_by.len(),
            order_collations_nulls,
            comparators,
        }),
    });
    Ok(HirSorterOutput {
        cursor,
        record: program.alloc_register(),
        row: program.alloc_registers(column_count),
        column_count,
        order_by: &query.order_by,
        key_sources,
        output_columns,
    })
}

fn hir_order_output_position(block: &hir::QueryBlock, expression: &hir::Expr) -> Option<usize> {
    let mut expression = expression;
    while let hir::Expr::Collate { expr, .. } = expression {
        expression = expr;
    }
    if let hir::Expr::Output(output) = expression {
        return block
            .outputs
            .iter()
            .position(|candidate| candidate.id == *output);
    }
    block
        .outputs
        .iter()
        .position(|output| expression.equivalent(&output.expr))
}

/// Insert one row into the ORDER BY sorter. `outputs_start` holds the
/// row's outputs side by side; without it they are already in their sorter
/// columns.
fn emit_hir_sorter_insert(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    outputs: &[hir::Output],
    outputs_start: Option<usize>,
    sorter: &HirSorterOutput<'_>,
    inputs: &[super::expr::ExprRegisterInput<'_>],
) -> Result<()> {
    let output_register = |output: usize| match outputs_start {
        Some(start) => start + output,
        None => sorter.row + sorter.output_columns[output],
    };
    for (key, (term, source)) in sorter.order_by.iter().zip(&sorter.key_sources).enumerate() {
        let target = sorter.row + key;
        match source {
            HirSortKeySource::Output(output) => {
                if output_register(*output) != target {
                    program.emit_insn(Insn::Copy {
                        src_reg: output_register(*output),
                        dst_reg: target,
                        extra_amount: 0,
                    });
                }
            }
            HirSortKeySource::EncodedOutput(output) => emit_hir_sort_key(
                program,
                document,
                &outputs[*output].expr,
                &term.type_fact,
                target,
                inputs,
            )?,
            HirSortKeySource::Expression => emit_hir_sort_key(
                program,
                document,
                &term.expr,
                &term.type_fact,
                target,
                inputs,
            )?,
        }
    }
    for (output, column) in sorter.output_columns.iter().enumerate() {
        if *column < sorter.order_by.len() || outputs_start.is_none() {
            continue;
        }
        program.emit_insn(Insn::Copy {
            src_reg: output_register(output),
            dst_reg: sorter.row + column,
            extra_amount: 0,
        });
    }
    sorter_insert(
        program,
        sorter.row,
        sorter.column_count,
        sorter.cursor,
        sorter.record,
    );
    Ok(())
}

fn emit_hir_sort_key(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    type_fact: &hir::TypeFact,
    target: usize,
    inputs: &[super::expr::ExprRegisterInput<'_>],
) -> Result<()> {
    let suppress_decode = type_fact
        .declared
        .as_ref()
        .and_then(|declared| declared.custom())
        .is_some_and(|custom| custom.value().decode().is_some());
    if suppress_decode {
        program.flags.set_suppress_custom_type_decode(true);
    }
    let result = super::expr::translate_expr_with_inputs_no_constant_opt(
        program, document, expression, target, inputs,
    );
    if suppress_decode {
        program.flags.set_suppress_custom_type_decode(false);
    }
    result.map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn emit_hir_sorted_rows(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block: &hir::QueryBlock,
    sorter: &HirSorterOutput<'_>,
    destination: &QueryDestination,
    limit: Option<QueryLimitRegisters>,
    done: BranchOffset,
) -> Result<()> {
    emit_hir_sorter_rows(
        program,
        document,
        &block.outputs,
        sorter.cursor,
        sorter.record,
        sorter.column_count,
        &sorter.output_columns,
        destination,
        limit,
        done,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_hir_sorter_rows(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    output_definitions: &[hir::Output],
    sorter_cursor: CursorID,
    sorter_record: usize,
    sorter_column_count: usize,
    output_columns: &[usize],
    destination: &QueryDestination,
    limit: Option<QueryLimitRegisters>,
    done: BranchOffset,
) -> Result<()> {
    if output_definitions.len() != output_columns.len() {
        return Err(LimboError::InternalError(format!(
            "HIR sorter has {} outputs but {} output columns",
            output_definitions.len(),
            output_columns.len()
        )));
    }
    emit_explain!(
        program,
        false,
        EqpDetail::OrderBy {
            method: EqpSortMethod::Sorter,
        }
    );
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let pseudo_cursor = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: sorter_column_count,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: sorter_record,
        num_fields: sorter_column_count,
    });
    program.emit_insn(Insn::SorterSort {
        cursor_id: sorter_cursor,
        pc_if_empty: done,
    });
    program.preassign_label_to_next_insn(loop_start);
    emit_before_row(program, limit, next);
    program.emit_insn(Insn::SorterData {
        cursor_id: sorter_cursor,
        dest_reg: sorter_record,
        pseudo_cursor,
    });
    let outputs = program.alloc_registers(output_definitions.len());
    for (output, column) in output_columns.iter().enumerate() {
        program.emit_column_or_rowid(pseudo_cursor, *column, outputs + output);
    }
    emit_array_results(program, document, output_definitions, outputs, &[])?;
    emit_hir_row_to_destination(program, destination, outputs, output_definitions.len())?;
    emit_after_row(
        program,
        limit,
        destination_stops_after_first_row(destination),
        done,
    );
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter_cursor,
        pc_if_next: loop_start,
    });
    Ok(())
}

/// `first_row_only` is set for a scalar or row-value subquery, which stops
/// after its first row: its LIMIT only decides whether that row is produced,
/// so a non-integer LIMIT becomes `limit != 0` instead of a type error.
fn initialize_limit(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    limit: Option<&hir::Limit>,
    done: BranchOffset,
    first_row_only: bool,
) -> Result<Option<QueryLimitRegisters>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let limit_register = program.alloc_register();
    if first_row_only {
        emit_first_row_limit_value(program, document, &limit.limit, limit_register)?;
    } else {
        emit_limit_value(program, document, &limit.limit, limit_register)?;
    }

    let offset = if let Some(offset) = &limit.offset {
        let offset_register = program.alloc_register();
        emit_offset_value(program, document, offset, offset_register)?;
        program.emit_insn(Insn::MustBeInt {
            reg: offset_register,
            target_pc: None,
        });
        let combined_register = program.alloc_register();
        program.emit_insn(Insn::OffsetLimit {
            limit_reg: limit_register,
            offset_reg: offset_register,
            combined_reg: combined_register,
        });
        Some(offset_register)
    } else {
        None
    };
    program.emit_insn(Insn::IfNot {
        reg: limit_register,
        target_pc: done,
        jump_if_null: false,
    });
    Ok(Some(QueryLimitRegisters {
        limit: limit_register,
        offset,
    }))
}

fn emit_limit_value(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    target: usize,
) -> Result<()> {
    match expression {
        hir::Expr::Literal(Literal::Numeric(value)) => match parse_numeric_literal(value)? {
            Value::Numeric(Numeric::Integer(value)) => {
                program.emit_insn(Insn::Integer {
                    value,
                    dest: target,
                });
            }
            Value::Numeric(Numeric::Float(value)) => {
                program.emit_insn(Insn::Real {
                    value: value.into(),
                    dest: target,
                });
                program.emit_insn(Insn::MustBeInt {
                    reg: target,
                    target_pc: None,
                });
            }
            _ => unreachable!("numeric parser returns a numeric value"),
        },
        _ => {
            super::expr::translate_expr_no_constant_opt(program, document, expression, target)?;
            program.emit_insn(Insn::MustBeInt {
                reg: target,
                target_pc: None,
            });
        }
    }
    Ok(())
}

/// Lower the LIMIT of a subquery that stops after its first row as
/// `limit != 0`, comparing with NUMERIC affinity so that `'0'` and `0.0`
/// mean no rows while `0.5` means one row. An integer literal is folded.
fn emit_first_row_limit_value(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    target: usize,
) -> Result<()> {
    if let hir::Expr::Literal(Literal::Numeric(value)) = expression {
        if let Value::Numeric(Numeric::Integer(value)) = parse_numeric_literal(value)? {
            program.emit_insn(Insn::Integer {
                value: i64::from(value != 0),
                dest: target,
            });
            return Ok(());
        }
    }
    super::expr::translate_expr_no_constant_opt(program, document, expression, target)?;
    let zero = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: zero,
    });
    let not_zero = program.allocate_label();
    let end = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: target,
        rhs: zero,
        target_pc: not_zero,
        flags: crate::vdbe::insn::CmpInsFlags::default().with_affinity(Affinity::Numeric),
        collation: None,
    });
    // Not jumping means the limit is zero or NULL; NULL stays NULL.
    program.emit_insn(Insn::IsNull {
        reg: target,
        target_pc: end,
    });
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: target,
    });
    program.emit_insn(Insn::Goto { target_pc: end });
    program.preassign_label_to_next_insn(not_zero);
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: target,
    });
    program.preassign_label_to_next_insn(end);
    Ok(())
}

fn emit_offset_value(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    target: usize,
) -> Result<()> {
    match expression {
        hir::Expr::Literal(Literal::Numeric(value)) => match parse_numeric_literal(value)? {
            Value::Numeric(Numeric::Integer(value)) => {
                program.emit_insn(Insn::Integer {
                    value,
                    dest: target,
                });
            }
            Value::Numeric(Numeric::Float(value)) => {
                program.emit_insn(Insn::Real {
                    value: value.into(),
                    dest: target,
                });
                program.emit_insn(Insn::MustBeInt {
                    reg: target,
                    target_pc: None,
                });
            }
            _ => unreachable!("numeric parser returns a numeric value"),
        },
        _ => {
            super::expr::translate_expr_no_constant_opt(program, document, expression, target)?;
        }
    }
    Ok(())
}

fn emit_before_row(
    program: &mut ProgramBuilder,
    limit: Option<QueryLimitRegisters>,
    skip_row: BranchOffset,
) {
    let Some(offset) = limit.and_then(|limit| limit.offset) else {
        return;
    };
    program.emit_insn(Insn::IfPos {
        reg: offset,
        target_pc: skip_row,
        decrement_by: 1,
    });
}

fn emit_after_row(
    program: &mut ProgramBuilder,
    limit: Option<QueryLimitRegisters>,
    stop_after_first: bool,
    done: BranchOffset,
) {
    if let Some(limit) = limit {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit.limit,
            target_pc: done,
        });
    }
    if stop_after_first {
        program.emit_insn(Insn::Goto { target_pc: done });
    }
}

fn query_output_registers(
    program: &mut ProgramBuilder,
    outputs: &[hir::Output],
    needs_values: bool,
) -> Result<usize> {
    if outputs.is_empty() {
        return Err(LimboError::InternalError(
            "HIR query has no outputs".to_string(),
        ));
    }
    Ok(if needs_values {
        program.alloc_registers(outputs.len())
    } else {
        0
    })
}

fn destination_stops_after_first_row(destination: &QueryDestination) -> bool {
    matches!(
        destination,
        QueryDestination::ExistsSubqueryResult { .. }
            | QueryDestination::RowValueSubqueryResult { .. }
    )
}

fn emit_query_row<'expr>(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    output: &HirRowOutput<'_>,
    outputs: &[hir::Output],
    expressions: impl ExactSizeIterator<Item = &'expr hir::Expr>,
    start: usize,
    skip_row: BranchOffset,
) -> Result<()> {
    emit_query_row_with_inputs(
        program,
        document,
        output,
        outputs,
        expressions,
        start,
        skip_row,
        &HirRowInputs::default(),
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_query_row_with_inputs<'expr>(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    output: &HirRowOutput<'_>,
    outputs: &[hir::Output],
    expressions: impl ExactSizeIterator<Item = &'expr hir::Expr>,
    start: usize,
    skip_row: BranchOffset,
    inputs: &HirRowInputs<'_>,
) -> Result<()> {
    if let HirRowTarget::Window(window) = output.target {
        if output.distinct.is_some() {
            return Err(LimboError::InternalError(
                "HIR window input rows cannot apply SELECT DISTINCT".into(),
            ));
        }
        return window.emit_source_row(program, &HirRowInputs::default());
    }
    if expressions.len() != outputs.len() {
        return Err(LimboError::InternalError(format!(
            "HIR row width {} does not match output width {}",
            expressions.len(),
            outputs.len()
        )));
    }
    // Without DISTINCT, which hashes the outputs side by side, a sorted row
    // computes each output straight into its sorter column.
    let sorter_in_place = match output.target {
        HirRowTarget::Sorter(sorter) if output.distinct.is_none() => Some(sorter),
        _ => None,
    };
    let stored_outputs: &[usize] = match &output.target {
        HirRowTarget::Direct {
            destination: QueryDestination::CoroutineYield { stored_outputs, .. },
            ..
        } => stored_outputs,
        _ => &[],
    };
    // A row written to an ephemeral index or table, yielded from a
    // coroutine, or queued for a recursive CTE keeps its constants in
    // place: another arm of a compound SELECT or a later run writes the
    // same registers. So does a row built from buffered values, which a
    // sorter or window output subroutine computes per buffered row.
    let constants_in_place = !inputs.values.is_empty()
        || matches!(
            output.target,
            HirRowTarget::Direct {
                destination: QueryDestination::EphemeralIndex { .. }
                    | QueryDestination::CoroutineYield { .. }
                    | QueryDestination::EphemeralTable { .. }
                    | QueryDestination::RecursiveCteQueue { .. },
                ..
            }
        );
    if !output.needs_values() {
        for expression in expressions {
            super::expr::register_parameters(program, expression);
        }
    } else {
        for (position, expression) in expressions.enumerate() {
            let register = match sorter_in_place {
                Some(sorter) => sorter.row + sorter.output_columns[position],
                None => start + position,
            };
            let translate = |program: &mut ProgramBuilder| {
                if constants_in_place {
                    super::expr::translate_expr_with_inputs_no_constant_opt(
                        program,
                        document,
                        expression,
                        register,
                        inputs.values,
                    )
                } else {
                    super::expr::translate_expr_with_inputs(
                        program,
                        document,
                        expression,
                        register,
                        inputs.values,
                    )
                }
            };
            if stored_outputs.contains(&position) {
                super::dml::with_stored_values(program, translate)?;
            } else {
                translate(program)?;
            }
            program.bind_output(outputs[position].id, register);
        }
    }

    if output.needs_values() && matches!(&output.target, HirRowTarget::Direct { .. }) {
        emit_array_results(program, document, outputs, start, stored_outputs)?;
    }

    if let Some(distinct) = output.distinct {
        program.emit_insn(Insn::HashDistinct {
            data: Box::new(HashDistinctData {
                hash_table_id: distinct.hash_table,
                key_start_reg: start,
                num_keys: outputs.len(),
                collations: distinct.collations.clone(),
                target_pc: skip_row,
            }),
        });
    }

    match output.target {
        HirRowTarget::Direct {
            destination,
            limit,
            done,
        } => {
            emit_before_row(program, limit, skip_row);
            emit_hir_row_to_destination(program, destination, start, outputs.len())?;
            emit_after_row(
                program,
                limit,
                destination_stops_after_first_row(destination),
                done,
            );
        }
        HirRowTarget::Sorter(sorter) => {
            let outputs_start = sorter_in_place.is_none().then_some(start);
            emit_hir_sorter_insert(
                program,
                document,
                outputs,
                outputs_start,
                sorter,
                inputs.sort_keys,
            )?;
        }
        HirRowTarget::Window(_) => {
            unreachable!("window input rows return before output evaluation")
        }
    }
    Ok(())
}

/// Sorter that orders a block's rows by window keys before they are buffered.
struct HirWindowInputSorter {
    cursor: CursorID,
    record: usize,
    /// First register of one sorter record: the keys, the optional sequence
    /// number, then the buffer columns that are not keys.
    row: usize,
    column_count: usize,
    sequence: Option<usize>,
    /// Sorter record position holding each window-buffer column.
    buffer_positions: Vec<usize>,
}

/// Where a window layer sends each finished row.
#[derive(Clone, Copy)]
enum HirWindowConsumer<'p, 'a> {
    /// The SELECT outputs and ORDER BY of the block.
    Select,
    /// The next outer window layer, which reads these buffer columns and keys.
    Layer {
        plan: &'p super::window::HirWindowBufferPlan<'a>,
        keys: &'p [super::window::HirWindowInputKey<'a>],
    },
}

/// Runtime state for one window over a query block's rows.
///
/// A block with several windows lowers one layer per window, nested: the
/// first window is the outermost layer.
/// The block's rows feed the innermost layer, and every other layer is fed by
/// the finished rows of the layer inside it. Each layer sorts its input by
/// PARTITION BY and ORDER BY, or streams it when the window has neither, then
/// runs the shared window frame code, which calls the layer's output
/// subroutine for every finished row.
struct HirWindowLayer<'a> {
    document: &'a HirDocument,
    plan: super::window::HirWindowBufferPlan<'a>,
    keys: Vec<super::window::HirWindowInputKey<'a>>,
    sorter: Option<HirWindowInputSorter>,
    functions: Vec<WindowFunctionRuntime<&'a hir::Expr>>,
    accumulators: usize,
    buffer: WindowBufferRuntime,
    input: WindowBufferInput,
    state: WindowInputState,
    peers: WindowPeerState,
    offsets: WindowFrameOffsets,
    tracking: WindowFrameTracking,
    labels: WindowLabels,
    /// Buffer columns the consumer reads, loaded into `outputs` per row.
    output_columns: Vec<usize>,
    outputs: HirWindowOutputRegisters<'a>,
    output_return: usize,
}

impl<'a> HirWindowLayer<'a> {
    #[allow(clippy::too_many_arguments)]
    fn prepare(
        program: &mut ProgramBuilder,
        document: &'a HirDocument,
        query: &'a hir::Query,
        block: &'a hir::QueryBlock,
        window: hir::WindowId,
        consumer: HirWindowConsumer<'_, 'a>,
        grouped_rows: bool,
        sorted_input: bool,
    ) -> Result<Self> {
        let mut plan = super::window::plan_hir_window_buffer(query, block, window)?;
        let output_plan = match consumer {
            HirWindowConsumer::Select => plan_hir_window_output_inputs(query, block, &mut plan)?,
            HirWindowConsumer::Layer { plan: outer, keys } => {
                plan_hir_window_layer_inputs(outer, keys, &mut plan)?
            }
        };
        let keys = plan.input_sort_keys();
        for key in &keys {
            check_hir_window_key_is_orderable(document, key)?;
        }
        let tracking = super::window::prepare_hir_window_frame_tracking(program, &plan);
        let buffer = super::window::open_hir_window_buffer(program, &plan);
        let accumulators = program.alloc_registers(plan.functions.len());
        let functions = super::window::prepare_hir_window_runtime(program, &plan)?;
        let outputs = output_plan.prepare_registers(program);
        let state = super::window::prepare_hir_window_input_state(program, &plan);
        let peers = super::window::prepare_hir_window_peer_state(program, &plan);
        let offsets = super::window::prepare_hir_window_frame_offsets(program, &plan);
        let labels = WindowLabels {
            flush_buffer: program.allocate_label(),
            row_output: program.allocate_label(),
            window_processing_end: program.allocate_label(),
        };
        let output_return = program.alloc_register();
        let input = WindowBufferInput {
            start: program.alloc_registers(plan.columns.len()),
            count: plan.columns.len(),
        };
        // Grouped rows carry a sequence number so equal keys keep their
        // group order.
        // Rows that already arrive in key order go straight to the window.
        let sorter = (!keys.is_empty() && !sorted_input)
            .then(|| open_hir_window_input_sorter(program, &plan, &keys, grouped_rows));
        Ok(Self {
            document,
            plan,
            keys,
            sorter,
            functions,
            accumulators,
            buffer,
            input,
            state,
            peers,
            offsets,
            tracking,
            labels,
            output_columns: output_plan.columns,
            outputs,
            output_return,
        })
    }

    /// Hand the current input row to the window: queue it in the sorter, or
    /// step the window directly when the window has no keys. Rows from an
    /// inner layer supply their values through `inputs`.
    fn emit_source_row(
        &self,
        program: &mut ProgramBuilder,
        inputs: &HirRowInputs<'_>,
    ) -> Result<()> {
        let column_inputs = |column: usize| {
            if self.plan.column_is_encoded(column) {
                inputs.sort_keys
            } else {
                inputs.values
            }
        };
        let Some(sorter) = &self.sorter else {
            for column in 0..self.input.count {
                super::window::emit_hir_window_input_column(
                    program,
                    self.document,
                    &self.plan,
                    column,
                    self.input.start + column,
                    column_inputs(column),
                )?;
            }
            return self.emit_step(program);
        };
        for (position, key) in self.keys.iter().enumerate() {
            if key.is_encoded() {
                key.emit(
                    program,
                    self.document,
                    sorter.row + position,
                    inputs.sort_keys,
                )?;
            }
        }
        if let Some(sequence) = sorter.sequence {
            program.emit_insn(Insn::Sequence {
                cursor_id: sorter.cursor,
                target_reg: sorter.row + sequence,
            });
        }
        for (column, position) in sorter.buffer_positions.iter().enumerate() {
            super::window::emit_hir_window_input_column(
                program,
                self.document,
                &self.plan,
                column,
                sorter.row + position,
                column_inputs(column),
            )?;
        }
        sorter_insert(
            program,
            sorter.row,
            sorter.column_count,
            sorter.cursor,
            sorter.record,
        );
        Ok(())
    }

    /// Read the sorted rows back into the window input registers, stepping the
    /// window for each one.
    fn emit_sorted_rows(&self, program: &mut ProgramBuilder) -> Result<()> {
        let Some(sorter) = &self.sorter else {
            return Ok(());
        };
        let pseudo_cursor = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
            column_count: sorter.column_count,
        }));
        program.emit_insn(Insn::OpenPseudo {
            cursor_id: pseudo_cursor,
            content_reg: sorter.record,
            num_fields: sorter.column_count,
        });
        let sorted_done = program.allocate_label();
        let loop_start = program.allocate_label();
        program.emit_insn(Insn::SorterSort {
            cursor_id: sorter.cursor,
            pc_if_empty: sorted_done,
        });
        program.preassign_label_to_next_insn(loop_start);
        program.emit_insn(Insn::SorterData {
            cursor_id: sorter.cursor,
            dest_reg: sorter.record,
            pseudo_cursor,
        });
        for (column, position) in sorter.buffer_positions.iter().enumerate() {
            program.emit_column_or_rowid(pseudo_cursor, *position, self.input.start + column);
        }
        self.emit_step(program)?;
        program.emit_insn(Insn::SorterNext {
            cursor_id: sorter.cursor,
            pc_if_next: loop_start,
        });
        program.preassign_label_to_next_insn(sorted_done);
        Ok(())
    }

    fn emit_step(&self, program: &mut ProgramBuilder) -> Result<()> {
        super::window::emit_hir_window_buffer_step(
            program,
            self.document,
            &self.plan,
            &self.functions,
            self.accumulators,
            &self.buffer,
            self.input,
            self.state,
            self.peers,
            self.offsets,
            self.tracking,
            self.labels,
            HIR_CUSTOM_TYPES_CHECKED,
            |program| self.emit_row_results(program),
        )
    }

    fn emit_row_results(&self, program: &mut ProgramBuilder) -> Result<()> {
        super::window::emit_hir_window_row_results(
            program,
            self.document,
            &self.plan,
            &self.functions,
            self.buffer.cursors,
            self.tracking,
            self.accumulators,
            &self.output_columns,
            self.outputs.start,
            self.labels.row_output,
            self.output_return,
            HIR_CUSTOM_TYPES_CHECKED,
        )
    }

    /// Finish the last partition and emit the output subroutine that every
    /// finished row calls: the SELECT output, or the next outer layer's input.
    fn emit_flush(
        &self,
        program: &mut ProgramBuilder,
        block: &hir::QueryBlock,
        output: &HirRowOutput<'_>,
        outer: Option<&HirWindowLayer<'a>>,
    ) -> Result<()> {
        super::window::emit_hir_window_partition_flush(
            program,
            self.document,
            &self.plan,
            &self.functions,
            self.accumulators,
            &self.buffer,
            self.state,
            self.peers,
            self.offsets,
            self.tracking,
            self.labels,
            HIR_CUSTOM_TYPES_CHECKED,
            |program| self.emit_row_results(program),
            |program| match outer {
                None => emit_hir_window_output_subroutine(
                    program,
                    self.document,
                    block,
                    output,
                    &self.outputs.row_inputs(),
                    self.labels.row_output,
                    self.output_return,
                ),
                Some(outer) => {
                    program.preassign_label_to_next_insn(self.labels.row_output);
                    outer.emit_source_row(program, &self.outputs.row_inputs())?;
                    program.emit_insn(Insn::Return {
                        return_reg: self.output_return,
                        can_fallthrough: false,
                    });
                    Ok(())
                }
            },
        )
    }
}

/// Window lowering always passes this to the shared aggregate step code.
/// Semantic analysis owns the custom-type feature check.
const HIR_CUSTOM_TYPES_CHECKED: bool = true;

/// A window key is sorted like an ORDER BY term, which rejects a
/// custom-type column that decodes values but does not declare `<`.
fn check_hir_window_key_is_orderable(
    document: &HirDocument,
    key: &super::window::HirWindowInputKey<'_>,
) -> Result<()> {
    let hir::Expr::Column(column) = key.expression else {
        return Ok(());
    };
    let Some(custom) = key
        .type_fact
        .declared
        .as_ref()
        .and_then(|declared| declared.custom())
        .filter(|custom| custom.value().decode().is_some())
    else {
        return Ok(());
    };
    if custom
        .value()
        .operators()
        .iter()
        .any(|operator| operator.op == "<")
    {
        return Ok(());
    }
    let name = document
        .source(column.source)
        .and_then(|source| source.columns.get(column.column))
        .map(|column| column.name.as_str())
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR window key references missing column {column:?}"
            ))
        })?;
    crate::bail_parse_error!(
        "cannot ORDER BY column '{}' of type '{}': type does not declare OPERATOR '<'",
        name,
        custom.value().name
    )
}

fn open_hir_window_input_sorter(
    program: &mut ProgramBuilder,
    plan: &super::window::HirWindowBufferPlan<'_>,
    keys: &[super::window::HirWindowInputKey<'_>],
    sequence: bool,
) -> HirWindowInputSorter {
    let mut next = keys.len() + usize::from(sequence);
    let buffer_positions = (0..plan.columns.len())
        .map(|column| {
            keys.iter()
                .position(|key| !key.is_encoded() && key.slot == column)
                .unwrap_or_else(|| {
                    let position = next;
                    next += 1;
                    position
                })
        })
        .collect();
    let column_count = next;
    let mut order_collations_nulls = keys
        .iter()
        .map(|key| (key.order, key.collation, key.nulls))
        .collect_vec();
    let mut comparators = keys
        .iter()
        .map(|key| custom_type_comparator_from_type_fact(key.type_fact))
        .collect_vec();
    if sequence {
        order_collations_nulls.push((SortOrder::Asc, Some(CollationSeq::default()), None));
        comparators.push(None);
    }
    let cursor = program.alloc_cursor_id(CursorType::Sorter);
    program.emit_insn(Insn::SorterOpen {
        data: Box::new(SorterOpenData {
            cursor_id: cursor,
            columns: order_collations_nulls.len(),
            order_collations_nulls,
            comparators,
        }),
    });
    HirWindowInputSorter {
        cursor,
        record: program.alloc_register(),
        row: program.alloc_registers(column_count),
        column_count,
        sequence: sequence.then_some(keys.len()),
        buffer_positions,
    }
}

/// Emit one query block whose SELECT outputs read window results.
fn emit_hir_window_block(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    output: &HirRowOutput<'_>,
) -> Result<()> {
    // `layers[0]` is the outermost layer; the block's rows feed the last one.
    let mut layers: Vec<HirWindowLayer<'_>> = Vec::with_capacity(block.windows.len());
    for (position, window) in block.windows.iter().enumerate() {
        let consumer = match layers.last() {
            None => HirWindowConsumer::Select,
            Some(outer) => HirWindowConsumer::Layer {
                plan: &outer.plan,
                keys: &outer.keys,
            },
        };
        let innermost = position + 1 == block.windows.len();
        let layer = HirWindowLayer::prepare(
            program,
            &plan.document,
            query,
            block,
            window.id,
            consumer,
            innermost && hir_grouping(block).is_some(),
            innermost && block_plan.window_sort_elided,
        )?;
        layers.push(layer);
    }
    let innermost = layers.last().ok_or_else(|| {
        LimboError::InternalError(format!("HIR query block {:?} has no windows", block.id))
    })?;
    // Each window that sorts its input shows the sort, innermost window
    // first.
    for layer in layers.iter().rev() {
        if layer.sorter.is_some() {
            emit_explain!(
                program,
                false,
                EqpDetail::OrderBy {
                    method: EqpSortMethod::Sorter,
                }
            );
        }
    }
    let rows_done = program.allocate_label();
    emit_planned_block_rows(
        program,
        plan,
        query,
        block,
        block_plan,
        &HirRowOutput {
            target: HirRowTarget::Window(innermost),
            distinct: None,
        },
        rows_done,
    )?;
    program.preassign_label_to_next_insn(rows_done);
    for (position, layer) in layers.iter().enumerate().rev() {
        layer.emit_sorted_rows(program)?;
        let outer = position.checked_sub(1).map(|outer| &layers[outer]);
        layer.emit_flush(program, block, output, outer)?;
    }
    Ok(())
}

/// Emit once; every window RETURN_ROW calls this after preparing buffered inputs
/// and binding computed window results. DISTINCT and OFFSET skips return to the
/// window loop, while a satisfied direct LIMIT exits through the output target.
#[cfg_attr(not(test), allow(dead_code))]
#[allow(clippy::too_many_arguments)]
fn emit_hir_window_output_subroutine(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block: &hir::QueryBlock,
    output: &HirRowOutput<'_>,
    inputs: &HirRowInputs<'_>,
    output_label: BranchOffset,
    output_return: usize,
) -> Result<()> {
    program.preassign_label_to_next_insn(output_label);
    let skip_row = program.allocate_label();
    let start = query_output_registers(program, &block.outputs, output.needs_values())?;
    emit_query_row_with_inputs(
        program,
        document,
        output,
        &block.outputs,
        block.outputs.iter().map(|output| &output.expr),
        start,
        skip_row,
        inputs,
    )?;
    program.preassign_label_to_next_insn(skip_row);
    program.emit_insn(Insn::Return {
        return_reg: output_return,
        can_fallthrough: false,
    });
    Ok(())
}

/// Decode each array output for display, except the outputs in
/// `stored_outputs`, which keep the array as the table stores it.
pub(super) fn emit_array_results(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    outputs: &[hir::Output],
    start: usize,
    stored_outputs: &[usize],
) -> Result<()> {
    for (position, output) in outputs.iter().enumerate() {
        if stored_outputs.contains(&position) {
            continue;
        }
        if !output.type_fact.is_array() && !hir_expr_passes_array(document, &output.expr) {
            continue;
        }
        let register = start + position;
        let skip = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: register,
            target_pc: skip,
        });
        // An array column of a custom type has its elements decoded for
        // display.
        if let Some(decode) = array_column_decode(document, &output.expr) {
            super::expr::translate_schema_calls_on_elements(program, document, decode, register)?;
        }
        program.emit_insn(Insn::ArrayDecode { reg: register });
        program.preassign_label_to_next_insn(skip);
    }
    Ok(())
}

/// The element decode programs of an array column of a custom type that
/// `expression` reads directly.
fn array_column_decode<'a>(
    document: &'a HirDocument,
    expression: &hir::Expr,
) -> Option<&'a [hir::BoundSchemaCall]> {
    let hir::Expr::Column(reference) = expression else {
        return None;
    };
    let source = document.source(reference.source)?;
    if !source.columns.get(reference.column)?.type_fact.is_array() {
        return None;
    }
    source
        .column_type_programs
        .get(reference.column)?
        .as_ref()
        .map(|programs| programs.decode.as_slice())
        .filter(|decode| !decode.is_empty())
}

/// An expression that can pass an array value through is decoded for
/// display too: COALESCE, IFNULL, scalar MIN and MAX, IIF,
/// NULLIF, CASE and `||` over array operands, and array functions.
fn hir_expr_passes_array(document: &HirDocument, expression: &hir::Expr) -> bool {
    let is_array = |expression| hir_expr_passes_array(document, expression);
    match expression {
        hir::Expr::Array(_) => true,
        hir::Expr::Column(reference) => document
            .source(reference.source)
            .and_then(|source| source.columns.get(reference.column))
            .is_some_and(|column| column.type_fact.is_array()),
        hir::Expr::Output(output) => document
            .output(*output)
            .is_some_and(|output| output.type_fact.is_array() || is_array(&output.expr)),
        hir::Expr::Function(call) => {
            let arguments = call.arguments.expressions();
            match call.function.value() {
                Func::Scalar(function) if function.returns_array_blob() => true,
                Func::Agg(AggFunc::ArrayAgg) => true,
                Func::Scalar(
                    ScalarFunc::Coalesce | ScalarFunc::IfNull | ScalarFunc::Min | ScalarFunc::Max,
                ) => arguments.iter().any(is_array),
                Func::Scalar(ScalarFunc::Iif) => arguments.iter().skip(1).any(is_array),
                Func::Scalar(ScalarFunc::Nullif) => arguments.first().is_some_and(is_array),
                _ => false,
            }
        }
        hir::Expr::Binary {
            operator: turso_parser::ast::Operator::Concat,
            lhs,
            rhs,
            ..
        } => is_array(lhs) || is_array(rhs),
        hir::Expr::Case {
            when_then,
            else_expr,
            ..
        } => {
            when_then.iter().any(|(_, then)| is_array(then))
                || else_expr.as_deref().is_some_and(is_array)
        }
        _ => false,
    }
}

/// Send one row to `destination`. The reader of a coroutine reads its row
/// from fixed registers, so the row is copied there first.
fn emit_hir_row_to_destination(
    program: &mut ProgramBuilder,
    destination: &QueryDestination,
    start: usize,
    count: usize,
) -> Result<()> {
    if let QueryDestination::CoroutineYield { yield_reg, .. } = destination {
        if let Some(row) = program.coroutine_row(*yield_reg) {
            if row != start && count > 0 {
                program.emit_insn(Insn::Copy {
                    src_reg: start,
                    dst_reg: row,
                    extra_amount: count - 1,
                });
            }
            return emit_columns_to_destination(program, destination, row, count);
        }
    }
    emit_columns_to_destination(program, destination, start, count)
}

fn row_value_destination(program: &mut ProgramBuilder, width: usize) -> DestinationBinding {
    let start = program.alloc_registers(width);
    DestinationBinding {
        destination: QueryDestination::RowValueSubqueryResult {
            result_reg_start: start,
            num_regs: width,
        },
        binding: SubqueryBinding::RowValue {
            start,
            count: width,
        },
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::{
        dialect::SqliteDialect,
        schema::{BTreeTable, Schema},
        translate::{
            optimizer::CostModelParams,
            semantic::{
                catalog::{SemanticCatalog, SemanticCatalogDatabase},
                context::DoubleQuotedDml,
                hir::{self, Expr, HirRoot},
                SemanticOptions, SemanticRootInput,
            },
            semantic_to_plan::HirPlan,
        },
        vdbe::builder::{ProgramBuilderOpts, QueryMode},
        SymbolTable, MAIN_DB_ID,
    };
    use turso_parser::parser::Parser;

    pub(in crate::translate::semantic_lowering) fn program() -> ProgramBuilder {
        program_with_mode(QueryMode::Normal)
    }

    fn program_with_mode(mode: QueryMode) -> ProgramBuilder {
        ProgramBuilder::new(mode, None, ProgramBuilderOpts::new(0, 4, 0))
    }

    fn analyze_sql_with_schema(mut schema: Schema, sql: &str) -> (HirDocument, Arc<Schema>) {
        schema
            .resolve_all_custom_type_affinities()
            .expect("custom affinities resolve");
        let schema = Arc::new(schema);
        let catalog = SemanticCatalog {
            databases: vec![SemanticCatalogDatabase {
                id: hir::DatabaseId::new(MAIN_DB_ID),
                name: "main".to_string(),
                schema: schema.clone(),
            }],
            unqualified_database_search_path: vec![hir::DatabaseId::new(MAIN_DB_ID)],
            has_temp_database: false,
            search_path_configured: false,
        };
        let command = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains a statement");
        let turso_parser::ast::Cmd::Stmt(statement) = command else {
            panic!("SQL contains a statement command");
        };
        let document = crate::translate::semantic::analyze_root_with_options(
            &catalog,
            &SymbolTable::new(),
            SemanticOptions {
                dialect: Arc::new(SqliteDialect),
                custom_types_enabled: true,
                dqs_dml: DoubleQuotedDml::Enabled,
                foreign_keys_enabled: true,
                capture_changes_table: None,
                long_column_names: false,
            },
            SemanticRootInput::Statement(&statement),
        )
        .inspect(|document| {
            document.validate().expect("HIR validates");
        })
        .expect("SQL analyzes");
        (document, schema)
    }

    fn analyze_sql(schema: Schema, sql: &str) -> HirDocument {
        analyze_sql_with_schema(schema, sql).0
    }

    pub(in crate::translate::semantic_lowering) fn analyze_plan(
        schema: Schema,
        sql: &str,
    ) -> HirPlan {
        let (document, schema) = analyze_sql_with_schema(schema, sql);
        HirPlan::build(Arc::new(document), &schema, &CostModelParams::default())
            .expect("HIR query plans")
    }

    pub(in crate::translate::semantic_lowering) fn indexed_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value INTEGER, extra TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        schema
            .add_index(Arc::new(Index {
                constraint_name: None,
                name: "items_value".to_string(),
                table_name: "items".to_string(),
                root_page: 3,
                columns: crate::alloc::vec![IndexColumn {
                    name: "value".to_string(),
                    order: SortOrder::Asc,
                    nulls_order: None,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                    expr: None,
                }],
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }))
            .expect("fixed index name is unique");
        schema
    }

    fn rowid_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        schema
    }

    fn ordered_custom_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql(
                "CREATE TYPE amount(value INTEGER, factor INTEGER) BASE INTEGER \
                 ENCODE value * factor DECODE value / factor OPERATOR '<' numeric_lt",
            )
            .expect("custom ordered type parses");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value amount(4), extra TEXT) STRICT", 2)
                    .expect("custom ordered table parses"),
            ))
            .expect("custom ordered table name is unique");
        schema
    }

    fn joined_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE right_items(value INTEGER, extra TEXT)", 3)
                    .expect("right table schema parses"),
            ))
            .expect("right table name is unique");
        schema
            .add_index(Arc::new(Index {
                constraint_name: None,
                name: "right_items_value".to_string(),
                table_name: "right_items".to_string(),
                root_page: 4,
                columns: crate::alloc::vec![IndexColumn {
                    name: "value".to_string(),
                    order: SortOrder::Asc,
                    nulls_order: None,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                    expr: None,
                }],
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }))
            .expect("right index name is unique");
        schema
    }

    fn automatic_join_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE right_items(value INTEGER, extra TEXT)", 3)
                    .expect("right table schema parses"),
            ))
            .expect("right table name is unique");
        schema.analyze_stats.table_stats_mut("left_items").row_count = Some(1_000);
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(1);
        schema
    }

    fn automatic_generated_join_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE right_items(\
                         value INTEGER, \
                         generated INTEGER AS (value + 1) VIRTUAL, \
                         extra TEXT\
                     )",
                    3,
                )
                .expect("generated table schema parses"),
            ))
            .expect("right table name is unique");
        schema.analyze_stats.table_stats_mut("left_items").row_count = Some(1_000);
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(1);
        schema
    }

    fn left_join_with_tail_schema() -> Schema {
        let mut schema = automatic_join_schema();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE tail_items(value INTEGER, extra TEXT)", 4)
                    .expect("tail table schema parses"),
            ))
            .expect("tail table name is unique");
        schema
    }

    fn root_expression(document: &HirDocument) -> &Expr {
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        &document.queries[root.query.index()].blocks[0].outputs[0].expr
    }

    fn root_query(document: &HirDocument) -> QueryId {
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        root.query
    }

    fn root_subquery(document: &HirDocument) -> &SubqueryExpr {
        match root_expression(document) {
            Expr::Subquery(subquery) => subquery,
            Expr::Binary { rhs, .. } => {
                let Expr::Subquery(subquery) = rhs.as_ref() else {
                    panic!("binary RHS is a subquery");
                };
                subquery
            }
            _ => panic!("root expression contains the expected subquery"),
        }
    }

    fn root_filter_subquery(document: &HirDocument) -> &SubqueryExpr {
        let query = document
            .query(root_query(document))
            .expect("root query exists");
        let hir::QueryBlockBody::Select {
            filter: Some(Expr::Subquery(subquery)),
            ..
        } = &query.blocks[0].body
        else {
            panic!("root filter is a subquery expression");
        };
        subquery
    }

    #[test]
    fn planned_ungrouped_aggregates_use_hir_ids_and_shared_steps() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT sum(value), count(*) FROM items WHERE value > 0",
        );
        let query_id = root_query(&plan.document);
        let query = plan.document.query(query_id).expect("root query exists");
        let block = &query.blocks[0];
        assert_eq!(block.aggregate_count, 2);

        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query_id, &QueryDestination::ResultRows)
            .expect("ungrouped aggregate HIR query emits");
        program.resolve_labels().expect("aggregate labels resolve");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::AggStep { .. }))
                .count(),
            2
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::AggFinal { .. }))
                .count(),
            2
        );
        for index in 0..block.aggregate_count {
            assert!(program
                .aggregate_result_register(hir::AggregateId::new(block.id, index))
                .is_some());
        }

        let last_step = program
            .insns
            .iter()
            .rposition(|(insn, _)| matches!(insn, Insn::AggStep { .. }))
            .expect("aggregate step emits");
        let scan_advance = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Next { .. }))
            .expect("table scan advances");
        let first_final = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::AggFinal { .. }))
            .expect("aggregate final emits");
        assert!(last_step < scan_advance && scan_advance < first_final);
    }

    #[test]
    fn planned_filtered_distinct_aggregate_uses_hir_argument_collation() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT count(DISTINCT value COLLATE nocase) FILTER (WHERE id > 0) FROM items",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("filtered DISTINCT aggregate HIR query emits");
        program
            .resolve_labels()
            .expect("filtered DISTINCT labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::HashClear { .. })));
        let filter = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: true,
                        ..
                    }
                )
            })
            .expect("aggregate FILTER emits");
        let distinct = program
            .insns
            .iter()
            .position(|(insn, _)| match insn {
                Insn::HashDistinct { data } => data.collations == [CollationSeq::NoCase],
                _ => false,
            })
            .expect("DISTINCT uses resolved argument collation");
        let step = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::AggStep { .. }))
            .expect("aggregate step emits");
        assert!(filter < distinct && distinct < step);
    }

    #[test]
    fn planned_percentile_checks_fraction_once_before_the_scan() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY value) FROM items",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("percentile HIR query emits");
        program.resolve_labels().expect("percentile labels resolve");

        let (check, fraction) = program
            .insns
            .iter()
            .enumerate()
            .find_map(|(index, (insn, _))| match insn {
                Insn::IsNull { reg, .. } => Some((index, *reg)),
                _ => None,
            })
            .expect("percentile fraction range check emits");
        let scan = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Rewind { .. }))
            .expect("table scan emits");
        let (step, delimiter) = program
            .insns
            .iter()
            .enumerate()
            .find_map(|(index, (insn, _))| match insn {
                Insn::AggStep { data }
                    if matches!(&data.func, AccumulatorFunc::Agg(AggFunc::PercentileCont)) =>
                {
                    Some((index, data.delimiter))
                }
                _ => None,
            })
            .expect("percentile step emits");
        assert_eq!(delimiter, fraction);
        assert!(check < scan && scan < step);
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Halt { description, .. }
                if description == "percentile value is not between 0 and 1"
        )));
    }

    #[test]
    fn planned_percentile_rejects_fraction_that_depends_on_aggregated_rows() {
        for sql in [
            "SELECT percentile_cont(value) WITHIN GROUP (ORDER BY extra) FROM items",
            "SELECT percentile_disc((SELECT 0.5)) WITHIN GROUP (ORDER BY value) FROM items",
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let error = emit_planned_query_body(
                &mut program(),
                &plan,
                query,
                &QueryDestination::ResultRows,
            )
            .expect_err("row-dependent percentile fraction fails");
            assert!(error.to_string().contains(
                "must be a constant expression that does not depend on the aggregated rows"
            ));
        }
    }

    #[test]
    fn planned_ungrouped_aggregate_captures_bare_columns_from_first_row() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT rowid, extra, sum(value) FROM items",
        );
        let query = root_query(&plan.document);
        let source = plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0]
            .source;
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("bare-column aggregate HIR query emits");

        let Some(SourceBinding::Registers { start, rowid }) = program.source_binding(source) else {
            panic!("aggregate source is rebound to captured registers");
        };
        assert!(rowid.is_some());
        assert!(program.insns.iter().any(|(insn, _)| {
            matches!(
                insn,
                Insn::Column {
                    column: 1,
                    dest,
                    ..
                } if *dest == start + 1
            )
        }));
    }

    #[test]
    fn planned_grouped_aggregate_sorts_then_uses_shared_aggregate_steps() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT extra COLLATE nocase, sum(value) FROM items \
             GROUP BY extra COLLATE nocase HAVING extra <> ''",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("grouped aggregate HIR query emits");
        program.resolve_labels().expect("GROUP BY labels resolve");

        let sorter_open = program
            .insns
            .iter()
            .position(|(insn, _)| match insn {
                Insn::SorterOpen { data } => {
                    data.order_collations_nulls.first()
                        == Some(&(SortOrder::Asc, Some(CollationSeq::NoCase), None))
                }
                _ => false,
            })
            .expect("GROUP BY sorter uses resolved key collation");
        let sorter_insert = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterInsert { .. }))
            .expect("scan rows enter GROUP BY sorter");
        let sorter_sort = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterSort { .. }))
            .expect("GROUP BY sorter drains");
        let aggregate_step = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::AggStep { .. }))
            .expect("group uses shared aggregate step");
        let aggregate_final = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::AggFinal { .. }))
            .expect("group uses shared aggregate final");
        assert!(sorter_open < sorter_insert && sorter_insert < sorter_sort);
        assert!(sorter_sort < aggregate_step && aggregate_step < aggregate_final);
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Compare { .. })));
    }

    #[test]
    fn planned_grouped_aggregate_uses_index_order_without_a_sorter() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value, count(*) FROM items GROUP BY value",
        );
        let query = root_query(&plan.document);
        assert!(
            plan.planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .group_sort_elided
        );

        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("index-ordered GROUP BY HIR query emits");
        program
            .resolve_labels()
            .expect("index-ordered GROUP BY labels resolve");

        assert!(!program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::SorterOpen { .. } | Insn::SorterInsert { .. } | Insn::SorterSort { .. }
        )));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Compare { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::AggStep { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::ResultRow { .. })));
    }

    #[test]
    fn planned_grouped_aggregate_keeps_sorter_when_index_collation_differs() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value COLLATE nocase, count(*) FROM items GROUP BY value COLLATE nocase",
        );
        let query = root_query(&plan.document);
        assert!(
            !plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .group_sort_elided
        );

        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("collation-mismatched GROUP BY HIR query emits");
        assert!(program.insns.iter().any(|(insn, _)| match insn {
            Insn::SorterOpen { data } => {
                data.order_collations_nulls.first()
                    == Some(&(SortOrder::Asc, Some(CollationSeq::NoCase), None))
            }
            _ => false,
        }));
    }

    #[test]
    fn planned_group_by_without_aggregates_still_emits_one_row_per_group() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT extra FROM items GROUP BY extra",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("aggregate-free GROUP BY HIR query emits");
        program
            .resolve_labels()
            .expect("aggregate-free GROUP BY labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SorterInsert { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Compare { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::ResultRow { .. })));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::AggStep { .. } | Insn::AggFinal { .. })));
    }

    #[test]
    fn planned_order_by_follows_an_index_without_a_sorter() {
        let emits_sorter = |sql| {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let mut program = program();
            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("ORDER BY HIR query emits");
            program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::SorterOpen { .. }))
        };
        assert!(!emits_sorter("SELECT value FROM items ORDER BY value"));
        assert!(!emits_sorter("SELECT value FROM items ORDER BY value DESC"));
        // The index has no `extra`.
        assert!(emits_sorter("SELECT value FROM items ORDER BY extra"));
        // Reading the index backwards gives the groups in ORDER BY order,
        // so neither GROUP BY nor ORDER BY sorts.
        assert!(!emits_sorter(
            "SELECT value, count(*) FROM items GROUP BY value ORDER BY value DESC"
        ));
        // An aggregate ORDER BY still sorts the groups.
        assert!(emits_sorter(
            "SELECT value, count(*) FROM items GROUP BY value ORDER BY count(*)"
        ));
    }

    #[test]
    fn planned_grouped_distinct_aggregate_clears_each_group_state() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT extra, count(DISTINCT value) FROM items GROUP BY extra",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("grouped DISTINCT aggregate HIR query emits");
        program
            .resolve_labels()
            .expect("grouped DISTINCT aggregate labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::HashDistinct { .. })));
        assert!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::HashClear { .. }))
                .count()
                >= 2
        );
    }

    #[test]
    fn function_argument_facts_are_resolved_and_validated() {
        let mut document = analyze_sql(
            rowid_items_schema(),
            "SELECT min(value COLLATE nocase) FROM items",
        );
        let Expr::Function(call) = root_expression(&document) else {
            panic!("root expression is aggregate call");
        };
        let hir::FunctionArguments::Expressions { values, facts, .. } = &call.arguments else {
            panic!("aggregate has expression arguments");
        };
        assert_eq!(values.len(), 1);
        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].collation.as_ref().map(|value| *value.value()),
            Some(CollationSeq::NoCase)
        );

        let Expr::Function(call) = &mut document.queries[0].blocks[0].outputs[0].expr else {
            panic!("root expression is aggregate call");
        };
        let hir::FunctionArguments::Expressions { facts, .. } = &mut call.arguments else {
            panic!("aggregate has expression arguments");
        };
        facts.clear();
        assert_eq!(
            document
                .validate()
                .expect_err("argument/fact width mismatch is invalid")
                .message(),
            "function argument facts do not match argument count"
        );
    }

    #[test]
    fn planned_window_inputs_feed_select_and_order_by_from_the_buffer() {
        use super::super::window::{open_hir_window_buffer, plan_hir_window_buffer};
        let document = analyze_sql(
            rowid_items_schema(),
            "SELECT DISTINCT sum(value) OVER () + value, value + 1 FROM items ORDER BY id + value LIMIT 2 OFFSET 1",
        );
        let query = document.query(root_query(&document)).unwrap();
        let block = &query.blocks[0];
        let mut buffer = plan_hir_window_buffer(query, block, block.windows[0].id).unwrap();
        let inputs = plan_hir_window_output_inputs(query, block, &mut buffer).unwrap();
        assert_eq!(
            buffer.columns.len(),
            2,
            "SELECT reuses the window argument; ORDER BY adds id"
        );
        assert_eq!(inputs.columns.len(), 2);
        assert_eq!(inputs.values.len(), 2);
        assert_eq!(inputs.values[0].1, inputs.values[1].1);
        assert!(
            !std::ptr::eq(inputs.values[0].0, inputs.values[1].0),
            "each occurrence keeps its own substitution"
        );
        let mut program = program();
        let runtime = open_hir_window_buffer(&mut program, &buffer);
        let registers = inputs.prepare_registers(&mut program);
        let result = program.alloc_register();
        program.bind_window_result(hir::WindowFunctionId::new(block.id, 0), result);
        let sorter = initialize_hir_sorter(&mut program, query, block).unwrap();
        let distinct = initialize_hir_distinct(&mut program, block).unwrap();
        let done = program.allocate_label();
        let output_label = program.allocate_label();
        let output_return = program.alloc_register();
        let limit =
            initialize_limit(&mut program, &document, query.limit.as_ref(), done, false).unwrap();
        let read_start = program.insns.len();
        crate::translate::window::emit_window_row_results(
            &mut program,
            runtime.cursors.csr_current,
            inputs.columns.iter().copied(),
            registers.start,
            false,
            output_label,
            output_return,
            |_, _| Ok(()),
        )
        .unwrap();
        program.emit_insn(Insn::Goto { target_pc: done });
        emit_hir_window_output_subroutine(
            &mut program,
            &document,
            block,
            &HirRowOutput {
                target: HirRowTarget::Sorter(&sorter),
                distinct: distinct.as_ref(),
            },
            &registers.row_inputs(),
            output_label,
            output_return,
        )
        .expect("no source bindings needed after buffering");
        let output_end = program.insns.len();
        emit_hir_sorted_rows(
            &mut program,
            &document,
            block,
            &sorter,
            &QueryDestination::ResultRows,
            limit,
            done,
        )
        .unwrap();
        program.preassign_label_to_next_insn(done);
        program.resolve_labels().unwrap();
        assert!(program.insns[read_start..output_end]
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SorterInsert { .. })));
        for (insn, _) in &program.insns[read_start..output_end] {
            if let Insn::Column { cursor_id, .. } | Insn::ColumnRange { cursor_id, .. } = insn {
                assert_eq!(*cursor_id, runtime.cursors.csr_current);
            }
        }
    }

    #[test]
    fn window_input_planning_stops_at_aggregates_subqueries_and_other_windows() {
        use super::super::window::plan_hir_window_buffer;
        for sql in [
            "SELECT row_number() OVER (), sum(value), (SELECT value FROM items LIMIT 1) FROM items",
            "SELECT row_number() OVER (), sum(value) OVER (ORDER BY id), random() FROM items",
        ] {
            let document = analyze_sql(rowid_items_schema(), sql);
            let query = document.query(root_query(&document)).unwrap();
            let block = &query.blocks[0];
            let mut buffer = plan_hir_window_buffer(query, block, block.windows[0].id).unwrap();
            let inputs = plan_hir_window_output_inputs(query, block, &mut buffer).unwrap();
            let expected = if block.windows.len() == 1 { 2 } else { 1 };
            assert_eq!(inputs.values.len(), expected);
            assert_eq!(buffer.columns.len(), expected);
            assert!(std::ptr::eq(inputs.values[0].0, &block.outputs[1].expr));
            assert!(inputs
                .values
                .iter()
                .all(|(expr, _)| !matches!(expr, hir::Expr::Column(_))));
        }
        let document = analyze_sql(
            rowid_items_schema(),
            "SELECT row_number() OVER (), random() FROM items",
        );
        let query = document.query(root_query(&document)).unwrap();
        let block = &query.blocks[0];
        let mut buffer = plan_hir_window_buffer(query, block, block.windows[0].id).unwrap();
        let inputs = plan_hir_window_output_inputs(query, block, &mut buffer).unwrap();
        assert!(
            inputs.columns.is_empty(),
            "current window and volatile scalar stay at output"
        );
    }

    #[test]
    fn planned_custom_window_inputs_encode_sort_keys_and_decode_select_values() {
        use super::super::window::{
            emit_hir_window_input_row, plan_hir_window_buffer, HirWindowBufferColumn,
        };
        let (document, schema) = analyze_sql_with_schema(
            ordered_custom_items_schema(),
            "SELECT value, row_number() OVER () FROM items ORDER BY value",
        );
        let query = document.query(root_query(&document)).unwrap();
        let block = &query.blocks[0];
        let mut buffer = plan_hir_window_buffer(query, block, block.windows[0].id).unwrap();
        let inputs = plan_hir_window_output_inputs(query, block, &mut buffer).unwrap();
        assert_eq!(buffer.columns.len(), 2);
        assert_ne!(inputs.values[0].1, inputs.sort_keys[0].1);
        assert!(matches!(
            buffer.columns[1],
            HirWindowBufferColumn::Encoded(_)
        ));
        let hir::Expr::Column(reference) = block.outputs[0].expr else {
            panic!("column output");
        };
        let mut program = program();
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(
            schema.get_btree_table("items").unwrap(),
        ));
        program.bind_source(
            reference.source,
            SourceBinding::BTree {
                scan_cursor: cursor,
                table_cursor: None,
            },
        );
        let row = emit_hir_window_input_row(&mut program, &document, &buffer).unwrap();
        assert!(!program.flags.suppress_custom_type_decode());
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Divide { dest, .. } if *dest == row.start)));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Divide { dest, .. } if *dest == row.start + 1)));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Column { dest, .. } if *dest == row.start + 1)));
    }

    #[test]
    fn window_output_uses_buffered_values_and_returns_after_skipped_rows() {
        for ordered in [false, true] {
            let order = if ordered { " ORDER BY id + 2" } else { "" };
            let document = analyze_sql(
                rowid_items_schema(),
                &format!(
                    "SELECT DISTINCT sum(value) OVER () + 1, value COLLATE nocase \
                          FROM items{order} LIMIT 2 OFFSET 1"
                ),
            );
            let query = document.query(root_query(&document)).unwrap();
            let block = &query.blocks[0];
            let mut program = program();
            let done = program.allocate_label();
            let label = program.allocate_label();
            let return_reg = program.alloc_register();
            let buffered = program.alloc_register();
            let result = program.alloc_register();
            let key = program.alloc_register();
            program.bind_window_result(hir::WindowFunctionId::new(block.id, 0), result);
            let distinct = initialize_hir_distinct(&mut program, block).unwrap();
            let sorter =
                ordered.then(|| initialize_hir_sorter(&mut program, query, block).unwrap());
            let limit =
                initialize_limit(&mut program, &document, query.limit.as_ref(), done, false)
                    .unwrap();
            let output = HirRowOutput {
                target: match &sorter {
                    Some(sorter) => HirRowTarget::Sorter(sorter),
                    None => HirRowTarget::Direct {
                        destination: &QueryDestination::ResultRows,
                        limit,
                        done,
                    },
                },
                distinct: distinct.as_ref(),
            };
            let values = [super::super::expr::ExprRegisterInput::new(
                &block.outputs[1].expr,
                buffered,
            )];
            let keys: Vec<_> = query
                .order_by
                .iter()
                .map(|term| super::super::expr::ExprRegisterInput::new(&term.expr, key))
                .collect();
            program.emit_insn(Insn::Gosub {
                target_pc: label,
                return_reg,
            });
            program.emit_insn(Insn::Goto { target_pc: done });
            let start = program.insns.len();
            let constant_spans = program.constant_spans.len();
            emit_hir_window_output_subroutine(
                &mut program,
                &document,
                block,
                &output,
                &HirRowInputs {
                    values: &values,
                    sort_keys: &keys,
                },
                label,
                return_reg,
            )
            .expect("window output lowers without original source cursor bindings");
            assert_eq!(
                program.constant_spans.len(),
                constant_spans,
                "row expressions cannot be hoisted outside the output subroutine"
            );
            let return_position = program.insns.len() - 1;
            if let Some(sorter) = &sorter {
                emit_hir_sorted_rows(
                    &mut program,
                    &document,
                    block,
                    sorter,
                    &QueryDestination::ResultRows,
                    limit,
                    done,
                )
                .unwrap();
            }
            program.preassign_label_to_next_insn(done);
            program.resolve_labels().unwrap();
            let insns = &program.insns[start..];
            for source in [buffered, result] {
                assert!(insns.iter().any(|(insn, _)| matches!(insn,
                    Insn::Copy { src_reg, .. } if *src_reg == source)));
            }
            let distinct_position = insns
                .iter()
                .position(|(insn, _)| match insn {
                    Insn::HashDistinct { data } => {
                        assert_eq!(data.collations[1], CollationSeq::NoCase);
                        assert_eq!(
                            data.target_pc,
                            BranchOffset::Offset(return_position.try_into().unwrap())
                        );
                        true
                    }
                    _ => false,
                })
                .unwrap();
            let offset_position = insns
                .iter()
                .position(|(insn, _)| matches!(insn, Insn::IfPos { .. }))
                .unwrap();
            assert!(distinct_position < offset_position);
            assert!(matches!(program.insns[return_position].0,
                Insn::Return { return_reg: actual, can_fallthrough: false } if actual == return_reg));
            if ordered {
                assert!(insns.iter().any(
                    |(insn, _)| matches!(insn, Insn::Copy { src_reg, .. } if *src_reg == key)
                ));
                let sort = insns
                    .iter()
                    .position(|(insn, _)| matches!(insn, Insn::SorterSort { .. }))
                    .unwrap();
                let insert = insns
                    .iter()
                    .position(|(insn, _)| matches!(insn, Insn::SorterInsert { .. }))
                    .unwrap();
                assert!(distinct_position < insert && insert < sort && sort < offset_position);
            } else {
                assert!(matches!(insns[offset_position].0,
                    Insn::IfPos { target_pc, .. } if target_pc == BranchOffset::Offset(return_position.try_into().unwrap())));
                let row = insns
                    .iter()
                    .position(|(insn, _)| matches!(insn, Insn::ResultRow { .. }))
                    .unwrap();
                let limit = insns
                    .iter()
                    .position(|(insn, _)| matches!(insn, Insn::DecrJumpZero { .. }))
                    .unwrap();
                assert!(offset_position < row && row < limit);
            }
        }
    }

    #[test]
    fn window_output_keeps_encoded_sort_keys_separate_from_decoded_values() {
        let document = analyze_sql(
            ordered_custom_items_schema(),
            "SELECT value, row_number() OVER () FROM items ORDER BY value",
        );
        let query = document.query(root_query(&document)).unwrap();
        let block = &query.blocks[0];
        let mut program = program();
        let sorter = initialize_hir_sorter(&mut program, query, block).unwrap();
        assert!(matches!(
            sorter.key_sources[0],
            HirSortKeySource::EncodedOutput(0)
        ));
        let decoded = program.alloc_register();
        let encoded = program.alloc_register();
        let window_result = program.alloc_register();
        program.bind_window_result(hir::WindowFunctionId::new(block.id, 0), window_result);
        let values = [super::super::expr::ExprRegisterInput::new(
            &block.outputs[0].expr,
            decoded,
        )];
        let keys = [super::super::expr::ExprRegisterInput::new(
            &block.outputs[0].expr,
            encoded,
        )];
        let label = program.allocate_label();
        let return_reg = program.alloc_register();
        let start = program.insns.len();
        emit_hir_window_output_subroutine(
            &mut program,
            &document,
            block,
            &HirRowOutput {
                target: HirRowTarget::Sorter(&sorter),
                distinct: None,
            },
            &HirRowInputs {
                values: &values,
                sort_keys: &keys,
            },
            label,
            return_reg,
        )
        .unwrap();
        let insns = &program.insns[start..];
        assert!(insns.iter().any(|(insn, _)| matches!(insn,
            Insn::Copy { src_reg, dst_reg, .. } if *src_reg == encoded && *dst_reg == sorter.row)));
        assert!(insns.iter().any(|(insn, _)| matches!(insn,
            Insn::Copy { src_reg, .. } if *src_reg == decoded)));
        assert!(!program.flags.suppress_custom_type_decode());
    }

    #[test]
    fn planned_distinct_uses_hir_collations_before_offset() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT DISTINCT value COLLATE nocase FROM items LIMIT 1 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("DISTINCT HIR query emits");
        program.resolve_labels().expect("DISTINCT labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::HashClear { .. })));
        let distinct = program
            .insns
            .iter()
            .position(|(insn, _)| match insn {
                Insn::HashDistinct { data } => data.collations == [CollationSeq::NoCase],
                _ => false,
            })
            .expect("DISTINCT uses the resolved output collation");
        let offset = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfPos {
                        decrement_by: 1,
                        ..
                    }
                )
            })
            .expect("OFFSET emits");
        assert!(distinct < offset, "OFFSET counts unique rows");
    }

    #[test]
    fn planned_order_by_uses_hir_terms_and_sorts_before_limit() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value, extra FROM items \
             ORDER BY extra COLLATE nocase DESC NULLS FIRST LIMIT 2 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("ordered HIR query emits");
        program.resolve_labels().expect("ORDER BY labels resolve");

        let sorter_open = program
            .insns
            .iter()
            .position(|(insn, _)| match insn {
                Insn::SorterOpen { data } => {
                    data.order_collations_nulls
                        == [(
                            SortOrder::Desc,
                            Some(CollationSeq::NoCase),
                            Some(NullsOrder::First),
                        )]
                }
                _ => false,
            })
            .expect("sorter uses resolved direction, collation, and NULL order");
        let sorter_insert = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterInsert { .. }))
            .expect("rows enter sorter");
        let sorter_sort = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterSort { .. }))
            .expect("sorter drains");
        let offset = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfPos {
                        decrement_by: 1,
                        ..
                    }
                )
            })
            .expect("OFFSET emits");
        assert!(sorter_open < sorter_insert && sorter_insert < sorter_sort);
        assert!(sorter_sort < offset, "LIMIT/OFFSET applies after sorting");
    }

    fn emit_planned_root(plan: &HirPlan) -> ProgramBuilder {
        let mut program = program();
        emit_planned_query_body(
            &mut program,
            plan,
            root_query(&plan.document),
            &QueryDestination::ResultRows,
        )
        .expect("HIR query emits");
        program.resolve_labels().expect("HIR labels resolve");
        program
    }

    fn insn_positions(program: &ProgramBuilder, matches: impl Fn(&Insn) -> bool) -> Vec<usize> {
        program
            .insns
            .iter()
            .enumerate()
            .filter(|(_, (insn, _))| matches(insn))
            .map(|(position, _)| position)
            .collect()
    }

    #[test]
    fn planned_window_sorts_source_rows_by_partition_then_order_before_buffering() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value, sum(value) OVER (PARTITION BY extra COLLATE nocase \
             ORDER BY value DESC NULLS FIRST) FROM items LIMIT 3",
        );
        let program = emit_planned_root(&plan);

        let window_sorter = program
            .insns
            .iter()
            .find_map(|(insn, _)| match insn {
                Insn::SorterOpen { data } => Some(data),
                _ => None,
            })
            .expect("window keys open a source sorter");
        assert_eq!(
            window_sorter.order_collations_nulls,
            [
                (SortOrder::Asc, Some(CollationSeq::NoCase), None),
                (SortOrder::Desc, None, Some(NullsOrder::First)),
            ]
        );
        let table_scan = insn_positions(&program, |insn| matches!(insn, Insn::Next { .. }));
        let sorter_insert =
            insn_positions(&program, |insn| matches!(insn, Insn::SorterInsert { .. }));
        let sorter_sort = insn_positions(&program, |insn| matches!(insn, Insn::SorterSort { .. }));
        let buffer_insert = insn_positions(&program, |insn| matches!(insn, Insn::Insert { .. }));
        let result_row = insn_positions(&program, |insn| matches!(insn, Insn::ResultRow { .. }));
        assert_eq!(sorter_insert.len(), 1);
        assert_eq!(sorter_sort.len(), 1);
        assert!(
            sorter_insert[0] < table_scan[0],
            "source rows are queued in the scan"
        );
        assert!(
            table_scan[0] < sorter_sort[0],
            "sorting starts after the scan"
        );
        assert!(
            buffer_insert.iter().all(|insert| *insert > sorter_sort[0]),
            "only sorted rows enter the window buffer"
        );
        assert_eq!(
            result_row.len(),
            1,
            "one output subroutine serves every row"
        );
        assert!(result_row[0] > *buffer_insert.last().unwrap());
        assert!(
            insn_positions(&program, |insn| matches!(insn, Insn::DecrJumpZero { .. })).len() == 1,
            "LIMIT applies to window output rows, not source rows"
        );
    }

    #[test]
    fn planned_window_without_keys_streams_scan_rows_into_the_buffer() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT id, row_number() OVER () FROM items",
        );
        let program = emit_planned_root(&plan);

        assert!(
            insn_positions(&program, |insn| matches!(insn, Insn::SorterOpen { .. })).is_empty()
        );
        let rewind = insn_positions(&program, |insn| matches!(insn, Insn::Rewind { .. }));
        let next = insn_positions(&program, |insn| matches!(insn, Insn::Next { .. }));
        let buffer_insert = insn_positions(&program, |insn| matches!(insn, Insn::Insert { .. }));
        let scan_start = rewind[0];
        let scan_end = *next.last().unwrap();
        assert!(
            buffer_insert
                .iter()
                .all(|insert| scan_start < *insert && *insert < scan_end),
            "each scanned row is buffered inside the scan loop"
        );
    }

    #[test]
    fn planned_window_over_groups_keeps_group_order_for_equal_keys() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT extra, count(*), sum(count(*)) OVER (PARTITION BY count(*)) \
             FROM items GROUP BY extra",
        );
        let program = emit_planned_root(&plan);

        let sorters = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::SorterOpen { data } => Some(data.order_collations_nulls.len()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            sorters.contains(&2),
            "window sorter has the partition key plus a sequence column: {sorters:?}"
        );
        assert_eq!(
            insn_positions(&program, |insn| matches!(insn, Insn::Sequence { .. })).len(),
            1
        );
    }

    #[test]
    fn planned_window_drops_order_terms_equal_to_partition_terms() {
        let plan = analyze_plan(
            indexed_items_schema(),
            // NOT INDEXED: an index on value would give the order unsorted.
            "SELECT rank() OVER (PARTITION BY value ORDER BY value DESC) FROM items NOT INDEXED",
        );
        let program = emit_planned_root(&plan);

        let keys = program
            .insns
            .iter()
            .find_map(|(insn, _)| match insn {
                Insn::SorterOpen { data } => Some(data.order_collations_nulls.clone()),
                _ => None,
            })
            .expect("window keys open a source sorter");
        assert_eq!(keys, [(SortOrder::Asc, None, None)]);
    }

    #[test]
    fn planned_query_with_two_windows_nests_one_layer_per_window() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT row_number() OVER (ORDER BY value), rank() OVER (ORDER BY extra) FROM items",
        );
        let program = emit_planned_root(&plan);

        let sorters = insn_positions(&program, |insn| matches!(insn, Insn::SorterOpen { .. }));
        assert_eq!(sorters.len(), 2, "each window sorts its own input");
        let sorts = insn_positions(&program, |insn| matches!(insn, Insn::SorterSort { .. }));
        let inserts = insn_positions(&program, |insn| matches!(insn, Insn::SorterInsert { .. }));
        let scan_end = *insn_positions(&program, |insn| matches!(insn, Insn::Next { .. }))
            .first()
            .unwrap();
        assert!(
            inserts[0] < scan_end,
            "table rows feed the innermost (last) window"
        );
        assert!(
            sorts[0] < inserts[1] && inserts[1] < sorts[1],
            "inner window output feeds the outer window before it sorts"
        );
        assert_eq!(
            insn_positions(&program, |insn| matches!(insn, Insn::ResultRow { .. })).len(),
            1
        );
    }

    #[test]
    fn planned_distinct_order_by_deduplicates_before_sorting() {
        let plan = analyze_plan(
            indexed_items_schema(),
            // No index supplies `extra` order, so rows go through a sorter.
            "SELECT DISTINCT value, extra FROM items ORDER BY extra DESC",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("DISTINCT ordered HIR query emits");
        program
            .resolve_labels()
            .expect("DISTINCT ORDER BY labels resolve");

        let distinct = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::HashDistinct { .. }))
            .expect("DISTINCT check emits");
        let sorter_insert = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterInsert { .. }))
            .expect("unique rows enter sorter");
        assert!(distinct < sorter_insert);
    }

    #[test]
    fn planned_order_by_uses_custom_comparator_from_hir_type_fact() {
        let plan = analyze_plan(
            ordered_custom_items_schema(),
            "SELECT value FROM items ORDER BY value",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("custom ordered HIR query emits");
        program
            .resolve_labels()
            .expect("custom ORDER BY labels resolve");

        assert!(program.insns.iter().any(|(insn, _)| match insn {
            Insn::SorterOpen { data } => {
                data.comparators == [Some(crate::vdbe::insn::SortComparatorType::NumericLt)]
            }
            _ => false,
        }));
    }

    #[test]
    fn planned_compound_operators_use_hir_blocks_and_existing_index_operations() {
        for (sql, expected) in [
            ("SELECT 1 UNION SELECT 1", "union"),
            ("SELECT 1 EXCEPT SELECT 2", "except"),
            ("SELECT 1 INTERSECT SELECT 1", "intersect"),
        ] {
            let plan = analyze_plan(Schema::new(), sql);
            let query = root_query(&plan.document);
            let mut program = program();
            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("compound HIR query emits");
            program.resolve_labels().expect("compound labels resolve");

            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::OpenEphemeral {
                    is_table: false,
                    ..
                }
            )));
            match expected {
                "union" => assert!(program
                    .insns
                    .iter()
                    .any(|(insn, _)| matches!(insn, Insn::IdxInsert { .. }))),
                "except" => assert!(program
                    .insns
                    .iter()
                    .any(|(insn, _)| matches!(insn, Insn::IdxDelete { .. }))),
                "intersect" => assert!(program
                    .insns
                    .iter()
                    .any(|(insn, _)| matches!(insn, Insn::NotFound { .. }))),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn planned_union_all_shares_limit_and_offset_across_arms() {
        let plan = analyze_plan(
            Schema::new(),
            "SELECT 1 UNION ALL SELECT 2 LIMIT 1 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("UNION ALL HIR query emits");
        program.resolve_labels().expect("compound labels resolve");

        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenEphemeral { .. })));
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::ResultRow { .. }))
                .count(),
            2
        );
        let limit_registers = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::DecrJumpZero { reg, .. } => Some(*reg),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(limit_registers.len(), 2);
        assert!(limit_registers
            .iter()
            .all(|register| *register == limit_registers[0]));
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn planned_compound_order_by_sorts_before_shared_limit_and_offset() {
        let plan = analyze_plan(
            Schema::new(),
            "SELECT 2 AS value UNION ALL SELECT 1 ORDER BY value DESC LIMIT 1 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("ordered compound HIR query emits");
        program.resolve_labels().expect("compound labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SorterOpen { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SorterSort { .. })));
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn planned_recursive_cte_accepts_compound_seed_query() {
        let plan = analyze_plan(
            Schema::new(),
            "WITH RECURSIVE seq(x) AS (\
                 SELECT 1 UNION ALL SELECT 10 \
                 UNION ALL SELECT x + 1 FROM seq WHERE x < 3\
             ) SELECT x FROM seq",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("recursive CTE with compound seed emits from HIR");
        program
            .resolve_labels()
            .expect("recursive compound labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IdxDelete { .. })));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenPseudo { .. })));
    }

    #[test]
    fn scalar_row_and_exists_destinations_install_matching_bindings() {
        for sql in [
            "SELECT (SELECT 1)",
            "SELECT (1, 2) = (SELECT 3, 4)",
            "SELECT EXISTS (SELECT 1)",
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let subquery = root_subquery(&document);
            let mut program = program();
            let prepared = prepare_subquery(&mut program, &document, subquery)
                .expect("subquery destination prepares");

            match (&prepared.destination, subquery) {
                (
                    QueryDestination::RowValueSubqueryResult {
                        result_reg_start,
                        num_regs,
                    },
                    SubqueryExpr::Scalar { .. } | SubqueryExpr::Row { .. },
                ) => assert_eq!(
                    program.subquery_binding(prepared.query),
                    Some(SubqueryBinding::RowValue {
                        start: *result_reg_start,
                        count: *num_regs,
                    })
                ),
                (
                    QueryDestination::ExistsSubqueryResult { result_reg },
                    SubqueryExpr::Exists(_),
                ) => assert_eq!(
                    program.subquery_binding(prepared.query),
                    Some(SubqueryBinding::Exists {
                        register: *result_reg,
                    })
                ),
                _ => panic!("subquery kind has its matching destination"),
            }
            assert!(!prepared.correlated);
        }
    }

    #[test]
    fn in_destination_uses_resolved_comparison_metadata() {
        let document = analyze_sql(
            Schema::new(),
            "SELECT ('a' COLLATE nocase, 2) IN (SELECT 'A', 3)",
        );
        let subquery = root_subquery(&document);
        let SubqueryExpr::In {
            comparison, query, ..
        } = subquery
        else {
            panic!("root is an IN subquery");
        };
        let expected_affinity = comparison
            .components
            .iter()
            .map(|component| component.affinity.aff_mask())
            .collect::<String>();
        let mut program = program();
        let prepared =
            prepare_subquery(&mut program, &document, subquery).expect("IN destination prepares");

        let QueryDestination::EphemeralIndex {
            cursor_id,
            index,
            affinity_str: Some(affinity_str),
            is_delete: false,
        } = &prepared.destination
        else {
            panic!("IN uses an ephemeral index");
        };
        assert_eq!(affinity_str.as_str(), expected_affinity);
        assert_eq!(index.columns.len(), comparison.components.len());
        for (column, component) in index.columns.iter().zip(&comparison.components) {
            assert_eq!(
                column.collation,
                component
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
            );
        }
        assert_eq!(
            program.subquery_binding(*query),
            Some(SubqueryBinding::InIndex { cursor: *cursor_id })
        );
    }

    #[test]
    fn correlation_comes_from_the_resolved_query_capture_set() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let document = analyze_sql(schema, "SELECT (SELECT value) FROM items");
        let subquery = root_subquery(&document);
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, subquery)
            .expect("correlated subquery destination prepares");

        assert!(prepared.correlated);
        assert!(!document
            .query(prepared.query)
            .expect("subquery exists")
            .captures
            .is_empty());
    }

    #[test]
    fn subquery_wrapper_runs_once_and_clears_results_before_the_body() {
        for sql in [
            "SELECT (1, 2) = (SELECT 3, 4)",
            "SELECT EXISTS (SELECT 1)",
            "SELECT 1 IN (SELECT 2)",
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let subquery = root_subquery(&document);
            let mut program = program();
            let prepared = prepare_subquery(&mut program, &document, subquery)
                .expect("subquery destination prepares");
            let marker = program.alloc_register();
            emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
                assert_eq!(query, prepared.query);
                assert!(std::ptr::eq(destination, &prepared.destination));
                program.emit_insn(Insn::Integer {
                    value: 99,
                    dest: marker,
                });
                Ok(())
            })
            .expect("subquery wrapper emits");

            assert!(matches!(
                program.insns.first(),
                Some((Insn::Once { .. }, _))
            ));
            let marker_position = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(insn, Insn::Integer { value: 99, dest } if *dest == marker)
                })
                .expect("body marker was emitted");
            match &prepared.destination {
                QueryDestination::RowValueSubqueryResult {
                    result_reg_start,
                    num_regs,
                } => {
                    assert!(matches!(
                        program.insns.get(1),
                        Some((Insn::BeginSubrtn { .. }, _))
                    ));
                    assert_eq!(marker_position, 2 + num_regs);
                    for (offset, register) in
                        (*result_reg_start..*result_reg_start + *num_regs).enumerate()
                    {
                        assert!(matches!(
                            program.insns.get(2 + offset),
                            Some((Insn::Null { dest, dest_end: None }, _)) if *dest == register
                        ));
                    }
                    assert!(matches!(
                        program.insns.get(marker_position + 1),
                        Some((Insn::Return { .. }, _))
                    ));
                }
                QueryDestination::ExistsSubqueryResult { result_reg } => {
                    assert!(matches!(
                        program.insns.as_slice(),
                        [
                            (Insn::Once { .. }, _),
                            (Insn::BeginSubrtn { .. }, _),
                            (Insn::Integer { value: 0, dest }, _),
                            (Insn::Integer { value: 99, .. }, _),
                            (Insn::Return { .. }, _),
                        ] if *dest == *result_reg
                    ));
                }
                QueryDestination::EphemeralIndex { cursor_id, .. } => {
                    assert!(matches!(
                        program.insns.as_slice(),
                        [
                            (Insn::Once { .. }, _),
                            (Insn::OpenEphemeral { cursor_id: opened, is_table: false }, _),
                            (Insn::Integer { value: 99, .. }, _),
                        ] if *opened == *cursor_id
                    ));
                }
                _ => panic!("expression subquery has a supported destination"),
            }
        }
    }

    #[test]
    fn correlated_subquery_wrapper_does_not_emit_once() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let document = analyze_sql(schema, "SELECT (SELECT value) FROM items");
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
            .expect("correlated subquery destination prepares");

        emit_prepared_subquery(&mut program, &prepared, |_, _, _| Ok(()))
            .expect("correlated wrapper emits");

        assert!(prepared.correlated);
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Once { .. })));
    }

    #[test]
    fn subquery_wrapper_preserves_scalar_and_list_eqp_nodes() {
        for (sql, list) in [
            ("SELECT (SELECT 1)", false),
            ("SELECT 1 IN (SELECT 2)", true),
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let mut program = program_with_mode(QueryMode::ExplainQueryPlan {
                format: turso_parser::ast::EqpFormat::Text,
            });
            let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
                .expect("subquery destination prepares");
            emit_prepared_subquery(&mut program, &prepared, |_, _, _| Ok(()))
                .expect("subquery wrapper emits");

            assert!(matches!(
                program.insns.first(),
                Some((
                    Insn::Explain { detail, .. },
                    _
                )) if matches!(
                    detail.as_ref(),
                    EqpDetail::ListSubquery { correlated: false, .. } if list
                ) || matches!(
                    detail.as_ref(),
                    EqpDetail::ScalarSubquery { correlated: false, .. } if !list
                )
            ));
        }
    }

    #[test]
    fn explain_query_plan_names_each_loop_before_its_cursors_open() {
        let rows = |sql| {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let mut program = program_with_mode(QueryMode::ExplainQueryPlan {
                format: turso_parser::ast::EqpFormat::Text,
            });
            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("EXPLAIN QUERY PLAN body emits");
            program
                .insns
                .iter()
                .enumerate()
                .filter_map(|(position, (insn, _))| match insn {
                    Insn::Explain { detail, .. } => Some((position, detail.to_string())),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        // The loop rows come first, so their ids are stable.
        assert_eq!(
            rows("SELECT value FROM items ORDER BY value"),
            [(0, "SCAN items USING COVERING INDEX items_value".to_string())]
        );
        assert_eq!(
            rows("SELECT a.extra FROM items a JOIN items b ON b.value = a.value"),
            [
                (0, "SCAN items AS a".to_string()),
                (
                    1,
                    "SEARCH b USING COVERING INDEX items_value (value=?)".to_string()
                ),
            ]
        );
        assert_eq!(rows("SELECT 1"), [(0, "SCAN CONSTANT ROW".to_string())]);
    }

    #[test]
    fn constant_select_binds_outputs_and_emits_one_result_row() {
        let document = analyze_sql(Schema::new(), "SELECT 1, 2");
        let query_id = root_query(&document);
        let query = document.query(query_id).expect("root query exists");
        let outputs = &query.blocks[0].outputs;
        let mut program = program();

        emit_query_body(
            &mut program,
            &document,
            query_id,
            &QueryDestination::ResultRows,
        )
        .expect("constant SELECT emits");

        let start = match program.insns.as_slice() {
            [(
                Insn::Integer {
                    value: 1,
                    dest: first,
                },
                _,
            ), (
                Insn::Integer {
                    value: 2,
                    dest: second,
                },
                _,
            ), (
                Insn::ResultRow {
                    start_reg,
                    count: 2,
                },
                _,
            )] if *second == *first + 1 && *start_reg == *first => *first,
            _ => panic!("constant SELECT keeps consecutive output registers"),
        };
        for (position, output) in outputs.iter().enumerate() {
            assert_eq!(
                program
                    .output_binding(output.id)
                    .expect("output register is bound")
                    .register,
                start + position
            );
        }
    }

    #[test]
    fn values_emits_every_row_without_hoisting_constants() {
        let document = analyze_sql(Schema::new(), "VALUES (1, 2), (3, 4)");
        let mut program = program();

        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("VALUES emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::ResultRow { count: 2, .. }))
                .count(),
            2
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter_map(|(insn, _)| match insn {
                    Insn::Integer { value, .. } => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn scalar_subquery_stops_after_one_row_and_in_subquery_inserts_every_row() {
        let scalar = analyze_sql(Schema::new(), "SELECT (VALUES (1), (2))");
        let mut scalar_program = program();
        let scalar_prepared =
            prepare_subquery(&mut scalar_program, &scalar, root_subquery(&scalar))
                .expect("scalar destination prepares");
        emit_prepared_subquery(
            &mut scalar_program,
            &scalar_prepared,
            |program, query, destination| emit_query_body(program, &scalar, query, destination),
        )
        .expect("scalar query emits");
        assert!(scalar_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Integer { value: 1, .. })));
        assert!(!scalar_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Integer { value: 2, .. })));
        assert_eq!(
            scalar_program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Copy { .. }))
                .count(),
            1
        );

        let membership = analyze_sql(Schema::new(), "SELECT 2 IN (VALUES (1), (2))");
        let mut membership_program = program();
        let membership_prepared = prepare_subquery(
            &mut membership_program,
            &membership,
            root_subquery(&membership),
        )
        .expect("IN destination prepares");
        emit_prepared_subquery(
            &mut membership_program,
            &membership_prepared,
            |program, query, destination| emit_query_body(program, &membership, query, destination),
        )
        .expect("IN query emits");
        assert_eq!(
            membership_program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IdxInsert { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn exists_skips_output_evaluation_and_arrays_are_decoded() {
        let exists = analyze_sql(Schema::new(), "SELECT EXISTS (SELECT random())");
        let mut exists_program = program();
        let prepared = prepare_subquery(&mut exists_program, &exists, root_subquery(&exists))
            .expect("EXISTS destination prepares");
        emit_prepared_subquery(
            &mut exists_program,
            &prepared,
            |program, query, destination| emit_query_body(program, &exists, query, destination),
        )
        .expect("EXISTS query emits");
        assert!(!exists_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Function { .. })));
        let QueryDestination::ExistsSubqueryResult { result_reg } = prepared.destination else {
            panic!("EXISTS uses its result destination");
        };
        assert!(exists_program.insns.iter().any(|(insn, _)| {
            matches!(insn, Insn::Integer { value: 1, dest } if *dest == result_reg)
        }));

        let array = analyze_sql(Schema::new(), "SELECT ARRAY[1, 2]");
        let mut array_program = program();
        emit_query_body(
            &mut array_program,
            &array,
            root_query(&array),
            &QueryDestination::ResultRows,
        )
        .expect("array SELECT emits");
        assert!(array_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::ArrayDecode { .. })));
    }

    #[test]
    fn no_from_filter_branches_around_result_emission() {
        let document = analyze_sql(Schema::new(), "SELECT 7 WHERE 0");
        let mut program = program();
        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("filtered SELECT emits");

        let filter_jump = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: true,
                        ..
                    }
                )
            })
            .expect("filter emits a false-or-null jump");
        let output = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Integer { value: 7, .. }))
            .expect("result expression is emitted");
        let result = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::ResultRow { .. }))
            .expect("result destination is emitted");
        assert!(filter_jump < output && output < result);
    }

    #[test]
    fn no_from_limit_and_offset_keep_row_counter_opcodes() {
        let document = analyze_sql(Schema::new(), "SELECT 1 LIMIT 1 OFFSET 1");
        let mut program = program();
        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("limited SELECT emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: false,
                        ..
                    }
                ))
                .count(),
            1
        );
        let offset_limit = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
            .expect("offset and limit are combined");
        let limit_exit = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: false,
                        ..
                    }
                )
            })
            .expect("limit exits before rows when its counter is false");
        assert!(offset_limit < limit_exit);
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IfPos { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::ResultRow { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::DecrJumpZero { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn scalar_select_offset_skips_its_only_candidate_row() {
        let document = analyze_sql(Schema::new(), "SELECT (SELECT 1 LIMIT 1 OFFSET 1)");
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
            .expect("scalar destination prepares");
        emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
            emit_query_body(program, &document, query, destination)
        })
        .expect("offset scalar query emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IfPos { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Copy { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Goto { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn floating_limit_and_offset_keep_integer_checks() {
        let document = analyze_sql(Schema::new(), "SELECT 1 LIMIT 1.0 OFFSET 0.0");
        let mut program = program();
        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("floating counters emit");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::MustBeInt { .. }))
                .count(),
            3
        );
    }

    #[test]
    fn planned_full_scan_binds_its_source_and_emits_residual_filter() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value INTEGER)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let plan = analyze_plan(
            schema,
            "SELECT value FROM items WHERE value LIMIT 2 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let source = plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0]
            .source;
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned full scan emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: None,
        } = program
            .source_binding(source)
            .copied()
            .expect("source has a physical binding")
        else {
            panic!("full table scan binds one B-tree cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let open = position(&|insn| {
            matches!(
                insn,
                Insn::OpenRead {
                    cursor_id,
                    root_page: 2,
                    db: MAIN_DB_ID,
                } if *cursor_id == scan_cursor
            )
        });
        let rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let filter = position(&|insn| {
            matches!(
                insn,
                Insn::IfNot {
                    jump_if_null: true,
                    ..
                }
            )
        });
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 1, .. }));
        let next = position(&|insn| {
            matches!(
                insn,
                Insn::Next {
                    cursor_id,
                    fullscan: true,
                    ..
                } if *cursor_id == scan_cursor
            )
        });
        assert!(open < rewind && rewind < filter && filter < result && result < next);
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IfPos { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::DecrJumpZero { .. })));
    }

    #[test]
    fn planned_derived_table_streams_its_hir_query_as_a_coroutine() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT d.value \
             FROM (SELECT value FROM items NOT INDEXED WHERE value > 1) AS d \
             WHERE d.value < 5",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let [planned_loop] = block_plan.loops.as_slice() else {
            panic!("derived table has one physical loop");
        };
        let HirSourceAccess::Derived {
            query: derived_query,
            ..
        } = planned_loop.access
        else {
            panic!("derived source keeps its prepared HIR query");
        };
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("derived-table query emits");

        assert_ne!(derived_query, query);
        assert_streams_as_coroutine(&program, planned_loop.source);
    }

    /// The source reads the row its coroutine yields: the coroutine is
    /// defined, then started and resumed by the loop, which emits the result.
    fn assert_streams_as_coroutine(program: &ProgramBuilder, source: SourceId) {
        assert!(matches!(
            program.source_binding(source),
            Some(SourceBinding::Registers { rowid: None, .. })
        ));
        let positions = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .enumerate()
                .filter_map(|(position, (insn, _))| matches(insn).then_some(position))
                .collect::<Vec<_>>()
        };
        let init = positions(&|insn| matches!(insn, Insn::InitCoroutine { .. }));
        let end = positions(&|insn| matches!(insn, Insn::EndCoroutine { .. }));
        let yields = positions(&|insn| matches!(insn, Insn::Yield { .. }));
        let result = positions(&|insn| matches!(insn, Insn::ResultRow { .. }));
        let [define, start] = init.as_slice() else {
            panic!("the coroutine is defined and started once each: {init:?}");
        };
        let [end] = end.as_slice() else {
            panic!("the coroutine has one end");
        };
        assert!(define < end && end < start);
        assert!(yields
            .iter()
            .any(|position| define < position && position < end));
        assert!(yields.iter().any(|position| start < position));
        assert!(result.iter().all(|position| start < position));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenEphemeral { is_table: true, .. })));
    }

    #[test]
    fn planned_derived_table_in_an_outer_join_is_materialized() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT d.value FROM items AS o \
             LEFT JOIN (SELECT value FROM items NOT INDEXED WHERE value > 1) AS d \
             ON d.value = o.value",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("outer-join derived-table query emits");
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenEphemeral { is_table: true, .. })));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::InitCoroutine { .. })));
    }

    #[test]
    fn planned_single_reference_cte_streams_its_hir_query() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "WITH chosen AS (\
                 SELECT value FROM items NOT INDEXED WHERE value > 1\
             ) \
             SELECT c.value FROM chosen AS c WHERE c.value < 5",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let [planned_loop] = block_plan.loops.as_slice() else {
            panic!("CTE reference has one physical loop");
        };
        let HirSourceAccess::Cte {
            query: cte_query,
            materialization,
            ..
        } = &planned_loop.access
        else {
            panic!("CTE source keeps its prepared HIR query");
        };
        assert_eq!(*materialization, HirCteMaterialization::PerReference);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("single-reference CTE query emits");

        assert_ne!(*cte_query, query);
        assert_streams_as_coroutine(&program, planned_loop.source);
    }

    #[test]
    fn planned_shared_cte_materializes_once_and_opens_one_cursor_per_reference() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "WITH chosen AS (\
                 SELECT value FROM items NOT INDEXED WHERE value > 1\
             ) \
             SELECT a.value, b.value \
             FROM chosen AS a JOIN chosen AS b",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 2);
        let ctes = block_plan
            .loops
            .iter()
            .map(|planned_loop| {
                let HirSourceAccess::Cte {
                    cte,
                    materialization,
                    ..
                } = &planned_loop.access
                else {
                    panic!("both sources use the shared CTE");
                };
                assert_eq!(*materialization, HirCteMaterialization::Shared);
                *cte
            })
            .collect::<Vec<_>>();
        assert_eq!(ctes[0], ctes[1]);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("shared CTE query emits");

        let cursors = block_plan
            .loops
            .iter()
            .map(|planned_loop| {
                let SourceBinding::BTree {
                    scan_cursor,
                    table_cursor: None,
                } = program
                    .source_binding(planned_loop.source)
                    .copied()
                    .expect("shared CTE source is bound")
                else {
                    panic!("each CTE reference uses one table cursor");
                };
                scan_cursor
            })
            .collect::<Vec<_>>();
        assert_ne!(cursors[0], cursors[1]);
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OpenEphemeral { is_table: true, .. }))
                .count(),
            1
        );
        let duplicates = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::OpenDup {
                    new_cursor_id,
                    original_cursor_id,
                } => Some((*new_cursor_id, *original_cursor_id)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let [duplicate] = duplicates.as_slice() else {
            panic!("second CTE reference opens one duplicate cursor");
        };
        assert!(cursors.contains(&duplicate.0));
        assert!(cursors.contains(&duplicate.1));
        assert!(cursors.iter().all(|cursor| program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::Rewind { cursor_id, .. } if cursor_id == cursor)
        )));
        assert!(program.hir_materialized_cte(ctes[0]).is_some());
    }

    #[test]
    fn planned_explicit_cte_uses_registered_materialized_storage() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "WITH chosen AS MATERIALIZED (\
                 SELECT value FROM items NOT INDEXED\
             ) \
             SELECT value FROM chosen",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        let HirSourceAccess::Cte {
            cte,
            materialization,
            ..
        } = &planned_loop.access
        else {
            panic!("explicit CTE keeps its CTE access");
        };
        assert_eq!(*materialization, HirCteMaterialization::Explicit);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("explicit CTE query emits");

        assert!(program.hir_materialized_cte(*cte).is_some());
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OpenEphemeral { is_table: true, .. }))
                .count(),
            1
        );
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenDup { .. })));
    }

    #[test]
    fn planned_recursive_cte_uses_queue_and_register_bound_input() {
        let plan = analyze_plan(
            Schema::new(),
            "WITH RECURSIVE numbers(value) AS (\
                 VALUES (1) \
                 UNION ALL \
                 SELECT value + 1 FROM numbers WHERE value < 3\
             ) \
             SELECT value FROM numbers",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        let HirSourceAccess::RecursiveCte { cte } = planned_loop.access else {
            panic!("recursive CTE keeps recursive access");
        };
        let hir::CteBody::Recursive(recursive) = &plan.document.cte(cte).expect("CTE exists").body
        else {
            panic!("CTE body is recursive");
        };
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("recursive CTE query emits");

        assert!(recursive.input_sources.iter().all(|source| matches!(
            program.source_binding(*source),
            Some(SourceBinding::Registers { .. })
        )));
        // A single reference streams the recursion instead of sharing a table.
        assert!(program.hir_materialized_cte(cte).is_none());
        assert!(matches!(
            program.source_binding(planned_loop.source),
            Some(SourceBinding::Registers { .. })
        ));
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(
                    insn,
                    Insn::OpenEphemeral {
                        is_table: false,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IdxDelete { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Goto { .. })));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenPseudo { .. })));
    }

    #[test]
    fn planned_recursive_union_uses_seen_rows_index() {
        let plan = analyze_plan(
            Schema::new(),
            "WITH RECURSIVE numbers(value) AS (\
                 VALUES (1) \
                 UNION \
                 SELECT value + 1 FROM numbers WHERE value < 3\
             ) \
             SELECT value FROM numbers",
        );
        let query = root_query(&plan.document);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("recursive UNION query emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(
                    insn,
                    Insn::OpenEphemeral {
                        is_table: false,
                        ..
                    }
                ))
                .count(),
            2
        );
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Found { .. })));
    }

    #[test]
    fn planned_recursive_queue_keeps_order_limit_and_offset() {
        let plan = analyze_plan(
            Schema::new(),
            "WITH RECURSIVE numbers(value) AS (\
                 VALUES (1) \
                 UNION ALL \
                 SELECT value + 1 FROM numbers WHERE value < 5 \
                 ORDER BY 1 DESC LIMIT 3 OFFSET 1\
             ) \
             SELECT value FROM numbers",
        );
        let query = root_query(&plan.document);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("ordered recursive query emits");

        let queue_cursor = program
            .insns
            .iter()
            .find_map(|(insn, _)| match insn {
                Insn::OpenEphemeral {
                    cursor_id,
                    is_table: false,
                } => Some(*cursor_id),
                _ => None,
            })
            .expect("recursive queue opens");
        let Some(CursorType::BTreeIndex(queue)) = program.get_cursor_type(queue_cursor) else {
            panic!("recursive queue uses an ephemeral index");
        };
        assert_eq!(queue.columns[0].order, SortOrder::Desc);
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IfPos { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::DecrJumpZero { .. })));
    }

    #[cfg(feature = "json")]
    #[test]
    fn planned_virtual_table_uses_resolved_arguments_and_virtual_cursor() {
        let plan = analyze_plan(Schema::new(), "SELECT value FROM json_each('[1]')");
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        let HirSourceAccess::Virtual(operation) = &planned_loop.access else {
            panic!("table function uses virtual-table access");
        };
        // Table-function arguments keep SQLite's unary plus wrapper so the
        // virtual table receives the argument as written.
        assert!(matches!(
            operation.arguments.as_slice(),
            [Expr::Unary {
                operator: turso_parser::ast::UnaryOperator::Positive,
                expr
            }] if matches!(expr.as_ref(), Expr::Literal(Literal::String(_)))
        ));
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("virtual-table query emits");

        let SourceBinding::Virtual { cursor } = program
            .source_binding(planned_loop.source)
            .copied()
            .expect("virtual source is bound")
        else {
            panic!("virtual source uses one virtual cursor");
        };
        assert!(matches!(
            program.get_cursor_type(cursor),
            Some(CursorType::VirtualTable(_))
        ));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::VOpen { cursor_id } if *cursor_id == cursor)));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::VFilter {
                cursor_id,
                arg_count: 1,
                idx_num,
                ..
            } if *cursor_id == cursor && *idx_num == operation.idx_num as usize
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::VColumn { cursor_id, .. } if *cursor_id == cursor
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::VNext { cursor_id, .. } if *cursor_id == cursor
        )));
    }

    #[test]
    fn planned_cross_join_nests_loops_and_places_predicates_when_ready() {
        let plan = analyze_plan(
            joined_items_schema(),
            "SELECT l.value, r.value \
             FROM left_items AS l NOT INDEXED CROSS JOIN right_items AS r NOT INDEXED \
             WHERE l.value AND r.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 2);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned cross join emits");

        let cursors = block_plan
            .loops
            .iter()
            .map(|planned_loop| {
                let SourceBinding::BTree {
                    scan_cursor,
                    table_cursor: None,
                } = program
                    .source_binding(planned_loop.source)
                    .copied()
                    .expect("join source is bound")
                else {
                    panic!("table scan uses one cursor");
                };
                scan_cursor
            })
            .collect::<Vec<_>>();
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let outer_rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == cursors[0]),
        );
        let outer_column = position(
            &|insn| matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == cursors[0]),
        );
        let inner_rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == cursors[1]),
        );
        let inner_column = position(
            &|insn| matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == cursors[1]),
        );
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 2, .. }));
        let advances = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::Next {
                    cursor_id,
                    fullscan: true,
                    ..
                } => Some(*cursor_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let empty_targets = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::Rewind {
                    cursor_id,
                    pc_if_empty,
                } if cursors.contains(cursor_id) => Some(*pc_if_empty),
                _ => None,
            })
            .collect::<Vec<_>>();
        let predicate_failures = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::IfNot {
                    target_pc,
                    jump_if_null: true,
                    ..
                } => Some(*target_pc),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(outer_rewind < outer_column && outer_column < inner_rewind);
        assert!(inner_rewind < inner_column && inner_column < result);
        assert_eq!(advances, vec![cursors[1], cursors[0]]);
        assert_eq!(empty_targets.len(), 2);
        assert_eq!(predicate_failures.len(), 2);
        for (empty, predicate_failure) in empty_targets.iter().zip(&predicate_failures) {
            assert_ne!(empty, predicate_failure);
        }
    }

    #[test]
    fn planned_inner_join_seek_reads_its_outer_hir_source() {
        for (output, table_required) in [("r.value", false), ("r.extra", true)] {
            let sql = format!(
                "SELECT l.value, {output} \
                 FROM left_items AS l NOT INDEXED \
                 JOIN right_items AS r INDEXED BY right_items_value \
                 ON r.value = l.value"
            );
            let plan = analyze_plan(joined_items_schema(), &sql);
            let query = root_query(&plan.document);
            let block_plan = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0];
            assert_eq!(block_plan.loops.len(), 2);
            let HirSourceAccess::BTree {
                operation:
                    HirBtreeOperation::Seek {
                        index: Some(index), ..
                    },
                table_lookup,
            } = &block_plan.loops[1].access
            else {
                panic!("right join source uses its requested index seek");
            };
            assert_eq!(index.name, "right_items_value");
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned inner seek emits");

            let outer_cursor = match program
                .source_binding(block_plan.loops[0].source)
                .copied()
                .expect("outer source is bound")
            {
                SourceBinding::BTree { scan_cursor, .. } => scan_cursor,
                _ => panic!("outer source is a B-tree"),
            };
            let outer_key = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == outer_cursor)
                })
                .expect("seek reads its outer HIR column");
            let seek = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(
                        insn,
                        Insn::SeekGE { is_index: true, .. }
                            | Insn::SeekGT { is_index: true, .. }
                            | Insn::SeekLE { is_index: true, .. }
                            | Insn::SeekLT { is_index: true, .. }
                    )
                })
                .expect("inner source emits an index seek");
            assert!(outer_key < seek);
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. }))
                    .count(),
                usize::from(table_required)
            );
        }
    }

    #[test]
    fn planned_left_join_builds_and_probes_automatic_index() {
        // `r` follows a NULL-extended loop, so it seeks an automatic index;
        // `m`, after a plain scan, looks its rows up in a hash table.
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS m ON m.value = l.value \
             LEFT JOIN right_items AS r ON r.value = m.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[2].access
        else {
            panic!("right join source uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("automatic-index join emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor,
        } = program
            .source_binding(block_plan.loops[2].source)
            .copied()
            .expect("automatic-index source is bound")
        else {
            panic!("automatic-index source has a B-tree binding");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let open = position(
            &|insn| matches!(insn, Insn::OpenAutoindex { cursor_id } if *cursor_id == scan_cursor),
        );
        let insert = position(
            &|insn| matches!(insn, Insn::IdxInsert { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let filter = position(
            &|insn| matches!(insn, Insn::Filter { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let seek = position(&|insn| {
            matches!(
                insn,
                Insn::SeekGE {
                    cursor_id,
                    is_index: true,
                    ..
                } if *cursor_id == scan_cursor
            )
        });

        assert!(open < insert && insert < filter && filter < seek);
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::FilterAdd { cursor_id, .. } if *cursor_id == scan_cursor)
        ));
        assert!(!program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::OpenRead { cursor_id, .. } if *cursor_id == scan_cursor)
        ));
        assert!(
            table_cursor.is_none(),
            "payload makes the automatic index covering"
        );
    }

    #[test]
    fn planned_inner_join_looks_rows_up_in_a_hash_table() {
        // A plain join of the two tables seeks a temporary index on the
        // small table instead; a filter on the small table makes hashing
        // its remaining rows cheaper.
        let mut schema = automatic_join_schema();
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(100_000);
        let plan = analyze_plan(
            schema,
            "SELECT l.value, r.extra \
             FROM left_items AS l \
             JOIN right_items AS r ON r.value = l.value \
             WHERE l.value > 5",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::Hash(lookup) = &block_plan.loops[1].access else {
            panic!("the inner join's second table uses a hash lookup");
        };
        assert_eq!(lookup.keys.len(), 1);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("hash join emits");

        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let build = position(&|insn| matches!(insn, Insn::HashBuild { .. }));
        let probe = position(&|insn| matches!(insn, Insn::HashProbe { .. }));
        let waiting = position(&|insn| matches!(insn, Insn::HashGraceInit { .. }));
        assert!(build < probe && probe < waiting);
        // The block may run again with the table built once, so it is not
        // closed.
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::HashClose { .. })));
        assert!(matches!(
            program.source_binding(block_plan.loops[1].source),
            Some(SourceBinding::Registers { rowid: Some(_), .. })
        ));
    }

    #[test]
    fn null_matching_automatic_index_does_not_use_bloom_filter() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT r.extra \
             FROM left_items AS l NOT INDEXED \
             CROSS JOIN right_items AS r \
             WHERE r.value IS l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("right join source uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("NULL-matching automatic-index join emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("automatic-index source is bound")
        else {
            panic!("automatic-index source has a B-tree binding");
        };
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::OpenAutoindex { cursor_id } if *cursor_id == scan_cursor)
        ));
        assert!(!program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::FilterAdd { cursor_id, .. } | Insn::Filter { cursor_id, .. }
                if *cursor_id == scan_cursor
        )));
    }

    #[test]
    fn automatic_index_build_lowers_virtual_generated_column_from_base_table() {
        let plan = analyze_plan(
            automatic_generated_join_schema(),
            "SELECT r.extra \
             FROM left_items AS l NOT INDEXED \
             CROSS JOIN right_items AS r \
             WHERE r.generated = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("generated join key uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        assert_eq!(index.columns[0].pos_in_table, 1);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("generated automatic-index join emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("automatic-index source is bound")
        else {
            panic!("automatic-index source has a B-tree binding");
        };
        let add = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Add { .. }))
            .expect("generated expression is lowered while building the index");
        let insert = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(insn, Insn::IdxInsert { cursor_id, .. } if *cursor_id == scan_cursor)
            })
            .expect("automatic index is populated");
        assert!(add < insert);
    }

    #[test]
    fn left_join_checks_on_before_marking_match_and_where_afterward() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r NOT INDEXED ON r.value = l.value \
             WHERE r.extra IS NULL",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let right_loop = &block_plan.loops[1];
        assert_eq!(block_plan.predicates.len(), 2);
        assert_eq!(
            block_plan.predicates[0].from_outer_join,
            Some(right_loop.source)
        );
        assert_eq!(block_plan.predicates[1].from_outer_join, None);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("LEFT JOIN emits");

        let SourceBinding::BTree {
            scan_cursor: right_cursor,
            table_cursor: None,
        } = program
            .source_binding(right_loop.source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("NOT INDEXED right source uses one B-tree cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let reset = position(&|insn| matches!(insn, Insn::Integer { value: 0, .. }));
        let match_register = match program.insns[reset].0 {
            Insn::Integer { dest, .. } => dest,
            _ => unreachable!(),
        };
        let rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == right_cursor),
        );
        let matched = position(
            &|insn| matches!(insn, Insn::Integer { value: 1, dest } if *dest == match_register),
        );
        let failures = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| {
                matches!(
                    insn,
                    Insn::IfNot { .. }
                        | Insn::Ne { .. }
                        | Insn::NotNull { .. }
                        | Insn::Eq { .. }
                        | Insn::Lt { .. }
                        | Insn::Le { .. }
                        | Insn::Gt { .. }
                        | Insn::Ge { .. }
                )
                .then_some(position)
            })
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 2);
        let [on_failure, where_failure] = failures.as_slice() else {
            unreachable!()
        };
        let result = position(&|insn| matches!(insn, Insn::ResultRow { .. }));
        let next = position(
            &|insn| matches!(insn, Insn::Next { cursor_id, .. } if *cursor_id == right_cursor),
        );
        let check =
            position(&|insn| matches!(insn, Insn::IfPos { reg, .. } if *reg == match_register));
        let null_row = position(
            &|insn| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == right_cursor),
        );
        let retry = program
            .insns
            .iter()
            .enumerate()
            .skip(null_row + 1)
            .find_map(|(position, (insn, _))| matches!(insn, Insn::Goto { .. }).then_some(position))
            .expect("NULL row re-enters after ON predicates");

        assert!(reset < rewind && rewind < *on_failure && *on_failure < matched);
        assert!(matched < *where_failure && *where_failure < result && result < next);
        assert!(next < check && check < null_row && null_row < retry);
    }

    #[test]
    fn left_join_constant_on_runs_at_the_right_loop_boundary() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r NOT INDEXED ON 0",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(
            block_plan.predicates[0].from_outer_join,
            Some(block_plan.loops[1].source)
        );
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("constant-ON LEFT JOIN emits");

        let reset = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Integer { value: 0, .. }))
            .expect("match flag is reset");
        let match_register = match program.insns[reset].0 {
            Insn::Integer { dest, .. } => dest,
            _ => unreachable!(),
        };
        let on_failure = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::IfNot { .. }))
            .expect("constant ON predicate is checked");
        let matched = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(insn, Insn::Integer { value: 1, dest } if *dest == match_register)
            })
            .expect("match flag is set after ON");
        assert!(reset < on_failure && on_failure < matched);
    }

    #[test]
    fn left_join_uses_automatic_index_and_null_extends_its_scan_cursor() {
        // `r` follows a NULL-extended loop, so it seeks an automatic index.
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS m ON m.value = l.value \
             LEFT JOIN right_items AS r ON r.value = m.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[2].access
        else {
            panic!("right source uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("automatic-index LEFT JOIN emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[2].source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("right source has a B-tree binding");
        };
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::OpenAutoindex { cursor_id } if *cursor_id == scan_cursor)
        ));
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == scan_cursor)
        ));
    }

    #[test]
    fn left_join_null_extends_index_and_table_cursors() {
        let plan = analyze_plan(
            joined_items_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r INDEXED BY right_items_value \
                 ON r.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("indexed LEFT JOIN emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: Some(table_cursor),
        } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("uncovered output uses index and table cursors");
        };
        for cursor in [scan_cursor, table_cursor] {
            assert!(program.insns.iter().any(
                |(insn, _)| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == cursor)
            ));
        }
    }

    #[test]
    fn unmatched_left_join_row_restarts_downstream_loop() {
        let plan = analyze_plan(
            left_join_with_tail_schema(),
            "SELECT l.value, r.extra, t.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r NOT INDEXED ON r.value = l.value \
             CROSS JOIN tail_items AS t NOT INDEXED \
             WHERE t.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 3);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("LEFT JOIN with downstream loop emits");

        let cursor = |source| match program
            .source_binding(source)
            .copied()
            .expect("source is bound")
        {
            SourceBinding::BTree { scan_cursor, .. } => scan_cursor,
            _ => panic!("source uses a B-tree cursor"),
        };
        let right_cursor = cursor(block_plan.loops[1].source);
        let tail_cursor = cursor(block_plan.loops[2].source);
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let matched = position(&|insn| matches!(insn, Insn::Integer { value: 1, .. }));
        let tail_rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == tail_cursor),
        );
        let tail_next = position(
            &|insn| matches!(insn, Insn::Next { cursor_id, .. } if *cursor_id == tail_cursor),
        );
        let null_row = position(
            &|insn| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == right_cursor),
        );
        let retry = program
            .insns
            .iter()
            .enumerate()
            .skip(null_row + 1)
            .find_map(|(position, (insn, _))| matches!(insn, Insn::Goto { .. }).then_some(position))
            .expect("NULL row re-enters the downstream loop");

        assert!(matched < tail_rewind && tail_rewind < tail_next);
        assert!(tail_next < null_row && null_row < retry);
    }

    #[test]
    fn parenthesized_left_join_marks_and_null_extends_the_whole_group() {
        let plan = analyze_plan(
            left_join_with_tail_schema(),
            "SELECT l.value, r.extra, t.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN (\
                 right_items AS r NOT INDEXED \
                 JOIN tail_items AS t NOT INDEXED ON t.value = r.value\
             ) ON r.value = l.value \
             WHERE r.extra IS NULL",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 3);
        let [group] = block_plan.groups.as_slice() else {
            panic!("one parenthesized FROM group is planned");
        };
        let group_loop_indices = block_plan
            .loops
            .iter()
            .enumerate()
            .filter_map(|(index, planned)| {
                group
                    .source_range
                    .contains(&planned.source_position)
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(group_loop_indices.len(), 2);
        assert_eq!(group_loop_indices[1], group_loop_indices[0] + 1);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("parenthesized LEFT JOIN emits");

        let group_cursors = group_loop_indices
            .iter()
            .map(|&index| {
                let source = block_plan.loops[index].source;
                let SourceBinding::BTree { scan_cursor, .. } = program
                    .source_binding(source)
                    .copied()
                    .expect("group source is bound")
                else {
                    panic!("group source uses a B-tree cursor");
                };
                scan_cursor
            })
            .collect::<Vec<_>>();
        let reset = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Integer { value: 0, .. }))
            .expect("group match flag is reset");
        let match_register = match program.insns[reset].0 {
            Insn::Integer { dest, .. } => dest,
            _ => unreachable!(),
        };
        let matched = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(insn, Insn::Integer { value: 1, dest } if *dest == match_register)
            })
            .expect("group match flag is set");
        let failures = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| {
                matches!(
                    insn,
                    Insn::IfNot { .. }
                        | Insn::Ne { .. }
                        | Insn::NotNull { .. }
                        | Insn::Eq { .. }
                        | Insn::Lt { .. }
                        | Insn::Le { .. }
                        | Insn::Gt { .. }
                        | Insn::Ge { .. }
                )
                .then_some(position)
            })
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 3);
        assert!(failures[..2].iter().all(|failure| *failure < matched));
        assert!(matched < failures[2]);

        let last_group_next = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| match insn {
                Insn::Next { cursor_id, .. } if group_cursors.contains(cursor_id) => Some(position),
                _ => None,
            })
            .max()
            .expect("both group loops advance");
        let null_rows = group_cursors
            .iter()
            .map(|cursor| {
                program
                    .insns
                    .iter()
                    .position(|(insn, _)| {
                        matches!(insn, Insn::NullRow { cursor_id } if cursor_id == cursor)
                    })
                    .expect("each group cursor is null-extended")
            })
            .collect::<Vec<_>>();
        assert!(null_rows.iter().all(|null_row| last_group_next < *null_row));
    }

    #[test]
    fn planned_index_scan_opens_table_only_for_uncovered_columns() {
        for (sql, table_required) in [
            ("SELECT value FROM items INDEXED BY items_value", false),
            ("SELECT extra FROM items INDEXED BY items_value", true),
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            let HirSourceAccess::BTree {
                operation:
                    HirBtreeOperation::Scan {
                        index: Some(index), ..
                    },
                table_lookup,
            } = &planned_loop.access
            else {
                let HirSourceAccess::BTree { operation, .. } = &planned_loop.access else {
                    panic!("INDEXED BY selects B-tree access for {sql}");
                };
                panic!("INDEXED BY selects an index scan for {sql}: {operation:?}");
            };
            assert_eq!(index.name, "items_value");
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let source = planned_loop.source;
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned index scan emits");

            let SourceBinding::BTree {
                scan_cursor,
                table_cursor,
            } = program
                .source_binding(source)
                .copied()
                .expect("source has a physical binding")
            else {
                panic!("index scan binds B-tree cursors");
            };
            assert!(matches!(
                program.get_cursor_type(scan_cursor),
                Some(CursorType::BTreeIndex(index)) if index.name == "items_value"
            ));
            assert_eq!(table_cursor.is_some(), table_required);
            assert!(program.insns.iter().any(|(insn, _)| {
                matches!(
                    insn,
                    Insn::OpenRead {
                        cursor_id,
                        root_page: 3,
                        db: MAIN_DB_ID,
                    } if *cursor_id == scan_cursor
                )
            }));

            match table_cursor {
                Some(table_cursor) => {
                    assert!(program.insns.iter().any(|(insn, _)| {
                        matches!(
                            insn,
                            Insn::DeferredSeek {
                                index_cursor_id,
                                table_cursor_id,
                            } if *index_cursor_id == scan_cursor
                                && *table_cursor_id == table_cursor
                        )
                    }));
                    assert!(program.insns.iter().any(|(insn, _)| {
                        matches!(
                            insn,
                            Insn::Column {
                                cursor_id,
                                column: 1,
                                ..
                            } if *cursor_id == table_cursor
                        )
                    }));
                }
                None => {
                    assert!(!program
                        .insns
                        .iter()
                        .any(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. })));
                    assert!(program.insns.iter().any(|(insn, _)| {
                        matches!(
                            insn,
                            Insn::Column {
                                cursor_id,
                                column: 0,
                                ..
                            } if *cursor_id == scan_cursor
                        )
                    }));
                }
            }
        }
    }

    #[test]
    fn not_indexed_keeps_the_hir_plan_on_the_table() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value FROM items NOT INDEXED",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];

        assert!(matches!(
            &planned_loop.access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::Scan { index: None, .. },
                table_lookup: BtreeTableLookup::ScanOnly,
            }
        ));
    }

    #[test]
    fn planned_rowid_equality_emits_one_point_lookup() {
        for sql in [
            "SELECT value FROM items WHERE rowid = 2",
            "SELECT value FROM items WHERE id = 2",
        ] {
            let plan = analyze_plan(rowid_items_schema(), sql);
            let query = root_query(&plan.document);
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            assert!(matches!(
                &planned_loop.access,
                HirSourceAccess::BTree {
                    operation: HirBtreeOperation::RowidEq { .. },
                    table_lookup: BtreeTableLookup::ScanOnly,
                }
            ));
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned rowid equality emits");

            let SourceBinding::BTree {
                scan_cursor,
                table_cursor: None,
            } = program
                .source_binding(planned_loop.source)
                .copied()
                .expect("source has a physical binding")
            else {
                panic!("rowid equality binds one table cursor");
            };
            assert!(matches!(
                program.get_cursor_type(scan_cursor),
                Some(CursorType::BTreeTable(table)) if table.name == "items"
            ));
            assert!(program.insns.iter().any(|(insn, _)| {
                matches!(
                    insn,
                    Insn::SeekRowid { cursor_id, .. } if *cursor_id == scan_cursor
                )
            }));
            assert!(!program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::Next { .. } | Insn::Prev { .. })));
        }
    }

    #[test]
    fn planned_hir_ranges_use_the_shared_seek_emitter() {
        for (sql, table_required) in [
            (
                "SELECT value FROM items INDEXED BY items_value WHERE value >= 2 AND value < 5",
                false,
            ),
            (
                "SELECT extra FROM items INDEXED BY items_value WHERE value >= 2 AND value < 5",
                true,
            ),
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            let HirSourceAccess::BTree {
                operation:
                    HirBtreeOperation::Seek {
                        index: Some(index), ..
                    },
                table_lookup,
            } = &planned_loop.access
            else {
                panic!("indexed range selects an index seek: {sql}");
            };
            assert_eq!(index.name, "items_value");
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned index range emits");

            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::SeekGE { is_index: true, .. }
                    | Insn::SeekGT { is_index: true, .. }
                    | Insn::SeekLE { is_index: true, .. }
                    | Insn::SeekLT { is_index: true, .. }
            )));
            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::IdxGE { .. } | Insn::IdxGT { .. } | Insn::IdxLE { .. } | Insn::IdxLT { .. }
            )));
            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::Next {
                    fullscan: false,
                    ..
                } | Insn::Prev {
                    fullscan: false,
                    ..
                }
            )));
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. }))
                    .count(),
                usize::from(table_required)
            );
        }

        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT value FROM items WHERE rowid >= 2 AND rowid < 5",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        assert!(matches!(
            &planned_loop.access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::Seek { index: None, .. },
                table_lookup: BtreeTableLookup::ScanOnly,
            }
        ));
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned rowid range emits");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::SeekGE {
                is_index: false,
                ..
            } | Insn::SeekGT {
                is_index: false,
                ..
            } | Insn::SeekLE {
                is_index: false,
                ..
            } | Insn::SeekLT {
                is_index: false,
                ..
            }
        )));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::RowId { .. })));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Ge { .. } | Insn::Gt { .. } | Insn::Le { .. } | Insn::Lt { .. }
        )));
    }

    #[test]
    fn planned_hir_in_lists_use_the_shared_two_level_loop() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT value FROM items WHERE rowid IN (1, 2, 2)",
        );
        let query = root_query(&plan.document);
        assert!(matches!(
            &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0]
                .access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::InSeek {
                    index: None,
                    source: HirInSeekSource::Values { values, .. },
                },
                ..
            } if values.len() == 3
        ));
        let mut rowid_program = program();

        emit_planned_query_body(
            &mut rowid_program,
            &plan,
            query,
            &QueryDestination::ResultRows,
        )
        .expect("planned rowid IN seek emits");

        assert_eq!(
            rowid_program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IdxInsert { .. }))
                .count(),
            3
        );
        assert!(rowid_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SeekRowid { .. })));
        assert!(!rowid_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IdxGT { .. })));

        for (sql, table_required) in [
            (
                "SELECT value FROM items INDEXED BY items_value WHERE value IN (1, 2, 2)",
                false,
            ),
            (
                "SELECT extra FROM items INDEXED BY items_value WHERE value IN (1, 2, 2)",
                true,
            ),
        ] {
            let mut plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let values = match &plan
                .document
                .query(query)
                .expect("root query exists")
                .blocks[0]
                .body
            {
                hir::QueryBlockBody::Select {
                    filter: Some(Expr::InList { values, .. }),
                    ..
                } => values.clone(),
                _ => panic!("indexed test has an IN-list filter"),
            };
            let planned_query = plan
                .queries
                .iter_mut()
                .find(|planned| planned.query == query)
                .expect("root query is planned");
            let HirSourceAccess::BTree { operation, .. } =
                &mut planned_query.blocks[0].loops[0].access
            else {
                panic!("indexed IN uses a B-tree plan");
            };
            let HirBtreeOperation::Scan {
                index: Some(index), ..
            } = operation
            else {
                panic!("cost model starts from the requested index");
            };
            *operation = HirBtreeOperation::InSeek {
                index: Some(index.clone()),
                source: HirInSeekSource::Values {
                    values,
                    affinity: Affinity::Integer,
                },
            };
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            let HirSourceAccess::BTree {
                operation,
                table_lookup,
            } = &planned_loop.access
            else {
                panic!("indexed IN uses a B-tree plan");
            };
            assert!(
                matches!(
                    operation,
                    HirBtreeOperation::InSeek {
                        index: Some(index),
                        source: HirInSeekSource::Values { .. },
                    } if index.name == "items_value"
                ),
                "unexpected indexed IN operation for {sql}: {operation:?}"
            );
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned indexed IN seek emits");

            assert!(program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::SeekGE { is_index: true, .. })));
            assert!(program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::IdxGT { .. })));
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. }))
                    .count(),
                usize::from(table_required)
            );
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(
                        insn,
                        Insn::Next {
                            fullscan: false,
                            ..
                        }
                    ))
                    .count(),
                2
            );
        }
    }

    #[test]
    fn planned_hir_in_subquery_reuses_its_prepared_index() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT value FROM items WHERE rowid IN (VALUES (1), (2))",
        );
        let query = root_query(&plan.document);
        let subquery = root_filter_subquery(&plan.document);
        let SubqueryExpr::In {
            query: source_query,
            ..
        } = subquery
        else {
            panic!("root filter is an IN subquery");
        };
        assert!(matches!(
            &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0]
                .access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::InSeek {
                    source: HirInSeekSource::Query(SubqueryExpr::In { query, .. }),
                    ..
                },
                ..
            } if query == source_query
        ));
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &plan.document, subquery)
            .expect("IN subquery destination prepares");
        let Some(SubqueryBinding::InIndex {
            cursor: source_cursor,
        }) = program.subquery_binding(*source_query)
        else {
            panic!("IN subquery has an index binding");
        };
        emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
            emit_planned_query_body(program, &plan, query, destination)
        })
        .expect("IN subquery emits");

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned query IN seek emits");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Rewind { cursor_id, .. } if *cursor_id == source_cursor
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Column {
                cursor_id,
                column: 0,
                ..
            } if *cursor_id == source_cursor
        )));
    }

    #[test]
    fn shared_hir_seek_emitter_keeps_backward_bounds() {
        use crate::{
            translate::plan::{SeekDef, SeekKey, SeekKeyComponent},
            types::SeekOp,
        };

        let document = analyze_sql(rowid_items_schema(), "SELECT value FROM items");
        let source = &document.sources[0];
        let seek_def = SeekDef {
            prefix: Vec::new(),
            start: SeekKey {
                last_component: SeekKeyComponent::Expr(Expr::Literal(Literal::Numeric(
                    "5".to_string(),
                ))),
                op: SeekOp::LE { eq_only: false },
                affinity: Affinity::Integer,
            },
            end: SeekKey {
                last_component: SeekKeyComponent::Expr(Expr::Literal(Literal::Numeric(
                    "2".to_string(),
                ))),
                op: SeekOp::LE { eq_only: false },
                affinity: Affinity::Integer,
            },
            iter_dir: IterationDirection::Backwards,
        };
        let mut program = program();
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(match &source.kind {
            hir::SourceKind::Table(table) => match table.value() {
                Table::BTree(table) => table.clone(),
                _ => panic!("test source is a B-tree table"),
            },
            _ => panic!("test source is a table"),
        }));
        let start_register = program.alloc_register();
        let loop_start = program.allocate_label();
        let done = program.allocate_label();

        SeekEmitter::with_lowering(
            &mut program,
            &seek_def,
            HirSeekExpressionLowering {
                document: &document,
                source,
            },
            cursor,
            start_register,
            done,
            None,
        )
        .emit(loop_start, false)
        .expect("backward HIR seek emits");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::SeekLE {
                is_index: false,
                cursor_id,
                ..
            } if *cursor_id == cursor
        )));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Le { .. })));
    }

    fn expression_index_schema() -> Schema {
        let mut schema = Schema::new();
        let table = Arc::new(
            BTreeTable::from_sql(
                "CREATE TABLE items(a INTEGER, b TEXT, c INTEGER, d INTEGER)",
                2,
            )
            .expect("items table schema parses"),
        );
        schema
            .add_btree_table(table.clone())
            .expect("items table name is unique");
        schema
            .add_index(Arc::new(
                Index::from_sql(
                    &SymbolTable::new(),
                    "CREATE INDEX items_idx ON items(d, a, lower(b), c)",
                    3,
                    &table,
                )
                .expect("expression index parses"),
            ))
            .expect("items index name is unique");
        schema
    }

    #[test]
    fn grouped_aggregates_read_the_stored_index_expression_once() {
        let plan = analyze_plan(
            expression_index_schema(),
            "SELECT a, \
                    sum(CASE WHEN lower(b) = 'x' THEN 1 END), \
                    sum(CASE WHEN lower(b) = 'x' THEN c END) \
             FROM items WHERE d BETWEEN 0 AND 10 GROUP BY a",
        );
        let query = root_query(&plan.document);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("grouped expression-index query emits");

        let index_cursor = program
            .cursor_ref
            .iter()
            .position(|(_, cursor)| {
                matches!(cursor, CursorType::BTreeIndex(index) if index.name == "items_idx")
            })
            .expect("the query reads the expression index");
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Function { .. })));
        let stored_expression_reads = program
            .insns
            .iter()
            .filter(|(insn, _)| {
                matches!(
                    insn,
                    Insn::Column {
                        cursor_id,
                        column: 2,
                        ..
                    } if *cursor_id == index_cursor
                )
            })
            .count();
        assert_eq!(stored_expression_reads, 1);
    }

    #[test]
    fn left_join_null_row_computes_the_indexed_expression_from_its_columns() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        let right = Arc::new(
            BTreeTable::from_sql("CREATE TABLE right_items(k INTEGER, v INTEGER)", 3)
                .expect("right table schema parses"),
        );
        schema
            .add_btree_table(right.clone())
            .expect("right table name is unique");
        schema
            .add_index(Arc::new(
                Index::from_sql(
                    &SymbolTable::new(),
                    "CREATE INDEX right_items_idx ON right_items(k, v, coalesce(v, -1))",
                    4,
                    &right,
                )
                .expect("expression index parses"),
            ))
            .expect("right index name is unique");
        let plan = analyze_plan(
            schema,
            "SELECT l.value, coalesce(r.v, -1) \
             FROM left_items AS l \
             LEFT JOIN right_items AS r INDEXED BY right_items_idx ON r.k = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("LEFT JOIN over an expression index emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("right source uses a B-tree cursor");
        };
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == scan_cursor)
        ));
        assert!(!program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Column {
                cursor_id,
                column: 2,
                ..
            } if *cursor_id == scan_cursor
        )));
    }
}
