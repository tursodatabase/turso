use crate::{
    alloc::{TursoFromIterator, TursoIteratorExt},
    emit_explain,
    schema::{BTreeCharacteristics, BTreeTable, Index, Table},
    sync::Arc,
    translate::{
        aggregation::emit_ungrouped_aggregation,
        collate::{get_collseq_from_expr_with_symbols, CollationSeq},
        compiler::{
            bind_cursor_input, constant, cursor_input, cursor_values, declare_ephemeral_index,
            initialize_cursor_once, insert_index_pack, literal_values,
            open_declared_ephemeral_index, open_ephemeral_index, open_index, open_table,
            pack_values, pure, result_row_pack, scan_index, scan_table, seek_index, seek_rowid,
            seek_table_range, select_pack, BoxedCompile, Compile, CompileRegion, CursorId,
            CursorInputId, DeferredInValues, DeferredIndexBound, DeferredIndexRange,
            DeferredTableBound, DeferredTableRange, InputProducer, InputRequirement,
            InputRequirements, InputSlot, OpenedTable, PhysicalInputBinding, Row, RowStream,
            ScanDirection, SortKey, SortedRow, ValueId, ValuePack,
        },
        emitter::{
            build_rowid_column, init_exists_result_regs, init_limit, Column, CursorID, CursorType,
            MaterializedBuildInput, MaterializedBuildInputMode, MaterializedColumnRef,
            OperationMode, ResultSetColumn, TableMask, TranslateCtx,
        },
        expr::{
            compile_symbolic_conjunction, compile_symbolic_expr, compile_symbolic_exprs,
            compile_symbolic_static_expr, ResolvedScalarExpr, RowExprResolver, RowLayout,
            ScalarInputKind, ScalarInputSource, SymbolicRows,
        },
        group_by::{group_by_agg_phase, group_by_emit_row_phase, EmitGroupBy, GroupByRowSource},
        main_loop::{init_distinct, CloseLoop, InitLoop, LoopBodyEmitter, OpenLoop},
        order_by::{custom_type_comparator, EmitOrderBy},
        plan::{
            BitSet, Distinctness, EphemeralRowidMode, EvalAt, InSeekSource, IndexMethodQuery,
            IterationDirection, JoinOrderMember, JoinType, Operation, Plan, QueryDestination, Scan,
            Search, SeekDef, SeekKey, SeekKeyComponent, SelectPlan, SimpleAggregate,
            SubqueryEvalPhase, SubqueryState, TableReferences,
        },
        planner::table_mask_from_expr,
        select::emit_simple_count,
        subquery::{emit_from_clause_subqueries, emit_non_from_clause_subqueries_for_eval_at},
        values::emit_values,
        window::{emit_window_flush, EmitWindow},
        ProgramBuilder, Resolver,
    },
    types::{SeekOp, Value},
    vdbe::{affinity::Affinity, builder::QueryMode, insn::Insn},
    HashMap, HashSet, LimboError, Result,
};
use smallvec::SmallVec;
use tracing::{instrument, Level};
use turso_macros::turso_assert;
use turso_parser::ast::{Expr, SortOrder, SubqueryType, TableInternalId};

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program_for_select(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: SelectPlan,
) -> Result<()> {
    emit_program_for_select_with_resolver(program, resolver.fork(), plan)
}

pub fn emit_program_for_select_with_resolver(
    program: &mut ProgramBuilder,
    resolver: Resolver,
    mut plan: SelectPlan,
) -> Result<()> {
    let declarative_outcome = program.with_scoped_result_cols_start(|program| {
        try_emit_declarative_table_scan(program, &resolver, &mut plan)
    })?;
    if let Some(outcome) = declarative_outcome {
        program.result_columns = plan.result_columns;
        program.table_references.extend(plan.table_references);
        if let DeclarativeSelectOutcome::ResultRows { result_cols_start } = outcome {
            program.reg_result_cols_start = Some(result_cols_start);
        }
        return Ok(());
    }

    let materialized_build_inputs = emit_materialized_build_inputs(program, &resolver, &mut plan)?;
    emit_program_for_select_with_inputs(program, &resolver, plan, materialized_build_inputs)
}

struct ResolvedIndexBound {
    suffix: ResolvedIndexSuffix,
    op: SeekOp,
}

enum ResolvedIndexSuffix {
    None,
    Null,
    Expression {
        expression: ResolvedScalarExpr,
        affinity: Affinity,
    },
}

impl ResolvedIndexBound {
    fn into_deferred<F>(self, compile: &mut F) -> Option<DeferredIndexBound>
    where
        F: FnMut(&ResolvedScalarExpr) -> Option<BoxedCompile<ValueId>>,
    {
        Some(match self.suffix {
            ResolvedIndexSuffix::None => DeferredIndexBound::prefix(self.op),
            ResolvedIndexSuffix::Null => DeferredIndexBound::null(self.op),
            ResolvedIndexSuffix::Expression {
                expression,
                affinity,
            } => DeferredIndexBound::expression(compile(&expression)?, affinity, self.op),
        })
    }
}

struct ResolvedIndexRange {
    prefix_values: SmallVec<[ResolvedScalarExpr; 4]>,
    prefix_affinities: SmallVec<[Affinity; 4]>,
    start: ResolvedIndexBound,
    end: ResolvedIndexBound,
    direction: ScanDirection,
}

impl ResolvedIndexRange {
    fn into_deferred<F>(self, mut compile: F) -> Option<DeferredIndexRange>
    where
        F: FnMut(&ResolvedScalarExpr) -> Option<BoxedCompile<ValueId>>,
    {
        let mut prefix_values = SmallVec::with_capacity(self.prefix_values.len());
        for expression in self.prefix_values {
            prefix_values.push(compile(&expression)?);
        }
        let start = self.start.into_deferred(&mut compile)?;
        let end = self.end.into_deferred(&mut compile)?;
        Some(DeferredIndexRange::new(
            prefix_values,
            self.prefix_affinities,
            start,
            end,
            self.direction,
        ))
    }

    fn into_static_deferred(self) -> Option<DeferredIndexRange> {
        self.into_deferred(compile_symbolic_static_expr)
    }

    fn into_row_deferred(self, rows: &SymbolicRows) -> DeferredIndexRange {
        self.into_deferred(|expression| Some(compile_symbolic_expr(rows, expression)))
            .expect("row-backed resolved index expressions must compile")
    }
}

fn resolve_index_bound(
    key: &SeekKey,
    expr_resolver: &mut RowExprResolver<'_, '_>,
) -> Result<Option<ResolvedIndexBound>> {
    Ok(Some(match &key.last_component {
        SeekKeyComponent::None => ResolvedIndexBound {
            suffix: ResolvedIndexSuffix::None,
            op: key.op,
        },
        SeekKeyComponent::Null => ResolvedIndexBound {
            suffix: ResolvedIndexSuffix::Null,
            op: key.op,
        },
        SeekKeyComponent::Expr(expression) => {
            let Some(resolved) = expr_resolver.resolve(expression)? else {
                return Ok(None);
            };
            let affinity = if key.affinity.expr_needs_no_affinity_change(expression) {
                crate::vdbe::affinity::Affinity::Blob
            } else {
                key.affinity
            };
            ResolvedIndexBound {
                suffix: ResolvedIndexSuffix::Expression {
                    expression: resolved,
                    affinity,
                },
                op: key.op,
            }
        }
    }))
}

fn resolve_index_range(
    direction: ScanDirection,
    index: &Index,
    seek_def: &SeekDef,
    expr_resolver: &mut RowExprResolver<'_, '_>,
) -> Result<Option<ResolvedIndexRange>> {
    if !seek_def
        .prefix
        .iter()
        .all(|constraint| constraint.eq.is_some())
    {
        return Ok(None);
    }

    let mut prefix_values = SmallVec::with_capacity(seek_def.prefix.len());
    let mut prefix_affinities = SmallVec::with_capacity(seek_def.prefix.len());
    for constraint in &seek_def.prefix {
        let (_, expression, affinity) = constraint
            .eq
            .as_ref()
            .expect("exact index seek prefix must contain only equalities");
        let Some(resolved) = expr_resolver.resolve(expression)? else {
            return Ok(None);
        };
        prefix_values.push(resolved);
        prefix_affinities.push(if affinity.expr_needs_no_affinity_change(expression) {
            Affinity::Blob
        } else {
            *affinity
        });
    }
    let Some(mut start) = resolve_index_bound(&seek_def.start, expr_resolver)? else {
        return Ok(None);
    };
    let Some(mut end) = resolve_index_bound(&seek_def.end, expr_resolver)? else {
        return Ok(None);
    };

    // An unconstrained physical edge can include NULL keys that SQL range
    // predicates must skip. Resolve that SQL policy before handing the range
    // to the SQL-agnostic compiler IR.
    let first_index_order = index
        .columns
        .first()
        .expect("planner index range must use a non-empty index")
        .order;
    if seek_def.prefix.is_empty() && matches!(seek_def.start.last_component, SeekKeyComponent::None)
    {
        start = match (seek_def.iter_dir, first_index_order) {
            (IterationDirection::Forwards, SortOrder::Asc) => ResolvedIndexBound {
                suffix: ResolvedIndexSuffix::Null,
                op: SeekOp::GT,
            },
            (IterationDirection::Backwards, SortOrder::Desc) => ResolvedIndexBound {
                suffix: ResolvedIndexSuffix::Null,
                op: SeekOp::LT,
            },
            _ => start,
        };
    }
    if seek_def.prefix.is_empty() && matches!(seek_def.end.last_component, SeekKeyComponent::None) {
        end = match (seek_def.iter_dir, first_index_order) {
            (IterationDirection::Forwards, SortOrder::Desc) => ResolvedIndexBound {
                suffix: ResolvedIndexSuffix::Null,
                op: SeekOp::GE { eq_only: false },
            },
            (IterationDirection::Backwards, SortOrder::Asc) => ResolvedIndexBound {
                suffix: ResolvedIndexSuffix::Null,
                op: SeekOp::LE { eq_only: false },
            },
            _ => end,
        };
    }

    Ok(Some(ResolvedIndexRange {
        prefix_values,
        prefix_affinities,
        start,
        end,
        direction,
    }))
}

struct ResolvedTableBound {
    rowid: Option<ResolvedScalarExpr>,
    op: SeekOp,
}

impl ResolvedTableBound {
    fn into_deferred<F>(self, compile: &mut F) -> Option<DeferredTableBound>
    where
        F: FnMut(&ResolvedScalarExpr) -> Option<BoxedCompile<ValueId>>,
    {
        Some(match self.rowid {
            Some(rowid) => DeferredTableBound::expression(compile(&rowid)?, self.op),
            None => DeferredTableBound::unbounded(self.op),
        })
    }
}

struct ResolvedTableRange {
    start: ResolvedTableBound,
    end: ResolvedTableBound,
    direction: ScanDirection,
    affinity: Affinity,
}

impl ResolvedTableRange {
    fn into_deferred<F>(self, mut compile: F) -> Option<DeferredTableRange>
    where
        F: FnMut(&ResolvedScalarExpr) -> Option<BoxedCompile<ValueId>>,
    {
        let start = self.start.into_deferred(&mut compile)?;
        let end = self.end.into_deferred(&mut compile)?;
        Some(DeferredTableRange::new(
            start,
            end,
            self.direction,
            self.affinity,
        ))
    }

    fn into_static_deferred(self) -> Option<DeferredTableRange> {
        self.into_deferred(compile_symbolic_static_expr)
    }

    fn into_row_deferred(self, rows: &SymbolicRows) -> DeferredTableRange {
        self.into_deferred(|expression| Some(compile_symbolic_expr(rows, expression)))
            .expect("row-backed resolved table range expressions must compile")
    }
}

fn resolve_table_bound(
    key: &SeekKey,
    expr_resolver: &mut RowExprResolver<'_, '_>,
) -> Result<Option<ResolvedTableBound>> {
    Ok(Some(match &key.last_component {
        SeekKeyComponent::None => ResolvedTableBound {
            rowid: None,
            op: key.op,
        },
        // A rowid range has no SQL NULL sentinel. If the planner ever
        // constructs one, retain the eager path until its meaning is explicit.
        SeekKeyComponent::Null => return Ok(None),
        SeekKeyComponent::Expr(expression) => {
            let Some(resolved) = expr_resolver.resolve(expression)? else {
                return Ok(None);
            };
            ResolvedTableBound {
                rowid: Some(resolved),
                op: key.op,
            }
        }
    }))
}

fn resolve_table_range(
    direction: ScanDirection,
    table: &BTreeTable,
    seek_def: &SeekDef,
    expr_resolver: &mut RowExprResolver<'_, '_>,
) -> Result<Option<ResolvedTableRange>> {
    if !seek_def.prefix.is_empty() {
        return Ok(None);
    }
    let Some(start) = resolve_table_bound(&seek_def.start, expr_resolver)? else {
        return Ok(None);
    };
    let Some(end) = resolve_table_bound(&seek_def.end, expr_resolver)? else {
        return Ok(None);
    };
    let affinity = table
        .columns()
        .iter()
        .find(|column| column.is_rowid_alias())
        .map(|column| column.affinity())
        .unwrap_or(Affinity::Numeric);
    Ok(Some(ResolvedTableRange {
        start,
        end,
        direction,
        affinity,
    }))
}

#[derive(Clone, Copy)]
enum DeclarativeBtreeAccess<'a> {
    Scan {
        direction: ScanDirection,
        index: Option<&'a Arc<Index>>,
    },
    RowidEq(&'a Expr),
    TableRange {
        direction: ScanDirection,
        seek_def: &'a SeekDef,
    },
    IndexRange {
        direction: ScanDirection,
        index: &'a Arc<Index>,
        seek_def: &'a SeekDef,
    },
    InValues {
        index: Option<&'a Arc<Index>>,
        source: DeclarativeInSource<'a>,
    },
}

#[derive(Clone, Copy)]
enum DeclarativeInnerJoinAccess<'a> {
    Scan(ScanDirection),
    Rowid(&'a Expr),
    TableRange {
        direction: ScanDirection,
        seek_def: &'a SeekDef,
    },
    IndexRange {
        direction: ScanDirection,
        index: &'a Arc<Index>,
        seek_def: &'a SeekDef,
    },
    InValues {
        index: Option<&'a Arc<Index>>,
        source: DeclarativeInSource<'a>,
    },
}

#[derive(Clone, Copy)]
enum DeclarativeInSource<'a> {
    Literal {
        values: &'a [Expr],
        affinity: Affinity,
    },
    Subquery {
        cursor_id: CursorID,
    },
}

enum ResolvedInnerInValues {
    Literal {
        values: SmallVec<[ResolvedScalarExpr; 4]>,
        affinity: Affinity,
        collation: Option<CollationSeq>,
    },
    Cursor {
        input: CursorInputId,
        collation: Option<CollationSeq>,
    },
}

impl ResolvedInnerInValues {
    fn into_deferred(self, outer: &SymbolicRows) -> DeferredInValues {
        match self {
            Self::Literal {
                values,
                affinity,
                collation,
            } => {
                let mut compilers = SmallVec::with_capacity(values.len());
                for value in &values {
                    compilers.push(compile_symbolic_expr(outer, value));
                }
                literal_values(compilers, affinity, collation)
            }
            Self::Cursor { input, collation } => cursor_values(input, collation),
        }
    }
}

enum DeclarativeSelectOutcome {
    ResultRows { result_cols_start: usize },
    Consumed,
}

struct DeclarativeSelectProgram {
    compiler: DeclarativeSelectCompiler,
    destination_index: Option<Arc<Index>>,
    result_column_count: usize,
}

enum DeclarativeSelectCompiler {
    Effect(CompileRegion<(), DeclarativeInputSource>),
    Scalar(CompileRegion<ValueId, DeclarativeInputSource>),
}

impl DeclarativeSelectCompiler {
    fn inputs(&self) -> &[InputRequirement<DeclarativeInputSource>] {
        match self {
            Self::Effect(region) => region.inputs(),
            Self::Scalar(region) => region.inputs(),
        }
    }

    fn bind_inputs(self, producers: Vec<InputProducer>) -> Result<Self> {
        match self {
            Self::Effect(region) => Ok(Self::Effect(region.bind_inputs(producers)?)),
            Self::Scalar(region) => Ok(Self::Scalar(region.bind_inputs(producers)?)),
        }
    }
}

#[derive(Clone, Copy)]
struct DeclarativeCursorBinding {
    input: CursorInputId,
    cursor: CursorID,
}

#[derive(Clone, Copy)]
struct DeclarativeInCursor {
    binding: DeclarativeCursorBinding,
    subquery_id: TableInternalId,
}

fn resolve_declarative_in_cursor(
    plan: &SelectPlan,
    cursor_id: CursorID,
    input: CursorInputId,
) -> Result<Option<DeclarativeInCursor>> {
    let mut matching = plan.non_from_clause_subqueries.iter().filter(|subquery| {
        matches!(
            &subquery.query_type,
            SubqueryType::In {
                cursor_id: subquery_cursor,
                ..
            } if *subquery_cursor == cursor_id
        )
    });
    let Some(subquery) = matching.next() else {
        return Ok(None);
    };
    if matching.next().is_some() {
        return Err(LimboError::InternalError(format!(
            "multiple IN subqueries use cursor {cursor_id}"
        )));
    }
    if subquery.correlated
        || subquery.eval_phase != SubqueryEvalPhase::BeforeLoop
        || !matches!(
            &subquery.state,
            SubqueryState::Unevaluated { plan: Some(_) }
        )
        || subquery.get_eval_at(&plan.join_order, Some(&plan.table_references))?
            != EvalAt::BeforeLoop
    {
        return Ok(None);
    }
    Ok(Some(DeclarativeInCursor {
        binding: DeclarativeCursorBinding {
            input,
            cursor: cursor_id,
        },
        subquery_id: subquery.internal_id,
    }))
}

#[derive(Clone, Copy)]
enum DeclarativeInputSource {
    DestinationCursor {
        cursor: CursorID,
    },
    Subquery {
        id: TableInternalId,
        kind: DeclarativeSubqueryInputKind,
    },
}

#[derive(Clone, Copy)]
enum DeclarativeSubqueryInputKind {
    InCursor { cursor: CursorID },
    Scalar,
}

impl DeclarativeInputSource {
    const fn subquery_id(self) -> Option<TableInternalId> {
        match self {
            Self::DestinationCursor { .. } => None,
            Self::Subquery { id, .. } => Some(id),
        }
    }
}

struct StagedDeclarativeProducer {
    subquery_id: TableInternalId,
    subquery_index: usize,
    producer: InputProducer,
    table_references: TableReferences,
}

fn validate_and_order_declarative_dependencies(
    plan: &SelectPlan,
    external_in_cursor: Option<DeclarativeInCursor>,
    scalar_inputs: InputRequirements<ScalarInputSource>,
) -> Result<Option<SmallVec<[InputRequirement<DeclarativeInputSource>; 2]>>> {
    let mut dependencies = SmallVec::with_capacity(
        scalar_inputs.inputs().len() + usize::from(external_in_cursor.is_some()),
    );
    let mut matched_scalars = 0;
    let mut matched_in_cursor = false;

    for subquery in &plan.non_from_clause_subqueries {
        let in_cursor =
            external_in_cursor.filter(|dependency| dependency.subquery_id == subquery.internal_id);
        let scalar = scalar_inputs
            .inputs()
            .iter()
            .find(|dependency| dependency.source().subquery_id == subquery.internal_id)
            .copied();
        let dependency = match (in_cursor, scalar) {
            (Some(_), Some(_)) => {
                return Err(LimboError::InternalError(format!(
                    "subquery {:?} is both a scalar and cursor dependency",
                    subquery.internal_id,
                )));
            }
            (Some(dependency), None) => {
                matched_in_cursor = true;
                InputRequirement::cursor(
                    dependency.binding.input,
                    DeclarativeInputSource::Subquery {
                        id: dependency.subquery_id,
                        kind: DeclarativeSubqueryInputKind::InCursor {
                            cursor: dependency.binding.cursor,
                        },
                    },
                )
            }
            (None, Some(dependency)) => {
                let source = *dependency.source();
                let matching_kind = matches!(
                    (&subquery.query_type, source.kind),
                    (SubqueryType::Exists { .. }, ScalarInputKind::Exists)
                        | (
                            SubqueryType::RowValue { num_regs: 1, .. },
                            ScalarInputKind::RowValue
                        )
                );
                if !matching_kind {
                    return Err(LimboError::InternalError(format!(
                        "subquery {:?} does not match its symbolic scalar dependency",
                        subquery.internal_id,
                    )));
                }
                matched_scalars += 1;
                let InputSlot::Value(input) = dependency.slot() else {
                    unreachable!("scalar expression requirements only contain value inputs");
                };
                InputRequirement::value(
                    input,
                    DeclarativeInputSource::Subquery {
                        id: source.subquery_id,
                        kind: DeclarativeSubqueryInputKind::Scalar,
                    },
                )
            }
            (None, None) => return Ok(None),
        };
        if subquery.correlated
            || subquery.eval_phase != SubqueryEvalPhase::BeforeLoop
            || !matches!(
                &subquery.state,
                SubqueryState::Unevaluated { plan: Some(_) }
            )
            || subquery.get_eval_at(&plan.join_order, Some(&plan.table_references))?
                != EvalAt::BeforeLoop
        {
            return Ok(None);
        }
        dependencies.push(dependency);
    }

    if matched_scalars != scalar_inputs.inputs().len()
        || matched_in_cursor != external_in_cursor.is_some()
    {
        return Err(LimboError::InternalError(
            "symbolic expression dependency references a missing planned subquery".to_owned(),
        ));
    }
    Ok(Some(dependencies))
}

enum DeclarativeSelectDestination {
    ResultRows,
    Exists,
    RowValue,
    EphemeralIndex {
        input: CursorInputId,
        index_name: String,
        affinity: Option<String>,
    },
}

enum DeclarativePackSink {
    ResultRows,
    EphemeralIndex {
        cursor: CursorId,
        index_name: String,
        affinity: Option<String>,
    },
}

impl DeclarativePackSink {
    fn consume(self, pack: ValuePack) -> BoxedCompile<()> {
        match self {
            Self::ResultRows => result_row_pack(pack).boxed(),
            Self::EphemeralIndex {
                cursor,
                index_name,
                affinity,
            } => insert_index_pack(cursor, pack, index_name, affinity).boxed(),
        }
    }
}

impl<'a> DeclarativeBtreeAccess<'a> {
    const fn direction(self) -> ScanDirection {
        match self {
            Self::Scan { direction, .. }
            | Self::TableRange { direction, .. }
            | Self::IndexRange { direction, .. } => direction,
            Self::RowidEq(_) | Self::InValues { .. } => ScanDirection::Forward,
        }
    }

    const fn index(self) -> Option<&'a Arc<Index>> {
        match self {
            Self::Scan { index, .. } => index,
            Self::IndexRange { index, .. } => Some(index),
            Self::InValues { index, .. } => index,
            Self::RowidEq(_) | Self::TableRange { .. } => None,
        }
    }
}

fn try_emit_declarative_table_scan(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: &mut SelectPlan,
) -> Result<Option<DeclarativeSelectOutcome>> {
    let Some(mut compilation) =
        try_compile_declarative_table_scan(program.get_query_mode(), resolver, plan)?
    else {
        return Ok(None);
    };
    // A scalar compiler has no standalone lowering contract: its value must be
    // bound by a declarative parent. If an eager parent reaches this entrypoint,
    // preserve the scalar destination registers and let the eager emitter own it.
    if matches!(&compilation.compiler, DeclarativeSelectCompiler::Scalar(_)) {
        return Ok(None);
    }
    compilation = compose_declarative_dependencies(
        program.get_query_mode(),
        resolver,
        plan,
        compilation,
        program.is_nested(),
    )?;
    let DeclarativeSelectProgram {
        compiler,
        destination_index: _,
        result_column_count,
    } = compilation;
    let DeclarativeSelectCompiler::Effect(region) = compiler else {
        return Err(LimboError::InternalError(
            "top-level declarative SELECT produced a scalar compiler".to_owned(),
        ));
    };
    let mut destination_cursor = None;
    let mut external_in_subqueries = SmallVec::<[TableInternalId; 2]>::new();
    let mut physical_inputs = SmallVec::<[PhysicalInputBinding; 2]>::new();
    for requirement in region.inputs() {
        let InputSlot::Cursor(input) = requirement.slot() else {
            return Ok(None);
        };
        let cursor = match *requirement.source() {
            DeclarativeInputSource::DestinationCursor { cursor } => {
                if destination_cursor.replace(cursor).is_some() {
                    return Err(LimboError::InternalError(
                        "declarative SELECT has multiple destination cursors".to_owned(),
                    ));
                }
                cursor
            }
            DeclarativeInputSource::Subquery {
                id,
                kind: DeclarativeSubqueryInputKind::InCursor { cursor },
            } => {
                external_in_subqueries.push(id);
                cursor
            }
            DeclarativeInputSource::Subquery {
                kind: DeclarativeSubqueryInputKind::Scalar,
                ..
            } => return Ok(None),
        };
        physical_inputs.push(PhysicalInputBinding::cursor(input, cursor));
    }
    let target_register = program.alloc_register();
    if !external_in_subqueries.is_empty() {
        emit_non_from_clause_subqueries_for_eval_at(
            program,
            resolver,
            &mut plan.non_from_clause_subqueries,
            &plan.join_order,
            Some(&plan.table_references),
            EvalAt::BeforeLoop,
            |subquery| external_in_subqueries.contains(&subquery.internal_id),
        )?;
    }
    let lowered = region.lower_effect_into(program, target_register, physical_inputs)?;
    if destination_cursor.is_some() {
        lowered.expect_no_result_rows()?;
        Ok(Some(DeclarativeSelectOutcome::Consumed))
    } else {
        let (result_cols_start, lowered_result_column_count) = lowered.single_result_row_pack()?;
        if lowered_result_column_count != result_column_count {
            return Err(LimboError::InternalError(format!(
                "compiler IR lowered {lowered_result_column_count} result columns for a {result_column_count}-column SELECT",
            )));
        }
        Ok(Some(DeclarativeSelectOutcome::ResultRows {
            result_cols_start,
        }))
    }
}

fn compose_declarative_dependencies(
    query_mode: QueryMode,
    resolver: &Resolver,
    plan: &mut SelectPlan,
    outer: DeclarativeSelectProgram,
    initialize_once: bool,
) -> Result<DeclarativeSelectProgram> {
    let dependencies = outer
        .compiler
        .inputs()
        .iter()
        .filter(|requirement| requirement.source().subquery_id().is_some())
        .copied()
        .collect::<SmallVec<[InputRequirement<DeclarativeInputSource>; 2]>>();
    if dependencies.is_empty() {
        return Ok(outer);
    }

    let mut producers = Vec::with_capacity(dependencies.len());
    for dependency in dependencies {
        let Some(subquery_id) = dependency.source().subquery_id() else {
            unreachable!("dependency list contains only subquery inputs");
        };
        let Some(subquery_index) = plan
            .non_from_clause_subqueries
            .iter()
            .position(|subquery| subquery.internal_id == subquery_id)
        else {
            return Err(LimboError::InternalError(format!(
                "declarative dependency references missing subquery {subquery_id:?}",
            )));
        };
        let subquery = &plan.non_from_clause_subqueries[subquery_index];
        let SubqueryState::Unevaluated {
            plan: Some(subquery_plan),
        } = &subquery.state
        else {
            return Err(LimboError::InternalError(format!(
                "declarative dependency {subquery_id:?} has no unevaluated plan",
            )));
        };
        let Plan::Select(select_plan) = subquery_plan.as_ref() else {
            return Ok(outer);
        };

        // Every sibling is staged before any original plan is consumed. A
        // failed child therefore returns the complete outer plan to fallback.
        let mut select_plan = select_plan.clone();
        let Some(inner) = try_compile_declarative_table_scan(query_mode, resolver, &select_plan)?
        else {
            return Ok(outer);
        };
        let child_initialize_once = matches!(
            dependency.source(),
            DeclarativeInputSource::Subquery {
                kind: DeclarativeSubqueryInputKind::InCursor { .. },
                ..
            }
        ) && initialize_once;
        let inner = compose_declarative_dependencies(
            query_mode,
            resolver,
            &mut select_plan,
            inner,
            child_initialize_once,
        )?;
        let producer = match (dependency.slot(), *dependency.source(), inner.compiler) {
            (
                InputSlot::Value(input),
                DeclarativeInputSource::Subquery {
                    kind: DeclarativeSubqueryInputKind::Scalar,
                    ..
                },
                DeclarativeSelectCompiler::Scalar(region),
            ) if region.inputs().is_empty() => {
                let (compiler, _) = region.into_parts();
                InputProducer::value(input, compiler)
            }
            (
                InputSlot::Value(_),
                DeclarativeInputSource::Subquery {
                    kind: DeclarativeSubqueryInputKind::Scalar,
                    ..
                },
                _,
            ) => {
                return Ok(outer);
            }
            (
                InputSlot::Cursor(input),
                DeclarativeInputSource::Subquery {
                    kind: DeclarativeSubqueryInputKind::InCursor { cursor },
                    ..
                },
                DeclarativeSelectCompiler::Effect(region),
            ) => {
                let [destination] = region.inputs() else {
                    return Ok(outer);
                };
                let (
                    InputSlot::Cursor(destination_input),
                    DeclarativeInputSource::DestinationCursor {
                        cursor: destination_cursor,
                    },
                ) = (destination.slot(), *destination.source())
                else {
                    return Ok(outer);
                };
                if destination_cursor != cursor {
                    return Ok(outer);
                }
                let Some(index) = inner.destination_index else {
                    return Err(LimboError::InternalError(format!(
                        "declarative IN producer {subquery_id:?} has no ephemeral destination index",
                    )));
                };
                let (compiler, _) = region.into_parts();
                InputProducer::cursor(
                    input,
                    compile_declarative_in_producer(
                        index,
                        destination_input,
                        compiler,
                        initialize_once,
                    ),
                )
            }
            (
                InputSlot::Cursor(_),
                DeclarativeInputSource::Subquery {
                    kind: DeclarativeSubqueryInputKind::InCursor { .. },
                    ..
                },
                DeclarativeSelectCompiler::Scalar(_),
            ) => {
                return Ok(outer);
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "declarative subquery {subquery_id:?} has an incompatible compiler input slot",
                )));
            }
        };
        producers.push(StagedDeclarativeProducer {
            subquery_id,
            subquery_index,
            producer,
            table_references: select_plan.table_references,
        });
    }

    for producer in &producers {
        let subquery = &mut plan.non_from_clause_subqueries[producer.subquery_index];
        assert_eq!(subquery.internal_id, producer.subquery_id);
        drop(subquery.consume_plan(EvalAt::BeforeLoop));
        plan.table_references
            .extend(producer.table_references.clone());
    }

    let compiler = outer.compiler.bind_inputs(
        producers
            .into_iter()
            .map(|producer| producer.producer)
            .collect(),
    )?;

    Ok(DeclarativeSelectProgram {
        compiler,
        destination_index: outer.destination_index,
        result_column_count: outer.result_column_count,
    })
}

fn compile_declarative_in_producer(
    index: Arc<Index>,
    producer_input: CursorInputId,
    producer: BoxedCompile<()>,
    initialize_once: bool,
) -> BoxedCompile<CursorId> {
    if initialize_once {
        declare_ephemeral_index(index)
            .and_then(move |unopened| {
                initialize_cursor_once(open_declared_ephemeral_index(unopened).and_then(
                    move |cursor| {
                        bind_cursor_input(producer_input, cursor, producer).map(move |()| cursor)
                    },
                ))
            })
            .boxed()
    } else {
        open_ephemeral_index(index)
            .and_then(move |cursor| {
                bind_cursor_input(producer_input, cursor, producer).map(move |()| cursor)
            })
            .boxed()
    }
}

fn try_compile_declarative_table_scan(
    query_mode: QueryMode,
    resolver: &Resolver,
    plan: &SelectPlan,
) -> Result<Option<DeclarativeSelectProgram>> {
    if matches!(query_mode, QueryMode::ExplainQueryPlan)
        || plan.group_by.is_some()
        || !plan.aggregates.is_empty()
        || plan.contains_constant_false_condition
        || !plan.values.is_empty()
        || plan.window.is_some()
        || plan.simple_aggregate.is_some()
        || !plan.phantom_params.is_empty()
        || plan.is_correlated()
        || plan.result_columns.is_empty()
    {
        return Ok(None);
    }

    let mut inputs = InputRequirements::new();
    let (destination, destination_cursor, destination_index) = match &plan.query_destination {
        QueryDestination::ResultRows => (DeclarativeSelectDestination::ResultRows, None, None),
        QueryDestination::ExistsSubqueryResult { .. }
            if matches!(plan.distinctness, Distinctness::NonDistinct) =>
        {
            (DeclarativeSelectDestination::Exists, None, None)
        }
        QueryDestination::RowValueSubqueryResult { num_regs: 1, .. }
            if plan.result_columns.len() == 1
                && plan.order_by.is_empty()
                && matches!(plan.distinctness, Distinctness::NonDistinct) =>
        {
            (DeclarativeSelectDestination::RowValue, None, None)
        }
        // The first producer migration covers the identity-shaped index used by
        // scalar IN subqueries. Wider/reordered keys and generated rowids retain
        // the eager destination code until those policies are explicit IR data.
        QueryDestination::EphemeralIndex {
            cursor_id,
            index,
            affinity_str,
            is_delete: false,
        } if plan.result_columns.len() == 1
            && index.ephemeral
            && !index.has_rowid
            && index.columns.len() == 1
            && index.columns[0].pos_in_table == 0 =>
        {
            let destination_input = inputs
                .require_cursor(DeclarativeInputSource::DestinationCursor { cursor: *cursor_id })?;
            (
                DeclarativeSelectDestination::EphemeralIndex {
                    input: destination_input,
                    index_name: index.name.clone(),
                    affinity: affinity_str.as_ref().map(|value| (**value).clone()),
                },
                Some(DeclarativeCursorBinding {
                    input: destination_input,
                    cursor: *cursor_id,
                }),
                Some(index.clone()),
            )
        }
        _ => return Ok(None),
    };

    if plan.join_order.len() > 2 {
        return try_compile_declarative_multi_scan_join(
            resolver,
            plan,
            destination,
            destination_index,
            inputs,
        );
    }

    if plan.join_order.len() == 2 {
        return try_compile_declarative_inner_join(
            resolver,
            plan,
            destination,
            destination_index,
            inputs,
        );
    }

    let [joined] = plan.table_references.joined_tables() else {
        return Ok(None);
    };
    let [member] = plan.join_order.as_slice() else {
        return Ok(None);
    };
    if member.original_idx != 0
        || member.table_id != joined.internal_id
        || member.is_outer
        || joined.join_info.is_some()
    {
        return Ok(None);
    }
    let access = match &joined.op {
        Operation::Scan(Scan::BTreeTable { iter_dir, index }) => DeclarativeBtreeAccess::Scan {
            direction: match iter_dir {
                IterationDirection::Forwards => ScanDirection::Forward,
                IterationDirection::Backwards => ScanDirection::Reverse,
            },
            index: index.as_ref(),
        },
        Operation::Search(Search::RowidEq { cmp_expr }) => {
            DeclarativeBtreeAccess::RowidEq(cmp_expr)
        }
        Operation::Search(Search::Seek {
            index: None,
            seek_def,
        }) => DeclarativeBtreeAccess::TableRange {
            direction: match seek_def.iter_dir {
                IterationDirection::Forwards => ScanDirection::Forward,
                IterationDirection::Backwards => ScanDirection::Reverse,
            },
            seek_def,
        },
        Operation::Search(Search::Seek {
            index: Some(index),
            seek_def,
        }) => DeclarativeBtreeAccess::IndexRange {
            direction: match seek_def.iter_dir {
                IterationDirection::Forwards => ScanDirection::Forward,
                IterationDirection::Backwards => ScanDirection::Reverse,
            },
            index,
            seek_def,
        },
        Operation::Search(Search::InSeek {
            index,
            source: InSeekSource::LiteralList { values, affinity },
        }) => DeclarativeBtreeAccess::InValues {
            index: index.as_ref(),
            source: DeclarativeInSource::Literal {
                values,
                affinity: *affinity,
            },
        },
        Operation::Search(Search::InSeek {
            index,
            source: InSeekSource::Subquery { cursor_id },
        }) => DeclarativeBtreeAccess::InValues {
            index: index.as_ref(),
            source: DeclarativeInSource::Subquery {
                cursor_id: *cursor_id,
            },
        },
        _ => return Ok(None),
    };
    let external_input = CursorInputId::new(u32::from(destination_cursor.is_some()));
    let external_in_cursor = match access {
        DeclarativeBtreeAccess::InValues {
            source: DeclarativeInSource::Subquery { cursor_id },
            ..
        } => resolve_declarative_in_cursor(plan, cursor_id, external_input)?,
        _ => None,
    };
    let direction = access.direction();
    let index = access.index();
    let Table::BTree(table) = &joined.table else {
        return Ok(None);
    };
    if index.is_some_and(|index| index.ephemeral || index.index_method.is_some()) {
        return Ok(None);
    }
    if matches!(
        access,
        DeclarativeBtreeAccess::IndexRange { .. }
            | DeclarativeBtreeAccess::InValues { index: Some(_), .. }
    ) && index.is_some_and(|index| {
        resolver.with_schema(joined.database_id, |schema| {
            index.columns.iter().any(|index_column| {
                table
                    .columns()
                    .get(index_column.pos_in_table)
                    .and_then(|column| schema.get_type_def(&column.ty_str, table.is_strict))
                    .is_some_and(|type_def| type_def.encode().is_some())
            })
        })
    }) {
        return Ok(None);
    }
    let covering_index = index
        .map(Arc::as_ref)
        .filter(|_| joined.utilizes_covering_index());
    let row_layout = covering_index
        .map(RowLayout::CoveringIndex)
        .unwrap_or(RowLayout::Table);
    let mut expr_resolver = RowExprResolver::new(
        resolver,
        joined.database_id,
        joined.internal_id,
        table,
        row_layout,
        &plan.table_references,
    );
    let in_values = match access {
        DeclarativeBtreeAccess::InValues {
            source: DeclarativeInSource::Literal { values, affinity },
            ..
        } => {
            let mut compiled = SmallVec::with_capacity(values.len());
            for value in values {
                let Some(resolved) = expr_resolver.resolve(value)? else {
                    return Ok(None);
                };
                let Some(value) = compile_symbolic_static_expr(&resolved) else {
                    return Ok(None);
                };
                compiled.push(value);
            }
            Some(literal_values(
                compiled,
                affinity,
                index
                    .and_then(|index| index.columns.first())
                    .and_then(|column| column.collation),
            ))
        }
        DeclarativeBtreeAccess::InValues {
            source: DeclarativeInSource::Subquery { .. },
            ..
        } => Some(cursor_values(
            external_in_cursor
                .expect("subquery IN access must declare its external cursor")
                .binding
                .input,
            index
                .and_then(|index| index.columns.first())
                .and_then(|column| column.collation),
        )),
        _ => None,
    };
    let rowid_eq = match access {
        DeclarativeBtreeAccess::RowidEq(rowid_eq) => {
            let Some(rowid_eq) = expr_resolver.resolve(rowid_eq)? else {
                return Ok(None);
            };
            let Some(rowid_eq) = compile_symbolic_static_expr(&rowid_eq) else {
                return Ok(None);
            };
            Some(rowid_eq)
        }
        _ => None,
    };
    let table_range = match access {
        DeclarativeBtreeAccess::TableRange {
            direction,
            seek_def,
        } => {
            let Some(range) = resolve_table_range(direction, table, seek_def, &mut expr_resolver)?
            else {
                return Ok(None);
            };
            let Some(range) = range.into_static_deferred() else {
                return Ok(None);
            };
            Some(range)
        }
        _ => None,
    };
    let index_range = match access {
        DeclarativeBtreeAccess::IndexRange {
            direction,
            index,
            seek_def,
        } => {
            let Some(range) = resolve_index_range(direction, index, seek_def, &mut expr_resolver)?
            else {
                return Ok(None);
            };
            let Some(range) = range.into_static_deferred() else {
                return Ok(None);
            };
            Some(range)
        }
        _ => None,
    };
    let Some(body) =
        resolve_declarative_select_body(plan, resolver, &mut expr_resolver, joined.database_id, 0)?
    else {
        return Ok(None);
    };

    let Some(dependencies) = validate_and_order_declarative_dependencies(
        plan,
        external_in_cursor,
        expr_resolver.into_scalar_inputs(),
    )?
    else {
        return Ok(None);
    };
    for dependency in dependencies {
        inputs.declare(dependency)?;
    }

    let table = table.clone();
    let database_id = joined.database_id;
    let schema_cookie = resolver.with_schema(database_id, |schema| schema.schema_version);
    let compiler = match access {
        DeclarativeBtreeAccess::RowidEq(_) => body.into_compiler(
            seek_rowid(
                table,
                database_id,
                schema_cookie,
                rowid_eq.expect("rowid equality access must compile a key"),
            ),
            destination,
            inputs,
        ),
        DeclarativeBtreeAccess::TableRange { .. } => body.into_compiler(
            seek_table_range(
                table,
                database_id,
                schema_cookie,
                table_range.expect("table range access must compile a range"),
            ),
            destination,
            inputs,
        ),
        DeclarativeBtreeAccess::IndexRange { index, .. } => body.into_compiler(
            seek_index(
                table,
                index.clone(),
                covering_index.is_some(),
                database_id,
                schema_cookie,
                index_range.expect("index range access must compile a range"),
            ),
            destination,
            inputs,
        ),
        DeclarativeBtreeAccess::InValues {
            index: Some(index), ..
        } => body.into_compiler(
            open_index(
                table,
                index.clone(),
                covering_index.is_some(),
                database_id,
                schema_cookie,
            )
            .then(in_values.expect("IN access must compile its value source"))
            .map(|(index, values)| index.seek_each(values)),
            destination,
            inputs,
        ),
        DeclarativeBtreeAccess::InValues { index: None, .. } => body.into_compiler(
            open_table(table, database_id, schema_cookie)
                .then(in_values.expect("IN access must compile its value source"))
                .map(|(table, values)| table.seek_each(values)),
            destination,
            inputs,
        ),
        DeclarativeBtreeAccess::Scan {
            index: Some(index), ..
        } => body.into_compiler(
            scan_index(
                table,
                index.clone(),
                covering_index.is_some(),
                database_id,
                schema_cookie,
                direction,
            ),
            destination,
            inputs,
        ),
        DeclarativeBtreeAccess::Scan { index: None, .. } => body.into_compiler(
            scan_table(table, database_id, schema_cookie, direction),
            destination,
            inputs,
        ),
    };
    Ok(Some(DeclarativeSelectProgram {
        compiler,
        destination_index,
        result_column_count: plan.result_columns.len(),
    }))
}

fn nested_table_scans(
    mut tables: SmallVec<[OpenedTable; 4]>,
    mut directions: SmallVec<[ScanDirection; 4]>,
) -> impl RowStream<Item = SymbolicRows> {
    assert_eq!(tables.len(), directions.len());
    assert!(!tables.is_empty());

    let first_table = tables.remove(0);
    let first_direction = directions.remove(0);
    let mut rows = first_table
        .scan(first_direction)
        .map(|row| pure(SymbolicRows::single(row)))
        .erase();
    for (table, direction) in tables.into_iter().zip(directions) {
        rows = rows
            .flat_map(move |outer_rows| {
                pure(
                    table
                        .scan(direction)
                        .map(move |row| pure(outer_rows.with_row(row)))
                        .erase(),
                )
            })
            .erase();
    }
    rows
}

fn try_compile_declarative_multi_scan_join(
    resolver: &Resolver,
    plan: &SelectPlan,
    destination: DeclarativeSelectDestination,
    destination_index: Option<Arc<Index>>,
    mut inputs: InputRequirements<DeclarativeInputSource>,
) -> Result<Option<DeclarativeSelectProgram>> {
    if plan.join_order.len() < 3 {
        return Ok(None);
    }
    let joined_tables = plan.table_references.joined_tables();
    if joined_tables.len() != plan.join_order.len()
        || joined_tables.iter().any(|joined| {
            joined
                .join_info
                .as_ref()
                .is_some_and(|info| info.join_type != JoinType::Inner)
        })
    {
        return Ok(None);
    }

    let Some(first_member) = plan.join_order.first() else {
        return Ok(None);
    };
    let Some(first_joined) = joined_tables.get(first_member.original_idx) else {
        return Ok(None);
    };
    let database_id = first_joined.database_id;

    let mut seen = SmallVec::<[usize; 4]>::new();
    let mut tables = SmallVec::<[Arc<BTreeTable>; 4]>::new();
    let mut directions = SmallVec::<[ScanDirection; 4]>::new();
    for member in &plan.join_order {
        let Some(joined) = joined_tables.get(member.original_idx) else {
            return Ok(None);
        };
        if member.table_id != joined.internal_id
            || member.is_outer
            || seen.contains(&member.original_idx)
        {
            return Ok(None);
        }
        seen.push(member.original_idx);
        if joined.database_id != database_id {
            return Ok(None);
        }
        let Table::BTree(table) = &joined.table else {
            return Ok(None);
        };
        let Operation::Scan(Scan::BTreeTable {
            iter_dir,
            index: None,
        }) = &joined.op
        else {
            return Ok(None);
        };
        tables.push(table.clone());
        directions.push(match iter_dir {
            IterationDirection::Forwards => ScanDirection::Forward,
            IterationDirection::Backwards => ScanDirection::Reverse,
        });
    }

    let Table::BTree(first_table) = &first_joined.table else {
        unreachable!("validated multi-table join contains only B-tree tables");
    };
    let mut expr_resolver = RowExprResolver::new(
        resolver,
        first_joined.database_id,
        first_joined.internal_id,
        first_table,
        RowLayout::Table,
        &plan.table_references,
    );
    for member in &plan.join_order[1..] {
        let joined = &joined_tables[member.original_idx];
        let Table::BTree(table) = &joined.table else {
            unreachable!("validated multi-table join contains only B-tree tables");
        };
        expr_resolver.add_source(
            joined.database_id,
            joined.internal_id,
            table,
            RowLayout::Table,
        );
    }
    let Some(body) = resolve_declarative_select_body(
        plan,
        resolver,
        &mut expr_resolver,
        database_id,
        plan.join_order.len() - 1,
    )?
    else {
        return Ok(None);
    };
    let Some(dependencies) = validate_and_order_declarative_dependencies(
        plan,
        None,
        expr_resolver.into_scalar_inputs(),
    )?
    else {
        return Ok(None);
    };
    for dependency in dependencies {
        inputs.declare(dependency)?;
    }

    let schema_cookie = resolver.with_schema(database_id, |schema| schema.schema_version);
    let mut opened = pure(SmallVec::<[OpenedTable; 4]>::new()).boxed();
    for table in tables {
        opened = opened
            .then(open_table(table, database_id, schema_cookie))
            .map(|(mut opened, table)| {
                opened.push(table);
                opened
            })
            .boxed();
    }
    let rows = opened.map(move |tables| nested_table_scans(tables, directions));
    let compiler = body.into_symbolic_compiler(rows, destination, inputs);
    Ok(Some(DeclarativeSelectProgram {
        compiler,
        destination_index,
        result_column_count: plan.result_columns.len(),
    }))
}

fn try_compile_declarative_inner_join(
    resolver: &Resolver,
    plan: &SelectPlan,
    destination: DeclarativeSelectDestination,
    destination_index: Option<Arc<Index>>,
    mut inputs: InputRequirements<DeclarativeInputSource>,
) -> Result<Option<DeclarativeSelectProgram>> {
    let [outer_member, inner_member] = plan.join_order.as_slice() else {
        return Ok(None);
    };
    let joined_tables = plan.table_references.joined_tables();
    let [_, _] = joined_tables else {
        return Ok(None);
    };
    let Some(outer) = joined_tables.get(outer_member.original_idx) else {
        return Ok(None);
    };
    let Some(inner) = joined_tables.get(inner_member.original_idx) else {
        return Ok(None);
    };
    if outer_member.table_id != outer.internal_id
        || inner_member.table_id != inner.internal_id
        || outer_member.original_idx == inner_member.original_idx
        || outer_member.is_outer
        || inner_member.is_outer
        || outer.database_id != inner.database_id
        || joined_tables.iter().any(|joined| {
            joined
                .join_info
                .as_ref()
                .is_some_and(|info| info.join_type != JoinType::Inner)
        })
    {
        return Ok(None);
    }

    let outer_direction = match &outer.op {
        Operation::Scan(Scan::BTreeTable {
            iter_dir,
            index: None,
        }) => match iter_dir {
            IterationDirection::Forwards => ScanDirection::Forward,
            IterationDirection::Backwards => ScanDirection::Reverse,
        },
        _ => return Ok(None),
    };
    let inner_access = match &inner.op {
        Operation::Scan(Scan::BTreeTable {
            iter_dir,
            index: None,
        }) => DeclarativeInnerJoinAccess::Scan(match iter_dir {
            IterationDirection::Forwards => ScanDirection::Forward,
            IterationDirection::Backwards => ScanDirection::Reverse,
        }),
        Operation::Search(Search::RowidEq { cmp_expr }) => {
            DeclarativeInnerJoinAccess::Rowid(cmp_expr)
        }
        Operation::Search(Search::Seek {
            index: None,
            seek_def,
        }) => DeclarativeInnerJoinAccess::TableRange {
            direction: match seek_def.iter_dir {
                IterationDirection::Forwards => ScanDirection::Forward,
                IterationDirection::Backwards => ScanDirection::Reverse,
            },
            seek_def,
        },
        Operation::Search(Search::Seek {
            index: Some(index),
            seek_def,
        }) => DeclarativeInnerJoinAccess::IndexRange {
            direction: match seek_def.iter_dir {
                IterationDirection::Forwards => ScanDirection::Forward,
                IterationDirection::Backwards => ScanDirection::Reverse,
            },
            index,
            seek_def,
        },
        Operation::Search(Search::InSeek {
            index,
            source: InSeekSource::LiteralList { values, affinity },
        }) => DeclarativeInnerJoinAccess::InValues {
            index: index.as_ref(),
            source: DeclarativeInSource::Literal {
                values,
                affinity: *affinity,
            },
        },
        Operation::Search(Search::InSeek {
            index,
            source: InSeekSource::Subquery { cursor_id },
        }) => DeclarativeInnerJoinAccess::InValues {
            index: index.as_ref(),
            source: DeclarativeInSource::Subquery {
                cursor_id: *cursor_id,
            },
        },
        _ => return Ok(None),
    };
    let (Table::BTree(outer_table), Table::BTree(inner_table)) = (&outer.table, &inner.table)
    else {
        return Ok(None);
    };
    let inner_index = match inner_access {
        DeclarativeInnerJoinAccess::IndexRange { index, .. } => Some(index),
        DeclarativeInnerJoinAccess::InValues { index, .. } => index,
        DeclarativeInnerJoinAccess::Scan(_)
        | DeclarativeInnerJoinAccess::Rowid(_)
        | DeclarativeInnerJoinAccess::TableRange { .. } => None,
    };
    let inner_covering_index = match inner_index {
        Some(index) => {
            if index.ephemeral
                || index.index_method.is_some()
                || resolver.with_schema(inner.database_id, |schema| {
                    index.columns.iter().any(|index_column| {
                        inner_table
                            .columns()
                            .get(index_column.pos_in_table)
                            .and_then(|column| {
                                schema.get_type_def(&column.ty_str, inner_table.is_strict)
                            })
                            .is_some_and(|type_def| type_def.encode().is_some())
                    })
                })
            {
                return Ok(None);
            }
            inner.utilizes_covering_index().then_some(index)
        }
        None => None,
    };

    let mut expr_resolver = RowExprResolver::new(
        resolver,
        outer.database_id,
        outer.internal_id,
        outer_table,
        RowLayout::Table,
        &plan.table_references,
    );
    let external_input = CursorInputId::new(u32::from(destination_index.is_some()));
    let external_in_cursor = match inner_access {
        DeclarativeInnerJoinAccess::InValues {
            source: DeclarativeInSource::Subquery { cursor_id },
            ..
        } => resolve_declarative_in_cursor(plan, cursor_id, external_input)?,
        _ => None,
    };
    let inner_in_values = match inner_access {
        DeclarativeInnerJoinAccess::InValues {
            source: DeclarativeInSource::Literal { values, affinity },
            ..
        } => {
            let mut resolved = SmallVec::with_capacity(values.len());
            for value in values {
                let Some(value) = expr_resolver.resolve(value)? else {
                    return Ok(None);
                };
                resolved.push(value);
            }
            Some(ResolvedInnerInValues::Literal {
                values: resolved,
                affinity,
                collation: inner_index
                    .and_then(|index| index.columns.first())
                    .and_then(|column| column.collation),
            })
        }
        DeclarativeInnerJoinAccess::InValues {
            source: DeclarativeInSource::Subquery { .. },
            ..
        } => Some(ResolvedInnerInValues::Cursor {
            input: external_in_cursor
                .expect("subquery IN join access must declare its external cursor")
                .binding
                .input,
            collation: inner_index
                .and_then(|index| index.columns.first())
                .and_then(|column| column.collation),
        }),
        DeclarativeInnerJoinAccess::Scan(_)
        | DeclarativeInnerJoinAccess::Rowid(_)
        | DeclarativeInnerJoinAccess::TableRange { .. }
        | DeclarativeInnerJoinAccess::IndexRange { .. } => None,
    };
    let inner_rowid = match inner_access {
        DeclarativeInnerJoinAccess::Rowid(expression) => {
            let Some(expression) = expr_resolver.resolve(expression)? else {
                return Ok(None);
            };
            Some(expression)
        }
        DeclarativeInnerJoinAccess::Scan(_)
        | DeclarativeInnerJoinAccess::TableRange { .. }
        | DeclarativeInnerJoinAccess::IndexRange { .. }
        | DeclarativeInnerJoinAccess::InValues { .. } => None,
    };
    let inner_table_range = match inner_access {
        DeclarativeInnerJoinAccess::TableRange {
            direction,
            seek_def,
        } => {
            let Some(range) =
                resolve_table_range(direction, inner_table, seek_def, &mut expr_resolver)?
            else {
                return Ok(None);
            };
            Some(range)
        }
        DeclarativeInnerJoinAccess::Scan(_)
        | DeclarativeInnerJoinAccess::Rowid(_)
        | DeclarativeInnerJoinAccess::IndexRange { .. }
        | DeclarativeInnerJoinAccess::InValues { .. } => None,
    };
    let inner_index_range = match inner_access {
        DeclarativeInnerJoinAccess::IndexRange {
            direction,
            index,
            seek_def,
        } => {
            let Some(range) = resolve_index_range(direction, index, seek_def, &mut expr_resolver)?
            else {
                return Ok(None);
            };
            Some(range)
        }
        DeclarativeInnerJoinAccess::Scan(_)
        | DeclarativeInnerJoinAccess::Rowid(_)
        | DeclarativeInnerJoinAccess::TableRange { .. }
        | DeclarativeInnerJoinAccess::InValues { .. } => None,
    };
    expr_resolver.add_source(
        inner.database_id,
        inner.internal_id,
        inner_table,
        inner_covering_index
            .map(|index| RowLayout::CoveringIndex(index.as_ref()))
            .unwrap_or(RowLayout::Table),
    );
    let Some(body) =
        resolve_declarative_select_body(plan, resolver, &mut expr_resolver, outer.database_id, 1)?
    else {
        return Ok(None);
    };
    let Some(dependencies) = validate_and_order_declarative_dependencies(
        plan,
        external_in_cursor,
        expr_resolver.into_scalar_inputs(),
    )?
    else {
        return Ok(None);
    };
    for dependency in dependencies {
        inputs.declare(dependency)?;
    }

    let database_id = outer.database_id;
    let schema_cookie = resolver.with_schema(database_id, |schema| schema.schema_version);
    let outer_table = open_table(outer_table.clone(), database_id, schema_cookie);
    let compiler = match inner_access {
        DeclarativeInnerJoinAccess::Scan(inner_direction) => {
            let tables =
                outer_table.then(open_table(inner_table.clone(), database_id, schema_cookie));
            let rows = tables.map(move |(outer, inner)| {
                outer.scan(outer_direction).flat_map(move |outer_row| {
                    pure(
                        inner
                            .scan(inner_direction)
                            .map(move |inner_row| pure(SymbolicRows::pair(outer_row, inner_row))),
                    )
                })
            });
            body.into_symbolic_compiler(rows, destination, inputs)
        }
        DeclarativeInnerJoinAccess::Rowid(_) => {
            let inner_rowid = inner_rowid.expect("rowid join access must resolve its key");
            let tables =
                outer_table.then(open_table(inner_table.clone(), database_id, schema_cookie));
            let rows = tables.map(move |(outer, inner)| {
                outer.scan(outer_direction).flat_map(move |outer_row| {
                    let rows = SymbolicRows::single(outer_row);
                    compile_symbolic_expr(&rows, &inner_rowid).map(move |rowid| {
                        inner
                            .seek_rowid(rowid)
                            .map(move |inner_row| pure(SymbolicRows::pair(outer_row, inner_row)))
                    })
                })
            });
            body.into_symbolic_compiler(rows, destination, inputs)
        }
        DeclarativeInnerJoinAccess::TableRange { .. } => {
            let range = inner_table_range.expect("table range join access must resolve its range");
            let tables =
                outer_table.then(open_table(inner_table.clone(), database_id, schema_cookie));
            let rows = tables.map(move |(outer, inner)| {
                outer.scan(outer_direction).flat_map(move |outer_row| {
                    let rows = SymbolicRows::single(outer_row);
                    let range = range.into_row_deferred(&rows);
                    inner.seek_range(range).map(move |inner_rows| {
                        inner_rows
                            .map(move |inner_row| pure(SymbolicRows::pair(outer_row, inner_row)))
                    })
                })
            });
            body.into_symbolic_compiler(rows, destination, inputs)
        }
        DeclarativeInnerJoinAccess::IndexRange { index, .. } => {
            let range = inner_index_range.expect("index join access must resolve its range");
            let sources = outer_table.then(open_index(
                inner_table.clone(),
                index.clone(),
                inner_covering_index.is_some(),
                database_id,
                schema_cookie,
            ));
            let rows = sources.map(move |(outer, inner)| {
                outer.scan(outer_direction).flat_map(move |outer_row| {
                    let rows = SymbolicRows::single(outer_row);
                    let range = range.into_row_deferred(&rows);
                    inner.seek(range).map(move |inner_rows| {
                        inner_rows
                            .map(move |inner_row| pure(SymbolicRows::pair(outer_row, inner_row)))
                    })
                })
            });
            body.into_symbolic_compiler(rows, destination, inputs)
        }
        DeclarativeInnerJoinAccess::InValues { index: None, .. } => {
            let values = inner_in_values.expect("IN join access must resolve its value source");
            let tables =
                outer_table.then(open_table(inner_table.clone(), database_id, schema_cookie));
            let rows = tables.map(move |(outer, inner)| {
                outer.scan(outer_direction).flat_map(move |outer_row| {
                    let outer_rows = SymbolicRows::single(outer_row);
                    values.into_deferred(&outer_rows).map(move |values| {
                        inner
                            .seek_each(values)
                            .map(move |inner_row| pure(SymbolicRows::pair(outer_row, inner_row)))
                    })
                })
            });
            body.into_symbolic_compiler(rows, destination, inputs)
        }
        DeclarativeInnerJoinAccess::InValues {
            index: Some(index), ..
        } => {
            let values = inner_in_values.expect("IN join access must resolve its value source");
            let sources = outer_table.then(open_index(
                inner_table.clone(),
                index.clone(),
                inner_covering_index.is_some(),
                database_id,
                schema_cookie,
            ));
            let rows = sources.map(move |(outer, inner)| {
                outer.scan(outer_direction).flat_map(move |outer_row| {
                    let outer_rows = SymbolicRows::single(outer_row);
                    values.into_deferred(&outer_rows).map(move |values| {
                        inner
                            .seek_each(values)
                            .map(move |inner_row| pure(SymbolicRows::pair(outer_row, inner_row)))
                    })
                })
            });
            body.into_symbolic_compiler(rows, destination, inputs)
        }
    };
    Ok(Some(DeclarativeSelectProgram {
        compiler,
        destination_index,
        result_column_count: plan.result_columns.len(),
    }))
}

struct DeclarativeSlice {
    limit: Option<BoxedCompile<ValueId>>,
    offset: Option<BoxedCompile<ValueId>>,
}

struct DeclarativeTerminal {
    slice: DeclarativeSlice,
    sink: DeclarativePackSink,
}

struct DeclarativeSelectBody {
    predicates: SmallVec<[ResolvedScalarExpr; 2]>,
    projections: SmallVec<[ResolvedScalarExpr; 4]>,
    sort_expressions: SmallVec<[ResolvedScalarExpr; 4]>,
    sort_keys: SmallVec<[SortKey; 4]>,
    result_column_count: usize,
    distinct_collations: Option<SmallVec<[CollationSeq; 4]>>,
    slice: DeclarativeSlice,
}

fn resolve_declarative_select_body(
    plan: &SelectPlan,
    resolver: &Resolver,
    expr_resolver: &mut RowExprResolver<'_, '_>,
    database_id: usize,
    last_loop_index: usize,
) -> Result<Option<DeclarativeSelectBody>> {
    let mut sort_keys = SmallVec::<[SortKey; 4]>::with_capacity(plan.order_by.len());
    let mut sort_expressions =
        SmallVec::<[ResolvedScalarExpr; 4]>::with_capacity(plan.order_by.len());
    for (expression, order, nulls) in &plan.order_by {
        let Some(resolved) = expr_resolver.resolve(expression)? else {
            return Ok(None);
        };
        let collation = get_collseq_from_expr_with_symbols(
            expression,
            &plan.table_references,
            Some(resolver.symbol_table),
        )?;
        let comparator = resolver.with_schema(database_id, |schema| {
            custom_type_comparator(expression, &plan.table_references, schema)
        });
        sort_keys.push(SortKey::new(*order, collation, *nulls, comparator));
        sort_expressions.push(resolved);
    }

    let limit = match plan.limit.as_deref() {
        None => None,
        Some(limit) => {
            let Some(limit) = expr_resolver.resolve(limit)? else {
                return Ok(None);
            };
            let Some(limit) = compile_symbolic_static_expr(&limit) else {
                return Ok(None);
            };
            Some(limit)
        }
    };
    let offset = match plan.offset.as_deref() {
        None => None,
        Some(offset) => {
            let Some(offset) = expr_resolver.resolve(offset)? else {
                return Ok(None);
            };
            let Some(offset) = compile_symbolic_static_expr(&offset) else {
                return Ok(None);
            };
            Some(offset)
        }
    };

    let mut projections =
        SmallVec::<[ResolvedScalarExpr; 4]>::with_capacity(plan.result_columns.len());
    for result_column in &plan.result_columns {
        let Some(expression) = expr_resolver.resolve(&result_column.expr)? else {
            return Ok(None);
        };
        projections.push(expression);
    }
    let distinct_collations = if matches!(plan.distinctness, Distinctness::Distinct { .. }) {
        Some(
            plan.result_columns
                .iter()
                .map(|column| {
                    get_collseq_from_expr_with_symbols(
                        &column.expr,
                        &plan.table_references,
                        Some(resolver.symbol_table),
                    )
                    .map(|collation| collation.unwrap_or(CollationSeq::Binary))
                })
                .collect::<Result<SmallVec<[CollationSeq; 4]>>>()?,
        )
    } else {
        None
    };

    let mut predicates = SmallVec::<[ResolvedScalarExpr; 2]>::new();
    for predicate in &plan.where_clause {
        if predicate.consumed {
            continue;
        }
        let can_evaluate = predicate.should_eval_before_loop(
            &plan.join_order,
            &plan.non_from_clause_subqueries,
            Some(&plan.table_references),
        ) || (0..=last_loop_index).any(|loop_index| {
            predicate.should_eval_at_loop(
                loop_index,
                &plan.join_order,
                &plan.non_from_clause_subqueries,
                Some(&plan.table_references),
            )
        });
        if predicate.from_outer_join.is_some() || !can_evaluate {
            return Ok(None);
        }
        let Some(predicate) = expr_resolver.resolve(&predicate.expr)? else {
            return Ok(None);
        };
        predicates.push(predicate);
    }

    Ok(Some(DeclarativeSelectBody {
        predicates,
        result_column_count: projections.len(),
        projections,
        sort_expressions,
        sort_keys,
        distinct_collations,
        slice: DeclarativeSlice { limit, offset },
    }))
}

impl DeclarativeSelectBody {
    fn into_compiler<Scan, Rows>(
        self,
        scan: Scan,
        destination: DeclarativeSelectDestination,
        inputs: InputRequirements<DeclarativeInputSource>,
    ) -> DeclarativeSelectCompiler
    where
        Scan: Compile<Output = Rows> + 'static,
        Rows: RowStream<Item = Row> + 'static,
    {
        self.into_symbolic_compiler(
            scan.and_then(|rows| pure(rows.map(|row| pure(SymbolicRows::single(row))))),
            destination,
            inputs,
        )
    }

    fn into_symbolic_compiler<Scan, Rows>(
        self,
        scan: Scan,
        destination: DeclarativeSelectDestination,
        inputs: InputRequirements<DeclarativeInputSource>,
    ) -> DeclarativeSelectCompiler
    where
        Scan: Compile<Output = Rows> + 'static,
        Rows: RowStream<Item = SymbolicRows> + 'static,
    {
        match destination {
            DeclarativeSelectDestination::ResultRows => {
                DeclarativeSelectCompiler::Effect(CompileRegion::new(
                    self.with_sink(scan, DeclarativePackSink::ResultRows),
                    inputs,
                ))
            }
            DeclarativeSelectDestination::Exists => {
                DeclarativeSelectCompiler::Scalar(CompileRegion::new(self.exists(scan), inputs))
            }
            DeclarativeSelectDestination::RowValue => {
                DeclarativeSelectCompiler::Scalar(CompileRegion::new(self.row_value(scan), inputs))
            }
            DeclarativeSelectDestination::EphemeralIndex {
                input,
                index_name,
                affinity,
            } => {
                // The description owns only a symbolic input slot. Its physical
                // destination cursor is not selected until lowering.
                DeclarativeSelectCompiler::Effect(CompileRegion::new(
                    cursor_input(input).and_then(move |cursor| {
                        self.with_sink(
                            scan,
                            DeclarativePackSink::EphemeralIndex {
                                cursor,
                                index_name,
                                affinity,
                            },
                        )
                    }),
                    inputs,
                ))
            }
        }
    }

    fn exists<Scan, Rows>(self, scan: Scan) -> BoxedCompile<ValueId>
    where
        Scan: Compile<Output = Rows> + 'static,
        Rows: RowStream<Item = SymbolicRows> + 'static,
    {
        let Self {
            predicates,
            slice: DeclarativeSlice { limit, offset },
            ..
        } = self;
        scan.and_then(move |rows| {
            if predicates.is_empty() {
                compile_declarative_exists(rows, limit, offset)
            } else {
                compile_declarative_exists(
                    rows.filter(move |rows| compile_symbolic_conjunction(&rows, &predicates)),
                    limit,
                    offset,
                )
            }
        })
        .boxed()
    }

    fn row_value<Scan, Rows>(self, scan: Scan) -> BoxedCompile<ValueId>
    where
        Scan: Compile<Output = Rows> + 'static,
        Rows: RowStream<Item = SymbolicRows> + 'static,
    {
        let Self {
            predicates,
            mut projections,
            slice: DeclarativeSlice { limit, offset },
            ..
        } = self;
        assert_eq!(projections.len(), 1);
        let projection = projections
            .pop()
            .expect("scalar subquery must have one projection");
        scan.and_then(move |rows| {
            if predicates.is_empty() {
                compile_declarative_row_value(rows, projection, limit, offset)
            } else {
                compile_declarative_row_value(
                    rows.filter(move |rows| compile_symbolic_conjunction(&rows, &predicates)),
                    projection,
                    limit,
                    offset,
                )
            }
        })
        .boxed()
    }

    fn with_sink<Scan, Rows>(self, scan: Scan, sink: DeclarativePackSink) -> BoxedCompile<()>
    where
        Scan: Compile<Output = Rows> + 'static,
        Rows: RowStream<Item = SymbolicRows> + 'static,
    {
        let Self {
            predicates,
            projections,
            sort_expressions,
            sort_keys,
            result_column_count,
            distinct_collations,
            slice,
        } = self;
        let terminal = DeclarativeTerminal { slice, sink };
        scan.and_then(move |rows| {
            if predicates.is_empty() {
                compile_declarative_rows(
                    rows,
                    projections,
                    sort_expressions,
                    sort_keys,
                    result_column_count,
                    distinct_collations,
                    terminal,
                )
            } else {
                compile_declarative_rows(
                    rows.filter(move |rows| compile_symbolic_conjunction(&rows, &predicates)),
                    projections,
                    sort_expressions,
                    sort_keys,
                    result_column_count,
                    distinct_collations,
                    terminal,
                )
            }
        })
        .boxed()
    }
}

fn compile_declarative_exists<Stream>(
    rows: Stream,
    limit: Option<BoxedCompile<ValueId>>,
    offset: Option<BoxedCompile<ValueId>>,
) -> BoxedCompile<ValueId>
where
    Stream: RowStream + 'static,
{
    match (limit, offset) {
        (Some(limit), Some(offset)) => rows.skip(offset).take(limit).has_rows(),
        (Some(limit), None) => rows.take(limit).has_rows(),
        (None, Some(offset)) => rows.skip(offset).has_rows(),
        (None, None) => rows.has_rows(),
    }
}

fn compile_declarative_row_value<Stream>(
    rows: Stream,
    projection: ResolvedScalarExpr,
    limit: Option<BoxedCompile<ValueId>>,
    offset: Option<BoxedCompile<ValueId>>,
) -> BoxedCompile<ValueId>
where
    Stream: RowStream<Item = SymbolicRows> + 'static,
{
    match (limit, offset) {
        (Some(limit), Some(offset)) => rows
            .skip(offset)
            .map(move |rows| compile_symbolic_expr(&rows, &projection))
            .take(limit)
            .first_or(constant(Value::Null)),
        (Some(limit), None) => rows
            .map(move |rows| compile_symbolic_expr(&rows, &projection))
            .take(limit)
            .first_or(constant(Value::Null)),
        (None, Some(offset)) => rows
            .skip(offset)
            .map(move |rows| compile_symbolic_expr(&rows, &projection))
            .first_or(constant(Value::Null)),
        (None, None) => rows
            .map(move |rows| compile_symbolic_expr(&rows, &projection))
            .first_or(constant(Value::Null)),
    }
}

fn compile_declarative_rows<Stream>(
    rows: Stream,
    projections: SmallVec<[ResolvedScalarExpr; 4]>,
    mut sort_expressions: SmallVec<[ResolvedScalarExpr; 4]>,
    sort_keys: SmallVec<[SortKey; 4]>,
    result_column_count: usize,
    distinct_collations: Option<SmallVec<[CollationSeq; 4]>>,
    terminal: DeclarativeTerminal,
) -> BoxedCompile<()>
where
    Stream: RowStream<Item = SymbolicRows> + 'static,
{
    if sort_keys.is_empty() {
        let DeclarativeTerminal {
            slice: DeclarativeSlice { limit, offset },
            sink,
        } = terminal;
        if let Some(collations) = distinct_collations {
            compile_declarative_distinct_projection(
                rows,
                projections,
                collations,
                limit,
                offset,
                sink,
            )
        } else {
            compile_declarative_projection(rows, projections, limit, offset, sink)
        }
    } else {
        sort_expressions.extend(projections);
        compile_declarative_sorted_projection(
            rows,
            sort_expressions,
            sort_keys,
            result_column_count,
            distinct_collations,
            terminal,
        )
    }
}

fn compile_declarative_distinct_projection<Stream>(
    rows: Stream,
    projections: SmallVec<[ResolvedScalarExpr; 4]>,
    collations: SmallVec<[CollationSeq; 4]>,
    limit: Option<BoxedCompile<ValueId>>,
    offset: Option<BoxedCompile<ValueId>>,
    sink: DeclarativePackSink,
) -> BoxedCompile<()>
where
    Stream: RowStream<Item = SymbolicRows> + 'static,
{
    let rows = rows
        .map(move |rows| compile_symbolic_exprs(&rows, &projections))
        .distinct(collations);
    match (limit, offset) {
        (Some(limit), Some(offset)) => rows
            .skip(offset)
            .take(limit)
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
        (Some(limit), None) => rows
            .take(limit)
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
        (None, Some(offset)) => rows
            .skip(offset)
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
        (None, None) => rows.for_each(move |pack| sink.consume(pack)).boxed(),
    }
}

fn compile_declarative_sorted_projection<Stream>(
    rows: Stream,
    expressions: SmallVec<[ResolvedScalarExpr; 4]>,
    sort_keys: SmallVec<[SortKey; 4]>,
    result_column_count: usize,
    distinct_collations: Option<SmallVec<[CollationSeq; 4]>>,
    terminal: DeclarativeTerminal,
) -> BoxedCompile<()>
where
    Stream: RowStream<Item = SymbolicRows> + 'static,
{
    let key_count = sort_keys.len();
    let record_width = expressions.len();
    let rows = rows.map(move |rows| compile_symbolic_exprs(&rows, &expressions));
    if let Some(collations) = distinct_collations {
        compile_declarative_sorted_stream(
            rows.distinct_by(collations, move |pack| {
                select_pack(pack, key_count, result_column_count)
            })
            .sort(sort_keys, record_width),
            key_count,
            result_column_count,
            terminal,
        )
    } else {
        compile_declarative_sorted_stream(
            rows.sort(sort_keys, record_width),
            key_count,
            result_column_count,
            terminal,
        )
    }
}

fn compile_declarative_sorted_stream<Stream>(
    rows: Stream,
    key_count: usize,
    result_column_count: usize,
    terminal: DeclarativeTerminal,
) -> BoxedCompile<()>
where
    Stream: RowStream<Item = SortedRow> + 'static,
{
    let DeclarativeTerminal {
        slice: DeclarativeSlice { limit, offset },
        sink,
    } = terminal;
    match (limit, offset) {
        (Some(limit), Some(offset)) => rows
            .skip(offset)
            .take(limit)
            .map(move |row| sorted_result_pack(row, key_count, result_column_count))
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
        (Some(limit), None) => rows
            .take(limit)
            .map(move |row| sorted_result_pack(row, key_count, result_column_count))
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
        (None, Some(offset)) => rows
            .skip(offset)
            .map(move |row| sorted_result_pack(row, key_count, result_column_count))
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
        (None, None) => rows
            .map(move |row| sorted_result_pack(row, key_count, result_column_count))
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
    }
}

fn sorted_result_pack(
    row: SortedRow,
    key_count: usize,
    result_column_count: usize,
) -> impl Compile<Output = crate::translate::compiler::ValuePack> {
    let mut values = SmallVec::with_capacity(result_column_count);
    for column in key_count..key_count + result_column_count {
        values.push(row.column(column).boxed());
    }
    pack_values(values)
}

fn compile_declarative_projection<Stream>(
    rows: Stream,
    projections: SmallVec<[ResolvedScalarExpr; 4]>,
    limit: Option<BoxedCompile<ValueId>>,
    offset: Option<BoxedCompile<ValueId>>,
    sink: DeclarativePackSink,
) -> BoxedCompile<()>
where
    Stream: RowStream<Item = SymbolicRows> + 'static,
{
    match (limit, offset) {
        (Some(limit), Some(offset)) => rows
            .skip(offset)
            .map(move |rows| compile_symbolic_exprs(&rows, &projections))
            .take(limit)
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
        (Some(limit), None) => rows
            .map(move |rows| compile_symbolic_exprs(&rows, &projections))
            .take(limit)
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
        (None, Some(offset)) => rows
            .skip(offset)
            .map(move |rows| compile_symbolic_exprs(&rows, &projections))
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
        (None, None) => rows
            .map(move |rows| compile_symbolic_exprs(&rows, &projections))
            .for_each(move |pack| sink.consume(pack))
            .boxed(),
    }
}

fn emit_program_for_select_with_inputs(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut plan: SelectPlan,
    materialized_build_inputs: HashMap<usize, MaterializedBuildInput>,
) -> Result<()> {
    let result_cols_start = program.with_scoped_result_cols_start(|program| {
        // Boxed to keep ~960 B off the prepare-path stack; see TranslateCtx size.
        let mut t_ctx = Box::new(TranslateCtx::new(
            program,
            resolver.fork_with_expr_cache(),
            plan.table_references.joined_tables().len(),
            false,
        ));
        t_ctx.materialized_build_inputs = materialized_build_inputs;
        emit_query(program, &mut plan, &mut t_ctx)
    })?;

    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    program.reg_result_cols_start = Some(result_cols_start);
    Ok(())
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_query<'a>(
    program: &mut ProgramBuilder,
    plan: &'a mut SelectPlan,
    t_ctx: &mut TranslateCtx<'a>,
) -> Result<usize> {
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    // Register parameters from EXISTS subquery result columns that were dropped
    // during semi/anti-join unnesting. No code is emitted for these, but the
    // parameter slots must exist for bind-time validation to succeed.
    for variable in &plan.phantom_params {
        program.register_variable(variable);
    }

    // Evaluate uncorrelated subqueries as early as possible, because even LIMIT can reference a subquery.
    // This must happen before VALUES emission since VALUES expressions may contain scalar subqueries.
    emit_non_from_clause_subqueries_for_eval_at(
        program,
        &t_ctx.resolver,
        &mut plan.non_from_clause_subqueries,
        &plan.join_order,
        Some(&plan.table_references),
        EvalAt::BeforeLoop,
        |_| true,
    )?;

    // Handle VALUES clause - emit values after subqueries are prepared
    if !plan.values.is_empty() {
        let reg_result_cols_start = emit_values(program, plan, t_ctx)?;
        program.preassign_label_to_next_insn(after_main_loop_label);
        return Ok(reg_result_cols_start);
    }

    // Emit FROM clause subqueries first so the results can be read in the main query loop.
    emit_from_clause_subqueries(program, t_ctx, &mut plan.table_references, &plan.join_order)?;

    // For non-grouped aggregation queries that also have non-aggregate columns,
    // we need to ensure non-aggregate columns are only emitted once.
    // This flag helps track whether we've already emitted these columns.
    let has_ungrouped_nonagg_cols = !plan.aggregates.is_empty()
        && plan.group_by.is_none()
        && plan.result_columns.iter().any(|c| !c.contains_aggregates);

    if has_ungrouped_nonagg_cols {
        let flag = program.alloc_register();
        program.emit_int(0, flag); // Initialize flag to 0 (not yet emitted)
        t_ctx.reg_nonagg_emit_once_flag = Some(flag);
    }

    // Allocate registers for result columns
    if t_ctx.reg_result_cols_start.is_none() {
        t_ctx.reg_result_cols_start = Some(program.alloc_registers(plan.result_columns.len()));
        program.reg_result_cols_start = t_ctx.reg_result_cols_start
    }

    // For ungrouped aggregates with non-aggregate columns, initialize EXISTS subquery
    // result_regs to 0. EXISTS returns 0 (not NULL) when the subquery is never evaluated
    // (correlated EXISTS in empty loop). Non-aggregate columns themselves are evaluated
    // after the loop in emit_ungrouped_aggregation if the loop never ran.
    // We only initialize EXISTS subqueries that haven't been evaluated yet (correlated ones).
    if has_ungrouped_nonagg_cols {
        for rc in plan.result_columns.iter() {
            if !rc.contains_aggregates {
                init_exists_result_regs(program, &rc.expr, &plan.non_from_clause_subqueries);
            }
        }
    }

    let has_group_by_exprs = plan
        .group_by
        .as_ref()
        .is_some_and(|gb| !gb.exprs.is_empty());

    // Initialize cursors and other resources needed for query execution
    if !plan.order_by.is_empty() {
        EmitOrderBy::init(
            program,
            t_ctx,
            &plan.result_columns,
            &plan.order_by,
            &plan.table_references,
            has_group_by_exprs,
            plan.distinctness != Distinctness::NonDistinct,
            &plan.aggregates,
        )?;
    }

    if has_group_by_exprs {
        if let Some(ref group_by) = plan.group_by {
            EmitGroupBy::init(
                program,
                t_ctx,
                group_by,
                plan,
                &plan.result_columns,
                &plan.order_by,
            )?;
        }
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY (or HAVING without GROUP BY)
        // Aggregate registers need to be NULLed at the start because the same registers might be reused on another invocation of a subquery,
        // and if they are not NULLed, the 2nd invocation of the same subquery will have values left over from the first invocation.
        t_ctx.reg_agg_start = Some(program.alloc_registers_and_init_w_null(plan.aggregates.len()));
    } else if let Some(window) = &plan.window {
        EmitWindow::init(
            program,
            t_ctx,
            window,
            plan,
            &plan.result_columns,
            &plan.order_by,
        )?;
    }

    let distinct_ctx = if let Distinctness::Distinct { .. } = &plan.distinctness {
        Some(init_distinct(program, plan, &t_ctx.resolver)?)
    } else {
        None
    };
    if let Distinctness::Distinct { ctx } = &mut plan.distinctness {
        *ctx = distinct_ctx
    }
    if let Distinctness::Distinct { ctx: Some(ctx) } = &plan.distinctness {
        program.emit_insn(Insn::HashClear {
            hash_table_id: ctx.hash_table_id,
        });
        emit_explain!(program, false, "USE HASH TABLE FOR DISTINCT".to_owned());
    }

    init_limit(program, t_ctx, &plan.limit, &plan.offset)?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    // however an aggregation might still happen,
    // e.g. SELECT COUNT(*) WHERE 0 returns a row with 0, not an empty result set.
    // This Goto must be placed AFTER all initialization (cursors, sorters, etc.) so that
    // resources like the GROUP BY sorter are properly opened before we skip to the aggregation phase.
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }
    InitLoop::emit(
        program,
        t_ctx,
        &plan.table_references,
        &mut plan.aggregates,
        &OperationMode::SELECT,
        &plan.where_clause,
        &plan.join_order,
        &mut plan.non_from_clause_subqueries,
    )?;

    if matches!(plan.simple_aggregate, Some(SimpleAggregate::Count))
        && emit_simple_count(program, t_ctx, plan)?
    {
        // Keep LIMIT's early-exit jump target valid even on the simple_count fast path.
        // init_limit may emit an IfNot to after_main_loop_label (e.g. scalar subquery injects LIMIT 1).
        // Without resolving this label before the early return, bytecode assembly fails
        // with an unresolved IfNot target.
        program.preassign_label_to_next_insn(after_main_loop_label);
        return Ok(t_ctx.reg_result_cols_start.unwrap());
    }

    // Set up main query execution loop
    OpenLoop::emit(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        &plan.where_clause,
        None,
        OperationMode::SELECT,
        &mut plan.non_from_clause_subqueries,
    )?;

    // Process result columns and expressions in the inner loop
    LoopBodyEmitter::emit(program, t_ctx, plan)?;

    // Clean up and close the main execution loop
    CloseLoop::emit(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        OperationMode::SELECT,
        Some(plan),
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);

    let has_order_by = !plan.order_by.is_empty();
    let order_by_necessary = has_order_by && !plan.contains_constant_false_condition;
    let mut grouped_output_subqueries = plan.non_from_clause_subqueries.clone();

    // Handle GROUP BY and aggregation processing
    if has_group_by_exprs {
        let row_source = &t_ctx
            .meta_group_by
            .as_ref()
            .expect("group by metadata not found")
            .row_source;
        if matches!(row_source, GroupByRowSource::Sorter { .. }) {
            group_by_agg_phase(program, t_ctx, plan)?;
        }
        group_by_emit_row_phase(program, t_ctx, plan, &mut grouped_output_subqueries)?;
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY (or HAVING without GROUP BY)
        emit_ungrouped_aggregation(program, t_ctx, plan)?;
    } else if plan.window.is_some() {
        emit_window_flush(program, t_ctx, plan)?;
    }

    // Process ORDER BY results if needed
    if has_order_by && order_by_necessary {
        EmitOrderBy::emit(program, t_ctx, plan)?;
    }

    Ok(t_ctx.reg_result_cols_start.unwrap())
}

#[derive(Debug, Clone)]
/// Captures the parameters needed to materialize one hash-build input.
struct MaterializationSpec {
    build_table_idx: usize,
    probe_table_idx: usize,
    mode: MaterializedBuildInputMode,
    prefix_tables: TableMask,
    key_exprs: Vec<Expr>,
    payload_columns: Vec<MaterializedColumnRef>,
}

/// Build materialized hash-build inputs for hash joins that depend on prior joins.
///
/// A materialized build input is an ephemeral table that captures the rows
/// a hash join is allowed to build from after earlier joins and filters have
/// been applied. This prevents the build side from being re-scanned in its
/// full, unfiltered form when prior join constraints must be respected.
///
/// The materialization uses a join-prefix: all tables that appear before the
/// probe table in the join order, plus the build table itself. This prefix
/// represents the minimal context needed to evaluate build-side constraints.
/// For probe->build chaining we store join keys and payload columns directly
/// in the ephemeral table; otherwise we only store rowids and `SeekRowid`
/// during probing when needed.
pub(crate) fn emit_materialized_build_inputs(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: &mut SelectPlan,
) -> Result<HashMap<usize, MaterializedBuildInput>> {
    let mut build_inputs: HashMap<usize, MaterializedBuildInput> = HashMap::default();
    let mut materializations: Vec<MaterializationSpec> = Vec::new();
    let mut hash_tables_to_keep_open = BitSet::default();

    // Keep hash tables open while running materialization subplans so we can reuse them.
    // A build table may appear in multiple hash joins when chaining, so we do not
    // treat repeated build tables as an error.
    for table in plan.table_references.joined_tables().iter() {
        if let Operation::HashJoin(hash_join_op) = &table.op {
            let build_table = &plan.table_references.joined_tables()[hash_join_op.build_table_idx];
            hash_tables_to_keep_open.set(build_table.internal_id.into())?;
        }
    }

    let mut seen_build_tables: TableMask = TableMask::default();

    // decide per-hash-join materialization mode (rowid-only vs key+payload).
    for member in plan.join_order.iter() {
        let table = &plan.table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table.op {
            if !hash_join_op.materialize_build_input
                || seen_build_tables.get(hash_join_op.build_table_idx)
            {
                continue;
            }
            seen_build_tables.set(hash_join_op.build_table_idx)?;

            let probe_table_idx = hash_join_op.probe_table_idx;
            let probe_pos = plan
                .join_order
                .iter()
                .position(|member| member.original_idx == probe_table_idx)
                .unwrap_or(plan.join_order.len());
            let build_table_was_prior_probe = plan.join_order[..probe_pos].iter().any(|member| {
                let table_ref = &plan.table_references.joined_tables()[member.original_idx];
                matches!(
                    table_ref.op,
                    Operation::HashJoin(ref hj) if hj.probe_table_idx == hash_join_op.build_table_idx
                )
            });

            // The join prefix is the set of tables we include when building this hash
            // input (all tables before the probe + the build table). If the prefix
            // has *any* table besides the build table, then rowid-only materialization
            // is unsafe. Here's why:
            //
            // Rowid-only keeps each build-table rowid at most once. That throws away
            // which prefix row it came from, so we lose the one-to-one link between
            // a prefix match and a build row.
            //
            // Example (t1 is a left-side table earlier in the join order):
            //   t1 rows:     t1_1(c=1), t1_2(c=2)
            //   t2 rows:     t2_7(c=1), t2_8(c=2)   (build table)
            //   t3 rows:     one row per t2 row
            //
            // Correct result after joining:
            //   t1_1 + t2_7 + t2_7's t3 row
            //   t1_2 + t2_8 + t2_8's t3 row   (2 rows)
            //
            // Key+payload materialization lets us PRUNE the prefix tables (like t1)
            // from the main join order, because their needed columns now live in
            // the payload. So the main plan does NOT loop t1 again.
            //
            // However, rowid-only materialization keeps just {t2_7, t2_8} with no link to t1_1/t1_2.
            // Since t1 stays in the main join loop, each t1 row joins against the
            // materialized t2 set. With no t1→t2 correlation, every t1 row matches
            // both t2 rows, incorrectly producing 4 rows (a cross product).
            //
            // Therefore: if the prefix has other tables, we must store key+payload
            // rows so each prefix match stays distinct and the main plan can drop
            // the prefix loops.
            let (_, included_tables) =
                materialization_prefix(plan, hash_join_op.build_table_idx, probe_table_idx)?;
            let prefix_has_other_tables = included_tables
                .iter()
                .any(|table_idx| table_idx != hash_join_op.build_table_idx);

            if build_table_was_prior_probe || prefix_has_other_tables {
                // Prior probe -> build chaining OR any multi-table prefix requires keys+payload
                // so we do not lose multiplicity or correlation.
                let payload_columns = collect_materialized_payload_columns(plan, &included_tables)?;
                let key_exprs: Vec<Expr> = hash_join_op
                    .join_keys
                    .iter()
                    .map(|key| key.get_build_expr(&plan.where_clause).clone())
                    .collect();
                let mode = MaterializedBuildInputMode::KeyPayload {
                    num_keys: key_exprs.len(),
                    payload_columns: payload_columns.clone(),
                };
                materializations.push(MaterializationSpec {
                    build_table_idx: hash_join_op.build_table_idx,
                    probe_table_idx,
                    mode,
                    prefix_tables: included_tables,
                    key_exprs,
                    payload_columns,
                });
            } else {
                // Single-table prefix: a rowid list preserves the build-side filters
                // without losing multiplicity (as explained in the comment above).
                materializations.push(MaterializationSpec {
                    build_table_idx: hash_join_op.build_table_idx,
                    probe_table_idx,
                    mode: MaterializedBuildInputMode::RowidOnly,
                    prefix_tables: TableMask::default(),
                    key_exprs: Vec::new(),
                    payload_columns: Vec::new(),
                });
            }
        }
    }

    // Now we emit each of the materialization subplans into an ephemeral table.
    for spec in materializations.iter() {
        let build_table = &plan.table_references.joined_tables()[spec.build_table_idx];
        let build_table_name = if build_table.table.get_name() == build_table.identifier {
            build_table.identifier.clone()
        } else {
            format!(
                "{} AS {}",
                build_table.table.get_name(),
                build_table.identifier
            )
        };
        let internal_id = program.table_reference_counter.next();
        let columns = match &spec.mode {
            MaterializedBuildInputMode::RowidOnly => {
                std::iter::once(build_rowid_column()).try_collect()?
            }
            MaterializedBuildInputMode::KeyPayload {
                num_keys,
                payload_columns,
            } => build_materialized_input_columns(*num_keys, payload_columns)?,
        };
        let ephemeral_table = Arc::new(BTreeTable::new(
            0,
            format!("hash_build_input_{internal_id}"),
            crate::alloc::vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ));
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(ephemeral_table.clone()));

        // Build a plan that emits only rowids for the build table using the join prefix
        // that makes the hash join legal (including any earlier hash joins).
        let materialize_plan = build_materialized_build_input_plan(
            plan,
            spec.build_table_idx,
            spec.probe_table_idx,
            cursor_id,
            ephemeral_table,
            &spec.mode,
            &spec.key_exprs,
            &spec.payload_columns,
            &build_inputs,
        )?;

        // Make the materialization plan show up as a subtree in EXPLAIN QUERY PLAN output.
        emit_explain!(
            program,
            true,
            format!("MATERIALIZE hash build input for {build_table_name}")
        );
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id,
            is_table: true,
        });
        program.nested(|program| -> Result<()> {
            program.set_hash_tables_to_keep_open(&hash_tables_to_keep_open);
            // emit_program_for_select_with_inputs unconditionally overwrites
            // program.result_columns and extends program.table_references with
            // the materialize subplan's columns/refs. In a nested context (e.g.
            // a compound branch or CTE) those belong to the *outer* SELECT, so
            // save and restore them around the nested emission.
            let saved_result_columns = std::mem::take(&mut program.result_columns);
            let saved_table_references = std::mem::take(&mut program.table_references);
            emit_program_for_select_with_inputs(
                program,
                resolver,
                materialize_plan,
                build_inputs.clone(),
            )?;
            program.result_columns = saved_result_columns;
            program.table_references = saved_table_references;
            program.clear_hash_tables_to_keep_open();
            Ok(())
        })?;
        program.pop_current_parent_explain();

        build_inputs.insert(
            spec.build_table_idx,
            MaterializedBuildInput {
                cursor_id,
                mode: spec.mode.clone(),
                prefix_tables: spec.prefix_tables.clone(),
            },
        );
    }

    // Drop any join-prefix tables already captured by key+payload materializations.
    prune_join_order_for_materialized_inputs(plan, &build_inputs)?;

    #[cfg(debug_assertions)]
    turso_assert!(
        {
            let join_order_tables: HashSet<_> = plan
                .join_order
                .iter()
                .map(|member| member.original_idx)
                .collect();
            let build_tables_in_plan: HashSet<_> = plan
                .join_order
                .iter()
                .filter_map(|member| {
                    let table = &plan.table_references.joined_tables()[member.original_idx];
                    if let Operation::HashJoin(hash_join_op) = &table.op {
                        Some(hash_join_op.build_table_idx)
                    } else {
                        None
                    }
                })
                .collect();
            build_inputs.iter().all(|(build_table_idx, input)| {
                if !build_tables_in_plan.contains(build_table_idx) {
                    return true;
                }
                if !matches!(input.mode, MaterializedBuildInputMode::KeyPayload { .. }) {
                    return true;
                }
                input
                    .prefix_tables
                    .iter()
                    .all(|table_idx| !join_order_tables.contains(&table_idx))
            })
        },
        "materialized build input prefix table still present in join order"
    );
    Ok(build_inputs)
}

/// Remove join-order entries already satisfied by key+payload materializations.
///
/// This prevents redundant scans (and cross products) when a hash-build input
/// already captures a join prefix. It also marks fully covered WHERE terms as
/// consumed so they are not re-applied later in the main plan.
fn prune_join_order_for_materialized_inputs(
    plan: &mut SelectPlan,
    build_inputs: &HashMap<usize, MaterializedBuildInput>,
) -> Result<()> {
    if build_inputs.is_empty() {
        return Ok(());
    }

    let mut build_tables_in_plan = TableMask::default();
    for member in plan.join_order.iter() {
        let table = &plan.table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table.op {
            build_tables_in_plan.set(hash_join_op.build_table_idx)?;
        }
    }

    let mut tables_to_remove: TableMask = TableMask::default();
    for (build_table_idx, input) in build_inputs.iter() {
        if !build_tables_in_plan.get(*build_table_idx) {
            continue;
        }
        if matches!(input.mode, MaterializedBuildInputMode::KeyPayload { .. }) {
            tables_to_remove.try_extend(input.prefix_tables.iter())?;
        }
    }

    if tables_to_remove.is_empty() {
        return Ok(());
    }

    for term in plan.where_clause.iter_mut() {
        if term.consumed {
            continue;
        }
        if term.from_outer_join.is_some() {
            // OUTER JOIN terms still belong to the right-table loop recorded in
            // `from_outer_join`. Materializing and pruning the build-side prefix
            // does not make those terms safe to consume here, because the
            // materialization subplan does not include the probe table that
            // determines the null-extension boundary.
            continue;
        }
        let mask = table_mask_from_expr(
            &term.expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
        if tables_to_remove.contains_all_set_bits_of(&mask) {
            term.consumed = true;
        }
    }
    plan.join_order
        .retain(|member| !tables_to_remove.get(member.original_idx));
    Ok(())
}

/// Compute the join-prefix used to materialize a hash-build input.
///
/// The prefix consists of all tables before the probe table plus the build
/// table itself (if not already present). The returned `included_tables`
/// list also includes build tables of earlier hash joins so payload collection
/// can capture all referenced columns.
fn materialization_prefix(
    plan: &SelectPlan,
    build_table_idx: usize,
    probe_table_idx: usize,
) -> Result<(Vec<JoinOrderMember>, TableMask)> {
    let mut join_order = plan.join_order.clone();
    if join_order
        .iter()
        .all(|member| member.original_idx != probe_table_idx)
    {
        let probe_table = &plan.table_references.joined_tables()[probe_table_idx];
        join_order.push(JoinOrderMember {
            table_id: probe_table.internal_id,
            original_idx: probe_table_idx,
            is_outer: probe_table
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.is_outer()),
        });
    }
    let probe_pos = join_order
        .iter()
        .position(|m| m.original_idx == probe_table_idx)
        .expect("probe table just ensured in join order");

    // Only include tables prior to the probe table. The materialization subplan
    // should filter the build table using prior join constraints, not scan the probe.
    let mut prefix_join_order = join_order[..probe_pos].to_vec();
    if prefix_join_order
        .iter()
        .all(|member| member.original_idx != build_table_idx)
    {
        let build_table = &plan.table_references.joined_tables()[build_table_idx];
        prefix_join_order.push(JoinOrderMember {
            table_id: build_table.internal_id,
            original_idx: build_table_idx,
            is_outer: build_table
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.is_outer()),
        });
    }

    let mut included_tables: TableMask = prefix_join_order
        .iter()
        .map(|m| m.original_idx)
        .try_collect()?;
    for member in prefix_join_order.iter() {
        let table_ref = &plan.table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table_ref.op {
            included_tables.set(hash_join_op.build_table_idx)?;
        }
    }
    Ok((prefix_join_order, included_tables))
}

/// Collect the payload columns needed for a materialized build input.
///
/// This gathers referenced columns from the included tables and always adds
/// rowids for tables that have them so probe-side expressions can be satisfied
/// without seeking back into base tables.
fn collect_materialized_payload_columns(
    plan: &SelectPlan,
    included_tables: &TableMask,
) -> Result<Vec<MaterializedColumnRef>> {
    let mut payload_columns: Vec<MaterializedColumnRef> = Vec::new();
    let mut seen: HashSet<MaterializedColumnRef> = HashSet::default();
    for table_idx in included_tables.iter() {
        let table = &plan.table_references.joined_tables()[table_idx];
        for col_idx in table.col_used_mask.iter() {
            let is_rowid_alias = table
                .columns()
                .get(col_idx)
                .is_some_and(|col| col.is_rowid_alias());
            let col_ref = MaterializedColumnRef::Column {
                table_id: table.internal_id,
                column_idx: col_idx,
                is_rowid_alias,
            };
            if seen.insert(col_ref.clone()) {
                payload_columns.push(col_ref);
            }
        }
        if table.btree().is_some_and(|btree| btree.has_rowid) {
            let rowid_ref = MaterializedColumnRef::RowId {
                table_id: table.internal_id,
            };
            if seen.insert(rowid_ref.clone()) {
                payload_columns.push(rowid_ref);
            }
        }
    }
    Ok(payload_columns)
}

/// Build the ephemeral-table schema for key+payload materializations.
///
/// Keys are stored first (typed as BLOB for join-key affinity handling),
/// followed by payload columns with integer or blob affinity.
fn build_materialized_input_columns(
    num_keys: usize,
    payload_columns: &[MaterializedColumnRef],
) -> Result<crate::alloc::Vec<Column>> {
    Ok((0..num_keys)
        .map(|i| Column::new_default_text(Some(format!("key_{i}")), "BLOB".to_string(), None))
        .chain(payload_columns.iter().enumerate().map(|(i, payload)| {
            let name = Some(format!("payload_{i}"));
            match payload {
                MaterializedColumnRef::RowId { .. } => {
                    Column::new_default_integer(name, "INTEGER".to_string(), None)
                }
                MaterializedColumnRef::Column { .. } => {
                    Column::new_default_text(name, "BLOB".to_string(), None)
                }
            }
        }))
        .try_collect()?)
}

/// Construct a SELECT plan that materializes build-side inputs into an ephemeral table.
/// This plan is separate from the main query plan and is exclusively used for the materialization.
/// process.
///
/// The join order is the original prefix up to (but excluding) the probe table, plus
/// the build table itself. This filters build rows using only prior join constraints
/// and then prunes any tables already captured by earlier key+payload materializations.
#[allow(clippy::too_many_arguments)]
fn build_materialized_build_input_plan(
    plan: &SelectPlan,
    build_table_idx: usize,
    probe_table_idx: usize,
    cursor_id: CursorID,
    table: Arc<BTreeTable>,
    mode: &MaterializedBuildInputMode,
    key_exprs: &[Expr],
    payload_columns: &[MaterializedColumnRef],
    materialized_build_inputs: &HashMap<usize, MaterializedBuildInput>,
) -> Result<SelectPlan> {
    // Build a materialization subplan that only includes the join prefix
    // (all tables prior to the probe + the build table). The resulting plan
    // is smaller than the original select plan, so any access methods or
    // predicates that depend on tables outside this prefix must be dropped.
    let (join_order, included_tables) =
        materialization_prefix(plan, build_table_idx, probe_table_idx)?;
    // Bitmask of tables that are actually in the prefix join order for
    // this materialization subplan. Anything that depends on other tables
    // cannot be evaluated during those table scans.
    let join_prefix_mask: TableMask = join_order.iter().map(|m| m.original_idx).try_collect()?;

    // Clone WHERE terms for the materialization subplan. We cannot reuse the
    // parent plan's consumed flags because the optimizer may have consumed
    // terms for access methods (e.g. ephemeral autoindex seeks) that get
    // overwritten to scans inside the subplan. Reset each term's consumed
    // flag: only terms referencing tables outside the prefix are consumed.
    let mut where_clause = plan.where_clause.clone();
    for term in where_clause.iter_mut() {
        let mask = table_mask_from_expr(
            &term.expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
        // Expressions can also reference build tables of earlier hash joins in this subplan,
        // because those tables are available during probe loops. Use the broader "included"
        // set when deciding which WHERE terms can be evaluated inside the materialization.
        term.consumed = !included_tables.contains_all_set_bits_of(&mask);
    }

    // Clone table references and then "sanitize" each access method so that
    // the materialization subplan does not try to use an access path that
    // requires tables outside the prefix. If it does, we fall back to a scan.
    let mut table_references = plan.table_references.clone();
    for joined_table in table_references.joined_tables_mut().iter_mut() {
        if let Operation::HashJoin(hash_join_op) = &mut joined_table.op {
            if hash_join_op.build_table_idx == build_table_idx {
                // Avoid recursive materialization and disable the hash join for the build table
                // so it can be accessed using the join constraints.
                hash_join_op.materialize_build_input = false;
                joined_table.op = Operation::default_scan_for(&joined_table.table);
            } else if hash_join_op.probe_table_idx == probe_table_idx {
                // The probe table is not part of the materialization prefix, so
                // disable hash joins anchored on it.
                joined_table.op = Operation::default_scan_for(&joined_table.table);
            }
        }
    }

    // Helper to decide whether an expression depends on tables outside
    // the prefix. If it does, any access method that relies on that
    // expression must be invalidated for the materialization subplan.
    let expr_depends_outside_prefix = |expr: &Expr| -> Result<bool> {
        let mask = table_mask_from_expr(
            expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
        Ok(!join_prefix_mask.contains_all_set_bits_of(&mask))
    };

    // Walk each table in the cloned plan and ensure its access method is
    // valid within the prefix. If the access method depends on tables
    // outside the prefix, downgrade to a plain scan.
    for (table_idx, joined_table) in table_references.joined_tables_mut().iter_mut().enumerate() {
        if !join_prefix_mask.get(table_idx) {
            continue;
        }

        let mut reset_op = false;
        match &joined_table.op {
            Operation::Search(Search::RowidEq { cmp_expr }) => {
                // Rowid equality searches may depend on other tables (e.g. column = other.col).
                reset_op = expr_depends_outside_prefix(cmp_expr)?;
            }
            Operation::Search(Search::Seek { seek_def, .. }) => {
                // Seek keys can include expressions bound by other tables. If so,
                // the seek is not valid in the prefix-only subplan.
                for component in seek_def.iter(&seek_def.start) {
                    if let SeekKeyComponent::Expr(expr) = component {
                        if expr_depends_outside_prefix(expr)? {
                            reset_op = true;
                            break;
                        }
                    }
                }
                if !reset_op {
                    for component in seek_def.iter(&seek_def.end) {
                        if let SeekKeyComponent::Expr(expr) = component {
                            if expr_depends_outside_prefix(expr)? {
                                reset_op = true;
                                break;
                            }
                        }
                    }
                }
            }
            Operation::IndexMethodQuery(IndexMethodQuery { arguments, .. }) => {
                // Index method queries are driven by argument expressions.
                // If any argument depends on non-prefix tables, we cannot use it.
                for expr in arguments {
                    if expr_depends_outside_prefix(expr)? {
                        reset_op = true;
                        break;
                    }
                }
            }
            Operation::Scan(Scan::VirtualTable { constraints, .. }) => {
                // Virtual table constraints are evaluated against expressions.
                // If any constraint depends on non-prefix tables, drop the scan
                // specialization and fall back to a full scan.
                for expr in constraints {
                    if expr_depends_outside_prefix(expr)? {
                        reset_op = true;
                        break;
                    }
                }
            }
            Operation::HashJoin(hash_join_op) => {
                // Hash joins are driven by the probe table's loop. That probe table
                // must be in the prefix; otherwise the hash join cannot be evaluated
                // inside this subplan. The build table may live outside the prefix
                // because the hash build phase scans it independently.
                if !join_prefix_mask.get(hash_join_op.probe_table_idx) {
                    reset_op = true;
                }
            }
            _ => {}
        }

        if reset_op {
            // Downgrade to a default scan. This ensures the subplan only uses
            // access paths that are valid within the prefix join order.
            joined_table.op = Operation::default_scan_for(&joined_table.table);
        }
    }

    let build_internal_id = plan.table_references.joined_tables()[build_table_idx].internal_id;
    let result_columns = match mode {
        MaterializedBuildInputMode::RowidOnly => vec![ResultSetColumn {
            expr: Expr::RowId {
                database: None,
                table: build_internal_id,
            },
            alias: None,
            implicit_column_name: None,
            contains_aggregates: false,
        }],
        MaterializedBuildInputMode::KeyPayload { num_keys, .. } => {
            turso_assert!(
                *num_keys == key_exprs.len(),
                "materialized hash build input key count mismatch"
            );
            let mut result_columns: Vec<ResultSetColumn> = Vec::new();
            for expr in key_exprs.iter() {
                result_columns.push(ResultSetColumn {
                    expr: expr.clone(),
                    alias: None,
                    implicit_column_name: None,
                    contains_aggregates: false,
                });
            }
            for payload in payload_columns.iter() {
                let expr = match payload {
                    MaterializedColumnRef::Column {
                        table_id,
                        column_idx,
                        is_rowid_alias,
                    } => Expr::Column {
                        database: None,
                        table: *table_id,
                        column: *column_idx,
                        is_rowid_alias: *is_rowid_alias,
                    },
                    MaterializedColumnRef::RowId { table_id } => Expr::RowId {
                        database: None,
                        table: *table_id,
                    },
                };
                result_columns.push(ResultSetColumn {
                    expr,
                    alias: None,
                    implicit_column_name: None,
                    contains_aggregates: false,
                });
            }
            result_columns
        }
    };

    let mut materialize_plan = SelectPlan {
        table_references,
        join_order,
        result_columns,
        where_clause,
        group_by: None,
        order_by: vec![],
        aggregates: vec![],
        limit: None,
        offset: None,
        contains_constant_false_condition: false,
        query_destination: QueryDestination::EphemeralTable {
            cursor_id,
            table,
            rowid_mode: match mode {
                MaterializedBuildInputMode::RowidOnly => EphemeralRowidMode::FromResultColumns,
                MaterializedBuildInputMode::KeyPayload { .. } => EphemeralRowidMode::Auto,
            },
        },
        distinctness: Distinctness::NonDistinct,
        values: vec![],
        window: None,
        non_from_clause_subqueries: plan.non_from_clause_subqueries.clone(),
        input_cardinality_hint: None,
        estimated_output_rows: None,
        simple_aggregate: None,
        phantom_params: vec![],
    };

    prune_join_order_for_materialized_inputs(&mut materialize_plan, materialized_build_inputs)?;

    Ok(materialize_plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{io::MemoryIO, types::Value, Database, SqliteDialect};

    #[test]
    fn rowid_equality_uses_a_declarative_point_stream() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE point(id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        connection
            .execute("INSERT INTO point VALUES (1, 'one'), (2, 'two')")
            .unwrap();

        let mut statement = connection
            .prepare("SELECT rowid, id, payload FROM point WHERE rowid = ?1 + 0 LIMIT 1")
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert_eq!(
            instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
                .count(),
            1
        );
        assert!(instructions.iter().all(|(instruction, _)| !matches!(
            instruction,
            Insn::Rewind { .. } | Insn::Last { .. } | Insn::Next { .. } | Insn::Prev { .. }
        )));
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("point stream must produce its projected row");
        assert!(
            instructions[result_row - 3..result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "point-stream projections must remain symbolic until lowering"
        );

        statement
            .bind_at(1.try_into().unwrap(), Value::from_i64(2))
            .unwrap();
        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![vec![
                Value::from_i64(2),
                Value::from_i64(2),
                Value::from_text("two"),
            ]]
        );

        statement.reset().unwrap();
        statement
            .bind_at(1.try_into().unwrap(), Value::from_i64(99))
            .unwrap();
        assert!(statement.run_collect_rows().unwrap().is_empty());

        statement.reset().unwrap();
        statement
            .bind_at(1.try_into().unwrap(), Value::Null)
            .unwrap();
        assert!(statement.run_collect_rows().unwrap().is_empty());
    }

    #[test]
    fn literal_rowid_in_uses_a_declarative_nested_stream() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE in_rows(id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO in_rows VALUES \
                 (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT id, payload FROM in_rows \
                 WHERE id IN (5, '1', 1, NULL, 3) LIMIT 2 OFFSET 1",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert_eq!(
            instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
                .count(),
            1
        );
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::SorterOpen { .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. })));
        assert!(instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::MakeRecord {
                affinity_str: Some(affinity),
                ..
            } if affinity == "D"
        )));
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. }))
            .expect("declarative IN stream must produce its projected row pack");
        assert!(instructions[result_row - 2..result_row]
            .iter()
            .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })));

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(3), Value::from_text("three")],
                vec![Value::from_i64(5), Value::from_text("five")],
            ]
        );
    }

    #[test]
    fn uncorrelated_in_subquery_composes_declarative_producer_and_consumer() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE subquery_rows(id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        connection
            .execute("CREATE TABLE subquery_keys(id)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO subquery_rows VALUES \
                 (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')",
            )
            .unwrap();
        connection
            .execute("INSERT INTO subquery_keys VALUES (5), (1), (1), (NULL), (3)")
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT id, payload FROM subquery_rows \
                 WHERE id IN (SELECT id FROM subquery_keys) LIMIT 2 OFFSET 1",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert_eq!(
            instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
                .count(),
            1
        );
        assert!(instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::Once { .. })));
        let (open_ephemeral, ephemeral_cursor) = instructions
            .iter()
            .enumerate()
            .find_map(|(position, (instruction, _))| match instruction {
                Insn::OpenEphemeral {
                    cursor_id,
                    is_table: false,
                } => Some((position, *cursor_id)),
                _ => None,
            })
            .expect("composed IN compiler must own its ephemeral index");
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. })));
        assert!(instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::SorterOpen { .. })));
        let materialize_record = instructions
            .iter()
            .position(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::MakeRecord {
                        index_name: Some(name),
                        ..
                    } if name.starts_with("ephemeral_index_where_sub_")
                )
            })
            .expect("declarative subquery producer must materialize its projected pack");
        let insert = materialize_record + 1;
        assert!(matches!(
            instructions[insert].0,
            Insn::IdxInsert { cursor_id, .. } if cursor_id == ephemeral_cursor
        ));
        let consume = instructions
            .iter()
            .enumerate()
            .skip(insert + 1)
            .find_map(|(position, (instruction, _))| match instruction {
                Insn::Rewind { cursor_id, .. } if *cursor_id == ephemeral_cursor => Some(position),
                _ => None,
            })
            .expect("composed IN consumer must scan the producer's ephemeral index");
        assert!(open_ephemeral < materialize_record && insert < consume);
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. }))
            .expect("declarative subquery IN stream must produce its projected row pack");
        assert!(instructions[result_row - 2..result_row]
            .iter()
            .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })));

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(3), Value::from_text("three")],
                vec![Value::from_i64(5), Value::from_text("five")],
            ]
        );

        let mut nested = connection
            .prepare(
                "SELECT id FROM subquery_rows WHERE id IN (\
                     SELECT id FROM subquery_rows WHERE id IN (\
                         SELECT id FROM subquery_keys\
                     )\
                 ) ORDER BY id",
            )
            .unwrap();
        let nested_instructions = &nested.get_program().insns;
        assert!(nested_instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::Once { .. })));
        let ephemeral_cursors = nested_instructions
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::OpenEphemeral {
                    cursor_id,
                    is_table: false,
                } => Some(*cursor_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(ephemeral_cursors.len(), 2);
        let destination_cursor = ephemeral_cursors[0];
        let source_cursor = ephemeral_cursors[1];
        let destination_open = nested_instructions
            .iter()
            .position(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::OpenEphemeral { cursor_id, .. } if *cursor_id == destination_cursor
                )
            })
            .unwrap();
        let source_open = nested_instructions
            .iter()
            .position(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::OpenEphemeral { cursor_id, .. } if *cursor_id == source_cursor
                )
            })
            .unwrap();
        let source_insert = nested_instructions
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::IdxInsert { cursor_id, .. } if *cursor_id == source_cursor)
            })
            .expect("nested producer must fill its source index");
        assert!(nested_instructions.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::Rewind { cursor_id, .. } if *cursor_id == source_cursor)
        }));
        let destination_insert = nested_instructions
            .iter()
            .enumerate()
            .find_map(|(position, (instruction, _))| {
                matches!(instruction, Insn::IdxInsert { cursor_id, .. } if *cursor_id == destination_cursor)
                    .then_some(position)
            })
            .expect("nested declarative producer must fill its distinct destination cursor");
        assert!(nested_instructions.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::Rewind { cursor_id, .. } if *cursor_id == destination_cursor)
        }));
        assert!(source_open < source_insert && destination_open < destination_insert);
        assert_eq!(
            nested.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1)],
                vec![Value::from_i64(3)],
                vec![Value::from_i64(5)],
            ]
        );

        let mut recursively_nested = connection
            .prepare(
                "SELECT id FROM subquery_rows WHERE id IN (\
                     SELECT id FROM subquery_rows WHERE id IN (\
                         SELECT id FROM subquery_rows WHERE id IN (\
                             SELECT id FROM subquery_keys\
                         )\
                     )\
                 ) ORDER BY id",
            )
            .unwrap();
        let recursive_instructions = &recursively_nested.get_program().insns;
        assert!(recursive_instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::Once { .. })));
        let recursive_ephemeral_cursors = recursive_instructions
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::OpenEphemeral {
                    cursor_id,
                    is_table: false,
                } => Some(*cursor_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(recursive_ephemeral_cursors.len(), 3);
        for cursor in recursive_ephemeral_cursors {
            assert!(recursive_instructions.iter().any(|(instruction, _)| {
                matches!(instruction, Insn::IdxInsert { cursor_id, .. } if *cursor_id == cursor)
            }));
            assert!(recursive_instructions.iter().any(|(instruction, _)| {
                matches!(instruction, Insn::Rewind { cursor_id, .. } if *cursor_id == cursor)
            }));
        }
        assert_eq!(
            recursively_nested.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1)],
                vec![Value::from_i64(3)],
                vec![Value::from_i64(5)],
            ]
        );

        let mut partially_supported = connection
            .prepare(
                "SELECT id FROM subquery_rows WHERE id IN (\
                     SELECT id FROM subquery_rows WHERE id IN (\
                         SELECT max(id) FROM subquery_keys\
                     )\
                 ) ORDER BY id",
            )
            .unwrap();
        assert!(partially_supported
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Once { .. })));
        assert_eq!(
            partially_supported.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(5)]]
        );

        let mut correlated = connection
            .prepare(
                "SELECT outer_row.id FROM subquery_rows AS outer_row \
                 WHERE outer_row.id IN (\
                     SELECT key_row.id FROM subquery_keys AS key_row \
                     WHERE key_row.id <= outer_row.id\
                 ) ORDER BY outer_row.id",
            )
            .unwrap();
        let correlated_instructions = &correlated.get_program().insns;
        let materialize_record = correlated_instructions
            .iter()
            .position(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::MakeRecord {
                        index_name: Some(name),
                        ..
                    } if name.starts_with("ephemeral_index_where_sub_")
                )
            })
            .expect("correlated eager fallback must materialize its projected row");
        assert!(!matches!(
            correlated_instructions[materialize_record - 1].0,
            Insn::Copy { .. }
        ));
        let result_row = correlated_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. }))
            .expect("correlated eager fallback must produce its projected row");
        assert!(!matches!(
            correlated_instructions[result_row - 1].0,
            Insn::Copy { .. }
        ));
        assert_eq!(
            correlated.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1)],
                vec![Value::from_i64(3)],
                vec![Value::from_i64(5)],
            ]
        );
    }

    #[test]
    fn mixed_scalar_and_in_dependencies_compose_in_one_ordered_pipeline() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE mixed_rows(id INTEGER PRIMARY KEY)")
            .unwrap();
        connection
            .execute("CREATE TABLE mixed_keys(id INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE mixed_bias(value INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE mixed_bound(value INTEGER)")
            .unwrap();
        connection
            .execute("INSERT INTO mixed_rows VALUES (1), (2), (3), (4), (5)")
            .unwrap();
        connection
            .execute("INSERT INTO mixed_keys VALUES (5), (1), (3)")
            .unwrap();
        connection
            .execute("INSERT INTO mixed_bias VALUES (10)")
            .unwrap();
        connection
            .execute("INSERT INTO mixed_bound VALUES (5), (4)")
            .unwrap();

        let mut composed = connection
            .prepare(
                "SELECT id, id + (SELECT value FROM mixed_bias) \
                 FROM mixed_rows \
                 WHERE id IN (SELECT id FROM mixed_keys) \
                   AND id < (SELECT value FROM mixed_bound)",
            )
            .unwrap();
        let instructions = &composed.get_program().insns;
        assert!(instructions.iter().all(|(instruction, _)| !matches!(
            instruction,
            Insn::BeginSubrtn { .. } | Insn::Return { .. } | Insn::Once { .. }
        )));
        assert_eq!(
            instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::OpenEphemeral { .. }))
                .count(),
            1
        );
        assert_eq!(
            composed.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(11)],
                vec![Value::from_i64(3), Value::from_i64(13)],
            ]
        );

        let mut nested_scalar_producer = connection
            .prepare(
                "SELECT id FROM mixed_rows WHERE id IN (\
                     SELECT id FROM mixed_keys \
                     WHERE id < (SELECT value FROM mixed_bound)\
                 ) ORDER BY id",
            )
            .unwrap();
        assert!(nested_scalar_producer.get_program().insns.iter().all(
            |(instruction, _)| !matches!(
                instruction,
                Insn::BeginSubrtn { .. } | Insn::Return { .. } | Insn::Once { .. }
            )
        ));
        assert_eq!(
            nested_scalar_producer.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(1)], vec![Value::from_i64(3)]],
        );

        let mut fallback = connection
            .prepare(
                "SELECT id, id + (SELECT value FROM mixed_bias) \
                 FROM mixed_rows \
                 WHERE id IN (SELECT id FROM mixed_keys) \
                   AND id < (SELECT value FROM mixed_bound ORDER BY value DESC)",
            )
            .unwrap();
        let fallback_instructions = &fallback.get_program().insns;
        assert!(fallback_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Once { .. })));
        assert_eq!(
            fallback_instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::BeginSubrtn { .. }))
                .count(),
            2,
            "an unsupported scalar must preserve every mixed eager dependency"
        );
        assert_eq!(
            fallback.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(11)],
                vec![Value::from_i64(3), Value::from_i64(13)],
            ]
        );
    }

    #[test]
    fn uncorrelated_exists_composes_a_symbolic_boolean_into_the_outer_scan() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE exists_outer(value INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE exists_inner(value INTEGER)")
            .unwrap();
        connection
            .execute("INSERT INTO exists_outer VALUES (1), (2), (3)")
            .unwrap();
        connection
            .execute("INSERT INTO exists_inner VALUES (10), (20)")
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT value FROM exists_outer \
                 WHERE EXISTS (SELECT 1 FROM exists_inner)",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(instructions.iter().all(|(instruction, _)| !matches!(
            instruction,
            Insn::BeginSubrtn { .. } | Insn::Return { .. } | Insn::Once { .. }
        )));
        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1)],
                vec![Value::from_i64(2)],
                vec![Value::from_i64(3)],
            ]
        );

        let mut nested_expression = connection
            .prepare(
                "SELECT value FROM exists_outer \
                 WHERE value + EXISTS (SELECT 1 FROM exists_inner) > 2",
            )
            .unwrap();
        assert!(nested_expression
            .get_program()
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(
                instruction,
                Insn::BeginSubrtn { .. } | Insn::Return { .. } | Insn::Once { .. }
            )));
        assert_eq!(
            nested_expression.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(2)], vec![Value::from_i64(3)]],
            "expression resolution must declare nested EXISTS inputs"
        );

        connection.execute("DELETE FROM exists_inner").unwrap();
        statement.reset().unwrap();
        assert!(statement.run_collect_rows().unwrap().is_empty());

        let mut aggregate_fallback = connection
            .prepare(
                "SELECT value FROM exists_outer \
                 WHERE EXISTS (SELECT max(value) FROM exists_inner)",
            )
            .unwrap();
        assert!(aggregate_fallback
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::BeginSubrtn { .. })));
        assert_eq!(
            aggregate_fallback.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1)],
                vec![Value::from_i64(2)],
                vec![Value::from_i64(3)],
            ],
            "an aggregate EXISTS child must remain available to the eager fallback"
        );

        let mut nested_aggregate_fallback = connection
            .prepare(
                "SELECT value FROM exists_outer \
                 WHERE value + EXISTS (SELECT max(value) FROM exists_inner) > 2",
            )
            .unwrap();
        assert!(nested_aggregate_fallback
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::BeginSubrtn { .. })));
        assert_eq!(
            nested_aggregate_fallback.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(2)], vec![Value::from_i64(3)]],
            "an unsupported nested dependency must return the whole expression to eager emission"
        );

        connection
            .execute("INSERT INTO exists_inner VALUES (10), (10)")
            .unwrap();
        let mut distinct_fallback = connection
            .prepare(
                "SELECT value FROM exists_outer WHERE EXISTS (\
                     SELECT DISTINCT value FROM exists_inner LIMIT -1 OFFSET 1\
                 )",
            )
            .unwrap();
        assert!(distinct_fallback
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::BeginSubrtn { .. })));
        assert!(distinct_fallback.run_collect_rows().unwrap().is_empty());
    }

    #[test]
    fn uncorrelated_scalar_subquery_composes_into_symbolic_expressions() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE scalar_outer(id INTEGER, value INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE scalar_inner(value INTEGER)")
            .unwrap();
        connection
            .execute("INSERT INTO scalar_outer VALUES (1, 2), (2, 3)")
            .unwrap();
        connection
            .execute("INSERT INTO scalar_inner VALUES (40), (99)")
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT id, value + (SELECT value FROM scalar_inner) \
                 FROM scalar_outer",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(instructions.iter().all(|(instruction, _)| !matches!(
            instruction,
            Insn::BeginSubrtn { .. } | Insn::Return { .. } | Insn::Once { .. }
        )));
        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(42)],
                vec![Value::from_i64(2), Value::from_i64(43)],
            ]
        );

        connection.execute("DELETE FROM scalar_inner").unwrap();
        statement.reset().unwrap();
        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::Null],
                vec![Value::from_i64(2), Value::Null],
            ],
            "an empty scalar stream must join through its NULL default"
        );

        connection
            .execute("INSERT INTO scalar_inner VALUES (40), (99)")
            .unwrap();
        let mut offset = connection
            .prepare(
                "SELECT id, (SELECT value FROM scalar_inner LIMIT 1 OFFSET 1) \
                 FROM scalar_outer",
            )
            .unwrap();
        assert!(offset
            .get_program()
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::BeginSubrtn { .. })));
        assert_eq!(
            offset.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(99)],
                vec![Value::from_i64(2), Value::from_i64(99)],
            ]
        );

        let mut ordered_fallback = connection
            .prepare(
                "SELECT id, (SELECT value FROM scalar_inner ORDER BY value DESC) \
                 FROM scalar_outer",
            )
            .unwrap();
        assert!(ordered_fallback
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::BeginSubrtn { .. })));
        assert_eq!(
            ordered_fallback.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(99)],
                vec![Value::from_i64(2), Value::from_i64(99)],
            ]
        );
    }

    #[test]
    fn sibling_scalar_subqueries_compose_together_or_fall_back_atomically() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE scalar_outer(id INTEGER, value INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE scalar_left(value INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE scalar_right(value INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE scalar_exists(value INTEGER)")
            .unwrap();
        connection
            .execute("INSERT INTO scalar_outer VALUES (1, 2), (2, 3)")
            .unwrap();
        connection
            .execute("INSERT INTO scalar_left VALUES (40), (99)")
            .unwrap();
        connection
            .execute("INSERT INTO scalar_right VALUES (7), (8)")
            .unwrap();
        connection
            .execute("INSERT INTO scalar_exists VALUES (1)")
            .unwrap();

        let mut composed = connection
            .prepare(
                "SELECT id, value + (SELECT value FROM scalar_left) \
                     + (SELECT value FROM scalar_right) \
                 FROM scalar_outer \
                 WHERE EXISTS (SELECT value FROM scalar_exists)",
            )
            .unwrap();
        assert!(composed
            .get_program()
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(
                instruction,
                Insn::BeginSubrtn { .. } | Insn::Return { .. } | Insn::Once { .. }
            )));
        assert_eq!(
            composed.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(49)],
                vec![Value::from_i64(2), Value::from_i64(50)],
            ]
        );

        let mut fallback = connection
            .prepare(
                "SELECT id, value + (SELECT value FROM scalar_left) \
                     + (SELECT value FROM scalar_right ORDER BY value DESC) \
                 FROM scalar_outer",
            )
            .unwrap();
        assert_eq!(
            fallback
                .get_program()
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::BeginSubrtn { .. }))
                .count(),
            2,
            "an unsupported sibling must preserve every eager subquery plan"
        );
        assert_eq!(
            fallback.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(50)],
                vec![Value::from_i64(2), Value::from_i64(51)],
            ]
        );
    }

    #[test]
    fn literal_index_in_preserves_target_duplicates_and_deduplicates_rhs_keys() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute(
                "CREATE TABLE in_indexed(\
                    id INTEGER PRIMARY KEY, key NUMERIC, payload TEXT\
                )",
            )
            .unwrap();
        connection
            .execute("CREATE INDEX in_indexed_key ON in_indexed(key)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO in_indexed VALUES \
                 (1, 1, 'one-a'), (2, 1, 'one-b'), (3, 2, 'two'), \
                 (4, 3, 'three-a'), (5, 3, 'three-b'), (6, 4, 'four')",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT id, payload FROM in_indexed \
                 WHERE key IN (3, '1', 3, NULL, 2) ORDER BY id",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(
            instructions.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::SeekGE {
                    is_index: true,
                    num_regs: 1,
                    ..
                }
            )),
            "literal index IN should lower to an exact seek: {instructions:#?}"
        );
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGT { num_regs: 1, .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::DeferredSeek { .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. })));
        assert!(instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::MakeRecord {
                affinity_str: Some(affinity),
                ..
            } if affinity == "C"
        )));

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_text("one-a")],
                vec![Value::from_i64(2), Value::from_text("one-b")],
                vec![Value::from_i64(3), Value::from_text("two")],
                vec![Value::from_i64(4), Value::from_text("three-a")],
                vec![Value::from_i64(5), Value::from_text("three-b")],
            ]
        );

        let mut eager_fallback = connection
            .prepare(
                "SELECT id FROM in_indexed \
                 WHERE key IN (abs(?1), 2) ORDER BY id",
            )
            .unwrap();
        let fallback_instructions = &eager_fallback.get_program().insns;
        assert!(fallback_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::OpenEphemeral { .. })));
        assert!(fallback_instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::HashDistinct { .. })));
        eager_fallback
            .bind_at(1.try_into().unwrap(), Value::from_i64(-3))
            .unwrap();
        assert_eq!(
            eager_fallback.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(3)],
                vec![Value::from_i64(4)],
                vec![Value::from_i64(5)],
            ]
        );
    }

    #[test]
    fn exact_composite_index_search_uses_a_declarative_range_stream() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute(
                "CREATE TABLE indexed(\
                    id INTEGER PRIMARY KEY, category TEXT, rank NUMERIC, payload TEXT\
                )",
            )
            .unwrap();
        connection
            .execute("CREATE INDEX indexed_category_rank ON indexed(category, rank)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO indexed VALUES \
                 (1, 'group', 1, 'one'), \
                 (2, 'group', 2, 'two'), \
                 (3, 'other', 2, 'other'), \
                 (4, 'group', NULL, 'null'), \
                 (5, 'group', 2, 'five')",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT id, payload FROM indexed INDEXED BY indexed_category_rank \
                 WHERE category = ?1 AND rank = ?2 LIMIT 1",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekGE {
                is_index: true,
                eq_only: true,
                num_regs: 2,
                ..
            }
        )));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGT { num_regs: 2, .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::DeferredSeek { .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Next { .. })));
        assert!(instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::Rewind { .. })));
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. }))
            .expect("exact index stream must produce its projected row");
        assert!(instructions[result_row - 2..result_row]
            .iter()
            .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })));

        statement
            .bind_at(1.try_into().unwrap(), Value::from_text("group"))
            .unwrap();
        statement
            .bind_at(2.try_into().unwrap(), Value::from_text("2"))
            .unwrap();
        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(2), Value::from_text("two")]]
        );

        statement.reset().unwrap();
        statement
            .bind_at(1.try_into().unwrap(), Value::from_text("missing"))
            .unwrap();
        statement
            .bind_at(2.try_into().unwrap(), Value::from_i64(2))
            .unwrap();
        assert!(statement.run_collect_rows().unwrap().is_empty());

        statement.reset().unwrap();
        statement
            .bind_at(1.try_into().unwrap(), Value::from_text("group"))
            .unwrap();
        statement
            .bind_at(2.try_into().unwrap(), Value::Null)
            .unwrap();
        assert!(statement.run_collect_rows().unwrap().is_empty());

        let mut reverse = connection
            .prepare(
                "SELECT id, payload FROM indexed INDEXED BY indexed_category_rank \
                 WHERE category = 'group' AND rank = 2 ORDER BY id DESC",
            )
            .unwrap();
        assert!(reverse
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(
                instruction,
                Insn::SeekLE {
                    is_index: true,
                    eq_only: true,
                    num_regs: 2,
                    ..
                }
            )));
        assert!(reverse
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxLT { num_regs: 2, .. })));
        assert!(reverse
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Prev { .. })));
        assert_eq!(
            reverse.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(5), Value::from_text("five")],
                vec![Value::from_i64(2), Value::from_text("two")],
            ]
        );
    }

    #[test]
    fn bounded_index_searches_use_declarative_range_endpoints() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute(
                "CREATE TABLE ranged(\
                    id INTEGER PRIMARY KEY, category TEXT, rank NUMERIC, payload TEXT\
                )",
            )
            .unwrap();
        connection
            .execute("CREATE INDEX ranged_category_rank ON ranged(category, rank)")
            .unwrap();
        connection
            .execute("CREATE INDEX ranged_category_rank_desc ON ranged(category, rank DESC)")
            .unwrap();
        connection
            .execute("CREATE INDEX ranged_rank ON ranged(rank)")
            .unwrap();
        connection
            .execute("CREATE INDEX ranged_rank_desc ON ranged(rank DESC)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO ranged VALUES \
                 (1, 'group', NULL, 'null'), \
                 (2, 'group', 1, 'one'), \
                 (3, 'group', 2, 'two-a'), \
                 (4, 'group', 2, 'two-b'), \
                 (5, 'group', 3, 'three'), \
                 (6, 'group', 4, 'four'), \
                 (7, 'other', 2, 'other')",
            )
            .unwrap();

        let mut two_sided = connection
            .prepare(
                "SELECT id FROM ranged INDEXED BY ranged_category_rank \
                 WHERE category = ?1 AND rank > ?2 AND rank <= ?3 ORDER BY id",
            )
            .unwrap();
        let instructions = &two_sided.get_program().insns;
        assert!(instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekGT {
                is_index: true,
                num_regs: 2,
                ..
            }
        )));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGT { num_regs: 2, .. })));
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. }))
            .expect("bounded index stream must produce its projected row");
        assert!(matches!(&instructions[result_row - 1].0, Insn::Copy { .. }));
        two_sided
            .bind_at(1.try_into().unwrap(), Value::from_text("group"))
            .unwrap();
        two_sided
            .bind_at(2.try_into().unwrap(), Value::from_text("1"))
            .unwrap();
        two_sided
            .bind_at(3.try_into().unwrap(), Value::from_text("3"))
            .unwrap();
        assert_eq!(
            two_sided.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(3)],
                vec![Value::from_i64(4)],
                vec![Value::from_i64(5)],
            ]
        );

        two_sided.reset().unwrap();
        two_sided
            .bind_at(1.try_into().unwrap(), Value::from_text("group"))
            .unwrap();
        two_sided
            .bind_at(2.try_into().unwrap(), Value::Null)
            .unwrap();
        two_sided
            .bind_at(3.try_into().unwrap(), Value::from_i64(3))
            .unwrap();
        assert!(two_sided.run_collect_rows().unwrap().is_empty());

        let mut prefix_upper = connection
            .prepare(
                "SELECT id FROM ranged INDEXED BY ranged_category_rank \
                 WHERE category = 'group' AND rank <= 2 ORDER BY id",
            )
            .unwrap();
        assert!(prefix_upper
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(
                instruction,
                Insn::SeekGT {
                    is_index: true,
                    num_regs: 2,
                    ..
                }
            )));
        assert_eq!(
            prefix_upper.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(2)],
                vec![Value::from_i64(3)],
                vec![Value::from_i64(4)],
            ]
        );

        let mut descending_upper = connection
            .prepare(
                "SELECT id FROM ranged INDEXED BY ranged_category_rank_desc \
                 WHERE category = 'group' AND rank <= 2 ORDER BY id",
            )
            .unwrap();
        assert!(descending_upper
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGE { num_regs: 2, .. })));
        assert_eq!(
            descending_upper.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(2)],
                vec![Value::from_i64(3)],
                vec![Value::from_i64(4)],
            ]
        );

        let mut no_end = connection
            .prepare(
                "SELECT id FROM ranged INDEXED BY ranged_rank \
                 WHERE rank > 2 ORDER BY rank, id",
            )
            .unwrap();
        assert!(no_end
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::SeekGT { num_regs: 1, .. })));
        assert!(no_end.get_program().insns.iter().all(|(instruction, _)| {
            !matches!(
                instruction,
                Insn::IdxGE { .. } | Insn::IdxGT { .. } | Insn::IdxLE { .. } | Insn::IdxLT { .. }
            )
        }));
        assert_eq!(
            no_end.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(5)], vec![Value::from_i64(6)]]
        );

        let mut no_end_limited = connection
            .prepare(
                "SELECT id FROM ranged INDEXED BY ranged_rank \
                 WHERE rank > 1 ORDER BY rank, id LIMIT 2 OFFSET 1",
            )
            .unwrap();
        assert_eq!(
            no_end_limited.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(4)], vec![Value::from_i64(7)]]
        );

        let mut no_start_seek = connection
            .prepare(
                "SELECT id FROM ranged INDEXED BY ranged_rank_desc \
                 WHERE rank > 2 ORDER BY rank DESC, id LIMIT 1",
            )
            .unwrap();
        assert!(no_start_seek
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Rewind { .. })));
        assert!(no_start_seek
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGE { num_regs: 1, .. })));
        assert_eq!(
            no_start_seek.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(6)]]
        );

        let mut no_start = connection
            .prepare(
                "SELECT id FROM ranged INDEXED BY ranged_rank \
                 WHERE rank < 3 ORDER BY id",
            )
            .unwrap();
        assert!(no_start
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::SeekGT { num_regs: 1, .. })));
        assert!(no_start
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGE { num_regs: 1, .. })));
        assert_eq!(
            no_start.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(2)],
                vec![Value::from_i64(3)],
                vec![Value::from_i64(4)],
                vec![Value::from_i64(7)],
            ]
        );

        let mut reverse = connection
            .prepare(
                "SELECT id FROM ranged INDEXED BY ranged_category_rank \
                 WHERE category = 'group' AND rank >= 2 AND rank < 4 \
                 ORDER BY rank DESC, id DESC",
            )
            .unwrap();
        let reverse_instructions = &reverse.get_program().insns;
        assert!(reverse_instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekLT {
                is_index: true,
                num_regs: 2,
                ..
            }
        )));
        assert!(reverse_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxLT { num_regs: 2, .. })));
        assert!(reverse_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Prev { .. })));
        assert_eq!(
            reverse.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(5)],
                vec![Value::from_i64(4)],
                vec![Value::from_i64(3)],
            ]
        );

        let mut eager_fallback = connection
            .prepare(
                "SELECT id FROM ranged INDEXED BY ranged_rank \
                 WHERE rank > abs(?1)",
            )
            .unwrap();
        let fallback_instructions = &eager_fallback.get_program().insns;
        let result_row = fallback_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. }))
            .expect("eager fallback must produce its projected row");
        assert!(!matches!(
            &fallback_instructions[result_row - 1].0,
            Insn::Copy { .. }
        ));
        eager_fallback
            .bind_at(1.try_into().unwrap(), Value::from_i64(-2))
            .unwrap();
        assert_eq!(
            eager_fallback.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(5)], vec![Value::from_i64(6)]]
        );
    }

    #[test]
    fn rowid_ranges_use_declarative_table_endpoints() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE rowid_ranged(id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO rowid_ranged VALUES \
                 (1, 'one'), (2, 'two'), (3, 'three'), \
                 (4, 'four'), (5, 'five'), (6, 'six')",
            )
            .unwrap();

        let mut forward = connection
            .prepare(
                "SELECT id, payload FROM rowid_ranged \
                 WHERE id > ?1 AND id <= ?2 ORDER BY id LIMIT 2 OFFSET 1",
            )
            .unwrap();
        let instructions = &forward.get_program().insns;
        assert!(instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekGT {
                is_index: false,
                num_regs: 1,
                ..
            }
        )));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Gt { .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Next { .. })));
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. }))
            .expect("bounded rowid stream must produce its projected row pack");
        assert!(instructions[result_row - 2..result_row]
            .iter()
            .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })));

        forward
            .bind_at(1.try_into().unwrap(), Value::from_text("1"))
            .unwrap();
        forward
            .bind_at(2.try_into().unwrap(), Value::from_text("5"))
            .unwrap();
        assert_eq!(
            forward.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(3), Value::from_text("three")],
                vec![Value::from_i64(4), Value::from_text("four")],
            ]
        );
        forward.reset().unwrap();
        forward.bind_at(1.try_into().unwrap(), Value::Null).unwrap();
        forward
            .bind_at(2.try_into().unwrap(), Value::from_i64(5))
            .unwrap();
        assert!(forward.run_collect_rows().unwrap().is_empty());

        let mut reverse = connection
            .prepare(
                "SELECT id FROM rowid_ranged \
                 WHERE id >= 2 AND id < 6 ORDER BY id DESC",
            )
            .unwrap();
        let reverse_instructions = &reverse.get_program().insns;
        assert!(reverse_instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekLT {
                is_index: false,
                num_regs: 1,
                ..
            }
        )));
        assert!(reverse_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Lt { .. })));
        assert!(reverse_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Prev { .. })));
        assert_eq!(
            reverse.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(5)],
                vec![Value::from_i64(4)],
                vec![Value::from_i64(3)],
                vec![Value::from_i64(2)],
            ]
        );

        let mut no_start = connection
            .prepare("SELECT id FROM rowid_ranged WHERE id < 4 ORDER BY id")
            .unwrap();
        assert!(no_start
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Rewind { .. })));
        assert!(no_start
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Ge { .. })));
        assert_eq!(
            no_start.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1)],
                vec![Value::from_i64(2)],
                vec![Value::from_i64(3)],
            ]
        );

        let mut no_end = connection
            .prepare("SELECT id FROM rowid_ranged WHERE id > 3 ORDER BY id")
            .unwrap();
        assert!(no_end
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(
                instruction,
                Insn::SeekGT {
                    is_index: false,
                    ..
                }
            )));
        assert_eq!(
            no_end.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(4)],
                vec![Value::from_i64(5)],
                vec![Value::from_i64(6)],
            ]
        );

        let mut eager_fallback = connection
            .prepare("SELECT id FROM rowid_ranged WHERE id > abs(?1)")
            .unwrap();
        let fallback_instructions = &eager_fallback.get_program().insns;
        let result_row = fallback_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. }))
            .expect("eager fallback must produce its projected row");
        assert!(!matches!(
            fallback_instructions[result_row - 1].0,
            Insn::Copy { .. }
        ));
        eager_fallback
            .bind_at(1.try_into().unwrap(), Value::from_i64(-3))
            .unwrap();
        assert_eq!(
            eager_fallback.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(4)],
                vec![Value::from_i64(5)],
                vec![Value::from_i64(6)],
            ]
        );
    }

    #[test]
    fn simple_table_scan_crosses_the_declarative_compiler_boundary() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        assert!(connection
            .prepare("SELECT id, name FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .is_empty());
        connection
            .execute("INSERT INTO t VALUES (1, 'one')")
            .unwrap();
        connection
            .execute("ALTER TABLE t ADD COLUMN score INTEGER DEFAULT 7")
            .unwrap();

        let mut statement = connection.prepare("SELECT id, name, score FROM t").unwrap();
        let instructions = &statement.get_program().insns;
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("table scan must produce its projected row");
        assert!(
            instructions[result_row - 3..result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "symbolic projection values must be packed only during IR lowering"
        );

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![vec![
                Value::from_i64(1),
                Value::Text("one".into()),
                Value::from_i64(7),
            ]]
        );

        let query_plan_rows = connection
            .prepare("EXPLAIN QUERY PLAN SELECT id, name, score FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert!(
            !query_plan_rows.is_empty(),
            "EXPLAIN QUERY PLAN must retain the eager emitter's explain tree"
        );

        connection
            .execute("CREATE TABLE filtered(flag INTEGER, name TEXT)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO filtered VALUES (0, 'zero'), (1, 'one'), (NULL, 'null'), (2, 'two')",
            )
            .unwrap();
        let mut filtered_statement = connection
            .prepare("SELECT name FROM filtered WHERE flag")
            .unwrap();
        let filtered_instructions = &filtered_statement.get_program().insns;
        let filtered_result_row = filtered_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. }))
            .expect("filtered stream must produce its projected row");
        assert!(matches!(
            filtered_instructions[filtered_result_row - 1].0,
            Insn::Copy { .. }
        ));
        assert_eq!(
            filtered_statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::Text("one".into())],
                vec![Value::Text("two".into())],
            ]
        );

        connection
            .execute("CREATE TABLE compared(n NUMERIC, name TEXT COLLATE NOCASE)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO compared VALUES \
                 (2, 'Alpha'), (10, 'beta'), (NULL, 'BETA'), (20, NULL)",
            )
            .unwrap();
        let mut compared_statement = connection
            .prepare("SELECT name FROM compared WHERE n >= '10' AND name = 'BETA'")
            .unwrap();
        let compared_instructions = &compared_statement.get_program().insns;
        assert!(compared_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Ge { .. })));
        assert!(compared_instructions
            .iter()
            .any(|(instruction, _)| matches!(
                instruction,
                Insn::Eq {
                    collation: Some(crate::translate::collate::CollationSeq::NoCase),
                    ..
                }
            )));
        assert!(
            compared_instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::IsNull { .. }))
                .count()
                >= 4,
            "each symbolic comparison must retain an explicit NULL edge"
        );
        assert_eq!(
            compared_statement.run_collect_rows().unwrap(),
            vec![vec![Value::Text("beta".into())]]
        );

        let mut null_safe_statement = connection
            .prepare(
                "SELECT n IS NULL, n IS NOT NULL, name FROM compared \
                 WHERE name IS NULL OR n IS NULL",
            )
            .unwrap();
        let null_safe_instructions = &null_safe_statement.get_program().insns;
        let null_safe_result_row = null_safe_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("NULL-safe declarative projection must produce its row");
        assert!(
            null_safe_instructions[null_safe_result_row - 3..null_safe_result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "NULL-safe expressions must remain symbolic until result-pack lowering"
        );
        assert!(null_safe_instructions
            .iter()
            .any(|(instruction, _)| matches!(
                instruction,
                Insn::Eq { flags, .. } if flags.has_nulleq()
            )));
        assert!(null_safe_instructions
            .iter()
            .any(|(instruction, _)| matches!(
                instruction,
                Insn::Ne { flags, .. } if flags.has_nulleq()
            )));
        assert!(null_safe_instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::IsNull { .. })));
        assert_eq!(
            null_safe_statement.run_collect_rows().unwrap(),
            vec![
                vec![
                    Value::from_i64(1),
                    Value::from_i64(0),
                    Value::Text("BETA".into()),
                ],
                vec![Value::from_i64(0), Value::from_i64(1), Value::Null],
            ]
        );

        connection
            .execute("CREATE TABLE expressions(a NUMERIC, b NUMERIC, name TEXT COLLATE NOCASE)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO expressions VALUES \
                 (1, 2, 'alpha'), (2, 2, 'beta'), (NULL, 4, 'BETA'), (5, -1, NULL), \
                 (7, 0, 'tail')",
            )
            .unwrap();
        let mut expression_statement = connection
            .prepare(
                "SELECT a + 1, 'tag', a > b, a + b, name COLLATE NOCASE FROM expressions \
                 WHERE a + b >= 4 AND name = 'BETA'",
            )
            .unwrap();
        let expression_instructions = &expression_statement.get_program().insns;
        assert!(
            expression_instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Add { .. }))
                .count()
                >= 3,
            "projection and predicate additions must both use symbolic scalar expressions"
        );
        let expression_result_row = expression_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 5, .. }))
            .expect("resolved scalar projections must produce one five-value pack");
        assert!(
            expression_instructions[expression_result_row - 5..expression_result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "independent expression values must be packed only during lowering"
        );
        assert_eq!(
            expression_statement.run_collect_rows().unwrap(),
            vec![vec![
                Value::from_i64(3),
                Value::Text("tag".into()),
                Value::from_i64(0),
                Value::from_i64(4),
                Value::Text("beta".into()),
            ]]
        );
        assert!(connection
            .prepare("SELECT a COLLATE missing_collation FROM expressions")
            .is_err());

        let mut arithmetic_statement = connection
            .prepare(
                "SELECT a - b, a * b, a / b, a % b, \
                        a & b, a | b, a << 1, a >> 1, (a - b) * (b + 1) \
                   FROM expressions \
                  WHERE name = 'BETA'",
            )
            .unwrap();
        let arithmetic_instructions = &arithmetic_statement.get_program().insns;
        let arithmetic_result_row = arithmetic_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 9, .. }))
            .expect("arithmetic projections must produce one nine-value pack");
        assert!(
            arithmetic_instructions[arithmetic_result_row - 9..arithmetic_result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "arithmetic values must remain symbolic until result-pack lowering"
        );
        assert_eq!(
            arithmetic_statement.run_collect_rows().unwrap(),
            vec![
                vec![
                    Value::from_i64(0),
                    Value::from_i64(4),
                    Value::from_i64(1),
                    Value::from_i64(0),
                    Value::from_i64(2),
                    Value::from_i64(2),
                    Value::from_i64(4),
                    Value::from_i64(1),
                    Value::from_i64(0),
                ],
                vec![Value::Null; 9],
            ]
        );

        let mut parameter_statement = connection
            .prepare(
                "SELECT a + ?1, a >= ?2, :name FROM expressions \
                 WHERE a + b >= ?2 AND name = :name",
            )
            .unwrap();
        assert_eq!(parameter_statement.parameters_count(), 3);
        assert_eq!(
            parameter_statement.parameter_index(":name").unwrap().get(),
            3
        );
        assert!(
            parameter_statement
                .get_program()
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Variable { .. }))
                .count()
                >= 5,
            "each symbolic parameter use must lower through the VDBE variable instruction"
        );
        parameter_statement
            .bind_at(1.try_into().unwrap(), Value::from_i64(10))
            .unwrap();
        parameter_statement
            .bind_at(2.try_into().unwrap(), Value::from_i64(4))
            .unwrap();
        let name_parameter = parameter_statement.parameter_index(":name").unwrap();
        parameter_statement
            .bind_at(name_parameter, Value::from_text("BETA"))
            .unwrap();
        assert_eq!(
            parameter_statement.run_collect_rows().unwrap(),
            vec![vec![
                Value::from_i64(12),
                Value::from_i64(0),
                Value::from_text("BETA"),
            ]]
        );

        let mut limited_statement = connection
            .prepare(
                "SELECT a + 1, name FROM expressions \
                 WHERE a + b >= 4 LIMIT 2",
            )
            .unwrap();
        assert!(limited_statement
            .get_program()
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::DecrJumpZero { .. })));
        assert_eq!(
            limited_statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(3), Value::from_text("beta")],
                vec![Value::from_i64(6), Value::Null],
            ]
        );
        assert!(connection
            .prepare("SELECT a FROM expressions LIMIT 0")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .is_empty());

        let mut dynamic_limit = connection
            .prepare("SELECT a FROM expressions LIMIT ?1")
            .unwrap();
        assert!(dynamic_limit
            .get_program()
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::DecrJumpZero { .. })));
        assert!(dynamic_limit
            .get_program()
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::MustBeInt { .. })));
        dynamic_limit
            .bind_at(1.try_into().unwrap(), Value::from_i64(0))
            .unwrap();
        assert!(dynamic_limit.run_collect_rows().unwrap().is_empty());
        dynamic_limit.reset().unwrap();
        dynamic_limit
            .bind_at(1.try_into().unwrap(), Value::from_text("2"))
            .unwrap();
        assert_eq!(
            dynamic_limit.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
        );
        dynamic_limit.reset().unwrap();
        dynamic_limit
            .bind_at(1.try_into().unwrap(), Value::from_i64(-1))
            .unwrap();
        assert_eq!(dynamic_limit.run_collect_rows().unwrap().len(), 5);

        let mut expression_limit = connection
            .prepare("SELECT a FROM expressions LIMIT ?1 + 1")
            .unwrap();
        assert!(expression_limit
            .get_program()
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::DecrJumpZero { .. })));
        expression_limit
            .bind_at(1.try_into().unwrap(), Value::from_i64(1))
            .unwrap();
        assert_eq!(
            expression_limit.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
        );

        let mut invalid_limit = connection
            .prepare("SELECT a FROM expressions LIMIT ?1")
            .unwrap();
        invalid_limit
            .bind_at(1.try_into().unwrap(), Value::from_f64(1.5))
            .unwrap();
        assert!(invalid_limit.run_collect_rows().is_err());

        let mut dynamic_offset = connection
            .prepare("SELECT a FROM expressions LIMIT ?1 OFFSET ?2 + 1")
            .unwrap();
        assert!(dynamic_offset
            .get_program()
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(
                instruction,
                Insn::DecrJumpZero { .. } | Insn::IfPos { .. } | Insn::OffsetLimit { .. }
            )));
        assert_eq!(
            dynamic_offset
                .get_program()
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::MustBeInt { .. }))
                .count(),
            2
        );
        dynamic_offset
            .bind_at(1.try_into().unwrap(), Value::from_i64(2))
            .unwrap();
        dynamic_offset
            .bind_at(2.try_into().unwrap(), Value::from_i64(1))
            .unwrap();
        assert_eq!(
            dynamic_offset.run_collect_rows().unwrap(),
            vec![vec![Value::Null], vec![Value::from_i64(5)]]
        );
        dynamic_offset.reset().unwrap();
        dynamic_offset
            .bind_at(1.try_into().unwrap(), Value::from_i64(2))
            .unwrap();
        dynamic_offset
            .bind_at(2.try_into().unwrap(), Value::from_i64(-2))
            .unwrap();
        assert_eq!(
            dynamic_offset.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
        );

        let mut zero_limit_invalid_offset = connection
            .prepare("SELECT a FROM expressions LIMIT ?1 OFFSET ?2")
            .unwrap();
        zero_limit_invalid_offset
            .bind_at(1.try_into().unwrap(), Value::from_i64(0))
            .unwrap();
        zero_limit_invalid_offset
            .bind_at(2.try_into().unwrap(), Value::from_f64(1.5))
            .unwrap();
        assert!(zero_limit_invalid_offset
            .run_collect_rows()
            .unwrap()
            .is_empty());
        zero_limit_invalid_offset.reset().unwrap();
        zero_limit_invalid_offset
            .bind_at(1.try_into().unwrap(), Value::from_i64(1))
            .unwrap();
        zero_limit_invalid_offset
            .bind_at(2.try_into().unwrap(), Value::from_f64(1.5))
            .unwrap();
        assert!(zero_limit_invalid_offset.run_collect_rows().is_err());

        let mut control_flow_statement = connection
            .prepare(
                "SELECT CASE \
                            WHEN a > b THEN 'gt' \
                            WHEN a = b THEN 'eq' \
                            ELSE 'other' \
                        END, \
                        (a > b) OR (name = 'BETA'), \
                        (a > b) AND (name = 'BETA') \
                   FROM expressions \
                  WHERE CASE WHEN name = 'BETA' THEN a + b >= 4 ELSE 0 END",
            )
            .unwrap();
        let control_flow_instructions = &control_flow_statement.get_program().insns;
        assert!(control_flow_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Or { .. })));
        assert!(control_flow_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::And { .. })));
        let control_flow_result_row = control_flow_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("searched CASE projections must join into one symbolic result pack");
        assert!(
            control_flow_instructions[control_flow_result_row - 3..control_flow_result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "CASE merge values must remain symbolic until result-pack lowering"
        );
        assert_eq!(
            control_flow_statement.run_collect_rows().unwrap(),
            vec![vec![
                Value::Text("eq".into()),
                Value::from_i64(1),
                Value::from_i64(0),
            ]]
        );

        connection
            .execute(
                "CREATE TABLE simple_cases(\
                    id INTEGER PRIMARY KEY, n INTEGER, tag TEXT COLLATE NOCASE\
                )",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO simple_cases VALUES \
                 (1, 1, 'ALPHA'), (2, 2, 'beta'), (3, NULL, 'BETA'), (4, 5, NULL)",
            )
            .unwrap();
        let mut simple_case_statement = connection
            .prepare(
                "SELECT CASE tag \
                            WHEN 'alpha' COLLATE BINARY THEN 700 \
                            WHEN 'alpha' THEN n + 10 \
                            WHEN 'beta' THEN n + 2 \
                            WHEN NULL THEN 500 \
                            ELSE 99 \
                        END, \
                        CASE n \
                            WHEN '1' THEN 'one' \
                            WHEN '2' THEN 'two' \
                            WHEN NULL THEN 'null' \
                            ELSE 'other' \
                        END \
                   FROM simple_cases",
            )
            .unwrap();
        let simple_case_instructions = &simple_case_statement.get_program().insns;
        let simple_case_result_row = simple_case_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. }))
            .expect("simple CASE must produce its projected row");
        assert!(
            simple_case_instructions[simple_case_result_row - 2..simple_case_result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. }))
        );
        assert_eq!(
            simple_case_instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Column { column: 2, .. }))
                .count(),
            1,
            "the simple CASE base expression must be compiled once"
        );
        let simple_case_comparisons = simple_case_instructions
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Eq {
                    flags, collation, ..
                } => Some((flags.get_affinity(), *collation)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(simple_case_comparisons.len(), 7);
        assert_eq!(
            simple_case_comparisons
                .iter()
                .filter(|(_, collation)| {
                    *collation == Some(crate::translate::collate::CollationSeq::NoCase)
                })
                .count(),
            3,
            "each WHEN comparison must resolve explicit and implicit collation precedence"
        );
        assert_eq!(
            simple_case_comparisons
                .iter()
                .filter(|(affinity, _)| *affinity == Affinity::Integer)
                .count(),
            3,
            "base column affinity must apply to every WHEN comparison"
        );
        assert_eq!(
            simple_case_statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(11), Value::from_text("one")],
                vec![Value::from_i64(4), Value::from_text("two")],
                vec![Value::Null, Value::from_text("other")],
                vec![Value::from_i64(99), Value::from_text("other")],
            ]
        );
    }

    #[test]
    fn inner_join_crosses_the_declarative_compiler_boundary() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE join_left(id INTEGER, a INTEGER)")
            .unwrap();
        connection
            .execute(
                "CREATE TABLE join_right(\
                     id INTEGER, left_id INTEGER, b INTEGER\
                 )",
            )
            .unwrap();
        connection
            .execute("INSERT INTO join_left VALUES (1, 10), (2, 20), (3, NULL)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO join_right VALUES \
                 (1, 1, 2), (2, 1, 3), (3, 2, 4), (4, 2, 5), (5, 3, 1)",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT l.id, l.a + r.b, r.b \
                   FROM join_left AS l JOIN join_right AS r ON r.left_id = l.id \
                  WHERE l.a >= r.b \
                  ORDER BY r.b DESC, l.id \
                  LIMIT 2 OFFSET 1",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert_eq!(
            instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Rewind { .. }))
                .count(),
            2,
            "the composed stream must rewind each table cursor"
        );
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::SorterOpen { .. })));
        instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("declarative join must produce one three-value result pack");

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(2), Value::from_i64(24), Value::from_i64(4),],
                vec![Value::from_i64(1), Value::from_i64(13), Value::from_i64(3),],
            ]
        );
    }

    #[test]
    fn three_table_inner_join_composes_nested_symbolic_streams() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE join_a(value INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE join_b(value INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE join_c(value INTEGER)")
            .unwrap();
        connection
            .execute("INSERT INTO join_a VALUES (1), (2)")
            .unwrap();
        connection
            .execute("INSERT INTO join_b VALUES (10), (20)")
            .unwrap();
        connection
            .execute("INSERT INTO join_c VALUES (100), (200)")
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT a.value, b.value, c.value, a.value + b.value + c.value \
                   FROM join_a AS a \
                  CROSS JOIN join_b AS b \
                  CROSS JOIN join_c AS c \
                  WHERE a.value + b.value + c.value = 221",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert_eq!(
            instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Rewind { .. }))
                .count(),
            3,
            "the composed stream must rewind every table cursor"
        );
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 4, .. }))
            .expect("declarative join must produce one four-value result pack");
        assert!(
            instructions[result_row - 4..result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "all joined projections must remain symbolic until result-pack lowering: {instructions:#?}"
        );

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![vec![
                Value::from_i64(1),
                Value::from_i64(20),
                Value::from_i64(200),
                Value::from_i64(221),
            ]]
        );
    }

    #[test]
    fn dependent_rowid_join_crosses_the_declarative_compiler_boundary() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE parent_rows(id INTEGER PRIMARY KEY, a INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE child_rows(id INTEGER, parent_id INTEGER, b INTEGER)")
            .unwrap();
        connection
            .execute("INSERT INTO parent_rows VALUES (1, 10), (2, 20), (3, NULL)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO child_rows VALUES \
                 (1, 1, 2), (2, 1, 3), (3, 2, 4), (4, 2, 5), (5, 3, 1)",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT p.id, p.a + c.b, c.b \
                   FROM parent_rows AS p JOIN child_rows AS c ON c.parent_id = p.id \
                  WHERE p.a >= c.b",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        let open_positions = instructions
            .iter()
            .enumerate()
            .filter_map(|(position, (instruction, _))| {
                matches!(instruction, Insn::OpenRead { .. }).then_some(position)
            })
            .collect::<Vec<_>>();
        assert_eq!(open_positions.len(), 2);
        let first_positioning = instructions
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::Rewind { .. } | Insn::SeekRowid { .. })
            })
            .expect("dependent join must position one of its table cursors");
        assert!(open_positions
            .iter()
            .all(|position| *position < first_positioning));
        assert_eq!(
            instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
                .count(),
            1
        );
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("dependent declarative join must produce a three-value result pack");
        assert!(
            instructions[result_row - 3..result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "dependent join projections must remain symbolic until pack lowering"
        );

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(12), Value::from_i64(2),],
                vec![Value::from_i64(1), Value::from_i64(13), Value::from_i64(3),],
                vec![Value::from_i64(2), Value::from_i64(24), Value::from_i64(4),],
                vec![Value::from_i64(2), Value::from_i64(25), Value::from_i64(5),],
            ]
        );
    }

    #[test]
    fn dependent_table_range_join_crosses_the_declarative_compiler_boundary() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute(
                "CREATE TABLE range_windows(\
                    id INTEGER PRIMARY KEY, lo INTEGER, hi INTEGER, bias INTEGER\
                )",
            )
            .unwrap();
        connection
            .execute("CREATE TABLE range_points(id INTEGER PRIMARY KEY, value INTEGER)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO range_windows VALUES \
                 (1, 1, 3, 10), (2, 3, 5, 100), (3, NULL, 6, 1000)",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO range_points VALUES \
                 (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT w.id, p.id, w.bias + p.value \
                   FROM range_windows AS w \
                   JOIN range_points AS p ON p.id > w.lo AND p.id <= w.hi \
                  WHERE p.value >= w.id",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekGT {
                is_index: false,
                num_regs: 1,
                ..
            }
        )));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Gt { .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Next { .. })));
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("dependent table range join must produce a three-value result pack");
        assert!(
            instructions[result_row - 3..result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "dependent table range projections must remain symbolic until pack lowering"
        );

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(2), Value::from_i64(30)],
                vec![Value::from_i64(1), Value::from_i64(3), Value::from_i64(40)],
                vec![Value::from_i64(2), Value::from_i64(4), Value::from_i64(140)],
                vec![Value::from_i64(2), Value::from_i64(5), Value::from_i64(150)],
            ]
        );
    }

    #[test]
    fn dependent_index_join_crosses_the_declarative_compiler_boundary() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE join_keys(id INTEGER PRIMARY KEY, key TEXT, bonus INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE join_values(id INTEGER PRIMARY KEY, key TEXT, amount INTEGER)")
            .unwrap();
        connection
            .execute("CREATE INDEX join_values_key ON join_values(key)")
            .unwrap();
        connection
            .execute("INSERT INTO join_keys VALUES (1, 'a', 10), (2, 'b', 20), (3, 'x', 30)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO join_values VALUES \
                 (1, 'a', 2), (2, 'a', 3), (3, 'b', 4), (4, 'c', 5)",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT k.id, v.id, k.bonus + v.amount \
                   FROM join_keys AS k \
                   JOIN join_values AS v INDEXED BY join_values_key ON v.key = k.key \
                  WHERE k.bonus >= v.amount",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekGE {
                is_index: true,
                eq_only: true,
                num_regs: 1,
                ..
            }
        )));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGT { num_regs: 1, .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::DeferredSeek { .. })));
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("dependent index join must produce a three-value result pack");
        assert!(
            instructions[result_row - 3..result_row]
                .iter()
                .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })),
            "dependent index join projections must remain symbolic until pack lowering"
        );

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(1), Value::from_i64(12)],
                vec![Value::from_i64(1), Value::from_i64(2), Value::from_i64(13)],
                vec![Value::from_i64(2), Value::from_i64(3), Value::from_i64(24)],
            ]
        );

        let mut covering = connection
            .prepare(
                "SELECT k.id, v.id, v.key \
                   FROM join_keys AS k \
                   JOIN join_values AS v INDEXED BY join_values_key ON v.key = k.key",
            )
            .unwrap();
        let covering_instructions = &covering.get_program().insns;
        assert!(covering_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxRowId { .. })));
        assert!(covering_instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::DeferredSeek { .. })));
        let covering_result = covering_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("covering dependent index join must produce a three-value result pack");
        assert!(covering_instructions[covering_result - 3..covering_result]
            .iter()
            .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })));
        assert_eq!(
            covering.run_collect_rows().unwrap(),
            vec![
                vec![
                    Value::from_i64(1),
                    Value::from_i64(1),
                    Value::from_text("a")
                ],
                vec![
                    Value::from_i64(1),
                    Value::from_i64(2),
                    Value::from_text("a")
                ],
                vec![
                    Value::from_i64(2),
                    Value::from_i64(3),
                    Value::from_text("b")
                ],
            ]
        );
    }

    #[test]
    fn dependent_in_join_rebuilds_rhs_values_for_each_outer_row() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute(
                "CREATE TABLE in_join_outer(\
                    id INTEGER PRIMARY KEY, k1 INTEGER, k2 INTEGER, bias INTEGER\
                )",
            )
            .unwrap();
        connection
            .execute("CREATE TABLE in_join_inner(id INTEGER PRIMARY KEY, value INTEGER)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO in_join_outer VALUES \
                 (1, 1, 3, 10), (2, 2, 2, 20), (3, NULL, 4, 30)",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO in_join_inner VALUES \
                 (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT o.id, i.id, o.bias + i.value \
                   FROM in_join_outer AS o \
                  CROSS JOIN in_join_inner AS i \
                  WHERE i.id IN (o.k1, o.k2, o.k1)",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::SorterOpen { .. })));
        assert!(instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::Once { .. })));
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. }))
            .expect("dependent IN join must produce a three-value result pack");
        assert!(instructions[result_row - 3..result_row]
            .iter()
            .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })));

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(1), Value::from_i64(110)],
                vec![Value::from_i64(1), Value::from_i64(3), Value::from_i64(310)],
                vec![Value::from_i64(2), Value::from_i64(2), Value::from_i64(220)],
                vec![Value::from_i64(3), Value::from_i64(4), Value::from_i64(430)],
            ]
        );
    }

    #[test]
    fn dependent_index_in_join_repositions_an_opened_index() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute(
                "CREATE TABLE index_in_outer(\
                    id INTEGER PRIMARY KEY, k1 TEXT, k2 TEXT, bias INTEGER\
                )",
            )
            .unwrap();
        connection
            .execute(
                "CREATE TABLE index_in_inner(\
                    id INTEGER PRIMARY KEY, key TEXT, value INTEGER\
                )",
            )
            .unwrap();
        connection
            .execute("CREATE INDEX index_in_inner_key ON index_in_inner(key)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO index_in_outer VALUES \
                 (1, 'a', 'c', 10), (2, 'b', 'b', 20), (3, NULL, 'd', 30)",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO index_in_inner VALUES \
                 (1, 'a', 100), (2, 'a', 101), (3, 'b', 200), \
                 (4, 'c', 300), (5, 'd', 400), (6, 'z', 500)",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT o.id, i.id, o.bias + i.value \
                   FROM index_in_outer AS o \
                  CROSS JOIN index_in_inner AS i \
                  WHERE i.key IN (o.k1, o.k2, o.k1)",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekGE {
                is_index: true,
                num_regs: 1,
                ..
            }
        )));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGT { num_regs: 1, .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::DeferredSeek { .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. })));
        assert!(instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::Once { .. })));

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(1), Value::from_i64(110)],
                vec![Value::from_i64(1), Value::from_i64(2), Value::from_i64(111)],
                vec![Value::from_i64(1), Value::from_i64(4), Value::from_i64(310)],
                vec![Value::from_i64(2), Value::from_i64(3), Value::from_i64(220)],
                vec![Value::from_i64(3), Value::from_i64(5), Value::from_i64(430)],
            ]
        );
    }

    #[test]
    fn in_subquery_join_composes_one_producer_with_repeated_inner_seeks() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE subquery_in_outer(id INTEGER PRIMARY KEY, bias INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE subquery_in_inner(id INTEGER PRIMARY KEY, value INTEGER)")
            .unwrap();
        connection
            .execute("CREATE TABLE subquery_in_keys(key)")
            .unwrap();
        connection
            .execute("INSERT INTO subquery_in_outer VALUES (1, 10), (2, 20)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO subquery_in_inner VALUES \
                 (1, 100), (2, 200), (3, 300), (4, 400)",
            )
            .unwrap();
        connection
            .execute("INSERT INTO subquery_in_keys VALUES (3), (1), (3), (NULL)")
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT o.id, i.id, o.bias + i.value \
                   FROM subquery_in_outer AS o \
                  CROSS JOIN subquery_in_inner AS i \
                  WHERE i.id IN (SELECT key FROM subquery_in_keys)",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(instructions.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::OpenEphemeral {
                is_table: false,
                ..
            }
        )));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. })));
        assert!(instructions.iter().all(|(instruction, _)| !matches!(
            instruction,
            Insn::SorterOpen { .. } | Insn::Once { .. }
        )));
        assert_eq!(
            instructions
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
                .count(),
            1
        );

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::from_i64(1), Value::from_i64(110)],
                vec![Value::from_i64(1), Value::from_i64(3), Value::from_i64(310)],
                vec![Value::from_i64(2), Value::from_i64(1), Value::from_i64(120)],
                vec![Value::from_i64(2), Value::from_i64(3), Value::from_i64(320)],
            ]
        );
    }

    #[test]
    fn reverse_table_scan_crosses_the_declarative_compiler_boundary() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE reversed(id INTEGER PRIMARY KEY, flag, name)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO reversed VALUES \
                 (1, 1, 'one'), (2, 1, 'two'), (3, 0, 'three'), (4, 1, 'four')",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT name, flag FROM reversed \
                 WHERE flag ORDER BY id DESC LIMIT 2 OFFSET 1",
            )
            .unwrap();
        let instructions = &statement.get_program().insns;
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Last { .. })));
        assert!(instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Prev { .. })));
        assert!(instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::SorterOpen { .. })));
        let result_row = instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. }))
            .expect("reverse declarative scan must produce its projected row pack");
        assert!(instructions[result_row - 2..result_row]
            .iter()
            .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })));

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![
                vec![Value::Text("two".into()), Value::from_i64(1)],
                vec![Value::Text("one".into()), Value::from_i64(1)],
            ]
        );
    }

    #[test]
    fn full_index_scans_cross_the_declarative_compiler_boundary() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE indexed(id INTEGER PRIMARY KEY, key TEXT, payload TEXT)")
            .unwrap();
        connection
            .execute("CREATE INDEX indexed_key ON indexed(key)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO indexed VALUES \
                 (1, 'charlie', 'third'), (2, 'alpha', 'first'), (3, 'bravo', 'second')",
            )
            .unwrap();

        let mut covering = connection
            .prepare("SELECT rowid, key FROM indexed ORDER BY key DESC")
            .unwrap();
        let covering_instructions = &covering.get_program().insns;
        assert!(covering_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxRowId { .. })));
        assert!(covering_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Last { .. })));
        assert!(covering_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Prev { .. })));
        assert!(covering_instructions
            .iter()
            .all(|(instruction, _)| !matches!(
                instruction,
                Insn::DeferredSeek { .. } | Insn::SorterOpen { .. }
            )));
        let covering_result = covering_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. }))
            .expect("covering declarative scan must produce one symbolic row pack");
        assert!(covering_instructions[covering_result - 2..covering_result]
            .iter()
            .all(|(instruction, _)| matches!(instruction, Insn::Copy { .. })));
        assert_eq!(
            covering.run_collect_rows().unwrap(),
            vec![
                vec![Value::from_i64(1), Value::Text("charlie".into())],
                vec![Value::from_i64(3), Value::Text("bravo".into())],
                vec![Value::from_i64(2), Value::Text("alpha".into())],
            ]
        );

        let mut non_covering = connection
            .prepare("SELECT payload FROM indexed ORDER BY key")
            .unwrap();
        let non_covering_instructions = &non_covering.get_program().insns;
        let deferred_seek = non_covering_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::DeferredSeek { .. }))
            .expect("non-covering symbolic index scan must seek its table row");
        let column = non_covering_instructions
            .iter()
            .enumerate()
            .skip(deferred_seek + 1)
            .find(|(_, (instruction, _))| matches!(instruction, Insn::Column { column: 2, .. }))
            .map(|(index, _)| index)
            .expect("payload must be read from the table after DeferredSeek");
        assert!(deferred_seek < column);
        assert!(non_covering_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Rewind { .. })));
        assert!(non_covering_instructions
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Next { .. })));
        assert!(non_covering_instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::SorterOpen { .. })));
        let non_covering_result = non_covering_instructions
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. }))
            .expect("non-covering declarative scan must produce one symbolic row pack");
        assert!(matches!(
            non_covering_instructions[non_covering_result - 1].0,
            Insn::Copy { .. }
        ));
        assert_eq!(
            non_covering.run_collect_rows().unwrap(),
            vec![
                vec![Value::Text("first".into())],
                vec![Value::Text("second".into())],
                vec![Value::Text("third".into())],
            ]
        );
    }
}
