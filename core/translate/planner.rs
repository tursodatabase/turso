use crate::sync::Arc;

use super::plan::PlannedWindowSpec;
use super::plan::{
    Aggregate, Distinctness, EvalAt, JoinOrderMember, JoinedTable, Operation, OuterQueryReference,
    Plan, PlanOuterOutputReference, PlanOutputFact, PlanSourceScope, QueryDestination,
    TableReferences, WhereTerm,
};
use super::{
    plan::{
        Frame, FrameBoundary, GroupBy, JoinInfo, JoinType as PlanJoinType, PlanCheckConstraint,
        PlanIndexExpressions, PlanIndexHint, PlanIndexMethodPattern, ResultColumnOrigin,
        ResultSetColumn, SelectPlan, SourceReadPrograms, Window,
    },
    plan_expr::{
        lower_hir_expr, plan_expr_contains_aggregate, plan_expr_dependencies,
        plan_expr_vector_size, plan_exprs_are_equivalent as plan_exprs_equivalent,
        type_fact_array_dimensions, walk_plan_expr, walk_plan_expr_mut, PlanColumnRef, PlanExpr,
        PlanExprAffinity, PlanIdentityMap, PlanOrderTerm, PlanOutputId, PlanSourceId,
        PlanWalkControl,
    },
    semantic::hir,
};
use crate::function::{AccumulatorFunc, AggFunc, ExtFunc};
use crate::translate::plan::BitSet;
use crate::translate::plan::{NonFromClauseSubquery, SubqueryState};
use crate::{function::Func, LimboError, Result, MAIN_DB_ID};
use crate::{translate::plan::WindowFunction, vdbe::builder::ProgramBuilder};
use turso_parser::ast;
use turso_parser::ast::Literal::Null;

/// Planner state for lowering one closed Semantic HIR document.
///
/// Semantic identities are allocated once by the statement entry point and
/// borrowed here. Nested queries reuse that exact map, so trigger row images,
/// output references, and subqueries all stay in one plan identity space.
pub struct HirPlanContext<'a> {
    pub(crate) document: &'a hir::HirDocument,
    pub(crate) identities: &'a PlanIdentityMap,
    pub(crate) program: &'a mut ProgramBuilder,
    pub(crate) outer_query_refs: Vec<OuterQueryReference>,
    pub(crate) recursive_inputs: rustc_hash::FxHashMap<hir::SourceId, JoinedTable>,
}

impl<'a> HirPlanContext<'a> {
    pub fn new(
        document: &'a hir::HirDocument,
        identities: &'a PlanIdentityMap,
        program: &'a mut ProgramBuilder,
    ) -> Self {
        Self {
            document,
            identities,
            program,
            outer_query_refs: Vec::new(),
            recursive_inputs: Default::default(),
        }
    }

    pub(crate) fn lower_expr(&self, expression: &hir::Expr) -> Result<PlanExpr> {
        self.lower_expr_for_owner(expression, None)
    }

    fn lower_query_expr(
        &self,
        expression: &hir::Expr,
        block: hir::QueryBlockId,
    ) -> Result<PlanExpr> {
        self.lower_expr_for_owner(expression, Some(hir::OutputOwner::QueryBlock(block)))
    }

    fn lower_expr_for_owner(
        &self,
        expression: &hir::Expr,
        owner: Option<hir::OutputOwner>,
    ) -> Result<PlanExpr> {
        let mut lowered = lower_hir_expr(expression, self.identities)
            .map_err(|error| LimboError::InternalError(error.to_string()))?;
        let mut expanding = rustc_hash::FxHashSet::default();
        expand_hir_outputs(
            self.document,
            self.identities,
            &mut lowered,
            owner,
            &mut expanding,
        )?;
        Ok(lowered)
    }

    pub(crate) fn lower_output(&self, output: &hir::Output) -> Result<ResultSetColumn> {
        let id = self.identities.output(output.id).ok_or_else(|| {
            LimboError::InternalError(format!(
                "missing plan identity for semantic output {:?}",
                output.id
            ))
        })?;
        let expr = self.lower_expr_for_owner(&output.expr, Some(output.id.owner))?;
        let origin = match &expr {
            PlanExpr::Column(column) => Some(ResultColumnOrigin::Column {
                source: column.source,
                column: column.column,
            }),
            PlanExpr::RowId(source) => Some(ResultColumnOrigin::RowId { source: *source }),
            _ => None,
        };
        let affinity = if output.has_affinity {
            PlanExprAffinity::with_affinity(output.affinity)
        } else {
            PlanExprAffinity::no_affinity()
        };
        let contains_aggregates = plan_expr_contains_aggregate(&expr)?;
        Ok(ResultSetColumn {
            id,
            name: output.name.clone(),
            name_kind: output.name_kind,
            origin,
            type_fact: output.type_fact.clone(),
            affinity,
            collation: output.collation.clone(),
            array_dimensions: type_fact_array_dimensions(&output.type_fact),
            expr,
            contains_aggregates,
        })
    }

    fn outer_output_reference(&self, output: PlanOutputId) -> Result<PlanOuterOutputReference> {
        let semantic_output = self.identities.semantic_output(output).ok_or_else(|| {
            LimboError::InternalError(format!("plan output {output} has no semantic identity"))
        })?;
        let semantic = self.document.output(semantic_output).ok_or_else(|| {
            LimboError::InternalError(format!(
                "semantic output {semantic_output:?} is missing from its HIR document"
            ))
        })?;
        let lowered = self.lower_output(semantic)?;
        let dependencies = plan_expr_dependencies(&lowered.expr)?;
        let mut source_dependencies = dependencies.sources().collect::<Vec<_>>();
        if !dependencies.subqueries.is_empty() {
            // A nested query hides its source reads behind a subquery identity.
            // The child that reads this alias must still wait until every source
            // in the alias owner's row is available. This owner-local closure is
            // deliberately conservative: sources declared inside the nested
            // query are excluded, while an outer read such as `(SELECT t.a)`
            // cannot make `ORDER BY (SELECT alias)` run before `t` is positioned.
            for source in &self.document.sources {
                let belongs_to_owner = match (source.owner, semantic_output.owner) {
                    (
                        hir::SourceOwner::QueryBlock(source),
                        hir::OutputOwner::QueryBlock(output),
                    ) => source == output,
                    (hir::SourceOwner::Root, hir::OutputOwner::Root) => true,
                    _ => false,
                };
                if belongs_to_owner {
                    source_dependencies.push(self.identities.source(source.id).ok_or_else(
                        || {
                            LimboError::InternalError(format!(
                                "missing plan identity for semantic source {}",
                                source.id
                            ))
                        },
                    )?);
                }
            }
        }
        source_dependencies.sort_unstable();
        source_dependencies.dedup();
        let fact = PlanOutputFact::from(&lowered);
        Ok(PlanOuterOutputReference {
            output,
            definition: lowered.expr,
            fact,
            source_dependencies,
        })
    }

    /// Copy the HIR document's cursorless source identities into one physical
    /// expression scope. Optimizer passes can then distinguish runtime values
    /// from missing table sources without consulting semantic state.
    pub(crate) fn add_runtime_sources_to(&self, tables: &mut TableReferences) -> Result<()> {
        for source in &self.document.sources {
            if !matches!(
                &source.kind,
                hir::SourceKind::Pseudo { .. } | hir::SourceKind::SchemaExpression
            ) {
                continue;
            }
            let plan_source = self.identities.source(source.id).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "runtime source {} has no plan identity",
                    source.id
                ))
            })?;
            tables.add_runtime_source(plan_source);
        }
        Ok(())
    }

    pub(crate) fn new_table_references(
        &self,
        joined_tables: Vec<JoinedTable>,
        outer_query_refs: Vec<OuterQueryReference>,
    ) -> Result<TableReferences> {
        let mut tables = TableReferences::new(joined_tables, outer_query_refs);
        self.add_runtime_sources_to(&mut tables)?;
        Ok(tables)
    }
}

fn expand_hir_outputs(
    document: &hir::HirDocument,
    identities: &PlanIdentityMap,
    expression: &mut PlanExpr,
    owner: Option<hir::OutputOwner>,
    expanding: &mut rustc_hash::FxHashSet<PlanOutputId>,
) -> Result<()> {
    walk_plan_expr_mut(expression, &mut |node| {
        let PlanExpr::Output(plan_output) = node else {
            return Ok(PlanWalkControl::Continue);
        };
        let semantic_output = identities.semantic_output(*plan_output).ok_or_else(|| {
            LimboError::InternalError(format!(
                "plan output {plan_output} has no semantic identity"
            ))
        })?;
        let output = document.output(semantic_output).ok_or_else(|| {
            LimboError::InternalError(format!(
                "semantic output {semantic_output:?} is missing from its HIR document"
            ))
        })?;
        if owner.is_some_and(|owner| semantic_output.owner != owner) {
            return Ok(PlanWalkControl::Continue);
        }
        if !expanding.insert(*plan_output) {
            return Err(LimboError::InternalError(format!(
                "cyclic semantic output reference while lowering {plan_output}"
            )));
        }
        let mut replacement = lower_hir_expr(&output.expr, identities)
            .map_err(|error| LimboError::InternalError(error.to_string()))?;
        expand_hir_outputs(document, identities, &mut replacement, owner, expanding)?;
        expanding.remove(plan_output);
        *node = replacement;
        Ok(PlanWalkControl::SkipChildren)
    })?;
    Ok(())
}

/// Result of lowering one resolved FROM clause.
pub struct PreparedHirFrom {
    pub table_references: TableReferences,
    pub predicates: Vec<WhereTerm>,
}

#[derive(Clone, Copy)]
enum HirSourcePlanKind {
    Derived(hir::QueryId),
    Cte(hir::CteId),
    Leaf,
}

#[derive(Clone, Copy)]
enum HirCtePlanKind {
    Query(hir::QueryId),
    Recursive,
}

/// Lower one semantic source occurrence into the physical table shape used by
/// the optimizer. This consumes only resolved HIR metadata; it never looks a
/// table or index up by name.
pub fn prepare_hir_source(
    context: &mut HirPlanContext<'_>,
    source_id: hir::SourceId,
    join_info: Option<JoinInfo>,
) -> Result<JoinedTable> {
    // A long CTE chain calls this once per link. Dispatch query sources before
    // creating all the metadata needed for a real table.
    let kind = hir_source_plan_kind(context, source_id)?;

    match kind {
        HirSourcePlanKind::Derived(query) => {
            prepare_hir_derived_source(context, source_id, query, join_info)
        }
        HirSourcePlanKind::Cte(cte) => prepare_hir_cte_source(context, source_id, cte, join_info),
        HirSourcePlanKind::Leaf => prepare_hir_leaf_source(context, source_id, join_info),
    }
}

fn hir_source_plan_kind(
    context: &HirPlanContext<'_>,
    source_id: hir::SourceId,
) -> Result<HirSourcePlanKind> {
    let source = context
        .document
        .source(source_id)
        .ok_or_else(|| LimboError::InternalError(format!("missing semantic source {source_id}")))?;
    let column_count = source.columns.len();
    if source.generated_expressions.len() != column_count
        || source.default_expressions.len() != column_count
        || source.column_type_programs.len() != column_count
    {
        return Err(LimboError::InternalError(format!(
            "read-program metadata for semantic source {} is not aligned with its columns",
            source.id
        )));
    }
    Ok(match &source.kind {
        hir::SourceKind::Derived(query) => HirSourcePlanKind::Derived(*query),
        hir::SourceKind::Cte(cte) => HirSourcePlanKind::Cte(*cte),
        _ => HirSourcePlanKind::Leaf,
    })
}

fn hir_source_plan_identity(
    context: &HirPlanContext<'_>,
    source_id: hir::SourceId,
) -> Result<(String, PlanSourceId)> {
    let source = context
        .document
        .source(source_id)
        .ok_or_else(|| LimboError::InternalError(format!("missing semantic source {source_id}")))?;
    let identifier = source.alias.clone().unwrap_or_else(|| source.name.clone());
    let internal_id = context.identities.source(source.id).ok_or_else(|| {
        LimboError::InternalError(format!("missing plan identity for source {}", source.id))
    })?;
    Ok((identifier, internal_id))
}

fn prepare_hir_derived_source(
    context: &mut HirPlanContext<'_>,
    source_id: hir::SourceId,
    query: hir::QueryId,
    join_info: Option<JoinInfo>,
) -> Result<JoinedTable> {
    let plan =
        prepare_hir_query_plan(context, query, QueryDestination::placeholder_for_subquery())?;
    let (identifier, internal_id) = hir_source_plan_identity(context, source_id)?;
    JoinedTable::new_subquery_from_plan(identifier, plan, join_info, internal_id, None, None, false)
}

fn prepare_hir_cte_source(
    context: &mut HirPlanContext<'_>,
    source_id: hir::SourceId,
    cte_id: hir::CteId,
    join_info: Option<JoinInfo>,
) -> Result<JoinedTable> {
    let kind = match &context
        .document
        .cte(cte_id)
        .ok_or_else(|| LimboError::InternalError(format!("missing semantic CTE {cte_id}")))?
        .body
    {
        hir::CteBody::Query(query) => HirCtePlanKind::Query(*query),
        hir::CteBody::Recursive(_) => HirCtePlanKind::Recursive,
    };
    match kind {
        HirCtePlanKind::Query(query) => {
            let plan = prepare_hir_query_plan(
                context,
                query,
                QueryDestination::placeholder_for_subquery(),
            )?;
            finish_hir_cte_source(context, source_id, cte_id, join_info, plan)
        }
        HirCtePlanKind::Recursive => {
            prepare_hir_recursive_cte_source(context, source_id, cte_id, join_info)
        }
    }
}

fn prepare_hir_recursive_cte_source(
    context: &mut HirPlanContext<'_>,
    source_id: hir::SourceId,
    cte_id: hir::CteId,
    join_info: Option<JoinInfo>,
) -> Result<JoinedTable> {
    let cte = context
        .document
        .cte(cte_id)
        .cloned()
        .ok_or_else(|| LimboError::InternalError(format!("missing semantic CTE {cte_id}")))?;
    let hir::CteBody::Recursive(recursive) = &cte.body else {
        unreachable!("non-recursive CTEs are dispatched before recursive CTE planning")
    };
    let plan = prepare_hir_recursive_cte_plan(context, &cte, recursive)?;
    finish_hir_cte_source(context, source_id, cte_id, join_info, plan)
}

fn finish_hir_cte_source(
    context: &HirPlanContext<'_>,
    source_id: hir::SourceId,
    cte_id: hir::CteId,
    join_info: Option<JoinInfo>,
    plan: Plan,
) -> Result<JoinedTable> {
    let (identifier, internal_id) = hir_source_plan_identity(context, source_id)?;
    let cte = context
        .document
        .cte(cte_id)
        .ok_or_else(|| LimboError::InternalError(format!("missing semantic CTE {cte_id}")))?;
    let explicit_columns = (!cte.columns.is_empty()).then(|| {
        cte.columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    });
    JoinedTable::new_subquery_from_plan(
        identifier,
        plan,
        join_info,
        internal_id,
        explicit_columns.as_deref(),
        Some(context.identities.cte(cte.id).ok_or_else(|| {
            LimboError::InternalError(format!("missing plan identity for semantic CTE {}", cte.id))
        })?),
        cte.materialized == ast::Materialized::Yes,
    )
}

fn prepare_hir_leaf_source(
    context: &mut HirPlanContext<'_>,
    source_id: hir::SourceId,
    join_info: Option<JoinInfo>,
) -> Result<JoinedTable> {
    let source =
        context.document.source(source_id).cloned().ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic source {source_id}"))
        })?;
    let (identifier, internal_id) = hir_source_plan_identity(context, source_id)?;

    match &source.kind {
        hir::SourceKind::SchemaExpression | hir::SourceKind::Pseudo { .. } => {
            return Err(LimboError::InternalError(format!(
                "cursorless runtime source {} cannot appear in FROM planning",
                source.id
            )));
        }
        hir::SourceKind::RecursiveInput(cte_id) => {
            let mut input = context
                .recursive_inputs
                .get(&source.id)
                .cloned()
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                    "recursive input source {} for CTE {} was planned outside its recursive arm",
                    source.id, cte_id
                ))
                })?;
            input.identifier = identifier;
            input.internal_id = internal_id;
            input.join_info = join_info;
            return Ok(input);
        }
        hir::SourceKind::Table(_) | hir::SourceKind::TableFunction { .. } => {}
        hir::SourceKind::Cte(_) | hir::SourceKind::Derived(_) => {
            unreachable!("query sources are dispatched before leaf planning")
        }
    }

    let resolved_table = match &source.kind {
        hir::SourceKind::Table(table) | hir::SourceKind::TableFunction { table, .. } => {
            table.clone()
        }
        hir::SourceKind::SchemaExpression
        | hir::SourceKind::Cte(_)
        | hir::SourceKind::Derived(_)
        | hir::SourceKind::RecursiveInput(_)
        | hir::SourceKind::Pseudo { .. } => unreachable!(),
    };
    let table = resolved_table.value().clone();
    let database_id = source.database.map_or(MAIN_DB_ID, hir::DatabaseId::index);
    let index_hint = match &source.index_hint {
        hir::IndexHint::None => PlanIndexHint::None,
        hir::IndexHint::NotIndexed => PlanIndexHint::NotIndexed,
        hir::IndexHint::Indexed(index) => PlanIndexHint::Indexed(index.clone()),
    };

    let generated_expressions = source
        .generated_expressions
        .iter()
        .map(|expression| match expression {
            hir::ColumnReadExpression::Absent | hir::ColumnReadExpression::NotRequired => Ok(None),
            hir::ColumnReadExpression::Planned(expression) => {
                context.lower_expr(expression).map(Some)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let default_expressions = source
        .default_expressions
        .iter()
        .map(|expression| match expression {
            hir::ColumnReadExpression::Absent | hir::ColumnReadExpression::NotRequired => Ok(None),
            hir::ColumnReadExpression::Planned(expression) => {
                context.lower_expr(expression).map(Some)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let column_type_programs = source
        .column_type_programs
        .iter()
        .map(|programs| {
            programs
                .as_ref()
                .map(|programs| {
                    context
                        .identities
                        .lower_column_type_programs(programs)
                        .map_err(|error| LimboError::InternalError(error.to_string()))
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    let read_programs = Arc::new(SourceReadPrograms {
        generated_expressions,
        default_expressions,
        column_type_programs,
    });
    let check_constraints = source
        .check_constraints
        .iter()
        .map(|check| {
            Ok(PlanCheckConstraint {
                expression: context.lower_expr(&check.expression)?,
                description: check.description.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let index_expressions = source
        .index_expressions
        .iter()
        .map(|definition| {
            Ok(PlanIndexExpressions {
                index: definition.index.clone(),
                columns: definition
                    .columns
                    .iter()
                    .map(|expression| {
                        expression
                            .as_ref()
                            .map(|expression| context.lower_expr(expression))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?,
                predicate: definition
                    .predicate
                    .as_ref()
                    .map(|expression| context.lower_expr(expression))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let index_method_patterns = source
        .index_method_patterns
        .iter()
        .enumerate()
        .map(|(pattern_idx, pattern)| {
            let (limit, offset) = lower_hir_limit(context, pattern.limit.as_ref())?;
            Ok(PlanIndexMethodPattern {
                index: pattern.index.clone(),
                pattern_idx,
                outputs: pattern
                    .outputs
                    .iter()
                    .map(|output| context.lower_output(output))
                    .collect::<Result<Vec<_>>>()?,
                predicate: pattern
                    .predicate
                    .as_ref()
                    .map(|expression| context.lower_expr(expression))
                    .transpose()?,
                order_by: pattern
                    .order_by
                    .iter()
                    .map(|term| {
                        Ok(super::plan_expr::PlanOrderTerm {
                            expr: context.lower_expr(&term.expr)?,
                            order: term.order,
                            nulls: term.nulls,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                limit,
                offset,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(JoinedTable {
        op: Operation::default_scan_for(&table),
        column_use_counts: vec![0; table.columns().len()],
        table,
        resolved_table: Some(resolved_table),
        identifier,
        internal_id,
        join_info,
        col_used_mask: Default::default(),
        expression_index_usages: Vec::new(),
        database_id,
        index_hint,
        index_method_patterns,
        index_expressions,
        read_programs,
        check_constraints,
    })
}

/// Lower a resolved FROM tree, including JOIN conditions and virtual-table
/// arguments, without repeating name resolution.
pub fn prepare_hir_from(
    context: &mut HirPlanContext<'_>,
    from: &hir::From,
) -> Result<PreparedHirFrom> {
    if let Some(first_join) = from
        .joins
        .first()
        .filter(|join| join.kind == hir::JoinKind::Right)
    {
        return prepare_hir_from_with_leading_right(context, from, first_join);
    }

    let first = prepare_hir_source(context, from.first, None)?;
    finish_hir_from_with_first(context, from, first)
}

fn finish_hir_from_with_first(
    context: &mut HirPlanContext<'_>,
    from: &hir::From,
    first: JoinedTable,
) -> Result<PreparedHirFrom> {
    let mut predicates = Vec::new();
    add_hir_table_function_predicates(context, from.first, &first, None, &mut predicates)?;
    let mut table_references =
        context.new_table_references(Vec::new(), context.outer_query_refs.clone())?;
    table_references.add_joined_table(first);
    finish_hir_from_joins(context, from, table_references, predicates, 0)
}

fn prepare_hir_from_with_leading_right(
    context: &mut HirPlanContext<'_>,
    from: &hir::From,
    first_join: &hir::Join,
) -> Result<PreparedHirFrom> {
    let mut predicates = Vec::new();
    let mut table_references =
        context.new_table_references(Vec::new(), context.outer_query_refs.clone())?;
    let right = prepare_hir_source(context, first_join.right, None)?;
    add_hir_table_function_predicates(context, first_join.right, &right, None, &mut predicates)?;
    table_references.add_joined_table(right);

    let using = hir_join_using_names(&first_join.constraint);
    let join_info = JoinInfo {
        join_type: PlanJoinType::LeftOuter,
        using,
        no_reorder: false,
    };
    let left = prepare_hir_source(context, from.first, Some(join_info))?;
    let outer_id = left.internal_id;
    add_hir_table_function_predicates(context, from.first, &left, Some(outer_id), &mut predicates)?;
    table_references.add_joined_table(left);
    add_hir_join_predicates(
        context,
        &first_join.constraint,
        Some(outer_id),
        &mut predicates,
    )?;
    table_references.set_right_join_swapped();
    finish_hir_from_joins(context, from, table_references, predicates, 1)
}

fn finish_hir_from_joins(
    context: &mut HirPlanContext<'_>,
    from: &hir::From,
    mut table_references: TableReferences,
    mut predicates: Vec<WhereTerm>,
    next_join: usize,
) -> Result<PreparedHirFrom> {
    for join in from.joins.iter().skip(next_join) {
        if join.kind == hir::JoinKind::Right {
            crate::bail_parse_error!(
                "RIGHT JOIN following another join is not yet supported. Try rewriting as LEFT JOIN or using a subquery."
            );
        }
        let join_info = hir_join_info(join);
        let right = prepare_hir_source(context, join.right, Some(join_info))?;
        let outer_id = right
            .join_info
            .as_ref()
            .is_some_and(JoinInfo::is_outer)
            .then_some(right.internal_id);
        add_hir_table_function_predicates(context, join.right, &right, outer_id, &mut predicates)?;
        table_references.add_joined_table(right);
        add_hir_join_predicates(context, &join.constraint, outer_id, &mut predicates)?;
    }

    if table_references.joined_tables().len() > TableReferences::MAX_JOINED_TABLES {
        crate::bail_parse_error!(
            "at most {} tables in a join",
            TableReferences::MAX_JOINED_TABLES
        );
    }
    Ok(PreparedHirFrom {
        table_references,
        predicates,
    })
}

fn hir_join_info(join: &hir::Join) -> JoinInfo {
    let (join_type, no_reorder) = match join.kind {
        hir::JoinKind::Comma | hir::JoinKind::Inner => (PlanJoinType::Inner, false),
        hir::JoinKind::Cross => (PlanJoinType::Inner, true),
        hir::JoinKind::Left | hir::JoinKind::Right => (PlanJoinType::LeftOuter, false),
        hir::JoinKind::Full => (PlanJoinType::FullOuter, false),
    };
    JoinInfo {
        join_type,
        using: hir_join_using_names(&join.constraint),
        no_reorder,
    }
}

fn hir_join_using_names(constraint: &hir::JoinConstraint) -> Vec<ast::Name> {
    match constraint {
        hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => columns
            .iter()
            .map(|column| ast::Name::exact(column.name.clone()))
            .collect(),
        hir::JoinConstraint::None | hir::JoinConstraint::On(_) => Vec::new(),
    }
}

fn add_hir_join_predicates(
    context: &HirPlanContext<'_>,
    constraint: &hir::JoinConstraint,
    outer_id: Option<PlanSourceId>,
    predicates: &mut Vec<WhereTerm>,
) -> Result<()> {
    match constraint {
        hir::JoinConstraint::None => {}
        hir::JoinConstraint::On(expression) => {
            split_hir_predicate(context.lower_expr(expression)?, outer_id, predicates);
        }
        hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
            for column in columns {
                let lhs = context.lower_expr(&column.left)?;
                let rhs = context.lower_expr(&hir::Expr::Column(column.right))?;
                predicates.push(WhereTerm {
                    expr: PlanExpr::Binary {
                        lhs: Box::new(lhs),
                        operator: ast::Operator::Equals,
                        rhs: Box::new(rhs),
                        custom: None,
                    },
                    from_outer_join: outer_id,
                    consumed: false,
                });
            }
        }
    }
    Ok(())
}

fn split_hir_predicate(
    expression: PlanExpr,
    outer_id: Option<PlanSourceId>,
    predicates: &mut Vec<WhereTerm>,
) {
    match expression {
        PlanExpr::Binary {
            lhs,
            operator: ast::Operator::And,
            rhs,
            custom: None,
        } => {
            split_hir_predicate(*lhs, outer_id, predicates);
            split_hir_predicate(*rhs, outer_id, predicates);
        }
        expression => predicates.push(WhereTerm {
            expr: expression,
            from_outer_join: outer_id,
            consumed: false,
        }),
    }
}

fn add_hir_table_function_predicates(
    context: &HirPlanContext<'_>,
    source_id: hir::SourceId,
    table: &JoinedTable,
    outer_id: Option<PlanSourceId>,
    predicates: &mut Vec<WhereTerm>,
) -> Result<()> {
    let source = context
        .document
        .source(source_id)
        .ok_or_else(|| LimboError::InternalError(format!("missing semantic source {source_id}")))?;
    let hir::SourceKind::TableFunction { arguments, .. } = &source.kind else {
        return Ok(());
    };

    let mut arguments = arguments.iter();
    let mut hidden_count = 0;
    for (column_index, schema_column) in table.table.columns().iter().enumerate() {
        if !schema_column.hidden() {
            continue;
        }
        hidden_count += 1;
        let Some(argument) = arguments.next() else {
            continue;
        };
        let metadata = source.columns.get(column_index).ok_or_else(|| {
            LimboError::InternalError(format!(
                "table-function source {} is missing hidden column {} metadata",
                source.id, column_index
            ))
        })?;
        let column = PlanExpr::Column(PlanColumnRef {
            source: table.internal_id,
            column: column_index,
            rowid_alias: metadata.rowid_alias,
            type_fact: metadata.type_fact.clone(),
            affinity: metadata.affinity,
            has_affinity: metadata.has_affinity,
            collation: metadata.collation.clone(),
        });
        let argument = context.lower_expr(argument)?;
        let expression = if matches!(&argument, PlanExpr::Literal(Null)) {
            PlanExpr::IsNull(Box::new(column))
        } else {
            PlanExpr::Binary {
                lhs: Box::new(column),
                operator: ast::Operator::Equals,
                rhs: Box::new(argument),
                custom: None,
            }
        };
        predicates.push(WhereTerm {
            expr: expression,
            from_outer_join: outer_id,
            consumed: false,
        });
    }
    if arguments.next().is_some() {
        return Err(LimboError::ParseError(format!(
            "Too many arguments for {}: expected at most {}, got {}",
            table.table.get_name(),
            hidden_count,
            hidden_count + 1 + arguments.count()
        )));
    }
    Ok(())
}

const HIR_MAX_RESULT_COLUMNS: usize = 2000;

/// Lower one resolved HIR query. Identity allocation belongs to the caller;
/// this function only consumes the supplied map and chooses physical query
/// shapes and runtime destinations.
pub fn prepare_hir_query_plan(
    context: &mut HirPlanContext<'_>,
    query_id: hir::QueryId,
    query_destination: QueryDestination,
) -> Result<Plan> {
    let simple = context
        .document
        .query(query_id)
        .ok_or_else(|| LimboError::InternalError(format!("missing semantic query {query_id}")))?
        .compounds
        .is_empty();
    if simple {
        return prepare_hir_simple_query_chain(context, query_id, query_destination);
    }

    prepare_hir_compound_query_plan(context, query_id, query_destination)
}

struct PendingHirCteLink {
    block_id: hir::QueryBlockId,
    from: hir::From,
    order_by: Vec<hir::OrderTerm>,
    limit: Option<hir::Limit>,
    query_destination: QueryDestination,
    source_id: hir::SourceId,
    cte_id: hir::CteId,
}

/// Plan a linear chain of ordinary CTE references without retaining one Rust
/// call stack per link. The physical plan remains nested exactly as before:
/// only the order in which its nodes are built changes.
fn prepare_hir_simple_query_chain(
    context: &mut HirPlanContext<'_>,
    query_id: hir::QueryId,
    query_destination: QueryDestination,
) -> Result<Plan> {
    let mut pending = Vec::new();
    let mut current_query_id = query_id;
    let mut current_destination = query_destination;

    let mut plan = loop {
        let query = context
            .document
            .query(current_query_id)
            .cloned()
            .ok_or_else(|| {
                LimboError::InternalError(format!("missing semantic query {current_query_id}"))
            })?;
        if !query.compounds.is_empty() {
            break prepare_hir_compound_query_plan(context, current_query_id, current_destination)?;
        }

        let block = context
            .document
            .query_block(query.first)
            .cloned()
            .ok_or_else(|| {
                LimboError::InternalError(format!("missing query block {}", query.first.index))
            })?;
        if block.outputs.is_empty() {
            crate::bail_parse_error!("SELECT without columns is not allowed");
        }
        if block.outputs.len() > HIR_MAX_RESULT_COLUMNS {
            crate::bail_parse_error!("too many columns in result set");
        }

        let Some(from) = block
            .from
            .as_ref()
            .filter(|from| from.joins.is_empty())
            .cloned()
        else {
            break Plan::Select(Box::new(prepare_hir_query_block(
                context,
                query.first,
                &query.order_by,
                query.limit.as_ref(),
                current_destination,
            )?));
        };
        let HirSourcePlanKind::Cte(cte_id) = hir_source_plan_kind(context, from.first)? else {
            break Plan::Select(Box::new(prepare_hir_query_block(
                context,
                query.first,
                &query.order_by,
                query.limit.as_ref(),
                current_destination,
            )?));
        };
        let cte = context
            .document
            .cte(cte_id)
            .ok_or_else(|| LimboError::InternalError(format!("missing semantic CTE {cte_id}")))?;
        let hir::CteBody::Query(next_query_id) = &cte.body else {
            break Plan::Select(Box::new(prepare_hir_query_block(
                context,
                query.first,
                &query.order_by,
                query.limit.as_ref(),
                current_destination,
            )?));
        };

        let source_id = from.first;
        pending.push(PendingHirCteLink {
            block_id: query.first,
            from,
            order_by: query.order_by.clone(),
            limit: query.limit.clone(),
            query_destination: current_destination,
            source_id,
            cte_id,
        });
        current_query_id = *next_query_id;
        current_destination = QueryDestination::placeholder_for_subquery();
    };

    while let Some(link) = pending.pop() {
        let first = finish_hir_cte_source(context, link.source_id, link.cte_id, None, plan)?;
        let prepared_from = finish_hir_from_with_first(context, &link.from, first)?;
        plan = Plan::Select(Box::new(finish_hir_query_block(
            context,
            link.block_id,
            &link.order_by,
            link.limit.as_ref(),
            link.query_destination,
            prepared_from,
        )?));
    }
    Ok(plan)
}

fn prepare_hir_compound_query_plan(
    context: &mut HirPlanContext<'_>,
    query_id: hir::QueryId,
    query_destination: QueryDestination,
) -> Result<Plan> {
    let query =
        context.document.query(query_id).cloned().ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic query {query_id}"))
        })?;

    let mut current =
        prepare_hir_query_block(context, query.first, &[], None, query_destination.clone())?;
    let mut left = Vec::with_capacity(query.compounds.len());
    for compound in &query.compounds {
        left.push((current, compound.operator));
        current = prepare_hir_query_block(
            context,
            compound.block,
            &[],
            None,
            query_destination.clone(),
        )?;
    }

    let (limit, offset) = lower_hir_limit(context, query.limit.as_ref())?;
    let order_by = lower_hir_order_terms(context, &query.order_by)?;
    let limit_expressions = limit.iter().chain(offset.iter()).collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        context,
        &mut current.table_references,
        &limit_expressions,
        super::plan::SubqueryOrigin::SelectLimitOffset,
        &mut current.non_from_clause_subqueries,
    )?;
    for expression in limit_expressions {
        if plan_expr_vector_size(expression, &current)? != 1 {
            crate::bail_parse_error!("row value misused");
        }
    }
    let mut plan = Plan::CompoundSelect {
        left,
        right_most: Box::new(current),
        limit,
        offset,
        order_by,
    };
    super::subquery::mark_shared_cte_materialization_requirements_in_plan(&mut plan);
    Ok(plan)
}

fn prepare_hir_query_block(
    context: &mut HirPlanContext<'_>,
    block_id: hir::QueryBlockId,
    query_order_by: &[hir::OrderTerm],
    query_limit: Option<&hir::Limit>,
    query_destination: QueryDestination,
) -> Result<SelectPlan> {
    let from = {
        let block = context.document.query_block(block_id).ok_or_else(|| {
            LimboError::InternalError(format!("missing query block {}", block_id.index))
        })?;
        if block.outputs.is_empty() {
            crate::bail_parse_error!("SELECT without columns is not allowed");
        }
        if block.outputs.len() > HIR_MAX_RESULT_COLUMNS {
            crate::bail_parse_error!("too many columns in result set");
        }
        block.from.clone()
    };
    let prepared_from = match &from {
        Some(from) => prepare_hir_from(context, from)?,
        None => PreparedHirFrom {
            table_references: context
                .new_table_references(Vec::new(), context.outer_query_refs.clone())?,
            predicates: Vec::new(),
        },
    };
    finish_hir_query_block(
        context,
        block_id,
        query_order_by,
        query_limit,
        query_destination,
        prepared_from,
    )
}

fn finish_hir_query_block(
    context: &mut HirPlanContext<'_>,
    block_id: hir::QueryBlockId,
    query_order_by: &[hir::OrderTerm],
    query_limit: Option<&hir::Limit>,
    query_destination: QueryDestination,
    prepared_from: PreparedHirFrom,
) -> Result<SelectPlan> {
    let block = context
        .document
        .query_block(block_id)
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!("missing query block {}", block_id.index))
        })?;
    let PreparedHirFrom {
        table_references,
        predicates,
    } = prepared_from;
    let result_columns = block
        .outputs
        .iter()
        .map(|output| context.lower_output(output))
        .collect::<Result<Vec<_>>>()?;
    let join_order = table_references
        .joined_tables()
        .iter()
        .enumerate()
        .map(|(original_idx, table)| JoinOrderMember {
            table_id: table.internal_id,
            original_idx,
            is_outer: table.join_info.as_ref().is_some_and(JoinInfo::is_outer),
        })
        .collect();

    let mut plan = SelectPlan {
        table_references,
        join_order,
        result_columns,
        where_clause: predicates,
        group_by: None,
        order_by: Vec::new(),
        aggregates: Vec::new(),
        limit: None,
        offset: None,
        contains_constant_false_condition: false,
        query_destination,
        distinctness: Distinctness::NonDistinct,
        values: Vec::new(),
        window: None,
        non_from_clause_subqueries: Vec::new(),
        input_cardinality_hint: None,
        estimated_output_rows: None,
        simple_aggregate: None,
        phantom_params: Vec::new(),
    };

    match &block.body {
        hir::QueryBlockBody::Select {
            distinctness,
            filter,
            grouping,
            windows: _,
        } => {
            plan.distinctness = Distinctness::from_ast(distinctness.as_ref());
            if let Some(filter) = filter {
                split_hir_predicate(
                    context.lower_query_expr(filter, block_id)?,
                    None,
                    &mut plan.where_clause,
                );
            }
            if let Some(grouping) = grouping {
                let exprs = grouping
                    .keys
                    .iter()
                    .map(|expression| context.lower_query_expr(expression, block_id))
                    .collect::<Result<Vec<_>>>()?;
                let having = grouping
                    .having
                    .as_ref()
                    .map(|expression| -> Result<Vec<PlanExpr>> {
                        let mut predicates = Vec::new();
                        split_hir_predicate(
                            context.lower_query_expr(expression, block_id)?,
                            None,
                            &mut predicates,
                        );
                        Ok(predicates
                            .into_iter()
                            .map(|predicate| predicate.expr)
                            .collect::<Vec<_>>())
                    })
                    .transpose()?;
                plan.group_by = Some(GroupBy {
                    sort_order: vec![ast::SortOrder::Asc; exprs.len()],
                    nulls_order: vec![None; exprs.len()],
                    exprs,
                    sort_elided: false,
                    having,
                });
            }
            plan.order_by = lower_hir_query_order_terms(context, block_id, query_order_by)?;
            (plan.limit, plan.offset) = lower_hir_query_limit(context, block_id, query_limit)?;
        }
        hir::QueryBlockBody::Values { rows } => {
            if !query_order_by.is_empty() {
                crate::bail_parse_error!("ORDER BY clause is not allowed with VALUES clause");
            }
            if query_limit.is_some() {
                crate::bail_parse_error!("LIMIT clause is not allowed with VALUES clause");
            }
            plan.values = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|expression| context.lower_query_expr(expression, block_id))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
        }
    }

    remove_duplicate_hir_order_terms(&mut plan.order_by);
    let (mut windows, aggregate_count_without_order_by) =
        collect_hir_aggregates_and_windows(&mut plan)?;
    if !plan.aggregates.is_empty()
        && plan
            .group_by
            .as_ref()
            .is_none_or(|group_by| group_by.exprs.is_empty())
        && windows.is_empty()
    {
        // An ungrouped aggregate produces one row, so ORDER BY cannot change
        // its result. Aggregates found only in the removed terms are not live
        // plan operations; in particular, their subqueries are intentionally
        // absent from the subquery preparation that follows.
        plan.order_by.clear();
        plan.aggregates.truncate(aggregate_count_without_order_by);
    }
    compute_hir_group_by_sort_order(&mut plan)?;
    register_hir_select_usage(context, &mut plan)?;
    prepare_hir_select_subqueries(context, &mut plan)?;
    super::subquery::finalize_hir_select_subqueries(&mut plan);
    validate_hir_select_vector_sizes(&plan)?;
    super::window::plan_windows(context.program, &mut plan, &mut windows)?;
    Ok(plan)
}

fn lower_hir_order_terms(
    context: &HirPlanContext<'_>,
    terms: &[hir::OrderTerm],
) -> Result<Vec<PlanOrderTerm>> {
    terms
        .iter()
        .map(|term| {
            Ok(PlanOrderTerm {
                expr: context.lower_expr(&term.expr)?,
                order: term.order,
                nulls: term.nulls,
            })
        })
        .collect()
}

fn lower_hir_query_order_terms(
    context: &HirPlanContext<'_>,
    block: hir::QueryBlockId,
    terms: &[hir::OrderTerm],
) -> Result<Vec<PlanOrderTerm>> {
    terms
        .iter()
        .map(|term| {
            Ok(PlanOrderTerm {
                expr: context.lower_query_expr(&term.expr, block)?,
                order: term.order,
                nulls: term.nulls,
            })
        })
        .collect()
}

fn lower_hir_limit(
    context: &HirPlanContext<'_>,
    limit: Option<&hir::Limit>,
) -> Result<(Option<PlanExpr>, Option<PlanExpr>)> {
    let Some(limit) = limit else {
        return Ok((None, None));
    };
    Ok((
        Some(context.lower_expr(&limit.limit)?),
        limit
            .offset
            .as_ref()
            .map(|offset| context.lower_expr(offset))
            .transpose()?,
    ))
}

fn lower_hir_query_limit(
    context: &HirPlanContext<'_>,
    block: hir::QueryBlockId,
    limit: Option<&hir::Limit>,
) -> Result<(Option<PlanExpr>, Option<PlanExpr>)> {
    let Some(limit) = limit else {
        return Ok((None, None));
    };
    Ok((
        Some(context.lower_query_expr(&limit.limit, block)?),
        limit
            .offset
            .as_ref()
            .map(|offset| context.lower_query_expr(offset, block))
            .transpose()?,
    ))
}

fn remove_duplicate_hir_order_terms(order_by: &mut Vec<PlanOrderTerm>) {
    let mut index = 0;
    while index < order_by.len() {
        if order_by[..index]
            .iter()
            .any(|previous| plan_exprs_equivalent(&previous.expr, &order_by[index].expr))
        {
            order_by.remove(index);
        } else {
            index += 1;
        }
    }
}

fn register_hir_select_usage(context: &HirPlanContext<'_>, plan: &mut SelectPlan) -> Result<()> {
    let expressions = plan
        .result_columns
        .iter()
        .map(|column| &column.expr)
        .chain(plan.where_clause.iter().map(|term| &term.expr))
        .chain(plan.order_by.iter().map(|term| &term.expr))
        .chain(plan.group_by.iter().flat_map(|group_by| {
            group_by
                .exprs
                .iter()
                .chain(group_by.having.iter().flatten())
        }))
        .chain(plan.values.iter().flatten())
        .chain(plan.limit.iter())
        .chain(plan.offset.iter())
        .cloned()
        .collect::<Vec<_>>();
    for expression in &expressions {
        let mut outputs = plan_expr_dependencies(expression)?
            .outputs
            .into_iter()
            .collect::<Vec<_>>();
        outputs.sort_unstable();
        for output in outputs {
            plan.table_references
                .add_outer_output(context.outer_output_reference(output)?);
        }
        plan.table_references.register_plan_expr_usage(expression)?;
    }
    Ok(())
}

fn prepare_hir_select_subqueries(
    context: &mut HirPlanContext<'_>,
    plan: &mut SelectPlan,
) -> Result<()> {
    let select_list = plan
        .result_columns
        .iter()
        .map(|column| &column.expr)
        .chain(plan.values.iter().flatten())
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        context,
        &mut plan.table_references,
        &select_list,
        super::plan::SubqueryOrigin::SelectList,
        &mut plan.non_from_clause_subqueries,
    )?;
    let where_clause = plan
        .where_clause
        .iter()
        .map(|term| &term.expr)
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        context,
        &mut plan.table_references,
        &where_clause,
        super::plan::SubqueryOrigin::SelectWhere,
        &mut plan.non_from_clause_subqueries,
    )?;
    let group_by = plan
        .group_by
        .iter()
        .flat_map(|group_by| group_by.exprs.iter())
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        context,
        &mut plan.table_references,
        &group_by,
        super::plan::SubqueryOrigin::SelectGroupBy,
        &mut plan.non_from_clause_subqueries,
    )?;
    let having = plan
        .group_by
        .iter()
        .flat_map(|group_by| group_by.having.iter().flatten())
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        context,
        &mut plan.table_references,
        &having,
        super::plan::SubqueryOrigin::SelectHaving,
        &mut plan.non_from_clause_subqueries,
    )?;
    let order_by = plan
        .order_by
        .iter()
        .map(|term| &term.expr)
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        context,
        &mut plan.table_references,
        &order_by,
        super::plan::SubqueryOrigin::SelectOrderBy,
        &mut plan.non_from_clause_subqueries,
    )?;
    let limit = plan
        .limit
        .iter()
        .chain(plan.offset.iter())
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        context,
        &mut plan.table_references,
        &limit,
        super::plan::SubqueryOrigin::SelectLimitOffset,
        &mut plan.non_from_clause_subqueries,
    )
}

fn validate_hir_select_vector_sizes(plan: &SelectPlan) -> Result<()> {
    for expression in plan
        .result_columns
        .iter()
        .map(|column| &column.expr)
        .chain(plan.where_clause.iter().map(|term| &term.expr))
        .chain(plan.order_by.iter().map(|term| &term.expr))
        .chain(plan.group_by.iter().flat_map(|group_by| {
            group_by
                .exprs
                .iter()
                .chain(group_by.having.iter().flatten())
        }))
        .chain(plan.values.iter().flatten())
        .chain(plan.limit.iter())
        .chain(plan.offset.iter())
    {
        if plan_expr_vector_size(expression, plan)? != 1 {
            crate::bail_parse_error!("row value misused");
        }
    }
    for aggregate in &plan.aggregates {
        for argument in &aggregate.args {
            if plan_expr_vector_size(argument, plan)? != 1 {
                crate::bail_parse_error!("row value misused");
            }
        }
    }
    Ok(())
}

fn collect_hir_aggregates_and_windows(plan: &mut SelectPlan) -> Result<(Vec<Window>, usize)> {
    let before_order = plan
        .result_columns
        .iter()
        .map(|column| column.expr.clone())
        .chain(
            plan.group_by
                .iter()
                .flat_map(|group_by| group_by.having.iter().flatten().cloned()),
        )
        .collect::<Vec<_>>();
    let mut windows = Vec::new();
    for expression in &before_order {
        collect_hir_functions(expression, &mut plan.aggregates, &mut windows)?;
    }

    let aggregate_count_before_order = plan.aggregates.len();
    let has_group_by = plan
        .group_by
        .as_ref()
        .is_some_and(|group_by| !group_by.exprs.is_empty());
    for term in &plan.order_by {
        let aggregate_count = plan.aggregates.len();
        collect_hir_functions(&term.expr, &mut plan.aggregates, &mut windows)?;
        let added_aggregate = plan.aggregates.len() > aggregate_count;
        if added_aggregate
            && !plan.table_references.joined_tables().is_empty()
            && !has_group_by
            && aggregate_count_before_order == 0
        {
            crate::bail_parse_error!(
                "misuse of aggregate: {}()",
                plan.aggregates[aggregate_count].func
            );
        }
    }

    if let Some(group_by) = &plan.group_by {
        if group_by.exprs.is_empty() && group_by.having.is_some() && plan.aggregates.is_empty() {
            crate::bail_parse_error!("HAVING clause on a non-aggregate query");
        }
    }
    Ok((windows, aggregate_count_before_order))
}

fn collect_hir_functions(
    expression: &PlanExpr,
    aggregates: &mut Vec<Aggregate>,
    windows: &mut Vec<Window>,
) -> Result<()> {
    walk_plan_expr(expression, &mut |node| {
        let PlanExpr::Function(function) = node else {
            return Ok(PlanWalkControl::Continue);
        };
        let aggregate = resolved_hir_aggregate(function.function.value());
        let window_only = match function.function.value() {
            Func::Window(window) => Some(AccumulatorFunc::Window(window.clone())),
            _ => None,
        };
        if function.window.is_some() || window_only.is_some() {
            let accumulator = window_only.or_else(|| aggregate.clone().map(AccumulatorFunc::Agg));
            let Some(accumulator) = accumulator else {
                return Err(LimboError::InternalError(
                    "resolved non-window function carries a window specification".to_string(),
                ));
            };
            collect_hir_window_input_aggregates(function, aggregates)?;
            add_hir_window_function(node, function, accumulator, windows)?;
            return Ok(PlanWalkControl::SkipChildren);
        }
        if let Some(aggregate) = aggregate {
            add_hir_aggregate(node, function, aggregate, aggregates)?;
            return Ok(PlanWalkControl::SkipChildren);
        }
        Ok(PlanWalkControl::Continue)
    })?;
    Ok(())
}

fn collect_hir_window_input_aggregates(
    function: &super::plan_expr::PlanFunctionCall,
    aggregates: &mut Vec<Aggregate>,
) -> Result<()> {
    let mut collect = |expression| collect_hir_group_aggregates(expression, aggregates);
    for expression in &function.arguments {
        collect(expression)?;
    }
    for term in function.argument_order.iter().chain(&function.within_group) {
        collect(&term.expr)?;
    }
    if let Some(filter) = &function.filter {
        collect(filter)?;
    }
    if let Some(window) = &function.window {
        for expression in &window.partition_by {
            collect(expression)?;
        }
        for term in &window.order_by {
            collect(&term.expr)?;
        }
        if let Some(frame) = &window.frame {
            if let super::plan_expr::PlanFrameBound::Following(expression)
            | super::plan_expr::PlanFrameBound::Preceding(expression) = &frame.start
            {
                collect(expression)?;
            }
            if let Some(
                super::plan_expr::PlanFrameBound::Following(expression)
                | super::plan_expr::PlanFrameBound::Preceding(expression),
            ) = &frame.end
            {
                collect(expression)?;
            }
        }
    }
    Ok(())
}

fn collect_hir_group_aggregates(
    expression: &PlanExpr,
    aggregates: &mut Vec<Aggregate>,
) -> Result<()> {
    walk_plan_expr(expression, &mut |node| {
        let PlanExpr::Function(function) = node else {
            return Ok(PlanWalkControl::Continue);
        };
        if function.window.is_some() || matches!(function.function.value(), Func::Window(_)) {
            return Ok(PlanWalkControl::SkipChildren);
        }
        let Some(aggregate) = resolved_hir_aggregate(function.function.value()) else {
            return Ok(PlanWalkControl::Continue);
        };
        add_hir_aggregate(node, function, aggregate, aggregates)?;
        Ok(PlanWalkControl::SkipChildren)
    })?;
    Ok(())
}

fn resolved_hir_aggregate(function: &Func) -> Option<AggFunc> {
    match function {
        Func::Agg(aggregate) => Some(aggregate.clone()),
        Func::External(external) if matches!(external.func, ExtFunc::Aggregate { .. }) => {
            Some(AggFunc::External(Arc::new(external.func.clone())))
        }
        _ => None,
    }
}

fn add_hir_aggregate(
    original_expr: &PlanExpr,
    function: &super::plan_expr::PlanFunctionCall,
    aggregate: AggFunc,
    aggregates: &mut Vec<Aggregate>,
) -> Result<()> {
    if aggregates
        .iter()
        .any(|candidate| plan_exprs_equivalent(&candidate.original_expr, original_expr))
    {
        return Ok(());
    }
    if !function.argument_order.is_empty() {
        crate::bail_parse_error!("ORDER BY clause is not supported yet in aggregate functions");
    }
    let distinctness = Distinctness::from_ast(function.distinctness.as_ref());
    let mut args = function.arguments.clone();
    if !function.within_group.is_empty() {
        if !matches!(
            aggregate,
            AggFunc::Mode | AggFunc::PercentileCont | AggFunc::PercentileDisc
        ) {
            crate::bail_parse_error!("WITHIN GROUP is not supported for this function");
        }
        if function.window.is_some() {
            crate::bail_parse_error!("ordered-set aggregate may not be used as a window function");
        }
        if function.distinctness.is_some() {
            crate::bail_parse_error!("DISTINCT is not supported for ordered-set aggregate");
        }
        if function.within_group.len() != 1 {
            crate::bail_parse_error!("WITHIN GROUP must specify exactly one ORDER BY expression");
        }
        let order = &function.within_group[0];
        if order.order == ast::SortOrder::Desc || order.nulls.is_some() {
            crate::bail_parse_error!(
                "DESC and NULLS ordering inside WITHIN GROUP are not supported yet"
            );
        }
        let expected_direct_args = usize::from(!matches!(aggregate, AggFunc::Mode));
        if args.len() != expected_direct_args {
            crate::bail_parse_error!("wrong number of arguments to ordered-set aggregate");
        }
        args.insert(0, order.expr.clone());
    } else if matches!(aggregate, AggFunc::Mode) {
        crate::bail_parse_error!("mode() requires a WITHIN GROUP (ORDER BY ...) clause");
    }
    if distinctness.is_distinct() && args.len() != 1 {
        crate::bail_parse_error!("DISTINCT aggregate functions must have exactly one argument");
    }
    aggregates.push(Aggregate {
        func: aggregate,
        args,
        original_expr: original_expr.clone(),
        distinctness,
        filter_expr: function.filter.as_deref().cloned(),
        fraction_reg: None,
    });
    Ok(())
}

fn add_hir_window_function(
    original_expr: &PlanExpr,
    function: &super::plan_expr::PlanFunctionCall,
    accumulator: AccumulatorFunc,
    windows: &mut Vec<Window>,
) -> Result<()> {
    if function.distinctness.is_some() {
        crate::bail_parse_error!("DISTINCT is not supported for window functions");
    }
    if matches!(accumulator, AccumulatorFunc::Window(_)) && function.filter.is_some() {
        crate::bail_parse_error!("FILTER clause may only be used with aggregate window functions");
    }
    if !function.within_group.is_empty() {
        crate::bail_parse_error!("ordered-set aggregate may not be used as a window function");
    }
    let specification = function.window.as_ref().ok_or_else(|| {
        LimboError::InternalError(
            "resolved window function has no window specification".to_string(),
        )
    })?;
    let planned = PlannedWindowSpec {
        partition_by: specification.partition_by.clone(),
        order_by: specification
            .order_by
            .iter()
            .map(|term| (term.expr.clone(), term.order, term.nulls))
            .collect(),
    };
    let frame = match &accumulator {
        AccumulatorFunc::Window(window) => window
            .coerced_frame()
            .unwrap_or(plan_hir_window_frame(specification)?),
        AccumulatorFunc::Agg(_) => plan_hir_window_frame(specification)?,
    };
    let window_index = windows
        .iter()
        .position(|window| window.is_equivalent_to_spec(&planned, &frame));
    let window = match window_index {
        Some(index) => &mut windows[index],
        None => {
            windows.push(Window::from_planned_spec(planned, frame));
            windows.last_mut().expect("window was just inserted")
        }
    };
    if !window
        .functions
        .iter()
        .any(|candidate| plan_exprs_equivalent(&candidate.original_expr, original_expr))
    {
        window.functions.push(WindowFunction {
            func: accumulator,
            original_expr: original_expr.clone(),
            rewritten: None,
        });
    }
    Ok(())
}

fn plan_hir_window_frame(specification: &super::plan_expr::PlanWindowSpec) -> Result<Frame> {
    let Some(frame) = &specification.frame else {
        return Ok(Frame::default());
    };
    if frame
        .exclude
        .as_ref()
        .is_some_and(|exclude| *exclude != ast::FrameExclude::NoOthers)
    {
        crate::bail_parse_error!("window frame EXCLUDE clauses are not supported yet");
    }
    Ok(Frame {
        mode: frame.mode,
        start: plan_hir_frame_boundary(&frame.start),
        end: frame
            .end
            .as_ref()
            .map(plan_hir_frame_boundary)
            .unwrap_or(FrameBoundary::CurrentRow),
    })
}

fn plan_hir_frame_boundary(boundary: &super::plan_expr::PlanFrameBound) -> FrameBoundary {
    match boundary {
        super::plan_expr::PlanFrameBound::CurrentRow => FrameBoundary::CurrentRow,
        super::plan_expr::PlanFrameBound::Following(expression) => {
            FrameBoundary::Following(expression.clone())
        }
        super::plan_expr::PlanFrameBound::Preceding(expression) => {
            FrameBoundary::Preceding(expression.clone())
        }
        super::plan_expr::PlanFrameBound::UnboundedFollowing => FrameBoundary::UnboundedFollowing,
        super::plan_expr::PlanFrameBound::UnboundedPreceding => FrameBoundary::UnboundedPreceding,
    }
}

fn compute_hir_group_by_sort_order(plan: &mut SelectPlan) -> Result<()> {
    let Some(group_by) = &mut plan.group_by else {
        return Ok(());
    };
    if group_by.exprs.is_empty() || plan.order_by.is_empty() {
        return Ok(());
    }
    let only_aggregate_or_constant = plan.order_by.iter().all(|term| {
        plan.aggregates
            .iter()
            .any(|aggregate| plan_exprs_equivalent(&aggregate.original_expr, &term.expr))
            || plan_expr_dependencies(&term.expr)
                .is_ok_and(|dependencies| dependencies.is_constant())
    });
    if only_aggregate_or_constant {
        group_by.sort_order.fill(plan.order_by[0].order);
        group_by.nulls_order.fill(plan.order_by[0].nulls);
        return Ok(());
    }
    for (index, expression) in group_by.exprs.iter().enumerate() {
        if let Some(term) = plan
            .order_by
            .iter()
            .find(|term| plan_exprs_equivalent(&term.expr, expression))
        {
            group_by.sort_order[index] = term.order;
            group_by.nulls_order[index] = term.nulls;
        }
    }
    Ok(())
}

fn prepare_hir_recursive_cte_plan(
    context: &mut HirPlanContext<'_>,
    cte: &hir::Cte,
    recursive: &hir::RecursiveCte,
) -> Result<Plan> {
    let mut initial_query = prepare_hir_query_plan(
        context,
        recursive.seed,
        QueryDestination::placeholder_for_subquery(),
    )?;
    let explicit_columns = cte
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    // A compound query gets its public column identity from its left-most
    // arm. Keep that identity when the recursive CTE overlays its stabilized
    // semantic facts; the right-most arm is only an execution detail.
    let mut result_columns = match &initial_query {
        Plan::Select(select) => select.result_columns.clone(),
        Plan::CompoundSelect {
            left, right_most, ..
        } => left
            .first()
            .map(|(select, _)| select.result_columns.clone())
            .unwrap_or_else(|| right_most.result_columns.clone()),
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => {
            return Err(LimboError::InternalError(
                "recursive CTE seed is not a SELECT query".to_string(),
            ));
        }
    };
    if result_columns.len() != cte.columns.len() {
        return Err(LimboError::InternalError(format!(
            "recursive CTE '{}' has {} semantic columns but {} seed columns",
            cte.name,
            cte.columns.len(),
            result_columns.len()
        )));
    }
    for (result, semantic) in result_columns.iter_mut().zip(&cte.columns) {
        result.name.clone_from(&semantic.name);
        result.type_fact.clone_from(&semantic.type_fact);
        result.affinity = if semantic.has_affinity {
            PlanExprAffinity::with_affinity(semantic.affinity)
        } else {
            PlanExprAffinity::no_affinity()
        };
        result.collation.clone_from(&semantic.collation);
        result.array_dimensions = type_fact_array_dimensions(&semantic.type_fact);
    }

    let mut previous_inputs = Vec::with_capacity(recursive.input_sources.len());
    let mut input_table_ids = Vec::with_capacity(recursive.input_sources.len());
    for source_id in &recursive.input_sources {
        let source = context.document.source(*source_id).ok_or_else(|| {
            LimboError::InternalError(format!("missing recursive input source {source_id}"))
        })?;
        let internal_id = context.identities.source(*source_id).ok_or_else(|| {
            LimboError::InternalError(format!(
                "missing plan identity for recursive input source {source_id}"
            ))
        })?;
        let identifier = source.alias.clone().unwrap_or_else(|| source.name.clone());
        let input = JoinedTable::new_recursive_cte_input(
            identifier,
            &result_columns,
            internal_id,
            Some(&explicit_columns),
        )?;
        input_table_ids.push(internal_id);
        previous_inputs.push((
            *source_id,
            context.recursive_inputs.insert(*source_id, input),
        ));
    }

    let arm_result = (|| {
        let mut arms = Vec::with_capacity(recursive.arms.len());
        for arm in &recursive.arms {
            let plan = prepare_hir_query_plan(
                context,
                arm.query,
                QueryDestination::placeholder_for_subquery(),
            )?;
            let Plan::Select(plan) = plan else {
                return Err(LimboError::InternalError(format!(
                    "recursive arm {} did not lower to one SELECT block",
                    arm.query
                )));
            };
            arms.push(plan);
        }
        Ok::<_, LimboError>(arms)
    })();
    for (source, previous) in previous_inputs {
        match previous {
            Some(previous) => {
                context.recursive_inputs.insert(source, previous);
            }
            None => {
                context.recursive_inputs.remove(&source);
            }
        }
    }
    let mut arms = arm_result?;
    let right_most = arms.pop().ok_or_else(|| {
        LimboError::InternalError(format!("recursive CTE '{}' has no recursive arm", cte.name))
    })?;
    let recursive_query = if arms.is_empty() {
        Plan::Select(right_most)
    } else {
        Plan::CompoundSelect {
            left: arms
                .into_iter()
                .map(|arm| (*arm, ast::CompoundOperator::UnionAll))
                .collect(),
            right_most,
            limit: None,
            offset: None,
            order_by: Vec::new(),
        }
    };
    reject_aggregates_and_windows_in_recursive_query(&recursive_query)?;

    let first_operator = recursive
        .arms
        .first()
        .map(|arm| arm.operator)
        .ok_or_else(|| {
            LimboError::InternalError(format!("recursive CTE '{}' has no operator", cte.name))
        })?;
    let union_all = match first_operator {
        ast::CompoundOperator::UnionAll => true,
        ast::CompoundOperator::Union => false,
        ast::CompoundOperator::Except | ast::CompoundOperator::Intersect => {
            crate::bail_parse_error!(
                "recursive CTEs must use UNION ALL or UNION between the initial and recursive queries"
            );
        }
    };
    let (limit, offset) = lower_hir_limit(context, recursive.limit.as_ref())?;
    let limit_expressions = limit.iter().chain(offset.iter()).collect::<Vec<_>>();
    if !limit_expressions.is_empty() {
        let limit_owner = match &mut initial_query {
            Plan::Select(select) => select.as_mut(),
            Plan::CompoundSelect { right_most, .. } => right_most.as_mut(),
            Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => {
                return Err(LimboError::InternalError(
                    "recursive CTE seed is not a SELECT query".to_string(),
                ));
            }
        };
        super::subquery::prepare_hir_expression_subqueries(
            context,
            &mut limit_owner.table_references,
            &limit_expressions,
            super::plan::SubqueryOrigin::SelectLimitOffset,
            &mut limit_owner.non_from_clause_subqueries,
        )?;
        for expression in limit_expressions {
            if plan_expr_vector_size(expression, limit_owner)? != 1 {
                crate::bail_parse_error!("row value misused");
            }
        }
    }
    let mut plan = Plan::RecursiveCte(Box::new(super::plan::RecursiveCtePlan {
        name: cte.name.clone(),
        initial_query: Box::new(initial_query),
        recursive_query: Box::new(recursive_query),
        result_columns,
        comparison_collations: recursive.comparison_collations.clone(),
        input_table_ids,
        union_all,
        limit,
        offset,
        queue_order: recursive
            .queue_order
            .iter()
            .map(|term| super::plan::RecursiveCteOrderTerm {
                result_column_index: term.output,
                order: term.order,
                nulls: term.nulls,
                explicit_collation: term.explicit_collation.clone(),
            })
            .collect(),
        query_destination: QueryDestination::placeholder_for_subquery(),
    }));
    super::subquery::mark_shared_cte_materialization_requirements_in_plan(&mut plan);
    Ok(plan)
}

/// Valid ways to refer to the rowid of a btree table.
pub const ROWID_STRS: [&str; 3] = ["rowid", "_rowid_", "oid"];

fn reject_aggregates_and_windows_in_recursive_query(query: &Plan) -> Result<()> {
    match query {
        Plan::Select(select) => {
            if !select.aggregates.is_empty() || select.group_by.is_some() {
                crate::bail_parse_error!("recursive aggregate queries not supported");
            }
            if select.window.is_some() {
                crate::bail_parse_error!("cannot use window functions in recursive queries");
            }
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            if left
                .iter()
                .any(|(select, _)| !select.aggregates.is_empty() || select.group_by.is_some())
                || !right_most.aggregates.is_empty()
                || right_most.group_by.is_some()
            {
                crate::bail_parse_error!("recursive aggregate queries not supported");
            }
            if left.iter().any(|(select, _)| select.window.is_some()) || right_most.window.is_some()
            {
                crate::bail_parse_error!("cannot use window functions in recursive queries");
            }
        }
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => {
            return Err(crate::LimboError::InternalError(
                "recursive CTE query is not a SELECT".to_string(),
            ));
        }
    }
    Ok(())
}

/**
  Returns the earliest point at which a WHERE term can be evaluated.
  For expressions referencing tables, this is the innermost loop that contains a row for each
  table referenced in the expression.
  For expressions not referencing any tables (e.g. constants), this is before the main loop is
  opened, because they do not need any table data.
*/
pub fn determine_where_to_eval_term(
    term: &WhereTerm,
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    if let Some(table_id) = term.from_outer_join {
        let loop_index = join_order
            .iter()
            .position(|table| table.table_id == table_id)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "outer-join source {table_id} is absent from the join order"
                ))
            })?;
        return Ok(EvalAt::Loop(loop_index));
    }

    determine_where_to_eval_expr(&term.expr, join_order, subqueries, table_references)
}

/// A bitmask representing a set of tables in a query plan.
/// Tables are numbered by their index in [SelectPlan::joined_tables].
/// In the bitmask, the first bit is unused so that a mask with all zeros
/// can represent "no tables".
///
/// E.g. table 0 is represented by bit index 1, table 1 by bit index 2, etc.
///
/// Usage in Join Optimization
///
/// In join optimization, [TableMask] is used to:
/// - Generate subsets of tables for dynamic programming in join optimization
/// - Ensure tables are joined in valid orders (e.g., respecting LEFT JOIN order)
///
/// Usage with constraints (WHERE clause)
///
/// [TableMask] helps determine:
/// - Which tables are referenced in a constraint
/// - When a constraint can be applied as a join condition (all referenced tables must be on the left side of the table being joined)
///
/// Note that although [TableReference]s contain an internal ID as well, in join order optimization
/// the [TableMask] refers to the index of the table in the original join order, not the internal ID.
/// This is simply because we want to represent the tables as a contiguous set of bits, and the internal ID
/// might not be contiguous after e.g. subquery unnesting or other transformations.
pub type TableMask = BitSet;

/// Returns a [TableMask] representing the tables referenced in the given expression.
///
/// This includes outer references from subqueries, even if the subquery plan has
/// already been consumed, by relying on the cached outer reference ids.
/// Used in the optimizer for constraint analysis.
pub fn table_mask_from_expr(
    top_level_expr: &PlanExpr,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<TableMask> {
    let mut mask = TableMask::default();
    let dependencies = plan_expr_dependencies(top_level_expr)?;

    for source in dependencies.sources() {
        add_source_to_table_mask(&mut mask, table_references, source)?;
    }

    for query in dependencies.subqueries {
        let subquery = subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "resolved subquery {query} is absent from the plan scope"
                ))
            })?;
        for source in subquery_outer_reference_ids(subquery) {
            add_source_to_table_mask(&mut mask, table_references, source)?;
        }
    }

    Ok(mask)
}

fn add_source_to_table_mask(
    mask: &mut TableMask,
    table_references: &TableReferences,
    source: PlanSourceId,
) -> Result<()> {
    match table_references.source_scope(source) {
        Some(PlanSourceScope::Joined(table_index)) => mask.set(table_index)?,
        Some(PlanSourceScope::OuterQuery | PlanSourceScope::Runtime) => {}
        None => {
            return Err(LimboError::InternalError(format!(
                "resolved source {source} is absent from the plan scope"
            )));
        }
    }
    Ok(())
}

/// Determines the earliest loop where an expression can be safely evaluated.
///
/// When a referenced table is not found in `join_order`, we check if it's a hash-join
/// build table and map the condition to the probe loop where its rows are produced.
/// Subquery references are also respected, even after their plans are consumed.
pub fn determine_where_to_eval_expr(
    top_level_expr: &PlanExpr,
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    let dependencies = plan_expr_dependencies(top_level_expr)?;
    let mut eval_at = EvalAt::BeforeLoop;

    for source in dependencies.sources() {
        if let Some(loop_idx) = source_loop_index(source, join_order, table_references)? {
            eval_at = eval_at.max(EvalAt::Loop(loop_idx));
        }
    }

    for query in dependencies.subqueries {
        let subquery = subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "resolved subquery {query} is absent from the plan scope"
                ))
            })?;
        match &subquery.state {
            SubqueryState::Evaluated { evaluated_at, .. } => {
                eval_at = eval_at.max(*evaluated_at);
            }
            SubqueryState::Unevaluated { .. } => {
                for source in subquery_outer_reference_ids(subquery) {
                    if let Some(loop_idx) = source_loop_index(source, join_order, table_references)?
                    {
                        eval_at = eval_at.max(EvalAt::Loop(loop_idx));
                    }
                }
            }
        }
    }

    Ok(eval_at)
}

fn subquery_outer_reference_ids(subquery: &NonFromClauseSubquery) -> Vec<PlanSourceId> {
    let mut sources = match &subquery.state {
        SubqueryState::Unevaluated { plan } => plan
            .as_ref()
            .map(|plan| plan.used_outer_query_ref_ids())
            .unwrap_or_default(),
        SubqueryState::Evaluated { outer_ref_ids, .. } => outer_ref_ids.clone(),
    };
    sources.extend(
        subquery
            .outer_outputs
            .iter()
            .flat_map(|output| output.source_dependencies.iter().copied()),
    );
    sources.sort_unstable();
    sources.dedup();
    sources
}

pub(crate) fn source_loop_index(
    source: PlanSourceId,
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
) -> Result<Option<usize>> {
    if let Some(loop_index) = join_order
        .iter()
        .position(|member| member.table_id == source)
    {
        return Ok(Some(loop_index));
    }

    let tables = table_references.ok_or_else(|| {
        LimboError::InternalError(format!(
            "resolved source {source} has no plan scope for evaluation"
        ))
    })?;
    if let Some(loop_index) = join_order
        .iter()
        .enumerate()
        .find_map(|(probe_idx, member)| {
            let probe_table = &tables.joined_tables()[member.original_idx];
            let Operation::HashJoin(hash_join) = &probe_table.op else {
                return None;
            };
            let build_table = &tables.joined_tables()[hash_join.build_table_idx];
            (build_table.internal_id == source).then_some(probe_idx)
        })
    {
        return Ok(Some(loop_index));
    }

    match tables.source_scope(source) {
        Some(PlanSourceScope::OuterQuery | PlanSourceScope::Runtime) => Ok(None),
        Some(PlanSourceScope::Joined(_)) => Err(LimboError::InternalError(format!(
            "joined source {source} is absent from the join order"
        ))),
        None => Err(LimboError::InternalError(format!(
            "resolved source {source} is absent from the plan scope"
        ))),
    }
}
