use std::sync::Arc;

use crate::alloc::TursoSliceExt;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast::{self, SortOrder};

use crate::{
    alloc::TursoIteratorExt,
    emit_explain,
    schema::{BTreeCharacteristics, BTreeTable, Column, Index, IndexColumn, Table},
    translate::{
        compound_select::emit_program_for_compound_select,
        emitter::select::{
            emit_materialized_build_inputs, emit_program_for_select,
            emit_program_for_select_with_resolver, emit_query,
        },
        expr::{translate_plan_expr_no_constant_opt, NoConstantOptReason},
        plan::{
            plan_has_outer_scope_dependency, plan_is_correlated, ColumnUsedMask, EvalAt,
            JoinOrderMember, JoinedTable, NonFromClauseSubquery, OuterQueryReference, Plan,
            PlanOuterOutputReference, PlanOutputFact, PlanSubqueryType, RuntimeOutputDefinition,
            RuntimeValueBinding, SetOperation, SubqueryEvalPhase, SubqueryOrigin, SubqueryState,
            TableReferences,
        },
        plan_expr::{
            parse_plan_signed_number, resolve_plan_comparison_affinity,
            resolve_plan_comparison_collation, walk_plan_expr, PlanCteId, PlanExpr,
            PlanExprAffinity, PlanExprFactSource, PlanSourceId, PlanSubqueryExpr, PlanSubqueryId,
            PlanWalkControl,
        },
    },
    types::Value,
    vdbe::{
        builder::{CursorKey, CursorType, MaterializedCteInfo, ProgramBuilder},
        insn::Insn,
        CursorID,
    },
    Numeric, Result,
};

use super::{
    emitter::{Resolver, TranslateCtx},
    main_loop::LoopLabels,
    plan::{Operation, QueryDestination, Scan, Search, SelectPlan},
    planner::{HirPlanContext, TableMask},
};

struct DirectMaterializedSubquery {
    index: Arc<Index>,
    affinity_str: Option<Arc<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaterializedFromClauseSubqueryStorage {
    TableBacked,
    DirectIndex,
}

enum FromClauseSubqueryExecutionMode {
    Coroutine,
    MaterializedTable,
    DirectMaterializedIndex(DirectMaterializedSubquery),
}

pub(crate) fn materialized_from_clause_subquery_storage(
    subquery: &crate::schema::FromClauseSubquery,
) -> Option<MaterializedFromClauseSubqueryStorage> {
    match subquery.plan.select_query_destination() {
        Some(QueryDestination::EphemeralTable { .. }) => {
            Some(MaterializedFromClauseSubqueryStorage::TableBacked)
        }
        Some(QueryDestination::EphemeralIndex { .. }) => {
            Some(MaterializedFromClauseSubqueryStorage::DirectIndex)
        }
        _ => None,
    }
}

/// Metadata for subqueries that have already been planned while walking one
/// outer expression tree. Columns carry their own facts in `PlanExpr`; this
/// adapter supplies the one kind of fact that lives outside the expression:
/// the output shape of a nested scalar subquery.
struct PlannedHirSubqueryFacts<'a> {
    subqueries: &'a [NonFromClauseSubquery],
    output_id: super::plan_expr::PlanOutputId,
    output: &'a PlanOutputFact,
}

impl PlannedHirSubqueryFacts<'_> {
    fn output_fact(&self, query: PlanSubqueryId, output: usize) -> Option<&PlanOutputFact> {
        self.subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)?
            .output_facts
            .get(output)
    }
}

impl PlanExprFactSource for PlannedHirSubqueryFacts<'_> {
    fn output_type_fact(
        &self,
        output: super::plan_expr::PlanOutputId,
    ) -> Option<super::semantic::hir::TypeFact> {
        (self.output_id == output).then(|| self.output.type_fact.clone())
    }

    fn output_affinity(&self, output: super::plan_expr::PlanOutputId) -> Option<PlanExprAffinity> {
        (self.output_id == output).then_some(self.output.affinity)
    }

    fn output_collation(
        &self,
        output: super::plan_expr::PlanOutputId,
    ) -> Option<super::collate::CollationSeq> {
        if self.output_id != output {
            return None;
        }
        self.output
            .collation
            .as_ref()
            .map(|collation| *collation.value())
    }

    fn subquery_output_type_fact(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<super::semantic::hir::TypeFact> {
        self.output_fact(query, output)
            .map(|fact| fact.type_fact.clone())
    }

    fn subquery_width(&self, query: PlanSubqueryId) -> Option<usize> {
        self.subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)
            .map(|subquery| subquery.output_facts.len())
    }

    fn subquery_output_affinity(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<PlanExprAffinity> {
        self.output_fact(query, output).map(|fact| fact.affinity)
    }

    fn subquery_output_collation(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<super::collate::CollationSeq> {
        self.output_fact(query, output)
            .and_then(|fact| fact.collation.as_ref())
            .map(|collation| *collation.value())
    }
}

/// Plan every resolved expression subquery reachable from `expressions`.
///
/// Semantic analysis has already resolved both the query identity and every
/// outer-column reference. This pass only chooses runtime storage, attaches
/// the caller's outer scope, and preserves output facts after emission consumes
/// the physical child plan.
pub fn prepare_hir_expression_subqueries(
    context: &mut HirPlanContext<'_>,
    referenced_tables: &mut TableReferences,
    expressions: &[&PlanExpr],
    origin: SubqueryOrigin,
    out: &mut Vec<NonFromClauseSubquery>,
) -> Result<()> {
    context.add_runtime_sources_to(referenced_tables)?;
    let mut subqueries = Vec::new();
    for expression in expressions {
        walk_plan_expr(expression, &mut |expression| {
            if let PlanExpr::Subquery(subquery) = expression {
                subqueries.push(subquery.clone());
            }
            Ok(PlanWalkControl::Continue)
        })?;
    }

    for subquery in subqueries {
        // An IN left-hand side can itself contain a scalar subquery. Plan that
        // dependency first so comparison affinity and collation see its facts.
        if let PlanSubqueryExpr::In { lhs, .. } = &subquery {
            prepare_hir_expression_subqueries(
                context,
                referenced_tables,
                &[lhs.as_ref()],
                origin,
                out,
            )?;
        }
        prepare_one_hir_expression_subquery(context, referenced_tables, &subquery, origin, out)?;
    }
    Ok(())
}

fn prepare_one_hir_expression_subquery(
    context: &mut HirPlanContext<'_>,
    referenced_tables: &mut TableReferences,
    subquery: &PlanSubqueryExpr,
    origin: SubqueryOrigin,
    out: &mut Vec<NonFromClauseSubquery>,
) -> Result<()> {
    let query = match subquery {
        PlanSubqueryExpr::Scalar { query, .. }
        | PlanSubqueryExpr::Exists(query)
        | PlanSubqueryExpr::In { query, .. } => *query,
    };
    if out.iter().any(|planned| planned.internal_id == query) {
        return Ok(());
    }

    let semantic_query = context.identities.semantic_subquery(query).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "plan subquery {query} has no semantic query identity"
        ))
    })?;
    let outer_query_refs = hir_outer_query_refs(referenced_tables)?;
    let previous_outer_query_refs =
        std::mem::replace(&mut context.outer_query_refs, outer_query_refs);

    let initial_destination = match subquery {
        PlanSubqueryExpr::Exists(_) => QueryDestination::ExistsSubqueryResult {
            result_reg: context.program.alloc_register(),
        },
        PlanSubqueryExpr::Scalar { .. } | PlanSubqueryExpr::In { .. } => QueryDestination::Unset,
    };
    let plan_result =
        super::planner::prepare_hir_query_plan(context, semantic_query, initial_destination);
    context.outer_query_refs = previous_outer_query_refs;
    let mut plan = plan_result?;
    let output_facts = PlanOutputFact::for_plan(&plan);

    let query_type = match subquery {
        PlanSubqueryExpr::Exists(_) => {
            let QueryDestination::ExistsSubqueryResult { result_reg } =
                plan.select_query_destination().ok_or_else(|| {
                    crate::LimboError::InternalError(format!(
                        "semantic EXISTS subquery {query} did not produce a SELECT plan"
                    ))
                })?
            else {
                return Err(crate::LimboError::InternalError(format!(
                    "semantic EXISTS subquery {query} lost its result destination"
                )));
            };
            PlanSubqueryType::Exists {
                result_reg: *result_reg,
            }
        }
        PlanSubqueryExpr::Scalar { output, .. } => {
            let width = output_facts.len();
            if *output >= width {
                return Err(crate::LimboError::InternalError(format!(
                    "scalar subquery {query} has {width} outputs, requested output {output}"
                )));
            }
            let result_reg_start = context.program.alloc_registers(width);
            *plan.select_query_destination_mut().ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "semantic scalar subquery {query} did not produce a SELECT plan"
                ))
            })? = QueryDestination::RowValueSubqueryResult {
                result_reg_start,
                num_regs: width,
            };
            clamp_hir_scalar_subquery_limit(&mut plan);
            PlanSubqueryType::RowValue {
                result_reg_start,
                num_regs: width,
            }
        }
        PlanSubqueryExpr::In { lhs, .. } => {
            let lhs_columns: &[PlanExpr] = match lhs.as_ref() {
                PlanExpr::Row(columns) => columns,
                scalar => std::slice::from_ref(scalar),
            };
            let result_columns = plan.select_result_columns();
            if lhs_columns.len() != result_columns.len() {
                crate::bail_parse_error!(
                    "sub-select returns {} columns - expected {}",
                    result_columns.len(),
                    lhs_columns.len()
                );
            }

            let mut affinity = String::with_capacity(lhs_columns.len());
            let columns = lhs_columns
                .iter()
                .zip(result_columns.iter().zip(&output_facts))
                .enumerate()
                .map(|(index, (lhs, (rhs, rhs_fact)))| {
                    let facts = PlannedHirSubqueryFacts {
                        subqueries: out,
                        output_id: rhs.id,
                        output: rhs_fact,
                    };
                    let rhs_output = PlanExpr::Output(rhs.id);
                    affinity.push(
                        resolve_plan_comparison_affinity(lhs, &rhs_output, &facts).aff_mask(),
                    );
                    Ok(IndexColumn {
                        name: rhs.name.clone(),
                        order: SortOrder::Asc,
                        pos_in_table: index,
                        collation: Some(resolve_plan_comparison_collation(
                            lhs,
                            &rhs_output,
                            &facts,
                        )?),
                        default: None,
                        expr: None,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let affinity_str = Arc::new(affinity);
            let index = Arc::new(Index {
                columns,
                name: format!("ephemeral_index_hir_subquery_{query}"),
                table_name: String::new(),
                ephemeral: true,
                has_rowid: false,
                root_page: 0,
                unique: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            });
            let cursor_id = context
                .program
                .alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            *plan.select_query_destination_mut().ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "semantic IN subquery {query} did not produce a SELECT plan"
                ))
            })? = QueryDestination::EphemeralIndex {
                cursor_id,
                index,
                affinity_str: Some(affinity_str.clone()),
                is_delete: false,
            };
            PlanSubqueryType::In {
                cursor_id,
                affinity_str,
            }
        }
    };

    let outer_outputs = plan_outer_output_references(&plan);
    let correlated = plan_has_outer_scope_dependency(&plan)
        || hir_plan_reads_runtime_row(context, &plan)?
        || !outer_outputs.is_empty();
    propagate_hir_outer_refs_from_plan(referenced_tables, &plan)?;
    out.push(NonFromClauseSubquery {
        internal_id: query,
        query_type,
        output_facts,
        outer_outputs,
        state: SubqueryState::Unevaluated {
            plan: Some(Box::new(plan)),
        },
        correlated,
        origin,
        eval_phase: origin.phase_floor(),
    });
    Ok(())
}

/// Collect result registers read from outside this physical child plan.
/// Outputs defined by any SELECT inside the child are local even when a deeper
/// nested query refers to them.
fn plan_outer_output_references(plan: &Plan) -> Vec<PlanOuterOutputReference> {
    fn collect_select(
        select: &SelectPlan,
        defined: &mut HashSet<super::plan_expr::PlanOutputId>,
        references: &mut Vec<PlanOuterOutputReference>,
    ) {
        defined.extend(select.result_columns.iter().map(|column| column.id));
        references.extend(select.table_references.outer_outputs().iter().cloned());
        for subquery in &select.non_from_clause_subqueries {
            references.extend(subquery.outer_outputs.iter().cloned());
        }
        for table in select.table_references.joined_tables() {
            if let Table::FromClauseSubquery(subquery) = &table.table {
                collect_plan(&subquery.plan, defined, references);
            }
        }
    }

    fn collect_plan(
        plan: &Plan,
        defined: &mut HashSet<super::plan_expr::PlanOutputId>,
        references: &mut Vec<PlanOuterOutputReference>,
    ) {
        match plan {
            Plan::Select(select) => collect_select(select, defined, references),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                for (select, _) in left {
                    collect_select(select, defined, references);
                }
                collect_select(right_most, defined, references);
            }
            Plan::RecursiveCte(recursive) => {
                collect_plan(&recursive.initial_query, defined, references);
                collect_plan(&recursive.recursive_query, defined, references);
            }
            Plan::Delete(_) | Plan::Update(_) => {}
        }
    }

    let mut defined = HashSet::default();
    let mut references = Vec::new();
    collect_plan(plan, &mut defined, &mut references);
    references.retain(|reference| !defined.contains(&reference.output));
    references.sort_by_key(|reference| reference.output);
    references.dedup_by_key(|reference| reference.output);
    references
}

/// Cursorless row images such as NEW, OLD, and EXCLUDED are supplied by the
/// resolver rather than `TableReferences`. They still make a child query vary
/// with its containing DML/trigger row, so it must not be guarded by `Once`.
fn hir_plan_reads_runtime_row(context: &HirPlanContext<'_>, plan: &Plan) -> Result<bool> {
    for source in &context.document.sources {
        if !matches!(
            &source.kind,
            super::semantic::hir::SourceKind::Pseudo { .. }
                | super::semantic::hir::SourceKind::SchemaExpression
        ) {
            continue;
        }
        let plan_source = context.identities.source(source.id).ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "runtime source {} has no plan identity",
                source.id
            ))
        })?;
        let dependency = plan.source_row_dependency(plan_source)?;
        if dependency.rowid || !dependency.columns.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn hir_outer_query_refs(referenced_tables: &TableReferences) -> Result<Vec<OuterQueryReference>> {
    referenced_tables
        .joined_tables()
        .iter()
        .map(|table| {
            Ok(OuterQueryReference {
                table: table.table.clone(),
                read_programs: Arc::clone(&table.read_programs),
                identifier: table.identifier.clone(),
                internal_id: table.internal_id,
                using_dedup_hidden_cols: table.using_dedup_hidden_cols()?,
                col_used_mask: ColumnUsedMask::default(),
                cte_definition_only: false,
                rowid_referenced: false,
                scope_depth: 0,
            })
        })
        .chain(referenced_tables.outer_query_refs().iter().map(|table| {
            Ok(OuterQueryReference {
                table: table.table.clone(),
                read_programs: Arc::clone(&table.read_programs),
                identifier: table.identifier.clone(),
                internal_id: table.internal_id,
                using_dedup_hidden_cols: table.using_dedup_hidden_cols.clone(),
                col_used_mask: ColumnUsedMask::default(),
                cte_definition_only: table.cte_definition_only,
                rowid_referenced: false,
                scope_depth: table.scope_depth + 1,
            })
        }))
        .collect()
}

fn clamp_hir_scalar_subquery_limit(plan: &mut Plan) {
    fn clamp(limit: &mut Option<PlanExpr>) {
        let keep = limit.as_ref().is_some_and(|limit| {
            matches!(
                parse_plan_signed_number(limit),
                Ok(Value::Numeric(Numeric::Integer(value))) if (0..=1).contains(&value)
            )
        });
        if !keep {
            *limit = Some(PlanExpr::Literal(ast::Literal::Numeric("1".to_string())));
        }
    }

    match plan {
        Plan::Select(select) => clamp(&mut select.limit),
        Plan::CompoundSelect { limit, .. } => clamp(limit),
        Plan::RecursiveCte(recursive) => clamp(&mut recursive.limit),
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("semantic scalar subquery must be a query plan")
        }
    }
}

fn propagate_hir_outer_refs_from_plan(parent: &mut TableReferences, plan: &Plan) -> Result<()> {
    enum PendingPlan<'a> {
        Plan(&'a Plan),
        Select(&'a SelectPlan),
    }

    let mut pending = vec![PendingPlan::Plan(plan)];
    while let Some(pending_plan) = pending.pop() {
        let select = match pending_plan {
            PendingPlan::Plan(Plan::Select(select)) => select,
            PendingPlan::Plan(Plan::CompoundSelect {
                left, right_most, ..
            }) => {
                pending.push(PendingPlan::Select(right_most));
                pending.extend(
                    left.iter()
                        .rev()
                        .map(|(select, _)| PendingPlan::Select(select)),
                );
                continue;
            }
            PendingPlan::Plan(Plan::RecursiveCte(recursive)) => {
                pending.push(PendingPlan::Plan(&recursive.recursive_query));
                pending.push(PendingPlan::Plan(&recursive.initial_query));
                continue;
            }
            PendingPlan::Plan(Plan::Delete(_) | Plan::Update(_)) => {
                return Err(crate::LimboError::InternalError(
                    "DML plan reached semantic expression-subquery propagation".to_string(),
                ));
            }
            PendingPlan::Select(select) => select,
        };

        for outer in select
            .table_references
            .outer_query_refs()
            .iter()
            .filter(|outer| outer.is_used())
        {
            if let Some(joined) = parent.find_joined_table_by_internal_id_mut(outer.internal_id) {
                for column in outer.col_used_mask.iter() {
                    if column >= joined.column_use_counts.len() {
                        joined.column_use_counts.resize(column + 1, 0);
                    }
                    joined.column_use_counts[column] += 1;
                }
                joined.col_used_mask.union_with(&outer.col_used_mask)?;
            }
            if let Some(parent_outer) =
                parent.find_outer_query_ref_by_internal_id_mut(outer.internal_id)
            {
                parent_outer
                    .col_used_mask
                    .union_with(&outer.col_used_mask)?;
                parent_outer.rowid_referenced |= outer.rowid_referenced;
            }
        }

        // A resolved derived table cannot see sibling FROM sources, but it can
        // still carry a reference to one of this query's inherited scopes.
        pending.extend(
            select
                .table_references
                .joined_tables()
                .iter()
                .rev()
                .filter_map(|table| match &table.table {
                    Table::FromClauseSubquery(subquery) => {
                        Some(PendingPlan::Plan(subquery.plan.as_ref()))
                    }
                    _ => None,
                }),
        );
    }
    Ok(())
}

// Count the CTE reads in this query tree that can share one materialized
// result.
//
// Reads from correlated post-write RETURNING subqueries are skipped because
// they run once per updated row instead of once for the statement.
fn count_shared_cte_references<'a>(
    counts: &mut HashMap<PlanCteId, usize>,
    table_references: &'a TableReferences,
    non_from_clause_subqueries: &'a [NonFromClauseSubquery],
) {
    let mut pending = Vec::new();
    let mut expanded_ctes = HashSet::default();
    collect_shared_cte_references(
        counts,
        table_references,
        non_from_clause_subqueries,
        &mut pending,
        &mut expanded_ctes,
    );
    count_shared_cte_references_in_pending(counts, pending, &mut expanded_ctes);
}

fn collect_shared_cte_references<'a>(
    counts: &mut HashMap<PlanCteId, usize>,
    table_references: &'a TableReferences,
    non_from_clause_subqueries: &'a [NonFromClauseSubquery],
    pending: &mut Vec<&'a Plan>,
    expanded_ctes: &mut HashSet<PlanCteId>,
) {
    for table in table_references.joined_tables() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &table.table {
            if let Some(cte_id) = from_clause_subquery.cte_id() {
                *counts.entry(cte_id).or_default() += 1;
                // Every CTE occurrence embeds a physical copy of the same
                // semantic definition. Count the occurrence, but walk that
                // definition only once so nested CTE reads are discovered
                // without multiplying them by the number of outer reads.
                if expanded_ctes.insert(cte_id) {
                    pending.push(from_clause_subquery.plan.as_ref());
                }
            } else {
                pending.push(from_clause_subquery.plan.as_ref());
            }
        }
    }

    for subquery in non_from_clause_subqueries {
        let SubqueryState::Unevaluated {
            plan: Some(subquery_plan),
        } = &subquery.state
        else {
            continue;
        };
        // A correlated RETURNING subquery runs after each updated row is
        // written, so its CTE reads must not be counted as part of the shared
        // pre-write snapshot used by earlier readers in the same statement.
        if subquery.origin.is_post_write_returning() && subquery.correlated {
            continue;
        }
        pending.push(subquery_plan);
    }
}

fn count_shared_cte_references_in_plan(counts: &mut HashMap<PlanCteId, usize>, plan: &Plan) {
    let mut expanded_ctes = HashSet::default();
    count_shared_cte_references_in_pending(counts, vec![plan], &mut expanded_ctes);
}

fn count_shared_cte_references_in_pending<'a>(
    counts: &mut HashMap<PlanCteId, usize>,
    mut pending: Vec<&'a Plan>,
    expanded_ctes: &mut HashSet<PlanCteId>,
) {
    while let Some(plan) = pending.pop() {
        match plan {
            Plan::Select(select_plan) => collect_shared_cte_references(
                counts,
                &select_plan.table_references,
                &select_plan.non_from_clause_subqueries,
                &mut pending,
                expanded_ctes,
            ),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                for (select_plan, _) in left {
                    collect_shared_cte_references(
                        counts,
                        &select_plan.table_references,
                        &select_plan.non_from_clause_subqueries,
                        &mut pending,
                        expanded_ctes,
                    );
                }
                collect_shared_cte_references(
                    counts,
                    &right_most.table_references,
                    &right_most.non_from_clause_subqueries,
                    &mut pending,
                    expanded_ctes,
                );
            }
            Plan::RecursiveCte(recursive_cte) => {
                pending.push(&recursive_cte.initial_query);
                pending.push(&recursive_cte.recursive_query);
            }
            Plan::Delete(_) | Plan::Update(_) => {}
        }
    }
}

/// Mark CTE references that must be materialized once and shared across
/// multiple reads of the same query tree.
pub(crate) fn mark_shared_cte_materialization_requirements(
    table_references: &mut TableReferences,
    non_from_clause_subqueries: &mut [NonFromClauseSubquery],
) {
    let mut shared_ref_counts = HashMap::default();
    count_shared_cte_references(
        &mut shared_ref_counts,
        table_references,
        non_from_clause_subqueries,
    );
    let mut nested_plans = Vec::new();
    annotate_shared_cte_materialization_requirements(
        &shared_ref_counts,
        table_references,
        non_from_clause_subqueries,
        &mut nested_plans,
    );
    for nested_plan in nested_plans {
        annotate_shared_cte_materialization_requirements_in_plan(&shared_ref_counts, nested_plan);
    }
}

/// Recompute CTE sharing for a complete query plan. Compound arms must be
/// counted together: a CTE read once in each arm is still a shared read of the
/// same semantic definition.
pub(crate) fn mark_shared_cte_materialization_requirements_in_plan(plan: &mut Plan) {
    let mut shared_ref_counts = HashMap::default();
    count_shared_cte_references_in_plan(&mut shared_ref_counts, plan);
    annotate_shared_cte_materialization_requirements_in_plan(&shared_ref_counts, plan);
}

fn annotate_shared_cte_materialization_requirements<'a>(
    shared_ref_counts: &HashMap<PlanCteId, usize>,
    table_references: &'a mut TableReferences,
    non_from_clause_subqueries: &'a mut [NonFromClauseSubquery],
    nested_plans: &mut Vec<&'a mut Plan>,
) {
    for table in table_references.joined_tables_mut().iter_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table.table {
            let from_clause_subquery = Arc::make_mut(from_clause_subquery);
            let shared_materialization = from_clause_subquery.cte_id().is_some_and(|cte_id| {
                shared_ref_counts.get(&cte_id).copied().unwrap_or_default() > 1
                    && !plan_has_outer_scope_dependency(&from_clause_subquery.plan)
            });
            from_clause_subquery.set_shared_materialization(shared_materialization);
            if let Some(cte_id) = from_clause_subquery.cte_id() {
                tracing::trace!(
                    cte_id = %cte_id,
                    shared_ref_count = shared_ref_counts.get(&cte_id).copied().unwrap_or_default(),
                    shared_materialization,
                    outer_scope_dependency = plan_has_outer_scope_dependency(
                        &from_clause_subquery.plan,
                    ),
                    contains_nested_correlation = plan_is_correlated(&from_clause_subquery.plan),
                    identifier = %table.identifier,
                    "annotated CTE materialization requirements"
                );
            }
            nested_plans.push(from_clause_subquery.plan.as_mut());
        }
    }

    for subquery in non_from_clause_subqueries.iter_mut() {
        let SubqueryState::Unevaluated {
            plan: Some(subquery_plan),
        } = &mut subquery.state
        else {
            continue;
        };
        nested_plans.push(subquery_plan.as_mut());
    }
}

fn annotate_shared_cte_materialization_requirements_in_plan(
    shared_ref_counts: &HashMap<PlanCteId, usize>,
    plan: &mut Plan,
) {
    let mut pending = vec![plan];
    while let Some(plan) = pending.pop() {
        match plan {
            Plan::Select(select_plan) => {
                annotate_shared_cte_materialization_requirements(
                    shared_ref_counts,
                    &mut select_plan.table_references,
                    &mut select_plan.non_from_clause_subqueries,
                    &mut pending,
                );
            }
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                for (select_plan, _) in left.iter_mut() {
                    annotate_shared_cte_materialization_requirements(
                        shared_ref_counts,
                        &mut select_plan.table_references,
                        &mut select_plan.non_from_clause_subqueries,
                        &mut pending,
                    );
                }
                annotate_shared_cte_materialization_requirements(
                    shared_ref_counts,
                    &mut right_most.table_references,
                    &mut right_most.non_from_clause_subqueries,
                    &mut pending,
                );
            }
            Plan::RecursiveCte(recursive_cte) => {
                pending.push(&mut recursive_cte.initial_query);
                pending.push(&mut recursive_cte.recursive_query);
            }
            Plan::Delete(_) | Plan::Update(_) => {}
        }
    }
}

enum CteMaterializationWork<'a> {
    Plan(&'a Plan),
    Select(&'a SelectPlan),
    Materialize(PlanSourceId),
}

fn push_cte_materialization_work_for_tables<'a>(
    pending: &mut Vec<CteMaterializationWork<'a>>,
    tables: &'a TableReferences,
) {
    for table in tables.joined_tables().iter().rev() {
        let Table::FromClauseSubquery(subquery) = &table.table else {
            continue;
        };
        pending.push(CteMaterializationWork::Materialize(table.internal_id));
        pending.push(CteMaterializationWork::Plan(subquery.plan.as_ref()));
    }
}

fn collect_cte_materialization_order<'a>(
    mut pending: Vec<CteMaterializationWork<'a>>,
) -> Vec<PlanSourceId> {
    let mut materialization_order = Vec::new();
    while let Some(work) = pending.pop() {
        let select = match work {
            CteMaterializationWork::Plan(Plan::Select(select)) => select,
            CteMaterializationWork::Plan(Plan::CompoundSelect {
                left, right_most, ..
            }) => {
                pending.push(CteMaterializationWork::Select(right_most));
                pending.extend(
                    left.iter()
                        .rev()
                        .map(|(select, _)| CteMaterializationWork::Select(select)),
                );
                continue;
            }
            CteMaterializationWork::Plan(Plan::RecursiveCte(recursive)) => {
                pending.push(CteMaterializationWork::Plan(&recursive.recursive_query));
                pending.push(CteMaterializationWork::Plan(&recursive.initial_query));
                continue;
            }
            CteMaterializationWork::Plan(Plan::Delete(_) | Plan::Update(_)) => continue,
            CteMaterializationWork::Select(select) => select,
            CteMaterializationWork::Materialize(internal_id) => {
                materialization_order.push(internal_id);
                continue;
            }
        };

        for subquery in select.non_from_clause_subqueries.iter().rev() {
            if let SubqueryState::Unevaluated {
                plan: Some(subquery_plan),
            } = &subquery.state
            {
                pending.push(CteMaterializationWork::Plan(subquery_plan));
            }
        }
        push_cte_materialization_work_for_tables(&mut pending, &select.table_references);
    }
    materialization_order
}

enum MutablePlanNode<'a> {
    Plan(&'a mut Plan),
    Select(&'a mut SelectPlan),
}

fn find_nested_table_mut<'a>(
    tables: &'a mut TableReferences,
    internal_id: PlanSourceId,
) -> Option<&'a mut JoinedTable> {
    if let Some(table_index) = tables
        .joined_tables()
        .iter()
        .position(|table| table.internal_id == internal_id)
    {
        return tables.joined_tables_mut().get_mut(table_index);
    }

    let mut pending = Vec::new();
    for table in tables.joined_tables_mut().iter_mut().rev() {
        if let Table::FromClauseSubquery(subquery) = &mut table.table {
            pending.push(MutablePlanNode::Plan(Arc::make_mut(subquery).plan.as_mut()));
        }
    }

    while let Some(node) = pending.pop() {
        let select = match node {
            MutablePlanNode::Plan(Plan::Select(select)) => select.as_mut(),
            MutablePlanNode::Plan(Plan::CompoundSelect {
                left, right_most, ..
            }) => {
                pending.push(MutablePlanNode::Select(right_most));
                pending.extend(
                    left.iter_mut()
                        .rev()
                        .map(|(select, _)| MutablePlanNode::Select(select)),
                );
                continue;
            }
            MutablePlanNode::Plan(Plan::RecursiveCte(recursive)) => {
                pending.push(MutablePlanNode::Plan(&mut recursive.recursive_query));
                pending.push(MutablePlanNode::Plan(&mut recursive.initial_query));
                continue;
            }
            MutablePlanNode::Plan(Plan::Delete(_) | Plan::Update(_)) => continue,
            MutablePlanNode::Select(select) => select,
        };

        if let Some(table_index) = select
            .table_references
            .joined_tables()
            .iter()
            .position(|table| table.internal_id == internal_id)
        {
            return select
                .table_references
                .joined_tables_mut()
                .get_mut(table_index);
        }

        for subquery in select.non_from_clause_subqueries.iter_mut().rev() {
            if let SubqueryState::Unevaluated {
                plan: Some(subquery_plan),
            } = &mut subquery.state
            {
                pending.push(MutablePlanNode::Plan(subquery_plan.as_mut()));
            }
        }
        for table in select.table_references.joined_tables_mut().iter_mut().rev() {
            if let Table::FromClauseSubquery(subquery) = &mut table.table {
                pending.push(MutablePlanNode::Plan(Arc::make_mut(subquery).plan.as_mut()));
            }
        }
    }
    None
}

fn materialize_cte_table(
    program: &mut ProgramBuilder,
    table_reference: &mut JoinedTable,
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    let Table::FromClauseSubquery(from_clause_subquery) = &mut table_reference.table else {
        unreachable!("CTE materialization work referenced a non-subquery table");
    };
    let from_clause_subquery = Arc::make_mut(from_clause_subquery);
    let Some(cte_id) = from_clause_subquery.cte_id() else {
        return Ok(());
    };
    if program.get_materialized_cte(cte_id).is_some()
        || !from_clause_subquery.requires_table_materialization()
    {
        return Ok(());
    }

    tracing::trace!(
        cte_id = %cte_id,
        identifier = %table_reference.identifier,
        "pre-materializing shared CTE"
    );
    let (result_columns_start, cte_cursor_id, cte_table) = emit_materialized_subquery_table(
        program,
        from_clause_subquery.plan.as_mut(),
        t_ctx,
        &from_clause_subquery.columns,
    )?;
    program.register_materialized_cte(
        cte_id,
        MaterializedCteInfo {
            cursor_id: cte_cursor_id,
            table: cte_table,
            num_columns: from_clause_subquery.columns.len(),
        },
    );
    from_clause_subquery.materialized_cursor_id = Some(cte_cursor_id);
    from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
    program.set_subquery_result_reg(table_reference.internal_id, result_columns_start);
    Ok(())
}

fn pre_materialize_multi_ref_ctes_in_tables(
    program: &mut ProgramBuilder,
    tables: &mut TableReferences,
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    let mut pending = Vec::new();
    push_cte_materialization_work_for_tables(&mut pending, tables);
    for internal_id in collect_cte_materialization_order(pending) {
        let table_reference = find_nested_table_mut(tables, internal_id)
            .expect("CTE materialization work must reference a reachable table");
        materialize_cte_table(program, table_reference, t_ctx)?;
    }
    Ok(())
}

fn choose_from_clause_subquery_execution_mode(
    operation: &Operation,
    from_clause_subquery: &crate::schema::FromClauseSubquery,
) -> FromClauseSubqueryExecutionMode {
    let needs_materialized_seek = matches!(
        operation,
        Operation::Search(Search::Seek {
            index: Some(index), ..
        }) if index.ephemeral
    );

    // Compound SELECTs still need their own internal ephemeral indexes for
    // UNION/INTERSECT/EXCEPT bookkeeping. Reusing the subquery's synthesized
    // seek index as the storage target would collapse those roles together and
    // break set-operation semantics, so keep the direct-index fast path limited
    // to simple SELECT plans.
    let can_direct_materialize_index = from_clause_subquery.supports_direct_index_materialization();

    match operation {
        Operation::Search(Search::Seek {
            index: Some(index),
            seek_def,
        }) if index.ephemeral && can_direct_materialize_index => {
            FromClauseSubqueryExecutionMode::DirectMaterializedIndex(DirectMaterializedSubquery {
                index: index.handle(),
                affinity_str: super::plan::synthesized_seek_affinity_str(index, seek_def),
            })
        }
        _ if needs_materialized_seek => FromClauseSubqueryExecutionMode::MaterializedTable,
        _ if from_clause_subquery.requires_table_materialization() => {
            FromClauseSubqueryExecutionMode::MaterializedTable
        }
        _ => FromClauseSubqueryExecutionMode::Coroutine,
    }
}

/// Emit the subqueries contained in the FROM clause.
/// This is done first so the results can be read in the main query loop.
pub fn emit_from_clause_subqueries(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &mut TableReferences,
    join_order: &[JoinOrderMember],
) -> Result<()> {
    if tables.joined_tables().is_empty() {
        emit_explain!(program, false, "SCAN CONSTANT ROW".to_owned());
    }

    // FIRST PASS: Pre-materialize all recursively reachable multi-ref / hinted CTEs
    // before any coroutine bodies are emitted. Otherwise a coroutine could try to
    // OpenDup a CTE whose backing table has not been created yet.
    pre_materialize_multi_ref_ctes_in_tables(program, tables, t_ctx)?;

    // Build the iteration order: join_order first (execution order), then any
    // hash-join build tables that aren't already in the join order.
    let mut visit_order: Vec<usize> = join_order
        .iter()
        .map(|member| member.original_idx)
        .collect();
    let visit_set: TableMask = visit_order.iter().copied().try_collect()?;
    for table in tables.joined_tables().iter() {
        if let Operation::HashJoin(hash_join_op) = &table.op {
            let build_idx = hash_join_op.build_table_idx;
            if !visit_set.get(build_idx) {
                visit_order.push(build_idx);
            }
        }
    }

    // Build lookup from table index to is_outer for LEFT-JOIN annotations
    let outer_table_set: TableMask = join_order
        .iter()
        .filter(|m| m.is_outer)
        .map(|m| m.original_idx)
        .try_collect()?;

    for table_index in visit_order {
        let table_reference = &mut tables.joined_tables_mut()[table_index];
        let left_join_suffix = if outer_table_set.get(table_index) {
            " LEFT-JOIN"
        } else {
            ""
        };
        emit_explain!(
            program,
            true,
            match &table_reference.op {
                Operation::Scan(scan) => {
                    let table_name =
                        if table_reference.table.get_name() == table_reference.identifier {
                            table_reference.identifier.clone()
                        } else {
                            format!(
                                "{} AS {}",
                                table_reference.table.get_name(),
                                table_reference.identifier
                            )
                        };

                    match scan {
                        Scan::BTreeTable { index, .. } => {
                            if let Some(index) = index {
                                if table_reference.utilizes_covering_index() {
                                    format!("SCAN {table_name} USING COVERING INDEX {}", index.name)
                                } else {
                                    format!("SCAN {table_name} USING INDEX {}", index.name)
                                }
                            } else {
                                format!("SCAN {table_name}")
                            }
                        }
                        Scan::VirtualTable { .. }
                        | Scan::Subquery { .. }
                        | Scan::RecursiveCteInput => {
                            format!("SCAN {table_name}")
                        }
                    }
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. }
                    | Search::Seek { index: None, .. }
                    | Search::InSeek { index: None, .. } => {
                        format!(
                            "SEARCH {} USING INTEGER PRIMARY KEY (rowid=?){left_join_suffix}",
                            table_reference.identifier
                        )
                    }
                    Search::Seek {
                        index: Some(index),
                        seek_def,
                    } => {
                        let constraints =
                            super::display::seek_constraint_annotation(index, seek_def);
                        format!(
                            "SEARCH {} USING INDEX {}{constraints}{left_join_suffix}",
                            table_reference.identifier, index.name
                        )
                    }
                    Search::InSeek {
                        index: Some(index), ..
                    } => {
                        let constraint = if let Some(col) = index.columns.first() {
                            format!(" ({}=?)", col.name)
                        } else {
                            String::new()
                        };
                        format!(
                            "SEARCH {} USING INDEX {}{constraint}{left_join_suffix}",
                            table_reference.identifier, index.name
                        )
                    }
                },
                Operation::IndexMethodQuery(query) => {
                    let index_method = query.index.index_method.as_ref().unwrap();
                    format!(
                        "QUERY INDEX METHOD {}",
                        index_method.definition().method_name
                    )
                }
                Operation::HashJoin(_) => {
                    let table_name =
                        if table_reference.table.get_name() == table_reference.identifier {
                            table_reference.identifier.clone()
                        } else {
                            format!(
                                "{} AS {}",
                                table_reference.table.get_name(),
                                table_reference.identifier
                            )
                        };
                    format!("HASH JOIN {table_name}")
                }
                Operation::MultiIndexScan(multi_idx) => {
                    let index_names: Vec<&str> = multi_idx
                        .branches
                        .iter()
                        .map(|b| {
                            b.index
                                .as_ref()
                                .map(|i| i.name.as_str())
                                .unwrap_or("PRIMARY KEY")
                        })
                        .collect();
                    format!(
                        "MULTI-INDEX {} {} ({})",
                        match multi_idx.set_op {
                            SetOperation::Union => "OR",
                            SetOperation::Intersection { .. } => "AND",
                        },
                        table_reference.identifier,
                        index_names.join(", ")
                    )
                }
            }
        );

        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table_reference.table {
            let execution_mode = {
                let from_clause_subquery = from_clause_subquery.as_ref();
                choose_from_clause_subquery_execution_mode(
                    &table_reference.op,
                    from_clause_subquery,
                )
            };
            let from_clause_subquery = Arc::make_mut(from_clause_subquery);
            // Check if this is a CTE that's already materialized
            if let Some(cte_id) = from_clause_subquery.cte_id() {
                if let Some(cte_info) = program.get_materialized_cte(cte_id).cloned() {
                    if from_clause_subquery.materialized_cursor_id.is_some() {
                        tracing::trace!(
                            cte_id = %cte_id,
                            identifier = %table_reference.identifier,
                            "reusing pre-materialized CTE on original reference"
                        );
                        program.pop_current_parent_explain();
                        continue;
                    }
                    // === SUBSEQUENT CTE REFERENCE: Use OpenDup ===
                    // Create a dup cursor pointing to the same ephemeral table
                    let dup_cursor_id =
                        program.alloc_cursor_id(CursorType::BTreeTable(cte_info.table.clone()));
                    program.emit_insn(Insn::OpenDup {
                        new_cursor_id: dup_cursor_id,
                        original_cursor_id: cte_info.cursor_id,
                    });
                    tracing::trace!(
                        cte_id = %cte_id,
                        identifier = %table_reference.identifier,
                        original_cursor_id = cte_info.cursor_id,
                        dup_cursor_id,
                        "opening duplicate cursor for materialized CTE"
                    );

                    // Update the plan's query destination to EphemeralTable so that
                    // main_loop knows to use Rewind/Next instead of coroutine Yield
                    if let Some(dest) = from_clause_subquery.plan.select_query_destination_mut() {
                        *dest = QueryDestination::EphemeralTable {
                            cursor_id: dup_cursor_id,
                            table: cte_info.table.clone(),
                            rowid_mode: super::plan::EphemeralRowidMode::Auto,
                        };
                    }

                    // Each CTE reference needs its OWN registers to read column values into.
                    // We cannot share the original's result_columns_start_reg because multiple
                    // iterators of the same CTE (e.g., outer query and subquery) would
                    // overwrite each other's values when reading columns from their cursors.
                    let result_columns_start = program.alloc_registers(cte_info.num_columns);
                    from_clause_subquery.materialized_cursor_id = Some(dup_cursor_id);
                    from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
                    program
                        .set_subquery_result_reg(table_reference.internal_id, result_columns_start);
                    program.pop_current_parent_explain();
                    continue; // Skip normal emission
                }
            }

            let result_columns_start = match execution_mode {
                FromClauseSubqueryExecutionMode::Coroutine => {
                    emit_from_clause_subquery(program, from_clause_subquery.plan.as_mut(), t_ctx)?
                }
                FromClauseSubqueryExecutionMode::MaterializedTable => {
                    let (result_columns_start, cte_cursor_id, cte_table) =
                        emit_materialized_subquery_table(
                            program,
                            from_clause_subquery.plan.as_mut(),
                            t_ctx,
                            &from_clause_subquery.columns,
                        )?;
                    from_clause_subquery.materialized_cursor_id = Some(cte_cursor_id);
                    if let Some(cte_id) = from_clause_subquery.cte_id() {
                        program.register_materialized_cte(
                            cte_id,
                            MaterializedCteInfo {
                                cursor_id: cte_cursor_id,
                                table: cte_table,
                                num_columns: from_clause_subquery.columns.len(),
                            },
                        );
                    }
                    result_columns_start
                }
                FromClauseSubqueryExecutionMode::DirectMaterializedIndex(direct_index) => {
                    emit_indexed_materialized_subquery(
                        program,
                        from_clause_subquery.plan.as_mut(),
                        t_ctx,
                        table_reference.internal_id,
                        direct_index.index,
                        direct_index.affinity_str,
                        from_clause_subquery.columns.len(),
                    )?
                }
            };

            from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
            program.set_subquery_result_reg(table_reference.internal_id, result_columns_start);
        }

        program.pop_current_parent_explain();
    }
    Ok(())
}

/// Emit a FROM clause subquery and return the start register of the result columns.
/// This is done by emitting a coroutine that stores the result columns in sequential registers.
/// Each FROM clause subquery has its own Plan (either SelectPlan or CompoundSelect) which is wrapped in a coroutine.
///
/// The resulting bytecode from a subquery is mostly exactly the same as a regular query, except:
/// - it ends in an EndCoroutine instead of a Halt.
/// - instead of emitting ResultRows, the coroutine yields to the main query loop.
/// - the first register of the result columns is returned to the parent query,
///   so that planned expression emission can read the subquery outputs,
///   as if it were reading from a regular table.
///
/// Since a subquery has its own Plan, it can contain nested subqueries,
/// which can contain even more nested subqueries, etc.
pub fn emit_from_clause_subquery(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
) -> Result<usize> {
    let yield_reg = program.alloc_register();
    let coroutine_implementation_start_offset = program.allocate_label();

    // Set up the coroutine yield destination for the plan
    match plan.select_query_destination_mut() {
        Some(QueryDestination::CoroutineYield {
            yield_reg: y,
            coroutine_implementation_start,
        }) => {
            // The parent query will use this register to jump to/from the subquery.
            *y = yield_reg;
            // The parent query will use this register to reinitialize the coroutine when it needs to run multiple times.
            *coroutine_implementation_start = coroutine_implementation_start_offset;
        }
        _ => unreachable!("emit_from_clause_subquery called on non-subquery"),
    }

    let subquery_body_end_label = program.allocate_label();

    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: subquery_body_end_label,
        start_offset: coroutine_implementation_start_offset,
    });
    program.preassign_label_to_next_insn(coroutine_implementation_start_offset);

    // Coroutine bodies may be re-invoked from an outer loop (e.g. as the inner
    // side of a LEFT JOIN). Emit under `nested()` so that HashClose for any
    // hash join inside the body is deferred to statement teardown; otherwise
    // the second invocation would find the hash table already removed and
    // produce no matches. The hash build itself is guarded by Once and
    // therefore correctly persists across re-invocations.
    let result_column_start_reg = program.nested(|program| -> Result<usize> {
        Ok(match plan {
            Plan::Select(select_plan) => {
                let mut metadata = Box::new(TranslateCtx {
                    labels_main_loop: (0..select_plan.joined_tables().len())
                        .map(|_| LoopLabels::new(program))
                        .collect(),
                    label_main_loop_end: None,
                    meta_group_by: None,
                    meta_left_joins: (0..select_plan.joined_tables().len())
                        .map(|_| None)
                        .collect(),
                    meta_semi_anti_joins: (0..select_plan.joined_tables().len())
                        .map(|_| None)
                        .collect(),
                    meta_sort: None,
                    reg_agg_start: None,
                    reg_nonagg_emit_once_flag: None,
                    reg_result_cols_start: None,
                    limit_ctx: None,
                    reg_offset: None,
                    reg_limit_offset_sum: None,
                    resolver: t_ctx.resolver.fork(),
                    non_aggregate_expressions: Vec::new(),
                    agg_leaf_columns: Vec::new(),
                    cdc_cursor_id: None,
                    meta_window: None,
                    meta_in_seeks: (0..select_plan.joined_tables().len())
                        .map(|_| None)
                        .collect(),
                    materialized_build_inputs: HashMap::default(),
                    hash_table_contexts: HashMap::default(),
                    source_row_dependencies: HashMap::default(),
                    unsafe_testing: t_ctx.unsafe_testing,
                });
                metadata.materialized_build_inputs =
                    emit_materialized_build_inputs(program, &metadata.resolver, select_plan)?;
                emit_query(program, select_plan, &mut metadata)?
            }
            Plan::CompoundSelect { .. } => {
                let resolver = t_ctx.resolver.fork();
                // emit_program_for_compound_select returns the result column start register
                // for coroutine mode, which is needed by the outer query.
                emit_program_for_compound_select(program, &resolver, plan)?
                    .expect("compound CTE in coroutine mode must have result register")
            }
            Plan::RecursiveCte(recursive_cte) => {
                super::recursive_cte::emit_recursive_cte(program, &t_ctx.resolver, recursive_cte)?
            }
            Plan::Delete(_) | Plan::Update(_) => {
                unreachable!("DELETE/UPDATE plans cannot be FROM clause subqueries")
            }
        })
    })?;

    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(subquery_body_end_label);
    Ok(result_column_start_reg)
}
/// Materialize a single-reference seekable FROM-subquery directly into an
/// ephemeral index.
///
/// This skips the intermediate EphemeralTable when we only need seek access and do
/// not need table-backed sharing via OpenDup. Result columns for this path are read
/// back from the index using `pos_in_table` mapping rather than raw index position.
fn emit_indexed_materialized_subquery(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
    internal_id: PlanSourceId,
    index: Arc<Index>,
    affinity_str: Option<Arc<String>>,
    num_columns: usize,
) -> Result<usize> {
    let cursor_id = program
        .alloc_cursor_index_if_not_exists(CursorKey::index(internal_id, index.clone()), &index)?;
    let result_columns_start_reg = program.alloc_registers(num_columns);

    if let Some(dest) = plan.select_query_destination_mut() {
        *dest = QueryDestination::EphemeralIndex {
            cursor_id,
            index,
            affinity_str,
            is_delete: false,
        };
    }

    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: false,
    });

    match plan {
        Plan::Select(select_plan) => {
            let mut metadata = Box::new(TranslateCtx {
                labels_main_loop: (0..select_plan.joined_tables().len())
                    .map(|_| LoopLabels::new(program))
                    .collect(),
                label_main_loop_end: None,
                meta_group_by: None,
                meta_left_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_semi_anti_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_sort: None,
                reg_agg_start: None,
                reg_nonagg_emit_once_flag: None,
                reg_result_cols_start: None,
                limit_ctx: None,
                reg_offset: None,
                reg_limit_offset_sum: None,
                resolver: t_ctx.resolver.fork(),
                non_aggregate_expressions: Vec::new(),
                agg_leaf_columns: Vec::new(),
                cdc_cursor_id: None,
                meta_window: None,
                meta_in_seeks: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                materialized_build_inputs: HashMap::default(),
                hash_table_contexts: HashMap::default(),
                source_row_dependencies: HashMap::default(),
                unsafe_testing: t_ctx.unsafe_testing,
            });
            metadata.materialized_build_inputs =
                emit_materialized_build_inputs(program, &metadata.resolver, select_plan)?;
            emit_query(program, select_plan, &mut metadata)?;
        }
        Plan::CompoundSelect { .. } => {
            let resolver = t_ctx.resolver.fork();
            emit_program_for_compound_select(program, &resolver, plan)?;
        }
        Plan::RecursiveCte(_) => {
            unreachable!("recursive CTEs require table-backed materialization for indexed access")
        }
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE/UPDATE plans cannot be FROM clause subqueries")
        }
    }

    Ok(result_columns_start_reg)
}

fn emit_materialized_subquery_table(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
    columns: &[Column],
) -> Result<(usize, CursorID, Arc<BTreeTable>)> {
    use super::plan::EphemeralRowidMode;

    // EphemeralTable (not EphemeralIndex) is required because it preserves
    // insertion order, which SQL semantics require for UNION ALL. It also
    // needs the subquery's column layout so later Column opcodes can read
    // materialized rows through the normal table-cursor path.
    let ephemeral_table = Arc::new(BTreeTable::new(
        0,
        String::new(),
        crate::alloc::vec![],
        columns.try_to_vec()?,
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ));

    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(ephemeral_table.clone()));

    // Allocate registers for reading result columns
    let result_columns_start_reg = program.alloc_registers(columns.len());

    // Open the ephemeral table
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: true,
    });

    // Set the query destination to write to the ephemeral table
    if let Some(dest) = plan.select_query_destination_mut() {
        *dest = QueryDestination::EphemeralTable {
            cursor_id,
            table: ephemeral_table.clone(),
            rowid_mode: EphemeralRowidMode::Auto,
        };
    }

    // Emit the subquery - it will insert rows into the ephemeral table
    match plan {
        Plan::Select(select_plan) => {
            let mut metadata = Box::new(TranslateCtx {
                labels_main_loop: (0..select_plan.joined_tables().len())
                    .map(|_| LoopLabels::new(program))
                    .collect(),
                label_main_loop_end: None,
                meta_group_by: None,
                meta_left_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_semi_anti_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_sort: None,
                reg_agg_start: None,
                reg_nonagg_emit_once_flag: None,
                reg_result_cols_start: None,
                limit_ctx: None,
                reg_offset: None,
                reg_limit_offset_sum: None,
                resolver: t_ctx.resolver.fork(),
                non_aggregate_expressions: Vec::new(),
                agg_leaf_columns: Vec::new(),
                cdc_cursor_id: None,
                meta_window: None,
                meta_in_seeks: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                materialized_build_inputs: HashMap::default(),
                hash_table_contexts: HashMap::default(),
                source_row_dependencies: HashMap::default(),
                unsafe_testing: t_ctx.unsafe_testing,
            });
            metadata.materialized_build_inputs =
                emit_materialized_build_inputs(program, &metadata.resolver, select_plan)?;
            emit_query(program, select_plan, &mut metadata)?;
        }
        Plan::CompoundSelect { .. } => {
            let resolver = t_ctx.resolver.fork();
            emit_program_for_compound_select(program, &resolver, plan)?;
        }
        Plan::RecursiveCte(recursive_cte) => {
            super::recursive_cte::emit_recursive_cte(program, &t_ctx.resolver, recursive_cte)?;
        }
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE/UPDATE plans cannot be FROM clause subqueries")
        }
    }

    Ok((result_columns_start_reg, cursor_id, ephemeral_table))
}

/// Translate a subquery that is not part of the FROM clause.
/// If a subquery is uncorrelated (i.e. does not reference columns from the outer query),
/// it will be executed only once.
///
/// If it is correlated (i.e. references columns from the outer query),
/// it will be executed for each row of the outer query.
///
/// The result of the subquery is stored in:
///
/// - a single register for EXISTS subqueries,
/// - a range of registers for RowValue subqueries,
/// - an ephemeral index for IN subqueries.
pub fn emit_non_from_clause_subquery(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: Plan,
    query_type: &PlanSubqueryType,
    is_correlated: bool,
    preserve_outer_expr_cache: bool,
) -> Result<()> {
    program.nested(|program| {
        let subquery_id = program.next_subquery_eqp_id();
        let correlated_prefix = if is_correlated { "CORRELATED " } else { "" };
        match query_type {
            PlanSubqueryType::Exists { .. } => {
                // EXISTS subqueries don't get a separate EQP annotation in SQLite;
                // instead the SEARCH/SCAN line gets an "EXISTS" suffix handled elsewhere.
            }
            PlanSubqueryType::In { .. } => {
                emit_explain!(
                    program,
                    true,
                    format!("{correlated_prefix}LIST SUBQUERY {subquery_id}")
                );
            }
            PlanSubqueryType::RowValue { .. } => {
                emit_explain!(
                    program,
                    true,
                    format!("{correlated_prefix}SCALAR SUBQUERY {subquery_id}")
                );
            }
        }

        let label_skip_after_first_run = if !is_correlated {
            let label = program.allocate_label();
            program.emit_insn(Insn::Once {
                target_pc_when_reentered: label,
            });
            Some(label)
        } else {
            None
        };

        // Helper closure to emit a select plan (simple or compound). The
        // closure captures `resolver`, `plan`, and `preserve_outer_expr_cache`
        // from the enclosing scope; only `program` is passed explicitly so
        // that the outer scope can keep emitting instructions in between.
        // Called at most once, hence `FnOnce`.
        let emit_plan = move |program: &mut ProgramBuilder| -> Result<()> {
            match plan {
                Plan::Select(select_plan) => {
                    if preserve_outer_expr_cache {
                        emit_program_for_select_with_resolver(
                            program,
                            resolver.fork_with_expr_cache(),
                            *select_plan,
                        )
                    } else {
                        emit_program_for_select(program, resolver, *select_plan)
                    }
                }
                mut compound @ Plan::CompoundSelect { .. } => {
                    emit_program_for_compound_select(program, resolver, &mut compound)?;
                    Ok(())
                }
                Plan::RecursiveCte(mut recursive_cte) => {
                    super::recursive_cte::emit_recursive_cte(
                        program,
                        resolver,
                        &mut recursive_cte,
                    )?;
                    Ok(())
                }
                Plan::Delete(_) | Plan::Update(_) => {
                    unreachable!("DML plans cannot be subqueries")
                }
            }
        };

        match query_type {
            PlanSubqueryType::Exists { result_reg, .. } => {
                let subroutine_reg = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: subroutine_reg,
                    dest_end: None,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
                emit_plan(program)?;
                program.emit_insn(Insn::Return {
                    return_reg: subroutine_reg,
                    can_fallthrough: true,
                });
            }
            PlanSubqueryType::In { cursor_id, .. } => {
                program.emit_insn(Insn::OpenEphemeral {
                    cursor_id: *cursor_id,
                    is_table: false,
                });
                emit_plan(program)?;
            }
            PlanSubqueryType::RowValue {
                result_reg_start,
                num_regs,
            } => {
                let subroutine_reg = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: subroutine_reg,
                    dest_end: None,
                });
                for result_reg in *result_reg_start..*result_reg_start + *num_regs {
                    program.emit_insn(Insn::Null {
                        dest: result_reg,
                        dest_end: None,
                    });
                }
                emit_plan(program)?;
                program.emit_insn(Insn::Return {
                    return_reg: subroutine_reg,
                    can_fallthrough: true,
                });
            }
        }
        // Pop the parent explain for LIST/SCALAR SUBQUERY annotations.
        if !matches!(query_type, PlanSubqueryType::Exists { .. }) {
            program.pop_current_parent_explain();
        }
        if let Some(label) = label_skip_after_first_run {
            program.preassign_label_to_next_insn(label);
        }
        Ok(())
    })
}

pub fn emit_non_from_clause_subqueries_for_phase(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    subqueries: &mut [NonFromClauseSubquery],
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
    phase: SubqueryEvalPhase,
    mut should_emit: impl FnMut(&NonFromClauseSubquery) -> bool,
) -> Result<()> {
    for subquery in subqueries.iter_mut() {
        if subquery.has_been_evaluated() || !should_emit(subquery) {
            continue;
        }

        let evaluated_at = match phase {
            SubqueryEvalPhase::BeforeLoop | SubqueryEvalPhase::Loop(_) => {
                if !matches!(subquery.eval_phase, SubqueryEvalPhase::BeforeLoop) {
                    continue;
                }
                let expected_eval_at = match phase {
                    SubqueryEvalPhase::BeforeLoop => EvalAt::BeforeLoop,
                    SubqueryEvalPhase::Loop(loop_idx) => EvalAt::Loop(loop_idx),
                    _ => unreachable!(),
                };
                let evaluated_at = subquery.get_eval_at(join_order, table_references)?;
                if evaluated_at != expected_eval_at {
                    continue;
                }
                evaluated_at
            }
            _ => {
                if subquery.eval_phase != phase {
                    continue;
                }
                subquery.get_eval_at(join_order, table_references)?
            }
        };

        emit_outer_output_values(program, resolver, table_references, &subquery.outer_outputs)?;
        let subquery_plan = subquery.consume_plan(evaluated_at);
        emit_non_from_clause_subquery(
            program,
            resolver,
            *subquery_plan,
            &subquery.query_type,
            subquery.correlated,
            !matches!(
                phase,
                SubqueryEvalPhase::BeforeLoop | SubqueryEvalPhase::Loop(_)
            ),
        )?;
    }

    Ok(())
}

fn emit_outer_output_values(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_references: Option<&TableReferences>,
    outputs: &[PlanOuterOutputReference],
) -> Result<()> {
    if outputs.is_empty() {
        return Ok(());
    }
    let table_references = table_references.ok_or_else(|| {
        crate::LimboError::InternalError(
            "correlated output reference has no enclosing plan scope".to_string(),
        )
    })?;
    for output in outputs {
        let binding = resolver
            .plan_runtime_bindings()
            .output(output.output)
            .cloned()
            .ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "enclosing plan output {} has no runtime binding",
                    output.output
                ))
            })?;
        let RuntimeValueBinding::Register { register, .. } = binding.value else {
            return Err(crate::LimboError::InternalError(format!(
                "enclosing plan output {} is not stored in a register",
                output.output
            )));
        };
        let RuntimeOutputDefinition::Plan(definition) = binding.definition else {
            return Err(crate::LimboError::InternalError(format!(
                "enclosing plan output {} has no SELECT definition",
                output.output
            )));
        };
        translate_plan_expr_no_constant_opt(
            program,
            Some(table_references),
            &definition,
            register,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    }
    Ok(())
}

pub fn emit_non_from_clause_subqueries_for_eval_at(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    subqueries: &mut [NonFromClauseSubquery],
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
    eval_at: EvalAt,
    should_emit: impl FnMut(&NonFromClauseSubquery) -> bool,
) -> Result<()> {
    emit_non_from_clause_subqueries_for_phase(
        program,
        resolver,
        subqueries,
        join_order,
        table_references,
        match eval_at {
            EvalAt::BeforeLoop => SubqueryEvalPhase::BeforeLoop,
            EvalAt::Loop(loop_idx) => SubqueryEvalPhase::Loop(loop_idx),
        },
        should_emit,
    )
}

pub(crate) fn finalize_hir_select_subqueries(plan: &mut SelectPlan) {
    assign_select_subquery_eval_phases(plan);
    mark_shared_cte_materialization_requirements(
        &mut plan.table_references,
        &mut plan.non_from_clause_subqueries,
    );
}

fn assign_select_subquery_eval_phases(plan: &mut SelectPlan) {
    let has_grouped_output = plan
        .group_by
        .as_ref()
        .is_some_and(|group_by| !group_by.exprs.is_empty());
    let has_ungrouped_aggregate_output = !has_grouped_output && !plan.aggregates.is_empty();

    // Subqueries inside an aggregate's arguments or FILTER clause are evaluated
    // per input row by the aggregate step code in the main loop, even when the
    // aggregate itself belongs to HAVING or ORDER BY. Deferring them to the
    // grouped output subroutine would emit their materialization after their
    // first use, so they must keep their phase floor (issue #6807).
    let mut aggregate_subquery_ids: Vec<PlanSubqueryId> = Vec::new();
    for agg in &plan.aggregates {
        for expr in agg.args.iter().chain(agg.filter_expr.iter()) {
            walk_plan_expr(expr, &mut |expression| {
                if let PlanExpr::Subquery(subquery) = expression {
                    let query = match subquery {
                        PlanSubqueryExpr::Scalar { query, .. }
                        | PlanSubqueryExpr::Exists(query)
                        | PlanSubqueryExpr::In { query, .. } => *query,
                    };
                    aggregate_subquery_ids.push(query);
                }
                Ok(PlanWalkControl::Continue)
            })
            .expect("walking an expression with an infallible visitor cannot fail");
        }
    }

    for subquery in plan.non_from_clause_subqueries.iter_mut() {
        subquery.eval_phase = match subquery.origin {
            SubqueryOrigin::SelectHaving | SubqueryOrigin::SelectOrderBy
                if has_grouped_output
                    && !aggregate_subquery_ids.contains(&subquery.internal_id) =>
            {
                SubqueryEvalPhase::GroupedOutput
            }
            SubqueryOrigin::SelectHaving | SubqueryOrigin::SelectOrderBy
                if has_ungrouped_aggregate_output
                    && !subquery.outer_outputs.is_empty()
                    && !aggregate_subquery_ids.contains(&subquery.internal_id) =>
            {
                SubqueryEvalPhase::UngroupedAggregateOutput
            }
            _ => subquery.origin.phase_floor(),
        };
    }
}
