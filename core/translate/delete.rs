use crate::schema::{BTreeTable, Table};
use crate::sync::Arc;
use crate::translate::emitter::{emit_program, Resolver};
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{
    DeletePlan, DmlSafety, DmlSafetyReason, IterationDirection, JoinOrderMember, Operation, Plan,
    QueryDestination, ResultSetColumn, Scan, SelectPlan,
};
use crate::translate::trigger_exec::has_triggers_including_temp;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::Result;
use smallvec::SmallVec;
use turso_parser::ast::{RefAct, TriggerEvent};

use super::plan::WhereTerm;

#[allow(clippy::too_many_arguments)]
#[turso_macros::trace_stack]
pub fn translate_delete(
    document: super::semantic::hir::HirDocument,
    identities: &super::plan_expr::PlanIdentityMap,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let super::semantic::hir::HirRoot::Delete(statement) = &document.root else {
        return Err(crate::LimboError::InternalError(
            "DELETE translator received a non-DELETE HIR root".to_string(),
        ));
    };
    let source = document.source(statement.target).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "missing DELETE target source {}",
            statement.target
        ))
    })?;
    let database_id = source.database.map_or(crate::MAIN_DB_ID, |id| id.index());
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    let mut delete_plan = prepare_delete_plan(
        &document, statement, identities, program, resolver, connection,
    )?;

    optimize_plan(program, &mut delete_plan, resolver)?;
    if let Plan::Delete(delete_plan_inner) = &mut delete_plan {
        // Re-check after optimization: chosen access paths can make "delete while scanning"
        // unsafe, so we may need to collect rowids first.
        record_delete_optimizer_safety(delete_plan_inner);
        if delete_plan_inner.safety.requires_stable_write_set() {
            ensure_delete_uses_rowset(program, delete_plan_inner);
        }

        // Rewrite the Delete plan after optimization whenever a RowSet is used (trigger/subquery
        // safety or optimizer-induced safety), so the joined table is treated as a plain table
        // scan again.
        //
        // RowSets re-seek the base table cursor for every delete, so expressions that reference
        // columns during index maintenance must bind to the table cursor again (not the index we
        // originally used to find the rowids).
        //
        // e.g. DELETE using idx_x gathers rowids, but BEFORE DELETE trigger causes re-seek on
        // table, so expression indexes must read from that table cursor.
        if delete_plan_inner.rowset_plan.is_some() {
            if let Some(joined_table) = delete_plan_inner
                .table_references
                .joined_tables_mut()
                .first_mut()
            {
                if matches!(joined_table.table, Table::BTree(_)) {
                    joined_table.op = Operation::Scan(Scan::BTreeTable {
                        iter_dir: IterationDirection::Forwards,
                        index: None,
                    });
                }
            }
        }
    }
    let Plan::Delete(ref delete) = delete_plan else {
        panic!("delete_plan is not a DeletePlan");
    };
    super::stmt_journal::set_delete_stmt_journal_flags(
        program,
        delete,
        resolver,
        connection,
        database_id,
    )?;
    let opts = ProgramBuilderOpts::new(1, estimate_num_instructions(delete), 0);
    program.extend(&opts);
    emit_program(connection, resolver, program, delete_plan, |_| {})?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[turso_macros::trace_stack]
pub fn prepare_delete_plan(
    document: &super::semantic::hir::HirDocument,
    statement: &super::semantic::hir::Delete,
    identities: &super::plan_expr::PlanIdentityMap,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    _connection: &Arc<crate::Connection>,
) -> Result<Plan> {
    let mut hir_ctx = super::planner::HirPlanContext::new(document, identities, program);
    let target = super::planner::prepare_hir_source(&mut hir_ctx, statement.target, None)?;
    let database_id = target.database_id;
    let table = target.table.clone();

    let btree_table_for_triggers = table.btree();
    let indexes = target
        .index_expressions
        .iter()
        .map(|index| index.index.clone())
        .collect();
    let mut table_references = hir_ctx.new_table_references(vec![target], vec![])?;

    let mut where_predicates = vec![];
    if let Some(predicate) = &statement.predicate {
        super::update::split_where_expr(
            super::plan_expr::lower_hir_expr(predicate, identities)
                .map_err(|error| crate::LimboError::InternalError(error.to_string()))?,
            &mut where_predicates,
        );
    }
    let result_columns = statement
        .returning
        .as_ref()
        .map(|returning| {
            returning
                .outputs
                .iter()
                .map(|output| super::update::lower_output(output, identities))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    let order_by = statement
        .order_by
        .iter()
        .map(|term| {
            Ok(super::plan_expr::PlanOrderTerm {
                expr: super::plan_expr::lower_hir_expr(&term.expr, identities)
                    .map_err(|error| crate::LimboError::InternalError(error.to_string()))?,
                order: term.order,
                nulls: term.nulls,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let (resolved_limit, resolved_offset) = match &statement.limit {
        Some(limit) => (
            Some(
                super::plan_expr::lower_hir_expr(&limit.limit, identities)
                    .map_err(|error| crate::LimboError::InternalError(error.to_string()))?,
            ),
            limit
                .offset
                .as_ref()
                .map(|expr| {
                    super::plan_expr::lower_hir_expr(expr, identities)
                        .map_err(|error| crate::LimboError::InternalError(error.to_string()))
                })
                .transpose()?,
        ),
        None => (None, None),
    };

    for term in &where_predicates {
        table_references.register_plan_expr_usage(&term.expr)?;
    }
    for output in &result_columns {
        table_references.register_plan_expr_usage(&output.expr)?;
    }
    for term in &order_by {
        table_references.register_plan_expr_usage(&term.expr)?;
    }
    if let Some(limit) = &resolved_limit {
        table_references.register_plan_expr_usage(limit)?;
    }
    if let Some(offset) = &resolved_offset {
        table_references.register_plan_expr_usage(offset)?;
    }

    let mut non_from_clause_subqueries = vec![];
    let where_expressions = where_predicates
        .iter()
        .map(|term| &term.expr)
        .chain(order_by.iter().map(|term| &term.expr))
        .chain(resolved_limit.iter())
        .chain(resolved_offset.iter())
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        &mut hir_ctx,
        &mut table_references,
        &where_expressions,
        super::plan::SubqueryOrigin::DmlWhere,
        &mut non_from_clause_subqueries,
    )?;
    let returning_expressions = result_columns
        .iter()
        .map(|output| &output.expr)
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        &mut hir_ctx,
        &mut table_references,
        &returning_expressions,
        super::plan::SubqueryOrigin::DmlReturning,
        &mut non_from_clause_subqueries,
    )?;
    drop(hir_ctx);

    // Check if there are DELETE triggers. If so, we need to materialize the write set into a RowSet first.
    // This is done in SQLite for all DELETE triggers on the affected table even if the trigger would not have an impact
    // on the target table -- presumably due to lack of static analysis capabilities to determine whether it's safe
    // to skip the rowset materialization.
    let has_delete_triggers = btree_table_for_triggers
        .as_ref()
        .map(|bt| {
            has_triggers_including_temp(resolver, database_id, TriggerEvent::Delete, None, bt)
        })
        .unwrap_or(false);

    let has_fk_cascade_triggers = match btree_table_for_triggers.as_ref() {
        Some(bt) => table_has_fk_cascade_triggers(resolver, database_id, &bt.name)?,
        None => false,
    };

    let mut safety = DmlSafety::default();
    if has_delete_triggers {
        safety.require(DmlSafetyReason::Trigger);
    }
    if has_fk_cascade_triggers {
        safety.require(DmlSafetyReason::FkCascade);
    }
    if where_clause_has_subquery(&where_predicates) {
        safety.require(DmlSafetyReason::SubqueryInWhere);
    }

    let mut delete_plan = DeletePlan {
        table_references,
        result_columns,
        where_clause: where_predicates,
        order_by,
        limit: resolved_limit,
        offset: resolved_offset,
        contains_constant_false_condition: false,
        indexes,
        rowset_plan: None,
        rowset_reg: None,
        non_from_clause_subqueries,
        safety,
    };

    if delete_plan.safety.requires_stable_write_set() {
        ensure_delete_uses_rowset(program, &mut delete_plan);
    }

    Ok(Plan::Delete(Box::new(delete_plan)))
}

/// Returns true if any FK referencing `table_name` (transitively, following CASCADE chains)
/// has triggers on the child table side, which could write back to `table_name` and
/// invalidate a live DELETE scan iterator.
fn table_has_fk_cascade_triggers(
    resolver: &crate::translate::emitter::Resolver,
    database_id: usize,
    table_name: &str,
) -> Result<bool> {
    let check_temp = database_id != crate::TEMP_DB_ID && resolver.has_temp_database();

    let mut visited: SmallVec<[Arc<BTreeTable>; 2]> = SmallVec::new();
    let mut worklist: SmallVec<[Arc<BTreeTable>; 2]> = SmallVec::new();

    let start = resolver
        .with_schema(database_id, |s| s.get_btree_table(table_name))
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "btree table {table_name} missing from schema after delete validation"
            ))
        })?;
    worklist.push(start);

    while let Some(current) = worklist.pop() {
        if visited.iter().any(|t| Arc::ptr_eq(t, &current)) {
            continue;
        }
        visited.push(current.clone());

        let referencing_fks =
            resolver.with_schema(database_id, |s| s.resolved_fks_referencing(&current.name))?;

        for fk_ref in referencing_fks {
            if matches!(fk_ref.fk.on_delete, RefAct::NoAction | RefAct::Restrict) {
                continue;
            }
            let child_name = fk_ref.child_table.name.as_str();
            let has_triggers = resolver.with_schema(database_id, |s| {
                s.get_triggers_for_table(child_name).next().is_some()
            });
            if has_triggers {
                return Ok(true);
            }
            if check_temp {
                let has_temp = resolver.with_schema(crate::TEMP_DB_ID, |s| {
                    s.get_triggers_for_table(child_name).next().is_some()
                });
                if has_temp {
                    return Ok(true);
                }
            }
            worklist.push(fk_ref.child_table);
        }
    }
    Ok(false)
}

/// Check if any WHERE predicate depends on a semantic subquery plan.
fn where_clause_has_subquery(predicates: &[WhereTerm]) -> bool {
    predicates.iter().any(|predicate| {
        super::plan_expr::plan_expr_dependencies(&predicate.expr)
            .is_ok_and(|dependencies| !dependencies.subqueries.is_empty())
    })
}

fn estimate_num_instructions(plan: &DeletePlan) -> usize {
    let base = 20;

    base + plan.table_references.joined_tables().len() * 10
}

/// Add post-optimizer reasons that force "collect rowids first, then delete".
fn record_delete_optimizer_safety(plan: &mut DeletePlan) {
    if plan
        .table_references
        .joined_tables()
        .first()
        .is_some_and(|table| matches!(table.op, Operation::MultiIndexScan(_)))
    {
        plan.safety.require(DmlSafetyReason::MultiIndexScan);
    }
    if let Some(Operation::IndexMethodQuery(query)) =
        plan.table_references.joined_tables().first().map(|t| &t.op)
    {
        let attachment = query
            .index
            .index_method
            .as_ref()
            .expect("IndexMethodQuery always has an index_method attachment");
        if !attachment.definition().results_materialized {
            plan.safety
                .require(DmlSafetyReason::IndexMethodNotMaterialized);
        }
    }
}

/// Convert a DELETE plan into a RowSet-driven delete:
/// 1. execute a SELECT-like rowid producer into RowSet
/// 2. iterate RowSet to perform actual deletes
fn ensure_delete_uses_rowset(program: &mut ProgramBuilder, plan: &mut DeletePlan) {
    if plan.rowset_plan.is_some() {
        return;
    }

    let rowid_internal_id = plan
        .table_references
        .joined_tables()
        .first()
        .expect("DELETE should have one target table")
        .internal_id;
    let rowset_reg = plan.rowset_reg.unwrap_or_else(|| {
        let reg = program.alloc_register();
        plan.rowset_reg = Some(reg);
        reg
    });
    let mut rowset_subqueries = Vec::new();
    let mut delete_subqueries = Vec::new();
    for subquery in std::mem::take(&mut plan.non_from_clause_subqueries) {
        if matches!(subquery.origin, super::plan::SubqueryOrigin::DmlWhere) {
            rowset_subqueries.push(subquery);
        } else {
            delete_subqueries.push(subquery);
        }
    }
    plan.non_from_clause_subqueries = delete_subqueries;

    let rowset_plan = SelectPlan {
        table_references: plan.table_references.clone(),
        result_columns: vec![ResultSetColumn {
            id: program.next_plan_output_id(),
            name: "rowid".to_string(),
            name_kind: super::semantic::hir::OutputNameKind::Inferred,
            origin: Some(super::plan::ResultColumnOrigin::RowId {
                source: rowid_internal_id,
            }),
            type_fact: super::semantic::hir::TypeFact::known(crate::schema::Type::Integer),
            affinity: super::plan_expr::PlanExprAffinity::with_affinity(
                crate::vdbe::affinity::Affinity::Integer,
            ),
            collation: None,
            array_dimensions: 0,
            expr: super::plan_expr::PlanExpr::rowid(rowid_internal_id),
            contains_aggregates: false,
        }],
        where_clause: std::mem::take(&mut plan.where_clause),
        group_by: None,
        order_by: vec![],
        aggregates: vec![],
        limit: plan.limit.take(),
        query_destination: QueryDestination::RowSet { rowset_reg },
        join_order: plan
            .table_references
            .joined_tables()
            .iter()
            .enumerate()
            .map(|(i, t)| JoinOrderMember {
                table_id: t.internal_id,
                original_idx: i,
                is_outer: false,
            })
            .collect(),
        offset: plan.offset.take(),
        contains_constant_false_condition: false,
        distinctness: super::plan::Distinctness::NonDistinct,
        values: vec![],
        window: None,
        non_from_clause_subqueries: rowset_subqueries,
        input_cardinality_hint: None,
        estimated_output_rows: None,
        simple_aggregate: None,
        phantom_params: vec![],
    };
    plan.rowset_plan = Some(rowset_plan);
}
