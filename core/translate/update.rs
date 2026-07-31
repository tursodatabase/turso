use crate::function::{Func, ScalarFunc};
use crate::schema::{EXPR_INDEX_SENTINEL, ROWID_SENTINEL};
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::plan::ColumnMask;
use crate::{
    bail_parse_error,
    schema::Table,
    util::normalize_ident,
    vdbe::builder::{ProgramBuilder, ProgramBuilderOpts},
    CaptureDataChangesExt,
};
use turso_parser::ast;

use super::emitter::emit_program;
use super::optimizer::optimize_plan;
use super::plan::{
    ColumnUsedMask, DmlSafety, JoinedTable, Plan, TableReferences, UpdatePlan, UpdateSetClause,
};
/*
* Update is simple. By default we scan the table, and for each row, we check the WHERE
* clause. If it evaluates to true, we build the new record with the updated value and insert.
*
* EXAMPLE:
*
sqlite> explain update t set a = 100 where b = 5;
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------------
0     Init           0     16    0                    0   Start at 16
1     Null           0     1     2                    0   r[1..2]=NULL
2     Noop           1     0     1                    0
3     OpenWrite      0     2     0     3              0   root=2 iDb=0; t
4     Rewind         0     15    0                    0
5       Column         0     1     6                    0   r[6]= cursor 0 column 1
6       Ne             7     14    6     BINARY-8       81  if r[6]!=r[7] goto 14
7       Rowid          0     2     0                    0   r[2]= rowid of 0
8       IsNull         2     15    0                    0   if r[2]==NULL goto 15
9       Integer        100   3     0                    0   r[3]=100
10      Column         0     1     4                    0   r[4]= cursor 0 column 1
11      Column         0     2     5                    0   r[5]= cursor 0 column 2
12      MakeRecord     3     3     1                    0   r[1]=mkrec(r[3..5])
13      Insert         0     1     2     t              7   intkey=r[2] data=r[1]
14    Next           0     5     0                    1
15    Halt           0     0     0                    0
16    Transaction    0     1     1     0              1   usesStmtJournal=0
17    Integer        5     7     0                    0   r[7]=5
18    Goto           0     1     0                    0
*/
pub fn translate_update(
    document: super::semantic::hir::HirDocument,
    identities: &super::plan_expr::PlanIdentityMap,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> crate::Result<()> {
    let super::semantic::hir::HirRoot::Update(statement) = &document.root else {
        return Err(crate::LimboError::InternalError(
            "UPDATE translator received a non-UPDATE HIR root".to_string(),
        ));
    };
    let plan = prepare_and_optimize_update_plan(
        &document, statement, identities, program, resolver, None,
    )?;
    let Plan::Update(ref update_plan) = plan else {
        unreachable!("prepare_and_optimize_update_plan must return Plan::Update");
    };
    super::stmt_journal::set_update_stmt_journal_flags(program, update_plan, resolver, connection)?;

    let opts = ProgramBuilderOpts::new(1, 20, 4);
    program.extend(&opts);
    emit_program(connection, resolver, program, plan, |_| {})?;
    Ok(())
}

/// Split one resolved assignment into the per-column expressions consumed by
/// the write emitter. Semantic analysis already represents a multi-column
/// subquery as a row of explicit output references, so planning only has to
/// validate and unpack that row.
fn split_update_set_values(
    expr: super::plan_expr::PlanExpr,
    target_count: usize,
) -> crate::Result<Vec<super::plan_expr::PlanExpr>> {
    use super::plan_expr::PlanExpr;

    match expr {
        PlanExpr::Row(values) => {
            if values.len() != target_count {
                bail_parse_error!("{} columns assigned {} values", target_count, values.len());
            }
            Ok(values)
        }
        expr => {
            if target_count != 1 {
                bail_parse_error!("{} columns assigned 1 values", target_count);
            }
            Ok(vec![expr])
        }
    }
}

fn lower_expr(
    expr: &super::semantic::hir::Expr,
    identities: &super::plan_expr::PlanIdentityMap,
) -> crate::Result<super::plan_expr::PlanExpr> {
    super::plan_expr::lower_hir_expr(expr, identities)
        .map_err(|error| crate::LimboError::InternalError(error.to_string()))
}

pub(crate) fn lower_output(
    output: &super::semantic::hir::Output,
    identities: &super::plan_expr::PlanIdentityMap,
) -> crate::Result<super::plan::ResultSetColumn> {
    use super::plan::ResultColumnOrigin;
    use super::plan_expr::PlanExpr;

    let id = identities.output(output.id).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "missing plan identity for output {:?}",
            output.id
        ))
    })?;
    let expr = lower_expr(&output.expr, identities)?;
    let origin = match &expr {
        PlanExpr::Column(column) => Some(ResultColumnOrigin::Column {
            source: column.source,
            column: column.column,
        }),
        PlanExpr::RowId(source) => Some(ResultColumnOrigin::RowId { source: *source }),
        _ => None,
    };
    let affinity = if output.has_affinity {
        super::plan_expr::PlanExprAffinity::with_affinity(output.affinity)
    } else {
        super::plan_expr::PlanExprAffinity::no_affinity()
    };
    Ok(super::plan::ResultSetColumn {
        id,
        name: output.name.clone(),
        name_kind: output.name_kind,
        origin,
        type_fact: output.type_fact.clone(),
        affinity,
        collation: output.collation.clone(),
        array_dimensions: super::plan_expr::type_fact_array_dimensions(&output.type_fact),
        expr,
        contains_aggregates: false,
    })
}

pub(crate) fn split_where_expr(
    expr: super::plan_expr::PlanExpr,
    terms: &mut Vec<super::plan::WhereTerm>,
) {
    match expr {
        super::plan_expr::PlanExpr::Binary {
            lhs,
            operator: ast::Operator::And,
            rhs,
            ..
        } => {
            split_where_expr(*lhs, terms);
            split_where_expr(*rhs, terms);
        }
        expr => terms.push(super::plan::WhereTerm {
            expr,
            from_outer_join: None,
            consumed: false,
        }),
    }
}

pub fn translate_update_for_schema_change(
    document: super::semantic::hir::HirDocument,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    ddl_query: &str,
    after: impl FnOnce(&mut ProgramBuilder),
) -> crate::Result<()> {
    let identities = program.allocate_plan_identities(&document);
    let super::semantic::hir::HirRoot::Update(statement) = &document.root else {
        return Err(crate::LimboError::InternalError(
            "schema-change UPDATE translator received a non-UPDATE HIR root".to_string(),
        ));
    };
    let plan = prepare_and_optimize_update_plan(
        &document,
        statement,
        &identities,
        program,
        resolver,
        Some(ddl_query),
    )?;
    let opts = ProgramBuilderOpts::new(1, 20, 4);
    program.extend(&opts);
    emit_program(connection, resolver, program, plan, after)?;
    Ok(())
}

fn prepare_and_optimize_update_plan(
    document: &super::semantic::hir::HirDocument,
    statement: &super::semantic::hir::Update,
    identities: &super::plan_expr::PlanIdentityMap,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    ddl_query_for_cdc_update: Option<&str>,
) -> crate::Result<Plan> {
    let mut update_plan = prepare_update_plan(document, statement, identities, program, resolver)?;

    if let Some(ddl_query_for_cdc_update) = ddl_query_for_cdc_update {
        if program.capture_data_changes_info().has_updates() {
            update_plan.cdc_update_alter_statement = Some(ddl_query_for_cdc_update.to_string());
        }
    }
    let mut plan = Plan::Update(Box::new(update_plan));
    optimize_plan(program, &mut plan, resolver)?;
    Ok(plan)
}

fn prepare_update_plan(
    document: &super::semantic::hir::HirDocument,
    statement: &super::semantic::hir::Update,
    identities: &super::plan_expr::PlanIdentityMap,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
) -> crate::Result<UpdatePlan> {
    let target_source = document.source(statement.target).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "missing UPDATE target source {}",
            statement.target
        ))
    })?;
    let database_id = target_source
        .database
        .map_or(crate::MAIN_DB_ID, |database| database.index());

    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    let mut hir_ctx = super::planner::HirPlanContext::new(document, identities, program);
    let mut target_table =
        super::planner::prepare_hir_source(&mut hir_ctx, statement.target, None)?;
    let table = target_table.table.clone();
    let (from_tables, mut where_clause) = if let Some(from) = &statement.from {
        let prepared = super::planner::prepare_hir_from(&mut hir_ctx, from)?;
        (prepared.table_references, prepared.predicates)
    } else {
        (
            hir_ctx.new_table_references(Vec::new(), Vec::new())?,
            Vec::new(),
        )
    };

    // SQLite rejects UPDATE FROM when a NATURAL JOIN (or explicit USING) introduces
    // a column name that already appears in another FROM-side table without being
    // deduplicated. This proactive check mirrors what SQLite does even when no
    // unqualified column reference appears in the query.
    if !from_tables.joined_tables().is_empty() {
        check_update_from_using_ambiguity(from_tables.joined_tables())?;
    }

    let mut read_scope_tables = TableReferences::new(vec![target_table.clone()], vec![]);
    if from_tables.right_join_swapped() {
        read_scope_tables.set_right_join_swapped();
    }
    read_scope_tables.extend(from_tables);
    let set_clauses = collect_update_set_clauses(&statement.assignments, identities)?;
    let defaults = statement
        .defaults
        .iter()
        .map(|default| Ok((default.column, lower_expr(&default.value, identities)?)))
        .collect::<crate::Result<Vec<_>>>()?;
    if let Some(predicate) = &statement.predicate {
        split_where_expr(lower_expr(predicate, identities)?, &mut where_clause);
    }
    let returning = statement
        .returning
        .as_ref()
        .map(|returning| {
            returning
                .outputs
                .iter()
                .map(|output| lower_output(output, identities))
                .collect::<crate::Result<Vec<_>>>()
        })
        .transpose()?;
    let (limit, offset) = match &statement.limit {
        Some(limit) => (
            Some(lower_expr(&limit.limit, identities)?),
            limit
                .offset
                .as_ref()
                .map(|offset| lower_expr(offset, identities))
                .transpose()?,
        ),
        None => (None, None),
    };

    for term in &where_clause {
        read_scope_tables.register_plan_expr_usage(&term.expr)?;
    }
    for set in &set_clauses {
        read_scope_tables.register_plan_expr_usage(&set.expr)?;
    }
    if let Some(returning) = &returning {
        for output in returning {
            read_scope_tables.register_plan_expr_usage(&output.expr)?;
        }
    }
    if let Some(limit) = &limit {
        read_scope_tables.register_plan_expr_usage(limit)?;
    }
    if let Some(offset) = &offset {
        read_scope_tables.register_plan_expr_usage(offset)?;
    }

    let indexes_to_update = collect_indexes_to_update(
        &table,
        &set_clauses,
        read_scope_tables
            .joined_tables_mut()
            .first_mut()
            .expect("UPDATE read scope must start with its target"),
    )?;

    let mut non_from_clause_subqueries = Vec::new();
    let where_expressions = where_clause
        .iter()
        .map(|term| &term.expr)
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        &mut hir_ctx,
        &mut read_scope_tables,
        &where_expressions,
        super::plan::SubqueryOrigin::DmlWhere,
        &mut non_from_clause_subqueries,
    )?;
    let set_expressions = set_clauses.iter().map(|set| &set.expr).collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        &mut hir_ctx,
        &mut read_scope_tables,
        &set_expressions,
        super::plan::SubqueryOrigin::DmlSet,
        &mut non_from_clause_subqueries,
    )?;
    if let Some(returning) = &returning {
        let returning_expressions = returning
            .iter()
            .map(|output| &output.expr)
            .collect::<Vec<_>>();
        super::subquery::prepare_hir_expression_subqueries(
            &mut hir_ctx,
            &mut read_scope_tables,
            &returning_expressions,
            super::plan::SubqueryOrigin::DmlReturning,
            &mut non_from_clause_subqueries,
        )?;
    }
    let limit_expressions = limit.iter().chain(offset.iter()).collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        &mut hir_ctx,
        &mut read_scope_tables,
        &limit_expressions,
        super::plan::SubqueryOrigin::DmlWhere,
        &mut non_from_clause_subqueries,
    )?;
    drop(hir_ctx);

    // UPDATE ... FROM and RETURNING are emitted in separate phases, but both
    // still belong to one statement query tree. Mark repeated CTE reads before
    // the optimizer splits that tree into its read and write plans so an
    // uncorrelated RETURNING read reuses the pre-write snapshot.
    super::subquery::mark_shared_cte_materialization_requirements(
        &mut read_scope_tables,
        &mut non_from_clause_subqueries,
    );

    target_table = read_scope_tables.joined_tables_mut().remove(0);
    let from_tables = read_scope_tables;

    Ok(UpdatePlan {
        target_table,
        from_tables,
        or_conflict: statement.conflict,
        defaults,
        set_clauses,
        where_clause,
        returning,
        limit,
        offset,
        contains_constant_false_condition: false,
        indexes_to_update,
        write_set_plan: None,
        cdc_update_alter_statement: None,
        non_from_clause_subqueries,
        safety: DmlSafety::default(),
    })
}

pub(crate) fn collect_update_set_clauses(
    assignments: &[super::semantic::hir::Assignment],
    identities: &super::plan_expr::PlanIdentityMap,
) -> crate::Result<Vec<UpdateSetClause>> {
    use super::semantic::hir::TargetColumn;

    let mut set_clauses: Vec<UpdateSetClause> = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let value = lower_expr(&assignment.value, identities)?;
        let values = split_update_set_values(value, assignment.columns.len())?;
        for (column, value) in assignment.columns.iter().copied().zip(values) {
            let column_index = match column {
                TargetColumn::Column(index) => index,
                TargetColumn::RowId => ROWID_SENTINEL,
            };
            match set_clauses
                .iter_mut()
                .find(|set| set.column_index == column_index)
            {
                Some(existing) => compose_update_set_clause(existing, value),
                None => set_clauses.push(UpdateSetClause::new(column_index, value)),
            }
        }
    }
    Ok(set_clauses)
}

fn compose_update_set_clause(existing: &mut UpdateSetClause, expr: super::plan_expr::PlanExpr) {
    let super::plan_expr::PlanExpr::Function(function) = &expr else {
        existing.expr = expr;
        return;
    };
    if !matches!(
        function.function.value(),
        Func::Scalar(ScalarFunc::ArraySetElement)
    ) || function.arguments.len() != 3
    {
        existing.expr = expr;
        return;
    }

    let mut composed = function.clone();
    composed.arguments[0].clone_from(&existing.expr);
    existing.expr = super::plan_expr::PlanExpr::Function(composed);
}

fn collect_indexes_to_update(
    table: &Table,
    set_clauses: &[UpdateSetClause],
    target_table: &mut JoinedTable,
) -> crate::Result<Vec<super::semantic::hir::ResolvedIndex>> {
    use crate::alloc::TursoIteratorExt;

    let columns = table.columns();
    let rowid_alias_used = set_clauses.iter().any(|set| {
        set.column_index == ROWID_SENTINEL
            || columns
                .get(set.column_index)
                .is_some_and(|column| column.is_rowid_alias())
    });
    let updated_cols: Option<ColumnMask> = (!rowid_alias_used)
        .then(|| set_clauses.iter().map(|set| set.column_index).try_collect())
        .transpose()?;
    let affected_cols = match (table.btree(), updated_cols.as_ref()) {
        (Some(table), Some(updated)) => Some(table.columns_affected_by_update(updated)?),
        (None, Some(updated)) => Some(updated.clone()),
        _ => None,
    };

    let target = target_table.internal_id;
    let mut indexes = Vec::new();
    let mut columns_to_mark = ColumnUsedMask::default();
    for planned in &target_table.index_expressions {
        let index = planned.index.value();
        let mut dependencies = ColumnUsedMask::default();
        for expression in planned.columns.iter().flatten() {
            dependencies.union_with(&plan_expr_column_usage(expression, target)?)?;
        }
        if let Some(predicate) = &planned.predicate {
            dependencies.union_with(&plan_expr_column_usage(predicate, target)?)?;
        }

        let direct_column_changed = affected_cols.as_ref().is_some_and(|affected| {
            index.columns.iter().any(|column| {
                column.pos_in_table != EXPR_INDEX_SENTINEL && affected.get(column.pos_in_table)
            })
        });
        let expression_changed = affected_cols
            .as_ref()
            .is_some_and(|affected| dependencies.iter().any(|column| affected.get(column)));
        if rowid_alias_used || direct_column_changed || expression_changed {
            columns_to_mark.union_with(&dependencies)?;
            if let Some(table) = table.btree() {
                let virtual_columns = index
                    .columns
                    .iter()
                    .filter(|column| column.pos_in_table != EXPR_INDEX_SENTINEL)
                    .map(|column| column.pos_in_table)
                    .filter(|&column| table.columns()[column].is_virtual_generated());
                for dependency in table.dependencies_of_columns(virtual_columns)?.iter() {
                    columns_to_mark.set(dependency)?;
                }
            }
            indexes.push(planned.index.clone());
        }
    }

    for column in columns_to_mark {
        target_table.mark_column_used(column);
    }
    Ok(indexes)
}

fn plan_expr_column_usage(
    expr: &super::plan_expr::PlanExpr,
    target: super::plan_expr::PlanSourceId,
) -> crate::Result<ColumnUsedMask> {
    use super::plan_expr::{plan_expr_dependencies, PlanColumnUse};

    let mut columns = ColumnUsedMask::default();
    for (source, column) in plan_expr_dependencies(expr)?.source_uses {
        if source == target {
            if let PlanColumnUse::Column(column) = column {
                columns.set(column)?;
            }
        }
    }
    Ok(columns)
}

/// SQLite rejects UPDATE FROM when a NATURAL JOIN or USING deduplicates a
/// column but another table in the FROM graph also exposes it without
/// deduplication, even when no unqualified reference names that column.
fn check_update_from_using_ambiguity(joined_tables: &[JoinedTable]) -> crate::Result<()> {
    // For each USING/NATURAL-deduplicated column, verify that no
    // other table in the FROM graph (preceding or following) exposes the
    // same column without its own deduplication.
    for (i, table) in joined_tables.iter().enumerate() {
        let using = match &table.join_info {
            Some(info) if !info.using.is_empty() => &info.using,
            _ => continue,
        };
        for using_col in using {
            let col_name = normalize_ident(using_col.as_str());

            // Count how many *other* tables expose this column without it
            // being covered by their own USING clause.
            let mut found_count = 0usize;
            for (j, other) in joined_tables.iter().enumerate() {
                if j == i {
                    continue;
                }
                let has_col = other.columns().iter().any(|c| {
                    c.name
                        .as_ref()
                        .is_some_and(|n| n.eq_ignore_ascii_case(&col_name))
                });
                if !has_col {
                    continue;
                }
                // If this table's own USING already covers the column,
                // it was deduplicated by its own NATURAL/USING JOIN — skip.
                let already_deduped = other.join_info.as_ref().is_some_and(|info| {
                    info.using
                        .iter()
                        .any(|u| u.as_str().eq_ignore_ascii_case(&col_name))
                });
                if !already_deduped {
                    found_count += 1;
                }
            }
            if found_count > 1 {
                bail_parse_error!("ambiguous column name: {}", using_col.as_str());
            }
        }
    }
    Ok(())
}
