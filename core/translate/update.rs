use crate::sync::Arc;
use rustc_hash::FxHashSet as HashSet;

use crate::schema::ROWID_SENTINEL;
use crate::translate::bind::BindContext;
use crate::translate::emitter::Resolver;
use crate::translate::expression_index::expression_index_column_usage;
use crate::translate::plan::{Operation, Scan};
use crate::{
    bail_parse_error,
    schema::{Schema, Table},
    vdbe::builder::{ProgramBuilder, ProgramBuilderOpts},
    CaptureDataChangesExt, Connection,
};
use turso_parser::ast::{self, Indexed, SortOrder};

use super::emitter::emit_program;
use super::optimizer::optimize_plan;
use super::plan::{
    DmlSafety, IterationDirection, Plan, ResultSetColumn, UpdatePlan,
};
use super::planner::{parse_where, plan_bound_ctes};
use super::subquery::{
    plan_subqueries_from_returning_with_bound, plan_subqueries_from_select_plan,
    plan_subqueries_from_set_clauses_with_bound, plan_subqueries_from_where_clause_with_bound,
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
    body: ast::Update,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> crate::Result<()> {
    let (mut plan, mut bound_subqueries) = bind_prepare_update_plan(program, resolver, body, connection, false)?;

    // Plan subqueries in the WHERE, SET, and RETURNING clauses
    if let Plan::Update(ref mut update_plan) = plan {
        if let Some(ref mut ephemeral_plan) = update_plan.ephemeral_plan {
            plan_subqueries_from_select_plan(program, ephemeral_plan, resolver, connection, Default::default())?;
        } else {
            plan_subqueries_from_where_clause_with_bound(
                program,
                &mut update_plan.non_from_clause_subqueries,
                &mut update_plan.table_references,
                &mut update_plan.where_clause,
                resolver,
                connection,
                &mut bound_subqueries,
            )?;
        }
        plan_subqueries_from_set_clauses_with_bound(
            program,
            &mut update_plan.non_from_clause_subqueries,
            &mut update_plan.table_references,
            &mut update_plan.set_clauses,
            resolver,
            connection,
            &mut bound_subqueries,
        )?;
        if let Some(ref mut returning) = update_plan.returning {
            plan_subqueries_from_returning_with_bound(
                program,
                &mut update_plan.non_from_clause_subqueries,
                &mut update_plan.table_references,
                returning,
                resolver,
                connection,
                &mut bound_subqueries,
            )?;
        }
    }

    optimize_plan(program, &mut plan, resolver)?;

    if let Plan::Update(ref update_plan) = plan {
        super::stmt_journal::set_update_stmt_journal_flags(
            program,
            update_plan,
            resolver,
            connection,
        )?;
    }

    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 20,
        approx_num_labels: 4,
    };
    program.extend(&opts);
    emit_program(connection, resolver, program, plan, |_| {})?;
    Ok(())
}

pub fn translate_update_for_schema_change(
    body: ast::Update,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    ddl_query: &str,
    after: impl FnOnce(&mut ProgramBuilder),
) -> crate::Result<()> {
    let (mut plan, mut bound_subqueries) = bind_prepare_update_plan(program, resolver, body, connection, true)?;

    if let Plan::Update(update_plan) = &mut plan {
        if program.capture_data_changes_info().has_updates() {
            update_plan.cdc_update_alter_statement = Some(ddl_query.to_string());
        }

        // Plan subqueries in the WHERE clause
        if let Some(ref mut ephemeral_plan) = update_plan.ephemeral_plan {
            plan_subqueries_from_select_plan(program, ephemeral_plan, resolver, connection, Default::default())?;
        } else {
            plan_subqueries_from_where_clause_with_bound(
                program,
                &mut update_plan.non_from_clause_subqueries,
                &mut update_plan.table_references,
                &mut update_plan.where_clause,
                resolver,
                connection,
                &mut bound_subqueries,
            )?;
        }
        plan_subqueries_from_set_clauses_with_bound(
            program,
            &mut update_plan.non_from_clause_subqueries,
            &mut update_plan.table_references,
            &mut update_plan.set_clauses,
            resolver,
            connection,
            &mut bound_subqueries,
        )?;
        if let Some(ref mut returning) = update_plan.returning {
            plan_subqueries_from_returning_with_bound(
                program,
                &mut update_plan.non_from_clause_subqueries,
                &mut update_plan.table_references,
                returning,
                resolver,
                connection,
                &mut bound_subqueries,
            )?;
        }
    }

    optimize_plan(program, &mut plan, resolver)?;
    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 20,
        approx_num_labels: 4,
    };
    program.extend(&opts);
    emit_program(connection, resolver, program, plan, after)?;
    Ok(())
}

fn validate_update(
    schema: &Schema,
    body: &ast::Update,
    table_name: &str,
    is_internal_schema_change: bool,
    conn: &Arc<Connection>,
) -> crate::Result<()> {
    // Check if this is a system table that should be protected from direct writes
    if !is_internal_schema_change
        && !conn.is_nested_stmt()
        && !conn.is_mvcc_bootstrap_connection()
        && !crate::schema::can_write_to_table(table_name)
    {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }
    if body.from.is_some() {
        bail_parse_error!("FROM clause is not supported in UPDATE");
    }
    if body
        .indexed
        .as_ref()
        .is_some_and(|i| matches!(i, Indexed::IndexedBy(_)))
    {
        bail_parse_error!("INDEXED BY clause is not supported in UPDATE");
    }

    if !body.order_by.is_empty() {
        bail_parse_error!("ORDER BY is not supported in UPDATE");
    }
    // Check if this is a materialized view
    if schema.is_materialized_view(table_name) {
        bail_parse_error!("cannot modify materialized view {}", table_name);
    }

    // Check if this table has any incompatible dependent views
    let incompatible_views = schema.has_incompatible_dependent_views(table_name);
    if !incompatible_views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        bail_parse_error!(
            "Cannot UPDATE table '{}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            table_name,
            incompatible_views.join(", "),
            DBSP_CIRCUIT_VERSION
        );
    }
    Ok(())
}

/// Bind and prepare an UPDATE plan: runs binding then planning.
fn bind_prepare_update_plan(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut body: ast::Update,
    connection: &Arc<crate::Connection>,
    is_internal_schema_change: bool,
) -> crate::Result<(Plan, rustc_hash::FxHashMap<ast::TableInternalId, super::bind::BoundSubquery>)> {
    let database_id = resolver.resolve_database_id(&body.tbl_name)?;
    let schema = resolver.schema();
    let table_name = &body.tbl_name.name;
    let table = match resolver.with_schema(database_id, |s| s.get_table(table_name.as_str())) {
        Some(table) => table,
        None => bail_parse_error!("Parse error: no such table: {}", table_name),
    };
    if program.trigger.is_some() && table.virtual_table().is_some() {
        bail_parse_error!(
            "unsafe use of virtual table \"{}\"",
            body.tbl_name.name.as_str()
        );
    }
    if crate::is_attached_db(database_id) {
        let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
        program.begin_write_on_database(database_id, schema_cookie);
    }
    validate_update(
        schema,
        &body,
        table_name.as_str(),
        is_internal_schema_change,
        connection,
    )?;

    // Bind phase
    let mut binder = BindContext::new(resolver, program);
    let mut bound = binder.bind_update(&mut body)?;

    // Extract bound data before consuming `bound` for table references.
    let cte_definitions = std::mem::take(&mut bound.cte_definitions);
    let set_clauses = std::mem::take(&mut bound.set_clauses);
    let bound_result_columns = std::mem::take(&mut bound.result_columns);
    let subquery_bindings = std::mem::take(&mut bound.subquery_bindings);

    // Plan CTEs using pre-bound data from the binder.
    let mut planned_ctes = plan_bound_ctes(cte_definitions, resolver, program, connection)?;

    let mut table_references = bound.into_table_references(&mut planned_ctes)?;

    // Convert bound result columns to ResultSetColumn for the plan
    let result_columns: Vec<ResultSetColumn> = bound_result_columns
        .into_iter()
        .map(|bc| ResultSetColumn {
            expr: bc.expr,
            alias: if bc.name.is_empty() {
                None
            } else {
                Some(bc.name)
            },
            contains_aggregates: false,
        })
        .collect();

    let or_conflict = body.or_conflict.take();
    let table_name = table.get_name();

    let iter_dir = body
        .order_by
        .first()
        .and_then(|ob| {
            ob.order.map(|o| match o {
                SortOrder::Asc => IterationDirection::Forwards,
                SortOrder::Desc => IterationDirection::Backwards,
            })
        })
        .unwrap_or(IterationDirection::Forwards);

    // Set the scan operation based on iteration direction
    if let Some(target) = table_references.joined_tables_mut().first_mut() {
        target.op = build_scan_op(&table, iter_dir);
        target.database_id = database_id;
    }

    let order_by = body
        .order_by
        .iter()
        .map(|o| (o.expr.clone(), o.order.unwrap_or(SortOrder::Asc)))
        .collect();

    // Parse the WHERE clause
    let mut where_clause = vec![];
    parse_where(body.where_clause.as_deref(), &mut where_clause)?;

    // Parse the LIMIT/OFFSET clause
    let (limit, offset) = body
        .limit
        .map_or((None, None), |l| (Some(l.expr), l.offset));

    // Check what indexes will need to be updated by checking set_clauses and see
    // if a column is contained in an index.
    let indexes: Vec<_> = resolver.with_schema(database_id, |s| {
        s.get_indices(table_name).cloned().collect()
    });
    let updated_cols: HashSet<usize> = set_clauses.iter().map(|(i, _)| *i).collect();
    let columns = table.columns();
    let rowid_alias_used = set_clauses
        .iter()
        .any(|(idx, _)| *idx == ROWID_SENTINEL || columns[*idx].is_rowid_alias());
    let target_table_ref = table_references
        .joined_tables()
        .first()
        .expect("UPDATE must have a target table reference");
    let indexes_to_update = if rowid_alias_used {
        // If the rowid alias is used in the SET clause, we need to update all indexes
        indexes
    } else {
        // otherwise we need to update the indexes whose columns are set in the SET clause,
        // or if the columns used in the partial index WHERE clause are being updated.
        let mut indexes_to_update = Vec::new();
        for idx in indexes {
            let mut needs = false;
            for col in idx.columns.iter() {
                if let Some(expr) = col.expr.as_ref() {
                    let cols_used =
                        expression_index_column_usage(expr.as_ref(), target_table_ref, resolver)?;
                    if cols_used.iter().any(|cidx| updated_cols.contains(&cidx)) {
                        needs = true;
                        break;
                    }
                } else if updated_cols.contains(&col.pos_in_table) {
                    needs = true;
                    break;
                }
            }

            if !needs {
                if let Some(where_expr) = &idx.where_clause {
                    let cols_used = expression_index_column_usage(
                        where_expr.as_ref(),
                        target_table_ref,
                        resolver,
                    )?;
                    // If any column used in the partial index WHERE clause is being updated,
                    // this index must be updated as well.
                    needs = cols_used.iter().any(|cidx| updated_cols.contains(&cidx));
                }
            }

            if needs {
                indexes_to_update.push(idx);
            }
        }
        indexes_to_update
    };

    Ok((Plan::Update(UpdatePlan {
        table_references,
        or_conflict,
        set_clauses,
        where_clause,
        returning: if result_columns.is_empty() {
            None
        } else {
            Some(result_columns)
        },
        order_by,
        limit,
        offset,
        contains_constant_false_condition: false,
        indexes_to_update,
        ephemeral_plan: None,
        cdc_update_alter_statement: None,
        non_from_clause_subqueries: vec![],
        safety: DmlSafety::default(),
    }), subquery_bindings))
}

fn build_scan_op(table: &Table, iter_dir: IterationDirection) -> Operation {
    match table {
        Table::BTree(_) => Operation::Scan(Scan::BTreeTable {
            iter_dir,
            index: None,
        }),
        Table::Virtual(_) => Operation::default_scan_for(table),
        _ => unreachable!(),
    }
}
