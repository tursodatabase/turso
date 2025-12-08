use crate::schema::{Schema, Table};
use crate::translate::emitter::{emit_program, Resolver};
use crate::translate::expr::process_returning_clause;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{
    DeletePlan, IterationDirection, JoinOrderMember, Operation, Plan, QueryDestination,
    ResultSetColumn, Scan, SelectPlan,
};
use crate::translate::planner::{parse_limit, parse_where};
use crate::translate::trigger_exec::has_relevant_triggers_type_only;
use crate::util::normalize_ident;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::Result;
use std::sync::Arc;
use turso_parser::ast::{Expr, Limit, QualifiedName, ResultColumn, TriggerEvent};

use super::plan::{ColumnUsedMask, JoinedTable, TableReferences};

pub fn translate_delete(
    tbl_name: &QualifiedName,
    resolver: &Resolver,
    where_clause: Option<Box<Expr>>,
    limit: Option<Limit>,
    returning: Vec<ResultColumn>,
    mut program: ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<ProgramBuilder> {
    let tbl_name = normalize_ident(tbl_name.name.as_str());

    // Check if this is a system table that should be protected from direct writes
    if crate::schema::is_system_table(&tbl_name) {
        crate::bail_parse_error!("table {} may not be modified", tbl_name);
    }

    let mut delete_plan = prepare_delete_plan(
        &mut program,
        resolver.schema,
        tbl_name,
        where_clause,
        limit,
        returning,
        connection,
    )?;
    optimize_plan(&mut program, &mut delete_plan, resolver.schema)?;
    if let Plan::Delete(delete_plan_inner) = &mut delete_plan {
        // Rewrite the Delete plan after optimization whenever a RowSet is used (DELETE triggers
        // are present), so the joined table is treated as a plain table scan again.
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
    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: estimate_num_instructions(delete),
        approx_num_labels: 0,
    };
    program.extend(&opts);
    emit_program(connection, resolver, &mut program, delete_plan, |_| {})?;
    Ok(program)
}

pub fn prepare_delete_plan(
    program: &mut ProgramBuilder,
    schema: &Schema,
    tbl_name: String,
    where_clause: Option<Box<Expr>>,
    limit: Option<Limit>,
    mut returning: Vec<ResultColumn>,
    connection: &Arc<crate::Connection>,
) -> Result<Plan> {
    let table = match schema.get_table(&tbl_name) {
        Some(table) => table,
        None => crate::bail_parse_error!("no such table: {}", tbl_name),
    };

    // Check if this is a materialized view
    if schema.is_materialized_view(&tbl_name) {
        crate::bail_parse_error!("cannot modify materialized view {}", tbl_name);
    }

    // Check if this table has any incompatible dependent views
    let incompatible_views = schema.has_incompatible_dependent_views(&tbl_name);
    if !incompatible_views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot DELETE from table '{}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            tbl_name,
            incompatible_views.join(", "),
            DBSP_CIRCUIT_VERSION
        );
    }

    let btree_table_for_triggers = table.btree();

    let table = if let Some(table) = table.virtual_table() {
        Table::Virtual(table.clone())
    } else if let Some(table) = table.btree() {
        Table::BTree(table.clone())
    } else {
        crate::bail_parse_error!("Table is neither a virtual table nor a btree table");
    };
    let indexes = schema.get_indices(table.get_name()).cloned().collect();
    let joined_tables = vec![JoinedTable {
        op: Operation::default_scan_for(&table),
        table,
        identifier: tbl_name,
        internal_id: program.table_reference_counter.next(),
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: 0,
    }];
    let mut table_references = TableReferences::new(joined_tables, vec![]);

    let mut where_predicates = vec![];

    // Parse the WHERE clause
    parse_where(
        where_clause.as_deref(),
        &mut table_references,
        None,
        &mut where_predicates,
        connection,
    )?;

    let result_columns =
        process_returning_clause(&mut returning, &mut table_references, connection)?;

    // Parse the LIMIT/OFFSET clause
    let (resolved_limit, resolved_offset) =
        limit.map_or(Ok((None, None)), |l| parse_limit(l, connection))?;

    // Check if there are DELETE triggers. If so, we need to materialize the write set into a RowSet first.
    // This is done in SQLite for all DELETE triggers on the affected table even if the trigger would not have an impact
    // on the target table -- presumably due to lack of static analysis capabilities to determine whether it's safe
    // to skip the rowset materialization.
    let has_delete_triggers = btree_table_for_triggers
        .as_ref()
        .map(|bt| has_relevant_triggers_type_only(schema, TriggerEvent::Delete, None, bt))
        .unwrap_or(false);

    if has_delete_triggers {
        // Create a SelectPlan that materializes rowids into a RowSet
        let rowid_internal_id = table_references
            .joined_tables()
            .first()
            .unwrap()
            .internal_id;
        let rowset_reg = program.alloc_register();

        let rowset_plan = SelectPlan {
            table_references: table_references.clone(),
            result_columns: vec![ResultSetColumn {
                expr: Expr::RowId {
                    database: None,
                    table: rowid_internal_id,
                },
                alias: None,
                contains_aggregates: false,
            }],
            where_clause: where_predicates,
            group_by: None,
            order_by: vec![],
            aggregates: vec![],
            limit: resolved_limit,
            query_destination: QueryDestination::RowSet { rowset_reg },
            join_order: table_references
                .joined_tables()
                .iter()
                .enumerate()
                .map(|(i, t)| JoinOrderMember {
                    table_id: t.internal_id,
                    original_idx: i,
                    is_outer: false,
                })
                .collect(),
            offset: resolved_offset,
            contains_constant_false_condition: false,
            distinctness: super::plan::Distinctness::NonDistinct,
            values: vec![],
            window: None,
            non_from_clause_subqueries: vec![],
        };

        Ok(Plan::Delete(DeletePlan {
            table_references,
            result_columns,
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            contains_constant_false_condition: false,
            indexes,
            rowset_plan: Some(rowset_plan),
            rowset_reg: Some(rowset_reg),
        }))
    } else {
        Ok(Plan::Delete(DeletePlan {
            table_references,
            result_columns,
            where_clause: where_predicates,
            order_by: vec![],
            limit: resolved_limit,
            offset: resolved_offset,
            contains_constant_false_condition: false,
            indexes,
            rowset_plan: None,
            rowset_reg: None,
        }))
    }
}

fn estimate_num_instructions(plan: &DeletePlan) -> usize {
    let base = 20;

    base + plan.table_references.joined_tables().len() * 10
}
