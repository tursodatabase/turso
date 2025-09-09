use crate::schema::Table;
use crate::translate::emitter::emit_program;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{DeletePlan, Operation, Plan};
use crate::translate::planner::{parse_limit, parse_where};
use crate::util::normalize_ident;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, TableRefIdCounter};
use crate::{schema::Schema, Result, SymbolTable};
use std::sync::Arc;
use turso_parser::ast::{Expr, Limit, QualifiedName, ResultColumn};

use super::plan::{ColumnUsedMask, JoinedTable, TableReferences};

#[allow(clippy::too_many_arguments)]
pub fn translate_delete(
    schema: &Schema,
    tbl_name: &QualifiedName,
    where_clause: Option<Box<Expr>>,
    limit: Option<Limit>,
    returning: Vec<ResultColumn>,
    syms: &SymbolTable,
    mut program: ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<ProgramBuilder> {
    let tbl_name = normalize_ident(tbl_name.name.as_str());

    // Check if this is a system table that should be protected from direct writes
    if crate::schema::is_system_table(&tbl_name) {
        crate::bail_parse_error!("table {} may not be modified", tbl_name);
    }

    if schema.table_has_indexes(&tbl_name) && !schema.indexes_enabled() {
        // Let's disable altering a table with indices altogether instead of checking column by
        // column to be extra safe.
        crate::bail_parse_error!(
            "DELETE for table with indexes is disabled. Omit the `--experimental-indexes=false` flag to enable this feature."
        );
    }

    // FIXME: SQLite's delete using Returning is complex. It scans the table in read mode first, building
    // the result set, and only after that it opens the table for writing and deletes the rows. It
    // also uses a couple of instructions that we don't implement yet (i.e.: RowSetAdd, RowSetRead,
    // RowSetTest). So for now I'll just defer it altogether.
    if !returning.is_empty() {
        crate::bail_parse_error!("RETURNING currently not implemented for DELETE statements.");
    }
    let result_columns = vec![];

    let mut delete_plan = prepare_delete_plan(
        schema,
        tbl_name,
        where_clause,
        limit,
        result_columns,
        &mut program.table_reference_counter,
        connection,
    )?;
    optimize_plan(&mut delete_plan, schema)?;
    let Plan::Delete(ref delete) = delete_plan else {
        panic!("delete_plan is not a DeletePlan");
    };
    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: estimate_num_instructions(delete),
        approx_num_labels: 0,
    };
    program.extend(&opts);
    emit_program(&mut program, delete_plan, schema, syms, |_| {})?;
    Ok(program)
}

pub fn prepare_delete_plan(
    schema: &Schema,
    tbl_name: String,
    where_clause: Option<Box<Expr>>,
    limit: Option<Limit>,
    result_columns: Vec<super::plan::ResultSetColumn>,
    table_ref_counter: &mut TableRefIdCounter,
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

    let table = if let Some(table) = table.virtual_table() {
        Table::Virtual(table.clone())
    } else if let Some(table) = table.btree() {
        Table::BTree(table.clone())
    } else {
        crate::bail_parse_error!("Table is neither a virtual table nor a btree table");
    };
    let indexes = schema.get_indices(table.get_name()).to_vec();
    let joined_tables = vec![JoinedTable {
        op: Operation::default_scan_for(&table),
        table,
        identifier: tbl_name,
        internal_id: table_ref_counter.next(),
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
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

    // Parse the LIMIT/OFFSET clause
    let (resolved_limit, resolved_offset) =
        limit.map_or(Ok((None, None)), |mut l| parse_limit(&mut l, connection))?;

    let plan = DeletePlan {
        table_references,
        result_columns,
        where_clause: where_predicates,
        order_by: vec![],
        limit: resolved_limit,
        offset: resolved_offset,
        contains_constant_false_condition: false,
        indexes,
    };

    Ok(Plan::Delete(plan))
}

fn estimate_num_instructions(plan: &DeletePlan) -> usize {
    let base = 20;

    base + plan.table_references.joined_tables().len() * 10
}
