use super::emitter::{emit_program, TranslateCtx};
use super::plan::{
    select_star, Distinctness, JoinOrderMember, Operation, OuterQueryReference, QueryDestination,
    Search, TableReferences, WhereTerm, Window,
};
use crate::schema::{ColDef, Column, Index, IndexColumn, RecursiveCteTable, Table};
use crate::sync::Arc;
use crate::translate::emitter::{OperationMode, Resolver};
use crate::translate::expr::{
    bind_and_rewrite_expr, expr_vector_size, translate_expr, BindingBehavior,
};
use crate::translate::group_by::compute_group_by_sort_order;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{GroupBy, Plan, ResultSetColumn, SelectPlan};
use crate::translate::planner::{
    break_predicate_at_and_boundaries, parse_from, parse_limit, parse_where,
    plan_ctes_as_outer_refs, resolve_window_and_aggregate_functions,
};
use crate::translate::subquery::{plan_subqueries_from_select_plan, plan_subqueries_from_values};
use crate::translate::window::plan_windows;
use crate::util::normalize_ident;
use crate::vdbe::builder::{CursorType, ProgramBuilderOpts};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, Insn};
use crate::vdbe::BranchOffset;
use crate::Connection;
use crate::{vdbe::builder::ProgramBuilder, Result};
use turso_parser::ast::{self, CompoundSelect, Expr};
use turso_parser::ast::{CompoundOperator, ResultColumn, SortOrder};

pub struct TranslateSelectResult {
    pub program: ProgramBuilder,
    pub num_result_cols: usize,
}

pub fn translate_select(
    select: ast::Select,
    resolver: &Resolver,
    mut program: ProgramBuilder,
    query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<TranslateSelectResult> {
    // Check if this is a recursive CTE - if so, use the DBSP execution path
    if has_recursive_cte(&select) {
        return translate_recursive_cte(select, resolver, program, query_destination, connection);
    }

    let mut select_plan = prepare_select_plan(
        select,
        resolver,
        &mut program,
        &[],
        query_destination,
        connection,
    )?;
    optimize_plan(&mut program, &mut select_plan, resolver.schema)?;
    let num_result_cols;
    let opts = match &select_plan {
        Plan::Select(select) => {
            num_result_cols = select.result_columns.len();
            ProgramBuilderOpts {
                num_cursors: count_required_cursors_for_simple_select(select),
                approx_num_insns: estimate_num_instructions_for_simple_select(select),
                approx_num_labels: estimate_num_labels_for_simple_select(select),
            }
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            // Compound Selects must return the same number of columns
            num_result_cols = right_most.result_columns.len();

            ProgramBuilderOpts {
                num_cursors: count_required_cursors_for_simple_select(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| count_required_cursors_for_simple_select(plan))
                        .sum::<usize>(),
                approx_num_insns: estimate_num_instructions_for_simple_select(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| estimate_num_instructions_for_simple_select(plan))
                        .sum::<usize>(),
                approx_num_labels: estimate_num_labels_for_simple_select(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| estimate_num_labels_for_simple_select(plan))
                        .sum::<usize>(),
            }
        }
        other => panic!("plan is not a SelectPlan: {other:?}"),
    };

    program.extend(&opts);
    emit_program(connection, resolver, &mut program, select_plan, |_| {})?;
    Ok(TranslateSelectResult {
        program,
        num_result_cols,
    })
}

/// Check if a SELECT statement contains a recursive CTE
fn has_recursive_cte(select: &ast::Select) -> bool {
    select.with.as_ref().is_some_and(|w| w.recursive)
}

/// Translate a recursive CTE query using native VDBE instructions.
///
/// This implements the two-queue algorithm for recursive CTEs:
/// 1. Execute base case, store in result table and queue_a
/// 2. Loop: read from queue_a, execute recursive step, insert new rows to queue_b
/// 3. Clear queue_a, swap queue roles (a becomes b, b becomes a)
/// 4. Repeat until queue is empty
/// 5. Read from result table and emit rows
///
/// ## Current Limitations
///
/// This is a simplified implementation that handles common simple cases:
/// - Base case must be literals or VALUES (no FROM clause)
/// - Recursive step must reference only the CTE itself (no JOINs with other tables)
/// - Expressions in recursive step are limited to basic arithmetic and column refs
///
/// ## Path to Full Implementation
///
/// To support the full recursive CTE feature set (JOINs, complex expressions, etc.):
///
/// 1. Register the CTE as a pseudo-table in the Resolver, backed by the queue cursor
/// 2. Use `prepare_select_plan()` to plan base case and recursive step as normal queries
/// 3. The CTE reference resolves to the queue cursor during planning
/// 4. Use `emit_program()` with a custom destination that inserts into result/queue
/// 5. The existing JOIN/WHERE/expression infrastructure handles everything else
///
/// This mirrors SQLite's approach where recursive CTEs are virtual tables during planning.
fn translate_recursive_cte(
    select: ast::Select,
    resolver: &Resolver,
    mut program: ProgramBuilder,
    _query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<TranslateSelectResult> {
    let with = select.with.as_ref().ok_or_else(|| {
        crate::LimboError::ParseError("Expected WITH clause for recursive CTE".into())
    })?;

    if with.ctes.len() != 1 {
        crate::bail_parse_error!("Only single recursive CTE is currently supported");
    }

    let cte = &with.ctes[0];
    let cte_name = normalize_ident(cte.tbl_name.as_str());

    // Parse the CTE's SELECT to extract base case and recursive step
    let cte_select = &cte.select;
    let base_case = &cte_select.body.select;

    if cte_select.body.compounds.is_empty() {
        crate::bail_parse_error!("Recursive CTE must have UNION or UNION ALL");
    }

    let compound = &cte_select.body.compounds[0];
    let is_union_all = matches!(compound.operator, CompoundOperator::UnionAll);
    let recursive_step = &compound.select;

    // Get column names from CTE definition or base case
    let column_names: Vec<String> = if !cte.columns.is_empty() {
        cte.columns
            .iter()
            .map(|c| c.col_name.as_str().to_string())
            .collect()
    } else {
        // Infer from base case result columns
        match base_case {
            ast::OneSelect::Select { columns, .. } => columns
                .iter()
                .enumerate()
                .map(|(i, col)| match col {
                    ResultColumn::Expr(_, Some(ast::As::As(name) | ast::As::Elided(name))) => {
                        name.as_str().to_string()
                    }
                    ResultColumn::Expr(expr, None) => {
                        if let Expr::Id(id) = expr.as_ref() {
                            id.as_str().to_string()
                        } else {
                            format!("column{i}")
                        }
                    }
                    _ => format!("column{i}"),
                })
                .collect(),
            _ => crate::bail_parse_error!("Unsupported base case in recursive CTE"),
        }
    };

    let num_cols = column_names.len();

    // Parse outer query columns to determine which columns to output
    let outer_columns: Vec<usize> = match &select.body.select {
        ast::OneSelect::Select { columns, from, .. } => {
            // Verify the FROM clause references our CTE
            if let Some(from_clause) = from {
                match from_clause.select.as_ref() {
                    ast::SelectTable::Table(qn, _, _)
                        if normalize_ident(qn.name.as_str()) == cte_name => {}
                    _ => crate::bail_parse_error!("Outer query must reference the recursive CTE"),
                }
            } else {
                crate::bail_parse_error!("Outer query must have FROM clause referencing the CTE");
            }

            // Map outer columns to CTE column indices
            columns
                .iter()
                .map(|col| {
                    match col {
                        ResultColumn::Star => {
                            // SELECT * - will output all columns
                            usize::MAX // marker for "all columns"
                        }
                        ResultColumn::TableStar(_) => {
                            usize::MAX // marker for "all columns"
                        }
                        ResultColumn::Expr(expr, _) => {
                            match expr.as_ref() {
                                Expr::Id(id) => {
                                    let col_name = normalize_ident(id.as_str());
                                    column_names
                                        .iter()
                                        .position(|n| normalize_ident(n) == col_name)
                                        .unwrap_or(0)
                                }
                                Expr::Qualified(table, col) => {
                                    let table_name = normalize_ident(table.as_str());
                                    if table_name == cte_name {
                                        let col_name = normalize_ident(col.as_str());
                                        column_names
                                            .iter()
                                            .position(|n| normalize_ident(n) == col_name)
                                            .unwrap_or(0)
                                    } else {
                                        0
                                    }
                                }
                                _ => 0, // Unsupported expression, default to first column
                            }
                        }
                    }
                })
                .collect()
        }
        _ => (0..num_cols).collect(), // Default to all columns
    };

    // Determine actual output columns
    let output_column_indices: Vec<usize> = if outer_columns.contains(&usize::MAX) {
        (0..num_cols).collect() // SELECT * - output all columns
    } else {
        outer_columns
    };

    // Add result column names to program (only for output columns)
    for &idx in &output_column_indices {
        if idx < column_names.len() {
            program.add_pragma_result_column(column_names[idx].clone());
        }
    }

    // Estimate program size
    program.extend(&ProgramBuilderOpts {
        num_cursors: 4, // result table, queue_a, queue_b
        approx_num_insns: 300,
        approx_num_labels: 30,
    });

    // Create ephemeral index for result table (for deduplication with UNION)
    let result_index = Arc::new(Index {
        columns: column_names
            .iter()
            .enumerate()
            .map(|(i, name)| IndexColumn {
                name: name.clone(),
                order: SortOrder::Asc,
                pos_in_table: i,
                collation: None,
                default: None,
                expr: None,
            })
            .collect(),
        name: "rcte_result".to_string(),
        root_page: 0,
        ephemeral: true,
        table_name: String::new(),
        unique: false,
        has_rowid: false,
        where_clause: None,
        index_method: None,
    });

    // Allocate cursors: result table and two queues for ping-pong
    let result_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(result_index.clone()));
    let queue_a_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(result_index.clone()));
    let queue_b_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(result_index.clone()));

    // Allocate registers
    let reg_row_data = program.alloc_registers(num_cols);
    let reg_record = program.alloc_register();
    let reg_iteration = program.alloc_register();
    let reg_flag = program.alloc_register(); // 0 = read from A, write to B; 1 = read from B, write to A

    // Allocate labels
    let label_init = program.allocate_label();
    let label_loop_start = program.allocate_label();
    // Labels for queue_a -> queue_b path
    let label_inner_loop_a = program.allocate_label();
    let label_loop_next_a = program.allocate_label();
    let label_skip_insert_a = program.allocate_label();
    // Labels for queue_b -> queue_a path
    let label_read_from_b = program.allocate_label();
    let label_inner_loop_b = program.allocate_label();
    let label_loop_next_b = program.allocate_label();
    let label_skip_insert_b = program.allocate_label();
    // Common labels
    let label_next_iteration = program.allocate_label();
    let label_output_start = program.allocate_label();
    let label_output_next = program.allocate_label();
    let label_done = program.allocate_label();

    // Init
    program.emit_insn(Insn::Init {
        target_pc: label_init,
    });

    // Open ephemeral tables
    program.preassign_label_to_next_insn(label_init);
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: result_cursor,
        is_table: false,
    });
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: queue_a_cursor,
        is_table: false,
    });
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: queue_b_cursor,
        is_table: false,
    });

    // Initialize iteration counter and flag
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_iteration,
    });
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_flag,
    }); // start reading from A

    // === Execute base case ===
    // For each base case row, insert into result and queue_a
    emit_base_case_rows(
        &mut program,
        resolver,
        base_case,
        result_cursor,
        queue_a_cursor,
        reg_row_data,
        reg_record,
        num_cols,
        is_union_all,
        label_loop_start, // jump here on skip (no-op since we go there anyway)
        connection,
    )?;

    // === Main recursion loop ===
    program.preassign_label_to_next_insn(label_loop_start);

    // Check iteration limit (default 1000)
    program.emit_insn(Insn::Integer {
        value: 1000,
        dest: reg_record,
    });
    program.emit_insn(Insn::Ge {
        lhs: reg_iteration,
        rhs: reg_record,
        target_pc: label_output_start,
        flags: CmpInsFlags::default(),
        collation: None,
    });

    // Branch based on flag: if flag != 0, go to read_from_b
    program.emit_insn(Insn::If {
        reg: reg_flag,
        target_pc: label_read_from_b,
        jump_if_null: false, // flag is never null
    });

    // === Path A: Read from queue_a, write to queue_b ===
    // Rewind queue_a
    program.emit_insn(Insn::Rewind {
        cursor_id: queue_a_cursor,
        pc_if_empty: label_output_start, // if queue_a empty, we're done
    });

    // Inner loop A
    program.preassign_label_to_next_insn(label_inner_loop_a);
    emit_recursive_step_rows(
        &mut program,
        resolver,
        recursive_step,
        &cte_name,
        result_cursor,
        queue_a_cursor, // read from
        queue_b_cursor, // write to
        reg_row_data,
        reg_record,
        &column_names,
        is_union_all,
        label_loop_next_a,
        label_skip_insert_a,
        connection,
    )?;

    program.preassign_label_to_next_insn(label_loop_next_a);
    program.emit_insn(Insn::Next {
        cursor_id: queue_a_cursor,
        pc_if_next: label_inner_loop_a,
    });

    // After processing queue_a: clear it (OpenEphemeral clears if cursor exists) and read from B next
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: queue_a_cursor,
        is_table: false,
    });
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: reg_flag,
    }); // next iteration reads from B
    program.emit_insn(Insn::Goto {
        target_pc: label_next_iteration,
    });

    // Skip insert A
    program.preassign_label_to_next_insn(label_skip_insert_a);
    program.emit_insn(Insn::Goto {
        target_pc: label_loop_next_a,
    });

    // === Path B: Read from queue_b, write to queue_a ===
    program.preassign_label_to_next_insn(label_read_from_b);
    program.emit_insn(Insn::Rewind {
        cursor_id: queue_b_cursor,
        pc_if_empty: label_output_start,
    });

    // Inner loop B
    program.preassign_label_to_next_insn(label_inner_loop_b);
    emit_recursive_step_rows(
        &mut program,
        resolver,
        recursive_step,
        &cte_name,
        result_cursor,
        queue_b_cursor, // read from
        queue_a_cursor, // write to
        reg_row_data,
        reg_record,
        &column_names,
        is_union_all,
        label_loop_next_b,
        label_skip_insert_b,
        connection,
    )?;

    program.preassign_label_to_next_insn(label_loop_next_b);
    program.emit_insn(Insn::Next {
        cursor_id: queue_b_cursor,
        pc_if_next: label_inner_loop_b,
    });

    // After processing queue_b: clear it and read from A next
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: queue_b_cursor,
        is_table: false,
    });
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_flag,
    }); // next iteration reads from A
    program.emit_insn(Insn::Goto {
        target_pc: label_next_iteration,
    });

    // Skip insert B
    program.preassign_label_to_next_insn(label_skip_insert_b);
    program.emit_insn(Insn::Goto {
        target_pc: label_loop_next_b,
    });

    // === Next iteration ===
    program.preassign_label_to_next_insn(label_next_iteration);
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: reg_record,
    });
    program.emit_insn(Insn::Add {
        lhs: reg_iteration,
        rhs: reg_record,
        dest: reg_iteration,
    });
    program.emit_insn(Insn::Goto {
        target_pc: label_loop_start,
    });

    // === Output phase: read from result table ===
    program.preassign_label_to_next_insn(label_output_start);
    program.emit_insn(Insn::Rewind {
        cursor_id: result_cursor,
        pc_if_empty: label_done,
    });

    // Allocate registers for output columns
    let num_output_cols = output_column_indices.len();
    let reg_output = program.alloc_registers(num_output_cols);

    program.preassign_label_to_next_insn(label_output_next);
    // Read only the columns we need for output
    for (out_idx, &col_idx) in output_column_indices.iter().enumerate() {
        program.emit_insn(Insn::Column {
            cursor_id: result_cursor,
            column: col_idx,
            dest: reg_output + out_idx,
            default: None,
        });
    }

    // Emit result row with only output columns
    program.emit_insn(Insn::ResultRow {
        start_reg: reg_output,
        count: num_output_cols,
    });

    program.emit_insn(Insn::Next {
        cursor_id: result_cursor,
        pc_if_next: label_output_next,
    });

    // Done
    program.preassign_label_to_next_insn(label_done);
    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });

    Ok(TranslateSelectResult {
        program,
        num_result_cols: num_output_cols,
    })
}

/// Emit VDBE instructions for base case rows
#[allow(clippy::too_many_arguments)]
fn emit_base_case_rows(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    base_case: &ast::OneSelect,
    result_cursor: usize,
    queue_cursor: usize,
    reg_row_data: usize,
    reg_record: usize,
    num_cols: usize,
    is_union_all: bool,
    label_skip_insert: BranchOffset,
    _connection: &Arc<crate::Connection>,
) -> Result<()> {
    match base_case {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            ..
        } => {
            // For simple base cases like "SELECT 1" or "SELECT 1, 0, 1"
            if from.is_none() && where_clause.is_none() {
                // Evaluate each column expression
                let t_ctx = TranslateCtx::new(program, resolver.schema, resolver.symbol_table, 0);
                for (i, col) in columns.iter().enumerate() {
                    match col {
                        ResultColumn::Expr(expr, _) => {
                            translate_expr(program, None, expr, reg_row_data + i, &t_ctx.resolver)?;
                        }
                        _ => crate::bail_parse_error!(
                            "Unsupported result column in recursive CTE base case"
                        ),
                    }
                }

                // Make record and insert into result
                program.emit_insn(Insn::MakeRecord {
                    start_reg: reg_row_data as u16,
                    count: num_cols as u16,
                    dest_reg: reg_record as u16,
                    index_name: None,
                    affinity_str: None,
                });

                // For UNION, check if row exists before inserting
                if !is_union_all {
                    program.emit_insn(Insn::Found {
                        cursor_id: result_cursor,
                        target_pc: label_skip_insert,
                        record_reg: reg_record,
                        num_regs: 0,
                    });
                }

                // Insert into result and queue
                program.emit_insn(Insn::IdxInsert {
                    cursor_id: result_cursor,
                    record_reg: reg_record,
                    flags: IdxInsertFlags::new(),
                    unpacked_start: None,
                    unpacked_count: None,
                });
                program.emit_insn(Insn::IdxInsert {
                    cursor_id: queue_cursor,
                    record_reg: reg_record,
                    flags: IdxInsertFlags::new(),
                    unpacked_start: None,
                    unpacked_count: None,
                });
            } else {
                // Complex base case with FROM clause - use the planning infrastructure
                // Construct an ast::Select from the ast::OneSelect
                let base_select = ast::Select {
                    with: None,
                    body: ast::SelectBody {
                        select: base_case.clone(),
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                };

                // Create the destination for the base case
                let base_destination = QueryDestination::RecursiveCte {
                    result_cursor,
                    queue_cursor,
                    num_cols,
                    is_union_all,
                };

                // Prepare and emit the base case plan
                let base_plan = prepare_select_plan(
                    base_select,
                    resolver,
                    program,
                    &[],
                    base_destination,
                    _connection,
                )?;

                emit_program(_connection, resolver, program, base_plan, |_| {})?;
            }
        }
        ast::OneSelect::Values(values_list) => {
            // Handle VALUES clause base case
            for values in values_list {
                let t_ctx = TranslateCtx::new(program, resolver.schema, resolver.symbol_table, 0);
                for (i, expr) in values.iter().enumerate() {
                    translate_expr(program, None, expr, reg_row_data + i, &t_ctx.resolver)?;
                }

                program.emit_insn(Insn::MakeRecord {
                    start_reg: reg_row_data as u16,
                    count: num_cols as u16,
                    dest_reg: reg_record as u16,
                    index_name: None,
                    affinity_str: None,
                });

                if !is_union_all {
                    program.emit_insn(Insn::Found {
                        cursor_id: result_cursor,
                        target_pc: label_skip_insert,
                        record_reg: reg_record,
                        num_regs: 0,
                    });
                }

                program.emit_insn(Insn::IdxInsert {
                    cursor_id: result_cursor,
                    record_reg: reg_record,
                    flags: IdxInsertFlags::new(),
                    unpacked_start: None,
                    unpacked_count: None,
                });
                program.emit_insn(Insn::IdxInsert {
                    cursor_id: queue_cursor,
                    record_reg: reg_record,
                    flags: IdxInsertFlags::new(),
                    unpacked_start: None,
                    unpacked_count: None,
                });
            }
        }
    }
    Ok(())
}

/// Emit VDBE instructions for recursive step
#[allow(clippy::too_many_arguments)]
fn emit_recursive_step_rows(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    recursive_step: &ast::OneSelect,
    cte_name: &str,
    result_cursor: usize,
    queue_read_cursor: usize,
    queue_write_cursor: usize,
    reg_row_data: usize,
    reg_record: usize,
    column_names: &[String],
    is_union_all: bool,
    label_next_row: BranchOffset,
    label_skip_insert: BranchOffset,
    _connection: &Arc<crate::Connection>,
) -> Result<()> {
    let num_cols = column_names.len();
    match recursive_step {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            ..
        } => {
            // Check that FROM clause references only the CTE (simple self-reference)
            if let Some(from_clause) = from {
                let is_simple_cte_ref = match from_clause.select.as_ref() {
                    ast::SelectTable::Table(qn, _, _) => {
                        normalize_ident(qn.name.as_str()) == cte_name
                    }
                    _ => false,
                };

                if !is_simple_cte_ref || !from_clause.joins.is_empty() {
                    // Complex recursive step with joins - use the planning infrastructure
                    // Create a RecursiveCteTable backed by the queue_read_cursor
                    let cte_columns: Vec<Column> = column_names
                        .iter()
                        .map(|name| {
                            Column::new(
                                Some(name.clone()),
                                String::new(),
                                None,
                                None,
                                crate::schema::Type::Null,
                                None,
                                ColDef::default(),
                            )
                        })
                        .collect();

                    let cte_table = RecursiveCteTable {
                        name: cte_name.to_string(),
                        columns: cte_columns,
                        cursor_id: queue_read_cursor,
                    };

                    // Create an OuterQueryReference for the CTE
                    let cte_outer_ref = OuterQueryReference {
                        identifier: cte_name.to_string(),
                        internal_id: program.table_reference_counter.next(),
                        table: Table::RecursiveCte(cte_table),
                        col_used_mask: crate::translate::plan::ColumnUsedMask::default(),
                    };

                    // Construct an ast::Select from the recursive step
                    let recursive_select = ast::Select {
                        with: None,
                        body: ast::SelectBody {
                            select: recursive_step.clone(),
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    };

                    // Create the destination for the recursive step
                    let recursive_destination = QueryDestination::RecursiveCte {
                        result_cursor,
                        queue_cursor: queue_write_cursor,
                        num_cols,
                        is_union_all,
                    };

                    // Prepare the recursive step plan with the CTE as an outer query reference
                    let recursive_plan = prepare_select_plan(
                        recursive_select,
                        resolver,
                        program,
                        &[cte_outer_ref],
                        recursive_destination,
                        _connection,
                    )?;

                    // Emit the recursive step program
                    emit_program(_connection, resolver, program, recursive_plan, |_| {})?;

                    return Ok(());
                }
            }

            // Simple self-referencing case (no JOINs) - use existing manual emission
            // Read current queue row values into a temporary area
            let reg_cte_row = program.alloc_registers(num_cols);
            for i in 0..num_cols {
                program.emit_insn(Insn::Column {
                    cursor_id: queue_read_cursor,
                    column: i,
                    dest: reg_cte_row + i,
                    default: None,
                });
            }

            // Now evaluate the recursive step expressions
            // We need to resolve column references to the CTE to point to reg_cte_row
            // For now, we'll use a simplified approach for expressions like "x+1"

            // Create a context that knows about the CTE columns
            let t_ctx = TranslateCtx::new(program, resolver.schema, resolver.symbol_table, 0);

            for (i, col) in columns.iter().enumerate() {
                match col {
                    ResultColumn::Expr(expr, _) => {
                        // Translate expression, substituting CTE column references
                        translate_cte_expr(
                            program,
                            expr,
                            reg_row_data + i,
                            &t_ctx.resolver,
                            cte_name,
                            reg_cte_row,
                            column_names,
                        )?;
                    }
                    _ => crate::bail_parse_error!("Unsupported result column in recursive step"),
                }
            }

            // Check WHERE clause if present
            if let Some(where_expr) = where_clause {
                let reg_cond = program.alloc_register();
                translate_cte_expr(
                    program,
                    where_expr,
                    reg_cond,
                    &t_ctx.resolver,
                    cte_name,
                    reg_cte_row,
                    column_names,
                )?;
                // If condition is false/null, skip this row
                program.emit_insn(Insn::IfNot {
                    reg: reg_cond,
                    target_pc: label_next_row,
                    jump_if_null: true,
                });
            }

            // Make record and insert
            program.emit_insn(Insn::MakeRecord {
                start_reg: reg_row_data as u16,
                count: num_cols as u16,
                dest_reg: reg_record as u16,
                index_name: None,
                affinity_str: None,
            });

            // For UNION, check if row exists
            if !is_union_all {
                program.emit_insn(Insn::Found {
                    cursor_id: result_cursor,
                    target_pc: label_skip_insert,
                    record_reg: reg_record,
                    num_regs: 0,
                });
            }

            // Insert into result and write queue
            program.emit_insn(Insn::IdxInsert {
                cursor_id: result_cursor,
                record_reg: reg_record,
                flags: IdxInsertFlags::new(),
                unpacked_start: None,
                unpacked_count: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: queue_write_cursor,
                record_reg: reg_record,
                flags: IdxInsertFlags::new(),
                unpacked_start: None,
                unpacked_count: None,
            });
        }
        _ => crate::bail_parse_error!("Unsupported recursive step type"),
    }
    Ok(())
}

/// Translate an expression, substituting CTE column references
fn translate_cte_expr(
    program: &mut ProgramBuilder,
    expr: &Expr,
    dest: usize,
    resolver: &Resolver,
    cte_name: &str,
    reg_cte_row: usize,
    column_names: &[String],
) -> Result<()> {
    match expr {
        Expr::Id(id) => {
            let col_name = normalize_ident(id.as_str());
            // Find column index by name
            if let Some(col_idx) = column_names
                .iter()
                .position(|n| normalize_ident(n) == col_name)
            {
                program.emit_insn(Insn::Copy {
                    src_reg: reg_cte_row + col_idx,
                    dst_reg: dest,
                    extra_amount: 0,
                });
            } else {
                crate::bail_parse_error!("Unknown column '{}' in recursive CTE", col_name);
            }
            Ok(())
        }
        Expr::Qualified(table, col) => {
            let table_name = normalize_ident(table.as_str());
            if table_name == cte_name {
                let col_name = normalize_ident(col.as_str());
                if let Some(col_idx) = column_names
                    .iter()
                    .position(|n| normalize_ident(n) == col_name)
                {
                    program.emit_insn(Insn::Copy {
                        src_reg: reg_cte_row + col_idx,
                        dst_reg: dest,
                        extra_amount: 0,
                    });
                } else {
                    crate::bail_parse_error!("Unknown column '{}' in recursive CTE", col_name);
                }
            }
            Ok(())
        }
        Expr::Binary(left, op, right) => {
            let reg_left = program.alloc_register();
            let reg_right = program.alloc_register();
            translate_cte_expr(
                program,
                left,
                reg_left,
                resolver,
                cte_name,
                reg_cte_row,
                column_names,
            )?;
            translate_cte_expr(
                program,
                right,
                reg_right,
                resolver,
                cte_name,
                reg_cte_row,
                column_names,
            )?;

            match op {
                ast::Operator::Add => {
                    program.emit_insn(Insn::Add {
                        lhs: reg_left,
                        rhs: reg_right,
                        dest,
                    });
                }
                ast::Operator::Subtract => {
                    program.emit_insn(Insn::Subtract {
                        lhs: reg_left,
                        rhs: reg_right,
                        dest,
                    });
                }
                ast::Operator::Multiply => {
                    program.emit_insn(Insn::Multiply {
                        lhs: reg_left,
                        rhs: reg_right,
                        dest,
                    });
                }
                ast::Operator::Less => {
                    // Comparison - result is 0 or 1
                    program.emit_insn(Insn::Integer { value: 0, dest });
                    let label_false = program.allocate_label();
                    program.emit_insn(Insn::Ge {
                        lhs: reg_left,
                        rhs: reg_right,
                        target_pc: label_false,
                        flags: CmpInsFlags::default(),
                        collation: None,
                    });
                    program.emit_insn(Insn::Integer { value: 1, dest });
                    program.preassign_label_to_next_insn(label_false);
                }
                _ => {
                    // Fallback to regular expression translation
                    translate_expr(program, None, expr, dest, resolver)?;
                }
            }
            Ok(())
        }
        Expr::Literal(lit) => {
            match lit {
                ast::Literal::Numeric(n) => {
                    if let Ok(v) = n.parse::<i64>() {
                        program.emit_insn(Insn::Integer { value: v, dest });
                    } else if let Ok(v) = n.parse::<f64>() {
                        program.emit_insn(Insn::Real { value: v, dest });
                    }
                }
                ast::Literal::String(s) => {
                    program.emit_insn(Insn::String8 {
                        value: s.clone(),
                        dest,
                    });
                }
                ast::Literal::Null => {
                    program.emit_insn(Insn::Null {
                        dest,
                        dest_end: None,
                    });
                }
                _ => {
                    translate_expr(program, None, expr, dest, resolver)?;
                }
            }
            Ok(())
        }
        _ => {
            // Fallback to regular expression translation for other cases
            translate_expr(program, None, expr, dest, resolver)?;
            Ok(())
        }
    }
}

pub fn prepare_select_plan(
    select: ast::Select,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    outer_query_refs: &[OuterQueryReference],
    query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<Plan> {
    let compounds = select.body.compounds;
    match compounds.is_empty() {
        true => Ok(Plan::Select(prepare_one_select_plan(
            select.body.select,
            resolver,
            program,
            select.limit,
            select.order_by,
            select.with,
            outer_query_refs,
            query_destination,
            connection,
        )?)),
        false => {
            // For compound SELECTs, the WITH clause applies to all parts.
            // We clone the WITH clause for each SELECT in the compound so that
            // each one can resolve CTE references independently.
            let with = select.with;

            let mut last = prepare_one_select_plan(
                select.body.select,
                resolver,
                program,
                None,
                vec![],
                with.clone(),
                outer_query_refs,
                query_destination.clone(),
                connection,
            )?;

            let mut left = Vec::with_capacity(compounds.len());
            for CompoundSelect {
                select: compound_select,
                operator,
            } in compounds
            {
                left.push((last, operator));
                last = prepare_one_select_plan(
                    compound_select,
                    resolver,
                    program,
                    None,
                    vec![],
                    with.clone(),
                    outer_query_refs,
                    query_destination.clone(),
                    connection,
                )?;
            }

            // Ensure all subplans have the same number of result columns
            let right_most_num_result_columns = last.result_columns.len();
            for (plan, operator) in left.iter() {
                if plan.result_columns.len() != right_most_num_result_columns {
                    crate::bail_parse_error!("SELECTs to the left and right of {} do not have the same number of result columns", operator);
                }
            }
            let (limit, offset) = select
                .limit
                .map_or(Ok((None, None)), |l| parse_limit(l, connection))?;

            // FIXME: handle ORDER BY for compound selects
            if !select.order_by.is_empty() {
                crate::bail_parse_error!("ORDER BY is not supported for compound SELECTs yet");
            }
            Ok(Plan::CompoundSelect {
                left,
                right_most: last,
                limit,
                offset,
                order_by: None,
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_one_select_plan(
    select: ast::OneSelect,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    limit: Option<ast::Limit>,
    order_by: Vec<ast::SortedColumn>,
    with: Option<ast::With>,
    outer_query_refs: &[OuterQueryReference],
    query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<SelectPlan> {
    if order_by
        .iter()
        .filter_map(|o| o.nulls)
        .any(|n| n == ast::NullsOrder::Last)
    {
        crate::bail_parse_error!("NULLS LAST is not supported yet in ORDER BY");
    }
    match select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            distinctness,
            window_clause,
        } => {
            let col_count = columns.len();
            if col_count == 0 {
                crate::bail_parse_error!("SELECT without columns is not allowed");
            }

            let mut where_predicates = vec![];
            let mut vtab_predicates = vec![];

            let mut table_references = TableReferences::new(vec![], outer_query_refs.to_vec());

            if from.is_none() {
                for column in &columns {
                    if matches!(column, ResultColumn::Star) {
                        crate::bail_parse_error!("no tables specified");
                    }
                }
            }

            // Parse the FROM clause into a vec of TableReferences. Fold all the join conditions expressions into the WHERE clause.
            parse_from(
                from,
                resolver,
                program,
                with,
                &mut where_predicates,
                &mut vtab_predicates,
                &mut table_references,
                connection,
            )?;

            // Preallocate space for the result columns
            let result_columns = Vec::with_capacity(
                columns
                    .iter()
                    .map(|c| match c {
                        // Allocate space for all columns in all tables
                        ResultColumn::Star => table_references
                            .joined_tables()
                            .iter()
                            .map(|t| t.columns().iter().filter(|col| !col.hidden()).count())
                            .sum(),
                        // Guess 5 columns if we can't find the table using the identifier (maybe it's in [brackets] or `tick_quotes`, or miXeDcAse)
                        ResultColumn::TableStar(n) => table_references
                            .joined_tables()
                            .iter()
                            .find(|t| t.identifier == n.as_str())
                            .map(|t| t.columns().iter().filter(|col| !col.hidden()).count())
                            .unwrap_or(5),
                        // Otherwise allocate space for 1 column
                        ResultColumn::Expr(_, _) => 1,
                    })
                    .sum(),
            );
            let mut plan = SelectPlan {
                join_order: table_references
                    .joined_tables()
                    .iter()
                    .enumerate()
                    .map(|(i, t)| JoinOrderMember {
                        table_id: t.internal_id,
                        original_idx: i,
                        is_outer: t.join_info.as_ref().is_some_and(|j| j.outer),
                    })
                    .collect(),
                table_references,
                result_columns,
                where_clause: where_predicates,
                group_by: None,
                order_by: vec![],
                aggregates: vec![],
                limit: None,
                offset: None,
                contains_constant_false_condition: false,
                query_destination,
                distinctness: Distinctness::from_ast(distinctness.as_ref()),
                values: vec![],
                window: None,
                non_from_clause_subqueries: vec![],
            };

            let mut windows = Vec::with_capacity(window_clause.len());
            for window_def in window_clause.iter() {
                let name = normalize_ident(window_def.name.as_str());
                let mut window = Window::new(Some(name), &window_def.window)?;

                for expr in window.partition_by.iter_mut() {
                    bind_and_rewrite_expr(
                        expr,
                        Some(&mut plan.table_references),
                        None,
                        connection,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                }
                for (expr, _) in window.order_by.iter_mut() {
                    bind_and_rewrite_expr(
                        expr,
                        Some(&mut plan.table_references),
                        None,
                        connection,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                }

                windows.push(window);
            }

            let mut aggregate_expressions = Vec::new();
            for column in columns.into_iter() {
                match column {
                    ResultColumn::Star => {
                        select_star(
                            plan.table_references.joined_tables(),
                            &mut plan.result_columns,
                        );
                        for table in plan.table_references.joined_tables_mut() {
                            for idx in 0..table.columns().len() {
                                let column = &table.columns()[idx];
                                if column.hidden() {
                                    continue;
                                }
                                table.mark_column_used(idx);
                            }
                        }
                    }
                    ResultColumn::TableStar(name) => {
                        let name_normalized = normalize_ident(name.as_str());
                        let referenced_table = plan
                            .table_references
                            .joined_tables_mut()
                            .iter_mut()
                            .find(|t| t.identifier == name_normalized);

                        if referenced_table.is_none() {
                            crate::bail_parse_error!("no such table: {}", name.as_str());
                        }
                        let table = referenced_table.unwrap();
                        let num_columns = table.columns().len();
                        for idx in 0..num_columns {
                            let column = &table.columns()[idx];
                            if column.hidden() {
                                continue;
                            }
                            plan.result_columns.push(ResultSetColumn {
                                expr: ast::Expr::Column {
                                    database: None, // TODO: support different databases
                                    table: table.internal_id,
                                    column: idx,
                                    is_rowid_alias: column.is_rowid_alias(),
                                },
                                alias: None,
                                contains_aggregates: false,
                            });
                            table.mark_column_used(idx);
                        }
                    }
                    ResultColumn::Expr(mut expr, maybe_alias) => {
                        bind_and_rewrite_expr(
                            &mut expr,
                            Some(&mut plan.table_references),
                            None,
                            connection,
                            BindingBehavior::ResultColumnsNotAllowed,
                        )?;
                        let contains_aggregates = resolve_window_and_aggregate_functions(
                            &expr,
                            resolver,
                            &mut aggregate_expressions,
                            Some(&mut windows),
                        )?;
                        plan.result_columns.push(ResultSetColumn {
                            alias: maybe_alias.as_ref().map(|alias| match alias {
                                ast::As::Elided(alias) => alias.as_str().to_string(),
                                ast::As::As(alias) => alias.as_str().to_string(),
                            }),
                            expr: *expr,
                            contains_aggregates,
                        });
                    }
                }
            }

            // This step can only be performed at this point, because all table references are now available.
            // Virtual table predicates may depend on column bindings from tables to the right in the join order,
            // so we must wait until the full set of references has been collected.
            add_vtab_predicates_to_where_clause(&mut vtab_predicates, &mut plan, connection)?;

            // Parse the actual WHERE clause and add its conditions to the plan WHERE clause that already contains the join conditions.
            parse_where(
                where_clause.as_deref(),
                &mut plan.table_references,
                Some(&plan.result_columns),
                &mut plan.where_clause,
                connection,
            )?;

            if let Some(mut group_by) = group_by {
                // Process HAVING clause if present
                let having_predicates = if let Some(having) = group_by.having {
                    Some(process_having_clause(
                        having,
                        &mut plan.table_references,
                        &plan.result_columns,
                        connection,
                        resolver,
                        &mut aggregate_expressions,
                    )?)
                } else {
                    None
                };

                if !group_by.exprs.is_empty() {
                    // Normal GROUP BY with expressions
                    for expr in group_by.exprs.iter_mut() {
                        replace_column_number_with_copy_of_column_expr(expr, &plan.result_columns)?;
                        bind_and_rewrite_expr(
                            expr,
                            Some(&mut plan.table_references),
                            Some(&plan.result_columns),
                            connection,
                            BindingBehavior::TryResultColumnsFirst,
                        )?;
                    }

                    plan.group_by = Some(GroupBy {
                        sort_order: None,
                        exprs: group_by.exprs.iter().map(|expr| *expr.clone()).collect(),
                        having: having_predicates,
                    });
                } else {
                    // HAVING without GROUP BY: treat as ungrouped aggregation with filter
                    plan.group_by = Some(GroupBy {
                        sort_order: None,
                        exprs: vec![],
                        having: having_predicates,
                    });
                }
            }

            plan.aggregates = aggregate_expressions;

            // HAVING without GROUP BY requires aggregates in the SELECT
            if let Some(ref group_by) = plan.group_by {
                if group_by.exprs.is_empty()
                    && group_by.having.is_some()
                    && plan.aggregates.is_empty()
                {
                    crate::bail_parse_error!("HAVING clause on a non-aggregate query");
                }
            }

            // Parse the ORDER BY clause
            let mut key = Vec::new();

            for mut o in order_by {
                replace_column_number_with_copy_of_column_expr(&mut o.expr, &plan.result_columns)?;

                bind_and_rewrite_expr(
                    &mut o.expr,
                    Some(&mut plan.table_references),
                    Some(&plan.result_columns),
                    connection,
                    BindingBehavior::TryResultColumnsFirst,
                )?;
                resolve_window_and_aggregate_functions(
                    &o.expr,
                    resolver,
                    &mut plan.aggregates,
                    Some(&mut windows),
                )?;

                key.push((o.expr, o.order.unwrap_or(ast::SortOrder::Asc)));
            }
            plan.order_by = key;

            // Single-row aggregate queries (aggregates without GROUP BY and without window functions)
            // produce exactly one row, so ORDER BY is meaningless. Clearing it here also avoids
            // eagerly validating subqueries in ORDER BY that SQLite would skip due to optimization.
            // Note: HAVING without GROUP BY sets group_by to Some with empty exprs, still single-row.
            let is_single_row_aggregate = !plan.aggregates.is_empty()
                && plan.group_by.as_ref().is_none_or(|gb| gb.exprs.is_empty())
                && windows.is_empty();
            if is_single_row_aggregate {
                plan.order_by.clear();
            }

            // SQLite optimizes away ORDER BY clauses after a rowid/INTEGER PRIMARY KEY column
            // when it's FIRST in the ORDER BY, since the table is stored in rowid order.
            // This means we truncate the ORDER BY to just the rowid column.
            // We do this for SQLite compatibility - SQLite truncates before validating, so
            // even invalid constructions like ORDER BY rowid, a IN (SELECT a, b FROM t) pass.
            if plan.order_by.len() > 1 && plan.table_references.joined_tables().len() == 1 {
                let joined = &plan.table_references.joined_tables()[0];
                let table_id = joined.internal_id;
                let rowid_alias_col = joined
                    .btree()
                    .and_then(|t| t.get_rowid_alias_column().map(|(idx, _)| idx));

                let first_is_rowid = match plan.order_by[0].0.as_ref() {
                    ast::Expr::Column { table, column, .. } => {
                        *table == table_id && rowid_alias_col == Some(*column)
                    }
                    ast::Expr::RowId { table, .. } => *table == table_id,
                    _ => false,
                };
                if first_is_rowid {
                    plan.order_by.truncate(1);
                }
            }

            if let Some(group_by) = &mut plan.group_by {
                // now that we have resolved the ORDER BY expressions and aggregates, we can
                // compute the necessary sort order for the GROUP BY clause
                group_by.sort_order = Some(compute_group_by_sort_order(
                    &group_by.exprs,
                    &plan.order_by,
                    &plan.aggregates,
                    resolver,
                ));
            }

            // Parse the LIMIT/OFFSET clause
            (plan.limit, plan.offset) =
                limit.map_or(Ok((None, None)), |l| parse_limit(l, connection))?;

            if !windows.is_empty() {
                plan_windows(
                    &mut plan,
                    resolver,
                    &mut program.table_reference_counter,
                    &mut windows,
                )?;
            }

            plan_subqueries_from_select_plan(program, &mut plan, resolver, connection)?;

            validate_expr_correct_column_counts(&plan)?;

            // Return the unoptimized query plan
            Ok(plan)
        }
        ast::OneSelect::Values(mut values) => {
            if !order_by.is_empty() {
                crate::bail_parse_error!("ORDER BY clause is not allowed with VALUES clause");
            }
            if limit.is_some() {
                crate::bail_parse_error!("LIMIT clause is not allowed with VALUES clause");
            }
            let len = values[0].len();
            let mut result_columns = Vec::with_capacity(len);
            for i in 0..len {
                result_columns.push(ResultSetColumn {
                    // these result_columns work as placeholders for the values, so the expr doesn't matter
                    expr: ast::Expr::Literal(ast::Literal::Numeric(i.to_string())),
                    alias: Some(format!("column{}", i + 1)),
                    contains_aggregates: false,
                });
            }

            let mut table_references = TableReferences::new(vec![], outer_query_refs.to_vec());

            // Plan CTEs from WITH clause so they're available for subqueries in VALUES
            plan_ctes_as_outer_refs(with, resolver, program, &mut table_references, connection)?;

            for value_row in values.iter_mut() {
                for value in value_row.iter_mut() {
                    // Before binding, we check for unquoted literals. Sqlite throws an error in this case
                    bind_and_rewrite_expr(
                        value,
                        Some(&mut table_references),
                        None,
                        connection,
                        // Allow sqlite quirk of inserting "double-quoted" literals (which our AST maps as identifiers)
                        BindingBehavior::TryResultColumnsFirst,
                    )?;
                }
            }

            // Plan subqueries in VALUES expressions
            let mut non_from_clause_subqueries = vec![];
            plan_subqueries_from_values(
                program,
                &mut non_from_clause_subqueries,
                &mut table_references,
                &mut values,
                resolver,
                connection,
            )?;

            let plan = SelectPlan {
                join_order: vec![],
                table_references,
                result_columns,
                where_clause: vec![],
                group_by: None,
                order_by: vec![],
                aggregates: vec![],
                limit: None,
                offset: None,
                contains_constant_false_condition: false,
                query_destination,
                distinctness: Distinctness::NonDistinct,
                values: values
                    .iter()
                    .map(|values| values.iter().map(|value| *value.clone()).collect())
                    .collect(),
                window: None,
                non_from_clause_subqueries,
            };

            validate_expr_correct_column_counts(&plan)?;

            Ok(plan)
        }
    }
}

/// Validate that all expressions in the plan return the correct number of values;
/// generally this only applies to parenthesized lists and subqueries.
fn validate_expr_correct_column_counts(plan: &SelectPlan) -> Result<()> {
    for result_column in plan.result_columns.iter() {
        let vec_size = expr_vector_size(&result_column.expr)?;
        if vec_size != 1 {
            crate::bail_parse_error!("result column must return 1 value, got {}", vec_size);
        }
    }
    for (expr, _) in plan.order_by.iter() {
        let vec_size = expr_vector_size(expr)?;
        if vec_size != 1 {
            crate::bail_parse_error!("order by expression must return 1 value, got {}", vec_size);
        }
    }
    if let Some(group_by) = &plan.group_by {
        for expr in group_by.exprs.iter() {
            let vec_size = expr_vector_size(expr)?;
            if vec_size != 1 {
                crate::bail_parse_error!(
                    "group by expression must return 1 value, got {}",
                    vec_size
                );
            }
        }
        if let Some(having) = &group_by.having {
            for expr in having.iter() {
                let vec_size = expr_vector_size(expr)?;
                if vec_size != 1 {
                    crate::bail_parse_error!(
                        "having expression must return 1 value, got {}",
                        vec_size
                    );
                }
            }
        }
    }
    for aggregate in plan.aggregates.iter() {
        for arg in aggregate.args.iter() {
            let vec_size = expr_vector_size(arg)?;
            if vec_size != 1 {
                crate::bail_parse_error!(
                    "aggregate argument must return 1 value, got {}",
                    vec_size
                );
            }
        }
    }
    for term in plan.where_clause.iter() {
        let vec_size = expr_vector_size(&term.expr)?;
        if vec_size != 1 {
            crate::bail_parse_error!(
                "where clause expression must return 1 value, got {}",
                vec_size
            );
        }
    }
    for expr in plan.values.iter() {
        for value in expr.iter() {
            let vec_size = expr_vector_size(value)?;
            if vec_size != 1 {
                crate::bail_parse_error!("value must return 1 value, got {}", vec_size);
            }
        }
    }
    if let Some(limit) = &plan.limit {
        let vec_size = expr_vector_size(limit)?;
        if vec_size != 1 {
            crate::bail_parse_error!("limit expression must return 1 value, got {}", vec_size);
        }
    }
    if let Some(offset) = &plan.offset {
        let vec_size = expr_vector_size(offset)?;
        if vec_size != 1 {
            crate::bail_parse_error!("offset expression must return 1 value, got {}", vec_size);
        }
    }
    Ok(())
}

fn add_vtab_predicates_to_where_clause(
    vtab_predicates: &mut Vec<Expr>,
    plan: &mut SelectPlan,
    connection: &Arc<Connection>,
) -> Result<()> {
    for expr in vtab_predicates.iter_mut() {
        bind_and_rewrite_expr(
            expr,
            Some(&mut plan.table_references),
            Some(&plan.result_columns),
            connection,
            BindingBehavior::TryCanonicalColumnsFirst,
        )?;
    }
    for expr in vtab_predicates.drain(..) {
        plan.where_clause.push(WhereTerm {
            expr,
            from_outer_join: None,
            consumed: false,
        });
    }
    Ok(())
}

/// Replaces a column number in an ORDER BY or GROUP BY expression with a copy of the column expression.
/// For example, in SELECT u.first_name, count(1) FROM users u GROUP BY 1 ORDER BY 2,
/// the column number 1 is replaced with u.first_name and the column number 2 is replaced with count(1).
///
/// Per SQLite documentation, only constant integers are treated as column references.
/// Non-integer numeric literals (floats) are treated as constant expressions.
fn replace_column_number_with_copy_of_column_expr(
    order_by_or_group_by_expr: &mut ast::Expr,
    columns: &[ResultSetColumn],
) -> Result<()> {
    if let ast::Expr::Literal(ast::Literal::Numeric(num)) = order_by_or_group_by_expr {
        // Only treat as column reference if it parses as a positive integer.
        // Float literals like "0.5" or "1.0" are valid constant expressions, not column references.
        if let Ok(column_number) = num.parse::<usize>() {
            if column_number == 0 {
                crate::bail_parse_error!("invalid column index: {}", column_number);
            }
            let maybe_result_column = columns.get(column_number - 1);
            match maybe_result_column {
                Some(ResultSetColumn { expr, .. }) => {
                    *order_by_or_group_by_expr = expr.clone();
                }
                None => {
                    crate::bail_parse_error!("invalid column index: {}", column_number)
                }
            };
        }
        // Otherwise, leave the expression as-is (constant expression, case 3 per SQLite docs)
    }
    Ok(())
}

/// Count required cursors for a Plan (either Select or CompoundSelect)
fn count_required_cursors_for_simple_or_compound_select(plan: &Plan) -> usize {
    match plan {
        Plan::Select(select_plan) => count_required_cursors_for_simple_select(select_plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            count_required_cursors_for_simple_select(right_most)
                + left
                    .iter()
                    .map(|(p, _)| count_required_cursors_for_simple_select(p))
                    .sum::<usize>()
        }
        Plan::Delete(_) | Plan::Update(_) => 0,
    }
}

fn count_required_cursors_for_simple_select(plan: &SelectPlan) -> usize {
    let num_table_cursors: usize = plan
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 1,
            Operation::Search(search) => match search {
                Search::RowidEq { .. } => 1,
                Search::Seek { index, .. } => 1 + index.is_some() as usize,
            }
            Operation::IndexMethodQuery(_) => 1,
            Operation::HashJoin(_) => 2,
            // One table cursor + one cursor per index branch
            Operation::MultiIndexScan(multi_idx) => 1 + multi_idx.branches.len(),
        } + if let Table::FromClauseSubquery(from_clause_subquery) = &t.table {
            count_required_cursors_for_simple_or_compound_select(&from_clause_subquery.plan)
        } else {
            0
        })
        .sum();
    let has_group_by_with_exprs = plan
        .group_by
        .as_ref()
        .is_some_and(|gb| !gb.exprs.is_empty());
    let num_sorter_cursors = has_group_by_with_exprs as usize + !plan.order_by.is_empty() as usize;
    let num_pseudo_cursors = has_group_by_with_exprs as usize + !plan.order_by.is_empty() as usize;

    num_table_cursors + num_sorter_cursors + num_pseudo_cursors
}

/// Estimate number of instructions for a Plan (either Select or CompoundSelect)
fn estimate_num_instructions_for_simple_or_compound_select(plan: &Plan) -> usize {
    match plan {
        Plan::Select(select_plan) => estimate_num_instructions_for_simple_select(select_plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            estimate_num_instructions_for_simple_select(right_most)
                + left
                    .iter()
                    .map(|(p, _)| estimate_num_instructions_for_simple_select(p))
                    .sum::<usize>()
                + 20 // overhead for compound select operations
        }
        Plan::Delete(_) | Plan::Update(_) => 0,
    }
}

fn estimate_num_instructions_for_simple_select(select: &SelectPlan) -> usize {
    let table_instructions: usize = select
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 10,
            Operation::Search(_) => 15,
            Operation::IndexMethodQuery(_) => 15,
            Operation::HashJoin(_) => 20,
            // Multi-index scan: scan overhead per branch + deduplication + final rowid fetch
            Operation::MultiIndexScan(multi_idx) => 15 * multi_idx.branches.len() + 10,
        } + if let Table::FromClauseSubquery(from_clause_subquery) = &t.table {
            10 + estimate_num_instructions_for_simple_or_compound_select(&from_clause_subquery.plan)
        } else {
            0
        })
        .sum();

    let group_by_instructions = select.group_by.is_some() as usize * 10;
    let order_by_instructions = !select.order_by.is_empty() as usize * 10;
    let condition_instructions = select.where_clause.len() * 3;

    20 + table_instructions + group_by_instructions + order_by_instructions + condition_instructions
}

/// Estimate number of labels for a Plan (either Select or CompoundSelect)
fn estimate_num_labels_for_simple_or_compound_select(plan: &Plan) -> usize {
    match plan {
        Plan::Select(select_plan) => estimate_num_labels_for_simple_select(select_plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            estimate_num_labels_for_simple_select(right_most)
                + left
                    .iter()
                    .map(|(p, _)| estimate_num_labels_for_simple_select(p))
                    .sum::<usize>()
                + 10 // overhead for compound select operations
        }
        Plan::Delete(_) | Plan::Update(_) => 0,
    }
}

fn estimate_num_labels_for_simple_select(select: &SelectPlan) -> usize {
    let init_halt_labels = 2;
    // 3 loop labels for each table in main loop + 1 to signify end of main loop
    let table_labels = select
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 3,
            Operation::Search(_) => 3,
            Operation::IndexMethodQuery(_) => 3,
            Operation::HashJoin(_) => 3,
            // Multi-index scan needs extra labels for each branch + rowset loop
            Operation::MultiIndexScan(multi_idx) => 3 + multi_idx.branches.len() * 2,
        } + if let Table::FromClauseSubquery(from_clause_subquery) = &t.table {
            3 + estimate_num_labels_for_simple_or_compound_select(&from_clause_subquery.plan)
        } else {
            0
        })
        .sum::<usize>()
        + 1;

    let group_by_labels = select.group_by.is_some() as usize * 10;
    let order_by_labels = !select.order_by.is_empty() as usize * 10;
    let condition_labels = select.where_clause.len() * 2;

    init_halt_labels + table_labels + group_by_labels + order_by_labels + condition_labels
}

pub fn emit_simple_count(
    program: &mut ProgramBuilder,
    _t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let cursors = plan
        .joined_tables()
        .first()
        .unwrap()
        .resolve_cursors(program, OperationMode::SELECT)?;

    let cursor_id = {
        match cursors {
            (_, Some(cursor_id)) | (Some(cursor_id), None) => cursor_id,
            _ => panic!("cursor for table should have been opened"),
        }
    };

    // TODO: I think this allocation can be avoided if we are smart with the `TranslateCtx`
    let target_reg = program.alloc_register();

    program.emit_insn(Insn::Count {
        cursor_id,
        target_reg,
        exact: true,
    });

    program.emit_insn(Insn::Close { cursor_id });
    let output_reg = program.alloc_register();
    program.emit_insn(Insn::Copy {
        src_reg: target_reg,
        dst_reg: output_reg,
        extra_amount: 0,
    });
    program.emit_result_row(output_reg, 1);
    Ok(())
}

fn process_having_clause(
    having: Box<ast::Expr>,
    table_references: &mut TableReferences,
    result_columns: &[ResultSetColumn],
    connection: &Arc<Connection>,
    resolver: &Resolver,
    aggregate_expressions: &mut Vec<super::plan::Aggregate>,
) -> Result<Vec<ast::Expr>> {
    let mut predicates = vec![];
    break_predicate_at_and_boundaries(&having, &mut predicates);

    for expr in predicates.iter_mut() {
        bind_and_rewrite_expr(
            expr,
            Some(table_references),
            Some(result_columns),
            connection,
            BindingBehavior::TryResultColumnsFirst,
        )?;
        resolve_window_and_aggregate_functions(expr, resolver, aggregate_expressions, None)?;
    }

    Ok(predicates)
}
