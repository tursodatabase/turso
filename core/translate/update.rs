use crate::sync::Arc;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::schema::{BTreeTable, ROWID_SENTINEL};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    bind_and_rewrite_expr, translate_expr_no_constant_opt, BindingBehavior, NoConstantOptReason,
};
use crate::translate::expression_index::expression_index_column_usage;
use crate::translate::plan::{Operation, QueryDestination, Scan};
use crate::translate::planner::{parse_limit, ROWID_STRS};
use crate::translate::select::translate_select;
use crate::translate::trigger_exec::{
    fire_trigger, get_relevant_triggers_type_and_time, TriggerContext,
};
use crate::{
    bail_parse_error,
    schema::{Schema, Table},
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder, ProgramBuilderOpts},
        insn::{to_u16, InsertFlags, Insn},
    },
    CaptureDataChangesExt, Connection,
};
use turso_parser::ast::{self, Expr, Indexed, SortOrder};

use super::emitter::emit_program;
use super::expr::process_returning_clause;
use super::optimizer::optimize_plan;
use super::plan::{
    ColumnUsedMask, DmlSafety, EvalAt, IterationDirection, JoinOrderMember, JoinedTable, Plan,
    TableReferences, UpdatePlan,
};
use super::planner::{parse_where, plan_ctes_as_outer_refs};
use super::subquery::{
    emit_non_from_clause_subquery, plan_subqueries_from_returning,
    plan_subqueries_from_select_plan, plan_subqueries_from_set_clauses,
    plan_subqueries_from_where_clause,
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
    let database_id = resolver.resolve_database_id(&body.tbl_name)?;
    let (table, view) = resolver.with_schema(database_id, |s| {
        (
            s.get_table(body.tbl_name.name.as_str()),
            s.get_view(body.tbl_name.name.as_str()),
        )
    });
    if table.is_none() {
        if let Some(view) = view {
            let mut resolver = resolver.fork();
            return translate_update_view(
                body,
                &mut resolver,
                program,
                connection,
                database_id,
                view,
            );
        }
    }

    let mut plan = prepare_update_plan(program, resolver, body, connection, false)?;

    // Plan subqueries in the WHERE clause and SET clause
    if let Plan::Update(ref mut update_plan) = plan {
        if let Some(ref mut ephemeral_plan) = update_plan.ephemeral_plan {
            // When using ephemeral plan (key columns are being updated), subqueries are in the ephemeral_plan's WHERE
            plan_subqueries_from_select_plan(program, ephemeral_plan, resolver, connection)?;
        } else {
            // Normal path: subqueries are in the UPDATE plan's WHERE
            plan_subqueries_from_where_clause(
                program,
                &mut update_plan.non_from_clause_subqueries,
                &mut update_plan.table_references,
                &mut update_plan.where_clause,
                resolver,
                connection,
            )?;
        }
        // Plan subqueries in the SET clause (e.g. UPDATE t SET col = (SELECT ...))
        plan_subqueries_from_set_clauses(
            program,
            &mut update_plan.non_from_clause_subqueries,
            &mut update_plan.table_references,
            &mut update_plan.set_clauses,
            resolver,
            connection,
        )?;
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

fn translate_update_view(
    mut body: ast::Update,
    resolver: &mut Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    view: Arc<crate::schema::View>,
) -> crate::Result<()> {
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
    if !body.returning.is_empty() {
        bail_parse_error!("RETURNING is not supported for UPDATE on views");
    }

    let table_name = normalize_ident(body.tbl_name.name.as_str());
    let view_table = Arc::new(BTreeTable {
        root_page: 0,
        name: table_name.clone(),
        primary_key_columns: vec![],
        columns: view.columns.clone(),
        has_rowid: false,
        is_strict: false,
        has_autoincrement: false,
        unique_sets: vec![],
        foreign_keys: vec![],
        check_constraints: vec![],
        pk_conflict_clause: None,
    });

    let column_lookup: HashMap<String, usize> = view_table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(i, col)| col.name.as_ref().map(|name| (normalize_ident(name), i)))
        .collect();

    let mut set_clauses: Vec<(usize, Box<Expr>)> = Vec::with_capacity(body.sets.len());
    for set in &mut body.sets {
        let values = match set.expr.as_ref() {
            Expr::Parenthesized(vals) => vals.clone(),
            expr => vec![expr.clone().into()],
        };

        if set.col_names.len() != values.len() {
            bail_parse_error!(
                "{} columns assigned {} values",
                set.col_names.len(),
                values.len()
            );
        }

        for (col_name, expr) in set.col_names.iter().zip(values.iter()) {
            let ident = normalize_ident(col_name.as_str());

            let col_index = match column_lookup.get(&ident) {
                Some(idx) => *idx,
                None => {
                    if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&ident)) {
                        ROWID_SENTINEL
                    } else {
                        bail_parse_error!("no such column: {}.{}", table_name, col_name);
                    }
                }
            };

            match set_clauses.iter_mut().find(|(idx, _)| *idx == col_index) {
                Some((_, existing_expr)) => {
                    if let Expr::FunctionCall {
                        name,
                        args: new_args,
                        ..
                    } = expr.as_ref()
                    {
                        if name.as_str().eq_ignore_ascii_case("array_set_element")
                            && new_args.len() == 3
                        {
                            let mut composed_args = new_args.clone();
                            composed_args[0].clone_from(existing_expr);
                            *existing_expr = Box::new(Expr::FunctionCall {
                                name: name.clone(),
                                distinctness: None,
                                args: composed_args,
                                order_by: vec![],
                                filter_over: turso_parser::ast::FunctionTail {
                                    filter_clause: None,
                                    over_clause: None,
                                },
                            });
                        } else {
                            existing_expr.clone_from(expr);
                        }
                    } else {
                        existing_expr.clone_from(expr);
                    }
                }
                None => set_clauses.push((col_index, expr.clone())),
            }
        }
    }

    let updated_column_indices: HashSet<usize> = set_clauses
        .iter()
        .filter_map(|(idx, _)| (*idx != ROWID_SENTINEL).then_some(*idx))
        .collect();
    let relevant_instead_update_triggers: Vec<_> = resolver.with_schema(database_id, |s| {
        get_relevant_triggers_type_and_time(
            s,
            ast::TriggerEvent::Update,
            ast::TriggerTime::InsteadOf,
            Some(updated_column_indices.clone()),
            &view_table,
        )
        .collect()
    });
    if relevant_instead_update_triggers.is_empty() {
        bail_parse_error!("cannot modify {} because it is a view", table_name);
    }

    let mut table_references = TableReferences::new(
        vec![JoinedTable {
            table: Table::BTree(view_table.clone()),
            identifier: body
                .tbl_name
                .alias
                .as_ref()
                .map_or_else(|| table_name.clone(), |alias| alias.as_str().to_string()),
            internal_id: program.table_reference_counter.next(),
            op: Operation::default_scan_for(&Table::BTree(view_table.clone())),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id,
        }],
        vec![],
    );
    plan_ctes_as_outer_refs(
        body.with.clone(),
        resolver,
        program,
        &mut table_references,
        connection,
    )?;
    for (_idx, expr) in &mut set_clauses {
        bind_and_rewrite_expr(
            expr,
            Some(&mut table_references),
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
    }
    let mut set_subqueries = vec![];
    plan_subqueries_from_set_clauses(
        program,
        &mut set_subqueries,
        &mut table_references,
        &mut set_clauses,
        resolver,
        connection,
    )?;
    let join_order = vec![JoinOrderMember {
        table_id: table_references
            .joined_tables()
            .first()
            .expect("view UPDATE table refs must contain one table")
            .internal_id,
        original_idx: 0,
        is_outer: false,
    }];
    for subquery in set_subqueries
        .iter_mut()
        .filter(|s| !s.has_been_evaluated())
    {
        let eval_at = subquery.get_eval_at(&join_order, Some(&table_references))?;
        match eval_at {
            EvalAt::BeforeLoop => {
                let subquery_plan = subquery.consume_plan(EvalAt::BeforeLoop);
                emit_non_from_clause_subquery(
                    program,
                    resolver,
                    *subquery_plan,
                    &subquery.query_type,
                    subquery.correlated,
                )?;
            }
            EvalAt::Loop(0) => {
                bail_parse_error!("correlated subqueries in UPDATE SET on views are not supported");
            }
            EvalAt::Loop(_) => {
                return Err(crate::LimboError::InternalError(
                    "unexpected evaluation loop for UPDATE SET view subquery".into(),
                ));
            }
        }
    }

    let mut source_columns = Vec::with_capacity(view_table.columns.len());
    for col in &view_table.columns {
        let col_name = col
            .name
            .as_ref()
            .ok_or_else(|| crate::LimboError::InternalError("view column missing name".into()))?;
        source_columns.push(ast::ResultColumn::Expr(
            Box::new(ast::Expr::Id(ast::Name::from_string(col_name))),
            None,
        ));
    }
    let source_select = ast::Select {
        with: body.with.take(),
        body: ast::SelectBody {
            select: ast::OneSelect::Select {
                distinctness: None,
                columns: source_columns,
                from: Some(ast::FromClause {
                    select: Box::new(ast::SelectTable::Table(
                        ast::QualifiedName {
                            db_name: body.tbl_name.db_name.clone(),
                            name: body.tbl_name.name.clone(),
                            alias: None,
                        },
                        body.tbl_name.alias.clone().map(ast::As::As),
                        None,
                    )),
                    joins: vec![],
                }),
                where_clause: body.where_clause.take(),
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
        },
        order_by: body.order_by,
        limit: body.limit.take(),
    };

    let yield_reg = program.alloc_register();
    let jump_on_definition_label = program.allocate_label();
    let start_offset_label = program.allocate_label();
    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: jump_on_definition_label,
        start_offset: start_offset_label,
    });
    program.preassign_label_to_next_insn(start_offset_label);

    let coroutine_end = program.allocate_label();
    let query_destination = QueryDestination::CoroutineYield {
        yield_reg,
        coroutine_implementation_start: coroutine_end,
    };
    let num_result_cols = program.nested(|program| {
        translate_select(
            source_select.clone(),
            resolver,
            program,
            query_destination,
            connection,
        )
    })?;
    let expected_result_cols = view_table.columns.len();
    if num_result_cols != expected_result_cols {
        return Err(crate::LimboError::InternalError(format!(
            "unexpected number of columns for UPDATE view trigger source: expected {expected_result_cols}, got {num_result_cols}"
        )));
    }
    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(jump_on_definition_label);

    let temp_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(view_table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: temp_cursor_id,
        is_table: true,
    });

    let fill_loop = program.allocate_label();
    let fill_done = program.allocate_label();
    program.preassign_label_to_next_insn(fill_loop);
    program.emit_insn(Insn::Yield {
        yield_reg,
        end_offset: fill_done,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(program.reg_result_cols_start.unwrap_or(yield_reg + 1)),
        count: to_u16(num_result_cols),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });
    let temp_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: temp_cursor_id,
        rowid_reg: temp_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: temp_cursor_id,
        key_reg: temp_rowid_reg,
        record_reg,
        flag: InsertFlags::new().require_seek(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Goto {
        target_pc: fill_loop,
    });
    program.preassign_label_to_next_insn(fill_done);

    let loop_start = program.allocate_label();
    let loop_end = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: temp_cursor_id,
        pc_if_empty: loop_end,
    });
    program.preassign_label_to_next_insn(loop_start);

    let row_regs_start = program.alloc_registers(num_result_cols);
    for i in 0..num_result_cols {
        program.emit_insn(Insn::Column {
            cursor_id: temp_cursor_id,
            column: i,
            dest: row_regs_start + i,
            default: None,
        });
    }

    let old_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: temp_cursor_id,
        dest: old_rowid_reg,
    });
    let old_registers: Vec<usize> = (0..view_table.columns.len())
        .map(|i| row_regs_start + i)
        .chain(std::iter::once(old_rowid_reg))
        .collect();
    let new_cols_start = program.alloc_registers(view_table.columns.len());
    let new_rowid_reg = program.alloc_register();
    let table_ref = table_references
        .joined_tables()
        .first()
        .expect("view UPDATE table refs must contain one table");

    resolver.enable_expr_to_reg_cache();
    let cache_len = resolver.expr_to_reg_cache.len();
    resolver.expr_to_reg_cache.push((
        std::borrow::Cow::Owned(ast::Expr::RowId {
            database: None,
            table: table_ref.internal_id,
        }),
        old_rowid_reg,
        false,
    ));
    for i in 0..view_table.columns.len() {
        resolver.expr_to_reg_cache.push((
            std::borrow::Cow::Owned(ast::Expr::Column {
                database: None,
                table: table_ref.internal_id,
                column: i,
                is_rowid_alias: false,
            }),
            row_regs_start + i,
            false,
        ));
    }

    for idx in 0..view_table.columns.len() {
        if let Some((_, expr)) = set_clauses.iter().find(|(col_idx, _)| *col_idx == idx) {
            translate_expr_no_constant_opt(
                program,
                Some(&table_references),
                expr,
                new_cols_start + idx,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        } else {
            program.emit_insn(Insn::Copy {
                src_reg: row_regs_start + idx,
                dst_reg: new_cols_start + idx,
                extra_amount: 0,
            });
        }
    }
    if let Some((_, expr)) = set_clauses.iter().find(|(idx, _)| *idx == ROWID_SENTINEL) {
        translate_expr_no_constant_opt(
            program,
            Some(&table_references),
            expr,
            new_rowid_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: old_rowid_reg,
            dst_reg: new_rowid_reg,
            extra_amount: 0,
        });
    }
    resolver.expr_to_reg_cache.truncate(cache_len);

    let new_registers: Vec<usize> = (0..view_table.columns.len())
        .map(|i| new_cols_start + i)
        .chain(std::iter::once(new_rowid_reg))
        .collect();
    let trigger_ctx = if let Some(override_conflict) = program.trigger_conflict_override {
        TriggerContext::new_with_override_conflict(
            view_table,
            Some(new_registers),
            Some(old_registers),
            override_conflict,
        )
    } else if matches!(body.or_conflict, Some(ast::ResolveType::Ignore)) {
        TriggerContext::new_with_override_conflict(
            view_table,
            Some(new_registers),
            Some(old_registers),
            ast::ResolveType::Ignore,
        )
    } else {
        TriggerContext::new(view_table, Some(new_registers), Some(old_registers))
    };

    let row_done = program.allocate_label();
    for trigger in relevant_instead_update_triggers {
        fire_trigger(
            program,
            resolver,
            trigger,
            &trigger_ctx,
            connection,
            database_id,
            row_done,
        )?;
    }
    program.preassign_label_to_next_insn(row_done);

    program.emit_insn(Insn::Next {
        cursor_id: temp_cursor_id,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(loop_end);
    program.emit_insn(Insn::Close {
        cursor_id: temp_cursor_id,
    });
    program.preassign_label_to_next_insn(coroutine_end);

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
    let mut plan = prepare_update_plan(program, resolver, body, connection, true)?;

    if let Plan::Update(update_plan) = &mut plan {
        if program.capture_data_changes_info().has_updates() {
            update_plan.cdc_update_alter_statement = Some(ddl_query.to_string());
        }

        // Plan subqueries in the WHERE clause
        if let Some(ref mut ephemeral_plan) = update_plan.ephemeral_plan {
            plan_subqueries_from_select_plan(program, ephemeral_plan, resolver, connection)?;
        } else {
            plan_subqueries_from_where_clause(
                program,
                &mut update_plan.non_from_clause_subqueries,
                &mut update_plan.table_references,
                &mut update_plan.where_clause,
                resolver,
                connection,
            )?;
        }
        // Plan subqueries in the SET clause (e.g. UPDATE t SET col = (SELECT ...))
        plan_subqueries_from_set_clauses(
            program,
            &mut update_plan.non_from_clause_subqueries,
            &mut update_plan.table_references,
            &mut update_plan.set_clauses,
            resolver,
            connection,
        )?;
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

pub fn prepare_update_plan(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut body: ast::Update,
    connection: &Arc<crate::Connection>,
    is_internal_schema_change: bool,
) -> crate::Result<Plan> {
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

    // Extract WITH and OR conflict clause before borrowing body mutably
    let with = body.with.take();
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

    let joined_tables = vec![JoinedTable {
        table: match table.as_ref() {
            Table::Virtual(vtab) => Table::Virtual(vtab.clone()),
            Table::BTree(btree_table) => Table::BTree(btree_table.clone()),
            _ => unreachable!(),
        },
        identifier: body.tbl_name.alias.as_ref().map_or_else(
            || table_name.to_string(),
            |alias| alias.as_str().to_string(),
        ),
        internal_id: program.table_reference_counter.next(),
        op: build_scan_op(&table, iter_dir),
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id,
    }];
    let mut table_references = TableReferences::new(joined_tables, vec![]);

    // Plan CTEs and add them as outer query references for subquery resolution
    plan_ctes_as_outer_refs(with, resolver, program, &mut table_references, connection)?;

    let column_lookup: HashMap<String, usize> = table
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, col)| col.name.as_ref().map(|name| (name.to_lowercase(), i)))
        .collect();

    let mut set_clauses: Vec<(usize, Box<Expr>)> = Vec::with_capacity(body.sets.len());

    // Process each SET assignment and map column names to expressions
    // e.g the statement `SET x = 1, y = 2, z = 3` has 3 set assigments
    for set in &mut body.sets {
        bind_and_rewrite_expr(
            &mut set.expr,
            Some(&mut table_references),
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;

        let values = match set.expr.as_ref() {
            Expr::Parenthesized(vals) => vals.clone(),
            expr => vec![expr.clone().into()],
        };

        if set.col_names.len() != values.len() {
            bail_parse_error!(
                "{} columns assigned {} values",
                set.col_names.len(),
                values.len()
            );
        }

        for (col_name, expr) in set.col_names.iter().zip(values.iter()) {
            let ident = normalize_ident(col_name.as_str());

            let col_index = match column_lookup.get(&ident) {
                Some(idx) => *idx,
                None => {
                    // Check if this is the 'rowid' keyword
                    if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&ident)) {
                        // Find the rowid alias column if it exists
                        if let Some((idx, _col)) = table
                            .columns()
                            .iter()
                            .enumerate()
                            .find(|(_i, c)| c.is_rowid_alias())
                        {
                            // Use the rowid alias column index
                            match set_clauses.iter_mut().find(|(i, _)| i == &idx) {
                                Some((_, existing_expr)) => existing_expr.clone_from(expr),
                                None => set_clauses.push((idx, expr.clone())),
                            }
                            idx
                        } else {
                            // No rowid alias, use sentinel value for actual rowid
                            match set_clauses.iter_mut().find(|(i, _)| *i == ROWID_SENTINEL) {
                                Some((_, existing_expr)) => existing_expr.clone_from(expr),
                                None => set_clauses.push((ROWID_SENTINEL, expr.clone())),
                            }
                            ROWID_SENTINEL
                        }
                    } else {
                        crate::bail_parse_error!("no such column: {}.{}", table_name, col_name);
                    }
                }
            };
            match set_clauses.iter_mut().find(|(idx, _)| *idx == col_index) {
                Some((_, existing_expr)) => {
                    // When multiple SET col[n] = val for the same column are desugared,
                    // compose them: replace the column reference in the new expression
                    // with the existing expression, so
                    //   col = array_set_element(col, 0, 'X')  then  col = array_set_element(col, 2, 'Z')
                    // becomes col = array_set_element(array_set_element(col, 0, 'X'), 2, 'Z')
                    if let Expr::FunctionCall {
                        name,
                        args: new_args,
                        ..
                    } = expr.as_ref()
                    {
                        if name.as_str().eq_ignore_ascii_case("array_set_element")
                            && new_args.len() == 3
                        {
                            let mut composed_args = new_args.clone();
                            composed_args[0].clone_from(existing_expr);
                            *existing_expr = Box::new(Expr::FunctionCall {
                                name: name.clone(),
                                distinctness: None,
                                args: composed_args,
                                order_by: vec![],
                                filter_over: turso_parser::ast::FunctionTail {
                                    filter_clause: None,
                                    over_clause: None,
                                },
                            });
                        } else {
                            existing_expr.clone_from(expr);
                        }
                    } else {
                        existing_expr.clone_from(expr);
                    }
                }
                None => set_clauses.push((col_index, expr.clone())),
            }
        }
    }

    // Plan subqueries in RETURNING expressions before processing
    // (so SubqueryResult nodes are cloned into result_columns)
    let mut non_from_clause_subqueries = vec![];
    plan_subqueries_from_returning(
        program,
        &mut non_from_clause_subqueries,
        &mut table_references,
        &mut body.returning,
        resolver,
        connection,
    )?;

    let result_columns =
        process_returning_clause(&mut body.returning, &mut table_references, resolver)?;

    let order_by = body
        .order_by
        .iter_mut()
        .map(|o| {
            let _ = bind_and_rewrite_expr(
                &mut o.expr,
                Some(&mut table_references),
                Some(&result_columns),
                resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            );
            (o.expr.clone(), o.order.unwrap_or(SortOrder::Asc))
        })
        .collect();

    // Sqlite determines we should create an ephemeral table if we do not have a FROM clause
    // Difficult to say what items from the plan can be checked for this so currently just checking if a RowId Alias is referenced
    // https://github.com/sqlite/sqlite/blob/master/src/update.c#L395
    // https://github.com/sqlite/sqlite/blob/master/src/update.c#L670
    let columns = table.columns();
    let mut where_clause = vec![];
    // Parse the WHERE clause
    parse_where(
        body.where_clause.as_deref(),
        &mut table_references,
        Some(&result_columns),
        &mut where_clause,
        resolver,
    )?;

    // Parse the LIMIT/OFFSET clause
    let (limit, offset) = body
        .limit
        .map_or(Ok((None, None)), |l| parse_limit(l, resolver))?;

    // Check what indexes will need to be updated by checking set_clauses and see
    // if a column is contained in an index.
    let indexes: Vec<_> = resolver.with_schema(database_id, |s| {
        s.get_indices(table_name).cloned().collect()
    });
    let updated_cols: HashSet<usize> = set_clauses.iter().map(|(i, _)| *i).collect();
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

    Ok(Plan::Update(UpdatePlan {
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
        non_from_clause_subqueries,
        safety: DmlSafety::default(),
    }))
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
