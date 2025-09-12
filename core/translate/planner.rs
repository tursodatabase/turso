use std::sync::Arc;

use super::{
    expr::walk_expr,
    plan::{
        Aggregate, ColumnUsedMask, Distinctness, EvalAt, IterationDirection, JoinInfo,
        JoinOrderMember, JoinedTable, Operation, OuterQueryReference, Plan, QueryDestination,
        ResultSetColumn, Scan, TableReferences, WhereTerm,
    },
    select::prepare_select_plan,
    SymbolTable,
};
use crate::function::{AggFunc, ExtFunc};
use crate::translate::expr::WalkControl;
use crate::{
    ast::Limit,
    function::Func,
    schema::{Schema, Table},
    translate::expr::walk_expr_mut,
    util::{exprs_are_equivalent, normalize_ident},
    vdbe::{builder::TableRefIdCounter, BranchOffset},
    Result,
};
use turso_macros::match_ignore_ascii_case;
use turso_parser::ast::Literal::Null;
use turso_parser::ast::{
    self, As, Expr, FromClause, JoinType, Literal, Materialized, QualifiedName, TableInternalId,
    With,
};

pub const ROWID: &str = "rowid";

pub fn resolve_aggregates(
    schema: &Schema,
    syms: &SymbolTable,
    top_level_expr: &Expr,
    aggs: &mut Vec<Aggregate>,
) -> Result<bool> {
    let mut contains_aggregates = false;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinctness,
                filter_over,
                order_by,
            } => {
                if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() {
                    crate::bail_parse_error!(
                        "FILTER clause is not supported yet in aggregate functions"
                    );
                }
                if !order_by.is_empty() {
                    crate::bail_parse_error!(
                        "ORDER BY clause is not supported yet in aggregate functions"
                    );
                }
                let args_count = args.len();
                let distinctness = Distinctness::from_ast(distinctness.as_ref());

                if !schema.indexes_enabled() && distinctness.is_distinct() {
                    crate::bail_parse_error!(
                        "SELECT with DISTINCT is not allowed without indexes enabled"
                    );
                }
                if distinctness.is_distinct() && args_count != 1 {
                    crate::bail_parse_error!(
                        "DISTINCT aggregate functions must have exactly one argument"
                    );
                }
                match Func::resolve_function(name.as_str(), args_count) {
                    Ok(Func::Agg(f)) => {
                        add_aggregate_if_not_exists(aggs, expr, args, distinctness, f);
                        contains_aggregates = true;
                        return Ok(WalkControl::SkipChildren);
                    }
                    Err(e) => {
                        if let Some(f) = syms.resolve_function(name.as_str(), args_count) {
                            if let ExtFunc::Aggregate { .. } = f.as_ref().func {
                                add_aggregate_if_not_exists(
                                    aggs,
                                    expr,
                                    args,
                                    distinctness,
                                    AggFunc::External(f.func.clone().into()),
                                );
                                contains_aggregates = true;
                                return Ok(WalkControl::SkipChildren);
                            }
                        } else {
                            return Err(e);
                        }
                    }
                    _ => {}
                }
            }
            Expr::FunctionCallStar { name, filter_over } => {
                if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() {
                    crate::bail_parse_error!(
                        "FILTER clause is not supported yet in aggregate functions"
                    );
                }
                match Func::resolve_function(name.as_str(), 0) {
                    Ok(Func::Agg(f)) => {
                        add_aggregate_if_not_exists(aggs, expr, &[], Distinctness::NonDistinct, f);
                        contains_aggregates = true;
                        return Ok(WalkControl::SkipChildren);
                    }
                    Ok(_) => {
                        crate::bail_parse_error!("Invalid aggregate function: {}", name.as_str());
                    }
                    Err(e) => match e {
                        crate::LimboError::ParseError(e) => {
                            crate::bail_parse_error!("{}", e);
                        }
                        _ => {
                            crate::bail_parse_error!(
                                "Invalid aggregate function: {}",
                                name.as_str()
                            );
                        }
                    },
                }
            }
            _ => {}
        }

        Ok(WalkControl::Continue)
    })?;

    Ok(contains_aggregates)
}

fn add_aggregate_if_not_exists(
    aggs: &mut Vec<Aggregate>,
    expr: &Expr,
    args: &[Box<Expr>],
    distinctness: Distinctness,
    func: AggFunc,
) {
    if aggs
        .iter()
        .all(|a| !exprs_are_equivalent(&a.original_expr, expr))
    {
        aggs.push(Aggregate::new(func, args, expr, distinctness));
    }
}

pub fn bind_column_references(
    top_level_expr: &mut Expr,
    referenced_tables: &mut TableReferences,
    result_columns: Option<&[ResultSetColumn]>,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    walk_expr_mut(top_level_expr, &mut |expr: &mut Expr| -> Result<()> {
        match expr {
            Expr::Id(id) => {
                // true and false are special constants that are effectively aliases for 1 and 0
                // and not identifiers of columns
                let id_bytes = id.as_str().as_bytes();
                match_ignore_ascii_case!(match id_bytes {
                    b"true" | b"false" => {
                        return Ok(());
                    }
                    _ => {}
                });
                let normalized_id = normalize_ident(id.as_str());

                if !referenced_tables.joined_tables().is_empty() {
                    if let Some(row_id_expr) = parse_row_id(
                        &normalized_id,
                        referenced_tables.joined_tables()[0].internal_id,
                        || referenced_tables.joined_tables().len() != 1,
                    )? {
                        *expr = row_id_expr;

                        return Ok(());
                    }
                }
                let mut match_result = None;

                // First check joined tables
                for joined_table in referenced_tables.joined_tables().iter() {
                    let col_idx = joined_table.table.columns().iter().position(|c| {
                        c.name
                            .as_ref()
                            .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                    });
                    if col_idx.is_some() {
                        if match_result.is_some() {
                            crate::bail_parse_error!("Column {} is ambiguous", id.as_str());
                        }
                        let col = joined_table.table.columns().get(col_idx.unwrap()).unwrap();
                        match_result = Some((
                            joined_table.internal_id,
                            col_idx.unwrap(),
                            col.is_rowid_alias,
                        ));
                    }
                }

                // Then check outer query references, if we still didn't find something.
                // Normally finding multiple matches for a non-qualified column is an error (column x is ambiguous)
                // but in the case of subqueries, the inner query takes precedence.
                // For example:
                // SELECT * FROM t WHERE x = (SELECT x FROM t2)
                // In this case, there is no ambiguity:
                // - x in the outer query refers to t.x,
                // - x in the inner query refers to t2.x.
                if match_result.is_none() {
                    for outer_ref in referenced_tables.outer_query_refs().iter() {
                        let col_idx = outer_ref.table.columns().iter().position(|c| {
                            c.name
                                .as_ref()
                                .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                        });
                        if col_idx.is_some() {
                            if match_result.is_some() {
                                crate::bail_parse_error!("Column {} is ambiguous", id.as_str());
                            }
                            let col = outer_ref.table.columns().get(col_idx.unwrap()).unwrap();
                            match_result =
                                Some((outer_ref.internal_id, col_idx.unwrap(), col.is_rowid_alias));
                        }
                    }
                }

                if let Some((table_id, col_idx, is_rowid_alias)) = match_result {
                    *expr = Expr::Column {
                        database: None, // TODO: support different databases
                        table: table_id,
                        column: col_idx,
                        is_rowid_alias,
                    };
                    referenced_tables.mark_column_used(table_id, col_idx);
                    return Ok(());
                }

                if let Some(result_columns) = result_columns {
                    for result_column in result_columns.iter() {
                        if result_column
                            .name(referenced_tables)
                            .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                        {
                            *expr = result_column.expr.clone();
                            return Ok(());
                        }
                    }
                }
                // SQLite behavior: Only double-quoted identifiers get fallback to string literals
                // Single quotes are handled as literals earlier, unquoted identifiers must resolve to columns
                if id.is_double_quoted() {
                    // Convert failed double-quoted identifier to string literal
                    *expr = Expr::Literal(Literal::String(id.as_str().to_string()));
                    Ok(())
                } else {
                    // Unquoted identifiers must resolve to columns - no fallback
                    crate::bail_parse_error!("no such column: {}", id.as_str())
                }
            }
            Expr::Qualified(tbl, id) => {
                let normalized_table_name = normalize_ident(tbl.as_str());
                let matching_tbl = referenced_tables
                    .find_table_and_internal_id_by_identifier(&normalized_table_name);
                if matching_tbl.is_none() {
                    crate::bail_parse_error!("no such table: {}", normalized_table_name);
                }
                let (tbl_id, tbl) = matching_tbl.unwrap();
                let normalized_id = normalize_ident(id.as_str());

                if let Some(row_id_expr) = parse_row_id(&normalized_id, tbl_id, || false)? {
                    *expr = row_id_expr;

                    return Ok(());
                }
                let col_idx = tbl.columns().iter().position(|c| {
                    c.name
                        .as_ref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                });
                let Some(col_idx) = col_idx else {
                    crate::bail_parse_error!("no such column: {}", normalized_id);
                };
                let col = tbl.columns().get(col_idx).unwrap();
                *expr = Expr::Column {
                    database: None, // TODO: support different databases
                    table: tbl_id,
                    column: col_idx,
                    is_rowid_alias: col.is_rowid_alias,
                };
                referenced_tables.mark_column_used(tbl_id, col_idx);
                Ok(())
            }
            Expr::DoublyQualified(db_name, tbl_name, col_name) => {
                let normalized_col_name = normalize_ident(col_name.as_str());

                // Create a QualifiedName and use existing resolve_database_id method
                let qualified_name = ast::QualifiedName {
                    db_name: Some(db_name.clone()),
                    name: tbl_name.clone(),
                    alias: None,
                };
                let database_id = connection.resolve_database_id(&qualified_name)?;

                // Get the table from the specified database
                let table = connection
                    .with_schema(database_id, |schema| schema.get_table(tbl_name.as_str()))
                    .ok_or_else(|| {
                        crate::LimboError::ParseError(format!(
                            "no such table: {}.{}",
                            db_name.as_str(),
                            tbl_name.as_str()
                        ))
                    })?;

                // Find the column in the table
                let col_idx = table
                    .columns()
                    .iter()
                    .position(|c| {
                        c.name
                            .as_ref()
                            .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_col_name))
                    })
                    .ok_or_else(|| {
                        crate::LimboError::ParseError(format!(
                            "Column: {}.{}.{} not found",
                            db_name.as_str(),
                            tbl_name.as_str(),
                            col_name.as_str()
                        ))
                    })?;

                let col = table.columns().get(col_idx).unwrap();

                // Check if this is a rowid alias
                let is_rowid_alias = col.is_rowid_alias;

                // Convert to Column expression - since this is a cross-database reference,
                // we need to create a synthetic table reference for it
                // For now, we'll error if the table isn't already in the referenced tables
                let normalized_tbl_name = normalize_ident(tbl_name.as_str());
                let matching_tbl = referenced_tables
                    .find_table_and_internal_id_by_identifier(&normalized_tbl_name);

                if let Some((tbl_id, _)) = matching_tbl {
                    // Table is already in referenced tables, use existing internal ID
                    *expr = Expr::Column {
                        database: Some(database_id),
                        table: tbl_id,
                        column: col_idx,
                        is_rowid_alias,
                    };
                    referenced_tables.mark_column_used(tbl_id, col_idx);
                } else {
                    return Err(crate::LimboError::ParseError(format!(
                        "table {normalized_tbl_name} is not in FROM clause - cross-database column references require the table to be explicitly joined"
                    )));
                }

                Ok(())
            }
            _ => Ok(()),
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn parse_from_clause_table(
    schema: &Schema,
    table: ast::SelectTable,
    table_references: &mut TableReferences,
    vtab_predicates: &mut Vec<Expr>,
    ctes: &mut Vec<JoinedTable>,
    syms: &SymbolTable,
    table_ref_counter: &mut TableRefIdCounter,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(qualified_name, maybe_alias, _) => parse_table(
            schema,
            syms,
            table_references,
            ctes,
            table_ref_counter,
            vtab_predicates,
            &qualified_name,
            maybe_alias.as_ref(),
            &[],
            connection,
        ),
        ast::SelectTable::Select(subselect, maybe_alias) => {
            let Plan::Select(subplan) = prepare_select_plan(
                schema,
                subselect,
                syms,
                table_references.outer_query_refs(),
                table_ref_counter,
                QueryDestination::CoroutineYield {
                    yield_reg: usize::MAX, // will be set later in bytecode emission
                    coroutine_implementation_start: BranchOffset::Placeholder, // will be set later in bytecode emission
                },
                connection,
            )?
            else {
                crate::bail_parse_error!("Only non-compound SELECT queries are currently supported in FROM clause subqueries");
            };
            let cur_table_index = table_references.joined_tables().len();
            let identifier = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id,
                    ast::As::Elided(id) => id,
                })
                .map(|id| normalize_ident(id.as_str()))
                .unwrap_or(format!("subquery_{cur_table_index}"));
            table_references.add_joined_table(JoinedTable::new_subquery(
                identifier,
                subplan,
                None,
                table_ref_counter.next(),
            ));
            Ok(())
        }
        ast::SelectTable::TableCall(qualified_name, args, maybe_alias) => parse_table(
            schema,
            syms,
            table_references,
            ctes,
            table_ref_counter,
            vtab_predicates,
            &qualified_name,
            maybe_alias.as_ref(),
            &args,
            connection,
        ),
        _ => todo!(),
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_table(
    schema: &Schema,
    syms: &SymbolTable,
    table_references: &mut TableReferences,
    ctes: &mut Vec<JoinedTable>,
    table_ref_counter: &mut TableRefIdCounter,
    vtab_predicates: &mut Vec<Expr>,
    qualified_name: &QualifiedName,
    maybe_alias: Option<&As>,
    args: &[Box<Expr>],
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let normalized_qualified_name = normalize_ident(qualified_name.name.as_str());
    let database_id = connection.resolve_database_id(qualified_name)?;
    let table_name = &qualified_name.name;

    // Check if the FROM clause table is referring to a CTE in the current scope.
    if let Some(cte_idx) = ctes
        .iter()
        .position(|cte| cte.identifier == normalized_qualified_name)
    {
        // TODO: what if the CTE is referenced multiple times?
        let cte_table = ctes.remove(cte_idx);
        table_references.add_joined_table(cte_table);
        return Ok(());
    };

    // Resolve table using connection's with_schema method
    let table = connection.with_schema(database_id, |schema| schema.get_table(table_name.as_str()));

    if let Some(table) = table {
        let alias = maybe_alias
            .map(|a| match a {
                ast::As::As(id) => id,
                ast::As::Elided(id) => id,
            })
            .map(|a| normalize_ident(a.as_str()));
        let internal_id = table_ref_counter.next();
        let tbl_ref = if let Table::Virtual(tbl) = table.as_ref() {
            transform_args_into_where_terms(args, internal_id, vtab_predicates, table.as_ref())?;
            Table::Virtual(tbl.clone())
        } else if let Table::BTree(table) = table.as_ref() {
            Table::BTree(table.clone())
        } else {
            return Err(crate::LimboError::InvalidArgument(
                "Table type not supported".to_string(),
            ));
        };
        table_references.add_joined_table(JoinedTable {
            op: Operation::default_scan_for(&tbl_ref),
            table: tbl_ref,
            identifier: alias.unwrap_or(normalized_qualified_name),
            internal_id,
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            database_id,
        });
        return Ok(());
    };

    let regular_view = connection.with_schema(database_id, |schema| {
        schema.get_view(table_name.as_str()).cloned()
    });
    if let Some(view) = regular_view {
        // Views are essentially query aliases, so just Expand the view as a subquery
        let view_select = view.select_stmt.clone();
        let subselect = Box::new(view_select);

        // Use the view name as alias if no explicit alias was provided
        let view_alias = maybe_alias
            .cloned()
            .or_else(|| Some(ast::As::As(table_name.clone())));

        // Recursively call parse_from_clause_table with the view as a SELECT
        return parse_from_clause_table(
            schema,
            ast::SelectTable::Select(*subselect.clone(), view_alias),
            table_references,
            vtab_predicates,
            ctes,
            syms,
            table_ref_counter,
            connection,
        );
    }

    let view = connection.with_schema(database_id, |schema| {
        schema.get_materialized_view(table_name.as_str())
    });
    if let Some(view) = view {
        // Check if this materialized view has persistent storage
        let view_guard = view.lock().unwrap();
        let root_page = view_guard.get_root_page();

        if root_page == 0 {
            drop(view_guard);
            return Err(crate::LimboError::InternalError(
                "Materialized view has no storage allocated".to_string(),
            ));
        }

        // This is a materialized view with storage - treat it as a regular BTree table
        // Create a BTreeTable from the view's metadata
        let btree_table = Arc::new(crate::schema::BTreeTable {
            name: view_guard.name().to_string(),
            root_page,
            columns: view_guard.columns.clone(),
            primary_key_columns: Vec::new(),
            has_rowid: true,
            is_strict: false,
            unique_sets: vec![],
        });
        drop(view_guard);

        let alias = maybe_alias
            .map(|a| match a {
                ast::As::As(id) => id,
                ast::As::Elided(id) => id,
            })
            .map(|a| normalize_ident(a.as_str()));

        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            table: Table::BTree(btree_table),
            identifier: alias.unwrap_or(normalized_qualified_name),
            internal_id: table_ref_counter.next(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            database_id,
        });
        return Ok(());
    }

    // CTEs are transformed into FROM clause subqueries.
    // If we find a CTE with this name in our outer query references,
    // we can use it as a joined table, but we must clone it since it's not MATERIALIZED.
    //
    // For other types of tables in the outer query references, we do not add them as joined tables,
    // because the query can simply _reference_ them in e.g. the SELECT columns or the WHERE clause,
    // but it's not part of the join order.
    if let Some(outer_ref) =
        table_references.find_outer_query_ref_by_identifier(&normalized_qualified_name)
    {
        if matches!(outer_ref.table, Table::FromClauseSubquery(_)) {
            table_references.add_joined_table(JoinedTable {
                op: Operation::default_scan_for(&outer_ref.table),
                table: outer_ref.table.clone(),
                identifier: outer_ref.identifier.clone(),
                internal_id: table_ref_counter.next(),
                join_info: None,
                col_used_mask: ColumnUsedMask::default(),
                database_id,
            });
            return Ok(());
        }
    }

    crate::bail_parse_error!("no such table: {}", normalized_qualified_name);
}

fn transform_args_into_where_terms(
    args: &[Box<Expr>],
    internal_id: TableInternalId,
    predicates: &mut Vec<Expr>,
    table: &Table,
) -> Result<()> {
    let mut args_iter = args.iter();
    let mut hidden_count = 0;
    for (i, col) in table.columns().iter().enumerate() {
        if !col.hidden {
            continue;
        }
        hidden_count += 1;

        if let Some(arg_expr) = args_iter.next() {
            let column_expr = Expr::Column {
                database: None,
                table: internal_id,
                column: i,
                is_rowid_alias: col.is_rowid_alias,
            };
            let expr = match arg_expr.as_ref() {
                Expr::Literal(Null) => Expr::IsNull(Box::new(column_expr)),
                other => Expr::Binary(
                    column_expr.into(),
                    ast::Operator::Equals,
                    other.clone().into(),
                ),
            };
            predicates.push(expr);
        }
    }

    if args_iter.next().is_some() {
        return Err(crate::LimboError::ParseError(format!(
            "Too many arguments for {}: expected at most {}, got {}",
            table.get_name(),
            hidden_count,
            hidden_count + 1 + args_iter.count()
        )));
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn parse_from(
    schema: &Schema,
    mut from: Option<FromClause>,
    syms: &SymbolTable,
    with: Option<With>,
    out_where_clause: &mut Vec<WhereTerm>,
    vtab_predicates: &mut Vec<Expr>,
    table_references: &mut TableReferences,
    table_ref_counter: &mut TableRefIdCounter,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    if from.is_none() {
        return Ok(());
    }

    let mut ctes_as_subqueries = vec![];

    if let Some(with) = with {
        if with.recursive {
            crate::bail_parse_error!("Recursive CTEs are not yet supported");
        }
        for cte in with.ctes {
            if cte.materialized == Materialized::Yes {
                crate::bail_parse_error!("Materialized CTEs are not yet supported");
            }
            if !cte.columns.is_empty() {
                crate::bail_parse_error!("CTE columns are not yet supported");
            }

            // Check if normalized name conflicts with catalog tables or other CTEs
            // TODO: sqlite actually allows overriding a catalog table with a CTE.
            // We should carry over the 'Scope' struct to all of our identifier resolution.
            let cte_name_normalized = normalize_ident(cte.tbl_name.as_str());
            if schema.get_table(&cte_name_normalized).is_some() {
                crate::bail_parse_error!(
                    "CTE name {} conflicts with catalog table name",
                    cte.tbl_name.as_str()
                );
            }
            if table_references
                .outer_query_refs()
                .iter()
                .any(|t| t.identifier == cte_name_normalized)
            {
                crate::bail_parse_error!(
                    "CTE name {} conflicts with WITH table name {}",
                    cte.tbl_name.as_str(),
                    cte_name_normalized
                );
            }

            let mut outer_query_refs_for_cte = table_references.outer_query_refs().to_vec();
            outer_query_refs_for_cte.extend(ctes_as_subqueries.iter().map(|t: &JoinedTable| {
                OuterQueryReference {
                    identifier: t.identifier.clone(),
                    internal_id: t.internal_id,
                    table: t.table.clone(),
                    col_used_mask: ColumnUsedMask::default(),
                }
            }));

            // CTE can refer to other CTEs that came before it, plus any schema tables or tables in the outer scope.
            let cte_plan = prepare_select_plan(
                schema,
                cte.select,
                syms,
                &outer_query_refs_for_cte,
                table_ref_counter,
                QueryDestination::CoroutineYield {
                    yield_reg: usize::MAX, // will be set later in bytecode emission
                    coroutine_implementation_start: BranchOffset::Placeholder, // will be set later in bytecode emission
                },
                connection,
            )?;
            let Plan::Select(cte_plan) = cte_plan else {
                crate::bail_parse_error!("Only SELECT queries are currently supported in CTEs");
            };
            ctes_as_subqueries.push(JoinedTable::new_subquery(
                cte_name_normalized,
                cte_plan,
                None,
                table_ref_counter.next(),
            ));
        }
    }

    let from_owned = std::mem::take(&mut from).unwrap();
    let select_owned = from_owned.select;
    let joins_owned = from_owned.joins;
    parse_from_clause_table(
        schema,
        *select_owned,
        table_references,
        vtab_predicates,
        &mut ctes_as_subqueries,
        syms,
        table_ref_counter,
        connection,
    )?;

    for join in joins_owned.into_iter() {
        parse_join(
            schema,
            join,
            syms,
            &mut ctes_as_subqueries,
            out_where_clause,
            vtab_predicates,
            table_references,
            table_ref_counter,
            connection,
        )?;
    }

    Ok(())
}

pub fn parse_where(
    where_clause: Option<&Expr>,
    table_references: &mut TableReferences,
    result_columns: Option<&[ResultSetColumn]>,
    out_where_clause: &mut Vec<WhereTerm>,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    if let Some(where_expr) = where_clause {
        let mut predicates = vec![];
        break_predicate_at_and_boundaries(where_expr, &mut predicates);
        for expr in predicates.iter_mut() {
            bind_column_references(expr, table_references, result_columns, connection)?;
        }
        for expr in predicates {
            out_where_clause.push(WhereTerm {
                expr,
                from_outer_join: None,
                consumed: false,
            });
        }
        Ok(())
    } else {
        Ok(())
    }
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
) -> Result<EvalAt> {
    if let Some(table_id) = term.from_outer_join {
        return Ok(EvalAt::Loop(
            join_order
                .iter()
                .position(|t| t.table_id == table_id)
                .unwrap_or(usize::MAX),
        ));
    }

    determine_where_to_eval_expr(&term.expr, join_order)
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableMask(pub u128);

impl std::ops::BitOrAssign for TableMask {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl TableMask {
    /// Creates a new empty table mask.
    ///
    /// The initial mask represents an empty set of tables.
    pub fn new() -> Self {
        Self(0)
    }

    /// Returns true if the mask represents an empty set of tables.
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Creates a new mask that is the same as this one but without the specified table.
    pub fn without_table(&self, table_no: usize) -> Self {
        assert!(table_no < 127, "table_no must be less than 127");
        Self(self.0 ^ (1 << (table_no + 1)))
    }

    /// Creates a table mask from raw bits.
    ///
    /// The bits are shifted left by 1 to maintain the convention that table 0 is at bit 1.
    pub fn from_bits(bits: u128) -> Self {
        Self(bits << 1)
    }

    /// Creates a table mask from an iterator of table numbers.
    pub fn from_table_number_iter(iter: impl Iterator<Item = usize>) -> Self {
        iter.fold(Self::new(), |mut mask, table_no| {
            assert!(table_no < 127, "table_no must be less than 127");
            mask.add_table(table_no);
            mask
        })
    }

    /// Adds a table to the mask.
    pub fn add_table(&mut self, table_no: usize) {
        assert!(table_no < 127, "table_no must be less than 127");
        self.0 |= 1 << (table_no + 1);
    }

    /// Returns true if the mask contains the specified table.
    pub fn contains_table(&self, table_no: usize) -> bool {
        assert!(table_no < 127, "table_no must be less than 127");
        self.0 & (1 << (table_no + 1)) != 0
    }

    /// Returns true if this mask contains all tables in the other mask.
    pub fn contains_all(&self, other: &TableMask) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns the number of tables in the mask.
    pub fn table_count(&self) -> usize {
        self.0.count_ones() as usize
    }

    /// Returns true if this mask shares any tables with the other mask.
    pub fn intersects(&self, other: &TableMask) -> bool {
        self.0 & other.0 != 0
    }
}

/// Returns a [TableMask] representing the tables referenced in the given expression.
/// Used in the optimizer for constraint analysis.
pub fn table_mask_from_expr(
    top_level_expr: &Expr,
    table_references: &TableReferences,
) -> Result<TableMask> {
    let mut mask = TableMask::new();
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if let Some(table_idx) = table_references
                    .joined_tables()
                    .iter()
                    .position(|t| t.internal_id == *table)
                {
                    mask.add_table(table_idx);
                } else if table_references
                    .find_outer_query_ref_by_internal_id(*table)
                    .is_none()
                {
                    // Tables from outer query scopes are guaranteed to be 'in scope' for this query,
                    // so they don't need to be added to the table mask. However, if the table is not found
                    // in the outer scope either, then it's an invalid reference.
                    crate::bail_parse_error!("table not found in joined_tables");
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(mask)
}

pub fn determine_where_to_eval_expr(
    top_level_expr: &Expr,
    join_order: &[JoinOrderMember],
) -> Result<EvalAt> {
    let mut eval_at: EvalAt = EvalAt::BeforeLoop;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                let join_idx = join_order
                    .iter()
                    .position(|t| t.table_id == *table)
                    .unwrap_or(usize::MAX);
                eval_at = eval_at.max(EvalAt::Loop(join_idx));
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(eval_at)
}

#[allow(clippy::too_many_arguments)]
fn parse_join(
    schema: &Schema,
    join: ast::JoinedSelectTable,
    syms: &SymbolTable,
    ctes: &mut Vec<JoinedTable>,
    out_where_clause: &mut Vec<WhereTerm>,
    vtab_predicates: &mut Vec<Expr>,
    table_references: &mut TableReferences,
    table_ref_counter: &mut TableRefIdCounter,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let ast::JoinedSelectTable {
        operator: join_operator,
        table,
        constraint,
    } = join;

    parse_from_clause_table(
        schema,
        table.as_ref().clone(),
        table_references,
        vtab_predicates,
        ctes,
        syms,
        table_ref_counter,
        connection,
    )?;

    let (outer, natural) = match join_operator {
        ast::JoinOperator::TypedJoin(Some(join_type)) => {
            if join_type.contains(JoinType::RIGHT) {
                crate::bail_parse_error!("RIGHT JOIN is not supported");
            }
            if join_type.contains(JoinType::CROSS) {
                crate::bail_parse_error!("CROSS JOIN is not supported");
            }
            let is_outer = join_type.contains(JoinType::OUTER);
            let is_natural = join_type.contains(JoinType::NATURAL);
            (is_outer, is_natural)
        }
        _ => (false, false),
    };

    if natural && constraint.is_some() {
        crate::bail_parse_error!("NATURAL JOIN cannot be combined with ON or USING clause");
    }

    let constraint = if natural {
        assert!(table_references.joined_tables().len() >= 2);
        let rightmost_table = table_references.joined_tables().last().unwrap();
        // NATURAL JOIN is first transformed into a USING join with the common columns
        let mut distinct_names: Vec<ast::Name> = vec![];
        // TODO: O(n^2) maybe not great for large tables or big multiway joins
        // SQLite doesn't use HIDDEN columns for NATURAL joins: https://www3.sqlite.org/src/info/ab09ef427181130b
        for right_col in rightmost_table.columns().iter().filter(|col| !col.hidden) {
            let mut found_match = false;
            for left_table in table_references
                .joined_tables()
                .iter()
                .take(table_references.joined_tables().len() - 1)
            {
                for left_col in left_table.columns().iter().filter(|col| !col.hidden) {
                    if left_col.name == right_col.name {
                        distinct_names.push(ast::Name::new(
                            left_col.name.clone().expect("column name is None"),
                        ));
                        found_match = true;
                        break;
                    }
                }
                if found_match {
                    break;
                }
            }
        }
        if distinct_names.is_empty() {
            crate::bail_parse_error!("No columns found to NATURAL join on");
        } else {
            Some(ast::JoinConstraint::Using(distinct_names))
        }
    } else {
        constraint
    };

    let mut using = vec![];

    if let Some(constraint) = constraint {
        match constraint {
            ast::JoinConstraint::On(ref expr) => {
                let mut preds = vec![];
                break_predicate_at_and_boundaries(expr, &mut preds);
                for predicate in preds.iter_mut() {
                    bind_column_references(predicate, table_references, None, connection)?;
                }
                for pred in preds {
                    out_where_clause.push(WhereTerm {
                        expr: pred,
                        from_outer_join: if outer {
                            Some(table_references.joined_tables().last().unwrap().internal_id)
                        } else {
                            None
                        },
                        consumed: false,
                    });
                }
            }
            ast::JoinConstraint::Using(distinct_names) => {
                // USING join is replaced with a list of equality predicates
                for distinct_name in distinct_names.iter() {
                    let name_normalized = normalize_ident(distinct_name.as_str());
                    let cur_table_idx = table_references.joined_tables().len() - 1;
                    let left_tables = &table_references.joined_tables()[..cur_table_idx];
                    assert!(!left_tables.is_empty());
                    let right_table = table_references.joined_tables().last().unwrap();
                    let mut left_col = None;
                    for (left_table_idx, left_table) in left_tables.iter().enumerate() {
                        left_col = left_table
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(_, col)| !natural || !col.hidden)
                            .find(|(_, col)| {
                                col.name
                                    .as_ref()
                                    .is_some_and(|name| *name == name_normalized)
                            })
                            .map(|(idx, col)| (left_table_idx, left_table.internal_id, idx, col));
                        if left_col.is_some() {
                            break;
                        }
                    }
                    if left_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            distinct_name.as_str()
                        );
                    }
                    let right_col = right_table.columns().iter().enumerate().find(|(_, col)| {
                        col.name
                            .as_ref()
                            .is_some_and(|name| *name == name_normalized)
                    });
                    if right_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            distinct_name.as_str()
                        );
                    }
                    let (left_table_idx, left_table_id, left_col_idx, left_col) = left_col.unwrap();
                    let (right_col_idx, right_col) = right_col.unwrap();
                    let expr = Expr::Binary(
                        Box::new(Expr::Column {
                            database: None,
                            table: left_table_id,
                            column: left_col_idx,
                            is_rowid_alias: left_col.is_rowid_alias,
                        }),
                        ast::Operator::Equals,
                        Box::new(Expr::Column {
                            database: None,
                            table: right_table.internal_id,
                            column: right_col_idx,
                            is_rowid_alias: right_col.is_rowid_alias,
                        }),
                    );

                    let left_table: &mut JoinedTable = table_references
                        .joined_tables_mut()
                        .get_mut(left_table_idx)
                        .unwrap();
                    left_table.mark_column_used(left_col_idx);
                    let right_table: &mut JoinedTable = table_references
                        .joined_tables_mut()
                        .get_mut(cur_table_idx)
                        .unwrap();
                    right_table.mark_column_used(right_col_idx);
                    out_where_clause.push(WhereTerm {
                        expr,
                        from_outer_join: if outer {
                            Some(right_table.internal_id)
                        } else {
                            None
                        },
                        consumed: false,
                    });
                }
                using = distinct_names;
            }
        }
    }

    assert!(table_references.joined_tables().len() >= 2);
    let last_idx = table_references.joined_tables().len() - 1;
    let rightmost_table = table_references
        .joined_tables_mut()
        .get_mut(last_idx)
        .unwrap();
    rightmost_table.join_info = Some(JoinInfo { outer, using });

    Ok(())
}

pub fn break_predicate_at_and_boundaries(predicate: &Expr, out_predicates: &mut Vec<Expr>) {
    match predicate {
        Expr::Binary(left, ast::Operator::And, right) => {
            break_predicate_at_and_boundaries(left, out_predicates);
            break_predicate_at_and_boundaries(right, out_predicates);
        }
        _ => {
            out_predicates.push(predicate.clone());
        }
    }
}

fn parse_row_id<F>(
    column_name: &str,
    table_id: TableInternalId,
    fn_check: F,
) -> Result<Option<Expr>>
where
    F: FnOnce() -> bool,
{
    if column_name.eq_ignore_ascii_case(ROWID) {
        if fn_check() {
            crate::bail_parse_error!("ROWID is ambiguous");
        }

        return Ok(Some(Expr::RowId {
            database: None, // TODO: support different databases
            table: table_id,
        }));
    }
    Ok(None)
}

#[allow(clippy::type_complexity)]
pub fn parse_limit(
    limit: &mut Limit,
    connection: &std::sync::Arc<crate::Connection>,
) -> Result<(Option<Box<Expr>>, Option<Box<Expr>>)> {
    let mut empty_refs = TableReferences::new(Vec::new(), Vec::new());
    bind_column_references(&mut limit.expr, &mut empty_refs, None, connection)?;
    if let Some(ref mut off_expr) = limit.offset {
        bind_column_references(off_expr, &mut empty_refs, None, connection)?;
    }
    Ok((Some(limit.expr.clone()), limit.offset.clone()))
}
