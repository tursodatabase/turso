use super::*;

/// The precedence of binding identifiers to columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingBehavior {
    /// `TryResultColumnsFirst` means that result columns (e.g. SELECT x AS y, ...) take precedence over canonical columns (e.g. SELECT x, y AS z, ...). This is the default behavior.
    TryResultColumnsFirst,
    /// `TryCanonicalColumnsFirst` means that canonical columns take precedence over result columns. This is used for e.g. WHERE clauses.
    TryCanonicalColumnsFirst,
    /// `ResultColumnsNotAllowed` means that referring to result columns is not allowed. This is used e.g. for DML statements.
    ResultColumnsNotAllowed,
    /// `AllowUnboundIdentifiers` means that unbound identifiers are allowed. This is used for INSERT ... ON CONFLICT DO UPDATE SET ... where binding is handled later than this phase.
    AllowUnboundIdentifiers,
}

/// The column found while resolving a qualified `<table>.<column>` name.
#[derive(Debug, Clone, Copy)]
enum QualifiedMatch {
    /// `<id>` named a real column on the candidate table.
    Column {
        column_index: usize,
        is_rowid_alias: bool,
    },
    /// `<id>` named the rowid (`rowid`/`oid`/`_rowid_`) of a btree.
    /// There is no column index — the result must become an
    /// `Expr::RowId`, not an `Expr::Column`.
    RowId,
}

/// The result of one qualified-name search in one table reference.
#[derive(Clone, Copy)]
enum QualifiedTableMatch {
    /// The qualifier does not name this table reference.
    NoTable,
    /// The qualifier names this table reference, but its column does not exist.
    NoColumn,
    /// The qualifier and column both match this table reference.
    Found(QualifiedMatch),
    /// More than one saved column in this table reference matches the name.
    Ambiguous,
}

/// The result of searching the current scope and its parent scopes.
#[derive(Clone, Copy)]
enum QualifiedNameMatch {
    /// No visible query scope contains the requested table.
    NoTable,
    /// A visible table exists, but its column does not exist.
    NoColumn,
    /// One visible column matches the requested name.
    Found(TableInternalId, QualifiedMatch),
    /// More than one visible column matches the requested name.
    Ambiguous,
}

/// Rewrite ast::Expr in place, binding Column references/rewriting Expr::Id -> Expr::Column
/// using the provided TableReferences, and replacing anonymous parameters with internal named
/// ones
#[turso_macros::trace_stack]
pub fn bind_and_rewrite_expr<'a>(
    top_level_expr: &mut ast::Expr,
    mut referenced_tables: Option<&'a mut TableReferences>,
    result_columns: Option<&'a [ResultSetColumn]>,
    resolver: &Resolver<'_>,
    binding_behavior: BindingBehavior,
) -> Result<()> {
    walk_expr_mut(
        top_level_expr,
        &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
            match expr {
                Expr::Id(id) => {
                    crate::stack::trace_stack!("bind_id");
                    let Some(referenced_tables) = &mut referenced_tables else {
                        if binding_behavior == BindingBehavior::AllowUnboundIdentifiers {
                            return Ok(WalkControl::Continue);
                        }
                        crate::bail_parse_error!("no such column: {}", id.as_str());
                    };
                    let normalized_id = normalize_ident(id.as_str());

                    if binding_behavior == BindingBehavior::TryResultColumnsFirst {
                        if let Some(result_columns) = result_columns {
                            for result_column in result_columns.iter() {
                                if let Some(alias) = &result_column.alias {
                                    if alias.eq_ignore_ascii_case(&normalized_id) {
                                        *expr = result_column.expr.clone();
                                        return Ok(WalkControl::Continue);
                                    }
                                }
                            }
                        }
                    }
                    let joined_tables = referenced_tables.joined_tables();
                    // First check joined tables
                    // A first-match search cannot model RIGHT reset or FULL fallback rules.
                    let resolved_column = crate::translate::plan::resolve_unqualified_column(
                        joined_tables,
                        id.as_str(),
                    )?;
                    if let Some(resolved_column) = resolved_column {
                        *expr = resolved_column.expr;
                        // FULL JOIN can make the merged value read several table columns.
                        for (table_id, column_index) in resolved_column.source_columns {
                            referenced_tables.mark_column_used(table_id, column_index);
                        }
                        return Ok(WalkControl::Continue);
                    }

                    let mut match_result = None;
                    // No real column matched. SQLite now tries rowid names before outer scopes.
                    for joined_table in joined_tables.iter() {
                        if let Table::BTree(btree) = &joined_table.table {
                            if let Some(row_id_expr) =
                                parse_row_id(&normalized_id, joined_tables[0].internal_id, || {
                                    joined_tables.len() != 1
                                })?
                            {
                                if !btree.has_rowid {
                                    crate::bail_parse_error!("no such column: {}", id.as_str());
                                }
                                *expr = row_id_expr;
                                return Ok(WalkControl::Continue);
                            }
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
                    //
                    // Ambiguity is only checked within the same scope depth. Once a match
                    // is found at depth N, deeper scopes (N+1, N+2, ...) are not checked.
                    if match_result.is_none() {
                        let mut matched_scope_depth = None;
                        for outer_ref in referenced_tables.outer_query_refs().iter() {
                            // Definition-only entries let a FROM clause find a CTE by
                            // name; the CTE's columns are visible only after a FROM
                            // clause actually adds the table. Every other outer ref is
                            // a real table in an enclosing scope, including CTEs and
                            // the recursive self-reference, and its columns can be
                            // referenced without qualification.
                            if outer_ref.cte_definition_only {
                                continue;
                            }
                            // Skip refs from deeper scopes once we found a match
                            if let Some(depth) = matched_scope_depth {
                                if outer_ref.scope_depth > depth {
                                    continue;
                                }
                            }
                            let col_idx =
                                find_unqualified_column(&outer_ref.table, &normalized_id)?;
                            if col_idx.is_some() {
                                let col_idx = col_idx.unwrap();
                                if outer_ref.using_dedup_hidden_cols.get(col_idx) {
                                    continue;
                                }
                                if match_result.is_some() {
                                    crate::bail_parse_error!(
                                        "ambiguous column name: {}",
                                        id.as_str()
                                    );
                                }
                                let col = outer_ref.table.columns().get(col_idx).unwrap();
                                match_result =
                                    Some((outer_ref.internal_id, col_idx, col.is_rowid_alias()));
                                matched_scope_depth = Some(outer_ref.scope_depth);
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
                        return Ok(WalkControl::Continue);
                    }

                    if binding_behavior == BindingBehavior::TryCanonicalColumnsFirst {
                        if let Some(result_columns) = result_columns {
                            for result_column in result_columns.iter() {
                                if let Some(alias) = &result_column.alias {
                                    if alias.eq_ignore_ascii_case(&normalized_id) {
                                        *expr = result_column.expr.clone();
                                        return Ok(WalkControl::Continue);
                                    }
                                }
                            }
                        }
                    }

                    // SQLite DQS misfeature: double-quoted identifiers fall back to string literals
                    // only when DQS is enabled for DML statements
                    if id.quoted_with('"') && resolver.dqs_dml.is_enabled() {
                        *expr = Expr::Literal(ast::Literal::String(id.as_literal()));
                        return Ok(WalkControl::Continue);
                    } else {
                        crate::bail_parse_error!("no such column: {}", id.as_str())
                    }
                }
                Expr::Qualified(tbl, id) => {
                    crate::stack::trace_stack!("bind_qualified");
                    // Resolve a `<tbl>.<id>` reference.
                    //
                    // Two-stage lookup with shadowing:
                    //   1. Search the current scope's FROM tables (`joined_tables`).
                    //   2. Fall back to enclosing scopes (`outer_query_refs`), restricted to
                    //      the *nearest* scope whose identifier matches — so an inner alias
                    //      shadows a same-named alias in an outer scope instead of conflicting.
                    //
                    // Produces either `Expr::Column` (real column) or `Expr::RowId`
                    // (bare rowid alias like `t.rowid` on a btree with rowids).
                    tracing::debug!("bind_and_rewrite_expr({:?}, {:?})", tbl, id);
                    let Some(referenced_tables) = &mut referenced_tables else {
                        if binding_behavior == BindingBehavior::AllowUnboundIdentifiers {
                            return Ok(WalkControl::Continue);
                        }
                        crate::bail_parse_error!(
                            "no such column: {}.{}",
                            tbl.as_str(),
                            id.as_str()
                        );
                    };
                    let normalized_table_name = normalize_ident(tbl.as_str());
                    let normalized_id = normalize_ident(id.as_str());

                    let qualified_match = resolve_qualified_name(
                        referenced_tables,
                        None,
                        &normalized_table_name,
                        &normalized_id,
                    )?;

                    // --- Error reporting. ---
                    if matches!(qualified_match, QualifiedNameMatch::NoTable) {
                        // No scope contains a table with this identifier. Normally we
                        // report "no such table", but there is one case where SQLite
                        // reports "no such column" instead: when the identifier names a
                        // CTE that was preplanned for subquery FROM visibility and kept
                        // as a definition-only outer ref. The CTE *name* is valid in
                        // principle; it's the column access through it that isn't,
                        // because the CTE hasn't been brought into this scope's FROM.
                        // The `cte_id`/`cte_select` check restricts this to real CTE
                        // definition refs so any other future use of `cte_definition_only`
                        // still falls through to "no such table".
                        let is_definition_only_cte = referenced_tables
                            .find_outer_query_ref_by_identifier(&normalized_table_name)
                            .is_some_and(|outer_ref| {
                                outer_ref.cte_definition_only
                                    && (outer_ref.cte_id.is_some()
                                        || outer_ref.cte_select.is_some())
                            });
                        if is_definition_only_cte {
                            crate::bail_parse_error!(
                                "no such column: {}.{}",
                                tbl.as_str(),
                                id.as_str()
                            );
                        }
                        // Dot-notation fallback for struct/union field access (DuckDB-style precedence).
                        //
                        // For `a.b`, resolution order is:
                        //   1. a=table, b=column       (handled above — if we're here, this failed)
                        //   2. a=column, b=struct field (handled below)
                        //
                        // This means table references always win over struct field access.
                        // If a table `t` has a struct column also named `t` with field `x`,
                        // `t.x` resolves as table.column, not column.field. The user can
                        // disambiguate with an alias: `SELECT s.t.x FROM t AS s`.
                        //
                        // We do NOT reject ambiguous schemas at CREATE TABLE time because
                        // the combinatorial explosion (CREATE TYPE, CREATE TABLE, ALTER TABLE)
                        // makes that impractical. Deterministic precedence is sufficient.
                        let field_name = normalize_ident(id.as_str());
                        if let Some(m) = find_custom_type_column(
                            referenced_tables,
                            &normalized_table_name,
                            resolver,
                        )? {
                            *expr = make_field_access_expr(
                                m.table_id,
                                m.col_idx,
                                m.is_rowid_alias,
                                &field_name,
                                m.type_def,
                            );
                            referenced_tables.mark_column_used(m.table_id, m.col_idx);
                            return Ok(WalkControl::Continue);
                        }
                        crate::bail_parse_error!("no such table: {}", normalized_table_name);
                    }
                    match qualified_match {
                        QualifiedNameMatch::Found(table_id, column) => {
                            bind_qualified_name(expr, referenced_tables, None, table_id, column)
                        }
                        QualifiedNameMatch::Ambiguous => crate::bail_parse_error!(
                            "ambiguous column name: {}.{}",
                            tbl.as_str(),
                            id.as_str()
                        ),
                        QualifiedNameMatch::NoColumn => crate::bail_parse_error!(
                            "no such column: {}.{}",
                            tbl.as_str(),
                            id.as_str()
                        ),
                        QualifiedNameMatch::NoTable => unreachable!("handled above"),
                    }
                    return Ok(WalkControl::Continue);
                }
                Expr::DoublyQualified(db_name, tbl_name, col_name) => {
                    crate::stack::trace_stack!("bind_doubly_qualified");
                    // Clone the names upfront so we can reassign *expr later
                    // without lifetime conflicts.
                    let db_name_str = db_name.as_str().to_string();
                    let tbl_name_str = tbl_name.as_str().to_string();
                    let col_name_str = col_name.as_str().to_string();
                    let tbl_name_clone = tbl_name.clone();
                    let db_name_clone = db_name.clone();

                    let Some(referenced_tables) = &mut referenced_tables else {
                        if binding_behavior == BindingBehavior::AllowUnboundIdentifiers {
                            return Ok(WalkControl::Continue);
                        }
                        crate::bail_parse_error!(
                            "no such column: {}.{}.{}",
                            db_name_str,
                            tbl_name_str,
                            col_name_str
                        );
                    };
                    let normalized_col_name = normalize_ident(&col_name_str);

                    // DoublyQualified: `a.b.c` — DuckDB-style precedence:
                    //   1. a=database, b=table, c=column     (tried first)
                    //   2. a=table,    b=column, c=struct field  (fallback)
                    //
                    // Same principle as Qualified: schema-level references always win.
                    let qualified_name = ast::QualifiedName {
                        db_name: Some(db_name_clone),
                        name: tbl_name_clone,
                        alias: None,
                    };
                    let db_resolution = resolver.resolve_database_id(&qualified_name);

                    if let Ok(database_id) = db_resolution.as_ref() {
                        match resolve_qualified_name(
                            referenced_tables,
                            Some(*database_id),
                            &tbl_name_str,
                            &normalized_col_name,
                        )? {
                            QualifiedNameMatch::Found(table_id, column) => {
                                bind_qualified_name(
                                    expr,
                                    referenced_tables,
                                    Some(*database_id),
                                    table_id,
                                    column,
                                );
                                return Ok(WalkControl::Continue);
                            }
                            QualifiedNameMatch::NoColumn => crate::bail_parse_error!(
                                "no such column: {}.{}.{}",
                                db_name_str,
                                tbl_name_str,
                                col_name_str
                            ),
                            QualifiedNameMatch::Ambiguous => crate::bail_parse_error!(
                                "ambiguous column name: {}.{}.{}",
                                db_name_str,
                                tbl_name_str,
                                col_name_str
                            ),
                            QualifiedNameMatch::NoTable => {}
                        }
                    }

                    // Try db.table.column interpretation first. If database resolves AND
                    // the table+column exist, use that. Otherwise fall through to
                    // table.column.field for struct/union field access.
                    let mut resolved_as_db_table_col = false;
                    if let Ok(database_id) = db_resolution {
                        let table = resolver
                            .with_schema(database_id, |schema| schema.get_table(&tbl_name_str));

                        if let Some(table) = table {
                            let col_idx = table.columns().iter().position(|c| {
                                c.name.as_ref().is_some_and(|name| {
                                    name.eq_ignore_ascii_case(&normalized_col_name)
                                })
                            });

                            if let Some(col_idx) = col_idx {
                                let col = table.columns().get(col_idx).unwrap();
                                let is_rowid_alias = col.is_rowid_alias();
                                let normalized_tbl_name = normalize_ident(&tbl_name_str);
                                let matching_tbl = referenced_tables
                                    .find_table_and_internal_id_by_identifier(&normalized_tbl_name);

                                if let Some((tbl_id, _)) = matching_tbl {
                                    *expr = Expr::Column {
                                        database: Some(database_id),
                                        table: tbl_id,
                                        column: col_idx,
                                        is_rowid_alias,
                                    };
                                    referenced_tables.mark_column_used(tbl_id, col_idx);
                                    resolved_as_db_table_col = true;
                                } else {
                                    // Table exists in database but not in FROM clause
                                    return Err(LimboError::ParseError(format!(
                                        "table {normalized_tbl_name} is not in FROM clause — \
                                         cross-database column references require the table to be explicitly joined"
                                    )));
                                }
                            }
                        }
                    }
                    if !resolved_as_db_table_col {
                        // db.table.column failed — try table.column.field for struct/union
                        let normalized_tbl_name = normalize_ident(&db_name_str);
                        let normalized_col = normalize_ident(&tbl_name_str);
                        let field_name = normalize_ident(&col_name_str);
                        let matching_tbl = referenced_tables
                            .find_table_and_internal_id_by_identifier(&normalized_tbl_name);
                        if let Some((tbl_id, tbl)) = matching_tbl {
                            let col_idx = tbl.columns().iter().position(|c| {
                                c.name
                                    .as_ref()
                                    .is_some_and(|n| n.eq_ignore_ascii_case(&normalized_col))
                            });
                            if let Some(col_idx) = col_idx {
                                let col = &tbl.columns()[col_idx];
                                let type_def =
                                    resolver.schema().get_type_def_unchecked(&col.ty_str);
                                let is_struct_or_union = type_def
                                    .map(|td| td.is_struct() || td.is_union())
                                    .unwrap_or(false);
                                if is_struct_or_union {
                                    *expr = make_field_access_expr(
                                        tbl_id,
                                        col_idx,
                                        col.is_rowid_alias(),
                                        &field_name,
                                        type_def.unwrap(),
                                    );
                                    referenced_tables.mark_column_used(tbl_id, col_idx);
                                    return Ok(WalkControl::Continue);
                                } else {
                                    // Column exists but is not a struct/union type
                                    return Err(LimboError::ParseError(format!(
                                        "column '{normalized_col}' is not a STRUCT or UNION type; \
                                         cannot access field '{field_name}'"
                                    )));
                                }
                            }
                        }
                        // Fallback (3): column.field.subfield for nested struct/union access
                        // Handles:
                        //   data.telegram.chat_id — UNION column, variant with struct type
                        //   data.sub.a            — STRUCT column, struct-typed field, sub-field
                        let col_name_norm = normalize_ident(&db_name_str);
                        let mid_name = normalize_ident(&tbl_name_str);
                        let leaf_field = normalize_ident(&col_name_str);
                        if let Some((nested_expr, tbl_id, col_idx)) =
                            try_resolve_nested_field_access(
                                referenced_tables,
                                &col_name_norm,
                                &mid_name,
                                &leaf_field,
                                resolver,
                            )?
                        {
                            *expr = nested_expr;
                            referenced_tables.mark_column_used(tbl_id, col_idx);
                        } else {
                            return Err(LimboError::ParseError(format!(
                                "no such column: {db_name_str}.{tbl_name_str}.{col_name_str}"
                            )));
                        }
                    }
                }
                Expr::FunctionCallStar { name, filter_over } => {
                    // For functions that need star expansion (json_object, jsonb_object),
                    // expand the * to all columns from the referenced tables as key-value pairs
                    // This needs to happen during bind/rewrite so WHERE clauses can use these functions
                    if let Some(referenced_tables) = &mut referenced_tables {
                        if let Ok(Some(func)) = Func::resolve_function(name.as_str(), 0) {
                            if func.needs_star_expansion() {
                                // Only expand if there are actual tables - otherwise leave as
                                // FunctionCallStar so translate_expr can generate the error
                                let joined_tables = referenced_tables.joined_tables();
                                if !joined_tables.is_empty() {
                                    // Mark all columns as used so the optimizer doesn't
                                    // create partial covering indexes that would miss columns
                                    let joined_tables = referenced_tables.joined_tables_mut();
                                    for table in joined_tables.iter_mut() {
                                        for col_idx in 0..table.columns().len() {
                                            table.mark_column_used(col_idx);
                                        }
                                    }

                                    // Build arguments: alternating column_name (as string literal), column_value (as column reference)
                                    let mut args: Vec<Box<ast::Expr>> = Vec::new();

                                    let joined_tables = referenced_tables.joined_tables();
                                    for table in joined_tables.iter() {
                                        for (col_idx, col) in table.columns().iter().enumerate() {
                                            // Skip hidden columns (like rowid in some cases)
                                            if col.hidden() {
                                                continue;
                                            }

                                            // Add column name as a string literal
                                            let col_name = col.name.clone().unwrap_or_else(|| {
                                                format!("column{}", col_idx + 1)
                                            });
                                            let quoted_col_name = format!("'{col_name}'");
                                            args.push(Box::new(ast::Expr::Literal(
                                                ast::Literal::String(quoted_col_name),
                                            )));

                                            // Add column reference using Expr::Column
                                            args.push(Box::new(ast::Expr::Column {
                                                database: None,
                                                table: table.internal_id,
                                                column: col_idx,
                                                is_rowid_alias: col.is_rowid_alias(),
                                            }));
                                        }
                                    }

                                    // Replace FunctionCallStar with expanded FunctionCall
                                    *expr = ast::Expr::FunctionCall {
                                        name: name.clone(),
                                        distinctness: None,
                                        args,
                                        filter_over: filter_over.clone(),
                                        order_by: vec![],
                                        within_group: vec![],
                                    };
                                }
                            }
                        }
                    }
                }
                // Validate struct/union function calls at bind time.
                // Principle: compile-time checks belong in the earliest phase that
                // has enough context. Binding has the resolver (for custom-types
                // gate) and the raw AST args (for arity and literal checks).
                // Catching errors here avoids wasting optimizer and translation
                // cycles on invalid queries, and keeps the translate_expr match
                // arms focused on code generation.
                Expr::FunctionCall { name, args, .. } => {
                    validate_custom_type_function_call(name.as_str(), args, resolver)?;
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        },
    )?;
    Ok(())
}

/// Find one column by its unqualified name.
///
/// A parenthesized join can keep several source columns with the same name.
/// Hidden source copies do not take part in an unqualified lookup. A visible
/// column wins over an implicit rowid. Other duplicate names remain ambiguous.
pub(in crate::translate) fn find_unqualified_column(
    table: &Table,
    column_name: &str,
) -> Result<Option<usize>> {
    let join_columns = match table {
        Table::FromClauseSubquery(subquery) => subquery.parenthesized_join_columns.as_ref(),
        _ => None,
    };
    let Some(join_columns) = join_columns else {
        return Ok(table
            .get_column_by_name(column_name)
            .map(|(column_index, _)| column_index));
    };

    let mut column = None;
    let mut rowid_column = None;
    let mut rowid_is_ambiguous = false;
    for (column_index, saved_column) in join_columns.iter().enumerate() {
        if !saved_column.source.matches_column_name(column_name) {
            continue;
        }
        if saved_column.source.is_rowid() {
            rowid_is_ambiguous |= rowid_column.replace(column_index).is_some();
        } else if saved_column.visibility != ParenthesizedJoinColumnVisibility::QualifiedOnly
            && column.replace(column_index).is_some()
        {
            crate::bail_parse_error!("ambiguous column name: {}", column_name);
        }
    }
    if column.is_none() && rowid_is_ambiguous {
        crate::bail_parse_error!("ambiguous column name: {}", column_name);
    }
    Ok(column.or(rowid_column))
}

/// Search the current query first, then search the nearest outer query.
fn resolve_qualified_name(
    table_references: &TableReferences,
    database_id: Option<usize>,
    table_name: &str,
    column_name: &str,
) -> Result<QualifiedNameMatch> {
    let mut table_found = false;
    let mut found = None;
    for joined_table in table_references.joined_tables() {
        let candidate = match_qualified_name_in_table(
            &joined_table.table,
            joined_table.internal_id,
            &joined_table.identifier,
            database_id,
            table_name,
            column_name,
        )?;
        match candidate {
            QualifiedTableMatch::NoTable => continue,
            QualifiedTableMatch::NoColumn => table_found = true,
            QualifiedTableMatch::Ambiguous => return Ok(QualifiedNameMatch::Ambiguous),
            QualifiedTableMatch::Found(column) => {
                table_found = true;
                if found.is_some() {
                    let duplicate_is_merged = matches!(column, QualifiedMatch::Column { .. })
                        && joined_table
                            .join_info
                            .as_ref()
                            .is_some_and(|join| join.merges_column(column_name));
                    if !duplicate_is_merged {
                        return Ok(QualifiedNameMatch::Ambiguous);
                    }
                } else {
                    found = Some((joined_table.internal_id, column));
                }
            }
        }
    }
    if table_found {
        return Ok(
            found.map_or(QualifiedNameMatch::NoColumn, |(table_id, column)| {
                QualifiedNameMatch::Found(table_id, column)
            }),
        );
    }

    // An inner table name hides the same name in outer queries. If no inner
    // table matches, only the nearest outer query can provide the column.
    let mut nearest_scope = None;
    let mut ambiguous = false;
    for outer_ref in table_references.outer_query_refs() {
        if outer_ref.cte_definition_only {
            continue;
        }
        if nearest_scope.is_some_and(|scope| outer_ref.scope_depth > scope) {
            continue;
        }
        let candidate = match_qualified_name_in_table(
            &outer_ref.table,
            outer_ref.internal_id,
            &outer_ref.identifier,
            database_id,
            table_name,
            column_name,
        )?;
        if matches!(candidate, QualifiedTableMatch::NoTable) {
            continue;
        }
        if nearest_scope.is_none_or(|scope| outer_ref.scope_depth < scope) {
            nearest_scope = Some(outer_ref.scope_depth);
            found = None;
            ambiguous = false;
        }
        match candidate {
            QualifiedTableMatch::Ambiguous => ambiguous = true,
            QualifiedTableMatch::Found(column) if found.is_some() => {
                let duplicate_is_merged = matches!(
                    column,
                    QualifiedMatch::Column { column_index, .. }
                        if outer_ref.using_dedup_hidden_cols.get(column_index)
                );
                ambiguous |= !duplicate_is_merged;
            }
            QualifiedTableMatch::Found(column) => {
                found = Some((outer_ref.internal_id, column));
            }
            QualifiedTableMatch::NoTable | QualifiedTableMatch::NoColumn => {}
        }
    }

    if ambiguous {
        Ok(QualifiedNameMatch::Ambiguous)
    } else if let Some((table_id, column)) = found {
        Ok(QualifiedNameMatch::Found(table_id, column))
    } else if nearest_scope.is_some() {
        Ok(QualifiedNameMatch::NoColumn)
    } else {
        Ok(QualifiedNameMatch::NoTable)
    }
}

/// Match a qualified name against one table reference.
///
/// SQLite lets a parenthesized join keep the names of its inner tables.
/// A group alias remains available only when no saved inner name matches.
fn match_qualified_name_in_table(
    table: &Table,
    table_id: TableInternalId,
    table_reference_name: &str,
    database_id: Option<usize>,
    table_name: &str,
    column_name: &str,
) -> Result<QualifiedTableMatch> {
    if let Table::FromClauseSubquery(subquery) = table {
        if let Some(join_columns) = &subquery.parenthesized_join_columns {
            let mut table_found = false;
            let mut real_column = None;
            let mut rowid_column = None;
            let mut rowid_is_ambiguous = false;
            for (column_index, saved_column) in join_columns.iter().enumerate() {
                if !saved_column.source.matches_table(database_id, table_name) {
                    continue;
                }
                table_found = true;
                if !saved_column.source.matches_column_name(column_name) {
                    continue;
                }
                if saved_column.source.is_rowid() {
                    rowid_is_ambiguous |= rowid_column.replace(column_index).is_some();
                } else if real_column.replace(column_index).is_some() {
                    return Ok(QualifiedTableMatch::Ambiguous);
                }
            }
            // A real column hides any implicit rowid candidate with the same
            // name. Two rowid candidates are ambiguous only when no real column wins.
            if real_column.is_none() && rowid_is_ambiguous {
                return Ok(QualifiedTableMatch::Ambiguous);
            }
            if let Some(column_index) = real_column.or(rowid_column) {
                let column = &table.columns()[column_index];
                return Ok(QualifiedTableMatch::Found(QualifiedMatch::Column {
                    column_index,
                    is_rowid_alias: column.is_rowid_alias(),
                }));
            }
            if table_found && !table_reference_name.eq_ignore_ascii_case(table_name) {
                return Ok(QualifiedTableMatch::NoColumn);
            }
        }
    }

    // The normal database.table.column path uses schema metadata in the
    // caller. Only a parenthesized join can match a saved database name here.
    if database_id.is_some() || !table_reference_name.eq_ignore_ascii_case(table_name) {
        return Ok(QualifiedTableMatch::NoTable);
    }
    if let Some((column_index, column)) = table.get_column_by_name(column_name) {
        return Ok(QualifiedTableMatch::Found(QualifiedMatch::Column {
            column_index,
            is_rowid_alias: column.is_rowid_alias(),
        }));
    }
    if let Table::BTree(btree) = table {
        if parse_row_id(column_name, table_id, || false)?.is_some() {
            if !btree.has_rowid {
                crate::bail_parse_error!("no such column: {}", column_name);
            }
            return Ok(QualifiedTableMatch::Found(QualifiedMatch::RowId));
        }
    }
    Ok(QualifiedTableMatch::NoColumn)
}

/// Replace a qualified name with its bound column and record the read.
fn bind_qualified_name(
    expr: &mut Expr,
    table_references: &mut TableReferences,
    database_id: Option<usize>,
    table_id: TableInternalId,
    column: QualifiedMatch,
) {
    match column {
        QualifiedMatch::Column {
            column_index,
            is_rowid_alias,
        } => {
            *expr = Expr::Column {
                database: database_id,
                table: table_id,
                column: column_index,
                is_rowid_alias,
            };
            tracing::debug!("rewritten to column");
            table_references.mark_column_used(table_id, column_index);
        }
        QualifiedMatch::RowId => {
            *expr = Expr::RowId {
                database: database_id,
                table: table_id,
            };
            tracing::debug!("rewritten to rowid");
            table_references.mark_rowid_referenced(table_id);
        }
    }
}

/// Extract a string literal value from an expression that has already been
/// validated as `Expr::Literal(Literal::String(_))` during bind-time checks.
pub(super) fn extract_string_literal(expr: &ast::Expr) -> crate::Result<String> {
    match expr {
        ast::Expr::Literal(ast::Literal::String(s)) => Ok(s.trim_matches('\'').to_string()),
        _ => crate::bail_parse_error!("expected a string literal argument"),
    }
}

/// Resolve the UnionDef for a column expression. Returns the variant names list
/// and optionally resolves a tag name to its numeric index.
/// Used by union_value, union_tag, union_extract function translation.
///
/// In the DML index-maintenance path (INSERT with expression indexes),
/// `referenced_tables` is `None` and columns use `SELF_TABLE`. We fall back
/// to the Resolver's `SelfTableContext::ForDML` to obtain column metadata.
/// Resolve the TypeDef for a column expression (Column or DML self-table column).
pub(super) fn resolve_typedef_from_column(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<Arc<TypeDef>> {
    let ty_str = match expr {
        ast::Expr::Column { table, column, .. } => {
            resolve_column_type_str(*table, *column, referenced_tables, resolver)?
        }
        ast::Expr::Variable(var) => var.col_type.as_ref()?.to_string(),
        _ => return None,
    };
    let td = resolver.schema().get_type_def_unchecked(&ty_str)?;
    Some(Arc::clone(td))
}

pub(super) fn resolve_union_from_column(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<Arc<TypeDef>> {
    resolve_typedef_from_column(expr, referenced_tables, resolver).filter(|td| td.is_union())
}

/// Resolve the struct TypeDef that an expression evaluates to.
///
/// Handles column references (direct struct column),
/// `union_extract(...)` (variant's struct type), and
/// `struct_extract(...)` (field's struct type for nested extraction).
pub(super) fn resolve_struct_from_expr(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<Arc<TypeDef>> {
    match expr {
        ast::Expr::Column { .. } => resolve_typedef_from_column(expr, referenced_tables, resolver)
            .filter(|td| td.is_struct()),
        ast::Expr::FunctionCall { name, args, .. } => {
            let normalized = crate::util::normalize_ident(name.as_str());
            match normalized.as_str() {
                // union_extract(col, 'tag') → variant's type
                "union_extract" if args.len() == 2 => {
                    let tag_name = extract_string_literal(&args[1]).ok()?;
                    let union_td =
                        resolve_union_from_column(&args[0], referenced_tables, resolver)?;
                    let (_, variant) = union_td.find_union_variant(&tag_name)?;
                    let struct_td = resolver
                        .schema()
                        .get_type_def_unchecked(&variant.type_name)?;
                    if struct_td.is_struct() {
                        Some(Arc::clone(struct_td))
                    } else {
                        None
                    }
                }
                // struct_extract(expr, 'field') → field's type (if it's a struct)
                "struct_extract" if args.len() == 2 => {
                    let field_name = extract_string_literal(&args[1]).ok()?;
                    let parent_td =
                        resolve_struct_from_expr(&args[0], referenced_tables, resolver)?;
                    let (_, field_def) = parent_td.find_struct_field(&field_name)?;
                    let field_td = resolver
                        .schema()
                        .get_type_def_unchecked(&field_def.type_name)?;
                    if field_td.is_struct() {
                        Some(Arc::clone(field_td))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Get the type string for a column
pub(super) fn resolve_column_type_str(
    table: ast::TableInternalId,
    column: usize,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<String> {
    if let Some(rt) = referenced_tables {
        if let Some((_, tbl)) = rt.find_table_by_internal_id(table) {
            return Some(tbl.columns().get(column)?.ty_str.clone());
        }
    }
    if table.is_self_table() {
        return resolver.self_table_column_type_str(column);
    }
    None
}

/// Result of finding a column with a custom (struct/union) type across joined tables.
pub(super) struct CustomTypeColumnMatch<'a> {
    table_id: TableInternalId,
    col_idx: usize,
    is_rowid_alias: bool,
    type_def: &'a crate::schema::TypeDef,
}

/// Search all joined tables for a column named `col_name` with a struct/union type.
/// Errors on ambiguity (>1 match). Returns `None` if no match.
#[turso_macros::trace_stack]
pub(super) fn find_custom_type_column<'a>(
    referenced_tables: &TableReferences,
    col_name: &str,
    resolver: &'a Resolver<'a>,
) -> crate::Result<Option<CustomTypeColumnMatch<'a>>> {
    let mut result: Option<CustomTypeColumnMatch<'a>> = None;
    let mut match_count = 0usize;
    for joined_table in referenced_tables.joined_tables().iter() {
        let cols = joined_table.table.columns();
        if let Some(col_idx) = cols.iter().position(|c| {
            c.name
                .as_ref()
                .is_some_and(|n| n.eq_ignore_ascii_case(col_name))
        }) {
            let col = &cols[col_idx];
            let type_def = resolver.schema().get_type_def_unchecked(&col.ty_str);
            let is_struct_or_union = type_def
                .map(|td| td.is_struct() || td.is_union())
                .unwrap_or(false);
            if is_struct_or_union {
                match_count += 1;
                result = Some(CustomTypeColumnMatch {
                    table_id: joined_table.internal_id,
                    col_idx,
                    is_rowid_alias: col.is_rowid_alias(),
                    type_def: type_def.unwrap(),
                });
            }
        }
    }
    if match_count > 1 {
        crate::bail_parse_error!(
            "ambiguous column reference: '{}' — multiple tables have a struct/union column with this name",
            col_name
        );
    }
    Ok(result)
}

/// Build an `Expr::FieldAccess { base: Expr::Column { ... }, field, resolved }` node,
/// pre-resolving the field index via `resolve_field_access`.
pub(super) fn make_field_access_expr(
    table_id: TableInternalId,
    col_idx: usize,
    is_rowid_alias: bool,
    field_name: &str,
    td: &crate::schema::TypeDef,
) -> Expr {
    let resolved = resolve_field_access(td, field_name);
    Expr::FieldAccess {
        base: Box::new(Expr::Column {
            database: None,
            table: table_id,
            column: col_idx,
            is_rowid_alias,
        }),
        field: ast::Name::from_bytes(field_name.as_bytes()),
        resolved,
    }
}

/// Try to resolve `col_name.mid_name.leaf_name` as 2-level deep field access
/// (e.g. `data.telegram.chat_id` where `data` is a UNION column, `telegram` is
/// a variant with struct type, and `chat_id` is a struct field).
///
/// Returns the nested FieldAccess expr plus table_id/col_idx for `mark_column_used`.
pub(super) fn try_resolve_nested_field_access<'a>(
    referenced_tables: &TableReferences,
    col_name: &str,
    mid_name: &str,
    leaf_name: &str,
    resolver: &'a Resolver<'a>,
) -> crate::Result<Option<(Expr, TableInternalId, usize)>> {
    let m = find_custom_type_column(referenced_tables, col_name, resolver)?;
    let Some(m) = m else {
        return Ok(None);
    };
    let td = m.type_def;

    // Resolve the inner type name reached via mid_name:
    // Case A: UNION column — mid_name is a variant tag
    // Case B: STRUCT column — mid_name is a struct field
    let inner_type_name = td
        .find_union_variant(mid_name)
        .map(|(_, v)| v.type_name.as_str())
        .or_else(|| {
            td.find_struct_field(mid_name)
                .map(|(_, f)| f.type_name.as_str())
        });

    let has_leaf = inner_type_name
        .and_then(|tn| resolver.schema().get_type_def_unchecked(tn))
        .is_some_and(|itd| itd.find_struct_field(leaf_name).is_some());

    if !has_leaf {
        return Ok(None);
    }

    let nested_expr = Expr::FieldAccess {
        base: Box::new(Expr::FieldAccess {
            base: Box::new(Expr::Column {
                database: None,
                table: m.table_id,
                column: m.col_idx,
                is_rowid_alias: m.is_rowid_alias,
            }),
            field: ast::Name::from_bytes(mid_name.as_bytes()),
            resolved: None,
        }),
        field: ast::Name::from_bytes(leaf_name.as_bytes()),
        resolved: None,
    };

    Ok(Some((nested_expr, m.table_id, m.col_idx)))
}

/// Resolve a field/variant name against a TypeDef to produce a FieldAccessResolution.
pub(super) fn resolve_field_access(
    td: &crate::schema::TypeDef,
    field_name: &str,
) -> Option<ast::FieldAccessResolution> {
    if let Some((idx, _)) = td.find_struct_field(field_name) {
        Some(ast::FieldAccessResolution::StructField { field_index: idx })
    } else if let Some((tag_idx, _)) = td.find_union_variant(field_name) {
        Some(ast::FieldAccessResolution::UnionVariant { tag_index: tag_idx })
    } else {
        None
    }
}

/// Recursively resolve the output TypeDef of an expression.
///
/// For `Expr::Column`, returns the column's declared custom type.
/// For `Expr::FieldAccess`, recurses into the base to find the parent type,
/// then looks up what type the accessed field/variant produces.
/// Returns `None` for expressions that don't produce a known custom type.
pub(super) fn resolve_expr_output_type<'a>(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &'a Resolver<'a>,
) -> crate::Result<&'a crate::schema::TypeDef> {
    match expr {
        ast::Expr::Column { table, column, .. } => {
            let Some(referenced_tables) = referenced_tables else {
                crate::bail_parse_error!("cannot resolve type: no table context");
            };
            let Some((_is_outer, tbl)) = referenced_tables.find_table_by_internal_id(*table) else {
                crate::bail_parse_error!("cannot resolve type: table not found");
            };
            let col = &tbl.columns()[*column];
            let Some(td) = resolver.schema().get_type_def_unchecked(&col.ty_str) else {
                crate::bail_parse_error!(
                    "column '{}' has type '{}' which is not a known struct or union type",
                    col.name.as_deref().unwrap_or("?"),
                    col.ty_str
                );
            };
            Ok(td)
        }
        ast::Expr::FieldAccess { base, field, .. } => {
            let parent_td = resolve_expr_output_type(base, referenced_tables, resolver)?;
            let field_name = normalize_ident(field.as_str());
            // Find what type this field/variant produces
            let inner_type_name =
                if let Some((_, variant)) = parent_td.find_union_variant(&field_name) {
                    &variant.type_name
                } else if let Some((_, f)) = parent_td.find_struct_field(&field_name) {
                    &f.type_name
                } else {
                    let kind = if parent_td.is_union() {
                        "variant"
                    } else {
                        "field"
                    };
                    crate::bail_parse_error!("no such {} '{}' in type", kind, field_name);
                };
            let Some(td) = resolver.schema().get_type_def_unchecked(inner_type_name) else {
                crate::bail_parse_error!(
                    "'{}' resolves to type '{}' which is not a known type",
                    field_name,
                    inner_type_name
                );
            };
            Ok(td)
        }
        _ => {
            crate::bail_parse_error!("expression does not produce a known custom type");
        }
    }
}

/// Validates custom-type function calls (arrays, structs, unions) at bind time.
///
/// Compile-time checks belong in the earliest phase that has enough context.
/// Binding has the resolver (for the custom-types gate) and the raw AST args
/// (for arity and literal checks). Catching errors here avoids wasting
/// optimizer and translation cycles on invalid queries, and keeps the
/// translate_expr match arms focused purely on register allocation and codegen.
pub(super) fn validate_custom_type_function_call(
    name: &str,
    args: &[Box<ast::Expr>],
    resolver: &Resolver<'_>,
) -> Result<()> {
    let normalized = crate::util::normalize_ident(name);
    match normalized.as_str() {
        // Arrays
        "array" | "array_element" | "array_set_element" | "array_length" | "array_append"
        | "array_prepend" | "array_cat" | "array_remove" | "array_contains" | "array_position"
        | "array_slice" | "string_to_array" | "array_to_string" | "array_overlap"
        | "array_contains_all" => {
            resolver.require_custom_types("Array features")?;
        }
        // Structs
        "struct_pack" => {
            resolver.require_custom_types("Struct features")?;
        }
        "struct_extract" => {
            resolver.require_custom_types("Struct features")?;
            if args.len() != 2 {
                crate::bail_parse_error!("struct_extract() requires exactly 2 arguments");
            }
            if !matches!(&*args[1], ast::Expr::Literal(ast::Literal::String(_))) {
                crate::bail_parse_error!(
                    "struct_extract() second argument must be a string literal"
                );
            }
        }
        // Unions
        "union_value" => {
            resolver.require_custom_types("Union features")?;
            if args.len() != 2 {
                crate::bail_parse_error!("union_value() requires exactly 2 arguments");
            }
            if !matches!(&*args[0], ast::Expr::Literal(ast::Literal::String(_))) {
                crate::bail_parse_error!("union_value() first argument must be a string literal");
            }
        }
        "union_tag" => {
            resolver.require_custom_types("Union features")?;
            if args.len() != 1 {
                crate::bail_parse_error!("union_tag() requires exactly 1 argument");
            }
        }
        "union_extract" => {
            resolver.require_custom_types("Union features")?;
            if args.len() != 2 {
                crate::bail_parse_error!("union_extract() requires exactly 2 arguments");
            }
            if !matches!(&*args[1], ast::Expr::Literal(ast::Literal::String(_))) {
                crate::bail_parse_error!(
                    "union_extract() second argument must be a string literal"
                );
            }
        }
        _ => {}
    }
    Ok(())
}
