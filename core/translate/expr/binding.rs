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

/// The result of resolving the `<id>` half of a qualified `<tbl>.<id>`
/// reference against a single candidate table whose identifier already
/// matches `<tbl>`.
#[derive(Debug, Clone, Copy)]
pub(super) enum QualifiedMatch {
    /// `<id>` named a real column on the candidate table.
    Column {
        col_idx: usize,
        is_rowid_alias: bool,
    },
    /// `<id>` named the rowid (`rowid`/`oid`/`_rowid_`) of a btree.
    /// There is no column index — the result must become an
    /// `Expr::RowId`, not an `Expr::Column`.
    RowId,
}

/// Resolve `<id>` against a single table reference.
///
/// The caller is responsible for:
///   * filtering candidate refs down to those whose identifier matches `<tbl>`,
///   * detecting ambiguity across multiple candidate refs,
///   * applying any scope-specific USING/NATURAL dedup rules.
///
/// Returns:
///   * `Ok(Some(Column { .. }))` — `<id>` is a real column on `table`.
///   * `Ok(Some(RowId))` — `<id>` is a rowid alias on a rowid btree.
///   * `Ok(None)` — `<id>` is not present on this ref.
///   * `Err(_)` — `<id>` is a rowid alias but the btree has no rowid
///     (definitively invalid; reported as "no such column: <id>" per SQLite).
pub(super) fn resolve_qualified_on_ref(
    table: &Table,
    internal_id: TableInternalId,
    normalized_id: &str,
) -> Result<Option<QualifiedMatch>> {
    if let Some(col_idx) = table.columns().iter().position(|c| {
        c.name
            .as_ref()
            .is_some_and(|name| name.eq_ignore_ascii_case(normalized_id))
    }) {
        let col = table.columns().get(col_idx).unwrap();
        return Ok(Some(QualifiedMatch::Column {
            col_idx,
            is_rowid_alias: col.is_rowid_alias(),
        }));
    }

    if let Table::BTree(btree) = table {
        if parse_row_id(normalized_id, internal_id, || false)?.is_some() {
            if !btree.has_rowid {
                crate::bail_parse_error!("no such column: {}", normalized_id);
            }
            return Ok(Some(QualifiedMatch::RowId));
        }
    }

    Ok(None)
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
                    let mut match_result = None;
                    let joined_tables = referenced_tables.joined_tables();

                    // First check joined tables
                    for joined_table in joined_tables.iter() {
                        let col_idx = joined_table.table.columns().iter().position(|c| {
                            c.name
                                .as_ref()
                                .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                        });
                        if col_idx.is_some() {
                            if match_result.is_some() {
                                let mut ok = false;
                                // Column name ambiguity is ok if it is in the USING clause because then it is deduplicated
                                // and the left table is used.
                                if let Some(join_info) = &joined_table.join_info {
                                    if join_info.using.iter().any(|using_col| {
                                        using_col.as_str().eq_ignore_ascii_case(&normalized_id)
                                    }) {
                                        ok = true;
                                    }
                                }
                                if !ok {
                                    crate::bail_parse_error!(
                                        "ambiguous column name: {}",
                                        id.as_str()
                                    );
                                }
                            } else {
                                let col =
                                    joined_table.table.columns().get(col_idx.unwrap()).unwrap();
                                match_result = Some((
                                    joined_table.internal_id,
                                    col_idx.unwrap(),
                                    col.is_rowid_alias(),
                                ));
                            }
                        // only if we haven't found a match, check for explicit rowid reference
                        } else if let Table::BTree(btree) = &joined_table.table {
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
                            // CTEs (FromClauseSubquery) in outer_query_refs are only for table
                            // lookup (e.g., FROM cte1), not for column resolution. Columns from
                            // CTEs should only be accessible when the CTE is explicitly in the
                            // FROM clause, not as implicit outer references.
                            if matches!(outer_ref.table, Table::FromClauseSubquery(_)) {
                                continue;
                            }
                            // Skip refs from deeper scopes once we found a match
                            if let Some(depth) = matched_scope_depth {
                                if outer_ref.scope_depth > depth {
                                    continue;
                                }
                            }
                            let col_idx = outer_ref.table.columns().iter().position(|c| {
                                c.name
                                    .as_ref()
                                    .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                            });
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

                    // `resolved` holds the accepted binding (at most one).
                    // `identifier_matched` is true once *any* scope produced a table whose
                    // identifier equals `tbl`; it distinguishes "no such table" from
                    // "no such column" in error reporting below.
                    let mut resolved: Option<(TableInternalId, QualifiedMatch)> = None;
                    let mut identifier_matched = false;

                    let ambiguous = || -> LimboError {
                        LimboError::ParseError(format!(
                            "ambiguous column name: {}.{}",
                            tbl.as_str(),
                            id.as_str()
                        ))
                    };

                    // --- Stage 1: search the current scope's FROM tables. ---
                    for joined_table in referenced_tables
                        .joined_tables()
                        .iter()
                        .filter(|t| t.identifier == normalized_table_name)
                    {
                        identifier_matched = true;
                        let Some(candidate) = resolve_qualified_on_ref(
                            &joined_table.table,
                            joined_table.internal_id,
                            &normalized_id,
                        )?
                        else {
                            continue;
                        };

                        // Multiple FROM tables share this identifier and both contain `id`.
                        // For column matches, a USING/NATURAL join on `id` lets the first
                        // match stand (the duplicate side is implicitly merged). Rowid
                        // matches never get this exception (rowid isn't a USING column).
                        if resolved.is_some() {
                            let allowed_by_using =
                                matches!(candidate, QualifiedMatch::Column { .. })
                                    && joined_table.join_info.as_ref().is_some_and(|ji| {
                                        ji.using.iter().any(|u| {
                                            u.as_str().eq_ignore_ascii_case(&normalized_id)
                                        })
                                    });
                            if !allowed_by_using {
                                return Err(ambiguous());
                            }
                            continue;
                        }
                        resolved = Some((joined_table.internal_id, candidate));
                    }
                    // --- Stage 2: fall back to enclosing scopes ---
                    // Only attempted if no inner-scope table matched the identifier — an
                    // inner alias of the same name shadows everything outside.
                    //
                    // We pick the *nearest* outer scope that contains a matching identifier
                    // (smallest `scope_depth`) and search only refs at that depth. This lets
                    // the same alias be reused at different nesting levels without triggering
                    // spurious "ambiguous column" errors across unrelated scopes.
                    //
                    // `cte_definition_only` refs are excluded: those entries exist purely so
                    // that a subquery's FROM clause can *look up* the CTE by name; once the
                    // CTE is consumed into a FROM, column resolution must go through the
                    // corresponding `joined_table`, not the definition-only ref.
                    if !identifier_matched {
                        let nearest_outer_scope = referenced_tables
                            .outer_query_refs()
                            .iter()
                            .filter(|t| {
                                !t.cte_definition_only && t.identifier == normalized_table_name
                            })
                            .map(|t| t.scope_depth)
                            .min();

                        if let Some(scope_depth) = nearest_outer_scope {
                            identifier_matched = true;
                            for outer_ref in
                                referenced_tables.outer_query_refs().iter().filter(|t| {
                                    !t.cte_definition_only
                                        && t.scope_depth == scope_depth
                                        && t.identifier == normalized_table_name
                                })
                            {
                                let Some(candidate) = resolve_qualified_on_ref(
                                    &outer_ref.table,
                                    outer_ref.internal_id,
                                    &normalized_id,
                                )?
                                else {
                                    continue;
                                };

                                // When multiple outer refs share this identifier
                                // (e.g. self-join `t1 JOIN t1 USING(a)`), a USING-hidden
                                // column on the duplicate side lets the first match stand,
                                // mirroring the Stage 1 logic for local-scope tables.
                                if resolved.is_some() {
                                    let allowed_by_using = matches!(
                                        candidate,
                                        QualifiedMatch::Column { col_idx, .. }
                                            if outer_ref.using_dedup_hidden_cols.get(col_idx)
                                    );
                                    if !allowed_by_using {
                                        return Err(ambiguous());
                                    }
                                    continue;
                                }
                                resolved = Some((outer_ref.internal_id, candidate));
                            }
                        }
                    }

                    // --- Error reporting. ---
                    if resolved.is_none() && !identifier_matched {
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
                    // Identifier matched somewhere but no column/rowid binding was
                    // produced — the table exists, the column doesn't.
                    let Some((tbl_id, binding)) = resolved else {
                        crate::bail_parse_error!("no such column: {}", normalized_id);
                    };

                    match binding {
                        QualifiedMatch::Column {
                            col_idx,
                            is_rowid_alias,
                        } => {
                            *expr = Expr::Column {
                                database: None, // TODO: support different databases
                                table: tbl_id,
                                column: col_idx,
                                is_rowid_alias,
                            };
                            tracing::debug!("rewritten to column");
                            referenced_tables.mark_column_used(tbl_id, col_idx);
                        }
                        QualifiedMatch::RowId => {
                            *expr = Expr::RowId {
                                database: None, // TODO: support different databases
                                table: tbl_id,
                            };
                            tracing::debug!("rewritten to rowid");
                            referenced_tables.mark_rowid_referenced(tbl_id);
                        }
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
                                "no such column or database: {db_name_str}.{tbl_name_str}.{col_name_str}"
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
