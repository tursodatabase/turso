use crate::alloc::TursoIteratorExt;
use crate::namespace::NamespaceId;
use crate::schema::view::{extract_view_columns, ViewColumnSchema};
use crate::translate::expr::{walk_expr_mut, WalkControl};
use crate::util::normalize_ident;
use crate::{
    schema::{Column, Schema},
    LimboError, Result,
};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast;

pub fn rewrite_fk_parent_cols_if_self_ref(
    clause: &mut ast::ForeignKeyClause,
    table: &str,
    from: &str,
    to: &str,
) {
    if clause.tbl_name.as_str().eq_ignore_ascii_case(table) {
        for c in &mut clause.columns {
            if c.col_name.as_str().eq_ignore_ascii_case(from) {
                c.col_name = ast::Name::exact(to.to_owned());
            }
        }
    }
}

/// Rewrite column name references; used in e.g. ALTER TABLE RENAME COLUMN
/// to rewrite references to the old column name to the new column name.
/// Replaces `Id(old)` and `Name(old)` with `Id(new)`, and updates the
/// column name in `Qualified(tbl, old)` references.
pub fn rename_identifiers(expr: &mut ast::Expr, from: &str, to: &str) {
    // The closure is infallible, so walk_expr_mut cannot fail.
    let _ = walk_expr_mut(
        expr,
        &mut |e: &mut ast::Expr| -> crate::Result<WalkControl> {
            match e {
                ast::Expr::Id(ref name) | ast::Expr::Name(ref name)
                    if name.as_str().eq_ignore_ascii_case(from) =>
                {
                    *e = ast::Expr::Id(ast::Name::exact(to.to_owned()));
                }
                ast::Expr::Qualified(ref tbl, ref col_name)
                    if col_name.as_str().eq_ignore_ascii_case(from) =>
                {
                    let tbl = tbl.clone();
                    *e = ast::Expr::Qualified(tbl, ast::Name::exact(to.to_owned()));
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        },
    );
}

mod rename_column_view {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct RewrittenView {
        pub sql: String,
        pub select_stmt: ast::Select,
        pub columns: crate::alloc::Vec<Column>,
    }

    /// `namespace` is the namespace of the view, where its names resolve.
    #[allow(clippy::too_many_arguments)]
    pub fn rewrite_view_sql_for_column_rename(
        dialect: &dyn crate::dialect::Dialect,
        view_sql: &str,
        schema: &Schema,
        namespace: NamespaceId,
        target_table: &str,
        target_db_name: &str,
        old_column: &str,
        new_column: &str,
    ) -> Result<Option<RewrittenView>> {
        let mut visiting_views = HashSet::default();
        rewrite_view_sql_for_column_rename_inner(
            dialect,
            view_sql,
            schema,
            namespace,
            target_table,
            target_db_name,
            old_column,
            new_column,
            &mut visiting_views,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn rewrite_view_sql_for_column_rename_inner(
        dialect: &dyn crate::dialect::Dialect,
        view_sql: &str,
        schema: &Schema,
        namespace: NamespaceId,
        target_table: &str,
        target_db_name: &str,
        old_column: &str,
        new_column: &str,
        visiting_views: &mut HashSet<String>,
    ) -> Result<Option<RewrittenView>> {
        let cmd = crate::dialect::parse_statement(dialect, view_sql)
            .map_err(|e| LimboError::ParseError(format!("failed to parse view SQL: {e}")))?;
        let Some(ast::Cmd::Stmt(ast::Stmt::CreateView {
            temporary,
            if_not_exists,
            view_name,
            columns: view_columns,
            mut select,
        })) = cmd
        else {
            return Ok(None);
        };

        let current_view_name = normalize_ident(view_name.name.as_str());
        if !visiting_views.insert(current_view_name.clone()) {
            return Err(LimboError::ParseError(format!(
                "view {current_view_name} is circularly defined"
            )));
        }

        let rewrite_result = (|| -> Result<Option<RewrittenView>> {
            let original_select = select.clone();
            let original_columns =
                view_columns_from_select(&original_select, schema, namespace, &view_columns)?;

            let ctx = ViewRewriteCtx::new(
                dialect,
                schema,
                namespace,
                target_table,
                target_db_name,
                old_column,
                new_column,
            );
            let sql_changed =
                rewrite_view_select_for_column_rename(&mut select, &ctx, &[], visiting_views)?;

            let view_column_schema = extract_view_columns(namespace, &select, schema)?;
            let mut final_columns = apply_view_column_rename(view_column_schema, &ctx);

            for (i, indexed_col) in view_columns.iter().enumerate() {
                if let Some(col) = final_columns.get_mut(i) {
                    col.name = Some(indexed_col.col_name.as_str().to_string());
                }
            }

            let columns_changed = !columns_equivalent(&original_columns, &final_columns);

            if !sql_changed && !columns_changed {
                return Ok(None);
            }

            let new_sql = if sql_changed {
                let new_stmt = ast::Stmt::CreateView {
                    temporary,
                    if_not_exists,
                    view_name,
                    columns: view_columns,
                    select: select.clone(),
                };
                new_stmt.to_string()
            } else {
                view_sql.to_string()
            };

            Ok(Some(RewrittenView {
                sql: new_sql,
                select_stmt: select,
                columns: final_columns,
            }))
        })();
        visiting_views.remove(&current_view_name);
        rewrite_result
    }

    fn apply_view_column_rename(
        view_columns: ViewColumnSchema,
        ctx: &ViewRewriteCtx,
    ) -> crate::alloc::Vec<Column> {
        let target_norm = ctx.target_table_norm.as_str();
        let mut columns = view_columns.columns;

        for view_column in &mut columns {
            if view_column.table_index == usize::MAX {
                continue;
            }
            let table = &view_columns.tables[view_column.table_index];
            if table_name_matches_target(
                &table.name,
                table.db_name.as_deref(),
                target_norm,
                &ctx.local_qualifiers,
            ) {
                if let Some(ref mut name) = view_column.column.name {
                    if name.as_str().eq_ignore_ascii_case(ctx.old_column) {
                        *name = ctx.new_column.to_string();
                    }
                }
            }
        }

        columns
            .into_iter()
            .map(|vc| vc.column)
            .try_collect()
            .expect(crate::alloc::ALLOC_ERR_MSG)
    }

    fn view_columns_from_select(
        select: &ast::Select,
        schema: &Schema,
        namespace: NamespaceId,
        explicit: &[ast::IndexedColumn],
    ) -> Result<crate::alloc::Vec<Column>> {
        let view_column_schema = extract_view_columns(namespace, select, schema)?;
        let mut columns = view_column_schema.flat_columns()?;
        for (i, indexed_col) in explicit.iter().enumerate() {
            if let Some(col) = columns.get_mut(i) {
                col.name = Some(indexed_col.col_name.as_str().to_string());
            }
        }
        Ok(columns)
    }

    fn columns_equivalent(left: &[Column], right: &[Column]) -> bool {
        if left.len() != right.len() {
            return false;
        }
        left.iter().zip(right.iter()).all(|(l, r)| {
            let l_name = l.name.as_deref().unwrap_or("");
            let r_name = r.name.as_deref().unwrap_or("");
            l_name.eq_ignore_ascii_case(r_name)
        })
    }

    #[derive(Clone)]
    struct ViewSourceInfo {
        qualifiers: Vec<String>,
        columns_before: HashSet<String>,
        rename_map: HashMap<String, String>,
        is_target_table: bool,
        db_name: Option<String>,
    }

    impl ViewSourceInfo {
        fn matches_qualifier(&self, qualifier: &str) -> bool {
            self.qualifiers.iter().any(|q| q == qualifier)
        }
    }

    fn alias_name(alias: &ast::As) -> &str {
        alias.name().as_str()
    }

    #[derive(Clone)]
    struct CteInfo {
        columns_before: HashSet<String>,
        rename_map: HashMap<String, String>,
    }

    struct ViewRewriteCtx<'a> {
        dialect: &'a dyn crate::dialect::Dialect,
        schema: &'a Schema,
        /// The namespace of the view being rewritten.
        namespace: NamespaceId,
        target_table: &'a str,
        target_table_norm: String,
        target_db_norm: String,
        /// The qualifiers that name this database in the view body: the
        /// database's name, and the name of the view's namespace when the
        /// namespace is stored, since a bound body qualifies tables with it.
        local_qualifiers: Vec<String>,
        old_column: &'a str,
        old_column_norm: String,
        new_column: &'a str,
    }

    impl<'a> ViewRewriteCtx<'a> {
        fn new(
            dialect: &'a dyn crate::dialect::Dialect,
            schema: &'a Schema,
            namespace: NamespaceId,
            target_table: &'a str,
            target_db_name: &'a str,
            old_column: &'a str,
            new_column: &'a str,
        ) -> Self {
            Self {
                dialect,
                schema,
                namespace,
                target_table,
                target_table_norm: normalize_ident(target_table),
                target_db_norm: normalize_ident(target_db_name),
                local_qualifiers: std::iter::once(normalize_ident(target_db_name))
                    .chain(schema.namespace_name(namespace).map(normalize_ident))
                    .collect(),
                old_column,
                old_column_norm: normalize_ident(old_column),
                new_column,
            }
        }
    }

    fn rewrite_view_select_for_column_rename(
        select: &mut ast::Select,
        ctx: &ViewRewriteCtx,
        outer_scopes: &[&[ViewSourceInfo]],
        visiting_views: &mut HashSet<String>,
    ) -> Result<bool> {
        let mut changed = false;

        let mut ctes: HashMap<String, CteInfo> = HashMap::default();
        if let Some(ref mut with_clause) = select.with {
            for cte in &mut with_clause.ctes {
                let mut before_cols = select_output_columns(&cte.select, ctx, false)?;
                apply_explicit_column_names(&mut before_cols, &cte.columns);
                let cte_changed = rewrite_view_select_for_column_rename(
                    &mut cte.select,
                    ctx,
                    &[],
                    visiting_views,
                )?;
                changed |= cte_changed;
                let mut after_cols = select_output_columns(&cte.select, ctx, true)?;
                apply_explicit_column_names(&mut after_cols, &cte.columns);
                let rename_map = build_rename_map(&before_cols, &after_cols, &ctx.old_column_norm);
                ctes.insert(
                    normalize_ident(cte.tbl_name.as_str()),
                    CteInfo {
                        columns_before: before_cols
                            .into_iter()
                            .map(|c| normalize_ident(&c))
                            .collect(),
                        rename_map,
                    },
                );
            }
        }

        let mut scope_sources = rewrite_one_select_for_column_rename(
            &mut select.body.select,
            ctx,
            &ctes,
            outer_scopes,
            &mut changed,
            visiting_views,
        )?;

        for compound in &mut select.body.compounds {
            let compound_sources = rewrite_one_select_for_column_rename(
                &mut compound.select,
                ctx,
                &ctes,
                outer_scopes,
                &mut changed,
                visiting_views,
            )?;
            if scope_sources.is_none() {
                scope_sources = compound_sources;
            }
        }

        if let Some(ref sources) = scope_sources {
            for sorted_col in &mut select.order_by {
                rewrite_expr_in_scope(
                    &mut sorted_col.expr,
                    sources,
                    outer_scopes,
                    ctx,
                    &mut changed,
                    visiting_views,
                )?;
            }
            if let Some(ref mut limit) = select.limit {
                rewrite_expr_in_scope(
                    &mut limit.expr,
                    sources,
                    outer_scopes,
                    ctx,
                    &mut changed,
                    visiting_views,
                )?;
                if let Some(ref mut offset) = limit.offset {
                    rewrite_expr_in_scope(
                        offset,
                        sources,
                        outer_scopes,
                        ctx,
                        &mut changed,
                        visiting_views,
                    )?;
                }
            }
        }

        Ok(changed)
    }

    fn rewrite_one_select_for_column_rename(
        one_select: &mut ast::OneSelect,
        ctx: &ViewRewriteCtx,
        ctes: &HashMap<String, CteInfo>,
        outer_scopes: &[&[ViewSourceInfo]],
        changed: &mut bool,
        visiting_views: &mut HashSet<String>,
    ) -> Result<Option<Vec<ViewSourceInfo>>> {
        match one_select {
            ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
                ..
            } => {
                let sources = if let Some(ref mut from_clause) = from {
                    rewrite_from_clause_for_column_rename(
                        from_clause,
                        ctx,
                        ctes,
                        outer_scopes,
                        changed,
                        visiting_views,
                    )?
                } else {
                    Vec::new()
                };

                for col in columns {
                    if let ast::ResultColumn::Expr(expr, _) = col {
                        rewrite_expr_in_scope(
                            expr,
                            &sources,
                            outer_scopes,
                            ctx,
                            changed,
                            visiting_views,
                        )?;
                    }
                }

                if let Some(ref mut where_expr) = where_clause {
                    rewrite_expr_in_scope(
                        where_expr,
                        &sources,
                        outer_scopes,
                        ctx,
                        changed,
                        visiting_views,
                    )?;
                }

                if let Some(ref mut group_by) = group_by {
                    for expr in &mut group_by.exprs {
                        rewrite_expr_in_scope(
                            expr,
                            &sources,
                            outer_scopes,
                            ctx,
                            changed,
                            visiting_views,
                        )?;
                    }
                    if let Some(ref mut having_expr) = group_by.having {
                        rewrite_expr_in_scope(
                            having_expr,
                            &sources,
                            outer_scopes,
                            ctx,
                            changed,
                            visiting_views,
                        )?;
                    }
                }

                for window_def in window_clause {
                    for expr in &mut window_def.window.partition_by {
                        rewrite_expr_in_scope(
                            expr,
                            &sources,
                            outer_scopes,
                            ctx,
                            changed,
                            visiting_views,
                        )?;
                    }
                    for sorted in &mut window_def.window.order_by {
                        rewrite_expr_in_scope(
                            &mut sorted.expr,
                            &sources,
                            outer_scopes,
                            ctx,
                            changed,
                            visiting_views,
                        )?;
                    }
                }

                Ok(Some(sources))
            }
            ast::OneSelect::Values(values) => {
                for row in values {
                    for expr in row {
                        rewrite_expr_in_scope(
                            expr,
                            &[],
                            outer_scopes,
                            ctx,
                            changed,
                            visiting_views,
                        )?;
                    }
                }
                Ok(None)
            }
        }
    }

    fn rewrite_from_clause_for_column_rename(
        from_clause: &mut ast::FromClause,
        ctx: &ViewRewriteCtx,
        ctes: &HashMap<String, CteInfo>,
        outer_scopes: &[&[ViewSourceInfo]],
        changed: &mut bool,
        visiting_views: &mut HashSet<String>,
    ) -> Result<Vec<ViewSourceInfo>> {
        let mut sources = Vec::new();
        let first_source = rewrite_select_table_for_column_rename(
            &mut from_clause.select,
            &[],
            ctx,
            ctes,
            outer_scopes,
            changed,
            visiting_views,
        )?;
        sources.push(first_source);

        for join in &mut from_clause.joins {
            let right_source = rewrite_select_table_for_column_rename(
                &mut join.table,
                &sources,
                ctx,
                ctes,
                outer_scopes,
                changed,
                visiting_views,
            )?;
            sources.push(right_source);
            let (right_source, left_sources) = sources
                .split_last()
                .expect("sources should include the right-hand side join source");
            if let Some(ref mut constraint) = join.constraint {
                match constraint {
                    ast::JoinConstraint::On(expr) => {
                        rewrite_expr_in_scope(
                            expr,
                            &sources,
                            outer_scopes,
                            ctx,
                            changed,
                            visiting_views,
                        )?;
                    }
                    ast::JoinConstraint::Using(cols) => {
                        *changed |= rewrite_using_columns(
                            cols,
                            left_sources,
                            right_source,
                            &ctx.old_column_norm,
                            ctx.new_column,
                        );
                    }
                }
            }
        }

        Ok(sources)
    }

    fn rewrite_select_table_for_column_rename(
        select_table: &mut ast::SelectTable,
        visible_sources: &[ViewSourceInfo],
        ctx: &ViewRewriteCtx,
        ctes: &HashMap<String, CteInfo>,
        outer_scopes: &[&[ViewSourceInfo]],
        changed: &mut bool,
        visiting_views: &mut HashSet<String>,
    ) -> Result<ViewSourceInfo> {
        match select_table {
            ast::SelectTable::Table(tbl_name, alias, _) => {
                let table_name_norm = normalize_ident(tbl_name.name.as_str());
                let table_db_norm = tbl_name
                    .namespace
                    .as_ref()
                    .map(|db| normalize_ident(db.as_str()));
                let mut qualifiers = Vec::new();
                qualifiers.push(table_name_norm.clone());
                if let Some(ref alias) = alias {
                    qualifiers.push(normalize_ident(alias_name(alias)));
                }
                if table_db_norm.is_none() {
                    if let Some(cte) = ctes.get(&table_name_norm) {
                        return Ok(ViewSourceInfo {
                            qualifiers,
                            columns_before: cte.columns_before.clone(),
                            rename_map: cte.rename_map.clone(),
                            is_target_table: false,
                            db_name: None,
                        });
                    }
                }

                let is_local = table_db_norm
                    .as_deref()
                    .is_none_or(|db| ctx.local_qualifiers.iter().any(|qualifier| qualifier == db));

                if is_local {
                    if let Some(view) = ctx.schema.get_view(ctx.namespace, &table_name_norm) {
                        let columns_before = view
                            .columns
                            .iter()
                            .filter_map(|col| col.name.clone())
                            .map(|name| normalize_ident(&name))
                            .collect();

                        let mut rename_map = HashMap::default();
                        if let Some(rewritten) = rewrite_view_sql_for_column_rename_inner(
                            ctx.dialect,
                            &view.sql,
                            ctx.schema,
                            ctx.namespace,
                            ctx.target_table,
                            &ctx.target_db_norm,
                            ctx.old_column,
                            ctx.new_column,
                            visiting_views,
                        )? {
                            rename_map = build_rename_map_from_columns(
                                &view.columns,
                                &rewritten.columns,
                                &ctx.old_column_norm,
                            );
                        }

                        return Ok(ViewSourceInfo {
                            qualifiers,
                            columns_before,
                            rename_map,
                            is_target_table: false,
                            db_name: table_db_norm,
                        });
                    }
                }
                let is_target = table_name_matches_target(
                    &table_name_norm,
                    table_db_norm.as_deref(),
                    &ctx.target_table_norm,
                    &ctx.local_qualifiers,
                );
                let columns_before = if is_local {
                    table_source_columns(ctx.schema, ctx.namespace, &table_name_norm)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|c| normalize_ident(&c))
                        .collect()
                } else {
                    HashSet::default()
                };

                Ok(ViewSourceInfo {
                    qualifiers,
                    columns_before,
                    rename_map: HashMap::default(),
                    is_target_table: is_target,
                    db_name: table_db_norm,
                })
            }
            ast::SelectTable::Select {
                select,
                alias,
                lateral,
            } => {
                let before_cols = select_output_columns(select, ctx, false)?;
                let lateral_scopes: Vec<&[ViewSourceInfo]> = if *lateral {
                    std::iter::once(visible_sources)
                        .chain(outer_scopes.iter().copied())
                        .collect()
                } else {
                    Vec::new()
                };
                *changed |= rewrite_view_select_for_column_rename(
                    select,
                    ctx,
                    &lateral_scopes,
                    visiting_views,
                )?;
                let after_cols = select_output_columns(select, ctx, true)?;
                let rename_map = build_rename_map(&before_cols, &after_cols, &ctx.old_column_norm);
                let qualifiers = alias
                    .as_ref()
                    .map(|alias| vec![normalize_ident(alias_name(alias))])
                    .unwrap_or_default();
                Ok(ViewSourceInfo {
                    qualifiers,
                    columns_before: before_cols
                        .into_iter()
                        .map(|c| normalize_ident(&c))
                        .collect(),
                    rename_map,
                    is_target_table: false,
                    db_name: None,
                })
            }
            ast::SelectTable::Sub(from_clause, alias) => {
                let before_cols = from_clause_output_columns(from_clause, ctx, false)?;
                let _ = rewrite_from_clause_for_column_rename(
                    from_clause,
                    ctx,
                    ctes,
                    outer_scopes,
                    changed,
                    visiting_views,
                )?;
                let after_cols = from_clause_output_columns(from_clause, ctx, true)?;
                let rename_map = build_rename_map(&before_cols, &after_cols, &ctx.old_column_norm);
                let qualifiers = alias
                    .as_ref()
                    .map(|alias| vec![normalize_ident(alias_name(alias))])
                    .unwrap_or_default();
                Ok(ViewSourceInfo {
                    qualifiers,
                    columns_before: before_cols
                        .into_iter()
                        .map(|c| normalize_ident(&c))
                        .collect(),
                    rename_map,
                    is_target_table: false,
                    db_name: None,
                })
            }
            ast::SelectTable::TableCall(_, args, alias) => {
                for arg in args {
                    rewrite_expr_in_scope(
                        arg,
                        visible_sources,
                        outer_scopes,
                        ctx,
                        changed,
                        visiting_views,
                    )?;
                }
                let qualifiers = alias
                    .as_ref()
                    .map(|alias| vec![normalize_ident(alias_name(alias))])
                    .unwrap_or_default();
                Ok(ViewSourceInfo {
                    qualifiers,
                    columns_before: HashSet::default(),
                    rename_map: HashMap::default(),
                    is_target_table: false,
                    db_name: None,
                })
            }
        }
    }

    fn rewrite_expr_in_scope(
        expr: &mut ast::Expr,
        sources: &[ViewSourceInfo],
        outer_scopes: &[&[ViewSourceInfo]],
        ctx: &ViewRewriteCtx,
        changed: &mut bool,
        visiting_views: &mut HashSet<String>,
    ) -> Result<()> {
        let mut outer_scopes_for_subqueries: Vec<&[ViewSourceInfo]> =
            Vec::with_capacity(outer_scopes.len() + 1);
        if !sources.is_empty() {
            outer_scopes_for_subqueries.push(sources);
        }
        outer_scopes_for_subqueries.extend_from_slice(outer_scopes);
        walk_expr_mut(expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
            if rewrite_expr_column_ref_view(
                e,
                sources,
                outer_scopes,
                &ctx.local_qualifiers,
                &ctx.old_column_norm,
                ctx.new_column,
            ) {
                *changed = true;
            }
            match e {
                ast::Expr::Subquery(select) | ast::Expr::Exists(select) => {
                    if rewrite_view_select_for_column_rename(
                        select,
                        ctx,
                        outer_scopes_for_subqueries.as_slice(),
                        visiting_views,
                    )? {
                        *changed = true;
                    }
                }
                ast::Expr::InSelect { rhs, .. } => {
                    if rewrite_view_select_for_column_rename(
                        rhs,
                        ctx,
                        outer_scopes_for_subqueries.as_slice(),
                        visiting_views,
                    )? {
                        *changed = true;
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        })?;
        Ok(())
    }

    fn rewrite_expr_column_ref_view(
        expr: &mut ast::Expr,
        sources: &[ViewSourceInfo],
        outer_scopes: &[&[ViewSourceInfo]],
        local_qualifiers: &[String],
        old_column_norm: &str,
        new_column: &str,
    ) -> bool {
        let apply_rename = |source: &ViewSourceInfo, set_name: &mut dyn FnMut(String)| {
            if source.is_target_table {
                set_name(new_column.to_string());
                return true;
            }
            if let Some(mapped) = source.rename_map.get(old_column_norm) {
                set_name(mapped.to_string());
                return true;
            }
            false
        };

        match expr {
            ast::Expr::Qualified(ns, col) => {
                let ns_norm = normalize_ident(ns.as_str());
                if !col.as_str().eq_ignore_ascii_case(old_column_norm) {
                    return false;
                }
                let (source, local_ambiguous) =
                    resolve_qualified(sources, &ns_norm, local_qualifiers);
                if let Some(source) = source {
                    return apply_rename(source, &mut |name| {
                        *col = ast::Name::exact(name);
                    });
                }
                if local_ambiguous {
                    return false;
                }
                for scope in outer_scopes {
                    let (source, ambiguous) = resolve_qualified(scope, &ns_norm, local_qualifiers);
                    if let Some(source) = source {
                        return apply_rename(source, &mut |name| {
                            *col = ast::Name::exact(name);
                        });
                    }
                    if ambiguous {
                        return false;
                    }
                }
            }
            ast::Expr::DoublyQualified(schema, ns, col) => {
                let schema_norm = normalize_ident(schema.as_str());
                if !local_qualifiers.contains(&schema_norm) {
                    return false;
                }
                let ns_norm = normalize_ident(ns.as_str());
                if !col.as_str().eq_ignore_ascii_case(old_column_norm) {
                    return false;
                }
                let (source, local_ambiguous) =
                    resolve_qualified(sources, &ns_norm, std::slice::from_ref(&schema_norm));
                if let Some(source) = source {
                    return apply_rename(source, &mut |name| {
                        *col = ast::Name::exact(name);
                    });
                }
                if local_ambiguous {
                    return false;
                }
                for scope in outer_scopes {
                    let (source, ambiguous) =
                        resolve_qualified(scope, &ns_norm, std::slice::from_ref(&schema_norm));
                    if let Some(source) = source {
                        return apply_rename(source, &mut |name| {
                            *col = ast::Name::exact(name);
                        });
                    }
                    if ambiguous {
                        return false;
                    }
                }
            }
            ast::Expr::Id(col) | ast::Expr::Name(col) => {
                if !col.as_str().eq_ignore_ascii_case(old_column_norm) {
                    return false;
                }
                let col_norm = normalize_ident(col.as_str());
                let (source, local_ambiguous) = resolve_unqualified(sources, &col_norm);
                if let Some(source) = source {
                    return apply_rename(source, &mut |name| {
                        *expr = ast::Expr::Id(ast::Name::exact(name));
                    });
                }
                if local_ambiguous {
                    return false;
                }
                for scope in outer_scopes {
                    let (source, ambiguous) = resolve_unqualified(scope, &col_norm);
                    if let Some(source) = source {
                        return apply_rename(source, &mut |name| {
                            *expr = ast::Expr::Id(ast::Name::exact(name));
                        });
                    }
                    if ambiguous {
                        return false;
                    }
                }
            }
            _ => {}
        }
        false
    }

    fn resolve_unqualified<'a>(
        candidates: &'a [ViewSourceInfo],
        old_column_norm: &str,
    ) -> (Option<&'a ViewSourceInfo>, bool) {
        let mut matches = candidates
            .iter()
            .filter(|s| s.columns_before.contains(old_column_norm));
        let Some(first) = matches.next() else {
            return (None, false);
        };
        if matches.next().is_some() {
            return (None, true);
        }
        (Some(first), false)
    }

    fn resolve_qualified<'a>(
        candidates: &'a [ViewSourceInfo],
        qualifier: &str,
        local_qualifiers: &[String],
    ) -> (Option<&'a ViewSourceInfo>, bool) {
        let mut matches = candidates.iter().filter(|s| {
            s.matches_qualifier(qualifier)
                && s.db_name
                    .as_deref()
                    .is_none_or(|db| local_qualifiers.iter().any(|qualifier| qualifier == db))
        });
        let Some(first) = matches.next() else {
            return (None, false);
        };
        if matches.next().is_some() {
            return (None, true);
        }
        (Some(first), false)
    }

    fn rewrite_using_columns(
        cols: &mut [ast::Name],
        left_sources: &[ViewSourceInfo],
        right: &ViewSourceInfo,
        old_column_norm: &str,
        new_column: &str,
    ) -> bool {
        let mut changed = false;
        let left_map = left_sources
            .iter()
            .find_map(|source| source.rename_map.get(old_column_norm));
        let left_has_target = left_sources.iter().any(|source| source.is_target_table);
        let right_map = right.rename_map.get(old_column_norm);
        let should_rename =
            left_has_target || right.is_target_table || left_map.is_some() || right_map.is_some();
        if !should_rename {
            return false;
        }
        let replacement = left_map
            .or(right_map)
            .map(|s| s.as_str())
            .unwrap_or(new_column);

        for col in cols {
            if col.as_str().eq_ignore_ascii_case(old_column_norm) {
                *col = ast::Name::exact(replacement.to_string());
                changed = true;
            }
        }
        changed
    }

    fn select_output_columns(
        select: &ast::Select,
        ctx: &ViewRewriteCtx,
        apply_rename: bool,
    ) -> Result<Vec<String>> {
        let view_columns = extract_view_columns(ctx.namespace, select, ctx.schema)?;
        let mut columns = view_columns.columns;
        if apply_rename {
            let target_norm = ctx.target_table_norm.as_str();
            for view_column in &mut columns {
                if view_column.table_index == usize::MAX {
                    continue;
                }
                let table = &view_columns.tables[view_column.table_index];
                if table_name_matches_target(
                    &table.name,
                    table.db_name.as_deref(),
                    target_norm,
                    &ctx.local_qualifiers,
                ) {
                    if let Some(ref mut name) = view_column.column.name {
                        if name.as_str().eq_ignore_ascii_case(ctx.old_column) {
                            *name = ctx.new_column.to_string();
                        }
                    }
                }
            }
        }

        Ok(columns
            .into_iter()
            .map(|vc| vc.column.name.unwrap_or_else(|| "?".to_string()))
            .collect())
    }

    fn apply_explicit_column_names(columns: &mut [String], explicit: &[ast::IndexedColumn]) {
        for (i, indexed_col) in explicit.iter().enumerate() {
            if let Some(col) = columns.get_mut(i) {
                *col = indexed_col.col_name.as_str().to_string();
            }
        }
    }

    fn from_clause_output_columns(
        from_clause: &ast::FromClause,
        ctx: &ViewRewriteCtx,
        apply_rename: bool,
    ) -> Result<Vec<String>> {
        let dummy_select = ast::Select {
            with: None,
            body: ast::SelectBody {
                select: ast::OneSelect::Select {
                    distinctness: None,
                    columns: vec![ast::ResultColumn::Star],
                    from: Some(from_clause.clone()),
                    where_clause: None,
                    group_by: None,
                    window_clause: Vec::new(),
                },
                compounds: Vec::new(),
            },
            order_by: Vec::new(),
            limit: None,
        };
        select_output_columns(&dummy_select, ctx, apply_rename)
    }

    fn build_rename_map(
        before_cols: &[String],
        after_cols: &[String],
        old_column_norm: &str,
    ) -> HashMap<String, String> {
        let mut map = HashMap::default();
        for (before, after) in before_cols.iter().zip(after_cols.iter()) {
            if before.as_str().eq_ignore_ascii_case(old_column_norm)
                && !after.as_str().eq_ignore_ascii_case(before.as_str())
            {
                map.insert(old_column_norm.to_string(), after.to_string());
            }
        }
        map
    }

    fn build_rename_map_from_columns(
        before_cols: &[Column],
        after_cols: &[Column],
        old_column_norm: &str,
    ) -> HashMap<String, String> {
        if before_cols.len() != after_cols.len() {
            return HashMap::default();
        }
        let mut map = HashMap::default();
        for (before, after) in before_cols.iter().zip(after_cols.iter()) {
            let Some(before_name) = before.name.as_ref() else {
                continue;
            };
            let Some(after_name) = after.name.as_ref() else {
                continue;
            };
            if before_name.as_str().eq_ignore_ascii_case(old_column_norm)
                && !after_name
                    .as_str()
                    .eq_ignore_ascii_case(before_name.as_str())
            {
                map.insert(old_column_norm.to_string(), after_name.to_string());
            }
        }
        map
    }

    fn table_name_matches_target(
        table_name: &str,
        table_db: Option<&str>,
        target_table_norm: &str,
        local_qualifiers: &[String],
    ) -> bool {
        if !table_name.eq_ignore_ascii_case(target_table_norm) {
            return false;
        }
        match table_db {
            None => true,
            Some(db) => local_qualifiers
                .iter()
                .any(|qualifier| qualifier.eq_ignore_ascii_case(db)),
        }
    }

    fn table_source_columns(
        schema: &Schema,
        namespace: NamespaceId,
        table_name: &str,
    ) -> Option<Vec<String>> {
        if let Some(table) = schema.get_table(namespace, table_name) {
            return Some(
                table
                    .columns()
                    .iter()
                    .filter_map(|col| col.name.clone())
                    .collect(),
            );
        }
        let table_norm = normalize_ident(table_name);
        if let Some(view) = schema.get_view(namespace, &table_norm) {
            return Some(
                view.columns
                    .iter()
                    .filter_map(|col| col.name.clone())
                    .collect(),
            );
        }
        None
    }
}

pub use rename_column_view::{rewrite_view_sql_for_column_rename, RewrittenView};

/// Rewrite table-qualified column references in a CHECK constraint expression,
/// replacing the table name from `from` to `to`. For example, `t1.a > 0` becomes
/// `t2.a > 0` when renaming t1 to t2. This matches SQLite 3.49.1+ behavior which
/// rewrites qualified refs during ALTER TABLE RENAME instead of rejecting them.
pub fn rewrite_check_expr_table_refs(expr: &mut ast::Expr, from: &str, to: &str) {
    rewrite_expr_table_refs(expr, TableRenameScope::new(from, to));
}

/// Update a column-level REFERENCES <tbl>(col,...) constraint
pub fn rewrite_column_references_if_needed(
    col: &mut ast::ColumnDefinition,
    table: &str,
    from: &str,
    to: &str,
) -> Result<()> {
    for cc in &mut col.constraints {
        match &mut cc.constraint {
            ast::ColumnConstraint::ForeignKey { clause, .. } => {
                rewrite_fk_parent_cols_if_self_ref(clause, table, from, to);
            }
            ast::ColumnConstraint::Check { expr, source } => {
                rename_identifiers(expr, from, to);
                // The captured source text no longer matches the rewritten
                // expression.
                *source = None;
            }
            ast::ColumnConstraint::Generated { expr, .. } => {
                rename_identifiers(expr, from, to);
            }
            _ => {}
        }
    }
    Ok(())
}

/// For a column definition like `parent_id REFERENCES parent(old_col)`, update
/// the referenced parent column names when another table renames
/// `old_col -> new_col`.
pub fn rewrite_column_level_fk_parent_columns_if_needed(
    col: &mut ast::ColumnDefinition,
    table: &str,
    from: &str,
    to: &str,
) {
    for cc in &mut col.constraints {
        if let ast::ColumnConstraint::ForeignKey { clause, .. } = &mut cc.constraint {
            rewrite_fk_parent_cols_if_self_ref(clause, table, from, to);
        }
    }
}

/// If a FK REFERENCES targets `old_tbl`, change it to `new_tbl`
pub fn rewrite_fk_parent_table_if_needed(
    clause: &mut ast::ForeignKeyClause,
    old_tbl: &str,
    new_tbl: &str,
) -> bool {
    if clause.tbl_name.as_str().eq_ignore_ascii_case(old_tbl) {
        clause.tbl_name = ast::Name::exact(new_tbl.to_owned());
        return true;
    }
    false
}

/// For inline REFERENCES tbl in a column definition.
pub fn rewrite_inline_col_fk_target_if_needed(
    col: &mut ast::ColumnDefinition,
    old_tbl: &str,
    new_tbl: &str,
) -> bool {
    let mut changed = false;
    for cc in &mut col.constraints {
        if let ast::NamedColumnConstraint {
            constraint: ast::ColumnConstraint::ForeignKey { clause, .. },
            ..
        } = cc
        {
            changed |= rewrite_fk_parent_table_if_needed(clause, old_tbl, new_tbl);
        }
    }
    changed
}

/// Rewrite table name references inside a trigger's body commands for ALTER TABLE RENAME.
/// Updates tbl_name fields in INSERT/UPDATE/DELETE commands and table references
/// in FROM clauses and qualified expressions throughout the trigger body.
pub fn rewrite_trigger_cmd_table_refs(cmd: &mut ast::TriggerCmd, old_tbl: &str, new_tbl: &str) {
    match cmd {
        ast::TriggerCmd::Update {
            tbl_name,
            sets,
            from,
            where_clause,
            ..
        } => {
            if tbl_name.as_str().eq_ignore_ascii_case(old_tbl) {
                *tbl_name = ast::Name::exact(new_tbl.to_owned());
            }
            let scope = TableRenameScope::new(old_tbl, new_tbl);
            let expr_scope = from
                .as_ref()
                .map_or(scope, |from| scope.inside_from_clause(from));
            for set in sets {
                rewrite_expr_table_refs(&mut set.expr, expr_scope);
            }
            if let Some(ref mut from) = from {
                rewrite_from_clause_table_refs(from, scope);
            }
            if let Some(ref mut wc) = where_clause {
                rewrite_expr_table_refs(wc, expr_scope);
            }
        }
        ast::TriggerCmd::Insert {
            tbl_name,
            select,
            upsert,
            ..
        } => {
            if tbl_name.as_str().eq_ignore_ascii_case(old_tbl) {
                *tbl_name = ast::Name::exact(new_tbl.to_owned());
            }
            rewrite_select_table_refs(select, old_tbl, new_tbl);
            if let Some(ref mut upsert) = upsert {
                rewrite_upsert_table_refs(upsert, old_tbl, new_tbl);
            }
        }
        ast::TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } => {
            if tbl_name.as_str().eq_ignore_ascii_case(old_tbl) {
                *tbl_name = ast::Name::exact(new_tbl.to_owned());
            }
            if let Some(ref mut wc) = where_clause {
                rewrite_check_expr_table_refs(wc, old_tbl, new_tbl);
            }
        }
        ast::TriggerCmd::Select(select) => {
            rewrite_select_table_refs(select, old_tbl, new_tbl);
        }
    }
}

/// Collect the names by which each result column of a single SELECT arm can be
/// referenced from ORDER BY. Mirrors the set of identifiers SQLite would
/// consider when resolving a bare ORDER BY identifier against that arm's
/// output column list.
pub(crate) fn output_column_aliases(one_select: &ast::OneSelect) -> Vec<String> {
    let ast::OneSelect::Select { columns, .. } = one_select else {
        return Vec::new();
    };
    columns
        .iter()
        .filter_map(|col| match col {
            ast::ResultColumn::Expr(_, Some(alias)) => Some(normalize_ident(alias.name().as_str())),
            ast::ResultColumn::Expr(expr, None) => match expr.as_ref() {
                ast::Expr::Id(name) | ast::Expr::Name(name) => Some(normalize_ident(name.as_str())),
                ast::Expr::Qualified(_, col) | ast::Expr::DoublyQualified(_, _, col) => {
                    Some(normalize_ident(col.as_str()))
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Returns true when `expr` is a bare identifier that names the renamed column
/// and resolves to one of the SELECT's output column aliases (in the body or
/// any compound arm). Per SQLite's ORDER BY resolution rules, such a reference
/// is to the alias label, not to a FROM-clause column, so a column rename
/// must leave it alone.
///
/// Cheap in the common case: short-circuits before scanning aliases when the
/// expr is not a bare identifier or does not match `old_col`.
pub(crate) fn is_order_by_alias_ref(
    body: &ast::SelectBody,
    expr: &ast::Expr,
    old_col: &str,
) -> bool {
    let name = match expr {
        ast::Expr::Id(n) | ast::Expr::Name(n) => n.as_str(),
        _ => return false,
    };
    if !name.eq_ignore_ascii_case(old_col) {
        return false;
    }
    one_select_has_explicit_alias(&body.select, name)
        || body
            .compounds
            .iter()
            .any(|c| one_select_has_explicit_alias(&c.select, name))
}

// Only user-provided aliases (`AS x` or elided form) block ORDER BY rewriting.
// `As::ImplicitColumnName` is synthesized by the parser from the original
// expression text to label unaliased columns and is decoupled from the
// underlying expression — it must not be treated as an alias for rename
// purposes, since SQLite rewrites such ORDER BY refs along with the column.
fn one_select_has_explicit_alias(one_select: &ast::OneSelect, name: &str) -> bool {
    let ast::OneSelect::Select { columns, .. } = one_select else {
        return false;
    };
    columns.iter().any(|col| match col {
        ast::ResultColumn::Expr(_, Some(alias)) if alias.is_explicit() => {
            alias.name().as_str().eq_ignore_ascii_case(name)
        }
        _ => false,
    })
}

pub fn rewrite_select_table_refs(select: &mut ast::Select, old_tbl: &str, new_tbl: &str) {
    rewrite_select_in_scope(select, TableRenameScope::new(old_tbl, new_tbl));
}

/// Renames the table `old_tbl` of a namespace to `new_tbl` in `select`, the
/// body of a view. A name written without a qualifier is the table only when
/// `unqualified_names_table`, that is, when the view is in the table's
/// namespace; a qualified name is the table only when its qualifier is one
/// of `table_qualifiers`, the names of the table's namespace.
pub fn rewrite_select_namespace_table_refs(
    select: &mut ast::Select,
    old_tbl: &str,
    new_tbl: &str,
    unqualified_names_table: bool,
    table_qualifiers: &[String],
) {
    rewrite_select_in_scope(
        select,
        TableRenameScope {
            unqualified_names_table,
            table_qualifiers: Some(table_qualifiers),
            ..TableRenameScope::new(old_tbl, new_tbl)
        },
    );
}

#[derive(Clone, Copy)]
struct TableRenameScope<'a> {
    old_tbl: &'a str,
    new_tbl: &'a str,
    table_is_visible: bool,
    qualifier_is_table: bool,
    /// Whether a name written without a qualifier can be the table.
    unqualified_names_table: bool,
    /// The qualifiers that name the table's namespace; None takes any.
    table_qualifiers: Option<&'a [String]>,
}

impl<'a> TableRenameScope<'a> {
    fn new(old_tbl: &'a str, new_tbl: &'a str) -> Self {
        Self {
            old_tbl,
            new_tbl,
            table_is_visible: true,
            qualifier_is_table: true,
            unqualified_names_table: true,
            table_qualifiers: None,
        }
    }

    fn inside_with_clause(self, with: &ast::With) -> Self {
        if with.ctes.iter().any(|cte| self.is_old_name(&cte.tbl_name)) {
            Self {
                table_is_visible: false,
                qualifier_is_table: false,
                ..self
            }
        } else {
            self
        }
    }

    fn inside_one_select(self, one: &ast::OneSelect) -> Self {
        match one {
            ast::OneSelect::Select {
                from: Some(from), ..
            } => self.inside_from_clause(from),
            _ => self,
        }
    }

    fn inside_from_clause(self, from: &ast::FromClause) -> Self {
        match self.qualifier_target_in_from_clause(from) {
            Some(qualifier_is_table) => Self {
                qualifier_is_table,
                ..self
            },
            None => self,
        }
    }

    fn qualifier_target_in_from_clause(&self, from: &ast::FromClause) -> Option<bool> {
        std::iter::once(from.select.as_ref())
            .chain(from.joins.iter().map(|join| join.table.as_ref()))
            .find_map(|table| self.qualifier_target(table))
    }

    fn qualifier_target(&self, table: &ast::SelectTable) -> Option<bool> {
        match table {
            ast::SelectTable::Table(_, Some(alias), _)
            | ast::SelectTable::TableCall(_, _, Some(alias))
            | ast::SelectTable::Select {
                alias: Some(alias), ..
            }
            | ast::SelectTable::Sub(_, Some(alias)) => {
                self.is_old_name(alias.name()).then_some(false)
            }
            ast::SelectTable::Table(name, None, _) | ast::SelectTable::TableCall(name, _, None) => {
                self.is_old_name(&name.name)
                    .then(|| self.renames_table(name))
            }
            ast::SelectTable::Select { alias: None, .. } => None,
            ast::SelectTable::Sub(from, None) => self.qualifier_target_in_from_clause(from),
        }
    }

    fn renames_table(&self, name: &ast::QualifiedName) -> bool {
        let names_table = match &name.namespace {
            Some(qualifier) => self.qualifier_names_table_namespace(qualifier),
            None => self.table_is_visible && self.unqualified_names_table,
        };
        names_table && self.is_old_name(&name.name)
    }

    fn qualifier_names_table_namespace(&self, qualifier: &ast::Name) -> bool {
        self.table_qualifiers.is_none_or(|qualifiers| {
            let qualifier = normalize_ident(qualifier.as_str());
            qualifiers.contains(&qualifier)
        })
    }

    fn renames_qualifier(&self, name: &ast::Name) -> bool {
        self.qualifier_is_table && self.is_old_name(name)
    }

    fn is_old_name(&self, name: &ast::Name) -> bool {
        name.as_str().eq_ignore_ascii_case(self.old_tbl)
    }

    fn new_name(&self) -> ast::Name {
        ast::Name::exact(self.new_tbl.to_owned())
    }
}

fn rewrite_select_in_scope(select: &mut ast::Select, scope: TableRenameScope) {
    let scope = select
        .with
        .as_ref()
        .map_or(scope, |with| scope.inside_with_clause(with));
    if let Some(with_clause) = &mut select.with {
        for cte in &mut with_clause.ctes {
            rewrite_select_in_scope(&mut cte.select, scope);
        }
    }
    let order_by_scope = if select.body.compounds.is_empty() {
        scope.inside_one_select(&select.body.select)
    } else {
        scope
    };
    rewrite_one_select_table_refs(&mut select.body.select, scope);
    for compound in &mut select.body.compounds {
        rewrite_one_select_table_refs(&mut compound.select, scope);
    }
    for col in &mut select.order_by {
        rewrite_expr_table_refs(&mut col.expr, order_by_scope);
    }
    if let Some(ref mut limit) = select.limit {
        rewrite_expr_table_refs(&mut limit.expr, scope);
        if let Some(ref mut offset) = limit.offset {
            rewrite_expr_table_refs(offset, scope);
        }
    }
}

fn rewrite_one_select_table_refs(one: &mut ast::OneSelect, scope: TableRenameScope) {
    let inner_scope = scope.inside_one_select(one);
    match one {
        ast::OneSelect::Select {
            from,
            where_clause,
            columns,
            group_by,
            window_clause,
            ..
        } => {
            if let Some(ref mut from) = from {
                rewrite_from_clause_table_refs(from, scope);
            }
            for window_def in window_clause {
                for expr in &mut window_def.window.partition_by {
                    rewrite_expr_table_refs(expr, inner_scope);
                }
                for col in &mut window_def.window.order_by {
                    rewrite_expr_table_refs(&mut col.expr, inner_scope);
                }
            }
            if let Some(ref mut wc) = where_clause {
                rewrite_expr_table_refs(wc, inner_scope);
            }
            for col in columns {
                match col {
                    ast::ResultColumn::Expr(ref mut expr, _) => {
                        rewrite_expr_table_refs(expr, inner_scope);
                    }
                    ast::ResultColumn::TableStar(ref mut name) => {
                        if inner_scope.renames_qualifier(name) {
                            *name = inner_scope.new_name();
                        }
                    }
                    ast::ResultColumn::Star => {}
                }
            }
            if let Some(ref mut gb) = group_by {
                for expr in &mut gb.exprs {
                    rewrite_expr_table_refs(expr, inner_scope);
                }
                if let Some(ref mut having) = gb.having {
                    rewrite_expr_table_refs(having, inner_scope);
                }
            }
        }
        ast::OneSelect::Values(rows) => {
            for row in rows {
                for expr in row {
                    rewrite_expr_table_refs(expr, inner_scope);
                }
            }
        }
    }
}

fn rewrite_from_clause_table_refs(from: &mut ast::FromClause, scope: TableRenameScope) {
    let from_scope = scope.inside_from_clause(from);
    rewrite_select_table_entry_table_refs(&mut from.select, scope, from_scope);
    for join in &mut from.joins {
        rewrite_select_table_entry_table_refs(&mut join.table, scope, from_scope);
        if let Some(ast::JoinConstraint::On(ref mut expr)) = join.constraint {
            rewrite_expr_table_refs(expr, from_scope);
        }
    }
}

fn rewrite_select_table_entry_table_refs(
    st: &mut ast::SelectTable,
    scope: TableRenameScope,
    from_scope: TableRenameScope,
) {
    match st {
        ast::SelectTable::Table(ref mut name, _, _) => {
            if scope.renames_table(name) {
                name.name = scope.new_name();
            }
        }
        ast::SelectTable::TableCall(ref mut name, ref mut args, _) => {
            if scope.renames_table(name) {
                name.name = scope.new_name();
            }
            for arg in args {
                rewrite_expr_table_refs(arg, from_scope);
            }
        }
        ast::SelectTable::Select {
            ref mut select,
            lateral,
            ..
        } => {
            rewrite_select_in_scope(select, if *lateral { from_scope } else { scope });
        }
        ast::SelectTable::Sub(ref mut from, _) => {
            rewrite_from_clause_table_refs(from, scope);
        }
    }
}

fn rewrite_expr_table_refs(expr: &mut ast::Expr, scope: TableRenameScope) {
    let _ = walk_expr_mut(
        expr,
        &mut |e: &mut ast::Expr| -> crate::Result<WalkControl> {
            match e {
                ast::Expr::Qualified(tbl, col) => {
                    if scope.renames_qualifier(tbl) {
                        let col = col.clone();
                        *e = ast::Expr::Qualified(scope.new_name(), col);
                    }
                }
                ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
                    rewrite_select_in_scope(select, scope);
                }
                ast::Expr::InSelect { rhs, .. } => {
                    rewrite_select_in_scope(rhs, scope);
                }
                ast::Expr::InTable { rhs, .. } => {
                    if scope.renames_table(rhs) {
                        rhs.name = scope.new_name();
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        },
    );
}

fn rewrite_upsert_table_refs(upsert: &mut ast::Upsert, old_tbl: &str, new_tbl: &str) {
    if let Some(ref mut index) = upsert.index {
        if let Some(ref mut wc) = index.where_clause {
            rewrite_check_expr_table_refs(wc, old_tbl, new_tbl);
        }
    }
    if let ast::UpsertDo::Set {
        ref mut sets,
        ref mut where_clause,
    } = upsert.do_clause
    {
        for set in sets {
            rewrite_check_expr_table_refs(&mut set.expr, old_tbl, new_tbl);
        }
        if let Some(ref mut wc) = where_clause {
            rewrite_check_expr_table_refs(wc, old_tbl, new_tbl);
        }
    }
    if let Some(ref mut next) = upsert.next {
        rewrite_upsert_table_refs(next, old_tbl, new_tbl);
    }
}
