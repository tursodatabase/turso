use crate::sync::Arc;

use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, JoinConstraint, TableInternalId};

use super::emitter::Resolver;
use super::plan::{JoinInfo, ResultSetColumn, TableReferences};
use crate::schema::Table;
use crate::util::normalize_ident;
use crate::Result;

// ── IdGenerator ─────────────────────────────────────────────────────────

pub trait IdGenerator {
    fn next_id(&mut self) -> TableInternalId;
}

// ── BindTable ───────────────────────────────────────────────────────────

/// Trait for table metadata needed during binding (column name resolution).
pub trait BindTable {
    fn column_count(&self) -> usize;
    fn column_name(&self, idx: usize) -> Option<&str>;
    fn column_is_rowid_alias(&self, idx: usize) -> bool;
}

impl dyn BindTable {
    /// Create a column iterator for any `dyn BindTable`.
    pub fn columns(&self) -> BindColumnIter<'_, Self> {
        BindColumnIter {
            table: self,
            idx: 0,
        }
    }
}

pub struct BindColumnIter<'a, T: BindTable + ?Sized> {
    table: &'a T,
    idx: usize,
}

pub struct BindColumnRef<'a> {
    pub idx: usize,
    pub name: &'a str,
    pub is_rowid_alias: bool,
}

impl<'a, T: BindTable + ?Sized> Iterator for BindColumnIter<'a, T> {
    type Item = BindColumnRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.table.column_count() {
            return None;
        }
        let i = self.idx;
        self.idx += 1;
        Some(BindColumnRef {
            idx: i,
            name: self.table.column_name(i).unwrap(),
            is_rowid_alias: self.table.column_is_rowid_alias(i),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.table.column_count() - self.idx;
        (remaining, Some(remaining))
    }
}

impl BindTable for Table {
    fn column_count(&self) -> usize {
        self.columns().len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns().get(idx).and_then(|c| c.name.as_deref())
    }

    fn column_is_rowid_alias(&self, idx: usize) -> bool {
        self.columns().get(idx).is_some_and(|c| c.is_rowid_alias())
    }
}

/// Lightweight table for CTEs — just column names, no schema object.
pub struct CteTable {
    pub name: String,
    pub columns: Vec<String>,
}

impl BindTable for CteTable {
    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| s.as_str())
    }

    fn column_is_rowid_alias(&self, _idx: usize) -> bool {
        false
    }
}

// ── BindPhase ────────────────────────────────────────────────────────────

/// Controls alias visibility per SQL clause.
///
/// Replaces `BindingBehavior`. The phase is set on the [`BindContext`]
/// before binding each clause rather than passed per-call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindPhase {
    /// Phases 1–4: CTE, FROM, Window definitions, SELECT expressions.
    /// Only table columns visible; aliases not accessible.
    NoAliases,
    /// Phase 5: WHERE clause.
    /// Table columns first; SELECT aliases as fallback.
    TableFirst,
    /// Phases 6–8: GROUP BY, HAVING, ORDER BY.
    /// SELECT aliases first; table columns as fallback.
    AliasFirst,
}

// ── ScopeTable ───────────────────────────────────────────────────────────

/// A table visible within a single query scope.
///
/// Cheap to clone (table metadata is Arc-wrapped).
#[derive(Clone)]
pub struct ScopeTable {
    /// The name used to refer to this table in the query (original name or alias).
    pub identifier: String,
    /// Opaque ID used in `Expr::Column` to reference this table.
    pub internal_id: TableInternalId,
    /// Table metadata for column resolution. Clone is an Arc bump.
    pub table: Arc<dyn BindTable>,
    /// Join constraint info (USING clause for dedup during unqualified lookup).
    pub join_info: Option<JoinInfo>,
}

// ── BindScope ────────────────────────────────────────────────────────────

#[derive(Clone)]
/// Snapshot of all tables visible at one query level.
///
/// Analogous to DataFusion's `DFSchema`. Owned, Arc-wrapped for cheap
/// sharing when pushed onto the outer-scope stack.
pub struct BindScope {
    pub tables: Vec<ScopeTable>,
    /// Whether tables were swapped for a RIGHT→LEFT JOIN rewrite
    /// (affects star expansion order).
    pub right_join_swapped: bool,
}

pub type BindScopeRef = Arc<BindScope>;

impl BindScope {
    pub fn empty() -> Self {
        Self {
            tables: Vec::new(),
            right_join_swapped: false,
        }
    }

    /// Find an unqualified column by name across all tables in scope.
    ///
    /// Returns `(table_internal_id, column_index, is_rowid_alias)` or `None`.
    /// Errors on ambiguity (unless deduplicated by USING clause).
    pub fn find_column_unqualified(
        &self,
        name: &str,
    ) -> Result<Option<(TableInternalId, usize, bool)>> {
        let normalized = normalize_ident(name);
        let mut result: Option<(TableInternalId, usize, bool)> = None;

        for st in &self.tables {
            let col_idx = st
                .table
                .columns()
                .position(|col| col.name.eq_ignore_ascii_case(&normalized));

            if let Some(idx) = col_idx {
                if result.is_some() {
                    let in_using = st.join_info.as_ref().is_some_and(|ji| {
                        ji.using
                            .iter()
                            .any(|u| u.as_str().eq_ignore_ascii_case(&normalized))
                    });
                    if !in_using {
                        crate::bail_parse_error!("Column {} is ambiguous", name);
                    }
                } else {
                    result = Some((st.internal_id, idx, st.table.column_is_rowid_alias(idx)));
                }
            }
        }

        Ok(result)
    }

    /// Find a qualified column (`table.column`) in this scope.
    ///
    /// Returns `None` if the table is not found (caller can try outer scopes).
    /// Errors if the table exists but the column doesn't.
    pub fn find_column_qualified(
        &self,
        table_name: &str,
        col_name: &str,
    ) -> Result<Option<(TableInternalId, usize, bool)>> {
        let Some(st) = self.find_table_by_identifier(table_name) else {
            return Ok(None);
        };

        let normalized_col = normalize_ident(col_name);
        let col_idx = st
            .table
            .columns()
            .position(|col| col.name.eq_ignore_ascii_case(&normalized_col));

        let Some(idx) = col_idx else {
            crate::bail_parse_error!("no such column: {}", col_name);
        };

        Ok(Some((
            st.internal_id,
            idx,
            st.table.column_is_rowid_alias(idx),
        )))
    }

    /// Find a table by its identifier (name or alias).
    pub fn find_table_by_identifier(&self, name: &str) -> Option<&ScopeTable> {
        let normalized = normalize_ident(name);
        self.tables.iter().find(|t| t.identifier == normalized)
    }
}

// ── BindTracking ─────────────────────────────────────────────────────────

/// Records what was accessed during binding.
///
/// Applied to `TableReferences` in a single flush after binding completes.
#[derive(Debug, Default)]
pub struct BindTracking {
    /// `(table_id, column_index)` pairs for columns referenced in the current scope.
    pub columns_used: Vec<(TableInternalId, usize)>,
    /// Tables whose rowid was referenced.
    pub rowids_used: Vec<TableInternalId>,
    /// `(table_id, column_index)` pairs for columns referenced from outer scopes.
    pub outer_refs_used: Vec<(TableInternalId, usize)>,
}

impl BindTracking {
    pub fn record_column(&mut self, table_id: TableInternalId, col_idx: usize) {
        self.columns_used.push((table_id, col_idx));
    }

    pub fn record_rowid(&mut self, table_id: TableInternalId) {
        self.rowids_used.push(table_id);
    }

    pub fn record_outer_ref(&mut self, table_id: TableInternalId, col_idx: usize) {
        self.outer_refs_used.push((table_id, col_idx));
    }

    /// Apply recorded usage back to `TableReferences`.
    pub fn flush(&self, table_references: &mut TableReferences) {
        for &(table_id, col_idx) in &self.columns_used {
            table_references.mark_column_used(table_id, col_idx);
        }
        for &table_id in &self.rowids_used {
            table_references.mark_rowid_referenced(table_id);
        }
        for &(table_id, col_idx) in &self.outer_refs_used {
            table_references.mark_column_used(table_id, col_idx);
        }
    }
}

// ── CteEntry ─────────────────────────────────────────────────────────────

/// A CTE definition stored in the binding context.
pub struct CteEntry {
    /// The raw AST for re-planning on each reference.
    pub select: ast::Select,
    /// Explicit column names from `WITH t(a, b) AS (...)`.
    pub explicit_columns: Vec<String>,
    /// CTE ID for materialization tracking.
    pub cte_id: Option<usize>,
    /// Result column names, populated after binding the CTE body.
    /// If explicit_columns is non-empty, equals explicit_columns.
    /// Otherwise, extracted from the SELECT result columns.
    pub resolved_columns: Vec<String>,
}

// ── BindContext ───────────────────────────────────────────────────────────

/// Scope-aware binding context, analogous to DataFusion's `PlannerContext`.
///
/// Manages the outer-scope stack for correlated subquery resolution,
/// CTE definitions, SELECT aliases, and binding phase tracking.
///
/// Does **not** borrow `TableReferences`. Column usage is recorded in
/// [`BindTracking`] and flushed back after binding completes.
pub struct BindContext<'a, G: IdGenerator> {
    /// Function and schema resolver.
    pub resolver: &'a Resolver<'a>,

    /// Generates unique table IDs for scope tables.
    id_gen: &'a mut G,

    /// Stack of outer query scopes.
    ///
    /// When entering a subquery, the parent scope is pushed here.
    /// `outer_scopes_iter()` returns them innermost-first (reversed),
    /// matching column lookup precedence.
    outer_scopes: Vec<BindScopeRef>,

    /// Outer FROM schema for LATERAL join support.
    outer_from_scope: Option<BindScopeRef>,

    /// CTE definitions visible in the current query.
    ctes: HashMap<String, CteEntry>,

    /// SELECT aliases for the current query.
    /// Empty until phase 4 populates them.
    aliases: Vec<ResultSetColumn>,

    /// Current binding phase — controls alias visibility.
    phase: BindPhase,

    /// Records column/rowid usage for post-binding flush.
    pub tracking: BindTracking,
}

impl<'a, G: IdGenerator> BindContext<'a, G> {
    fn new(resolver: &'a Resolver<'a>, id_gen: &'a mut G) -> Self {
        Self {
            resolver,
            id_gen,
            outer_scopes: Vec::new(),
            outer_from_scope: None,
            ctes: HashMap::default(),
            aliases: Vec::new(),
            phase: BindPhase::NoAliases,
            tracking: BindTracking::default(),
        }
    }

    // ── Outer scope stack (mirrors DataFusion PlannerContext) ─────────

    /// Push a scope onto the outer-scope stack (entering a subquery).
    fn append_outer_query_scope(&mut self, scope: BindScopeRef) {
        self.outer_scopes.push(scope);
    }

    /// Pop the most recent outer scope (exiting a subquery).
    fn pop_outer_query_scope(&mut self) -> Option<BindScopeRef> {
        self.outer_scopes.pop()
    }

    /// Iterate outer scopes innermost-first (reversed storage order).
    /// Matches column lookup precedence: nearest enclosing query first.
    fn outer_scopes_iter(&self) -> impl Iterator<Item = &BindScopeRef> {
        self.outer_scopes.iter().rev()
    }

    /// The immediately enclosing query's scope (if any).
    fn latest_outer_scope(&self) -> Option<&BindScopeRef> {
        self.outer_scopes.last()
    }

    // ── Outer FROM (LATERAL support) ─────────────────────────────────

    fn outer_from_scope(&self) -> Option<&BindScopeRef> {
        self.outer_from_scope.as_ref()
    }

    /// Set the outer FROM scope, returning the previous value.
    fn set_outer_from_scope(&mut self, scope: Option<BindScopeRef>) -> Option<BindScopeRef> {
        std::mem::replace(&mut self.outer_from_scope, scope)
    }

    /// Extend the outer FROM scope by merging tables from another scope.
    /// Used during LATERAL join planning: each left-side table's scope
    /// is accumulated so the right side can reference it.
    fn extend_outer_from_scope(&mut self, scope: &BindScopeRef) {
        match self.outer_from_scope.as_mut() {
            Some(existing) => {
                let merged = Arc::make_mut(existing);
                merged.tables.extend(scope.tables.iter().cloned());
            }
            None => self.outer_from_scope = Some(Arc::clone(scope)),
        }
    }

    // ── CTEs ─────────────────────────────────────────────────────────

    fn insert_cte(&mut self, name: String, entry: CteEntry) {
        self.ctes.insert(name, entry);
    }

    fn get_cte(&self, name: &str) -> Option<&CteEntry> {
        self.ctes.get(name)
    }

    // ── Phase and aliases ────────────────────────────────────────────

    fn set_phase(&mut self, phase: BindPhase) {
        self.phase = phase;
    }

    fn phase(&self) -> BindPhase {
        self.phase
    }

    fn set_aliases(&mut self, aliases: Vec<ResultSetColumn>) {
        self.aliases = aliases;
    }

    fn aliases(&self) -> &[ResultSetColumn] {
        &self.aliases
    }

    /// Run `f` with a fresh per-query state (phase, aliases).
    /// Saves and restores on exit, so recursive calls (CTEs, subqueries) don't clobber.
    fn with_scope<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let saved_aliases = std::mem::take(&mut self.aliases);

        let result = f(self);

        self.aliases = saved_aliases;

        result
    }

    /// Run `f` with a temporary phase, restoring the previous phase on exit.
    fn with_phase<T>(
        &mut self,
        phase: BindPhase,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let saved = self.phase;
        self.phase = phase;
        let result = f(self);
        self.phase = saved;
        result
    }

    /// Extract result column names from a SELECT list before binding.
    ///
    /// These names are used for:
    /// - CTE column resolution (when referenced in FROM)
    /// - FROM subquery column resolution
    ///
    /// The name is determined by:
    /// - Explicit alias (`SELECT expr AS foo` → "foo")
    /// - Bare column reference (`SELECT x` → "x", `SELECT t.x` → "x")
    /// - Star expansion (`SELECT *` → all column names from scope)
    /// - Complex expressions without alias are unreferenceable by name,
    ///   so we use an empty string placeholder.
    fn select_result_column_names(
        columns: &[ast::ResultColumn],
        scope: &BindScope,
    ) -> Result<Vec<String>> {
        let mut names = Vec::with_capacity(columns.len());
        for col in columns {
            match col {
                ast::ResultColumn::Expr(expr, alias) => {
                    // Explicit alias takes priority
                    if let Some(a) = alias {
                        let name = match a {
                            ast::As::As(id) | ast::As::Elided(id) => normalize_ident(id.as_str()),
                        };
                        names.push(name);
                        continue;
                    }
                    // Bare or qualified column reference — extract the column part
                    let name = match expr.as_ref() {
                        ast::Expr::Id(id) => normalize_ident(id.as_str()),
                        ast::Expr::Qualified(_, id) => normalize_ident(id.as_str()),
                        ast::Expr::DoublyQualified(_, _, id) => normalize_ident(id.as_str()),
                        // Complex expressions (count(*), x+1, etc.) without an alias
                        // can't be referenced by name from outer queries.
                        _ => String::new(),
                    };
                    names.push(name);
                }
                ast::ResultColumn::Star => {
                    // Expand * into all column names from all tables in scope
                    for st in &scope.tables {
                        for col in st.table.columns() {
                            names.push(col.name.to_string());
                        }
                    }
                }
                ast::ResultColumn::TableStar(table_name) => {
                    // Expand table.* into that table's column names
                    let Some(st) = scope.find_table_by_identifier(table_name.as_str()) else {
                        crate::bail_parse_error!("no such table: {}", table_name);
                    };
                    for col in st.table.columns() {
                        names.push(col.name.to_string());
                    }
                }
            }
        }
        Ok(names)
    }

    /// Bind a SELECT statement, resolving all name references in-place.
    /// Returns the result column names of the SELECT.
    fn bind_select(&mut self, select: &mut ast::Select) -> Result<Vec<String>> {
        // 1. Bind CTEs from WITH clause
        if let Some(with) = &mut select.with {
            self.bind_cte(with)?;
        }

        // 2. Bind the main OneSelect
        let result_columns = self.bind_one_select(&mut select.body.select)?;

        // 3. Bind compound selects (UNION, INTERSECT, EXCEPT)
        for compound in &mut select.body.compounds {
            self.bind_one_select(&mut compound.select)?;
        }

        // 4. Bind ORDER BY (AliasFirst phase — aliases take priority)
        // ORDER BY lives on the outer Select, not on OneSelect.
        // It needs the FROM scope from the main select, but we don't
        // have it here anymore. For now we bind against an empty scope.
        // TODO: pass the main select's scope through
        let empty = BindScope::empty();
        self.with_phase(BindPhase::AliasFirst, |ctx| {
            for sort_col in &mut select.order_by {
                ctx.bind_expr(&mut sort_col.expr, &empty)?;
            }
            Ok(())
        })?;

        Ok(result_columns)
    }

    /// Bind a single SELECT (not compound). Returns result column names.
    fn bind_one_select(&mut self, one: &mut ast::OneSelect) -> Result<Vec<String>> {
        self.with_scope(|ctx| {
            match one {
                ast::OneSelect::Select {
                    columns,
                    from,
                    where_clause,
                    group_by,
                    ..
                } => {
                    // 1. Bind FROM → build scope
                    let scope = match from {
                        Some(from) => ctx.bind_from(from)?,
                        None => BindScope::empty(),
                    };

                    // 2. Extract result column names before binding expressions
                    //    (binding rewrites Expr::Id into Expr::Column, losing the name)
                    let result_names = Self::select_result_column_names(columns, &scope)?;

                    // 3. Bind SELECT expressions (NoAliases phase)
                    ctx.with_phase(BindPhase::NoAliases, |ctx| {
                        ctx.bind_select_list(columns, &scope)
                    })?;

                    // 4. Bind WHERE (TableFirst phase — table columns first, aliases as fallback)
                    if let Some(where_expr) = where_clause {
                        ctx.with_phase(BindPhase::TableFirst, |ctx| {
                            ctx.bind_expr(where_expr, &scope)
                        })?;
                    }

                    // 5. Bind GROUP BY and HAVING (AliasFirst phase)
                    if let Some(group_by) = group_by {
                        ctx.with_phase(BindPhase::AliasFirst, |ctx| {
                            ctx.bind_group_by(group_by, &scope)
                        })?;
                    }

                    Ok(result_names)
                }
                ast::OneSelect::Values(_) => {
                    // VALUES clauses have no column references to bind
                    Ok(vec![])
                }
            }
        })
    }

    /// Bind expressions in the SELECT list.
    fn bind_select_list(
        &mut self,
        columns: &mut [ast::ResultColumn],
        scope: &BindScope,
    ) -> Result<()> {
        for col in columns.iter_mut() {
            match col {
                ast::ResultColumn::Expr(expr, _) => {
                    self.bind_expr(expr, scope)?;
                }
                // Star and TableStar don't contain expressions to bind
                ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => {}
            }
        }
        Ok(())
    }

    /// Bind GROUP BY expressions and HAVING clause.
    fn bind_group_by(&mut self, group_by: &mut ast::GroupBy, scope: &BindScope) -> Result<()> {
        for expr in &mut group_by.exprs {
            self.bind_expr(expr, scope)?;
        }
        if let Some(having) = &mut group_by.having {
            self.bind_expr(having, scope)?;
        }
        Ok(())
    }

    fn bind_cte(&mut self, with: &mut ast::With) -> Result<()> {
        if with.recursive {
            crate::bail_parse_error!("Recursive CTEs are not yet supported");
        }

        // Pass 1: register all CTE names
        for cte in &with.ctes {
            let cte_name = normalize_ident(cte.tbl_name.as_str());
            if self.ctes.contains_key(&cte_name) {
                crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
            }
            let explicit_columns: Vec<String> = cte
                .columns
                .iter()
                .map(|c| normalize_ident(c.col_name.as_str()))
                .collect();
            self.insert_cte(
                cte_name,
                CteEntry {
                    select: cte.select.clone(),
                    explicit_columns,
                    cte_id: None,
                    resolved_columns: vec![],
                },
            );
        }

        // Pass 2: bind each CTE body and populate resolved columns
        for cte in &mut with.ctes {
            let cte_name = normalize_ident(cte.tbl_name.as_str());
            let result_columns = self.bind_select(&mut cte.select)?;

            let entry = self.ctes.get_mut(&cte_name).unwrap();
            if entry.explicit_columns.is_empty() {
                entry.resolved_columns = result_columns;
            } else {
                entry.resolved_columns = entry.explicit_columns.clone();
            }
        }
        Ok(())
    }

    fn resolve_select_table(&mut self, table: &mut ast::SelectTable) -> Result<ScopeTable> {
        match table {
            // Named table: CTE lookup first, then schema lookup
            ast::SelectTable::Table(name, alias, _indexed) => {
                let table_name = normalize_ident(name.name.as_str());
                // 1. Determine identifier (alias or table name)
                let identifier = alias
                    .as_ref()
                    .map(|a| match a {
                        ast::As::As(id) | ast::As::Elided(id) => normalize_ident(id.as_str()),
                    })
                    .unwrap_or_else(|| table_name.clone());

                // 2. Check self.ctes for a CTE match
                if let Some(cte) = self.ctes.get(&table_name) {
                    //    - resolved_columns was populated by bind_cte pass 2
                    //    - Build Arc<CteTable> as the BindTable
                    let cte_table = Arc::new(CteTable {
                        name: table_name,
                        columns: cte.resolved_columns.clone(),
                    });
                    // 4. Generate internal_id via self.id_gen.next_id()
                    return Ok(ScopeTable {
                        identifier,
                        internal_id: self.id_gen.next_id(),
                        table: cte_table,
                        join_info: None,
                    });
                }

                // 3. Otherwise, schema lookup via resolver
                //    - Build Arc<Table> as the BindTable (Table already implements BindTable)
                let schema_table =
                    self.resolver
                        .schema()
                        .get_table(&table_name)
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!("no such table: {}", table_name))
                        })?;

                // 4. Generate internal_id via self.id_gen.next_id()
                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_id(),
                    table: schema_table,
                    join_info: None,
                })
            }
            // Inline subquery in FROM: SELECT ... FROM (SELECT ...)
            ast::SelectTable::Select(subselect, alias) => {
                // 1. Alias is required by SQLite for FROM subqueries
                let identifier = alias
                    .as_ref()
                    .map(|a| match a {
                        ast::As::As(id) | ast::As::Elided(id) => normalize_ident(id.as_str()),
                    })
                    .unwrap_or_else(|| String::from("subquery"));

                // FROM subqueries don't correlate with the query being built.
                // The outer_scopes stack already contains any enclosing query scopes.
                let result_columns = self.bind_select(subselect)?;

                // 5-6. Build Arc<CteTable> with the result column names
                let subquery_table = Arc::new(CteTable {
                    name: identifier.clone(),
                    columns: result_columns,
                });

                // 7. Generate internal_id
                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_id(),
                    table: subquery_table,
                    join_info: None,
                })
            }
            // Virtual table function call: SELECT ... FROM table_func(args)
            ast::SelectTable::TableCall(name, args, alias) => {
                let table_name = normalize_ident(name.name.as_str());
                // 1. Look up the virtual table via resolver
                let schema_table =
                    self.resolver
                        .schema()
                        .get_table(&table_name)
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!("no such table: {}", table_name))
                        })?;

                let identifier = alias
                    .as_ref()
                    .map(|a| match a {
                        ast::As::As(id) | ast::As::Elided(id) => normalize_ident(id.as_str()),
                    })
                    .unwrap_or_else(|| table_name.clone());

                // 2. Bind argument expressions (typically literals, no FROM scope yet)
                let empty_scope = BindScope::empty();
                for arg in args.iter_mut() {
                    self.bind_expr(arg, &empty_scope)?;
                }

                // 3. Build ScopeTable from the virtual table's columns
                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_id(),
                    table: schema_table,
                    join_info: None,
                })
            }
            // Parenthesized FROM subclause: SELECT ... FROM (t1 JOIN t2 ON ...)
            ast::SelectTable::Sub(from_clause, alias) => {
                // 1. Recursively bind_from(from_clause)
                let inner_scope = self.bind_from(from_clause)?;

                // 2-3. Collect all column names from inner scope tables
                let all_columns: Vec<String> = inner_scope
                    .tables
                    .iter()
                    .flat_map(|table| table.table.columns())
                    .map(|col| col.name.to_string())
                    .collect();

                let identifier = alias
                    .as_ref()
                    .map(|a| match a {
                        ast::As::As(id) | ast::As::Elided(id) => normalize_ident(id.as_str()),
                    })
                    .unwrap_or_else(|| String::from("subquery"));

                // If alias is present, wrap all columns under that alias
                // If no alias, flatten tables into parent scope
                let sub_table = Arc::new(CteTable {
                    name: identifier.clone(),
                    columns: all_columns,
                });

                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_id(),
                    table: sub_table,
                    join_info: None,
                })
            }
        }
    }

    /// Bind an expression, resolving column references against the given scope.
    fn bind_expr(&mut self, _expr: &mut ast::Expr, _scope: &BindScope) -> Result<()> {
        // TODO: walk the expression tree and resolve Expr::Id, Expr::Qualified, etc.
        Ok(())
    }

    fn bind_from(&mut self, from: &mut ast::FromClause) -> Result<BindScope> {
        let mut tables: Vec<ScopeTable> = Vec::new();

        tables.push(self.resolve_select_table(&mut from.select)?);
        for join in &mut from.joins {
            tables.push(self.resolve_select_table(&mut join.table)?);
        }

        let scope = BindScope {
            tables,
            right_join_swapped: false,
        };

        // Bind ON expressions against the complete scope
        for join in &mut from.joins {
            match &mut join.constraint {
                Some(JoinConstraint::On(expr)) => {
                    self.bind_expr(expr, &scope)?;
                }
                Some(JoinConstraint::Using(columns)) => {
                    for col_name in columns {
                        if scope.find_column_unqualified(col_name.as_str())?.is_none() {
                            crate::bail_parse_error!("cannot join using column {} - column not present in both sides of the join", col_name);
                        }
                    }
                }
                None => {}
            }
        }

        Ok(scope)
    }
}

// ── BoundSelect ──────────────────────────────────────────────────────────

/// Result of binding a SELECT statement.
pub struct BoundSelect {
    /// Bound result columns with aliases extracted.
    /// `contains_aggregates` is `false` — the planner fills it in.
    pub result_columns: Vec<ResultSetColumn>,
}
