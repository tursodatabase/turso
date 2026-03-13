use crate::function::Func;
use crate::sync::Arc;
use crate::vdbe::builder::ProgramBuilder;

use rustc_hash::FxHashMap as HashMap;
use smallvec::SmallVec;
use turso_parser::ast::{self, JoinConstraint, TableInternalId};

use super::emitter::Resolver;
use super::expr::{walk_expr, walk_expr_mut, WalkControl};
use super::optimizer::TakeOwnership;
use super::plan::{JoinInfo, JoinOrderMember, TableReferences};
use super::planner::parse_row_id;
use crate::schema::Table;
use crate::util::normalize_ident;
use crate::Result;

// ── IdGenerator ─────────────────────────────────────────────────────────

pub trait IdGenerator {
    fn next_table_id(&mut self) -> TableInternalId;
    fn next_cte_id(&mut self) -> usize;
}

impl IdGenerator for ProgramBuilder {
    fn next_table_id(&mut self) -> ast::TableInternalId {
        self.table_reference_counter.next()
    }

    fn next_cte_id(&mut self) -> usize {
        self.alloc_cte_id()
    }
}

// ── BindTable ───────────────────────────────────────────────────────────

/// Trait for table metadata needed during binding (column name resolution).
pub trait BindTable {
    fn column_count(&self) -> usize;
    fn column_name(&self, idx: usize) -> Option<&str>;
    fn column_is_rowid_alias(&self, idx: usize) -> bool;
    fn column_is_hidden(&self, idx: usize) -> bool;
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
    pub is_hidden: bool,
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
            is_hidden: self.table.column_is_hidden(i),
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

    fn column_is_hidden(&self, idx: usize) -> bool {
        self.columns().get(idx).is_some_and(|c| c.hidden())
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

    fn column_is_hidden(&self, _idx: usize) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct DerivedTable {
    pub name: String,
    pub columns: Vec<String>,
}

impl BindTable for DerivedTable {
    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| s.as_str())
    }

    fn column_is_rowid_alias(&self, _idx: usize) -> bool {
        false
    }

    fn column_is_hidden(&self, _idx: usize) -> bool {
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
    /// Planner-facing source data for producing `TableReferences`.
    pub source: ScopeTableSource,
    /// Table metadata for column resolution. Clone is an Arc bump.
    pub table: Arc<dyn BindTable>,
    /// Join constraint info (USING clause for dedup during unqualified lookup).
    pub join_info: Option<JoinInfo>,
}

#[derive(Clone)]
pub enum ScopeTableSource {
    Table(Arc<Table>),
    Cte {
        name: String,
        columns: Vec<String>,
        cte_id: usize,
        select: ast::Select,
    },
    Derived {
        name: String,
        columns: Vec<String>,
    },
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

// ── BoundColumn ─────────────────────────────────────────────────────────

/// A resolved result column from a SELECT list.
/// Used for alias resolution in later phases (WHERE, GROUP BY, ORDER BY)
/// and for propagating column names to CTEs/subqueries.
#[derive(Clone)]
pub struct BoundColumn {
    /// The column name — explicit alias or inferred from the expression.
    pub name: String,
    /// The original expression (before binding), cloned into alias references.
    pub expr: ast::Expr,
}

/// A subquery expression that was bound during binding.
/// The inner `ast::Select` is already bound (column refs resolved).
/// The planner uses this to plan the subquery without re-binding.
pub struct BoundSubquery {
    /// The bound inner SELECT.
    pub select: ast::Select,
    /// Inner binding results (scopes → table references).
    pub inner_bound: BoundSelect,
}

pub struct BoundSelect {
    pub result_columns: Vec<BoundColumn>,
    pub main_scope: BindScope,
    pub main_join_order: Vec<JoinOrderMember>,
    pub compound_scopes: Vec<BindScope>,
    pub compound_join_orders: Vec<Vec<JoinOrderMember>>,
    pub tracking: BindTracking,
    /// Expression subqueries (EXISTS, scalar subquery, IN SELECT) keyed by
    /// the `subquery_id` stored in the corresponding `Expr::SubqueryResult`.
    pub subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    /// CTE definitions from the WITH clause, in definition order.
    /// Populated only for the top-level select that owns the WITH clause.
    pub cte_definitions: Vec<(String, CteEntry)>,
}

#[derive(Clone)]
struct OuterQueryFrame {
    scope: BindScopeRef,
    aliases: Vec<BoundColumn>,
}

impl BoundSelect {
    pub fn into_table_references(
        self,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
    ) -> Result<Vec<TableReferences>> {
        let mut all = Vec::with_capacity(1 + self.compound_scopes.len());

        let main_refs =
            Self::scope_to_table_references(self.main_scope, &self.tracking, planned_ctes)?;
        all.push(main_refs);

        for scope in self.compound_scopes {
            all.push(Self::scope_to_table_references(scope, &self.tracking, planned_ctes)?);
        }

        Ok(all)
    }

    fn scope_to_table_references(
        scope: BindScope,
        tracking: &BindTracking,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
    ) -> Result<TableReferences> {
        let joined_tables = scope
            .tables
            .into_iter()
            .map(|scope_table| match scope_table.source {
                ScopeTableSource::Table(table) => Ok(super::plan::JoinedTable {
                    op: super::plan::Operation::default_scan_for(&table),
                    column_use_counts: vec![0; table.columns().len()],
                    table: (*table).clone(),
                    identifier: scope_table.identifier,
                    internal_id: scope_table.internal_id,
                    join_info: scope_table.join_info,
                    col_used_mask: Default::default(),
                    expression_index_usages: Vec::new(),
                    database_id: 0,
                }),
                ScopeTableSource::Cte { name, .. } => {
                    let mut cte_table = planned_ctes.remove(&name).ok_or_else(|| {
                        crate::LimboError::InternalError(format!(
                            "CTE '{name}' was not planned before into_table_references"
                        ))
                    })?;
                    // Use the scope's identifier (may be aliased) and internal_id
                    cte_table.identifier = scope_table.identifier;
                    cte_table.internal_id = scope_table.internal_id;
                    cte_table.join_info = scope_table.join_info;
                    Ok(cte_table)
                }
                ScopeTableSource::Derived { name, .. } => {
                    Err(crate::LimboError::InternalError(format!(
                        "derived bind source {name} cannot yet be converted into planner table references"
                    )))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let mut table_references = TableReferences::new(joined_tables, Vec::new());
        tracking.flush(&mut table_references);
        Ok(table_references)
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
///
/// `Clone` copies metadata (name, columns, IDs) but sets `inner_bound` to `None`.
/// This is intentional: `with_query` clones CTEs for subquery scoping, where only
/// the column/name info is needed for resolution, not the full binding output.
pub struct CteEntry {
    /// The bound AST (column refs resolved).
    pub select: ast::Select,
    /// Explicit column names from `WITH t(a, b) AS (...)`.
    pub explicit_columns: Vec<String>,
    /// Globally unique CTE identity for materialization tracking.
    pub cte_id: usize,
    /// Result column names, populated after binding the CTE body.
    /// If explicit_columns is non-empty, equals explicit_columns.
    /// Otherwise, extracted from the SELECT result columns.
    pub resolved_columns: Vec<String>,
    /// Inner binding results (scopes, tracking, subquery bindings).
    pub inner_bound: Option<BoundSelect>,
    /// Indexes of CTEs (in definition order) that this CTE directly references.
    pub referenced_cte_indices: SmallVec<[usize; 2]>,
    /// True if `AS MATERIALIZED` was specified, forcing materialization.
    pub materialize_hint: bool,
}

impl Clone for CteEntry {
    fn clone(&self) -> Self {
        Self {
            select: self.select.clone(),
            explicit_columns: self.explicit_columns.clone(),
            cte_id: self.cte_id,
            resolved_columns: self.resolved_columns.clone(),
            inner_bound: None,
            referenced_cte_indices: self.referenced_cte_indices.clone(),
            materialize_hint: self.materialize_hint,
        }
    }
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

    /// Stack of outer query scopes plus visible aliases.
    outer_query_frames: Vec<OuterQueryFrame>,

    /// Outer FROM schema for LATERAL join support.
    outer_from_scope: Option<BindScopeRef>,

    /// CTE definitions visible in the current query.
    ctes: HashMap<String, CteEntry>,

    /// SELECT result columns for alias resolution in later phases.
    /// Populated after binding the SELECT list.
    aliases: Vec<BoundColumn>,

    /// Current binding phase — controls alias visibility.
    phase: BindPhase,

    /// Records column/rowid usage for post-binding flush.
    pub tracking: BindTracking,

    /// Expression subqueries bound during this query, keyed by subquery_id.
    /// Moved into `BoundSelect` when binding completes.
    subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
}

impl<'a, G: IdGenerator> BindContext<'a, G> {
    pub fn new(resolver: &'a Resolver<'a>, id_gen: &'a mut G) -> Self {
        Self {
            resolver,
            id_gen,
            outer_query_frames: Vec::new(),
            outer_from_scope: None,
            ctes: HashMap::default(),
            aliases: Vec::new(),
            phase: BindPhase::NoAliases,
            tracking: BindTracking::default(),
            subquery_bindings: HashMap::default(),
        }
    }

    // ── Outer scope stack (mirrors DataFusion PlannerContext) ─────────

    /// Push a scope onto the outer-scope stack (entering a subquery).
    fn append_outer_query_scope(&mut self, scope: BindScopeRef, aliases: Vec<BoundColumn>) {
        self.outer_query_frames
            .push(OuterQueryFrame { scope, aliases });
    }

    /// Pop the most recent outer scope (exiting a subquery).
    fn pop_outer_query_scope(&mut self) -> Option<OuterQueryFrame> {
        self.outer_query_frames.pop()
    }

    /// Iterate outer scopes innermost-first (reversed storage order).
    /// Matches column lookup precedence: nearest enclosing query first.
    fn outer_scopes_iter(&self) -> impl Iterator<Item = &BindScopeRef> {
        self.outer_query_frames
            .iter()
            .rev()
            .map(|frame| &frame.scope)
    }

    fn outer_query_frames_iter(&self) -> impl Iterator<Item = &OuterQueryFrame> {
        self.outer_query_frames.iter().rev()
    }

    /// The immediately enclosing query's scope (if any).
    fn latest_outer_scope(&self) -> Option<&BindScopeRef> {
        self.outer_query_frames.last().map(|frame| &frame.scope)
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

    pub fn insert_cte(&mut self, name: String, entry: CteEntry) {
        self.ctes.insert(name, entry);
    }

    pub fn has_cte(&self, name: &str) -> bool {
        self.ctes.contains_key(name)
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

    fn set_aliases(&mut self, aliases: Vec<BoundColumn>) {
        self.aliases = aliases;
    }

    fn aliases(&self) -> &[BoundColumn] {
        &self.aliases
    }

    /// Run `f` with a fresh per-select-core state (phase, aliases).
    /// Saves and restores on exit so individual SELECT cores in the same
    /// compound query do not clobber each other.
    fn with_scope<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let saved_aliases = std::mem::take(&mut self.aliases);
        let saved_phase = self.phase;

        let result = f(self);

        self.aliases = saved_aliases;
        self.phase = saved_phase;

        result
    }

    /// Run `f` with a fresh query state, restoring CTE/alias/phase state on exit.
    ///
    /// This mirrors DataFusion's per-query PlannerContext cloning semantics:
    /// subqueries inherit outer CTEs, but their own WITH items remain private.
    fn with_query<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let saved_ctes = self.ctes.clone();
        let saved_aliases = std::mem::take(&mut self.aliases);
        let saved_phase = self.phase;
        let saved_outer_from_scope = self.outer_from_scope.clone();
        let saved_tracking = std::mem::take(&mut self.tracking);

        let result = f(self);

        self.ctes = saved_ctes;
        self.aliases = saved_aliases;
        self.phase = saved_phase;
        self.outer_from_scope = saved_outer_from_scope;
        self.tracking = saved_tracking;

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

    /// Extract result columns from a SELECT list before the main bind pass.
    ///
    /// Captures the name and a bound expression for each result column.
    /// For identifiers and star expansions, the expression is resolved
    /// to `Expr::Column` immediately. For complex expressions, the
    /// original AST is cloned and bound via `bind_expr`.
    ///
    /// Must be called before `bind_select_list` rewrites the AST in-place,
    /// since we need the raw identifiers to infer column names.
    fn extract_bound_columns(
        &mut self,
        columns: &mut [ast::ResultColumn],
        scope: &BindScope,
    ) -> Result<Vec<BoundColumn>> {
        let mut result = Vec::with_capacity(columns.len());
        for col in columns {
            match col {
                ast::ResultColumn::Expr(expr, alias) => {
                    // Determine the column name
                    let name = if let Some(a) = alias {
                        match a {
                            ast::As::As(id) | ast::As::Elided(id) => normalize_ident(id.as_str()),
                        }
                    } else {
                        match expr.as_ref() {
                            ast::Expr::Id(id) => normalize_ident(id.as_str()),
                            ast::Expr::Qualified(_, id) => normalize_ident(id.as_str()),
                            ast::Expr::DoublyQualified(_, _, id) => normalize_ident(id.as_str()),
                            // Complex expressions without an alias can't be
                            // referenced by name from outer queries.
                            _ => String::new(),
                        }
                    };
                    // Resolve the expression
                    self.bind_expr(expr, scope)?;
                    result.push(BoundColumn {
                        name,
                        expr: *expr.clone(),
                    });
                }
                ast::ResultColumn::Star => {
                    // Expand * — each column becomes a resolved Expr::Column
                    for st in &scope.tables {
                        for col_ref in st.table.columns() {
                            result.push(BoundColumn {
                                name: col_ref.name.to_string(),
                                expr: ast::Expr::Column {
                                    database: None,
                                    table: st.internal_id,
                                    column: col_ref.idx,
                                    is_rowid_alias: col_ref.is_rowid_alias,
                                },
                            });
                        }
                    }
                }
                ast::ResultColumn::TableStar(table_name) => {
                    let Some(st) = scope.find_table_by_identifier(table_name.as_str()) else {
                        crate::bail_parse_error!("no such table: {}", table_name);
                    };
                    for col_ref in st.table.columns() {
                        result.push(BoundColumn {
                            name: col_ref.name.to_string(),
                            expr: ast::Expr::Column {
                                database: None,
                                table: st.internal_id,
                                column: col_ref.idx,
                                is_rowid_alias: col_ref.is_rowid_alias,
                            },
                        });
                    }
                }
            }
        }
        Ok(result)
    }

    /// Bind a SELECT statement, resolving all name references in-place.
    /// Returns the bound query result needed by planning.
    pub fn bind_select(&mut self, select: &mut ast::Select) -> Result<BoundSelect> {
        self.with_query(|ctx| {
            // 1. Bind CTEs from WITH clause
            if let Some(with) = &mut select.with {
                ctx.bind_cte(with)?;
            }

            // 2. Bind the main OneSelect. Its aliases and FROM scope are the ones
            // visible to the query-level ORDER BY.
            let (result_columns, main_scope, main_join_order) =
                ctx.bind_one_select(&mut select.body.select)?;

            // 3. Bind compound selects (UNION, INTERSECT, EXCEPT)
            let mut compound_scopes = Vec::with_capacity(select.body.compounds.len());
            let mut compound_join_orders = Vec::with_capacity(select.body.compounds.len());
            for compound in &mut select.body.compounds {
                let (_compound_cols, compound_scope, compound_join_order) =
                    ctx.bind_one_select(&mut compound.select)?;
                compound_scopes.push(compound_scope);
                compound_join_orders.push(compound_join_order);
            }

            // 4. Bind ORDER BY (AliasFirst phase — aliases take priority)
            ctx.set_aliases(result_columns.clone());
            ctx.with_phase(BindPhase::AliasFirst, |ctx| {
                for sort_col in &mut select.order_by {
                    ctx.replace_column_number(&mut sort_col.expr)?;
                    ctx.bind_expr(&mut sort_col.expr, &main_scope)?;
                }
                Ok(())
            })?;

            // 5. Bind LIMIT/OFFSET (no scope — these are standalone expressions)
            if let Some(limit) = select.limit.as_mut() {
                let empty = BindScope::empty();
                ctx.bind_expr(&mut limit.expr, &empty)?;
                if let Some(offset) = limit.offset.as_mut() {
                    ctx.bind_expr(offset, &empty)?;
                }
            }

            // 6. Extract CTE definitions before with_query restores them
            let cte_definitions: Vec<(String, CteEntry)> = if select.with.is_some() {
                std::mem::take(&mut ctx.ctes).into_iter().collect()
            } else {
                vec![]
            };

            Ok(BoundSelect {
                result_columns,
                main_scope,
                main_join_order,
                compound_scopes,
                compound_join_orders,
                tracking: std::mem::take(&mut ctx.tracking),
                subquery_bindings: std::mem::take(&mut ctx.subquery_bindings),
                cte_definitions,
            })
        })
    }

    /// Bind a single SELECT (not compound). Returns bound result columns,
    /// the scope, and the join order.
    fn bind_one_select(
        &mut self,
        one: &mut ast::OneSelect,
    ) -> Result<(Vec<BoundColumn>, BindScope, Vec<JoinOrderMember>)> {
        self.with_scope(|ctx| {
            match one {
                ast::OneSelect::Select {
                    columns,
                    from,
                    where_clause,
                    group_by,
                    window_clause,
                    ..
                } => {
                    // 1. Bind FROM → build scope
                    let scope = match from {
                        Some(from) => ctx.bind_from(from)?,
                        None => BindScope::empty(),
                    };

                    // 2. Expand Star/TableStar in-place before any binding
                    ctx.expand_stars(columns, &scope);

                    // 3. Build join order from scope
                    let join_order = Self::build_join_order(&scope);

                    // 4. Bind WINDOW definitions (NoAliases — same phase as SELECT list)
                    ctx.with_phase(BindPhase::NoAliases, |ctx| {
                        ctx.bind_window_defs(window_clause, &scope)
                    })?;

                    // 5. Extract bound columns (names + resolved exprs) before
                    //    the main bind pass rewrites the AST in-place.
                    let bound_columns = ctx.extract_bound_columns(columns, &scope)?;

                    // 6. Store as aliases for later phases (WHERE, GROUP BY, ORDER BY)
                    ctx.set_aliases(bound_columns.clone());

                    // 7. Bind SELECT expressions in-place (NoAliases phase)
                    ctx.with_phase(BindPhase::NoAliases, |ctx| {
                        ctx.bind_select_list(columns, &scope)
                    })?;

                    // 8. Bind WHERE (TableFirst phase — table columns first, aliases as fallback)
                    if let Some(where_expr) = where_clause {
                        ctx.with_phase(BindPhase::TableFirst, |ctx| {
                            ctx.bind_expr(where_expr, &scope)
                        })?;
                    }

                    // 9. Bind GROUP BY and HAVING (table columns win in GROUP BY)
                    if let Some(group_by) = group_by {
                        ctx.with_phase(BindPhase::TableFirst, |ctx| {
                            ctx.bind_group_by(group_by, &scope)
                        })?;
                    }

                    Ok((bound_columns, scope, join_order))
                }
                ast::OneSelect::Values(rows) => {
                    let scope = BindScope::empty();
                    for row in rows.iter_mut() {
                        for expr in row.iter_mut() {
                            ctx.bind_expr(expr, &scope)?;
                        }
                    }
                    Ok((Vec::new(), scope, vec![]))
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

    /// Bind WINDOW definition expressions (PARTITION BY, ORDER BY).
    fn bind_window_defs(
        &mut self,
        window_defs: &mut [ast::WindowDef],
        scope: &BindScope,
    ) -> Result<()> {
        for def in window_defs.iter_mut() {
            for expr in &mut def.window.partition_by {
                self.bind_expr(expr, scope)?;
            }
            for sorted_col in &mut def.window.order_by {
                self.bind_expr(&mut sorted_col.expr, scope)?;
            }
        }
        Ok(())
    }

    /// Replace a numeric literal (e.g. `1`, `2`) with the corresponding
    /// SELECT result column expression. Per SQLite semantics, only positive
    /// integer literals are treated as column references; floats and negative
    /// numbers are left as constant expressions.
    fn replace_column_number(&self, expr: &mut ast::Expr) -> Result<()> {
        if let ast::Expr::Literal(ast::Literal::Numeric(num)) = expr {
            if let Ok(column_number) = num.parse::<usize>() {
                if column_number == 0 {
                    crate::bail_parse_error!("invalid column index: {}", column_number);
                }
                let aliases = self.aliases();
                match aliases.get(column_number - 1) {
                    Some(bound_col) => {
                        *expr = bound_col.expr.clone();
                    }
                    None => {
                        crate::bail_parse_error!("invalid column index: {}", column_number);
                    }
                }
            }
        }
        Ok(())
    }

    /// Bind GROUP BY expressions and HAVING clause.
    fn bind_group_by(&mut self, group_by: &mut ast::GroupBy, scope: &BindScope) -> Result<()> {
        let saved_outer_query_frames = std::mem::take(&mut self.outer_query_frames);
        let group_result: Result<()> = (|| {
            for expr in &mut group_by.exprs {
                self.replace_column_number(expr)?;
                self.bind_expr(expr, scope)?;
            }
            Ok(())
        })();
        self.outer_query_frames = saved_outer_query_frames;
        group_result?;
        if let Some(having) = &mut group_by.having {
            self.with_phase(BindPhase::AliasFirst, |ctx| ctx.bind_expr(having, scope))?;
        }
        Ok(())
    }

    /// Expand `Star` and `TableStar` result columns in-place.
    ///
    /// After this, the `columns` vec contains only `ResultColumn::Expr` entries.
    /// Handles USING dedup, hidden columns, semi/anti-join filtering, and
    /// right_join_swapped ordering — matching the planner's `select_star`.
    fn expand_stars(&mut self, columns: &mut Vec<ast::ResultColumn>, scope: &BindScope) {
        let mut expanded = Vec::with_capacity(columns.len());
        for col in columns.drain(..) {
            match col {
                ast::ResultColumn::Star => {
                    let table_iter: Vec<&ScopeTable> = if scope.right_join_swapped {
                        scope.tables.iter().rev().collect()
                    } else {
                        scope.tables.iter().collect()
                    };
                    for st in table_iter {
                        // Semi/anti-join tables don't contribute to SELECT *
                        if st.join_info.as_ref().is_some_and(|ji| ji.is_semi_or_anti()) {
                            continue;
                        }
                        for col_ref in st.table.columns() {
                            if col_ref.is_hidden {
                                continue;
                            }
                            // USING dedup: skip columns from right table that are in USING
                            if let Some(ji) = &st.join_info {
                                if ji
                                    .using
                                    .iter()
                                    .any(|u| u.as_str().eq_ignore_ascii_case(col_ref.name))
                                {
                                    continue;
                                }
                            }
                            self.tracking.record_column(st.internal_id, col_ref.idx);
                            expanded.push(ast::ResultColumn::Expr(
                                Box::new(ast::Expr::Column {
                                    database: None,
                                    table: st.internal_id,
                                    column: col_ref.idx,
                                    is_rowid_alias: col_ref.is_rowid_alias,
                                }),
                                None,
                            ));
                        }
                    }
                }
                ast::ResultColumn::TableStar(ref name) => {
                    let normalized = normalize_ident(name.as_str());
                    if let Some(st) = scope
                        .tables
                        .iter()
                        .find(|t| t.identifier.eq_ignore_ascii_case(&normalized))
                    {
                        for col_ref in st.table.columns() {
                            if col_ref.is_hidden {
                                continue;
                            }
                            self.tracking.record_column(st.internal_id, col_ref.idx);
                            expanded.push(ast::ResultColumn::Expr(
                                Box::new(ast::Expr::Column {
                                    database: None,
                                    table: st.internal_id,
                                    column: col_ref.idx,
                                    is_rowid_alias: col_ref.is_rowid_alias,
                                }),
                                None,
                            ));
                        }
                    } else {
                        // Table not found — leave as-is, planner will error
                        expanded.push(col);
                    }
                }
                other => expanded.push(other),
            }
        }
        *columns = expanded;
    }

    /// Build join order from scope tables.
    fn build_join_order(scope: &BindScope) -> Vec<JoinOrderMember> {
        scope
            .tables
            .iter()
            .enumerate()
            .map(|(i, st)| JoinOrderMember {
                table_id: st.internal_id,
                original_idx: i,
                is_outer: st.join_info.as_ref().is_some_and(|ji| ji.is_outer()),
            })
            .collect()
    }

    fn bind_cte(&mut self, with: &mut ast::With) -> Result<()> {
        if with.recursive {
            crate::bail_parse_error!("Recursive CTEs are not yet supported");
        }

        // Collect CTE names in definition order for referenced_cte_indices lookup.
        let mut cte_names: Vec<String> = Vec::with_capacity(with.ctes.len());

        // Pass 1: register all CTE names, allocate IDs, compute cross-references
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

            let cte_id = self.id_gen.next_cte_id();
            let materialize_hint =
                cte.materialized == turso_parser::ast::Materialized::Yes;

            // Determine which preceding CTEs this one directly references.
            let mut referenced_tables = Vec::new();
            super::planner::collect_from_clause_table_refs(&cte.select, &mut referenced_tables);
            let idx = cte_names.len();
            let referenced_cte_indices: SmallVec<[usize; 2]> = (0..idx)
                .filter(|&i| referenced_tables.contains(&cte_names[i]))
                .collect();

            cte_names.push(cte_name.clone());
            self.insert_cte(
                cte_name,
                CteEntry {
                    select: cte.select.clone(),
                    explicit_columns,
                    cte_id,
                    resolved_columns: vec![],
                    inner_bound: None,
                    referenced_cte_indices,
                    materialize_hint,
                },
            );
        }

        // Pass 2: bind each CTE body and populate resolved columns + inner binding.
        // We collect inner_bound values separately because bind_select calls with_query
        // which clones self.ctes (setting inner_bound = None via the custom Clone impl),
        // then restores them — destroying inner_bound values set in prior iterations.
        let mut inner_bounds: Vec<(String, BoundSelect)> = Vec::with_capacity(with.ctes.len());
        for cte in &mut with.ctes {
            let cte_name = normalize_ident(cte.tbl_name.as_str());
            let bound = self.bind_select(&mut cte.select)?;

            let entry = self.ctes.get_mut(&cte_name).unwrap();
            if entry.explicit_columns.is_empty() {
                entry.resolved_columns = bound
                    .result_columns
                    .iter()
                    .map(|bc| bc.name.clone())
                    .collect();
            } else {
                entry.resolved_columns = entry.explicit_columns.clone();
            }
            entry.select = cte.select.clone();
            inner_bounds.push((cte_name, bound));
        }
        // Assign inner_bound values after all binding is done.
        for (cte_name, bound) in inner_bounds {
            self.ctes.get_mut(&cte_name).unwrap().inner_bound = Some(bound);
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
                        name: table_name.clone(),
                        columns: cte.resolved_columns.clone(),
                    });
                    // 4. Generate internal_id via self.id_gen.next_table_id()
                    return Ok(ScopeTable {
                        identifier,
                        internal_id: self.id_gen.next_table_id(),
                        source: ScopeTableSource::Cte {
                            name: table_name,
                            columns: cte.resolved_columns.clone(),
                            cte_id: cte.cte_id,
                            select: cte.select.clone(),
                        },
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

                // 4. Generate internal_id via self.id_gen.next_table_id()
                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Table(schema_table.clone()),
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
                // TODO: change subquery name

                // FROM subqueries don't correlate with the query being built.
                // The outer_scopes stack already contains any enclosing query scopes.
                let bound_select = self.bind_select(subselect)?;

                // Build CteTable with the result column names
                let subquery_columns: Vec<String> = bound_select
                    .result_columns
                    .into_iter()
                    .map(|bc| bc.name)
                    .collect();
                let subquery_table = Arc::new(DerivedTable {
                    name: identifier.clone(),
                    columns: subquery_columns.clone(),
                });

                // 7. Generate internal_id
                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Derived {
                        name: subquery_table.name.clone(),
                        columns: subquery_columns,
                    },
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
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Table(schema_table.clone()),
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
                let sub_table = Arc::new(DerivedTable {
                    name: identifier.clone(),
                    columns: all_columns,
                });

                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Derived {
                        name: sub_table.name.clone(),
                        columns: sub_table.columns.clone(),
                    },
                    table: sub_table,
                    join_info: None,
                })
            }
        }
    }

    fn resolve_alias(&self, name: &str) -> Option<ast::Expr> {
        let normalized = normalize_ident(name);
        self.aliases()
            .iter()
            .find(|alias| alias.name.eq_ignore_ascii_case(&normalized))
            .map(|alias| alias.expr.clone())
    }

    fn resolve_outer_alias(&mut self, name: &str) -> Option<ast::Expr> {
        let normalized = normalize_ident(name);
        let resolved = self.outer_query_frames_iter().find_map(|frame| {
            frame
                .aliases
                .iter()
                .find(|alias| alias.name.eq_ignore_ascii_case(&normalized))
                .map(|alias| alias.expr.clone())
        })?;
        self.record_outer_refs_in_expr(&resolved);
        Some(resolved)
    }

    fn record_outer_refs_in_expr(&mut self, expr: &ast::Expr) {
        let _ = walk_expr(expr, &mut |expr| {
            if let ast::Expr::Column { table, column, .. } = expr {
                self.tracking.record_outer_ref(*table, *column);
            }
            Ok(WalkControl::Continue)
        });
    }

    fn resolve_unqualified_column(
        &mut self,
        name: &str,
        scope: &BindScope,
    ) -> Result<Option<ast::Expr>> {
        if let Some((table_id, col_idx, is_rowid_alias)) = scope.find_column_unqualified(name)? {
            self.tracking.record_column(table_id, col_idx);
            return Ok(Some(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }));
        }

        for st in &scope.tables {
            if let Some(row_id_expr) =
                parse_row_id(name, st.internal_id, || scope.tables.len() != 1)?
            {
                self.tracking.record_rowid(st.internal_id);
                return Ok(Some(row_id_expr));
            }
        }

        let outer_match = {
            let mut result = None;
            for outer_scope in self.outer_scopes_iter() {
                if let Some(found) = outer_scope.find_column_unqualified(name)? {
                    result = Some(found);
                    break;
                }
            }
            result
        };
        if let Some((table_id, col_idx, is_rowid_alias)) = outer_match {
            self.tracking.record_outer_ref(table_id, col_idx);
            return Ok(Some(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }));
        }

        Ok(None)
    }

    fn resolve_qualified_column(
        &mut self,
        table_name: &str,
        col_name: &str,
        scope: &BindScope,
    ) -> Result<Option<ast::Expr>> {
        if let Some((table_id, col_idx, is_rowid_alias)) =
            scope.find_column_qualified(table_name, col_name)?
        {
            self.tracking.record_column(table_id, col_idx);
            return Ok(Some(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }));
        }

        let outer_match = {
            let mut result = None;
            for outer_scope in self.outer_scopes_iter() {
                if let Some(found) = outer_scope.find_column_qualified(table_name, col_name)? {
                    result = Some(found);
                    break;
                }
            }
            result
        };
        if let Some((table_id, col_idx, is_rowid_alias)) = outer_match {
            self.tracking.record_outer_ref(table_id, col_idx);
            return Ok(Some(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }));
        }

        Ok(None)
    }

    fn bind_identifier(&mut self, expr: &mut ast::Expr, scope: &BindScope) -> Result<()> {
        match expr {
            ast::Expr::Id(id) => {
                let resolved = match self.phase() {
                    BindPhase::NoAliases => self.resolve_unqualified_column(id.as_str(), scope)?,
                    BindPhase::TableFirst => self
                        .resolve_unqualified_column(id.as_str(), scope)?
                        .or_else(|| self.resolve_alias(id.as_str()))
                        .or_else(|| self.resolve_outer_alias(id.as_str())),
                    BindPhase::AliasFirst => {
                        if let Some(alias) = self.resolve_alias(id.as_str()) {
                            Some(alias)
                        } else {
                            self.resolve_unqualified_column(id.as_str(), scope)?
                                .or_else(|| self.resolve_outer_alias(id.as_str()))
                        }
                    }
                };

                if let Some(resolved) = resolved {
                    *expr = resolved;
                    return Ok(());
                }

                if id.quoted_with('"') {
                    *expr = ast::Expr::Literal(ast::Literal::String(id.as_literal()));
                } else {
                    crate::bail_parse_error!("no such column: {}", id.as_str());
                }
            }
            ast::Expr::Qualified(tbl, col) => {
                if let Some(resolved) =
                    self.resolve_qualified_column(tbl.as_str(), col.as_str(), scope)?
                {
                    *expr = resolved;
                } else {
                    crate::bail_parse_error!("no such column: {}.{}", tbl.as_str(), col.as_str());
                }
            }
            ast::Expr::DoublyQualified(db_name, tbl_name, col_name) => {
                let database_id = self.resolver.resolve_database_id(&ast::QualifiedName {
                    db_name: Some(db_name.clone()),
                    name: tbl_name.clone(),
                    alias: None,
                })?;

                let Some(resolved) =
                    self.resolve_qualified_column(tbl_name.as_str(), col_name.as_str(), scope)?
                else {
                    crate::bail_parse_error!(
                        "no such column: {}.{}.{}",
                        db_name.as_str(),
                        tbl_name.as_str(),
                        col_name.as_str()
                    );
                };

                match resolved {
                    ast::Expr::Column {
                        table,
                        column,
                        is_rowid_alias,
                        ..
                    } => {
                        *expr = ast::Expr::Column {
                            database: Some(database_id),
                            table,
                            column,
                            is_rowid_alias,
                        };
                    }
                    other => *expr = other,
                }
            }
            _ => unreachable!("bind_identifier only handles identifier nodes"),
        }

        Ok(())
    }

    fn bind_subquery_expr(
        &mut self,
        select: &mut ast::Select,
        scope: &BindScope,
    ) -> Result<BoundSelect> {
        self.append_outer_query_scope(Arc::new(scope.clone()), self.aliases.clone());
        let result = self.bind_select(select);
        self.pop_outer_query_scope();
        result
    }

    /// Bind an expression, resolving column references against the given scope.
    fn bind_expr(&mut self, expr: &mut ast::Expr, scope: &BindScope) -> Result<()> {
        walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
            match expr {
                ast::Expr::Between {
                    lhs,
                    not,
                    start,
                    end,
                } => {
                    let (lower_op, upper_op) = if *not {
                        (ast::Operator::Greater, ast::Operator::Greater)
                    } else {
                        (ast::Operator::LessEquals, ast::Operator::LessEquals)
                    };
                    let start = start.take_ownership();
                    let lhs_v = lhs.take_ownership();
                    let end = end.take_ownership();

                    let lower =
                        ast::Expr::Binary(Box::new(start), lower_op, Box::new(lhs_v.clone()));
                    let upper = ast::Expr::Binary(Box::new(lhs_v), upper_op, Box::new(end));

                    *expr = if *not {
                        ast::Expr::Binary(Box::new(lower), ast::Operator::Or, Box::new(upper))
                    } else {
                        ast::Expr::Binary(Box::new(lower), ast::Operator::And, Box::new(upper))
                    };
                }
                ast::Expr::Id(_)
                | ast::Expr::Qualified(_, _)
                | ast::Expr::DoublyQualified(_, _, _) => {
                    self.bind_identifier(expr, scope)?;
                }
                ast::Expr::Exists(_) => {
                    let subquery_id = self.id_gen.next_table_id();
                    let ast::Expr::Exists(mut select) =
                        std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                    else {
                        unreachable!();
                    };
                    let inner_bound = self.bind_subquery_expr(&mut select, scope)?;
                    self.subquery_bindings.insert(
                        subquery_id,
                        BoundSubquery {
                            select,
                            inner_bound,
                        },
                    );
                    *expr = ast::Expr::SubqueryResult {
                        subquery_id,
                        lhs: None,
                        not_in: false,
                        query_type: ast::SubqueryType::Exists { result_reg: 0 },
                    };
                    return Ok(WalkControl::SkipChildren);
                }
                ast::Expr::Subquery(_) => {
                    let subquery_id = self.id_gen.next_table_id();
                    let ast::Expr::Subquery(mut select) =
                        std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                    else {
                        unreachable!();
                    };
                    let inner_bound = self.bind_subquery_expr(&mut select, scope)?;
                    self.subquery_bindings.insert(
                        subquery_id,
                        BoundSubquery {
                            select,
                            inner_bound,
                        },
                    );
                    *expr = ast::Expr::SubqueryResult {
                        subquery_id,
                        lhs: None,
                        not_in: false,
                        query_type: ast::SubqueryType::RowValue {
                            result_reg_start: 0,
                            num_regs: 0,
                        },
                    };
                    return Ok(WalkControl::SkipChildren);
                }
                ast::Expr::InSelect { .. } => {
                    let subquery_id = self.id_gen.next_table_id();
                    let ast::Expr::InSelect { lhs, not, rhs: mut select } =
                        std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                    else {
                        unreachable!();
                    };
                    // Bind lhs first against the current scope
                    // (already handled by walker for non-subquery children,
                    // but InSelect lhs needs explicit binding since we took ownership)
                    let mut lhs = lhs;
                    self.bind_expr(&mut lhs, scope)?;
                    let inner_bound = self.bind_subquery_expr(&mut select, scope)?;
                    self.subquery_bindings.insert(
                        subquery_id,
                        BoundSubquery {
                            select,
                            inner_bound,
                        },
                    );
                    *expr = ast::Expr::SubqueryResult {
                        subquery_id,
                        lhs: Some(lhs),
                        not_in: not,
                        query_type: ast::SubqueryType::In {
                            cursor_id: 0,
                            affinity_str: Arc::new(String::new()),
                        },
                    };
                    return Ok(WalkControl::SkipChildren);
                }
                ast::Expr::FunctionCallStar { name, filter_over } => {
                    if let Ok(func) = Func::resolve_function(name.as_str(), 0) {
                        if func.needs_star_expansion() && !scope.tables.is_empty() {
                            let mut args: Vec<Box<ast::Expr>> = Vec::new();
                            for st in &scope.tables {
                                for col_ref in st.table.columns() {
                                    if col_ref.is_hidden {
                                        continue;
                                    }
                                    // Column name as string literal
                                    let quoted = format!("'{}'", col_ref.name);
                                    args.push(Box::new(ast::Expr::Literal(ast::Literal::String(
                                        quoted,
                                    ))));
                                    // Column reference
                                    args.push(Box::new(ast::Expr::Column {
                                        database: None,
                                        table: st.internal_id,
                                        column: col_ref.idx,
                                        is_rowid_alias: col_ref.is_rowid_alias,
                                    }));
                                    self.tracking.record_column(st.internal_id, col_ref.idx);
                                }
                            }
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
                _ => {}
            }
            Ok(WalkControl::Continue)
        })?;
        Ok(())
    }

    fn bind_from(&mut self, from: &mut ast::FromClause) -> Result<BindScope> {
        use super::plan::JoinType as PlanJoinType;

        let mut tables: Vec<ScopeTable> = Vec::new();
        let mut right_join_swapped = false;

        tables.push(self.resolve_select_table(&mut from.select)?);
        for join in &mut from.joins {
            let mut st = self.resolve_select_table(&mut join.table)?;

            let (is_outer, is_full_outer, is_right, is_cross, is_natural) = match &join.operator {
                ast::JoinOperator::TypedJoin(Some(jt)) => {
                    let is_left = jt.contains(ast::JoinType::LEFT);
                    let is_right = jt.contains(ast::JoinType::RIGHT);
                    let is_outer = jt.contains(ast::JoinType::OUTER) || is_left;
                    let is_full = (is_left && is_right) || (is_outer && !is_left && !is_right);
                    let is_cross = jt.contains(ast::JoinType::CROSS);
                    let is_natural = jt.contains(ast::JoinType::NATURAL);
                    (
                        is_outer && !is_full,
                        is_full,
                        is_right && !is_left && !is_full,
                        is_cross,
                        is_natural,
                    )
                }
                _ => (false, false, false, false, false),
            };

            // NATURAL JOIN: find common columns and rewrite constraint to USING
            if is_natural {
                if join.constraint.is_some() {
                    crate::bail_parse_error!(
                        "NATURAL JOIN cannot be combined with ON or USING clause"
                    );
                }
                let right_table: &dyn BindTable = st.table.as_ref();
                let mut common_cols: Vec<ast::Name> = Vec::new();
                for right_col in right_table.columns() {
                    if right_col.is_hidden {
                        continue;
                    }
                    let mut found = false;
                    for left_st in &tables {
                        let left_table: &dyn BindTable = left_st.table.as_ref();
                        for left_col in left_table.columns() {
                            if left_col.is_hidden {
                                continue;
                            }
                            if left_col.name == right_col.name {
                                found = true;
                                break;
                            }
                        }
                        if found {
                            break;
                        }
                    }
                    if found {
                        common_cols.push(ast::Name::exact(right_col.name.to_string()));
                    }
                }
                if common_cols.is_empty() {
                    crate::bail_parse_error!("No columns found to NATURAL join on");
                }
                join.constraint = Some(JoinConstraint::Using(common_cols));
            }

            // Determine USING columns from (possibly rewritten) constraint
            let using_cols = match &join.constraint {
                Some(JoinConstraint::Using(cols)) => cols.iter().cloned().collect::<Vec<_>>(),
                _ => vec![],
            };

            // RIGHT JOIN: swap tables
            if is_right {
                let len = tables.len();
                if len > 1 {
                    crate::bail_parse_error!(
                        "RIGHT JOIN following another join is not yet supported. \
                         Try rewriting as LEFT JOIN or using a subquery."
                    );
                }
                tables.swap(0, len - 1);
                // The originally-left table (now at end position after push) gets the outer flag
                // But we haven't pushed yet, so mark the existing table as outer
                if let Some(first) = tables.first_mut() {
                    first.join_info = Some(JoinInfo {
                        join_type: PlanJoinType::LeftOuter,
                        using: using_cols.clone(),
                        no_reorder: false,
                    });
                }
                right_join_swapped = true;
                // The right-side table (being pushed) has no join_info (it's now the "left" side)
                tables.push(st);
            } else {
                let plan_join_type = if is_full_outer {
                    PlanJoinType::FullOuter
                } else if is_outer {
                    PlanJoinType::LeftOuter
                } else {
                    PlanJoinType::Inner
                };
                st.join_info = Some(JoinInfo {
                    join_type: plan_join_type,
                    using: using_cols,
                    no_reorder: is_cross,
                });
                tables.push(st);
            }
        }

        let scope = BindScope {
            tables,
            right_join_swapped,
        };

        // Bind ON expressions against the complete scope
        for join in &mut from.joins {
            match &mut join.constraint {
                Some(JoinConstraint::On(expr)) => {
                    self.bind_expr(expr, &scope)?;
                }
                Some(JoinConstraint::Using(cols)) => {
                    // Record USING columns as used so they get marked in TableReferences
                    for col_name in cols.iter() {
                        let name = normalize_ident(col_name.as_str());
                        for st in &scope.tables {
                            let bt: &dyn BindTable = st.table.as_ref();
                            for col_ref in bt.columns() {
                                if col_ref.name == name {
                                    self.tracking.record_column(st.internal_id, col_ref.idx);
                                    break;
                                }
                            }
                        }
                    }
                }
                None => {}
            }
        }

        Ok(scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{BTreeTable, Schema};
    use crate::{DatabaseCatalog, RwLock, SymbolTable};
    use turso_parser::ast::{Cmd, Stmt};
    use turso_parser::parser::Parser;

    #[derive(Default)]
    struct TestIdGenerator {
        next: usize,
    }

    impl IdGenerator for TestIdGenerator {
        fn next_table_id(&mut self) -> TableInternalId {
            let id = self.next;
            self.next += 1;
            id.into()
        }

        fn next_cte_id(&mut self) -> usize {
            let id = self.next;
            self.next += 1;
            id
        }
    }

    fn parse_select(sql: &str) -> ast::Select {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser
            .next_cmd()
            .expect("SQL should parse")
            .expect("SQL should contain a statement");
        match cmd {
            Cmd::Stmt(Stmt::Select(select)) => select,
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    fn with_bind_context<T>(
        table_ddls: &[&str],
        f: impl FnOnce(&mut BindContext<'_, TestIdGenerator>) -> T,
    ) -> T {
        let mut schema = Schema::new();
        for (idx, ddl) in table_ddls.iter().enumerate() {
            schema
                .add_btree_table(Arc::new(
                    BTreeTable::from_sql(ddl, (idx + 2) as i64).expect("table DDL should parse"),
                ))
                .expect("table should be added to schema");
        }

        let database_schemas = RwLock::new(HashMap::default());
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let symbol_table = SymbolTable::new();
        let resolver = Resolver::new(
            &schema,
            &database_schemas,
            &attached_databases,
            &symbol_table,
            false,
        );
        let mut id_gen = TestIdGenerator::default();
        let mut ctx = BindContext::new(&resolver, &mut id_gen);
        f(&mut ctx)
    }

    fn select_expr(select: &ast::Select, idx: usize) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { columns, .. } => match &columns[idx] {
                ast::ResultColumn::Expr(expr, _) => expr,
                other => panic!("expected expression result column, got {other:?}"),
            },
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn where_expr(select: &ast::Select) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { where_clause, .. } => where_clause
                .as_deref()
                .expect("expected WHERE clause on bound select"),
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn group_by_expr(select: &ast::Select, idx: usize) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { group_by, .. } => {
                &group_by.as_ref().expect("expected GROUP BY clause").exprs[idx]
            }
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn having_expr(select: &ast::Select) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { group_by, .. } => group_by
                .as_ref()
                .and_then(|group_by| group_by.having.as_deref())
                .expect("expected HAVING clause"),
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn order_by_expr(select: &ast::Select, idx: usize) -> &ast::Expr {
        &select.order_by[idx].expr
    }

    fn exists_subquery_id(select: &ast::Select) -> TableInternalId {
        match where_expr(select) {
            ast::Expr::SubqueryResult { subquery_id, .. } => *subquery_id,
            other => panic!("expected SubqueryResult in WHERE, got {other:?}"),
        }
    }

    fn subquery_id_from_expr(expr: &ast::Expr) -> TableInternalId {
        match expr {
            ast::Expr::SubqueryResult { subquery_id, .. } => *subquery_id,
            other => panic!("expected SubqueryResult expression, got {other:?}"),
        }
    }

    fn assert_column_expr(expr: &ast::Expr, table: usize, column: usize) {
        assert_eq!(
            expr,
            &ast::Expr::Column {
                database: None,
                table: TableInternalId::from(table),
                column,
                is_rowid_alias: false,
            }
        );
    }

    fn bind_select_error(
        ctx: &mut BindContext<'_, TestIdGenerator>,
        sql: &str,
    ) -> crate::LimboError {
        let mut select = parse_select(sql);
        match ctx.bind_select(&mut select) {
            Ok(_) => panic!("expected bind failure for SQL: {sql}"),
            Err(err) => err,
        }
    }

    #[test]
    fn bind_select_returns_main_scope_and_tracking() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT b FROM t WHERE a = 1 ORDER BY b");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_scope.tables.len(), 1);
            assert_eq!(bound.main_scope.tables[0].identifier, "t");
            assert_eq!(
                bound.main_scope.tables[0].internal_id,
                TableInternalId::from(0usize)
            );
            assert_eq!(bound.tracking.columns_used.len(), 2);
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 0)));
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 1)));
        });
    }

    #[test]
    fn bind_select_keeps_subquery_tracking_out_of_outer_tracking() {
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b)"], |ctx| {
            let mut select =
                parse_select("SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.b = a)");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(
                bound.tracking.columns_used,
                vec![(TableInternalId::from(0usize), 0)]
            );
            assert!(bound.tracking.outer_refs_used.is_empty());
        });
    }

    #[test]
    fn bound_select_into_table_references_populates_joined_tables() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT b FROM t WHERE a = 1");
            let bound = ctx.bind_select(&mut select).unwrap();
            let mut all_refs = bound.into_table_references(&mut HashMap::default()).unwrap();

            assert_eq!(all_refs.len(), 1);
            let table_references = all_refs.remove(0);
            assert_eq!(table_references.joined_tables().len(), 1);
            let table = &table_references.joined_tables()[0];
            assert_eq!(table.identifier, "t");
            assert_eq!(table.internal_id, TableInternalId::from(0usize));
            assert_eq!(table.table.get_name(), "t");
            assert!(table.col_used_mask.get(0));
            assert!(table.col_used_mask.get(1));
            assert!(table_references.outer_query_refs().is_empty());
        });
    }

    #[test]
    fn bind_cte_uses_bound_select_result_columns() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select =
                parse_select("WITH cte(col_x, col_y) AS (SELECT x, y FROM t) SELECT * FROM cte");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let cte = ctx.get_cte("cte").expect("cte should exist");
            assert_eq!(cte.resolved_columns, vec!["col_x", "col_y"]);
        });
    }

    #[test]
    fn bind_cte_allocates_cte_id_and_stores_inner_bound() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select =
                parse_select("WITH c AS (SELECT a, b FROM t WHERE a > 1) SELECT * FROM c");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let cte = ctx.get_cte("c").expect("cte should exist");
            // cte_id was allocated
            assert_eq!(cte.cte_id, 0);
            // inner_bound is populated
            assert!(cte.inner_bound.is_some());
            let inner = cte.inner_bound.as_ref().unwrap();
            assert_eq!(inner.main_scope.tables.len(), 1);
            assert_eq!(inner.main_scope.tables[0].identifier, "t");
            // resolved columns inferred from SELECT list
            assert_eq!(cte.resolved_columns, vec!["a", "b"]);
        });
    }

    #[test]
    fn bind_cte_tracks_referenced_cte_indices() {
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut select = parse_select(
                "WITH a AS (SELECT x FROM t), b AS (SELECT * FROM a) SELECT * FROM b",
            );
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let a = ctx.get_cte("a").expect("cte a should exist");
            assert!(a.referenced_cte_indices.is_empty());

            let b = ctx.get_cte("b").expect("cte b should exist");
            assert_eq!(b.referenced_cte_indices.as_slice(), &[0]);
        });
    }

    #[test]
    fn bind_cte_materialize_hint() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select(
                "WITH c AS MATERIALIZED (SELECT a FROM t) SELECT * FROM c",
            );
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let cte = ctx.get_cte("c").expect("cte should exist");
            assert!(cte.materialize_hint);
        });
    }

    #[test]
    fn select_list_uses_no_aliases_phase() {
        with_bind_context(&["CREATE TABLE t(x, a)"], |ctx| {
            let mut select = parse_select("SELECT a AS x, x FROM t");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_column_expr(select_expr(&select, 0), 0, 1);
            assert_column_expr(select_expr(&select, 1), 0, 0);
            assert_eq!(bound.result_columns[0].name, "x");
            assert_eq!(bound.result_columns[1].name, "x");
        });
    }

    #[test]
    fn where_clause_prefers_table_column_over_alias() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a AS b FROM t WHERE b = 1");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = where_expr(&select) else {
                panic!("expected bound WHERE binary expression");
            };
            assert_column_expr(lhs, 0, 1);
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("1".into()))
            );
        });
    }

    #[test]
    fn where_clause_falls_back_to_alias_when_no_table_column_matches() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS x FROM t WHERE x = 3");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = where_expr(&select) else {
                panic!("expected bound WHERE binary expression");
            };
            assert_eq!(
                lhs.as_ref(),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("3".into()))
            );
        });
    }

    #[test]
    fn group_by_prefers_source_column_over_alias_expression() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS b FROM t GROUP BY b");
            ctx.bind_select(&mut select).unwrap();

            assert_column_expr(group_by_expr(&select, 0), 0, 1);
        });
    }

    #[test]
    fn having_prefers_alias_expression_over_table_column() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS b FROM t GROUP BY c HAVING b > 10");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(&select) else {
                panic!("expected bound HAVING binary expression");
            };
            assert_eq!(
                lhs.as_ref(),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("10".into()))
            );
        });
    }

    #[test]
    fn order_by_prefers_alias_expression_over_table_column() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS b FROM t ORDER BY b");
            ctx.bind_select(&mut select).unwrap();

            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
        });
    }

    #[test]
    fn order_by_falls_back_to_main_scope_column_when_alias_is_missing() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a AS renamed FROM t ORDER BY b");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_column_expr(order_by_expr(&select, 0), 0, 1);
            assert_eq!(bound.tracking.columns_used.len(), 2);
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 0)));
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 1)));
        });
    }

    #[test]
    fn correlated_grouped_subquery_binds_inner_aliases_and_outer_references() {
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select(
                "SELECT t.a \
                 FROM t \
                 WHERE EXISTS (\
                    SELECT u.c + 2 AS a \
                    FROM u \
                    WHERE u.b = t.a \
                    GROUP BY a \
                    HAVING a > t.a \
                    ORDER BY a\
                 )",
            );
            let bound = ctx.bind_select(&mut select).unwrap();
            let sq_id = exists_subquery_id(&select);
            let subquery = &bound.subquery_bindings[&sq_id].select;

            assert_eq!(
                bound.tracking.columns_used,
                vec![(TableInternalId::from(0usize), 0)]
            );
            assert!(bound.tracking.outer_refs_used.is_empty());

            // t=0, subquery_id=1, u=2
            assert_eq!(
                select_expr(subquery, 0),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(2usize),
                        column: 1,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("2".into())).into_boxed(),
                )
            );

            let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = where_expr(subquery) else {
                panic!("expected bound inner WHERE binary expression");
            };
            assert_column_expr(lhs, 2, 0);
            assert_column_expr(rhs, 0, 0);

            assert_eq!(group_by_expr(subquery, 0), select_expr(subquery, 0));

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(subquery) else {
                panic!("expected bound inner HAVING binary expression");
            };
            assert_eq!(lhs.as_ref(), select_expr(subquery, 0));
            assert_column_expr(rhs, 0, 0);

            assert_eq!(order_by_expr(subquery, 0), select_expr(subquery, 0));
        });
    }

    #[test]
    fn derived_table_columns_flow_into_outer_alias_binding() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select(
                "SELECT sq.x AS y \
                 FROM (SELECT t.a + 1 AS x FROM t) AS sq \
                 WHERE y > 2 \
                 ORDER BY y",
            );
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_scope.tables.len(), 1);
            assert_eq!(bound.main_scope.tables[0].identifier, "sq");

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = where_expr(&select) else {
                panic!("expected bound outer WHERE binary expression");
            };
            assert_eq!(
                lhs.as_ref(),
                &ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(1usize),
                    column: 0,
                    is_rowid_alias: false,
                }
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("2".into()))
            );

            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(1usize),
                    column: 0,
                    is_rowid_alias: false,
                }
            );
        });
    }

    #[test]
    fn cte_query_combines_cte_scope_group_by_having_and_order_by() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select(
                "WITH cte AS (SELECT a, b FROM t) \
                 SELECT a + 1 AS b \
                 FROM cte \
                 GROUP BY b \
                 HAVING b > 2 \
                 ORDER BY b",
            );
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_scope.tables.len(), 1);
            assert_eq!(bound.main_scope.tables[0].identifier, "cte");
            assert!(matches!(
                bound.main_scope.tables[0].source,
                ScopeTableSource::Cte { .. }
            ));

            // cte_id=0, t (inside CTE body)=1, cte (outer FROM)=2
            let alias_expr = ast::Expr::Binary(
                ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(2usize),
                    column: 0,
                    is_rowid_alias: false,
                }
                .into_boxed(),
                ast::Operator::Add,
                ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
            );

            assert_eq!(select_expr(&select, 0), &alias_expr);
            assert_column_expr(group_by_expr(&select, 0), 2, 1);

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(&select) else {
                panic!("expected bound HAVING binary expression");
            };
            assert_eq!(lhs.as_ref(), &alias_expr);
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("2".into()))
            );

            assert_eq!(order_by_expr(&select, 0), &alias_expr);
        });
    }

    #[test]
    fn table_alias_hides_base_name_and_qualified_alias_resolves() {
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut good = parse_select("SELECT u.x FROM t AS u");
            ctx.bind_select(&mut good).unwrap();
            assert_column_expr(select_expr(&good, 0), 0, 0);

            let err = bind_select_error(ctx, "SELECT t.x FROM t AS u").to_string();
            assert!(
                err.contains("no such column: t.x"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn correlated_subquery_group_by_does_not_capture_outer_column_without_inner_match() {
        with_bind_context(&["CREATE TABLE t1(a, b)", "CREATE TABLE t2(x, y)"], |ctx| {
            let err = bind_select_error(
                ctx,
                "SELECT a FROM t1 WHERE EXISTS (SELECT x FROM t2 GROUP BY a)",
            )
            .to_string();
            assert!(err.contains("no such column: a"), "unexpected error: {err}");
        });
    }

    #[test]
    fn correlated_subquery_group_by_prefers_inner_column_when_present() {
        with_bind_context(&["CREATE TABLE t1(a, b)", "CREATE TABLE t3(a, x)"], |ctx| {
            let mut select =
                parse_select("SELECT a FROM t1 WHERE EXISTS (SELECT x FROM t3 GROUP BY a)");
            let bound = ctx.bind_select(&mut select).unwrap();

            let sq_id = exists_subquery_id(&select);
            let subquery = &bound.subquery_bindings[&sq_id].select;
            // t1=0, subquery_id=1, t3=2
            assert_column_expr(group_by_expr(subquery, 0), 2, 0);
        });
    }

    #[test]
    fn duplicate_aliases_are_allowed_in_order_by() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select = parse_select("SELECT x AS a, y AS a FROM t ORDER BY a");
            ctx.bind_select(&mut select).unwrap();

            assert_column_expr(order_by_expr(&select, 0), 0, 0);
        });
    }

    #[test]
    fn sqlite_compat_where_and_order_by_precedence_cases() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut where_select = parse_select("SELECT -a AS b, a, t.b FROM t WHERE b > 15");
            ctx.bind_select(&mut where_select).unwrap();
            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = where_expr(&where_select)
            else {
                panic!("expected bound WHERE binary expression");
            };
            assert_column_expr(lhs, 0, 1);
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("15".into()))
            );

            let mut order_select = parse_select("SELECT -a AS b, a, t.b FROM t ORDER BY b");
            ctx.bind_select(&mut order_select).unwrap();
            assert_eq!(
                order_by_expr(&order_select, 0),
                &ast::Expr::Unary(
                    ast::UnaryOperator::Negative,
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(1usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                )
            );
        });
    }

    #[test]
    fn sqlite_compat_group_by_prefers_source_column_over_alias() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT -a AS b, COUNT(*) FROM t GROUP BY b ORDER BY 1");
            ctx.bind_select(&mut select).unwrap();

            assert_column_expr(group_by_expr(&select, 0), 0, 1);
        });
    }

    #[test]
    fn order_by_subquery_can_see_select_alias_and_prefer_source_column() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut alias_visible = parse_select("SELECT a, -a AS x FROM t ORDER BY (SELECT x)");
            let bound = ctx.bind_select(&mut alias_visible).unwrap();
            let sq_id = subquery_id_from_expr(order_by_expr(&alias_visible, 0));
            let order_subquery = &bound.subquery_bindings[&sq_id].select;
            assert_eq!(
                select_expr(order_subquery, 0),
                &ast::Expr::Unary(
                    ast::UnaryOperator::Negative,
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                )
            );

            let mut source_preferred =
                parse_select("SELECT -a AS b, a, t.b FROM t ORDER BY (SELECT b)");
            let bound = ctx.bind_select(&mut source_preferred).unwrap();
            let sq_id = subquery_id_from_expr(order_by_expr(&source_preferred, 0));
            let order_subquery = &bound.subquery_bindings[&sq_id].select;
            assert_eq!(
                select_expr(order_subquery, 0),
                select_expr(&source_preferred, 2)
            );
        });
    }

    #[test]
    fn subqueries_in_having_and_where_can_see_outer_aliases() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut having_select = parse_select(
                "SELECT a % 2 AS g, SUM(b) AS s FROM t GROUP BY g HAVING (SELECT s) > 15 ORDER BY g",
            );
            let bound = ctx.bind_select(&mut having_select).unwrap();
            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(&having_select)
            else {
                panic!("expected bound HAVING binary expression");
            };
            let sq_id = subquery_id_from_expr(lhs);
            let having_subquery = &bound.subquery_bindings[&sq_id].select;
            assert_eq!(
                select_expr(having_subquery, 0),
                select_expr(&having_select, 1)
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("15".into()))
            );

            let mut nested_where = parse_select(
                "SELECT -a AS x, a \
                 FROM t \
                 WHERE EXISTS (SELECT 1 WHERE EXISTS (SELECT x WHERE x < 0))",
            );
            let bound = ctx.bind_select(&mut nested_where).unwrap();
            let outer_id = exists_subquery_id(&nested_where);
            let first_exists = &bound.subquery_bindings[&outer_id].select;
            let inner_id = exists_subquery_id(first_exists);
            let second_exists = &bound.subquery_bindings[&outer_id]
                .inner_bound
                .subquery_bindings[&inner_id]
                .select;
            assert_eq!(select_expr(second_exists, 0), select_expr(&nested_where, 0));
        });
    }

    fn window_clause(select: &ast::Select) -> &[ast::WindowDef] {
        match &select.body.select {
            ast::OneSelect::Select { window_clause, .. } => window_clause,
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    #[test]
    fn window_partition_by_and_order_by_are_bound() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select =
                parse_select("SELECT a FROM t WINDOW w AS (PARTITION BY b ORDER BY c)");
            ctx.bind_select(&mut select).unwrap();

            let defs = window_clause(&select);
            assert_eq!(defs.len(), 1);
            assert_column_expr(&defs[0].window.partition_by[0], 0, 1);
            assert_column_expr(&defs[0].window.order_by[0].expr, 0, 2);
        });
    }

    #[test]
    fn window_binds_qualified_column_refs() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select =
                parse_select("SELECT x FROM t WINDOW w AS (PARTITION BY t.y ORDER BY t.x)");
            ctx.bind_select(&mut select).unwrap();

            let defs = window_clause(&select);
            assert_column_expr(&defs[0].window.partition_by[0], 0, 1);
            assert_column_expr(&defs[0].window.order_by[0].expr, 0, 0);
        });
    }

    #[test]
    fn window_does_not_resolve_aliases() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT a AS z FROM t WINDOW w AS (PARTITION BY z)")
                .to_string();
            assert!(err.contains("no such column: z"), "unexpected error: {err}");
        });
    }

    #[test]
    fn order_by_column_number_replaces_with_result_expr() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a, b FROM t ORDER BY 2");
            ctx.bind_select(&mut select).unwrap();

            // ORDER BY 2 should resolve to column b (index 1)
            assert_column_expr(order_by_expr(&select, 0), 0, 1);
        });
    }

    #[test]
    fn group_by_column_number_replaces_with_result_expr() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a, b FROM t GROUP BY 1");
            ctx.bind_select(&mut select).unwrap();

            // GROUP BY 1 should resolve to column a (index 0)
            assert_column_expr(group_by_expr(&select, 0), 0, 0);
        });
    }

    #[test]
    fn column_number_zero_is_invalid() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT a FROM t ORDER BY 0").to_string();
            assert!(
                err.contains("invalid column index: 0"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn column_number_out_of_range_is_invalid() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT a FROM t ORDER BY 5").to_string();
            assert!(
                err.contains("invalid column index: 5"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn float_literal_in_order_by_is_not_treated_as_column_number() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t ORDER BY 1.5");
            ctx.bind_select(&mut select).unwrap();

            // 1.5 should remain as a numeric literal, not replaced
            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Literal(ast::Literal::Numeric("1.5".into()))
            );
        });
    }

    #[test]
    fn order_by_column_number_with_complex_result_expr() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a + 1, b FROM t ORDER BY 1");
            ctx.bind_select(&mut select).unwrap();

            // ORDER BY 1 should expand to the expression `a + 1` (already bound)
            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
        });
    }

    #[test]
    fn limit_clause_is_bound() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LIMIT 10");
            ctx.bind_select(&mut select).unwrap();

            let limit = select.limit.as_ref().expect("expected LIMIT clause");
            assert_eq!(
                limit.expr.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("10".into()))
            );
        });
    }

    #[test]
    fn limit_with_offset_is_bound() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LIMIT 10 OFFSET 5");
            ctx.bind_select(&mut select).unwrap();

            let limit = select.limit.as_ref().expect("expected LIMIT clause");
            assert_eq!(
                limit.expr.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("10".into()))
            );
            let offset = limit.offset.as_ref().expect("expected OFFSET");
            assert_eq!(
                offset.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("5".into()))
            );
        });
    }

    #[test]
    fn limit_double_quoted_string_becomes_literal() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LIMIT \"1\"");
            ctx.bind_select(&mut select).unwrap();

            let limit = select.limit.as_ref().expect("expected LIMIT clause");
            assert_eq!(
                limit.expr.as_ref(),
                &ast::Expr::Literal(ast::Literal::String("'1'".into()))
            );
        });
    }

    #[test]
    #[cfg(feature = "json")]
    fn function_call_star_expands_to_column_pairs() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select = parse_select("SELECT json_object(*) FROM t");
            ctx.bind_select(&mut select).unwrap();

            match select_expr(&select, 0) {
                ast::Expr::FunctionCall { name, args, .. } => {
                    assert_eq!(name.as_str(), "json_object");
                    // 2 columns × 2 (name + ref) = 4 args
                    assert_eq!(args.len(), 4);
                    assert_eq!(
                        args[0].as_ref(),
                        &ast::Expr::Literal(ast::Literal::String("'x'".into()))
                    );
                    assert_column_expr(&args[1], 0, 0);
                    assert_eq!(
                        args[2].as_ref(),
                        &ast::Expr::Literal(ast::Literal::String("'y'".into()))
                    );
                    assert_column_expr(&args[3], 0, 1);
                }
                other => panic!("expected FunctionCall, got {other:?}"),
            }
        });
    }

    #[test]
    fn function_call_star_without_expansion_stays_unchanged() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT count(*) FROM t");
            ctx.bind_select(&mut select).unwrap();

            // count(*) should remain as FunctionCallStar (not expanded)
            assert!(matches!(
                select_expr(&select, 0),
                ast::Expr::FunctionCallStar { .. }
            ));
        });
    }

    fn values_exprs(select: &ast::Select) -> &[Vec<Box<ast::Expr>>] {
        match &select.body.select {
            ast::OneSelect::Values(rows) => rows,
            other => panic!("expected VALUES, got {other:?}"),
        }
    }

    #[test]
    fn values_double_quoted_identifier_becomes_string_literal() {
        with_bind_context(&[], |ctx| {
            let mut select = parse_select("VALUES (\"hello\")");
            ctx.bind_select(&mut select).unwrap();

            let rows = values_exprs(&select);
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][0].as_ref(),
                &ast::Expr::Literal(ast::Literal::String("'hello'".into()))
            );
        });
    }

    #[test]
    fn values_numeric_literals_are_untouched() {
        with_bind_context(&[], |ctx| {
            let mut select = parse_select("VALUES (1, 2, 3)");
            ctx.bind_select(&mut select).unwrap();

            let rows = values_exprs(&select);
            assert_eq!(rows[0].len(), 3);
            assert_eq!(
                rows[0][0].as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("1".into()))
            );
        });
    }

    #[test]
    fn values_unquoted_identifier_errors() {
        with_bind_context(&[], |ctx| {
            let err = bind_select_error(ctx, "VALUES (x)").to_string();
            assert!(err.contains("no such column: x"), "unexpected error: {err}");
        });
    }

    #[test]
    fn multiple_window_defs_are_all_bound() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select = parse_select(
                "SELECT a FROM t WINDOW w1 AS (PARTITION BY a), w2 AS (ORDER BY b, c)",
            );
            ctx.bind_select(&mut select).unwrap();

            let defs = window_clause(&select);
            assert_eq!(defs.len(), 2);
            assert_column_expr(&defs[0].window.partition_by[0], 0, 0);
            assert_eq!(defs[1].window.order_by.len(), 2);
            assert_column_expr(&defs[1].window.order_by[0].expr, 0, 1);
            assert_column_expr(&defs[1].window.order_by[1].expr, 0, 2);
        });
    }

    // ── expand_stars tests ──────────────────────────────────────────────

    fn select_columns(select: &ast::Select) -> &[ast::ResultColumn] {
        match &select.body.select {
            ast::OneSelect::Select { columns, .. } => columns,
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    #[test]
    fn expand_star_single_table() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t");
            ctx.bind_select(&mut select).unwrap();

            let cols = select_columns(&select);
            assert_eq!(cols.len(), 3);
            assert_column_expr(select_expr(&select, 0), 0, 0);
            assert_column_expr(select_expr(&select, 1), 0, 1);
            assert_column_expr(select_expr(&select, 2), 0, 2);
        });
    }

    #[test]
    fn expand_star_multiple_tables() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(x, y)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t, u");
            ctx.bind_select(&mut select).unwrap();

            let cols = select_columns(&select);
            assert_eq!(cols.len(), 4);
            // t.a, t.b, u.x, u.y
            assert_column_expr(select_expr(&select, 0), 0, 0);
            assert_column_expr(select_expr(&select, 1), 0, 1);
            assert_column_expr(select_expr(&select, 2), 1, 0);
            assert_column_expr(select_expr(&select, 3), 1, 1);
        });
    }

    #[test]
    fn expand_table_star() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(x, y)"], |ctx| {
            let mut select = parse_select("SELECT u.* FROM t, u");
            ctx.bind_select(&mut select).unwrap();

            let cols = select_columns(&select);
            assert_eq!(cols.len(), 2);
            // u.x, u.y
            assert_column_expr(select_expr(&select, 0), 1, 0);
            assert_column_expr(select_expr(&select, 1), 1, 1);
        });
    }

    #[test]
    fn expand_star_with_join_using_dedup() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t JOIN u USING(b)");
            ctx.bind_select(&mut select).unwrap();

            let cols = select_columns(&select);
            // t.a, t.b, u.c — u.b is deduped by USING
            assert_eq!(cols.len(), 3);
            assert_column_expr(select_expr(&select, 0), 0, 0);
            assert_column_expr(select_expr(&select, 1), 0, 1);
            assert_column_expr(select_expr(&select, 2), 1, 1);
        });
    }

    #[test]
    fn expand_star_mixed_with_explicit_columns() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT 1, *, a FROM t");
            ctx.bind_select(&mut select).unwrap();

            let cols = select_columns(&select);
            // literal 1, t.a, t.b, t.a
            assert_eq!(cols.len(), 4);
            assert_eq!(
                select_expr(&select, 0),
                &ast::Expr::Literal(ast::Literal::Numeric("1".into()))
            );
            assert_column_expr(select_expr(&select, 1), 0, 0);
            assert_column_expr(select_expr(&select, 2), 0, 1);
            assert_column_expr(select_expr(&select, 3), 0, 0);
        });
    }

    #[test]
    fn expand_star_no_tables_errors() {
        with_bind_context(&[], |ctx| {
            let mut select = parse_select("SELECT *");
            let cols = select_columns(&select);
            // No FROM → no tables in scope → star expands to nothing
            assert_eq!(cols.len(), 1); // still Star before binding
                                       // Binding should succeed but star expands to zero columns
                                       // Actually the parser requires FROM for star, let's just test
                                       // the expand_stars produces empty
            let scope = BindScope::empty();
            let mut columns = vec![ast::ResultColumn::Star];
            ctx.expand_stars(&mut columns, &scope);
            assert_eq!(columns.len(), 0);
        });
    }

    // ── build_join_order tests ──────────────────────────────────────────

    #[test]
    fn build_join_order_single_table() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_join_order.len(), 1);
            assert_eq!(
                bound.main_join_order[0].table_id,
                TableInternalId::from(0usize)
            );
            assert_eq!(bound.main_join_order[0].original_idx, 0);
            assert!(!bound.main_join_order[0].is_outer);
        });
    }

    #[test]
    fn build_join_order_multiple_tables() {
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b)"], |ctx| {
            let mut select = parse_select("SELECT a, b FROM t, u");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_join_order.len(), 2);
            assert_eq!(
                bound.main_join_order[0].table_id,
                TableInternalId::from(0usize)
            );
            assert_eq!(bound.main_join_order[0].original_idx, 0);
            assert_eq!(
                bound.main_join_order[1].table_id,
                TableInternalId::from(1usize)
            );
            assert_eq!(bound.main_join_order[1].original_idx, 1);
        });
    }

    #[test]
    fn build_join_order_left_join_marks_outer() {
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LEFT JOIN u ON t.a = u.b");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_join_order.len(), 2);
            assert!(!bound.main_join_order[0].is_outer);
            assert!(bound.main_join_order[1].is_outer);
        });
    }

    #[test]
    fn build_join_order_values_is_empty() {
        with_bind_context(&[], |ctx| {
            let mut select = parse_select("VALUES (1, 2), (3, 4)");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert!(bound.main_join_order.is_empty());
        });
    }

    // ── NATURAL JOIN tests ──────────────────────────────────────────────

    fn join_constraint(select: &ast::Select) -> &Option<ast::JoinConstraint> {
        match &select.body.select {
            ast::OneSelect::Select { from, .. } => {
                let from = from.as_ref().expect("expected FROM clause");
                &from.joins[0].constraint
            }
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    #[test]
    fn natural_join_rewrites_to_using_with_common_columns() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
            ctx.bind_select(&mut select).unwrap();

            match join_constraint(&select) {
                Some(JoinConstraint::Using(cols)) => {
                    assert_eq!(cols.len(), 1);
                    assert_eq!(cols[0].as_str(), "b");
                }
                other => panic!("expected USING constraint, got {other:?}"),
            }
        });
    }

    #[test]
    fn natural_join_multiple_common_columns() {
        with_bind_context(
            &["CREATE TABLE t(a, b, c)", "CREATE TABLE u(b, c, d)"],
            |ctx| {
                let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
                ctx.bind_select(&mut select).unwrap();

                match join_constraint(&select) {
                    Some(JoinConstraint::Using(cols)) => {
                        assert_eq!(cols.len(), 2);
                        let names: Vec<&str> = cols.iter().map(|c| c.as_str()).collect();
                        assert!(names.contains(&"b"));
                        assert!(names.contains(&"c"));
                    }
                    other => panic!("expected USING constraint, got {other:?}"),
                }
            },
        );
    }

    #[test]
    fn natural_join_no_common_columns_errors() {
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT * FROM t NATURAL JOIN u").to_string();
            assert!(
                err.contains("No columns found to NATURAL join on"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn natural_join_with_on_clause_errors() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let err =
                bind_select_error(ctx, "SELECT * FROM t NATURAL JOIN u ON t.b = u.b").to_string();
            assert!(
                err.contains("NATURAL JOIN cannot be combined with ON or USING clause"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn natural_join_star_deduplicates_common_columns() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
            ctx.bind_select(&mut select).unwrap();

            let cols = select_columns(&select);
            // t.a, t.b, u.c — u.b is deduped by USING(b)
            assert_eq!(cols.len(), 3);
            assert_column_expr(select_expr(&select, 0), 0, 0); // t.a
            assert_column_expr(select_expr(&select, 1), 0, 1); // t.b
            assert_column_expr(select_expr(&select, 2), 1, 1); // u.c
        });
    }

    #[test]
    fn natural_join_tracks_using_columns_as_used() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t NATURAL JOIN u");
            let bound = ctx.bind_select(&mut select).unwrap();

            // Column b should be tracked as used in both tables (for USING equality)
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 0))); // t.a from SELECT
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 1))); // t.b from USING
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(1usize), 0))); // u.b from USING
        });
    }
}
