use std::sync::Arc;

use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, TableInternalId};

use super::emitter::Resolver;
use super::plan::{JoinInfo, JoinedTable, ResultSetColumn, TableReferences};
use crate::schema::Table;
use crate::util::normalize_ident;
use crate::Result;

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
/// Cheap to clone (`Table` is Arc-wrapped).
#[derive(Clone)]
pub struct ScopeTable {
    /// The name used to refer to this table in the query (original name or alias).
    pub identifier: String,
    /// Opaque ID used in `Expr::Column` to reference this table.
    pub internal_id: TableInternalId,
    /// Table metadata (columns, types, etc). Clone is an Arc bump.
    pub table: Table,
    /// Join constraint info (USING clause for dedup during unqualified lookup).
    pub join_info: Option<JoinInfo>,
}

// ── BindScope ────────────────────────────────────────────────────────────

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
    /// Snapshot the current query's joined tables into a scope.
    pub fn from_joined_tables(joined: &[JoinedTable], right_join_swapped: bool) -> Self {
        Self {
            tables: joined
                .iter()
                .map(|jt| ScopeTable {
                    identifier: jt.identifier.clone(),
                    internal_id: jt.internal_id,
                    table: jt.table.clone(),
                    join_info: jt.join_info.clone(),
                })
                .collect(),
            right_join_swapped,
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
            let col_idx = st.table.columns().iter().position(|c| {
                c.name
                    .as_ref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(&normalized))
            });

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
                    let col = &st.table.columns()[idx];
                    result = Some((st.internal_id, idx, col.is_rowid_alias()));
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
        let col_idx = st.table.columns().iter().position(|c| {
            c.name
                .as_ref()
                .is_some_and(|n| n.eq_ignore_ascii_case(&normalized_col))
        });

        let Some(idx) = col_idx else {
            crate::bail_parse_error!("no such column: {}", col_name);
        };

        let col = &st.table.columns()[idx];
        Ok(Some((st.internal_id, idx, col.is_rowid_alias())))
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
}

// ── BindContext ───────────────────────────────────────────────────────────

/// Scope-aware binding context, analogous to DataFusion's `PlannerContext`.
///
/// Manages the outer-scope stack for correlated subquery resolution,
/// CTE definitions, SELECT aliases, and binding phase tracking.
///
/// Does **not** borrow `TableReferences`. Column usage is recorded in
/// [`BindTracking`] and flushed back after binding completes.
pub struct BindContext<'a> {
    /// Function and schema resolver.
    pub resolver: &'a Resolver<'a>,

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

impl<'a> BindContext<'a> {
    fn new(resolver: &'a Resolver<'a>) -> Self {
        Self {
            resolver,
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

    fn bind_select(&mut self, select: &mut ast::Select) -> Result<()> {
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
            let explicit_columns = cte
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
                },
            );
        }

        // Pass 2: bind each CTE body
        for cte in &mut with.ctes {
            self.bind_select(&mut cte.select)?;
        }
        Ok(())
    }
}

// ── BoundSelect ──────────────────────────────────────────────────────────

/// Result of binding a SELECT statement.
pub struct BoundSelect {
    /// Bound result columns with aliases extracted.
    /// `contains_aggregates` is `false` — the planner fills it in.
    pub result_columns: Vec<ResultSetColumn>,
}
