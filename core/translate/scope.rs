use crate::schema::Table;
use crate::translate::plan::{ResultSetColumn, TableReferences};
use crate::translate::planner::parse_row_id;
use crate::util::normalize_ident;
use crate::Result;
use turso_parser::ast::{self, TableInternalId};

/// Canonical output of name resolution. The binder turns this into concrete AST nodes.
#[derive(Debug, Clone)]
pub enum ResolvedColumn {
    Column {
        database: Option<usize>,
        table: TableInternalId,
        column: usize,
        is_rowid_alias: bool,
    },
    RowId {
        database: Option<usize>,
        table: TableInternalId,
    },
    AliasExpansion {
        expr: Box<ast::Expr>,
    },
}

/// Tri-state lookup result used by composable scopes.
///
/// We keep ambiguity as data so callers can decide whether to short-circuit
/// or continue fallback resolution.
#[derive(Debug, Clone)]
pub enum LookupResult {
    Found(ResolvedColumn),
    NotFound,
    Ambiguous(String),
}

/// A clause-local name-resolution context.
///
/// Different SQL clauses compose different scope chains to reproduce SQLite
/// name-resolution precedence without global behavior flags.
pub trait Scope {
    fn resolve_column(&mut self, name: &str) -> Result<LookupResult>;
    fn resolve_qualified(&mut self, table: &str, col: &str) -> Result<LookupResult>;
    fn table_references_mut(&mut self) -> Option<&mut TableReferences> {
        None
    }
}

fn resolve_column_in_joined_tables(refs: &TableReferences, name: &str) -> Result<LookupResult> {
    let normalized_name = normalize_ident(name);
    let mut match_result = None;

    for joined_table in refs.joined_tables() {
        let database = Some(joined_table.database_id);

        let col_idx = joined_table.table.columns().iter().position(|c| {
            c.name
                .as_ref()
                .is_some_and(|col_name| col_name.eq_ignore_ascii_case(&normalized_name))
        });

        if let Some(col_idx) = col_idx {
            if match_result.is_some() {
                let mut ok = false;
                // Column name ambiguity is ok if it is in the USING clause because then it
                // is deduplicated and the left table is used.
                if let Some(join_info) = &joined_table.join_info {
                    if join_info
                        .using
                        .iter()
                        .any(|using_col| using_col.as_str().eq_ignore_ascii_case(&normalized_name))
                    {
                        ok = true;
                    }
                }
                if !ok {
                    return Ok(LookupResult::Ambiguous(format!(
                        "Column {name} is ambiguous"
                    )));
                }
            } else {
                let col = &joined_table.table.columns()[col_idx];

                match_result = Some(ResolvedColumn::Column {
                    database,
                    table: joined_table.internal_id,
                    column: col_idx,
                    is_rowid_alias: col.is_rowid_alias(),
                });
            }
        // only if we haven't found a match, check for explicit rowid reference
        } else if let Table::BTree(btree) = &joined_table.table {
            if parse_row_id(
                &normalized_name,
                refs.joined_tables()[0].internal_id,
                || refs.joined_tables().len() != 1,
            )?
            .is_some()
            {
                if !btree.has_rowid {
                    crate::bail_parse_error!("no such column: {}", name);
                }
                return Ok(LookupResult::Found(ResolvedColumn::RowId {
                    database,
                    table: refs.joined_tables()[0].internal_id,
                }));
            }
        }
    }

    Ok(match match_result {
        Some(found) => LookupResult::Found(found),
        None => LookupResult::NotFound,
    })
}

fn resolve_column_in_outer_refs(refs: &TableReferences, name: &str) -> LookupResult {
    let normalized_name = normalize_ident(name);
    let mut match_result = None;

    for outer_ref in refs.outer_query_refs() {
        // CTEs (FromClauseSubquery) in outer_query_refs are only for table
        // lookup (e.g., FROM cte1), not for column resolution. Columns from
        // CTEs should only be accessible when the CTE is explicitly in the
        // FROM clause, not as implicit outer references.
        if matches!(outer_ref.table, Table::FromClauseSubquery(_)) {
            continue;
        }

        let col_idx = outer_ref.table.columns().iter().position(|c| {
            c.name
                .as_ref()
                .is_some_and(|col_name| col_name.eq_ignore_ascii_case(&normalized_name))
        });

        if let Some(col_idx) = col_idx {
            if match_result.is_some() {
                return LookupResult::Ambiguous(format!("Column {name} is ambiguous"));
            }

            let col = &outer_ref.table.columns()[col_idx];
            match_result = Some(ResolvedColumn::Column {
                database: None, // TODO: support different databases
                table: outer_ref.internal_id,
                column: col_idx,
                is_rowid_alias: col.is_rowid_alias(),
            });
        }
    }

    match match_result {
        Some(found) => LookupResult::Found(found),
        None => LookupResult::NotFound,
    }
}

fn resolve_qualified_in_joined_tables(
    refs: &TableReferences,
    table: &str,
    col: &str,
) -> Result<LookupResult> {
    let normalized_table = normalize_ident(table);
    let Some(joined_table) = refs
        .joined_tables()
        .iter()
        .find(|t| t.identifier == normalized_table)
    else {
        return Ok(LookupResult::NotFound);
    };

    let normalized_col = normalize_ident(col);
    let col_idx = joined_table.table.columns().iter().position(|c| {
        c.name
            .as_ref()
            .is_some_and(|col_name| col_name.eq_ignore_ascii_case(&normalized_col))
    });

    if let Some(col_idx) = col_idx {
        let col = &joined_table.table.columns()[col_idx];
        let database = Some(joined_table.database_id);
        return Ok(LookupResult::Found(ResolvedColumn::Column {
            database,
            table: joined_table.internal_id,
            column: col_idx,
            is_rowid_alias: col.is_rowid_alias(),
        }));
    }

    // User-defined columns take precedence over rowid aliases (oid, rowid,
    // _rowid_). Only fall back to parse_row_id() when no matching user
    // column exists.
    //
    // Note: Only BTree tables have rowid; derived tables
    // (FromClauseSubquery) don't have a rowid.
    if let Table::BTree(btree) = &joined_table.table {
        if parse_row_id(&normalized_col, joined_table.internal_id, || false)?.is_some() {
            if !btree.has_rowid {
                crate::bail_parse_error!("no such column: {}", normalized_col);
            }
            let database = Some(joined_table.database_id);
            return Ok(LookupResult::Found(ResolvedColumn::RowId {
                database,
                table: joined_table.internal_id,
            }));
        }
    }

    Ok(LookupResult::NotFound)
}

fn resolve_qualified_in_outer_refs(
    refs: &TableReferences,
    table: &str,
    col: &str,
) -> Result<LookupResult> {
    let normalized_table = normalize_ident(table);
    let Some(outer_ref) = refs
        .outer_query_refs()
        .iter()
        .find(|t| t.identifier == normalized_table && !t.cte_definition_only)
    else {
        return Ok(LookupResult::NotFound);
    };

    let normalized_col = normalize_ident(col);
    let col_idx = outer_ref.table.columns().iter().position(|c| {
        c.name
            .as_ref()
            .is_some_and(|col_name| col_name.eq_ignore_ascii_case(&normalized_col))
    });

    if let Some(col_idx) = col_idx {
        let col = &outer_ref.table.columns()[col_idx];
        return Ok(LookupResult::Found(ResolvedColumn::Column {
            database: None, // TODO: support different databases
            table: outer_ref.internal_id,
            column: col_idx,
            is_rowid_alias: col.is_rowid_alias(),
        }));
    }

    if let Table::BTree(btree) = &outer_ref.table {
        if parse_row_id(&normalized_col, outer_ref.internal_id, || false)?.is_some() {
            if !btree.has_rowid {
                crate::bail_parse_error!("no such column: {}", normalized_col);
            }
            return Ok(LookupResult::Found(ResolvedColumn::RowId {
                database: None, // TODO: support different databases
                table: outer_ref.internal_id,
            }));
        }
    }

    Ok(LookupResult::NotFound)
}

/// Default column scope: resolve against current FROM tables first, then outer refs.
///
/// This is used by most expression contexts where correlation is allowed.
pub struct FullTableScope<'a> {
    refs: &'a mut TableReferences,
}

impl<'a> FullTableScope<'a> {
    pub fn new(refs: &'a mut TableReferences) -> Self {
        Self { refs }
    }
}

impl Scope for FullTableScope<'_> {
    fn resolve_column(&mut self, name: &str) -> Result<LookupResult> {
        match resolve_column_in_joined_tables(self.refs, name)? {
            LookupResult::Found(found @ ResolvedColumn::Column { table, column, .. }) => {
                self.refs.mark_column_used(table, column);
                return Ok(LookupResult::Found(found));
            }
            LookupResult::Found(found) => return Ok(LookupResult::Found(found)),
            LookupResult::Ambiguous(msg) => return Ok(LookupResult::Ambiguous(msg)),
            LookupResult::NotFound => {}
        }

        match resolve_column_in_outer_refs(self.refs, name) {
            LookupResult::Found(found @ ResolvedColumn::Column { table, column, .. }) => {
                self.refs.mark_column_used(table, column);
                Ok(LookupResult::Found(found))
            }
            other => Ok(other),
        }
    }

    fn resolve_qualified(&mut self, table: &str, col: &str) -> Result<LookupResult> {
        let normalized_table = normalize_ident(table);
        let table_exists = self
            .refs
            .joined_tables()
            .iter()
            .any(|t| t.identifier == normalized_table)
            || self
                .refs
                .outer_query_refs()
                .iter()
                .any(|t| t.identifier == normalized_table && !t.cte_definition_only);
        if !table_exists {
            return Ok(LookupResult::Ambiguous(format!(
                "no such table: {normalized_table}",
            )));
        }

        match resolve_qualified_in_joined_tables(self.refs, table, col)? {
            LookupResult::Found(found @ ResolvedColumn::Column { table, column, .. }) => {
                self.refs.mark_column_used(table, column);
                return Ok(LookupResult::Found(found));
            }
            LookupResult::Found(found) => return Ok(LookupResult::Found(found)),
            LookupResult::Ambiguous(msg) => return Ok(LookupResult::Ambiguous(msg)),
            LookupResult::NotFound => {}
        }

        match resolve_qualified_in_outer_refs(self.refs, table, col)? {
            LookupResult::Found(found @ ResolvedColumn::Column { table, column, .. }) => {
                self.refs.mark_column_used(table, column);
                Ok(LookupResult::Found(found))
            }
            LookupResult::Found(found @ ResolvedColumn::RowId { table, .. }) => {
                self.refs.mark_rowid_referenced(table);
                Ok(LookupResult::Found(found))
            }
            other => Ok(other),
        }
    }

    fn table_references_mut(&mut self) -> Option<&mut TableReferences> {
        Some(self.refs)
    }
}

/// Isolated table scope: resolve only against current FROM tables.
///
/// This is used for clause contexts that must not capture outer references
/// (for example GROUP BY / HAVING / ORDER BY name resolution layers).
pub struct LocalTableScope<'a> {
    refs: &'a mut TableReferences,
}

impl<'a> LocalTableScope<'a> {
    pub fn new(refs: &'a mut TableReferences) -> Self {
        Self { refs }
    }
}

impl Scope for LocalTableScope<'_> {
    fn resolve_column(&mut self, name: &str) -> Result<LookupResult> {
        match resolve_column_in_joined_tables(self.refs, name)? {
            LookupResult::Found(found @ ResolvedColumn::Column { table, column, .. }) => {
                self.refs.mark_column_used(table, column);
                Ok(LookupResult::Found(found))
            }
            other => Ok(other),
        }
    }

    fn resolve_qualified(&mut self, table: &str, col: &str) -> Result<LookupResult> {
        let normalized_table = normalize_ident(table);
        let table_exists = self
            .refs
            .joined_tables()
            .iter()
            .any(|t| t.identifier == normalized_table);
        if !table_exists {
            return Ok(LookupResult::Ambiguous(format!(
                "no such table: {normalized_table}",
            )));
        }

        match resolve_qualified_in_joined_tables(self.refs, table, col)? {
            LookupResult::Found(found @ ResolvedColumn::Column { table, column, .. }) => {
                self.refs.mark_column_used(table, column);
                Ok(LookupResult::Found(found))
            }
            other => Ok(other),
        }
    }

    fn table_references_mut(&mut self) -> Option<&mut TableReferences> {
        Some(self.refs)
    }
}

/// Result-column alias scope (e.g. SELECT x AS y ... ORDER BY y).
///
/// This scope only handles unqualified names and rewrites them to the aliased
/// expression tree.
pub struct AliasScope<'a> {
    result_columns: &'a [ResultSetColumn],
}

impl<'a> AliasScope<'a> {
    pub fn new(result_columns: &'a [ResultSetColumn]) -> Self {
        Self { result_columns }
    }
}

impl Scope for AliasScope<'_> {
    fn resolve_column(&mut self, name: &str) -> Result<LookupResult> {
        let normalized_name = normalize_ident(name);
        for result_column in self.result_columns {
            if let Some(alias) = &result_column.alias {
                if alias.eq_ignore_ascii_case(&normalized_name) {
                    return Ok(LookupResult::Found(ResolvedColumn::AliasExpansion {
                        expr: Box::new(result_column.expr.clone()),
                    }));
                }
            }
        }

        Ok(LookupResult::NotFound)
    }

    fn resolve_qualified(&mut self, _table: &str, _col: &str) -> Result<LookupResult> {
        Ok(LookupResult::NotFound)
    }
}

/// Scope that resolves nothing.
///
/// Used for contexts like LIMIT/OFFSET or INSERT VALUES where column binding
/// should not succeed.
pub struct EmptyScope;

impl Scope for EmptyScope {
    fn resolve_column(&mut self, _name: &str) -> Result<LookupResult> {
        Ok(LookupResult::NotFound)
    }

    fn resolve_qualified(&mut self, _table: &str, _col: &str) -> Result<LookupResult> {
        Ok(LookupResult::NotFound)
    }
}

/// Generic fallback composition: try `inner`, then `outer` on NotFound.
///
/// Clause call sites build concrete chains to encode precedence explicitly
/// (for example Alias -> LocalTable, or Table -> Alias).
pub struct ChainedScope<I: Scope, O: Scope> {
    pub inner: I,
    pub outer: O,
}

impl<I: Scope, O: Scope> Scope for ChainedScope<I, O> {
    fn resolve_column(&mut self, name: &str) -> Result<LookupResult> {
        match self.inner.resolve_column(name)? {
            LookupResult::NotFound => self.outer.resolve_column(name),
            other => Ok(other),
        }
    }

    fn resolve_qualified(&mut self, table: &str, col: &str) -> Result<LookupResult> {
        match self.inner.resolve_qualified(table, col)? {
            LookupResult::NotFound => self.outer.resolve_qualified(table, col),
            other => Ok(other),
        }
    }

    fn table_references_mut(&mut self) -> Option<&mut TableReferences> {
        if let Some(table_references) = self.inner.table_references_mut() {
            return Some(table_references);
        }
        self.outer.table_references_mut()
    }
}
