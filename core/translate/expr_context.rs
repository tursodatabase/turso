use turso_parser::ast::{Expr, TableInternalId};

use crate::{schema::Schema, vdbe::affinity::Affinity, LimboError, Result};

use super::{
    catalog::CatalogContext,
    collate::{column_collation, expr_collation, CollationSeq, TextCollation},
    emitter::Resolver,
    expr::expr_type::TypeEnv,
    plan::TableReferences,
};

pub(crate) trait ExprProperties<
    SourceId: Copy = TableInternalId,
    SubqueryId: Copy = TableInternalId,
>
{
    fn column_affinity(&self, source: SourceId, column: usize) -> Option<Affinity>;
    fn column_collation(&self, source: SourceId, column: usize) -> Result<Option<TextCollation>>;
    /// Whether the column is never NULL in this query. An outer join can make
    /// a NOT NULL column NULL.
    fn column_is_nonnull(&self, source: SourceId, column: usize) -> bool;
    /// Whether the rowid is never NULL in this query. An outer join can make
    /// the rowid NULL.
    fn rowid_is_nonnull(&self, source: SourceId) -> bool;
    fn register_affinity(&self, register: usize) -> Option<Affinity>;
    fn register_collation(&self, register: usize) -> Option<TextCollation>;
    fn subquery_affinity(&self, query: SubqueryId, result_reg: usize) -> Option<Affinity>;
}

#[derive(Clone, Copy)]
pub(crate) struct ExprContext<'a, 'schema> {
    tables: TableMetadata<'a>,
    resolver: &'a Resolver<'schema>,
}

impl<'a, 'schema> ExprContext<'a, 'schema> {
    pub(crate) fn new(
        tables: Option<&'a TableReferences>,
        resolver: &'a Resolver<'schema>,
    ) -> Self {
        Self {
            tables: TableMetadata::new(tables),
            resolver,
        }
    }

    pub(crate) fn expr_collation(&self, expr: &Expr) -> Result<CollationSeq> {
        expr_collation(expr, self)
    }

    /// What `type_of` needs to know the type of an expression here, such as
    /// the custom type of a column.
    pub(crate) fn type_env(&self) -> TypeEnv<'_> {
        TypeEnv::new(self.tables.tables, self.resolver)
    }

    pub(crate) fn catalog(&self) -> &CatalogContext<'schema> {
        &self.resolver.catalog
    }

    pub(crate) fn schema(&self) -> &Schema {
        self.resolver.schema()
    }

    pub(crate) fn resolver(&self) -> &Resolver<'schema> {
        self.resolver
    }
}

impl ExprProperties for ExprContext<'_, '_> {
    fn column_affinity(&self, source: TableInternalId, column: usize) -> Option<Affinity> {
        if source.is_self_table() {
            if let Some(affinity) = self.resolver.self_table_affinity(column) {
                return Some(affinity);
            }
        }
        self.tables.column_affinity(source, column)
    }

    fn column_collation(
        &self,
        source: TableInternalId,
        column: usize,
    ) -> Result<Option<TextCollation>> {
        if source.is_self_table() {
            let table = self.resolver.self_table().ok_or_else(|| {
                LimboError::InternalError("no table for a self-table column".into())
            })?;
            let column = table
                .columns()
                .get(column)
                .ok_or_else(|| LimboError::ParseError("column not found".to_string()))?;
            return Ok(column_collation(column));
        }
        self.tables.column_collation(source, column)
    }

    fn column_is_nonnull(&self, source: TableInternalId, column: usize) -> bool {
        self.tables.column_is_nonnull(source, column)
    }

    fn rowid_is_nonnull(&self, source: TableInternalId) -> bool {
        self.tables.rowid_is_nonnull(source)
    }

    fn register_affinity(&self, register: usize) -> Option<Affinity> {
        self.resolver
            .register_columns
            .get(&register)
            .map(|column| column.affinity)
    }

    fn register_collation(&self, register: usize) -> Option<TextCollation> {
        self.resolver
            .register_columns
            .get(&register)
            .and_then(|column| column.collation)
    }

    fn subquery_affinity(&self, query: TableInternalId, result_reg: usize) -> Option<Affinity> {
        let subquery = self.resolver.row_value_subqueries.get(query)?;
        Some(subquery.column(result_reg)?.affinity)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TableMetadata<'a> {
    tables: Option<&'a TableReferences>,
}

impl<'a> TableMetadata<'a> {
    pub(crate) fn new(tables: Option<&'a TableReferences>) -> Self {
        Self { tables }
    }
}

impl ExprProperties for TableMetadata<'_> {
    fn column_affinity(&self, source: TableInternalId, column: usize) -> Option<Affinity> {
        let (_, table) = self.tables?.find_table_by_internal_id(source)?;
        table.get_column_at(column).map(|column| column.affinity())
    }

    fn column_collation(
        &self,
        source: TableInternalId,
        column: usize,
    ) -> Result<Option<TextCollation>> {
        if source.is_self_table() {
            return Err(LimboError::InternalError(
                "no table for a self-table column".into(),
            ));
        }
        if self
            .tables
            .is_some_and(|tables| tables.provided_by_index_method(source, column))
        {
            return Ok(None);
        }
        let (_, table) = self
            .tables
            .and_then(|tables| tables.find_table_by_internal_id(source))
            .ok_or_else(|| LimboError::ParseError("table not found".to_string()))?;
        let column = table
            .get_column_at(column)
            .ok_or_else(|| LimboError::ParseError("column not found".to_string()))?;
        Ok(column_collation(column))
    }

    fn column_is_nonnull(&self, source: TableInternalId, column: usize) -> bool {
        // A null-extended row has a NULL rowid and NULL in every column.
        if !self.rowid_is_nonnull(source) {
            return false;
        }
        let (_, table) = self
            .tables
            .and_then(|tables| tables.find_table_by_internal_id(source))
            .expect("table not found");
        let column = &table.columns()[column];
        // Only INTEGER PRIMARY KEY (rowid alias) is implicitly NOT NULL.
        // Other PRIMARY KEY types (e.g., TEXT PRIMARY KEY) can contain NULL.
        column.is_rowid_alias() || column.notnull()
    }

    fn rowid_is_nonnull(&self, source: TableInternalId) -> bool {
        // SQLite marks columns from the nullable side of an outer join as
        // nullable, even when the table schema says NOT NULL. A correlated
        // outer reference carries the nullability from its own scope.
        !self
            .tables
            .is_some_and(|tables| tables.outer_join_may_null_extend(source))
    }

    fn register_affinity(&self, _register: usize) -> Option<Affinity> {
        None
    }

    fn register_collation(&self, _register: usize) -> Option<TextCollation> {
        None
    }

    fn subquery_affinity(&self, query: TableInternalId, _result_reg: usize) -> Option<Affinity> {
        self.tables?.scalar_subquery_affinity(query)
    }
}
