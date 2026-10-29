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
    fn register_affinity(&self, register: usize) -> Option<Affinity>;
    fn register_collation(&self, register: usize) -> Option<TextCollation>;
    fn subquery_affinity(&self, query: SubqueryId) -> Option<Affinity>;
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

    fn subquery_affinity(&self, query: TableInternalId) -> Option<Affinity> {
        self.resolver
            .subquery_affinities
            .borrow()
            .get(&query)
            .copied()
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

    fn register_affinity(&self, _register: usize) -> Option<Affinity> {
        None
    }

    fn register_collation(&self, _register: usize) -> Option<TextCollation> {
        None
    }

    fn subquery_affinity(&self, _query: TableInternalId) -> Option<Affinity> {
        None
    }
}
