use turso_parser::ast::TableInternalId;

use crate::{vdbe::affinity::Affinity, LimboError, Result};

use super::{
    collate::{column_collation, CollationSeq},
    emitter::Resolver,
    plan::TableReferences,
};

pub(crate) trait ExprMetadata {
    type SourceId: Copy;
    type SubqueryId: Copy;

    fn column_affinity(&self, source: Self::SourceId, column: usize) -> Option<Affinity>;
    fn column_collation(
        &self,
        source: Self::SourceId,
        column: usize,
    ) -> Result<Option<CollationSeq>>;
    fn register_affinity(&self, register: usize) -> Option<Affinity>;
    fn register_collation(&self, register: usize) -> Option<CollationSeq>;
    fn subquery_affinity(&self, query: Self::SubqueryId) -> Option<Affinity>;
}

pub(crate) struct ExprContext<'a, 'schema> {
    tables: Option<&'a TableReferences>,
    resolver: Option<&'a Resolver<'schema>>,
}

impl<'a, 'schema> ExprContext<'a, 'schema> {
    pub(crate) fn new(
        tables: Option<&'a TableReferences>,
        resolver: Option<&'a Resolver<'schema>>,
    ) -> Self {
        Self { tables, resolver }
    }
}

impl ExprMetadata for ExprContext<'_, '_> {
    type SourceId = TableInternalId;
    type SubqueryId = TableInternalId;

    fn column_affinity(&self, source: Self::SourceId, column: usize) -> Option<Affinity> {
        if source.is_self_table() {
            if let Some(affinity) = self.resolver.and_then(|r| r.self_table_affinity(column)) {
                return Some(affinity);
            }
        }
        let (_, table) = self.tables?.find_table_by_internal_id(source)?;
        table.get_column_at(column).map(|column| column.affinity())
    }

    fn column_collation(
        &self,
        source: Self::SourceId,
        column: usize,
    ) -> Result<Option<CollationSeq>> {
        if source.is_self_table() {
            let table = self.resolver.and_then(|r| r.self_table()).ok_or_else(|| {
                LimboError::InternalError("no table for a self-table column".into())
            })?;
            let column = table
                .columns()
                .get(column)
                .ok_or_else(|| LimboError::ParseError("column not found".to_string()))?;
            return Ok(column_collation(column));
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

    fn register_affinity(&self, register: usize) -> Option<Affinity> {
        self.resolver?
            .register_columns
            .get(&register)
            .map(|column| column.affinity)
    }

    fn register_collation(&self, register: usize) -> Option<CollationSeq> {
        self.resolver?
            .register_columns
            .get(&register)
            .and_then(|column| column.collation)
    }

    fn subquery_affinity(&self, query: Self::SubqueryId) -> Option<Affinity> {
        self.resolver?
            .subquery_affinities
            .borrow()
            .get(&query)
            .copied()
    }
}
