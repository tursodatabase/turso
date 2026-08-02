//! Display context for the remaining parser-expression emitters.

use turso_parser::ast::{fmt::ToSqlContext, TableInternalId};

use super::plan::TableReferences;

/// Resolve the internal column identities used by non-query DDL and the
/// incremental compiler when their expressions need to be rendered.
pub struct PlanContext<'a>(pub &'a [&'a TableReferences]);

impl ToSqlContext for PlanContext<'_> {
    fn get_column_name(&self, table_id: TableInternalId, column: usize) -> Option<Option<&str>> {
        let (_, table) = self
            .0
            .iter()
            .find_map(|tables| tables.find_table_by_internal_id(table_id))?;
        table
            .columns()
            .get(column)
            .map(|column| column.name.as_deref())
    }

    fn get_table_name(&self, table_id: TableInternalId) -> Option<&str> {
        let tables = self
            .0
            .iter()
            .find(|tables| tables.find_table_by_internal_id(table_id).is_some())?;
        tables
            .find_joined_table_by_internal_id(table_id)
            .map(|table| table.identifier.as_str())
            .or_else(|| {
                tables
                    .find_outer_query_ref_by_internal_id(table_id)
                    .map(|table| table.identifier.as_str())
            })
    }
}
