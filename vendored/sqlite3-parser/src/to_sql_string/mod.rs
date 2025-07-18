//! ToSqlString trait definition and implementations

mod expr;
mod stmt;

use crate::ast::TableInternalId;

/// Context to be used in ToSqlString
pub trait ToSqlContext {
    /// Given an id, get the table name
    ///
    /// Currently not considering aliases
    fn get_table_name(&self, id: TableInternalId) -> &str;
    /// Given a table id and a column index, get the column name
    fn get_column_name(&self, table_id: TableInternalId, col_idx: usize) -> &str;
}

#[cfg(test)]
mod tests {
    use super::ToSqlContext;

    struct TestContext;

    impl ToSqlContext for TestContext {
        fn get_column_name(&self, _table_id: crate::ast::TableInternalId, _col_idx: usize) -> &str {
            "placeholder_column"
        }

        fn get_table_name(&self, _id: crate::ast::TableInternalId) -> &str {
            "placeholder_table"
        }
    }
}
