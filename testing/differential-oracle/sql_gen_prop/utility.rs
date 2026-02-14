//! Utility statements.
//!
//! Includes VACUUM, ANALYZE, and REINDEX.

use proptest::prelude::*;
use std::fmt;

use crate::schema::Schema;

/// VACUUM statement.
#[derive(Debug, Clone)]
pub struct VacuumStatement;

impl fmt::Display for VacuumStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VACUUM")
    }
}

/// ANALYZE statement.
#[derive(Debug, Clone)]
pub struct AnalyzeStatement {
    /// Optional table or index name to analyze.
    pub target: Option<String>,
}

impl fmt::Display for AnalyzeStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ANALYZE")?;
        if let Some(target) = &self.target {
            write!(f, " {target}")?;
        }
        Ok(())
    }
}

/// REINDEX statement.
#[derive(Debug, Clone)]
pub struct ReindexStatement {
    /// Optional collation, table, or index name to reindex.
    pub target: Option<String>,
}

impl fmt::Display for ReindexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "REINDEX")?;
        if let Some(target) = &self.target {
            write!(f, " {target}")?;
        }
        Ok(())
    }
}

/// Generate a VACUUM statement.
pub fn vacuum() -> impl Strategy<Value = VacuumStatement> {
    Just(VacuumStatement)
}

/// Generate an ANALYZE statement.
pub fn analyze() -> impl Strategy<Value = AnalyzeStatement> {
    Just(AnalyzeStatement { target: None })
}

/// Generate an ANALYZE statement for a schema (targeting existing tables).
pub fn analyze_for_schema(schema: &Schema) -> BoxedStrategy<AnalyzeStatement> {
    if schema.tables.is_empty() {
        Just(AnalyzeStatement { target: None }).boxed()
    } else {
        let table_names: Vec<String> = schema.tables.iter().map(|t| t.name.clone()).collect();
        prop_oneof![
            Just(AnalyzeStatement { target: None }),
            proptest::sample::select(table_names)
                .prop_map(|name| AnalyzeStatement { target: Some(name) }),
        ]
        .boxed()
    }
}

/// Generate a REINDEX statement.
pub fn reindex() -> impl Strategy<Value = ReindexStatement> {
    Just(ReindexStatement { target: None })
}

/// Generate a REINDEX statement for a schema (targeting existing tables/indexes).
pub fn reindex_for_schema(schema: &Schema) -> BoxedStrategy<ReindexStatement> {
    let mut targets: Vec<String> = schema.tables.iter().map(|t| t.name.clone()).collect();
    targets.extend(schema.indexes.iter().map(|i| i.name.clone()));

    if targets.is_empty() {
        Just(ReindexStatement { target: None }).boxed()
    } else {
        prop_oneof![
            Just(ReindexStatement { target: None }),
            proptest::sample::select(targets)
                .prop_map(|name| ReindexStatement { target: Some(name) }),
        ]
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{prop_assert, prop_assert_eq};

    proptest::proptest! {
        #[test]
        fn vacuum_generates_valid_sql(stmt in vacuum()) {
            let sql = stmt.to_string();
            prop_assert_eq!(sql, "VACUUM");
        }

        #[test]
        fn analyze_generates_valid_sql(stmt in analyze()) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("ANALYZE"));
        }

        #[test]
        fn reindex_generates_valid_sql(stmt in reindex()) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("REINDEX"));
        }
    }
}
