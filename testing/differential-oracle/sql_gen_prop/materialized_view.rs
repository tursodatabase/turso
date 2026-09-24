//! Materialized view statements.
//!
//! Turso maintains a materialized view incrementally. The differential fuzzer
//! runs the same SELECT as a plain view on SQLite, so both engines must return
//! the same rows after every write.

use std::collections::HashSet;
use std::fmt;

use proptest::prelude::*;

use crate::create_table::identifier_excluding;
use crate::schema::{ColumnDef, DataType, Schema, Table, TableRef};
use crate::view::DropViewStatement;

/// CREATE MATERIALIZED VIEW statement.
#[derive(Debug, Clone)]
pub struct CreateMaterializedViewStatement {
    pub if_not_exists: bool,
    pub view_name: String,
    pub select_sql: String,
    /// Result columns of `select_sql`, so that later views can read this one.
    pub output_columns: Vec<ColumnDef>,
}

impl fmt::Display for CreateMaterializedViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE MATERIALIZED VIEW")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {} AS {}", self.view_name, self.select_sql)
    }
}

impl CreateMaterializedViewStatement {
    /// The same view as a plain `CREATE VIEW`, for engines without materialized views.
    pub fn plain_view_sql(&self) -> String {
        let if_not_exists = if self.if_not_exists {
            " IF NOT EXISTS"
        } else {
            ""
        };
        format!(
            "CREATE VIEW{if_not_exists} {} AS {}",
            self.view_name, self.select_sql
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum Shape {
    /// SELECT * FROM t
    Star,
    /// SELECT c1, c2, c3 FROM t WHERE <predicate>
    FilteredColumns,
    /// SELECT g, COUNT(*) FROM t GROUP BY g
    Aggregate,
}

/// Tables and materialized views that a materialized view can read.
///
/// Turso refuses materialized views over `temp` or attached tables. Columns named
/// by an expression (from `CREATE TABLE ... AS SELECT expr`) are left out
/// because this module writes column names without quotes.
pub fn materialized_view_sources(schema: &Schema) -> Vec<TableRef> {
    schema
        .tables
        .iter()
        .filter(|t| t.database.is_none())
        .chain(schema.materialized_views.iter())
        .filter(|t| {
            is_plain_identifier(&t.name) && t.columns.iter().all(|c| is_plain_identifier(&c.name))
        })
        .cloned()
        .collect()
}

fn is_plain_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// From https://sqlite.org/lang_keywords.html
const SQLITE_KEYWORDS: &str = "ABORT ACTION ADD AFTER ALL ALTER ALWAYS ANALYZE AND AS ASC ATTACH \
    AUTOINCREMENT BEFORE BEGIN BETWEEN BY CASCADE CASE CAST CHECK COLLATE COLUMN COMMIT CONFLICT \
    CONSTRAINT CREATE CROSS CURRENT CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP DATABASE DEFAULT \
    DEFERRABLE DEFERRED DELETE DESC DETACH DISTINCT DO DROP EACH ELSE END ESCAPE EXCEPT EXCLUDE \
    EXCLUSIVE EXISTS EXPLAIN FAIL FILTER FIRST FOLLOWING FOR FOREIGN FROM FULL GENERATED GLOB \
    GROUP GROUPS HAVING IF IGNORE IMMEDIATE IN INDEX INDEXED INITIALLY INNER INSERT INSTEAD \
    INTERSECT INTO IS ISNULL JOIN KEY LAST LEFT LIKE LIMIT MATCH MATERIALIZED NATURAL NO NOT \
    NOTHING NOTNULL NULL NULLS OF OFFSET ON OR ORDER OTHERS OUTER OVER PARTITION PLAN PRAGMA \
    PRECEDING PRIMARY QUERY RAISE RANGE RECURSIVE REFERENCES REGEXP REINDEX RELEASE RENAME \
    REPLACE RESTRICT RETURNING RIGHT ROLLBACK ROW ROWS SAVEPOINT SELECT SET TABLE TEMP TEMPORARY \
    THEN TIES TO TRANSACTION TRIGGER UNBOUNDED UNION UNIQUE UPDATE USING VACUUM VALUES VIEW \
    VIRTUAL WHEN WHERE WINDOW WITH WITHOUT";

/// Generate a CREATE MATERIALIZED VIEW statement over a table or another materialized view.
pub fn create_materialized_view(schema: &Schema) -> BoxedStrategy<CreateMaterializedViewStatement> {
    let sources = materialized_view_sources(schema);
    assert!(
        !sources.is_empty(),
        "Schema must have a table that a materialized view can read"
    );
    let existing_names: HashSet<String> = schema
        .table_names()
        .into_iter()
        .chain(schema.view_names())
        .chain(schema.materialized_view_names())
        .chain(schema.index_names())
        .chain(schema.trigger_names())
        // Generated names are lowercase.
        .chain(SQLITE_KEYWORDS.split_whitespace().map(str::to_lowercase))
        .collect();

    (
        any::<bool>(),
        identifier_excluding(existing_names),
        proptest::sample::select(sources),
        prop_oneof![
            1 => Just(Shape::Star),
            2 => Just(Shape::FilteredColumns),
            1 => Just(Shape::Aggregate),
        ],
    )
        .prop_flat_map(|(if_not_exists, view_name, source, shape)| {
            select_for_shape(&source, shape).prop_map(move |(select_sql, output_columns)| {
                CreateMaterializedViewStatement {
                    if_not_exists,
                    view_name: view_name.clone(),
                    select_sql,
                    output_columns,
                }
            })
        })
        .boxed()
}

fn select_for_shape(source: &Table, shape: Shape) -> BoxedStrategy<(String, Vec<ColumnDef>)> {
    let name = source.name.clone();
    let filterable: Vec<ColumnDef> = source.filterable_columns().cloned().collect();
    match shape {
        Shape::FilteredColumns if source.columns.len() >= 2 && !filterable.is_empty() => {
            let projected = &source.columns[..source.columns.len().min(3)];
            let projection = projected
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let output_columns = view_columns(projected);
            (0..PREDICATE_KINDS)
                .prop_map(move |kind| {
                    let predicate = predicate(&filterable, kind);
                    (
                        format!("SELECT {projection} FROM {name} WHERE {predicate}"),
                        output_columns.clone(),
                    )
                })
                .boxed()
        }
        Shape::Aggregate if !filterable.is_empty() => {
            let group = &filterable[0];
            // Turso refuses a materialized view whose count alias equals the GROUP BY column.
            let count = format!("{}_cnt", group.name);
            Just((
                format!(
                    "SELECT {g}, COUNT(*) AS {count} FROM {name} GROUP BY {g}",
                    g = group.name
                ),
                vec![
                    ColumnDef::new(group.name.clone(), group.data_type),
                    ColumnDef::new(count, DataType::Integer),
                ],
            ))
            .boxed()
        }
        Shape::Aggregate => Just((
            format!("SELECT COUNT(*) AS cnt FROM {name}"),
            vec![ColumnDef::new("cnt", DataType::Integer)],
        ))
        .boxed(),
        Shape::Star | Shape::FilteredColumns => Just((
            format!("SELECT * FROM {name}"),
            view_columns(&source.columns),
        ))
        .boxed(),
    }
}

/// A view column keeps the name and type of its source column but none of its constraints.
fn view_columns(columns: &[ColumnDef]) -> Vec<ColumnDef> {
    columns
        .iter()
        .map(|c| ColumnDef::new(c.name.clone(), c.data_type))
        .collect()
}

const PREDICATE_KINDS: u32 = 11;

/// A WHERE predicate over the filterable columns. Nullable columns are preferred
/// for comparisons, so that NULL makes the predicate unknown.
fn predicate(filterable: &[ColumnDef], kind: u32) -> String {
    let first = &filterable[0].name;
    let compared = filterable
        .iter()
        .find(|c| c.nullable)
        .unwrap_or(&filterable[0]);
    let cmp = &compared.name;
    let literal = match compared.data_type {
        DataType::Integer | DataType::Blob | DataType::Null => "0",
        DataType::Real => "0.0",
        DataType::Text => "''",
    };
    let second = filterable.iter().map(|c| &c.name).find(|n| *n != first);
    match (kind, second) {
        (1, _) => format!("{cmp} != {literal}"),
        (2, _) => format!("{cmp} < {literal}"),
        (3, Some(other)) => format!("{first} IS NOT NULL AND {other} IS NOT NULL"),
        (4, Some(other)) => format!("{first} IS NULL AND {other} IS NOT NULL"),
        (5, Some(other)) => format!("{first} IS NOT NULL AND {other} IS NULL"),
        (6, Some(other)) => format!("({first} IS NOT NULL) AND ({other} IS NOT NULL)"),
        (7, _) => format!("CAST({cmp} AS TEXT) NOT LIKE '%zzq%'"),
        (8, _) => format!("CAST({cmp} AS TEXT) LIKE '%'"),
        (9, _) => format!("{cmp} IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)"),
        (10, _) => format!("CAST({cmp} AS TEXT) IS NOT NULL"),
        _ => format!("{first} IS NOT NULL"),
    }
}

/// Generate a DROP VIEW statement for an existing materialized view.
pub fn drop_materialized_view(schema: &Schema) -> BoxedStrategy<DropViewStatement> {
    let names: Vec<String> = schema
        .materialized_views
        .iter()
        .map(|v| v.name.clone())
        .collect();
    assert!(
        !names.is_empty(),
        "Schema must have a materialized view to drop"
    );
    (any::<bool>(), proptest::sample::select(names))
        .prop_map(|(if_exists, view_name)| DropViewStatement {
            if_exists,
            view_name,
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaBuilder;

    fn schema_with_matview() -> Schema {
        SchemaBuilder::new()
            .add_table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                    ColumnDef::new("team", DataType::Text),
                ],
            ))
            .add_table(Table::new(
                "calc",
                vec![ColumnDef::new("TYPEOF(x)", DataType::Text)],
            ))
            .add_table(
                Table::new("far", vec![ColumnDef::new("id", DataType::Integer)]).in_database("aux"),
            )
            .add_materialized_view(Table::new(
                "mv_users",
                vec![
                    ColumnDef::new("id", DataType::Integer),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .build()
    }

    #[test]
    fn sources_are_main_tables_and_materialized_views_with_plain_column_names() {
        let names: Vec<String> = materialized_view_sources(&schema_with_matview())
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(names, vec!["users", "mv_users"]);
    }

    proptest! {
        #[test]
        fn plain_view_sql_differs_only_in_the_keyword(stmt in create_materialized_view(&schema_with_matview())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE MATERIALIZED VIEW"));
            prop_assert_eq!(sql.replacen("MATERIALIZED ", "", 1), stmt.plain_view_sql());
            prop_assert!(!stmt.output_columns.is_empty());
        }
    }

    #[test]
    fn every_shape_is_generated_and_materialized_views_are_read() {
        let schema = schema_with_matview();
        let strategy = create_materialized_view(&schema);
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let sqls: Vec<String> = (0..500)
            .map(|_| strategy.new_tree(&mut runner).unwrap().current().select_sql)
            .collect();
        assert!(sqls.iter().any(|sql| sql.starts_with("SELECT * FROM")));
        assert!(sqls.iter().any(|sql| sql.contains(" WHERE ")));
        assert!(sqls.iter().any(|sql| sql.contains(" GROUP BY ")));
        assert!(sqls.iter().any(|sql| sql.contains("FROM mv_users")));
    }
}
