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
    /// SELECT t.a, u.b FROM t JOIN u ON t.a = u.b
    Join,
    /// SELECT t.a AS c0, ... FROM t UNION ALL SELECT u.b AS c0, ... FROM u
    UnionAll,
    /// Two joins of t and u on different columns of u, combined with UNION ALL
    UnionAllJoin,
    /// SELECT t.c AS c_l, u.c AS c_r FROM t JOIN u ON t.k = u.k WHERE <complex predicate>,
    /// where `c` is a column name of both t and u
    ComplexFilterJoin,
    /// SELECT a.k, a.c, b.c FROM t a JOIN t b ON a.r = b.k WHERE <complex predicate>
    ComplexFilterSelfJoin,
}

/// Tables and materialized views that a materialized view can read.
///
/// Turso refuses materialized views over `temp` or attached tables. Columns named
/// by an expression or a keyword (from `CREATE TABLE ... AS SELECT expr`) are
/// left out because this module writes column names without quotes.
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
        && !is_keyword(name)
}

fn is_keyword(name: &str) -> bool {
    SQLITE_KEYWORDS
        .split_whitespace()
        .any(|keyword| keyword.eq_ignore_ascii_case(name))
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

    let mut shapes = vec![
        (1, Shape::Star),
        (2, Shape::FilteredColumns),
        (1, Shape::Aggregate),
        (1, Shape::ComplexFilterSelfJoin),
    ];
    if sources.len() >= 2 {
        shapes.extend([
            (1, Shape::Join),
            (1, Shape::UnionAll),
            (1, Shape::UnionAllJoin),
            (1, Shape::ComplexFilterJoin),
        ]);
    }
    let shape = proptest::strategy::Union::new_weighted(
        shapes
            .into_iter()
            .map(|(weight, shape)| (weight, Just(shape)))
            .collect(),
    );

    (
        any::<bool>(),
        identifier_excluding(existing_names),
        0..sources.len(),
        shape,
    )
        .prop_flat_map(move |(if_not_exists, view_name, index, shape)| {
            let others: Vec<TableRef> = sources
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != index)
                .map(|(_, t)| t.clone())
                .collect();
            select_for_shape(sources[index].clone(), others, shape).prop_map(
                move |(select_sql, output_columns)| CreateMaterializedViewStatement {
                    if_not_exists,
                    view_name: view_name.clone(),
                    select_sql,
                    output_columns,
                },
            )
        })
        .boxed()
}

fn select_for_shape(
    source: TableRef,
    others: Vec<TableRef>,
    shape: Shape,
) -> BoxedStrategy<(String, Vec<ColumnDef>)> {
    let wide_others: Vec<TableRef> = others
        .iter()
        .filter(|t| t.columns.len() >= 2)
        .cloned()
        .collect();
    let same_name_others: Vec<TableRef> = others
        .iter()
        .filter(|t| same_name_columns(&source, t).is_some())
        .cloned()
        .collect();
    let name = source.name.clone();
    let filterable: Vec<ColumnDef> = source.filterable_columns().cloned().collect();
    let integer_columns = source
        .columns
        .iter()
        .filter(|c| c.data_type == DataType::Integer)
        .count();
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
        Shape::ComplexFilterSelfJoin if integer_columns >= 2 => (0..COMPLEX_PREDICATE_KINDS)
            .prop_map(move |kind| complex_filter_self_join(&source, kind))
            .boxed(),
        Shape::Star | Shape::FilteredColumns | Shape::ComplexFilterSelfJoin => Just((
            format!("SELECT * FROM {name}"),
            view_columns(&source.columns),
        ))
        .boxed(),
        Shape::ComplexFilterJoin if !same_name_others.is_empty() => (
            proptest::sample::select(same_name_others),
            0..COMPLEX_PREDICATE_KINDS,
        )
            .prop_map(move |(other, kind)| complex_filter_join(&source, &other, kind))
            .boxed(),
        Shape::UnionAllJoin if !wide_others.is_empty() => proptest::sample::select(wide_others)
            .prop_map(move |other| union_all_join(&source, &other))
            .boxed(),
        Shape::Join | Shape::UnionAllJoin | Shape::ComplexFilterJoin => {
            proptest::sample::select(others)
                .prop_map(move |other| join(&source, &other))
                .boxed()
        }
        Shape::UnionAll => proptest::sample::select(others)
            .prop_map(move |other| union_all(&source, &other))
            .boxed(),
    }
}

fn join(left: &Table, right: &Table) -> (String, Vec<ColumnDef>) {
    let (l, r) = (&left.columns[0], &right.columns[0]);
    let (ln, rn) = (&left.name, &right.name);
    // Both result columns need distinct names, so that later views can read them.
    let (right_projection, right_output) = if l.name == r.name {
        let alias = format!("{rn}_{}", r.name);
        (format!("{rn}.{} AS {alias}", r.name), alias)
    } else {
        (format!("{rn}.{}", r.name), r.name.clone())
    };
    (
        format!(
            "SELECT {ln}.{l}, {right_projection} FROM {ln} JOIN {rn} ON {ln}.{l} = {rn}.{r}",
            l = l.name,
            r = r.name
        ),
        vec![
            ColumnDef::new(l.name.clone(), l.data_type),
            ColumnDef::new(right_output, r.data_type),
        ],
    )
}

fn union_all(left: &Table, right: &Table) -> (String, Vec<ColumnDef>) {
    let n = left.columns.len().min(right.columns.len()).min(3);
    let projection = |table: &Table| {
        table.columns[..n]
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{}.{} AS c{i}", table.name, c.name))
            .collect::<Vec<_>>()
            .join(", ")
    };
    (
        format!(
            "SELECT {} FROM {} UNION ALL SELECT {} FROM {}",
            projection(left),
            left.name,
            projection(right),
            right.name
        ),
        left.columns[..n]
            .iter()
            .enumerate()
            .map(|(i, c)| ColumnDef::new(format!("c{i}"), c.data_type))
            .collect(),
    )
}

fn union_all_join(left: &Table, right: &Table) -> (String, Vec<ColumnDef>) {
    let (ln, rn) = (&left.name, &right.name);
    let l0 = &left.columns[0].name;
    let (r0, r1) = (&right.columns[0].name, &right.columns[1].name);
    (
        format!(
            "SELECT {ln}.{l0} AS c0, {rn}.{r0} AS c1 FROM {ln} JOIN {rn} ON {ln}.{l0} = {rn}.{r0} \
             UNION ALL \
             SELECT {ln}.{l0} AS c0, {rn}.{r1} AS c1 FROM {ln} JOIN {rn} ON {ln}.{l0} = {rn}.{r1}"
        ),
        vec![
            ColumnDef::new("c0", left.columns[0].data_type),
            ColumnDef::new("c1", right.columns[0].data_type),
        ],
    )
}

fn complex_filter_join(left: &Table, right: &Table, kind: u32) -> (String, Vec<ColumnDef>) {
    let (ln, rn) = (&left.name, &right.name);
    let (l, r) = same_name_columns(left, right)
        .expect("the right table was chosen because it shares a column name with the left one");
    let (lk, rk) = (&join_key(left).name, &join_key(right).name);
    let c = &l.name;
    let predicate = complex_predicate(&format!("{ln}.{lk}"), kind);
    (
        format!(
            "SELECT {ln}.{c} AS {c}_l, {rn}.{c} AS {c}_r FROM {ln} JOIN {rn} \
             ON {ln}.{lk} = {rn}.{rk} WHERE {predicate}"
        ),
        vec![
            ColumnDef::new(format!("{c}_l"), l.data_type),
            ColumnDef::new(format!("{c}_r"), r.data_type),
        ],
    )
}

/// A column name of both tables that is neither table's join key, so that the
/// two projected columns hold different values.
fn same_name_columns<'a>(
    left: &'a Table,
    right: &'a Table,
) -> Option<(&'a ColumnDef, &'a ColumnDef)> {
    let keys = [&join_key(left).name, &join_key(right).name];
    left.columns
        .iter()
        .filter(|c| !keys.contains(&&c.name))
        .find_map(|l| {
            right
                .columns
                .iter()
                .find(|r| r.name == l.name)
                .map(|r| (l, r))
        })
}

/// The primary key if there is one, because a key that is never NULL pairs up
/// more rows in a join.
fn join_key(table: &Table) -> &ColumnDef {
    table
        .columns
        .iter()
        .find(|c| c.primary_key)
        .unwrap_or(&table.columns[0])
}

fn complex_filter_self_join(table: &Table, kind: u32) -> (String, Vec<ColumnDef>) {
    let t = &table.name;
    let mut integers = table
        .columns
        .iter()
        .filter(|c| c.data_type == DataType::Integer);
    let (k, r) = (
        &integers.next().unwrap().name,
        &integers.next().unwrap().name,
    );
    let projected = table
        .columns
        .iter()
        .find(|c| &c.name != k && &c.name != r)
        .unwrap_or(&table.columns[1]);
    let (c, data_type) = (&projected.name, projected.data_type);
    let predicate = complex_predicate(&format!("a.{k}"), kind);
    (
        format!(
            "SELECT a.{k} AS sjk, a.{c} AS sja, b.{c} AS sjb FROM {t} a JOIN {t} b \
             ON a.{r} = b.{k} WHERE {predicate}"
        ),
        vec![
            ColumnDef::new("sjk", DataType::Integer),
            ColumnDef::new("sja", data_type),
            ColumnDef::new("sjb", data_type),
        ],
    )
}

const COMPLEX_PREDICATE_KINDS: u32 = 4;

/// A predicate that is not a plain comparison of a column with a literal or
/// another column. Turso compiles such a filter over a join through an extra
/// projection, which must keep the table of every column.
fn complex_predicate(column: &str, kind: u32) -> String {
    match kind {
        0 => format!("{column} BETWEEN 0 AND 999999999"),
        1 => format!("{column} IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)"),
        2 => format!("CAST({column} AS TEXT) IS NOT NULL"),
        _ => format!("CAST({column} AS TEXT) NOT LIKE '%zzq%'"),
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
                    ColumnDef::new("manager_id", DataType::Integer),
                ],
            ))
            .add_table(Table::new(
                "calc",
                vec![ColumnDef::new("TYPEOF(x)", DataType::Text)],
            ))
            .add_table(Table::new(
                "nulls",
                vec![ColumnDef::new("NULL", DataType::Text)],
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
        assert!(
            sqls.iter()
                .any(|sql| sql.contains(" JOIN ") && !sql.contains("UNION ALL"))
        );
        assert!(
            sqls.iter()
                .any(|sql| sql.contains("UNION ALL") && !sql.contains(" JOIN "))
        );
        assert!(
            sqls.iter()
                .any(|sql| sql.contains("UNION ALL") && sql.contains(" JOIN "))
        );
        assert!(sqls.iter().any(|sql| {
            sql.contains("SELECT users.name AS name_l, mv_users.name AS name_r")
                || sql.contains("SELECT mv_users.name AS name_l, users.name AS name_r")
        }));
        assert!(sqls.iter().any(|sql| sql.contains(
            "SELECT a.id AS sjk, a.name AS sja, b.name AS sjb FROM users a JOIN users b ON a.manager_id = b.id"
        )));
    }
}
