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

/// How often each view shape is generated. A shape with weight 0 is never generated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewProfile {
    pub star_weight: u32,
    pub filtered_columns_weight: u32,
    pub aggregate_weight: u32,
    pub complex_filter_self_join_weight: u32,
    pub join_weight: u32,
    pub union_all_weight: u32,
    pub union_all_join_weight: u32,
    pub complex_filter_join_weight: u32,
    /// A view may read another materialized view.
    pub read_materialized_views: bool,
}

impl Default for MaterializedViewProfile {
    fn default() -> Self {
        Self {
            star_weight: 1,
            filtered_columns_weight: 2,
            aggregate_weight: 1,
            complex_filter_self_join_weight: 1,
            join_weight: 1,
            union_all_weight: 1,
            union_all_join_weight: 1,
            complex_filter_join_weight: 1,
            read_materialized_views: true,
        }
    }
}

impl MaterializedViewProfile {
    fn weight(&self, shape: Shape) -> u32 {
        match shape {
            Shape::Star => self.star_weight,
            Shape::FilteredColumns => self.filtered_columns_weight,
            Shape::Aggregate => self.aggregate_weight,
            Shape::ComplexFilterSelfJoin => self.complex_filter_self_join_weight,
            Shape::Join => self.join_weight,
            Shape::UnionAll => self.union_all_weight,
            Shape::UnionAllJoin => self.union_all_join_weight,
            Shape::ComplexFilterJoin => self.complex_filter_join_weight,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// SELECT * FROM t
    Star,
    /// SELECT c1, c2, c3 FROM t WHERE <predicate>
    FilteredColumns,
    /// SELECT g, COUNT(*) FROM t GROUP BY g
    Aggregate,
    /// SELECT a.k, a.c, b.c FROM t a JOIN t b ON a.r = b.k WHERE <complex predicate>
    ComplexFilterSelfJoin,
    /// SELECT t.a, u.b FROM t JOIN u ON t.a = u.b
    Join,
    /// SELECT t.a AS c0, ... FROM t UNION ALL SELECT u.b AS c0, ... FROM u
    UnionAll,
    /// Two joins of t and u on different columns of u, combined with UNION ALL
    UnionAllJoin,
    /// SELECT t.c AS c_l, u.c AS c_r FROM t JOIN u ON t.k = u.k WHERE <complex predicate>,
    /// where `c` is a column name of both t and u
    ComplexFilterJoin,
}

/// Tables and materialized views that a materialized view can read.
///
/// Turso refuses materialized views over `temp` or attached tables. Columns named
/// by an expression or a keyword (from `CREATE TABLE ... AS SELECT expr`) are
/// left out because this module writes column names without quotes.
pub fn materialized_view_sources(
    schema: &Schema,
    profile: &MaterializedViewProfile,
) -> Vec<TableRef> {
    let materialized_views: &[TableRef] = if profile.read_materialized_views {
        &schema.materialized_views
    } else {
        &[]
    };
    schema
        .tables
        .iter()
        .filter(|t| t.database.is_none())
        .chain(materialized_views)
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

/// Whether the schema has a table, and the profile a shape, that a materialized view can use.
pub fn can_create_materialized_view(schema: &Schema, profile: &MaterializedViewProfile) -> bool {
    let sources = materialized_view_sources(schema, profile);
    let shapes = weighted_shapes(profile, sources.len());
    (0..sources.len()).any(|index| {
        let others = others_than(&sources, index);
        shapes
            .iter()
            .any(|(_, shape)| profile.weight(generated_shape(&sources[index], &others, *shape)) > 0)
    })
}

/// Generate a CREATE MATERIALIZED VIEW statement over a table or another materialized view.
pub fn create_materialized_view(
    schema: &Schema,
    profile: &MaterializedViewProfile,
) -> BoxedStrategy<CreateMaterializedViewStatement> {
    assert!(
        can_create_materialized_view(schema, profile),
        "Schema must have a table that a materialized view of an enabled shape can read"
    );
    let sources = materialized_view_sources(schema, profile);
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
    let shape = proptest::strategy::Union::new_weighted(
        weighted_shapes(profile, sources.len())
            .into_iter()
            .map(|(weight, shape)| (weight, Just(shape)))
            .collect(),
    );
    let profile = profile.clone();

    (
        any::<bool>(),
        identifier_excluding(existing_names),
        0..sources.len(),
        shape,
    )
        .prop_filter_map(
            "the shape generated for this source is switched off",
            move |(if_not_exists, view_name, index, shape)| {
                let source = sources[index].clone();
                let others = others_than(&sources, index);
                let shape = generated_shape(&source, &others, shape);
                (profile.weight(shape) > 0).then_some((
                    if_not_exists,
                    view_name,
                    source,
                    others,
                    shape,
                ))
            },
        )
        .prop_flat_map(|(if_not_exists, view_name, source, others, shape)| {
            select_for_shape(source, others, shape).prop_map(move |(select_sql, output_columns)| {
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

fn weighted_shapes(profile: &MaterializedViewProfile, source_count: usize) -> Vec<(u32, Shape)> {
    let mut shapes = vec![
        Shape::Star,
        Shape::FilteredColumns,
        Shape::Aggregate,
        Shape::ComplexFilterSelfJoin,
    ];
    if source_count >= 2 {
        shapes.extend([
            Shape::Join,
            Shape::UnionAll,
            Shape::UnionAllJoin,
            Shape::ComplexFilterJoin,
        ]);
    }
    shapes
        .into_iter()
        .map(|shape| (profile.weight(shape), shape))
        .filter(|(weight, _)| *weight > 0)
        .collect()
}

fn others_than(sources: &[TableRef], index: usize) -> Vec<TableRef> {
    sources
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != index)
        .map(|(_, t)| t.clone())
        .collect()
}

/// A shape that needs columns or tables that the schema does not have falls
/// back to a simpler shape.
fn generated_shape(source: &Table, others: &[TableRef], shape: Shape) -> Shape {
    let integer_columns = source
        .columns
        .iter()
        .filter(|c| c.data_type == DataType::Integer)
        .count();
    match shape {
        Shape::FilteredColumns
            if source.columns.len() < 2 || source.filterable_columns().next().is_none() =>
        {
            Shape::Star
        }
        Shape::ComplexFilterSelfJoin if integer_columns < 2 => Shape::Star,
        Shape::UnionAllJoin if others.iter().all(|t| t.columns.len() < 2) => Shape::Join,
        Shape::ComplexFilterJoin
            if others
                .iter()
                .all(|t| same_name_columns(source, t).is_none()) =>
        {
            Shape::Join
        }
        shape => shape,
    }
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
    match shape {
        Shape::FilteredColumns => {
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
        Shape::ComplexFilterSelfJoin => (0..COMPLEX_PREDICATE_KINDS)
            .prop_map(move |kind| complex_filter_self_join(&source, kind))
            .boxed(),
        Shape::Star => Just((
            format!("SELECT * FROM {name}"),
            view_columns(&source.columns),
        ))
        .boxed(),
        Shape::ComplexFilterJoin => (
            proptest::sample::select(same_name_others),
            0..COMPLEX_PREDICATE_KINDS,
        )
            .prop_map(move |(other, kind)| complex_filter_join(&source, &other, kind))
            .boxed(),
        Shape::UnionAllJoin => proptest::sample::select(wide_others)
            .prop_map(move |other| union_all_join(&source, &other))
            .boxed(),
        Shape::Join => proptest::sample::select(others)
            .prop_map(move |other| join(&source, &other))
            .boxed(),
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
        let names: Vec<String> =
            materialized_view_sources(&schema_with_matview(), &MaterializedViewProfile::default())
                .iter()
                .map(|t| t.name.clone())
                .collect();
        assert_eq!(names, vec!["users", "mv_users"]);
    }

    proptest! {
        #[test]
        fn plain_view_sql_differs_only_in_the_keyword(stmt in create_materialized_view(&schema_with_matview(), &MaterializedViewProfile::default())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE MATERIALIZED VIEW"));
            prop_assert_eq!(sql.replacen("MATERIALIZED ", "", 1), stmt.plain_view_sql());
            prop_assert!(!stmt.output_columns.is_empty());
        }
    }

    #[test]
    fn every_shape_is_generated_and_materialized_views_are_read() {
        let schema = schema_with_matview();
        let strategy = create_materialized_view(&schema, &MaterializedViewProfile::default());
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

    const ALL_SHAPES: [Shape; 8] = [
        Shape::Star,
        Shape::FilteredColumns,
        Shape::Aggregate,
        Shape::ComplexFilterSelfJoin,
        Shape::Join,
        Shape::UnionAll,
        Shape::UnionAllJoin,
        Shape::ComplexFilterJoin,
    ];

    /// `solo` has one column, so shapes that need two columns fall back to simpler shapes over it.
    fn schema_with_a_narrow_table() -> Schema {
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
                "solo",
                vec![ColumnDef::new("x", DataType::Integer)],
            ))
            .add_materialized_view(Table::new(
                "mv_users",
                vec![
                    ColumnDef::new("id", DataType::Integer),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .build()
    }

    fn only(shape: Shape) -> MaterializedViewProfile {
        let mut profile = MaterializedViewProfile {
            star_weight: 0,
            filtered_columns_weight: 0,
            aggregate_weight: 0,
            complex_filter_self_join_weight: 0,
            join_weight: 0,
            union_all_weight: 0,
            union_all_join_weight: 0,
            complex_filter_join_weight: 0,
            read_materialized_views: true,
        };
        let weight = match shape {
            Shape::Star => &mut profile.star_weight,
            Shape::FilteredColumns => &mut profile.filtered_columns_weight,
            Shape::Aggregate => &mut profile.aggregate_weight,
            Shape::ComplexFilterSelfJoin => &mut profile.complex_filter_self_join_weight,
            Shape::Join => &mut profile.join_weight,
            Shape::UnionAll => &mut profile.union_all_weight,
            Shape::UnionAllJoin => &mut profile.union_all_join_weight,
            Shape::ComplexFilterJoin => &mut profile.complex_filter_join_weight,
        };
        *weight = 1;
        profile
    }

    fn shape_of(sql: &str) -> Shape {
        if sql.contains(" AS sjk, ") {
            Shape::ComplexFilterSelfJoin
        } else if sql.contains(" UNION ALL ") {
            if sql.contains(" JOIN ") {
                Shape::UnionAllJoin
            } else {
                Shape::UnionAll
            }
        } else if sql.contains("_l, ") {
            Shape::ComplexFilterJoin
        } else if sql.contains(" JOIN ") {
            Shape::Join
        } else if sql.contains("COUNT(*)") {
            Shape::Aggregate
        } else if sql.starts_with("SELECT * FROM") {
            Shape::Star
        } else {
            assert!(sql.contains(" WHERE "), "unknown shape: {sql}");
            Shape::FilteredColumns
        }
    }

    fn generated_selects(schema: &Schema, profile: &MaterializedViewProfile) -> Vec<String> {
        let strategy = create_materialized_view(schema, profile);
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        (0..200)
            .map(|_| strategy.new_tree(&mut runner).unwrap().current().select_sql)
            .collect()
    }

    #[test]
    fn a_profile_with_one_shape_generates_only_that_shape() {
        let schema = schema_with_a_narrow_table();
        for shape in ALL_SHAPES {
            for sql in generated_selects(&schema, &only(shape)) {
                assert_eq!(shape_of(&sql), shape, "{sql}");
            }
        }
    }

    #[test]
    fn views_read_only_tables_when_reading_materialized_views_is_off() {
        let profile = MaterializedViewProfile {
            read_materialized_views: false,
            ..MaterializedViewProfile::default()
        };
        for sql in generated_selects(&schema_with_a_narrow_table(), &profile) {
            assert!(!sql.contains("mv_users"), "{sql}");
        }
    }

    #[test]
    fn a_shape_that_no_table_allows_cannot_be_created() {
        let schema = SchemaBuilder::new()
            .add_table(Table::new(
                "solo",
                vec![ColumnDef::new("x", DataType::Integer)],
            ))
            .build();
        assert!(can_create_materialized_view(&schema, &only(Shape::Star)));
        assert!(!can_create_materialized_view(
            &schema,
            &only(Shape::FilteredColumns)
        ));
        assert!(!can_create_materialized_view(&schema, &only(Shape::Join)));
    }
}
