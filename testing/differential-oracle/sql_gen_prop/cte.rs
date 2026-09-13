//! Common Table Expression (CTE) generation.
//!
//! This module generates both ordinary CTEs and bounded recursive CTEs. The
//! recursive shapes are intentionally self-terminating so differential fuzzing
//! can exercise queue semantics without relying on timeouts.

use proptest::prelude::*;
use std::fmt;
use std::ops::RangeInclusive;

use crate::profile::StatementProfile;
use crate::schema::{ColumnDef, DataType, Schema};
use crate::select::SelectStatement;

#[derive(Debug, Clone)]
pub enum CteQuery {
    Select(SelectStatement),
    Recursive(String),
}

impl CteQuery {
    pub(crate) fn is_recursive(&self) -> bool {
        matches!(self, Self::Recursive(_))
    }
}

impl fmt::Display for CteQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select(select) => write!(f, "{select}"),
            Self::Recursive(sql) => f.write_str(sql),
        }
    }
}

/// Materialization hint for a CTE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CteMaterialization {
    /// No materialization hint (default behavior).
    Default,
    /// Force materialization (AS MATERIALIZED).
    Materialized,
    /// Prevent materialization (AS NOT MATERIALIZED).
    NotMaterialized,
}

impl fmt::Display for CteMaterialization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CteMaterialization::Default => Ok(()),
            CteMaterialization::Materialized => write!(f, "MATERIALIZED "),
            CteMaterialization::NotMaterialized => write!(f, "NOT MATERIALIZED "),
        }
    }
}

/// A single CTE definition (name AS SELECT ...).
#[derive(Debug, Clone)]
pub struct CteDefinition {
    /// The name of the CTE.
    pub name: String,
    /// Optional column aliases for the CTE.
    pub column_aliases: Option<Vec<String>>,
    /// Materialization hint.
    pub materialization: CteMaterialization,
    /// The SELECT statement defining the CTE.
    pub query: CteQuery,
    /// The effective columns this CTE exposes (derived from aliases or query).
    pub effective_columns: Vec<crate::schema::ColumnDef>,
}

impl CteDefinition {
    /// Convert this CTE to a Table that can be used as a query source.
    pub fn as_table(&self) -> crate::schema::Table {
        crate::schema::Table::new(self.name.clone(), self.effective_columns.clone())
    }

    /// Derive effective columns from aliases, query SELECT list, or source table.
    ///
    /// - If `aliases` is provided, use those names
    /// - Otherwise, derive from `query.columns`:
    ///   - Column expressions use the column name
    ///   - Other expressions get positional names (c0, c1, ...)
    /// - If query.columns is empty (SELECT *), use source_columns
    pub fn derive_effective_columns(
        aliases: &Option<Vec<String>>,
        query: &SelectStatement,
        source_columns: &[crate::schema::ColumnDef],
    ) -> Vec<crate::schema::ColumnDef> {
        use crate::schema::{ColumnDef, DataType};

        if let Some(alias_names) = aliases {
            // Use explicit aliases with inferred types from query or default to Text
            alias_names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    // Try to get type from the corresponding expression in query
                    let data_type = query
                        .columns
                        .get(i)
                        .and_then(|expr| Self::infer_expression_type(expr, source_columns))
                        .unwrap_or(DataType::Text);
                    ColumnDef::new(name.clone(), data_type)
                })
                .collect()
        } else if !query.columns.is_empty() {
            // Derive from SELECT list
            query
                .columns
                .iter()
                .enumerate()
                .map(|(i, expr)| {
                    let name = Self::derive_column_name(expr, i);
                    let data_type =
                        Self::infer_expression_type(expr, source_columns).unwrap_or(DataType::Text);
                    ColumnDef::new(name, data_type)
                })
                .collect()
        } else {
            // SELECT * - inherit all columns from source
            source_columns.to_vec()
        }
    }

    /// Derive a column name from an expression.
    fn derive_column_name(expr: &crate::expression::Expression, index: usize) -> String {
        use crate::expression::Expression;
        match expr {
            Expression::Column(name) => name.clone(),
            _ => format!("c{index}"),
        }
    }

    /// Try to infer the data type of an expression.
    fn infer_expression_type(
        expr: &crate::expression::Expression,
        source_columns: &[crate::schema::ColumnDef],
    ) -> Option<crate::schema::DataType> {
        use crate::expression::Expression;

        match expr {
            Expression::Column(name) => source_columns
                .iter()
                .find(|c| &c.name == name)
                .map(|c| c.data_type),
            Expression::Value(v) => Some(v.data_type()),
            Expression::Cast { target_type, .. } => Some(*target_type),
            // For complex expressions, default to None (caller will use Text)
            _ => None,
        }
    }
}

impl fmt::Display for CteDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;

        if let Some(aliases) = &self.column_aliases {
            write!(f, "({})", aliases.join(", "))?;
        }

        write!(f, " AS {}({})", self.materialization, self.query)
    }
}

/// A WITH clause containing one or more CTEs.
#[derive(Debug, Clone)]
pub struct WithClause {
    /// The CTE definitions.
    pub ctes: Vec<CteDefinition>,
}

impl WithClause {
    /// True when any CTE in this clause (or nested in one of its bodies) is
    /// recursive.
    pub fn has_recursive_cte(&self) -> bool {
        self.ctes.iter().any(|cte| match &cte.query {
            CteQuery::Recursive(_) => true,
            CteQuery::Select(select) => select.has_recursive_cte(),
        })
    }
}

impl fmt::Display for WithClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ctes.iter().any(|cte| cte.query.is_recursive()) {
            write!(f, "WITH RECURSIVE ")?;
        } else {
            write!(f, "WITH ")?;
        }
        for (i, cte) in self.ctes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{cte}")?;
        }
        Ok(())
    }
}

/// Profile for controlling CTE generation.
#[derive(Debug, Clone)]
pub struct CteProfile {
    /// Weight for generating a CTE (vs. no CTE).
    pub cte_weight: u32,
    /// Weight for no CTE.
    pub no_cte_weight: u32,
    /// Range for number of CTEs in a WITH clause.
    pub cte_count_range: RangeInclusive<usize>,
    /// Weight for default materialization (no hint).
    pub default_materialization_weight: u32,
    /// Weight for MATERIALIZED hint.
    pub materialized_weight: u32,
    /// Weight for NOT MATERIALIZED hint.
    pub not_materialized_weight: u32,
    /// Weight for generating column aliases.
    pub column_aliases_weight: u32,
    /// Weight for no column aliases.
    pub no_column_aliases_weight: u32,
    /// Weight for recursive definitions among generated CTEs.
    pub recursive_weight: u32,
    /// Weight for ordinary definitions among generated CTEs.
    pub non_recursive_weight: u32,
}

impl Default for CteProfile {
    fn default() -> Self {
        Self {
            cte_weight: 10,
            no_cte_weight: 90,
            cte_count_range: 1..=2,
            default_materialization_weight: 50,
            materialized_weight: 25,
            not_materialized_weight: 25,
            column_aliases_weight: 20,
            no_column_aliases_weight: 80,
            recursive_weight: 20,
            non_recursive_weight: 80,
        }
    }
}

impl CteProfile {
    /// Builder method to disable CTEs entirely.
    pub fn disabled(self) -> Self {
        Self {
            cte_weight: 0,
            no_cte_weight: 100,
            ..self
        }
    }

    /// Builder method to always generate CTEs.
    pub fn always(self) -> Self {
        Self {
            cte_weight: 100,
            no_cte_weight: 0,
            ..self
        }
    }

    /// Builder method to set CTE count range.
    pub fn with_cte_count_range(mut self, range: RangeInclusive<usize>) -> Self {
        self.cte_count_range = range;
        self
    }

    /// Builder method to disable materialization hints.
    pub fn with_default_materialization_only(mut self) -> Self {
        self.materialized_weight = 0;
        self.not_materialized_weight = 0;
        self.default_materialization_weight = 100;
        self
    }

    /// Returns true if CTEs are enabled.
    pub fn is_enabled(&self) -> bool {
        self.cte_weight > 0
    }
}

/// Generate a materialization hint.
pub fn materialization(profile: &CteProfile) -> BoxedStrategy<CteMaterialization> {
    let default_w = profile.default_materialization_weight;
    let mat_w = profile.materialized_weight;
    let not_mat_w = profile.not_materialized_weight;

    prop_oneof![
        default_w => Just(CteMaterialization::Default),
        mat_w => Just(CteMaterialization::Materialized),
        not_mat_w => Just(CteMaterialization::NotMaterialized),
    ]
    .boxed()
}

/// Generate a CTE name that doesn't conflict with existing tables.
fn cte_name(schema: &Schema, existing_cte_names: &[String]) -> BoxedStrategy<String> {
    let table_names: Vec<String> = schema.table_names().into_iter().collect();
    let existing: Vec<String> = existing_cte_names.to_vec();

    // Generate names like cte_0, cte_1, etc.
    (0..100u32)
        .prop_map(|i| format!("cte_{i}"))
        .prop_filter("name must not conflict", move |name| {
            !table_names.contains(name) && !existing.contains(name)
        })
        .boxed()
}

/// Generate optional column aliases for a CTE.
fn column_aliases(num_columns: usize, profile: &CteProfile) -> BoxedStrategy<Option<Vec<String>>> {
    // Only generate aliases if the query has explicit columns (not SELECT *)
    if num_columns == 0 {
        return Just(None).boxed();
    }

    let no_alias_w = profile.no_column_aliases_weight;
    let alias_w = profile.column_aliases_weight;

    prop_oneof![
        no_alias_w => Just(None),
        alias_w => {
            proptest::collection::vec(
                // Regex guarantees at least 1 char, so no filter needed
                "[a-z][a-z0-9_]{0,5}",
                num_columns..=num_columns
            )
            .prop_filter("aliases must be unique", |aliases| {
                let unique: std::collections::HashSet<_> = aliases.iter().collect();
                unique.len() == aliases.len()
            })
            .prop_map(Some)
        },
    ]
    .boxed()
}

/// Generate a single CTE definition.
fn non_recursive_cte_definition(
    schema: &Schema,
    existing_cte_names: &[String],
    profile: &StatementProfile,
) -> BoxedStrategy<CteDefinition> {
    let cte_profile = profile.select_profile().cte_profile.clone();

    // First, pick a table to base the CTE query on
    let tables = schema.tables.clone();
    // Note: This function should not be called with an empty schema.
    // The optional_with_clause() function checks for empty tables and returns None.
    assert!(
        !tables.is_empty(),
        "cte_definition requires at least one table in the schema"
    );

    let schema_clone = schema.clone();
    let existing_names = existing_cte_names.to_vec();

    // Create a profile for inner queries that disables CTEs to avoid recursion
    let mut inner_profile = profile.clone();
    inner_profile.select.extra.cte_profile = CteProfile::default().disabled();

    proptest::sample::select((*tables).clone())
        .prop_flat_map(move |table| {
            let schema = schema_clone.clone();
            let inner_profile = inner_profile.clone();
            let cte_profile = cte_profile.clone();
            let existing = existing_names.clone();
            let source_columns = table.columns.clone();

            // Generate the base SELECT query for the CTE
            crate::select::select_for_table(&table, &schema, &inner_profile).prop_flat_map(
                move |query| {
                    let schema = schema.clone();
                    let cte_profile = cte_profile.clone();
                    let existing = existing.clone();
                    let query_clone = query.clone();
                    let source_columns = source_columns.clone();

                    let num_columns = if query.columns.is_empty() {
                        source_columns.len()
                    } else {
                        query.columns.len()
                    };
                    (
                        cte_name(&schema, &existing),
                        column_aliases(num_columns, &cte_profile),
                        materialization(&cte_profile),
                    )
                        .prop_map(move |(name, aliases, mat)| {
                            let effective_columns = CteDefinition::derive_effective_columns(
                                &aliases,
                                &query_clone,
                                &source_columns,
                            );
                            CteDefinition {
                                name,
                                column_aliases: aliases,
                                materialization: mat,
                                query: CteQuery::Select(query_clone.clone()),
                                effective_columns,
                            }
                        })
                },
            )
        })
        .boxed()
}

fn recursive_cte_definition(
    schema: &Schema,
    existing_cte_names: &[String],
    profile: &StatementProfile,
) -> BoxedStrategy<CteDefinition> {
    let cte_profile = profile.select_profile().cte_profile.clone();
    let existing = existing_cte_names.to_vec();
    let schema = schema.clone();
    let tables = schema.tables.clone();

    (
        cte_name(&schema, &existing),
        proptest::sample::select((*tables).clone()),
        0u8..5,
        -32i64..=32,
        1i64..=7,
        0i64..=64,
        any::<bool>(),
        0u8..5,
        any::<bool>(),
        0u32..=128,
        0u32..=32,
        materialization(&cte_profile),
    )
        .prop_map(
            |(
                name,
                table,
                shape,
                seed,
                magnitude,
                span,
                descending,
                order_mode,
                has_limit,
                limit,
                offset,
                materialization,
            )| {
                let order_suffix = match order_mode {
                    0 => String::new(),
                    1 => " ORDER BY 1 ASC".to_string(),
                    2 => " ORDER BY 1 DESC".to_string(),
                    3 => " ORDER BY 1 ASC NULLS LAST".to_string(),
                    _ => " ORDER BY 1 DESC NULLS FIRST".to_string(),
                };
                let limit_suffix = if has_limit {
                    format!(" LIMIT {limit} OFFSET {offset}")
                } else {
                    String::new()
                };
                let bounded_limit_suffix = if has_limit {
                    format!(" LIMIT {} OFFSET {}", limit.min(64), offset.min(32))
                } else {
                    " LIMIT 64".to_string()
                };

                let (column_aliases, sql) = match shape {
                    0 => {
                        let step = if descending { -magnitude } else { magnitude };
                        let bound = seed.saturating_add(step.saturating_mul(span));
                        let comparison = if step > 0 { "<" } else { ">" };
                        let union = if span % 3 == 0 { "UNION" } else { "UNION ALL" };
                        (
                            vec!["x".to_string()],
                            format!(
                                "VALUES({seed}) {union} SELECT x + ({step}) FROM {name} \
                                 WHERE x {comparison} {bound}{order_suffix}{limit_suffix}"
                            ),
                        )
                    }
                    1 => {
                        let modulus = magnitude + span.max(1);
                        let start = seed.rem_euclid(modulus);
                        (
                            vec!["x".to_string()],
                            format!(
                                "VALUES({start}) UNION SELECT (x + {magnitude}) % {modulus} \
                                 FROM {name}{order_suffix}{limit_suffix}"
                            ),
                        )
                    }
                    2 => {
                        let max_depth = span.min(7);
                        let branch_order = match order_mode {
                            0 => String::new(),
                            1 | 3 => " ORDER BY 2 ASC, 1 ASC".to_string(),
                            _ => " ORDER BY 2 DESC, 1 ASC".to_string(),
                        };
                        (
                            vec!["x".to_string(), "depth".to_string()],
                            format!(
                                "VALUES({seed}, 0) \
                                 UNION ALL SELECT x * 2, depth + 1 FROM {name} \
                                 WHERE depth < {max_depth} \
                                 UNION ALL SELECT x * 2 + 1, depth + 1 FROM {name} \
                                WHERE depth < {max_depth}{branch_order}{limit_suffix}"
                            ),
                        )
                    }
                    3 => {
                        let bound = seed.saturating_add(span.clamp(1, 4));
                        let table_name = table.qualified_name();
                        let column_name = &table
                            .columns
                            .first()
                            .expect("generated tables must have at least one column")
                            .name;
                        (
                            vec!["x".to_string()],
                            format!(
                                "VALUES({seed}) \
                                 UNION ALL SELECT x + 1 FROM {name} \
                                 JOIN {table_name} AS recursive_source \
                                   ON recursive_source.{column_name} IS NOT NULL \
                                 WHERE x < {bound}{order_suffix}{bounded_limit_suffix}"
                            ),
                        )
                    }
                    4 => {
                        let bound = seed.saturating_add(span.clamp(1, 4));
                        let table_name = table.qualified_name();
                        let column_name = &table
                            .columns
                            .first()
                            .expect("generated tables must have at least one column")
                            .name;
                        (
                            vec!["x".to_string()],
                            format!(
                                "VALUES({seed}) \
                                 UNION ALL SELECT {name}.x + 1 FROM {name} \
                                 LEFT JOIN {table_name} AS recursive_source \
                                   ON recursive_source.{column_name} = {name}.x \
                                 WHERE {name}.x < {bound}{order_suffix}{bounded_limit_suffix}"
                            ),
                        )
                    }
                    _ => unreachable!("recursive CTE generator produced unknown shape {shape}"),
                };
                let effective_columns = column_aliases
                    .iter()
                    .map(|name| ColumnDef::new(name.clone(), DataType::Integer))
                    .collect();
                CteDefinition {
                    name,
                    column_aliases: Some(column_aliases),
                    materialization,
                    query: CteQuery::Recursive(sql),
                    effective_columns,
                }
            },
        )
        .boxed()
}

/// Generate a scalar subquery whose recursive CTE bound is correlated with a
/// column of the outer query's source table.
///
/// The recursion bound is clamped through `min(<outer column>, <small
/// constant>)`, which is never larger than the constant regardless of the
/// outer column's type or value, so every invocation terminates. Aggregating
/// the CTE inside the subquery keeps the result a single value while still
/// depending on the full per-invocation row set, so stale queue, distinct, or
/// LIMIT state from a previous outer row changes the compared output.
fn correlated_recursive_subquery(
    outer: &crate::schema::Table,
    schema: &Schema,
    excluded_cte_names: &[String],
) -> BoxedStrategy<crate::expression::Expression> {
    use crate::expression::Expression;

    let outer_name = outer.qualified_name();
    let column_names: Vec<String> = outer.columns.iter().map(|c| c.name.clone()).collect();
    assert!(
        !column_names.is_empty(),
        "correlated recursive subquery requires an outer table with columns"
    );

    // SQLite 3.50.2, currently bundled by rusqlite in this workspace, drops
    // rows from a recursive CTE that combines ORDER BY with placement inside
    // a scalar subquery (fixed in later SQLite versions), so no ORDER BY is
    // generated here. recursive-cte.sqltest covers that combination against
    // current SQLite instead.
    (
        cte_name(schema, excluded_cte_names),
        proptest::sample::select(column_names),
        -8i64..=8,
        1i64..=8,
        any::<bool>(),
        proptest::option::of((0u32..=16, 0u32..=4)),
        proptest::sample::select(vec!["count", "sum", "max", "min", "total"]),
    )
        .prop_map(
            move |(name, column, seed, span, union_all, limit, aggregate)| {
                let union = if union_all { "UNION ALL" } else { "UNION" };
                let bound = seed + span;
                let limit_suffix = limit
                    .map(|(limit, offset)| format!(" LIMIT {limit} OFFSET {offset}"))
                    .unwrap_or_default();
                let body = format!(
                    "VALUES({seed}) {union} SELECT x + 1 FROM {name} \
                     WHERE x < min({outer_name}.{column}, {bound}){limit_suffix}"
                );
                let cte = CteDefinition {
                    name: name.clone(),
                    column_aliases: Some(vec!["x".to_string()]),
                    materialization: CteMaterialization::Default,
                    query: CteQuery::Recursive(body),
                    effective_columns: vec![ColumnDef::new("x", DataType::Integer)],
                };
                Expression::Subquery(Box::new(SelectStatement {
                    with_clause: Some(WithClause { ctes: vec![cte] }),
                    table: name,
                    columns: vec![Expression::FunctionCall {
                        name: aggregate.to_string(),
                        args: vec![Expression::Column("x".to_string())],
                        filter: None,
                    }],
                    where_clause: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                }))
            },
        )
        .boxed()
}

/// Generate extra SELECT-list columns holding correlated recursive scalar
/// subqueries, scaled by how strongly the profile favors recursive CTEs.
/// Generating up to two per statement exercises repeated re-entry of separate
/// recursive CTEs for each outer row.
pub fn correlated_recursive_subquery_columns(
    outer: &crate::schema::Table,
    schema: &Schema,
    excluded_cte_names: &[String],
    profile: &StatementProfile,
) -> BoxedStrategy<Vec<crate::expression::Expression>> {
    let cte_profile = &profile.select_profile().cte_profile;
    let weight = (cte_profile.cte_weight * cte_profile.recursive_weight / 100).min(50);
    if weight == 0 {
        return Just(Vec::new()).boxed();
    }
    prop_oneof![
        (100 - weight) => Just(Vec::new()),
        weight => proptest::collection::vec(
            correlated_recursive_subquery(outer, schema, excluded_cte_names),
            1..=2,
        ),
    ]
    .boxed()
}

/// Generate a single ordinary or recursive CTE definition.
pub fn cte_definition(
    schema: &Schema,
    existing_cte_names: &[String],
    profile: &StatementProfile,
) -> BoxedStrategy<CteDefinition> {
    let recursive_weight = profile.select_profile().cte_profile.recursive_weight;
    let non_recursive_weight = profile.select_profile().cte_profile.non_recursive_weight;
    prop_oneof![
        non_recursive_weight =>
            non_recursive_cte_definition(schema, existing_cte_names, profile),
        recursive_weight => recursive_cte_definition(schema, existing_cte_names, profile),
    ]
    .boxed()
}

/// Generate a WITH clause with one or more CTEs.
pub fn with_clause(schema: &Schema, profile: &StatementProfile) -> BoxedStrategy<WithClause> {
    let cte_profile = profile.select_profile().cte_profile.clone();
    let count_range = cte_profile.cte_count_range;

    // Generate CTEs one at a time, collecting names to avoid conflicts
    let schema_clone = schema.clone();
    let profile_clone = profile.clone();

    count_range
        .prop_flat_map(move |count| {
            let schema = schema_clone.clone();
            let profile = profile_clone.clone();

            // Generate CTEs sequentially to avoid name conflicts
            (0..count).fold(Just(Vec::<CteDefinition>::new()).boxed(), move |prev, _| {
                let schema = schema.clone();
                let profile = profile.clone();

                prev.prop_flat_map(move |ctes| {
                    let existing_names: Vec<String> = ctes.iter().map(|c| c.name.clone()).collect();
                    let schema = schema.clone();
                    let profile = profile.clone();

                    cte_definition(&schema, &existing_names, &profile).prop_map(move |cte| {
                        let mut new_ctes = ctes.clone();
                        new_ctes.push(cte);
                        new_ctes
                    })
                })
                .boxed()
            })
        })
        .prop_map(|ctes| WithClause { ctes })
        .boxed()
}

/// Generate an optional WITH clause.
pub fn optional_with_clause(
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<Option<WithClause>> {
    let cte_profile = &profile.select_profile().cte_profile;

    if !cte_profile.is_enabled() || schema.tables.is_empty() {
        return Just(None).boxed();
    }

    let cte_weight = cte_profile.cte_weight;
    let no_cte_weight = cte_profile.no_cte_weight;
    let schema_clone = schema.clone();
    let profile_clone = profile.clone();

    prop_oneof![
        no_cte_weight => Just(None),
        cte_weight => with_clause(&schema_clone, &profile_clone).prop_map(Some),
    ]
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType, SchemaBuilder, Table};
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .add_table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .add_table(Table::new(
                "orders",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("user_id", DataType::Integer),
                ],
            ))
            .build()
    }

    #[test]
    fn test_materialization_display() {
        assert_eq!(CteMaterialization::Default.to_string(), "");
        assert_eq!(
            CteMaterialization::Materialized.to_string(),
            "MATERIALIZED "
        );
        assert_eq!(
            CteMaterialization::NotMaterialized.to_string(),
            "NOT MATERIALIZED "
        );
    }

    #[test]
    fn test_cte_definition_display() {
        use crate::expression::Expression;

        let cte = CteDefinition {
            name: "my_cte".to_string(),
            column_aliases: None,
            materialization: CteMaterialization::Default,
            query: CteQuery::Select(SelectStatement {
                with_clause: None,
                table: "users".to_string(),
                columns: vec![Expression::Column("id".to_string())],
                where_clause: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
            effective_columns: vec![ColumnDef::new("id", DataType::Integer)],
        };

        assert_eq!(cte.to_string(), "my_cte AS (SELECT id FROM users)");

        let cte_materialized = CteDefinition {
            name: "my_cte".to_string(),
            column_aliases: Some(vec!["a".to_string(), "b".to_string()]),
            materialization: CteMaterialization::Materialized,
            query: CteQuery::Select(SelectStatement {
                with_clause: None,
                table: "users".to_string(),
                columns: vec![
                    Expression::Column("id".to_string()),
                    Expression::Column("name".to_string()),
                ],
                where_clause: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
            effective_columns: vec![
                ColumnDef::new("a", DataType::Integer),
                ColumnDef::new("b", DataType::Text),
            ],
        };

        assert_eq!(
            cte_materialized.to_string(),
            "my_cte(a, b) AS MATERIALIZED (SELECT id, name FROM users)"
        );
    }

    #[test]
    fn test_with_clause_display() {
        use crate::expression::Expression;

        let with = WithClause {
            ctes: vec![
                CteDefinition {
                    name: "cte1".to_string(),
                    column_aliases: None,
                    materialization: CteMaterialization::Default,
                    query: CteQuery::Select(SelectStatement {
                        with_clause: None,
                        table: "users".to_string(),
                        columns: vec![Expression::Column("id".to_string())],
                        where_clause: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    }),
                    effective_columns: vec![ColumnDef::new("id", DataType::Integer)],
                },
                CteDefinition {
                    name: "cte2".to_string(),
                    column_aliases: None,
                    materialization: CteMaterialization::NotMaterialized,
                    query: CteQuery::Select(SelectStatement {
                        with_clause: None,
                        table: "orders".to_string(),
                        columns: vec![Expression::Column("user_id".to_string())],
                        where_clause: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    }),
                    effective_columns: vec![ColumnDef::new("user_id", DataType::Integer)],
                },
            ],
        };

        assert_eq!(
            with.to_string(),
            "WITH cte1 AS (SELECT id FROM users), cte2 AS NOT MATERIALIZED (SELECT user_id FROM orders)"
        );
    }

    #[test]
    fn recursive_definition_display_enables_recursive_keyword() {
        let with = WithClause {
            ctes: vec![CteDefinition {
                name: "seq".to_string(),
                column_aliases: Some(vec!["x".to_string()]),
                materialization: CteMaterialization::NotMaterialized,
                query: CteQuery::Recursive(
                    "VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < 3".to_string(),
                ),
                effective_columns: vec![ColumnDef::new("x", DataType::Integer)],
            }],
        };

        assert_eq!(
            with.to_string(),
            "WITH RECURSIVE seq(x) AS NOT MATERIALIZED (VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < 3)"
        );
    }

    #[test]
    fn recursive_generator_produces_bounded_well_formed_definitions() {
        let schema = test_schema();
        let mut profile = StatementProfile::default();
        profile.select.extra.cte_profile = CteProfile {
            cte_weight: 100,
            no_cte_weight: 0,
            cte_count_range: 1..=4,
            recursive_weight: 100,
            non_recursive_weight: 0,
            ..CteProfile::default()
        };
        let strategy = with_clause(&schema, &profile);
        let mut runner = TestRunner::deterministic();

        for _ in 0..512 {
            let with = strategy
                .new_tree(&mut runner)
                .expect("recursive CTE strategy must produce a value")
                .current();
            let sql = with.to_string();
            assert!(sql.starts_with("WITH RECURSIVE "));

            let mut names = std::collections::HashSet::new();
            for cte in &with.ctes {
                assert!(names.insert(cte.name.clone()), "duplicate CTE name: {sql}");
                assert_eq!(
                    cte.column_aliases.as_ref().map(Vec::len),
                    Some(cte.effective_columns.len())
                );
                assert!(
                    cte.effective_columns
                        .iter()
                        .all(|column| column.data_type == DataType::Integer)
                );

                let CteQuery::Recursive(body) = &cte.query else {
                    panic!("recursive-only profile generated an ordinary CTE: {sql}");
                };
                assert!(
                    body.contains(&format!("FROM {}", cte.name))
                        || body.contains(&format!("JOIN {}", cte.name))
                );
                assert!(
                    body.contains(" WHERE ") || body.contains(" UNION SELECT "),
                    "recursive body is not structurally bounded: {body}"
                );
            }
        }
    }

    #[test]
    fn correlated_recursive_subquery_is_clamped_to_the_outer_source() {
        let schema = test_schema();
        let outer = schema
            .tables
            .iter()
            .find(|table| table.unqualified_name() == "users")
            .expect("test schema must contain users")
            .clone();
        let mut profile = StatementProfile::default();
        profile.select.extra.cte_profile = CteProfile {
            cte_weight: 100,
            no_cte_weight: 0,
            recursive_weight: 100,
            non_recursive_weight: 0,
            ..CteProfile::default()
        };
        let strategy = correlated_recursive_subquery_columns(
            &outer,
            &schema,
            &["cte_0".to_string()],
            &profile,
        );
        let mut runner = TestRunner::deterministic();

        let mut saw_subquery = false;
        for _ in 0..256 {
            let columns = strategy
                .new_tree(&mut runner)
                .expect("correlated subquery strategy must produce a value")
                .current();
            for column in &columns {
                saw_subquery = true;
                let sql = column.to_string();
                assert!(sql.contains("WITH RECURSIVE "), "not recursive: {sql}");
                assert!(
                    sql.contains(" WHERE x < min(users."),
                    "recursion bound is not clamped to the outer source: {sql}"
                );
                assert!(!sql.contains("cte_0"), "excluded CTE name reused: {sql}");
            }
        }
        assert!(
            saw_subquery,
            "strategy never produced a correlated subquery"
        );
    }

    proptest::proptest! {
        #[test]
        fn generated_with_clause_is_valid(
            with in {
                let schema = test_schema();
                with_clause(&schema, &StatementProfile::default())
            }
        ) {
            let sql = with.to_string();
            proptest::prop_assert!(sql.starts_with("WITH "));
            proptest::prop_assert!(sql.contains(" AS "));
        }
    }

    #[test]
    fn test_cte_profile_disabled() {
        let profile = CteProfile::default().disabled();
        assert!(!profile.is_enabled());
    }

    #[test]
    fn test_cte_profile_always() {
        let profile = CteProfile::default().always();
        assert!(profile.is_enabled());
        assert_eq!(profile.cte_weight, 100);
        assert_eq!(profile.no_cte_weight, 0);
    }
}
