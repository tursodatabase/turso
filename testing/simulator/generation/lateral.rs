use rand::Rng;
use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, pick, pick_index, pick_n_unique},
    model::{
        query::predicate::Predicate,
        table::{ColumnType, Table},
    },
};
use turso_core::{Numeric, Value};
use turso_parser::ast;

use crate::model::lateral::{
    ColumnRef, Comparison, LateralColumn, LateralForm, LateralJoin, LateralJoinType, LateralSelect,
    SubqueryJoin,
};

const TABLE_ALIAS: &str = "o";
const MAX_JOINS: usize = 3;
const MAX_JOINED_TABLES: usize = 2;
const MAX_RESULT_ROWS: usize = 10_000;
const MAX_SELECTED_COLUMNS: usize = 3;
const COMPARISON_OPERATORS: [ast::Operator; 8] = [
    ast::Operator::Equals,
    ast::Operator::NotEquals,
    ast::Operator::Less,
    ast::Operator::LessEquals,
    ast::Operator::Greater,
    ast::Operator::GreaterEquals,
    ast::Operator::Is,
    ast::Operator::IsNot,
];

#[derive(Debug)]
struct VisibleColumn {
    source: ColumnRef,
    origin: ColumnOrigin,
}

#[derive(Debug, Clone, Copy)]
enum ColumnOrigin {
    Table(Option<ExactJsonValue>),
    Lateral(Option<ExactJsonValue>),
}

#[derive(Debug, Clone, Copy)]
enum ExactJsonValue {
    Integer,
    Text,
}

#[derive(Debug)]
struct LocalColumn {
    visible: VisibleColumn,
    name: String,
    column_type: ColumnType,
    leads_an_index: bool,
}

impl Arbitrary for LateralSelect {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let tables: Vec<&Table> = context.tables().iter().collect();
        let smallest_table = *tables.iter().min_by_key(|table| table.rows.len()).unwrap();
        let row_limit = MAX_RESULT_ROWS / smallest_table.rows.len().max(1);
        let table = pick_table(rng, &tables, 2, row_limit).unwrap_or(smallest_table);
        let aliased_table = with_alias(table, TABLE_ALIAS);
        let mut visible_columns = table_columns(&aliased_table);
        let mut max_rows = table.rows.len().max(1);
        let mut used_tables = vec![table];
        let mut joins = Vec::new();
        for position in 0..rng.random_range(1..=MAX_JOINS) {
            let row_limit = MAX_RESULT_ROWS / max_rows;
            let inner_table = match pick_subquery_table(rng, &tables, &used_tables, row_limit) {
                Some(inner_table) => inner_table,
                None if joins.is_empty() => smallest_table,
                None => break,
            };
            max_rows *= inner_table.rows.len().max(1);
            used_tables.push(inner_table);
            let mut subquery_tables = vec![inner_table];
            for _ in 0..rng.random_range(0..=MAX_JOINED_TABLES) {
                let row_limit = MAX_RESULT_ROWS / max_rows;
                let Some(table) = pick_subquery_table(rng, &tables, &used_tables, row_limit) else {
                    break;
                };
                max_rows *= table.rows.len().max(1);
                used_tables.push(table);
                subquery_tables.push(table);
            }
            let (join, join_columns) = arbitrary_join(
                rng,
                context,
                &aliased_table,
                &subquery_tables,
                &visible_columns,
                position,
            );
            visible_columns.extend(join_columns);
            joins.push(join);
        }
        let column_count = rng.random_range(1..=table.columns.len().min(MAX_SELECTED_COLUMNS));
        let columns = pick_n_unique(0..table.columns.len(), column_count, rng)
            .map(|column| table.columns[column].name.clone())
            .collect();
        LateralSelect {
            table: table.name.clone(),
            table_alias: TABLE_ALIAS.to_string(),
            columns,
            joins,
            where_clause: true_or_arbitrary(rng, context, &aliased_table),
        }
    }
}

fn pick_subquery_table<'a, R: Rng + ?Sized>(
    rng: &mut R,
    tables: &[&'a Table],
    used_tables: &[&'a Table],
    row_limit: usize,
) -> Option<&'a Table> {
    if rng.random_bool(0.5) {
        if let Some(table) = pick_table(rng, used_tables, 1, row_limit) {
            return Some(table);
        }
    }
    pick_table(rng, tables, 1, row_limit)
}

fn pick_table<'a, R: Rng + ?Sized>(
    rng: &mut R,
    tables: &[&'a Table],
    preferred_min_rows: usize,
    row_limit: usize,
) -> Option<&'a Table> {
    let tables: Vec<&Table> = tables
        .iter()
        .copied()
        .filter(|table| table.rows.len().max(1) <= row_limit)
        .collect();
    let tables_with_enough_rows: Vec<&Table> = tables
        .iter()
        .copied()
        .filter(|table| table.rows.len() >= preferred_min_rows)
        .collect();
    if tables_with_enough_rows.is_empty() {
        (!tables.is_empty()).then(|| *pick(&tables, rng))
    } else {
        Some(*pick(&tables_with_enough_rows, rng))
    }
}

fn arbitrary_join<R: Rng + ?Sized, C: GenerationContext>(
    rng: &mut R,
    context: &C,
    aliased_table: &Table,
    tables: &[&Table],
    visible_columns: &[VisibleColumn],
    position: usize,
) -> (LateralJoin, Vec<VisibleColumn>) {
    let alias = format!("s{position}");
    let aliased_tables: Vec<Table> = tables
        .iter()
        .enumerate()
        .map(|(index, table)| {
            let table_alias = if index == 0 {
                format!("i{position}")
            } else {
                format!("i{position}_{index}")
            };
            with_alias(table, &table_alias)
        })
        .collect();
    let columns_per_table: Vec<Vec<LocalColumn>> =
        aliased_tables.iter().map(local_columns).collect();
    let local_columns: Vec<&LocalColumn> = columns_per_table.iter().flatten().collect();

    let column_count = rng.random_range(1..=local_columns.len().min(MAX_SELECTED_COLUMNS));
    let mut selected: Vec<&VisibleColumn> =
        pick_n_unique(0..local_columns.len(), column_count, rng)
            .map(|column| &local_columns[column].visible)
            .collect();
    if rng.random_bool(0.3) {
        selected.push(pick(visible_columns, rng));
    }
    let (columns, origins): (Vec<LateralColumn>, Vec<ColumnOrigin>) =
        selected.into_iter().map(VisibleColumn::select).unzip();
    let outputs = origins
        .into_iter()
        .enumerate()
        .map(|(column, origin)| VisibleColumn {
            source: ColumnRef::Lateral {
                join: position,
                column,
            },
            origin,
        })
        .collect();

    let joined_tables = (1..aliased_tables.len())
        .map(|index| {
            let earlier_columns: Vec<&LocalColumn> =
                columns_per_table[..index].iter().flatten().collect();
            let mut on = Vec::new();
            if rng.random_bool(0.6) {
                on.push(equi_join(rng, &columns_per_table[index], &earlier_columns));
            }
            if on.is_empty() || rng.random_bool(0.6) {
                let operators = correlation_operators(rng);
                let columns: Vec<&LocalColumn> = columns_per_table[index].iter().collect();
                let column = pick_correlated_column(rng, &columns);
                on.push(correlated_comparison(
                    rng,
                    column,
                    visible_columns,
                    operators,
                ));
            }
            SubqueryJoin {
                table: tables[index].name.clone(),
                alias: aliased_tables[index].name.clone(),
                on,
            }
        })
        .collect();
    let correlated_column = pick_correlated_column(rng, &local_columns);
    let operators = correlation_operators(rng);
    let correlation = correlated_comparison(rng, correlated_column, visible_columns, operators);

    let join_type = match rng.random_range(0..4) {
        0 => LateralJoinType::Comma,
        1 => LateralJoinType::Cross,
        2 => LateralJoinType::Inner {
            on: true_or_arbitrary(rng, context, aliased_table),
        },
        _ => LateralJoinType::Left {
            on: true_or_arbitrary(rng, context, aliased_table),
        },
    };
    let filter_operator = if rng.random_bool(0.7) {
        ast::Operator::And
    } else {
        ast::Operator::Or
    };
    let limit = rng.random_bool(0.3).then(|| rng.random_range(1..=3));
    let order_by = match limit {
        Some(_) => arbitrary_order_by(rng, columns.len(), columns.len()),
        None if rng.random_bool(0.2) => {
            let sort_key_count = rng.random_range(1..=columns.len());
            arbitrary_order_by(rng, columns.len(), sort_key_count)
        }
        None => Vec::new(),
    };
    let filtered_table = pick(&aliased_tables, rng);
    let join = LateralJoin {
        join_type,
        form: LateralForm::Lateral,
        alias,
        table: tables[0].name.clone(),
        table_alias: aliased_tables[0].name.clone(),
        joined_tables,
        distinct: rng.random_bool(0.2),
        columns,
        correlation,
        filter_operator,
        filter: true_or_arbitrary(rng, context, filtered_table),
        order_by,
        limit,
    };
    (join, outputs)
}

fn equi_join<R: Rng + ?Sized>(
    rng: &mut R,
    columns: &[LocalColumn],
    earlier_columns: &[&LocalColumn],
) -> Comparison {
    let column = pick(columns, rng);
    let same_name: Vec<&LocalColumn> = earlier_columns
        .iter()
        .copied()
        .filter(|earlier| earlier.name == column.name)
        .collect();
    let same_type: Vec<&LocalColumn> = earlier_columns
        .iter()
        .copied()
        .filter(|earlier| earlier.column_type == column.column_type)
        .collect();
    let other = if !same_name.is_empty() {
        *pick(&same_name, rng)
    } else if !same_type.is_empty() {
        *pick(&same_type, rng)
    } else {
        *pick(earlier_columns, rng)
    };
    either_order(
        rng,
        column.visible.source.clone(),
        ast::Operator::Equals,
        other.visible.source.clone(),
    )
}

fn pick_correlated_column<'a, R: Rng + ?Sized>(
    rng: &mut R,
    columns: &[&'a LocalColumn],
) -> &'a LocalColumn {
    let indexed_columns: Vec<&LocalColumn> = columns
        .iter()
        .copied()
        .filter(|column| column.leads_an_index)
        .collect();
    let candidates: &[&LocalColumn] = if !indexed_columns.is_empty() && rng.random_bool(0.5) {
        &indexed_columns
    } else {
        columns
    };
    candidates[pick_index(candidates.len(), rng)]
}

fn correlation_operators<R: Rng + ?Sized>(rng: &mut R) -> &'static [ast::Operator] {
    if rng.random_bool(0.5) {
        &[ast::Operator::Equals]
    } else {
        &COMPARISON_OPERATORS
    }
}

fn correlated_comparison<R: Rng + ?Sized>(
    rng: &mut R,
    column: &LocalColumn,
    visible_columns: &[VisibleColumn],
    operators: &[ast::Operator],
) -> Comparison {
    let comparable_columns: Vec<&VisibleColumn> = visible_columns
        .iter()
        .filter(|visible| visible.compares_the_same_in_both_forms(column.column_type))
        .collect();
    let same_columns: Vec<&VisibleColumn> = comparable_columns
        .iter()
        .copied()
        .filter(|visible| {
            matches!(&visible.source, ColumnRef::Table { column: name, .. } if *name == column.name)
        })
        .collect();
    let other = if !same_columns.is_empty() && rng.random_bool(0.6) {
        *pick(&same_columns, rng)
    } else {
        *pick(&comparable_columns, rng)
    };
    let operator = *pick(operators, rng);
    either_order(
        rng,
        column.visible.source.clone(),
        operator,
        other.source.clone(),
    )
}

fn either_order<R: Rng + ?Sized>(
    rng: &mut R,
    first: ColumnRef,
    operator: ast::Operator,
    second: ColumnRef,
) -> Comparison {
    let (left, right) = if rng.random_bool(0.5) {
        (first, second)
    } else {
        (second, first)
    };
    Comparison {
        left,
        operator,
        right,
    }
}

fn arbitrary_order_by<R: Rng + ?Sized>(
    rng: &mut R,
    column_count: usize,
    sort_key_count: usize,
) -> Vec<(usize, ast::SortOrder)> {
    pick_n_unique(0..column_count, sort_key_count, rng)
        .collect::<Vec<_>>()
        .into_iter()
        .map(|column| {
            let order = if rng.random_bool(0.5) {
                ast::SortOrder::Asc
            } else {
                ast::SortOrder::Desc
            };
            (column, order)
        })
        .collect()
}

impl VisibleColumn {
    fn select(&self) -> (LateralColumn, ColumnOrigin) {
        let (quoted, exact_json_value) = match self.origin {
            ColumnOrigin::Table(exact_json_value) => (exact_json_value.is_none(), exact_json_value),
            ColumnOrigin::Lateral(exact_json_value) => (false, exact_json_value),
        };
        let column = LateralColumn {
            source: self.source.clone(),
            quoted,
        };
        (column, ColumnOrigin::Lateral(exact_json_value))
    }

    fn compares_the_same_in_both_forms(&self, column_type: ColumnType) -> bool {
        match self.origin {
            ColumnOrigin::Table(_) | ColumnOrigin::Lateral(None) => true,
            ColumnOrigin::Lateral(Some(ExactJsonValue::Integer)) => {
                matches!(column_type, ColumnType::Integer | ColumnType::Float)
            }
            ColumnOrigin::Lateral(Some(ExactJsonValue::Text)) => {
                matches!(column_type, ColumnType::Text)
            }
        }
    }
}

fn true_or_arbitrary<R: Rng + ?Sized, C: GenerationContext>(
    rng: &mut R,
    context: &C,
    aliased_table: &Table,
) -> Predicate {
    if rng.random_bool(0.5) {
        Predicate::true_()
    } else {
        Predicate::arbitrary_from(rng, context, aliased_table)
    }
}

fn with_alias(table: &Table, alias: &str) -> Table {
    Table {
        name: alias.to_string(),
        columns: table.columns.clone(),
        rows: table.rows.clone(),
        indexes: table.indexes.clone(),
    }
}

fn local_columns(aliased_table: &Table) -> Vec<LocalColumn> {
    table_columns(aliased_table)
        .into_iter()
        .zip(&aliased_table.columns)
        .map(|(visible, column)| LocalColumn {
            visible,
            name: column.name.clone(),
            column_type: column.column_type,
            leads_an_index: aliased_table.indexes.iter().any(|index| {
                index
                    .columns
                    .first()
                    .is_some_and(|(name, _)| *name == column.name)
            }),
        })
        .collect()
}

fn table_columns(aliased_table: &Table) -> Vec<VisibleColumn> {
    aliased_table
        .columns
        .iter()
        .enumerate()
        .map(|(position, column)| VisibleColumn {
            source: ColumnRef::Table {
                alias: aliased_table.name.clone(),
                column: column.name.clone(),
            },
            origin: ColumnOrigin::Table(exact_json_value(aliased_table, position)),
        })
        .collect()
}

fn exact_json_value(table: &Table, position: usize) -> Option<ExactJsonValue> {
    let column = &table.columns[position];
    if column.is_generated() {
        return None;
    }
    let mut values = table.rows.iter().map(|row| &row[position].0);
    match column.column_type {
        ColumnType::Integer
            if values.all(|value| {
                matches!(value, Value::Null | Value::Numeric(Numeric::Integer(_)))
            }) =>
        {
            Some(ExactJsonValue::Integer)
        }
        ColumnType::Text if values.all(|value| matches!(value, Value::Null | Value::Text(_))) => {
            Some(ExactJsonValue::Text)
        }
        _ => None,
    }
}
