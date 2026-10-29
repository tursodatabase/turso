use super::model::*;
use hegel::{
    generators::{self as gs, Generator},
    TestCase,
};

#[hegel::composite]
pub(super) fn inserts(tc: TestCase, table: Table) -> Vec<Statement> {
    let max_rows = if table
        .columns
        .iter()
        .any(|c| c.is_primary_key() && matches!(c.data_type, DataType::Boolean))
    {
        2
    } else {
        12
    };
    let rows = tc.draw(gs::integers::<usize>().max_value(max_rows));
    let mut statements = (0..rows)
        .map(|index| {
            tc.draw(
                row(table.columns.clone(), index).map(|row| Statement::Insert {
                    table: table.name.clone(),
                    row,
                }),
            )
        })
        .collect::<Vec<_>>();
    if !table.has_primary_key() && !statements.is_empty() && tc.draw(gs::booleans()) {
        statements.push(tc.draw(gs::sampled_from(&statements)));
    }
    statements
}

#[hegel::composite]
pub(super) fn row(tc: TestCase, columns: Vec<Column>, index: usize) -> Row {
    Row(columns
        .iter()
        .map(|column| tc.draw(value(column, index)))
        .collect())
}

pub(super) fn value(column: &Column, row: usize) -> gs::BoxedGenerator<'static, Value> {
    if column.is_primary_key() {
        return gs::just(match column.data_type {
            DataType::Integer | DataType::BigInt => Value::Integer(row as i64),
            DataType::Text => Value::Text(row.to_string()),
            DataType::Boolean => Value::Boolean(row != 0),
        })
        .boxed();
    }
    let scalar = match column.data_type {
        DataType::Integer => gs::integers::<i32>()
            .map(|n| Value::Integer(i64::from(n)))
            .boxed(),
        DataType::BigInt => gs::integers::<i64>().map(Value::Integer).boxed(),
        DataType::Text => gs::text()
            .max_size(64)
            .map(|text| Value::Text(text.replace('\0', "")))
            .boxed(),
        DataType::Boolean => gs::booleans().map(Value::Boolean).boxed(),
    };
    if column.is_nullable() {
        gs::optional(scalar)
            .map(|value| value.unwrap_or(Value::Null))
            .boxed()
    } else {
        scalar
    }
}
