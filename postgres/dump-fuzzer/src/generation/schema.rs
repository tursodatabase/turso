use super::{model::*, Config, Feature};
use hegel::{generators as gs, TestCase};

#[hegel::composite]
pub(super) fn table(tc: TestCase, index: usize, config: Config) -> Table {
    let count = tc.draw(gs::integers::<usize>().min_value(1).max_value(8));
    let primary = tc.draw(gs::optional(gs::integers::<usize>().max_value(count - 1)));
    let columns = (0..count)
        .map(|position| tc.draw(column(position, primary == Some(position), config.clone())))
        .collect();
    Table {
        name: Identifier::numbered(
            tc.draw(gs::sampled_from(
                if config.enabled(Feature::MixedCaseIdentifiers) {
                    &["table", "Table", "select", "data \"", "δ"][..]
                } else {
                    &["table", "select", "data \"", "δ"][..]
                },
            )),
            index,
        ),
        index,
        columns,
    }
}

#[hegel::composite]
fn column(tc: TestCase, index: usize, primary: bool, config: Config) -> Column {
    let data_type = tc.draw(gs::sampled_from(&[
        DataType::Integer,
        DataType::BigInt,
        DataType::Text,
        DataType::Boolean,
    ]));
    let mut constraints = Vec::new();
    if primary {
        constraints.push(Constraint::PrimaryKey);
    } else if tc.draw(gs::booleans()) {
        constraints.push(Constraint::NotNull);
    }
    if config.enabled(Feature::Defaults)
        && matches!(data_type, DataType::Integer | DataType::BigInt)
        && tc.draw(gs::booleans())
    {
        constraints.push(Constraint::Default(tc.draw(gs::integers::<i32>())));
    }
    Column {
        name: Identifier::numbered(
            tc.draw(gs::sampled_from(
                if config.enabled(Feature::MixedCaseIdentifiers) {
                    &["column", "select", "Mixed Case", "quote\"", "λ"][..]
                } else {
                    &["column", "select", "quote\"", "λ"][..]
                },
            )),
            index,
        ),
        data_type,
        constraints,
    }
}

#[hegel::composite]
pub(super) fn indexes(tc: TestCase, table: Table, config: Config) -> Vec<Statement> {
    if !config.enabled(Feature::Indexes) {
        return vec![];
    }
    let count = tc.draw(gs::integers::<usize>().max_value(3));
    (0..count)
        .map(|index| tc.draw(index_definition(table.clone(), index, config.clone())))
        .collect()
}

#[hegel::composite]
fn index_definition(tc: TestCase, table: Table, index: usize, config: Config) -> Statement {
    let max_columns = if config.enabled(Feature::CompositeIndexes) {
        table.columns.len().min(3)
    } else {
        1
    };
    let count = tc.draw(gs::integers::<usize>().min_value(1).max_value(max_columns));
    let mut available = table.columns.clone();
    let mut keys = Vec::new();
    let mut includes_primary = false;
    for _ in 0..count {
        let position = tc.draw(gs::integers::<usize>().max_value(available.len() - 1));
        let column = available.remove(position);
        includes_primary |= column.is_primary_key();
        keys.push(IndexKey {
            column: column.name,
            direction: if config.enabled(Feature::DescendingIndexes) && tc.draw(gs::booleans()) {
                SortDirection::Descending
            } else {
                SortDirection::Ascending
            },
        });
    }
    Statement::CreateIndex {
        name: Identifier::numbered("index", table.index * 3 + index),
        table: table.name.clone(),
        keys,
        unique: includes_primary
            && config.enabled(Feature::UniqueIndexes)
            && tc.draw(gs::booleans()),
    }
}
