mod access_control;
mod config;
mod data;
pub mod model;
mod schema;

pub use config::{disabled_features, Feature, Profile};
use hegel::{
    generators::{self as gs, Generator},
    TestCase,
};

use config::Config;
use model::{Statement, Table, Workload};

pub fn workload(profile: Profile, disabled: Vec<Feature>) -> impl Generator<String> {
    cases(profile, disabled).map(|case| case.to_string())
}

#[hegel::composite]
pub fn cases(tc: TestCase, profile: Profile, disabled: Vec<Feature>) -> Workload {
    let config = Config {
        profile,
        disabled: disabled.clone(),
    };
    let mut statements = vec![Statement::StandardConformingStrings];
    let tables = tc.draw(gs::integers::<usize>().max_value(4));
    for index in 0..tables {
        let generated = schema::table(index, config.clone())
            .flat_map(|table| table_statements(table, config.clone()));
        statements.extend(tc.draw(generated));
    }
    Workload(statements)
}

#[hegel::composite]
fn table_statements(tc: TestCase, table: Table, config: Config) -> Vec<Statement> {
    let mut statements = vec![Statement::CreateTable(table.clone())];
    statements.extend(tc.draw(data::inserts(table.clone())));
    statements.extend(tc.draw(schema::indexes(table.clone(), config.clone())));
    statements.extend(tc.draw(access_control::access_control(
        table.clone(),
        config.clone(),
    )));
    statements
}

#[cfg(test)]
mod tests;
