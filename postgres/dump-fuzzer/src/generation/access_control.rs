use super::{model::*, Config, Feature, Profile};
use hegel::{generators as gs, TestCase};

#[hegel::composite]
pub(super) fn access_control(tc: TestCase, table: Table, config: Config) -> Vec<Statement> {
    if config.profile == Profile::Tables {
        return vec![];
    }
    let mut statements = Vec::new();
    if config.enabled(Feature::Rls) {
        statements.push(Statement::RowSecurity {
            table: table.name.clone(),
            action: RlsAction::Enable,
        });
        if config.enabled(Feature::ForceRls) && tc.draw(gs::booleans()) {
            statements.push(Statement::RowSecurity {
                table: table.name.clone(),
                action: RlsAction::Force,
            });
        }
    }
    if config.enabled(Feature::Policies) {
        let policy = tc.draw(policy(table.clone()));
        let has_using = policy.using.is_some();
        statements.push(Statement::CreatePolicy(policy.clone()));
        if config.enabled(Feature::AlterPolicy) && has_using && tc.draw(gs::booleans()) {
            statements.push(Statement::AlterPolicy {
                name: policy.name.clone(),
                table: table.name.clone(),
                using: tc.draw(predicate(table.clone())),
            });
        }
        if config.enabled(Feature::DropPolicy) && tc.draw(gs::booleans()) {
            statements.push(Statement::DropPolicy {
                name: policy.name,
                table: table.name.clone(),
            });
        }
    }
    if config.enabled(Feature::Grants) {
        let command = tc.draw(command());
        statements.push(Statement::Privilege {
            action: PrivilegeAction::Grant,
            command,
            table: table.name.clone(),
        });
        if config.enabled(Feature::Revokes) && tc.draw(gs::booleans()) {
            statements.push(Statement::Privilege {
                action: PrivilegeAction::Revoke,
                command,
                table: table.name.clone(),
            });
        }
    }
    if config.enabled(Feature::DefaultPrivileges) && tc.draw(gs::booleans()) {
        statements.push(Statement::DefaultPrivilege {
            schema: Identifier::new("public"),
            command: tc.draw(command()),
        });
    }
    if config.enabled(Feature::Rls) && tc.draw(gs::booleans()) {
        statements.push(Statement::RowSecurity {
            table: table.name.clone(),
            action: RlsAction::Disable,
        });
    }
    statements
}

#[hegel::composite]
fn policy(tc: TestCase, table: Table) -> Policy {
    let command = tc.draw(command());
    let mode = tc.draw(gs::sampled_from(&[
        PolicyMode::Permissive,
        PolicyMode::Restrictive,
    ]));
    let predicate = tc.draw(predicate(table.clone()));
    Policy {
        name: Identifier::numbered("policy", table.index),
        table: table.name.clone(),
        mode,
        command,
        using: (command != Command::Insert).then(|| predicate.clone()),
        check: matches!(command, Command::All | Command::Insert | Command::Update)
            .then_some(predicate),
    }
}

#[hegel::composite]
fn predicate(tc: TestCase, table: Table) -> Predicate {
    let column = tc.draw(gs::sampled_from(&table.columns));
    match column.data_type {
        DataType::Integer | DataType::BigInt => Predicate::Compare {
            column: column.name,
            comparison: tc.draw(gs::sampled_from(&[
                Comparison::GreaterOrEqual,
                Comparison::NotEqual,
            ])),
            value: tc.draw(gs::integers::<i32>()),
        },
        DataType::Text | DataType::Boolean => Predicate::IsNull {
            column: column.name,
            negated: tc.draw(gs::booleans()),
        },
    }
}

fn command() -> impl gs::Generator<Command> {
    gs::sampled_from(&[
        Command::All,
        Command::Select,
        Command::Insert,
        Command::Update,
        Command::Delete,
    ])
}
