//! Row-level security policies, PostgreSQL's `pg_policy` rows. The relation
//! catalog stores the policies of a table in the `relpolicies` column of the
//! table's row, so they follow the table when it is renamed and go away when
//! it is dropped.
//!
//! The stored form is a JSON array with one object per policy, whose keys
//! use `pg_policy`'s column names:
//!
//! | key             | value                                                  |
//! |-----------------|--------------------------------------------------------|
//! | `polname`       | policy name                                            |
//! | `polcmd`        | `*` for ALL, `r` SELECT, `a` INSERT, `w` UPDATE, `d` DELETE |
//! | `polpermissive` | `true` for `AS PERMISSIVE`, `false` for `AS RESTRICTIVE` |
//! | `polroles`      | role ids, `-1` for `PUBLIC`                            |
//! | `polqual`       | the `USING` condition as engine SQL, or null           |
//! | `polwithcheck`  | the `WITH CHECK` condition as engine SQL, or null      |
//!
//! The conditions are what the engine compiled `CREATE POLICY` into, written
//! out as SQL and read back with the engine's parser.

use serde::{Deserialize, Serialize};
use turso_core::security::policy::RowPolicy;
use turso_core::security::roles::RoleId;
use turso_parser::ast;
use turso_parser::parser::Parser;

#[derive(Serialize, Deserialize)]
struct StoredPolicy {
    polname: String,
    polcmd: String,
    polpermissive: bool,
    polroles: Vec<i64>,
    polqual: Option<String>,
    polwithcheck: Option<String>,
}

/// Reads the stored form, which [`stored_text`] writes. None when it is not
/// valid.
pub(crate) fn parse(text: &str) -> Option<Vec<RowPolicy>> {
    let stored: Vec<StoredPolicy> = serde_json::from_str(text).ok()?;
    stored
        .into_iter()
        .map(|policy| {
            Some(RowPolicy {
                name: policy.polname,
                permissive: policy.polpermissive,
                command: command_of_letter(&policy.polcmd)?,
                roles: policy
                    .polroles
                    .into_iter()
                    .map(role_of_stored)
                    .collect::<Option<_>>()?,
                using_expr: parse_optional_condition(policy.polqual.as_deref())?,
                check_expr: parse_optional_condition(policy.polwithcheck.as_deref())?,
            })
        })
        .collect()
}

/// The stored form of `policies`.
pub(crate) fn stored_text(policies: &[RowPolicy]) -> String {
    let stored: Vec<StoredPolicy> = policies
        .iter()
        .map(|policy| StoredPolicy {
            polname: policy.name.clone(),
            polcmd: command_letter(policy.command).to_string(),
            polpermissive: policy.permissive,
            polroles: policy.roles.iter().map(|role| role.get()).collect(),
            polqual: policy.using_expr.as_ref().map(ToString::to_string),
            polwithcheck: policy.check_expr.as_ref().map(ToString::to_string),
        })
        .collect();
    serde_json::to_string(&stored).expect("a policy list serializes")
}

/// `pg_policy.polcmd`.
pub(crate) fn command_letter(command: ast::PolicyCommand) -> char {
    match command {
        ast::PolicyCommand::All => '*',
        ast::PolicyCommand::Select => 'r',
        ast::PolicyCommand::Insert => 'a',
        ast::PolicyCommand::Update => 'w',
        ast::PolicyCommand::Delete => 'd',
    }
}

fn command_of_letter(letter: &str) -> Option<ast::PolicyCommand> {
    Some(match letter {
        "*" => ast::PolicyCommand::All,
        "r" => ast::PolicyCommand::Select,
        "a" => ast::PolicyCommand::Insert,
        "w" => ast::PolicyCommand::Update,
        "d" => ast::PolicyCommand::Delete,
        _ => return None,
    })
}

fn role_of_stored(id: i64) -> Option<RoleId> {
    match id {
        id if id == RoleId::PUBLIC.get() => Some(RoleId::PUBLIC),
        id if id >= 0 => Some(RoleId::from_stored(id)),
        _ => None,
    }
}

/// Reads back a condition that may be missing. The outer None means it is
/// not valid.
fn parse_optional_condition(sql: Option<&str>) -> Option<Option<ast::Expr>> {
    match sql {
        None => Some(None),
        Some(sql) => parse_condition(sql).map(Some),
    }
}

/// Reads back a condition that [`stored_text`] wrote.
pub(crate) fn parse_condition(sql: &str) -> Option<ast::Expr> {
    let select_sql = format!("SELECT {sql}");
    let mut parser = Parser::new(select_sql.as_bytes());
    let Ok(Some(ast::Cmd::Stmt(ast::Stmt::Select(select)))) = parser.next_cmd() else {
        return None;
    };
    if !matches!(parser.next_cmd(), Ok(None)) {
        return None;
    }
    let ast::OneSelect::Select {
        mut columns,
        from: None,
        where_clause: None,
        group_by: None,
        ..
    } = select.body.select
    else {
        return None;
    };
    if columns.len() != 1 || !select.body.compounds.is_empty() {
        return None;
    }
    match columns.remove(0) {
        ast::ResultColumn::Expr(expr, None | Some(ast::As::ImplicitColumnName(_))) => Some(*expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(using_sql: &str) -> RowPolicy {
        RowPolicy {
            name: "p".to_string(),
            permissive: false,
            command: ast::PolicyCommand::Update,
            roles: vec![RoleId::PUBLIC, RoleId::SUPERUSER, RoleId::from_stored(3)],
            using_expr: Some(parse_condition(using_sql).unwrap()),
            check_expr: None,
        }
    }

    #[test]
    fn policies_read_back_as_written() {
        let policies = vec![
            policy("\"owner\" = current_user()"),
            policy("\"true\" AND \"x\" > 1 OR \"x\" IN (1, 2)"),
        ];

        let read_back = parse(&stored_text(&policies)).unwrap();

        assert_eq!(read_back, policies);
    }

    #[test]
    fn invalid_stored_policies_are_rejected() {
        for text in [
            "not json",
            r#"[{"polname":"p","polcmd":"x","polpermissive":true,"polroles":[],"polqual":null,"polwithcheck":null}]"#,
            r#"[{"polname":"p","polcmd":"r","polpermissive":true,"polroles":[-2],"polqual":null,"polwithcheck":null}]"#,
            r#"[{"polname":"p","polcmd":"r","polpermissive":true,"polroles":[],"polqual":"1 +","polwithcheck":null}]"#,
            r#"[{"polname":"p","polcmd":"r","polpermissive":true,"polroles":[],"polqual":"1 FROM t","polwithcheck":null}]"#,
            r#"[{"polname":"p","polcmd":"r","polpermissive":true,"polroles":[],"polqual":"1; DELETE FROM t","polwithcheck":null}]"#,
        ] {
            assert!(parse(text).is_none(), "{text}");
        }
    }
}
