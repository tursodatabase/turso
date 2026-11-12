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
//! | `oid`           | the policy's oid                                       |
//! | `polname`       | policy name                                            |
//! | `polcmd`        | `*` for ALL, `r` SELECT, `a` INSERT, `w` UPDATE, `d` DELETE |
//! | `polpermissive` | `true` for `AS PERMISSIVE`, `false` for `AS RESTRICTIVE` |
//! | `polroles`      | role ids, `-1` for `PUBLIC`                            |
//! | `polqual`       | the `USING` condition as engine SQL, or null           |
//! | `polwithcheck`  | the `WITH CHECK` condition as engine SQL, or null      |
//!
//! The conditions are what the engine compiled `CREATE POLICY` into, written
//! out as SQL and read back with the engine's parser.
//!
//! Policies take their oids from the same counter as relations, so an oid
//! is never used twice in one database.

use serde::{Deserialize, Serialize};
use turso_core::security::policy::RowPolicy;
use turso_core::security::roles::RoleId;
use turso_parser::ast;
use turso_parser::parser::Parser;

#[derive(Serialize, Deserialize)]
struct StoredPolicy {
    oid: i64,
    polname: String,
    polcmd: String,
    polpermissive: bool,
    polroles: Vec<i64>,
    polqual: Option<String>,
    polwithcheck: Option<String>,
}

/// The row-level security policies of a table, in the order they were
/// created, with their oids.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TablePolicies {
    policies: Vec<RowPolicy>,
    /// The oid of each policy of `policies`.
    oids: Vec<i64>,
}

impl TablePolicies {
    pub(crate) fn as_slice(&self) -> &[RowPolicy] {
        &self.policies
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// The policies with their oids.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (i64, &RowPolicy)> {
        self.oids.iter().copied().zip(&self.policies)
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.policies.iter().any(|policy| policy.name == name)
    }

    /// The policies without the one named `name`.
    pub(crate) fn without(&self, name: &str) -> Self {
        let (oids, policies) = self
            .iter()
            .filter(|(_, policy)| policy.name != name)
            .map(|(oid, policy)| (oid, policy.clone()))
            .unzip();
        Self { policies, oids }
    }

    /// The stored form of the policies.
    pub(crate) fn stored_text(&self) -> String {
        let stored: Vec<StoredPolicy> = self
            .iter()
            .map(|(oid, policy)| stored_policy(oid, policy))
            .collect();
        serde_json::to_string(&stored).expect("a policy list serializes")
    }

    /// The stored form of the policies and `added` after them, split where
    /// the oid of `added` goes: the statement that adds it only learns its
    /// oid when it runs.
    pub(crate) fn stored_text_around_new_oid(&self, added: RowPolicy) -> (String, String) {
        // No other policy has oid 0, and a quote inside a JSON string is
        // escaped, so the marker only matches where the added policy starts.
        const MARKER: &str = r#"{"oid":0,"#;
        let mut with_added = self.clone();
        with_added.policies.push(added);
        with_added.oids.push(0);
        let text = with_added.stored_text();
        let added_at = text.rfind(MARKER).expect("the added policy is stored last");
        let oid_at = added_at + r#"{"oid":"#.len();
        (
            text[..oid_at].to_string(),
            text[oid_at + "0".len()..].to_string(),
        )
    }
}

/// Reads the stored form, which [`TablePolicies::stored_text`] writes. None
/// when it is not valid.
pub(crate) fn parse(text: &str) -> Option<TablePolicies> {
    let stored: Vec<StoredPolicy> = serde_json::from_str(text).ok()?;
    let mut policies = TablePolicies::default();
    for policy in stored {
        if policy.oid <= 0 {
            return None;
        }
        policies.oids.push(policy.oid);
        policies.policies.push(RowPolicy {
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
        });
    }
    Some(policies)
}

fn stored_policy(oid: i64, policy: &RowPolicy) -> StoredPolicy {
    StoredPolicy {
        oid,
        polname: policy.name.clone(),
        polcmd: command_letter(policy.command).to_string(),
        polpermissive: policy.permissive,
        polroles: policy.roles.iter().map(|role| role.get()).collect(),
        polqual: policy.using_expr.as_ref().map(ToString::to_string),
        polwithcheck: policy.check_expr.as_ref().map(ToString::to_string),
    }
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

    fn table_policies(using_sqls: &[&str]) -> TablePolicies {
        let mut next_oid = 16384;
        TablePolicies {
            policies: using_sqls.iter().map(|sql| policy(sql)).collect(),
            oids: using_sqls
                .iter()
                .map(|_| {
                    next_oid += 1;
                    next_oid
                })
                .collect(),
        }
    }

    #[test]
    fn policies_read_back_as_written() {
        let policies = table_policies(&[
            "\"owner\" = current_user()",
            "\"true\" AND \"x\" > 1 OR \"x\" IN (1, 2)",
        ]);

        let read_back = parse(&policies.stored_text()).unwrap();

        assert_eq!(read_back, policies);
    }

    #[test]
    fn added_policy_reads_back_with_the_oid_put_between_the_parts() {
        let policies = table_policies(&["\"x\" = '{\"oid\":0,'"]);
        let added = policy("\"x\" = '{\"oid\":0,'");

        let (before, after) = policies.stored_text_around_new_oid(added.clone());
        let read_back = parse(&format!("{before}20000{after}")).unwrap();

        let mut expected = policies;
        expected.policies.push(added);
        expected.oids.push(20000);
        assert_eq!(read_back, expected);
    }

    #[test]
    fn invalid_stored_policies_are_rejected() {
        for text in [
            "not json",
            r#"[{"oid":1,"polname":"p","polcmd":"x","polpermissive":true,"polroles":[],"polqual":null,"polwithcheck":null}]"#,
            r#"[{"oid":1,"polname":"p","polcmd":"r","polpermissive":true,"polroles":[-2],"polqual":null,"polwithcheck":null}]"#,
            r#"[{"oid":1,"polname":"p","polcmd":"r","polpermissive":true,"polroles":[],"polqual":"1 +","polwithcheck":null}]"#,
            r#"[{"oid":1,"polname":"p","polcmd":"r","polpermissive":true,"polroles":[],"polqual":"1 FROM t","polwithcheck":null}]"#,
            r#"[{"oid":1,"polname":"p","polcmd":"r","polpermissive":true,"polroles":[],"polqual":"1; DELETE FROM t","polwithcheck":null}]"#,
            r#"[{"oid":0,"polname":"p","polcmd":"r","polpermissive":true,"polroles":[],"polqual":null,"polwithcheck":null}]"#,
            r#"[{"polname":"p","polcmd":"r","polpermissive":true,"polroles":[],"polqual":null,"polwithcheck":null}]"#,
        ] {
            assert!(parse(text).is_none(), "{text}");
        }
    }
}
