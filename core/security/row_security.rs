//! Row-level security: which tables hide rows from roles, and the policies
//! that show some of them.
//!
//! The catalog is stored in the `__turso_internal_row_security` table, one
//! row per object. A `row_security` row marks a table whose rows are hidden
//! from roles; its other columns are empty. A `policy` row keeps everything
//! `CREATE POLICY` says:
//!
//! | column       | value                                                        |
//! |--------------|--------------------------------------------------------------|
//! | `kind`       | `policy`                                                     |
//! | `name`       | policy name                                                  |
//! | `tbl_name`   | table                                                        |
//! | `permissive` | `1` for `AS PERMISSIVE`, `0` for `AS RESTRICTIVE`            |
//! | `command`    | `all`, `select`, `insert`, `update` or `delete`              |
//! | `roles`      | SQL string literals such as `'anon', 'authenticated'`; empty for `PUBLIC` |
//! | `using_expr` | the `USING` expression as SQL, or empty                      |
//! | `check_expr` | the `WITH CHECK` expression as SQL, or empty                 |
//!
//! `roles`, `using_expr` and `check_expr` are read back with the SQL parser.
//!
//! At schema load every row is turned into a [RowSecurityChange] and applied
//! to an empty [RowSecurityCatalog]. DDL statements write the row and then
//! apply the same change to the in-memory catalog, so both paths share
//! [RowSecurityCatalog::apply]. `CREATE POLICY` also builds its [Policy] from
//! the row it writes, so a policy is the same whether it was just created or
//! loaded.
//!
//! Row-level security is only supported for tables of the main database.
//! Tables are identified by the name they are stored under, which includes
//! their schema.

use std::collections::{HashMap, HashSet};

use turso_parser::ast;
use turso_parser::parser::Parser;

use crate::{util::normalize_ident, LimboError, Result};

pub const ROW_SECURITY_TABLE_NAME: &str = "__turso_internal_row_security";

pub const CREATE_ROW_SECURITY_TABLE_SQL: &str = "CREATE TABLE __turso_internal_row_security(kind TEXT, name TEXT, tbl_name TEXT, permissive INTEGER, command TEXT, roles TEXT, using_expr TEXT, check_expr TEXT)";

pub(crate) const SELECT_ROW_SECURITY_SQL: &str = "SELECT kind, name, tbl_name, permissive, command, roles, using_expr, check_expr FROM __turso_internal_row_security";

#[derive(Debug, Clone, Default)]
pub struct RowSecurityCatalog {
    tables_with_row_security: HashSet<String>,
    policies: HashMap<String, Vec<Policy>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub name: String,
    /// Permissive policies combine with OR, restrictive ones with AND.
    pub permissive: bool,
    pub command: ast::PolicyCommand,
    /// The roles the policy applies to. Empty means every role (`PUBLIC`).
    pub roles: Vec<String>,
    /// Which existing rows the policy shows. A scalar subquery without FROM,
    /// such as `(SELECT auth.uid())`, is replaced by its expression.
    pub using_expr: Option<ast::Expr>,
    /// Which new rows the policy lets a role write.
    pub check_expr: Option<ast::Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowSecurityChange {
    SetRowSecurity { table: String, enabled: bool },
    CreatePolicy { table: String, policy: Box<Policy> },
    DropPolicy { table: String, name: String },
    DropTable(String),
}

/// One row of `__turso_internal_row_security`.
#[derive(Debug, Clone, Default)]
pub struct RowSecurityRow {
    pub kind: String,
    pub name: String,
    pub tbl_name: String,
    pub permissive: bool,
    pub command: String,
    pub roles: String,
    pub using_expr: String,
    pub check_expr: String,
}

impl RowSecurityRow {
    pub(crate) fn from_row(row: &crate::Row) -> Result<Self> {
        Ok(Self {
            kind: row.get::<&str>(0)?.to_string(),
            name: row.get::<&str>(1)?.to_string(),
            tbl_name: row.get::<&str>(2)?.to_string(),
            permissive: row.get::<i64>(3)? != 0,
            command: row.get::<&str>(4)?.to_string(),
            roles: row.get::<&str>(5)?.to_string(),
            using_expr: row.get::<&str>(6)?.to_string(),
            check_expr: row.get::<&str>(7)?.to_string(),
        })
    }

    /// The row that marks `table` as hiding its rows from roles.
    pub fn for_row_security(table: &str) -> Self {
        Self {
            kind: "row_security".to_string(),
            tbl_name: table.to_string(),
            ..Default::default()
        }
    }

    /// The row that stores a policy of `table`.
    pub fn for_policy(table: &str, policy: &ast::CreatePolicy) -> Self {
        let roles = policy
            .roles
            .iter()
            .map(|role| format!("'{}'", role.as_str().replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        Self {
            kind: "policy".to_string(),
            name: normalize_ident(policy.policy_name.as_str()),
            tbl_name: table.to_string(),
            permissive: !policy.restrictive,
            command: command_name(policy.command).to_string(),
            roles,
            using_expr: policy
                .using_expr
                .as_ref()
                .map_or_else(String::new, |expr| expr.to_string()),
            check_expr: policy
                .check_expr
                .as_ref()
                .map_or_else(String::new, |expr| expr.to_string()),
        }
    }
}

impl RowSecurityCatalog {
    pub fn load(rows: &[RowSecurityRow]) -> Result<Self> {
        let mut catalog = Self::default();
        for row in rows {
            catalog.apply(&RowSecurityChange::from_row(row)?);
        }
        Ok(catalog)
    }

    pub fn apply(&mut self, change: &RowSecurityChange) {
        match change {
            RowSecurityChange::SetRowSecurity { table, enabled } => {
                if *enabled {
                    self.tables_with_row_security.insert(table.clone());
                } else {
                    self.tables_with_row_security.remove(table);
                }
            }
            RowSecurityChange::CreatePolicy { table, policy } => {
                self.policies
                    .entry(table.clone())
                    .or_default()
                    .push(policy.as_ref().clone());
            }
            RowSecurityChange::DropPolicy { table, name } => {
                if let Some(policies) = self.policies.get_mut(table) {
                    policies.retain(|policy| &policy.name != name);
                }
            }
            RowSecurityChange::DropTable(table) => {
                self.tables_with_row_security.remove(table);
                self.policies.remove(table);
            }
        }
    }

    pub fn has_row_security(&self, table: &str) -> bool {
        self.tables_with_row_security
            .contains(&normalize_ident(table))
    }

    pub fn policies(&self, table: &str) -> &[Policy] {
        self.policies
            .get(&normalize_ident(table))
            .map_or(&[], Vec::as_slice)
    }
}

impl RowSecurityChange {
    pub fn from_row(row: &RowSecurityRow) -> Result<Self> {
        match row.kind.as_str() {
            "row_security" => Ok(Self::SetRowSecurity {
                table: row.tbl_name.clone(),
                enabled: true,
            }),
            "policy" => Ok(Self::CreatePolicy {
                table: row.tbl_name.clone(),
                policy: Box::new(Policy::from_row(row)?),
            }),
            kind => Err(corrupt(format!(
                "unknown row security object kind \"{kind}\""
            ))),
        }
    }
}

impl Policy {
    fn from_row(row: &RowSecurityRow) -> Result<Self> {
        let command = match row.command.as_str() {
            "all" => ast::PolicyCommand::All,
            "select" => ast::PolicyCommand::Select,
            "insert" => ast::PolicyCommand::Insert,
            "update" => ast::PolicyCommand::Update,
            "delete" => ast::PolicyCommand::Delete,
            command => return Err(corrupt(format!("unknown policy command \"{command}\""))),
        };
        let roles = parse_select_list(&row.roles)?
            .into_iter()
            .map(|role| match role {
                ast::Expr::Literal(ast::Literal::String(quoted)) => {
                    Ok(crate::util::dequote(&quoted))
                }
                role => Err(corrupt(format!("policy role {role} is not a string"))),
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            name: row.name.clone(),
            permissive: row.permissive,
            command,
            roles,
            using_expr: parse_policy_expr(&row.using_expr)?,
            check_expr: parse_policy_expr(&row.check_expr)?,
        })
    }

    pub fn applies_to_role(&self, role: &str) -> bool {
        self.roles.is_empty() || self.roles.iter().any(|name| name == role)
    }

    /// Whether the policy decides which rows a SELECT shows.
    pub fn applies_to_reads(&self) -> bool {
        matches!(
            self.command,
            ast::PolicyCommand::All | ast::PolicyCommand::Select
        )
    }
}

fn command_name(command: ast::PolicyCommand) -> &'static str {
    match command {
        ast::PolicyCommand::All => "all",
        ast::PolicyCommand::Select => "select",
        ast::PolicyCommand::Insert => "insert",
        ast::PolicyCommand::Update => "update",
        ast::PolicyCommand::Delete => "delete",
    }
}

fn parse_policy_expr(sql: &str) -> Result<Option<ast::Expr>> {
    let mut exprs = parse_select_list(sql)?;
    match exprs.len() {
        0 => Ok(None),
        1 => {
            let mut expr = exprs.remove(0);
            replace_scalar_selects(&mut expr);
            Ok(Some(expr))
        }
        _ => Err(corrupt(format!("policy expression {sql} is a list"))),
    }
}

/// The expressions of `SELECT <sql>`, or none for empty `sql`.
fn parse_select_list(sql: &str) -> Result<Vec<ast::Expr>> {
    if sql.is_empty() {
        return Ok(Vec::new());
    }
    let select_sql = format!("SELECT {sql}");
    let mut parser = Parser::new(select_sql.as_bytes());
    let cmd = parser
        .next_cmd()
        .map_err(|error| corrupt(format!("cannot parse {sql}: {error}")))?;
    let Some(ast::Cmd::Stmt(ast::Stmt::Select(select))) = cmd else {
        return Err(corrupt(format!("cannot parse {sql}")));
    };
    let ast::OneSelect::Select {
        columns,
        from: None,
        where_clause: None,
        ..
    } = select.body.select
    else {
        return Err(corrupt(format!("cannot parse {sql}")));
    };
    columns
        .into_iter()
        .map(|column| match column {
            ast::ResultColumn::Expr(expr, _) => Ok(*expr),
            _ => Err(corrupt(format!("cannot parse {sql}"))),
        })
        .collect()
}

/// Replaces each `(SELECT <expr>)` without FROM or other clauses by
/// `(<expr>)`. Supabase recommends writing `(SELECT auth.uid())` in policies
/// so that PostgreSQL evaluates the function once per query.
fn replace_scalar_selects(expr: &mut ast::Expr) {
    crate::translate::expr::walk_expr_mut(expr, &mut |expr: &mut ast::Expr| {
        if let ast::Expr::Subquery(select) = expr {
            if let Some(value) = single_value_of(select) {
                *expr = ast::Expr::Parenthesized(vec![Box::new(value.clone())]);
            }
        }
        Ok(crate::translate::expr::WalkControl::Continue)
    })
    .expect("the walk never fails");
}

/// The expression of `SELECT <expr>` without FROM or any other clause.
fn single_value_of(select: &ast::Select) -> Option<&ast::Expr> {
    if select.with.is_some()
        || !select.body.compounds.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
    {
        return None;
    }
    match &select.body.select {
        ast::OneSelect::Select {
            distinctness: None,
            columns,
            from: None,
            where_clause: None,
            group_by: None,
            window_clause,
        } if window_clause.is_empty() => match columns.as_slice() {
            [ast::ResultColumn::Expr(expr, _)] => Some(expr),
            _ => None,
        },
        _ => None,
    }
}

fn corrupt(message: String) -> LimboError {
    LimboError::Corrupt(format!("{ROW_SECURITY_TABLE_NAME}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy_row(name: &str, roles: &str, using_expr: &str) -> RowSecurityRow {
        RowSecurityRow {
            kind: "policy".to_string(),
            name: name.to_string(),
            tbl_name: "docs".to_string(),
            permissive: true,
            command: "select".to_string(),
            roles: roles.to_string(),
            using_expr: using_expr.to_string(),
            check_expr: String::new(),
        }
    }

    #[test]
    fn load_rejects_unknown_kind() {
        let row = RowSecurityRow {
            kind: "mystery".to_string(),
            ..Default::default()
        };
        assert!(RowSecurityCatalog::load(&[row]).is_err());
    }

    #[test]
    fn load_enables_row_security() {
        let mut catalog =
            RowSecurityCatalog::load(&[RowSecurityRow::for_row_security("docs")]).unwrap();
        assert!(catalog.has_row_security("DOCS"));
        catalog.apply(&RowSecurityChange::DropTable("docs".to_string()));
        assert!(!catalog.has_row_security("docs"));
    }

    #[test]
    fn load_policies_and_drop_them_with_the_table() {
        let mut catalog = RowSecurityCatalog::load(&[
            policy_row("all", "", "true"),
            policy_row("own", "'anon', 'it''s'", "owner = (SELECT current_user())"),
        ])
        .unwrap();
        let own = &catalog.policies("docs")[1];
        assert_eq!(own.roles, ["anon", "it's"]);
        assert!(matches!(
            own.using_expr.as_ref().unwrap(),
            ast::Expr::Binary(_, ast::Operator::Equals, rhs)
                if matches!(rhs.as_ref(), ast::Expr::Parenthesized(_))
        ));
        assert!(catalog.policies("docs")[0].roles.is_empty());
        catalog.apply(&RowSecurityChange::DropTable("docs".to_string()));
        assert!(catalog.policies("docs").is_empty());
    }

    #[test]
    fn load_rejects_corrupt_policies() {
        for row in [
            RowSecurityRow {
                command: "merge".to_string(),
                ..policy_row("p", "", "true")
            },
            policy_row("p", "anon", "true"),
            policy_row("p", "", "true, false"),
            policy_row("p", "", "1 +"),
            policy_row("p", "", "1 FROM t"),
        ] {
            assert!(RowSecurityCatalog::load(&[row]).is_err());
        }
    }
}
