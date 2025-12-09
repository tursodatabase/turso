use std::path::Path;

use sql_generation::model::query::{
    Begin, Commit, Create, CreateIndex, Delete, Drop, Insert, Rollback, Select, Update,
};
use turso_parser::{
    ast::{Cmd, Stmt},
    parser::Parser as SqlParser,
};

use crate::{
    generation::plan::{Interaction, InteractionGroup, InteractionPlan, InteractionType},
    model::Query,
};

impl From<turso_parser::ast::Stmt> for Query {
    fn from(stmt: Stmt) -> Self {
        match stmt {
            Stmt::CreateTable {
                temporary,
                if_not_exists,
                tbl_name,
                body,
            } => Query::Create(Create::from((temporary, if_not_exists, tbl_name, body))),
            Stmt::Select(select) => Query::Select(Select::from(select)),
            Stmt::Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            } => Query::Insert(Insert::from((
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            ))),
            Stmt::Delete {
                with,
                tbl_name,
                indexed,
                where_clause,
                returning,
                order_by,
                limit,
            } => Query::Delete(Delete::from((
                with,
                tbl_name,
                indexed,
                where_clause,
                returning,
                order_by,
                limit,
            ))),
            Stmt::Update(update) => Query::Update(Update::from(update)),
            Stmt::DropTable {
                if_exists: _,
                tbl_name,
            } => Query::Drop(Drop {
                table: tbl_name.to_string(),
            }),
            Stmt::CreateIndex {
                unique,
                if_not_exists,
                idx_name,
                tbl_name,
                columns,
                where_clause,
            } => Query::CreateIndex(CreateIndex::from((
                unique,
                if_not_exists,
                idx_name,
                tbl_name,
                columns,
                where_clause,
            ))),
            Stmt::Begin { typ, name: _ } => Query::Begin(match typ {
                Some(turso_parser::ast::TransactionType::Deferred) | None => Begin::Deferred,
                Some(turso_parser::ast::TransactionType::Immediate) => Begin::Immediate,
                Some(turso_parser::ast::TransactionType::Concurrent) => Begin::Concurrent,
                Some(turso_parser::ast::TransactionType::Exclusive) => {
                    panic!("Exclusive transactions are not supported in BEGIN statements")
                }
            }),
            Stmt::Commit { .. } => Query::Commit(Commit),
            Stmt::Rollback { .. } => Query::Rollback(Rollback),
            _ => {
                panic!("Statement type not supported in the simulator")
            }
        }
    }
}

pub fn parse_plan_file(plan_path: &Path) -> InteractionPlan {
    let input = std::fs::read_to_string(plan_path).unwrap();
    parse_plan(input)
}

pub fn parse_plan(plan_string: String) -> InteractionPlan {
    // Fallback simple line-based extraction using SQL parser until full pest mapping is wired
    let mut plan = InteractionPlan::new(false);
    let mut bind = None;
    let mut ignore_error = false;
    for line in plan_string.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") && !trimmed.starts_with("-- @") {
            continue;
        }

        if trimmed.starts_with("-- @bind") {
            bind = trimmed.strip_prefix("-- @bind");
            continue;
        }
        if trimmed.starts_with("-- @ignore_error") {
            ignore_error = true;
            continue;
        }

        // Allow optional trailing semicolon and inline comment with connection index
        let mut parts = trimmed.splitn(2, "--");
        let sql_part = parts.next().unwrap();
        let sql = sql_part.trim_end_matches(';').trim();
        let conn_index = parts
            .next()
            .and_then(|c| c.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if sql.is_empty() {
            continue;
        }
        let mut p = SqlParser::new(sql.as_bytes());
        if let Some(Ok(Cmd::Stmt(stmt))) = p.next() {
            let query = Query::from(stmt);
            let mut interaction = Interaction::new(conn_index, InteractionType::Query(query));
            if let Some(b) = bind {
                interaction = interaction.bind(b.trim().to_string());
            }
            if ignore_error {
                interaction.ignore_error = true;
            }
            plan.push(InteractionGroup::single(interaction));
        }
    }
    plan
}
