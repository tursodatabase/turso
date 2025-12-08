use std::path::Path;

use turso_parser::{ast::Cmd, parser::Parser as SqlParser};

use crate::{
    generation::plan::{InteractionPlan, Interactions, InteractionsType},
    model::Query,
};

pub fn parse_plan_file(plan_path: &Path) -> InteractionPlan {
    let input = std::fs::read_to_string(plan_path).unwrap();

    // Fallback simple line-based extraction using SQL parser until full pest mapping is wired
    let mut plan = InteractionPlan::new(false);
    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
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
            todo!()
            // let query = Query::from(stmt);
            // plan.push(Interactions::new(
            //     conn_index,
            //     InteractionsType::Query(query),
            // ));
        }
    }
    plan
}
