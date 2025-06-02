use limbo_sqlite3_parser::ast;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Predicate(ast::Expr);
