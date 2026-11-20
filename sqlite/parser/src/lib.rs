pub mod lexer;
pub mod parser;

pub use parser::MAX_EXPR_DEPTH;

type Result<T, E = turso_core::ast::error::Error> = std::result::Result<T, E>;
