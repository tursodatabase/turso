pub mod ast;
pub mod error;
pub mod identifier;
pub mod lexer;
pub mod parser;
pub mod token;

type Result<T, E = error::Error> = std::result::Result<T, E>;
