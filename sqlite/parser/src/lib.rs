// rustc 1.92 reports spurious `unused_assignments` against `error::Error`
// variant fields when the parser macros construct them; earlier and later
// compilers (including CI's stable clippy) don't. Remove this allow when the
// pinned toolchain moves past 1.92.
#![allow(unused_assignments)]

pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod token;

pub use parser::MAX_EXPR_DEPTH;

type Result<T, E = error::Error> = std::result::Result<T, E>;
