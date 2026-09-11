//! The rule language of the logical plan stage.
//!
//! A rule file holds operator definitions and rules. The syntax is the one
//! of Optgen, the language of the CockroachDB optimizer:
//!
//! ```text
//! [Scalar, Bool]
//! define And {
//!     Left  Scalar
//!     Right Scalar
//! }
//!
//! [SimplifyAndTrue, Normalize]
//! (And $left:* (True)) => $left
//! ```
//!
//! The part before `=>` is the match pattern, the part after it is the
//! replace pattern. `$left:*` binds a child to a variable. A name that is not
//! an operator, such as `(IsConst $left)`, calls a function written in Rust.
//! `compile` parses and checks the files. The rule engine in
//! `super::rules::engine` runs the compiled rules.

pub(crate) mod compiler;
pub(crate) mod parser;
pub(crate) mod scanner;

pub(crate) use compiler::{compile, Compiled};
pub(crate) use parser::{Expr, ExprKind, FuncName, Rule};
