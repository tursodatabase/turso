//! Parser-expression syntax utilities.
//!
//! SQL binding and bytecode emission use semantic HIR. This module only keeps
//! syntax walking/display helpers plus literal bytecode shared with HIR emission.

mod emission;
mod utils;
mod walk;

pub use emission::emit_literal;
pub use utils::{sanitize_string, unwrap_parens};
pub use walk::{walk_expr, walk_expr_mut, WalkControl};
