//! Expression helpers shared by the HIR lowering and the planner: AST
//! walking, affinity rules, literal emission, and conditional jumps.

mod affinity;
mod emission;
pub(crate) mod functions;
mod metadata;
mod utils;
mod walk;

pub(crate) use affinity::StorageClassMask;
pub use emission::emit_literal;
pub(crate) use metadata::assert_register_range_allocated as assert_vector_register_range_allocated;
pub(crate) use metadata::emit_cond_jump;
pub use metadata::ConditionMetadata;
pub use utils::{maybe_apply_affinity, sanitize_string, unwrap_parens};
pub use walk::{walk_expr, walk_expr_mut, WalkControl};
