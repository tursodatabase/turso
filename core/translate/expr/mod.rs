//! Expression helpers shared by the HIR lowering and the planner: AST
//! walking, affinity rules, literal emission, and conditional jumps.

mod affinity;
mod arrays;
mod emission;
pub(crate) mod functions;
mod metadata;
mod utils;
mod vectors;
mod walk;

pub(crate) use affinity::{
    comparison_affinity, expr_data_type, get_expr_affinity, StorageClassMask,
};
pub(crate) use arrays::expr_is_array;
pub use emission::emit_literal;
pub(crate) use metadata::assert_register_range_allocated as assert_vector_register_range_allocated;
pub(crate) use metadata::emit_cond_jump;
pub use metadata::ConditionMetadata;
pub use utils::{
    as_binary_components, maybe_apply_affinity, sanitize_string, truth_test_rhs, unwrap_parens,
};
pub use vectors::expr_vector_size;
pub use walk::{expr_references_any_subquery, walk_expr, walk_expr_mut, WalkControl};
