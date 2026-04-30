use crate::error::{SQLITE_CONSTRAINT, SQLITE_CONSTRAINT_TRIGGER, SQLITE_ERROR};
use crate::translate::optimizer::constraints::ConstraintOperator;
use crate::turso_assert;
use tracing::{instrument, Level};
use turso_parser::ast::{self, Expr, ResolveType, SubqueryType, TableInternalId, UnaryOperator};

use super::collate::{get_collseq_from_expr, get_expr_collation_ctx, CollationSeq};
use super::emitter::{CachedExprCollation, Resolver};
use super::optimizer::Optimizable;
use super::plan::TableReferences;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use crate::function::FtsFunc;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{AggFunc, Func, FuncCtx, MathFuncArity, ScalarFunc, VectorFunc};
use crate::functions::datetime;
use crate::schema::{BTreeTable, Column, ColumnLayout, GeneratedType, Table, Type, TypeDef};
use crate::sync::Arc;
use crate::translate::expression_index::{
    normalize_expr_for_index_matching, single_table_column_usage,
};
use crate::translate::plan::{ColumnMask, Operation, ResultSetColumn, Search};
use crate::translate::planner::parse_row_id;
use crate::util::{exprs_are_equivalent, normalize_ident, parse_numeric_literal};
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{CursorKey, DmlColumnContext, SelfTableContext};
use crate::vdbe::{
    builder::ProgramBuilder,
    insn::{CmpInsFlags, InsertFlags, Insn},
    BranchOffset, CursorID,
};
use crate::{LimboError, Numeric, Result, Value};

#[macro_use]
mod metadata;

mod affinity;
mod arrays;
mod binary;
mod binding;
mod columns;
mod condition;
mod custom_types;
mod emission;
mod functions;
mod translator;
mod utils;
mod vectors;
mod walk;

#[allow(unused_imports)]
use affinity::*;
#[allow(unused_imports)]
use arrays::*;
#[allow(unused_imports)]
use binary::*;
#[allow(unused_imports)]
use binding::*;
#[allow(unused_imports)]
use columns::*;
#[allow(unused_imports)]
use condition::*;
#[allow(unused_imports)]
use custom_types::*;
#[allow(unused_imports)]
use emission::*;
#[allow(unused_imports)]
use functions::*;
#[allow(unused_imports)]
use metadata::*;
#[allow(unused_imports)]
use translator::*;
#[allow(unused_imports)]
use utils::*;
#[allow(unused_imports)]
use vectors::*;
#[allow(unused_imports)]
use walk::*;

pub(crate) use affinity::{compare_affinity, get_expr_affinity_info, ExprAffinityInfo};
pub use affinity::{comparison_affinity, get_expr_affinity};
pub(crate) use arrays::{
    emit_array_decode, emit_custom_type_decode_columns, emit_custom_type_encode_columns,
};
pub(crate) use binary::expr_is_array;
pub use binding::{bind_and_rewrite_expr, BindingBehavior};
pub use columns::{emit_table_column, emit_table_column_for_dml};
pub use condition::translate_condition_expr;
pub(crate) use custom_types::{
    emit_dml_expr_index_value, emit_trigger_decode_registers, emit_type_expr,
    emit_user_facing_column_value,
};
pub use emission::{
    emit_function_call, emit_literal, process_returning_clause, ReturningBufferCtx,
};
pub(crate) use emission::{
    emit_returning_results, emit_returning_scan_back, restore_returning_row_image_in_cache,
    seed_returning_row_image_in_cache,
};
pub use metadata::ConditionMetadata;
pub use translator::{
    resolve_expr, translate_expr, translate_expr_no_constant_opt, NoConstantOptReason,
};
pub use utils::{
    as_binary_components, maybe_apply_affinity, sanitize_string, unwrap_parens, unwrap_parens_owned,
};
pub use vectors::expr_vector_size;
pub use walk::{
    expr_references_any_subquery, expr_references_subquery_id, walk_expr, walk_expr_mut,
    WalkControl,
};
