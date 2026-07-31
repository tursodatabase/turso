use crate::error::{SQLITE_CONSTRAINT_TRIGGER, SQLITE_ERROR};
use crate::translate::optimizer::constraints::ConstraintOperator;
use crate::turso_assert;
use tracing::{instrument, Level};
use turso_parser::ast::{self, Expr, ResolveType, UnaryOperator};

use super::collate::CollationSeq;
use super::emitter::Resolver;
use super::optimizer::Optimizable;
use super::plan::TableReferences;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use crate::function::FtsFunc;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{AggFunc, Func, FuncCtx, MathFuncArity, ScalarFunc, VectorFunc};
use crate::functions::datetime;
use crate::schema::{ColDef, Column, ColumnLayout, Type};
use crate::translate::expression_index::single_table_column_usage;
use crate::translate::plan::{ColumnMask, ResultSetColumn};
use crate::util::{exprs_are_equivalent, normalize_ident, parse_numeric_literal};
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{CursorKey, DmlColumnContext};
use crate::vdbe::{
    builder::ProgramBuilder,
    insn::{CmpInsFlags, InsertFlags, Insn},
    BranchOffset, CursorID,
};
use crate::{LimboError, Numeric, Result, Value};

#[macro_use]
mod metadata;
mod plan;

mod affinity;
mod arrays;
mod binary;
mod columns;
mod condition;
mod custom_types;
mod emission;
pub(crate) mod functions;
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

pub use affinity::{comparison_affinity, get_expr_affinity};
pub(crate) use arrays::{
    emit_plan_source_decode_columns, emit_plan_source_decode_columns_for_reencode,
    emit_plan_source_encode_columns,
};
pub use columns::{emit_table_column, emit_table_column_for_dml};
pub(crate) use custom_types::emit_user_facing_column_value_from_schema;
pub use emission::{emit_function_call, emit_literal, ReturningBufferCtx};
pub(crate) use emission::{
    emit_returning_results, emit_returning_scan_back, restore_returning_row_image_in_cache,
    seed_returning_row_image_in_cache,
};
pub use metadata::ConditionMetadata;
pub(crate) use plan::{
    emit_plan_column_value_decode, emit_plan_column_value_decode_for_reencode,
    emit_plan_column_value_encode, emit_plan_result_array_decode, emit_schema_domain_constraints,
    emit_schema_type_transform,
};
pub use plan::{
    translate_plan_condition_expr, translate_plan_expr, translate_plan_expr_no_constant_opt,
};
pub use translator::{translate_expr, NoConstantOptReason};
pub use utils::{sanitize_string, unwrap_parens};
pub use vectors::expr_vector_size;
pub use walk::{walk_expr, walk_expr_mut, WalkControl};
