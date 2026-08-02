use crate::error::{SQLITE_CONSTRAINT, SQLITE_CONSTRAINT_TRIGGER, SQLITE_ERROR};
use crate::turso_assert;
use tracing::{Level, instrument};
use turso_parser::ast::{self, Expr, ResolveType, SubqueryType, TableInternalId, UnaryOperator};

use super::collate::{CollationSeq, get_collseq_from_expr_with_symbols};
use super::emitter::Resolver;
use super::plan::TableReferences;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use crate::function::FtsFunc;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{AggFunc, Func, FuncCtx, MathFuncArity, ScalarFunc, VectorFunc};
use crate::functions::datetime;
use crate::schema::{
    BTreeTable, ColDef, Column, ColumnLayout, GeneratedType, Table, Type, TypeDef,
};
use crate::sync::Arc;
use crate::translate::plan::{ColumnMask, Operation, ResultSetColumn, Search};
use crate::util::{exprs_are_equivalent, normalize_ident, parse_numeric_literal};
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{CursorKey, DmlColumnContext, SelfTableContext};
use crate::vdbe::{
    BranchOffset, CursorID,
    builder::ProgramBuilder,
    insn::{CmpInsFlags, InsertFlags, Insn},
};
use crate::{LimboError, Numeric, Result, Value};

#[macro_use]
mod metadata;

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

pub(crate) use affinity::{
    ExprAffinityInfo, StorageClassMask, compare_affinity, expr_data_type, get_expr_affinity_info,
};
pub use affinity::{comparison_affinity, get_expr_affinity};
pub(crate) use arrays::{
    emit_array_decode, emit_custom_type_decode_columns, emit_custom_type_encode_columns,
};
pub(crate) use binary::expr_is_array;
pub use columns::{emit_table_column, emit_table_column_for_dml};
pub use condition::translate_condition_expr;
pub(crate) use custom_types::{
    emit_dml_expr_index_value, emit_trigger_decode_registers, emit_type_expr,
    emit_user_facing_column_value,
};
pub use emission::{emit_function_call, emit_literal};
pub use metadata::ConditionMetadata;
pub(crate) use metadata::{normalize_expr_for_index_matching, single_table_column_usage};
pub use translator::{
    NoConstantOptReason, resolve_expr, translate_expr, translate_expr_no_constant_opt,
};
pub use utils::{
    ConstraintOperator, as_binary_components, maybe_apply_affinity, sanitize_string, unwrap_parens,
    unwrap_parens_owned,
};
pub use vectors::expr_vector_size;
pub use walk::{
    WalkControl, expr_contains_nondeterministic_scalar_function, expr_references_any_subquery,
    expr_references_subquery_id, walk_expr, walk_expr_mut,
};
