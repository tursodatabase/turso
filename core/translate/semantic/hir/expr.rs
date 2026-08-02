//! Resolved expression trees.

use std::num::NonZeroU32;

use turso_parser::ast::{
    Distinctness, FrameExclude, FrameMode, LikeOperator, Literal, NullsOrder, Operator,
    ResolveType, SortOrder, UnaryOperator,
};

use super::{
    BoundCastPrograms, BoundSchemaCall, DatabaseId, MergedColumnValue, OutputId, QueryBlockId,
    QueryId, ResolvedCollation, ResolvedFunction, ResolvedTable, ResolvedType, SourceId, TypeFact,
};
use crate::schema::Sequence;
use crate::sync::Arc;
use crate::vdbe::affinity::Affinity;

#[derive(Clone, Debug)]
pub struct Parameter {
    pub index: NonZeroU32,
    pub name: Option<String>,
    pub type_fact: TypeFact,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct ColumnRef {
    pub source: SourceId,
    pub column: usize,
}

/// One visible value formed by a resolved USING or NATURAL join column.
#[derive(Clone, Debug)]
pub struct MergedColumn {
    /// The visible value produced by the joins to the left. This may itself
    /// be a merged column when USING/NATURAL joins are chained.
    pub left: Box<Expr>,
    pub right: ColumnRef,
    pub value: MergedColumnValue,
    pub type_fact: TypeFact,
    pub affinity: Affinity,
    pub has_affinity: bool,
    pub collation: Option<ResolvedCollation>,
}

/// A resolved cast target. Parameters are semantic expressions, not parser
/// expressions, because PostgreSQL-style type parameters can be expressions.
#[derive(Clone, Debug)]
pub struct TypeName {
    pub name: String,
    pub parameters: Vec<Expr>,
    pub array_dimensions: u32,
    pub type_fact: TypeFact,
    /// Exact VDBE affinity selected for the CAST operation.
    pub affinity: Affinity,
    pub programs: BoundCastPrograms,
}

#[derive(Clone, Debug)]
pub struct OrderTerm {
    pub expr: Expr,
    pub order: SortOrder,
    pub nulls: Option<NullsOrder>,
    /// Final type facts of `expr` in the scope where this term was bound.
    /// Physical sorting uses these facts without reopening the catalog.
    pub type_fact: TypeFact,
    /// Final SQLite collation after explicit-COLLATE and declared-column
    /// precedence have been applied during semantic analysis.
    pub collation: Option<ResolvedCollation>,
}

#[derive(Clone, Debug)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderTerm>,
    pub frame: Option<WindowFrame>,
}

#[derive(Clone, Debug)]
pub struct WindowFrame {
    pub mode: FrameMode,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
    pub exclude: Option<FrameExclude>,
}

#[derive(Clone, Debug)]
pub enum WindowFrameBound {
    CurrentRow,
    Following(Box<Expr>),
    Preceding(Box<Expr>),
    UnboundedFollowing,
    UnboundedPreceding,
}

/// Pre-resolved behavior for custom-type scalar functions that bypass the
/// generic scalar-function path.
#[derive(Clone, Debug)]
pub enum CustomTypeOperation {
    UnionValue {
        union_type: ResolvedType,
        tag_index: u8,
        result_type: TypeFact,
    },
    UnionTag {
        union_type: ResolvedType,
        tag_names: Arc<[String]>,
    },
    UnionExtract {
        union_type: ResolvedType,
        tag_index: u8,
        result_type: TypeFact,
    },
    StructExtract {
        struct_type: ResolvedType,
        field_index: usize,
        result_type: TypeFact,
    },
}

/// Which original SQL operand of a custom binary operator is a literal that
/// must be encoded before calling the operator function. This is deliberately
/// defined before `swap_args` is applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOperand {
    Left,
    Right,
}

/// The schema program, if any, needed to encode one literal operand into a
/// custom type's stored representation.
#[derive(Clone, Debug)]
pub struct CustomBinaryLiteralEncoding {
    pub operand: BinaryOperand,
    pub encoder: Option<BoundSchemaCall>,
}

/// A custom binary operator chosen during semantic analysis. Later phases
/// invoke this exact function and never search a live schema by operator name.
#[derive(Clone, Debug)]
pub struct CustomBinaryOperator {
    pub function: ResolvedFunction,
    pub swap_args: bool,
    pub negate: bool,
    pub literal_encoding: Option<CustomBinaryLiteralEncoding>,
}

/// Runtime comparison behavior fixed during semantic analysis.
///
/// Row values have one entry per position. Keeping affinity and collation here
/// prevents physical planning from rebuilding SQLite's name/type rules after
/// the semantic scope has been discarded.
#[derive(Clone, Debug, PartialEq)]
pub struct ComparisonSemantics {
    pub components: Vec<ComparisonComponent>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ComparisonComponent {
    pub affinity: Affinity,
    pub collation: Option<ResolvedCollation>,
    pub array: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SequenceOperationKind {
    NextValue,
    SetValue,
}

/// Catalog state needed to compile a sequence function without repeating
/// name or schema lookup after semantic analysis.
#[derive(Clone, Debug)]
pub struct SequenceOperation {
    pub kind: SequenceOperationKind,
    pub database: DatabaseId,
    /// The string supplied by SQL, without its outer quotes. Runtime currval
    /// tracking uses this spelling, including an optional schema prefix.
    pub user_name: String,
    pub normalized_name: String,
    pub backing_table: ResolvedTable,
    /// Present only for the internal sequence behind an AUTOINCREMENT table.
    /// Physical lowering uses this frozen object to keep sqlite_sequence in
    /// sync without reopening the catalog.
    pub sqlite_sequence: Option<ResolvedTable>,
    pub sequence: Arc<Sequence>,
    pub schema_cookie: u32,
}

#[derive(Clone, Debug)]
pub struct FunctionCall {
    pub function: ResolvedFunction,
    /// How this call is evaluated after semantic analysis. Aggregate and
    /// window identities are owned by one query block; physical planning maps
    /// those identities to runtime state without matching expression trees.
    pub evaluation: FunctionEvaluation,
    pub star: bool,
    pub arguments: Vec<Expr>,
    pub distinctness: Option<Distinctness>,
    pub argument_order: Vec<OrderTerm>,
    pub within_group: Vec<OrderTerm>,
    pub filter: Option<Box<Expr>>,
    pub window: Option<WindowSpec>,
    pub result_type: TypeFact,
    pub custom_type_operation: Option<CustomTypeOperation>,
    pub sequence_operation: Option<SequenceOperation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FunctionEvaluation {
    Scalar,
    Aggregate(AggregateId),
    Window(WindowFunctionId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AggregateId {
    pub block: QueryBlockId,
    pub index: usize,
}

impl AggregateId {
    pub const fn new(block: QueryBlockId, index: usize) -> Self {
        Self { block, index }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WindowFunctionId {
    pub block: QueryBlockId,
    pub index: usize,
}

impl WindowFunctionId {
    pub const fn new(block: QueryBlockId, index: usize) -> Self {
        Self { block, index }
    }
}

#[derive(Clone, Debug)]
pub struct FieldAccess {
    pub base: Box<Expr>,
    pub field_name: String,
    pub kind: FieldAccessKind,
    pub container_type: ResolvedType,
    pub result_type: TypeFact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldAccessKind {
    Struct { field_index: usize },
    Union { tag_index: u8 },
}

#[derive(Clone, Debug)]
pub enum SubqueryExpr {
    Scalar {
        query: QueryId,
        output: usize,
    },
    Exists(QueryId),
    In {
        lhs: Box<Expr>,
        query: QueryId,
        negated: bool,
        comparison: ComparisonSemantics,
    },
}

/// An expression whose names and type-dependent operations are fully resolved.
#[derive(Clone, Debug)]
pub enum Expr {
    Literal(Literal),
    Parameter(Parameter),
    Column(ColumnRef),
    MergedColumn(MergedColumn),
    RowId(SourceId),
    Output(OutputId),
    Unary {
        operator: UnaryOperator,
        expr: Box<Expr>,
    },
    Binary {
        lhs: Box<Expr>,
        operator: Operator,
        rhs: Box<Expr>,
        /// `||` uses the array opcode when either operand is an array. This
        /// decision depends on bound type facts and must not be rediscovered
        /// during physical emission.
        array_concat: bool,
        custom: Option<CustomBinaryOperator>,
        comparison: Option<ComparisonSemantics>,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        start: Box<Expr>,
        end: Box<Expr>,
        start_comparison: ComparisonSemantics,
        end_comparison: ComparisonSemantics,
    },
    Case {
        base: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
        /// Present exactly when `base` is present, one comparison per WHEN.
        base_comparisons: Vec<ComparisonSemantics>,
    },
    Cast {
        expr: Box<Expr>,
        target: TypeName,
    },
    Collate {
        expr: Box<Expr>,
        collation: ResolvedCollation,
    },
    Function(FunctionCall),
    IsNull(Box<Expr>),
    NotNull(Box<Expr>),
    InList {
        lhs: Box<Expr>,
        negated: bool,
        values: Vec<Expr>,
        comparisons: Vec<ComparisonSemantics>,
    },
    Subquery(SubqueryExpr),
    Like {
        lhs: Box<Expr>,
        negated: bool,
        operator: LikeOperator,
        function: ResolvedFunction,
        argument_count: usize,
        rhs: Box<Expr>,
        escape: Option<Box<Expr>>,
    },
    Row(Vec<Expr>),
    Array(Vec<Expr>),
    Subscript {
        base: Box<Expr>,
        index: Box<Expr>,
    },
    FieldAccess(FieldAccess),
    Raise {
        action: ResolveType,
        message: Option<Box<Expr>>,
    },
}

impl Expr {
    pub fn column(source: SourceId, column: usize) -> Self {
        Self::Column(ColumnRef { source, column })
    }

    pub fn rowid(source: SourceId) -> Self {
        Self::RowId(source)
    }

    pub fn output(output: OutputId) -> Self {
        Self::Output(output)
    }

    /// Visit this expression and every expression it owns. References to
    /// outputs and subqueries remain references; their definitions are walked
    /// by the query that owns them.
    pub(crate) fn walk<'expr>(&'expr self, visitor: &mut impl FnMut(&'expr Expr)) {
        visitor(self);
        match self {
            Self::Literal(_)
            | Self::Parameter(_)
            | Self::Column(_)
            | Self::RowId(_)
            | Self::Output(_)
            | Self::Subquery(SubqueryExpr::Scalar { .. } | SubqueryExpr::Exists(_)) => {}
            Self::MergedColumn(column) => column.left.walk(visitor),
            Self::Unary { expr, .. }
            | Self::IsNull(expr)
            | Self::NotNull(expr)
            | Self::Collate { expr, .. } => expr.walk(visitor),
            Self::Binary {
                lhs, rhs, custom, ..
            } => {
                lhs.walk(visitor);
                rhs.walk(visitor);
                if let Some(call) = custom
                    .as_ref()
                    .and_then(|custom| custom.literal_encoding.as_ref())
                    .and_then(|encoding| encoding.encoder.as_ref())
                {
                    walk_schema_call(call, visitor);
                }
            }
            Self::Between {
                expr, start, end, ..
            } => {
                expr.walk(visitor);
                start.walk(visitor);
                end.walk(visitor);
            }
            Self::Case {
                base,
                when_then,
                else_expr,
                ..
            } => {
                walk_optional_expr(base.as_deref(), visitor);
                for (when, then) in when_then {
                    when.walk(visitor);
                    then.walk(visitor);
                }
                walk_optional_expr(else_expr.as_deref(), visitor);
            }
            Self::Cast { expr, target } => {
                expr.walk(visitor);
                walk_exprs(&target.parameters, visitor);
                for call in &target.programs.encode {
                    walk_schema_call(call, visitor);
                }
                if let Some(domain) = &target.programs.domain {
                    for check in &domain.checks {
                        walk_schema_call(&check.call, visitor);
                    }
                }
            }
            Self::Function(call) => {
                walk_exprs(&call.arguments, visitor);
                walk_order_terms(&call.argument_order, visitor);
                walk_order_terms(&call.within_group, visitor);
                walk_optional_expr(call.filter.as_deref(), visitor);
                if let Some(window) = &call.window {
                    walk_window_spec(window, visitor);
                }
            }
            Self::InList { lhs, values, .. } => {
                lhs.walk(visitor);
                walk_exprs(values, visitor);
            }
            Self::Subquery(SubqueryExpr::In { lhs, .. }) => lhs.walk(visitor),
            Self::Like {
                lhs, rhs, escape, ..
            } => {
                lhs.walk(visitor);
                rhs.walk(visitor);
                walk_optional_expr(escape.as_deref(), visitor);
            }
            Self::Row(expressions) | Self::Array(expressions) => {
                walk_exprs(expressions, visitor);
            }
            Self::Subscript { base, index } => {
                base.walk(visitor);
                index.walk(visitor);
            }
            Self::FieldAccess(access) => access.base.walk(visitor),
            Self::Raise { message, .. } => walk_optional_expr(message.as_deref(), visitor),
        }
    }
}

fn walk_exprs<'expr>(expressions: &'expr [Expr], visitor: &mut impl FnMut(&'expr Expr)) {
    for expression in expressions {
        expression.walk(visitor);
    }
}

fn walk_optional_expr<'expr>(
    expression: Option<&'expr Expr>,
    visitor: &mut impl FnMut(&'expr Expr),
) {
    if let Some(expression) = expression {
        expression.walk(visitor);
    }
}

fn walk_order_terms<'expr>(terms: &'expr [OrderTerm], visitor: &mut impl FnMut(&'expr Expr)) {
    for term in terms {
        term.expr.walk(visitor);
    }
}

fn walk_schema_call<'expr>(call: &'expr BoundSchemaCall, visitor: &mut impl FnMut(&'expr Expr)) {
    walk_exprs(&call.arguments, visitor);
}

fn walk_window_spec<'expr>(window: &'expr WindowSpec, visitor: &mut impl FnMut(&'expr Expr)) {
    walk_exprs(&window.partition_by, visitor);
    walk_order_terms(&window.order_by, visitor);
    let Some(frame) = &window.frame else {
        return;
    };
    walk_window_bound(&frame.start, visitor);
    if let Some(end) = &frame.end {
        walk_window_bound(end, visitor);
    }
}

fn walk_window_bound<'expr>(bound: &'expr WindowFrameBound, visitor: &mut impl FnMut(&'expr Expr)) {
    if let WindowFrameBound::Following(expression) | WindowFrameBound::Preceding(expression) = bound
    {
        expression.walk(visitor);
    }
}
