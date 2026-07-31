//! Resolved expression trees.

use std::num::NonZeroU32;

use turso_parser::ast::{
    Distinctness, FrameExclude, FrameMode, LikeOperator, Literal, NullsOrder, Operator,
    ResolveType, SortOrder, UnaryOperator,
};

use super::{
    BoundCastPrograms, BoundSchemaCall, DatabaseId, MergedColumnValue, OutputId, QueryId,
    ResolvedCollation, ResolvedFunction, ResolvedTable, ResolvedType, SourceId, TypeFact,
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
    pub programs: BoundCastPrograms,
}

#[derive(Clone, Debug)]
pub struct OrderTerm {
    pub expr: Expr,
    pub order: SortOrder,
    pub nulls: Option<NullsOrder>,
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
    pub sequence: Arc<Sequence>,
    pub schema_cookie: u32,
}

#[derive(Clone, Debug)]
pub struct FunctionCall {
    pub function: ResolvedFunction,
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
        custom: Option<CustomBinaryOperator>,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        start: Box<Expr>,
        end: Box<Expr>,
    },
    Case {
        base: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
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
}
