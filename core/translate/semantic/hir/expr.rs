//! Resolved expression trees.

use std::num::NonZeroU32;

use smallvec::SmallVec;
use turso_parser::ast::{
    Distinctness, FrameExclude, FrameMode, LikeOperator, Literal, NullsOrder, Operator,
    ResolveType, SortOrder, UnaryOperator,
};

use super::{
    BoundCastPrograms, BoundSchemaCall, MergedColumnValue, OutputId, QueryBlockId, QueryId,
    ResolvedCollation, ResolvedFunction, ResolvedSequence, ResolvedTable, ResolvedType, SourceId,
    TypeFact, WindowId,
};
use crate::sync::Arc;
use crate::util::check_literal_equivalency;
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
pub struct ResolvedWindow {
    pub id: WindowId,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderTerm>,
    pub frame: WindowFrame,
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
    },
    UnionTag {
        union_type: ResolvedType,
        tag_names: Arc<[String]>,
    },
    UnionExtract {
        union_type: ResolvedType,
        tag_index: u8,
    },
    StructExtract {
        struct_type: ResolvedType,
        field_index: usize,
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
    /// The string supplied by SQL, without its outer quotes. Runtime currval
    /// tracking uses this spelling, including an optional schema prefix.
    pub user_name: String,
    pub normalized_name: String,
    pub sequence: ResolvedSequence,
    pub backing_table: ResolvedTable,
    /// Present only for the internal sequence behind an AUTOINCREMENT table.
    /// Physical lowering uses this frozen object to keep sqlite_sequence in
    /// sync without reopening the catalog.
    pub sqlite_sequence: Option<ResolvedTable>,
}

#[derive(Clone, Debug)]
pub enum FunctionOperation {
    Ordinary,
    CustomType(CustomTypeOperation),
    Sequence(SequenceOperation),
}

#[derive(Clone, Debug)]
pub struct FunctionCall {
    pub function: ResolvedFunction,
    /// How this call is evaluated after semantic analysis. Aggregate and
    /// window identities are owned by one query block, so later stages do not
    /// need to match expression trees to identify shared function state.
    pub evaluation: FunctionEvaluation,
    pub arguments: FunctionArguments,
    pub result_type: TypeFact,
    pub operation: FunctionOperation,
}

/// Resolved function argument shape. Star cannot coexist with expression
/// arguments.
#[derive(Clone, Debug)]
pub enum FunctionArguments {
    Star,
    Expressions {
        values: Vec<Expr>,
        distinctness: Option<Distinctness>,
        order_by: Vec<OrderTerm>,
    },
    OrderedSet {
        direct: Vec<Expr>,
        order_by: Box<OrderTerm>,
    },
}

impl FunctionArguments {
    pub fn expressions(&self) -> &[Expr] {
        match self {
            Self::Star => &[],
            Self::Expressions { values, .. } => values,
            Self::OrderedSet { direct, .. } => direct,
        }
    }

    pub fn order_terms(&self) -> &[OrderTerm] {
        match self {
            Self::Star => &[],
            Self::Expressions { order_by, .. } => order_by,
            Self::OrderedSet { order_by, .. } => std::slice::from_ref(order_by.as_ref()),
        }
    }

    pub fn distinctness(&self) -> Option<Distinctness> {
        match self {
            Self::Star => None,
            Self::Expressions { distinctness, .. } => *distinctness,
            Self::OrderedSet { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum FunctionEvaluation {
    Scalar,
    Aggregate {
        id: AggregateId,
        filter: Option<Box<Expr>>,
    },
    Window {
        id: WindowFunctionId,
        window: WindowId,
        filter: Option<Box<Expr>>,
    },
}

impl FunctionEvaluation {
    pub fn filter(&self) -> Option<&Expr> {
        match self {
            Self::Aggregate { filter, .. } | Self::Window { filter, .. } => filter.as_deref(),
            Self::Scalar => None,
        }
    }
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
    Row {
        query: QueryId,
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

struct ExprFrame<'expr, T> {
    expression: &'expr Expr,
    next_child: usize,
    child_values: SmallVec<[T; 3]>,
}

impl<'expr, T> ExprFrame<'expr, T> {
    fn new(expression: &'expr Expr) -> Self {
        Self {
            expression,
            next_child: 0,
            child_values: SmallVec::new(),
        }
    }

    fn next_child(&mut self) -> Option<&'expr Expr> {
        let child = self.expression.child(self.next_child)?;
        self.next_child += 1;
        Some(child)
    }
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

    fn child(&self, mut index: usize) -> Option<&Self> {
        match self {
            Self::Literal(_)
            | Self::Parameter(_)
            | Self::Column(_)
            | Self::RowId(_)
            | Self::Output(_)
            | Self::Subquery(
                SubqueryExpr::Scalar { .. } | SubqueryExpr::Row { .. } | SubqueryExpr::Exists(_),
            ) => None,
            Self::MergedColumn(column) => (index == 0).then_some(column.left.as_ref()),
            Self::Unary { expr, .. }
            | Self::IsNull(expr)
            | Self::NotNull(expr)
            | Self::Collate { expr, .. } => (index == 0).then_some(expr.as_ref()),
            Self::Binary {
                lhs, rhs, custom, ..
            } => match index {
                0 => Some(lhs),
                1 => Some(rhs),
                _ => {
                    index -= 2;
                    let encoder = custom
                        .as_ref()
                        .and_then(|custom| custom.literal_encoding.as_ref())
                        .and_then(|encoding| encoding.encoder.as_ref());
                    child_from_optional_schema_call(encoder, &mut index)
                }
            },
            Self::Between {
                expr, start, end, ..
            } => [expr.as_ref(), start.as_ref(), end.as_ref()]
                .get(index)
                .copied(),
            Self::Case {
                base,
                when_then,
                else_expr,
                ..
            } => {
                if let Some(child) = child_from_optional_expr(base.as_deref(), &mut index) {
                    return Some(child);
                }
                let pair_index = index / 2;
                if let Some((when, then)) = when_then.get(pair_index) {
                    return Some(if index % 2 == 0 { when } else { then });
                }
                index -= when_then.len() * 2;
                child_from_optional_expr(else_expr.as_deref(), &mut index)
            }
            Self::Cast { expr, target } => {
                if index == 0 {
                    return Some(expr);
                }
                index -= 1;
                if let Some(child) = child_from_exprs(&target.parameters, &mut index) {
                    return Some(child);
                }
                if let Some(child) = child_from_schema_calls(&target.programs.encode, &mut index) {
                    return Some(child);
                }
                target.programs.domain.as_ref().and_then(|domain| {
                    child_from_schema_calls(
                        domain.checks.iter().map(|check| &check.call),
                        &mut index,
                    )
                })
            }
            Self::Function(call) => {
                if let Some(child) = child_from_exprs(call.arguments.expressions(), &mut index) {
                    return Some(child);
                }
                if let Some(child) =
                    child_from_order_terms(call.arguments.order_terms(), &mut index)
                {
                    return Some(child);
                }
                child_from_optional_expr(call.evaluation.filter(), &mut index)
            }
            Self::InList { lhs, values, .. } => {
                if index == 0 {
                    return Some(lhs);
                }
                index -= 1;
                child_from_exprs(values, &mut index)
            }
            Self::Subquery(SubqueryExpr::In { lhs, .. }) => (index == 0).then_some(lhs.as_ref()),
            Self::Like {
                lhs, rhs, escape, ..
            } => match index {
                0 => Some(lhs),
                1 => Some(rhs),
                2 => escape.as_deref(),
                _ => None,
            },
            Self::Row(expressions) | Self::Array(expressions) => expressions.get(index),
            Self::Subscript {
                base,
                index: offset,
            } => match index {
                0 => Some(base),
                1 => Some(offset),
                _ => None,
            },
            Self::FieldAccess(access) => (index == 0).then_some(access.base.as_ref()),
            Self::Raise { message, .. } => (index == 0).then(|| message.as_deref()).flatten(),
        }
    }

    /// Compare resolved expression shapes for expression-index matching.
    ///
    /// Work stays on explicit stacks because index expressions can be deeply
    /// nested. Commutative operators retain the existing planner rule that
    /// permits their operands to appear in either order.
    pub(crate) fn equivalent_for_index(&self, other: &Self) -> bool {
        let mut pending = vec![(self, other)];
        let mut alternatives = Vec::new();

        'search: loop {
            while let Some((left, right)) = pending.pop() {
                let equivalent = match (left, right) {
                    (Self::Literal(left), Self::Literal(right)) => {
                        check_literal_equivalency(left, right)
                    }
                    (Self::Column(left), Self::Column(right)) => left == right,
                    (Self::RowId(left), Self::RowId(right)) => left == right,
                    (
                        Self::Unary {
                            operator: left_operator,
                            expr: left,
                        },
                        Self::Unary {
                            operator: right_operator,
                            expr: right,
                        },
                    ) => {
                        pending.push((left, right));
                        left_operator == right_operator
                    }
                    (
                        Self::Binary {
                            lhs: left_lhs,
                            operator: left_operator,
                            rhs: left_rhs,
                            array_concat: left_array_concat,
                            custom: left_custom,
                            comparison: left_comparison,
                        },
                        Self::Binary {
                            lhs: right_lhs,
                            operator: right_operator,
                            rhs: right_rhs,
                            array_concat: right_array_concat,
                            custom: right_custom,
                            comparison: right_comparison,
                        },
                    ) => {
                        let metadata_matches = left_operator == right_operator
                            && left_array_concat == right_array_concat
                            && custom_binary_operators_match(left_custom, right_custom)
                            && left_comparison == right_comparison;
                        if metadata_matches && left_operator.is_commutative() {
                            let mut swapped = pending.clone();
                            swapped.push((left_lhs, right_rhs));
                            swapped.push((left_rhs, right_lhs));
                            alternatives.push(swapped);
                        }
                        pending.push((left_lhs, right_lhs));
                        pending.push((left_rhs, right_rhs));
                        metadata_matches
                    }
                    (
                        Self::Between {
                            expr: left_expr,
                            negated: left_negated,
                            start: left_start,
                            end: left_end,
                            start_comparison: left_start_comparison,
                            end_comparison: left_end_comparison,
                        },
                        Self::Between {
                            expr: right_expr,
                            negated: right_negated,
                            start: right_start,
                            end: right_end,
                            start_comparison: right_start_comparison,
                            end_comparison: right_end_comparison,
                        },
                    ) => {
                        pending.push((left_expr, right_expr));
                        pending.push((left_start, right_start));
                        pending.push((left_end, right_end));
                        left_negated == right_negated
                            && left_start_comparison == right_start_comparison
                            && left_end_comparison == right_end_comparison
                    }
                    (
                        Self::Case {
                            base: left_base,
                            when_then: left_when_then,
                            else_expr: left_else,
                            base_comparisons: left_comparisons,
                        },
                        Self::Case {
                            base: right_base,
                            when_then: right_when_then,
                            else_expr: right_else,
                            base_comparisons: right_comparisons,
                        },
                    ) => {
                        let shapes_match = push_optional_pair(
                            &mut pending,
                            left_base.as_deref(),
                            right_base.as_deref(),
                        ) && push_optional_pair(
                            &mut pending,
                            left_else.as_deref(),
                            right_else.as_deref(),
                        ) && left_when_then.len() == right_when_then.len();
                        if shapes_match {
                            for ((left_when, left_then), (right_when, right_then)) in
                                left_when_then.iter().zip(right_when_then)
                            {
                                pending.push((left_when, right_when));
                                pending.push((left_then, right_then));
                            }
                        }
                        shapes_match && left_comparisons == right_comparisons
                    }
                    (
                        Self::Cast {
                            expr: left_expr,
                            target: left_target,
                        },
                        Self::Cast {
                            expr: right_expr,
                            target: right_target,
                        },
                    ) => {
                        pending.push((left_expr, right_expr));
                        push_pairs(
                            &mut pending,
                            &left_target.parameters,
                            &right_target.parameters,
                        ) && left_target.name.eq_ignore_ascii_case(&right_target.name)
                            && left_target.array_dimensions == right_target.array_dimensions
                            && left_target.type_fact == right_target.type_fact
                            && left_target.affinity == right_target.affinity
                            && left_target.programs.encode.is_empty()
                            && right_target.programs.encode.is_empty()
                            && left_target.programs.domain.is_none()
                            && right_target.programs.domain.is_none()
                            && left_target.programs.apply_builtin_affinity
                                == right_target.programs.apply_builtin_affinity
                    }
                    (
                        Self::Collate {
                            expr: left_expr,
                            collation: left_collation,
                        },
                        Self::Collate {
                            expr: right_expr,
                            collation: right_collation,
                        },
                    ) => {
                        pending.push((left_expr, right_expr));
                        left_collation == right_collation
                    }
                    (Self::Function(left), Self::Function(right)) => {
                        function_calls_match(left, right, &mut pending)
                    }
                    (Self::IsNull(left), Self::IsNull(right))
                    | (Self::NotNull(left), Self::NotNull(right)) => {
                        pending.push((left, right));
                        true
                    }
                    (
                        Self::InList {
                            lhs: left_lhs,
                            negated: left_negated,
                            values: left_values,
                            comparisons: left_comparisons,
                        },
                        Self::InList {
                            lhs: right_lhs,
                            negated: right_negated,
                            values: right_values,
                            comparisons: right_comparisons,
                        },
                    ) => {
                        pending.push((left_lhs, right_lhs));
                        push_pairs(&mut pending, left_values, right_values)
                            && left_negated == right_negated
                            && left_comparisons == right_comparisons
                    }
                    (
                        Self::Like {
                            lhs: left_lhs,
                            negated: left_negated,
                            operator: left_operator,
                            function: left_function,
                            argument_count: left_argument_count,
                            rhs: left_rhs,
                            escape: left_escape,
                        },
                        Self::Like {
                            lhs: right_lhs,
                            negated: right_negated,
                            operator: right_operator,
                            function: right_function,
                            argument_count: right_argument_count,
                            rhs: right_rhs,
                            escape: right_escape,
                        },
                    ) => {
                        pending.push((left_lhs, right_lhs));
                        pending.push((left_rhs, right_rhs));
                        push_optional_pair(
                            &mut pending,
                            left_escape.as_deref(),
                            right_escape.as_deref(),
                        ) && left_negated == right_negated
                            && left_operator == right_operator
                            && left_function == right_function
                            && left_argument_count == right_argument_count
                    }
                    (Self::Row(left), Self::Row(right))
                    | (Self::Array(left), Self::Array(right)) => {
                        push_pairs(&mut pending, left, right)
                    }
                    (
                        Self::Subscript {
                            base: left_base,
                            index: left_index,
                        },
                        Self::Subscript {
                            base: right_base,
                            index: right_index,
                        },
                    ) => {
                        pending.push((left_base, right_base));
                        pending.push((left_index, right_index));
                        true
                    }
                    (Self::FieldAccess(left), Self::FieldAccess(right)) => {
                        pending.push((&left.base, &right.base));
                        left.field_name.eq_ignore_ascii_case(&right.field_name)
                            && left.kind == right.kind
                            && left.container_type == right.container_type
                            && left.result_type == right.result_type
                    }
                    _ => false,
                };

                if !equivalent {
                    let Some(alternative) = alternatives.pop() else {
                        return false;
                    };
                    pending = alternative;
                    continue 'search;
                }
            }
            return true;
        }
    }

    /// Visit this expression and every expression it owns. References to
    /// outputs and subqueries remain references; their definitions are walked
    /// by the query that owns them.
    pub(crate) fn walk<'expr>(&'expr self, visitor: &mut impl FnMut(&'expr Expr)) {
        visitor(self);
        let mut frames = vec![ExprFrame::<()>::new(self)];
        while let Some(frame) = frames.last_mut() {
            let Some(child) = frame.next_child() else {
                frames.pop();
                continue;
            };
            visitor(child);
            frames.push(ExprFrame::new(child));
        }
    }

    /// Reduce this expression from leaves to root without using the call stack.
    /// Child values keep expression-child order, letting callers handle nodes
    /// whose result depends on more than one child.
    pub(crate) fn fold<T>(&self, folder: &mut impl FnMut(&Expr, &[T]) -> T) -> T {
        let mut frames = vec![ExprFrame::new(self)];
        loop {
            if let Some(child) = frames
                .last_mut()
                .expect("root expression frame exists")
                .next_child()
            {
                frames.push(ExprFrame::new(child));
                continue;
            }

            let frame = frames.pop().expect("completed expression frame exists");
            let value = folder(frame.expression, &frame.child_values);
            let Some(parent) = frames.last_mut() else {
                return value;
            };
            parent.child_values.push(value);
        }
    }
}

fn push_pairs<'expr>(
    pending: &mut Vec<(&'expr Expr, &'expr Expr)>,
    left: &'expr [Expr],
    right: &'expr [Expr],
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    pending.extend(left.iter().zip(right));
    true
}

fn push_optional_pair<'expr>(
    pending: &mut Vec<(&'expr Expr, &'expr Expr)>,
    left: Option<&'expr Expr>,
    right: Option<&'expr Expr>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            pending.push((left, right));
            true
        }
        (None, None) => true,
        _ => false,
    }
}

fn custom_binary_operators_match(
    left: &Option<CustomBinaryOperator>,
    right: &Option<CustomBinaryOperator>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.function == right.function
                && left.swap_args == right.swap_args
                && left.negate == right.negate
                && match (&left.literal_encoding, &right.literal_encoding) {
                    (None, None) => true,
                    (Some(left), Some(right)) => {
                        left.operand == right.operand
                            && left.encoder.is_none()
                            && right.encoder.is_none()
                    }
                    _ => false,
                }
        }
        _ => false,
    }
}

fn function_calls_match<'expr>(
    left: &'expr FunctionCall,
    right: &'expr FunctionCall,
    pending: &mut Vec<(&'expr Expr, &'expr Expr)>,
) -> bool {
    left.function == right.function
        && matches!(
            (&left.evaluation, &right.evaluation),
            (FunctionEvaluation::Scalar, FunctionEvaluation::Scalar)
        )
        && left.result_type == right.result_type
        && function_operations_match(&left.operation, &right.operation)
        && function_arguments_match(&left.arguments, &right.arguments, pending)
}

fn function_operations_match(left: &FunctionOperation, right: &FunctionOperation) -> bool {
    match (left, right) {
        (FunctionOperation::Ordinary, FunctionOperation::Ordinary) => true,
        (FunctionOperation::CustomType(left), FunctionOperation::CustomType(right)) => {
            match (left, right) {
                (
                    CustomTypeOperation::UnionValue {
                        union_type: left_type,
                        tag_index: left_tag,
                    },
                    CustomTypeOperation::UnionValue {
                        union_type: right_type,
                        tag_index: right_tag,
                    },
                )
                | (
                    CustomTypeOperation::UnionExtract {
                        union_type: left_type,
                        tag_index: left_tag,
                    },
                    CustomTypeOperation::UnionExtract {
                        union_type: right_type,
                        tag_index: right_tag,
                    },
                ) => left_type == right_type && left_tag == right_tag,
                (
                    CustomTypeOperation::UnionTag {
                        union_type: left_type,
                        tag_names: left_names,
                    },
                    CustomTypeOperation::UnionTag {
                        union_type: right_type,
                        tag_names: right_names,
                    },
                ) => left_type == right_type && left_names == right_names,
                (
                    CustomTypeOperation::StructExtract {
                        struct_type: left_type,
                        field_index: left_field,
                    },
                    CustomTypeOperation::StructExtract {
                        struct_type: right_type,
                        field_index: right_field,
                    },
                ) => left_type == right_type && left_field == right_field,
                _ => false,
            }
        }
        (FunctionOperation::Sequence(_), FunctionOperation::Sequence(_)) => false,
        _ => false,
    }
}

fn function_arguments_match<'expr>(
    left: &'expr FunctionArguments,
    right: &'expr FunctionArguments,
    pending: &mut Vec<(&'expr Expr, &'expr Expr)>,
) -> bool {
    match (left, right) {
        (FunctionArguments::Star, FunctionArguments::Star) => true,
        (
            FunctionArguments::Expressions {
                values: left_values,
                distinctness: left_distinctness,
                order_by: left_order,
            },
            FunctionArguments::Expressions {
                values: right_values,
                distinctness: right_distinctness,
                order_by: right_order,
            },
        ) => {
            push_pairs(pending, left_values, right_values)
                && left_distinctness == right_distinctness
                && order_terms_match(left_order, right_order, pending)
        }
        (
            FunctionArguments::OrderedSet {
                direct: left_direct,
                order_by: left_order,
            },
            FunctionArguments::OrderedSet {
                direct: right_direct,
                order_by: right_order,
            },
        ) => {
            push_pairs(pending, left_direct, right_direct)
                && order_terms_match(
                    std::slice::from_ref(left_order.as_ref()),
                    std::slice::from_ref(right_order.as_ref()),
                    pending,
                )
        }
        _ => false,
    }
}

fn order_terms_match<'expr>(
    left: &'expr [OrderTerm],
    right: &'expr [OrderTerm],
    pending: &mut Vec<(&'expr Expr, &'expr Expr)>,
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    for (left, right) in left.iter().zip(right) {
        if left.order != right.order
            || left.nulls != right.nulls
            || left.type_fact != right.type_fact
            || left.collation != right.collation
        {
            return false;
        }
        pending.push((&left.expr, &right.expr));
    }
    true
}

fn child_from_exprs<'expr>(expressions: &'expr [Expr], index: &mut usize) -> Option<&'expr Expr> {
    if let Some(expression) = expressions.get(*index) {
        return Some(expression);
    }
    *index -= expressions.len();
    None
}

fn child_from_optional_expr<'expr>(
    expression: Option<&'expr Expr>,
    index: &mut usize,
) -> Option<&'expr Expr> {
    let expression = expression?;
    if *index == 0 {
        return Some(expression);
    }
    *index -= 1;
    None
}

fn child_from_order_terms<'expr>(
    terms: &'expr [OrderTerm],
    index: &mut usize,
) -> Option<&'expr Expr> {
    if let Some(term) = terms.get(*index) {
        return Some(&term.expr);
    }
    *index -= terms.len();
    None
}

fn child_from_optional_schema_call<'expr>(
    call: Option<&'expr BoundSchemaCall>,
    index: &mut usize,
) -> Option<&'expr Expr> {
    child_from_schema_calls(call, index)
}

fn child_from_schema_calls<'expr>(
    calls: impl IntoIterator<Item = &'expr BoundSchemaCall>,
    index: &mut usize,
) -> Option<&'expr Expr> {
    for call in calls {
        if let Some(child) = child_from_exprs(&call.arguments, index) {
            return Some(child);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expression_frames_preserve_child_order_for_walk_and_fold() {
        let expression = Expr::Binary {
            lhs: Box::new(Expr::Literal(Literal::Numeric("2".into()))),
            operator: Operator::Add,
            rhs: Box::new(Expr::Unary {
                operator: UnaryOperator::Negative,
                expr: Box::new(Expr::Literal(Literal::Numeric("3".into()))),
            }),
            array_concat: false,
            custom: None,
            comparison: None,
        };

        let mut visited = Vec::new();
        expression.walk(&mut |expression| {
            visited.push(match expression {
                Expr::Binary { .. } => "binary",
                Expr::Unary { .. } => "unary",
                Expr::Literal(_) => "literal",
                _ => unreachable!("test expression contains only binary, unary, and literals"),
            });
        });
        assert_eq!(visited, ["binary", "literal", "unary", "literal"]);

        let value = expression.fold(&mut |expression, children: &[i64]| match expression {
            Expr::Literal(Literal::Numeric(value)) => value.parse().expect("integer literal"),
            Expr::Unary {
                operator: UnaryOperator::Negative,
                ..
            } => -children[0],
            Expr::Binary {
                operator: Operator::Add,
                ..
            } => children[0] + children[1],
            _ => unreachable!("test expression contains only addition and negation"),
        });
        assert_eq!(value, -1);
    }
}
