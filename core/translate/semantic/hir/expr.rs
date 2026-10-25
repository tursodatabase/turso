//! Resolved expression trees.

use std::convert::Infallible;
use std::num::NonZeroU32;
use std::ops::ControlFlow;

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
use crate::parameters::ParameterSpelling;
use crate::sync::Arc;
use crate::util::check_literal_equivalency;
use crate::vdbe::affinity::Affinity;

#[derive(Clone, Debug)]
pub struct Parameter {
    pub index: NonZeroU32,
    pub spelling: ParameterSpelling,
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
    pub left: ExprId,
    pub right: ExprId,
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
    pub parameters: Vec<ExprId>,
    pub array_dimensions: u32,
    pub type_fact: TypeFact,
    /// Exact VDBE affinity selected for the CAST operation.
    pub affinity: Affinity,
    pub programs: BoundCastPrograms,
}

#[derive(Clone, Debug)]
pub struct OrderTerm {
    pub expr: ExprId,
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
pub struct WindowPartitionTerm {
    pub expr: ExprId,
    /// Final type facts of `expr` in the scope where this term was bound.
    pub type_fact: TypeFact,
    /// Final SQLite collation after explicit-COLLATE and declared-column
    /// precedence have been applied during semantic analysis.
    pub collation: Option<ResolvedCollation>,
}

#[derive(Clone, Debug)]
pub struct ResolvedWindow {
    pub id: WindowId,
    pub partition_by: Vec<WindowPartitionTerm>,
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
    Following(ExprId),
    Preceding(ExprId),
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

#[derive(Clone, Debug, PartialEq)]
pub struct FunctionArgumentFacts {
    pub type_fact: TypeFact,
    pub collation: Option<ResolvedCollation>,
}

/// Resolved function argument shape. Star cannot coexist with expression
/// arguments.
#[derive(Clone, Debug)]
pub enum FunctionArguments {
    Star,
    Expressions {
        values: Vec<ExprId>,
        facts: Vec<FunctionArgumentFacts>,
        distinctness: Option<Distinctness>,
        order_by: Vec<OrderTerm>,
    },
    OrderedSet {
        direct: Vec<ExprId>,
        order_by: Box<OrderTerm>,
    },
}

impl FunctionArguments {
    pub fn expressions(&self) -> &[ExprId] {
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
        filter: Option<ExprId>,
    },
    Window {
        id: WindowFunctionId,
        window: WindowId,
        filter: Option<ExprId>,
    },
}

impl FunctionEvaluation {
    pub fn filter(&self) -> Option<ExprId> {
        match self {
            Self::Aggregate { filter, .. } | Self::Window { filter, .. } => *filter,
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
    pub base: ExprId,
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
        lhs: ExprId,
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
    MergedColumn(Box<MergedColumn>),
    RowId(SourceId),
    Output(OutputId),
    Unary {
        operator: UnaryOperator,
        expr: ExprId,
    },
    Binary {
        lhs: ExprId,
        operator: Operator,
        rhs: ExprId,
        /// `||` uses the array opcode when either operand is an array. This
        /// decision depends on bound type facts and must not be rediscovered
        /// during physical emission.
        array_concat: bool,
        custom: Option<Box<CustomBinaryOperator>>,
        comparison: Option<ComparisonSemantics>,
    },
    Between {
        expr: ExprId,
        negated: bool,
        start: ExprId,
        end: ExprId,
        start_comparison: ComparisonSemantics,
        end_comparison: ComparisonSemantics,
    },
    Case {
        base: Option<ExprId>,
        when_then: Vec<(ExprId, ExprId)>,
        else_expr: Option<ExprId>,
        /// Present exactly when `base` is present, one comparison per WHEN.
        base_comparisons: Vec<ComparisonSemantics>,
    },
    Cast {
        expr: ExprId,
        target: Box<TypeName>,
    },
    Collate {
        expr: ExprId,
        collation: ResolvedCollation,
    },
    /// Boxed: a call is the largest node by far, and every expression is
    /// moved by value while it is built and planned.
    Function(Box<FunctionCall>),
    IsNull(ExprId),
    NotNull(ExprId),
    TruthTest {
        expr: ExprId,
        is_true: bool,
        negated: bool,
    },
    InList {
        lhs: ExprId,
        negated: bool,
        values: Vec<ExprId>,
        comparisons: Vec<ComparisonSemantics>,
    },
    Subquery(SubqueryExpr),
    Like {
        lhs: ExprId,
        negated: bool,
        operator: LikeOperator,
        function: ResolvedFunction,
        argument_count: usize,
        rhs: ExprId,
        escape: Option<ExprId>,
    },
    Row(Vec<ExprId>),
    Array(Vec<ExprId>),
    Subscript {
        base: ExprId,
        index: ExprId,
    },
    FieldAccess(Box<FieldAccess>),
    Raise {
        action: ResolveType,
        message: Option<ExprId>,
    },
}

/// Index of one expression node in its document's [`ExprArena`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExprId(u32);

impl ExprId {
    pub(crate) const DETACHED: ExprId = ExprId(u32::MAX);

    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Every expression node of one HIR document. A node's children are always
/// stored before it, so walking the nodes in order visits every child
/// before its parent.
#[derive(Clone, Debug, Default)]
pub struct ExprArena {
    nodes: Vec<Expr>,
    constant: Vec<bool>,
}

impl ExprArena {
    pub fn push(&mut self, expression: Expr) -> ExprId {
        let id = ExprId(u32::try_from(self.nodes.len()).expect("expression arena fits in u32"));
        self.constant.push(self.children_constant(&expression));
        self.nodes.push(expression);
        id
    }

    pub(crate) fn is_constant(&self, expression: ExprRef<'_>) -> bool {
        if expression.id() == ExprId::DETACHED {
            return self.children_constant(expression.node());
        }
        self.constant[expression.id().index()]
    }

    fn children_constant(&self, expression: &Expr) -> bool {
        if !expression.constant_by_kind() {
            return false;
        }
        let mut index = 0;
        while let Some(child) = expression.child(index) {
            if !self.constant[child.index()] {
                return false;
            }
            index += 1;
        }
        true
    }

    pub fn at(&self, id: ExprId) -> ExprRef<'_> {
        ExprRef {
            arena: self,
            id,
            node: &self[id],
        }
    }

    /// `node`, which is not stored in this arena, with its children read
    /// from the arena. Its id matches no stored node.
    pub(crate) fn detached<'a>(&'a self, node: &'a Expr) -> ExprRef<'a> {
        ExprRef {
            arena: self,
            id: ExprId::DETACHED,
            node,
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.constant.reserve(additional);
    }

    pub fn get_mut(&mut self, id: ExprId) -> &mut Expr {
        &mut self.nodes[id.index()]
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn truncate(&mut self, len: usize) {
        self.nodes.truncate(len);
        self.constant.truncate(len);
    }

    /// Visit `node`, which may not be in this arena yet, and every
    /// expression it owns.
    pub(crate) fn for_each_from(&self, node: &Expr, visitor: &mut impl FnMut(&Expr)) {
        visitor(node);
        let mut index = 0;
        while let Some(child) = node.child(index) {
            self.at(child)
                .for_each(&mut |expression| visitor(expression.node()));
            index += 1;
        }
    }

    /// Copy the expression at `id` and everything it owns into new nodes,
    /// and return the copy's root.
    #[cfg(test)]
    pub fn deep_copy(&mut self, id: ExprId) -> ExprId {
        let mut order = Vec::new();
        let mut pending = vec![id];
        while let Some(next) = pending.pop() {
            order.push(next);
            let node = &self.nodes[next.index()];
            let mut index = 0;
            while let Some(child) = node.child(index) {
                pending.push(child);
                index += 1;
            }
        }
        let mut copies: rustc_hash::FxHashMap<ExprId, ExprId> = Default::default();
        for old in order.into_iter().rev() {
            if copies.contains_key(&old) {
                continue;
            }
            let mut node = self.nodes[old.index()].clone();
            node.for_each_child_mut(&mut |child| *child = copies[child]);
            let new = self.push(node);
            copies.insert(old, new);
        }
        copies[&id]
    }
}

impl std::ops::Index<ExprId> for ExprArena {
    type Output = Expr;

    fn index(&self, id: ExprId) -> &Expr {
        &self.nodes[id.index()]
    }
}

/// One expression node together with the arena that holds its children.
#[derive(Clone, Copy)]
pub struct ExprRef<'a> {
    arena: &'a ExprArena,
    id: ExprId,
    node: &'a Expr,
}

impl<'a> ExprRef<'a> {
    pub fn id(self) -> ExprId {
        self.id
    }

    pub fn arena(self) -> &'a ExprArena {
        self.arena
    }

    pub fn node(self) -> &'a Expr {
        self.node
    }

    /// Another expression of the same arena, such as a child of this one.
    pub fn at(self, id: ExprId) -> ExprRef<'a> {
        self.arena.at(id)
    }

    pub(crate) fn child(self, index: usize) -> Option<ExprRef<'a>> {
        self.node().child(index).map(|id| self.at(id))
    }

    /// The terms this expression ANDs together, left to right.
    pub(crate) fn conjuncts(self) -> impl Iterator<Item = ExprRef<'a>> {
        let mut pending = vec![self];
        std::iter::from_fn(move || {
            while let Some(expression) = pending.pop() {
                if let Expr::Binary {
                    lhs,
                    operator: turso_parser::ast::Operator::And,
                    rhs,
                    ..
                } = expression.node()
                {
                    pending.push(expression.at(*rhs));
                    pending.push(expression.at(*lhs));
                } else {
                    return Some(expression);
                }
            }
            None
        })
    }
}

impl std::ops::Deref for ExprRef<'_> {
    type Target = Expr;

    fn deref(&self) -> &Expr {
        self.node
    }
}

impl std::fmt::Debug for ExprRef<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {:?}", self.id, self.node())
    }
}

struct ExprFrame<'expr, C, T> {
    expression: ExprRef<'expr>,
    context: C,
    next_child: usize,
    child_values: SmallVec<[T; 3]>,
}

pub(crate) trait ExprVisitor<'expr> {
    type Context;
    type Output;
    type Error;

    fn child(&mut self, expression: ExprRef<'expr>, index: usize) -> Option<ExprRef<'expr>> {
        expression.child(index)
    }

    fn pre_order(
        &mut self,
        parent: ExprRef<'expr>,
        context: &mut Self::Context,
        child_index: usize,
        child: ExprRef<'expr>,
    ) -> Result<ControlFlow<(), Self::Context>, Self::Error>;

    fn post_order(
        &mut self,
        expression: ExprRef<'expr>,
        context: Self::Context,
        children: &[Self::Output],
    ) -> Result<Self::Output, Self::Error>;
}

struct WalkVisitor<'a, F> {
    visit: &'a mut F,
}

impl<'expr, F: FnMut(ExprRef<'expr>)> ExprVisitor<'expr> for WalkVisitor<'_, F> {
    type Context = ();
    type Output = ();
    type Error = Infallible;

    fn pre_order(
        &mut self,
        _parent: ExprRef<'expr>,
        _context: &mut (),
        _child_index: usize,
        child: ExprRef<'expr>,
    ) -> Result<ControlFlow<(), ()>, Infallible> {
        (self.visit)(child);
        Ok(ControlFlow::Continue(()))
    }

    fn post_order(
        &mut self,
        _expression: ExprRef<'expr>,
        _context: (),
        _children: &[()],
    ) -> Result<(), Infallible> {
        Ok(())
    }
}

struct FoldVisitor<'a, F, T> {
    fold: &'a mut F,
    output: std::marker::PhantomData<fn() -> T>,
}

impl<'expr, T, F: FnMut(ExprRef<'expr>, &[T]) -> T> ExprVisitor<'expr> for FoldVisitor<'_, F, T> {
    type Context = ();
    type Output = T;
    type Error = Infallible;

    fn pre_order(
        &mut self,
        _parent: ExprRef<'expr>,
        _context: &mut (),
        _child_index: usize,
        _child: ExprRef<'expr>,
    ) -> Result<ControlFlow<(), ()>, Infallible> {
        Ok(ControlFlow::Continue(()))
    }

    fn post_order(
        &mut self,
        expression: ExprRef<'expr>,
        _context: (),
        children: &[T],
    ) -> Result<T, Infallible> {
        Ok((self.fold)(expression, children))
    }
}

impl<'expr, C, T> ExprFrame<'expr, C, T> {
    fn new(expression: ExprRef<'expr>, context: C) -> Self {
        Self {
            expression,
            context,
            next_child: 0,
            child_values: SmallVec::new(),
        }
    }

    fn next_child<V: ExprVisitor<'expr>>(
        &mut self,
        visitor: &mut V,
    ) -> Option<(usize, ExprRef<'expr>)> {
        let index = self.next_child;
        let child = visitor.child(self.expression, index)?;
        self.next_child += 1;
        Some((index, child))
    }
}

impl Expr {
    pub fn column(source: SourceId, column: usize) -> Self {
        Self::Column(ColumnRef { source, column })
    }

    #[cfg(test)]
    pub fn output(output: OutputId) -> Self {
        Self::Output(output)
    }

    pub fn rowid(source: SourceId) -> Self {
        Self::RowId(source)
    }

    fn constant_by_kind(&self) -> bool {
        match self {
            Expr::Literal(_)
            | Expr::Parameter(_)
            | Expr::Unary { .. }
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::Collate { .. }
            | Expr::IsNull(_)
            | Expr::NotNull(_)
            | Expr::TruthTest { .. }
            | Expr::InList { .. }
            | Expr::Row(_) => true,
            Expr::Binary { custom, .. } => custom.is_none(),
            Expr::Function(call) => {
                // Like SQLite's SQLITE_FUNC_SLOCHNG functions, the date and
                // time functions return the same value for the whole
                // statement, so a call with constant arguments is computed
                // once instead of on every row.
                let function = call.function.value();
                let same_for_whole_statement =
                    crate::function::Deterministic::is_deterministic(function)
                        || matches!(
                            function,
                            crate::function::Func::Scalar(scalar) if scalar.is_date_time_function()
                        );
                matches!(call.evaluation, FunctionEvaluation::Scalar) && same_for_whole_statement
            }
            _ => false,
        }
    }

    pub(crate) fn child(&self, mut index: usize) -> Option<ExprId> {
        match self {
            Self::Literal(_)
            | Self::Parameter(_)
            | Self::Column(_)
            | Self::RowId(_)
            | Self::Output(_)
            | Self::Subquery(
                SubqueryExpr::Scalar { .. } | SubqueryExpr::Row { .. } | SubqueryExpr::Exists(_),
            ) => None,
            Self::MergedColumn(column) => match column.value {
                MergedColumnValue::Left => (index == 0).then_some(column.left),
                MergedColumnValue::Right => (index == 0).then_some(column.right),
                MergedColumnValue::Coalesce => [column.left, column.right].get(index).copied(),
            },
            Self::Unary { expr, .. }
            | Self::IsNull(expr)
            | Self::NotNull(expr)
            | Self::TruthTest { expr, .. }
            | Self::Collate { expr, .. } => (index == 0).then_some(*expr),
            Self::Binary {
                lhs, rhs, custom, ..
            } => match index {
                0 => Some(*lhs),
                1 => Some(*rhs),
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
            } => [*expr, *start, *end].get(index).copied(),
            Self::Case {
                base,
                when_then,
                else_expr,
                ..
            } => {
                if let Some(child) = child_from_optional_expr(*base, &mut index) {
                    return Some(child);
                }
                let pair_index = index / 2;
                if let Some((when, then)) = when_then.get(pair_index) {
                    return Some(if index % 2 == 0 { *when } else { *then });
                }
                index -= when_then.len() * 2;
                child_from_optional_expr(*else_expr, &mut index)
            }
            Self::Cast { expr, target } => {
                if index == 0 {
                    return Some(*expr);
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
                    return Some(*lhs);
                }
                index -= 1;
                child_from_exprs(values, &mut index)
            }
            Self::Subquery(SubqueryExpr::In { lhs, .. }) => (index == 0).then_some(*lhs),
            Self::Like {
                lhs, rhs, escape, ..
            } => match index {
                0 => Some(*lhs),
                1 => Some(*rhs),
                2 => *escape,
                _ => None,
            },
            Self::Row(expressions) | Self::Array(expressions) => expressions.get(index).copied(),
            Self::Subscript {
                base,
                index: offset,
            } => match index {
                0 => Some(*base),
                1 => Some(*offset),
                _ => None,
            },
            Self::FieldAccess(access) => (index == 0).then_some(access.base),
            Self::Raise { message, .. } => (index == 0).then_some(*message).flatten(),
        }
    }
}

impl Expr {
    /// Call `visit` on every child id this node owns, in no particular order.
    pub(crate) fn for_each_child_mut(&mut self, visit: &mut impl FnMut(&mut ExprId)) {
        match self {
            Self::Literal(_)
            | Self::Parameter(_)
            | Self::Column(_)
            | Self::RowId(_)
            | Self::Output(_)
            | Self::Subquery(
                SubqueryExpr::Scalar { .. } | SubqueryExpr::Row { .. } | SubqueryExpr::Exists(_),
            ) => {}
            Self::MergedColumn(column) => {
                visit(&mut column.left);
                visit(&mut column.right);
            }
            Self::Unary { expr, .. }
            | Self::IsNull(expr)
            | Self::NotNull(expr)
            | Self::TruthTest { expr, .. }
            | Self::Collate { expr, .. }
            | Self::Subquery(SubqueryExpr::In { lhs: expr, .. }) => visit(expr),
            Self::Binary {
                lhs, rhs, custom, ..
            } => {
                visit(lhs);
                visit(rhs);
                if let Some(encoder) = custom
                    .as_mut()
                    .and_then(|custom| custom.literal_encoding.as_mut())
                    .and_then(|encoding| encoding.encoder.as_mut())
                {
                    encoder.arguments.iter_mut().for_each(&mut *visit);
                }
            }
            Self::Between {
                expr, start, end, ..
            } => {
                visit(expr);
                visit(start);
                visit(end);
            }
            Self::Case {
                base,
                when_then,
                else_expr,
                ..
            } => {
                base.iter_mut().for_each(&mut *visit);
                for (when, then) in when_then {
                    visit(when);
                    visit(then);
                }
                else_expr.iter_mut().for_each(&mut *visit);
            }
            Self::Cast { expr, target } => {
                visit(expr);
                target.parameters.iter_mut().for_each(&mut *visit);
                for call in &mut target.programs.encode {
                    call.arguments.iter_mut().for_each(&mut *visit);
                }
                if let Some(domain) = &mut target.programs.domain {
                    for check in &mut domain.checks {
                        check.call.arguments.iter_mut().for_each(&mut *visit);
                    }
                }
            }
            Self::Function(call) => {
                match &mut call.arguments {
                    FunctionArguments::Star => {}
                    FunctionArguments::Expressions {
                        values, order_by, ..
                    } => {
                        values.iter_mut().for_each(&mut *visit);
                        for term in order_by {
                            visit(&mut term.expr);
                        }
                    }
                    FunctionArguments::OrderedSet { direct, order_by } => {
                        direct.iter_mut().for_each(&mut *visit);
                        visit(&mut order_by.expr);
                    }
                }
                match &mut call.evaluation {
                    FunctionEvaluation::Scalar => {}
                    FunctionEvaluation::Aggregate { filter, .. }
                    | FunctionEvaluation::Window { filter, .. } => {
                        filter.iter_mut().for_each(&mut *visit)
                    }
                }
            }
            Self::InList { lhs, values, .. } => {
                visit(lhs);
                values.iter_mut().for_each(&mut *visit);
            }
            Self::Like {
                lhs, rhs, escape, ..
            } => {
                visit(lhs);
                visit(rhs);
                escape.iter_mut().for_each(&mut *visit);
            }
            Self::Row(expressions) | Self::Array(expressions) => {
                expressions.iter_mut().for_each(&mut *visit)
            }
            Self::Subscript { base, index } => {
                visit(base);
                visit(index);
            }
            Self::FieldAccess(access) => visit(&mut access.base),
            Self::Raise { message, .. } => message.iter_mut().for_each(&mut *visit),
        }
    }
}

impl<'a> ExprRef<'a> {
    /// Compare resolved expression shapes.
    ///
    /// Work stays on explicit stacks because expressions can be deeply
    /// nested. Commutative operators retain the existing equivalence rule that
    /// permits their operands to appear in either order.
    pub(crate) fn equivalent(self, other: ExprRef<'_>) -> bool {
        let (left_arena, right_arena) = (self.arena, other.arena);
        let mut pending = vec![(self.id, other.id)];
        let mut alternatives = Vec::new();

        'search: loop {
            while let Some((left, right)) = pending.pop() {
                let equivalent = match (&left_arena[left], &right_arena[right]) {
                    (Expr::Literal(left), Expr::Literal(right)) => {
                        check_literal_equivalency(left, right)
                    }
                    (Expr::Parameter(left), Expr::Parameter(right)) => left.index == right.index,
                    (Expr::Column(left), Expr::Column(right)) => left == right,
                    (Expr::MergedColumn(left), Expr::MergedColumn(right)) => {
                        pending.push((left.left, right.left));
                        pending.push((left.right, right.right));
                        left.value == right.value
                            && left.type_fact == right.type_fact
                            && left.affinity == right.affinity
                            && left.has_affinity == right.has_affinity
                            && left.collation == right.collation
                    }
                    (Expr::RowId(left), Expr::RowId(right)) => left == right,
                    (Expr::Output(left), Expr::Output(right)) => left == right,
                    (
                        Expr::Unary {
                            operator: left_operator,
                            expr: left,
                        },
                        Expr::Unary {
                            operator: right_operator,
                            expr: right,
                        },
                    ) => {
                        pending.push((*left, *right));
                        left_operator == right_operator
                    }
                    (
                        Expr::Binary {
                            lhs: left_lhs,
                            operator: left_operator,
                            rhs: left_rhs,
                            array_concat: left_array_concat,
                            custom: left_custom,
                            comparison: left_comparison,
                        },
                        Expr::Binary {
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
                            swapped.push((*left_lhs, *right_rhs));
                            swapped.push((*left_rhs, *right_lhs));
                            alternatives.push(swapped);
                        }
                        pending.push((*left_lhs, *right_lhs));
                        pending.push((*left_rhs, *right_rhs));
                        metadata_matches
                    }
                    (
                        Expr::Between {
                            expr: left_expr,
                            negated: left_negated,
                            start: left_start,
                            end: left_end,
                            start_comparison: left_start_comparison,
                            end_comparison: left_end_comparison,
                        },
                        Expr::Between {
                            expr: right_expr,
                            negated: right_negated,
                            start: right_start,
                            end: right_end,
                            start_comparison: right_start_comparison,
                            end_comparison: right_end_comparison,
                        },
                    ) => {
                        pending.push((*left_expr, *right_expr));
                        pending.push((*left_start, *right_start));
                        pending.push((*left_end, *right_end));
                        left_negated == right_negated
                            && left_start_comparison == right_start_comparison
                            && left_end_comparison == right_end_comparison
                    }
                    (
                        Expr::Case {
                            base: left_base,
                            when_then: left_when_then,
                            else_expr: left_else,
                            base_comparisons: left_comparisons,
                        },
                        Expr::Case {
                            base: right_base,
                            when_then: right_when_then,
                            else_expr: right_else,
                            base_comparisons: right_comparisons,
                        },
                    ) => {
                        let shapes_match =
                            push_optional_pair(&mut pending, *left_base, *right_base)
                                && push_optional_pair(&mut pending, *left_else, *right_else)
                                && left_when_then.len() == right_when_then.len();
                        if shapes_match {
                            for ((left_when, left_then), (right_when, right_then)) in
                                left_when_then.iter().zip(right_when_then)
                            {
                                pending.push((*left_when, *right_when));
                                pending.push((*left_then, *right_then));
                            }
                        }
                        shapes_match && left_comparisons == right_comparisons
                    }
                    (
                        Expr::Cast {
                            expr: left_expr,
                            target: left_target,
                        },
                        Expr::Cast {
                            expr: right_expr,
                            target: right_target,
                        },
                    ) => {
                        pending.push((*left_expr, *right_expr));
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
                        Expr::Collate {
                            expr: left_expr,
                            collation: left_collation,
                        },
                        Expr::Collate {
                            expr: right_expr,
                            collation: right_collation,
                        },
                    ) => {
                        pending.push((*left_expr, *right_expr));
                        left_collation == right_collation
                    }
                    (Expr::Function(left), Expr::Function(right)) => {
                        function_calls_match(left, right, &mut pending)
                    }
                    (Expr::IsNull(left), Expr::IsNull(right))
                    | (Expr::NotNull(left), Expr::NotNull(right)) => {
                        pending.push((*left, *right));
                        true
                    }
                    (
                        Expr::TruthTest {
                            expr: left,
                            is_true: left_is_true,
                            negated: left_negated,
                        },
                        Expr::TruthTest {
                            expr: right,
                            is_true: right_is_true,
                            negated: right_negated,
                        },
                    ) => {
                        pending.push((*left, *right));
                        left_is_true == right_is_true && left_negated == right_negated
                    }
                    (
                        Expr::InList {
                            lhs: left_lhs,
                            negated: left_negated,
                            values: left_values,
                            comparisons: left_comparisons,
                        },
                        Expr::InList {
                            lhs: right_lhs,
                            negated: right_negated,
                            values: right_values,
                            comparisons: right_comparisons,
                        },
                    ) => {
                        pending.push((*left_lhs, *right_lhs));
                        push_pairs(&mut pending, left_values, right_values)
                            && left_negated == right_negated
                            && left_comparisons == right_comparisons
                    }
                    (
                        Expr::Like {
                            lhs: left_lhs,
                            negated: left_negated,
                            operator: left_operator,
                            function: left_function,
                            argument_count: left_argument_count,
                            rhs: left_rhs,
                            escape: left_escape,
                        },
                        Expr::Like {
                            lhs: right_lhs,
                            negated: right_negated,
                            operator: right_operator,
                            function: right_function,
                            argument_count: right_argument_count,
                            rhs: right_rhs,
                            escape: right_escape,
                        },
                    ) => {
                        pending.push((*left_lhs, *right_lhs));
                        pending.push((*left_rhs, *right_rhs));
                        push_optional_pair(&mut pending, *left_escape, *right_escape)
                            && left_negated == right_negated
                            && left_operator == right_operator
                            && left_function == right_function
                            && left_argument_count == right_argument_count
                    }
                    (Expr::Row(left), Expr::Row(right))
                    | (Expr::Array(left), Expr::Array(right)) => {
                        push_pairs(&mut pending, left, right)
                    }
                    (
                        Expr::Subscript {
                            base: left_base,
                            index: left_index,
                        },
                        Expr::Subscript {
                            base: right_base,
                            index: right_index,
                        },
                    ) => {
                        pending.push((*left_base, *right_base));
                        pending.push((*left_index, *right_index));
                        true
                    }
                    (Expr::FieldAccess(left), Expr::FieldAccess(right)) => {
                        pending.push((left.base, right.base));
                        left.field_name.eq_ignore_ascii_case(&right.field_name)
                            && left.kind == right.kind
                            && left.container_type == right.container_type
                            && left.result_type == right.result_type
                    }
                    (
                        Expr::Subquery(SubqueryExpr::Scalar {
                            query: left_query,
                            output: left_output,
                        }),
                        Expr::Subquery(SubqueryExpr::Scalar {
                            query: right_query,
                            output: right_output,
                        }),
                    ) => left_query == right_query && left_output == right_output,
                    (
                        Expr::Subquery(SubqueryExpr::Row { query: left }),
                        Expr::Subquery(SubqueryExpr::Row { query: right }),
                    )
                    | (
                        Expr::Subquery(SubqueryExpr::Exists(left)),
                        Expr::Subquery(SubqueryExpr::Exists(right)),
                    ) => left == right,
                    (
                        Expr::Subquery(SubqueryExpr::In {
                            lhs: left_lhs,
                            query: left_query,
                            negated: left_negated,
                            comparison: left_comparison,
                        }),
                        Expr::Subquery(SubqueryExpr::In {
                            lhs: right_lhs,
                            query: right_query,
                            negated: right_negated,
                            comparison: right_comparison,
                        }),
                    ) => {
                        pending.push((*left_lhs, *right_lhs));
                        left_query == right_query
                            && left_negated == right_negated
                            && left_comparison == right_comparison
                    }
                    (
                        Expr::Raise {
                            action: left_action,
                            message: left_message,
                        },
                        Expr::Raise {
                            action: right_action,
                            message: right_message,
                        },
                    ) => {
                        left_action == right_action
                            && push_optional_pair(&mut pending, *left_message, *right_message)
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
    pub(crate) fn for_each(self, visitor: &mut impl FnMut(ExprRef<'a>)) {
        visitor(self);
        let result = self.walk((), &mut WalkVisitor { visit: visitor });
        if let Err(error) = result {
            match error {}
        }
    }

    /// Reduce this expression from leaves to root without using the call stack.
    /// Child values keep expression-child order, letting callers handle nodes
    /// whose result depends on more than one child.
    pub(crate) fn fold<T>(self, folder: &mut impl FnMut(ExprRef<'a>, &[T]) -> T) -> T {
        match self.walk(
            (),
            &mut FoldVisitor {
                fold: folder,
                output: std::marker::PhantomData,
            },
        ) {
            Ok(value) => value,
            Err(error) => match error {},
        }
    }

    /// Visit an expression iteratively with callbacks before each child and
    /// after all selected children. The pre-order callback chooses the
    /// context for a child or skips that child. The post-order callback
    /// reduces completed child values into the current node's value.
    pub(crate) fn walk<V: ExprVisitor<'a>>(
        self,
        root_context: V::Context,
        visitor: &mut V,
    ) -> Result<V::Output, V::Error> {
        if visitor.child(self, 0).is_none() {
            return visitor.post_order(self, root_context, &[]);
        }
        #[cfg(test)]
        tests::FRAME_STACKS_CREATED.with(|created| created.set(created.get() + 1));
        let mut frames = vec![ExprFrame::new(self, root_context)];
        loop {
            let frame = frames.last_mut().expect("root expression frame exists");
            if let Some((index, child)) = frame.next_child(visitor) {
                match visitor.pre_order(frame.expression, &mut frame.context, index, child)? {
                    ControlFlow::Continue(context) if visitor.child(child, 0).is_none() => {
                        let value = visitor.post_order(child, context, &[])?;
                        frame.child_values.push(value);
                    }
                    ControlFlow::Continue(context) => {
                        #[cfg(test)]
                        tests::CHILD_FRAMES_CREATED.with(|created| created.set(created.get() + 1));
                        frames.push(ExprFrame::new(child, context));
                    }
                    ControlFlow::Break(()) => {}
                }
                continue;
            }

            let frame = frames.pop().expect("completed expression frame exists");
            let value = visitor.post_order(frame.expression, frame.context, &frame.child_values)?;
            let Some(parent) = frames.last_mut() else {
                return Ok(value);
            };
            parent.child_values.push(value);
        }
    }
}

fn push_pairs(pending: &mut Vec<(ExprId, ExprId)>, left: &[ExprId], right: &[ExprId]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    pending.extend(left.iter().copied().zip(right.iter().copied()));
    true
}

fn push_optional_pair(
    pending: &mut Vec<(ExprId, ExprId)>,
    left: Option<ExprId>,
    right: Option<ExprId>,
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
    left: &Option<Box<CustomBinaryOperator>>,
    right: &Option<Box<CustomBinaryOperator>>,
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
    pending: &mut Vec<(ExprId, ExprId)>,
) -> bool {
    // Two calls of one aggregate (the same `AggregateId`) are the same
    // value; calls of different aggregates never are, even when they look
    // alike, because each is evaluated on its own.
    let same_evaluation = match (&left.evaluation, &right.evaluation) {
        (FunctionEvaluation::Scalar, FunctionEvaluation::Scalar) => true,
        (
            FunctionEvaluation::Aggregate { id: left_id, .. },
            FunctionEvaluation::Aggregate { id: right_id, .. },
        ) => left_id == right_id,
        _ => false,
    };
    left.function == right.function
        && same_evaluation
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
    pending: &mut Vec<(ExprId, ExprId)>,
) -> bool {
    match (left, right) {
        (FunctionArguments::Star, FunctionArguments::Star) => true,
        (
            FunctionArguments::Expressions {
                values: left_values,
                facts: left_facts,
                distinctness: left_distinctness,
                order_by: left_order,
            },
            FunctionArguments::Expressions {
                values: right_values,
                facts: right_facts,
                distinctness: right_distinctness,
                order_by: right_order,
            },
        ) => {
            push_pairs(pending, left_values, right_values)
                && left_facts == right_facts
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
    pending: &mut Vec<(ExprId, ExprId)>,
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
        pending.push((left.expr, right.expr));
    }
    true
}

fn child_from_exprs(expressions: &[ExprId], index: &mut usize) -> Option<ExprId> {
    if let Some(expression) = expressions.get(*index) {
        return Some(*expression);
    }
    *index -= expressions.len();
    None
}

fn child_from_optional_expr(expression: Option<ExprId>, index: &mut usize) -> Option<ExprId> {
    let expression = expression?;
    if *index == 0 {
        return Some(expression);
    }
    *index -= 1;
    None
}

fn child_from_order_terms(terms: &[OrderTerm], index: &mut usize) -> Option<ExprId> {
    if let Some(term) = terms.get(*index) {
        return Some(term.expr);
    }
    *index -= terms.len();
    None
}

fn child_from_optional_schema_call(
    call: Option<&BoundSchemaCall>,
    index: &mut usize,
) -> Option<ExprId> {
    child_from_schema_calls(call, index)
}

fn child_from_schema_calls<'expr>(
    calls: impl IntoIterator<Item = &'expr BoundSchemaCall>,
    index: &mut usize,
) -> Option<ExprId> {
    for call in calls {
        if let Some(child) = child_from_exprs(&call.arguments, index) {
            return Some(child);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    thread_local! {
        /// How many expression walks on this thread allocated a stack of frames.
        pub(super) static FRAME_STACKS_CREATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        /// How many frames expression walks on this thread pushed for children.
        pub(super) static CHILD_FRAMES_CREATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    use super::*;

    #[test]
    fn an_expression_is_constant_only_when_every_node_under_it_is() {
        let mut exprs = ExprArena::default();
        let add = |exprs: &mut ExprArena, lhs, rhs| {
            exprs.push(Expr::Binary {
                lhs,
                operator: Operator::Add,
                rhs,
                array_concat: false,
                custom: None,
                comparison: None,
            })
        };
        let one = exprs.push(Expr::Literal(Literal::Numeric("1".to_string())));
        let two = exprs.push(Expr::Literal(Literal::Numeric("2".to_string())));
        let column = exprs.push(Expr::column(SourceId::new(0), 1));
        let constant = add(&mut exprs, one, two);
        let reads_column = add(&mut exprs, constant, column);
        let negated = exprs.push(Expr::Unary {
            operator: UnaryOperator::Negative,
            expr: reads_column,
        });

        assert!(exprs.is_constant(exprs.at(constant)));
        assert!(!exprs.is_constant(exprs.at(reads_column)));
        assert!(!exprs.is_constant(exprs.at(negated)));
        let detached = Expr::Unary {
            operator: UnaryOperator::Negative,
            expr: constant,
        };
        assert!(exprs.is_constant(exprs.detached(&detached)));
    }

    #[test]
    fn walking_children_without_children_pushes_no_frames() {
        let mut exprs = ExprArena::default();
        let first = exprs.push(Expr::column(SourceId::new(0), 1));
        let negated = exprs.push(Expr::column(SourceId::new(0), 2));
        let unary = exprs.push(Expr::Unary {
            operator: UnaryOperator::Negative,
            expr: negated,
        });
        let last = exprs.push(Expr::column(SourceId::new(0), 3));
        let row = exprs.push(Expr::Row(vec![first, unary, last]));
        CHILD_FRAMES_CREATED.with(|created| created.set(0));

        let mut visited = Vec::new();
        exprs.at(row).for_each(&mut |expression| {
            visited.push(matches!(*expression, Expr::Column(_)));
        });

        assert_eq!(visited, [false, true, false, true, true]);
        assert_eq!(CHILD_FRAMES_CREATED.with(|created| created.get()), 1);
    }

    #[test]
    fn walking_an_expression_without_children_creates_no_frame_stack() {
        let mut exprs = ExprArena::default();
        let leaf = exprs.push(Expr::column(SourceId::new(0), 3));
        let lhs = exprs.push(Expr::column(SourceId::new(0), 1));
        let rhs = exprs.push(Expr::column(SourceId::new(0), 2));
        let binary = exprs.push(Expr::Binary {
            lhs,
            operator: Operator::Add,
            rhs,
            array_concat: false,
            custom: None,
            comparison: None,
        });
        FRAME_STACKS_CREATED.with(|created| created.set(0));

        let mut visited = 0;
        exprs.at(leaf).for_each(&mut |_| visited += 1);
        assert_eq!(visited, 1);
        assert_eq!(FRAME_STACKS_CREATED.with(|created| created.get()), 0);

        exprs.at(binary).for_each(&mut |_| visited += 1);
        assert_eq!(visited, 4);
        assert_eq!(FRAME_STACKS_CREATED.with(|created| created.get()), 1);
    }

    #[test]
    fn expression_frames_preserve_child_order_for_walk_and_fold() {
        let mut exprs = ExprArena::default();
        let two = exprs.push(Expr::Literal(Literal::Numeric("2".into())));
        let three = exprs.push(Expr::Literal(Literal::Numeric("3".into())));
        let negated = exprs.push(Expr::Unary {
            operator: UnaryOperator::Negative,
            expr: three,
        });
        let sum = exprs.push(Expr::Binary {
            lhs: two,
            operator: Operator::Add,
            rhs: negated,
            array_concat: false,
            custom: None,
            comparison: None,
        });
        let expression = exprs.at(sum);

        let mut visited = Vec::new();
        expression.for_each(&mut |expression| {
            visited.push(match *expression {
                Expr::Binary { .. } => "binary",
                Expr::Unary { .. } => "unary",
                Expr::Literal(_) => "literal",
                _ => unreachable!("test expression contains only binary, unary, and literals"),
            });
        });
        assert_eq!(visited, ["binary", "literal", "unary", "literal"]);

        let value = expression.fold(
            &mut |expression, children: &[i64]| match expression.node() {
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
            },
        );
        assert_eq!(value, -1);
    }
}
