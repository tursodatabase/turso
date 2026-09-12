//! See expressions and plan nodes as operators with children.
//!
//! A rule names an operator, such as `And` or `Filter`, and its children in
//! the order of the fields of its `define` in `ops.opt`. This module maps
//! `ast::Expr` and `LogicalPlan` to that view and builds nodes back from it.
//! A field that holds no node, such as a literal value or a join type, is a
//! private field. Rules pass private fields through unchanged.
//!
//! One-element parentheses are transparent: `(a = 1)` is the `Eq` node inside.
//! A node that a rule rebuilds loses them.

use smallvec::SmallVec;
use turso_parser::ast::{
    self, Distinctness as AstDistinctness, Expr, FunctionTail, LikeOperator, Literal, Name,
    Operator, SortedColumn, TableInternalId, Type, UnaryOperator,
};

use crate::translate::expr::truth_test_rhs;
use crate::translate::logical::{
    Aggregate, DependentJoin, DependentJoinKind, DerivedTable, Distinct, Filter, Join, Limit,
    LogicalPlan, Project, Scan, Sort, SortKey,
};
use crate::translate::plan::{
    self as plan, Distinctness, GroupBy, JoinInfo, JoinType, ResultSetColumn, WhereTerm,
};
use crate::{LimboError, Result};

macro_rules! ops {
    ($($variant:ident = $fields:literal),* $(,)?) => {
        /// Every operator that a rule can name. The number is the count of
        /// fields, which must match the `define` in `ops.opt`.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub(crate) enum Op {
            $($variant),*
        }

        impl Op {
            pub const ALL: &'static [Op] = &[$(Op::$variant),*];

            pub fn name(self) -> &'static str {
                match self {
                    $(Op::$variant => stringify!($variant)),*
                }
            }

            pub fn from_name(name: &str) -> Option<Op> {
                match name {
                    $(stringify!($variant) => Some(Op::$variant),)*
                    _ => None,
                }
            }

            pub fn field_count(self) -> usize {
                match self {
                    $(Op::$variant => $fields),*
                }
            }
        }
    };
}

ops! {
    Const = 1,
    Null = 0,
    True = 0,
    False = 0,
    Variable = 1,
    Placeholder = 1,
    Keyword = 1,
    Opaque = 1,
    Absent = 0,
    And = 2,
    Or = 2,
    Not = 1,
    Eq = 2,
    Ne = 2,
    Lt = 2,
    Le = 2,
    Gt = 2,
    Ge = 2,
    Is = 2,
    IsNot = 2,
    IsTrue = 1,
    IsFalse = 1,
    IsNotTrue = 1,
    IsNotFalse = 1,
    IsNull = 1,
    IsNotNull = 1,
    Plus = 2,
    Minus = 2,
    Mult = 2,
    Div = 2,
    Mod = 2,
    Concat = 2,
    BitAnd = 2,
    BitOr = 2,
    LShift = 2,
    RShift = 2,
    JsonExtract = 2,
    JsonExtractText = 2,
    ArrayContains = 2,
    ArrayOverlap = 2,
    UnaryMinus = 1,
    UnaryPlus = 1,
    BitNot = 1,
    In = 2,
    NotIn = 2,
    Between = 3,
    NotBetween = 3,
    Like = 3,
    NotLike = 3,
    Glob = 3,
    NotGlob = 3,
    Match = 3,
    NotMatch = 3,
    Regexp = 3,
    NotRegexp = 3,
    Case = 3,
    When = 2,
    Cast = 2,
    Collate = 2,
    Coalesce = 1,
    Function = 2,
    FunctionStar = 1,
    Tuple = 1,
    SubqueryResult = 2,
    FieldAccess = 2,
    OneRow = 0,
    Scan = 1,
    DerivedTable = 1,
    InnerJoin = 3,
    CrossJoin = 3,
    LeftJoin = 3,
    FullJoin = 3,
    SemiJoin = 3,
    AntiJoin = 3,
    DependentJoin = 3,
    Filter = 2,
    Term = 2,
    Aggregate = 2,
    Project = 2,
    Distinct = 2,
    Sort = 2,
    Limit = 2,
}

/// How the value of an expression is used where it stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Context {
    /// The value is only tested for truth, so `5` and `1` are the same.
    pub truth_value: bool,
    /// `NULL` has the same effect as `0`. This holds for a WHERE term and for
    /// the operands of `AND` and `OR` in one, but not under `NOT`.
    pub null_is_false: bool,
}

impl Context {
    pub const VALUE: Context = Context {
        truth_value: false,
        null_is_false: false,
    };
    pub const CONDITION: Context = Context {
        truth_value: true,
        null_is_false: true,
    };

    pub(crate) fn under_not() -> Context {
        Context {
            truth_value: true,
            null_is_false: false,
        }
    }

    pub(crate) fn under_and_or(self) -> Context {
        Context {
            truth_value: true,
            null_is_false: self.null_is_false,
        }
    }
}

/// A borrowed node or field, as a rule sees it.
#[derive(Clone, Copy)]
pub(crate) enum NodeRef<'a> {
    Expr(&'a Expr),
    Plan(&'a LogicalPlan),
    Term(&'a WhereTerm),
    Terms(&'a [WhereTerm]),
    Exprs(&'a [Box<Expr>]),
    When(&'a (Box<Expr>, Box<Expr>)),
    Whens(&'a [(Box<Expr>, Box<Expr>)]),
    Absent,
    Private(PrivateRef<'a>),
}

#[derive(Clone, Copy)]
pub(crate) enum PrivateRef<'a> {
    /// The whole expression of a leaf node, or of a node whose other parts
    /// are not children.
    Expr(&'a Expr),
    Type(&'a Option<Type>),
    Name(&'a Name),
    Function(FunctionRef<'a>),
    Scan(&'a Scan),
    DerivedTable(&'a DerivedTable),
    JoinInfo(&'a JoinInfo),
    DependentJoinKind(&'a DependentJoinKind),
    TermMarks {
        from_outer_join: Option<TableInternalId>,
        consumed: bool,
    },
    Aggregate {
        group_by: &'a Option<GroupBy>,
        aggregates: &'a [plan::Aggregate],
    },
    Columns(&'a [ResultSetColumn]),
    Distinctness(&'a Distinctness),
    SortKeys(&'a [SortKey]),
    Limit {
        limit: &'a Option<Box<Expr>>,
        offset: &'a Option<Box<Expr>>,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct FunctionRef<'a> {
    pub name: &'a Name,
    pub distinctness: Option<&'a AstDistinctness>,
    pub order_by: &'a [SortedColumn],
    pub within_group: &'a [SortedColumn],
    pub filter_over: &'a FunctionTail,
}

/// An owned node or field, as a rule builds it.
#[derive(Clone, Debug)]
pub(crate) enum Value {
    Expr(Box<Expr>),
    Plan(Box<LogicalPlan>),
    Term(Box<WhereTerm>),
    Terms(Vec<WhereTerm>),
    Exprs(Vec<Expr>),
    When(Box<(Expr, Expr)>),
    Whens(Vec<(Expr, Expr)>),
    Absent,
    Private(Box<Private>),
    /// A list written in a replace pattern.
    List(Vec<Value>),
    /// The results of a function that gives more than one.
    Tuple(Vec<Value>),
    Bool(bool),
    Op(Op),
    Str(String),
    Int(i64),
}

#[derive(Clone, Debug)]
pub(crate) enum Private {
    Expr(Expr),
    Type(Option<Type>),
    Name(Name),
    Function(FunctionPrivate),
    Scan(Scan),
    DerivedTable(DerivedTable),
    JoinInfo(JoinInfo),
    DependentJoinKind(DependentJoinKind),
    TermMarks {
        from_outer_join: Option<TableInternalId>,
        consumed: bool,
    },
    Aggregate {
        group_by: Option<GroupBy>,
        aggregates: Vec<plan::Aggregate>,
    },
    Columns(Vec<ResultSetColumn>),
    Distinctness(Distinctness),
    SortKeys(Vec<SortKey>),
    Limit {
        limit: Option<Box<Expr>>,
        offset: Option<Box<Expr>>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct FunctionPrivate {
    pub name: Name,
    pub distinctness: Option<AstDistinctness>,
    pub order_by: Vec<SortedColumn>,
    pub within_group: Vec<SortedColumn>,
    pub filter_over: FunctionTail,
}

pub(crate) type Children<'a> = SmallVec<[NodeRef<'a>; 4]>;

pub(crate) fn expr_value(expr: Expr) -> Value {
    Value::Expr(Box::new(expr))
}

pub(crate) fn term_value(term: WhereTerm) -> Value {
    Value::Term(Box::new(term))
}

pub(crate) fn strip_parens(expr: &Expr) -> &Expr {
    let mut expr = expr;
    while let Expr::Parenthesized(exprs) = expr {
        if exprs.len() != 1 {
            break;
        }
        expr = &exprs[0];
    }
    expr
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(strip_parens(expr), Expr::Literal(Literal::Null))
}

fn is_numeric_literal(expr: &Expr) -> bool {
    matches!(strip_parens(expr), Expr::Literal(Literal::Numeric(_)))
}

fn is_coalesce(name: &Name) -> bool {
    let name = name.as_str();
    name.eq_ignore_ascii_case("coalesce") || name.eq_ignore_ascii_case("ifnull")
}

impl<'a> NodeRef<'a> {
    /// The operator of a node. A list or a private field has none.
    pub fn op(self) -> Option<Op> {
        match self {
            NodeRef::Expr(expr) => Some(expr_op(expr)),
            NodeRef::Plan(plan) => Some(plan_op(plan)),
            NodeRef::Term(_) => Some(Op::Term),
            NodeRef::When(_) => Some(Op::When),
            NodeRef::Absent => Some(Op::Absent),
            NodeRef::Terms(_) | NodeRef::Exprs(_) | NodeRef::Whens(_) | NodeRef::Private(_) => None,
        }
    }

    /// The children of a node, in the order of the fields of its `define`.
    pub fn children(self) -> Children<'a> {
        match self {
            NodeRef::Expr(expr) => expr_children(strip_parens(expr)),
            NodeRef::Plan(plan) => plan_children(plan),
            NodeRef::Term(term) => SmallVec::from_slice(&[
                NodeRef::Expr(&term.expr),
                NodeRef::Private(PrivateRef::TermMarks {
                    from_outer_join: term.from_outer_join,
                    consumed: term.consumed,
                }),
            ]),
            NodeRef::When((condition, result)) => {
                SmallVec::from_slice(&[NodeRef::Expr(condition), NodeRef::Expr(result)])
            }
            NodeRef::Absent
            | NodeRef::Terms(_)
            | NodeRef::Exprs(_)
            | NodeRef::Whens(_)
            | NodeRef::Private(_) => SmallVec::new(),
        }
    }

    /// The items of a list node.
    /// The number of items of a list node, or nothing for another node.
    pub fn list_len(self) -> Option<usize> {
        match self {
            NodeRef::Terms(terms) => Some(terms.len()),
            NodeRef::Exprs(exprs) => Some(exprs.len()),
            NodeRef::Whens(whens) => Some(whens.len()),
            _ => None,
        }
    }

    pub fn list_item(self, index: usize) -> Option<NodeRef<'a>> {
        match self {
            NodeRef::Terms(terms) => terms.get(index).map(NodeRef::Term),
            NodeRef::Exprs(exprs) => exprs.get(index).map(|expr| NodeRef::Expr(expr)),
            NodeRef::Whens(whens) => whens.get(index).map(NodeRef::When),
            _ => None,
        }
    }

    pub fn to_value(self) -> Value {
        match self {
            NodeRef::Expr(expr) => expr_value(expr.clone()),
            NodeRef::Plan(plan) => Value::Plan(Box::new(plan.clone())),
            NodeRef::Term(term) => term_value(term.clone()),
            NodeRef::Terms(terms) => Value::Terms(terms.to_vec()),
            NodeRef::Exprs(exprs) => {
                Value::Exprs(exprs.iter().map(|expr| (**expr).clone()).collect())
            }
            NodeRef::When((condition, result)) => {
                Value::When(Box::new(((**condition).clone(), (**result).clone())))
            }
            NodeRef::Whens(whens) => Value::Whens(
                whens
                    .iter()
                    .map(|(condition, result)| ((**condition).clone(), (**result).clone()))
                    .collect(),
            ),
            NodeRef::Absent => Value::Absent,
            NodeRef::Private(private) => Value::Private(Box::new(private.to_value())),
        }
    }
}

impl PrivateRef<'_> {
    pub fn to_value(self) -> Private {
        match self {
            PrivateRef::Expr(expr) => Private::Expr(expr.clone()),
            PrivateRef::Type(type_name) => Private::Type(type_name.clone()),
            PrivateRef::Name(name) => Private::Name(name.clone()),
            PrivateRef::Function(function) => Private::Function(FunctionPrivate {
                name: function.name.clone(),
                distinctness: function.distinctness.cloned(),
                order_by: function.order_by.to_vec(),
                within_group: function.within_group.to_vec(),
                filter_over: function.filter_over.clone(),
            }),
            PrivateRef::Scan(scan) => Private::Scan(scan.clone()),
            PrivateRef::DerivedTable(derived) => Private::DerivedTable(derived.clone()),
            PrivateRef::JoinInfo(info) => Private::JoinInfo(info.clone()),
            PrivateRef::DependentJoinKind(kind) => Private::DependentJoinKind(kind.clone()),
            PrivateRef::TermMarks {
                from_outer_join,
                consumed,
            } => Private::TermMarks {
                from_outer_join,
                consumed,
            },
            PrivateRef::Aggregate {
                group_by,
                aggregates,
            } => Private::Aggregate {
                group_by: group_by.clone(),
                aggregates: aggregates.to_vec(),
            },
            PrivateRef::Columns(columns) => Private::Columns(columns.to_vec()),
            PrivateRef::Distinctness(distinctness) => Private::Distinctness(distinctness.clone()),
            PrivateRef::SortKeys(keys) => Private::SortKeys(keys.to_vec()),
            PrivateRef::Limit { limit, offset } => Private::Limit {
                limit: limit.clone(),
                offset: offset.clone(),
            },
        }
    }
}

pub(crate) fn expr_op(expr: &Expr) -> Op {
    match strip_parens(expr) {
        Expr::Literal(literal) => literal_op(literal),
        Expr::Unary(UnaryOperator::Negative, inner) if is_numeric_literal(inner) => Op::Const,
        Expr::Unary(op, _) => match op {
            UnaryOperator::Not => Op::Not,
            UnaryOperator::Negative => Op::UnaryMinus,
            UnaryOperator::Positive => Op::UnaryPlus,
            UnaryOperator::BitwiseNot => Op::BitNot,
        },
        Expr::Binary(_, op, right) => binary_op(*op, right),
        Expr::IsNull(_) => Op::IsNull,
        Expr::NotNull(_) => Op::IsNotNull,
        Expr::InList { not, .. } => {
            if *not {
                Op::NotIn
            } else {
                Op::In
            }
        }
        Expr::Between { not, .. } => {
            if *not {
                Op::NotBetween
            } else {
                Op::Between
            }
        }
        Expr::Like { not, op, .. } => match (op, not) {
            (LikeOperator::Like, false) => Op::Like,
            (LikeOperator::Like, true) => Op::NotLike,
            (LikeOperator::Glob, false) => Op::Glob,
            (LikeOperator::Glob, true) => Op::NotGlob,
            (LikeOperator::Match, false) => Op::Match,
            (LikeOperator::Match, true) => Op::NotMatch,
            (LikeOperator::Regexp, false) => Op::Regexp,
            (LikeOperator::Regexp, true) => Op::NotRegexp,
        },
        Expr::Case { .. } => Op::Case,
        Expr::Cast { .. } => Op::Cast,
        Expr::Collate(..) => Op::Collate,
        Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            within_group,
            filter_over,
        } => {
            if is_coalesce(name)
                && distinctness.is_none()
                && order_by.is_empty()
                && within_group.is_empty()
                && filter_over.filter_clause.is_none()
                && filter_over.over_clause.is_none()
                && args.len() >= 2
            {
                Op::Coalesce
            } else {
                Op::Function
            }
        }
        Expr::FunctionCallStar { .. } => Op::FunctionStar,
        Expr::Parenthesized(_) => Op::Tuple,
        Expr::Column { .. } | Expr::RowId { .. } => Op::Variable,
        Expr::Variable(_) => Op::Placeholder,
        Expr::SubqueryResult { .. } => Op::SubqueryResult,
        Expr::FieldAccess { .. } => Op::FieldAccess,
        Expr::Register(_)
        | Expr::DoublyQualified(..)
        | Expr::Exists(_)
        | Expr::Id(_)
        | Expr::InSelect { .. }
        | Expr::InTable { .. }
        | Expr::Name(_)
        | Expr::Qualified(..)
        | Expr::Raise(..)
        | Expr::Subquery(_)
        | Expr::Default
        | Expr::Array { .. }
        | Expr::Subscript { .. } => Op::Opaque,
    }
}

fn literal_op(literal: &Literal) -> Op {
    match literal {
        Literal::Null => Op::Null,
        Literal::True => Op::True,
        Literal::False => Op::False,
        Literal::Numeric(text) => match text.as_str() {
            "1" => Op::True,
            "0" => Op::False,
            _ => Op::Const,
        },
        Literal::String(_) | Literal::Blob(_) => Op::Const,
        Literal::Keyword(_)
        | Literal::CurrentDate
        | Literal::CurrentTime
        | Literal::CurrentTimestamp => Op::Keyword,
    }
}

fn binary_op(op: Operator, right: &Expr) -> Op {
    match op {
        Operator::And => Op::And,
        Operator::Or => Op::Or,
        Operator::Equals => Op::Eq,
        Operator::NotEquals => Op::Ne,
        Operator::Less => Op::Lt,
        Operator::LessEquals => Op::Le,
        Operator::Greater => Op::Gt,
        Operator::GreaterEquals => Op::Ge,
        Operator::Is => match truth_test_rhs(right) {
            Some(true) => Op::IsTrue,
            Some(false) => Op::IsFalse,
            None if is_null_literal(right) => Op::IsNull,
            None => Op::Is,
        },
        Operator::IsNot => match truth_test_rhs(right) {
            Some(true) => Op::IsNotTrue,
            Some(false) => Op::IsNotFalse,
            None if is_null_literal(right) => Op::IsNotNull,
            None => Op::IsNot,
        },
        Operator::Add => Op::Plus,
        Operator::Subtract => Op::Minus,
        Operator::Multiply => Op::Mult,
        Operator::Divide => Op::Div,
        Operator::Modulus => Op::Mod,
        Operator::Concat => Op::Concat,
        Operator::BitwiseAnd => Op::BitAnd,
        Operator::BitwiseOr => Op::BitOr,
        Operator::LeftShift => Op::LShift,
        Operator::RightShift => Op::RShift,
        Operator::ArrowRight => Op::JsonExtract,
        Operator::ArrowRightShift => Op::JsonExtractText,
        Operator::ArrayContains => Op::ArrayContains,
        Operator::ArrayOverlap => Op::ArrayOverlap,
        Operator::BitwiseNot => Op::Opaque,
    }
}

fn optional(expr: &Option<Box<Expr>>) -> NodeRef<'_> {
    match expr {
        Some(expr) => NodeRef::Expr(expr),
        None => NodeRef::Absent,
    }
}

/// `expr` has no one-element parentheses around it.
fn expr_children(expr: &Expr) -> Children<'_> {
    let mut children = SmallVec::new();
    match expr {
        Expr::Literal(Literal::Null | Literal::True | Literal::False) => {}
        Expr::Literal(Literal::Numeric(text)) if text == "1" || text == "0" => {}
        Expr::Literal(_) => children.push(NodeRef::Private(PrivateRef::Expr(expr))),
        Expr::Unary(UnaryOperator::Negative, inner) if is_numeric_literal(inner) => {
            children.push(NodeRef::Private(PrivateRef::Expr(expr)))
        }
        Expr::Unary(_, inner) => children.push(NodeRef::Expr(inner)),
        Expr::Binary(left, Operator::Is | Operator::IsNot, right)
            if truth_test_rhs(right).is_some() || is_null_literal(right) =>
        {
            children.push(NodeRef::Expr(left))
        }
        Expr::Binary(_, Operator::BitwiseNot, _) => {
            children.push(NodeRef::Private(PrivateRef::Expr(expr)))
        }
        Expr::Binary(left, _, right) => {
            children.push(NodeRef::Expr(left));
            children.push(NodeRef::Expr(right));
        }
        Expr::IsNull(inner) | Expr::NotNull(inner) => children.push(NodeRef::Expr(inner)),
        Expr::InList { lhs, rhs, .. } => {
            children.push(NodeRef::Expr(lhs));
            children.push(NodeRef::Exprs(rhs));
        }
        Expr::Between {
            lhs, start, end, ..
        } => {
            children.push(NodeRef::Expr(lhs));
            children.push(NodeRef::Expr(start));
            children.push(NodeRef::Expr(end));
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            children.push(NodeRef::Expr(lhs));
            children.push(NodeRef::Expr(rhs));
            children.push(optional(escape));
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            children.push(optional(base));
            children.push(NodeRef::Whens(when_then_pairs));
            children.push(optional(else_expr));
        }
        Expr::Cast {
            expr: inner,
            type_name,
        } => {
            children.push(NodeRef::Expr(inner));
            children.push(NodeRef::Private(PrivateRef::Type(type_name)));
        }
        Expr::Collate(inner, name) => {
            children.push(NodeRef::Expr(inner));
            children.push(NodeRef::Private(PrivateRef::Name(name)));
        }
        Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            within_group,
            filter_over,
        } => {
            children.push(NodeRef::Exprs(args));
            if expr_op(expr) == Op::Function {
                children.push(NodeRef::Private(PrivateRef::Function(FunctionRef {
                    name,
                    distinctness: distinctness.as_ref(),
                    order_by,
                    within_group,
                    filter_over,
                })));
            }
        }
        Expr::FunctionCallStar { name, filter_over } => {
            children.push(NodeRef::Private(PrivateRef::Function(FunctionRef {
                name,
                distinctness: None,
                order_by: &[],
                within_group: &[],
                filter_over,
            })));
        }
        Expr::Parenthesized(exprs) => children.push(NodeRef::Exprs(exprs)),
        Expr::SubqueryResult { lhs, .. } => {
            children.push(optional(lhs));
            children.push(NodeRef::Private(PrivateRef::Expr(expr)));
        }
        Expr::FieldAccess { base, .. } => {
            children.push(NodeRef::Expr(base));
            children.push(NodeRef::Private(PrivateRef::Expr(expr)));
        }
        Expr::Column { .. }
        | Expr::RowId { .. }
        | Expr::Variable(_)
        | Expr::Register(_)
        | Expr::DoublyQualified(..)
        | Expr::Exists(_)
        | Expr::Id(_)
        | Expr::InSelect { .. }
        | Expr::InTable { .. }
        | Expr::Name(_)
        | Expr::Qualified(..)
        | Expr::Raise(..)
        | Expr::Subquery(_)
        | Expr::Default
        | Expr::Array { .. }
        | Expr::Subscript { .. } => children.push(NodeRef::Private(PrivateRef::Expr(expr))),
    }
    children
}

/// The expression children of a node with the context each one is used in.
/// The context of child `index` of a node with operator `op` that stands
/// in `context`, for a node that a rule builds. The condition of a `When`
/// gets the context of the `When` itself, which the `Case` decides.
pub(crate) fn child_context(op: Op, index: usize, context: Context) -> Context {
    match op {
        Op::And | Op::Or => context.under_and_or(),
        Op::Not => Context::under_not(),
        Op::When if index == 0 => context,
        _ => Context::VALUE,
    }
}

pub(crate) fn expr_children_mut(
    expr: &mut Expr,
    context: Context,
) -> SmallVec<[(&mut Expr, Context); 4]> {
    let mut children: SmallVec<[(&mut Expr, Context); 4]> = SmallVec::new();
    match expr {
        Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                children.push((&mut exprs[0], context));
            } else {
                for inner in exprs {
                    children.push((inner, Context::VALUE));
                }
            }
        }
        Expr::Binary(left, Operator::And | Operator::Or, right) => {
            children.push((left, context.under_and_or()));
            children.push((right, context.under_and_or()));
        }
        Expr::Binary(left, _, right) => {
            children.push((left, Context::VALUE));
            children.push((right, Context::VALUE));
        }
        Expr::Unary(UnaryOperator::Not, inner) => children.push((inner, Context::under_not())),
        Expr::Unary(_, inner)
        | Expr::IsNull(inner)
        | Expr::NotNull(inner)
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate(inner, _) => children.push((inner, Context::VALUE)),
        Expr::InList { lhs, rhs, .. } => {
            children.push((lhs, Context::VALUE));
            for inner in rhs {
                children.push((inner, Context::VALUE));
            }
        }
        Expr::Between {
            lhs, start, end, ..
        } => {
            children.push((lhs, Context::VALUE));
            children.push((start, Context::VALUE));
            children.push((end, Context::VALUE));
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            children.push((lhs, Context::VALUE));
            children.push((rhs, Context::VALUE));
            if let Some(escape) = escape {
                children.push((escape, Context::VALUE));
            }
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            let condition_context = if base.is_some() {
                Context::VALUE
            } else {
                Context::CONDITION
            };
            if let Some(base) = base {
                children.push((base, Context::VALUE));
            }
            for (condition, result) in when_then_pairs {
                children.push((condition, condition_context));
                children.push((result, Context::VALUE));
            }
            if let Some(else_expr) = else_expr {
                children.push((else_expr, Context::VALUE));
            }
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                children.push((arg, Context::VALUE));
            }
        }
        Expr::SubqueryResult { lhs: Some(lhs), .. } => children.push((lhs, Context::VALUE)),
        Expr::FieldAccess { base, .. } => children.push((base, Context::VALUE)),
        Expr::Raise(_, Some(inner)) => children.push((inner, Context::VALUE)),
        Expr::InTable { lhs, args, .. } => {
            children.push((lhs, Context::VALUE));
            for arg in args {
                children.push((arg, Context::VALUE));
            }
        }
        Expr::InSelect { lhs, .. } => children.push((lhs, Context::VALUE)),
        Expr::Literal(_)
        | Expr::FunctionCallStar { .. }
        | Expr::Column { .. }
        | Expr::RowId { .. }
        | Expr::Variable(_)
        | Expr::SubqueryResult { lhs: None, .. }
        | Expr::Raise(_, None)
        | Expr::Register(_)
        | Expr::DoublyQualified(..)
        | Expr::Exists(_)
        | Expr::Id(_)
        | Expr::Name(_)
        | Expr::Qualified(..)
        | Expr::Subquery(_)
        | Expr::Default
        | Expr::Array { .. }
        | Expr::Subscript { .. } => {}
    }
    children
}

pub(crate) fn plan_op(plan: &LogicalPlan) -> Op {
    match plan {
        LogicalPlan::OneRow => Op::OneRow,
        LogicalPlan::Scan(_) => Op::Scan,
        LogicalPlan::DerivedTable(_) => Op::DerivedTable,
        LogicalPlan::Join(join) => match join.info.join_type {
            JoinType::Inner if join.info.no_reorder => Op::CrossJoin,
            JoinType::Inner => Op::InnerJoin,
            JoinType::LeftOuter => Op::LeftJoin,
            JoinType::FullOuter => Op::FullJoin,
            JoinType::Semi => Op::SemiJoin,
            JoinType::Anti => Op::AntiJoin,
        },
        LogicalPlan::DependentJoin(_) => Op::DependentJoin,
        LogicalPlan::Filter(_) => Op::Filter,
        LogicalPlan::Aggregate(_) => Op::Aggregate,
        LogicalPlan::Project(_) => Op::Project,
        LogicalPlan::Distinct(_) => Op::Distinct,
        LogicalPlan::Sort(_) => Op::Sort,
        LogicalPlan::Limit(_) => Op::Limit,
    }
}

fn plan_children(plan: &LogicalPlan) -> Children<'_> {
    let mut children = SmallVec::new();
    match plan {
        LogicalPlan::OneRow => {}
        LogicalPlan::Scan(scan) => children.push(NodeRef::Private(PrivateRef::Scan(scan))),
        LogicalPlan::DerivedTable(derived) => {
            children.push(NodeRef::Private(PrivateRef::DerivedTable(derived)))
        }
        LogicalPlan::Join(join) => {
            children.push(NodeRef::Plan(&join.left));
            children.push(NodeRef::Plan(&join.right));
            children.push(NodeRef::Private(PrivateRef::JoinInfo(&join.info)));
        }
        LogicalPlan::DependentJoin(join) => {
            children.push(NodeRef::Plan(&join.left));
            children.push(NodeRef::Plan(&join.right));
            children.push(NodeRef::Private(PrivateRef::DependentJoinKind(&join.kind)));
        }
        LogicalPlan::Filter(filter) => {
            children.push(NodeRef::Plan(&filter.input));
            children.push(NodeRef::Terms(&filter.terms));
        }
        LogicalPlan::Aggregate(aggregate) => {
            children.push(NodeRef::Plan(&aggregate.input));
            children.push(NodeRef::Private(PrivateRef::Aggregate {
                group_by: &aggregate.group_by,
                aggregates: &aggregate.aggregates,
            }));
        }
        LogicalPlan::Project(project) => {
            children.push(NodeRef::Plan(&project.input));
            children.push(NodeRef::Private(PrivateRef::Columns(&project.columns)));
        }
        LogicalPlan::Distinct(distinct) => {
            children.push(NodeRef::Plan(&distinct.input));
            children.push(NodeRef::Private(PrivateRef::Distinctness(
                &distinct.distinctness,
            )));
        }
        LogicalPlan::Sort(sort) => {
            children.push(NodeRef::Plan(&sort.input));
            children.push(NodeRef::Private(PrivateRef::SortKeys(&sort.keys)));
        }
        LogicalPlan::Limit(limit) => {
            children.push(NodeRef::Plan(&limit.input));
            children.push(NodeRef::Private(PrivateRef::Limit {
                limit: &limit.limit,
                offset: &limit.offset,
            }));
        }
    }
    children
}

fn engine_error(text: String) -> LimboError {
    LimboError::InternalError(format!("logical plan rules: {text}"))
}

impl Value {
    pub fn kind(&self) -> &'static str {
        match self {
            Value::Expr(_) => "an expression",
            Value::Plan(_) => "a plan node",
            Value::Term(_) => "a filter term",
            Value::Terms(_) => "a list of filter terms",
            Value::Exprs(_) => "a list of expressions",
            Value::When(_) => "a CASE branch",
            Value::Whens(_) => "a list of CASE branches",
            Value::Absent => "a missing child",
            Value::Private(_) => "a private field",
            Value::List(_) => "a list",
            Value::Tuple(_) => "several results",
            Value::Bool(_) => "a boolean",
            Value::Op(_) => "an operator name",
            Value::Str(_) => "a string",
            Value::Int(_) => "a number",
        }
    }

    pub fn into_expr(self) -> Result<Expr> {
        let value = match self {
            Value::Private(private) => match *private {
                Private::Expr(expr) => return Ok(expr),
                other => Value::Private(Box::new(other)),
            },
            other => other,
        };
        match value {
            Value::Expr(expr) => Ok(*expr),
            other => Err(engine_error(format!(
                "expected an expression, got {}",
                other.kind()
            ))),
        }
    }

    pub fn into_optional_expr(self) -> Result<Option<Expr>> {
        match self {
            Value::Absent => Ok(None),
            other => other.into_expr().map(Some),
        }
    }

    pub fn into_plan(self) -> Result<LogicalPlan> {
        match self {
            Value::Plan(plan) => Ok(*plan),
            other => Err(engine_error(format!(
                "expected a plan node, got {}",
                other.kind()
            ))),
        }
    }

    pub fn into_exprs(self) -> Result<Vec<Expr>> {
        match self {
            Value::Exprs(exprs) => Ok(exprs),
            Value::List(items) => items.into_iter().map(Value::into_expr).collect(),
            other => Err(engine_error(format!(
                "expected a list of expressions, got {}",
                other.kind()
            ))),
        }
    }

    pub fn into_terms(self) -> Result<Vec<WhereTerm>> {
        match self {
            Value::Terms(terms) => Ok(terms),
            Value::List(items) => items
                .into_iter()
                .map(|item| match item {
                    Value::Term(term) => Ok(*term),
                    other => Err(engine_error(format!(
                        "expected a filter term, got {}",
                        other.kind()
                    ))),
                })
                .collect(),
            other => Err(engine_error(format!(
                "expected a list of filter terms, got {}",
                other.kind()
            ))),
        }
    }

    pub fn into_whens(self) -> Result<Vec<(Expr, Expr)>> {
        match self {
            Value::Whens(whens) => Ok(whens),
            Value::List(items) => items
                .into_iter()
                .map(|item| match item {
                    Value::When(when) => Ok(*when),
                    other => Err(engine_error(format!(
                        "expected a CASE branch, got {}",
                        other.kind()
                    ))),
                })
                .collect(),
            other => Err(engine_error(format!(
                "expected a list of CASE branches, got {}",
                other.kind()
            ))),
        }
    }

    pub fn into_private(self) -> Result<Private> {
        match self {
            Value::Private(private) => Ok(*private),
            Value::Expr(expr) => Ok(Private::Expr(*expr)),
            other => Err(engine_error(format!(
                "expected a private field, got {}",
                other.kind()
            ))),
        }
    }
}

fn binary(left: Value, op: Operator, right: Value) -> Result<Value> {
    let left = left.into_expr()?;
    let mut right = right.into_expr()?;
    if matches!(op, Operator::Is | Operator::IsNot) {
        if let Some(truth) = truth_test_rhs(&right) {
            right = Expr::Literal(Literal::Numeric(if truth { "1" } else { "0" }.to_string()));
        }
    }
    Ok(expr_value(Expr::Binary(
        Box::new(left),
        op,
        Box::new(right),
    )))
}

fn unary(op: UnaryOperator, input: Value) -> Result<Value> {
    Ok(expr_value(Expr::Unary(op, Box::new(input.into_expr()?))))
}

fn truth_test(input: Value, op: Operator, truth: bool) -> Result<Value> {
    let literal = if truth { Literal::True } else { Literal::False };
    Ok(expr_value(Expr::Binary(
        Box::new(input.into_expr()?),
        op,
        Box::new(Expr::Literal(literal)),
    )))
}

fn like(op: LikeOperator, not: bool, mut args: std::vec::IntoIter<Value>) -> Result<Value> {
    let lhs = args.next().expect("checked: three arguments").into_expr()?;
    let rhs = args.next().expect("checked: three arguments").into_expr()?;
    let escape = args
        .next()
        .expect("checked: three arguments")
        .into_optional_expr()?;
    Ok(expr_value(Expr::Like {
        lhs: Box::new(lhs),
        not,
        op,
        rhs: Box::new(rhs),
        escape: escape.map(Box::new),
    }))
}

fn join(
    join_type: JoinType,
    no_reorder: bool,
    mut args: std::vec::IntoIter<Value>,
) -> Result<Value> {
    let left = args.next().expect("checked: three arguments").into_plan()?;
    let right = args.next().expect("checked: three arguments").into_plan()?;
    let mut info = match args
        .next()
        .expect("checked: three arguments")
        .into_private()?
    {
        Private::JoinInfo(info) => info,
        other => {
            return Err(engine_error(format!(
                "expected join information, got {other:?}"
            )))
        }
    };
    info.join_type = join_type;
    info.no_reorder = no_reorder;
    Ok(Value::Plan(Box::new(LogicalPlan::Join(Join {
        left: Box::new(left),
        right: Box::new(right),
        info,
    }))))
}

fn unary_plan(op: Op, input: Value, private: Value) -> Result<Value> {
    let input = Box::new(input.into_plan()?);
    let private = private.into_private()?;
    let plan = match (op, private) {
        (
            Op::Aggregate,
            Private::Aggregate {
                group_by,
                aggregates,
            },
        ) => LogicalPlan::Aggregate(Aggregate {
            input,
            group_by,
            aggregates,
        }),
        (Op::Project, Private::Columns(columns)) => {
            LogicalPlan::Project(Project { input, columns })
        }
        (Op::Distinct, Private::Distinctness(distinctness)) => LogicalPlan::Distinct(Distinct {
            input,
            distinctness,
        }),
        (Op::Sort, Private::SortKeys(keys)) => LogicalPlan::Sort(Sort { input, keys }),
        (Op::Limit, Private::Limit { limit, offset }) => LogicalPlan::Limit(Limit {
            input,
            limit,
            offset,
        }),
        (op, private) => {
            return Err(engine_error(format!(
                "{} cannot be built from {private:?}",
                op.name()
            )))
        }
    };
    Ok(Value::Plan(Box::new(plan)))
}

/// Build a node from its operator and its children.
pub(crate) fn construct(op: Op, args: Vec<Value>) -> Result<Value> {
    if args.len() != op.field_count() {
        return Err(engine_error(format!(
            "{} takes {} children, got {}",
            op.name(),
            op.field_count(),
            args.len()
        )));
    }
    let mut args = args.into_iter();
    let mut next = || args.next().expect("checked: the argument count");
    let value = match op {
        Op::Const | Op::Variable | Op::Placeholder | Op::Keyword | Op::Opaque => {
            expr_value(next().into_expr()?)
        }
        Op::Null => expr_value(Expr::Literal(Literal::Null)),
        Op::True => expr_value(Expr::Literal(Literal::Numeric("1".to_string()))),
        Op::False => expr_value(Expr::Literal(Literal::Numeric("0".to_string()))),
        Op::Absent => Value::Absent,
        Op::And => binary(next(), Operator::And, next())?,
        Op::Or => binary(next(), Operator::Or, next())?,
        Op::Not => unary(UnaryOperator::Not, next())?,
        Op::Eq => binary(next(), Operator::Equals, next())?,
        Op::Ne => binary(next(), Operator::NotEquals, next())?,
        Op::Lt => binary(next(), Operator::Less, next())?,
        Op::Le => binary(next(), Operator::LessEquals, next())?,
        Op::Gt => binary(next(), Operator::Greater, next())?,
        Op::Ge => binary(next(), Operator::GreaterEquals, next())?,
        Op::Is => binary(next(), Operator::Is, next())?,
        Op::IsNot => binary(next(), Operator::IsNot, next())?,
        Op::IsTrue => truth_test(next(), Operator::Is, true)?,
        Op::IsFalse => truth_test(next(), Operator::Is, false)?,
        Op::IsNotTrue => truth_test(next(), Operator::IsNot, true)?,
        Op::IsNotFalse => truth_test(next(), Operator::IsNot, false)?,
        Op::IsNull => expr_value(Expr::Binary(
            Box::new(next().into_expr()?),
            Operator::Is,
            Box::new(Expr::Literal(Literal::Null)),
        )),
        Op::IsNotNull => expr_value(Expr::Binary(
            Box::new(next().into_expr()?),
            Operator::IsNot,
            Box::new(Expr::Literal(Literal::Null)),
        )),
        Op::Plus => binary(next(), Operator::Add, next())?,
        Op::Minus => binary(next(), Operator::Subtract, next())?,
        Op::Mult => binary(next(), Operator::Multiply, next())?,
        Op::Div => binary(next(), Operator::Divide, next())?,
        Op::Mod => binary(next(), Operator::Modulus, next())?,
        Op::Concat => binary(next(), Operator::Concat, next())?,
        Op::BitAnd => binary(next(), Operator::BitwiseAnd, next())?,
        Op::BitOr => binary(next(), Operator::BitwiseOr, next())?,
        Op::LShift => binary(next(), Operator::LeftShift, next())?,
        Op::RShift => binary(next(), Operator::RightShift, next())?,
        Op::JsonExtract => binary(next(), Operator::ArrowRight, next())?,
        Op::JsonExtractText => binary(next(), Operator::ArrowRightShift, next())?,
        Op::ArrayContains => binary(next(), Operator::ArrayContains, next())?,
        Op::ArrayOverlap => binary(next(), Operator::ArrayOverlap, next())?,
        Op::UnaryMinus => unary(UnaryOperator::Negative, next())?,
        Op::UnaryPlus => unary(UnaryOperator::Positive, next())?,
        Op::BitNot => unary(UnaryOperator::BitwiseNot, next())?,
        Op::In | Op::NotIn => {
            let lhs = next().into_expr()?;
            let rhs = next().into_exprs()?;
            expr_value(Expr::InList {
                lhs: Box::new(lhs),
                not: op == Op::NotIn,
                rhs: rhs.into_iter().map(Box::new).collect(),
            })
        }
        Op::Between | Op::NotBetween => {
            let lhs = next().into_expr()?;
            let start = next().into_expr()?;
            let end = next().into_expr()?;
            expr_value(Expr::Between {
                lhs: Box::new(lhs),
                not: op == Op::NotBetween,
                start: Box::new(start),
                end: Box::new(end),
            })
        }
        Op::Like => like(LikeOperator::Like, false, args)?,
        Op::NotLike => like(LikeOperator::Like, true, args)?,
        Op::Glob => like(LikeOperator::Glob, false, args)?,
        Op::NotGlob => like(LikeOperator::Glob, true, args)?,
        Op::Match => like(LikeOperator::Match, false, args)?,
        Op::NotMatch => like(LikeOperator::Match, true, args)?,
        Op::Regexp => like(LikeOperator::Regexp, false, args)?,
        Op::NotRegexp => like(LikeOperator::Regexp, true, args)?,
        Op::Case => {
            let base = next().into_optional_expr()?;
            let whens = next().into_whens()?;
            let else_expr = next().into_optional_expr()?;
            expr_value(Expr::Case {
                base: base.map(Box::new),
                when_then_pairs: whens
                    .into_iter()
                    .map(|(condition, result)| (Box::new(condition), Box::new(result)))
                    .collect(),
                else_expr: else_expr.map(Box::new),
            })
        }
        Op::When => {
            let condition = next().into_expr()?;
            let result = next().into_expr()?;
            Value::When(Box::new((condition, result)))
        }
        Op::Cast => {
            let inner = next().into_expr()?;
            let Private::Type(type_name) = next().into_private()? else {
                return Err(engine_error("Cast needs a type".to_string()));
            };
            expr_value(Expr::Cast {
                expr: Box::new(inner),
                type_name,
            })
        }
        Op::Collate => {
            let inner = next().into_expr()?;
            let Private::Name(name) = next().into_private()? else {
                return Err(engine_error("Collate needs a collation name".to_string()));
            };
            expr_value(Expr::Collate(Box::new(inner), name))
        }
        Op::Coalesce => {
            let mut exprs = next().into_exprs()?;
            match exprs.len() {
                0 => return Err(engine_error("Coalesce needs arguments".to_string())),
                1 => expr_value(exprs.remove(0)),
                _ => expr_value(Expr::FunctionCall {
                    name: Name::exact("coalesce".to_string()),
                    distinctness: None,
                    args: exprs.into_iter().map(Box::new).collect(),
                    order_by: Vec::new(),
                    within_group: Vec::new(),
                    filter_over: FunctionTail {
                        filter_clause: None,
                        over_clause: None,
                    },
                }),
            }
        }
        Op::Function => {
            let exprs = next().into_exprs()?;
            let Private::Function(function) = next().into_private()? else {
                return Err(engine_error("Function needs its private field".to_string()));
            };
            expr_value(Expr::FunctionCall {
                name: function.name,
                distinctness: function.distinctness,
                args: exprs.into_iter().map(Box::new).collect(),
                order_by: function.order_by,
                within_group: function.within_group,
                filter_over: function.filter_over,
            })
        }
        Op::FunctionStar => {
            let Private::Function(function) = next().into_private()? else {
                return Err(engine_error(
                    "FunctionStar needs its private field".to_string(),
                ));
            };
            expr_value(Expr::FunctionCallStar {
                name: function.name,
                filter_over: function.filter_over,
            })
        }
        Op::Tuple => expr_value(Expr::Parenthesized(
            next().into_exprs()?.into_iter().map(Box::new).collect(),
        )),
        Op::SubqueryResult => {
            let lhs = next().into_optional_expr()?;
            let mut result = next().into_expr()?;
            let Expr::SubqueryResult { lhs: slot, .. } = &mut result else {
                return Err(engine_error(
                    "SubqueryResult needs its private field".to_string(),
                ));
            };
            *slot = lhs.map(Box::new);
            expr_value(result)
        }
        Op::FieldAccess => {
            let base = next().into_expr()?;
            let mut result = next().into_expr()?;
            let Expr::FieldAccess { base: slot, .. } = &mut result else {
                return Err(engine_error(
                    "FieldAccess needs its private field".to_string(),
                ));
            };
            *slot = Box::new(base);
            expr_value(result)
        }
        Op::OneRow => Value::Plan(Box::new(LogicalPlan::OneRow)),
        Op::Scan => match next().into_private()? {
            Private::Scan(scan) => Value::Plan(Box::new(LogicalPlan::Scan(scan))),
            other => return Err(engine_error(format!("Scan cannot be built from {other:?}"))),
        },
        Op::DerivedTable => match next().into_private()? {
            Private::DerivedTable(derived) => {
                Value::Plan(Box::new(LogicalPlan::DerivedTable(derived)))
            }
            other => {
                return Err(engine_error(format!(
                    "DerivedTable cannot be built from {other:?}"
                )))
            }
        },
        Op::InnerJoin => join(JoinType::Inner, false, args)?,
        Op::CrossJoin => join(JoinType::Inner, true, args)?,
        Op::LeftJoin => join(JoinType::LeftOuter, false, args)?,
        Op::FullJoin => join(JoinType::FullOuter, false, args)?,
        Op::SemiJoin => join(JoinType::Semi, false, args)?,
        Op::AntiJoin => join(JoinType::Anti, false, args)?,
        Op::DependentJoin => {
            let left = next().into_plan()?;
            let right = next().into_plan()?;
            let Private::DependentJoinKind(kind) = next().into_private()? else {
                return Err(engine_error("DependentJoin needs its kind".to_string()));
            };
            Value::Plan(Box::new(LogicalPlan::DependentJoin(DependentJoin {
                left: Box::new(left),
                right: Box::new(right),
                kind,
            })))
        }
        Op::Filter => {
            let input = next().into_plan()?;
            let terms = next().into_terms()?;
            Value::Plan(Box::new(LogicalPlan::Filter(Filter {
                input: Box::new(input),
                terms,
            })))
        }
        Op::Term => {
            let expr = next().into_expr()?;
            let Private::TermMarks {
                from_outer_join,
                consumed,
            } = next().into_private()?
            else {
                return Err(engine_error("Term needs its marks".to_string()));
            };
            term_value(WhereTerm {
                expr,
                from_outer_join,
                consumed,
            })
        }
        Op::Aggregate | Op::Project | Op::Distinct | Op::Sort | Op::Limit => {
            let input = next();
            let private = next();
            unary_plan(op, input, private)?
        }
    };
    Ok(value)
}

impl From<ast::Expr> for Value {
    fn from(expr: ast::Expr) -> Self {
        expr_value(expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The engine moves values around on every rule attempt, so the large
    /// kinds are boxed to keep a value small.
    #[test]
    fn a_value_stays_small() {
        assert!(std::mem::size_of::<Value>() <= 288);
        assert!(std::mem::size_of::<NodeRef>() <= 64);
    }
}
