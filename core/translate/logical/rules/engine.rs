//! Run the rules on expressions and plan nodes.
//!
//! The build script compiles the rule files and generates one function per
//! rule, plus a dispatch that tries the rules of an operator in the order of
//! the files. To normalize a node, the engine normalizes the children first,
//! then tries the rules of the node's operator until no rule matches.
//!
//! A match binds variables to borrowed nodes. The replacement is built from
//! clones of the bound nodes, so a rule that keeps a large subtree copies it.
//! A replacement is normalized while it is built: each node that a pattern
//! constructs gets the rules of its operator as soon as its children exist,
//! and a bound subtree keeps the form it already has. A function written in
//! Rust that builds new nodes applies the rules to them through the context
//! it gets, so no part of a replacement is visited twice.

use turso_parser::ast::{Expr, Operator, TableInternalId};

use crate::translate::emitter::Resolver;
use crate::translate::logical::LogicalPlan;
use crate::{LimboError, Result};

use super::funcs;
use super::nodes::{self, Children, Context, NodeRef, Op, PrivateRef, Value};

/// The most rule applications on one node before the engine gives up.
const MAX_STEPS: usize = 256;

/// The rules of `rules/*.opt`.
pub(crate) fn rule_set() -> &'static RuleSet {
    &RuleSet
}

/// What a rule can read while it runs.
pub(crate) struct EngineContext<'a, 'r> {
    pub resolver: Option<&'a Resolver<'r>>,
    pub rules: &'a RuleSet,
    /// The virtual tables in scope. A comparison of one of their columns
    /// with NULL is an argument for the table, not a condition, so the
    /// rules keep it.
    pub virtual_tables: &'a [TableInternalId],
}

impl EngineContext<'_, '_> {
    /// Apply the rules to an expression that a function built from
    /// normalized parts, until no rule matches.
    pub fn settle(&self, expr: &mut Expr, context: Context) -> Result<()> {
        self.rules.settle_expr(self, expr, context)?;
        Ok(())
    }

    /// `left AND right` with the rules applied. The operands must be
    /// normalized for the context of an AND operand.
    pub fn and(&self, left: Expr, right: Expr, context: Context) -> Result<Expr> {
        let mut expr = Expr::Binary(Box::new(left), Operator::And, Box::new(right));
        self.settle(&mut expr, context)?;
        Ok(expr)
    }

    /// `left OR right` with the rules applied. The operands must be
    /// normalized for the context of an OR operand.
    pub fn or(&self, left: Expr, right: Expr, context: Context) -> Result<Expr> {
        let mut expr = Expr::Binary(Box::new(left), Operator::Or, Box::new(right));
        self.settle(&mut expr, context)?;
        Ok(expr)
    }
}

/// An argument of a function written in Rust.
pub(crate) enum ArgRef<'a, 'b> {
    Node(NodeRef<'a>),
    Value(&'b Value),
    Op(Op),
    Int(i64),
}

impl ArgRef<'_, '_> {
    pub(super) fn value(&self) -> Option<&Value> {
        match self {
            ArgRef::Node(_) | ArgRef::Op(_) | ArgRef::Int(_) => None,
            ArgRef::Value(value) => Some(value),
        }
    }

    pub fn expr(&self) -> Option<&Expr> {
        match self {
            ArgRef::Node(NodeRef::Expr(expr)) => Some(expr),
            ArgRef::Node(NodeRef::Private(PrivateRef::Expr(expr))) => Some(expr),
            ArgRef::Node(_) => None,
            _ => match self.value()? {
                Value::Expr(expr) => Some(expr.as_ref()),
                Value::Private(private) => match private.as_ref() {
                    nodes::Private::Expr(expr) => Some(expr),
                    _ => None,
                },
                _ => None,
            },
        }
    }

    pub fn op(&self) -> Option<Op> {
        match self {
            ArgRef::Node(node) => node.op(),
            ArgRef::Op(op) => Some(*op),
            _ => match self.value()? {
                Value::Op(op) => Some(*op),
                Value::Expr(expr) => Some(nodes::expr_op(expr)),
                Value::Plan(plan) => Some(nodes::plan_op(plan)),
                Value::Term(_) => Some(Op::Term),
                Value::When(_) => Some(Op::When),
                Value::Absent => Some(Op::Absent),
                _ => None,
            },
        }
    }

    pub fn exprs(&self) -> Option<Vec<&Expr>> {
        match self {
            ArgRef::Node(NodeRef::Exprs(exprs)) => {
                Some(exprs.iter().map(|expr| expr.as_ref()).collect())
            }
            ArgRef::Node(_) => None,
            _ => match self.value()? {
                Value::Exprs(exprs) => Some(exprs.iter().collect()),
                Value::List(items) => items
                    .iter()
                    .map(|item| match item {
                        Value::Expr(expr) => Some(expr.as_ref()),
                        _ => None,
                    })
                    .collect(),
                _ => None,
            },
        }
    }

    pub fn terms(&self) -> Option<Vec<&crate::translate::plan::WhereTerm>> {
        match self {
            ArgRef::Node(NodeRef::Terms(terms)) => Some(terms.iter().collect()),
            ArgRef::Node(_) => None,
            _ => match self.value()? {
                Value::Terms(terms) => Some(terms.iter().collect()),
                Value::List(items) => items
                    .iter()
                    .map(|item| match item {
                        Value::Term(term) => Some(term.as_ref()),
                        _ => None,
                    })
                    .collect(),
                _ => None,
            },
        }
    }

    pub fn term(&self) -> Option<&crate::translate::plan::WhereTerm> {
        match self {
            ArgRef::Node(NodeRef::Term(term)) => Some(term),
            ArgRef::Node(_) => None,
            _ => match self.value()? {
                Value::Term(term) => Some(term.as_ref()),
                _ => None,
            },
        }
    }

    pub fn whens(&self) -> Option<Vec<(&Expr, &Expr)>> {
        match self {
            ArgRef::Node(NodeRef::Whens(whens)) => Some(
                whens
                    .iter()
                    .map(|(condition, result)| (condition.as_ref(), result.as_ref()))
                    .collect(),
            ),
            ArgRef::Node(_) => None,
            _ => match self.value()? {
                Value::Whens(whens) => Some(
                    whens
                        .iter()
                        .map(|(condition, result)| (condition, result))
                        .collect(),
                ),
                _ => None,
            },
        }
    }

    pub fn plan(&self) -> Option<&LogicalPlan> {
        match self {
            ArgRef::Node(NodeRef::Plan(plan)) => Some(plan),
            ArgRef::Node(_) => None,
            _ => match self.value()? {
                Value::Plan(plan) => Some(plan.as_ref()),
                _ => None,
            },
        }
    }

    pub fn is_absent(&self) -> bool {
        matches!(self, ArgRef::Node(NodeRef::Absent)) || matches!(self.value(), Some(Value::Absent))
    }

    pub fn str(&self) -> Option<&str> {
        match self.value()? {
            Value::Str(text) => Some(text),
            _ => None,
        }
    }

    pub fn int(&self) -> Option<i64> {
        match self {
            ArgRef::Int(value) => Some(*value),
            _ => None,
        }
    }
}

/// A function written in Rust. It gets the context of the node that the
/// rule replaces, so it can apply the rules to the nodes it builds.
fn engine_error(text: String) -> LimboError {
    LimboError::InternalError(format!("logical plan rules: {text}"))
}

/// The replacement a rule gives, the name of the rule, and whether the rules
/// were already applied to the top of the replacement: a node that the
/// pattern constructs gets them while it is built, a bound node does not.
type Found = (Value, &'static str, bool);

/// The rule engine. The rules live in the generated code.
pub(crate) struct RuleSet;

impl RuleSet {
    /// Normalize an expression and everything below it. Return whether it
    /// changed.
    pub fn normalize_expr(
        &self,
        ctx: &EngineContext<'_, '_>,
        expr: &mut Expr,
        context: Context,
    ) -> Result<bool> {
        let mut changed = false;
        for (child, child_context) in nodes::expr_children_mut(expr, context) {
            changed |= self.normalize_expr(ctx, child, child_context)?;
        }
        Ok(self.settle_expr(ctx, expr, context)? || changed)
    }

    /// Apply the rules to an expression whose children are normalized,
    /// until no rule matches. Return whether it changed.
    fn settle_expr(
        &self,
        ctx: &EngineContext<'_, '_>,
        expr: &mut Expr,
        context: Context,
    ) -> Result<bool> {
        let mut changed = false;
        for _ in 0..MAX_STEPS {
            match self.apply_rules(ctx, NodeRef::Expr(expr), context)? {
                None => return Ok(changed),
                Some((Value::Expr(replacement), rule, settled)) => {
                    tracing::trace!(rule, "logical plan rule changed an expression");
                    *expr = *replacement;
                    if settled {
                        return Ok(true);
                    }
                    changed = true;
                }
                Some((other, rule, _)) => {
                    return Err(engine_error(format!(
                        "{rule} replaced an expression with {}",
                        other.kind()
                    )))
                }
            }
        }
        Err(engine_error(
            "the rules did not stop changing an expression".to_string(),
        ))
    }

    /// Normalize a plan node, its expressions, and everything below it.
    /// Return whether it changed. Nested blocks are not visited.
    pub fn normalize_plan(
        &self,
        ctx: &EngineContext<'_, '_>,
        node: &mut LogicalPlan,
    ) -> Result<bool> {
        let changed = self.normalize_plan_children(ctx, node)?;
        Ok(self.settle_plan(ctx, node)? || changed)
    }

    /// Apply the rules to a plan node whose children are normalized, until
    /// no rule matches. Return whether it changed.
    fn settle_plan(&self, ctx: &EngineContext<'_, '_>, node: &mut LogicalPlan) -> Result<bool> {
        let mut changed = false;
        for _ in 0..MAX_STEPS {
            match self.apply_rules(ctx, NodeRef::Plan(node), Context::VALUE)? {
                None => return Ok(changed),
                Some((Value::Plan(replacement), rule, settled)) => {
                    tracing::trace!(rule, "logical plan rule changed a node");
                    *node = *replacement;
                    if settled {
                        return Ok(true);
                    }
                    changed = true;
                }
                Some((other, rule, _)) => {
                    return Err(engine_error(format!(
                        "{rule} replaced a plan node with {}",
                        other.kind()
                    )))
                }
            }
        }
        Err(engine_error(
            "the rules did not stop changing a plan node".to_string(),
        ))
    }

    fn normalize_plan_children(
        &self,
        ctx: &EngineContext<'_, '_>,
        node: &mut LogicalPlan,
    ) -> Result<bool> {
        let mut changed = false;
        match node {
            LogicalPlan::OneRow | LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_) => {}
            LogicalPlan::Join(join) => {
                changed |= self.normalize_plan(ctx, &mut join.left)?;
                changed |= self.normalize_plan(ctx, &mut join.right)?;
            }
            LogicalPlan::DependentJoin(join) => {
                changed |= self.normalize_plan(ctx, &mut join.left)?;
                changed |= self.normalize_plan(ctx, &mut join.right)?;
            }
            LogicalPlan::Filter(filter) => {
                changed |= self.normalize_plan(ctx, &mut filter.input)?;
                for term in &mut filter.terms {
                    if !term.consumed {
                        changed |= self.normalize_expr(ctx, &mut term.expr, Context::CONDITION)?;
                    }
                }
            }
            LogicalPlan::Aggregate(aggregate) => {
                changed |= self.normalize_plan(ctx, &mut aggregate.input)?;
                if let Some(group_by) = &mut aggregate.group_by {
                    for expr in &mut group_by.exprs {
                        changed |= self.normalize_expr(ctx, expr, Context::VALUE)?;
                    }
                    if let Some(having) = &mut group_by.having {
                        for expr in having {
                            changed |= self.normalize_expr(ctx, expr, Context::CONDITION)?;
                        }
                    }
                }
                for aggregate in &mut aggregate.aggregates {
                    changed |=
                        self.normalize_expr(ctx, &mut aggregate.original_expr, Context::VALUE)?;
                    for arg in &mut aggregate.args {
                        changed |= self.normalize_expr(ctx, arg, Context::VALUE)?;
                    }
                }
            }
            LogicalPlan::Project(project) => {
                changed |= self.normalize_plan(ctx, &mut project.input)?;
                for column in &mut project.columns {
                    changed |= self.normalize_expr(ctx, &mut column.expr, Context::VALUE)?;
                }
            }
            LogicalPlan::Distinct(distinct) => {
                changed |= self.normalize_plan(ctx, &mut distinct.input)?;
            }
            LogicalPlan::Sort(sort) => {
                changed |= self.normalize_plan(ctx, &mut sort.input)?;
                for (expr, _, _) in &mut sort.keys {
                    changed |= self.normalize_expr(ctx, expr, Context::VALUE)?;
                }
            }
            LogicalPlan::Limit(limit) => {
                changed |= self.normalize_plan(ctx, &mut limit.input)?;
                if let Some(expr) = &mut limit.limit {
                    changed |= self.normalize_expr(ctx, expr, Context::VALUE)?;
                }
                if let Some(expr) = &mut limit.offset {
                    changed |= self.normalize_expr(ctx, expr, Context::VALUE)?;
                }
            }
        }
        Ok(changed)
    }

    /// Try the rules of the node's operator. Return the replacement of the
    /// first rule that matches.
    fn apply_rules<'a>(
        &self,
        ctx: &EngineContext<'_, '_>,
        node: NodeRef<'a>,
        context: Context,
    ) -> Result<Option<Found>> {
        match node.op() {
            Some(op) => self.apply_rules_of(ctx, node, op, context),
            None => Ok(None),
        }
    }

    /// Build a node from its operator and its arguments, then apply the
    /// rules of the operator to it. The arguments are built in the context
    /// of their position, so they are normalized when the node is.
    fn build_node(
        &self,
        ctx: &EngineContext<'_, '_>,
        op: Op,
        args: Vec<Value>,
        context: Context,
    ) -> Result<Value> {
        match nodes::construct(op, args)? {
            Value::Expr(mut expr) => {
                self.settle_expr(ctx, &mut expr, context)?;
                Ok(Value::Expr(expr))
            }
            Value::Plan(mut plan) => {
                self.settle_plan(ctx, &mut plan)?;
                Ok(Value::Plan(plan))
            }
            other => Ok(other),
        }
    }
}

/// The boolean that a match function gave.
fn bool_of(rule: &str, value: Value) -> Result<bool> {
    match value {
        Value::Bool(matched) => Ok(matched),
        other => Err(engine_error(format!(
            "{rule}: a match function gave {}, not a boolean",
            other.kind()
        ))),
    }
}

/// The boolean that a `Let` in a match pattern gives as its result.
fn bool_value(rule: &str, value: &Value) -> Result<bool> {
    match value {
        Value::Bool(matched) => Ok(*matched),
        _ => Err(engine_error(format!(
            "{rule}: the result of a Let in a match pattern is not a boolean"
        ))),
    }
}

/// The values that a function gave to a `Let` that binds `count` variables.
fn let_values(rule: &str, value: Value, count: usize) -> Result<Vec<Value>> {
    let values = match value {
        Value::Tuple(values) => values,
        single => vec![single],
    };
    if values.len() != count {
        return Err(engine_error(format!(
            "{rule}: a Let binds {count} variables but the function gave {} values",
            values.len()
        )));
    }
    Ok(values)
}

/// The operator of a bound node, for `(OpName $var)`.
fn op_of(rule: &str, node: NodeRef<'_>) -> Result<Op> {
    node.op()
        .ok_or_else(|| engine_error(format!("{rule}: OpName of a node without an operator")))
}

mod generated {
    #![allow(clippy::all, unused_variables, unused_mut)]

    use super::*;

    include!(concat!(env!("OUT_DIR"), "/rules_generated.rs"));
}

#[cfg(test)]
mod tests {
    use turso_parser::ast::{
        fmt::{ToSqlContext, ToTokens},
        Cmd, Expr, OneSelect, ResultColumn, Stmt, TableInternalId,
    };
    use turso_parser::parser::Parser;

    use super::*;
    use crate::schema::{BTreeTable, Table};
    use crate::sync::Arc;
    use crate::translate::expr::{walk_expr_mut, WalkControl};
    use crate::translate::logical::{Filter, Join, Scan};
    use crate::translate::plan::{
        ColumnUsedMask, JoinInfo, JoinType, JoinedTable, Operation, WhereTerm,
    };

    const COLUMNS: &[&str] = &["a", "b", "c", "d"];

    struct Names;

    impl ToSqlContext for Names {
        fn get_table_name(&self, _: TableInternalId) -> Option<&str> {
            Some("t")
        }

        fn get_column_name(&self, _: TableInternalId, column: usize) -> Option<Option<&str>> {
            COLUMNS.get(column).map(|name| Some(*name))
        }
    }

    fn display(expr: &Expr) -> String {
        expr.displayer(&Names).to_string()
    }

    /// Parse an expression. The names a, b, c, and d are columns of one
    /// table.
    fn parse(sql: &str) -> Expr {
        let statement = format!("SELECT {sql}");
        let mut parser = Parser::new(statement.as_bytes());
        let Some(Ok(Cmd::Stmt(Stmt::Select(select)))) = parser.next() else {
            panic!("{sql} does not parse");
        };
        let OneSelect::Select { mut columns, .. } = select.body.select else {
            panic!("{sql} is not a plain select");
        };
        let ResultColumn::Expr(expr, _) = columns.remove(0) else {
            panic!("{sql} is not an expression");
        };
        let mut expr = *expr;
        walk_expr_mut(&mut expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
            if let Expr::Id(name) = expr {
                if let Some(column) = COLUMNS
                    .iter()
                    .position(|known| known.eq_ignore_ascii_case(name.as_str()))
                {
                    *expr = Expr::Column {
                        database: None,
                        table: TableInternalId::from(0),
                        column,
                        is_rowid_alias: false,
                    };
                }
            }
            Ok(WalkControl::Continue)
        })
        .unwrap();
        expr
    }

    /// The expression without one-element parentheses, so that two ways of
    /// writing one expression compare equal.
    fn canonical(mut expr: Expr) -> Expr {
        walk_expr_mut(&mut expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
            while let Expr::Parenthesized(exprs) = expr {
                if exprs.len() != 1 {
                    break;
                }
                *expr = *exprs.remove(0);
            }
            Ok(WalkControl::Continue)
        })
        .unwrap();
        expr
    }

    fn normalized(sql: &str, context: Context) -> Expr {
        let mut expr = parse(sql);
        rule_set()
            .normalize_expr(
                &EngineContext {
                    resolver: None,
                    rules: rule_set(),
                    virtual_tables: &[],
                },
                &mut expr,
                context,
            )
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        canonical(expr)
    }

    fn assert_rewrites(cases: &[(&str, &str)], context: Context) {
        for (sql, expected) in cases {
            let actual = normalized(sql, context);
            let expected = canonical(parse(expected));
            assert_eq!(
                actual,
                expected,
                "{sql}: got {}, expected {}",
                display(&actual),
                display(&expected)
            );
        }
    }

    #[test]
    fn constants_fold() {
        assert_rewrites(
            &[
                ("1 + 2 * 3", "7"),
                ("'ab' || 'c'", "'abc'"),
                ("NULL + 1", "NULL"),
                ("10 / 4", "2"),
                ("10 / 0", "NULL"),
                ("1 / 2.0", "0.5"),
                ("-(-5)", "5"),
                ("-'5'", "-5"),
                ("+'abc'", "'abc'"),
                ("~0", "-1"),
                ("NOT 5", "0"),
                ("1 = 1", "1"),
                ("'a' < 'b'", "1"),
                ("1 < '1'", "1"),
                ("NULL = 1", "NULL"),
                ("1 IS 1.0", "1"),
                ("CAST('12' AS INTEGER)", "12"),
                ("CAST(NULL AS TEXT)", "NULL"),
                ("CAST(1 AS mytype)", "CAST(1 AS mytype)"),
                ("CAST(1.5 AS TEXT)", "'1.5'"),
                ("NULL IS NULL", "1"),
                ("5 IS NULL", "0"),
                ("NULL IS NOT NULL", "0"),
                ("a + (1 + 1)", "a + 2"),
                ("random() + 1", "random() + 1"),
                ("a IN ()", "0"),
                ("a NOT IN ()", "1"),
                ("NULL IN (1, 2)", "NULL"),
                ("a IN (NULL)", "NULL"),
            ],
            Context::VALUE,
        );
    }

    #[test]
    fn comparisons_are_normalized() {
        assert_rewrites(
            &[
                ("5 = a", "a = 5"),
                ("5 < a", "a > 5"),
                ("5 >= a + 1", "a + 1 <= 5"),
                ("NOT (a = 5)", "a != 5"),
                ("NOT (a < 5)", "a >= 5"),
                ("NOT (a IS NULL)", "a IS NOT NULL"),
                ("NOT (a IS TRUE)", "a IS NOT TRUE"),
                ("NOT (a IN (1, 2))", "a NOT IN (1, 2)"),
                ("NOT (a LIKE 'x')", "a NOT LIKE 'x'"),
                ("NULL IS a", "a IS NULL"),
                ("TRUE IS a", "a IS 1"),
                ("a = a", "a IS NOT NULL OR NULL"),
                ("a != a", "a IS NULL AND NULL"),
                ("(a, b) = (1, 2)", "a = 1 AND b = 2"),
                ("a IN (5)", "a = 5"),
                ("a NOT IN (5)", "a != 5"),
                ("a IN (3, 1, 3)", "a IN (1, 3)"),
                ("a IN (1, 1.0)", "a IN (1, 1.0)"),
                ("a IN (b)", "a IN (b)"),
                ("a LIKE '%%x%%'", "a LIKE '%x%'"),
                ("a BETWEEN 1 AND 5", "a BETWEEN 1 AND 5"),
                ("a IS 5", "a IS 5"),
            ],
            Context::VALUE,
        );
    }

    #[test]
    fn conditions_use_truth_values() {
        assert_rewrites(
            &[
                ("a BETWEEN 1 AND 5", "a >= 1 AND a <= 5"),
                ("NOT (a BETWEEN 1 AND 5)", "a < 1 OR a > 5"),
                ("random() BETWEEN 1 AND 5", "random() BETWEEN 1 AND 5"),
                ("1 AND a", "a"),
                ("a AND 0", "0"),
                ("2 OR a", "1"),
                ("'abc' OR a", "a"),
                ("a = 5 OR NULL", "a = 5"),
                ("NOT NOT a", "a"),
                ("NOT (NULL OR a)", "0"),
                ("NOT (a = 1 AND b = 2)", "a != 1 OR b != 2"),
                ("NOT (a = 1 OR b = 2)", "a != 1 AND b != 2"),
                ("a IS 5", "a = 5"),
                ("a LIKE '%'", "a IS NOT NULL"),
                ("CASE WHEN a THEN 1 ELSE 0 END", "a"),
                ("CASE WHEN a THEN 0 ELSE 0 END", "0"),
                ("CASE WHEN a THEN 1 WHEN b THEN 0 ELSE 0 END", "a"),
                (
                    "CASE WHEN a THEN c WHEN b THEN 0 ELSE 0 END",
                    "CASE WHEN a THEN c ELSE 0 END",
                ),
                ("(a AND b) OR (a AND c)", "a AND (b OR c)"),
                ("a OR (a AND b)", "a"),
                ("(a AND b AND c) OR (a AND d)", "a AND ((b AND c) OR d)"),
                ("(a AND b) OR (c AND d)", "(a AND b) OR (c AND d)"),
            ],
            Context::CONDITION,
        );
    }

    #[test]
    fn between_over_a_function_is_rewritten_only_when_the_function_is_deterministic() {
        assert_rewrites(
            &[
                (
                    "length(b) BETWEEN 1 AND 5",
                    "length(b) >= 1 AND length(b) <= 5",
                ),
                ("abs(b) NOT BETWEEN 1 AND 5", "abs(b) < 1 OR abs(b) > 5"),
                ("random() BETWEEN 1 AND 5", "random() BETWEEN 1 AND 5"),
                (
                    "no_such_function(b) BETWEEN 1 AND 5",
                    "no_such_function(b) BETWEEN 1 AND 5",
                ),
            ],
            Context::CONDITION,
        );
    }

    #[test]
    fn values_keep_their_form() {
        assert_rewrites(
            &[
                ("1 AND a", "1 AND a"),
                ("a AND 0", "0"),
                ("NOT NOT a", "NOT NOT a"),
                ("a LIKE '%'", "a LIKE '%'"),
                (
                    "CASE WHEN a THEN 1 ELSE 0 END",
                    "CASE WHEN a THEN 1 ELSE 0 END",
                ),
                ("a = 5 OR NULL", "a = 5 OR NULL"),
                ("NOT (1 AND a)", "NOT a"),
            ],
            Context::VALUE,
        );
    }

    #[test]
    fn case_and_coalesce_simplify() {
        assert_rewrites(
            &[
                ("coalesce(NULL, 5, a)", "5"),
                ("coalesce(NULL, a, b)", "coalesce(a, b)"),
                ("coalesce(a, NULL)", "coalesce(a, NULL)"),
                ("ifnull(NULL, a)", "a"),
                (
                    "CASE WHEN 0 THEN 1 WHEN a THEN 2 WHEN 1 THEN 3 WHEN b THEN 4 ELSE 5 END",
                    "CASE WHEN a THEN 2 ELSE 3 END",
                ),
                ("CASE 1 WHEN 2 THEN 'x' WHEN 1 THEN 'y' END", "'y'"),
                ("CASE WHEN 0 THEN 1 END", "NULL"),
                ("CASE a WHEN 1 THEN 'x' END", "CASE a WHEN 1 THEN 'x' END"),
            ],
            Context::VALUE,
        );
    }

    fn terms(sqls: &[&str]) -> Vec<WhereTerm> {
        sqls.iter()
            .map(|sql| WhereTerm {
                expr: parse(sql),
                from_outer_join: None,
                consumed: false,
            })
            .collect()
    }

    /// Normalize a filter over these terms. `None` means the filter went
    /// away because every term was true.
    fn normalized_terms(terms: Vec<WhereTerm>) -> Option<Vec<WhereTerm>> {
        let mut node = LogicalPlan::Filter(Filter {
            input: Box::new(LogicalPlan::OneRow),
            terms,
        });
        rule_set()
            .normalize_plan(
                &EngineContext {
                    resolver: None,
                    rules: rule_set(),
                    virtual_tables: &[],
                },
                &mut node,
            )
            .unwrap();
        match node {
            LogicalPlan::Filter(filter) => Some(filter.terms),
            LogicalPlan::OneRow => None,
            other => panic!("unexpected node {other:?}"),
        }
    }

    fn assert_terms(input: &[&str], expected: &[&str]) {
        let actual = normalized_terms(terms(input)).unwrap_or_default();
        let actual: Vec<Expr> = actual
            .into_iter()
            .map(|term| canonical(term.expr))
            .collect();
        let expected: Vec<Expr> = expected.iter().map(|sql| canonical(parse(sql))).collect();
        assert_eq!(
            actual,
            expected,
            "{input:?}: got {:?}",
            actual.iter().map(display).collect::<Vec<_>>()
        );
    }

    #[test]
    fn filter_terms_are_simplified() {
        assert_terms(&["a = 1 AND b = 2", "1"], &["a = 1", "b = 2"]);
        assert_terms(&["a = 1", "0"], &["0"]);
        assert_terms(&["a = 1", "NULL"], &["0"]);
        assert_terms(&["a = 1", "a = 1"], &["a = 1"]);
        assert_terms(
            &["(a = 1 AND b = 2) OR (a = 1 AND c = 3)"],
            &["a = 1", "b = 2 OR c = 3"],
        );
        assert_terms(&["a BETWEEN 1 AND 5"], &["a >= 1", "a <= 5"]);
        assert_terms(&["NOT (a = 1 OR b = 2)"], &["a != 1", "b != 2"]);
        assert_terms(&["'abc'"], &["0"]);
        assert_terms(&["'9x'", "a = 1"], &["a = 1"]);
        assert!(normalized_terms(terms(&["1"])).is_none());
    }

    #[test]
    fn a_false_outer_join_term_stays() {
        let mut terms = terms(&["0", "b = 2", "1 AND a = 1"]);
        terms[0].from_outer_join = Some(TableInternalId::from(0));
        terms[2].from_outer_join = Some(TableInternalId::from(0));
        let actual = normalized_terms(terms).unwrap();
        let shown: Vec<String> = actual.iter().map(|term| display(&term.expr)).collect();
        assert_eq!(actual.len(), 3, "{shown:?}");
        assert_eq!(actual[0].from_outer_join, Some(TableInternalId::from(0)));
        assert_eq!(actual[0].expr, parse("0"));
        assert_eq!(actual[1].from_outer_join, None);
        assert_eq!(actual[1].expr, canonical(parse("b = 2")));
        assert_eq!(actual[2].from_outer_join, Some(TableInternalId::from(0)));
        assert_eq!(actual[2].expr, canonical(parse("a = 1")));
    }

    fn scan(sql: &str, id: usize) -> LogicalPlan {
        let table = Table::BTree(Arc::new(BTreeTable::from_sql(sql, 2).unwrap()));
        LogicalPlan::Scan(Scan {
            table: JoinedTable {
                op: Operation::default_scan_for(&table),
                table,
                identifier: format!("t{id}"),
                internal_id: TableInternalId::from(id),
                join_info: None,
                col_used_mask: ColumnUsedMask::default(),
                column_use_counts: Vec::new(),
                expression_index_usages: Vec::new(),
                database_id: 0,
                indexed: None,
                plan_estimate: None,
            },
        })
    }

    fn filter_over(input: LogicalPlan, sqls: &[&str]) -> LogicalPlan {
        LogicalPlan::Filter(Filter {
            input: Box::new(input),
            terms: terms(sqls),
        })
    }

    fn shown_terms(node: &LogicalPlan) -> Vec<String> {
        match node {
            LogicalPlan::Filter(filter) => filter
                .terms
                .iter()
                .map(|term| display(&term.expr))
                .collect(),
            _ => Vec::new(),
        }
    }

    #[test]
    fn a_null_comparison_on_a_virtual_table_column_stays() {
        let mut expr = parse("a = +NULL");
        let virtual_tables = [TableInternalId::from(0)];
        rule_set()
            .normalize_expr(
                &EngineContext {
                    resolver: None,
                    rules: rule_set(),
                    virtual_tables: &virtual_tables,
                },
                &mut expr,
                Context::CONDITION,
            )
            .unwrap();
        assert_eq!(canonical(expr), canonical(parse("a = NULL")));
        assert_eq!(
            normalized("a = +NULL", Context::CONDITION),
            canonical(parse("NULL"))
        );
    }

    #[test]
    fn null_tests_on_not_null_columns_simplify() {
        let table = "CREATE TABLE t(a INTEGER NOT NULL, b)";
        let mut node = filter_over(scan(table, 0), &["a IS NOT NULL", "b = 1"]);
        rule_set()
            .normalize_plan(
                &EngineContext {
                    resolver: None,
                    rules: rule_set(),
                    virtual_tables: &[],
                },
                &mut node,
            )
            .unwrap();
        assert_eq!(shown_terms(&node), vec!["t.b = 1"]);

        let mut node = filter_over(scan(table, 0), &["b IS NOT NULL"]);
        rule_set()
            .normalize_plan(
                &EngineContext {
                    resolver: None,
                    rules: rule_set(),
                    virtual_tables: &[],
                },
                &mut node,
            )
            .unwrap();
        assert_eq!(shown_terms(&node), vec!["t.b IS NOT NULL"]);

        let mut node = filter_over(scan(table, 0), &["a IS NULL", "b = 1"]);
        rule_set()
            .normalize_plan(
                &EngineContext {
                    resolver: None,
                    rules: rule_set(),
                    virtual_tables: &[],
                },
                &mut node,
            )
            .unwrap();
        assert_eq!(shown_terms(&node), vec!["0"]);

        let joins = LogicalPlan::Join(Join {
            left: Box::new(scan("CREATE TABLE t1(x)", 1)),
            right: Box::new(scan(table, 0)),
            info: JoinInfo {
                join_type: JoinType::LeftOuter,
                using: Vec::new(),
                no_reorder: false,
            },
        });
        let mut node = filter_over(joins, &["a IS NOT NULL"]);
        rule_set()
            .normalize_plan(
                &EngineContext {
                    resolver: None,
                    rules: rule_set(),
                    virtual_tables: &[],
                },
                &mut node,
            )
            .unwrap();
        assert_eq!(shown_terms(&node), vec!["t.a IS NOT NULL"]);
    }

    #[test]
    fn a_consumed_term_is_left_alone() {
        let mut terms = terms(&["1 AND a = 1"]);
        terms[0].consumed = true;
        let actual = normalized_terms(terms).unwrap();
        assert_eq!(actual.len(), 1);
        assert!(actual[0].consumed);
        assert_eq!(actual[0].expr, parse("1 AND a = 1"));
    }
}
