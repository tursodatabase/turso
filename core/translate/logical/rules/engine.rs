//! Run compiled rules on expressions and plan nodes.
//!
//! The engine reads the rule files at first use, compiles them, and keeps
//! the rules indexed by the operator each one matches at its top. To
//! normalize a node, it normalizes the children first, then tries the rules
//! of the node's operator in order, and starts over on the replacement until
//! no rule matches.
//!
//! A match binds variables to borrowed nodes. The replacement is built from
//! clones of the bound nodes, so a rule that keeps a large subtree copies it.

use std::collections::HashMap;
use std::sync::OnceLock;

use turso_parser::ast::Expr;

use crate::translate::emitter::Resolver;
use crate::translate::logical::optgen::{self, Compiled, ExprKind, FuncName};
use crate::translate::logical::LogicalPlan;
use crate::{LimboError, Result};

use super::funcs;
use super::nodes::{self, Context, NodeRef, Op, PrivateRef, Value};

const OP_NAME_FUNCTION: &str = "OpName";
const TRUTH_VALUE_TAG: &str = "TruthValue";
const NULL_IS_FALSE_TAG: &str = "NullIsFalse";
const HIGH_PRIORITY_TAG: &str = "HighPriority";
const LOW_PRIORITY_TAG: &str = "LowPriority";

/// The most rule applications on one node before the engine gives up.
const MAX_STEPS: usize = 256;

const RULE_FILES: &[(&str, &str)] = &[
    ("ops.opt", include_str!("ops.opt")),
    ("bool.opt", include_str!("bool.opt")),
    ("comp.opt", include_str!("comp.opt")),
    ("fold_constants.opt", include_str!("fold_constants.opt")),
    ("scalar.opt", include_str!("scalar.opt")),
    ("filter.opt", include_str!("filter.opt")),
];

/// The rules of `rules/*.opt`, compiled once.
pub(crate) fn rule_set() -> &'static RuleSet {
    static RULE_SET: OnceLock<RuleSet> = OnceLock::new();
    RULE_SET.get_or_init(|| {
        RuleSet::from_files(RULE_FILES).unwrap_or_else(|errors| {
            panic!(
                "the logical plan rule files do not compile:\n{}",
                errors.join("\n")
            )
        })
    })
}

/// What a rule can read while it runs.
pub(crate) struct EngineContext<'a, 'r> {
    pub resolver: Option<&'a Resolver<'r>>,
}

/// An argument of a function written in Rust.
pub(crate) enum ArgRef<'a, 'b> {
    Node(NodeRef<'a>),
    Value(&'b Value),
    Owned(Box<Value>),
}

impl ArgRef<'_, '_> {
    pub(super) fn value(&self) -> Option<&Value> {
        match self {
            ArgRef::Node(_) => None,
            ArgRef::Value(value) => Some(value),
            ArgRef::Owned(value) => Some(value),
        }
    }

    pub fn expr(&self) -> Option<&Expr> {
        match self {
            ArgRef::Node(NodeRef::Expr(expr)) => Some(expr),
            ArgRef::Node(NodeRef::Private(PrivateRef::Expr(expr))) => Some(expr),
            ArgRef::Node(_) => None,
            _ => match self.value()? {
                Value::Expr(expr) | Value::Private(nodes::Private::Expr(expr)) => Some(expr),
                _ => None,
            },
        }
    }

    pub fn op(&self) -> Option<Op> {
        match self {
            ArgRef::Node(node) => node.op(),
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
                        Value::Expr(expr) => Some(expr),
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
                        Value::Term(term) => Some(term),
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
                Value::Term(term) => Some(term),
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
                Value::Plan(plan) => Some(plan),
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
        match self.value()? {
            Value::Int(value) => Some(*value),
            _ => None,
        }
    }
}

pub(crate) type CustomFn = fn(&EngineContext<'_, '_>, &[ArgRef<'_, '_>]) -> Result<Value>;

pub(crate) struct RuleSet {
    rules: Vec<CompiledRule>,
    /// The rules for each operator, in the order to try them. Indexed by
    /// the position of the operator in `Op::ALL`.
    by_op: Vec<Vec<usize>>,
}

struct CompiledRule {
    name: String,
    needs_truth_value: bool,
    needs_null_is_false: bool,
    root: Op,
    /// 0 for HighPriority, 1 for a plain rule, 2 for LowPriority. Rules of
    /// one priority stay in file order.
    priority: u8,
    slots: usize,
    matcher: Matcher,
    replace: Builder,
}

enum Matcher {
    Any,
    Node {
        ops: Vec<Op>,
        args: Vec<Matcher>,
    },
    Bind {
        slot: usize,
        target: Box<Matcher>,
    },
    And(Box<Matcher>, Box<Matcher>),
    Not(Box<Matcher>),
    List {
        kind: ListKind,
        item: Option<Box<Matcher>>,
    },
    Custom {
        func: CustomFn,
        args: Vec<Builder>,
    },
    Let {
        slots: Vec<usize>,
        func: CustomFn,
        args: Vec<Builder>,
        result: usize,
    },
    Str(String),
    Number(i64),
}

#[derive(Clone, Copy)]
enum ListKind {
    Empty,
    Single,
    First,
    Last,
    Any,
}

enum Builder {
    Ref(usize),
    Construct {
        op: Op,
        args: Vec<Builder>,
    },
    DynamicConstruct {
        slot: usize,
        args: Vec<Builder>,
    },
    Custom {
        func: CustomFn,
        args: Vec<Builder>,
    },
    Let {
        slots: Vec<usize>,
        func: CustomFn,
        args: Vec<Builder>,
        result: usize,
    },
    OpName(usize),
    Name(Op),
    Str(String),
    Number(i64),
    List(Vec<Builder>),
    /// `$var:(Func ...)` inside the arguments of a function: build the
    /// target, keep it in the variable, and give it.
    BindValue {
        slot: usize,
        target: Box<Builder>,
    },
}

enum Binding<'a> {
    Node(NodeRef<'a>),
    Value(Box<Value>),
}

type Bindings<'a> = Vec<Option<Binding<'a>>>;

fn priority(rule: &optgen::Rule) -> u8 {
    if rule.has_tag(HIGH_PRIORITY_TAG) {
        0
    } else if rule.has_tag(LOW_PRIORITY_TAG) {
        2
    } else {
        1
    }
}

fn engine_error(text: String) -> LimboError {
    LimboError::InternalError(format!("logical plan rules: {text}"))
}

impl RuleSet {
    pub fn from_files(files: &[(&str, &str)]) -> std::result::Result<RuleSet, Vec<String>> {
        let compiled = optgen::compile(files)?;
        RuleSet::from_compiled(&compiled, funcs::lookup).map_err(|error| vec![error])
    }

    pub fn from_compiled(
        compiled: &Compiled,
        lookup: fn(&str) -> Option<CustomFn>,
    ) -> std::result::Result<RuleSet, String> {
        let mut define_ops = Vec::with_capacity(compiled.defines.len());
        for define in &compiled.defines {
            let Some(op) = Op::from_name(&define.name) else {
                return Err(format!(
                    "{}: {} is defined but the engine has no such operator",
                    define.src, define.name
                ));
            };
            if define.fields.len() != op.field_count() {
                return Err(format!(
                    "{}: {} has {} fields but the engine gives it {}",
                    define.src,
                    define.name,
                    define.fields.len(),
                    op.field_count()
                ));
            }
            define_ops.push(op);
        }
        for op in Op::ALL {
            if compiled.lookup_define(op.name()).is_none() {
                return Err(format!("the operator {} has no define", op.name()));
            }
        }

        let mut rules = Vec::with_capacity(compiled.rules.len());
        for op in Op::ALL {
            for &index in compiled.lookup_matching_rules(op.name()) {
                let rule = &compiled.rules[index];
                let mut builder = RuleBuilder {
                    compiled,
                    define_ops: &define_ops,
                    lookup,
                    slots: HashMap::new(),
                    rule: &rule.name,
                };
                let matcher = builder.matcher(&rule.match_pattern)?;
                let replace = builder.builder(&rule.replace)?;
                let root = builder.op_of_define_name(rule.match_pattern.single_name())?;
                let needs_null_is_false = rule.has_tag(NULL_IS_FALSE_TAG);
                rules.push(CompiledRule {
                    name: rule.name.clone(),
                    needs_truth_value: rule.has_tag(TRUTH_VALUE_TAG) || needs_null_is_false,
                    needs_null_is_false,
                    root,
                    priority: priority(rule),
                    slots: builder.slots.len(),
                    matcher,
                    replace,
                });
            }
        }

        let mut by_op = vec![Vec::new(); Op::ALL.len()];
        for (index, rule) in rules.iter().enumerate() {
            by_op[rule.root as usize].push(index);
        }
        for indexes in &mut by_op {
            indexes.sort_by_key(|&index| rules[index].priority);
        }
        Ok(RuleSet { rules, by_op })
    }

    #[cfg(test)]
    pub fn rule_names(&self) -> Vec<&str> {
        self.rules.iter().map(|rule| rule.name.as_str()).collect()
    }

    /// Normalize an expression and everything below it. Return whether it
    /// changed.
    pub fn normalize_expr(
        &self,
        ctx: &EngineContext<'_, '_>,
        expr: &mut Expr,
        context: Context,
    ) -> Result<bool> {
        let mut changed = false;
        for _ in 0..MAX_STEPS {
            for (child, child_context) in nodes::expr_children_mut(expr, context) {
                changed |= self.normalize_expr(ctx, child, child_context)?;
            }
            match self.apply_rules(ctx, NodeRef::Expr(expr), context)? {
                None => return Ok(changed),
                Some((Value::Expr(replacement), rule)) => {
                    tracing::trace!(rule, "logical plan rule changed an expression");
                    *expr = replacement;
                    changed = true;
                }
                Some((other, rule)) => {
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
        let mut changed = false;
        for _ in 0..MAX_STEPS {
            changed |= self.normalize_plan_children(ctx, node)?;
            match self.apply_rules(ctx, NodeRef::Plan(node), Context::VALUE)? {
                None => return Ok(changed),
                Some((Value::Plan(replacement), rule)) => {
                    tracing::trace!(rule, "logical plan rule changed a node");
                    *node = replacement;
                    changed = true;
                }
                Some((other, rule)) => {
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
    /// first rule that matches, with the name of the rule.
    fn apply_rules<'a>(
        &self,
        ctx: &EngineContext<'_, '_>,
        node: NodeRef<'a>,
        context: Context,
    ) -> Result<Option<(Value, &str)>> {
        let Some(op) = node.op() else {
            return Ok(None);
        };
        for &index in &self.by_op[op as usize] {
            let rule = &self.rules[index];
            if rule.needs_truth_value && !context.truth_value {
                continue;
            }
            if rule.needs_null_is_false && !context.null_is_false {
                continue;
            }
            let mut bindings: Bindings<'a> = (0..rule.slots).map(|_| None).collect();
            if !self.matches(ctx, rule, &rule.matcher, node, &mut bindings)? {
                continue;
            }
            let replacement = self.build(ctx, rule, &rule.replace, &mut bindings)?;
            return Ok(Some((replacement, &rule.name)));
        }
        Ok(None)
    }

    fn matches<'a>(
        &self,
        ctx: &EngineContext<'_, '_>,
        rule: &CompiledRule,
        matcher: &Matcher,
        node: NodeRef<'a>,
        bindings: &mut Bindings<'a>,
    ) -> Result<bool> {
        match matcher {
            Matcher::Any => Ok(true),
            Matcher::Node { ops, args } => {
                let Some(op) = node.op() else {
                    return Ok(false);
                };
                if !ops.contains(&op) {
                    return Ok(false);
                }
                let children = node.children();
                for (index, arg) in args.iter().enumerate() {
                    let Some(child) = children.get(index) else {
                        return Ok(false);
                    };
                    if !self.matches(ctx, rule, arg, *child, bindings)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Matcher::Bind { slot, target } => {
                bindings[*slot] = Some(Binding::Node(node));
                self.matches(ctx, rule, target, node, bindings)
            }
            Matcher::And(left, right) => Ok(self.matches(ctx, rule, left, node, bindings)?
                && self.matches(ctx, rule, right, node, bindings)?),
            Matcher::Not(input) => Ok(!self.matches(ctx, rule, input, node, bindings)?),
            Matcher::List { kind, item } => {
                let Some(items) = node.list_items() else {
                    return Ok(false);
                };
                match kind {
                    ListKind::Empty => Ok(items.is_empty()),
                    ListKind::Single => {
                        let [only] = items.as_slice() else {
                            return Ok(false);
                        };
                        self.matches_item(ctx, rule, item, *only, bindings)
                    }
                    ListKind::First => match items.first() {
                        Some(first) => self.matches_item(ctx, rule, item, *first, bindings),
                        None => Ok(false),
                    },
                    ListKind::Last => match items.last() {
                        Some(last) => self.matches_item(ctx, rule, item, *last, bindings),
                        None => Ok(false),
                    },
                    ListKind::Any => {
                        for candidate in items {
                            if self.matches_item(ctx, rule, item, candidate, bindings)? {
                                return Ok(true);
                            }
                        }
                        Ok(false)
                    }
                }
            }
            Matcher::Custom { func, args } => {
                let value = self.call(ctx, rule, *func, args, bindings)?;
                match value {
                    Value::Bool(matched) => Ok(matched),
                    other => Err(engine_error(format!(
                        "{}: a match function gave {}, not a boolean",
                        rule.name,
                        other.kind()
                    ))),
                }
            }
            Matcher::Let {
                slots,
                func,
                args,
                result,
            } => {
                self.bind_let(ctx, rule, slots, *func, args, bindings)?;
                match bindings[*result].as_ref().map(|binding| match binding {
                    Binding::Value(value) => Some(value.as_ref()),
                    Binding::Node(_) => None,
                }) {
                    Some(Some(Value::Bool(matched))) => Ok(*matched),
                    _ => Err(engine_error(format!(
                        "{}: the result of a Let in a match pattern is not a boolean",
                        rule.name
                    ))),
                }
            }
            Matcher::Str(text) => Ok(funcs::node_is_string(node, text)),
            Matcher::Number(value) => Ok(funcs::node_is_integer(node, *value)),
        }
    }

    fn matches_item<'a>(
        &self,
        ctx: &EngineContext<'_, '_>,
        rule: &CompiledRule,
        item: &Option<Box<Matcher>>,
        node: NodeRef<'a>,
        bindings: &mut Bindings<'a>,
    ) -> Result<bool> {
        match item {
            Some(item) => self.matches(ctx, rule, item, node, bindings),
            None => Ok(true),
        }
    }

    fn bind_let<'a>(
        &self,
        ctx: &EngineContext<'_, '_>,
        rule: &CompiledRule,
        slots: &[usize],
        func: CustomFn,
        args: &[Builder],
        bindings: &mut Bindings<'a>,
    ) -> Result<()> {
        let value = self.call(ctx, rule, func, args, bindings)?;
        let values = match value {
            Value::Tuple(values) => values,
            single => vec![single],
        };
        if values.len() != slots.len() {
            return Err(engine_error(format!(
                "{}: a Let binds {} variables but the function gave {} values",
                rule.name,
                slots.len(),
                values.len()
            )));
        }
        for (slot, value) in slots.iter().zip(values) {
            bindings[*slot] = Some(Binding::Value(Box::new(value)));
        }
        Ok(())
    }

    fn call<'a>(
        &self,
        ctx: &EngineContext<'_, '_>,
        rule: &CompiledRule,
        func: CustomFn,
        args: &[Builder],
        bindings: &mut Bindings<'a>,
    ) -> Result<Value> {
        let mut owned: Vec<Option<Value>> = Vec::with_capacity(args.len());
        for arg in args {
            owned.push(match arg {
                Builder::Ref(_) => None,
                other => Some(self.build(ctx, rule, other, bindings)?),
            });
        }
        let mut arg_refs: Vec<ArgRef<'a, '_>> = Vec::with_capacity(args.len());
        for (arg, owned) in args.iter().zip(owned) {
            arg_refs.push(match (arg, owned) {
                (Builder::Ref(slot), _) => match &bindings[*slot] {
                    Some(Binding::Node(node)) => ArgRef::Node(*node),
                    Some(Binding::Value(value)) => ArgRef::Value(value),
                    None => {
                        return Err(engine_error(format!(
                            "{}: a variable is used before it is bound",
                            rule.name
                        )))
                    }
                },
                (_, Some(value)) => ArgRef::Owned(Box::new(value)),
                (_, None) => unreachable!("every argument that is not a reference is built"),
            });
        }
        func(ctx, &arg_refs)
    }

    fn build<'a>(
        &self,
        ctx: &EngineContext<'_, '_>,
        rule: &CompiledRule,
        builder: &Builder,
        bindings: &mut Bindings<'a>,
    ) -> Result<Value> {
        match builder {
            Builder::Ref(slot) => match &bindings[*slot] {
                Some(Binding::Node(node)) => Ok(node.to_value()),
                Some(Binding::Value(value)) => Ok((**value).clone()),
                None => Err(engine_error(format!(
                    "{}: a variable is used before it is bound",
                    rule.name
                ))),
            },
            Builder::Construct { op, args } => {
                let args = self.build_all(ctx, rule, args, bindings)?;
                nodes::construct(*op, args)
            }
            Builder::DynamicConstruct { slot, args } => {
                let op = self.bound_op(rule, *slot, bindings)?;
                let args = self.build_all(ctx, rule, args, bindings)?;
                nodes::construct(op, args)
            }
            Builder::Custom { func, args } => self.call(ctx, rule, *func, args, bindings),
            Builder::Let {
                slots,
                func,
                args,
                result,
            } => {
                self.bind_let(ctx, rule, slots, *func, args, bindings)?;
                self.build(ctx, rule, &Builder::Ref(*result), bindings)
            }
            Builder::OpName(slot) => Ok(Value::Op(self.bound_op(rule, *slot, bindings)?)),
            Builder::Name(op) => Ok(Value::Op(*op)),
            Builder::Str(text) => Ok(Value::Str(text.clone())),
            Builder::Number(value) => Ok(Value::Int(*value)),
            Builder::List(items) => Ok(Value::List(self.build_all(ctx, rule, items, bindings)?)),
            Builder::BindValue { slot, target } => {
                let value = self.build(ctx, rule, target, bindings)?;
                bindings[*slot] = Some(Binding::Value(Box::new(value.clone())));
                Ok(value)
            }
        }
    }

    fn build_all<'a>(
        &self,
        ctx: &EngineContext<'_, '_>,
        rule: &CompiledRule,
        builders: &[Builder],
        bindings: &mut Bindings<'a>,
    ) -> Result<Vec<Value>> {
        builders
            .iter()
            .map(|builder| self.build(ctx, rule, builder, bindings))
            .collect()
    }

    fn bound_op(&self, rule: &CompiledRule, slot: usize, bindings: &Bindings<'_>) -> Result<Op> {
        let op = match &bindings[slot] {
            Some(Binding::Node(node)) => node.op(),
            Some(Binding::Value(value)) => match value.as_ref() {
                Value::Op(op) => Some(*op),
                Value::Expr(expr) => Some(nodes::expr_op(expr)),
                Value::Plan(plan) => Some(nodes::plan_op(plan)),
                _ => None,
            },
            None => None,
        };
        op.ok_or_else(|| {
            engine_error(format!(
                "{}: OpName of a variable that holds no operator",
                rule.name
            ))
        })
    }
}

/// Turns the patterns of one rule into matchers and builders.
struct RuleBuilder<'a> {
    compiled: &'a Compiled,
    define_ops: &'a [Op],
    lookup: fn(&str) -> Option<CustomFn>,
    slots: HashMap<String, usize>,
    rule: &'a str,
}

impl RuleBuilder<'_> {
    fn error(&self, text: &str) -> String {
        format!("rule {}: {text}", self.rule)
    }

    fn slot(&mut self, label: &str) -> usize {
        let next = self.slots.len();
        *self.slots.entry(label.to_string()).or_insert(next)
    }

    fn known_slot(&self, label: &str) -> std::result::Result<usize, String> {
        self.slots
            .get(label)
            .copied()
            .ok_or_else(|| self.error(&format!("${label} is not bound")))
    }

    fn function(&self, name: &str) -> std::result::Result<CustomFn, String> {
        (self.lookup)(name).ok_or_else(|| self.error(&format!("no function named {name}")))
    }

    fn op_of_define_name(&self, name: &str) -> std::result::Result<Op, String> {
        let define = self
            .compiled
            .lookup_define(name)
            .ok_or_else(|| self.error(&format!("{name} is not an operator")))?;
        Ok(self.define_ops[define])
    }

    fn ops_of_names(&self, names: &[String]) -> std::result::Result<Vec<Op>, String> {
        let mut ops = Vec::new();
        for name in names {
            let defines = self.compiled.lookup_matching_defines(name);
            if defines.is_empty() {
                return Err(self.error(&format!("{name} is not an operator or a tag")));
            }
            ops.extend(defines.into_iter().map(|define| self.define_ops[define]));
        }
        Ok(ops)
    }

    fn matcher(&mut self, expr: &optgen::Expr) -> std::result::Result<Matcher, String> {
        Ok(match &expr.kind {
            ExprKind::Func {
                name: FuncName::Names(names),
                args,
            } => Matcher::Node {
                ops: self.ops_of_names(names)?,
                args: args
                    .iter()
                    .map(|arg| self.matcher(arg))
                    .collect::<std::result::Result<_, _>>()?,
            },
            ExprKind::Func {
                name: FuncName::Dynamic(_),
                ..
            } => return Err(self.error("a match pattern cannot use a dynamic name")),
            ExprKind::CustomFunc { name, args } => Matcher::Custom {
                func: self.function(name)?,
                args: self.builders(args)?,
            },
            ExprKind::And(left, right) => Matcher::And(
                Box::new(self.matcher(left)?),
                Box::new(self.matcher(right)?),
            ),
            ExprKind::Not(input) => Matcher::Not(Box::new(self.matcher(input)?)),
            ExprKind::List(items) => self.list_matcher(items)?,
            ExprKind::Bind { label, target } => {
                let slot = self.slot(label);
                Matcher::Bind {
                    slot,
                    target: Box::new(self.matcher(target)?),
                }
            }
            ExprKind::Let {
                labels,
                target,
                result,
            } => {
                let ExprKind::CustomFunc { name, args } = &target.kind else {
                    return Err(self.error("a Let must call a function"));
                };
                let func = self.function(name)?;
                let args = self.builders(args)?;
                let slots = labels.iter().map(|label| self.slot(label)).collect();
                Matcher::Let {
                    slots,
                    func,
                    args,
                    result: self.known_slot(result)?,
                }
            }
            ExprKind::Any => Matcher::Any,
            ExprKind::Str(text) => Matcher::Str(text.clone()),
            ExprKind::Number(value) => Matcher::Number(*value),
            ExprKind::Ref(_) | ExprKind::Name(_) | ExprKind::ListAny => {
                return Err(self.error(&format!("{expr} cannot be matched")))
            }
        })
    }

    fn list_matcher(&mut self, items: &[optgen::Expr]) -> std::result::Result<Matcher, String> {
        let is_any = |item: &optgen::Expr| matches!(item.kind, ExprKind::ListAny);
        let (kind, item) = match items {
            [] => (ListKind::Empty, None),
            [item] if !is_any(item) => (ListKind::Single, Some(item)),
            [item, rest] if !is_any(item) && is_any(rest) => (ListKind::First, Some(item)),
            [rest, item] if is_any(rest) && !is_any(item) => (ListKind::Last, Some(item)),
            [before, item, after] if is_any(before) && is_any(after) && !is_any(item) => {
                (ListKind::Any, Some(item))
            }
            _ => return Err(self.error("unsupported list pattern")),
        };
        let item = match item {
            Some(item) => Some(Box::new(self.matcher(item)?)),
            None => None,
        };
        Ok(Matcher::List { kind, item })
    }

    fn builders(&mut self, exprs: &[optgen::Expr]) -> std::result::Result<Vec<Builder>, String> {
        exprs.iter().map(|expr| self.builder(expr)).collect()
    }

    fn builder(&mut self, expr: &optgen::Expr) -> std::result::Result<Builder, String> {
        Ok(match &expr.kind {
            ExprKind::Ref(label) => Builder::Ref(self.known_slot(label)?),
            ExprKind::Func {
                name: FuncName::Names(names),
                args,
            } => {
                let [name] = names.as_slice() else {
                    return Err(self.error("a constructor has one name"));
                };
                Builder::Construct {
                    op: self.op_of_define_name(name)?,
                    args: self.builders(args)?,
                }
            }
            ExprKind::Func {
                name: FuncName::Dynamic(name),
                args,
            } => {
                let slot = self.op_name_slot(name)?;
                Builder::DynamicConstruct {
                    slot,
                    args: self.builders(args)?,
                }
            }
            ExprKind::CustomFunc { name, .. } if name == OP_NAME_FUNCTION => {
                Builder::OpName(self.op_name_slot(expr)?)
            }
            ExprKind::CustomFunc { name, args } => Builder::Custom {
                func: self.function(name)?,
                args: self.builders(args)?,
            },
            ExprKind::Let {
                labels,
                target,
                result,
            } => {
                let ExprKind::CustomFunc { name, args } = &target.kind else {
                    return Err(self.error("a Let must call a function"));
                };
                let func = self.function(name)?;
                let args = self.builders(args)?;
                let slots = labels.iter().map(|label| self.slot(label)).collect();
                Builder::Let {
                    slots,
                    func,
                    args,
                    result: self.known_slot(result)?,
                }
            }
            ExprKind::Name(name) => Builder::Name(self.op_of_define_name(name)?),
            ExprKind::Str(text) => Builder::Str(text.clone()),
            ExprKind::Number(value) => Builder::Number(*value),
            ExprKind::List(items) => Builder::List(self.builders(items)?),
            ExprKind::And(..) | ExprKind::Not(_) | ExprKind::Any | ExprKind::ListAny => {
                return Err(self.error(&format!("{expr} cannot be built")))
            }
            ExprKind::Bind { label, target } => {
                let slot = self.slot(label);
                Builder::BindValue {
                    slot,
                    target: Box::new(self.builder(target)?),
                }
            }
        })
    }

    /// The variable of an `(OpName $var)` call.
    fn op_name_slot(&self, expr: &optgen::Expr) -> std::result::Result<usize, String> {
        let ExprKind::CustomFunc { name, args } = &expr.kind else {
            return Err(self.error("a dynamic name must be an OpName call"));
        };
        if name != OP_NAME_FUNCTION {
            return Err(self.error("a dynamic name must be an OpName call"));
        }
        let [arg] = args.as_slice() else {
            return Err(self.error("OpName takes one variable"));
        };
        let ExprKind::Ref(label) = &arg.kind else {
            return Err(self.error("OpName takes one variable"));
        };
        self.known_slot(label)
    }
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
            .normalize_expr(&EngineContext { resolver: None }, &mut expr, context)
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
    fn the_rule_files_compile() {
        let names = rule_set().rule_names();
        for name in [
            "NormalizeNestedAnds",
            "ExtractRedundantConjunct",
            "FoldBinary",
            "CommuteVar",
            "SimplifyFilterTerms",
            "RemoveNotNullCondition",
        ] {
            assert!(names.contains(&name), "{name} is missing from {names:?}");
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
            .normalize_plan(&EngineContext { resolver: None }, &mut node)
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
    fn null_tests_on_not_null_columns_simplify() {
        let table = "CREATE TABLE t(a INTEGER NOT NULL, b)";
        let mut node = filter_over(scan(table, 0), &["a IS NOT NULL", "b = 1"]);
        rule_set()
            .normalize_plan(&EngineContext { resolver: None }, &mut node)
            .unwrap();
        assert_eq!(shown_terms(&node), vec!["t.b = 1"]);

        let mut node = filter_over(scan(table, 0), &["b IS NOT NULL"]);
        rule_set()
            .normalize_plan(&EngineContext { resolver: None }, &mut node)
            .unwrap();
        assert_eq!(shown_terms(&node), vec!["t.b IS NOT NULL"]);

        let mut node = filter_over(scan(table, 0), &["a IS NULL", "b = 1"]);
        rule_set()
            .normalize_plan(&EngineContext { resolver: None }, &mut node)
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
            .normalize_plan(&EngineContext { resolver: None }, &mut node)
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
