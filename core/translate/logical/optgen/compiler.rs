//! Check rule files and give every pattern a type.
//!
//! The compiler parses the files, checks that every name is an operator, a
//! tag, or a function written in Rust, checks that every variable is bound
//! before it is used, and expands a rule that matches several operators into
//! one rule per operator. It then infers the type of every pattern, so that a
//! pattern that can never match is reported as an error.

use std::collections::HashMap;
#[cfg(test)]
use std::fmt::{self, Display, Formatter};

use super::parser::{Define, Expr, ExprKind, FuncName, Parser, Root, Rule, SourceLoc};

const OP_NAME_FUNCTION: &str = "OpName";

/// What a pattern matches or constructs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DataType {
    /// Nothing is known.
    Any,
    List,
    Str,
    Int64,
    /// One of these operators. The values index `Compiled::defines`.
    Defines(Vec<usize>),
    /// A type named in a define, such as a private field.
    External(String),
}

impl DataType {
    pub fn is_builtin(&self) -> bool {
        matches!(self, DataType::List | DataType::Str | DataType::Int64)
    }

    /// Whether the two types cannot both describe one expression.
    pub fn contradicts(&self, other: &DataType) -> bool {
        if self == other || *self == DataType::Any || *other == DataType::Any {
            return false;
        }
        match (self, other) {
            (DataType::Defines(left), DataType::Defines(right)) => {
                let (small, large) = if left.len() <= right.len() {
                    (left, right)
                } else {
                    (right, left)
                };
                small.iter().any(|define| !large.contains(define))
            }
            (DataType::Defines(_), other) | (other, DataType::Defines(_)) => other.is_builtin(),
            (DataType::External(left), DataType::External(right)) => left != right,
            (left, right) => {
                if left.is_builtin() && right.is_builtin() {
                    return true;
                }
                false
            }
        }
    }

    /// Whether this type says more than `other`.
    pub fn is_more_restrictive_than(&self, other: &DataType) -> bool {
        match self {
            DataType::Defines(defines) => match other {
                DataType::Defines(others) => defines.len() < others.len(),
                _ => true,
            },
            DataType::Any => false,
            DataType::List => *other == DataType::Any,
            _ => matches!(other, DataType::List | DataType::Any),
        }
    }

    pub fn most_restrictive(self, other: DataType) -> DataType {
        if other.is_more_restrictive_than(&self) {
            other
        } else {
            self
        }
    }
}

#[cfg(test)]
pub(crate) struct DataTypeDisplay<'a> {
    typ: &'a DataType,
    compiled: &'a Compiled,
}

#[cfg(test)]
impl Display for DataTypeDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.typ {
            DataType::Any => write!(f, "<any>"),
            DataType::List => write!(f, "<list>"),
            DataType::Str => write!(f, "<string>"),
            DataType::Int64 => write!(f, "<int64>"),
            DataType::External(name) => write!(f, "{name}"),
            DataType::Defines(defines) => {
                if defines.len() > 1 {
                    write!(f, "[")?;
                }
                for (index, define) in defines.iter().enumerate() {
                    if index > 0 {
                        write!(f, " | ")?;
                    }
                    write!(f, "{}", self.compiled.defines[*define].name)?;
                }
                if defines.len() > 1 {
                    write!(f, "]")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Compiled {
    pub defines: Vec<Define>,
    /// One rule per operator that the written rule matches at the top.
    pub rules: Vec<Rule>,
    pub define_tags: Vec<String>,
    define_index: HashMap<String, usize>,
}

impl Compiled {
    pub fn lookup_define(&self, name: &str) -> Option<usize> {
        self.define_index.get(name).copied()
    }

    /// The operators with this name, or with this tag.
    pub fn lookup_matching_defines(&self, name: &str) -> Vec<usize> {
        if let Some(define) = self.lookup_define(name) {
            return vec![define];
        }
        self.defines
            .iter()
            .enumerate()
            .filter(|(_, define)| define.has_tag(name))
            .map(|(index, _)| index)
            .collect()
    }

    /// The rules that match this operator at the top of their pattern.
    /// The rules that match this operator at their top.
    #[cfg(test)]
    pub fn lookup_matching_rules(&self, op_name: &str) -> Vec<usize> {
        self.rules
            .iter()
            .enumerate()
            .filter(|(_, rule)| rule.match_pattern.single_name() == op_name)
            .map(|(index, _)| index)
            .collect()
    }

    #[cfg(test)]
    pub fn display_type<'a>(&'a self, typ: &'a DataType) -> DataTypeDisplay<'a> {
        DataTypeDisplay {
            typ,
            compiled: self,
        }
    }
}

/// Parse and check rule files. `files` holds `(name, source)` pairs.
pub(crate) fn compile(files: &[(&str, &str)]) -> Result<Compiled, Vec<String>> {
    let root = Parser::new(files).parse()?;
    let mut compiler = Compiler {
        compiled: Compiled::default(),
        errors: Vec::new(),
    };
    compiler.compile_root(root);
    if compiler.errors.is_empty() {
        Ok(compiler.compiled)
    } else {
        Err(compiler.errors)
    }
}

struct Compiler {
    compiled: Compiled,
    errors: Vec<String>,
}

impl Compiler {
    fn compile_root(&mut self, root: Root) {
        self.compile_defines(root.defines);
        self.compile_rules(root.rules);
    }

    fn compile_defines(&mut self, defines: Vec<Define>) {
        for (index, define) in defines.iter().enumerate() {
            if self.compiled.define_index.contains_key(&define.name) {
                self.add_error(
                    Some(&define.src),
                    &format!("duplicate '{}' define statement", define.name),
                );
            }
            for (position, field) in define.fields.iter().enumerate() {
                if define.fields[..position]
                    .iter()
                    .any(|earlier| earlier.name == field.name)
                {
                    self.add_error(
                        Some(&field.src),
                        &format!("duplicate '{}' field in {}", field.name, define.name),
                    );
                }
            }
            self.compiled
                .define_index
                .insert(define.name.clone(), index);
            for tag in &define.tags {
                if !self.compiled.define_tags.contains(tag) {
                    self.compiled.define_tags.push(tag.clone());
                }
            }
        }
        self.compiled.defines = defines;
    }

    fn compile_rules(&mut self, rules: Vec<Rule>) {
        let mut names: Vec<&str> = Vec::new();
        for rule in &rules {
            if names.contains(&rule.name.as_str()) {
                self.add_error(
                    Some(&rule.src),
                    &format!("duplicate rule name '{}'", rule.name),
                );
            }
            names.push(&rule.name);
        }
        for rule in &rules {
            self.compile_rule(rule);
        }
    }

    fn compile_rule(&mut self, rule: &Rule) {
        let ExprKind::Func {
            name: FuncName::Names(names),
            ..
        } = &rule.match_pattern.kind
        else {
            self.add_error(Some(&rule.match_pattern.src), "cannot match dynamic name");
            return;
        };
        for name in names {
            let defines = self.compiled.lookup_matching_defines(name);
            if defines.is_empty() {
                self.add_error(
                    Some(&rule.match_pattern.src),
                    &format!("unrecognized match name '{name}'"),
                );
            }
            for define in defines {
                let op_name = self.compiled.defines[define].name.clone();
                self.expand_rule(rule, op_name);
            }
        }
    }

    /// Make a copy of the rule that matches one operator at the top.
    fn expand_rule(&mut self, rule: &Rule, op_name: String) {
        let errors_before = self.errors.len();
        let ExprKind::Func { args, .. } = &rule.match_pattern.kind else {
            unreachable!("compile_rule checks the match pattern")
        };
        let mut match_pattern = Expr::new(
            ExprKind::Func {
                name: FuncName::Names(vec![op_name.clone()]),
                args: args.clone(),
            },
            rule.match_pattern.src.clone(),
        );
        let mut rule_compiler = RuleCompiler {
            compiler: self,
            bindings: HashMap::new(),
            op_name,
        };
        match_pattern = ContentCompiler::for_match().compile(&mut rule_compiler, match_pattern);
        let mut replace =
            ContentCompiler::for_replace().compile(&mut rule_compiler, rule.replace.clone());
        if errors_before == rule_compiler.compiler.errors.len() {
            rule_compiler.infer_types(&mut match_pattern, DataType::Any);
            rule_compiler.infer_types(&mut replace, DataType::Any);
        }
        self.compiled.rules.push(Rule {
            comments: Vec::new(),
            name: rule.name.clone(),
            tags: rule.tags.clone(),
            match_pattern,
            replace,
            src: rule.src.clone(),
        });
    }

    fn add_error(&mut self, src: Option<&SourceLoc>, text: &str) {
        let message = match src {
            Some(src) => format!("{src}: {text}"),
            None => text.to_string(),
        };
        if !self.errors.contains(&message) {
            self.errors.push(message);
        }
    }
}

struct RuleCompiler<'a> {
    compiler: &'a mut Compiler,
    bindings: HashMap<String, DataType>,
    op_name: String,
}

impl RuleCompiler<'_> {
    fn add_error(&mut self, src: &SourceLoc, text: &str) {
        self.compiler.add_error(Some(src), text);
    }

    fn infer_types(&mut self, expr: &mut Expr, suggested: DataType) {
        match &mut expr.kind {
            ExprKind::Func { name, args } => {
                let defines = match name {
                    FuncName::Dynamic(name_expr) => {
                        let (defines, names_the_root) = match &name_expr.kind {
                            ExprKind::CustomFunc { name, args }
                                if name == OP_NAME_FUNCTION && args.is_empty() =>
                            {
                                let Some(define) =
                                    self.compiler.compiled.lookup_define(&self.op_name)
                                else {
                                    let src = name_expr.src.clone();
                                    self.add_error(&src, "the match pattern has no operator");
                                    return;
                                };
                                (vec![define], true)
                            }
                            ExprKind::CustomFunc { name, args }
                                if name == OP_NAME_FUNCTION && args.len() == 1 =>
                            {
                                let label = match &args[0].kind {
                                    ExprKind::Ref(label) => label.clone(),
                                    _ => unreachable!("compile_func checks the OpName argument"),
                                };
                                let Some(typ) = self.bindings.get(&label).cloned() else {
                                    let src = name_expr.src.clone();
                                    self.add_error(
                                        &src,
                                        &format!("${label} does not have its type set"),
                                    );
                                    return;
                                };
                                let DataType::Defines(defines) = typ else {
                                    let src = name_expr.args_src();
                                    self.add_error(
                                        &src,
                                        "cannot infer type of construction expression",
                                    );
                                    return;
                                };
                                (defines, false)
                            }
                            _ => {
                                let src = name_expr.src.clone();
                                self.add_error(&src, "dynamic name must be an OpName call");
                                return;
                            }
                        };
                        if !names_the_root && defines.len() == 1 {
                            let static_name =
                                self.compiler.compiled.defines[defines[0]].name.clone();
                            *name = FuncName::Names(vec![static_name]);
                        }
                        defines
                    }
                    FuncName::Names(names) => {
                        let mut defines = Vec::new();
                        for name in names.iter() {
                            defines.extend(self.compiler.compiled.lookup_matching_defines(name));
                        }
                        defines
                    }
                };
                expr.typ = DataType::Defines(defines.clone());
                let prototype = &self.compiler.compiled.defines[defines[0]];
                if args.len() > prototype.fields.len() {
                    let message = format!("{} has too many args", prototype.name);
                    let src = expr.src.clone();
                    self.add_error(&src, &message);
                    return;
                }
                let field_types: Vec<String> = prototype
                    .fields
                    .iter()
                    .map(|field| field.type_name.clone())
                    .collect();
                for (arg, field_type) in args.iter_mut().zip(field_types) {
                    self.infer_types(arg, DataType::External(field_type));
                }
            }
            ExprKind::CustomFunc { args, .. } => {
                expr.typ = suggested;
                for arg in args {
                    self.infer_types(arg, DataType::Any);
                }
            }
            ExprKind::Let { result, .. } => {
                let Some(typ) = self.bindings.get(result).cloned() else {
                    let src = expr.src.clone();
                    self.add_error(&src, &format!("${result} does not have its type set"));
                    return;
                };
                expr.typ = typ.most_restrictive(suggested);
            }
            ExprKind::Bind { label, target } => {
                self.infer_types(target, suggested);
                expr.typ = target.typ.clone();
                self.bindings.insert(label.clone(), expr.typ.clone());
            }
            ExprKind::Ref(label) => {
                let Some(typ) = self.bindings.get(label).cloned() else {
                    let src = expr.src.clone();
                    self.add_error(&src, &format!("${label} does not have its type set"));
                    return;
                };
                expr.typ = typ.most_restrictive(suggested);
            }
            ExprKind::And(left, right) => {
                self.infer_types(left, suggested.clone());
                self.infer_types(right, suggested);
                if left.typ.contradicts(&right.typ) {
                    let src = expr.src.clone();
                    self.add_error(
                        &src,
                        "match patterns contradict one another; both cannot match",
                    );
                }
                expr.typ = left.typ.clone().most_restrictive(right.typ.clone());
            }
            ExprKind::Not(input) => {
                self.infer_types(input, suggested.clone());
                expr.typ = suggested;
            }
            ExprKind::List(items) => {
                expr.typ = DataType::List.most_restrictive(suggested);
                for item in items {
                    self.infer_types(item, DataType::Any);
                }
            }
            ExprKind::Any => expr.typ = suggested,
            ExprKind::Str(_) => expr.typ = DataType::Str,
            ExprKind::Number(_) => expr.typ = DataType::Int64,
            ExprKind::Name(_) | ExprKind::ListAny => {}
        }
    }
}

impl Expr {
    /// The location of the first argument of a function, or of the function.
    fn args_src(&self) -> SourceLoc {
        match &self.kind {
            ExprKind::Func { args, .. } | ExprKind::CustomFunc { args, .. } => args
                .first()
                .map(|arg| arg.src.clone())
                .unwrap_or_else(|| self.src.clone()),
            _ => self.src.clone(),
        }
    }
}

/// Checks one match pattern or one replace pattern.
struct ContentCompiler {
    match_pattern: bool,
    custom_func: bool,
    in_let: bool,
}

impl ContentCompiler {
    fn for_match() -> Self {
        Self {
            match_pattern: true,
            custom_func: false,
            in_let: false,
        }
    }

    fn for_replace() -> Self {
        Self {
            match_pattern: false,
            custom_func: false,
            in_let: false,
        }
    }

    fn nested(&self) -> Self {
        Self {
            match_pattern: self.match_pattern,
            custom_func: false,
            in_let: false,
        }
    }

    fn compile(&self, rule: &mut RuleCompiler<'_>, expr: Expr) -> Expr {
        let src = expr.src.clone();
        match expr.kind {
            ExprKind::Func { name, args } => self.compile_func(rule, name, args, src),
            ExprKind::Let {
                labels,
                target,
                result,
            } => self.compile_let(rule, labels, *target, result, src),
            ExprKind::Bind { label, target } => self.compile_bind(rule, label, *target, src),
            ExprKind::Ref(label) => {
                if self.match_pattern && !self.custom_func && !self.in_let {
                    self.add_disallowed_error(rule, &src, "cannot use variable references");
                } else if !rule.bindings.contains_key(&label) {
                    rule.add_error(&src, &format!("unrecognized variable name '{label}'"));
                }
                Expr::new(ExprKind::Ref(label), src)
            }
            ExprKind::List(items) => {
                if self.match_pattern && self.custom_func {
                    self.add_disallowed_error(rule, &src, "cannot use lists");
                } else {
                    self.check_list(rule, &items, &src);
                }
                let items = items
                    .into_iter()
                    .map(|item| self.compile(rule, item))
                    .collect();
                Expr::new(ExprKind::List(items), src)
            }
            ExprKind::And(left, right) => {
                if !self.match_pattern || self.custom_func {
                    self.add_disallowed_error(rule, &src, "cannot use boolean expressions");
                }
                let left = self.compile(rule, *left);
                let right = self.compile(rule, *right);
                Expr::new(ExprKind::And(Box::new(left), Box::new(right)), src)
            }
            ExprKind::Not(input) => {
                if !self.match_pattern || self.custom_func {
                    self.add_disallowed_error(rule, &src, "cannot use boolean expressions");
                }
                let input = self.compile(rule, *input);
                Expr::new(ExprKind::Not(Box::new(input)), src)
            }
            ExprKind::Name(name) => {
                if self.match_pattern && !self.custom_func {
                    rule.add_error(&src, &format!("cannot match literal name '{name}'"));
                } else if rule.compiler.compiled.lookup_define(&name).is_none() {
                    rule.add_error(&src, &format!("{name} is not an operator name"));
                }
                Expr::new(ExprKind::Name(name), src)
            }
            ExprKind::Any => {
                if !self.match_pattern || self.custom_func {
                    self.add_disallowed_error(rule, &src, "cannot use wildcard matcher");
                }
                Expr::new(ExprKind::Any, src)
            }
            kind @ (ExprKind::CustomFunc { .. }
            | ExprKind::ListAny
            | ExprKind::Str(_)
            | ExprKind::Number(_)) => Expr::new(kind, src),
        }
    }

    fn compile_bind(
        &self,
        rule: &mut RuleCompiler<'_>,
        label: String,
        target: Expr,
        src: SourceLoc,
    ) -> Expr {
        if rule.bindings.contains_key(&label) {
            rule.add_error(&src, &format!("duplicate bind label '{label}'"));
        }
        rule.bindings.insert(label.clone(), DataType::Any);
        let target = self.compile(rule, target);
        Expr::new(
            ExprKind::Bind {
                label,
                target: Box::new(target),
            },
            src,
        )
    }

    fn check_list(&self, rule: &mut RuleCompiler<'_>, items: &[Expr], src: &SourceLoc) {
        let mut found_item = false;
        for item in items {
            if matches!(item.kind, ExprKind::ListAny) {
                if !self.match_pattern {
                    rule.add_error(src, "list constructor cannot use '...'");
                }
            } else {
                if self.match_pattern && found_item {
                    rule.add_error(
                        &item.src,
                        "list matcher cannot contain multiple expressions",
                    );
                    break;
                }
                found_item = true;
            }
        }
    }

    fn compile_let(
        &self,
        rule: &mut RuleCompiler<'_>,
        labels: Vec<String>,
        target: Expr,
        result: String,
        src: SourceLoc,
    ) -> Expr {
        let mut nested = self.nested();
        nested.in_let = true;
        let target = nested.compile(rule, target);
        if !matches!(target.kind, ExprKind::CustomFunc { .. }) {
            rule.add_error(&src, "let target must be a custom function");
        }
        let mut let_bindings = HashMap::new();
        for label in &labels {
            if rule.bindings.contains_key(label) {
                rule.add_error(&src, &format!("duplicate bind label '{label}'"));
            }
            rule.bindings.insert(label.clone(), DataType::Any);
            let_bindings.insert(label.clone(), DataType::Any);
        }
        if !let_bindings.contains_key(&result) {
            rule.add_error(&src, &format!("unrecognized variable name '{result}'"));
        }
        Expr::new(
            ExprKind::Let {
                labels,
                target: Box::new(target),
                result,
            },
            src,
        )
    }

    fn compile_func(
        &self,
        rule: &mut RuleCompiler<'_>,
        name: FuncName,
        args: Vec<Expr>,
        src: SourceLoc,
    ) -> Expr {
        let mut nested = self.nested();
        let func_name = match name {
            FuncName::Dynamic(name_expr) => {
                if self.match_pattern {
                    rule.add_error(&src, "cannot match dynamic name");
                }
                let ExprKind::Func {
                    name: inner_name,
                    args: inner_args,
                } = name_expr.kind
                else {
                    unreachable!("the parser gives a function as a dynamic name")
                };
                let compiled_name = self.compile_func(rule, inner_name, inner_args, name_expr.src);
                match compiled_name.kind {
                    ExprKind::Name(name) => FuncName::Names(vec![name]),
                    _ => FuncName::Dynamic(Box::new(compiled_name)),
                }
            }
            FuncName::Names(names) => {
                let Some(names) = self.check_names(rule, names, &src) else {
                    return Expr::new(
                        ExprKind::Func {
                            name: FuncName::Names(Vec::new()),
                            args,
                        },
                        src,
                    );
                };
                let mut prototype: Option<usize> = None;
                for name in &names {
                    let defines = rule.compiler.compiled.lookup_matching_defines(name);
                    if !defines.is_empty() {
                        for define in defines {
                            let fields = &rule.compiler.compiled.defines[define].fields;
                            if fields.len() < args.len() {
                                let message = format!(
                                    "{} has only {} fields",
                                    rule.compiler.compiled.defines[define].name,
                                    fields.len()
                                );
                                rule.add_error(&src, &message);
                                continue;
                            }
                            let Some(prototype) = prototype else {
                                prototype = Some(define);
                                continue;
                            };
                            let prototype_fields =
                                &rule.compiler.compiled.defines[prototype].fields;
                            let same_types = (0..args.len())
                                .all(|i| fields[i].type_name == prototype_fields[i].type_name);
                            if !same_types {
                                let message = format!(
                                    "{} and {} fields do not have same types",
                                    rule.compiler.compiled.defines[define].name,
                                    rule.compiler.compiled.defines[prototype].name
                                );
                                rule.add_error(&src, &message);
                            }
                        }
                        continue;
                    }
                    if names.len() != 1 {
                        rule.add_error(&src, "custom function cannot have multiple names");
                        return Expr::new(
                            ExprKind::Func {
                                name: FuncName::Names(names),
                                args,
                            },
                            src,
                        );
                    }
                    if name == OP_NAME_FUNCTION {
                        if let Some(op_name) = self.compile_op_name(rule, &args, &src) {
                            return op_name;
                        }
                    }
                    nested.custom_func = true;
                }
                FuncName::Names(names)
            }
        };
        if self.match_pattern && self.custom_func && !nested.custom_func {
            rule.add_error(&src, "custom function name cannot be an operator name");
            return Expr::new(
                ExprKind::Func {
                    name: func_name,
                    args,
                },
                src,
            );
        }
        let args: Vec<Expr> = args
            .into_iter()
            .map(|arg| nested.compile(rule, arg))
            .collect();
        if nested.custom_func {
            let FuncName::Names(mut names) = func_name else {
                unreachable!("a custom function has a static name")
            };
            return Expr::new(
                ExprKind::CustomFunc {
                    name: names.remove(0),
                    args,
                },
                src,
            );
        }
        Expr::new(
            ExprKind::Func {
                name: func_name,
                args,
            },
            src,
        )
    }

    /// Check that the names of a function are legal here.
    fn check_names(
        &self,
        rule: &mut RuleCompiler<'_>,
        names: Vec<String>,
        src: &SourceLoc,
    ) -> Option<Vec<String>> {
        if !self.match_pattern {
            if names.len() != 1 {
                rule.add_error(src, "constructor cannot have multiple names");
                return None;
            }
            let compiled = &rule.compiler.compiled;
            if compiled.lookup_matching_defines(&names[0]).is_empty() {
                return Some(names);
            }
            if compiled.lookup_define(&names[0]).is_none() {
                rule.add_error(src, "construct name cannot be a tag");
                return None;
            }
        }
        Some(names)
    }

    /// `(OpName)` names the operator of the whole match pattern. It stays a
    /// function call, so one compiled rule can serve every operator that the
    /// rule matches. `(OpName $var)` also stays a function call; type
    /// inference can still turn it into a name when the variable has one
    /// possible operator.
    fn compile_op_name(
        &self,
        rule: &mut RuleCompiler<'_>,
        args: &[Expr],
        src: &SourceLoc,
    ) -> Option<Expr> {
        if args.len() > 1 {
            rule.add_error(src, "too many arguments to OpName function");
            return None;
        }
        if args.is_empty() {
            return None;
        }
        if !matches!(args[0].kind, ExprKind::Ref(_)) {
            rule.add_error(
                src,
                "invalid OpName argument: argument must be a variable reference",
            );
        }
        None
    }

    fn add_disallowed_error(&self, rule: &mut RuleCompiler<'_>, src: &SourceLoc, what: &str) {
        let scope = match (self.match_pattern, self.custom_func) {
            (true, true) => "custom match function",
            (true, false) => "match pattern",
            (false, true) => "custom replace function",
            (false, false) => "replace pattern",
        };
        rule.add_error(src, &format!("{scope} {what}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPS: &str = "\
[Scalar]
define Variable { Col Column }
[Scalar, ConstValue]
define Const { Value Literal }
[Scalar, Bool]
define True {}
[Scalar, Bool]
define False {}
[Scalar, Bool]
define And { Left Scalar Right Scalar }
[Scalar, Bool]
define Or { Left Scalar Right Scalar }
[Scalar, Bool]
define Not { Input Scalar }
[Scalar, Comparison]
define Eq { Left Scalar Right Scalar }
[Scalar, Comparison]
define Lt { Left Scalar Right Scalar }
[Scalar]
define Tuple { Elems ScalarList }
[Relational]
define Select { Input Relational Filters FiltersList }
";

    fn compile_rules(rules: &str) -> Result<Compiled, Vec<String>> {
        compile(&[("ops.opt", OPS), ("rules.opt", rules)])
    }

    fn compile_ok(rules: &str) -> Compiled {
        compile_rules(rules).unwrap_or_else(|errors| panic!("compile errors: {errors:?}"))
    }

    fn compile_errors(rules: &str) -> Vec<String> {
        compile_rules(rules).expect_err("the rules have errors")
    }

    fn typ(compiled: &Compiled, expr: &Expr) -> String {
        compiled.display_type(&expr.typ).to_string()
    }

    #[test]
    fn a_rule_over_a_tag_becomes_one_rule_per_operator() {
        let compiled = compile_ok("[Negate]\n(Not $input:(Comparison $left:* $right:*)) => (Not $input)\n[Swap]\n(Eq | Lt $left:* $right:*) => ((OpName) $right $left)\n");
        assert_eq!(compiled.rules.len(), 3);
        assert_eq!(compiled.lookup_matching_rules("Not").len(), 1);
        assert_eq!(compiled.lookup_matching_rules("Eq").len(), 1);
        assert_eq!(compiled.lookup_matching_rules("Lt").len(), 1);
        assert!(compiled.lookup_matching_rules("And").is_empty());
        let swap_for_lt = &compiled.rules[compiled.lookup_matching_rules("Lt")[0]];
        assert_eq!(swap_for_lt.replace.to_string(), "((OpName) $right $left)");
        assert_eq!(
            swap_for_lt.match_pattern.to_string(),
            "(Lt $left:* $right:*)"
        );
    }

    #[test]
    fn types_flow_from_defines_and_bindings() {
        let compiled = compile_ok(
            "[Rule]\n(Select $input:* $filters:[ ... $item:(Eq | Lt) & (Check $item) ... ]) => (Select $input (Fix $filters))\n",
        );
        let rule = &compiled.rules[0];
        assert_eq!(typ(&compiled, &rule.match_pattern), "Select");
        let ExprKind::Func { args, .. } = &rule.match_pattern.kind else {
            panic!("the match pattern is a function")
        };
        assert_eq!(typ(&compiled, &args[0]), "Relational");
        assert_eq!(typ(&compiled, &args[1]), "FiltersList");
        let ExprKind::Bind { target, .. } = &args[1].kind else {
            panic!("the filters are bound")
        };
        let ExprKind::List(items) = &target.kind else {
            panic!("the filters are a list")
        };
        assert_eq!(typ(&compiled, &items[1]), "[Eq | Lt]");
        let ExprKind::Func { args, .. } = &rule.replace.kind else {
            panic!("the replace pattern is a function")
        };
        assert_eq!(typ(&compiled, &args[0]), "Relational");
        assert!(matches!(args[1].kind, ExprKind::CustomFunc { .. }));
        assert_eq!(typ(&compiled, &args[1]), "FiltersList");
    }

    #[test]
    fn an_op_name_with_one_possible_operator_becomes_a_static_name() {
        let compiled = compile_ok(
            "[Rule]\n(Not $input:(Eq $left:* $right:*)) => ((OpName $input) $right $left)\n",
        );
        assert_eq!(compiled.rules[0].replace.to_string(), "(Eq $right $left)");
        let compiled = compile_ok("[Rule]\n(Not $input:(Comparison $left:* $right:*)) => ((OpName $input) $right $left)\n");
        assert_eq!(
            compiled.rules[0].replace.to_string(),
            "((OpName $input) $right $left)"
        );
    }

    #[test]
    fn let_binds_the_results_of_a_custom_function() {
        let compiled = compile_ok(
            "[Rule]\n(Or $left:* $right:* & (Let ($conjunct $ok):(FindShared $left $right) $ok)) => (Extract $conjunct $left $right)\n",
        );
        assert_eq!(
            compiled.rules[0].match_pattern.to_string(),
            "(Or $left:* $right:* & (Let ($conjunct $ok):(FindShared $left $right) $ok))"
        );
        assert_eq!(
            compiled.rules[0].replace.to_string(),
            "(Extract $conjunct $left $right)"
        );
    }

    #[test]
    fn semantic_errors() {
        let cases: Vec<(&str, &str)> = vec![
            (
                "[Rule]\n(Unknown $x:*) => $x\n",
                "rules.opt:2:1: unrecognized match name 'Unknown'",
            ),
            (
                "[Rule]\n(And $x:* $x:*) => $x\n",
                "rules.opt:2:11: duplicate bind label 'x'",
            ),
            (
                "[Rule]\n(And $x:* *) => $y\n",
                "rules.opt:2:17: unrecognized variable name 'y'",
            ),
            (
                "[Rule]\n(And $x:* $x) => $x\n",
                "rules.opt:2:11: match pattern cannot use variable references",
            ),
            (
                "[Rule]\n(And $x:* *) => (And $x *)\n",
                "rules.opt:2:25: replace pattern cannot use wildcard matcher",
            ),
            (
                "[Rule]\n(And $x:* *) => (And $x (True) & (False))\n",
                "rules.opt:2:25: replace pattern cannot use boolean expressions",
            ),
            (
                "[Rule]\n(And $x:* *) => (Comparison $x $x)\n",
                "rules.opt:2:17: construct name cannot be a tag",
            ),
            (
                "[Rule]\n(And $x:* *) => (And | Or $x $x)\n",
                "rules.opt:2:17: constructor cannot have multiple names",
            ),
            (
                "[Rule]\n(And $x:* $y:* $z:*) => $x\n",
                "rules.opt:2:1: And has only 2 fields",
            ),
            (
                "[Rule]\n(And $x:(Eq) & (Lt) *) => $x\n",
                "rules.opt:2:9: match patterns contradict one another; both cannot match",
            ),
            (
                "[Rule]\n(And $x:* *) => (Custom Unknown)\n",
                "rules.opt:2:25: Unknown is not an operator name",
            ),
            (
                "[Rule]\n(And $x:* Eq) => $x\n",
                "rules.opt:2:11: cannot match literal name 'Eq'",
            ),
            (
                "[Rule]\n(And $x:* *) => (Custom (Let ($a $b):(F $x) $b) [$x $x])\n[Rule]\n(Or $x:* *) => $x\n",
                "rules.opt:3:1: duplicate rule name 'Rule'",
            ),
            (
                "[Rule]\n(And $x:* *) => (OpName $x $x)\n",
                "rules.opt:2:17: too many arguments to OpName function",
            ),
            (
                "[Rule]\n(And $x:* *) => (OpName (True))\n",
                "rules.opt:2:17: invalid OpName argument: argument must be a variable reference",
            ),
            (
                "[Rule]\n(Tuple [ $a:* $b:* ]) => $a\n",
                "rules.opt:2:15: list matcher cannot contain multiple expressions",
            ),
            (
                "[Rule]\n(Tuple $x:*) => (Tuple [ ... ])\n",
                "rules.opt:2:24: list constructor cannot use '...'",
            ),
            (
                "[Rule]\n(And $x:* & (Custom [ $x ]) *) => $x\n",
                "rules.opt:2:21: custom match function cannot use lists",
            ),
            (
                "[Rule]\n(And $x:* & (Custom (Eq $x $x)) *) => $x\n",
                "rules.opt:2:21: custom function name cannot be an operator name",
            ),
            (
                "[Rule]\n(And $x:* & (Let ($a):(Eq $x $x) $a) *) => $x\n",
                "rules.opt:2:18: let target must be a custom function",
            ),
            (
                "[Rule]\n((OpName $x) $x:* *) => $x\n",
                "rules.opt:2:1: cannot match dynamic name",
            ),
        ];
        for (source, expected) in cases {
            let errors = compile_errors(source);
            assert!(
                errors.contains(&expected.to_string()),
                "for {source:?}: expected {expected:?} in {errors:?}"
            );
        }
    }

    #[test]
    fn duplicate_defines_and_fields_are_errors() {
        let errors = compile(&[("ops.opt", "define A {}\ndefine A {}\n")]).unwrap_err();
        assert_eq!(errors, vec!["ops.opt:2:1: duplicate 'A' define statement"]);
        let errors =
            compile(&[("ops.opt", "define A {\n  X Scalar\n  X Scalar\n}\n")]).unwrap_err();
        assert_eq!(errors, vec!["ops.opt:3:3: duplicate 'X' field in A"]);
    }

    #[test]
    fn rules_are_indexed_by_the_operator_they_match() {
        let compiled =
            compile_ok("[Rule]\n(Comparison $left:* $right:*) => (Not (Eq $left $right))\n");
        assert_eq!(compiled.lookup_matching_rules("Eq").len(), 1);
        assert_eq!(compiled.lookup_matching_rules("Lt").len(), 1);
        assert!(compiled.lookup_matching_rules("And").is_empty());
        let rule = &compiled.rules[compiled.lookup_matching_rules("Lt")[0]];
        assert_eq!(rule.match_pattern.to_string(), "(Lt $left:* $right:*)");
    }

    #[test]
    fn data_type_rules() {
        let eq_lt = DataType::Defines(vec![7, 8]);
        let eq = DataType::Defines(vec![7]);
        assert!(!eq.contradicts(&eq_lt));
        assert!(DataType::Defines(vec![4]).contradicts(&eq_lt));
        assert!(eq.contradicts(&DataType::List));
        assert!(!eq.contradicts(&DataType::External("Scalar".to_string())));
        assert!(
            DataType::External("A".to_string()).contradicts(&DataType::External("B".to_string()))
        );
        assert!(!DataType::External("A".to_string()).contradicts(&DataType::List));
        assert!(DataType::Str.contradicts(&DataType::Int64));
        assert!(!DataType::Any.contradicts(&DataType::Str));
        assert!(eq.is_more_restrictive_than(&eq_lt));
        assert!(eq_lt.is_more_restrictive_than(&DataType::External("Scalar".to_string())));
        assert!(DataType::List.is_more_restrictive_than(&DataType::Any));
        assert!(!DataType::List.is_more_restrictive_than(&DataType::Str));
        assert!(DataType::Str.is_more_restrictive_than(&DataType::List));
        assert_eq!(DataType::Any.most_restrictive(DataType::Str), DataType::Str);
        assert_eq!(DataType::Str.most_restrictive(DataType::Any), DataType::Str);
    }
}
