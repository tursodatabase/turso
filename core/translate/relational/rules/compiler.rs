use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

pub fn compile(path: &str, source: &str) -> Result<String, String> {
    let tokens = lex(path, source)?;
    let mut parser = Parser {
        path,
        tokens,
        next: 0,
    };
    let mut definitions = Definitions::default();
    let mut rules = Vec::new();
    let mut names = BTreeSet::new();
    while !parser.done() {
        let token = parser.take()?;
        match token.text.as_str() {
            "operator" => {
                let name = parser.identifier()?;
                parser.expect("(")?;
                let mut fields = Vec::new();
                let mut field_names = BTreeSet::new();
                while !parser.at(")") {
                    let field = parser.identifier()?;
                    if !field_names.insert(field.text.clone()) {
                        return Err(parser.error(&field, "duplicate operator field"));
                    }
                    parser.expect(":")?;
                    let ty = parser.identifier()?.text;
                    fields.push((field.text, ty));
                    if !parser.at(")") {
                        parser.expect(",")?;
                    }
                }
                parser.expect(")")?;
                parser.expect(";")?;
                if !names.insert(name.text.clone()) {
                    return Err(parser.error(&name, "duplicate definition"));
                }
                definitions.operators.insert(name.text, fields);
            }
            "predicate" | "constructor" => {
                let name = parser.identifier()?;
                parser.expect("(")?;
                let mut args = Vec::new();
                while !parser.at(")") {
                    args.push(parser.identifier()?.text);
                    if !parser.at(")") {
                        parser.expect(",")?;
                    }
                }
                parser.expect(")")?;
                let result = if token.text == "predicate" {
                    "Bool".to_owned()
                } else {
                    parser.expect(":")?;
                    parser.identifier()?.text
                };
                parser.expect("=")?;
                let rust = parser.identifier()?.text;
                parser.expect(";")?;
                if !names.insert(name.text.clone()) {
                    return Err(parser.error(&name, "duplicate definition"));
                }
                definitions
                    .functions
                    .insert(name.text, Function { args, result, rust });
            }
            "rule" => {
                let name = parser.identifier()?;
                if !names.insert(name.text.clone()) {
                    return Err(parser.error(&name, "duplicate definition"));
                }
                let phase = parser.take()?;
                if !matches!(phase.text.as_str(), "normalize" | "explore") {
                    return Err(parser.error(&phase, "expected normalize or explore"));
                }
                let priority = parser.take()?;
                let priority = priority
                    .text
                    .parse::<usize>()
                    .map_err(|_| parser.error(&priority, "expected numeric priority"))?;
                let growth = if parser.at("grow") {
                    parser.expect("grow")?;
                    let growth = parser.take()?;
                    growth
                        .text
                        .parse::<usize>()
                        .map_err(|_| parser.error(&growth, "expected numeric operator growth"))?
                } else {
                    0
                };
                let pattern = parser.expression()?;
                let mut guards = Vec::new();
                while parser.at("when") {
                    parser.expect("when")?;
                    guards.push(parser.expression()?);
                }
                parser.expect("=>")?;
                let replacement = parser.expression()?;
                parser.expect(";")?;
                rules.push(Rule {
                    name,
                    phase: phase.text,
                    priority,
                    growth,
                    pattern,
                    guards,
                    replacement,
                });
            }
            _ => {
                return Err(
                    parser.error(&token, "expected operator, predicate, constructor or rule")
                )
            }
        }
    }
    if rules.is_empty() {
        return Err(format!("{path}:1:1: no rules defined"));
    }
    let types = [
        "Relation", "Scalars", "Outputs", "JoinKind", "TableId", "Bool",
    ];
    for ty in definitions
        .operators
        .values()
        .flat_map(|fields| fields.iter().map(|(_, ty)| ty))
        .chain(
            definitions
                .functions
                .values()
                .flat_map(|function| function.args.iter().chain([&function.result])),
        )
    {
        if !types.contains(&ty.as_str()) {
            return Err(format!("{path}:1:1: unknown field type {ty}"));
        }
    }
    for rule in &rules {
        validate(path, &definitions, rule)?;
    }
    rules.sort_by_key(|rule| rule.priority);
    Ok(generate(&definitions, &rules))
}

#[derive(Clone, Debug)]
struct Token {
    text: String,
    line: usize,
    column: usize,
}

fn lex(path: &str, source: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    for (line, text) in source.lines().enumerate() {
        let text = text.split('#').next().unwrap();
        let bytes = text.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index].is_ascii_whitespace() {
                index += 1;
                continue;
            }
            let start = index;
            if bytes[index].is_ascii_alphanumeric() || matches!(bytes[index], b'_' | b'$') {
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
            } else if text[index..].starts_with("=>") {
                index += 2;
            } else if b"(),:;=".contains(&bytes[index]) {
                index += 1;
            } else {
                return Err(format!(
                    "{path}:{}:{}: unexpected character",
                    line + 1,
                    index + 1
                ));
            }
            tokens.push(Token {
                text: text[start..index].to_owned(),
                line: line + 1,
                column: start + 1,
            });
        }
    }
    Ok(tokens)
}

struct Parser<'a> {
    path: &'a str,
    tokens: Vec<Token>,
    next: usize,
}

impl Parser<'_> {
    fn done(&self) -> bool {
        self.next == self.tokens.len()
    }

    fn at(&self, text: &str) -> bool {
        self.tokens
            .get(self.next)
            .is_some_and(|token| token.text == text)
    }

    fn take(&mut self) -> Result<Token, String> {
        let token = self.tokens.get(self.next).cloned().ok_or_else(|| {
            let line = self.tokens.last().map_or(1, |token| token.line);
            format!("{}:{line}:1: unexpected end of rule file", self.path)
        })?;
        self.next += 1;
        Ok(token)
    }

    fn expect(&mut self, text: &str) -> Result<(), String> {
        let token = self.take()?;
        if token.text != text {
            return Err(self.error(&token, &format!("expected {text}")));
        }
        Ok(())
    }

    fn identifier(&mut self) -> Result<Token, String> {
        let token = self.take()?;
        if !identifier(&token.text) {
            return Err(self.error(&token, "expected identifier"));
        }
        Ok(token)
    }

    fn expression(&mut self) -> Result<Expression, String> {
        let token = self.take()?;
        if token.text == "(" {
            let name = self.identifier()?;
            let mut args = Vec::new();
            while !self.at(")") {
                args.push(self.expression()?);
            }
            self.expect(")")?;
            Ok(Expression::Call(name, args))
        } else if token.text.strip_prefix('$').is_some_and(identifier) {
            Ok(Expression::Binding(token))
        } else {
            Err(self.error(&token, "expected an operator call or $binding"))
        }
    }

    fn error(&self, token: &Token, message: &str) -> String {
        diagnostic(self.path, token, message)
    }
}

fn identifier(text: &str) -> bool {
    text.as_bytes()
        .first()
        .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
        && text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[derive(Default)]
struct Definitions {
    operators: BTreeMap<String, Vec<(String, String)>>,
    functions: BTreeMap<String, Function>,
}

struct Function {
    args: Vec<String>,
    result: String,
    rust: String,
}

struct Rule {
    name: Token,
    phase: String,
    priority: usize,
    growth: usize,
    pattern: Expression,
    guards: Vec<Expression>,
    replacement: Expression,
}

enum Expression {
    Binding(Token),
    Call(Token, Vec<Expression>),
}

fn validate(path: &str, definitions: &Definitions, rule: &Rule) -> Result<(), String> {
    let mut bindings = BTreeMap::new();
    check_pattern(path, definitions, &rule.pattern, "Relation", &mut bindings)?;
    if matches!(rule.pattern, Expression::Binding(_)) {
        return Err(diagnostic(path, &rule.name, "rule must match an operator"));
    }
    for guard in &rule.guards {
        check_expression(
            path,
            definitions,
            guard,
            "Bool",
            &bindings,
            &mut BTreeSet::new(),
            true,
        )?;
    }
    check_expression(
        path,
        definitions,
        &rule.replacement,
        "Relation",
        &bindings,
        &mut BTreeSet::new(),
        false,
    )?;
    if rule.phase == "normalize" && rule.growth != 0 {
        return Err(diagnostic(
            path,
            &rule.name,
            "normalization cannot declare operator growth",
        ));
    }
    if operator_count(&rule.replacement, definitions)
        .saturating_sub(operator_count(&rule.pattern, definitions))
        > rule.growth
    {
        return Err(diagnostic(
            path,
            &rule.name,
            "replacement exceeds declared operator growth",
        ));
    }
    Ok(())
}

fn check_pattern(
    path: &str,
    definitions: &Definitions,
    pattern: &Expression,
    expected: &str,
    bindings: &mut BTreeMap<String, String>,
) -> Result<(), String> {
    match pattern {
        Expression::Binding(token) => {
            if bindings
                .insert(token.text.clone(), expected.to_owned())
                .is_some()
            {
                return Err(diagnostic(
                    path,
                    token,
                    "pattern binds a name more than once",
                ));
            }
        }
        Expression::Call(token, args) => {
            let fields = definitions
                .operators
                .get(&token.text)
                .ok_or_else(|| diagnostic(path, token, "unknown pattern operator"))?;
            if expected != "Relation" {
                return Err(diagnostic(
                    path,
                    token,
                    &format!("expected {expected}, found Relation"),
                ));
            }
            if fields.len() != args.len() {
                return Err(diagnostic(path, token, "wrong operator arity"));
            }
            for ((_, ty), arg) in fields.iter().zip(args) {
                check_pattern(path, definitions, arg, ty, bindings)?;
            }
        }
    }
    Ok(())
}

fn check_expression(
    path: &str,
    definitions: &Definitions,
    expression: &Expression,
    expected: &str,
    bindings: &BTreeMap<String, String>,
    used: &mut BTreeSet<String>,
    borrowed: bool,
) -> Result<(), String> {
    match expression {
        Expression::Binding(token) => {
            let ty = bindings
                .get(&token.text)
                .ok_or_else(|| diagnostic(path, token, "unbound name"))?;
            if ty != expected {
                return Err(diagnostic(
                    path,
                    token,
                    &format!("expected {expected}, found {ty}"),
                ));
            }
            if !borrowed && !used.insert(token.text.clone()) {
                return Err(diagnostic(
                    path,
                    token,
                    "replacement uses a binding more than once",
                ));
            }
        }
        Expression::Call(token, args) => {
            let (types, result) = if let Some(fields) = definitions.operators.get(&token.text) {
                if borrowed {
                    return Err(diagnostic(
                        path,
                        token,
                        "operator construction in a precondition",
                    ));
                }
                (
                    fields.iter().map(|(_, ty)| ty.as_str()).collect::<Vec<_>>(),
                    "Relation",
                )
            } else if let Some(function) = definitions.functions.get(&token.text) {
                if borrowed && function.result != "Bool" {
                    return Err(diagnostic(path, token, "constructor in a precondition"));
                }
                if !borrowed && function.result == "Bool" {
                    return Err(diagnostic(path, token, "predicate in a replacement"));
                }
                (
                    function.args.iter().map(String::as_str).collect(),
                    function.result.as_str(),
                )
            } else {
                return Err(diagnostic(path, token, "unknown operator or function"));
            };
            if expected != result {
                return Err(diagnostic(
                    path,
                    token,
                    &format!("expected {expected}, found {result}"),
                ));
            }
            if args.len() != types.len() {
                return Err(diagnostic(path, token, "wrong call arity"));
            }
            for (arg, ty) in args.iter().zip(types) {
                if borrowed && !matches!(arg, Expression::Binding(_)) {
                    return Err(diagnostic(
                        path,
                        token,
                        "precondition arguments must be bindings",
                    ));
                }
                check_expression(path, definitions, arg, ty, bindings, used, borrowed)?;
            }
        }
    }
    Ok(())
}

fn operator_count(expression: &Expression, definitions: &Definitions) -> usize {
    match expression {
        Expression::Binding(_) => 0,
        Expression::Call(token, args) => {
            usize::from(definitions.operators.contains_key(&token.text))
                + args
                    .iter()
                    .map(|arg| operator_count(arg, definitions))
                    .sum::<usize>()
        }
    }
}

fn diagnostic(path: &str, token: &Token, message: &str) -> String {
    format!("{path}:{}:{}: {message}", token.line, token.column)
}

fn generate(definitions: &Definitions, rules: &[Rule]) -> String {
    let mut output =
        String::from("#[derive(Clone, Copy, Debug, PartialEq, Eq)]\npub(super) enum Rule {\n");
    for rule in rules {
        writeln!(output, "{},", rule.name.text).unwrap();
    }
    writeln!(
        output,
        "}}\npub(super) const RULE_COUNT: usize = {};",
        rules.len()
    )
    .unwrap();
    output.push_str("pub(super) const RULES: [Rule; RULE_COUNT] = [");
    for rule in rules {
        write!(output, "Rule::{},", rule.name.text).unwrap();
    }
    output.push_str("];\n");
    output.push_str("impl Rule { pub(super) fn name(self) -> &'static str { match self {\n");
    for rule in rules {
        writeln!(output, "Self::{} => {:?},", rule.name.text, rule.name.text).unwrap();
    }
    output.push_str("} } }\n");
    for phase in ["normalize", "explore"] {
        writeln!(output, "#[allow(unused_variables, reason = \"matched fields can be discarded by a rule\")]\npub(super) fn apply_{phase}(node: &mut Relation, plan: &mut LogicalPlan, report: &mut RewriteReport) -> Result<Option<Rule>> {{").unwrap();
        for rule in rules.iter().filter(|rule| rule.phase == phase) {
            let mut closed = 0;
            emit_pattern(
                &mut output,
                definitions,
                &rule.pattern,
                "&*node",
                false,
                &mut 0,
                &mut closed,
            );
            output.push_str("if ");
            if rule.guards.is_empty() {
                output.push_str("true");
            }
            output.push_str(
                &rule
                    .guards
                    .iter()
                    .map(|guard| emit_expression(definitions, guard, true, false))
                    .collect::<Vec<_>>()
                    .join(" && "),
            );
            output.push_str(" {\n");
            if rule.growth != 0 {
                writeln!(
                    output,
                    "if !report.reserve_growth({}) {{ return Ok(None); }}",
                    rule.growth
                )
                .unwrap();
            }
            emit_pattern(
                &mut output,
                definitions,
                &rule.pattern,
                "std::mem::replace(node, Relation::OneRow)",
                true,
                &mut 0,
                &mut 0,
            );
            writeln!(
                output,
                "*node = {};\nreturn Ok(Some(Rule::{}));\n}}",
                emit_expression(definitions, &rule.replacement, false, false),
                rule.name.text
            )
            .unwrap();
            for _ in 0..closed {
                output.push_str("}\n");
            }
        }
        output.push_str("Ok(None)\n}\n");
    }
    output.push_str("#[allow(unused_variables, unused_mut, reason = \"rules can have no preconditions or use only some matched fields\")]\npub(super) fn normalization_declines(node: &Relation, plan: &LogicalPlan, mut declined: impl FnMut(&'static str, &'static str)) -> Result<()> {\n");
    for rule in rules
        .iter()
        .filter(|rule| rule.phase == "normalize" && !rule.guards.is_empty())
    {
        let mut closed = 0;
        emit_pattern(
            &mut output,
            definitions,
            &rule.pattern,
            "node",
            false,
            &mut 0,
            &mut closed,
        );
        output.push_str("'preconditions: {\n");
        for guard in &rule.guards {
            let Expression::Call(predicate, _) = guard else {
                unreachable!("preconditions are validated predicate calls")
            };
            writeln!(
                output,
                "if !({}) {{ declined({:?}, {:?}); break 'preconditions; }}",
                emit_expression(definitions, guard, true, false),
                rule.name.text,
                predicate.text,
            )
            .unwrap();
        }
        output.push_str("}\n");
        for _ in 0..closed {
            output.push_str("}\n");
        }
    }
    output.push_str("Ok(())\n}\n");
    output
}

fn emit_pattern(
    output: &mut String,
    definitions: &Definitions,
    pattern: &Expression,
    input: &str,
    owned: bool,
    next: &mut usize,
    closed: &mut usize,
) {
    let Expression::Call(token, args) = pattern else {
        unreachable!()
    };
    let fields = &definitions.operators[&token.text];
    let mut nested = Vec::new();
    write!(
        output,
        "{} Relation::{} {{",
        if owned { "let" } else { "if let" },
        token.text
    )
    .unwrap();
    for ((field, _), arg) in fields.iter().zip(args) {
        let name = match arg {
            Expression::Binding(token) => format!("binding_{}", &token.text[1..]),
            Expression::Call(_, _) => {
                let name = format!("nested_{}", *next);
                *next += 1;
                nested.push((name.clone(), arg));
                name
            }
        };
        write!(output, "{field}: {name},").unwrap();
    }
    if owned {
        writeln!(
            output,
            "}} = {input} else {{ unreachable!(\"rule pattern was checked\") }};"
        )
        .unwrap();
    } else {
        writeln!(output, "}} = {input} {{").unwrap();
        *closed += 1;
    }
    for (name, arg) in nested {
        let input = if owned {
            format!("*{name}")
        } else {
            format!("{name}.as_ref()")
        };
        emit_pattern(output, definitions, arg, &input, owned, next, closed);
    }
}

fn emit_expression(
    definitions: &Definitions,
    expression: &Expression,
    borrowed: bool,
    boxed: bool,
) -> String {
    match expression {
        Expression::Binding(token) => {
            let name = format!("binding_{}", &token.text[1..]);
            if boxed || borrowed {
                name
            } else {
                // All relation fields in the matched operators own boxed inputs.
                // Non-relation bindings are passed to constructors with boxed=true.
                format!("*{name}")
            }
        }
        Expression::Call(token, args) => {
            if let Some(fields) = definitions.operators.get(&token.text) {
                let fields = fields
                    .iter()
                    .zip(args)
                    .map(|((field, ty), arg)| {
                        let expr = if ty == "Relation" {
                            match arg {
                                Expression::Binding(_) => {
                                    emit_expression(definitions, arg, false, true)
                                }
                                _ => format!(
                                    "Box::new({})",
                                    emit_expression(definitions, arg, false, false)
                                ),
                            }
                        } else {
                            emit_expression(definitions, arg, false, true)
                        };
                        format!("{field}: {expr}")
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                format!("Relation::{} {{ {fields} }}", token.text)
            } else {
                let function = &definitions.functions[&token.text];
                let args = args
                    .iter()
                    .zip(&function.args)
                    .map(|(arg, ty)| emit_expression(definitions, arg, borrowed, ty != "Relation"))
                    .chain(["plan".to_owned()])
                    .collect::<Vec<_>>()
                    .join(",");
                format!("{}({args})?", function.rust)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFINITIONS: &str = "operator Filter(input: Relation, predicates: Scalars);\n\
        predicate Empty(Scalars) = empty;\n";

    #[test]
    fn generates_typed_matching_and_owned_replacements() {
        let rust = compile(
            "test.rules",
            &format!(
                "{DEFINITIONS}\n\
            rule EmptyFilter normalize 10 (Filter $input $terms) when (Empty $terms) => $input;\n\
            rule Other explore 20 (Filter $input $terms) when (Empty $terms) => $input;"
            ),
        )
        .unwrap();
        assert!(rust.contains("if let Relation::Filter"));
        assert!(rust.contains("empty(binding_terms,plan)?"));
        assert!(rust.contains("*node = *binding_input"));
        assert!(rust.contains("fn apply_normalize"));
        assert!(rust.contains("fn apply_explore"));
    }

    #[test]
    fn exploration_growth_is_explicit_and_reserved_before_consuming_the_input() {
        let definitions = format!("{DEFINITIONS}operator Wrap(input: Relation);\n");
        let rule = "rule Expand explore 1 grow 2 (Filter $input $terms) => (Wrap (Wrap (Filter $input $terms)));";
        let rust = compile("test.rules", &format!("{definitions}{rule}")).unwrap();
        let reservation = rust.find("report.reserve_growth(2)").unwrap();
        let consumed = rust.find("std::mem::replace(node").unwrap();
        assert!(reservation < consumed);
        for invalid in [rule.replace("grow 2", "grow 1"), rule.replace("grow 2", "")] {
            assert!(compile("test.rules", &format!("{definitions}{invalid}"))
                .unwrap_err()
                .contains("exceeds declared operator growth"));
        }
        assert!(compile(
            "test.rules",
            &format!("{definitions}{}", rule.replace("explore", "normalize"))
        )
        .unwrap_err()
        .contains("normalization cannot declare operator growth"));
    }

    #[test]
    fn rejects_invalid_rules_with_source_locations() {
        for (rule, message) in [
            (
                "rule X normalize 0 (Unknown $x) => $x;",
                "unknown pattern operator",
            ),
            (
                "rule X normalize 0 (Filter $x) => $x;",
                "wrong operator arity",
            ),
            ("rule X normalize 0 (Filter $x $x) => $x;", "more than once"),
            (
                "rule X normalize 0 (Filter $x $p) => $missing;",
                "unbound name",
            ),
            (
                "rule X normalize 0 (Filter $x $p) => $p;",
                "expected Relation, found Scalars",
            ),
            (
                "rule X normalize 0 (Filter $x $p) when (Empty $x) => $x;",
                "expected Scalars, found Relation",
            ),
            (
                "rule X normalize 0 (Filter $x $p) => (Filter (Filter $x $p) $p);",
                "more than once",
            ),
            (
                "rule X forever 0 (Filter $x $p) => $x;",
                "expected normalize or explore",
            ),
            (
                "rule X normalize 0 (Filter $x $p) => $x; rule X normalize 0 (Filter $x $p) => $x;",
                "duplicate definition",
            ),
        ] {
            let error = compile("broken.rules", &format!("{DEFINITIONS}{rule}")).unwrap_err();
            assert!(error.starts_with("broken.rules:3:"), "{error}");
            assert!(error.contains(message), "{error}");
        }
    }
}
