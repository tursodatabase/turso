//! User-defined functions written in a Starlark subset.
//!
//! A `CREATE FUNCTION ... LANGUAGE starlark` body is the body of the function
//! (PL/Python style): the declared SQL parameters are in scope as variables
//! and `return` produces the function result. Falling off the end returns
//! NULL.
//!
//! The body is parsed here into a small statement/expression IR
//! ([`UdfStmt`]/[`UdfExpr`]) which the translator compiles into VDBE bytecode
//! inline at each call site (see `translate::udf`). There is no interpreter:
//! runtime semantics for arithmetic, comparisons, and truthiness are SQL
//! semantics, and calls resolve against the SQL function namespace (built-ins
//! and other user-defined functions).
//!
//! Supported subset: `if`/`elif`/`else`, `while`, `for <var> in range(...)`,
//! `break`/`continue`/`pass`, assignments (including augmented assignments),
//! `return`, integer/float/string/boolean/`None` literals, arithmetic,
//! comparison and boolean operators, conditional expressions
//! (`a if cond else b`), and function calls.

mod lexer;
mod parser;

use crate::Result;

/// A parsed Starlark function body.
#[derive(Debug, Clone, PartialEq)]
pub struct StarlarkBody {
    pub stmts: Vec<UdfStmt>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UdfStmt {
    /// `name = value` (augmented assignments are lowered to this)
    Assign {
        name: String,
        value: UdfExpr,
    },
    /// An expression evaluated for its (side) effects; result is discarded.
    Expr(UdfExpr),
    /// `return [expr]`
    Return(Option<UdfExpr>),
    /// `if`/`elif`/`else` chain: each arm is (condition, body).
    If {
        arms: Vec<(UdfExpr, Vec<UdfStmt>)>,
        else_body: Option<Vec<UdfStmt>>,
    },
    While {
        cond: UdfExpr,
        body: Vec<UdfStmt>,
    },
    /// `for var in range(start, stop, step)`
    For {
        var: String,
        start: Option<UdfExpr>,
        stop: UdfExpr,
        /// Step must be a non-zero integer literal (checked at parse time).
        step: i64,
        body: Vec<UdfStmt>,
    },
    Break,
    Continue,
    Pass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfBinOp {
    Add,
    Sub,
    Mul,
    Div,
    FloorDiv,
    Mod,
    BitAnd,
    BitOr,
    Shl,
    Shr,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfUnOp {
    Neg,
    Pos,
    BitNot,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UdfExpr {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    None,
    Var(String),
    Binary(UdfBinOp, Box<UdfExpr>, Box<UdfExpr>),
    Unary(UdfUnOp, Box<UdfExpr>),
    /// `then if cond else else_`
    Ternary {
        cond: Box<UdfExpr>,
        then: Box<UdfExpr>,
        else_: Box<UdfExpr>,
    },
    /// Call into the SQL function namespace (built-ins or other UDFs).
    Call {
        name: String,
        args: Vec<UdfExpr>,
    },
}

/// Parse a Starlark function body. `params` are the declared SQL parameter
/// names, used only for error checking (e.g. a `for` loop variable may not
/// shadow nothing here; unknown variables are caught at compile time).
pub fn parse_body(src: &str) -> Result<StarlarkBody> {
    let toks = lexer::tokenize(src)?;
    parser::parse(toks)
}

/// Collect every name assigned anywhere in the body (assignment targets and
/// `for` loop variables), in first-seen order. The bytecode compiler
/// allocates one register per such local.
pub fn collect_assigned_names(stmts: &[UdfStmt], out: &mut Vec<String>) {
    let push = |name: &str, out: &mut Vec<String>| {
        if !out.iter().any(|n| n == name) {
            out.push(name.to_owned());
        }
    };
    for stmt in stmts {
        match stmt {
            UdfStmt::Assign { name, .. } => push(name, out),
            UdfStmt::For { var, body, .. } => {
                push(var, out);
                collect_assigned_names(body, out);
            }
            UdfStmt::If { arms, else_body } => {
                for (_, body) in arms {
                    collect_assigned_names(body, out);
                }
                if let Some(body) = else_body {
                    collect_assigned_names(body, out);
                }
            }
            UdfStmt::While { body, .. } => collect_assigned_names(body, out),
            UdfStmt::Expr(_)
            | UdfStmt::Return(_)
            | UdfStmt::Break
            | UdfStmt::Continue
            | UdfStmt::Pass => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_line_return() {
        let body = parse_body("return a + b").unwrap();
        assert_eq!(
            body.stmts,
            vec![UdfStmt::Return(Some(UdfExpr::Binary(
                UdfBinOp::Add,
                Box::new(UdfExpr::Var("a".into())),
                Box::new(UdfExpr::Var("b".into())),
            )))]
        );
    }

    #[test]
    fn parse_if_elif_else() {
        let src = "\nif a > 0:\n    return 'pos'\nelif a < 0:\n    return 'neg'\nelse:\n    return 'zero'\n";
        let body = parse_body(src).unwrap();
        match &body.stmts[0] {
            UdfStmt::If { arms, else_body } => {
                assert_eq!(arms.len(), 2);
                assert!(else_body.is_some());
            }
            other => panic!("expected If, got {other:?}"),
        }
    }

    #[test]
    fn parse_while_loop_with_augmented_assign() {
        let src = "total = 0\ni = 0\nwhile i < n:\n    total += i\n    i += 1\nreturn total";
        let body = parse_body(src).unwrap();
        assert_eq!(body.stmts.len(), 4);
        match &body.stmts[2] {
            UdfStmt::While { body, .. } => assert_eq!(body.len(), 2),
            other => panic!("expected While, got {other:?}"),
        }
        // `total += i` lowers to `total = total + i`
        let mut names = Vec::new();
        collect_assigned_names(&body.stmts, &mut names);
        assert_eq!(names, vec!["total".to_string(), "i".to_string()]);
    }

    #[test]
    fn parse_for_range() {
        let src = "acc = 1\nfor i in range(1, n + 1):\n    acc = acc * i\nreturn acc";
        let body = parse_body(src).unwrap();
        match &body.stmts[1] {
            UdfStmt::For {
                var, start, step, ..
            } => {
                assert_eq!(var, "i");
                assert!(start.is_some());
                assert_eq!(*step, 1);
            }
            other => panic!("expected For, got {other:?}"),
        }
    }

    #[test]
    fn parse_ternary_and_call() {
        let body = parse_body("return upper(s) if flag else lower(s)").unwrap();
        match &body.stmts[0] {
            UdfStmt::Return(Some(UdfExpr::Ternary { then, .. })) => match then.as_ref() {
                UdfExpr::Call { name, args } => {
                    assert_eq!(name, "upper");
                    assert_eq!(args.len(), 1);
                }
                other => panic!("expected Call, got {other:?}"),
            },
            other => panic!("expected ternary return, got {other:?}"),
        }
    }

    #[test]
    fn parse_single_line_suite() {
        let body = parse_body("if a: return 1\nreturn 2").unwrap();
        assert_eq!(body.stmts.len(), 2);
    }

    #[test]
    fn parse_operator_precedence() {
        // 1 + 2 * 3 parses as 1 + (2 * 3)
        let body = parse_body("return 1 + 2 * 3").unwrap();
        match &body.stmts[0] {
            UdfStmt::Return(Some(UdfExpr::Binary(UdfBinOp::Add, lhs, rhs))) => {
                assert_eq!(**lhs, UdfExpr::Int(1));
                assert!(matches!(**rhs, UdfExpr::Binary(UdfBinOp::Mul, _, _)));
            }
            other => panic!("unexpected parse: {other:?}"),
        }
        // not a == b parses as not (a == b)
        let body = parse_body("return not a == b").unwrap();
        match &body.stmts[0] {
            UdfStmt::Return(Some(UdfExpr::Unary(UdfUnOp::Not, inner))) => {
                assert!(matches!(**inner, UdfExpr::Binary(UdfBinOp::Eq, _, _)));
            }
            other => panic!("unexpected parse: {other:?}"),
        }
    }

    #[test]
    fn parse_string_escapes_and_comments() {
        let src = "# leading comment\ns = 'it''s not' # not sql escaping\nreturn s";
        // In Starlark, '' inside a string terminates it; adjacent strings do
        // not concatenate implicitly in our subset -> parse error.
        assert!(parse_body(src).is_err());
        let body = parse_body("return 'a\\'b\\n'").unwrap();
        assert_eq!(
            body.stmts,
            vec![UdfStmt::Return(Some(UdfExpr::Str("a'b\n".into())))]
        );
    }

    #[test]
    fn parse_errors() {
        for src in [
            "def f(): return 1",               // nested defs unsupported
            "lambda x: x",                     // lambdas unsupported
            "for x in [1, 2]: pass",           // only range() iteration
            "for x in range(): pass",          // range needs 1-3 args
            "for x in range(1, 2, 0): pass",   // zero step
            "for x in range(1, 2, n): pass",   // non-literal step
            "while True:\nreturn 1",           // missing indent
            "if a:\n    return 1\n  return 2", // bad dedent
            "return 99999999999999999999",     // i64 overflow
            "return 1e999",                    // float literal out of range
            "x[0] = 1",                        // subscript assignment
            "a.b",                             // attribute access
            "f(x=1)",                          // keyword arguments
            "break",                           // break outside loop
            "continue",                        // continue outside loop
            "return 1 < 2 < 3",                // chained comparison (invalid in Starlark)
        ] {
            assert!(parse_body(src).is_err(), "expected parse error for: {src}");
        }
    }

    #[test]
    fn parse_paren_continuation() {
        let body = parse_body("return (1 +\n        2)").unwrap();
        assert!(matches!(
            body.stmts[0],
            UdfStmt::Return(Some(UdfExpr::Binary(UdfBinOp::Add, _, _)))
        ));
    }
}
