#![no_main]
use core::fmt;
use std::{error::Error, num::NonZero, sync::Arc};

use arbitrary::Arbitrary;
use libfuzzer_sys::{fuzz_target, Corpus};

#[macro_use]
mod common;
use common::*;
use turso_core::StepResult;

str_enum! {
    pub enum Table {
        A => "a",
    }
}

str_enum! {
    pub enum Column {
        X => "x",
        Y => "y",
        Z => "z",
    }
}

#[derive(Debug)]
struct Input {
    inserts: Vec<(Value, Value, Value)>,
    expr: Expr,
}

impl<'a> Arbitrary<'a> for Input {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let mut inserts = vec![];
        for _ in 0..u.int_in_range(0..=20)? {
            inserts.push(u.arbitrary::<(Value, Value, Value)>()?);
        }

        Ok(Self {
            inserts,
            expr: u.arbitrary()?,
        })
    }
}

#[derive(Debug, Arbitrary)]
pub enum Expr {
    Value(Value),
    Binary(Binary, Box<Expr>, Box<Expr>),
    Unary(Unary, Box<Expr>),
    Cast(Box<Expr>, CastType),
    UnaryFunc(UnaryFunc, Box<Expr>),
    BinaryFunc(BinaryFunc, Box<Expr>, Box<Expr>),
    Reference(Table, Column),
}

impl Expr {
    pub fn lower(&self) -> Output {
        match self {
            Expr::Value(value) => Output {
                query: "?".to_string(),
                parameters: vec![value.clone()],
                depth: 0,
            },
            Expr::Unary(op, expr) => {
                let expr = expr.lower();
                Output {
                    query: format!("{op} ({})", expr.query),
                    parameters: expr.parameters,
                    depth: expr.depth + 1,
                }
            }
            Expr::Binary(op, lhs, rhs) => {
                let mut lhs = lhs.lower();
                let mut rhs = rhs.lower();
                Output {
                    query: format!("({}) {op} ({})", lhs.query, rhs.query),
                    parameters: {
                        lhs.parameters.append(&mut rhs.parameters);
                        lhs.parameters
                    },
                    depth: lhs.depth.max(rhs.depth) + 1,
                }
            }
            Expr::BinaryFunc(func, lhs, rhs) => {
                let mut lhs = lhs.lower();
                let mut rhs = rhs.lower();
                Output {
                    query: format!("{func}({}, {})", lhs.query, rhs.query),
                    parameters: {
                        lhs.parameters.append(&mut rhs.parameters);
                        lhs.parameters
                    },
                    depth: lhs.depth.max(rhs.depth) + 1,
                }
            }
            Expr::UnaryFunc(func, expr) => {
                let expr = expr.lower();
                Output {
                    query: format!("{func}({})", expr.query),
                    parameters: expr.parameters,
                    depth: expr.depth + 1,
                }
            }
            Expr::Cast(expr, cast_type) => {
                let expr = expr.lower();
                Output {
                    query: format!("cast({} as {cast_type})", expr.query),
                    parameters: expr.parameters,
                    depth: expr.depth + 1,
                }
            }
            Expr::Reference(table, column) => Output {
                query: format!("{table}.{column}"),
                parameters: vec![],
                depth: 0,
            },
        }
    }
}

fn do_fuzz(input: Input) -> Result<Corpus, Box<dyn Error>> {
    // dbg!(&input);
    let db = FuzzDatabase::new();

    db.execute("CREATE TABLE a (x, y, z);");

    for insert in input.inserts {
        db.sqlite.execute("INSERT INTO a VALUES (?, ?, ?)", insert.clone()).unwrap();

        let mut stmt = db.turso.prepare("INSERT INTO a VALUES (?, ?, ?)").unwrap();
        stmt.bind_at(NonZero::new(1).unwrap(), insert.0.into());
        stmt.bind_at(NonZero::new(2).unwrap(), insert.1.into());
        stmt.bind_at(NonZero::new(3).unwrap(), insert.2.into());
        assert!(matches!(stmt.step().unwrap(), StepResult::Done));
    }

    let expr = input.expr.lower();

    // FIX: `turso_core::translate::expr::translate_expr` causes a overflow if this is any higher.
    if expr.depth > 100 {
        return Ok(Corpus::Reject);
    }

    let sql = format!("SELECT {} FROM a", expr.query);

    db.assert(&sql, &expr.parameters);

    Ok(Corpus::Keep)
}

fuzz_target!(|input: Input| -> Corpus { do_fuzz(input).unwrap_or(Corpus::Keep) });
