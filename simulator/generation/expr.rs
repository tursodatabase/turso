use limbo_sqlite3_parser::ast::{
    self, Expr, LikeOperator, Name, Operator, QualifiedName, Type, UnaryOperator,
};

use crate::{
    generation::{gen_random_text, pick, pick_index, Arbitrary, ArbitraryFrom},
    model::table::Value,
    SimulatorEnv,
};

impl<T> Arbitrary for Box<T>
where
    T: Arbitrary,
{
    fn arbitrary<R: rand::Rng>(rng: &mut R) -> Self {
        Box::from(T::arbitrary(rng))
    }
}

impl<A, T> ArbitraryFrom<A> for Box<T>
where
    T: ArbitraryFrom<A>,
{
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, t: A) -> Self {
        Box::from(T::arbitrary_from(rng, t))
    }
}

impl<T> Arbitrary for Option<T>
where
    T: Arbitrary,
{
    fn arbitrary<R: rand::Rng>(rng: &mut R) -> Self {
        rng.gen_bool(0.5).then_some(T::arbitrary(rng))
    }
}

impl<A, T> ArbitraryFrom<A> for Option<T>
where
    T: ArbitraryFrom<A>,
{
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, t: A) -> Self {
        rng.gen_bool(0.5).then_some(T::arbitrary_from(rng, t))
    }
}

impl<A: Copy, T> ArbitraryFrom<A> for Vec<T>
where
    T: ArbitraryFrom<A>,
{
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, t: A) -> Self {
        let size = rng.gen_range(0..5);
        (0..size)
            .into_iter()
            .map(|_| T::arbitrary_from(rng, t))
            .collect()
    }
}

// Freestyling generation
impl ArbitraryFrom<&SimulatorEnv> for Expr {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, t: &SimulatorEnv) -> Self {
        let choice = rng.gen_range(0..13);
        let expr = match choice {
            0 => Expr::Between {
                lhs: Box::arbitrary_from(rng, t),
                not: rng.gen_bool(0.5),
                start: Box::arbitrary_from(rng, t),
                end: Box::arbitrary_from(rng, t),
            },
            1 => Expr::Binary(
                Box::arbitrary_from(rng, t),
                Operator::arbitrary(rng),
                Box::arbitrary_from(rng, t),
            ),
            2 => Expr::Case {
                base: Option::arbitrary_from(rng, t),
                when_then_pairs: {
                    let size = rng.gen_range(0..5);
                    (0..size)
                        .into_iter()
                        .map(|_| (Self::arbitrary_from(rng, t), Self::arbitrary_from(rng, t)))
                        .collect()
                },
                else_expr: Option::arbitrary_from(rng, t),
            },
            3 => Expr::Cast {
                expr: Box::arbitrary_from(rng, t),
                type_name: Option::arbitrary(rng),
            },
            4 => Expr::Collate(Box::arbitrary_from(rng, t), CollateName::arbitrary(rng).0),
            5 => Expr::InList {
                lhs: Box::arbitrary_from(rng, t),
                not: rng.gen_bool(0.5),
                rhs: Option::arbitrary_from(rng, t),
            },
            6 => Expr::IsNull(Box::arbitrary_from(rng, t)),
            7 => {
                let op = LikeOperator::arbitrary_from(rng, t);
                let escape = if matches!(op, LikeOperator::Like) {
                    Option::arbitrary_from(rng, t)
                } else {
                    None
                };
                Expr::Like {
                    lhs: Box::arbitrary_from(rng, t),
                    not: rng.gen_bool(0.5),
                    op,
                    rhs: Box::arbitrary_from(rng, t),
                    escape,
                }
            }
            8 => Expr::Literal(ast::Literal::arbitrary_from(rng, t)),
            9 => Expr::NotNull(Box::arbitrary_from(rng, t)),
            // TODO: only supports one paranthesized expression
            10 => Expr::Parenthesized(vec![Expr::arbitrary_from(rng, t)]),
            11 => {
                let table_idx = pick_index(t.tables.len(), rng);
                let table = &t.tables[table_idx];
                let col_idx = pick_index(table.columns.len(), rng);
                let col = &table.columns[col_idx];
                Expr::Qualified(Name(table.name.clone()), Name(col.name.clone()))
            }
            12 => Expr::Unary(
                UnaryOperator::arbitrary_from(rng, t),
                Box::arbitrary_from(rng, t),
            ),
            // TODO: skip Exists for now
            // TODO: skip Function Call for now
            // TODO: skip Function Call Star for now
            // TODO: skip ID for now
            // TODO: skip InSelect as still need to implement ArbitratyFrom for Select
            // TODO: skip InTable
            // TODO: skip Name
            // TODO: Skip DoublyQualified for now
            // TODO: skip Raise
            // TODO: skip subquery
            _ => unreachable!(),
        };
        expr
    }
}

impl Arbitrary for Operator {
    fn arbitrary<R: rand::Rng>(rng: &mut R) -> Self {
        let choice = rng.gen_range(0..23);
        match choice {
            0 => Operator::Add,
            1 => Operator::And,
            2 => Operator::ArrowRight,
            3 => Operator::ArrowRightShift,
            4 => Operator::BitwiseAnd,
            5 => Operator::BitwiseNot,
            6 => Operator::BitwiseOr,
            7 => Operator::Concat,
            8 => Operator::Divide,
            9 => Operator::Equals,
            10 => Operator::Greater,
            11 => Operator::GreaterEquals,
            12 => Operator::Is,
            13 => Operator::IsNot,
            14 => Operator::LeftShift,
            15 => Operator::Less,
            16 => Operator::LessEquals,
            17 => Operator::Modulus,
            18 => Operator::Multiply,
            19 => Operator::NotEquals,
            20 => Operator::Or,
            21 => Operator::RightShift,
            22 => Operator::Subtract,
            _ => unreachable!(),
        }
    }
}

impl Arbitrary for Type {
    fn arbitrary<R: rand::Rng>(rng: &mut R) -> Self {
        let name = pick(&["INT", "INTEGER", "REAL", "TEXT", "BLOB", "ANY"], rng).to_string();
        Self {
            name,
            size: None, // TODO: come back later here
        }
    }
}

struct CollateName(String);

impl Arbitrary for CollateName {
    fn arbitrary<R: rand::Rng>(rng: &mut R) -> Self {
        let choice = rng.gen_range(0..3);
        CollateName(
            match choice {
                0 => "BINARY",
                1 => "RTRIM",
                2 => "NOCASE",
                _ => unreachable!(),
            }
            .to_string(),
        )
    }
}

impl ArbitraryFrom<&SimulatorEnv> for QualifiedName {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, t: &SimulatorEnv) -> Self {
        // TODO: for now just generate table name
        let table_idx = pick_index(t.tables.len(), rng);
        let table = &t.tables[table_idx];
        // TODO: for now forego alias
        Self::single(Name(table.name.clone()))
    }
}

impl ArbitraryFrom<&SimulatorEnv> for LikeOperator {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, _t: &SimulatorEnv) -> Self {
        let choice = rng.gen_range(0..4);
        match choice {
            0 => LikeOperator::Glob,
            1 => LikeOperator::Like,
            2 => LikeOperator::Match,
            3 => LikeOperator::Regexp,
            _ => unreachable!(),
        }
    }
}

// Current implementation does not take into account the columns affinity nor if table is Strict
impl ArbitraryFrom<&SimulatorEnv> for ast::Literal {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, _t: &SimulatorEnv) -> Self {
        loop {
            let choice = rng.gen_range(0..5);
            let lit = match choice {
                0 => ast::Literal::Numeric({
                    let integer = rng.gen_bool(0.5);
                    if integer {
                        rng.gen_range(i64::MIN..i64::MAX).to_string()
                    } else {
                        rng.gen_range(-1e10..1e10).to_string()
                    }
                }),
                1 => ast::Literal::String(format!("'{}'", gen_random_text(rng))),
                2 => ast::Literal::Blob(hex::encode(gen_random_text(rng).as_bytes().to_vec())),
                // TODO: skip Keyword
                3 => continue,
                4 => ast::Literal::Null,
                // TODO: Ignore Date stuff for now
                _ => continue,
            };
            break lit;
        }
    }
}

// Creates a litreal value
impl ArbitraryFrom<&Vec<&Value>> for ast::Expr {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, values: &Vec<&Value>) -> Self {
        if values.is_empty() {
            return Self::Literal(ast::Literal::Null);
        }
        // TODO: for now just convert the value to an ast::Literal
        let values = values
            .iter()
            .map(|value| ast::Expr::Literal((*value).into()))
            .collect::<Vec<_>>();

        pick(&values, rng).to_owned().clone()
    }
}

impl ArbitraryFrom<&SimulatorEnv> for UnaryOperator {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, _t: &SimulatorEnv) -> Self {
        let choice = rng.gen_range(0..4);
        match choice {
            0 => Self::BitwiseNot,
            1 => Self::Negative,
            2 => Self::Not,
            3 => Self::Positive,
            _ => unreachable!(),
        }
    }
}
