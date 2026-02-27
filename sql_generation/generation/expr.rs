use turso_parser::ast::{
    self, Expr, LikeOperator, Name, Operator, QualifiedName, Type, UnaryOperator,
};

use crate::{
    generation::{
        frequency, gen_random_text, one_of, pick, pick_index, Arbitrary, ArbitraryFrom,
        ArbitrarySized, ArbitrarySizedFrom, GenerationContext,
    },
    model::table::SimValue,
};

impl<T> Arbitrary for Box<T>
where
    T: Arbitrary,
{
    fn arbitrary<R: rand::Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        Box::from(T::arbitrary(rng, context))
    }
}

impl<T> ArbitrarySized for Box<T>
where
    T: ArbitrarySized,
{
    fn arbitrary_sized<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        size: usize,
    ) -> Self {
        Box::from(T::arbitrary_sized(rng, context, size))
    }
}

impl<A, T> ArbitrarySizedFrom<A> for Box<T>
where
    T: ArbitrarySizedFrom<A>,
{
    fn arbitrary_sized_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        t: A,
        size: usize,
    ) -> Self {
        Box::from(T::arbitrary_sized_from(rng, context, t, size))
    }
}

impl<T> Arbitrary for Option<T>
where
    T: Arbitrary,
{
    fn arbitrary<R: rand::Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        rng.random_bool(0.5).then_some(T::arbitrary(rng, context))
    }
}

impl<A, T> ArbitrarySizedFrom<A> for Option<T>
where
    T: ArbitrarySizedFrom<A>,
{
    fn arbitrary_sized_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        t: A,
        size: usize,
    ) -> Self {
        rng.random_bool(0.5)
            .then_some(T::arbitrary_sized_from(rng, context, t, size))
    }
}

impl<A: Copy, T> ArbitraryFrom<A> for Vec<T>
where
    T: ArbitraryFrom<A>,
{
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        t: A,
    ) -> Self {
        let size = rng.random_range(0..5);
        (0..size)
            .map(|_| T::arbitrary_from(rng, context, t))
            .collect()
    }
}

// Freestyling generation
impl ArbitrarySized for Expr {
    fn arbitrary_sized<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        size: usize,
    ) -> Self {
        frequency(
            vec![
                (
                    1,
                    Box::new(|rng| Expr::Literal(ast::Literal::arbitrary(rng, context))),
                ),
                (
                    size,
                    Box::new(|rng| {
                        one_of(
                            vec![
                                // Box::new(|rng: &mut R| Expr::Between {
                                //     lhs: Box::arbitrary_sized_from(rng, t, size - 1),
                                //     not: rng.gen_bool(0.5),
                                //     start: Box::arbitrary_sized_from(rng, t, size - 1),
                                //     end: Box::arbitrary_sized_from(rng, t, size - 1),
                                // }),
                                Box::new(|rng: &mut R| {
                                    Expr::Binary(
                                        Box::arbitrary_sized(rng, context, size - 1),
                                        Operator::arbitrary(rng, context),
                                        Box::arbitrary_sized(rng, context, size - 1),
                                    )
                                }),
                                // Box::new(|rng| Expr::Case {
                                //     base: Option::arbitrary_from(rng, t),
                                //     when_then_pairs: {
                                //         let size = rng.gen_range(0..5);
                                //         (0..size)
                                //             .map(|_| (Self::arbitrary_from(rng, t), Self::arbitrary_from(rng, t)))
                                //             .collect()
                                //     },
                                //     else_expr: Option::arbitrary_from(rng, t),
                                // }),
                                // Box::new(|rng| Expr::Cast {
                                //     expr: Box::arbitrary_sized_from(rng, t),
                                //     type_name: Option::arbitrary(rng),
                                // }),
                                // Box::new(|rng| Expr::Collate(Box::arbitrary_sized_from(rng, t), CollateName::arbitrary(rng).0)),
                                // Box::new(|rng| Expr::InList {
                                //     lhs: Box::arbitrary_sized_from(rng, t),
                                //     not: rng.gen_bool(0.5),
                                //     rhs: Option::arbitrary_from(rng, t),
                                // }),
                                // Box::new(|rng| Expr::IsNull(Box::arbitrary_sized_from(rng, t))),
                                // Box::new(|rng| {
                                //     // let op = LikeOperator::arbitrary_from(rng, t);
                                //     let op = ast::LikeOperator::Like; // todo: remove this line when LikeOperator is implemented
                                //     let escape = if matches!(op, LikeOperator::Like) {
                                //         Option::arbitrary_sized_from(rng, t, size - 1)
                                //     } else {
                                //         None
                                //     };
                                //     Expr::Like {
                                //         lhs: Box::arbitrary_sized_from(rng, t, size - 1),
                                //         not: rng.gen_bool(0.5),
                                //         op,
                                //         rhs: Box::arbitrary_sized_from(rng, t, size - 1),
                                //         escape,
                                //     }
                                // }),
                                // Box::new(|rng| Expr::NotNull(Box::arbitrary_sized_from(rng, t))),
                                // // TODO: only supports one paranthesized expression
                                // Box::new(|rng| Expr::Parenthesized(vec![Expr::arbitrary_from(rng, t)])),
                                // Box::new(|rng| {
                                //     let table_idx = pick_index(t.tables.len(), rng);
                                //     let table = &t.tables[table_idx];
                                //     let col_idx = pick_index(table.columns.len(), rng);
                                //     let col = &table.columns[col_idx];
                                //     Expr::Qualified(Name(table.name.clone()), Name(col.name.clone()))
                                // })
                                Box::new(|rng| {
                                    Expr::Unary(
                                        UnaryOperator::arbitrary(rng, context),
                                        Box::arbitrary_sized(rng, context, size - 1),
                                    )
                                }),
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
                            ],
                            rng,
                        )
                    }),
                ),
            ],
            rng,
        )
    }
}

impl Arbitrary for Operator {
    fn arbitrary<R: rand::Rng + ?Sized, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        let choices = [
            Operator::Add,
            Operator::And,
            // Operator::ArrowRight, -- todo: not implemented in `binary_compare` yet
            // Operator::ArrowRightShift, -- todo: not implemented in `binary_compare` yet
            Operator::BitwiseAnd,
            // Operator::BitwiseNot, -- todo: not implemented in `binary_compare` yet
            Operator::BitwiseOr,
            // Operator::Concat, -- todo: not implemented in `exec_concat`
            Operator::Divide,
            Operator::Equals,
            Operator::Greater,
            Operator::GreaterEquals,
            Operator::Is,
            Operator::IsNot,
            Operator::LeftShift,
            Operator::Less,
            Operator::LessEquals,
            Operator::Modulus,
            Operator::Multiply,
            Operator::NotEquals,
            Operator::Or,
            Operator::RightShift,
            Operator::Subtract,
        ];
        *pick(&choices, rng)
    }
}

impl Arbitrary for Type {
    fn arbitrary<R: rand::Rng + ?Sized, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        let name = pick(&["INT", "INTEGER", "REAL", "TEXT", "BLOB", "ANY"], rng).to_string();
        Self {
            name,
            size: None, // TODO: come back later here
            size_text: None,
        }
    }
}

impl Arbitrary for QualifiedName {
    fn arbitrary<R: rand::Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        // TODO: for now just generate table name
        let table_idx = pick_index(context.tables().len(), rng);
        let table = &context.tables()[table_idx];
        // TODO: for now forego alias
        Self {
            db_name: None,
            name: Name::from_string(&table.name),
            alias: None,
        }
    }
}

impl Arbitrary for LikeOperator {
    fn arbitrary<R: rand::Rng + ?Sized, C: GenerationContext>(rng: &mut R, _t: &C) -> Self {
        let choice = rng.random_range(0..4);
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
impl Arbitrary for ast::Literal {
    fn arbitrary<R: rand::Rng + ?Sized, C: GenerationContext>(rng: &mut R, _t: &C) -> Self {
        loop {
            let choice = rng.random_range(0..5);
            let lit = match choice {
                0 => ast::Literal::Numeric({
                    let integer = rng.random_bool(0.5);
                    if integer {
                        rng.random_range(i64::MIN..i64::MAX).to_string()
                    } else {
                        rng.random_range(-1e10..1e10).to_string()
                    }
                }),
                1 => ast::Literal::String(format!("'{}'", gen_random_text(rng))),
                2 => ast::Literal::Blob(hex::encode(gen_random_text(rng).as_bytes())),
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
impl ArbitraryFrom<&Vec<&SimValue>> for ast::Expr {
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        _context: &C,
        values: &Vec<&SimValue>,
    ) -> Self {
        if values.is_empty() {
            return Self::Literal(ast::Literal::Null);
        }
        // TODO: for now just convert the value to an ast::Literal
        let value = pick(values, rng);
        Expr::Literal((*value).into())
    }
}

impl Arbitrary for UnaryOperator {
    fn arbitrary<R: rand::Rng + ?Sized, C: GenerationContext>(rng: &mut R, _t: &C) -> Self {
        let choice = rng.random_range(0..4);
        match choice {
            0 => Self::BitwiseNot,
            1 => Self::Negative,
            2 => Self::Not,
            3 => Self::Positive,
            _ => unreachable!(),
        }
    }
}
