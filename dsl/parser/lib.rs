use std::{borrow::Cow, marker::PhantomData};

use chumsky::prelude::*;
use rusqlite::types::Value;

#[derive(Debug)]
struct Test<'a> {
    /// List of databases to test on
    databases: Option<Vec<&'a str>>,
    /// Test Name
    ident: Cow<'a, str>,
    /// Sql Statement(s) to run
    statement: Statement<'a>,
    /// Expected Output Value from test
    values: Vec<Vec<Value>>,
}

/// Sql Statement(s)
#[derive(Debug)]
enum Statement<'a> {
    Single(&'a str),
    Many(Vec<&'a str>),
}

fn parser_test<'src>() -> impl Parser<'src, &'src str, Test<'src>, extra::Err<Rich<'src, char>>> {
    let test_keyword = text::keyword("test");

    let ident: Boxed<'_, '_, &'src str, &'src str, extra::Full<Rich<'src, char>, (), ()>> =
        text::unicode::ident().padded().boxed();

    let sql_query = text::unicode::ident();

    let statement = choice((
        sql_query.map(|s: &'src str| Statement::Single(s)),
        sql_query
            .separated_by(just(',').padded())
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(just('[').padded(), just(']').padded())
            .map(|s: Vec<&str>| Statement::Many(s)),
    ))
    .boxed();

    let contents = ident
        .then_ignore(just(',').padded())
        .then(statement)
        .map(|(ident, statement)| Test {
            databases: None,
            ident: ident.into(),
            statement,
            values: vec![],
        })
        .delimited_by(just('(').padded(), just(')').padded())
        .boxed();

    test_keyword.ignore_then(contents).boxed()
}

fn parser_test_many<'src>(
) -> impl Parser<'src, &'src str, Vec<Test<'src>>, extra::Err<Rich<'src, char>>> {
    parser_test()
        .padded()
        .repeated()
        .collect::<Vec<_>>()
        .boxed()
}

#[cfg(test)]
mod tests {
    use chumsky::Parser;

    use crate::parser_test_many;

    #[test]
    fn test_basic() {
        let parser = parser_test_many();
        let x = parser.parse("test(hi, [SELECT ])\n test(second, [SELECT ])");
        dbg!(&x);
        x.unwrap();
    }
}
