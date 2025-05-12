mod sqlite_values;

use std::borrow::Cow;

use chumsky::prelude::*;
use rusqlite::types::Value;
use sqlite_values::sqlite_values_parser;

#[derive(Debug, PartialEq)]
pub struct Test<'a> {
    /// List of databases to test on
    databases: Option<Vec<&'a str>>,
    /// Test Name
    // Idea of a Cow<'a, str> is that you can manipulate the ident if needed and still use this struct
    ident: Cow<'a, str>,
    /// Sql Statement(s) to run
    statement: Statement<'a>,
    /// Expected Output Value from test
    values: Vec<Vec<Value>>,
}

/// Sql Statement(s)
#[derive(Debug, PartialEq, Eq)]
pub enum Statement<'a> {
    Single(&'a str),
    Many(Vec<&'a str>),
}

pub fn test_parser<'src>() -> impl Parser<'src, &'src str, Test<'src>, extra::Err<Rich<'src, char>>>
{
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
        .then(
            just(',')
                .padded()
                .ignore_then(sqlite_values_parser())
                .or_not(),
        )
        .map(|((ident, statement), values)| Test {
            databases: None,
            ident: ident.into(),
            statement,
            values: values.unwrap_or(vec![vec![]]),
        })
        .delimited_by(just('(').padded(), just(')').padded())
        .boxed();

    test_keyword.ignore_then(contents).boxed()
}

pub fn test_parser_many<'src>(
) -> impl Parser<'src, &'src str, Vec<Test<'src>>, extra::Err<Rich<'src, char>>> {
    test_parser()
        .padded()
        .repeated()
        .collect::<Vec<_>>()
        .boxed()
}

#[cfg(test)]
mod tests {
    use chumsky::Parser;

    use crate::{test_parser, test_parser_many};

    #[macro_export]
    macro_rules! assert_debug_snapshot_with_input {
        ($input:expr, $result:expr) => {
            ::insta::with_settings!({
                description => &format!("input: {}", $input)
            }, {
                ::insta::assert_debug_snapshot!($result);
            });
        };
    }

    #[test]
    fn test_single_statement() {
        let parser = test_parser();
        let input = "test(test_single, SELECT)";
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_many_statements_1() {
        let parser = test_parser();
        let input = "test(test_many, [SELECT,])";
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_many_statements_2() {
        let parser = test_parser();
        let input = "test(test_many, [SELECT, INSERT, DELETE])";
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_many_tests() {
        let parser = test_parser_many();
        let input = r#"
            test(test_many, [SELECT, INSERT, DELETE])
            test(test_many_2, [SELECT,])
            test(test_single, SELECT)
        "#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_single_statement_with_value() {
        let parser = test_parser();
        let input = "test(test_single, SELECT, [Null, 1])";
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_many_tests_with_value() {
        let parser = test_parser_many();
        let input = r#"
            test(test_many, [SELECT, INSERT, DELETE], [Null, 1])
            test(test_many_2, [SELECT,], [[Null, 1], ["hi"]])
            test(test_single, SELECT, 1.234)
        "#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }
}
