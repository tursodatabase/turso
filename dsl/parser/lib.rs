mod sqlite_values;

use std::borrow::Cow;

use chumsky::prelude::*;
use rusqlite::types::Value;
use sqlite_values::sqlite_values_parser;

#[derive(Debug, PartialEq)]
pub struct Test<'a> {
    /// List of databases to test on
    kind: TestKind<'a>,
    /// Test mode
    mode: TestMode,
    /// Test Name
    // Idea of a Cow<'a, str> is that you can manipulate the ident with format! and still use this struct
    // e.g converting the ident to snake_case
    ident: Cow<'a, str>,
    /// Sql Statement(s) to run
    statement: Statement<'a>,
    /// Expected Output Value from test
    values: Vec<Vec<Value>>,
}

#[derive(Debug, PartialEq)]
pub enum TestKind<'a> {
    /// Default Databases
    Default,
    /// Specific Databases
    Databases(Vec<&'a str>),
    /// In-memory Database
    Memory,
    /// In-memory Regex test
    Regex,
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum TestMode {
    /// Follow normal execution
    #[default]
    Normal,
    /// Expect an Error to occur
    Error,
}

/// Sql Statement(s)
#[derive(Debug, PartialEq, Eq)]
pub enum Statement<'a> {
    Single(&'a str),
    Many(Vec<&'a str>),
}

pub fn test_contents_without_values<'src>(
) -> impl Parser<'src, &'src str, (&'src str, Statement<'src>), extra::Err<Rich<'src, char>>> {
    let ident = text::unicode::ident().padded();

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
        .boxed();
    contents
}

pub fn test_parser<'src>() -> impl Parser<'src, &'src str, Test<'src>, extra::Err<Rich<'src, char>>>
{
    let test_keyword = text::keyword("test");
    let test_error_keyword = text::keyword("test_error");

    let contents_with_value = test_contents_without_values()
        .then(
            just(',')
                .padded()
                .ignore_then(sqlite_values_parser())
                .or_not(),
        )
        .map(|((ident, statement), values)| Test {
            kind: TestKind::Default,
            mode: TestMode::Normal,
            ident: ident.into(),
            statement,
            values: values.unwrap_or(vec![vec![]]),
        })
        .delimited_by(just('(').padded(), just(')').padded())
        .boxed();

    let contents_no_value = test_contents_without_values()
        .map(|(ident, statement)| Test {
            kind: TestKind::Default,
            mode: TestMode::Error,
            ident: ident.into(),
            statement,
            values: vec![vec![]],
        })
        .delimited_by(just('(').padded(), just(')').padded())
        .boxed();

    choice((
        test_keyword.ignore_then(contents_with_value),
        test_error_keyword.ignore_then(contents_no_value),
    ))
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

    #[test]
    fn test_error() {
        let parser = test_parser();
        let input = r#"test_error(test_error, SELECT)"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);

        let input = r#"test_error(test_error, [SELECT, INSERT, DELETE])"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }
}
