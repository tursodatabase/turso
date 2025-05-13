mod sqlite_values;

use std::borrow::Cow;

use chumsky::prelude::*;
pub use chumsky::Parser;
use regex::Regex;
use rusqlite::types::Value;
use sqlite_values::{sqlite_values_parser, text};

#[derive(Debug, PartialEq)]
pub struct Test<'a> {
    /// List of databases to test on
    pub kind: TestKind<'a>,
    /// Test mode
    pub mode: TestMode,
    /// Test Name
    // Idea of a Cow<'a, str> is that you can manipulate the ident with format! and still use this struct
    // e.g converting the ident to snake_case
    pub ident: Cow<'a, str>,
    /// Sql Statement(s) to run
    pub statement: Statement<'a>,
    /// Expected Output Value from test
    pub values: Vec<Vec<Value>>,
}

#[derive(Debug, Clone)]
pub struct WrappedRegex(pub Regex);

impl PartialEq for WrappedRegex {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str().eq(other.0.as_str())
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
pub enum TestKind<'a> {
    /// Default Databases
    #[default]
    Default,
    /// Specific Databases
    Databases(Vec<&'a str>),
    /// In-memory Database
    Memory,
    /// In-memory Regex test
    Regex(WrappedRegex),
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

fn escape<'src>() -> impl Parser<'src, &'src str, (), extra::Err<Rich<'src, char>>> + Copy {
    let escape = just('\\').then(any()).ignored();

    escape
}

fn kind<'src>() -> impl Parser<'src, &'src str, TestKind<'src>, extra::Err<Rich<'src, char>>> {
    let escape = escape();

    let db_path = none_of("\\\"")
        .ignored()
        .or(escape)
        .repeated()
        .to_slice()
        .delimited_by(just('"'), just('"'));

    let kind = choice((
        just("memory").to(TestKind::Memory),
        none_of("\\\"")
            .ignored()
            .or(escape)
            .repeated()
            .to_slice()
            .delimited_by(just("r\""), just('"'))
            .try_map(|re, span| {
                let regex = Regex::new(re).map_err(|err| Rich::custom(span, err))?;
                Ok(TestKind::Regex(WrappedRegex(regex)))
            })
            .boxed()
            .labelled("regex"),
        db_path
            .separated_by(just(',').padded())
            .allow_trailing()
            .at_least(1)
            .collect::<Vec<_>>()
            .map(|dbs| TestKind::Databases(dbs))
            .delimited_by(just('[').padded(), just(']').padded()),
    ))
    .boxed();
    kind
}

fn statement<'src>() -> impl Parser<'src, &'src str, Statement<'src>, extra::Err<Rich<'src, char>>>
{
    let sql_query = text();

    let statement = choice((
        sql_query.clone().map(|s| Statement::Single(s)),
        sql_query
            .separated_by(just(',').padded())
            .allow_trailing()
            .at_least(1)
            .collect::<Vec<_>>()
            .delimited_by(just('[').padded(), just(']').padded())
            .map(|s: Vec<&str>| Statement::Many(s)),
    ))
    .boxed();
    statement
}

pub fn contents_without_values<'src>() -> impl Parser<
    'src,
    &'src str,
    (Option<TestKind<'src>>, &'src str, Statement<'src>),
    extra::Err<Rich<'src, char>>,
> {
    let ident = text::unicode::ident().padded();

    let contents = kind()
        .then_ignore(just(',').padded())
        .or_not()
        .then(ident)
        .then_ignore(just(',').padded())
        .then(statement())
        .map(|((kind, ident), statement)| (kind, ident, statement))
        .boxed();
    contents
}

pub fn test_parser<'src>() -> impl Parser<'src, &'src str, Test<'src>, extra::Err<Rich<'src, char>>>
{
    let test_keyword = text::keyword("test");
    let test_error_keyword = text::keyword("test_error");

    let contents_with_value = contents_without_values()
        .then(
            just(',')
                .padded()
                .ignore_then(sqlite_values_parser())
                .or_not(),
        )
        .try_map(|((kind, ident, statement), values), span| {
            let kind = kind.unwrap_or_default();
            if matches!(kind, TestKind::Regex(..)) && values.is_some() {
                return Err(Rich::custom(
                    span,
                    "regex text cannot have expected values declared",
                ));
            }
            let values = values.unwrap_or(vec![vec![]]);

            Ok(Test {
                kind,
                mode: TestMode::Normal,
                ident: ident.into(),
                statement,
                values,
            })
        })
        .delimited_by(just('(').padded(), just(')').padded())
        .boxed();

    let contents_no_value = contents_without_values()
        .map(|(kind, ident, statement)| Test {
            kind: kind.unwrap_or_default(),
            mode: TestMode::Error,
            ident: ident.into(),
            statement,
            values: vec![vec![]],
        })
        .delimited_by(just('(').padded(), just(')').padded())
        .boxed();

    choice((
        test_keyword.ignore_then(contents_with_value),
        test_error_keyword.ignore_then(contents_no_value.validate(|test, extra, emitter| {
            if matches!(test.mode, TestMode::Error) && matches!(test.kind, TestKind::Regex(..)) {
                emitter.emit(Rich::custom(
                    extra.span(),
                    "cannot have an error test with regex",
                ));
            }
            test
        })),
    ))
}

pub fn parser_dsl<'src>(
) -> impl Parser<'src, &'src str, Vec<Test<'src>>, extra::Err<Rich<'src, char>>> + Clone {
    test_parser()
        .padded()
        .repeated()
        .collect::<Vec<_>>()
        .boxed()
}

#[cfg(test)]
mod tests {
    use chumsky::Parser;

    use crate::{parser_dsl, test_parser};

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
        let input = r#"test(test_single, "SELECT 1")"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);

        let input = r#"test(test_single, "SELECT 1 '")"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_many_statements_1() {
        let parser = test_parser();
        let input = r#"test(test_many, ["SELECT",])"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_many_statements_2() {
        let parser = test_parser();
        let input = r#"test(test_many, ["SELECT", "INSERT", "DELETE"])"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_many_tests() {
        let parser = parser_dsl();
        let input = r#"
            test(test_many, ["SELECT", "INSERT", "DELETE"])
            test(test_many_2, ["SELECT",])
            test(test_single, "SELECT")
        "#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_single_statement_with_value() {
        let parser = test_parser();
        let input = r#"test(test_single, "SELECT", [Null, 1])"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_many_tests_with_value() {
        let parser = parser_dsl();
        let input = r#"
            test(test_many, ["SELECT", "INSERT", "DELETE"], [Null, 1])
            test(test_many_2, ["SELECT",], [[Null, 1], ["hi"]])
            test(test_single, "SELECT", 1.234)
        "#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_error() {
        let parser = test_parser();
        let input = r#"test_error(test_error, "SELECT")"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);

        let input = r#"test_error(test_error, ["SELECT", "INSERT", "DELETE"])"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);
    }

    #[test]
    fn test_with_kind() {
        let parser = test_parser();
        let input = r#"test(["testing/users.db"],test_single, "SELECT", [Null, 1])"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);

        let parser = test_parser();
        let input = r#"test(["testing/users.db", "anything"],test_single, "SELECT", [Null, 1])"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);

        let parser = test_parser();
        let input = r#"test(memory,test_single, "SELECT", [Null, 1])"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);

        let input = r#"test(r"\d+\.\d+\.\d+", test_single, "SELECT")"#;
        let res = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, res);

        let input = r#"test(r"\d+\.\d+\.\d+", test_single, "SELECT", [Null, 1])"#;
        let res = parser.parse(input).has_errors();
        assert!(res);
    }
}
