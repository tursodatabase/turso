mod sqlite_values;

use std::borrow::Cow;

use chumsky::prelude::*;
use rusqlite::types::Value;

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

pub fn test_parser_many<'src>(
) -> impl Parser<'src, &'src str, Vec<Test<'src>>, extra::Err<Rich<'src, char>>> {
    test_parser()
        .padded()
        .repeated()
        .collect::<Vec<_>>()
        .boxed()
}

// TODO: use INSTA for snapshot testing output of parser, instead of writing by hand
#[cfg(test)]
mod tests {
    use chumsky::Parser;

    use crate::{test_parser, test_parser_many, Test};

    #[test]
    fn test_single_statement() {
        let parser = test_parser();
        let res = parser.parse("test(test_single, SELECT)").unwrap();
        assert_eq!(
            res,
            Test {
                databases: None,
                ident: "test_single".into(),
                statement: crate::Statement::Single("SELECT"),
                values: vec![]
            }
        );
    }

    #[test]
    fn test_many_statements_1() {
        let parser = test_parser();
        let res = parser.parse("test(test_many, [SELECT,])").unwrap();
        assert_eq!(
            res,
            Test {
                databases: None,
                ident: "test_many".into(),
                statement: crate::Statement::Many(vec!["SELECT"]),
                values: vec![]
            }
        );
    }

    #[test]
    fn test_many_statements_2() {
        let parser = test_parser();
        let res = parser
            .parse("test(test_many, [SELECT, INSERT, DELETE])")
            .unwrap();
        assert_eq!(
            res,
            Test {
                databases: None,
                ident: "test_many".into(),
                statement: crate::Statement::Many(vec!["SELECT", "INSERT", "DELETE"]),
                values: vec![]
            }
        );
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
        let expected = vec![
            Test {
                databases: None,
                ident: "test_many".into(),
                statement: crate::Statement::Many(vec!["SELECT", "INSERT", "DELETE"]),
                values: vec![],
            },
            Test {
                databases: None,
                ident: "test_many_2".into(),
                statement: crate::Statement::Many(vec!["SELECT"]),
                values: vec![],
            },
            Test {
                databases: None,
                ident: "test_single".into(),
                statement: crate::Statement::Single("SELECT"),
                values: vec![],
            },
        ];
        res.into_iter()
            .zip(expected.into_iter())
            .for_each(|(got, expected)| assert_eq!(got, expected));
    }
}
