//! The grammar of `postgres.bki`, as chumsky parsers.
//!
//! The crate documentation shows the format.

use chumsky::prelude::*;

use crate::bki::{Column, Index, Nullness, Token};

/// One command of the file.
pub(crate) enum Statement<'src> {
    Create {
        name: &'src str,
        columns: Vec<Column>,
    },
    Open(&'src str),
    Close,
    Insert(Vec<Token>),
    Index {
        catalog: &'src str,
        index: Index,
    },
    /// A command that the generator does not use: a comment, or the
    /// declaration of a TOAST table.
    Other,
}

type ParseError<'src> = extra::Err<Rich<'src, char>>;

/// The commands of the file, one per line, each with its place in the
/// file. A line that starts with a word that is not a command of the
/// format fails the parse, so a malformed `insert` is not skipped.
pub(crate) fn file_parser<'src>(
) -> impl Parser<'src, &'src str, Vec<(Statement<'src>, SimpleSpan)>, ParseError<'src>> {
    let rest_of_line = none_of('\n').repeated();

    let open = just("open ").ignore_then(word()).map(Statement::Open);
    let close = just("close ").then(word()).map(|_| Statement::Close);
    let insert = insert_parser().map(Statement::Insert);
    // A malformed index declaration fails the parse, as it does not start
    // with `declare toast`.
    let other = choice((just("declare toast "), just("build "), just("#")))
        .then(rest_of_line)
        .map(|_| Statement::Other);

    choice((create_parser(), open, close, insert, index_parser(), other))
        .map_with(|statement, extra| (statement, extra.span()))
        .separated_by(just('\n'))
        .allow_trailing()
        .collect()
        .then_ignore(end())
}

/// A `create` command and the column list on the lines after it:
///
/// ```text
/// create pg_cast 2605
///  (
///  oid = oid ,
///  castcontext = char
///  )
/// ```
///
/// The words after the catalog name are its oid and its storage options,
/// which the generator does not use.
fn create_parser<'src>() -> impl Parser<'src, &'src str, Statement<'src>, ParseError<'src>> {
    just("create ")
        .ignore_then(word())
        .then_ignore(none_of('\n').repeated())
        .then_ignore(just("\n (\n"))
        .then(column_parser().then_ignore(just('\n')).repeated().collect())
        .then_ignore(just(" )"))
        .map(|(name, columns)| Statement::Create { name, columns })
}

/// One line of a column list: `name = type`, then `FORCE NOT NULL` or
/// `FORCE NULL` for some columns, then a comma unless it is the last.
fn column_parser<'src>() -> impl Parser<'src, &'src str, Column, ParseError<'src>> {
    let nullness = choice((
        just(" FORCE NOT NULL").to(Nullness::ForceNotNull),
        just(" FORCE NULL").to(Nullness::ForceNull),
    ))
    .or_not()
    .map(|forced| forced.unwrap_or(Nullness::Auto));

    just(' ')
        .ignore_then(word())
        .then_ignore(just(" = "))
        .then(word())
        .then(nullness)
        .then_ignore(just(" ,").or_not())
        .map(|((name, bki_type), nullness)| Column {
            name: name.to_string(),
            bki_type: bki_type.to_string(),
            nullness,
        })
}

/// The declaration of an index, unique or not:
///
/// ```text
/// declare unique index pg_am_oid_index 2652 on pg_am using btree(oid oid_ops)
/// declare index pg_index_indrelid_index 2678 on pg_index using btree(indrelid oid_ops)
/// ```
///
/// The number after the index name is the oid of the index. Each column is
/// followed by its operator class.
fn index_parser<'src>() -> impl Parser<'src, &'src str, Statement<'src>, ParseError<'src>> {
    let name = none_of(" \n,()").repeated().at_least(1).to_slice();
    let column = name.then_ignore(just(' ')).then_ignore(name);

    just("declare ")
        .ignore_then(just("unique ").or_not().map(|unique| unique.is_some()))
        .then_ignore(just("index "))
        .then(word())
        .then_ignore(just(' '))
        .then_ignore(word())
        .then_ignore(just(" on "))
        .then(word())
        .then_ignore(just(" using "))
        .then_ignore(name)
        .then(
            column
                .separated_by(just(", "))
                .at_least(1)
                .collect::<Vec<&str>>()
                .delimited_by(just('('), just(')')),
        )
        .map(|(((unique, name), catalog), columns)| Statement::Index {
            catalog,
            index: Index {
                name: name.to_string(),
                unique,
                columns: columns.into_iter().map(str::to_string).collect(),
            },
        })
}

/// An `insert` command and its values: `insert ( 16 bool ',' _null_ )`.
pub(crate) fn insert_parser<'src>() -> impl Parser<'src, &'src str, Vec<Token>, ParseError<'src>> {
    // A quote inside a quoted value is written twice.
    let quoted = just("''")
        .to('\'')
        .or(none_of("'\n"))
        .repeated()
        .collect::<String>()
        .delimited_by(just('\''), just('\''))
        .map(Token::Quoted);
    // The closing parenthesis follows a space, as a value does. A value
    // that starts with a quote is a quoted value that does not end.
    let bare = word()
        .filter(|text: &&str| *text != ")" && !text.starts_with('\''))
        .map(|text: &str| Token::Bare(text.to_string()));

    quoted
        .or(bare)
        .separated_by(just(' '))
        .collect()
        .delimited_by(just("insert ( "), just(" )"))
}

/// Characters up to a space or the end of the line, at least one.
fn word<'src>() -> impl Parser<'src, &'src str, &'src str, ParseError<'src>> + Clone {
    none_of(" \n").repeated().at_least(1).to_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quote_inside_a_quoted_value_is_written_twice() {
        let row = insert_parser()
            .parse("insert ( 100 'database''s default' '' - )")
            .into_result()
            .unwrap();
        let texts: Vec<&str> = row.iter().map(Token::text).collect();
        assert_eq!(texts, ["100", "database's default", "", "-"]);
        assert!(row[1] == Token::Quoted("database's default".to_string()));
    }

    #[test]
    fn a_quoted_null_word_is_text_and_not_null() {
        let row = insert_parser()
            .parse("insert ( '_null_' _null_ )")
            .into_result()
            .unwrap();
        assert!(!row[0].is_null());
        assert!(row[1].is_null());
    }
}
