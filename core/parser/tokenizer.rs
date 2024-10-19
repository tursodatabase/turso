use std::ops::Range;

use winnow::ascii::Caseless;
use winnow::combinator::{alt, delimited};
use winnow::error::{AddContext, ParserError, StrContext};
use winnow::token::{literal, take_while};
use winnow::PResult;
use winnow::{prelude::*, Located};

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SqlTokenKind {
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    As,
    GroupBy,
    OrderBy,
    Limit,
    ParenL,
    ParenR,
    Comma,
    Asterisk,
    Plus,
    Minus,
    Slash,
    Eq,
    Neq,
    Ge,
    Gt,
    Le,
    Lt,
    Join,
    Left,
    Inner,
    Outer,
    On,
    Asc,
    Desc,
    Semicolon,
    Period,
    Like,
    Literal,
    Identifier,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SqlToken {
    kind: SqlTokenKind,
    span: Range<usize>,
}

impl SqlToken {
    pub fn materialize<'a>(&self, source: &'a [u8]) -> &'a [u8] {
        &source[self.span.start as usize..self.span.end as usize]
    }

    pub fn print(&self, source: &[u8]) -> String {
        let token_slice = &source[self.span.start as usize..self.span.end as usize];
        let token_info = match self.kind {
            SqlTokenKind::Literal => {
                format!("literal: {}", std::str::from_utf8(token_slice).unwrap())
            }
            SqlTokenKind::Identifier => {
                format!("identifier: {}", std::str::from_utf8(token_slice).unwrap())
            }
            SqlTokenKind::Select => format!("'SELECT'"),
            SqlTokenKind::From => format!("'FROM'"),
            SqlTokenKind::Where => format!("'WHERE'"),
            SqlTokenKind::And => format!("'AND'"),
            SqlTokenKind::Or => format!("'OR'"),
            SqlTokenKind::Not => format!("'NOT'"),
            SqlTokenKind::As => format!("'AS'"),
            SqlTokenKind::GroupBy => format!("'GROUP BY'"),
            SqlTokenKind::OrderBy => format!("'ORDER BY'"),
            SqlTokenKind::Limit => format!("'LIMIT'"),
            SqlTokenKind::ParenL => format!("'('"),
            SqlTokenKind::ParenR => format!("')'"),
            SqlTokenKind::Comma => format!("','"),
            SqlTokenKind::Asterisk => format!("'*'"),
            SqlTokenKind::Plus => format!("'+'"),
            SqlTokenKind::Minus => format!("'-'"),
            SqlTokenKind::Slash => format!("/"),
            SqlTokenKind::Eq => format!("="),
            SqlTokenKind::Neq => format!("!="),
            SqlTokenKind::Ge => format!(">="),
            SqlTokenKind::Gt => format!(">"),
            SqlTokenKind::Le => format!("<="),
            SqlTokenKind::Lt => format!("<"),
            SqlTokenKind::Join => format!("JOIN"),
            SqlTokenKind::Left => format!("LEFT"),
            SqlTokenKind::Inner => format!("INNER"),
            SqlTokenKind::Outer => format!("OUTER"),
            SqlTokenKind::On => format!("ON"),
            SqlTokenKind::Asc => format!("ASC"),
            SqlTokenKind::Desc => format!("DESC"),
            SqlTokenKind::Semicolon => format!(";"),
            SqlTokenKind::Period => format!("."),
            SqlTokenKind::Like => format!("LIKE"),
        };
        // Add context before token start, but minimum is 0
        let error_span_left = std::cmp::max(0, self.span.start as i64 - 10);
        let error_span_right = std::cmp::min(source.len(), self.span.end as usize + 10);
        format!(
            "{}, near '{}'",
            token_info,
            std::str::from_utf8(&source[error_span_left as usize..error_span_right as usize])
                .unwrap()
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SqlTokenStream<'a> {
    pub source: &'a [u8],
    pub tokens: Vec<SqlToken>,
    pub index: usize,
}

impl<'a> SqlTokenStream<'a> {
    pub fn next_token(&mut self) -> Option<SqlToken> {
        if self.index < self.tokens.len() {
            let token = self.tokens[self.index].clone();

            self.index += 1;
            Some(token)
        } else {
            None
        }
    }

    pub fn peek(&self, offset: usize) -> Option<SqlToken> {
        if self.index + offset < self.tokens.len() {
            Some(self.tokens[self.index + offset].clone())
        } else {
            None
        }
    }

    pub fn peek_kind(&self, offset: usize) -> Option<SqlTokenKind> {
        self.peek(offset).map(|token| token.kind)
    }
}

pub fn parse_sql_string_to_tokens<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &'i [u8],
) -> PResult<SqlTokenStream<'i>, E> {
    let mut tokens = Vec::new();
    let mut stream = Located::new(input);

    while !stream.is_empty() {
        match parse_sql_token(&mut stream) {
            Ok(token) => {
                if token.kind == SqlTokenKind::Semicolon {
                    break;
                }
                tokens.push(token);
            }
            Err(e) => return Err(e),
        }
    }

    Ok(SqlTokenStream {
        source: input,
        tokens,
        index: 0,
    })
}

fn parse_sql_token<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlToken, E> {
    delimited(ws, sql_token, ws).parse_next(input)
}

fn comparison_operator<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    alt((
        tk_neq.value(SqlTokenKind::Neq),
        tk_eq.value(SqlTokenKind::Eq),
        tk_ge.value(SqlTokenKind::Ge),
        tk_gt.value(SqlTokenKind::Gt),
        tk_le.value(SqlTokenKind::Le),
        tk_lt.value(SqlTokenKind::Lt),
    ))
    .parse_next(input)
}

fn join_related_stuff<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    alt((
        tk_join.value(SqlTokenKind::Join),
        tk_inner.value(SqlTokenKind::Inner),
        tk_outer.value(SqlTokenKind::Outer),
        tk_left.value(SqlTokenKind::Left),
        tk_on.value(SqlTokenKind::On),
    ))
    .parse_next(input)
}

fn asc_desc<'i, E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    alt((
        tk_asc.value(SqlTokenKind::Asc),
        tk_desc.value(SqlTokenKind::Desc),
    ))
    .parse_next(input)
}

fn mathy_operator<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    alt((
        tk_plus.value(SqlTokenKind::Plus),
        tk_minus.value(SqlTokenKind::Minus),
        tk_slash.value(SqlTokenKind::Slash),
        tk_asterisk.value(SqlTokenKind::Asterisk),
        tk_paren_l.value(SqlTokenKind::ParenL),
        tk_paren_r.value(SqlTokenKind::ParenR),
        tk_comma.value(SqlTokenKind::Comma),
    ))
    .parse_next(input)
}

fn keyword<'i, E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    alt((
        tk_select.value(SqlTokenKind::Select),
        tk_from.value(SqlTokenKind::From),
        tk_where.value(SqlTokenKind::Where),
        tk_and.value(SqlTokenKind::And),
        tk_order_by.value(SqlTokenKind::OrderBy),
        tk_or.value(SqlTokenKind::Or),
        tk_not.value(SqlTokenKind::Not),
        tk_group_by.value(SqlTokenKind::GroupBy),
        tk_limit.value(SqlTokenKind::Limit),
        join_related_stuff,
        asc_desc,
        tk_as.value(SqlTokenKind::As),
        tk_like.value(SqlTokenKind::Like),
    ))
    .parse_next(input)
}

fn sql_token<'i, E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlToken, E> {
    alt((
        keyword,
        comparison_operator,
        mathy_operator,
        tk_period.value(SqlTokenKind::Period),
        tk_literal.value(SqlTokenKind::Literal),
        tk_identifier.value(SqlTokenKind::Identifier),
        tk_semicolon,
    ))
    .with_span()
    .parse_next(input)
    .map(|(kind, span)| SqlToken { kind, span: span })
}

fn ws<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    take_while(0.., |b| WS.contains(&b)).parse_next(input)
}

const WS: &[u8] = &[b' ', b'\t', b'\r', b'\n'];

fn tk_like<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("LIKE")).parse_next(input)
}

fn tk_period<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(".").parse_next(input)
}

fn tk_semicolon<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    literal(";")
        .value(SqlTokenKind::Semicolon)
        .parse_next(input)
}

fn tk_join<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("JOIN")).parse_next(input)
}

fn tk_inner<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("INNER")).parse_next(input)
}

fn tk_outer<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("OUTER")).parse_next(input)
}

fn tk_left<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("LEFT")).parse_next(input)
}

fn tk_on<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("ON")).parse_next(input)
}

fn tk_as<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("AS")).parse_next(input)
}

fn tk_select<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("SELECT")).parse_next(input)
}

fn tk_from<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("FROM")).parse_next(input)
}

fn tk_where<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("WHERE")).parse_next(input)
}

fn tk_and<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("AND")).parse_next(input)
}

fn tk_or<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("OR")).parse_next(input)
}

fn tk_not<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("NOT")).parse_next(input)
}

fn tk_group_by<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("GROUP BY")).parse_next(input)
}

fn tk_order_by<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("ORDER BY")).parse_next(input)
}

fn tk_limit<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("LIMIT")).parse_next(input)
}

fn tk_eq<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("=").parse_next(input)
}

fn tk_neq<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    alt((literal("!="), literal("<>"))).parse_next(input)
}

fn tk_ge<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(">=").parse_next(input)
}

fn tk_gt<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(">").parse_next(input)
}

fn tk_le<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("<=").parse_next(input)
}

fn tk_lt<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("<").parse_next(input)
}

fn tk_paren_l<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("(").parse_next(input)
}

fn tk_paren_r<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(")").parse_next(input)
}

fn tk_comma<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(",").parse_next(input)
}

fn tk_asterisk<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("*").parse_next(input)
}

fn tk_plus<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("+").parse_next(input)
}

fn tk_minus<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("-").parse_next(input)
}

fn tk_slash<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("/").parse_next(input)
}

const HIGHEST_ASCII_CHARACTER: u8 = 0x7F;

fn utf8_compatible_identifier_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c > HIGHEST_ASCII_CHARACTER
}

fn tk_identifier<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    take_while(1.., utf8_compatible_identifier_char).parse_next(input)
}

fn tk_literal<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    alt((
        delimited(
            literal("'"),
            take_while(0.., |b: u8| b != b'\''),
            literal("'"),
        ),
        take_while(1.., |b: u8| b.is_ascii_digit() || b == b'.'),
    ))
    .parse_next(input)
}

fn tk_asc<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("ASC")).parse_next(input)
}

fn tk_desc<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(Caseless("DESC")).parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    type Error = winnow::error::ContextError;

    #[test]
    fn test_token_parsing() {
        let mut test_cases = vec![
            (
                "SELECT",
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 0..6,
                },
            ),
            (
                "FROM",
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 0..4,
                },
            ),
            (
                "WHERE",
                SqlToken {
                    kind: SqlTokenKind::Where,
                    span: 0..5,
                },
            ),
            (
                "AND",
                SqlToken {
                    kind: SqlTokenKind::And,
                    span: 0..3,
                },
            ),
            (
                "OR",
                SqlToken {
                    kind: SqlTokenKind::Or,
                    span: 0..2,
                },
            ),
            (
                "NOT",
                SqlToken {
                    kind: SqlTokenKind::Not,
                    span: 0..3,
                },
            ),
            (
                "GROUP BY",
                SqlToken {
                    kind: SqlTokenKind::GroupBy,
                    span: 0..8,
                },
            ),
            (
                "ORDER BY",
                SqlToken {
                    kind: SqlTokenKind::OrderBy,
                    span: 0..8,
                },
            ),
            (
                "LIMIT",
                SqlToken {
                    kind: SqlTokenKind::Limit,
                    span: 0..5,
                },
            ),
            (
                "=",
                SqlToken {
                    kind: SqlTokenKind::Eq,
                    span: 0..1,
                },
            ),
            (
                "!=",
                SqlToken {
                    kind: SqlTokenKind::Neq,
                    span: 0..2,
                },
            ),
            (
                "<>",
                SqlToken {
                    kind: SqlTokenKind::Neq,
                    span: 0..2,
                },
            ),
            (
                ">=",
                SqlToken {
                    kind: SqlTokenKind::Ge,
                    span: 0..2,
                },
            ),
            (
                ">",
                SqlToken {
                    kind: SqlTokenKind::Gt,
                    span: 0..1,
                },
            ),
            (
                "<=",
                SqlToken {
                    kind: SqlTokenKind::Le,
                    span: 0..2,
                },
            ),
            (
                "<",
                SqlToken {
                    kind: SqlTokenKind::Lt,
                    span: 0..1,
                },
            ),
            (
                "(",
                SqlToken {
                    kind: SqlTokenKind::ParenL,
                    span: 0..1,
                },
            ),
            (
                ")",
                SqlToken {
                    kind: SqlTokenKind::ParenR,
                    span: 0..1,
                },
            ),
            (
                ",",
                SqlToken {
                    kind: SqlTokenKind::Comma,
                    span: 0..1,
                },
            ),
            (
                "*",
                SqlToken {
                    kind: SqlTokenKind::Asterisk,
                    span: 0..1,
                },
            ),
            (
                "+",
                SqlToken {
                    kind: SqlTokenKind::Plus,
                    span: 0..1,
                },
            ),
            (
                "-",
                SqlToken {
                    kind: SqlTokenKind::Minus,
                    span: 0..1,
                },
            ),
            (
                "/",
                SqlToken {
                    kind: SqlTokenKind::Slash,
                    span: 0..1,
                },
            ),
            (
                "column_name",
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 0..11,
                },
            ),
            (
                "🤡",
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 0..4,
                },
            ),
            (
                "'string literal'",
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 0..16,
                },
            ),
            (
                "123",
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 0..3,
                },
            ),
            (
                "123.45",
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 0..6,
                },
            ),
        ];

        for (input, expected) in test_cases.iter_mut() {
            assert_eq!(
                parse_sql_token::<Error>(&mut Located::new(input.as_bytes())),
                Ok(expected.clone())
            );
        }

        // Test case-insensitivity
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("select".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Select,
                span: 0..6
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("FROM".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::From,
                span: 0..4
            })
        );

        // Test with trailing content
        let input = "SELECT ".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Select,
                span: 0..6
            })
        );

        // Test with trailing content for other tokens
        let input = "WHERE condition".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Where,
                span: 0..5
            })
        );

        let input = "AND another_condition".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::And,
                span: 0..3
            })
        );

        let input = "OR alternative".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Or,
                span: 0..2
            })
        );

        let input = "NOT excluded".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Not,
                span: 0..3
            })
        );

        let input = "GROUP BY column".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::GroupBy,
                span: 0..8
            })
        );

        let input = "ORDER BY column DESC".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::OrderBy,
                span: 0..8
            })
        );

        let input = "LIMIT 10".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Limit,
                span: 0..5
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("FROM table".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::From,
                span: 0..4
            })
        );

        // Test identifier
        let input = "column_name".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Identifier,
                span: 0..11
            })
        );

        // Test string literal
        let input = "'string value'".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Literal,
                span: 0..14
            })
        );

        // Test numeric literal
        let input = "123.45".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Literal,
                span: 0..6
            })
        );

        // Test comparison operators
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("=".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Eq,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("!=".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Neq,
                span: 0..2
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("<>".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Neq,
                span: 0..2
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(">=".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Ge,
                span: 0..2
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(">".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Gt,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("<=".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Le,
                span: 0..2
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("<".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Lt,
                span: 0..1
            })
        );

        // Test new tokens
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("(".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::ParenL,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(")".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::ParenR,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(",".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Comma,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("*".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Asterisk,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("+".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Plus,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("-".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Minus,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("/".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Slash,
                span: 0..1
            })
        );
    }

    #[test]
    fn test_parse_sql_string_into_tokens() {
        let input = b"SELECT column1 FROM table WHERE condition ORDER BY column1 LIMIT 10";
        let expected = SqlTokenStream {
            source: input,
            tokens: vec![
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 0..6,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 7..14,
                },
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 15..19,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 20..25,
                },
                SqlToken {
                    kind: SqlTokenKind::Where,
                    span: 26..31,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 32..41,
                },
                SqlToken {
                    kind: SqlTokenKind::OrderBy,
                    span: 42..50,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 51..58,
                },
                SqlToken {
                    kind: SqlTokenKind::Limit,
                    span: 59..64,
                },
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 65..67,
                },
            ],
            index: 0,
        };

        let result = parse_sql_string_to_tokens::<Error>(input);
        assert_eq!(result, Ok(expected));

        // Test with mixed case and literals
        let input = b"select col1 from TABLE where COL1 = 'value' GROUP BY col1";
        let expected = SqlTokenStream {
            source: input,
            tokens: vec![
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 0..6,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 7..11,
                },
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 12..16,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 17..22,
                },
                SqlToken {
                    kind: SqlTokenKind::Where,
                    span: 23..28,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 29..33,
                },
                SqlToken {
                    kind: SqlTokenKind::Eq,
                    span: 34..35,
                },
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 36..43,
                },
                SqlToken {
                    kind: SqlTokenKind::GroupBy,
                    span: 44..52,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 53..57,
                },
            ],
            index: 0,
        };

        let result = parse_sql_string_to_tokens::<Error>(input);
        assert_eq!(result, Ok(expected));

        // Test with new tokens
        let input = b"SELECT * FROM (SELECT id, name FROM users) AS subquery WHERE id > 5";
        let expected = SqlTokenStream {
            source: input,
            tokens: vec![
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 0..6,
                },
                SqlToken {
                    kind: SqlTokenKind::Asterisk,
                    span: 7..8,
                },
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 9..13,
                },
                SqlToken {
                    kind: SqlTokenKind::ParenL,
                    span: 14..15,
                },
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 15..21,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 22..24,
                },
                SqlToken {
                    kind: SqlTokenKind::Comma,
                    span: 24..25,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 26..30,
                },
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 31..35,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 36..41,
                },
                SqlToken {
                    kind: SqlTokenKind::ParenR,
                    span: 41..42,
                },
                SqlToken {
                    kind: SqlTokenKind::As,
                    span: 43..45,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 46..54,
                },
                SqlToken {
                    kind: SqlTokenKind::Where,
                    span: 55..60,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 61..63,
                },
                SqlToken {
                    kind: SqlTokenKind::Gt,
                    span: 64..65,
                },
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 66..67,
                },
            ],
            index: 0,
        };

        let result = parse_sql_string_to_tokens::<Error>(input);
        assert_eq!(result, Ok(expected));
    }
}
