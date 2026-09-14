//! Lexer for the Starlark subset used by `CREATE FUNCTION ... LANGUAGE
//! starlark` bodies.
//!
//! Produces a flat token stream with Python-style `Indent`/`Dedent`/`Newline`
//! tokens derived from leading whitespace. Blank and comment-only lines are
//! skipped, newlines inside parentheses are treated as implicit line
//! continuations, and `\` at end of line continues the logical line.

use crate::{LimboError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Newline,
    Indent,
    Dedent,
    Ident(String),
    Int(i64),
    Float(f64),
    Str(String),
    // Keywords
    Return,
    If,
    Elif,
    Else,
    While,
    For,
    In,
    Break,
    Continue,
    Pass,
    And,
    Or,
    Not,
    NoneLit,
    True,
    False,
    // Punctuation and operators
    LParen,
    RParen,
    Comma,
    Colon,
    Semi,
    Plus,
    Minus,
    Star,
    StarStar,
    Slash,
    SlashSlash,
    Percent,
    Amp,
    Pipe,
    LtLt,
    GtGt,
    Tilde,
    EqEq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Assign,
    PlusEq,
    MinusEq,
    StarEq,
    SlashEq,
    SlashSlashEq,
    PercentEq,
    Eof,
}

/// Keywords that are reserved by Starlark (or needed for future extensions)
/// but not supported in function bodies. Rejected with a targeted error so the
/// user is not left with a generic syntax error.
const UNSUPPORTED_KEYWORDS: &[&str] = &[
    "def", "lambda", "load", "as", "assert", "async", "await", "class", "del", "except", "finally",
    "from", "global", "import", "is", "nonlocal", "raise", "try", "with", "yield",
];

pub struct SpannedTok {
    pub tok: Tok,
    /// 1-based line number, for error messages.
    pub line: usize,
}

fn err(line: usize, msg: impl std::fmt::Display) -> LimboError {
    LimboError::ParseError(format!("starlark (line {line}): {msg}"))
}

pub fn tokenize(src: &str) -> Result<Vec<SpannedTok>> {
    let bytes = src.as_bytes();
    let mut toks: Vec<SpannedTok> = Vec::new();
    let mut indents: Vec<usize> = vec![0];
    let mut pos = 0usize;
    let mut line = 1usize;
    let mut paren_depth = 0usize;
    // True whenever the lexer is at the start of a logical line and must
    // process indentation before emitting tokens.
    let mut at_line_start = true;

    macro_rules! push {
        ($t:expr) => {
            toks.push(SpannedTok { tok: $t, line })
        };
    }

    while pos < bytes.len() {
        if at_line_start && paren_depth == 0 {
            // Measure indentation; tabs advance to the next multiple of 8.
            let mut col = 0usize;
            let mut i = pos;
            while i < bytes.len() {
                match bytes[i] {
                    b' ' => col += 1,
                    b'\t' => col = (col / 8 + 1) * 8,
                    _ => break,
                }
                i += 1;
            }
            // Blank or comment-only lines do not affect indentation.
            if i >= bytes.len() {
                break;
            }
            if bytes[i] == b'\n' || bytes[i] == b'\r' {
                pos = skip_line_ending(bytes, i);
                line += 1;
                continue;
            }
            if bytes[i] == b'#' {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                pos = skip_line_ending(bytes, i);
                line += 1;
                continue;
            }
            let current = *indents.last().expect("indent stack is never empty");
            match col.cmp(&current) {
                std::cmp::Ordering::Greater => {
                    indents.push(col);
                    push!(Tok::Indent);
                }
                std::cmp::Ordering::Less => {
                    while *indents.last().expect("indent stack is never empty") > col {
                        indents.pop();
                        push!(Tok::Dedent);
                    }
                    if *indents.last().expect("indent stack is never empty") != col {
                        return Err(err(line, "unindent does not match any outer indentation"));
                    }
                }
                std::cmp::Ordering::Equal => {}
            }
            pos = i;
            at_line_start = false;
            continue;
        }

        let b = bytes[pos];
        match b {
            b' ' | b'\t' => pos += 1,
            b'\r' | b'\n' => {
                pos = skip_line_ending(bytes, pos);
                if paren_depth == 0 {
                    // Collapse consecutive newlines.
                    if !matches!(
                        toks.last(),
                        Some(SpannedTok {
                            tok: Tok::Newline,
                            ..
                        }) | None
                    ) {
                        push!(Tok::Newline);
                    }
                    at_line_start = true;
                }
                line += 1;
            }
            b'#' => {
                while pos < bytes.len() && bytes[pos] != b'\n' {
                    pos += 1;
                }
            }
            b'\\' => {
                let next = skip_line_ending(bytes, pos + 1);
                if next == pos + 1 {
                    return Err(err(line, "unexpected character '\\'"));
                }
                pos = next;
                line += 1;
            }
            b'(' => {
                paren_depth += 1;
                push!(Tok::LParen);
                pos += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                push!(Tok::RParen);
                pos += 1;
            }
            b',' => {
                push!(Tok::Comma);
                pos += 1;
            }
            b':' => {
                push!(Tok::Colon);
                pos += 1;
            }
            b';' => {
                push!(Tok::Semi);
                pos += 1;
            }
            b'+' => {
                pos += op(bytes, pos, &mut toks, line, Tok::PlusEq, Tok::Plus);
            }
            b'-' => {
                pos += op(bytes, pos, &mut toks, line, Tok::MinusEq, Tok::Minus);
            }
            b'%' => {
                pos += op(bytes, pos, &mut toks, line, Tok::PercentEq, Tok::Percent);
            }
            b'*' => {
                if bytes.get(pos + 1) == Some(&b'*') {
                    push!(Tok::StarStar);
                    pos += 2;
                } else {
                    pos += op(bytes, pos, &mut toks, line, Tok::StarEq, Tok::Star);
                }
            }
            b'/' => {
                if bytes.get(pos + 1) == Some(&b'/') {
                    if bytes.get(pos + 2) == Some(&b'=') {
                        push!(Tok::SlashSlashEq);
                        pos += 3;
                    } else {
                        push!(Tok::SlashSlash);
                        pos += 2;
                    }
                } else {
                    pos += op(bytes, pos, &mut toks, line, Tok::SlashEq, Tok::Slash);
                }
            }
            b'&' => {
                push!(Tok::Amp);
                pos += 1;
            }
            b'|' => {
                push!(Tok::Pipe);
                pos += 1;
            }
            b'~' => {
                push!(Tok::Tilde);
                pos += 1;
            }
            b'=' => {
                if bytes.get(pos + 1) == Some(&b'=') {
                    push!(Tok::EqEq);
                    pos += 2;
                } else {
                    push!(Tok::Assign);
                    pos += 1;
                }
            }
            b'!' => {
                if bytes.get(pos + 1) == Some(&b'=') {
                    push!(Tok::NotEq);
                    pos += 2;
                } else {
                    return Err(err(line, "unexpected character '!'"));
                }
            }
            b'<' => {
                if bytes.get(pos + 1) == Some(&b'=') {
                    push!(Tok::LtEq);
                    pos += 2;
                } else if bytes.get(pos + 1) == Some(&b'<') {
                    push!(Tok::LtLt);
                    pos += 2;
                } else {
                    push!(Tok::Lt);
                    pos += 1;
                }
            }
            b'>' => {
                if bytes.get(pos + 1) == Some(&b'=') {
                    push!(Tok::GtEq);
                    pos += 2;
                } else if bytes.get(pos + 1) == Some(&b'>') {
                    push!(Tok::GtGt);
                    pos += 2;
                } else {
                    push!(Tok::Gt);
                    pos += 1;
                }
            }
            b'\'' | b'"' => {
                let (s, new_pos) = lex_string(bytes, pos, line)?;
                push!(Tok::Str(s));
                pos = new_pos;
            }
            b'0'..=b'9' => {
                let (tok, new_pos) = lex_number(bytes, pos, line)?;
                push!(tok);
                pos = new_pos;
            }
            b'.' if bytes.get(pos + 1).is_some_and(|c| c.is_ascii_digit()) => {
                let (tok, new_pos) = lex_number(bytes, pos, line)?;
                push!(tok);
                pos = new_pos;
            }
            b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                let start = pos;
                while pos < bytes.len()
                    && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_')
                {
                    pos += 1;
                }
                let word = &src[start..pos];
                let tok = match word {
                    "return" => Tok::Return,
                    "if" => Tok::If,
                    "elif" => Tok::Elif,
                    "else" => Tok::Else,
                    "while" => Tok::While,
                    "for" => Tok::For,
                    "in" => Tok::In,
                    "break" => Tok::Break,
                    "continue" => Tok::Continue,
                    "pass" => Tok::Pass,
                    "and" => Tok::And,
                    "or" => Tok::Or,
                    "not" => Tok::Not,
                    "None" => Tok::NoneLit,
                    "True" => Tok::True,
                    "False" => Tok::False,
                    w if UNSUPPORTED_KEYWORDS.contains(&w) => {
                        return Err(err(
                            line,
                            format_args!("'{w}' is not supported in function bodies"),
                        ));
                    }
                    _ => Tok::Ident(word.to_owned()),
                };
                push!(tok);
            }
            _ => {
                let ch = src[pos..].chars().next().expect("pos is a char boundary");
                return Err(err(line, format_args!("unexpected character {ch:?}")));
            }
        }
    }

    if paren_depth > 0 {
        return Err(err(line, "unexpected end of input inside parentheses"));
    }
    if !matches!(
        toks.last(),
        Some(SpannedTok {
            tok: Tok::Newline,
            ..
        }) | None
    ) {
        toks.push(SpannedTok {
            tok: Tok::Newline,
            line,
        });
    }
    while indents.len() > 1 {
        indents.pop();
        toks.push(SpannedTok {
            tok: Tok::Dedent,
            line,
        });
    }
    toks.push(SpannedTok {
        tok: Tok::Eof,
        line,
    });
    Ok(toks)
}

/// Emit either `two` (when followed by `=`) or `one`; returns consumed length.
fn op(
    bytes: &[u8],
    pos: usize,
    toks: &mut Vec<SpannedTok>,
    line: usize,
    two: Tok,
    one: Tok,
) -> usize {
    if bytes.get(pos + 1) == Some(&b'=') {
        toks.push(SpannedTok { tok: two, line });
        2
    } else {
        toks.push(SpannedTok { tok: one, line });
        1
    }
}

/// Advance past `\n`, `\r` or `\r\n` starting at `pos`; returns the new
/// position (or `pos` unchanged if there is no line ending there).
fn skip_line_ending(bytes: &[u8], pos: usize) -> usize {
    match bytes.get(pos) {
        Some(b'\r') if bytes.get(pos + 1) == Some(&b'\n') => pos + 2,
        Some(b'\r') | Some(b'\n') => pos + 1,
        _ => pos,
    }
}

fn lex_string(bytes: &[u8], start: usize, line: usize) -> Result<(String, usize)> {
    let quote = bytes[start];
    if bytes.get(start + 1) == Some(&quote) && bytes.get(start + 2) == Some(&quote) {
        return Err(err(line, "triple-quoted strings are not supported"));
    }
    let mut out = String::new();
    let mut pos = start + 1;
    loop {
        match bytes.get(pos) {
            None | Some(b'\n') => return Err(err(line, "unterminated string literal")),
            Some(&b) if b == quote => return Ok((out, pos + 1)),
            Some(b'\\') => {
                let esc = bytes
                    .get(pos + 1)
                    .ok_or_else(|| err(line, "unterminated string literal"))?;
                match esc {
                    b'n' => out.push('\n'),
                    b't' => out.push('\t'),
                    b'r' => out.push('\r'),
                    b'\\' => out.push('\\'),
                    b'\'' => out.push('\''),
                    b'"' => out.push('"'),
                    _ => {
                        return Err(err(
                            line,
                            format_args!("unsupported escape sequence '\\{}'", *esc as char),
                        ))
                    }
                }
                pos += 2;
            }
            Some(_) => {
                // Copy the full UTF-8 character.
                let len = utf8_len(bytes[pos]);
                let chunk = bytes
                    .get(pos..pos + len)
                    .ok_or_else(|| err(line, "invalid UTF-8 in string literal"))?;
                out.push_str(
                    std::str::from_utf8(chunk)
                        .map_err(|_| err(line, "invalid UTF-8 in string literal"))?,
                );
                pos += len;
            }
        }
    }
}

const fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

fn lex_number(bytes: &[u8], start: usize, line: usize) -> Result<(Tok, usize)> {
    let mut pos = start;
    // Radix prefixes produce integers only.
    if bytes[pos] == b'0'
        && matches!(
            bytes.get(pos + 1),
            Some(b'x' | b'X' | b'o' | b'O' | b'b' | b'B')
        )
    {
        let radix = match bytes[pos + 1] {
            b'x' | b'X' => 16,
            b'o' | b'O' => 8,
            _ => 2,
        };
        pos += 2;
        let digits_start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_alphanumeric() {
            pos += 1;
        }
        let digits = std::str::from_utf8(&bytes[digits_start..pos]).expect("ascii digits");
        let value = i64::from_str_radix(digits, radix)
            .map_err(|e| err(line, format_args!("invalid integer literal: {e}")))?;
        return Ok((Tok::Int(value), pos));
    }
    let mut is_float = false;
    while pos < bytes.len() {
        match bytes[pos] {
            b'0'..=b'9' => pos += 1,
            b'.' if !is_float => {
                is_float = true;
                pos += 1;
            }
            b'e' | b'E' => {
                is_float = true;
                pos += 1;
                if matches!(bytes.get(pos), Some(b'+' | b'-')) {
                    pos += 1;
                }
            }
            _ => break,
        }
    }
    let text = std::str::from_utf8(&bytes[start..pos]).expect("ascii number");
    if is_float {
        let value: f64 = text
            .parse()
            .map_err(|e| err(line, format_args!("invalid float literal: {e}")))?;
        if !value.is_finite() {
            return Err(err(
                line,
                format_args!("float literal {text} is out of range"),
            ));
        }
        Ok((Tok::Float(value), pos))
    } else {
        let value: i64 = text.parse().map_err(|_| {
            err(
                line,
                format_args!("integer literal {text} does not fit in a 64-bit integer"),
            )
        })?;
        Ok((Tok::Int(value), pos))
    }
}
