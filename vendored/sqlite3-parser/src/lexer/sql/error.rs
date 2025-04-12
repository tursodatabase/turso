use std::error;
use std::fmt;
use std::io;

use crate::lexer::scan::ScanError;
use crate::parser::ParserError;

/// SQL lexer and parser errors
#[non_exhaustive]
#[derive(Debug, miette::Diagnostic)]
#[diagnostic()]
pub enum Error {
    /// I/O Error
    Io(io::Error),
    /// Lexer error
    UnrecognizedToken(
        Option<(u64, usize)>,
        #[label("here")] Option<miette::SourceSpan>,
    ),
    /// Missing quote or double-quote or backtick
    UnterminatedLiteral(
        Option<(u64, usize)>,
        #[label("here")] Option<miette::SourceSpan>,
    ),
    /// Missing `]`
    UnterminatedBracket(
        Option<(u64, usize)>,
        #[label("here")] Option<miette::SourceSpan>,
    ),
    /// Missing `*/`
    UnterminatedBlockComment(
        Option<(u64, usize)>,
        #[label("here")] Option<miette::SourceSpan>,
    ),
    /// Invalid parameter name
    BadVariableName(
        Option<(u64, usize)>,
        #[label("here")] Option<miette::SourceSpan>,
    ),
    /// Invalid number format
    #[diagnostic(help("Invalid digit in `{3}`"))]
    BadNumber(
        Option<(u64, usize)>,
        #[label("here")] Option<miette::SourceSpan>,
        Option<usize>,
        String, // Holds the offending number as a string
    ),
    /// Invalid or missing sign after `!`
    ExpectedEqualsSign(
        Option<(u64, usize)>,
        #[label("here")] Option<miette::SourceSpan>,
    ),
    /// BLOB literals are string literals containing hexadecimal data and preceded by a single "x" or "X" character.
    MalformedBlobLiteral(
        Option<(u64, usize)>,
        #[label("here")] Option<miette::SourceSpan>,
    ),
    /// Hexadecimal integer literals follow the C-language notation of "0x" or "0X" followed by hexadecimal digits.
    MalformedHexInteger(
        Option<(u64, usize)>,
        #[label("here")] Option<miette::SourceSpan>,
        Option<usize>,
        #[help] Option<&'static str>,
    ),
    /// Grammar error
    ParserError(
        ParserError,
        Option<(u64, usize)>,
        #[label("syntax error")] Option<miette::SourceSpan>,
    ),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Io(ref err) => err.fmt(f),
            Self::UnrecognizedToken(pos, _) => {
                write!(f, "unrecognized token at {:?}", pos.unwrap())
            }
            Self::UnterminatedLiteral(pos, _) => {
                write!(f, "non-terminated literal at {:?}", pos.unwrap())
            }
            Self::UnterminatedBracket(pos, _) => {
                write!(f, "non-terminated bracket at {:?}", pos.unwrap())
            }
            Self::UnterminatedBlockComment(pos, _) => {
                write!(f, "non-terminated block comment at {:?}", pos.unwrap())
            }
            Self::BadVariableName(pos, _) => write!(f, "bad variable name at {:?}", pos.unwrap()),
            Self::BadNumber(pos, _, _, _) => write!(f, "bad number at {:?}", pos.unwrap()),
            Self::ExpectedEqualsSign(pos, _) => write!(f, "expected = sign at {:?}", pos.unwrap()),
            Self::MalformedBlobLiteral(pos, _) => {
                write!(f, "malformed blob literal at {:?}", pos.unwrap())
            }
            Self::MalformedHexInteger(pos, _, _, _) => {
                write!(f, "malformed hex integer at {:?}", pos.unwrap())
            }
            Self::ParserError(ref msg, Some(pos), _) => write!(f, "{msg} at {pos:?}"),
            Self::ParserError(ref msg, _, _) => write!(f, "{msg}"),
        }
    }
}

impl error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<ParserError> for Error {
    fn from(err: ParserError) -> Self {
        Self::ParserError(err, None, None)
    }
}

impl ScanError for Error {
    fn position(&mut self, line: u64, column: usize, offset: usize) {
        match *self {
            Self::Io(_) => {}
            Self::UnrecognizedToken(ref mut pos, ref mut src) => {
                *pos = Some((line, column));
                *src = Some((offset).into());
            }
            Self::UnterminatedLiteral(ref mut pos, ref mut src) => {
                *pos = Some((line, column));
                *src = Some((offset).into());
            }
            Self::UnterminatedBracket(ref mut pos, ref mut src) => {
                *pos = Some((line, column));
                *src = Some((offset).into());
            }
            Self::UnterminatedBlockComment(ref mut pos, ref mut src) => {
                *pos = Some((line, column));
                *src = Some((offset).into());
            }
            Self::BadVariableName(ref mut pos, ref mut src) => {
                *pos = Some((line, column));
                *src = Some((offset).into());
            }
            Self::ExpectedEqualsSign(ref mut pos, ref mut src) => {
                *pos = Some((line, column));
                *src = Some((offset).into());
            }
            Self::MalformedBlobLiteral(ref mut pos, ref mut src) => {
                *pos = Some((line, column));
                *src = Some((offset).into());
            }
            // Exact same handling here
            Self::MalformedHexInteger(ref mut pos, ref mut src, len, _)
            | Self::BadNumber(ref mut pos, ref mut src, len, _) => {
                *pos = Some((line, column));
                *src = Some((offset, len.unwrap_or(0)).into());
            }
            Self::ParserError(_, ref mut pos, _) => *pos = Some((line, column)),
        }
    }
}
