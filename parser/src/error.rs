use crate::token::TokenType;

/// SQL lexer and parser errors
#[non_exhaustive]
#[derive(Clone, Debug, miette::Diagnostic, thiserror::Error)]
#[diagnostic()]
pub enum Error {
    /// Lexer error
    #[error("unrecognized token at {0:?}")]
    UnrecognizedToken(#[label("here")] miette::SourceSpan),
    /// Missing quote or double-quote or backtick
    #[error("non-terminated literal at {0:?}")]
    UnterminatedLiteral(#[label("here")] miette::SourceSpan),
    /// Missing `]`
    #[error("non-terminated bracket at {0:?}")]
    UnterminatedBracket(#[label("here")] miette::SourceSpan),
    /// Invalid parameter name
    #[error("bad variable name at {0:?}")]
    BadVariableName(#[label("here")] miette::SourceSpan),
    /// Invalid number format
    #[error("bad number at {0:?}")]
    BadNumber(#[label("here")] miette::SourceSpan),
    // Bad fractional part of a number
    #[error("bad fractional part at {0:?}")]
    BadFractionalPart(#[label("here")] miette::SourceSpan),
    // Bad exponent part of a number
    #[error("bad exponent part at {0:?}")]
    BadExponentPart(#[label("here")] miette::SourceSpan),
    /// Invalid or missing sign after `!`
    #[error("expected = sign at {0:?}")]
    ExpectedEqualsSign(#[label("here")] miette::SourceSpan),
    /// Hexadecimal integer literals follow the C-language notation of "0x" or "0X" followed by hexadecimal digits.
    #[error("malformed hex integer at {0:?}")]
    MalformedHexInteger(#[label("here")] miette::SourceSpan),
    // parse errors
    // Unexpected end of file
    #[error("unexpected end of file")]
    ParseUnexpectedEOF,
    // Unexpected token
    #[error("unexpected token at {parsed_offset:?}")]
    #[diagnostic(help("expected {expected:?} but found {got:?}"))]
    ParseUnexpectedToken {
        #[label("here")]
        parsed_offset: miette::SourceSpan,

        got: TokenType,
        expected: &'static [TokenType],
    },
    // Custom error message
    #[error("{0}")]
    Custom(String),
}
