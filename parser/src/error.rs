use crate::token::TokenType;

/// SQL lexer and parser errors
#[non_exhaustive]
#[derive(Clone, Debug, miette::Diagnostic, thiserror::Error)]
#[diagnostic()]
pub enum Error {
    /// Lexer error
    #[error("unrecognized token '{token_text}' at offset {offset}")]
    UnrecognizedToken {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    /// Missing quote or double-quote or backtick
    #[error("non-terminated literal '{token_text}' at offset {offset}")]
    UnterminatedLiteral {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    /// Missing `]`
    #[error("non-terminated bracket '{token_text}' at offset {offset}")]
    UnterminatedBracket {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    /// Missing `*/`
    #[error("non-terminated block comment '{token_text}' at offset {offset}")]
    UnterminatedBlockComment {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    /// Invalid parameter name
    #[error("bad variable name '{token_text}' at offset {offset}")]
    BadVariableName {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    /// Invalid number format
    #[error("bad number '{token_text}' at offset {offset}")]
    BadNumber {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    // Bad fractional part of a number
    #[error("bad fractional part '{token_text}' at offset {offset}")]
    BadFractionalPart {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    // Bad exponent part of a number
    #[error("bad exponent part '{token_text}' at offset {offset}")]
    BadExponentPart {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    /// Invalid or missing sign after `!`
    #[error("expected = sign '{token_text}' at offset {offset}")]
    ExpectedEqualsSign {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    /// Hexadecimal integer literals follow the C-language notation of "0x" or "0X" followed by hexadecimal digits.
    #[error("malformed hex integer '{token_text}' at offset {offset}")]
    MalformedHexInteger {
        #[label("here")]
        span: miette::SourceSpan,
        token_text: String,
        offset: usize,
    },
    // parse errors
    // Unexpected end of file
    #[error("unexpected end of file")]
    ParseUnexpectedEOF,
    // Unexpected token
    #[error("unexpected token '{token_text}' at offset {offset}")]
    #[diagnostic(help("expected {expected_display} but found '{token_text}'"))]
    ParseUnexpectedToken {
        #[label("here")]
        parsed_offset: miette::SourceSpan,

        got: TokenType,
        expected: &'static [TokenType],
        token_text: String,
        offset: usize,
        expected_display: String,
    },
    // Custom error message
    #[error("{0}")]
    Custom(String),
}
