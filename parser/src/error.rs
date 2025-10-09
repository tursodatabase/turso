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

#[non_exhaustive]
#[derive(Clone, Debug, miette::Diagnostic, thiserror::Error)]
#[diagnostic()]
pub enum ParseError {
    /// No such view in drop_view
    #[error("no such view: {0:?}")]
    NoSuchView(String),
    /// Duplicate view in create_view
    #[error("View {0:?} already exists")]
    DuplicateView(String),
    /// Table has more than one PRIMARY KEY
    #[error("table \"{0}\" has more than one primary key")]
    MultiplePrimaryKeys(String),
    /// Column not found within table
    #[error("Column {0} not found in table {1}")]
    ColumnNotFoundInTable(String, String),
    /// Reused error for malformed JSON inputs
    #[error("malformed JSON")]
    MalformedJson,
    /// Reused error for JSON string serialization failures
    #[error("Failed to serialize string!")]
    FailedToSerializeString,
    /// Reused error for missing node lookups
    #[error("Node not found")]
    NodeNotFound,
    /// Failed to parse a virtual table module schema
    #[error("Failed to parse schema from virtual table module")]
    VirtualTableSchemaParseFailed,
    /// Missing join condition column in either input
    #[error("Join condition column '{0}' not found in either input")]
    JoinConditionColumnNotFound(String),
    /// Incremental circuit has no root node
    #[error("Circuit has no root node")]
    CircuitHasNoRootNode,
    /// Only column references are supported in a given feature for incremental views
    #[error("Only column references are supported in {0} for incremental views")]
    MatviewOnlyColumnReferencesSupported(String),
    /// <feature> column '{column}' not found in input
    #[error("{0} column '{1}' not found in input")]
    FeatureColumnNotFoundInInput(String, String),
    /// Operation requires an argument
    #[error("{0} requires an argument")]
    OperationRequiresArgument(String),
    /// Table not found
    #[error("table not found")]
    TableNotFound,
    /// Column not found
    #[error("column not found")]
    ColumnNotFound,
    // Custom error message
    #[error("{0}")]
    Custom(String),
}
