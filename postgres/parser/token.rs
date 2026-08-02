//! PostgreSQL token types

use strum_macros::{Display, EnumString};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, EnumString)]
pub enum TokenType {
    // Literals
    IntegerLiteral,
    FloatLiteral,
    String,
    DollarQuotedString,
    Identifier,
    QuotedIdentifier,
    DollarParameter, // $1, $2, etc.

    // Keywords (PostgreSQL-specific and common SQL)
    Abort,
    Add,
    All,
    Alter,
    Analyze,
    And,
    Array,
    As,
    Attach,
    Asc,
    Begin,
    Between,
    Bigint,
    Bigserial,
    Boolean,
    Buffers,
    By,
    Cascade,
    Case,
    Cast,
    Check,
    Char,
    Checkpoint,
    Cluster,
    Collate,
    Column,
    Commit,
    Conflict,
    Constraint,
    Copy,
    Costs,
    Create,
    Cross,
    Csv,
    Current,
    Database,
    Date,
    Deallocate,
    Decimal,
    Default,
    Delete,
    Delimiter,
    Desc,
    Discard,
    Distinct,
    Do,
    Double,
    Drop,
    Else,
    End,
    Except,
    Exists,
    Explain,
    Extension,
    False,
    First,
    Float,
    Following,
    For,
    Foreign,
    Freeze,
    From,
    Full,
    Function,
    Grant,
    Group,
    Groups,
    Having,
    Header,
    If,
    Ilike,
    In,
    Index,
    Inet,
    Inner,
    Insert,
    Int,
    Integer,
    Intersect,
    Interval,
    Into,
    Is,
    Isolation,
    Isnull,
    Join,
    Json,
    Jsonb,
    Key,
    Last,
    Lateral,
    Left,
    Level,
    Like,
    Limit,
    Listen,
    Lock,
    Locked,
    Materialized,
    Natural,
    No,
    Normalize,
    Not,
    Nothing,
    Notify,
    Notnull,
    Nowait,
    Null,
    Nulls,
    Numeric,
    Of,
    Offset,
    On,
    Only,
    Or,
    Order,
    Ordinality,
    Outer,
    Over,
    Partition,
    Plans,
    Preceding,
    Precision,
    Prepare,
    Primary,
    Procedure,
    Range,
    Read,
    Real,
    Recursive,
    References,
    Reindex,
    Release,
    Reset,
    Returning,
    Revoke,
    Right,
    Role,
    Rollback,
    Rollup,
    Row,
    Rows,
    Savepoint,
    Schema,
    Select,
    Sequences,
    Serial,
    Session,
    Set,
    Share,
    Show,
    Similar,
    Skip,
    Smallint,
    Smallserial,
    Stdin,
    Stdout,
    System,
    Table,
    Tablespace,
    Tablesample,
    Temp,
    Temporary,
    Text,
    Then,
    Time,
    Timestamp,
    Timestamptz,
    To,
    Transaction,
    Trigger,
    True,
    Truncate,
    Type,
    Unbounded,
    Union,
    Unique,
    Update,
    User,
    Using,
    Uuid,
    Vacuum,
    Values,
    Varchar,
    Verbose,
    View,
    When,
    Where,
    Window,
    With,
    Write,
    Zone,

    // Operators
    Plus,          // +
    Minus,         // -
    Star,          // *
    Slash,         // /
    Percent,       // %
    Equal,         // =
    NotEqual,      // != or <>
    Less,          // <
    Greater,       // >
    LessEqual,     // <=
    GreaterEqual,  // >=
    Concat,        // ||
    TypeCast,      // ::
    Arrow,         // ->
    LongArrow,     // ->>
    Contains,      // @>
    ContainedBy,   // <@
    Overlap,       // &&
    Question,      // ?
    QuestionAnd,   // ?&
    QuestionPipe,  // ?|
    HashArrow,     // #>
    HashLongArrow, // #>>
    AtAt,          // @@ (text search)
    Tilde,         // ~ (regex match)
    TildeAny,      // ~* (case-insensitive regex)
    NotTilde,      // !~ (not regex match)
    NotTildeAny,   // !~* (case-insensitive not regex)

    // Delimiters
    LeftParen,    // (
    RightParen,   // )
    LeftBracket,  // [
    RightBracket, // ]
    LeftBrace,    // {
    RightBrace,   // }
    Comma,        // ,
    Dot,          // .
    Semicolon,    // ;
    Colon,        // :

    // Special
    Eof,
    Whitespace,
    Comment,
}

impl TokenType {
    /// Check if this token type is a keyword
    pub fn is_keyword(&self) -> bool {
        use TokenType::*;
        matches!(
            self,
            Abort
                | Add
                | All
                | Alter
                | Analyze
                | And
                | Array
                | As
                | Attach
                | Asc
                | Begin
                | Between
                | Bigint
                | Bigserial
                | Boolean
                | Buffers
                | By
                | Cascade
                | Case
                | Cast
                | Check
                | Char
                | Checkpoint
                | Cluster
                | Collate
                | Column
                | Commit
                | Conflict
                | Constraint
                | Copy
                | Costs
                | Create
                | Cross
                | Csv
                | Current
                | Database
                | Date
                | Deallocate
                | Decimal
                | Default
                | Delete
                | Delimiter
                | Desc
                | Discard
                | Distinct
                | Do
                | Double
                | Drop
                | Else
                | End
                | Except
                | Exists
                | Explain
                | Extension
                | False
                | First
                | Float
                | Following
                | For
                | Foreign
                | Freeze
                | From
                | Full
                | Function
                | Grant
                | Group
                | Groups
                | Having
                | Header
                | If
                | Ilike
                | In
                | Index
                | Inet
                | Inner
                | Insert
                | Int
                | Integer
                | Intersect
                | Interval
                | Into
                | Is
                | Isolation
                | Isnull
                | Join
                | Json
                | Jsonb
                | Key
                | Last
                | Lateral
                | Left
                | Level
                | Like
                | Limit
                | Listen
                | Lock
                | Locked
                | Materialized
                | Natural
                | No
                | Normalize
                | Not
                | Nothing
                | Notify
                | Notnull
                | Nowait
                | Null
                | Nulls
                | Numeric
                | Of
                | Offset
                | On
                | Only
                | Or
                | Order
                | Ordinality
                | Outer
                | Over
                | Partition
                | Plans
                | Preceding
                | Precision
                | Prepare
                | Primary
                | Procedure
                | Range
                | Read
                | Real
                | Recursive
                | References
                | Reindex
                | Release
                | Reset
                | Returning
                | Revoke
                | Right
                | Role
                | Rollback
                | Rollup
                | Row
                | Rows
                | Savepoint
                | Schema
                | Select
                | Sequences
                | Serial
                | Session
                | Set
                | Share
                | Show
                | Similar
                | Skip
                | Smallint
                | Smallserial
                | Stdin
                | Stdout
                | System
                | Table
                | Tablespace
                | Tablesample
                | Temp
                | Temporary
                | Text
                | Then
                | Time
                | Timestamp
                | Timestamptz
                | To
                | Transaction
                | Trigger
                | True
                | Truncate
                | Type
                | Unbounded
                | Union
                | Unique
                | Update
                | User
                | Using
                | Uuid
                | Vacuum
                | Values
                | Varchar
                | Verbose
                | View
                | When
                | Where
                | Window
                | With
                | Write
                | Zone
        )
    }

    /// Check if this token can be used as an identifier without quotes
    pub fn can_be_identifier(&self) -> bool {
        // PostgreSQL allows many keywords as identifiers in non-reserved contexts
        // Reserved keywords that cannot be used as identifiers
        !matches!(
            self,
            TokenType::Select
                | TokenType::From
                | TokenType::Where
                | TokenType::And
                | TokenType::Or
                | TokenType::Not
                | TokenType::Insert
                | TokenType::Update
                | TokenType::Delete
                | TokenType::Create
                | TokenType::Drop
                | TokenType::Alter
                | TokenType::Into
                | TokenType::Values
                | TokenType::Set
                | TokenType::Join
                | TokenType::On
                | TokenType::As
                | TokenType::With
                | TokenType::Union
                | TokenType::Except
                | TokenType::Intersect
                | TokenType::Order
                | TokenType::By
                | TokenType::Group
                | TokenType::Having
                | TokenType::Distinct
                | TokenType::All
                | TokenType::Case
                | TokenType::When
                | TokenType::Then
                | TokenType::Else
                | TokenType::End
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub token_type: TokenType,
    pub value: String,
    pub position: usize,
}

impl Token {
    pub fn new(token_type: TokenType, value: String, position: usize) -> Self {
        Token {
            token_type,
            value,
            position,
        }
    }
}
