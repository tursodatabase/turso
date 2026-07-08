//! PostgreSQL AST definitions
//!
//! This module defines the Abstract Syntax Tree for PostgreSQL SQL.
//! Where possible, we reuse or mirror Turso's AST structure for compatibility.

use std::fmt;

/// A SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Select(Box<SelectStmt>),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    CreateIndex(CreateIndexStmt),
    AlterTable(AlterTableStmt),
    DropTable(DropTableStmt),
    Begin(BeginStmt),
    Commit,
    Rollback,

    // Administrative commands
    Explain(ExplainStmt),
    Copy(CopyStmt),
    Set(SetStmt),
    Show(ShowStmt),
    Reset(ResetStmt),
    Analyze(AnalyzeStmt),
    Vacuum(VacuumStmt),
    Checkpoint,
    Discard(DiscardStmt),
    Deallocate(DeallocateStmt),

    // Additional DDL
    CreateSchema(CreateSchemaStmt),
    CreateSequence(CreateSequenceStmt),
    CreateView(CreateViewStmt),
    CreateFunction(CreateFunctionStmt),
    CreateTrigger(CreateTriggerStmt),
    CreateType(CreateTypeStmt),
    CreateExtension(CreateExtensionStmt),
    DropSchema(DropSchemaStmt),
    DropSequence(DropSequenceStmt),
    DropView(DropViewStmt),
    DropFunction(DropFunctionStmt),
    DropTrigger(DropTriggerStmt),
    DropType(DropTypeStmt),
    DropExtension(DropExtensionStmt),

    // Session/Transaction
    Savepoint(SavepointStmt),
    ReleaseSavepoint(ReleaseSavepointStmt),
    RollbackToSavepoint(RollbackToSavepointStmt),
    Prepare(PrepareStmt),
    Execute(ExecuteStmt),
    Listen(ListenStmt),
    Notify(NotifyStmt),
    Lock(LockStmt),
    Grant(GrantStmt),
    Revoke(RevokeStmt),

    // DML Extensions
    Merge(MergeStmt),
    Truncate(TruncateStmt),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub distinct: Option<Distinct>,
    pub columns: Vec<SelectColumn>,
    pub from: Option<Vec<TableRef>>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Expr>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub for_clause: Option<ForClause>, // PostgreSQL FOR UPDATE/SHARE
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table: TableRef,
    pub columns: Option<Vec<ColumnSpec>>,
    pub values: InsertValues,
    pub on_conflict: Option<OnConflict>, // PostgreSQL ON CONFLICT
    pub returning: Option<Vec<SelectColumn>>, // PostgreSQL RETURNING
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table: TableRef,
    pub set: Vec<Assignment>,
    pub from: Option<Vec<TableRef>>, // PostgreSQL FROM clause
    pub where_clause: Option<Expr>,
    pub returning: Option<Vec<SelectColumn>>, // PostgreSQL RETURNING
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub table: TableRef,
    pub using: Option<Vec<TableRef>>, // PostgreSQL USING clause
    pub where_clause: Option<Expr>,
    pub returning: Option<Vec<SelectColumn>>, // PostgreSQL RETURNING
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub temporary: bool,
    pub if_not_exists: bool,
    pub table: TableRef,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub as_select: Option<Box<SelectStmt>>,
    pub partition_of: Option<PartitionSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionSpec {
    pub parent_table: TableRef,
    pub partition_bound: PartitionBound,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionBound {
    /// FOR VALUES IN (value1, value2, ...)
    In(Vec<Expr>),
    /// FOR VALUES FROM (start_values...) TO (end_values...)
    Range { from: Vec<Expr>, to: Vec<Expr> },
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
    pub unique: bool,
    pub if_not_exists: bool,
    pub name: String,
    pub table: TableRef,
    pub columns: Vec<IndexColumn>,
    pub where_clause: Option<Expr>,
}

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStmt {
    pub table: TableRef,
    pub action: AlterTableAction,
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    pub if_exists: bool,
    pub tables: Vec<TableRef>,
    pub cascade: bool,
}

/// BEGIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct BeginStmt {
    pub isolation_level: Option<IsolationLevel>,
    pub read_write: Option<bool>,
}

// Helper types

#[derive(Debug, Clone, PartialEq)]
pub enum Distinct {
    All,
    On(Vec<Expr>), // PostgreSQL DISTINCT ON
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectColumn {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub schema: Option<String>,
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: Option<bool>, // PostgreSQL NULLS FIRST/LAST
}

#[derive(Debug, Clone, PartialEq)]
pub enum ForClause {
    Update,
    NoKeyUpdate,
    Share,
    KeyShare,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertValues {
    Values(Vec<Vec<Expr>>),
    Select(Box<SelectStmt>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict {
    pub target: Option<OnConflictTarget>,
    pub action: OnConflictAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictTarget {
    Columns(Vec<String>),
    Constraint(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate(Vec<Assignment>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentTarget {
    /// Simple column assignment: SET col = value
    Column(String),
    /// Tuple assignment: SET (col1, col2) = (val1, val2)
    Tuple(Vec<String>),
    /// Array element assignment: SET col[index] = value
    ArrayElement { column: String, index: Expr },
    /// Field assignment: SET col.field = value
    Field { column: String, field: String },
    /// Complex nested assignment: SET col[idx].field = value
    Complex { column: String, accessors: Vec<FieldAccessor> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldAccessor {
    ArrayIndex(Expr),
    Field(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub target: AssignmentTarget,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnSpec {
    /// Simple column: col
    Simple(String),
    /// Array element: col[index]
    ArrayElement { column: String, index: Expr },
    /// Field access: col.field
    Field { column: String, field: String },
    /// Complex access: col[idx].field or nested combinations
    Complex { column: String, accessors: Vec<FieldAccessor> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    NotNull,
    Null,
    PrimaryKey,
    Unique,
    References(TableRef, Option<String>),
    Check(Expr),
    Default(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        references: TableRef,
        ref_columns: Vec<String>,
    },
    Check(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    AddColumn(ColumnDef),
    DropColumn(String),
    AlterColumn(String, AlterColumnAction),
    AddConstraint(TableConstraint),
    DropConstraint(String),
    AttachPartition {
        partition_table: TableRef,
        partition_bound: PartitionBound
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumnAction {
    SetType(DataType),
    SetDefault(Expr),
    DropDefault,
    SetNotNull,
    DropNotNull,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// PostgreSQL data types
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    // Numeric types
    SmallInt,
    Integer,
    BigInt,
    Decimal(Option<u32>, Option<u32>),
    Numeric(Option<u32>, Option<u32>),
    Real,
    DoublePrecision,
    SmallSerial,
    Serial,
    BigSerial,

    // Character types
    Varchar(Option<u32>),
    Char(Option<u32>),
    Text,

    // Binary
    Bytea,

    // Date/Time
    Date,
    Time(Option<u32>),
    TimeTz(Option<u32>),
    Timestamp(Option<u32>),
    TimestampTz(Option<u32>),
    Interval,

    // Boolean
    Boolean,

    // Network
    Inet,
    Cidr,
    MacAddr,

    // UUID
    Uuid,

    // JSON
    Json,
    Jsonb,

    // Array
    Array(Box<DataType>),

    // Custom/User-defined
    Custom(String),
}

/// Expressions
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    // Literals
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,

    // Identifiers
    Identifier(String),
    QualifiedIdentifier(Vec<String>), // schema.table.column

    // Parameters
    DollarParameter(u32), // $1, $2, etc.
    QuestionParameter,    // ?

    // Binary operations
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },

    // Unary operations
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },

    // PostgreSQL type cast
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },

    // PostgreSQL :: type cast
    TypeCast {
        expr: Box<Expr>,
        type_name: String,
    },

    // PostgreSQL INTERVAL literal
    Interval {
        value: Box<Expr>,
        unit: Option<String>,
    },

    // Array
    Array(Vec<Expr>),
    // ROW constructor
    Row(Vec<Expr>),
    ArraySubscript {
        array: Box<Expr>,
        index: Box<Expr>,
    },
    ArraySlice {
        array: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },

    // JSON operators
    JsonAccess {
        expr: Box<Expr>,
        op: JsonOperator,
        path: Box<Expr>,
    },

    // Function call
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
        order_by: Option<Vec<OrderByExpr>>,
        filter: Option<Box<Expr>>,
        over: Option<WindowSpec>,
    },

    // Subquery
    Subquery(Box<SelectStmt>),

    // EXISTS
    Exists(Box<SelectStmt>),

    // IN
    In {
        expr: Box<Expr>,
        list: InList,
        negated: bool,
    },

    // BETWEEN
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    // CASE
    Case {
        expr: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<Expr>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Power,

    // Comparison
    Equal,
    NotEqual,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,

    // Logical
    And,
    Or,

    // String
    Concat,
    Like,
    Ilike,
    Similar,
    NotLike,
    NotIlike,
    NotSimilar,

    // Regex
    RegexMatch,
    RegexMatchCI,
    NotRegexMatch,
    NotRegexMatchCI,

    // Array/JSON
    Contains,
    ContainedBy,
    Overlap,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonOperator {
    Arrow,         // ->
    LongArrow,     // ->>
    HashArrow,     // #>
    HashLongArrow, // #>>
}

#[derive(Debug, Clone, PartialEq)]
pub enum InList {
    Values(Vec<Expr>),
    Subquery(Box<SelectStmt>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Option<Vec<Expr>>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub mode: WindowFrameMode,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameMode {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(Box<Expr>),
    CurrentRow,
    Following(Box<Expr>),
    UnboundedFollowing,
}

// Administrative command AST nodes

/// EXPLAIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStmt {
    pub analyze: bool,
    pub verbose: bool,
    pub costs: Option<bool>,
    pub buffers: bool,
    pub statement: Box<Stmt>,
}

/// COPY statement
#[derive(Debug, Clone, PartialEq)]
pub struct CopyStmt {
    pub source: CopySource,
    pub direction: CopyDirection,
    pub target: CopyTarget,
    pub options: Vec<CopyOption>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopySource {
    Table { table: TableRef, columns: Option<Vec<String>> },
    Query(Box<SelectStmt>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyDirection {
    From,
    To,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyTarget {
    File(String),
    Stdin,
    Stdout,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyOption {
    Delimiter(String),
    Csv,
    Header,
}

/// SET statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetStmt {
    pub parameter: String,
    pub value: SetValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetValue {
    String(String),
    Number(f64),
    Identifier(String),
    Default,
}

/// SHOW statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStmt {
    pub parameter: ShowParameter,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShowParameter {
    All,
    Specific(String),
}

/// RESET statement
#[derive(Debug, Clone, PartialEq)]
pub struct ResetStmt {
    pub parameter: Option<String>, // None means RESET ALL
}

/// ANALYZE statement
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeStmt {
    pub verbose: bool,
    pub tables: Option<Vec<AnalyzeTable>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeTable {
    pub table: TableRef,
    pub columns: Option<Vec<String>>,
}

/// VACUUM statement
#[derive(Debug, Clone, PartialEq)]
pub struct VacuumStmt {
    pub full: bool,
    pub freeze: bool,
    pub verbose: bool,
    pub analyze: bool,
    pub tables: Option<Vec<TableRef>>,
}

/// DISCARD statement
#[derive(Debug, Clone, PartialEq)]
pub struct DiscardStmt {
    pub target: DiscardTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DiscardTarget {
    All,
    Plans,
    Sequences,
    Temp,
}

/// DEALLOCATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeallocateStmt {
    pub name: Option<String>, // None means DEALLOCATE ALL
}

// Additional DDL AST nodes

/// CREATE SCHEMA statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchemaStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub authorization: Option<String>,
}

/// CREATE SEQUENCE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSequenceStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub options: Vec<SequenceOption>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SequenceOption {
    IncrementBy(i64),
    MinValue(i64),
    MaxValue(i64),
    StartWith(i64),
    Cache(i64),
    Cycle,
    NoCycle,
}

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt {
    pub or_replace: bool,
    pub materialized: bool,
    pub if_not_exists: bool,
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<SelectStmt>,
}

/// CREATE FUNCTION statement (simplified)
#[derive(Debug, Clone, PartialEq)]
pub struct CreateFunctionStmt {
    pub or_replace: bool,
    pub name: String,
    pub parameters: Vec<FunctionParameter>,
    pub returns: DataType,
    pub language: String,
    pub body: String, // Function body as string for now
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionParameter {
    pub name: Option<String>,
    pub data_type: DataType,
    pub mode: Option<ParameterMode>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParameterMode {
    In,
    Out,
    InOut,
    Variadic,
}

/// CREATE TRIGGER statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTriggerStmt {
    pub name: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub table: TableRef,
    pub function_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEvent {
    Insert,
    Update(Option<Vec<String>>), // column list
    Delete,
    Truncate,
}

/// CREATE TYPE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTypeStmt {
    pub name: String,
    pub definition: TypeDefinition,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TypeDefinition {
    Composite(Vec<ColumnDef>),
    Enum(Vec<String>),
    Range(RangeTypeDefinition),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeTypeDefinition {
    pub subtype: DataType,
    pub subtype_opclass: Option<String>,
    pub collation: Option<String>,
}

/// CREATE EXTENSION statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateExtensionStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub schema: Option<String>,
    pub version: Option<String>,
}

/// DROP statements (simplified)
#[derive(Debug, Clone, PartialEq)]
pub struct DropSchemaStmt {
    pub if_exists: bool,
    pub names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropSequenceStmt {
    pub if_exists: bool,
    pub names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStmt {
    pub if_exists: bool,
    pub names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropFunctionStmt {
    pub if_exists: bool,
    pub signatures: Vec<FunctionSignature>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionSignature {
    pub name: String,
    pub parameters: Vec<DataType>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTriggerStmt {
    pub if_exists: bool,
    pub name: String,
    pub table: TableRef,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTypeStmt {
    pub if_exists: bool,
    pub names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropExtensionStmt {
    pub if_exists: bool,
    pub names: Vec<String>,
    pub cascade: bool,
}

// Session/Transaction AST nodes

/// SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SavepointStmt {
    pub name: String,
}

/// RELEASE SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseSavepointStmt {
    pub name: String,
}

/// ROLLBACK TO SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackToSavepointStmt {
    pub name: String,
}

/// PREPARE statement
#[derive(Debug, Clone, PartialEq)]
pub struct PrepareStmt {
    pub name: String,
    pub parameter_types: Option<Vec<DataType>>,
    pub statement: Box<Stmt>,
}

/// EXECUTE statement
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteStmt {
    pub name: String,
    pub parameters: Option<Vec<Expr>>,
}

/// LISTEN statement
#[derive(Debug, Clone, PartialEq)]
pub struct ListenStmt {
    pub channel: String,
}

/// NOTIFY statement
#[derive(Debug, Clone, PartialEq)]
pub struct NotifyStmt {
    pub channel: String,
    pub payload: Option<String>,
}

/// LOCK statement
#[derive(Debug, Clone, PartialEq)]
pub struct LockStmt {
    pub tables: Vec<TableRef>,
    pub mode: LockMode,
    pub nowait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    ShareUpdateExclusive,
    Share,
    ShareRowExclusive,
    Exclusive,
    AccessExclusive,
}

/// GRANT statement (simplified)
#[derive(Debug, Clone, PartialEq)]
pub struct GrantStmt {
    pub privileges: Vec<Privilege>,
    pub objects: Vec<String>,
    pub grantees: Vec<String>,
    pub with_grant_option: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
    References,
    Trigger,
    Create,
    Connect,
    Temporary,
    Execute,
    Usage,
    All,
}

/// REVOKE statement (simplified)
#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStmt {
    pub privileges: Vec<Privilege>,
    pub objects: Vec<String>,
    pub grantees: Vec<String>,
    pub cascade: bool,
}

// DML Extensions

/// MERGE statement (PostgreSQL 15+)
#[derive(Debug, Clone, PartialEq)]
pub struct MergeStmt {
    pub target_table: TableRef,
    pub source: MergeSource,
    pub join_condition: Expr,
    pub when_clauses: Vec<MergeWhenClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeSource {
    Table(TableRef),
    Query(Box<SelectStmt>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeWhenClause {
    WhenMatched {
        condition: Option<Expr>,
        action: MergeAction,
    },
    WhenNotMatched {
        condition: Option<Expr>,
        action: MergeAction,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeAction {
    Update(Vec<Assignment>),
    Delete,
    Insert {
        columns: Option<Vec<String>>,
        values: Vec<Expr>,
    },
    DoNothing,
}

/// TRUNCATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateStmt {
    pub tables: Vec<TableRef>,
    pub restart_identity: bool,
    pub cascade: bool,
}

// Display implementations for debugging
impl fmt::Display for Stmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Stmt::Select(_) => write!(f, "SELECT"),
            Stmt::Insert(_) => write!(f, "INSERT"),
            Stmt::Update(_) => write!(f, "UPDATE"),
            Stmt::Delete(_) => write!(f, "DELETE"),
            Stmt::CreateTable(_) => write!(f, "CREATE TABLE"),
            Stmt::CreateIndex(_) => write!(f, "CREATE INDEX"),
            Stmt::AlterTable(_) => write!(f, "ALTER TABLE"),
            Stmt::DropTable(_) => write!(f, "DROP TABLE"),
            Stmt::Begin(_) => write!(f, "BEGIN"),
            Stmt::Commit => write!(f, "COMMIT"),
            Stmt::Rollback => write!(f, "ROLLBACK"),
            Stmt::Explain(_) => write!(f, "EXPLAIN"),
            Stmt::Copy(_) => write!(f, "COPY"),
            Stmt::Set(_) => write!(f, "SET"),
            Stmt::Show(_) => write!(f, "SHOW"),
            Stmt::Reset(_) => write!(f, "RESET"),
            Stmt::Analyze(_) => write!(f, "ANALYZE"),
            Stmt::Vacuum(_) => write!(f, "VACUUM"),
            Stmt::Checkpoint => write!(f, "CHECKPOINT"),
            Stmt::Discard(_) => write!(f, "DISCARD"),
            Stmt::Deallocate(_) => write!(f, "DEALLOCATE"),
            Stmt::CreateSchema(_) => write!(f, "CREATE SCHEMA"),
            Stmt::CreateSequence(_) => write!(f, "CREATE SEQUENCE"),
            Stmt::CreateView(_) => write!(f, "CREATE VIEW"),
            Stmt::CreateFunction(_) => write!(f, "CREATE FUNCTION"),
            Stmt::CreateTrigger(_) => write!(f, "CREATE TRIGGER"),
            Stmt::CreateType(_) => write!(f, "CREATE TYPE"),
            Stmt::CreateExtension(_) => write!(f, "CREATE EXTENSION"),
            Stmt::DropSchema(_) => write!(f, "DROP SCHEMA"),
            Stmt::DropSequence(_) => write!(f, "DROP SEQUENCE"),
            Stmt::DropView(_) => write!(f, "DROP VIEW"),
            Stmt::DropFunction(_) => write!(f, "DROP FUNCTION"),
            Stmt::DropTrigger(_) => write!(f, "DROP TRIGGER"),
            Stmt::DropType(_) => write!(f, "DROP TYPE"),
            Stmt::DropExtension(_) => write!(f, "DROP EXTENSION"),
            Stmt::Savepoint(_) => write!(f, "SAVEPOINT"),
            Stmt::ReleaseSavepoint(_) => write!(f, "RELEASE SAVEPOINT"),
            Stmt::RollbackToSavepoint(_) => write!(f, "ROLLBACK TO SAVEPOINT"),
            Stmt::Prepare(_) => write!(f, "PREPARE"),
            Stmt::Execute(_) => write!(f, "EXECUTE"),
            Stmt::Listen(_) => write!(f, "LISTEN"),
            Stmt::Notify(_) => write!(f, "NOTIFY"),
            Stmt::Lock(_) => write!(f, "LOCK"),
            Stmt::Grant(_) => write!(f, "GRANT"),
            Stmt::Revoke(_) => write!(f, "REVOKE"),
            Stmt::Merge(_) => write!(f, "MERGE"),
            Stmt::Truncate(_) => write!(f, "TRUNCATE"),
        }
    }
}
