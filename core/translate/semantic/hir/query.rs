//! Resolved queries, row sources, joins, and CTEs.

use turso_parser::ast::{CompoundOperator, Distinctness, Materialized, NullsOrder, SortOrder};

use super::{
    BoundColumnTypePrograms, ColumnRef, CteId, DatabaseId, Expr, OutputId, QueryBlockId, QueryId,
    ResolvedIndex, ResolvedTable, SourceId, TypeFact, WindowSpec,
};
use crate::vdbe::affinity::Affinity;

#[derive(Clone, Debug)]
pub struct Query {
    pub id: QueryId,
    /// Lexical query whose scope this query may capture. Root statement and
    /// uncorrelated CTE queries have no parent.
    pub parent: Option<QueryId>,
    /// Exact source identities read from outside this query's own blocks.
    /// Kept sorted by document-local identity for stable planning and tests.
    pub captures: Vec<SourceId>,
    pub reachable_ctes: Vec<CteId>,
    pub blocks: Vec<QueryBlock>,
    pub first: QueryBlockId,
    pub compounds: Vec<CompoundArm>,
    pub order_by: Vec<super::OrderTerm>,
    pub limit: Option<Limit>,
    pub output: Vec<OutputId>,
}

#[derive(Clone, Debug)]
pub struct CompoundArm {
    pub operator: CompoundOperator,
    pub block: QueryBlockId,
}

#[derive(Clone, Debug)]
pub struct QueryBlock {
    pub id: QueryBlockId,
    pub from: Option<From>,
    pub outputs: Vec<Output>,
    /// Number of stable aggregate identities owned by this block.
    pub aggregate_count: usize,
    /// Number of stable window-function identities owned by this block.
    pub window_function_count: usize,
    pub body: QueryBlockBody,
}

#[derive(Clone, Debug)]
pub enum QueryBlockBody {
    Select {
        distinctness: Option<Distinctness>,
        filter: Option<Expr>,
        grouping: Option<Grouping>,
        windows: Vec<NamedWindow>,
    },
    Values {
        rows: Vec<Vec<Expr>>,
    },
}

#[derive(Clone, Debug)]
pub struct Output {
    pub id: OutputId,
    pub name: String,
    pub expr: Expr,
    pub type_fact: TypeFact,
    pub affinity: Affinity,
    /// Affinity used when this result becomes a stored schema column.
    /// Compound queries preserve the leftmost arm here even when their
    /// runtime comparison affinity is merged across every arm.
    pub schema_affinity: Affinity,
    /// Whether SQLite comparison rules treat `affinity` as declared affinity.
    /// Literals and most computed expressions have no affinity even when their
    /// storage type is known; a real BLOB column does have BLOB affinity.
    pub has_affinity: bool,
    pub collation: Option<super::ResolvedCollation>,
    /// Whether `collation` came from an explicit COLLATE inside this output.
    /// This is needed when an output participates in a later comparison:
    /// explicit collations outrank declared column collations on either side.
    pub collation_is_explicit: bool,
    pub name_kind: OutputNameKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputNameKind {
    ExplicitAlias,
    StarExpansion,
    Inferred,
}

#[derive(Clone, Debug)]
pub struct Grouping {
    pub keys: Vec<Expr>,
    /// Type and custom-comparison facts aligned with `keys`.
    pub key_type_facts: Vec<TypeFact>,
    /// Resolved SQL collations aligned with `keys`.
    pub key_collations: Vec<Option<super::ResolvedCollation>>,
    pub having: Option<Expr>,
}

#[derive(Clone, Debug)]
pub struct NamedWindow {
    pub name: String,
    pub spec: WindowSpec,
}

#[derive(Clone, Debug)]
pub struct Limit {
    pub limit: Expr,
    pub offset: Option<Expr>,
}

/// A non-empty resolved FROM clause.
#[derive(Clone, Debug)]
pub struct From {
    pub first: SourceId,
    pub joins: Vec<Join>,
}

#[derive(Clone, Debug)]
pub struct Join {
    pub right: SourceId,
    pub kind: JoinKind,
    pub constraint: JoinConstraint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Comma,
    Inner,
    Cross,
    Left,
    Right,
    Full,
}

#[derive(Clone, Debug)]
pub enum JoinConstraint {
    None,
    On(Expr),
    Using(Vec<UsingColumn>),
    Natural(Vec<UsingColumn>),
}

/// The two source columns merged into one visible column by USING or NATURAL.
#[derive(Clone, Debug)]
pub struct UsingColumn {
    pub name: String,
    /// The visible value from the already-built left side. It may represent
    /// an earlier USING/NATURAL merge.
    pub left: Box<Expr>,
    pub right: ColumnRef,
    pub value: MergedColumnValue,
    pub type_fact: TypeFact,
    pub affinity: Affinity,
    pub has_affinity: bool,
    pub collation: Option<super::ResolvedCollation>,
    pub comparison: super::ComparisonSemantics,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergedColumnValue {
    Left,
    Right,
    Coalesce,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceOwner {
    QueryBlock(QueryBlockId),
    Cte(CteId),
    Root,
}

#[derive(Clone, Debug)]
pub struct CheckConstraint {
    /// Position in the frozen catalog table's CHECK list.
    pub catalog_position: usize,
    /// CHECK expression instantiated for one exact source occurrence.
    pub expression: Expr,
    /// Constraint name, or the stored expression rendered against its table
    /// columns when the constraint is unnamed.
    pub description: String,
}

/// Semantic state of a stored expression associated with one source column.
///
/// Keeping `Absent` separate from `NotRequired` preserves the distinction
/// between an ordinary column and a generated/default expression that this
/// statement does not need. Every required expression must be `Planned`
/// before the HIR document is finished.
#[derive(Clone, Debug)]
pub enum ColumnReadExpression {
    Absent,
    NotRequired,
    Planned(Expr),
}

#[derive(Clone, Debug)]
pub struct Source {
    pub id: SourceId,
    pub owner: SourceOwner,
    pub database: Option<DatabaseId>,
    pub name: String,
    pub alias: Option<String>,
    pub kind: SourceKind,
    pub columns: Vec<SourceColumn>,
    /// Virtual generated-column expressions for this exact source occurrence,
    /// aligned with `columns` and materialized when the statement needs them.
    pub generated_expressions: Vec<ColumnReadExpression>,
    /// Read-time defaults for short records, aligned with `columns` and
    /// materialized when the statement needs them.
    pub default_expressions: Vec<ColumnReadExpression>,
    /// Custom-type encode/decode programs aligned with `columns`. Plain and
    /// built-in typed columns contain `None`.
    pub column_type_programs: Vec<Option<BoundColumnTypePrograms>>,
    /// CHECK constraints selected and instantiated for this exact DML target.
    /// `None` means this source does not enforce CHECKs for the current
    /// statement; `Some([])` means enforcement is active but no catalog CHECK
    /// can be changed by this write.
    pub check_constraints: Option<Vec<CheckConstraint>>,
    pub rowid_available: bool,
    pub index_hint: IndexHint,
    /// Expression-index keys and partial predicates instantiated for this
    /// exact source occurrence.
    pub index_expressions: Vec<IndexExpressions>,
    /// Whether `index_expressions` covers every index in the frozen catalog or
    /// only the candidates usable by this read.
    pub index_coverage: IndexCoverage,
    /// Custom index-method match patterns resolved against this source.
    pub index_method_patterns: Vec<IndexMethodPattern>,
}

#[derive(Clone, Debug)]
pub enum SourceKind {
    /// Column namespace used while instantiating a stored schema expression.
    /// It has no catalog table or runtime cursor of its own; the consumer
    /// binds its columns to the row it is already processing.
    SchemaExpression,
    Table(ResolvedTable),
    TableFunction {
        table: ResolvedTable,
        arguments: Vec<Expr>,
    },
    Cte(CteId),
    Derived(QueryId),
    RecursiveInput(CteId),
    Pseudo {
        kind: PseudoSource,
        table: ResolvedTable,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PseudoSource {
    Excluded,
    New,
    Old,
}

#[derive(Clone, Debug)]
pub struct SourceColumn {
    pub name: String,
    pub type_fact: TypeFact,
    pub affinity: Affinity,
    pub has_affinity: bool,
    pub collation: Option<super::ResolvedCollation>,
    pub hidden: bool,
    pub rowid_alias: bool,
}

#[derive(Clone, Debug)]
pub enum IndexHint {
    None,
    NotIndexed,
    Indexed(ResolvedIndex),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexCoverage {
    Selective,
    Complete {
        indexes: Vec<super::CatalogObjectId>,
    },
}

#[derive(Clone, Debug)]
pub struct IndexExpressions {
    pub index: ResolvedIndex,
    pub columns: Vec<Option<Expr>>,
    pub predicate: Option<Expr>,
}

#[derive(Clone, Debug)]
pub struct IndexMethodPattern {
    pub id: super::IndexMethodPatternId,
    pub index: ResolvedIndex,
    pub outputs: Vec<Output>,
    pub predicate: Option<Expr>,
    pub order_by: Vec<super::OrderTerm>,
    pub limit: Option<Limit>,
}

#[derive(Clone, Debug)]
pub struct Cte {
    pub id: CteId,
    pub name: String,
    pub columns: Vec<CteColumn>,
    pub materialized: Materialized,
    pub body: CteBody,
}

#[derive(Clone, Debug)]
pub struct CteColumn {
    pub name: String,
    pub type_fact: TypeFact,
    pub affinity: Affinity,
    pub has_affinity: bool,
    pub collation: Option<super::ResolvedCollation>,
}

#[derive(Clone, Debug)]
pub enum CteBody {
    Query(QueryId),
    Recursive(RecursiveCte),
}

#[derive(Clone, Debug)]
pub struct RecursiveCte {
    pub seed: QueryId,
    pub arms: Vec<RecursiveArm>,
    /// One source per syntactic self-reference. Runtime recursion is keyed by
    /// the enclosing CteId; these IDs retain occurrence-local aliases/owners.
    pub input_sources: Vec<SourceId>,
    /// Effective collation for queue deduplication and UNION comparison. This
    /// follows compound left precedence across the seed and recursive arms;
    /// it is separate from the outward column collation of the seed.
    pub comparison_collations: Vec<Option<super::ResolvedCollation>>,
    pub queue_order: Vec<RecursiveOrderTerm>,
    pub limit: Option<Limit>,
}

/// One fully resolved priority key for the recursive work queue.
#[derive(Clone, Debug)]
pub struct RecursiveOrderTerm {
    pub output: usize,
    pub order: SortOrder,
    pub nulls: Option<NullsOrder>,
    pub explicit_collation: Option<super::ResolvedCollation>,
}

#[derive(Clone, Debug)]
pub struct RecursiveArm {
    pub operator: CompoundOperator,
    pub query: QueryId,
}
