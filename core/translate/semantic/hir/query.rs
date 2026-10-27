//! Resolved queries, row sources, joins, and CTEs.

use turso_parser::ast::{CompoundOperator, Distinctness, Materialized, NullsOrder, SortOrder};

use super::{
    BoundColumnTypePrograms, ColumnRef, CteId, DatabaseId, ExprId, OutputId, QueryBlockId, QueryId,
    ResolvedIndex, ResolvedTable, ResolvedWindow, SourceId, TypeFact,
};
use crate::vdbe::affinity::Affinity;

#[derive(Clone, Debug)]
pub struct Query {
    pub id: QueryId,
    /// Lexical query whose scope this query may capture. Queries without an
    /// enclosing query scope have no parent.
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

impl Query {
    /// Whether planning moved this query's sources and terms into the query
    /// block that ran it, leaving it empty: no expression runs it anymore.
    /// Semantic analysis never makes a query without outputs.
    pub(crate) fn is_detached(&self) -> bool {
        self.output.is_empty()
            && self.parent.is_none()
            && self.captures.is_empty()
            && matches!(
                self.blocks.as_slice(),
                [QueryBlock {
                    from: None,
                    body: QueryBlockBody::Select {
                        filter: None,
                        grouping: None,
                        ..
                    },
                    ..
                }]
            )
            && self.blocks[0].outputs.is_empty()
    }
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
    /// Effective windows referenced by functions in this block.
    pub windows: Vec<ResolvedWindow>,
    pub body: QueryBlockBody,
}

impl QueryBlock {
    pub(crate) fn new(id: QueryBlockId, body: QueryBlockBody) -> Self {
        Self {
            id,
            from: None,
            outputs: Vec::new(),
            aggregate_count: 0,
            window_function_count: 0,
            windows: Vec::new(),
            body,
        }
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum QueryBlockBody {
    Select {
        distinctness: Option<Distinctness>,
        filter: Option<ExprId>,
        grouping: Option<Grouping>,
    },
    Values {
        rows: Vec<Vec<ExprId>>,
    },
}

#[derive(Clone, Debug)]
pub struct Output {
    pub id: OutputId,
    pub name: crate::sync::Arc<str>,
    pub expr: ExprId,
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
    pub keys: Vec<ExprId>,
    /// Type and custom-comparison facts aligned with `keys`.
    pub key_type_facts: Vec<TypeFact>,
    /// Resolved SQL collations aligned with `keys`.
    pub key_collations: Vec<Option<super::ResolvedCollation>>,
    pub having: Option<ExprId>,
}

#[derive(Clone, Debug)]
pub struct Limit {
    pub limit: ExprId,
    pub offset: Option<ExprId>,
}

/// A non-empty resolved FROM clause.
#[derive(Clone, Debug)]
pub struct From {
    pub first: SourceId,
    pub joins: Vec<Join>,
}

impl From {
    /// Return the source's position in the original FROM order used by join
    /// planning masks.
    pub(crate) fn source_position(&self, source: SourceId) -> Option<usize> {
        if self.first == source {
            return Some(0);
        }
        self.joins
            .iter()
            .position(|join| join.right == source)
            .map(|position| position + 1)
    }

    /// Whether an outer join can supply NULLs for this source's columns.
    ///
    /// LEFT affects its right source, RIGHT affects every source already on
    /// its left, and FULL affects both sides.
    pub(crate) fn outer_join_may_null_extend(&self, source: SourceId) -> bool {
        let Some(source_position) = self.source_position(source) else {
            return false;
        };
        self.joins.iter().enumerate().any(|(join_position, join)| {
            let right_position = join_position + 1;
            match join.kind {
                JoinKind::Left => source_position == right_position,
                JoinKind::Right => source_position < right_position,
                JoinKind::Full => source_position <= right_position,
                JoinKind::Comma
                | JoinKind::Inner
                | JoinKind::Cross
                | JoinKind::Semi
                | JoinKind::Anti => false,
            }
        })
    }

    /// Whether a FULL JOIN can supply NULLs for this source's columns.
    ///
    /// This is separate because FULL JOIN emits unmatched rows without
    /// re-checking constraints consumed by the source scan.
    pub(crate) fn full_join_may_null_extend(&self, source: SourceId) -> bool {
        let Some(source_position) = self.source_position(source) else {
            return false;
        };
        self.joins.iter().enumerate().any(|(join_position, join)| {
            join.kind == JoinKind::Full && source_position <= join_position + 1
        })
    }
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
    /// Keeps a row of the sources on its left when at least one row of its
    /// right source matches. Never written in SQL: the planner makes it from
    /// a correlated `EXISTS` or `IN` in `semantic_unnest.rs`.
    /// Its right source comes after every other source of the FROM clause.
    Semi,
    /// Keeps a row of the sources on its left when no row of its right
    /// source matches. The planner makes it from a correlated `NOT EXISTS`.
    Anti,
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum JoinConstraint {
    None,
    On(ExprId),
    Using(Vec<UsingColumn>),
    Natural(Vec<UsingColumn>),
}

/// The two source columns merged into one visible column by USING or NATURAL.
#[derive(Clone, Debug)]
pub struct UsingColumn {
    pub name: String,
    /// The visible value from the already-built left side. It may represent
    /// an earlier USING/NATURAL merge.
    pub left: ExprId,
    pub right: ColumnRef,
    pub value: MergedColumnValue,
    pub type_fact: TypeFact,
    pub affinity: Affinity,
    pub has_affinity: bool,
    pub collation: Option<super::ResolvedCollation>,
    pub comparison: super::ComparisonSemantics,
    /// `left = right`, the term the join plans.
    pub equality: ExprId,
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
    Root,
}

#[derive(Clone, Debug)]
pub struct CheckConstraint {
    /// Position in the frozen catalog table's CHECK list.
    pub catalog_position: usize,
    /// CHECK expression instantiated for one exact source occurrence.
    pub expression: ExprId,
    /// What a failure reports: the constraint's name, or its expression's
    /// text as the user wrote it.
    pub description: String,
    pub name: Option<String>,
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
    Planned(ExprId),
}

/// One value per column of a source. While every column holds the empty
/// value, no list is stored; the first write to a column stores one.
#[derive(Clone, Debug)]
pub struct ColumnSlots<T: EmptySlot> {
    width: usize,
    values: Vec<T>,
}

pub trait EmptySlot: Clone + 'static {
    const EMPTY: &'static Self;
}

impl EmptySlot for ColumnReadExpression {
    const EMPTY: &'static Self = &ColumnReadExpression::Absent;
}

impl EmptySlot for Option<Box<BoundColumnTypePrograms>> {
    const EMPTY: &'static Self = &None;
}

impl<T: EmptySlot> ColumnSlots<T> {
    pub fn empty(width: usize) -> Self {
        Self {
            width,
            values: Vec::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.width
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.width {
            return None;
        }
        Some(self.values.get(index).unwrap_or(T::EMPTY))
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + Clone + '_ {
        (0..self.width).map(|index| &self[index])
    }

    #[cfg(test)]
    pub fn stores_values(&self) -> bool {
        !self.values.is_empty()
    }

    #[cfg(test)]
    pub fn truncate(&mut self, width: usize) {
        self.width = self.width.min(width);
        self.values.truncate(self.width);
    }

    #[cfg(test)]
    pub fn pop(&mut self) -> Option<T> {
        let index = self.width.checked_sub(1)?;
        let value = self.get(index).cloned();
        self.truncate(index);
        value
    }
}

impl<T: EmptySlot> std::ops::Index<usize> for ColumnSlots<T> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &T {
        self.get(index)
            .unwrap_or_else(|| panic!("source has no column {index}"))
    }
}

impl<T: EmptySlot> std::ops::IndexMut<usize> for ColumnSlots<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        assert!(index < self.width, "source has no column {index}");
        if self.values.is_empty() {
            self.values = vec![T::EMPTY.clone(); self.width];
        }
        &mut self.values[index]
    }
}

impl<T: EmptySlot> std::convert::From<Vec<T>> for ColumnSlots<T> {
    fn from(values: Vec<T>) -> Self {
        Self {
            width: values.len(),
            values,
        }
    }
}

impl<T: EmptySlot> std::iter::FromIterator<T> for ColumnSlots<T> {
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        values.into_iter().collect::<Vec<_>>().into()
    }
}

const _: () =
    assert!(std::mem::size_of::<ColumnReadExpression>() == 2 * std::mem::size_of::<u32>());
const _: () = assert!(
    std::mem::size_of::<Option<Box<BoundColumnTypePrograms>>>() == std::mem::size_of::<usize>()
);

#[derive(Clone, Debug)]
pub struct Source {
    pub id: SourceId,
    pub owner: SourceOwner,
    pub database: Option<DatabaseId>,
    pub name: String,
    pub alias: Option<String>,
    pub kind: SourceKind,
    pub columns: SourceColumns,
    /// Virtual generated-column expressions for this exact source occurrence,
    /// aligned with `columns` and materialized when the statement needs them.
    pub generated_expressions: ColumnSlots<ColumnReadExpression>,
    /// Read-time defaults for short records, aligned with `columns` and
    /// materialized when the statement needs them.
    pub default_expressions: ColumnSlots<ColumnReadExpression>,
    /// Custom-type encode/decode programs aligned with `columns`. Plain and
    /// built-in typed columns contain `None`.
    pub column_type_programs: ColumnSlots<Option<Box<BoundColumnTypePrograms>>>,
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
        /// Resolved hidden-column constraints produced from call arguments.
        argument_predicates: Vec<ExprId>,
    },
    Cte(CteId),
    Derived(QueryId),
    FromGroup(FromGroup),
    RecursiveInput(CteId),
    Pseudo {
        kind: PseudoSource,
        table: ResolvedTable,
    },
}

#[derive(Clone, Debug)]
pub struct FromGroup {
    pub from: Box<From>,
    /// Value supplied by each group column, aligned with `Source::columns`.
    pub columns: Vec<ExprId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PseudoSource {
    Excluded,
    New,
    Old,
}

#[derive(Clone, Debug)]
pub struct SourceColumn {
    pub name: crate::sync::Arc<str>,
    pub type_fact: TypeFact,
    pub affinity: Affinity,
    pub has_affinity: bool,
    pub collation: Option<super::ResolvedCollation>,
    pub hidden: bool,
    pub rowid_alias: bool,
}

#[derive(Clone, Debug)]
pub enum SourceColumns {
    Table(TableColumns),
    Owned(crate::sync::Arc<[SourceColumn]>),
}

impl SourceColumns {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Table(columns) => columns.table.columns().len(),
            Self::Owned(columns) => columns.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<SourceColumnRef<'_>> {
        match self {
            Self::Table(columns) => {
                columns
                    .table
                    .columns()
                    .get(index)
                    .map(|column| SourceColumnRef::Table {
                        column,
                        columns,
                        index,
                    })
            }
            Self::Owned(columns) => columns.get(index).map(SourceColumnRef::Owned),
        }
    }

    #[inline]
    pub fn column(&self, index: usize) -> SourceColumnRef<'_> {
        self.get(index)
            .unwrap_or_else(|| panic!("source has no column {index}"))
    }

    #[inline]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = SourceColumnRef<'_>> + Clone + '_ {
        (0..self.len()).map(|index| self.column(index))
    }

    pub fn named_positions<'a>(&'a self, name: &'a str) -> impl Iterator<Item = usize> + 'a {
        let unique = matches!(self, Self::Table(columns) if matches!(columns.table.as_ref(), crate::schema::Table::BTree(_)));
        let (table, owned) = match self {
            Self::Table(columns) => (Some(columns.table.columns()), None),
            Self::Owned(columns) => (None, Some(columns.as_ref())),
        };
        table
            .into_iter()
            .flatten()
            .map(|column| {
                column
                    .name
                    .as_deref()
                    .expect("a table source only reads named columns from its schema")
            })
            .chain(owned.into_iter().flatten().map(|column| &*column.name))
            .enumerate()
            .filter_map(move |(position, column)| {
                column.eq_ignore_ascii_case(name).then_some(position)
            })
            .take(if unique { 1 } else { usize::MAX })
    }
}

impl std::iter::FromIterator<SourceColumn> for SourceColumns {
    fn from_iter<I: IntoIterator<Item = SourceColumn>>(columns: I) -> Self {
        Self::Owned(columns.into_iter().collect())
    }
}

impl std::convert::From<Vec<SourceColumn>> for SourceColumns {
    fn from(columns: Vec<SourceColumn>) -> Self {
        Self::Owned(columns.into())
    }
}

impl std::convert::From<crate::sync::Arc<[SourceColumn]>> for SourceColumns {
    fn from(columns: crate::sync::Arc<[SourceColumn]>) -> Self {
        Self::Owned(columns)
    }
}

#[derive(Clone, Debug)]
pub struct TableColumns {
    table: crate::sync::Arc<crate::schema::Table>,
    type_facts: Option<crate::sync::Arc<crate::sync::OnceLock<Box<[TypeFact]>>>>,
}

impl TableColumns {
    pub fn new(table: crate::sync::Arc<crate::schema::Table>) -> Self {
        let type_facts = match table.as_ref() {
            crate::schema::Table::BTree(_) => None,
            _ => Some(crate::sync::Arc::new(crate::sync::OnceLock::new())),
        };
        Self { table, type_facts }
    }

    #[inline]
    fn type_fact(&self, index: usize) -> &TypeFact {
        match (self.table.as_ref(), &self.type_facts) {
            (crate::schema::Table::BTree(table), _) => &table.declared_type_facts()[index],
            (table, Some(type_facts)) => {
                &type_facts.get_or_init(|| declared_type_facts(table.columns()))[index]
            }
            (_, None) => unreachable!("only a B-tree table keeps its type facts in the schema"),
        }
    }
}

pub(crate) fn declared_type_facts(columns: &[crate::schema::Column]) -> Box<[TypeFact]> {
    let mut declarations: Vec<crate::sync::Arc<super::DeclaredType>> = Vec::new();
    columns
        .iter()
        .map(|column| {
            if column.ty_str.is_empty() {
                return TypeFact::known(column.ty());
            }
            let same_type = |declared: &&crate::sync::Arc<super::DeclaredType>| {
                declared.name == column.ty_str
                    && declared.storage == column.ty()
                    && declared.array_dimensions == column.array_dimensions()
            };
            let declared = match declarations.iter().find(same_type) {
                Some(declared) => declared.clone(),
                None => {
                    let declared = crate::sync::Arc::new(super::DeclaredType {
                        name: column.ty_str.clone(),
                        storage: column.ty(),
                        custom_chain: Vec::new(),
                        array_dimensions: column.array_dimensions(),
                    });
                    declarations.push(declared.clone());
                    declared
                }
            };
            TypeFact::shared_declared(declared)
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
pub enum SourceColumnRef<'a> {
    Table {
        column: &'a crate::schema::Column,
        columns: &'a TableColumns,
        index: usize,
    },
    Owned(&'a SourceColumn),
}

impl<'a> SourceColumnRef<'a> {
    pub fn shared_name(self) -> crate::sync::Arc<str> {
        match self {
            Self::Table { columns, index, .. } => match columns.table.as_ref() {
                crate::schema::Table::BTree(table) => table.shared_column_names()[index].clone(),
                _ => self.name().into(),
            },
            Self::Owned(column) => crate::sync::Arc::from(&*column.name),
        }
    }

    #[inline]
    pub fn name(self) -> &'a str {
        match self {
            Self::Table { column, .. } => column
                .name
                .as_deref()
                .expect("a table source only reads named columns from its schema"),
            Self::Owned(column) => &column.name,
        }
    }

    #[inline]
    pub fn type_fact(self) -> std::borrow::Cow<'a, TypeFact> {
        match self {
            Self::Table { columns, index, .. } => {
                std::borrow::Cow::Borrowed(columns.type_fact(index))
            }
            Self::Owned(column) => std::borrow::Cow::Borrowed(&column.type_fact),
        }
    }

    #[inline]
    pub fn affinity(self) -> Affinity {
        match self {
            Self::Table { column, .. } => column.affinity(),
            Self::Owned(column) => column.affinity,
        }
    }

    #[inline]
    pub fn has_affinity(self) -> bool {
        match self {
            Self::Table { .. } => true,
            Self::Owned(column) => column.has_affinity,
        }
    }

    #[inline]
    pub fn collation(self) -> Option<&'a super::ResolvedCollation> {
        match self {
            Self::Table { .. } => None,
            Self::Owned(column) => column.collation.as_ref(),
        }
    }

    #[inline]
    pub fn hidden(self) -> bool {
        match self {
            Self::Table { column, .. } => column.hidden(),
            Self::Owned(column) => column.hidden,
        }
    }

    #[inline]
    pub fn rowid_alias(self) -> bool {
        match self {
            Self::Table { column, .. } => column.is_rowid_alias(),
            Self::Owned(column) => column.rowid_alias,
        }
    }

    pub fn to_owned(self) -> SourceColumn {
        match self {
            Self::Table { .. } => SourceColumn {
                name: self.name().into(),
                type_fact: self.type_fact().into_owned(),
                affinity: self.affinity(),
                has_affinity: self.has_affinity(),
                collation: None,
                hidden: self.hidden(),
                rowid_alias: self.rowid_alias(),
            },
            Self::Owned(column) => column.clone(),
        }
    }
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
    pub columns: Vec<Option<ExprId>>,
    pub predicate: Option<ExprId>,
}

#[derive(Clone, Debug)]
pub struct IndexMethodPattern {
    pub id: super::IndexMethodPatternId,
    pub index: ResolvedIndex,
    pub outputs: Vec<Output>,
    pub predicate: Option<ExprId>,
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
#[allow(clippy::large_enum_variant)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn from_with_joins(kinds: &[JoinKind]) -> From {
        From {
            first: SourceId::new(0),
            joins: kinds
                .iter()
                .enumerate()
                .map(|(position, kind)| Join {
                    right: SourceId::new(position + 1),
                    kind: *kind,
                    constraint: JoinConstraint::None,
                })
                .collect(),
        }
    }

    #[test]
    fn left_join_null_extends_only_its_right_source() {
        let from = from_with_joins(&[JoinKind::Left, JoinKind::Inner]);

        assert!(!from.outer_join_may_null_extend(SourceId::new(0)));
        assert!(from.outer_join_may_null_extend(SourceId::new(1)));
        assert!(!from.outer_join_may_null_extend(SourceId::new(2)));
        assert!(!from.full_join_may_null_extend(SourceId::new(1)));
    }

    #[test]
    fn right_join_null_extends_sources_to_its_left() {
        let from = from_with_joins(&[JoinKind::Right]);

        assert!(from.outer_join_may_null_extend(SourceId::new(0)));
        assert!(!from.outer_join_may_null_extend(SourceId::new(1)));
        assert!(!from.full_join_may_null_extend(SourceId::new(0)));
    }

    #[test]
    fn full_join_null_extends_both_sides_but_not_later_sources() {
        let from = from_with_joins(&[JoinKind::Full, JoinKind::Inner]);

        assert!(from.outer_join_may_null_extend(SourceId::new(0)));
        assert!(from.outer_join_may_null_extend(SourceId::new(1)));
        assert!(!from.outer_join_may_null_extend(SourceId::new(2)));
        assert!(from.full_join_may_null_extend(SourceId::new(0)));
        assert!(from.full_join_may_null_extend(SourceId::new(1)));
        assert!(!from.full_join_may_null_extend(SourceId::new(2)));
    }

    #[test]
    fn later_full_join_null_extends_every_source_already_present() {
        let from = from_with_joins(&[JoinKind::Inner, JoinKind::Full]);

        for source in 0..=2 {
            assert!(from.outer_join_may_null_extend(SourceId::new(source)));
            assert!(from.full_join_may_null_extend(SourceId::new(source)));
        }
        assert!(!from.outer_join_may_null_extend(SourceId::new(3)));
        assert!(!from.full_join_may_null_extend(SourceId::new(3)));
    }
}
