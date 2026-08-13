//! Resolved statement and trigger roots.

use crate::{alloc::BoxedSlice, schema::ForeignKey, sync::Arc};
use turso_parser::ast::{ResolveType, SortOrder};

use super::{
    Expr, From, Limit, OrderTerm, Output, QueryId, ResolvedCollation, ResolvedIndex, ResolvedTable,
    ResolvedTrigger, SourceId,
};

#[derive(Clone, Debug)]
pub struct CdcPlan {
    pub info: crate::CaptureDataChangesInfo,
    pub table: ResolvedTable,
    pub sequence: Option<super::SequenceOperation>,
}

#[derive(Clone, Debug)]
pub enum HirRoot {
    Query(QueryRoot),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Trigger(TriggerRoot),
    SchemaExpressions(SchemaExpressionRoot),
}

/// A closed batch of stored expressions sharing one positional source.
#[derive(Clone, Debug)]
pub struct SchemaExpressionRoot {
    pub source: SourceId,
    pub expressions: Vec<Expr>,
}

#[derive(Clone, Debug)]
pub struct QueryRoot {
    pub query: QueryId,
}

#[derive(Clone, Debug)]
pub struct TriggerRoot {
    pub environment: TriggerEnvironment,
    pub body: TriggerBody,
}

#[derive(Clone, Debug)]
pub enum TriggerBody {
    Predicate(Expr),
    Command(TriggerCommand),
    Program(TriggerProgram),
}

#[derive(Clone, Debug)]
pub struct TriggerProgram {
    pub predicate: Option<Expr>,
    pub commands: Vec<TriggerCommand>,
}

#[derive(Clone, Debug)]
pub enum TriggerCommand {
    Select(QueryId),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

/// Pseudo-sources visible while analyzing one trigger command or predicate.
#[derive(Clone, Debug)]
pub struct TriggerEnvironment {
    pub table: ResolvedTable,
    pub new_source: Option<SourceId>,
    pub old_source: Option<SourceId>,
}

#[derive(Clone, Debug)]
pub struct Insert {
    pub target: SourceId,
    pub target_kind: InsertTargetKind,
    pub columns: Vec<InsertTarget>,
    pub source: InsertSource,
    pub conflict: Option<ResolveType>,
    pub upserts: Vec<Upsert>,
    pub excluded_source: Option<SourceId>,
    pub returning: Option<Returning>,
}

/// Metadata required by the concrete kind of INSERT destination.
#[derive(Clone, Debug)]
pub enum InsertTargetKind {
    BTree {
        autoincrement: Option<ResolvedAutoincrement>,
        defaults: Vec<ResolvedDefault>,
        triggers: InsertTriggers,
        foreign_keys: DmlForeignKeys,
    },
    Virtual,
}

/// Trigger identities selected for an INSERT and its possible UPSERT update.
#[derive(Clone, Debug, Default)]
pub struct InsertTriggers {
    pub insert: Vec<ResolvedTrigger>,
    pub upsert_update: Vec<ResolvedTrigger>,
}

/// Catalog objects needed to allocate and persist an AUTOINCREMENT key.
#[derive(Clone, Debug)]
pub struct ResolvedAutoincrement {
    /// SQLite-compatible sequence storage required in every journal mode.
    pub sqlite_sequence: ResolvedTable,
    /// Hidden MVCC sequence used instead of scanning sqlite_sequence for key
    /// allocation. Present only when the target database exposes that
    /// sequence in the semantic snapshot.
    pub mvcc_sequence: Option<super::SequenceOperation>,
}

#[derive(Clone, Debug)]
pub struct ResolvedDefault {
    pub column: usize,
    pub value: Expr,
}

/// One INSERT target position and whether its paired source value supplies it.
///
/// SQLite keeps duplicate targets for arity and expression analysis. Ordinary
/// columns use their first value, while rowid targets use their last value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InsertTarget {
    pub column: TargetColumn,
    pub uses_value: bool,
}

#[derive(Clone, Debug)]
pub enum InsertSource {
    DefaultValues,
    Values(Vec<Vec<Expr>>),
    Query(QueryId),
}

#[derive(Clone, Debug)]
pub struct Upsert {
    /// `None` is the final catch-all ON CONFLICT clause.
    pub target: Option<ConflictTarget>,
    pub action: UpsertAction,
}

#[derive(Clone, Debug)]
pub struct ConflictTarget {
    pub terms: Vec<ConflictTerm>,
    pub predicate: Option<Expr>,
    pub matched_index: Option<ResolvedIndex>,
}

#[derive(Clone, Debug)]
pub struct ConflictTerm {
    pub expr: Expr,
    pub collation: Option<ResolvedCollation>,
    pub order: SortOrder,
}

#[derive(Clone, Debug)]
pub enum UpsertAction {
    Nothing,
    Update {
        assignments: Vec<Assignment>,
        predicate: Option<Expr>,
    },
}

#[derive(Clone, Debug)]
pub struct Assignment {
    pub columns: Vec<TargetColumn>,
    pub value: Expr,
}

/// A writable destination in a DML target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetColumn {
    Column(usize),
    RowId,
}

#[derive(Clone, Debug)]
pub struct Update {
    /// OLD row identity used by predicates, assignment inputs, and index removal.
    pub target: SourceId,
    /// NEW row identity used by generated values, constraints, index insertion,
    /// and RETURNING.
    pub new_source: SourceId,
    pub target_kind: UpdateTargetKind,
    pub from: Option<From>,
    pub assignments: Vec<Assignment>,
    pub predicate: Option<Expr>,
    pub order_by: Vec<OrderTerm>,
    pub limit: Option<Limit>,
    pub conflict: Option<ResolveType>,
    pub returning: Option<Returning>,
    /// For an internal sqlite_schema update, CDC stores the user's DDL text in
    /// the changed `sql` field instead of the generated UPDATE statement.
    pub cdc_updates_override: Option<(usize, String)>,
}

/// Metadata required by the concrete kind of UPDATE destination.
#[derive(Clone, Debug)]
pub enum UpdateTargetKind {
    BTree {
        defaults: Vec<ResolvedDefault>,
        triggers: Vec<ResolvedTrigger>,
        foreign_keys: DmlForeignKeys,
    },
    Virtual,
}

#[derive(Clone, Debug)]
pub struct Delete {
    pub target: SourceId,
    pub target_kind: DeleteTargetKind,
    pub predicate: Option<Expr>,
    pub order_by: Vec<OrderTerm>,
    pub limit: Option<Limit>,
    pub returning: Option<Returning>,
}

/// Metadata required by the concrete kind of DELETE destination.
#[derive(Clone, Debug)]
pub enum DeleteTargetKind {
    BTree {
        triggers: Vec<ResolvedTrigger>,
        foreign_keys: DmlForeignKeys,
    },
    Virtual,
}

/// Foreign-key identities and positions frozen for one DML target.
#[derive(Clone, Debug, Default)]
pub struct DmlForeignKeys {
    /// Constraints declared by the target, where it is the child table.
    pub outgoing: Vec<ResolvedForeignKey>,
    /// Constraints declared by other tables, where the target is the parent.
    pub incoming: Vec<ResolvedForeignKey>,
}

#[derive(Clone, Debug)]
pub struct ResolvedForeignKey {
    pub child_table: ResolvedTable,
    /// Exact source occurrence used when a parent mutation scans child keys.
    /// Generated child columns are closed against this identity.
    pub child_source: SourceId,
    pub parent_table: ResolvedTable,
    pub declaration: Arc<ForeignKey>,
    pub parent_columns: Box<[String]>,
    pub child_positions: BoxedSlice<usize>,
    pub parent_positions: BoxedSlice<usize>,
    pub parent_uses_rowid: bool,
    pub parent_unique_index: Option<ResolvedIndex>,
    /// This generated CASCADE update copies the key of a parent row that the
    /// calling mutation has already created.
    pub parent_action_guarantees_new_parent: bool,
}

#[derive(Clone, Debug)]
pub struct Returning {
    pub outputs: Vec<Output>,
}

/// The final type fact for a writable DML destination.
#[derive(Clone, Debug)]
pub struct DestinationType {
    pub column: TargetColumn,
    pub type_fact: super::TypeFact,
}
